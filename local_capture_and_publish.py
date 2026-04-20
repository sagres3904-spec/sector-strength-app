import argparse
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sector_app_jq import (
    CLOUD_VIEWER_MODES,
    DEFAULT_CONTROL_PLANE_REQUEST_MODE,
    GITHUB_CONTROL_TOKEN_SECRET_NAME,
    ROOT_DIR,
    _short_body,
    _bundle_for_storage,
    _github_deploy_snapshot_json_path,
    _github_deploy_snapshot_md_path,
    bundle_to_json_text,
    bundle_to_markdown,
    get_github_control_config,
    get_settings,
    github_read_json_file,
    github_read_text_file,
    github_write_text_file,
    normalize_cloud_viewer_mode,
    read_control_plane_request,
    read_control_plane_status,
    run_cli,
    write_control_plane_request,
    write_control_plane_status,
)


LOCK_PATH = ROOT_DIR / "data" / "poll_update.lock"
LOCK_STALE_SECONDS = 15 * 60

logger = logging.getLogger("local_capture_and_publish")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _lock_payload() -> dict[str, Any]:
    return {"pid": os.getpid(), "created_at": datetime.now(timezone.utc).isoformat()}


def _read_lock_payload(lock_path: Path) -> dict[str, Any]:
    try:
        return json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _lock_age_seconds(lock_path: Path) -> float:
    return max(0.0, datetime.now().timestamp() - lock_path.stat().st_mtime)


def _pid_is_running(pid: Any) -> bool:
    try:
        pid_int = int(pid)
    except Exception:
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _cleanup_stale_lock(lock_path: Path, stale_seconds: int = LOCK_STALE_SECONDS) -> bool:
    if not lock_path.exists():
        return False
    age_seconds = _lock_age_seconds(lock_path)
    payload = _read_lock_payload(lock_path)
    pid_running = _pid_is_running(payload.get("pid"))
    if age_seconds < stale_seconds:
        logger.warning("lock exists and is still fresh: path=%s age_seconds=%.1f payload=%s", lock_path, age_seconds, payload)
        return False
    if pid_running:
        logger.warning("lock is stale by age but pid is still running: path=%s age_seconds=%.1f payload=%s", lock_path, age_seconds, payload)
        return False
    logger.warning("removing stale lock: path=%s age_seconds=%.1f payload=%s", lock_path, age_seconds, payload)
    try:
        lock_path.chmod(0o666)
    except Exception:
        pass
    try:
        lock_path.unlink(missing_ok=True)
        return True
    except PermissionError as exc:
        logger.error("failed to remove stale lock: path=%s error=%s", lock_path, exc)
        return False


@contextmanager
def single_instance_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(2):
        fd = None
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            payload = json.dumps(_lock_payload(), ensure_ascii=True)
            os.write(fd, payload.encode("utf-8"))
            logger.info("lock acquired: path=%s attempt=%s", lock_path, attempt + 1)
            try:
                yield
            finally:
                os.close(fd)
                lock_path.unlink(missing_ok=True)
                logger.info("lock released: path=%s", lock_path)
            return
        except FileExistsError:
            if fd is not None:
                os.close(fd)
            cleaned = _cleanup_stale_lock(lock_path)
            if cleaned:
                logger.info("retrying lock acquisition after stale lock cleanup: path=%s", lock_path)
                continue
            payload = _read_lock_payload(lock_path)
            age_seconds = _lock_age_seconds(lock_path) if lock_path.exists() else 0.0
            raise RuntimeError(f"lock already exists: {lock_path} payload={payload}")
    raise RuntimeError(f"failed to acquire lock after stale cleanup retry: {lock_path}")


def _github_token() -> str:
    return str(os.environ.get(GITHUB_CONTROL_TOKEN_SECRET_NAME, "")).strip()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_status(
    token: str,
    settings: dict[str, Any],
    *,
    status: str,
    message: str,
    last_run_at: str,
    request_mode: str = "",
    session: Any = None,
) -> None:
    logger.info("updating control-plane status: status=%s request_mode=%s last_run_at=%s message=%s", status, request_mode, last_run_at, message)
    payload, sha = read_control_plane_status(token, settings, session=session)
    payload = dict(payload)
    payload.update({"last_run_at": last_run_at, "status": status, "message": message, "request_mode": str(request_mode or "").strip()})
    write_control_plane_status(
        token,
        payload,
        settings,
        sha=sha,
        session=session,
        message=f"Set collector status: {status}",
    )


def _clear_request(token: str, settings: dict[str, Any], *, session: Any = None, status: str = "idle", request_mode: str = "") -> None:
    logger.info("clearing control-plane request: status=%s request_mode=%s", status, request_mode)
    payload, sha = read_control_plane_request(token, settings, session=session)
    payload = dict(payload)
    payload.update({"request_update": False, "request_mode": str(request_mode or "").strip(), "status": status})
    write_control_plane_request(
        token,
        payload,
        settings,
        sha=sha,
        session=session,
        message=f"Clear update request after {status}",
    )


def _load_local_snapshot_bundle(mode: str) -> dict[str, Any]:
    snapshot_path = ROOT_DIR / "data" / "snapshots" / f"latest_{mode}.json"
    if not snapshot_path.exists():
        raise FileNotFoundError(f"Local snapshot not found: {snapshot_path}")
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def _resolve_deploy_snapshot_paths(settings: dict[str, Any], mode: str) -> tuple[dict[str, str], str, str]:
    config = get_github_control_config(settings)
    normalized_mode = normalize_cloud_viewer_mode(mode)
    return config, _github_deploy_snapshot_json_path(normalized_mode, settings), _github_deploy_snapshot_md_path(normalized_mode, settings)


def _read_published_snapshot_meta(
    token: str,
    settings: dict[str, Any],
    mode: str,
    *,
    session: Any = None,
) -> dict[str, str]:
    config, json_path, _ = _resolve_deploy_snapshot_paths(settings, mode)
    payload, json_sha = github_read_json_file(config["repository"], config["deploy_branch"], json_path, token, session=session)
    meta = dict(payload.get("meta", {}) or {})
    return {
        "json_path": json_path,
        "json_sha": str(json_sha or "").strip(),
        "meta_mode": str(meta.get("mode", "") or "").strip(),
        "generated_at": str(meta.get("generated_at", "") or meta.get("generated_at_utc", "") or "").strip(),
        "generated_at_jst": str(meta.get("generated_at_jst", "") or "").strip(),
    }


def _verify_published_snapshot_target(
    token: str,
    settings: dict[str, Any],
    *,
    request_mode: str,
    bundle: dict[str, Any],
    publish_result: dict[str, str],
    session: Any = None,
) -> dict[str, str]:
    expected_mode = normalize_cloud_viewer_mode(request_mode)
    published_mode = normalize_cloud_viewer_mode(publish_result.get("mode", ""))
    if published_mode != expected_mode:
        raise RuntimeError(f"publish mode mismatch: requested={expected_mode} published={published_mode}")
    published_meta = _read_published_snapshot_meta(token, settings, expected_mode, session=session)
    stored_mode = normalize_cloud_viewer_mode(
        published_meta.get("meta_mode", ""),
        default=expected_mode,
    )
    if stored_mode != expected_mode:
        raise RuntimeError(f"published snapshot mode mismatch: requested={expected_mode} stored={stored_mode}")
    expected_generated_at = str(bundle.get("meta", {}).get("generated_at", "") or bundle.get("meta", {}).get("generated_at_utc", "") or "").strip()
    stored_generated_at = str(published_meta.get("generated_at", "") or "").strip()
    if expected_generated_at and stored_generated_at and stored_generated_at != expected_generated_at:
        raise RuntimeError(
            f"published snapshot timestamp mismatch: requested={expected_mode} expected_generated_at={expected_generated_at} stored_generated_at={stored_generated_at}"
        )
    published_meta["mode"] = expected_mode
    return published_meta


def publish_snapshot_bundle(token: str, settings: dict[str, Any], bundle: dict[str, Any], *, mode: str | None = None, session: Any = None) -> dict[str, str]:
    publish_mode = normalize_cloud_viewer_mode(mode or bundle.get("meta", {}).get("mode", ""))
    storage_bundle = _bundle_for_storage(bundle)
    config, json_path, md_path = _resolve_deploy_snapshot_paths(settings, publish_mode)
    logger.info("publishing deploy snapshot mode=%s branch=%s json_path=%s md_path=%s", publish_mode, config["deploy_branch"], json_path, md_path)

    json_sha = ""
    md_sha = ""
    try:
        _, json_sha = github_read_json_file(config["repository"], config["deploy_branch"], json_path, token, session=session)
    except FileNotFoundError:
        json_sha = ""
    try:
        _, md_sha = github_read_text_file(config["repository"], config["deploy_branch"], md_path, token, session=session)
    except FileNotFoundError:
        md_sha = ""

    github_write_text_file(
        config["repository"],
        config["deploy_branch"],
        json_path,
        token,
        bundle_to_json_text(storage_bundle),
        f"Publish latest_{publish_mode} snapshot from local collector",
        sha=json_sha,
        session=session,
    )
    github_write_text_file(
        config["repository"],
        config["deploy_branch"],
        md_path,
        token,
        bundle_to_markdown(bundle),
        f"Publish latest_{publish_mode} snapshot markdown from local collector",
        sha=md_sha,
        session=session,
    )
    return {"mode": publish_mode, "json_path": json_path, "markdown_path": md_path, "deploy_branch": config["deploy_branch"]}


def _publish_local_snapshot_mode(mode: str, *, session: Any = None) -> None:
    mode = normalize_cloud_viewer_mode(mode)
    settings = get_settings()
    token = _github_token()
    if not token:
        raise RuntimeError(f"{GITHUB_CONTROL_TOKEN_SECRET_NAME} is missing.")
    bundle = _load_local_snapshot_bundle(mode)
    publish_snapshot_bundle(token, settings, bundle, mode=mode, session=session)


def _run_cli_options(mode: str) -> tuple[bool, str]:
    mode = normalize_cloud_viewer_mode(mode)
    if mode == "0915":
        return False, "0915 snapshot refresh is running."
    if mode == "1130":
        return True, "1130 snapshot refresh is running."
    if mode == "1530":
        return False, "1530 snapshot refresh is running."
    if mode == "now":
        return False, "Now snapshot refresh is running."
    raise ValueError(f"Unsupported request_mode: {mode}")


def process_update_request(
    *,
    session: Any = None,
    runner: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    token = _github_token()
    if not token:
        raise RuntimeError(f"{GITHUB_CONTROL_TOKEN_SECRET_NAME} is missing.")

    runner = runner or run_cli
    logger.info("checking control-plane update request")
    request_payload, _ = read_control_plane_request(token, settings, session=session)
    if not bool(request_payload.get("request_update")):
        logger.info("no pending update request")
        return {"handled": False, "reason": "no_request"}
    request_mode_raw = str(request_payload.get("request_mode", "") or "").strip()
    if not request_mode_raw:
        failed_at = _utc_now()
        summary = "Missing request_mode in control-plane request."
        logger.error(summary)
        _update_status(token, settings, status="failed", message=summary, last_run_at=failed_at, request_mode="", session=session)
        _clear_request(token, settings, session=session, status="failed", request_mode="")
        return {"handled": True, "status": "failed", "message": summary, "request_mode": ""}
    request_mode = normalize_cloud_viewer_mode(request_mode_raw)
    try:
        fast_check, status_message = _run_cli_options(request_mode)
    except ValueError:
        failed_at = _utc_now()
        summary = f"Unsupported request_mode: {request_mode}"
        logger.error(summary)
        _update_status(token, settings, status="failed", message=summary, last_run_at=failed_at, request_mode=request_mode, session=session)
        _clear_request(token, settings, session=session, status="failed", request_mode=request_mode)
        return {"handled": True, "status": "failed", "message": summary, "request_mode": request_mode}

    started_at = _utc_now()
    logger.info("update request accepted: request_mode=%s requested_at=%s requested_by=%s", request_mode, request_payload.get("requested_at", ""), request_payload.get("requested_by", ""))
    try:
        _update_status(token, settings, status="running", message=status_message, last_run_at=started_at, request_mode=request_mode, session=session)
        logger.info("starting run_cli for mode=%s fast_check=%s", request_mode, fast_check)
        bundle = runner(mode=request_mode, write_drive=False, fast_check=fast_check)
        publish_result = publish_snapshot_bundle(token, settings, bundle, mode=request_mode, session=session)
        published_meta = _verify_published_snapshot_target(
            token,
            settings,
            request_mode=request_mode,
            bundle=bundle,
            publish_result=publish_result,
            session=session,
        )
        finished_at = _utc_now()
        summary = f"{published_meta['json_path'].split('/')[-1]} updated at {finished_at}"
        _update_status(token, settings, status="success", message=summary, last_run_at=finished_at, request_mode=request_mode, session=session)
        _clear_request(token, settings, session=session, status="success", request_mode=request_mode)
        logger.info("update request completed successfully")
        return {"handled": True, "status": "success", "message": summary, "request_mode": request_mode}
    except Exception as exc:
        failed_at = _utc_now()
        summary = _short_body(str(exc), limit=180)
        logger.exception("update request failed: %s", summary)
        try:
            _update_status(token, settings, status="failed", message=summary, last_run_at=failed_at, request_mode=request_mode, session=session)
            _clear_request(token, settings, session=session, status="failed", request_mode=request_mode)
        except Exception:
            pass
        return {"handled": True, "status": "failed", "message": summary, "request_mode": request_mode}


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll control-plane update requests and publish the requested latest_<mode> snapshot.")
    exclusive = parser.add_mutually_exclusive_group()
    exclusive.add_argument("--force", action="store_true", help=f"Ignore request flag and run once immediately using the default mode ({DEFAULT_CONTROL_PLANE_REQUEST_MODE}).")
    exclusive.add_argument("--force-mode", choices=list(CLOUD_VIEWER_MODES), help="Ignore request flag and run once immediately for the specified mode.")
    exclusive.add_argument("--publish-local-mode", choices=list(CLOUD_VIEWER_MODES), help="Publish an existing local snapshot for the specified mode to the deploy branch without touching control-plane status.")
    args = parser.parse_args()

    try:
        with single_instance_lock(LOCK_PATH):
            if args.publish_local_mode:
                try:
                    _publish_local_snapshot_mode(args.publish_local_mode)
                    print(f"published latest_{args.publish_local_mode} snapshot")
                    return 0
                except Exception as exc:
                    summary = _short_body(str(exc), limit=180)
                    logger.exception("publish-local-mode failed: %s", summary)
                    print(summary)
                    return 1
            force_mode = ""
            if args.force_mode:
                force_mode = normalize_cloud_viewer_mode(args.force_mode)
            elif args.force:
                force_mode = DEFAULT_CONTROL_PLANE_REQUEST_MODE
            if force_mode:
                settings = get_settings()
                token = _github_token()
                if not token:
                    raise RuntimeError(f"{GITHUB_CONTROL_TOKEN_SECRET_NAME} is missing.")
                started_at = _utc_now()
                try:
                    fast_check, status_message = _run_cli_options(force_mode)
                    logger.info("forced snapshot refresh started mode=%s fast_check=%s", force_mode, fast_check)
                    _update_status(token, settings, status="running", message=f"Forced {status_message[0].lower()}{status_message[1:]}", last_run_at=started_at, request_mode=force_mode)
                    bundle = run_cli(mode=force_mode, write_drive=False, fast_check=fast_check)
                    publish_result = publish_snapshot_bundle(token, settings, bundle, mode=force_mode)
                    published_meta = _verify_published_snapshot_target(
                        token,
                        settings,
                        request_mode=force_mode,
                        bundle=bundle,
                        publish_result=publish_result,
                    )
                    finished_at = _utc_now()
                    _update_status(
                        token,
                        settings,
                        status="success",
                        message=f"Forced {published_meta['json_path'].split('/')[-1]} updated at {finished_at}",
                        last_run_at=finished_at,
                        request_mode=force_mode,
                    )
                    _clear_request(token, settings, status="success", request_mode=force_mode)
                    logger.info("forced snapshot refresh completed successfully")
                    print(f"forced {force_mode} update completed")
                    return 0
                except Exception as exc:
                    failed_at = _utc_now()
                    summary = _short_body(str(exc), limit=180)
                    logger.exception("forced snapshot refresh failed: %s", summary)
                    try:
                        _update_status(token, settings, status="failed", message=summary, last_run_at=failed_at, request_mode=force_mode)
                        _clear_request(token, settings, status="failed", request_mode=force_mode)
                    except Exception:
                        pass
                    print(summary)
                    return 1
            result = process_update_request()
            print(result)
            return 0 if result.get("status") != "failed" else 1
    except RuntimeError as exc:
        logger.error("poller runtime error: %s", exc)
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
