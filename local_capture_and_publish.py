import argparse
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sector_app_jq import (
    GITHUB_CONTROL_TOKEN_SECRET_NAME,
    ROOT_DIR,
    _short_body,
    bundle_to_json_text,
    bundle_to_markdown,
    get_github_control_config,
    get_settings,
    github_read_json_file,
    github_read_text_file,
    github_write_text_file,
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
    session: Any = None,
) -> None:
    logger.info("updating control-plane status: status=%s last_run_at=%s message=%s", status, last_run_at, message)
    payload, sha = read_control_plane_status(token, settings, session=session)
    payload = dict(payload)
    payload.update({"last_run_at": last_run_at, "status": status, "message": message})
    write_control_plane_status(
        token,
        payload,
        settings,
        sha=sha,
        session=session,
        message=f"Set collector status: {status}",
    )


def _clear_request(token: str, settings: dict[str, Any], *, session: Any = None, status: str = "idle") -> None:
    logger.info("clearing control-plane request: status=%s", status)
    payload, sha = read_control_plane_request(token, settings, session=session)
    payload = dict(payload)
    payload.update({"request_update": False, "status": status})
    write_control_plane_request(
        token,
        payload,
        settings,
        sha=sha,
        session=session,
        message=f"Clear update request after {status}",
    )


def _publish_deploy_snapshot(token: str, settings: dict[str, Any], bundle: dict[str, Any], *, session: Any = None) -> None:
    config = get_github_control_config(settings)
    json_path = config["deploy_snapshot_json_path"]
    md_path = config["deploy_snapshot_md_path"]
    logger.info("publishing deploy snapshot: branch=%s json_path=%s md_path=%s", config["deploy_branch"], json_path, md_path)

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
        bundle_to_json_text(bundle),
        "Publish latest_1130 snapshot from local collector",
        sha=json_sha,
        session=session,
    )
    github_write_text_file(
        config["repository"],
        config["deploy_branch"],
        md_path,
        token,
        bundle_to_markdown(bundle),
        "Publish latest_1130 snapshot markdown from local collector",
        sha=md_sha,
        session=session,
    )


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

    started_at = _utc_now()
    logger.info("update request accepted: requested_at=%s requested_by=%s", request_payload.get("requested_at", ""), request_payload.get("requested_by", ""))
    try:
        _update_status(token, settings, status="running", message="Fast snapshot refresh is running.", last_run_at=started_at, session=session)
        logger.info("starting run_cli for mode=1130 fast_check=True")
        bundle = runner(mode="1130", write_drive=False, fast_check=True)
        _publish_deploy_snapshot(token, settings, bundle, session=session)
        finished_at = _utc_now()
        summary = f"latest_1130.json updated at {finished_at}"
        _update_status(token, settings, status="success", message=summary, last_run_at=finished_at, session=session)
        _clear_request(token, settings, session=session, status="success")
        logger.info("update request completed successfully")
        return {"handled": True, "status": "success", "message": summary}
    except Exception as exc:
        failed_at = _utc_now()
        summary = _short_body(str(exc), limit=180)
        logger.exception("update request failed: %s", summary)
        try:
            _update_status(token, settings, status="failed", message=summary, last_run_at=failed_at, session=session)
            _clear_request(token, settings, session=session, status="failed")
        except Exception:
            pass
        return {"handled": True, "status": "failed", "message": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll control-plane update request and publish latest_1130 snapshot.")
    parser.add_argument("--force", action="store_true", help="Ignore request flag and run once immediately.")
    args = parser.parse_args()

    try:
        with single_instance_lock(LOCK_PATH):
            if args.force:
                settings = get_settings()
                token = _github_token()
                if not token:
                    raise RuntimeError(f"{GITHUB_CONTROL_TOKEN_SECRET_NAME} is missing.")
                started_at = _utc_now()
                try:
                    logger.info("forced snapshot refresh started")
                    _update_status(token, settings, status="running", message="Forced fast snapshot refresh is running.", last_run_at=started_at)
                    bundle = run_cli(mode="1130", write_drive=False, fast_check=True)
                    _publish_deploy_snapshot(token, settings, bundle)
                    finished_at = _utc_now()
                    _update_status(token, settings, status="success", message=f"Forced latest_1130.json updated at {finished_at}", last_run_at=finished_at)
                    _clear_request(token, settings, status="success")
                    logger.info("forced snapshot refresh completed successfully")
                    print("forced update completed")
                    return 0
                except Exception as exc:
                    failed_at = _utc_now()
                    summary = _short_body(str(exc), limit=180)
                    logger.exception("forced snapshot refresh failed: %s", summary)
                    try:
                        _update_status(token, settings, status="failed", message=summary, last_run_at=failed_at)
                        _clear_request(token, settings, status="failed")
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
