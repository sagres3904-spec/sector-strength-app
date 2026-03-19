import argparse
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


@contextmanager
def single_instance_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = None
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("ascii"))
        yield
    except FileExistsError:
        raise RuntimeError(f"lock already exists: {lock_path}")
    finally:
        if fd is not None:
            os.close(fd)
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass


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
    request_payload, _ = read_control_plane_request(token, settings, session=session)
    if not bool(request_payload.get("request_update")):
        return {"handled": False, "reason": "no_request"}

    started_at = _utc_now()
    try:
        _update_status(token, settings, status="running", message="Fast snapshot refresh is running.", last_run_at=started_at, session=session)
        bundle = runner(mode="1130", write_drive=False, fast_check=True)
        _publish_deploy_snapshot(token, settings, bundle, session=session)
        finished_at = _utc_now()
        summary = f"latest_1130.json updated at {finished_at}"
        _update_status(token, settings, status="success", message=summary, last_run_at=finished_at, session=session)
        _clear_request(token, settings, session=session, status="success")
        return {"handled": True, "status": "success", "message": summary}
    except Exception as exc:
        failed_at = _utc_now()
        summary = _short_body(str(exc), limit=180)
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
                    _update_status(token, settings, status="running", message="Forced fast snapshot refresh is running.", last_run_at=started_at)
                    bundle = run_cli(mode="1130", write_drive=False, fast_check=True)
                    _publish_deploy_snapshot(token, settings, bundle)
                    finished_at = _utc_now()
                    _update_status(token, settings, status="success", message=f"Forced latest_1130.json updated at {finished_at}", last_run_at=finished_at)
                    _clear_request(token, settings, status="success")
                    print("forced update completed")
                    return 0
                except Exception as exc:
                    failed_at = _utc_now()
                    summary = _short_body(str(exc), limit=180)
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
        if "lock already exists" in str(exc):
            print(str(exc))
            return 0
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
