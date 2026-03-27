import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from local_capture_and_publish import _github_token, publish_snapshot_bundle
from sector_app_jq import run_cli


logger = logging.getLogger("run_scheduled_snapshot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _build_task_log_path(mode: str, started_at: datetime) -> Path:
    log_dir = ROOT_DIR / "data" / "task_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = started_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return log_dir / f"scheduled_snapshot_{mode}_{timestamp}.log"


def _attach_task_log_handler(log_path: Path) -> logging.Handler:
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    return handler


def _detach_task_log_handler(handler: logging.Handler) -> None:
    root_logger = logging.getLogger()
    root_logger.removeHandler(handler)
    handler.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a scheduled sector snapshot on this local PC.")
    parser.add_argument("--mode", choices=["0915", "1130", "1530"], required=True, help="Snapshot mode to capture.")
    parser.add_argument("--write-drive", action="store_true", help="Also write to the configured drive sync directory.")
    parser.add_argument("--publish-after-success", action="store_true", help="Publish latest_{mode}.json/.md to the deploy branch after snapshot success.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started_at = datetime.now(timezone.utc)
    log_path = _build_task_log_path(args.mode, started_at)
    handler = _attach_task_log_handler(log_path)
    exit_code = 1
    json_path = ""
    publish_json_path = ""
    try:
        logger.info(
            "scheduled snapshot start mode=%s started_at=%s write_drive=%s publish_after_success=%s log_path=%s",
            args.mode,
            started_at.isoformat(),
            args.write_drive,
            args.publish_after_success,
            log_path,
        )
        bundle = run_cli(mode=args.mode, write_drive=args.write_drive, fast_check=False)
        json_path = str(bundle.get("paths", {}).get("json_path", ""))
        logger.info("scheduled snapshot success mode=%s json_path=%s", args.mode, json_path)
        if args.publish_after_success:
            token = _github_token()
            if not token:
                raise RuntimeError("GITHUB_CONTROL_TOKEN is missing.")
            logger.info("publish start mode=%s", args.mode)
            publish_result = publish_snapshot_bundle(token, {}, bundle, mode=args.mode)
            publish_json_path = str(publish_result.get("json_path", ""))
            logger.info(
                "publish success mode=%s publish_json_path=%s deploy_branch=%s",
                args.mode,
                publish_json_path,
                publish_result.get("deploy_branch", ""),
            )
        print(f"scheduled snapshot completed mode={args.mode} json_path={json_path}")
        exit_code = 0
        return 0
    except Exception:
        if args.publish_after_success and json_path:
            logger.exception("publish failed mode=%s", args.mode)
        logger.exception("scheduled snapshot failed mode=%s", args.mode)
        print(f"scheduled snapshot failed mode={args.mode} log_path={log_path}")
        exit_code = 1
        return 1
    finally:
        finished_at = datetime.now(timezone.utc)
        logger.info(
            "scheduled snapshot finish mode=%s started_at=%s finished_at=%s success=%s exit_code=%s json_path=%s publish_json_path=%s log_path=%s",
            args.mode,
            started_at.isoformat(),
            finished_at.isoformat(),
            exit_code == 0,
            exit_code,
            json_path,
            publish_json_path,
            log_path,
        )
        _detach_task_log_handler(handler)


if __name__ == "__main__":
    raise SystemExit(main())
