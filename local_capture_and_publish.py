import argparse
import logging
import subprocess
from pathlib import Path
from typing import Sequence

from sector_app_jq import run_cli


ROOT_DIR = Path(__file__).resolve().parent
TARGET_BRANCH = "deploy/streamlit-live"
logger = logging.getLogger("local_capture_and_publish")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _parse_bool(value: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {value}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture local snapshot and optionally publish latest files")
    parser.add_argument("--mode", choices=["0915", "1130", "1530", "now"], required=True)
    parser.add_argument("--push", type=_parse_bool, default=True)
    parser.add_argument("--allow-non-true-timepoint", type=_parse_bool, default=False)
    return parser.parse_args()


def _run_git(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=ROOT_DIR, text=True, capture_output=True, check=True)


def _latest_snapshot_paths(bundle: dict) -> list[str]:
    paths = bundle.get("paths", {})
    latest_json = str(paths.get("latest_json", "")).strip()
    latest_md = str(paths.get("latest_md", "")).strip()
    relative_paths: list[str] = []
    for raw_path in [latest_json, latest_md]:
        if not raw_path:
            continue
        path = Path(raw_path)
        relative_paths.append(str(path.relative_to(ROOT_DIR)))
    return relative_paths


def run_capture_and_publish(*, mode: str, push: bool = True, allow_non_true_timepoint: bool = False) -> dict:
    if push:
        _run_git(["switch", TARGET_BRANCH])
    bundle = run_cli(mode=mode, write_drive=False, fast_check=False)
    if not push:
        logger.info("push=false のため Git publish をスキップします。")
        return bundle
    if not bool(bundle.get("meta", {}).get("is_true_timepoint")) and not allow_non_true_timepoint:
        logger.warning("is_true_timepoint=false のため push をスキップしました。")
        return bundle
    latest_paths = _latest_snapshot_paths(bundle)
    if len(latest_paths) != 2:
        raise RuntimeError("latest snapshot paths が不足しています。")
    _run_git(["add", "--", *latest_paths])
    diff_result = subprocess.run(["git", "diff", "--cached", "--quiet", "--exit-code"], cwd=ROOT_DIR, text=True)
    if diff_result.returncode == 0:
        logger.info("latest snapshot に差分がないため commit/push をスキップします。")
        return bundle
    if diff_result.returncode != 1:
        raise RuntimeError("git diff --cached --quiet に失敗しました。")
    commit_message = f"data: update latest_{mode} snapshot"
    _run_git(["commit", "-m", commit_message])
    _run_git(["push", "origin", TARGET_BRANCH])
    logger.info("publish 完了 branch=%s mode=%s", TARGET_BRANCH, mode)
    return bundle


def main() -> int:
    args = parse_args()
    try:
        run_capture_and_publish(mode=args.mode, push=args.push, allow_non_true_timepoint=args.allow_non_true_timepoint)
    except Exception as exc:
        logger.exception("capture/publish failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
