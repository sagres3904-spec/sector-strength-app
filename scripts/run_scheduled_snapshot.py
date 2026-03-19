import argparse
import logging
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sector_app_jq import run_cli


logger = logging.getLogger("run_scheduled_snapshot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a scheduled sector snapshot on this local PC.")
    parser.add_argument("--mode", choices=["0915", "1130", "1530"], required=True, help="Snapshot mode to capture.")
    parser.add_argument("--write-drive", action="store_true", help="Also write to the configured drive sync directory.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger.info("scheduled snapshot start mode=%s", args.mode)
    bundle = run_cli(mode=args.mode, write_drive=args.write_drive, fast_check=False)
    json_path = Path(str(bundle.get("paths", {}).get("json_path", ""))).resolve() if bundle.get("paths") else None
    logger.info("scheduled snapshot completed mode=%s json_path=%s", args.mode, json_path or "")
    print(f"scheduled snapshot completed mode={args.mode} json_path={json_path or ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
