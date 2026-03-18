import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from jq_snapshot_core import build_daily_base_data, build_jq_yanoshin_snapshot, get_settings
from snapshot_bundle import bundle_to_json_text, bundle_to_markdown
from snapshot_store import resolve_snapshot_backend, write_snapshot_bundle


ROOT_DIR = Path(__file__).resolve().parent
logger = logging.getLogger("collector_jq_yanoshin")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="J-Quants + Yanoshin snapshot collector")
    parser.add_argument("--mode", choices=["now", "0915", "1130", "1530"], required=True)
    parser.add_argument("--date", help="Snapshot base date in YYYY-MM-DD")
    parser.add_argument("--backend", choices=["local", "gcs"], help="Snapshot backend override")
    return parser.parse_args()


def run_collector(*, mode: str, snapshot_date: str | None = None, backend: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    if backend:
        settings["SNAPSHOT_BACKEND"] = backend
    resolved_backend, backend_warning = resolve_snapshot_backend(settings)
    base_df, base_meta = build_daily_base_data(as_of_date=snapshot_date)
    bundle = build_jq_yanoshin_snapshot(
        mode,
        base_df,
        now_ts=datetime.now(),
        source_profile="cloud_jq_yanoshin",
        snapshot_backend=resolved_backend,
    )
    bundle["meta"]["snapshot_date"] = snapshot_date or str(base_meta.get("latest_date", ""))
    bundle["diagnostics"]["base_meta"] = base_meta
    json_text = bundle_to_json_text(bundle)
    markdown_text = bundle_to_markdown(bundle)
    store_result = write_snapshot_bundle(
        mode=mode,
        generated_at=str(bundle["meta"]["generated_at"]),
        json_text=json_text,
        markdown_text=markdown_text,
        settings=settings,
        root_dir=ROOT_DIR,
    )
    if store_result.backend_name != bundle["meta"]["snapshot_backend"]:
        bundle["meta"]["snapshot_backend"] = store_result.backend_name
        json_text = bundle_to_json_text(bundle)
        markdown_text = bundle_to_markdown(bundle)
        store_result = write_snapshot_bundle(
            mode=mode,
            generated_at=str(bundle["meta"]["generated_at"]),
            json_text=json_text,
            markdown_text=markdown_text,
            settings=settings,
            root_dir=ROOT_DIR,
        )
    bundle["paths"] = store_result.paths
    bundle["snapshot_source_label"] = store_result.source_label
    bundle["snapshot_warning_message"] = store_result.warning_message or backend_warning
    logger.info("collector completed mode=%s backend=%s paths=%s", mode, store_result.backend_name, store_result.paths)
    if bundle["snapshot_warning_message"]:
        logger.warning("collector backend warning: %s", bundle["snapshot_warning_message"])
    return bundle


def main() -> int:
    args = parse_args()
    try:
        bundle = run_collector(mode=args.mode, snapshot_date=args.date, backend=args.backend)
    except Exception as exc:
        logger.exception("collector failed: %s", exc)
        return 1
    print(bundle["snapshot_source_label"])
    if bundle["snapshot_warning_message"]:
        print(f"warning: {bundle['snapshot_warning_message']}")
    for key, value in bundle["paths"].items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
