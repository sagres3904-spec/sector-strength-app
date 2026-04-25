import base64
import hashlib
import html
import json
import logging
import math
import os
import re
import time
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components


def _snapshot_dir(settings: dict[str, Any], root_dir: Path) -> Path:
    output_dir = str(settings.get("SNAPSHOT_OUTPUT_DIR", "data/snapshots")).strip() or "data/snapshots"
    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = root_dir / output_path
    return output_path


@dataclass
class _SnapshotStoreResult:
    paths: dict[str, str]
    source_label: str
    backend_name: str
    warning_message: str


try:
    from snapshot_bundle import bundle_to_json_text, bundle_to_markdown
    from snapshot_store import read_snapshot_json, write_snapshot_bundle as write_snapshot_bundle_to_store
    from snapshot_time import build_snapshot_meta, normalize_snapshot_meta, saved_snapshot_timing_warning
except ModuleNotFoundError:
    def _json_ready(value: Any) -> Any:
        if isinstance(value, pd.DataFrame):
            return value.to_dict(orient="records")
        if isinstance(value, pd.Series):
            return value.to_dict()
        if isinstance(value, dict):
            return {str(k): _json_ready(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_json_ready(item) for item in value]
        if isinstance(value, tuple):
            return [_json_ready(item) for item in value]
        if isinstance(value, Path):
            return str(value)
        return value


    def bundle_to_json_text(bundle: dict[str, Any]) -> str:
        return json.dumps(_json_ready(bundle), ensure_ascii=False, indent=2, default=str)


    def bundle_to_markdown(bundle: dict[str, Any]) -> str:
        meta = normalize_snapshot_meta(bundle.get("meta", {}))
        diagnostics = bundle.get("diagnostics", {})
        generated_at_jst = str(meta.get("generated_at_jst", "") or meta.get("generated_at", ""))
        return "\n".join(
            [
                "# Sector Strength Snapshot",
                "",
                f"- mode: {meta.get('mode', '')}",
                f"- generated_at_jst: {generated_at_jst}",
                f"- is_true_timepoint: {bool(meta.get('is_true_timepoint'))}",
                f"- diagnostics_keys: {', '.join(sorted(str(key) for key in diagnostics.keys()))}",
            ]
        )


    def _expected_time_label(mode: str) -> str:
        return {"0915": "09:15", "1130": "11:30", "1530": "15:30", "now": "now"}.get(str(mode), str(mode))


    def build_snapshot_meta(mode: str, generated_at: Any, source_profile: str = "", includes_kabu: bool = True) -> dict[str, Any]:
        if isinstance(generated_at, str):
            dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        else:
            dt = generated_at
        if not isinstance(dt, datetime):
            dt = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        dt_jst = dt_utc.astimezone(timezone(timedelta(hours=9)))
        return {
            "generated_at": dt_utc.isoformat(),
            "generated_at_utc": dt_utc.isoformat(),
            "generated_at_jst": dt_jst.strftime("%Y-%m-%d %H:%M:%S JST"),
            "mode": str(mode),
            "is_true_timepoint": False,
            "expected_time_label": _expected_time_label(str(mode)),
            "source_profile": str(source_profile),
            "includes_kabu": bool(includes_kabu),
        }


    def normalize_snapshot_meta(meta: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(meta or {})
        generated_at = str(normalized.get("generated_at", "") or normalized.get("generated_at_utc", ""))
        mode = str(normalized.get("mode", ""))
        if generated_at:
            normalized.update(build_snapshot_meta(mode=mode, generated_at=generated_at, source_profile=str(normalized.get("source_profile", "")), includes_kabu=bool(normalized.get("includes_kabu", True))))
            normalized.update(meta or {})
            normalized["expected_time_label"] = str(normalized.get("expected_time_label", "") or _expected_time_label(mode))
            if "generated_at_jst" not in normalized:
                normalized["generated_at_jst"] = build_snapshot_meta(mode=mode, generated_at=generated_at).get("generated_at_jst", "")
        else:
            normalized.setdefault("generated_at_jst", "")
            normalized.setdefault("expected_time_label", _expected_time_label(mode))
            normalized.setdefault("is_true_timepoint", False)
        return normalized


def saved_snapshot_timing_warning(meta: dict[str, Any]) -> str:
    normalized = normalize_snapshot_meta(meta)
    if bool(normalized.get("is_true_timepoint")):
        return ""
    generated_at_jst = str(normalized.get("generated_at_jst", "")).strip()
    expected_time_label = str(normalized.get("expected_time_label", "")).strip()
    if generated_at_jst and expected_time_label:
        return f"この表示は {expected_time_label} 向けのスナップショットです。実際の保存時刻は {generated_at_jst} で、対象時点ぴったりの固定値ではありません。"
    return ""


def _parse_snapshot_generated_at(meta: dict[str, Any]) -> datetime | None:
    raw_value = str(meta.get("generated_at", "") or meta.get("generated_at_utc", "")).strip()
    if not raw_value:
        return None
    try:
        dt = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone(timedelta(hours=9)))


def _snapshot_expected_jst(mode: str, date_jst: datetime) -> datetime | None:
    mapping = {"0915": (9, 15), "1130": (11, 30), "1530": (15, 30)}
    if mode not in mapping:
        return None
    hour, minute = mapping[mode]
    return date_jst.replace(hour=hour, minute=minute, second=0, microsecond=0)


def evaluate_snapshot_guard(mode: str, meta: dict[str, Any], *, now_ts: datetime | None = None) -> dict[str, Any]:
    now_ts = now_ts or datetime.now(timezone.utc)
    now_jst = now_ts.astimezone(timezone(timedelta(hours=9)))
    generated_at_jst = _parse_snapshot_generated_at(meta)
    result = {
        "mode": str(mode),
        "is_missing": False,
        "is_stale": False,
        "generated_at_jst": generated_at_jst.isoformat() if generated_at_jst else "",
        "reason": "",
    }
    if generated_at_jst is None:
        result["is_stale"] = True
        result["reason"] = f"{mode} は本日データなし / stale です。保存時刻を判定できません。"
        return result
    if generated_at_jst.date() != now_jst.date():
        result["is_stale"] = True
        result["reason"] = f"{mode} は本日データなし / stale です。保存日が {generated_at_jst.strftime('%Y-%m-%d')} です。"
        return result
    expected_jst = _snapshot_expected_jst(str(mode), generated_at_jst)
    if expected_jst and generated_at_jst < (expected_jst - timedelta(minutes=30)):
        result["is_stale"] = True
        result["reason"] = f"{mode} は本日データなし / stale です。保存時刻 {generated_at_jst.strftime('%H:%M')} が想定時点より早すぎます。"
        return result
    return result

def read_snapshot_json(mode: str, settings: dict[str, Any], root_dir: Path) -> tuple[str, "_SnapshotStoreResult"]:
    snapshot_path = _snapshot_dir(settings, root_dir) / f"latest_{mode}.json"
    if not snapshot_path.exists():
        raise FileNotFoundError(f"まだ snapshot がありません: latest_{mode}.json")
    payload_text = snapshot_path.read_text(encoding="utf-8")
    return payload_text, _SnapshotStoreResult(
        paths={"json_path": str(snapshot_path)},
        source_label=f"latest_{mode}.json を読み込みました",
        backend_name="local",
        warning_message="",
    )


def write_snapshot_bundle_to_store(
    *,
    mode: str,
    generated_at: str,
    json_text: str,
    markdown_text: str,
    settings: dict[str, Any],
    root_dir: Path,
    write_drive: bool = False,
) -> "_SnapshotStoreResult":
    snapshot_dir = _snapshot_dir(settings, root_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    generated_dt = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
    generated_jst = generated_dt.astimezone(timezone(timedelta(hours=9)))
    timestamp_prefix = generated_jst.strftime("%Y-%m-%d_%H%M%S")
    latest_json = snapshot_dir / f"latest_{mode}.json"
    latest_md = snapshot_dir / f"latest_{mode}.md"
    dated_json = snapshot_dir / f"{timestamp_prefix}_{mode}.json"
    dated_md = snapshot_dir / f"{timestamp_prefix}_{mode}.md"
    latest_json.write_text(json_text, encoding="utf-8")
    latest_md.write_text(markdown_text, encoding="utf-8")
    dated_json.write_text(json_text, encoding="utf-8")
    dated_md.write_text(markdown_text, encoding="utf-8")
    return _SnapshotStoreResult(
        paths={
            "json_path": str(latest_json),
            "markdown_path": str(latest_md),
            "dated_json_path": str(dated_json),
            "dated_markdown_path": str(dated_md),
        },
        source_label=f"latest_{mode}.json を更新しました",
        backend_name="local",
        warning_message="" if not write_drive else "fallback writer ignores write_drive option",
    )

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None

BASE_URL = "https://api.jquants.com/v2"
ROOT_DIR = Path(__file__).resolve().parent
SETTINGS_PATH = ROOT_DIR / "config" / "settings.toml"
EDINETDB_BASE_URL = "https://edinetdb.jp"
EDINETDB_CALENDAR_ENDPOINT = f"{EDINETDB_BASE_URL}/v1/calendar"
EDINETDB_CALENDAR_CACHE_DIR = ROOT_DIR / "data" / "cache"
EDINETDB_CALENDAR_CACHE_VERSION = 1
EDINETDB_CALENDAR_WINDOW_DAYS = 120
EDINETDB_CALENDAR_CHUNK_DAYS = 7
EDINETDB_CALENDAR_LIMIT = 2000
RANKING_TYPE_MAP = {"price_up": 1, "turnover": 4, "volume_surge": 6, "turnover_surge": 7, "industry_up": 14}
RANKING_SCORE_WEIGHTS = {"price_up": 1.0, "turnover": 1.35, "volume_surge": 1.0, "turnover_surge": 1.25}
BOARD_REQUEST_EXCHANGES = {1, 3, 5, 6}
BOARD_ACCEPTED_RESPONSE_EXCHANGES = {1, 3, 5, 6, 27}
BOARD_MAJOR_FIELDS = ["CurrentPrice", "Volume", "Turnover", "Open", "High", "Low"]
MODE_SCORE_WEIGHTS = {
    "0915": {"live_ret_from_open": 1.5, "live_ret_vs_prev_close": 1.2, "gap_pct": 1.4, "live_volume_ratio_20d": 1.1, "live_turnover_ratio_20d": 1.3, "ret_1w": 0.6, "ret_1m": 0.5, "material_score": 0.3},
    "1130": {"live_ret_from_open": 1.4, "live_ret_vs_prev_close": 1.0, "morning_strength": 1.2, "live_volume_ratio_20d": 1.1, "live_turnover_ratio_20d": 1.3, "ret_1w": 0.6, "ret_1m": 0.5, "material_score": 0.3},
    "1530": {"live_ret_from_open": 1.3, "live_ret_vs_prev_close": 1.2, "closing_strength": 1.2, "high_close_score": 1.0, "live_volume_ratio_20d": 1.0, "live_turnover_ratio_20d": 1.2, "ret_1w": 0.7, "ret_1m": 0.6, "material_score": 0.3},
    "now": {"live_ret_from_open": 1.4, "live_ret_vs_prev_close": 1.1, "gap_pct": 1.0, "live_volume_ratio_20d": 1.1, "live_turnover_ratio_20d": 1.3, "ret_1w": 0.6, "ret_1m": 0.5, "material_score": 0.3},
}
INTRADAY_BLOCK_COMPONENT_WEIGHTS = {
    "price": {
        "industry_up_rank_norm": 1.65,
        "price_up_share_of_sector": 1.0,
        "price_up_count_norm": 0.65,
        "ranking_confirmed_count_norm": 0.45,
    },
    "flow": {
        "turnover_share_of_sector": 0.95,
        "turnover_count_norm": 0.45,
        "volume_surge_share_of_sector": 0.8,
        "volume_surge_count_norm": 0.35,
        "turnover_surge_share_of_sector": 0.8,
        "turnover_surge_count_norm": 0.35,
    },
    "participation": {
        "ranking_source_breadth_ex_basket_norm": 1.0,
        "ranking_confirmed_count_norm": 0.9,
        "ranking_confirmed_share_of_sector": 1.1,
    },
}
INTRADAY_BLOCK_MODE_WEIGHTS_BASELINE = {
    "0915": {"price": 0.50, "flow": 0.30, "participation": 0.20},
    "1130": {"price": 0.48, "flow": 0.32, "participation": 0.20},
    "1530": {"price": 0.44, "flow": 0.34, "participation": 0.22},
    "now": {"price": 0.47, "flow": 0.32, "participation": 0.21},
}
INTRADAY_BLOCK_MODE_WEIGHTS = {
    "0915": {"price": 0.49, "flow": 0.29, "participation": 0.22},
    "1130": {"price": 0.46, "flow": 0.30, "participation": 0.24},
    "1530": {"price": 0.42, "flow": 0.31, "participation": 0.27},
    "now": {"price": 0.45, "flow": 0.30, "participation": 0.25},
}
INTRADAY_INDUSTRY_RANK_TETHER = {
    "base_shift": 0,
    "strong_shift": 1,
    "very_strong_shift": 2,
    "max_downshift": 2,
}
TODAY_SECTOR_RANK_MODE_RULES = {
    "ranking_union_count_min": 120,
    "sectors_with_ranking_confirmed_ge5_min": 8,
    "sectors_with_source_breadth_ge2_min": 6,
}
INTRADAY_SCAN_SAMPLE_WARNING_RULES = {
    "critical_count": 4.0,
    "warn_count": 7.0,
    "warn_coverage": 0.16,
    "thin_count": 10.0,
    "thin_coverage": 0.10,
}
WIDE_SCAN_BASKET_MIN_PER_SECTOR = 5
WIDE_SCAN_BASKET_TARGET_PER_SECTOR = 7
WIDE_SCAN_BASKET_MAX_PER_SECTOR = 10
WIDE_SCAN_TARGET_RANGE = (200, 400)
INTRADAY_BREADTH_SLOT_SETTINGS_BASELINE = {
    "0915": {
        "reliability_k": 3.0,
        "warn_up_rate": 0.40,
        "warn_balance": -0.02,
        "warn_sample": 3.0,
        "penalty_up_rate": 0.32,
        "penalty_balance": -0.10,
        "penalty_sample": 2.0,
        "light_penalty": 0.06,
        "heavy_penalty": 0.12,
    },
    "1130": {
        "reliability_k": 5.0,
        "warn_up_rate": 0.44,
        "warn_balance": 0.02,
        "warn_sample": 4.0,
        "penalty_up_rate": 0.36,
        "penalty_balance": -0.04,
        "penalty_sample": 3.0,
        "light_penalty": 0.08,
        "heavy_penalty": 0.16,
    },
    "1530": {
        "reliability_k": 7.0,
        "warn_up_rate": 0.47,
        "warn_balance": 0.05,
        "warn_sample": 5.0,
        "penalty_up_rate": 0.39,
        "penalty_balance": 0.00,
        "penalty_sample": 4.0,
        "light_penalty": 0.10,
        "heavy_penalty": 0.20,
    },
    "now": {
        "reliability_k": 5.0,
        "warn_up_rate": 0.44,
        "warn_balance": 0.02,
        "warn_sample": 4.0,
        "penalty_up_rate": 0.36,
        "penalty_balance": -0.04,
        "penalty_sample": 3.0,
        "light_penalty": 0.08,
        "heavy_penalty": 0.16,
    },
}
INTRADAY_BREADTH_SLOT_SETTINGS = {
    "0915": {
        "reliability_k": 3.0,
        "warn_up_rate": 0.42,
        "warn_balance": 0.00,
        "warn_sample": 3.0,
        "penalty_up_rate": 0.34,
        "penalty_balance": -0.08,
        "penalty_sample": 2.0,
        "light_penalty": 0.08,
        "heavy_penalty": 0.16,
    },
    "1130": {
        "reliability_k": 5.0,
        "warn_up_rate": 0.45,
        "warn_balance": 0.03,
        "warn_sample": 4.0,
        "penalty_up_rate": 0.38,
        "penalty_balance": 0.00,
        "penalty_sample": 4.0,
        "light_penalty": 0.12,
        "heavy_penalty": 0.24,
    },
    "1530": {
        "reliability_k": 7.0,
        "warn_up_rate": 0.50,
        "warn_balance": 0.08,
        "warn_sample": 6.0,
        "penalty_up_rate": 0.42,
        "penalty_balance": 0.02,
        "penalty_sample": 5.0,
        "light_penalty": 0.18,
        "heavy_penalty": 0.34,
    },
    "now": {
        "reliability_k": 5.0,
        "warn_up_rate": 0.45,
        "warn_balance": 0.03,
        "warn_sample": 4.0,
        "penalty_up_rate": 0.38,
        "penalty_balance": 0.00,
        "penalty_sample": 4.0,
        "light_penalty": 0.12,
        "heavy_penalty": 0.24,
    },
}
INTRADAY_CONCENTRATION_PENALTY_SETTINGS_BASELINE = {
    "0915": {"warn_share": 1.0, "light_share": 1.0, "heavy_share": 1.0, "light_penalty": 0.0, "heavy_penalty": 0.0, "micro_sample_penalty": 0.0},
    "1130": {"warn_share": 1.0, "light_share": 1.0, "heavy_share": 1.0, "light_penalty": 0.0, "heavy_penalty": 0.0, "micro_sample_penalty": 0.0},
    "1530": {"warn_share": 1.0, "light_share": 1.0, "heavy_share": 1.0, "light_penalty": 0.0, "heavy_penalty": 0.0, "micro_sample_penalty": 0.0},
    "now": {"warn_share": 1.0, "light_share": 1.0, "heavy_share": 1.0, "light_penalty": 0.0, "heavy_penalty": 0.0, "micro_sample_penalty": 0.0},
}
INTRADAY_CONCENTRATION_PENALTY_SETTINGS = {
    "0915": {"warn_share": 0.55, "light_share": 0.65, "heavy_share": 0.80, "light_penalty": 0.04, "heavy_penalty": 0.10, "micro_sample_penalty": 0.02},
    "1130": {"warn_share": 0.52, "light_share": 0.62, "heavy_share": 0.78, "light_penalty": 0.05, "heavy_penalty": 0.12, "micro_sample_penalty": 0.03},
    "1530": {"warn_share": 0.50, "light_share": 0.60, "heavy_share": 0.75, "light_penalty": 0.08, "heavy_penalty": 0.18, "micro_sample_penalty": 0.05},
    "now": {"warn_share": 0.52, "light_share": 0.62, "heavy_share": 0.78, "light_penalty": 0.05, "heavy_penalty": 0.12, "micro_sample_penalty": 0.03},
}
REPRESENTATIVE_STOCK_SCORE_WEIGHTS_BASELINE = {
    "live_turnover": 1.2,
    "live_turnover_ratio_20d": 1.15,
    "live_volume_ratio_20d": 0.95,
    "live_ret_vs_prev_close": 0.9,
    "avg_turnover_20d": 0.75,
}
REPRESENTATIVE_STOCK_SCORE_WEIGHTS = {
    "live_turnover": 1.35,
    "live_turnover_ratio_20d": 0.85,
    "live_volume_ratio_20d": 0.60,
    "live_ret_vs_prev_close": 0.35,
    "avg_turnover_20d": 1.25,
    "TradingValue_latest": 1.10,
}
REPRESENTATIVE_SORT_COLUMNS_BASELINE = ["sector_name", "representative_score", "stock_turnover_share_of_sector", "live_turnover", "live_ret_vs_prev_close"]
REPRESENTATIVE_SORT_ASCENDING_BASELINE = [True, False, False, False, False]
REPRESENTATIVE_SORT_COLUMNS = ["sector_name", "stock_turnover_share_of_sector", "representative_score", "avg_turnover_20d", "live_turnover", "live_ret_vs_prev_close"]
REPRESENTATIVE_SORT_ASCENDING = [True, False, False, False, False, False]
DEEP_WATCH_MUST_HAVE_TOP_SECTORS = 10
DEEP_WATCH_MUST_HAVE_PER_SECTOR = 2
DEEP_WATCH_MUST_HAVE_MAX = 20
DEEP_WATCH_REPRESENTATIVE_SUPPLEMENTAL_PER_SECTOR = 2
DEEP_WATCH_REPRESENTATIVE_POOL_MIN_PER_SECTOR = 4
DEEP_WATCH_REPRESENTATIVE_SUPPLEMENTAL_MAX = 16
DEEP_WATCH_MUST_HAVE_TODAY_WEIGHTS = {
    "ranking_combo_score": 1.65,
    "ret_1w": 0.45,
    "rel_1w": 0.45,
    "ranking_union_member_bonus": 0.65,
    "industry_basket_member_bonus": 0.20,
}
DEEP_WATCH_MUST_HAVE_REPRESENTATIVE_WEIGHTS = {
    "sector_contribution_full": 0.55,
    "contribution_rank_in_sector": 0.35,
    "turnover_rank_in_sector": 0.30,
    "avg_turnover_20d": 0.25,
    "TradingValue_latest": 0.20,
    "avg_volume_20d": 0.10,
    "liquidity_ok_bonus": 0.15,
}
CENTER_LEADER_TRACE_LIMIT = 10
CENTER_LEADER_CENTRALITY_WEIGHTS = {
    "sector_contribution_full": 1.55,
    "contribution_rank_in_sector": 1.05,
    "turnover_rank_in_sector": 0.95,
    "avg_turnover_20d": 0.90,
    "avg_volume_20d": 0.45,
    "stock_turnover_share_of_sector": 0.55,
    "liquidity_ok": 0.50,
}
CENTER_LEADER_TODAY_WEIGHTS = {
    "live_ret_vs_prev_close": 1.80,
    "live_ret_from_open": 0.80,
    "closing_strength_signal": 0.80,
    "live_turnover_ratio_20d": 0.60,
    "live_volume_ratio_20d": 0.45,
    "sector_relative_live_ret": 0.85,
}
SECTOR_CONFIDENCE_PRIORITY = {"高": 2, "中": 1, "低": 0}
SWING_SELECTION_CONFIG_BASELINE = {
    "extension_threshold_1w": 12.0,
    "extension_threshold_1m": 12.0,
    "candidate_ma20_weight_1m": 0.35,
    "sector_confidence_bonus_high_1w": 0.0,
    "sector_confidence_bonus_mid_1w": 0.0,
    "sector_confidence_bonus_high_1m": 0.0,
    "sector_confidence_bonus_mid_1m": 0.0,
    "top_today_sector_limit_1w": 6,
    "top_persistence_sector_limit_1m": 8,
    "earnings_hard_block_days_1w": 7,
    "earnings_hard_block_days_1m": 7,
    "flow_ratio_gate_1w": 1.20,
    "volume_ratio_gate_1w": 1.20,
    "flow_ratio_gate_1m": 0.90,
    "volume_ratio_gate_1m": 0.90,
    "score_gate_1w": 2.60,
    "score_gate_1m": 3.10,
    "sector_tailwind_bonus_strong_1w": 0.75,
    "sector_tailwind_bonus_mid_1w": 0.35,
    "sector_tailwind_bonus_strong_1m": 0.80,
    "sector_tailwind_bonus_mid_1m": 0.40,
    "display_total_limit_1w": 5,
    "display_total_limit_1m": 5,
    "display_sector_limit_1w": 2,
    "display_sector_limit_1m": 2,
}
SWING_SELECTION_CONFIG = {
    "extension_threshold_1w": 10.0,
    "extension_threshold_1m": 10.0,
    "candidate_ma20_weight_1m": 0.55,
    "sector_confidence_bonus_high_1w": 0.35,
    "sector_confidence_bonus_mid_1w": 0.15,
    "sector_confidence_bonus_high_1m": 0.30,
    "sector_confidence_bonus_mid_1m": 0.12,
    "top_today_sector_limit_1w": 8,
    "top_persistence_sector_limit_1m": 10,
    "earnings_hard_block_days_1w": 5,
    "earnings_hard_block_days_1m": 7,
    "flow_ratio_gate_1w": 1.05,
    "volume_ratio_gate_1w": 1.10,
    "flow_ratio_gate_1m": 0.85,
    "volume_ratio_gate_1m": 0.90,
    "score_gate_1w": 2.30,
    "score_gate_1m": 2.90,
    "score_gate_3m": 3.00,
    "buy_score_gate_1w": 1.05,
    "buy_score_gate_1m": 1.25,
    "buy_score_gate_3m": 1.85,
    "sector_tailwind_bonus_strong_1w": 0.70,
    "sector_tailwind_bonus_mid_1w": 0.28,
    "sector_tailwind_bonus_strong_1m": 0.85,
    "sector_tailwind_bonus_mid_1m": 0.45,
    "sector_tailwind_bonus_strong_3m": 0.90,
    "sector_tailwind_bonus_mid_3m": 0.42,
    "display_total_limit_1w": 5,
    "display_total_limit_1m": 5,
    "display_total_limit_3m": 5,
    "display_sector_limit_1w": 2,
    "display_sector_limit_1m": 2,
    "display_sector_limit_3m": 2,
}
CLOUD_VIEWER_MODES = ("0915", "1130", "1530", "now")
VIEWER_ONLY_SNAPSHOT_MODES = CLOUD_VIEWER_MODES
DEFAULT_CONTROL_PLANE_REQUEST_MODE = "1130"
DEFAULT_GITHUB_REPOSITORY = "sagres3904-spec/sector-strength-app"
DEFAULT_GITHUB_CONTROL_BRANCH = "control-plane"
DEFAULT_GITHUB_DEPLOY_BRANCH = "deploy/streamlit-live"
DEFAULT_GITHUB_REQUEST_PATH = "commands/update_request.json"
DEFAULT_GITHUB_STATUS_PATH = "commands/update_status.json"
DEFAULT_GITHUB_DEPLOY_SNAPSHOT_JSON_PATH = "data/snapshots/latest_1130.json"
DEFAULT_GITHUB_DEPLOY_SNAPSHOT_MD_PATH = "data/snapshots/latest_1130.md"
GITHUB_CONTROL_TOKEN_SECRET_NAME = "GITHUB_CONTROL_TOKEN"
DEFAULT_VIEWER_AUTO_REFRESH_SECONDS = 60
SNAPSHOT_VIEWER_CACHE_TTL_SECONDS = 120


def normalize_cloud_viewer_mode(mode: Any, *, default: str | None = None, allow_blank: bool = False) -> str:
    normalized = str(mode or "").strip()
    if not normalized:
        fallback = str(default or DEFAULT_CONTROL_PLANE_REQUEST_MODE)
        return "" if allow_blank else fallback
    if normalized not in CLOUD_VIEWER_MODES:
        raise ValueError(f"Unsupported mode: {normalized}")
    return normalized


MASTER_PRODUCT_ATTRIBUTE_SOURCES = {
    "security_type": ["SecurityType", "TypeOfInstrument", "SecurityTypeName", "IssueType"],
    "instrument_type": ["InstrumentType", "InstrumentCategory", "InstrumentTypeName"],
    "product_category": ["ProductCategory", "ProductCategoryName", "FundType", "FundTypeName"],
    "listing_category": ["ListingCategory", "ListingCategoryName", "MarketType", "MarketTypeName"],
    "underlying_index": ["UnderlyingIndex", "UnderlyingIndexName", "BenchmarkName", "ReferenceIndex", "IndexName"],
    "market_code": ["MarketCode", "MktCode"],
}
NON_CORPORATE_PRODUCT_ATTRIBUTE_COLUMNS = tuple(MASTER_PRODUCT_ATTRIBUTE_SOURCES.keys()) + ("exchange_name",)
NON_CORPORATE_PRODUCT_DIRECT_PATTERNS = [
    ("etf", re.compile(r"\bETF\b", re.IGNORECASE)),
    ("etn", re.compile(r"\bETN\b", re.IGNORECASE)),
    ("listed_fund", re.compile(r"上場投信|上場インデックスファンド|投資信託", re.IGNORECASE)),
    ("index_linked", re.compile(r"(指数|インデックス).{0,8}連動|連動型上場投信|連動型ETF|連動型ETN", re.IGNORECASE)),
    ("leverage", re.compile(r"レバレッジ|ブル\s*\d*倍", re.IGNORECASE)),
    ("inverse", re.compile(r"ダブルインバース|インバース|ベア\s*\d*倍", re.IGNORECASE)),
]
NON_CORPORATE_PRODUCT_INDEX_PATTERN = re.compile(
    r"S&P\s*500|NASDAQ\s*-?\s*100|日経\s*平均|TOPIX|JPX\s*日経\s*400|東証\s*REIT|REIT\s*CORE|MSCI|FTSE|ダウ",
    re.IGNORECASE,
)
NON_CORPORATE_PRODUCT_INDEX_LINK_PATTERN = re.compile(
    r"指数|インデックス|連動|上場投信|ETF|ETN|レバレッジ|インバース",
    re.IGNORECASE,
)
NON_CORPORATE_PRODUCT_BRAND_PATTERN = re.compile(
    r"NEXT\s*FUNDS|NEXT\s*NOTES|MAXIS|I\s*シェアーズ|ISHARES|GLOBAL\s*X|NZAM\s*上場投信|ONE\s*ETF|SPDR|WISDOMTREE|IFREEETF",
    re.IGNORECASE,
)

logger = logging.getLogger("sector_app_jq")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


UI_COLUMN_LABELS = {
    "sector_name": "セクター名",
    "n": "注目銘柄数",
    "sector_constituent_count": "構成銘柄数",
    "today_rank": "今日の順位",
    "today_display_rank": "表示順位",
    "persistence_rank": "継続順位",
    "breadth": "scan内当日上昇:下落",
    "median_ret": "当日中央値騰落率",
    "median_live_ret": "当日中央値騰落率",
    "turnover_ratio_median": "売買代金倍率",
    "live_turnover_total": "セクター売買代金合計",
    "leader_live_turnover": "最大売買代金",
    "live_aggregate_observed_count": "live観測件数",
    "live_aggregate_ret_count": "騰落率観測件数",
    "live_aggregate_turnover_count": "売買代金観測件数",
    "live_aggregate_turnover_ratio_count": "売買代金倍率観測件数",
    "live_aggregate_status": "live集計状態",
    "live_aggregate_reason": "live集計理由",
    "industry_up": "東証業種順位/上昇率",
    "industry_up_value": "東証業種上昇率",
    "industry_up_rank": "東証業種順位",
    "industry_rank_live": "東証業種順位",
    "industry_up_anchor_rank": "東証アンカー順位",
    "industry_anchor_rank": "東証順位",
    "final_rank_delta": "差分",
    "sector_rank_1w": "1週順位",
    "sector_rank_1m": "1か月順位",
    "sector_rank_3m": "3か月順位",
    "leaders": "代表銘柄",
    "leader_contribution_pct": "上位1銘柄寄与率(%)",
    "price_up_count": "price_up件数",
    "turnover_count": "turnover件数",
    "volume_surge_count": "volume_surge件数",
    "turnover_surge_count": "turnover_surge件数",
    "code": "コード",
    "name": "銘柄名",
    "live_price": "現在値",
    "current_price": "現在値",
    "live_ret_vs_prev_close": "前日終値比(%)",
    "live_ret_from_open": "始値比(%)",
    "live_turnover": "売買代金",
    "live_turnover_value": "売買代金",
    "live_volume": "出来高",
    "live_volume_ratio_20d": "20日平均比出来高",
    "live_turnover_ratio_20d": "20日平均比売買代金",
    "avg_volume_20d": "20日平均出来高",
    "avg_turnover_20d": "20日平均売買代金",
    "ret_1w": "1週騰落率(%)",
    "ret_1m": "1か月騰落率(%)",
    "ret_3m": "3か月騰落率(%)",
    "rel_1w": "1週相対強度",
    "rel_1m": "1か月相対強度",
    "rel_3m": "3か月相対強度",
    "topix_ret_1w": "TOPIX1週(%)",
    "topix_ret_1m": "TOPIX1か月(%)",
    "topix_ret_3m": "TOPIX3か月(%)",
    "rs_vs_topix_1w": "TOPIX比1週RS",
    "rs_vs_topix_1m": "TOPIX比1か月RS",
    "rs_vs_topix_3m": "TOPIX比3か月RS",
    "sector_rs_vs_topix_1w": "セクターTOPIX比1週RS",
    "sector_rs_vs_topix_1m": "セクターTOPIX比1か月RS",
    "sector_rs_vs_topix_3m": "セクターTOPIX比3か月RS",
    "price_vs_ma20_pct": "20日線乖離(%)",
    "52w_flag": "20日高値近辺",
    "high_20d_flag": "20日高値近辺",
    "earnings_buffer_days": "決算まで日数",
    "earnings_announcement_date": "決算発表予定日",
    "finance_health_score": "財務健全度",
    "finance_health_flag": "財務健全フラグ",
    "price_block_score": "価格の強さ",
    "flow_block_score": "資金流入の強さ",
    "participation_block_score": "ランキング広がり",
    "ranking_breadth_display": "ランキング広がり",
    "intraday_total_score": "intraday総合",
    "scan_member_count": "scan銘柄数",
    "scan_participation_rate": "scan参加率",
    "price_up_rate": "上昇銘柄比率",
    "turnover_count_rate": "売買代金流入比率",
    "volume_surge_rate": "出来高急増比率",
    "turnover_surge_rate": "売買代金急増比率",
    "price_up_share_of_sector": "上昇銘柄比率",
    "price_up_share_of_market_scan": "市場scan内上昇シェア",
    "turnover_share_of_sector": "売買代金流入比率",
    "turnover_share_of_market_scan": "市場scan内売買代金流入シェア",
    "turnover_surge_share_of_sector": "売買代金急増比率",
    "turnover_surge_share_of_market_scan": "市場scan内売買代金急増シェア",
    "volume_surge_share_of_sector": "出来高急増比率",
    "volume_surge_share_of_market_scan": "市場scan内出来高急増シェア",
    "breadth_up_rate": "scan内当日上昇比率",
    "breadth_down_rate": "scan内当日下落比率",
    "breadth_balance": "scan内当日上昇-下落差",
    "breadth_net_rate": "scan内当日純上昇比率",
    "breadth_sample_count": "scan内breadth母数",
    "breadth_active_coverage": "scan内breadth有効比率",
    "breadth_reliability": "scan内breadth信頼度",
    "breadth_core_score": "scan内breadthコア",
    "scan_coverage": "scanカバー率",
    "signal_breadth_count": "ランキング種類数",
    "signal_breadth_share": "ランキング広がり率",
    "industry_up_rank_norm": "業種上昇率順位norm",
    "sector_positive_ratio": "セクター上昇比率",
    "candidate_rank_1w": "順位",
    "candidate_rank_1m": "順位",
    "candidate_rank_3m": "順位",
    "candidate_quality": "候補品質",
    "entry_fit": "今の判定",
    "entry_stance_label": "エントリー判断",
    "stretch_caution_label": "過熱注意",
    "watch_reason_label": "監視理由",
    "selection_reason": "採用理由",
    "horizon_fit_reason": "時間軸理由",
    "entry_caution": "買い注意",
    "candidate_bucket_label": "候補分類",
    "event_caution_reason": "イベント注意",
    "risk_note": "注意点",
    "candidate_commentary": "コメント",
    "axis_rank": "順位",
    "sector_summary": "根拠",
    "center_stocks": "中心銘柄",
    "center_summary": "中心メモ",
    "center_note": "代表理由",
    "candidate_rank": "順位",
    "candidate_source_label": "候補種別",
    "candidate_basis": "根拠",
    "sector_scope": "強セクター内",
    "sector_confidence": "信頼度",
    "sector_caution": "注意点",
    "quality_pass": "品質OK",
    "quality_warn": "品質注意",
    "quality_fail_reason": "品質理由",
    "sector_gate_pass": "gate通過",
    "sector_gate_fail_reason": "gate理由",
    "sector_display_eligible": "表示可",
    "core_representatives": "主力3銘柄",
    "core_representatives_count": "代表数",
    "core_representatives_reason": "代表抽出理由",
    "scan_sample_warning_level": "scan母数警告レベル",
    "scan_sample_warning_reason": "scan母数警告理由",
    "representative_stock": "代表銘柄",
    "representative_rank": "代表順位",
    "representative_score": "代表銘柄スコア",
    "representative_selected_reason": "代表理由",
    "representative_quality_flag": "品質/注意",
    "representative_fallback_reason": "補足",
    "stock_turnover_share_of_sector": "セクター売買代金寄与率",
    "swing_score_1w": "1週間候補スコア",
    "swing_score_1m": "1か月候補スコア",
    "swing_score_3m": "3か月候補スコア",
    "buy_strength_score": "強さスコア",
    "entry_timing_adjustment": "買いタイミング調整",
    "event_candidate_flag": "イベント注意",
    "event_candidate_type": "イベント種別",
    "candidate_bucket": "候補分類",
    "material_title": "材料タイトル",
    "focus_reason": "注目理由",
    "total_score": "総合スコア",
    "center_stock_score": "中心銘柄スコア",
    "watch_score": "監視候補スコア",
    "buyability_score": "買い候補スコア",
    "buyability_label": "買い候補判定",
    "today_sector_score": "本命セクタースコア",
    "intraday_sector_score": "intraday主順位",
    "intraday_sector_score_raw": "intraday主順位raw",
    "participation_block_score_raw": "参加・広がりraw",
    "breadth_penalty": "breadth軽減点",
    "earnings_proximity_flag": "決算接近除外(仮)",
    "atr_pct": "ATR%(土台)",
    "nikkei_search": "日経リンク",
    "material_link": "材料リンク",
}


INDUSTRY_NAME_ALIASES = {
    "海運": "海運業",
    "鉱業": "鉱業",
    "空運": "空運業",
    "銀行": "銀行業",
    "保険": "保険業",
    "証券商品先物": "証券、商品先物取引業",
    "証券・商品先物": "証券、商品先物取引業",
    "その他金融": "その他金融業",
    "卸売": "卸売業",
    "小売": "小売業",
    "情報通信": "情報・通信業",
    "倉庫運輸": "倉庫・運輸関連業",
}

INDUSTRY_KEY_ALIASES = {
    "その他": "その他",
    "その他製品": "その他製品",
    "製品": "その他製品",
    "その他金融": "その他金融業",
    "その他金融業": "その他金融業",
    "金融": "その他金融業",
    "ガラス・土石製品": "ガラス･土石製品",
    "ガラス･土石製品": "ガラス･土石製品",
    "ｶﾞﾗｽ": "ガラス･土石製品",
    "ゴム製品": "ゴム製品",
    "ｺﾞﾑ": "ゴム製品",
    "サービス業": "サービス業",
    "ｻｰﾋﾞｽ": "サービス業",
    "パルプ・紙": "パルプ・紙",
    "ﾊﾟﾙﾌﾟ": "パルプ・紙",
    "不動産業": "不動産業",
    "不動": "不動産業",
    "保険": "保険業",
    "保険業": "保険業",
    "倉庫・運輸関連業": "倉庫･運輸関連業",
    "倉庫･運輸関連業": "倉庫･運輸関連業",
    "倉庫": "倉庫･運輸関連業",
    "化学": "化学",
    "医薬": "医薬品",
    "医薬品": "医薬品",
    "卸売": "卸売業",
    "卸売業": "卸売業",
    "小売": "小売業",
    "小売業": "小売業",
    "建設": "建設業",
    "建設業": "建設業",
    "情報通信": "情報･通信業",
    "情報・通信": "情報･通信業",
    "情報･通信": "情報･通信業",
    "情報・通信業": "情報･通信業",
    "情報･通信業": "情報･通信業",
    "機械": "機械",
    "水産": "水産･農林業",
    "水産・農林": "水産･農林業",
    "水産･農林": "水産･農林業",
    "水産・農林業": "水産･農林業",
    "水産･農林業": "水産･農林業",
    "海運": "海運業",
    "海運業": "海運業",
    "石油": "石油･石炭製品",
    "石油・石炭製品": "石油･石炭製品",
    "石油･石炭製品": "石油･石炭製品",
    "空運": "空運業",
    "空運業": "空運業",
    "精密": "精密機器",
    "精密機器": "精密機器",
    "繊維": "繊維製品",
    "繊維製品": "繊維製品",
    "証券": "証券･商品先物取引業",
    "証券、商品先物取引業": "証券･商品先物取引業",
    "証券･商品先物取引業": "証券･商品先物取引業",
    "輸送": "輸送用機器",
    "輸送用機器": "輸送用機器",
    "金属": "金属製品",
    "金属製品": "金属製品",
    "鉄鋼": "鉄鋼",
    "鉱業": "鉱業",
    "銀行": "銀行業",
    "銀行業": "銀行業",
    "陸運": "陸運業",
    "陸運業": "陸運業",
    "電気": "電気機器",
    "電気機器": "電気機器",
    "ｶﾞｽ": "電気･ガス業",
    "電気・ガス業": "電気･ガス業",
    "電気･ガス業": "電気･ガス業",
    "非鉄": "非鉄金属",
    "非鉄金属": "非鉄金属",
    "食料": "食料品",
    "食料品": "食料品",
}


class JQuantsAuthError(RuntimeError):
    pass


class PipelineFailClosed(RuntimeError):
    pass


def _short_body(text: str, limit: int = 160) -> str:
    return " ".join(str(text or "").split())[:limit]


def _is_streamlit_runtime() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        if get_script_run_ctx() is not None:
            return True
    except Exception:
        pass
    try:
        runtime_module = getattr(st, "runtime", None)
        runtime_exists = getattr(runtime_module, "exists", None)
        if callable(runtime_exists) and bool(runtime_exists()):
            return True
    except Exception:
        pass
    return bool(
        os.environ.get("STREAMLIT_SERVER_PORT")
        or os.environ.get("STREAMLIT_RUNTIME")
        or os.environ.get("STREAMLIT_SHARING_MODE")
    )


def _explicit_streamlit_cloud_flags() -> dict[str, str]:
    return {
        "STREAMLIT_SHARING_MODE": str(os.environ.get("STREAMLIT_SHARING_MODE", "")).strip(),
        "STREAMLIT_CLOUD": str(os.environ.get("STREAMLIT_CLOUD", "")).strip(),
        "STREAMLIT_RUNTIME": str(os.environ.get("STREAMLIT_RUNTIME", "")).strip(),
        "STREAMLIT_SERVER_PORT": str(os.environ.get("STREAMLIT_SERVER_PORT", "")).strip(),
        "STREAMLIT_SERVER_HEADLESS": str(os.environ.get("STREAMLIT_SERVER_HEADLESS", "")).strip(),
        "FORCE_VIEWER_ONLY": str(os.environ.get("FORCE_VIEWER_ONLY", "")).strip(),
        "FORCE_LOCAL_COLLECTOR_UI": str(os.environ.get("FORCE_LOCAL_COLLECTOR_UI", "")).strip(),
    }


def _is_local_collector_capable(settings: dict[str, Any] | None = None) -> bool:
    settings = settings or get_settings()
    api_key = (
        _read_streamlit_secret("JQUANTS_API_KEY")
        or str(os.environ.get("JQUANTS_API_KEY", "")).strip()
        or str(settings.get("JQUANTS_API_KEY", "")).strip()
    )
    kabu_password = (
        _read_streamlit_secret("KABU_API_PASSWORD")
        or str(os.environ.get("KABU_API_PASSWORD", "")).strip()
        or str(settings.get("KABU_API_PASSWORD", "")).strip()
    )
    kabu_base_url = str(settings.get("KABU_API_BASE_URL", "") or "").strip()
    parsed = urlparse(kabu_base_url)
    host = str(parsed.hostname or "").strip().lower()
    local_hosts = {"localhost", "127.0.0.1", "::1"}
    return bool(api_key) and bool(kabu_password) and host in local_hosts


def _streamlit_runtime_context(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or get_settings()
    env_flags = _explicit_streamlit_cloud_flags()
    runtime_detected = _is_streamlit_runtime()
    sharing_mode = str(os.environ.get("STREAMLIT_SHARING_MODE", "")).strip().lower()
    cloud_flag = str(os.environ.get("STREAMLIT_CLOUD", "")).strip().lower()
    explicit_cloud_detected = sharing_mode in {"1", "true", "cloud"} or cloud_flag in {"1", "true", "yes"}
    force_viewer_only = str(env_flags.get("FORCE_VIEWER_ONLY", "")).strip().lower() in {"1", "true", "yes", "on"}
    force_local_collector_ui = str(env_flags.get("FORCE_LOCAL_COLLECTOR_UI", "")).strip().lower() in {"1", "true", "yes", "on"}
    local_collector_capable = _is_local_collector_capable(settings)
    override_mode = ""
    if force_viewer_only:
        override_mode = "viewer_only"
    elif force_local_collector_ui:
        override_mode = "local_collector_ui"
    if override_mode == "viewer_only":
        viewer_only = True
        cloud_detected = True
        detection_reason = "override_force_viewer_only"
    elif override_mode == "local_collector_ui":
        viewer_only = False
        cloud_detected = False
        detection_reason = "override_force_local_collector_ui"
    elif runtime_detected:
        viewer_only = True
        cloud_detected = True
        detection_reason = "streamlit_runtime_default_viewer_only_fail_closed"
    elif explicit_cloud_detected:
        viewer_only = True
        cloud_detected = True
        detection_reason = "explicit_streamlit_cloud_env"
    else:
        viewer_only = False
        cloud_detected = False
        detection_reason = "non_streamlit_runtime_default_local_ui"
    return {
        "runtime_detected": bool(runtime_detected),
        "cloud_detected": bool(cloud_detected),
        "viewer_only": bool(viewer_only),
        "local_collector_capable": bool(local_collector_capable),
        "detection_reason": detection_reason,
        "explicit_override": override_mode or "none",
        "final_mode": "viewer_only" if viewer_only else "local_collector_ui",
        "env_flags": env_flags,
    }


def _is_streamlit_cloud(settings: dict[str, Any] | None = None) -> bool:
    return bool(_streamlit_runtime_context(settings).get("cloud_detected"))


def _snapshot_json_path(mode: str, settings: dict[str, Any] | None = None) -> Path:
    settings = settings or get_settings()
    output_dir = str(settings.get("SNAPSHOT_OUTPUT_DIR", "data/snapshots")).strip() or "data/snapshots"
    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = ROOT_DIR / output_path
    return output_path / f"latest_{mode}.json"


def _github_deploy_snapshot_json_path(mode: str, settings: dict[str, Any] | None = None) -> str:
    config = get_github_control_config(settings)
    base_path = str(config["deploy_snapshot_json_path"]).strip().replace("\\", "/")
    parent_dir, _, _ = base_path.rpartition("/")
    normalized_mode = normalize_cloud_viewer_mode(mode)
    file_name = f"latest_{normalized_mode}.json"
    return f"{parent_dir}/{file_name}" if parent_dir else file_name


def _github_deploy_snapshot_md_path(mode: str, settings: dict[str, Any] | None = None) -> str:
    config = get_github_control_config(settings)
    base_path = str(config["deploy_snapshot_md_path"]).strip().replace("\\", "/")
    parent_dir, _, _ = base_path.rpartition("/")
    normalized_mode = normalize_cloud_viewer_mode(mode)
    file_name = f"latest_{normalized_mode}.md"
    return f"{parent_dir}/{file_name}" if parent_dir else file_name


def _viewer_snapshot_github_token() -> str:
    runtime_context = _streamlit_runtime_context()
    use_streamlit_secrets = bool(runtime_context.get("runtime_detected"))
    token = _github_control_token(use_streamlit_secrets=use_streamlit_secrets)
    if token:
        return token
    if use_streamlit_secrets:
        return _github_control_token(use_streamlit_secrets=False)
    return ""


def _set_viewer_snapshot_mode_warnings(messages: list[str]) -> None:
    try:
        st.session_state["_viewer_snapshot_mode_warnings"] = list(messages)
    except Exception:
        pass


def _get_viewer_snapshot_mode_warnings() -> list[str]:
    try:
        messages = st.session_state.get("_viewer_snapshot_mode_warnings", [])
    except Exception:
        return []
    return [str(message).strip() for message in messages if str(message).strip()]


def _request_viewer_snapshot_reload() -> None:
    previous_tokens: dict[str, dict[str, str]] = {}
    try:
        existing_tokens = st.session_state.get("_viewer_snapshot_tokens", {})
        if isinstance(existing_tokens, dict):
            previous_tokens = {
                str(mode): {
                    str(key): str(value or "")
                    for key, value in token.items()
                }
                for mode, token in existing_tokens.items()
                if isinstance(token, dict)
            }
        st.session_state["_viewer_snapshot_refresh_previous_tokens"] = previous_tokens
    except Exception:
        previous_tokens = {}
    st.cache_data.clear()


def _finalize_viewer_snapshot_reload(current_tokens: dict[str, dict[str, str]]) -> None:
    normalized_tokens = {
        str(mode): {
            "generated_at_jst": str(token.get("generated_at_jst", "") or "").strip(),
            "json_sha": str(token.get("json_sha", "") or "").strip(),
            "display_signature": str(token.get("display_signature", "") or "").strip(),
        }
        for mode, token in current_tokens.items()
        if isinstance(token, dict)
    }
    previous_tokens: dict[str, dict[str, str]] | None = None
    try:
        previous_tokens = st.session_state.pop("_viewer_snapshot_refresh_previous_tokens", None)
        st.session_state["_viewer_snapshot_tokens"] = normalized_tokens
    except Exception:
        previous_tokens = None
    if not isinstance(previous_tokens, dict):
        return
    changed_modes: list[str] = []
    unchanged_modes: list[str] = []
    for mode, token in normalized_tokens.items():
        previous_token = previous_tokens.get(mode, {})
        if not isinstance(previous_token, dict):
            previous_token = {}
        changed = any(
            token.get(key, "") != str(previous_token.get(key, "") or "")
            for key in ["generated_at_jst", "json_sha", "display_signature"]
        )
        if changed:
            generated_at_jst = token.get("generated_at_jst", "") or "-"
            json_sha = token.get("json_sha", "")
            sha_note = f" / sha={json_sha[:7]}" if json_sha else ""
            changed_modes.append(f"{mode}: {generated_at_jst}{sha_note}")
        else:
            unchanged_modes.append(str(mode))
    if changed_modes:
        st.success("更新状態と保存データを再読込しました。反映: " + " | ".join(changed_modes))
        return
    if normalized_tokens:
        unchanged_label = ", ".join(unchanged_modes) if unchanged_modes else ", ".join(sorted(normalized_tokens.keys()))
        st.info(f"更新状態と保存データを再読込しましたが、表示内容は前回と同じです。mode={unchanged_label}")


def _read_github_deploy_snapshot_text(mode: str, settings: dict[str, Any] | None = None) -> tuple[str, str, str]:
    settings = settings or get_settings()
    config = get_github_control_config(settings)
    path = _github_deploy_snapshot_json_path(mode, settings)
    text, sha = github_read_text_file(
        config["repository"],
        config["deploy_branch"],
        path,
        _viewer_snapshot_github_token(),
    )
    source_path = f"{config['repository']}@{config['deploy_branch']}:{path}"
    return text, sha, source_path


def _load_saved_snapshot_payload_from_github(mode: str, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    payload_text, sha, source_path = _read_github_deploy_snapshot_text(mode, settings)
    return {
        "payload": json.loads(payload_text),
        "paths": {"json_path": source_path, "json_sha": sha},
        "source_label": f"{source_path} を読み込みました",
        "backend_name": "github-deploy",
        "warning_message": "",
    }


def _probe_viewer_snapshot_mode(mode: str, settings: dict[str, Any] | None = None) -> tuple[bool, str]:
    settings = settings or get_settings()
    try:
        if _is_streamlit_cloud():
            _load_saved_snapshot_payload_from_github(mode, settings)
        else:
            snapshot_path = _snapshot_json_path(mode, settings)
            if not snapshot_path.exists():
                raise FileNotFoundError(f"latest_{mode}.json がありません")
            json.loads(snapshot_path.read_text(encoding="utf-8"))
        return True, ""
    except FileNotFoundError as exc:
        return False, f"{mode}: missing ({exc})"
    except json.JSONDecodeError as exc:
        return False, f"{mode}: invalid json ({exc})"
    except Exception as exc:
        return False, f"{mode}: unavailable ({exc})"


def _available_viewer_snapshot_modes(settings: dict[str, Any] | None = None) -> list[str]:
    settings = settings or get_settings()
    available_modes: list[str] = []
    warnings: list[str] = []
    for mode in VIEWER_ONLY_SNAPSHOT_MODES:
        is_available, warning_message = _probe_viewer_snapshot_mode(mode, settings)
        if is_available:
            available_modes.append(mode)
        elif warning_message:
            warnings.append(warning_message)
    _set_viewer_snapshot_mode_warnings(warnings)
    return available_modes


def _snapshot_mtime_ns(snapshot_path: Path) -> int:
    try:
        return snapshot_path.stat().st_mtime_ns
    except FileNotFoundError:
        return -1


def _frame_display_signature(frame: Any) -> str:
    if not isinstance(frame, pd.DataFrame):
        return "missing"
    if frame.empty:
        return "empty"
    normalized = frame.copy()
    normalized = normalized.reindex(columns=sorted(str(column) for column in normalized.columns))
    normalized = normalized.astype("object")
    normalized = normalized.where(pd.notna(normalized), "")
    for column in normalized.columns:
        normalized[column] = normalized[column].map(lambda value: str(value))
    payload = normalized.to_json(orient="split", force_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _bundle_snapshot_token(bundle: dict[str, Any]) -> dict[str, str]:
    meta = bundle.get("meta", {}) if isinstance(bundle, dict) else {}
    paths = bundle.get("paths", {}) if isinstance(bundle, dict) else {}
    signature_keys = [
        "today_sector_leaderboard",
        "sector_representatives_display",
        "swing_candidates_1w_display",
        "swing_candidates_1m_display",
        "swing_candidates_3m_display",
    ]
    signature_seed = "|".join(
        f"{key}:{_frame_display_signature(bundle.get(key, pd.DataFrame()) if isinstance(bundle, dict) else pd.DataFrame())}"
        for key in signature_keys
    )
    return {
        "generated_at": str(meta.get("generated_at", "") or "").strip(),
        "generated_at_jst": str(meta.get("generated_at_jst", "") or meta.get("generated_at", "") or "").strip(),
        "json_sha": str(paths.get("json_sha", "") or "").strip(),
        "json_path": str(paths.get("json_path", "") or "").strip(),
        "display_signature": hashlib.sha1(signature_seed.encode("utf-8")).hexdigest(),
    }


@st.cache_data(ttl=SNAPSHOT_VIEWER_CACHE_TTL_SECONDS, show_spinner=False)
def _load_saved_snapshot_payload_cached(mode: str, snapshot_path_str: str, snapshot_mtime_ns: int) -> dict[str, Any]:
    snapshot_path = Path(snapshot_path_str)
    if snapshot_mtime_ns < 0 or not snapshot_path.exists():
        raise FileNotFoundError(f"まだ snapshot がありません: latest_{mode}.json")
    payload_text = snapshot_path.read_text(encoding="utf-8")
    return {
        "payload": json.loads(payload_text),
        "paths": {"json_path": str(snapshot_path)},
        "source_label": f"latest_{mode}.json を読み込みました",
        "backend_name": "local",
        "warning_message": "",
    }


def _should_show_snapshot_cache_admin() -> bool:
    try:
        admin_value = st.query_params.get("snapshot_cache_admin", "")
    except Exception:
        return False
    return str(admin_value).strip().lower() in {"1", "true", "yes", "on"}


def _render_snapshot_cache_admin_tools() -> None:
    if not _should_show_snapshot_cache_admin():
        return
    with st.expander("Snapshot cache admin", expanded=False):
        st.caption("非常用です。通常運用では不要です。")
        if st.button("snapshot cache clear", key="snapshot-cache-clear"):
            st.cache_data.clear()
            st.success("snapshot cache をクリアしました。")
            st.rerun()


def _render_runtime_detection_diagnostics(runtime_context: dict[str, Any]) -> None:
    if not runtime_context:
        return
    summary = {
        "runtime_detected": bool(runtime_context.get("runtime_detected")),
        "cloud_detected": bool(runtime_context.get("cloud_detected")),
        "viewer_only": bool(runtime_context.get("viewer_only")),
        "local_collector_capable": bool(runtime_context.get("local_collector_capable")),
        "explicit_override": str(runtime_context.get("explicit_override", "") or "none"),
        "final_mode": str(runtime_context.get("final_mode", "") or ""),
        "detection_reason": str(runtime_context.get("detection_reason", "") or ""),
        "env_flags": runtime_context.get("env_flags", {}),
    }
    st.caption(
        "runtime="
        f"{summary['runtime_detected']} / override={summary['explicit_override']} / "
        f"capable={summary['local_collector_capable']} / final={summary['final_mode']} / "
        f"reason={summary['detection_reason']}"
    )
    with st.expander("runtime detection", expanded=False):
        st.json(summary)


@contextmanager
def safe_spinner(text: str, *, enabled: bool = False):
    if enabled:
        with st.spinner(text):
            yield
        return
    yield


def _read_settings_toml() -> dict[str, Any]:
    if tomllib is None or not SETTINGS_PATH.exists():
        return {}
    with SETTINGS_PATH.open("rb") as fh:
        return tomllib.load(fh)


def get_settings() -> dict[str, Any]:
    settings = {
        "JQUANTS_API_KEY": "",
        "KABU_API_PASSWORD": "",
        "KABU_API_BASE_URL": "http://localhost:18080/kabusapi",
        "KABU_API_WS_URL": "ws://localhost:18080/kabusapi/websocket",
        "SNAPSHOT_BACKEND": "local",
        "SNAPSHOT_LOCAL_DIR": "data/snapshots",
        "SNAPSHOT_GCS_BUCKET": "",
        "SNAPSHOT_GCS_PREFIX": "sector-app/snapshots",
        "SNAPSHOT_OUTPUT_DIR": "data/snapshots",
        "DRIVE_SYNC_DIR": "",
        "KABU_REGISTER_LIMIT": 50,
        "KABU_PUSH_TIMEOUT_SECONDS": 4.0,
        "GITHUB_REPOSITORY": DEFAULT_GITHUB_REPOSITORY,
        "GITHUB_CONTROL_BRANCH": DEFAULT_GITHUB_CONTROL_BRANCH,
        "GITHUB_DEPLOY_BRANCH": DEFAULT_GITHUB_DEPLOY_BRANCH,
        "GITHUB_CONTROL_REQUEST_PATH": DEFAULT_GITHUB_REQUEST_PATH,
        "GITHUB_CONTROL_STATUS_PATH": DEFAULT_GITHUB_STATUS_PATH,
        "GITHUB_DEPLOY_SNAPSHOT_JSON_PATH": DEFAULT_GITHUB_DEPLOY_SNAPSHOT_JSON_PATH,
        "GITHUB_DEPLOY_SNAPSHOT_MD_PATH": DEFAULT_GITHUB_DEPLOY_SNAPSHOT_MD_PATH,
        "VIEWER_AUTO_REFRESH_SECONDS": DEFAULT_VIEWER_AUTO_REFRESH_SECONDS,
    }
    settings.update(_read_settings_toml())
    for key in list(settings.keys()):
        env_value = os.environ.get(key)
        if env_value is not None:
            settings[key] = env_value
    return settings


def _read_streamlit_secret(name: str) -> str:
    try:
        return str(st.secrets.get(name, "")).strip()
    except Exception:
        return ""


def _github_control_token(*, use_streamlit_secrets: bool) -> str:
    if use_streamlit_secrets:
        return _read_streamlit_secret(GITHUB_CONTROL_TOKEN_SECRET_NAME)
    return str(os.environ.get(GITHUB_CONTROL_TOKEN_SECRET_NAME, "")).strip()


def get_github_control_config(settings: dict[str, Any] | None = None) -> dict[str, str]:
    settings = settings or get_settings()
    return {
        "repository": str(settings.get("GITHUB_REPOSITORY", DEFAULT_GITHUB_REPOSITORY)).strip() or DEFAULT_GITHUB_REPOSITORY,
        "control_branch": str(settings.get("GITHUB_CONTROL_BRANCH", DEFAULT_GITHUB_CONTROL_BRANCH)).strip() or DEFAULT_GITHUB_CONTROL_BRANCH,
        "deploy_branch": str(settings.get("GITHUB_DEPLOY_BRANCH", DEFAULT_GITHUB_DEPLOY_BRANCH)).strip() or DEFAULT_GITHUB_DEPLOY_BRANCH,
        "request_path": str(settings.get("GITHUB_CONTROL_REQUEST_PATH", DEFAULT_GITHUB_REQUEST_PATH)).strip() or DEFAULT_GITHUB_REQUEST_PATH,
        "status_path": str(settings.get("GITHUB_CONTROL_STATUS_PATH", DEFAULT_GITHUB_STATUS_PATH)).strip() or DEFAULT_GITHUB_STATUS_PATH,
        "deploy_snapshot_json_path": str(settings.get("GITHUB_DEPLOY_SNAPSHOT_JSON_PATH", DEFAULT_GITHUB_DEPLOY_SNAPSHOT_JSON_PATH)).strip() or DEFAULT_GITHUB_DEPLOY_SNAPSHOT_JSON_PATH,
        "deploy_snapshot_md_path": str(settings.get("GITHUB_DEPLOY_SNAPSHOT_MD_PATH", DEFAULT_GITHUB_DEPLOY_SNAPSHOT_MD_PATH)).strip() or DEFAULT_GITHUB_DEPLOY_SNAPSHOT_MD_PATH,
    }


def _viewer_auto_refresh_seconds(settings: dict[str, Any] | None = None) -> int:
    settings = settings or get_settings()
    raw_value = settings.get("VIEWER_AUTO_REFRESH_SECONDS", DEFAULT_VIEWER_AUTO_REFRESH_SECONDS)
    try:
        return max(0, int(raw_value))
    except Exception:
        return DEFAULT_VIEWER_AUTO_REFRESH_SECONDS


def _enable_viewer_auto_refresh(settings: dict[str, Any] | None = None) -> None:
    refresh_seconds = _viewer_auto_refresh_seconds(settings)
    if refresh_seconds <= 0:
        return
    components.html(
        f"""
        <script>
        const refreshMs = {refresh_seconds * 1000};
        window.setTimeout(function () {{
            const parentWindow = window.parent;
            if (parentWindow && parentWindow.location) {{
                parentWindow.location.reload();
            }} else {{
                window.location.reload();
            }}
        }}, refreshMs);
        </script>
        """,
        height=0,
        width=0,
    )
    st.caption(f"viewer は約 {refresh_seconds} 秒ごとに自動更新します。")


def _github_api_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
    }


def _github_contents_url(repository: str, path: str) -> str:
    return f"https://api.github.com/repos/{repository}/contents/{path}"


def _github_read_raw_text_fallback(
    repository: str,
    branch: str,
    path: str,
    token: str,
    *,
    session: requests.sessions.Session | None = None,
    download_url: str = "",
) -> str:
    session = session or requests
    fallback_errors: list[str] = []
    if str(download_url or "").strip():
        response = session.get(
            str(download_url).strip(),
            headers=_github_api_headers(token),
            timeout=20,
        )
        if response.status_code == 404:
            fallback_errors.append("download_url=404")
        elif response.status_code < 400 and response.text:
            return response.text
        else:
            fallback_errors.append(f"download_url_status={response.status_code}")
    raw_headers = dict(_github_api_headers(token))
    raw_headers["Accept"] = "application/vnd.github.raw"
    response = session.get(
        _github_contents_url(repository, path),
        headers=raw_headers,
        params={"ref": branch, "_ts": str(time.time_ns())},
        timeout=20,
    )
    if response.status_code == 404:
        raise FileNotFoundError(f"GitHub file not found: {repository}@{branch}:{path}")
    if response.status_code >= 400:
        details = ", ".join(fallback_errors)
        suffix = f" fallback={details}" if details else ""
        raise RuntimeError(f"GitHub raw read failed status={response.status_code} path={path}{suffix} body={_short_body(response.text)}")
    if not response.text:
        details = ", ".join(fallback_errors)
        suffix = f" fallback={details}" if details else ""
        raise RuntimeError(f"GitHub raw read returned empty content for {path}{suffix}")
    return response.text


def github_read_json_file(
    repository: str,
    branch: str,
    path: str,
    token: str,
    *,
    session: requests.sessions.Session | None = None,
) -> tuple[dict[str, Any], str]:
    text, sha = github_read_text_file(repository, branch, path, token, session=session)
    return json.loads(text), sha


def github_read_text_file(
    repository: str,
    branch: str,
    path: str,
    token: str,
    *,
    session: requests.sessions.Session | None = None,
) -> tuple[str, str]:
    session = session or requests
    response = session.get(
        _github_contents_url(repository, path),
        headers=_github_api_headers(token),
        params={"ref": branch, "_ts": str(time.time_ns())},
        timeout=20,
    )
    if response.status_code == 404:
        raise FileNotFoundError(f"GitHub file not found: {repository}@{branch}:{path}")
    if response.status_code >= 400:
        raise RuntimeError(f"GitHub read failed status={response.status_code} path={path} body={_short_body(response.text)}")
    payload = response.json()
    sha = str(payload.get("sha", ""))
    encoded = str(payload.get("content", "")).replace("\n", "")
    encoding = str(payload.get("encoding", "") or "").strip().lower()
    if encoded and encoding in {"base64", ""}:
        text = base64.b64decode(encoded).decode("utf-8")
        if text:
            return text, sha
    text = _github_read_raw_text_fallback(
        repository,
        branch,
        path,
        token,
        session=session,
        download_url=str(payload.get("download_url", "") or ""),
    )
    return text, sha


def github_write_json_file(
    repository: str,
    branch: str,
    path: str,
    token: str,
    payload: dict[str, Any],
    message: str,
    *,
    sha: str = "",
    session: requests.sessions.Session | None = None,
) -> dict[str, Any]:
    session = session or requests
    response = session.put(
        _github_contents_url(repository, path),
        headers=_github_api_headers(token),
        json={
            "message": message,
            "content": base64.b64encode(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")).decode("ascii"),
            "branch": branch,
            **({"sha": sha} if sha else {}),
        },
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"GitHub write failed status={response.status_code} path={path} body={_short_body(response.text)}")
    return response.json()


def github_write_text_file(
    repository: str,
    branch: str,
    path: str,
    token: str,
    text: str,
    message: str,
    *,
    sha: str = "",
    session: requests.sessions.Session | None = None,
) -> dict[str, Any]:
    session = session or requests
    response = session.put(
        _github_contents_url(repository, path),
        headers=_github_api_headers(token),
        json={
            "message": message,
            "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
            "branch": branch,
            **({"sha": sha} if sha else {}),
        },
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"GitHub write failed status={response.status_code} path={path} body={_short_body(response.text)}")
    return response.json()


def read_control_plane_request(token: str, settings: dict[str, Any] | None = None, *, session: requests.sessions.Session | None = None) -> tuple[dict[str, Any], str]:
    config = get_github_control_config(settings)
    return github_read_json_file(config["repository"], config["control_branch"], config["request_path"], token, session=session)


def write_control_plane_request(
    token: str,
    payload: dict[str, Any],
    settings: dict[str, Any] | None = None,
    *,
    sha: str = "",
    session: requests.sessions.Session | None = None,
    message: str = "Update control-plane request",
) -> dict[str, Any]:
    config = get_github_control_config(settings)
    return github_write_json_file(config["repository"], config["control_branch"], config["request_path"], token, payload, message, sha=sha, session=session)


def read_control_plane_status(token: str, settings: dict[str, Any] | None = None, *, session: requests.sessions.Session | None = None) -> tuple[dict[str, Any], str]:
    config = get_github_control_config(settings)
    return github_read_json_file(config["repository"], config["control_branch"], config["status_path"], token, session=session)


def write_control_plane_status(
    token: str,
    payload: dict[str, Any],
    settings: dict[str, Any] | None = None,
    *,
    sha: str = "",
    session: requests.sessions.Session | None = None,
    message: str = "Update control-plane status",
) -> dict[str, Any]:
    config = get_github_control_config(settings)
    return github_write_json_file(config["repository"], config["control_branch"], config["status_path"], token, payload, message, sha=sha, session=session)


def submit_control_plane_update_request(
    token: str,
    settings: dict[str, Any] | None = None,
    *,
    requested_by: str = "streamlit-viewer",
    requested_mode: str = DEFAULT_CONTROL_PLANE_REQUEST_MODE,
    session: requests.sessions.Session | None = None,
) -> tuple[bool, dict[str, Any]]:
    payload, sha = read_control_plane_request(token, settings, session=session)
    if bool(payload.get("request_update")):
        return False, payload
    mode = normalize_cloud_viewer_mode(requested_mode)
    updated_payload = dict(payload)
    updated_payload.update(
        {
            "request_update": True,
            "request_mode": mode,
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "requested_by": str(requested_by),
            "status": "pending",
        }
    )
    write_control_plane_request(token, updated_payload, settings, sha=sha, session=session, message=f"Request {mode} snapshot refresh from viewer")
    return True, updated_payload


def get_api_key(settings: dict[str, Any] | None = None) -> str:
    settings = settings or get_settings()
    streamlit_secret = _read_streamlit_secret("JQUANTS_API_KEY") if _is_streamlit_runtime() else ""
    api_key = streamlit_secret or str(os.environ.get("JQUANTS_API_KEY", "")).strip() or str(settings.get("JQUANTS_API_KEY", "")).strip()
    if not api_key:
        raise RuntimeError("J-Quants API key is missing. Set JQUANTS_API_KEY or config/settings.toml.")
    return api_key


def _normalize_code4(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:4] if len(digits) >= 4 else digits


def _is_code4(value: Any) -> bool:
    return bool(re.fullmatch(r"\d{4}", str(value or "")))


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> str:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"Expected columns {candidates} not found. actual={list(df.columns)}")


def pick_optional_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _normalize_security_text(value: Any) -> str:
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if not text or text.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", text).upper()


def _normalize_security_code(value: Any) -> str:
    text = _normalize_security_text(value).replace(" ", "")
    if text.endswith(".T"):
        text = text[:-2]
    return text


_EMBEDDED_SECURITY_CODE_PATTERN = re.compile(
    r"(?:(?<=^)|(?<=[\s\(\[（【]))([0-9]{4,5}|[0-9]{3}[A-Z])(?:(?=$)|(?=[\s\)\]）】]))",
    re.IGNORECASE,
)


def _extract_embedded_security_codes(value: Any) -> list[str]:
    if value is None:
        return []
    text = unicodedata.normalize("NFKC", str(value))
    codes: list[str] = []
    for matched in _EMBEDDED_SECURITY_CODE_PATTERN.findall(text):
        normalized = _normalize_security_code(matched)
        if not normalized:
            continue
        codes.extend(_security_code_lookup_keys(normalized))
    return list(dict.fromkeys([code for code in codes if code]))


def _strip_embedded_security_codes(value: Any) -> str:
    text = _normalize_security_text(value)
    if not text:
        return ""
    text = re.sub(r"[\(（【]\s*(?:[0-9]{4,5}|[0-9]{3}[A-Z])\s*[\)）】]", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"^(?:[0-9]{4,5}|[0-9]{3}[A-Z])\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+(?:[0-9]{4,5}|[0-9]{3}[A-Z])$", "", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def _security_code_lookup_keys(value: Any) -> list[str]:
    normalized = _normalize_security_code(value)
    if not normalized:
        return []
    keys = [normalized]
    if re.fullmatch(r"\d{5}", normalized) and normalized.endswith("0"):
        keys.append(normalized[:-1])
    elif re.fullmatch(r"\d{4}", normalized):
        keys.append(f"{normalized}0")
    return list(dict.fromkeys([key for key in keys if key]))


def _normalize_iso_date_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    parsed = pd.to_datetime([text], errors="coerce")
    if len(parsed) and pd.notna(parsed[0]):
        return parsed[0].strftime("%Y-%m-%d")
    return text[:10]


def _build_earnings_announcement_lookup(*frames: Any) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for frame in frames:
        if not isinstance(frame, pd.DataFrame) or frame.empty or "code" not in frame.columns:
            continue
        dates = frame.get("earnings_announcement_date", pd.Series("", index=frame.index))
        for code_value, date_value in zip(frame["code"], dates):
            normalized_code = _normalize_security_code(code_value)
            normalized_date = _normalize_iso_date_text(date_value)
            if normalized_code and normalized_date:
                lookup[normalized_code] = normalized_date
    return lookup


def _resolve_frame_earnings_announcement_dates(
    frame: pd.DataFrame,
    *,
    lookup: dict[str, str] | None = None,
) -> pd.Series:
    if frame is None or frame.empty:
        return pd.Series(dtype="object")
    resolved = frame.get("earnings_announcement_date", pd.Series("", index=frame.index)).apply(_normalize_iso_date_text)
    if not lookup or "code" not in frame.columns:
        return resolved
    normalized_codes = frame["code"].map(_normalize_security_code)
    lookup_dates = normalized_codes.map(lambda value: lookup.get(str(value), "") if str(value) else "")
    fill_mask = resolved.eq("") & lookup_dates.astype(str).str.strip().ne("")
    if fill_mask.any():
        resolved = resolved.copy()
        resolved.loc[fill_mask] = lookup_dates.loc[fill_mask]
    return resolved


def _prefer_security_reference(current: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    if not current:
        return dict(candidate)
    current_has_date = bool(str(current.get("earnings_announcement_date", "") or "").strip())
    candidate_has_date = bool(str(candidate.get("earnings_announcement_date", "") or "").strip())
    if candidate_has_date and not current_has_date:
        return dict(candidate)
    if candidate.get("sector_name") and not current.get("sector_name"):
        return dict(candidate)
    if candidate.get("name") and not current.get("name"):
        return dict(candidate)
    return current


def _build_security_reference_lookup(
    *frames: Any,
    earnings_announcement_lookup: dict[str, str] | None = None,
) -> dict[str, Any]:
    by_code: dict[str, dict[str, Any]] = {}
    by_sector_name: dict[tuple[str, str], dict[str, Any]] = {}
    by_name: dict[str, dict[str, dict[str, Any]]] = {}
    announcement_lookup = earnings_announcement_lookup or {}
    for frame in frames:
        if not isinstance(frame, pd.DataFrame) or frame.empty or "code" not in frame.columns:
            continue
        names = frame.get("name", pd.Series("", index=frame.index))
        sectors = frame.get("sector_name", pd.Series("", index=frame.index))
        raw_dates = frame.get("earnings_announcement_date", pd.Series("", index=frame.index))
        for code_value, name_value, sector_value, raw_date_value in zip(frame["code"], names, sectors, raw_dates):
            normalized_code = _normalize_security_code(code_value)
            if not normalized_code:
                continue
            normalized_name = _normalize_security_text(name_value)
            stripped_name = _strip_embedded_security_codes(name_value)
            candidate_name = stripped_name or normalized_name
            normalized_sector = _normalize_industry_key(sector_value)
            normalized_date = announcement_lookup.get(normalized_code, "") or _normalize_iso_date_text(raw_date_value)
            candidate = {
                "code": normalized_code,
                "name": candidate_name,
                "sector_name": normalized_sector,
                "earnings_announcement_date": normalized_date,
            }
            by_code[normalized_code] = _prefer_security_reference(by_code.get(normalized_code), candidate)
            for name_key in dict.fromkeys([normalized_name, stripped_name]):
                if not name_key:
                    continue
                by_name.setdefault(name_key, {})
                by_name[name_key][normalized_code] = _prefer_security_reference(
                    by_name[name_key].get(normalized_code),
                    candidate,
                )
                if normalized_sector:
                    sector_key = (normalized_sector, name_key)
                    by_sector_name[sector_key] = _prefer_security_reference(by_sector_name.get(sector_key), candidate)
    return {
        "by_code": by_code,
        "by_sector_name": by_sector_name,
        "by_name": {key: list(code_map.values()) for key, code_map in by_name.items()},
    }


def _resolve_security_reference(
    name: Any,
    *,
    sector_name: Any = "",
    security_reference_lookup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(security_reference_lookup, dict) or not security_reference_lookup:
        return {}
    by_code = security_reference_lookup.get("by_code", {})
    normalized_sector = _normalize_industry_key(sector_name)
    explicit_code_candidates = [by_code.get(code) for code in _extract_embedded_security_codes(name)]
    explicit_code_candidates = [candidate for candidate in explicit_code_candidates if isinstance(candidate, dict) and candidate]
    if normalized_sector:
        sector_matched = [
            candidate
            for candidate in explicit_code_candidates
            if _normalize_industry_key(candidate.get("sector_name")) == normalized_sector
        ]
        if len(sector_matched) == 1:
            return sector_matched[0]
    if len(explicit_code_candidates) == 1:
        return explicit_code_candidates[0]
    normalized_name = _normalize_security_text(name)
    stripped_name = _strip_embedded_security_codes(name)
    name_keys = [key for key in dict.fromkeys([stripped_name, normalized_name]) if key]
    if not name_keys:
        return {}
    by_sector_name = security_reference_lookup.get("by_sector_name", {})
    for name_key in name_keys:
        if normalized_sector:
            matched = by_sector_name.get((normalized_sector, name_key))
            if isinstance(matched, dict) and matched:
                return matched
    by_name = security_reference_lookup.get("by_name", {})
    for name_key in name_keys:
        candidates = by_name.get(name_key, [])
        if not candidates:
            continue
        if len(candidates) == 1:
            return candidates[0]
        with_date = [candidate for candidate in candidates if str(candidate.get("earnings_announcement_date", "") or "").strip()]
        unique_codes = {str(candidate.get("code", "") or "").strip() for candidate in candidates if str(candidate.get("code", "") or "").strip()}
        if len(with_date) == 1:
            return with_date[0]
        if len(unique_codes) == 1:
            return candidates[0]
    return {}


def _representative_stocks_to_frame(frame: Any) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty or "representative_stocks" not in frame.columns:
        return pd.DataFrame(columns=["sector_name", "code", "name", "earnings_announcement_date"])
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        sector_name = _clean_ui_value(row.get("sector_name"))
        for item in row.get("representative_stocks", []):
            if not isinstance(item, dict):
                continue
            code = _normalize_security_code(item.get("code"))
            if not code:
                continue
            rows.append(
                {
                    "sector_name": sector_name,
                    "code": code,
                    "name": _normalize_security_text(item.get("name")),
                    "earnings_announcement_date": _normalize_iso_date_text(item.get("earnings_announcement_date")),
                }
            )
    return pd.DataFrame(rows)


def _snapshot_security_frames(bundle: dict[str, Any]) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for key in [
        "sector_representatives_display",
        "sector_representatives",
        "focus_candidates",
        "swing_candidates_1w",
        "swing_candidates_1m",
        "swing_candidates_3m",
        "swing_candidates_1w_display",
        "swing_candidates_1m_display",
        "swing_candidates_3m_display",
    ]:
        frame = bundle.get(key, pd.DataFrame())
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            frames.append(frame)
    for key in ["today_sector_leaderboard", "sector_persistence_1w", "sector_persistence_1m", "sector_persistence_3m"]:
        representative_stocks_frame = _representative_stocks_to_frame(bundle.get(key, pd.DataFrame()))
        if not representative_stocks_frame.empty:
            frames.append(representative_stocks_frame)
    return frames


def get_edinetdb_api_key() -> str:
    return str(os.environ.get("EDINETDB_API_KEY", "")).strip()


def _pick_first_non_empty_text(*values: Any) -> str:
    for value in values:
        text = _normalize_security_text(value)
        if text:
            return text
    return ""


def _pick_first_non_empty_label(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return text
    return ""


def _match_non_corporate_product_reason(text: Any) -> str:
    normalized = _normalize_security_text(text)
    if not normalized:
        return ""
    for reason, pattern in NON_CORPORATE_PRODUCT_DIRECT_PATTERNS:
        if pattern.search(normalized):
            return reason
    if NON_CORPORATE_PRODUCT_INDEX_PATTERN.search(normalized) and NON_CORPORATE_PRODUCT_INDEX_LINK_PATTERN.search(normalized):
        return "index_product"
    if NON_CORPORATE_PRODUCT_BRAND_PATTERN.search(normalized) and (
        NON_CORPORATE_PRODUCT_INDEX_PATTERN.search(normalized) or NON_CORPORATE_PRODUCT_INDEX_LINK_PATTERN.search(normalized)
    ):
        return "listed_product_brand"
    return ""


def _classify_non_corporate_product_row(row: pd.Series) -> str:
    for column in NON_CORPORATE_PRODUCT_ATTRIBUTE_COLUMNS:
        reason = _match_non_corporate_product_reason(row.get(column, ""))
        if reason:
            return f"attr:{column}:{reason}"
    name_reason = _match_non_corporate_product_reason(
        _pick_first_non_empty_text(
            row.get("name", ""),
            row.get("ranking_name", ""),
            row.get("Name", ""),
        )
    )
    return f"name:{name_reason}" if name_reason else ""


def _annotate_non_corporate_products(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame(columns=["is_non_corporate_product", "non_corporate_product_reason"])
    out = frame.copy()
    if out.empty:
        out["is_non_corporate_product"] = pd.Series(dtype=bool)
        out["non_corporate_product_reason"] = pd.Series(dtype=str)
        return out
    reasons = out.apply(_classify_non_corporate_product_row, axis=1)
    out["non_corporate_product_reason"] = reasons.fillna("").astype(str)
    out["is_non_corporate_product"] = out["non_corporate_product_reason"].str.strip().ne("")
    return out


def _record_non_corporate_product_diagnostics(diagnostics: dict[str, Any] | None, frame: pd.DataFrame, *, context: str) -> None:
    if diagnostics is None:
        return
    excluded = frame[frame.get("is_non_corporate_product", pd.Series(False, index=frame.index)).fillna(False)].copy() if not frame.empty else frame.copy()
    sample_names: list[str] = []
    if not excluded.empty:
        for _, row in excluded.head(8).iterrows():
            sample_name = _pick_first_non_empty_label(row.get("name", ""), row.get("ranking_name", ""), row.get("code", ""))
            if sample_name and sample_name not in sample_names:
                sample_names.append(sample_name)
            if len(sample_names) >= 5:
                break
    diagnostics.setdefault("non_corporate_products", {})[context] = {
        "excluded_count": int(len(excluded)),
        "sample_names": sample_names,
    }
    if len(excluded):
        logger.info("excluded non-corporate products context=%s count=%s sample=%s", context, len(excluded), sample_names)


def _exclude_non_corporate_products(frame: pd.DataFrame, diagnostics: dict[str, Any] | None = None, *, context: str) -> pd.DataFrame:
    annotated = _annotate_non_corporate_products(frame)
    _record_non_corporate_product_diagnostics(diagnostics, annotated, context=context)
    if annotated.empty:
        return annotated
    return annotated[~annotated["is_non_corporate_product"].fillna(False)].copy()


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    backoff_seconds = [1.5, 3.0, 6.0]
    for attempt in range(len(backoff_seconds) + 1):
        response = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout)
        if response.status_code in {401, 403}:
            raise JQuantsAuthError(f"J-Quants authentication failed (401/403). The API key is invalid or expired. body={_short_body(response.text)}")
        if response.status_code == 429 and attempt < len(backoff_seconds):
            retry_after = _coerce_numeric(pd.Series([response.headers.get("Retry-After")])).iloc[0]
            sleep_seconds = float(retry_after) if pd.notna(retry_after) and float(retry_after) > 0 else backoff_seconds[attempt]
            logger.warning("rate limited status=429 url=%s sleep=%.1fs attempt=%s", response.url, sleep_seconds, attempt + 1)
            time.sleep(sleep_seconds)
            continue
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP error status={response.status_code} url={response.url} body={_short_body(response.text)}")
        return response.json() if response.text else {}
    raise RuntimeError(f"HTTP error status=429 url={url} body=rate limit retry exhausted")


def jquants_get_all(path: str, params: dict[str, Any], api_key: str | None = None) -> list[dict[str, Any]]:
    api_key = api_key or get_api_key()
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    url = f"{BASE_URL}{path}"
    rows_all: list[dict[str, Any]] = []
    pagination_key = None
    while True:
        request_params = dict(params)
        if pagination_key:
            request_params["pagination_key"] = pagination_key
        payload = _request_json("GET", url, headers=headers, params=request_params, timeout=30.0)
        rows = payload.get("data", [])
        if isinstance(rows, list):
            rows_all.extend(rows)
        pagination_key = payload.get("pagination_key")
        if not pagination_key:
            return rows_all
        time.sleep(0.03)


def get_recent_trading_dates(n: int = 260, *, api_key: str | None = None) -> list[str]:
    logger.info("get_recent_trading_dates start n=%s", n)
    today = datetime.now().date()
    start_date = (today - timedelta(days=max(n * 2, 420))).isoformat()
    rows = jquants_get_all("/markets/calendar", {"from": start_date, "to": today.isoformat()}, api_key=api_key)
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("markets/calendar returned no rows.")
    date_col = pick_first_existing(df, ["Date", "date"])
    flag_col = pick_optional_existing(df, ["HolidayDivisionName", "HolidayDivision", "IsTradingDay", "TradingDay"])
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).copy()
    df = df[df[date_col].dt.dayofweek < 5].copy()
    if flag_col:
        normalized = df[flag_col].astype(str).str.lower()
        keep = normalized.str.contains("open|trading|営業") | ~normalized.str.contains("holiday|closed|休日|休場")
        df = df[keep].copy()
    dates = df[date_col].dt.strftime("%Y-%m-%d").drop_duplicates().sort_values().tolist()
    if len(dates) < n:
        raise RuntimeError(f"Not enough trading dates. count={len(dates)} expected={n}")
    logger.info("get_recent_trading_dates end count=%s", len(dates[-n:]))
    return dates[-n:]


def get_master_df(date_str: str, *, api_key: str | None = None) -> pd.DataFrame:
    df = pd.DataFrame(jquants_get_all("/equities/master", {"date": date_str}, api_key=api_key))
    if df.empty:
        raise RuntimeError("equities/master returned no rows.")
    df["code"] = df["Code"].astype(str).map(_normalize_code4)
    df = df[df["code"].map(_is_code4)].copy()
    out = pd.DataFrame(
        {
            "code": df["code"],
            "name": df.get("CoName", "").astype(str),
            "sector_name": df.get("S33Nm", "").astype(str),
            "sector_code": df.get("S33", "").astype(str),
            "exchange_name": df.get("MktNm", "").astype(str),
        }
    )
    for output_col, candidate_cols in MASTER_PRODUCT_ATTRIBUTE_SOURCES.items():
        source_col = pick_optional_existing(df, candidate_cols)
        out[output_col] = df[source_col].astype(str) if source_col else pd.Series([""] * len(df), index=df.index, dtype=str)
    return out.drop_duplicates(subset=["code"]).reset_index(drop=True)


def get_price_df(date_str: str, *, api_key: str | None = None) -> pd.DataFrame:
    df = pd.DataFrame(jquants_get_all("/equities/bars/daily", {"date": date_str}, api_key=api_key))
    if df.empty:
        return pd.DataFrame(columns=["code", "date", "close", "volume", "turnover"])
    df["code"] = df["Code"].astype(str).map(_normalize_code4)
    df = df[df["code"].map(_is_code4)].copy()
    close_col = pick_first_existing(df, ["AdjClose", "AdjustmentClose", "Close", "AdjC", "C"])
    volume_col = pick_optional_existing(df, ["Volume", "Vo", "Vol", "V"])
    turnover_col = pick_optional_existing(df, ["TurnoverValue", "TradingValue", "Va"])
    out = pd.DataFrame(
        {
            "code": df["code"],
            "date": date_str,
            "close": _coerce_numeric(df[close_col]),
            "volume": _coerce_numeric(df[volume_col]) if volume_col else pd.NA,
            "turnover": _coerce_numeric(df[turnover_col]) if turnover_col else pd.NA,
        }
    )
    return out.dropna(subset=["close"]).drop_duplicates(subset=["code"]).reset_index(drop=True)


def get_price_history(trading_dates: list[str], *, api_key: str | None = None, lookback_days: int = 80) -> pd.DataFrame:
    date_list = trading_dates[-lookback_days:]
    logger.info("get_price_history start: date count=%s", len(date_list))
    frames: list[pd.DataFrame] = []
    total = len(date_list)
    for idx, date_str in enumerate(date_list, start=1):
        frames.append(get_price_df(date_str, api_key=api_key))
        time.sleep(0.2)
        if idx == total or idx % 10 == 0:
            logger.info("get_price_df progress %s/%s", idx, total)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["code", "date", "close", "volume", "turnover"])
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out.dropna(subset=["date"]).sort_values(["code", "date"]).reset_index(drop=True)


def _extract_offset_close(aligned_close: pd.DataFrame, trading_dates: list[str], offset: int, column_name: str) -> pd.DataFrame:
    if aligned_close.empty:
        return pd.DataFrame(columns=["code", column_name])
    if len(trading_dates) < offset + 1:
        return pd.DataFrame(columns=["code", column_name])
    target_date = pd.Timestamp(trading_dates[-(offset + 1)])
    series = aligned_close.loc[target_date] if target_date in aligned_close.index else pd.Series(dtype=float)
    return series.rename(column_name).reset_index().rename(columns={"index": "code"})


def _classify_optional_dataset_error(exc: Exception) -> tuple[str, str]:
    message = str(exc or "").strip()
    lowered = message.lower()
    if isinstance(exc, requests.exceptions.Timeout):
        return "network_or_timeout", message
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.ProxyError)):
        return "network_or_timeout", message
    if isinstance(exc, requests.exceptions.RequestException):
        return "network_or_timeout", message
    if "status=404" in lowered or "does not exist" in lowered or "endpoint does not exist" in lowered:
        return "endpoint_not_found", message
    if "status=401" in lowered or "status=403" in lowered or "authentication failed" in lowered or "permission" in lowered:
        return "auth_or_permission_error", message
    if "status=400" in lowered:
        return "bad_request", message
    return "unknown_error", message


def _request_edinetdb_json(
    url: str,
    *,
    params: dict[str, Any],
    api_key: str = "",
    timeout: float = 20.0,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    response = requests.get(url, headers=headers, params=params, timeout=timeout)
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP error status={response.status_code} url={response.url} body={_short_body(response.text)}")
    try:
        return response.json() if response.text else {}
    except ValueError as exc:
        raise RuntimeError(f"Invalid JSON from {response.url}: {_short_body(response.text)}") from exc


def _extract_edinetdb_calendar_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", payload)
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ["calendar", "items", "results", "rows"]:
            value = data.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _normalize_edinetdb_calendar_record(row: dict[str, Any]) -> dict[str, str] | None:
    code = _pick_first_non_empty_text(
        row.get("secCode"),
        row.get("sec_code"),
        row.get("security_code"),
        row.get("securityCode"),
        row.get("code"),
    )
    announcement_date = _normalize_iso_date_text(
        _pick_first_non_empty_text(
            row.get("announcementDate"),
            row.get("announcement_date"),
            row.get("date"),
            row.get("earningsDate"),
            row.get("earnings_date"),
            row.get("scheduledDate"),
            row.get("scheduled_date"),
            row.get("disclosure_date"),
        )
    )
    normalized_code = _normalize_security_code(code)
    if not normalized_code or not announcement_date:
        return None
    return {
        "code": normalized_code,
        "announcement_date": announcement_date,
        "company_name": _pick_first_non_empty_label(
            row.get("companyName"),
            row.get("company_name"),
            row.get("name"),
            row.get("filerName"),
        ),
        "period_type": _pick_first_non_empty_label(
            row.get("periodType"),
            row.get("period_type"),
            row.get("quarter_type"),
            row.get("type"),
        ),
        "market_segment": _pick_first_non_empty_label(
            row.get("marketSegment"),
            row.get("market_segment"),
            row.get("market"),
        ),
    }


def _dedupe_edinetdb_calendar_records(records: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str], dict[str, str]] = {}
    for record in records:
        key = (str(record.get("code", "")).strip(), str(record.get("announcement_date", "")).strip())
        if not key[0] or not key[1]:
            continue
        deduped.setdefault(key, record)
    return sorted(deduped.values(), key=lambda item: (item.get("announcement_date", ""), item.get("code", "")))


def _edinetdb_calendar_cache_path(target_date: pd.Timestamp) -> Path:
    return EDINETDB_CALENDAR_CACHE_DIR / f"edinetdb_calendar_{target_date.strftime('%Y%m%d')}.json"


def _load_edinetdb_calendar_cache(cache_path: Path) -> dict[str, Any] | None:
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as exc:
        logger.warning("EDINET DB cache read failed path=%s detail=%s", cache_path, exc)
        return None
    if int(payload.get("version", 0) or 0) != EDINETDB_CALENDAR_CACHE_VERSION:
        return None
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        return None
    return payload


def _write_edinetdb_calendar_cache(cache_path: Path, payload: dict[str, Any]) -> str:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with cache_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        return ""
    except Exception as exc:
        logger.warning("EDINET DB cache write failed path=%s detail=%s", cache_path, exc)
        return str(exc)


def _fetch_edinetdb_calendar_chunk(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    *,
    api_key: str,
    depth: int = 0,
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    params = {
        "from": start_date.strftime("%Y-%m-%d"),
        "to": end_date.strftime("%Y-%m-%d"),
        "limit": EDINETDB_CALENDAR_LIMIT,
    }
    payload = _request_edinetdb_json(EDINETDB_CALENDAR_ENDPOINT, params=params, api_key=api_key)
    normalized_rows = [
        record
        for record in (
            _normalize_edinetdb_calendar_record(row)
            for row in _extract_edinetdb_calendar_rows(payload)
        )
        if record
    ]
    if len(normalized_rows) >= EDINETDB_CALENDAR_LIMIT and start_date < end_date and depth < 8:
        midpoint = start_date + timedelta(days=max(1, (end_date - start_date).days // 2))
        if midpoint >= end_date:
            midpoint = end_date - timedelta(days=1)
        if midpoint >= start_date:
            left_rows, left_chunks = _fetch_edinetdb_calendar_chunk(start_date, midpoint, api_key=api_key, depth=depth + 1)
            right_rows, right_chunks = _fetch_edinetdb_calendar_chunk(midpoint + timedelta(days=1), end_date, api_key=api_key, depth=depth + 1)
            return _dedupe_edinetdb_calendar_records(left_rows + right_rows), left_chunks + right_chunks
    return normalized_rows, [{"from": params["from"], "to": params["to"], "row_count": int(len(normalized_rows)), "split_depth": int(depth)}]


def _fetch_edinetdb_calendar_records(
    *,
    target_date: pd.Timestamp,
    api_key: str,
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    start_date = target_date.normalize()
    end_date = start_date + timedelta(days=EDINETDB_CALENDAR_WINDOW_DAYS)
    all_rows: list[dict[str, str]] = []
    chunk_meta: list[dict[str, Any]] = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=EDINETDB_CALENDAR_CHUNK_DAYS - 1), end_date)
        rows, meta = _fetch_edinetdb_calendar_chunk(current, chunk_end, api_key=api_key)
        all_rows.extend(rows)
        chunk_meta.extend(meta)
        current = chunk_end + timedelta(days=1)
    return _dedupe_edinetdb_calendar_records(all_rows), chunk_meta


def _get_optional_dataset(path: str, params: dict[str, Any], *, dataset_name: str, api_key: str | None = None) -> pd.DataFrame:
    try:
        return pd.DataFrame(jquants_get_all(path, params, api_key=api_key))
    except Exception as exc:
        reason_code, detail = _classify_optional_dataset_error(exc)
        logger.warning("optional dataset skipped dataset=%s reason=%s path=%s", dataset_name, reason_code, path)
        logger.warning("optional J-Quants dataset unavailable dataset=%s path=%s params=%s reason=%s detail=%s", dataset_name, path, params, reason_code, detail)
        return pd.DataFrame()


def get_topix_history(trading_dates: list[str], *, api_key: str | None = None) -> pd.DataFrame:
    if not trading_dates:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close"])
    start_date = trading_dates[0]
    end_date = trading_dates[-1]
    path = "/indices/bars/daily/topix"
    params = {"from": start_date, "to": end_date}
    df = _get_optional_dataset(path, params, dataset_name="topix_daily", api_key=api_key)
    if not df.empty:
        date_col = pick_optional_existing(df, ["Date", "date"])
        close_col = pick_optional_existing(df, ["Close", "close", "AdjClose", "AdjustmentClose", "C"])
        open_col = pick_optional_existing(df, ["Open", "open", "O"])
        high_col = pick_optional_existing(df, ["High", "high", "H"])
        low_col = pick_optional_existing(df, ["Low", "low", "L"])
        if date_col and close_col:
            out = pd.DataFrame(
                {
                    "date": pd.to_datetime(df[date_col], errors="coerce"),
                    "open": _coerce_numeric(df[open_col]) if open_col else pd.NA,
                    "high": _coerce_numeric(df[high_col]) if high_col else pd.NA,
                    "low": _coerce_numeric(df[low_col]) if low_col else pd.NA,
                    "close": _coerce_numeric(df[close_col]),
                }
            ).dropna(subset=["date", "close"])
            if not out.empty:
                calendar_index = pd.to_datetime(pd.Index(trading_dates), errors="coerce")
                logger.info("topix direct endpoint used path=%s rows=%s", path, len(out))
                return out.drop_duplicates(subset=["date"]).set_index("date").reindex(calendar_index).ffill().reset_index().rename(columns={"index": "date"})
    # Fallback: use a TOPIX ETF proxy if the dedicated endpoint is not available.
    etf_history = get_price_history(trading_dates, api_key=api_key, lookback_days=len(trading_dates))
    etf_history = etf_history[etf_history["code"] == "1306"].copy()
    if etf_history.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close"])
    logger.warning("topix direct endpoint unavailable; fallback=etf_proxy code=1306")
    out = etf_history[["date", "close"]].copy()
    out["open"] = pd.NA
    out["high"] = pd.NA
    out["low"] = pd.NA
    calendar_index = pd.to_datetime(pd.Index(trading_dates), errors="coerce")
    return out.drop_duplicates(subset=["date"]).set_index("date").reindex(calendar_index).ffill().reset_index().rename(columns={"index": "date"})


def _build_topix_return_map(topix_history: pd.DataFrame, trading_dates: list[str]) -> dict[str, float | None]:
    if topix_history.empty:
        return {"topix_ret_1w": None, "topix_ret_1m": None, "topix_ret_3m": None}
    aligned = topix_history.set_index("date")["close"].sort_index()
    latest_close = aligned.iloc[-1] if len(aligned) else pd.NA
    result: dict[str, float | None] = {"topix_ret_1w": None, "topix_ret_1m": None, "topix_ret_3m": None}
    for label, offset in [("topix_ret_1w", 5), ("topix_ret_1m", 20), ("topix_ret_3m", 63)]:
        if len(trading_dates) < offset + 1:
            continue
        base_date = pd.Timestamp(trading_dates[-(offset + 1)])
        base_close = aligned.loc[base_date] if base_date in aligned.index else pd.NA
        if pd.notna(latest_close) and pd.notna(base_close) and float(base_close) != 0.0:
            result[label] = (float(latest_close) / float(base_close) - 1.0) * 100.0
    return result


def get_earnings_buffer_frame(
    trading_dates: list[str],
    *,
    candidate_codes: list[Any] | None = None,
    api_key: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    meta: dict[str, Any] = {
        "status": "empty_dataset",
        "rows_raw": 0,
        "rows_future_window": 0,
        "rows_target_date": 0,
        "min_event_date": "",
        "max_event_date": "",
        "target_date": "",
        "request_mode": "",
        "source_path": "",
        "announcement_source": "edinetdb_calendar",
        "cache_path": "",
        "cache_hit": False,
        "failure_reason": "",
        "matched_code_count": 0,
        "jquants_fallback_used": False,
    }
    if not trading_dates:
        meta["status"] = "no_trading_dates"
        return pd.DataFrame(columns=["code", "earnings_buffer_days", "earnings_today_announcement_flag", "earnings_announcement_date"]), meta
    latest_date = pd.Timestamp(trading_dates[-1])
    target_date = latest_date.normalize()
    meta["target_date"] = target_date.strftime("%Y-%m-%d")
    meta["source_path"] = "/v1/calendar"
    cache_path = _edinetdb_calendar_cache_path(target_date)
    meta["cache_path"] = str(cache_path)
    requested_codes = [
        code
        for code in (
            _normalize_security_code(value)
            for value in (candidate_codes or [])
        )
        if code
    ]
    requested_frame = pd.DataFrame({"code": list(dict.fromkeys(requested_codes))})
    cache_payload = _load_edinetdb_calendar_cache(cache_path)
    normalized_records: list[dict[str, str]] = []
    if cache_payload:
        meta["cache_hit"] = True
        meta["request_mode"] = "cache_hit"
        normalized_records = _dedupe_edinetdb_calendar_records(
            [record for record in cache_payload.get("rows", []) if isinstance(record, dict)]
        )
    else:
        calendar_api_key = str(api_key or get_edinetdb_api_key() or "").strip()
        meta["request_mode"] = "api_key" if calendar_api_key else "anonymous"
        try:
            normalized_records, chunk_meta = _fetch_edinetdb_calendar_records(target_date=target_date, api_key=calendar_api_key)
            cache_write_error = _write_edinetdb_calendar_cache(
                cache_path,
                {
                    "version": EDINETDB_CALENDAR_CACHE_VERSION,
                    "target_date": meta["target_date"],
                    "request_mode": meta["request_mode"],
                    "source_path": meta["source_path"],
                    "rows": normalized_records,
                    "chunks": chunk_meta,
                },
            )
            if cache_write_error:
                meta["failure_reason"] = f"cache_write_failed: {cache_write_error}"
        except Exception as exc:
            meta["status"] = "request_failed"
            meta["failure_reason"] = str(exc)
            logger.warning(
                "EDINET DB calendar fetch failed request_mode=%s target_date=%s detail=%s",
                meta["request_mode"],
                meta["target_date"],
                exc,
            )
            return pd.DataFrame(columns=["code", "earnings_buffer_days", "earnings_today_announcement_flag", "earnings_announcement_date"]), meta
    calendar_frame = pd.DataFrame(normalized_records)
    meta["rows_raw"] = int(len(calendar_frame))
    if calendar_frame.empty:
        meta["status"] = "no_rows"
        return pd.DataFrame(columns=["code", "earnings_buffer_days", "earnings_today_announcement_flag", "earnings_announcement_date"]), meta
    calendar_frame["announcement_date"] = pd.to_datetime(calendar_frame.get("announcement_date", pd.Series(dtype=str)), errors="coerce")
    calendar_frame = calendar_frame.dropna(subset=["announcement_date"]).copy()
    calendar_frame = calendar_frame[calendar_frame["announcement_date"].dt.normalize().ge(target_date)].copy()
    if calendar_frame.empty:
        meta["status"] = "no_future_rows"
        return pd.DataFrame(columns=["code", "earnings_buffer_days", "earnings_today_announcement_flag", "earnings_announcement_date"]), meta
    meta["rows_future_window"] = int(len(calendar_frame))
    meta["rows_target_date"] = int(calendar_frame["announcement_date"].dt.normalize().eq(target_date).sum())
    meta["min_event_date"] = calendar_frame["announcement_date"].min().strftime("%Y-%m-%d")
    meta["max_event_date"] = calendar_frame["announcement_date"].max().strftime("%Y-%m-%d")
    lookup_records: list[dict[str, Any]] = []
    for _, row in calendar_frame.iterrows():
        announcement_date = row.get("announcement_date", pd.NaT)
        if pd.isna(announcement_date):
            continue
        for lookup_code in _security_code_lookup_keys(row.get("code", "")):
            lookup_records.append(
                {
                    "lookup_code": lookup_code,
                    "announcement_date": announcement_date,
                    "announcement_date_text": announcement_date.strftime("%Y-%m-%d"),
                }
            )
    lookup_frame = pd.DataFrame(lookup_records).drop_duplicates()
    if requested_frame.empty:
        requested_frame = pd.DataFrame({"code": sorted(set(lookup_frame.get("lookup_code", pd.Series(dtype=str)).astype(str).tolist()))})
    requested_lookup = requested_frame.assign(lookup_code=requested_frame["code"].map(_security_code_lookup_keys)).explode("lookup_code")
    matched = requested_lookup.merge(lookup_frame, on="lookup_code", how="left") if not requested_lookup.empty else pd.DataFrame(columns=["code", "announcement_date", "announcement_date_text"])
    if not matched.empty:
        matched = matched.dropna(subset=["announcement_date"]).sort_values(["code", "announcement_date", "lookup_code"], kind="mergesort")
        matched = matched.drop_duplicates("code", keep="first")
    out = requested_frame.merge(
        matched[["code", "announcement_date", "announcement_date_text"]] if not matched.empty else pd.DataFrame(columns=["code", "announcement_date", "announcement_date_text"]),
        on="code",
        how="left",
    )
    if "announcement_date" in out.columns:
        out["earnings_buffer_days"] = (
            (pd.to_datetime(out["announcement_date"], errors="coerce").dt.normalize() - target_date).dt.days.astype("Int64")
        )
    else:
        out["earnings_buffer_days"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")
    out["earnings_today_announcement_flag"] = out["earnings_buffer_days"].eq(0).fillna(False)
    out["earnings_announcement_date"] = out.get("announcement_date_text", pd.Series("", index=out.index)).fillna("").astype(str)
    meta["matched_code_count"] = int(out["earnings_announcement_date"].astype(str).str.strip().ne("").sum()) if "earnings_announcement_date" in out.columns else 0
    meta["status"] = "ok"
    return out[["code", "earnings_buffer_days", "earnings_today_announcement_flag", "earnings_announcement_date"]].reset_index(drop=True), meta


def get_finance_health_frame(trading_dates: list[str], *, api_key: str | None = None) -> pd.DataFrame:
    if not trading_dates:
        return pd.DataFrame(columns=["code", "finance_health_score", "finance_health_flag"])
    path = "/fins/summary"
    frames: list[pd.DataFrame] = []
    query_dates = list(reversed(trading_dates[-30:]))
    for date_str in query_dates:
        df = _get_optional_dataset(path, {"date": date_str}, dataset_name="financial_summary", api_key=api_key)
        if df.empty:
            continue
        frames.append(df)
    if frames:
        df = pd.concat(frames, ignore_index=True)
        code_col = pick_optional_existing(df, ["Code", "LocalCode", "code"])
        disclosed_col = pick_optional_existing(df, ["DiscDate", "DisclosedDate", "Date", "date"])
        if code_col:
            out = pd.DataFrame({"code": df[code_col].map(_normalize_code4)})
            out = out[out["code"].map(_is_code4)].copy()
            if not out.empty:
                out["disclosed_date"] = pd.to_datetime(df[disclosed_col], errors="coerce") if disclosed_col else pd.NaT
                revenue = _coerce_numeric(df[pick_optional_existing(df, ["Sales", "Revenue", "NetSales"])]) if pick_optional_existing(df, ["Sales", "Revenue", "NetSales"]) else pd.Series([pd.NA] * len(df))
                operating = _coerce_numeric(df[pick_optional_existing(df, ["OP", "OperatingProfit", "OperatingIncome"])]) if pick_optional_existing(df, ["OP", "OperatingProfit", "OperatingIncome"]) else pd.Series([pd.NA] * len(df))
                ordinary = _coerce_numeric(df[pick_optional_existing(df, ["OdP", "OrdinaryProfit", "OrdinaryIncome"])]) if pick_optional_existing(df, ["OdP", "OrdinaryProfit", "OrdinaryIncome"]) else pd.Series([pd.NA] * len(df))
                profit = _coerce_numeric(df[pick_optional_existing(df, ["NP", "Profit", "NetIncome"])]) if pick_optional_existing(df, ["NP", "Profit", "NetIncome"]) else pd.Series([pd.NA] * len(df))
                equity_ratio = _coerce_numeric(df[pick_optional_existing(df, ["EqAR", "EquityToAssetRatio", "EquityRatio"])]) if pick_optional_existing(df, ["EqAR", "EquityToAssetRatio", "EquityRatio"]) else pd.Series([pd.NA] * len(df))
                cashflow = _coerce_numeric(df[pick_optional_existing(df, ["CFO"])]) if pick_optional_existing(df, ["CFO"]) else pd.Series([pd.NA] * len(df))
                out["finance_health_score"] = 0.0
                out.loc[equity_ratio >= 0.4, "finance_health_score"] += 1.0
                out.loc[(equity_ratio >= 0.2) & (equity_ratio < 0.4), "finance_health_score"] += 0.5
                out.loc[(equity_ratio > 0) & (equity_ratio < 0.1), "finance_health_score"] -= 1.0
                out.loc[operating > 0, "finance_health_score"] += 0.8
                out.loc[operating < 0, "finance_health_score"] -= 1.0
                out.loc[ordinary > 0, "finance_health_score"] += 0.4
                out.loc[ordinary < 0, "finance_health_score"] -= 0.6
                out.loc[profit > 0, "finance_health_score"] += 0.8
                out.loc[profit < 0, "finance_health_score"] -= 1.0
                out.loc[cashflow > 0, "finance_health_score"] += 0.3
                out.loc[cashflow < 0, "finance_health_score"] -= 0.3
                out.loc[(revenue <= 0) & revenue.notna(), "finance_health_score"] -= 0.6
                out["finance_health_flag"] = out["finance_health_score"] >= -0.5
                out = out.sort_values(["code", "disclosed_date"]).drop_duplicates("code", keep="last")
                logger.info("financial summary endpoint used path=%s dates=%s rows=%s", path, len(query_dates), len(out))
                return out[["code", "finance_health_score", "finance_health_flag"]].reset_index(drop=True)
    return pd.DataFrame(columns=["code", "finance_health_score", "finance_health_flag"])


def build_daily_base_data(*, fast_check: bool = False) -> tuple[pd.DataFrame, dict[str, Any]]:
    logger.info("build_daily_base_data start fast_check=%s", fast_check)
    logger.info("building daily base via J-Quants")
    api_key = get_api_key()
    lookback_trading_days = 70 if fast_check else 70
    trading_dates = get_recent_trading_dates(n=lookback_trading_days, api_key=api_key)
    master_df = get_master_df(trading_dates[-1], api_key=api_key)
    price_history = get_price_history(trading_dates, api_key=api_key, lookback_days=lookback_trading_days)
    price_history["volume"] = _coerce_numeric(price_history["volume"])
    price_history["turnover"] = _coerce_numeric(price_history["turnover"])
    price_history["close"] = _coerce_numeric(price_history["close"])
    grouped = price_history.groupby("code", group_keys=False)
    price_history["avg_volume_20d"] = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price_history["avg_turnover_20d"] = grouped["turnover"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price_history["close_ma_20d"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price_history["high_20d"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).max())
    aligned_close = (
        price_history.pivot_table(index="date", columns="code", values="close", aggfunc="last")
        .reindex(pd.to_datetime(pd.Index(trading_dates), errors="coerce"))
        .sort_index()
        .ffill()
    )
    latest = grouped.tail(1).rename(
        columns={
            "close": "close_latest",
            "volume": "volume_latest",
            "turnover": "turnover_latest",
            "date": "latest_date",
            "close_ma_20d": "close_ma_20d",
        }
    )
    week = _extract_offset_close(aligned_close, trading_dates, 5, "close_1w")
    month = _extract_offset_close(aligned_close, trading_dates, 20, "close_1m")
    quarter = _extract_offset_close(aligned_close, trading_dates, 63, "close_3m")
    base = master_df.merge(
        latest[["code", "close_latest", "volume_latest", "turnover_latest", "latest_date", "avg_volume_20d", "avg_turnover_20d", "close_ma_20d", "high_20d"]],
        on="code",
        how="inner",
    )
    base = base.merge(week, on="code", how="left").merge(month, on="code", how="left").merge(quarter, on="code", how="left")
    base["ret_1w"] = (base["close_latest"] / base["close_1w"] - 1.0) * 100.0
    base["ret_1m"] = (base["close_latest"] / base["close_1m"] - 1.0) * 100.0
    base["ret_3m"] = (base["close_latest"] / base["close_3m"] - 1.0) * 100.0
    topix_history = get_topix_history(trading_dates, api_key=api_key)
    topix_returns = _build_topix_return_map(topix_history, trading_dates)
    for label, value in topix_returns.items():
        base[label] = value
    base["rs_vs_topix_1w"] = base["ret_1w"] - float(topix_returns.get("topix_ret_1w") or 0.0)
    base["rs_vs_topix_1m"] = base["ret_1m"] - float(topix_returns.get("topix_ret_1m") or 0.0)
    base["rs_vs_topix_3m"] = base["ret_3m"] - float(topix_returns.get("topix_ret_3m") or 0.0)
    sector_rank_1w = _sector_rank_from_returns(base, "rs_vs_topix_1w", "sector_rs_vs_topix_1w", "sector_rank_1w")
    sector_rank_1m = _sector_rank_from_returns(base, "rs_vs_topix_1m", "sector_rs_vs_topix_1m", "sector_rank_1m")
    sector_rank_3m = _sector_rank_from_returns(base, "rs_vs_topix_3m", "sector_rs_vs_topix_3m", "sector_rank_3m")
    base = base.merge(sector_rank_1w, on="sector_name", how="left")
    base = base.merge(sector_rank_1m, on="sector_name", how="left")
    base = base.merge(sector_rank_3m, on="sector_name", how="left")
    base["rel_1w"] = base["ret_1w"] - base["sector_rs_vs_topix_1w"]
    base["rel_1m"] = base["ret_1m"] - base["sector_rs_vs_topix_1m"]
    base["rel_3m"] = base["ret_3m"] - base["sector_rs_vs_topix_3m"]
    sector_counts = base.groupby("sector_name", dropna=False)["code"].nunique().reset_index(name="sector_constituent_count")
    base = base.merge(sector_counts, on="sector_name", how="left")
    earnings_frame, earnings_meta = get_earnings_buffer_frame(
        trading_dates,
        candidate_codes=master_df.get("code", pd.Series(dtype=str)).tolist(),
        api_key=None,
    )
    finance_frame = get_finance_health_frame(trading_dates, api_key=api_key)
    base = base.merge(earnings_frame, on="code", how="left")
    base = base.merge(finance_frame, on="code", how="left")
    base["TradingValue_latest"] = _coerce_numeric(base["turnover_latest"]).fillna(0.0)
    base["is_near_52w_high"] = False if fast_check else (base["close_latest"] >= base["high_20d"] * 0.97)
    base["is_new_52w_high"] = False if fast_check else (base["close_latest"] >= base["high_20d"])
    base["high_20d_flag"] = base.apply(lambda row: "new_20d_high" if bool(row.get("is_new_52w_high")) else ("near_20d_high" if bool(row.get("is_near_52w_high")) else ""), axis=1)
    base["reversal_candidates"] = (base["ret_1w"] > 0) & (base["ret_1m"] < 0)
    base["finance_health_score"] = _coerce_numeric(base.get("finance_health_score", pd.Series([pd.NA] * len(base))))
    base["finance_health_flag"] = base.get("finance_health_flag", pd.Series([pd.NA] * len(base))).fillna(pd.NA)
    base["earnings_today_announcement_flag"] = base.get("earnings_today_announcement_flag", pd.Series(False, index=base.index)).fillna(False).astype(bool)
    base["earnings_announcement_date"] = base.get("earnings_announcement_date", pd.Series("", index=base.index)).fillna("").astype(str)
    base["material_title"] = ""
    base["material_link"] = ""
    base["material_score"] = 0.0
    base["latest_date"] = pd.to_datetime(base["latest_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    base = _annotate_non_corporate_products(base)
    logger.info("build_daily_base_data end rows=%s", len(base))
    earnings_forward_buffer_available = bool(base.get("earnings_buffer_days", pd.Series(dtype="Int64")).notna().any())
    earnings_forward_buffer_reason = "" if earnings_forward_buffer_available else str(earnings_meta.get("failure_reason", "") or earnings_meta.get("status", "") or "")
    return base, {
        "latest_date": max(trading_dates),
        "trading_date_count": len(trading_dates),
        "lookback_days": lookback_trading_days,
        "fast_check": fast_check,
        "topix_source_rows": int(len(topix_history)),
        "earnings_coverage_count": int(base["earnings_today_announcement_flag"].fillna(False).sum()) if "earnings_today_announcement_flag" in base.columns else 0,
        "earnings_dataset_status": str(earnings_meta.get("status", "")),
        "earnings_rows_raw": int(earnings_meta.get("rows_raw", 0) or 0),
        "earnings_rows_future_window": int(earnings_meta.get("rows_future_window", 0) or 0),
        "earnings_min_event_date": str(earnings_meta.get("min_event_date", "") or ""),
        "earnings_max_event_date": str(earnings_meta.get("max_event_date", "") or ""),
        "earnings_announcement_status": str(earnings_meta.get("status", "")),
        "earnings_announcement_rows_target_date": int(earnings_meta.get("rows_target_date", 0) or 0),
        "earnings_announcement_target_date": str(earnings_meta.get("target_date", "") or ""),
        "earnings_announcement_request_mode": str(earnings_meta.get("request_mode", "") or ""),
        "earnings_announcement_source_path": str(earnings_meta.get("source_path", "") or ""),
        "earnings_announcement_source": str(earnings_meta.get("announcement_source", "") or ""),
        "earnings_announcement_cache_path": str(earnings_meta.get("cache_path", "") or ""),
        "earnings_announcement_cache_hit": bool(earnings_meta.get("cache_hit", False)),
        "earnings_announcement_failure_reason": str(earnings_meta.get("failure_reason", "") or ""),
        "earnings_announcement_jquants_fallback_used": bool(earnings_meta.get("jquants_fallback_used", False)),
        "edinetdb_calendar_status": str(earnings_meta.get("status", "") or ""),
        "edinetdb_calendar_source": str(earnings_meta.get("announcement_source", "") or ""),
        "edinetdb_calendar_cache_path": str(earnings_meta.get("cache_path", "") or ""),
        "edinetdb_calendar_cache_hit": bool(earnings_meta.get("cache_hit", False)),
        "edinetdb_calendar_request_mode": str(earnings_meta.get("request_mode", "") or ""),
        "edinetdb_calendar_failure_reason": str(earnings_meta.get("failure_reason", "") or ""),
        "edinetdb_calendar_jquants_fallback_used": bool(earnings_meta.get("jquants_fallback_used", False)),
        "earnings_forward_buffer_available": earnings_forward_buffer_available,
        "earnings_forward_buffer_reason": earnings_forward_buffer_reason,
        "finance_coverage_count": int(base["finance_health_score"].notna().sum()) if "finance_health_score" in base.columns else 0,
        "non_corporate_product_count": int(base["is_non_corporate_product"].fillna(False).sum()) if "is_non_corporate_product" in base.columns else 0,
        "non_corporate_product_samples": base.loc[base["is_non_corporate_product"].fillna(False), "name"].astype(str).drop_duplicates().head(5).tolist() if "is_non_corporate_product" in base.columns else [],
    }


def _kabu_headers(token: str) -> dict[str, str]:
    return {"X-API-KEY": token, "Content-Type": "application/json"}


def kabu_get_token(settings: dict[str, Any]) -> str:
    password = str(settings.get("KABU_API_PASSWORD", "")).strip()
    if not password:
        raise PipelineFailClosed("fail-closed: KABU_API_PASSWORD is missing.")
    url = f"{str(settings['KABU_API_BASE_URL']).rstrip('/')}/token"
    response = requests.post(url, json={"APIPassword": password}, timeout=10)
    if response.status_code >= 400:
        raise PipelineFailClosed(f"fail-closed: kabu token request failed status={response.status_code} body={_short_body(response.text)}")
    token = response.json().get("Token", "")
    if not token:
        raise PipelineFailClosed("fail-closed: kabu token response did not include Token.")
    return token


def _extract_kabu_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    for key in ["Ranking", "ranking", "data", "Data"]:
        rows = payload.get(key)
        if isinstance(rows, list):
            return rows
    return []


def _normalize_industry_name(value: Any) -> str:
    name = str(value or "").strip()
    if name.startswith("IS "):
        name = name[3:].strip()
    name = INDUSTRY_NAME_ALIASES.get(name, name)
    return name


def _canonicalize_industry_key_text(value: Any) -> str:
    name = str(value or "").strip()
    if name.startswith("IS "):
        name = name[3:].strip()
    return name.replace("・", "･")


def _normalize_industry_key(value: Any) -> str:
    name = _canonicalize_industry_key_text(value)
    name = INDUSTRY_KEY_ALIASES.get(name, name)
    name = _canonicalize_industry_key_text(name)
    name = INDUSTRY_KEY_ALIASES.get(name, name)
    return _canonicalize_industry_key_text(name)


def _sector_key_column(*frames: pd.DataFrame) -> str:
    for frame in frames:
        if not frame.empty and "normalized_sector_name" in frame.columns:
            return "normalized_sector_name"
    return "sector_name"


def _sorted_unique_codes(series: pd.Series) -> list[str]:
    if series.empty:
        return []
    values = series.astype(str).map(str.strip)
    values = values[values != ""].drop_duplicates()
    return sorted(values.tolist())


def _group_sector_codes(frame: pd.DataFrame, *, sector_col: str, code_col: str = "code") -> dict[str, list[str]]:
    if frame.empty or sector_col not in frame.columns or code_col not in frame.columns:
        return {}
    working = frame[[sector_col, code_col]].copy()
    working[sector_col] = working[sector_col].astype(str).map(str.strip)
    working[code_col] = working[code_col].astype(str).map(str.strip)
    working = working[(working[sector_col] != "") & (working[code_col] != "")]
    if working.empty:
        return {}
    return {
        str(sector_name or ""): _sorted_unique_codes(group[code_col])
        for sector_name, group in working.groupby(sector_col, dropna=False)
    }


def fetch_kabu_ranking(settings: dict[str, Any], token: str, source_type: str) -> pd.DataFrame:
    ranking_type = RANKING_TYPE_MAP[source_type]
    url = f"{str(settings['KABU_API_BASE_URL']).rstrip('/')}/ranking"
    params: dict[str, Any] = {"Type": str(ranking_type), "ExchangeDivision": "ALL"}
    response = requests.get(url, headers={"X-API-KEY": token}, params=params, timeout=15)
    if response.status_code >= 400:
        raise PipelineFailClosed(f"fail-closed: ranking type={ranking_type} request failed status={response.status_code} body={_short_body(response.text)}")
    rows = _extract_kabu_rows(response.json())
    logger.info("ranking fetched type=%s source_type=%s count=%s", ranking_type, source_type, len(rows))
    if not rows:
        if source_type == "industry_up":
            return pd.DataFrame(columns=["sector_name", "industry_up_value", "source_type", "ranking_type", "rank_position"])
        return pd.DataFrame(columns=["code", "name", "sector_name", "exchange", "source_type", "ranking_type", "rank_position", "rank_score"])
    frame = pd.DataFrame(rows)
    if source_type == "industry_up":
        sector_col = pick_optional_existing(frame, ["CategoryName", "IndustryName", "SectorName", "Name", "symbol_name"]) or frame.columns[0]
        value_col = pick_optional_existing(
            frame,
            [
                "ChangeRate",
                "ChangeRatio",
                "ChangePercentage",
                "ChangePercent",
                "PercentChange",
                "UpDownRate",
                "Performance",
            ],
        )
        return pd.DataFrame(
            {
                "sector_name": frame[sector_col].map(_normalize_industry_name),
                "industry_up_value": _coerce_numeric(frame[value_col]) if value_col else pd.Series([pd.NA] * len(frame), index=frame.index),
                "source_type": source_type,
                "ranking_type": ranking_type,
                "rank_position": range(1, len(frame) + 1),
            }
        )
    code_col = pick_optional_existing(frame, ["Symbol", "Code", "symbol"])
    name_col = pick_optional_existing(frame, ["SymbolName", "Name", "symbol_name"])
    sector_col = pick_optional_existing(frame, ["IndustryName", "SectorName", "industry_name"])
    exchange_col = pick_optional_existing(frame, ["Exchange", "exchange"])
    out = pd.DataFrame(
        {
            "code": frame[code_col].map(_normalize_code4) if code_col else "",
            "name": frame[name_col].astype(str) if name_col else "",
            "sector_name": frame[sector_col].astype(str) if sector_col else "",
            "exchange": frame[exchange_col] if exchange_col else pd.NA,
            "source_type": source_type,
            "ranking_type": ranking_type,
            "rank_position": range(1, len(frame) + 1),
        }
    )
    out["rank_score"] = (len(out) - out["rank_position"] + 1) * RANKING_SCORE_WEIGHTS.get(source_type, 1.0)
    return out


def _build_industry_representative_basket(
    base_df: pd.DataFrame,
    *,
    min_per_sector: int = WIDE_SCAN_BASKET_MIN_PER_SECTOR,
    target_per_sector: int = WIDE_SCAN_BASKET_TARGET_PER_SECTOR,
    max_per_sector: int = WIDE_SCAN_BASKET_MAX_PER_SECTOR,
    diagnostics: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    target_size = max(int(min_per_sector), min(int(target_per_sector), int(max_per_sector)))
    if base_df.empty:
        empty_columns = [
            "code",
            "name",
            "sector_name",
            "TradingValue_latest",
            "avg_turnover_20d",
            "avg_volume_20d",
            "basket_liquidity_score",
            "basket_rank_in_sector",
            "industry_basket_member",
        ]
        empty_diag = {
            "industry_basket_count": 0,
            "sector_basket_counts": {},
            "basket_target_per_sector": int(target_size),
            "basket_min_per_sector": int(min_per_sector),
            "basket_max_per_sector": int(max_per_sector),
        }
        return pd.DataFrame(columns=empty_columns), empty_diag
    eligible = _exclude_non_corporate_products(base_df, diagnostics, context="wide_scan_industry_basket")
    eligible = eligible.copy()
    eligible["sector_name"] = eligible.get("sector_name", pd.Series(dtype=str)).map(_normalize_industry_name)
    eligible = eligible[eligible["code"].astype(str).map(_is_code4)].copy()
    eligible = eligible[eligible["sector_name"].astype(str).str.strip() != ""].copy()
    if eligible.empty:
        return pd.DataFrame(columns=["code", "name", "sector_name"]), {
            "industry_basket_count": 0,
            "sector_basket_counts": {},
            "basket_target_per_sector": int(target_size),
            "basket_min_per_sector": int(min_per_sector),
            "basket_max_per_sector": int(max_per_sector),
        }
    eligible["basket_liquidity_score"] = (
        _score_percentile(_coerce_numeric(eligible.get("TradingValue_latest", pd.Series([pd.NA] * len(eligible), index=eligible.index))).fillna(0.0)) * 0.55
        + _score_percentile(_coerce_numeric(eligible.get("avg_turnover_20d", pd.Series([pd.NA] * len(eligible), index=eligible.index))).fillna(0.0)) * 0.30
        + _score_percentile(_coerce_numeric(eligible.get("avg_volume_20d", pd.Series([pd.NA] * len(eligible), index=eligible.index))).fillna(0.0)) * 0.15
    )
    basket_frames: list[pd.DataFrame] = []
    sector_basket_counts: dict[str, int] = {}
    for sector_name, group in eligible.groupby("sector_name", dropna=False):
        sorted_group = group.sort_values(
            ["basket_liquidity_score", "TradingValue_latest", "avg_turnover_20d", "avg_volume_20d", "code"],
            ascending=[False, False, False, False, True],
            kind="mergesort",
        ).copy()
        sector_limit = min(len(sorted_group), int(max_per_sector), int(target_size))
        chosen = sorted_group.head(sector_limit).copy()
        chosen["basket_rank_in_sector"] = range(1, len(chosen) + 1)
        chosen["industry_basket_member"] = True
        basket_frames.append(chosen)
        sector_basket_counts[str(sector_name or "")] = int(len(chosen))
    basket_df = pd.concat(basket_frames, ignore_index=True) if basket_frames else pd.DataFrame(columns=eligible.columns.tolist() + ["basket_rank_in_sector", "industry_basket_member"])
    basket_df = basket_df.drop_duplicates("code").reset_index(drop=True)
    return basket_df, {
        "industry_basket_count": int(len(basket_df)),
        "sector_basket_counts": sector_basket_counts,
        "basket_target_per_sector": int(target_size),
        "basket_min_per_sector": int(min_per_sector),
        "basket_max_per_sector": int(max_per_sector),
    }


def _classify_wide_scan_mode(
    ranking_union_count: int,
    sectors_with_ranking_confirmed_ge5: int,
    sectors_with_source_breadth_ge2: int,
    *,
    sectors_with_ranking_confirmed_ge4: int | None = None,
) -> str:
    ranking_union_min = int(TODAY_SECTOR_RANK_MODE_RULES["ranking_union_count_min"])
    confirmed_min = int(TODAY_SECTOR_RANK_MODE_RULES["sectors_with_ranking_confirmed_ge5_min"])
    breadth_min = int(TODAY_SECTOR_RANK_MODE_RULES["sectors_with_source_breadth_ge2_min"])
    ranking_union_count = int(ranking_union_count)
    sectors_with_ranking_confirmed_ge5 = int(sectors_with_ranking_confirmed_ge5)
    sectors_with_source_breadth_ge2 = int(sectors_with_source_breadth_ge2)
    sectors_with_ranking_confirmed_ge4 = int(sectors_with_ranking_confirmed_ge4 or 0)
    ranking_short = max(0, ranking_union_min - ranking_union_count)
    confirmed_short = max(0, confirmed_min - sectors_with_ranking_confirmed_ge5)
    breadth_short = max(0, breadth_min - sectors_with_source_breadth_ge2)
    if ranking_short == 0 and confirmed_short == 0 and breadth_short == 0:
        return "anchored_overlay"
    # If sector-level corroboration is already broad and confirmed, allow a modest union shortfall.
    if (
        breadth_short == 0
        and confirmed_short == 0
        and (sectors_with_source_breadth_ge2 - breadth_min) >= 8
        and ranking_short <= 9
    ):
        return "anchored_overlay"
    if (
        breadth_short == 0
        and ranking_short <= 15
        and confirmed_short <= 2
        and sectors_with_ranking_confirmed_ge4 >= confirmed_min
    ):
        return "anchored_overlay"
    return "anchor_only"


def build_market_scan_universe(base_df: pd.DataFrame, settings: dict[str, Any], token: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Build a market-wide rough scan from kabu ranking endpoints."""
    logger.info("build_market_scan_universe start")
    ranking_frames: list[pd.DataFrame] = []
    diagnostics: dict[str, Any] = {"ranking_counts": {}, "non_corporate_products": {}}
    for source_type in ["price_up", "turnover", "volume_surge", "turnover_surge"]:
        frame = fetch_kabu_ranking(settings, token, source_type)
        diagnostics["ranking_counts"][source_type] = int(len(frame))
        ranking_frames.append(frame)
    ranking_df = pd.concat(ranking_frames, ignore_index=True) if ranking_frames else pd.DataFrame()
    if ranking_df.empty:
        raise PipelineFailClosed("fail-closed: market scan rankings returned no rows.")
    ranking_df = ranking_df[ranking_df["code"].map(_is_code4)].copy()
    ranking_union = (
        ranking_df.groupby("code", as_index=False)
        .agg(
            ranking_combo_score=("rank_score", "sum"),
            ranking_sources=("source_type", lambda s: ",".join(sorted(set(s)))),
            ranking_name=("name", "first"),
            ranking_sector_name=("sector_name", "first"),
            exchange=("exchange", "first"),
        )
        .sort_values("ranking_combo_score", ascending=False)
        .reset_index(drop=True)
    )
    base_merge_columns = [
        "code",
        "name",
        "sector_name",
        "sector_rank_1w",
        "sector_rank_1m",
        "sector_rank_3m",
        "ret_1w",
        "ret_1m",
        "ret_3m",
        "TradingValue_latest",
        "avg_volume_20d",
        "avg_turnover_20d",
        "close_ma_20d",
        "exchange_name",
        "is_non_corporate_product",
        "non_corporate_product_reason",
    ] + [column for column in NON_CORPORATE_PRODUCT_ATTRIBUTE_COLUMNS if column in base_df.columns and column not in {"exchange_name"}]
    ranking_union = ranking_union.merge(
        base_df[base_merge_columns],
        on="code",
        how="left",
        suffixes=("", "_base"),
    )
    ranking_union["name"] = ranking_union["name"].fillna(ranking_union["ranking_name"]).fillna("")
    ranking_union["sector_name"] = ranking_union["sector_name"].fillna(ranking_union["ranking_sector_name"]).fillna("").map(_normalize_industry_name)
    ranking_union = _exclude_non_corporate_products(ranking_union, diagnostics, context="market_scan_ranking_union")
    ranking_union["ranking_union_member"] = True
    ranking_union["industry_basket_member"] = False
    ranking_union_count = int(len(ranking_union))
    industry_basket_df, basket_diag = _build_industry_representative_basket(base_df, diagnostics=diagnostics)
    industry_basket_df = industry_basket_df.copy()
    if not industry_basket_df.empty:
        industry_basket_df["ranking_combo_score"] = 0.0
        industry_basket_df["ranking_sources"] = ""
        industry_basket_df["ranking_name"] = industry_basket_df.get("name", pd.Series([""] * len(industry_basket_df), index=industry_basket_df.index)).fillna("")
        industry_basket_df["ranking_sector_name"] = industry_basket_df.get("sector_name", pd.Series([""] * len(industry_basket_df), index=industry_basket_df.index)).map(_normalize_industry_name)
        industry_basket_df["ranking_union_member"] = False
        industry_basket_df["industry_basket_member"] = True
    combine_columns = sorted(
        set(ranking_union.columns).union(industry_basket_df.columns if not industry_basket_df.empty else []).union({"ranking_union_member", "industry_basket_member"})
    )
    combined_scan = pd.concat(
        [
            ranking_union.reindex(columns=combine_columns),
            industry_basket_df.reindex(columns=combine_columns),
        ],
        ignore_index=True,
        sort=False,
    )
    if combined_scan.empty:
        raise PipelineFailClosed("fail-closed: wide market scan returned no rows.")
    wide_scan = (
        combined_scan.groupby("code", as_index=False)
        .agg(
            ranking_combo_score=("ranking_combo_score", "max"),
            ranking_sources=("ranking_sources", lambda s: ",".join(sorted({str(value).strip() for value in s if str(value).strip()}))),
            ranking_name=("ranking_name", "first"),
            ranking_sector_name=("ranking_sector_name", "first"),
            exchange=("exchange", "first"),
            name=("name", "first"),
            sector_name=("sector_name", "first"),
            sector_rank_1w=("sector_rank_1w", "first"),
            sector_rank_1m=("sector_rank_1m", "first"),
            sector_rank_3m=("sector_rank_3m", "first"),
            ret_1w=("ret_1w", "first"),
            ret_1m=("ret_1m", "first"),
            ret_3m=("ret_3m", "first"),
            TradingValue_latest=("TradingValue_latest", "first"),
            avg_volume_20d=("avg_volume_20d", "first"),
            avg_turnover_20d=("avg_turnover_20d", "first"),
            close_ma_20d=("close_ma_20d", "first"),
            exchange_name=("exchange_name", "first"),
            is_non_corporate_product=("is_non_corporate_product", "max"),
            non_corporate_product_reason=("non_corporate_product_reason", "first"),
            ranking_union_member=("ranking_union_member", "max"),
            industry_basket_member=("industry_basket_member", "max"),
            basket_liquidity_score=("basket_liquidity_score", "max"),
            basket_rank_in_sector=("basket_rank_in_sector", "min"),
        )
        .sort_values(["ranking_combo_score", "TradingValue_latest", "avg_turnover_20d"], ascending=[False, False, False], kind="mergesort")
        .reset_index(drop=True)
    )
    wide_scan["name"] = wide_scan["name"].fillna(wide_scan["ranking_name"]).fillna("")
    wide_scan["sector_name"] = wide_scan["sector_name"].fillna(wide_scan["ranking_sector_name"]).fillna("").map(_normalize_industry_name)
    wide_scan["wide_scan_sources"] = wide_scan.apply(
        lambda row: ",".join(
            [
                label
                for label, enabled in [
                    ("ranking_union", bool(row.get("ranking_union_member"))),
                    ("industry_basket", bool(row.get("industry_basket_member"))),
                ]
                if enabled
            ]
        ),
        axis=1,
    )
    wide_scan = _exclude_non_corporate_products(wide_scan, diagnostics, context="market_scan_wide")
    wide_scan_total_count = int(len(wide_scan))
    market_scan_quality = _summarize_market_scan_quality(
        scan_df=wide_scan,
        ranking_union_count=int(wide_scan["ranking_union_member"].fillna(False).sum()) if not wide_scan.empty else 0,
        sector_basket_counts=basket_diag.get("sector_basket_counts", {}),
    )
    wide_scan_mode = _classify_wide_scan_mode(
        market_scan_quality["ranking_union_count"],
        market_scan_quality["sectors_with_ranking_confirmed_ge5"],
        market_scan_quality["sectors_with_source_breadth_ge2"],
    )
    diagnostics.update(
        {
            "ranking_union_count": int(wide_scan["ranking_union_member"].fillna(False).sum()) if not wide_scan.empty else 0,
            "industry_basket_count": int(wide_scan["industry_basket_member"].fillna(False).sum()) if not wide_scan.empty else 0,
            "wide_scan_total_count": wide_scan_total_count,
            "wide_scan_mode": wide_scan_mode,
            "sector_basket_counts": basket_diag.get("sector_basket_counts", {}),
            "sectors_with_ranking_confirmed_ge5": int(market_scan_quality["sectors_with_ranking_confirmed_ge5"]),
            "sectors_with_source_breadth_ge2": int(market_scan_quality["sectors_with_source_breadth_ge2"]),
            "rank_mode_reason": str(market_scan_quality["reason"]),
            "market_scan_quality_summary": str(market_scan_quality["summary"]),
            "basket_target_per_sector": int(basket_diag.get("basket_target_per_sector", WIDE_SCAN_BASKET_TARGET_PER_SECTOR)),
            "basket_min_per_sector": int(basket_diag.get("basket_min_per_sector", WIDE_SCAN_BASKET_MIN_PER_SECTOR)),
            "basket_max_per_sector": int(basket_diag.get("basket_max_per_sector", WIDE_SCAN_BASKET_MAX_PER_SECTOR)),
            "wide_scan_target_range": {"min": int(WIDE_SCAN_TARGET_RANGE[0]), "max": int(WIDE_SCAN_TARGET_RANGE[1])},
            "ranking_union_count_raw": ranking_union_count,
        }
    )
    industry_df = fetch_kabu_ranking(settings, token, "industry_up")
    diagnostics["ranking_counts"]["industry_up"] = int(len(industry_df))
    logger.info("build_market_scan_universe end candidates=%s industries=%s mode=%s", len(wide_scan), len(industry_df), wide_scan_mode)
    return wide_scan, industry_df, diagnostics


def _resolve_primary_exchange(code: str, exchange: Any, exchange_name: Any, source_hint: str | None = None) -> int:
    raw_exchange = str(exchange).strip()
    if raw_exchange:
        digits = re.sub(r"\D", "", raw_exchange)
        if digits:
            value = int(digits)
            if value == 9:
                logger.warning("exchange=SOR ignored for board request code=%s source=%s fallback=1", code, source_hint or "")
                return 1
            if value in BOARD_REQUEST_EXCHANGES:
                return value
            logger.warning("invalid exchange value for board request code=%s exchange=%s source=%s fallback=1", code, raw_exchange, source_hint or "")
            return 1
    normalized_name = str(exchange_name or "").strip().lower()
    for key, value in {"東証": 1, "tse": 1, "名証": 3, "nse": 3, "福証": 5, "fse": 5, "札証": 6, "sse": 6}.items():
        if key.lower() in normalized_name:
            return value
    return 1


def _build_board_symbol(code: str, exchange_code: int) -> str:
    code4 = _normalize_code4(code)
    if not _is_code4(code4):
        raise ValueError(f"board target must be 4-digit code. code={code}")
    return f"{code4}@{exchange_code if exchange_code in BOARD_REQUEST_EXCHANGES else 1}"


def _fetch_board(settings: dict[str, Any], token: str, request_symbol: str) -> dict[str, Any]:
    payload, error_info = _try_fetch_board(settings, token, request_symbol)
    if payload is not None:
        return payload
    assert error_info is not None
    raise PipelineFailClosed(
        f"fail-closed: board request failed symbol={request_symbol} status={error_info.get('status_code')} body={error_info.get('body_short')}"
    )


def _try_fetch_board(settings: dict[str, Any], token: str, request_symbol: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    url = f"{str(settings['KABU_API_BASE_URL']).rstrip('/')}/board/{quote_plus(request_symbol)}"
    retry_sleep = 0.15
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        response = requests.get(url, headers=_kabu_headers(token), timeout=10)
        if response.status_code == 429 and attempt < max_attempts:
            logger.warning("board request rate-limited symbol=%s attempt=%s/%s; sleeping %.2fs", request_symbol, attempt, max_attempts, retry_sleep)
            time.sleep(retry_sleep)
            continue
        if response.status_code >= 400:
            return None, _build_board_error_info(response, request_symbol, attempt=attempt)
        return _normalize_board_payload(response.json()), None
    return None, {"request_symbol": request_symbol, "status_code": 429, "body_short": "board request exhausted retries", "recoverable": False, "attempt": max_attempts}


def _build_board_error_info(response: requests.Response, request_symbol: str, *, attempt: int) -> dict[str, Any]:
    payload: dict[str, Any]
    try:
        payload = response.json()
    except Exception:
        payload = {}
    message = str(payload.get("Message", "") or "").strip()
    return {
        "request_symbol": request_symbol,
        "status_code": int(response.status_code),
        "error_code": payload.get("Code"),
        "message": message,
        "body_short": _short_body(response.text),
        "recoverable": response.status_code in {400, 404} and "銘柄が見つからない" in message,
        "attempt": attempt,
    }


def _attempt_unregister_all(settings: dict[str, Any], token: str, *, context_label: str) -> dict[str, Any]:
    try:
        _unregister_all(settings, token)
        return {"called": True, "succeeded": True, "context": context_label, "error_code": None, "message": ""}
    except Exception as exc:
        logger.warning("unregister/all failed context=%s reason=%s", context_label, exc)
        return {"called": True, "succeeded": False, "context": context_label, "error_code": None, "message": str(exc)}


def _normalize_board_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    key_aliases = {
        "PreviousClose": "PrevClose",
        "OpeningPrice": "Open",
        "HighPrice": "High",
        "LowPrice": "Low",
        "TradingVolume": "Volume",
        "TradingValue": "Turnover",
    }
    for src, dst in key_aliases.items():
        if normalized.get(dst) in {None, ""} and normalized.get(src) not in {None, ""}:
            normalized[dst] = normalized[src]
    return normalized


def _board_has_major_fields(payload: dict[str, Any]) -> bool:
    return all(payload.get(key) not in {None, ""} for key in BOARD_MAJOR_FIELDS)


def _fill_prev_close_from_base(payload: dict[str, Any], base_df: pd.DataFrame, code: str, *, request_symbol: str = "", resolved_exchange: Any = "", mode: str = "") -> bool:
    if payload.get("PrevClose") not in {None, ""}:
        return True
    base_row = base_df.loc[base_df["code"].astype(str) == str(code)]
    base_hit = not base_row.empty
    latest_date = base_row["latest_date"].iloc[0] if base_hit and "latest_date" in base_row.columns else ""
    if base_row.empty:
        logger.warning(
            "board prev_close missing and base fallback unavailable mode=%s code=%s exchange=%s request_symbol=%s base_df_hit=%s latest_date=%s",
            mode,
            code,
            resolved_exchange,
            request_symbol,
            base_hit,
            latest_date,
        )
        return False
    close_value = base_row["close_latest"].iloc[0] if "close_latest" in base_row.columns else None
    if pd.isna(close_value) or close_value in {None, ""}:
        logger.warning(
            "board prev_close missing and base fallback unavailable mode=%s code=%s exchange=%s request_symbol=%s base_df_hit=%s latest_date=%s",
            mode,
            code,
            resolved_exchange,
            request_symbol,
            base_hit,
            latest_date,
        )
        return False
    payload["PrevClose"] = close_value
    logger.warning(
        "board prev_close missing; filled from base_df mode=%s code=%s exchange=%s request_symbol=%s base_df_hit=%s latest_date=%s",
        mode,
        code,
        resolved_exchange,
        request_symbol,
        base_hit,
        latest_date,
    )
    return True


def _board_to_row(code: str, payload: dict[str, Any], request_symbol: str, resolved_exchange: int, *, mode: str = "") -> dict[str, Any]:
    if payload.get("BidPrice") in {None, ""} or payload.get("AskPrice") in {None, ""}:
        logger.warning("board bid/ask missing mode=%s code=%s request_symbol=%s exchange=%s", mode, code, request_symbol, resolved_exchange)
    exchange_value = payload.get("Exchange", resolved_exchange)
    try:
        exchange_value = int(exchange_value)
    except Exception:
        exchange_value = resolved_exchange
    if exchange_value not in BOARD_ACCEPTED_RESPONSE_EXCHANGES:
        exchange_value = resolved_exchange
    return {
        "code": code,
        "request_symbol": request_symbol,
        "resolved_exchange": resolved_exchange,
        "response_exchange": exchange_value,
        "CurrentPrice": payload.get("CurrentPrice"),
        "CurrentPriceTime": payload.get("CurrentPriceTime"),
        "PrevClose": payload.get("PrevClose"),
        "Open": payload.get("Open"),
        "High": payload.get("High"),
        "Low": payload.get("Low"),
        "Volume": payload.get("Volume"),
        "Turnover": payload.get("Turnover"),
        "BidPrice": payload.get("BidPrice"),
        "AskPrice": payload.get("AskPrice"),
    }


def _board_exchange_candidates(code: str, exchange: Any, exchange_name: Any) -> list[int]:
    first_exchange = _resolve_primary_exchange(code, exchange, exchange_name, source_hint="deep_watch")
    return [first_exchange] + [candidate for candidate in sorted(BOARD_REQUEST_EXCHANGES) if candidate != first_exchange]


def _fetch_board_with_exchange_fallback(settings: dict[str, Any], token: str, base_df: pd.DataFrame, row: pd.Series, *, mode: str = "") -> tuple[dict[str, Any] | None, dict[str, Any]]:
    code = str(row["code"])
    tried_exchanges = _board_exchange_candidates(code, row.get("exchange"), row.get("exchange_name"))
    first_exchange = tried_exchanges[0]
    recoverable_errors: list[dict[str, Any]] = []
    registry_reset_used = False
    for exchange_code in tried_exchanges:
        request_symbol = _build_board_symbol(code, exchange_code)
        logger.debug(
            "board attempt code=%s first_exchange=%s tried_exchanges=%s chosen_exchange=%s result=%s",
            code,
            first_exchange,
            tried_exchanges,
            exchange_code,
            "attempt",
        )
        time.sleep(0.13)
        payload, error_info = _try_fetch_board(settings, token, request_symbol)
        if payload is not None:
            has_prev_close = _fill_prev_close_from_base(payload, base_df, code, request_symbol=request_symbol, resolved_exchange=exchange_code, mode=mode)
            has_major_fields = _board_has_major_fields(payload)
            result = "success" if has_major_fields and has_prev_close else ("retry_required" if not has_major_fields else "missing_prev_close")
            logger.debug(
                "board attempt code=%s first_exchange=%s tried_exchanges=%s chosen_exchange=%s result=%s",
                code,
                first_exchange,
                tried_exchanges,
                exchange_code,
                result,
            )
            return payload, {
                "code": code,
                "first_exchange": first_exchange,
                "tried_exchanges": tried_exchanges,
                "chosen_exchange": exchange_code,
                "request_symbol": request_symbol,
                "result": result,
                "recoverable_errors": recoverable_errors,
                "fallback_exchange_used": exchange_code != first_exchange,
                "has_prev_close": has_prev_close,
                "has_major_fields": has_major_fields,
                "registry_reset_used": registry_reset_used,
                "hard_fail_reason": "",
            }
        assert error_info is not None
        if int(error_info.get("error_code") or 0) == 4002006 and not registry_reset_used:
            registry_reset_used = True
            reset_result = _attempt_unregister_all(settings, token, context_label=f"board_fetch:{code}")
            error_info["registry_reset"] = reset_result
            time.sleep(float(settings.get("KABU_PUSH_TIMEOUT_SECONDS", 4.0)))
            retry_payload, retry_error_info = _try_fetch_board(settings, token, request_symbol)
            if retry_payload is not None:
                has_prev_close = _fill_prev_close_from_base(retry_payload, base_df, code, request_symbol=request_symbol, resolved_exchange=exchange_code, mode=mode)
                has_major_fields = _board_has_major_fields(retry_payload)
                result = "success" if has_major_fields and has_prev_close else ("retry_required" if not has_major_fields else "missing_prev_close")
                logger.debug(
                    "board attempt code=%s first_exchange=%s tried_exchanges=%s chosen_exchange=%s result=%s",
                    code,
                    first_exchange,
                    tried_exchanges,
                    exchange_code,
                    f"{result}_after_registry_reset",
                )
                return retry_payload, {
                    "code": code,
                    "first_exchange": first_exchange,
                    "tried_exchanges": tried_exchanges,
                    "chosen_exchange": exchange_code,
                    "request_symbol": request_symbol,
                    "result": result,
                    "recoverable_errors": recoverable_errors,
                    "fallback_exchange_used": exchange_code != first_exchange,
                    "has_prev_close": has_prev_close,
                    "has_major_fields": has_major_fields,
                    "registry_reset_used": True,
                    "hard_fail_reason": "",
                }
            if retry_error_info is not None and int(retry_error_info.get("error_code") or 0) == 4002006:
                raise PipelineFailClosed(
                    f"fail-closed: board request failed after registry reset symbol={request_symbol} status={retry_error_info.get('status_code')} body={retry_error_info.get('body_short')}"
                )
            if retry_error_info is not None:
                error_info = retry_error_info
        if bool(error_info.get("recoverable")):
            recoverable_errors.append(error_info)
            continue
        logger.debug(
            "board attempt code=%s first_exchange=%s tried_exchanges=%s chosen_exchange=%s result=%s",
            code,
            first_exchange,
            tried_exchanges,
            exchange_code,
            "hard_fail",
        )
        raise PipelineFailClosed(
            f"fail-closed: board request failed symbol={request_symbol} status={error_info.get('status_code')} body={error_info.get('body_short')}"
        )
    logger.debug(
        "board attempt code=%s first_exchange=%s tried_exchanges=%s chosen_exchange=%s result=%s",
        code,
        first_exchange,
        tried_exchanges,
        "",
        "not_found",
    )
    return None, {
        "code": code,
        "first_exchange": first_exchange,
        "tried_exchanges": tried_exchanges,
        "chosen_exchange": None,
        "request_symbol": "",
        "result": "not_found",
        "recoverable_errors": recoverable_errors,
        "fallback_exchange_used": False,
        "has_prev_close": False,
        "has_major_fields": False,
        "registry_reset_used": registry_reset_used,
        "hard_fail_reason": "",
    }


def _register_symbols(settings: dict[str, Any], token: str, register_df: pd.DataFrame) -> None:
    if register_df.empty:
        return
    url = f"{str(settings['KABU_API_BASE_URL']).rstrip('/')}/register"
    payload = {"Symbols": [{"Symbol": row["code"], "Exchange": int(row["resolved_exchange"])} for _, row in register_df.iterrows()]}
    response = requests.put(url, headers=_kabu_headers(token), json=payload, timeout=10)
    if response.status_code >= 400:
        raise PipelineFailClosed(f"fail-closed: register request failed status={response.status_code} body={_short_body(response.text)}")


def _unregister_all(settings: dict[str, Any], token: str) -> None:
    url = f"{str(settings['KABU_API_BASE_URL']).rstrip('/')}/unregister/all"
    response = requests.put(url, headers=_kabu_headers(token), timeout=10)
    if response.status_code >= 400:
        raise PipelineFailClosed(f"fail-closed: unregister all failed status={response.status_code} body={_short_body(response.text)}")


def _select_deep_watch_protected_sectors(
    market_scan_df: pd.DataFrame,
    fallback_frame: pd.DataFrame,
    *,
    pre_today_sector_leaderboard: pd.DataFrame | None = None,
) -> tuple[list[str], dict[str, int]]:
    if pre_today_sector_leaderboard is not None and not pre_today_sector_leaderboard.empty:
        sorted_proxy = _sort_today_sector_leaderboard_for_display(pre_today_sector_leaderboard)
        proxy = sorted_proxy.copy()
        proxy["sector_name"] = proxy.get("sector_name", pd.Series(dtype=str)).map(_normalize_industry_name)
        proxy = proxy[proxy["sector_name"].astype(str).str.strip() != ""].copy()
        if not proxy.empty:
            top_proxy = proxy.head(DEEP_WATCH_MUST_HAVE_TOP_SECTORS).copy()
            sector_names = top_proxy["sector_name"].astype(str).tolist()
            sector_rank_map = {
                _normalize_industry_key(row.get("sector_name", "")): int(_coerce_numeric(pd.Series([row.get("today_rank", pd.NA)])).fillna(pd.Series([9999])).iloc[0] or 9999)
                for _, row in top_proxy.iterrows()
            }
            return sector_names, sector_rank_map
    fallback = fallback_frame.copy()
    fallback["sector_name"] = fallback.get("sector_name", pd.Series(dtype=str)).map(_normalize_industry_name)
    fallback = fallback[fallback["sector_name"].astype(str).str.strip() != ""].copy()
    if fallback.empty:
        return [], {}
    top_sector_rows = (
        fallback[["sector_name", "sector_rank_1w"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["sector_rank_1w", "sector_name"])
        .head(DEEP_WATCH_MUST_HAVE_TOP_SECTORS)
        .copy()
    )
    sector_names = top_sector_rows["sector_name"].astype(str).tolist()
    sector_rank_map = {
        _normalize_industry_key(row.get("sector_name", "")): int(_coerce_numeric(pd.Series([row.get("sector_rank_1w", pd.NA)])).fillna(pd.Series([9999])).iloc[0] or 9999)
        for _, row in top_sector_rows.iterrows()
    }
    return sector_names, sector_rank_map


def _build_pre_deep_watch_sector_proxy(
    market_scan_df: pd.DataFrame,
    industry_df: pd.DataFrame,
    base_df: pd.DataFrame,
    mode: str,
) -> pd.DataFrame:
    if market_scan_df is None or market_scan_df.empty or industry_df is None or industry_df.empty or base_df is None or base_df.empty:
        return _empty_sector_leaderboard()
    proxy = _build_intraday_sector_leaderboard(mode, market_scan_df, industry_df, market_scan_df, base_df)
    return _sort_today_sector_leaderboard_for_display(proxy)


def _build_deep_watch_must_have_pool(
    market_scan_df: pd.DataFrame,
    base_df: pd.DataFrame,
    *,
    pre_today_sector_leaderboard: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if base_df.empty:
        return pd.DataFrame()
    market_scan_cols = [column for column in ["code", "ranking_combo_score", "ranking_union_member", "industry_basket_member"] if column in market_scan_df.columns]
    market_scan_meta = market_scan_df[market_scan_cols].copy() if market_scan_cols else pd.DataFrame(columns=["code"])
    if not market_scan_meta.empty:
        market_scan_meta["code"] = market_scan_meta["code"].astype(str)
        market_scan_meta = market_scan_meta.drop_duplicates("code")
    working = base_df.copy()
    working["code"] = working["code"].astype(str)
    working["sector_name"] = working.get("sector_name", pd.Series(dtype=str)).map(_normalize_industry_name)
    working = working[working["code"].map(_is_code4)].copy()
    working = working[working["sector_name"].astype(str).str.strip() != ""].copy()
    if working.empty:
        return working
    if not market_scan_meta.empty:
        working = working.merge(market_scan_meta, on="code", how="left")
    working["ranking_combo_score"] = _coerce_numeric(working.get("ranking_combo_score", pd.Series([0.0] * len(working), index=working.index))).fillna(0.0)
    working["ranking_union_member"] = working.get("ranking_union_member", pd.Series(False, index=working.index)).fillna(False).astype(bool)
    working["industry_basket_member"] = working.get("industry_basket_member", pd.Series(False, index=working.index)).fillna(False).astype(bool)
    sector_turnover_total = working.groupby("sector_name")["TradingValue_latest"].transform("sum")
    working["sector_contribution_full"] = _safe_ratio(working["TradingValue_latest"], sector_turnover_total).fillna(0.0)
    working["contribution_rank_in_sector"] = working.groupby("sector_name")["sector_contribution_full"].rank(method="dense", ascending=False)
    working["turnover_rank_in_sector"] = working.groupby("sector_name")["avg_turnover_20d"].rank(method="dense", ascending=False)
    turnover_floor = float(_coerce_numeric(working["avg_turnover_20d"]).median(skipna=True) or 0.0)
    volume_floor = float(_coerce_numeric(working["avg_volume_20d"]).median(skipna=True) or 0.0)
    working["liquidity_ok"] = (
        _coerce_numeric(working["avg_turnover_20d"]).fillna(0.0) >= turnover_floor
    ) & (
        _coerce_numeric(working["avg_volume_20d"]).fillna(0.0) >= volume_floor
    )
    top_sector_names, protected_sector_rank_map = _select_deep_watch_protected_sectors(
        market_scan_df,
        working,
        pre_today_sector_leaderboard=pre_today_sector_leaderboard,
    )
    working["normalized_sector_name"] = working["sector_name"].map(_normalize_industry_key)
    protected_sector_keys = {str(key or "") for key in protected_sector_rank_map.keys() if str(key or "").strip()}
    working = working[working["normalized_sector_name"].astype(str).isin(protected_sector_keys)].copy()
    if working.empty:
        return working
    working["protected_sector_rank"] = working["normalized_sector_name"].map(lambda value: float(protected_sector_rank_map.get(str(value or ""), 9999)))
    working["must_have_today_strength"] = 0.0
    working["must_have_today_strength"] += _score_percentile(working["ranking_combo_score"]) * DEEP_WATCH_MUST_HAVE_TODAY_WEIGHTS["ranking_combo_score"]
    working["must_have_today_strength"] += _score_percentile(working["ret_1w"]) * DEEP_WATCH_MUST_HAVE_TODAY_WEIGHTS["ret_1w"]
    working["must_have_today_strength"] += _score_percentile(working["rel_1w"]) * DEEP_WATCH_MUST_HAVE_TODAY_WEIGHTS["rel_1w"]
    working.loc[working["ranking_union_member"], "must_have_today_strength"] += DEEP_WATCH_MUST_HAVE_TODAY_WEIGHTS["ranking_union_member_bonus"]
    working.loc[working["industry_basket_member"], "must_have_today_strength"] += DEEP_WATCH_MUST_HAVE_TODAY_WEIGHTS["industry_basket_member_bonus"]
    working["must_have_representative_support"] = 0.0
    working["must_have_representative_support"] += _score_percentile(working["sector_contribution_full"]) * DEEP_WATCH_MUST_HAVE_REPRESENTATIVE_WEIGHTS["sector_contribution_full"]
    working["must_have_representative_support"] += _score_rank_ascending(working["contribution_rank_in_sector"]) * DEEP_WATCH_MUST_HAVE_REPRESENTATIVE_WEIGHTS["contribution_rank_in_sector"]
    working["must_have_representative_support"] += _score_rank_ascending(working["turnover_rank_in_sector"]) * DEEP_WATCH_MUST_HAVE_REPRESENTATIVE_WEIGHTS["turnover_rank_in_sector"]
    working["must_have_representative_support"] += _score_percentile(working["avg_turnover_20d"]) * DEEP_WATCH_MUST_HAVE_REPRESENTATIVE_WEIGHTS["avg_turnover_20d"]
    working["must_have_representative_support"] += _score_percentile(working["TradingValue_latest"]) * DEEP_WATCH_MUST_HAVE_REPRESENTATIVE_WEIGHTS["TradingValue_latest"]
    working["must_have_representative_support"] += _score_percentile(working["avg_volume_20d"]) * DEEP_WATCH_MUST_HAVE_REPRESENTATIVE_WEIGHTS["avg_volume_20d"]
    working.loc[working["liquidity_ok"], "must_have_representative_support"] += DEEP_WATCH_MUST_HAVE_REPRESENTATIVE_WEIGHTS["liquidity_ok_bonus"]
    working["must_have_priority"] = working["must_have_today_strength"] * 100.0 + working["must_have_representative_support"]
    working = working.sort_values(
        ["protected_sector_rank", "must_have_today_strength", "must_have_representative_support", "ranking_combo_score", "sector_contribution_full", "TradingValue_latest", "avg_turnover_20d"],
        ascending=[True, False, False, False, False, False, False],
        kind="mergesort",
    ).copy()
    working["must_have_rank_in_sector"] = working.groupby("sector_name").cumcount() + 1
    return working


def _build_deep_watch_representative_supplemental_pool(must_have_pool: pd.DataFrame) -> pd.DataFrame:
    if must_have_pool is None or must_have_pool.empty:
        return pd.DataFrame()
    required = {"sector_name", "must_have_rank_in_sector", "must_have_representative_support"}
    if not required.issubset(set(must_have_pool.columns)):
        return pd.DataFrame()
    working = must_have_pool.copy()
    rank = _coerce_numeric(working["must_have_rank_in_sector"]).fillna(9999.0)
    working = working[
        rank.gt(float(DEEP_WATCH_MUST_HAVE_PER_SECTOR))
        & rank.le(float(DEEP_WATCH_MUST_HAVE_PER_SECTOR + DEEP_WATCH_REPRESENTATIVE_SUPPLEMENTAL_PER_SECTOR))
    ].copy()
    if working.empty:
        return working
    working = working.sort_values(
        [
            "protected_sector_rank",
            "must_have_representative_support",
            "sector_contribution_full",
            "TradingValue_latest",
            "avg_turnover_20d",
            "code",
        ],
        ascending=[True, False, False, False, False, True],
        kind="mergesort",
    )
    return working.head(DEEP_WATCH_REPRESENTATIVE_SUPPLEMENTAL_MAX).copy()


def select_deep_watch_universe(
    market_scan_df: pd.DataFrame,
    industry_df: pd.DataFrame,
    base_df: pd.DataFrame,
    settings: dict[str, Any],
    mode: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Select the 50-name deep-watch universe for board enrichment."""
    logger.info("select_deep_watch_universe start mode=%s", mode)
    register_limit = int(settings.get("KABU_REGISTER_LIMIT", 50))
    diagnostics: dict[str, Any] = {"non_corporate_products": {}}
    market_scan_df = _exclude_non_corporate_products(market_scan_df, diagnostics, context="deep_watch_market_scan")
    deep_candidates = _exclude_non_corporate_products(base_df, diagnostics, context="deep_watch_base")
    if "sector_name" in deep_candidates.columns:
        deep_candidates["sector_name"] = deep_candidates["sector_name"].map(_normalize_industry_name)
        deep_candidates["normalized_sector_name"] = deep_candidates["sector_name"].map(_normalize_industry_key)
    pre_today_sector_leaderboard = _build_pre_deep_watch_sector_proxy(market_scan_df, industry_df, deep_candidates, mode)
    protected_sector_names, protected_sector_rank_map = _select_deep_watch_protected_sectors(
        market_scan_df,
        deep_candidates,
        pre_today_sector_leaderboard=pre_today_sector_leaderboard,
    )
    protected_sector_keys = {str(key or "") for key in protected_sector_rank_map.keys() if str(key or "").strip()}
    deep_candidates["candidate_seed_score"] = 0.0
    deep_candidates.loc[deep_candidates["rel_1w"].rank(ascending=False, method="min") <= 80, "candidate_seed_score"] += 1.0
    deep_candidates.loc[deep_candidates["ret_1w"].rank(ascending=False, method="min") <= 80, "candidate_seed_score"] += 1.0
    deep_candidates.loc[deep_candidates["TradingValue_latest"].rank(ascending=False, method="min") <= 100, "candidate_seed_score"] += 1.2
    deep_candidates.loc[deep_candidates["reversal_candidates"].fillna(False), "candidate_seed_score"] += 0.8
    deep_candidates.loc[deep_candidates["is_near_52w_high"].fillna(False), "candidate_seed_score"] += 0.8
    if protected_sector_names:
        deep_candidates["protected_sector_rank"] = deep_candidates["normalized_sector_name"].map(lambda value: float(protected_sector_rank_map.get(str(value or ""), 9999)))
        deep_candidates.loc[deep_candidates["normalized_sector_name"].astype(str).isin(protected_sector_keys), "candidate_seed_score"] += 1.05
        deep_candidates["candidate_seed_score"] += _score_rank_ascending(deep_candidates["protected_sector_rank"]).fillna(0.0) * 0.45
    top_ranked_candidates = market_scan_df.head(80).merge(base_df, on="code", how="left", suffixes=("", "_base"))
    for column in ["name", "sector_name", "exchange_name"]:
        base_column = f"{column}_base"
        if base_column in top_ranked_candidates.columns:
            current = top_ranked_candidates.get(column, pd.Series(pd.NA, index=top_ranked_candidates.index))
            fallback_mask = current.isna() | current.astype(str).str.strip().eq("")
            top_ranked_candidates[column] = current.where(~fallback_mask, top_ranked_candidates[base_column])
    if "sector_name" in top_ranked_candidates.columns:
        top_ranked_candidates["sector_name"] = top_ranked_candidates["sector_name"].map(_normalize_industry_name)
        top_ranked_candidates["normalized_sector_name"] = top_ranked_candidates["sector_name"].map(_normalize_industry_key)
    combined = pd.concat([top_ranked_candidates, deep_candidates], ignore_index=True, sort=False)
    if "sector_name" in combined.columns:
        combined["sector_name"] = combined["sector_name"].map(_normalize_industry_name)
        combined["normalized_sector_name"] = combined["sector_name"].map(_normalize_industry_key)
    combined["combined_priority"] = combined.get("ranking_combo_score", 0).fillna(0) + combined["candidate_seed_score"].fillna(0)
    if protected_sector_names:
        combined["protected_sector_rank"] = combined.get(
            "protected_sector_rank",
            combined["normalized_sector_name"].map(lambda value: float(protected_sector_rank_map.get(str(value or ""), 9999))),
        )
        protected_sector_bonus = _score_rank_ascending(combined["protected_sector_rank"]).fillna(0.0) * 0.60
        combined.loc[combined["normalized_sector_name"].astype(str).isin(protected_sector_keys), "combined_priority"] += 0.45
        combined["combined_priority"] += protected_sector_bonus
    combined["code"] = combined["code"].astype(str)
    pre_count = len(combined)
    invalid_code_count = int((~combined["code"].map(_is_code4)).sum())
    duplicate_count = int(combined["code"].duplicated().sum())
    combined = combined[combined["code"].map(_is_code4)].copy()
    combined = combined.sort_values(["combined_priority", "TradingValue_latest"], ascending=[False, False]).drop_duplicates("code")
    combined["was_in_selected50"] = False
    combined["was_in_must_have"] = False
    combined["deep_watch_selected_reason"] = ""
    combined["selected_from_primary_or_supplemental"] = "primary"
    combined["deep_watch_combined_priority"] = _coerce_numeric(combined["combined_priority"]).fillna(0.0)
    must_have_pool = _build_deep_watch_must_have_pool(
        market_scan_df,
        deep_candidates,
        pre_today_sector_leaderboard=pre_today_sector_leaderboard,
    )
    must_have_selected = pd.DataFrame(columns=combined.columns.tolist() + ["must_have_priority", "must_have_rank_in_sector"])
    if not must_have_pool.empty:
        must_have_selected = (
            must_have_pool[must_have_pool["must_have_rank_in_sector"] <= DEEP_WATCH_MUST_HAVE_PER_SECTOR]
            .head(DEEP_WATCH_MUST_HAVE_MAX)
            .copy()
        )
        if not must_have_selected.empty:
            must_have_codes = must_have_selected["code"].astype(str).tolist()
            combined.loc[combined["code"].astype(str).isin(must_have_codes), "was_in_must_have"] = True
    representative_supplemental_selected = _build_deep_watch_representative_supplemental_pool(must_have_pool)
    selected_frames: list[pd.DataFrame] = []
    selected_codes: set[str] = set()
    if not must_have_selected.empty:
        must_have_selected = must_have_selected.merge(
            combined.drop(columns=[column for column in ["must_have_priority", "must_have_rank_in_sector"] if column in combined.columns]),
            on="code",
            how="left",
            suffixes=("", "_combined"),
        )
        for column in ["combined_priority", "deep_watch_combined_priority", "candidate_seed_score", "ranking_combo_score", "sector_name", "name"]:
            combined_col = f"{column}_combined"
            if column not in must_have_selected.columns and combined_col in must_have_selected.columns:
                must_have_selected[column] = must_have_selected[combined_col]
            elif combined_col in must_have_selected.columns:
                must_have_selected[column] = must_have_selected[column].where(must_have_selected[column].notna(), must_have_selected[combined_col])
        must_have_selected["was_in_selected50"] = True
        must_have_selected["was_in_must_have"] = True
        must_have_selected["deep_watch_selected_reason"] = "must_have_lane"
        must_have_selected["deep_watch_combined_priority"] = _coerce_numeric(must_have_selected.get("combined_priority", must_have_selected.get("deep_watch_combined_priority", 0.0))).fillna(0.0)
        must_have_selected = must_have_selected[~must_have_selected["code"].astype(str).isin(selected_codes)].copy()
        selected_frames.append(must_have_selected)
        selected_codes.update(must_have_selected["code"].astype(str).tolist())
    if not representative_supplemental_selected.empty:
        representative_supplemental_selected = representative_supplemental_selected.merge(
            combined.drop(columns=[column for column in ["must_have_priority", "must_have_rank_in_sector"] if column in combined.columns]),
            on="code",
            how="left",
            suffixes=("", "_combined"),
        )
        for column in ["combined_priority", "deep_watch_combined_priority", "candidate_seed_score", "ranking_combo_score", "sector_name", "name"]:
            combined_col = f"{column}_combined"
            if column not in representative_supplemental_selected.columns and combined_col in representative_supplemental_selected.columns:
                representative_supplemental_selected[column] = representative_supplemental_selected[combined_col]
            elif combined_col in representative_supplemental_selected.columns:
                representative_supplemental_selected[column] = representative_supplemental_selected[column].where(
                    representative_supplemental_selected[column].notna(),
                    representative_supplemental_selected[combined_col],
                )
        representative_supplemental_selected = representative_supplemental_selected[
            ~representative_supplemental_selected["code"].astype(str).isin(selected_codes)
        ].copy()
        if not representative_supplemental_selected.empty:
            representative_supplemental_selected["was_in_selected50"] = True
            representative_supplemental_selected["was_in_must_have"] = False
            representative_supplemental_selected["deep_watch_selected_reason"] = "representative_supplemental_lane"
            representative_supplemental_selected["selected_from_primary_or_supplemental"] = "supplemental"
            representative_supplemental_selected["deep_watch_combined_priority"] = _coerce_numeric(
                representative_supplemental_selected.get("combined_priority", representative_supplemental_selected.get("deep_watch_combined_priority", 0.0))
            ).fillna(0.0)
            selected_frames.append(representative_supplemental_selected)
            selected_codes.update(representative_supplemental_selected["code"].astype(str).tolist())
    remaining_slots = max(register_limit - len(selected_codes), 0)
    if remaining_slots > 0:
        remaining = combined[~combined["code"].astype(str).isin(selected_codes)].head(remaining_slots).copy()
        if not remaining.empty:
            remaining["was_in_selected50"] = True
            remaining["deep_watch_selected_reason"] = remaining["was_in_must_have"].map(lambda value: "must_have_plus_priority" if bool(value) else "priority_fill")
            remaining["selected_from_primary_or_supplemental"] = remaining.get("selected_from_primary_or_supplemental", pd.Series("primary", index=remaining.index)).fillna("primary").replace("", "primary")
            selected_frames.append(remaining)
            selected_codes.update(remaining["code"].astype(str).tolist())
    selected = pd.concat(selected_frames, ignore_index=True, sort=False) if selected_frames else combined.head(register_limit).copy()
    if not selected.empty:
        selected["was_in_selected50"] = True
        selected["was_in_must_have"] = selected.get("was_in_must_have", pd.Series(False, index=selected.index)).fillna(False).astype(bool)
        selected["deep_watch_selected_reason"] = selected.get("deep_watch_selected_reason", pd.Series(["priority_fill"] * len(selected), index=selected.index)).replace("", "priority_fill")
        selected["selected_from_primary_or_supplemental"] = selected.get(
            "selected_from_primary_or_supplemental",
            pd.Series(["primary"] * len(selected), index=selected.index),
        ).fillna("primary").replace("", "primary")
        selected["deep_watch_combined_priority"] = _coerce_numeric(selected.get("deep_watch_combined_priority", selected.get("combined_priority", 0.0))).fillna(0.0)
    logger.debug("deep-watch candidate_count=%s selected=%s excluded_duplicate=%s excluded_invalid=%s excluded_market_unknown=%s", pre_count, len(selected), duplicate_count, invalid_code_count, 0)
    logger.info("select_deep_watch_universe end selected=%s", len(selected))
    return selected, {
        "candidate_count": pre_count,
        "selected_count": int(len(selected)),
        "must_have_selected_count": int(selected.get("was_in_must_have", pd.Series(dtype=bool)).fillna(False).sum()) if not selected.empty else 0,
        "must_have_selected_codes": selected.loc[selected.get("was_in_must_have", pd.Series(False, index=selected.index)).fillna(False), "code"].astype(str).tolist()[:20] if not selected.empty else [],
        "representative_supplemental_selected_count": int(selected.get("selected_from_primary_or_supplemental", pd.Series(dtype=str)).fillna("").astype(str).eq("supplemental").sum()) if not selected.empty else 0,
        "representative_supplemental_selected_codes": selected.loc[selected.get("selected_from_primary_or_supplemental", pd.Series("", index=selected.index)).fillna("").astype(str).eq("supplemental"), "code"].astype(str).tolist()[:20] if not selected.empty else [],
        "protected_top_sector_names": protected_sector_names,
        "pre_today_sector_proxy_top10": _summarize_sector_rank_table(pre_today_sector_leaderboard, limit=10),
        "excluded_invalid_code": invalid_code_count,
        "excluded_duplicate": duplicate_count,
        "excluded_market_unknown": 0,
        "non_corporate_products": diagnostics["non_corporate_products"],
    }


def enrich_with_board_snapshot(quotes_df: pd.DataFrame, base_df: pd.DataFrame, settings: dict[str, Any], token: str, *, mode: str = "") -> tuple[pd.DataFrame, dict[str, Any]]:
    """Enrich selected quotes with board snapshots and retry once after register."""
    logger.info("enrich_with_board_snapshot start")
    rows: list[dict[str, Any]] = []
    register_targets: list[dict[str, Any]] = []
    excluded_missing_prev_close = 0
    attempted_count = 0
    skipped_not_found_count = 0
    skipped_codes: list[dict[str, Any]] = []
    fallback_exchange_success_count = 0
    failed_hard_count = 0
    unregister_all_called = False
    unregister_all_retry_called = False
    register_error_count = 0
    register_error_codes: list[str] = []
    hard_fail_reason = ""
    used_live_data = True
    initial_unregister_result = _attempt_unregister_all(settings, token, context_label="board_phase_start")
    unregister_all_called = bool(initial_unregister_result.get("called"))
    for _, row in quotes_df.iterrows():
        code = str(row["code"])
        if not _is_code4(code):
            logger.debug("board skipped invalid code=%s", code)
            continue
        attempted_count += 1
        try:
            payload, attempt_diag = _fetch_board_with_exchange_fallback(settings, token, base_df, row, mode=mode)
        except PipelineFailClosed as exc:
            failed_hard_count += 1
            hard_fail_reason = str(exc)
            raise
        if payload is None:
            skipped_not_found_count += 1
            if len(skipped_codes) < 20:
                skipped_codes.append(
                    {
                        "code": code,
                        "first_exchange": attempt_diag["first_exchange"],
                        "tried_exchanges": attempt_diag["tried_exchanges"],
                        "errors": [
                            {
                                "status_code": item.get("status_code"),
                                "error_code": item.get("error_code"),
                                "message": item.get("message"),
                            }
                            for item in attempt_diag["recoverable_errors"]
                        ],
                    }
                )
            continue
        resolved_exchange = int(attempt_diag["chosen_exchange"])
        request_symbol = str(attempt_diag["request_symbol"])
        has_major_fields = bool(attempt_diag["has_major_fields"])
        has_prev_close = bool(attempt_diag["has_prev_close"])
        if bool(attempt_diag["fallback_exchange_used"]):
            fallback_exchange_success_count += 1
        if bool(attempt_diag.get("registry_reset_used")):
            unregister_all_called = True
        if has_major_fields and has_prev_close:
            rows.append(_board_to_row(code, payload, request_symbol, resolved_exchange, mode=mode))
            continue
        if not has_major_fields:
            register_targets.append({"code": code, "resolved_exchange": resolved_exchange, "request_symbol": request_symbol})
            rows.append(_board_to_row(code, payload, request_symbol, resolved_exchange, mode=mode))
            continue
        excluded_missing_prev_close += 1
        logger.warning("board excluded due to missing prev_close after base fallback mode=%s code=%s exchange=%s request_symbol=%s", mode, code, resolved_exchange, request_symbol)
    retry_count = 0
    if register_targets:
        successful_codes = {str(row["code"]) for row in rows}
        register_df = pd.DataFrame(register_targets)
        register_df["code"] = register_df["code"].astype(str)
        register_df = register_df[~register_df["code"].isin(successful_codes)].drop_duplicates(["code", "resolved_exchange"]).reset_index(drop=True)
        retry_unregister_result = _attempt_unregister_all(settings, token, context_label="board_register_retry")
        unregister_all_retry_called = bool(retry_unregister_result.get("called"))
        try:
            _register_symbols(settings, token, register_df)
        except PipelineFailClosed as exc:
            register_error_count += 1
            register_error_codes.append("register_failed")
            hard_fail_reason = str(exc)
            raise
        time.sleep(float(settings.get("KABU_PUSH_TIMEOUT_SECONDS", 4.0)))
        row_map = {row["code"]: row for row in rows}
        for _, register_row in register_df.iterrows():
            code = str(register_row["code"])
            request_symbol = str(register_row["request_symbol"])
            resolved_exchange = int(register_row["resolved_exchange"])
            logger.debug("board request code=%s request_symbol=%s resolved_exchange=%s retry=%s", code, request_symbol, resolved_exchange, True)
            time.sleep(0.13)
            payload = _fetch_board(settings, token, request_symbol)
            retry_count += 1
            if not _board_has_major_fields(payload):
                raise PipelineFailClosed(f"fail-closed: board snapshot still missing major fields after register retry code={code} request_symbol={request_symbol}")
            if not _fill_prev_close_from_base(payload, base_df, code, request_symbol=request_symbol, resolved_exchange=resolved_exchange, mode=mode):
                excluded_missing_prev_close += 1
                row_map.pop(code, None)
                logger.warning("board excluded due to missing prev_close after retry/base fallback mode=%s code=%s exchange=%s request_symbol=%s", mode, code, resolved_exchange, request_symbol)
                continue
            row_map[code] = _board_to_row(code, payload, request_symbol, resolved_exchange, mode=mode)
        rows = list(row_map.values())
    board_df = pd.DataFrame(rows)
    selected_count = int(len(quotes_df))
    minimum_success_count = int(math.ceil(selected_count * 0.5)) if selected_count > 0 else 0
    success_count = int(len(board_df))
    if board_df.empty:
        raise PipelineFailClosed("fail-closed: board enrichment produced no rows.")
    if success_count < minimum_success_count:
        raise PipelineFailClosed(
            f"fail-closed: board enrichment success rate too low success_count={success_count} selected_count={selected_count} minimum_success_count={minimum_success_count}"
        )
    warning_messages: list[str] = []
    if skipped_not_found_count or excluded_missing_prev_close:
        warning_messages.append(
            f"board partial success: success_count={success_count} selected_count={selected_count} skipped_not_found_count={skipped_not_found_count} excluded_missing_prev_close={excluded_missing_prev_close}"
        )
    logger.info(
        "enrich_with_board_snapshot end success_count=%s selected_count=%s retry_count=%s skipped_not_found_count=%s fallback_exchange_success_count=%s",
        success_count,
        selected_count,
        retry_count,
        skipped_not_found_count,
        fallback_exchange_success_count,
    )
    return board_df, {
        "attempted_count": attempted_count,
        "success_count": success_count,
        "skipped_not_found_count": skipped_not_found_count,
        "skipped_codes": skipped_codes,
        "retried_count": retry_count,
        "fallback_exchange_success_count": fallback_exchange_success_count,
        "failed_hard_count": failed_hard_count,
        "row_count": success_count,
        "selected_count": selected_count,
        "minimum_success_count": minimum_success_count,
        "excluded_missing_prev_close": excluded_missing_prev_close,
        "warning_messages": warning_messages,
        "unregister_all_called": unregister_all_called,
        "unregister_all_retry_called": unregister_all_retry_called,
        "register_target_count": int(len(register_df)) if register_targets else 0,
        "register_error_count": register_error_count,
        "register_error_codes": register_error_codes,
        "hard_fail_reason": hard_fail_reason,
        "used_live_data": used_live_data,
        "initial_unregister": initial_unregister_result,
    }


def _score_percentile(series: pd.Series) -> pd.Series:
    numeric = _coerce_numeric(series)
    if numeric.dropna().empty:
        return pd.Series([0.0] * len(series), index=series.index)
    return numeric.rank(pct=True, ascending=True).fillna(0.0)


def _score_rank_ascending(series: pd.Series) -> pd.Series:
    numeric = _coerce_numeric(series)
    if numeric.dropna().empty:
        return pd.Series([0.0] * len(series), index=series.index)
    return 1.0 - numeric.rank(pct=True, ascending=True).fillna(1.0)


def _score_percentile_within_group(series: pd.Series, groups: pd.Series) -> pd.Series:
    numeric = _coerce_numeric(series)
    if numeric.dropna().empty:
        return pd.Series([0.0] * len(series), index=series.index)
    grouped = pd.Series(groups, index=series.index)
    return numeric.groupby(grouped).rank(pct=True, ascending=True).fillna(0.0)


def _group_rank_desc(series: pd.Series, groups: pd.Series) -> pd.Series:
    numeric = _coerce_numeric(series)
    if numeric.dropna().empty:
        return pd.Series([float("inf")] * len(series), index=series.index)
    grouped = pd.Series(groups, index=series.index)
    return numeric.groupby(grouped).rank(method="min", ascending=False).fillna(float("inf"))


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return _coerce_numeric(numerator) / _coerce_numeric(denominator).replace(0, pd.NA)


def _summarize_market_scan_quality(
    *,
    scan_df: pd.DataFrame | None = None,
    sector_frame: pd.DataFrame | None = None,
    ranking_union_count: int | None = None,
    sector_basket_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    thresholds = TODAY_SECTOR_RANK_MODE_RULES
    summary_sector_frame = pd.DataFrame()
    base_scan = scan_df.copy() if isinstance(scan_df, pd.DataFrame) and not scan_df.empty else pd.DataFrame()
    if not base_scan.empty:
        expanded_scan = _ensure_scan_source_type(base_scan)
        sector_name_raw = expanded_scan.get("sector_name", pd.Series("", index=expanded_scan.index)).fillna("")
        if "sector_name_base" in expanded_scan.columns:
            sector_name_raw = sector_name_raw.where(sector_name_raw.astype(str).str.strip() != "", expanded_scan["sector_name_base"].fillna(""))
        expanded_scan["sector_name_effective"] = sector_name_raw.astype(str).map(str.strip).map(_normalize_industry_name)
        expanded_scan = expanded_scan[expanded_scan["sector_name_effective"].astype(str).str.strip() != ""].copy()
        if not expanded_scan.empty and "code" in expanded_scan.columns:
            summary_sector_frame = (
                expanded_scan.groupby("sector_name_effective", as_index=False)
                .agg(
                    ranking_confirmed_count=(
                        "ranking_union_member",
                        lambda s: int(
                            expanded_scan.loc[s.index, "code"].astype(str)[expanded_scan.loc[s.index, "ranking_union_member"].fillna(False)].drop_duplicates().shape[0]
                        ),
                    ),
                    basket_member_count=(
                        "industry_basket_member",
                        lambda s: int(
                            expanded_scan.loc[s.index, "code"].astype(str)[expanded_scan.loc[s.index, "industry_basket_member"].fillna(False)].drop_duplicates().shape[0]
                        ),
                    ),
                )
                .rename(columns={"sector_name_effective": "sector_name"})
            )
            breadth_map = (
                expanded_scan[expanded_scan.get("source_type", pd.Series(dtype=str)).astype(str).ne("industry_basket")]
                .groupby("sector_name_effective")["source_type"]
                .nunique()
                .to_dict()
            )
            summary_sector_frame["ranking_source_breadth_ex_basket"] = summary_sector_frame["sector_name"].map(lambda value: int(breadth_map.get(str(value or ""), 0) or 0))
            if ranking_union_count is None:
                ranking_union_count = int(
                    expanded_scan[expanded_scan.get("ranking_union_member", pd.Series(False, index=expanded_scan.index)).fillna(False)]["code"].astype(str).drop_duplicates().shape[0]
                )
            if sector_basket_counts is None:
                sector_basket_counts = {
                    str(key): int(value or 0)
                    for key, value in (
                        expanded_scan[expanded_scan.get("industry_basket_member", pd.Series(False, index=expanded_scan.index)).fillna(False)]
                        .groupby("sector_name_effective")["code"]
                        .nunique()
                        .to_dict()
                    ).items()
                }
    if summary_sector_frame.empty:
        summary_sector_frame = sector_frame.copy() if isinstance(sector_frame, pd.DataFrame) and not sector_frame.empty else pd.DataFrame()
    if summary_sector_frame.empty:
        if ranking_union_count is None:
            ranking_union_count = 0
        if sector_basket_counts is None:
            sector_basket_counts = {}
    else:
        if ranking_union_count is None:
            ranking_union_count = 0
        if sector_basket_counts is None:
            if {"sector_name", "basket_member_count"}.issubset(summary_sector_frame.columns):
                sector_basket_counts = {
                    str(row.get("sector_name", "") or ""): int(row.get("basket_member_count", 0) or 0)
                    for _, row in summary_sector_frame.iterrows()
                }
            else:
                sector_basket_counts = {}
    ranking_union_count = int(ranking_union_count or 0)
    sector_basket_counts = {str(key): int(value or 0) for key, value in (sector_basket_counts or {}).items()}
    sectors_with_ranking_confirmed_ge5 = (
        int(_coerce_numeric(summary_sector_frame.get("ranking_confirmed_count", pd.Series(dtype="float64"))).fillna(0.0).ge(5.0).sum())
        if not summary_sector_frame.empty
        else 0
    )
    sectors_with_ranking_confirmed_ge4 = (
        int(_coerce_numeric(summary_sector_frame.get("ranking_confirmed_count", pd.Series(dtype="float64"))).fillna(0.0).ge(4.0).sum())
        if not summary_sector_frame.empty
        else 0
    )
    sectors_with_source_breadth_ge2 = (
        int(_coerce_numeric(summary_sector_frame.get("ranking_source_breadth_ex_basket", pd.Series(dtype="float64"))).fillna(0.0).ge(2.0).sum())
        if not summary_sector_frame.empty
        else 0
    )
    gate_failures: list[str] = []
    if ranking_union_count < int(thresholds["ranking_union_count_min"]):
        gate_failures.append(f"ranking_union_count={ranking_union_count}<{int(thresholds['ranking_union_count_min'])}")
    if sectors_with_ranking_confirmed_ge5 < int(thresholds["sectors_with_ranking_confirmed_ge5_min"]):
        gate_failures.append(
            f"sectors_with_ranking_confirmed_ge5={sectors_with_ranking_confirmed_ge5}<{int(thresholds['sectors_with_ranking_confirmed_ge5_min'])}"
        )
    if sectors_with_source_breadth_ge2 < int(thresholds["sectors_with_source_breadth_ge2_min"]):
        gate_failures.append(
            f"sectors_with_source_breadth_ge2={sectors_with_source_breadth_ge2}<{int(thresholds['sectors_with_source_breadth_ge2_min'])}"
        )
    mode = _classify_wide_scan_mode(
        ranking_union_count,
        sectors_with_ranking_confirmed_ge5,
        sectors_with_source_breadth_ge2,
        sectors_with_ranking_confirmed_ge4=sectors_with_ranking_confirmed_ge4,
    )
    if mode == "anchored_overlay":
        if gate_failures:
            reason = f"near_pass_override: {'; '.join(gate_failures)}"
        else:
            reason = (
                f"ranking_union_count={ranking_union_count}>={int(thresholds['ranking_union_count_min'])}; "
                f"sectors_with_ranking_confirmed_ge5={sectors_with_ranking_confirmed_ge5}>={int(thresholds['sectors_with_ranking_confirmed_ge5_min'])}; "
                f"sectors_with_source_breadth_ge2={sectors_with_source_breadth_ge2}>={int(thresholds['sectors_with_source_breadth_ge2_min'])}"
            )
    else:
        reason = "; ".join(gate_failures)
    summary_text = (
        f"mode={mode}; ranking_union_count={ranking_union_count}; "
        f"sectors_with_ranking_confirmed_ge5={sectors_with_ranking_confirmed_ge5}; "
        f"sectors_with_ranking_confirmed_ge4={sectors_with_ranking_confirmed_ge4}; "
        f"sectors_with_source_breadth_ge2={sectors_with_source_breadth_ge2}; "
        f"reason={reason}"
    )
    return {
        "mode": mode,
        "reason": reason,
        "summary": summary_text,
        "ranking_union_count": ranking_union_count,
        "sector_basket_counts": sector_basket_counts,
        "sectors_with_ranking_confirmed_ge5": sectors_with_ranking_confirmed_ge5,
        "sectors_with_ranking_confirmed_ge4": sectors_with_ranking_confirmed_ge4,
        "sectors_with_source_breadth_ge2": sectors_with_source_breadth_ge2,
        "thresholds": {
            "ranking_union_count_min": int(thresholds["ranking_union_count_min"]),
            "sectors_with_ranking_confirmed_ge5_min": int(thresholds["sectors_with_ranking_confirmed_ge5_min"]),
            "sectors_with_source_breadth_ge2_min": int(thresholds["sectors_with_source_breadth_ge2_min"]),
        },
    }


def _rank_display_series(primary: pd.Series | None, *, fallback: pd.Series | None = None) -> pd.Series:
    numeric = _coerce_numeric(primary) if primary is not None else pd.Series(dtype="float64")
    if fallback is not None:
        numeric = numeric.fillna(_coerce_numeric(fallback))
    if numeric.empty:
        return pd.Series(dtype="Int64")
    return numeric.round().clip(lower=1.0).astype("Int64")


def _scan_sample_warning_details(
    scan_member_count: pd.Series,
    scan_coverage: pd.Series,
    rules: dict[str, float] | None = None,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    rules = rules or INTRADAY_SCAN_SAMPLE_WARNING_RULES
    count = _coerce_numeric(scan_member_count).fillna(0.0)
    coverage = _coerce_numeric(scan_coverage).fillna(0.0)
    no_scan_mask = count <= 0.0
    critical_mask = (~no_scan_mask) & (count < float(rules["critical_count"]))
    warn_mask = (~no_scan_mask) & (~critical_mask) & (count < float(rules["warn_count"])) & (coverage < float(rules["warn_coverage"]))
    thin_mask = (~no_scan_mask) & (~critical_mask) & (~warn_mask) & (count < float(rules["thin_count"])) & (coverage < float(rules["thin_coverage"]))
    level = pd.Series([""] * len(count), index=count.index, dtype="object")
    level.loc[no_scan_mask] = "no_scan"
    level.loc[thin_mask] = "thin"
    level.loc[warn_mask] = "warn"
    level.loc[critical_mask] = "critical"
    label = pd.Series([""] * len(count), index=count.index, dtype="object")
    label.loc[no_scan_mask] = "wide scan母数不足"
    label.loc[thin_mask] = "wide scan母数不足"
    label.loc[warn_mask] = "wide scan母数不足"
    label.loc[critical_mask] = "wide scan母数不足"
    reason = pd.Series([""] * len(count), index=count.index, dtype="object")
    reason.loc[no_scan_mask] = count.loc[no_scan_mask].map(lambda value: f"wide_scan_member_count={int(value)}") + coverage.loc[no_scan_mask].map(lambda value: f", wide_scan_coverage={value:.3f}")
    reason.loc[thin_mask] = count.loc[thin_mask].map(lambda value: f"wide_scan_member_count={int(value)}") + coverage.loc[thin_mask].map(lambda value: f", wide_scan_coverage={value:.3f}")
    reason.loc[warn_mask] = count.loc[warn_mask].map(lambda value: f"wide_scan_member_count={int(value)}") + coverage.loc[warn_mask].map(lambda value: f", wide_scan_coverage={value:.3f}")
    reason.loc[critical_mask] = count.loc[critical_mask].map(lambda value: f"wide_scan_member_count={int(value)}") + coverage.loc[critical_mask].map(lambda value: f", wide_scan_coverage={value:.3f}")
    return level, label, reason


def _sector_rank_from_returns(base_df: pd.DataFrame, return_col: str, sector_ret_col: str, rank_col: str) -> pd.DataFrame:
    sector_rank = (
        base_df.groupby("sector_name", dropna=False)[return_col]
        .median()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={return_col: sector_ret_col})
    )
    sector_rank[rank_col] = range(1, len(sector_rank) + 1)
    return sector_rank


def _summarize_sector_frame(frame: pd.DataFrame, *, sort_columns: list[str], ascending: list[bool]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "sector_name",
                "n",
                "breadth",
                "industry_rank_live",
                "sector_rank_1w",
                "sector_rank_1m",
                "sector_rank_3m",
                "leaders",
                "leader_contribution_pct",
                "price_up_count",
                "turnover_count",
                "volume_surge_count",
                "turnover_surge_count",
                "intraday_sector_score",
            ]
        )
    return frame.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)


def _ensure_scan_source_type(scan_df: pd.DataFrame) -> pd.DataFrame:
    if scan_df is None or scan_df.empty:
        return pd.DataFrame() if scan_df is None else scan_df.copy()

    out = scan_df.copy()
    if "source_type" in out.columns:
        return out

    if "ranking_sources" in out.columns:
        source_labels = {"price_up", "turnover", "volume_surge", "turnover_surge", "industry_basket"}
        expanded = out.copy()
        expanded["source_type"] = expanded["ranking_sources"].fillna("").astype(str).str.split(",")
        expanded = expanded.explode("source_type")
        expanded["source_type"] = expanded["source_type"].fillna("").astype(str).str.strip()
        expanded = expanded[expanded["source_type"].isin(source_labels)].copy()
        if "industry_basket_member" in out.columns:
            basket_rows = out[out["industry_basket_member"].fillna(False)].copy()
            if not basket_rows.empty:
                basket_rows["source_type"] = "industry_basket"
                expanded = pd.concat([expanded, basket_rows], ignore_index=True, sort=False)
        if not expanded.empty:
            dedupe_cols = [c for c in ["code", "sector_name", "source_type"] if c in expanded.columns]
            if dedupe_cols:
                expanded = expanded.drop_duplicates(subset=dedupe_cols)
            return expanded

    rank_map = {
        "price_up_rank": "price_up",
        "turnover_rank": "turnover",
        "volume_surge_rank": "volume_surge",
        "turnover_surge_rank": "turnover_surge",
        "industry_up_rank": "industry_up",
    }

    pieces: list[pd.DataFrame] = []
    for col, label in rank_map.items():
        if col in out.columns:
            mask = pd.to_numeric(out[col], errors="coerce").notna()
            if mask.any():
                part = out.loc[mask].copy()
                part["source_type"] = label
                pieces.append(part)

    if pieces:
        rebuilt = pd.concat(pieces, ignore_index=True)
        dedupe_cols = [c for c in ["code", "sector_name", "source_type"] if c in rebuilt.columns]
        if dedupe_cols:
            rebuilt = rebuilt.drop_duplicates(subset=dedupe_cols)
        return rebuilt

    out["source_type"] = "market_scan"
    return out


def _empty_sector_live_aggregate_audit_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=SECTOR_LIVE_AGGREGATE_AUDIT_COLUMNS)


def _build_sector_live_aggregate_frame(merged: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if merged is None or merged.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "sector_name",
                "normalized_sector_name",
                "current_price",
                "live_ret_vs_prev_close",
                "live_turnover_value",
                "turnover_ratio_20d",
            ]
        ), {
            "source_frame": "stock_merged_observed_live_rows",
            "turnover_ratio_source_column": "live_turnover_ratio_20d",
            "has_turnover_ratio_source": False,
            "observed_row_count": 0,
        }
    working = merged.copy()
    if "code" not in working.columns:
        working["code"] = ""
    working["code"] = working["code"].astype(str)
    if "sector_name" not in working.columns:
        working["sector_name"] = ""
    working["sector_name"] = working["sector_name"].map(_normalize_industry_name)
    working["normalized_sector_name"] = working.get("normalized_sector_name", working["sector_name"].map(_normalize_industry_key))
    working = working[working["code"].map(_is_code4)].copy()
    working = working[working["normalized_sector_name"].astype(str).str.strip() != ""].copy()
    if working.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "sector_name",
                "normalized_sector_name",
                "current_price",
                "live_ret_vs_prev_close",
                "live_turnover_value",
                "turnover_ratio_20d",
            ]
        ), {
            "source_frame": "stock_merged_observed_live_rows",
            "turnover_ratio_source_column": "live_turnover_ratio_20d",
            "has_turnover_ratio_source": False,
            "observed_row_count": 0,
        }
    working["current_price"] = _coerce_numeric(working.get("current_price", working.get("live_price", pd.Series(pd.NA, index=working.index))))
    working["live_ret_vs_prev_close"] = _coerce_numeric(working.get("live_ret_vs_prev_close", pd.Series(pd.NA, index=working.index)))
    working["live_turnover_value"] = _coerce_numeric(working.get("live_turnover_value", working.get("live_turnover", pd.Series(pd.NA, index=working.index))))
    turnover_ratio_source_column = "live_turnover_ratio_20d" if "live_turnover_ratio_20d" in working.columns else ""
    if turnover_ratio_source_column:
        working["turnover_ratio_20d"] = _coerce_numeric(working[turnover_ratio_source_column])
    else:
        working["turnover_ratio_20d"] = pd.Series([math.nan] * len(working), index=working.index, dtype="float64")
    keep_columns = [
        "code",
        "sector_name",
        "normalized_sector_name",
        "current_price",
        "live_ret_vs_prev_close",
        "live_turnover_value",
        "turnover_ratio_20d",
    ]
    live_frame = working[keep_columns].drop_duplicates("code").reset_index(drop=True)
    return live_frame, {
        "source_frame": "stock_merged_observed_live_rows",
        "turnover_ratio_source_column": turnover_ratio_source_column or "",
        "has_turnover_ratio_source": bool(turnover_ratio_source_column),
        "observed_row_count": int(len(live_frame)),
    }


def _resolve_live_aggregate_status(
    *,
    observed_count: int,
    ret_count: int,
    turnover_count: int,
    turnover_ratio_count: int,
    has_turnover_ratio_source: bool,
) -> tuple[str, str]:
    if observed_count <= 0:
        return "no_live_rows", "observed live rows がありません。"
    if ret_count <= 0 and turnover_count <= 0:
        return "partial_missing_multiple", f"observed={observed_count}, ret=0, turnover=0, turnover_ratio={turnover_ratio_count}"
    if ret_count <= 0:
        return "partial_missing_ret", f"observed={observed_count}, ret=0, turnover={turnover_count}, turnover_ratio={turnover_ratio_count}"
    if turnover_count <= 0:
        return "partial_missing_turnover", f"observed={observed_count}, ret={ret_count}, turnover=0, turnover_ratio={turnover_ratio_count}"
    if not has_turnover_ratio_source:
        return "no_turnover_ratio_source", f"observed={observed_count}, ret={ret_count}, turnover={turnover_count}, turnover_ratio_source=missing"
    if turnover_ratio_count <= 0:
        return "no_turnover_ratio_source", f"observed={observed_count}, ret={ret_count}, turnover={turnover_count}, turnover_ratio=0"
    missing_parts: list[str] = []
    if ret_count < observed_count:
        missing_parts.append(f"ret={ret_count}/{observed_count}")
    if turnover_count < observed_count:
        missing_parts.append(f"turnover={turnover_count}/{observed_count}")
    if turnover_ratio_count < observed_count:
        missing_parts.append(f"turnover_ratio={turnover_ratio_count}/{observed_count}")
    if missing_parts:
        if ret_count < observed_count and turnover_count >= observed_count and turnover_ratio_count >= observed_count:
            return "partial_missing_ret", ", ".join(missing_parts)
        if turnover_count < observed_count and ret_count >= observed_count and turnover_ratio_count >= observed_count:
            return "partial_missing_turnover", ", ".join(missing_parts)
        return "partial_missing_multiple", ", ".join(missing_parts)
    return "observed", f"observed={observed_count}, ret={ret_count}, turnover={turnover_count}, turnover_ratio={turnover_ratio_count}"


def _apply_sector_live_aggregates(
    sector_base: pd.DataFrame,
    live_frame: pd.DataFrame,
    *,
    sector_key_col: str = "normalized_sector_name",
    source_meta: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if sector_base is None or sector_base.empty:
        return sector_base.copy() if isinstance(sector_base, pd.DataFrame) else _empty_sector_leaderboard(), _empty_sector_live_aggregate_audit_frame()
    source_meta = source_meta or {}
    working = sector_base.copy()
    for column in [
        "live_turnover_total",
        "leader_live_turnover",
        "median_live_ret",
        "turnover_ratio_median",
        "live_aggregate_observed_count",
        "live_aggregate_ret_count",
        "live_aggregate_turnover_count",
        "live_aggregate_turnover_ratio_count",
        "live_aggregate_status",
        "live_aggregate_reason",
        "live_aggregate_use_for_score",
    ]:
        if column not in working.columns:
            working[column] = pd.NA
    if live_frame is None or live_frame.empty or sector_key_col not in working.columns:
        working["live_turnover_total"] = pd.NA
        working["leader_live_turnover"] = pd.NA
        working["median_live_ret"] = pd.NA
        working["turnover_ratio_median"] = pd.NA
        working["live_aggregate_observed_count"] = 0
        working["live_aggregate_ret_count"] = 0
        working["live_aggregate_turnover_count"] = 0
        working["live_aggregate_turnover_ratio_count"] = 0
        working["live_aggregate_status"] = "no_live_rows"
        working["live_aggregate_reason"] = "observed live rows がありません。"
        working["live_aggregate_use_for_score"] = False
        audit_rows = [
            {
                "today_rank": int(row.get("today_rank", 0) or 0) if pd.notna(row.get("today_rank")) else None,
                "sector_name": str(row.get("sector_name", "") or ""),
                "normalized_sector_name": str(row.get(sector_key_col, "") or ""),
                "live_aggregate_observed_count": 0,
                "live_aggregate_ret_count": 0,
                "live_aggregate_turnover_count": 0,
                "live_aggregate_turnover_ratio_count": 0,
                "live_turnover_total_raw": None,
                "leader_live_turnover_raw": None,
                "median_live_ret_raw": None,
                "turnover_ratio_median_raw": None,
                "live_aggregate_status": "no_live_rows",
                "live_aggregate_reason": "observed live rows がありません。",
                "sample_codes": [],
            }
            for _, row in working.iterrows()
        ]
        return working, pd.DataFrame(audit_rows, columns=SECTOR_LIVE_AGGREGATE_AUDIT_COLUMNS)
    observed = live_frame.copy()
    observed[sector_key_col] = observed.get(sector_key_col, pd.Series(dtype=str)).astype(str)
    observed = observed[observed[sector_key_col].str.strip() != ""].copy()
    observed["live_ret_vs_prev_close"] = _coerce_numeric(observed["live_ret_vs_prev_close"])
    observed["live_turnover_value"] = _coerce_numeric(observed["live_turnover_value"])
    observed["turnover_ratio_20d"] = _coerce_numeric(observed["turnover_ratio_20d"])
    observed["current_price"] = _coerce_numeric(observed["current_price"])
    has_turnover_ratio_source = bool(source_meta.get("has_turnover_ratio_source", False))
    audit_rows: list[dict[str, Any]] = []
    for idx, sector_row in working.iterrows():
        sector_key = str(sector_row.get(sector_key_col, "") or "")
        sector_rows = observed[observed[sector_key_col] == sector_key].copy()
        observed_count = int(len(sector_rows))
        ret_count = int(sector_rows["live_ret_vs_prev_close"].notna().sum()) if not sector_rows.empty else 0
        turnover_count = int(sector_rows["live_turnover_value"].notna().sum()) if not sector_rows.empty else 0
        turnover_ratio_count = int(sector_rows["turnover_ratio_20d"].notna().sum()) if not sector_rows.empty else 0
        status, reason = _resolve_live_aggregate_status(
            observed_count=observed_count,
            ret_count=ret_count,
            turnover_count=turnover_count,
            turnover_ratio_count=turnover_ratio_count,
            has_turnover_ratio_source=has_turnover_ratio_source,
        )
        live_turnover_total = float(sector_rows["live_turnover_value"].dropna().sum()) if turnover_count > 0 else None
        leader_live_turnover = float(sector_rows["live_turnover_value"].dropna().max()) if turnover_count > 0 else None
        median_live_ret = float(sector_rows["live_ret_vs_prev_close"].dropna().median()) if ret_count > 0 else None
        turnover_ratio_median = float(sector_rows["turnover_ratio_20d"].dropna().median()) if turnover_ratio_count > 0 else None
        working.at[idx, "live_turnover_total"] = live_turnover_total if live_turnover_total is not None else pd.NA
        working.at[idx, "leader_live_turnover"] = leader_live_turnover if leader_live_turnover is not None else pd.NA
        working.at[idx, "median_live_ret"] = median_live_ret if median_live_ret is not None else pd.NA
        working.at[idx, "turnover_ratio_median"] = turnover_ratio_median if turnover_ratio_median is not None else pd.NA
        working.at[idx, "live_aggregate_observed_count"] = observed_count
        working.at[idx, "live_aggregate_ret_count"] = ret_count
        working.at[idx, "live_aggregate_turnover_count"] = turnover_count
        working.at[idx, "live_aggregate_turnover_ratio_count"] = turnover_ratio_count
        working.at[idx, "live_aggregate_status"] = status
        working.at[idx, "live_aggregate_reason"] = reason
        working.at[idx, "live_aggregate_use_for_score"] = bool(status == "observed")
        sample_codes = (
            sector_rows.sort_values(["live_turnover_value", "code"], ascending=[False, True], kind="mergesort")["code"].astype(str).head(5).tolist()
            if not sector_rows.empty
            else []
        )
        audit_rows.append(
            {
                "today_rank": int(sector_row.get("today_rank", 0) or 0) if pd.notna(sector_row.get("today_rank")) else None,
                "sector_name": str(sector_row.get("sector_name", "") or ""),
                "normalized_sector_name": sector_key,
                "live_aggregate_observed_count": observed_count,
                "live_aggregate_ret_count": ret_count,
                "live_aggregate_turnover_count": turnover_count,
                "live_aggregate_turnover_ratio_count": turnover_ratio_count,
                "live_turnover_total_raw": live_turnover_total,
                "leader_live_turnover_raw": leader_live_turnover,
                "median_live_ret_raw": median_live_ret,
                "turnover_ratio_median_raw": turnover_ratio_median,
                "live_aggregate_status": status,
                "live_aggregate_reason": reason,
                "sample_codes": sample_codes,
            }
        )
    for column in [
        "live_aggregate_observed_count",
        "live_aggregate_ret_count",
        "live_aggregate_turnover_count",
        "live_aggregate_turnover_ratio_count",
    ]:
        working[column] = _coerce_numeric(working[column]).fillna(0.0).astype(int)
    working["live_aggregate_use_for_score"] = working["live_aggregate_use_for_score"].fillna(False).astype(bool)
    return working, pd.DataFrame(audit_rows, columns=SECTOR_LIVE_AGGREGATE_AUDIT_COLUMNS)


def _finalize_sector_live_aggregate_audit(
    audit_frame: pd.DataFrame,
    today_sector_leaderboard: pd.DataFrame,
    *,
    sector_key_col: str = "normalized_sector_name",
) -> pd.DataFrame:
    if not isinstance(audit_frame, pd.DataFrame) or audit_frame.empty:
        return _empty_sector_live_aggregate_audit_frame()
    working = audit_frame.copy()
    if isinstance(today_sector_leaderboard, pd.DataFrame) and not today_sector_leaderboard.empty:
        lookup_columns = [column for column in ["normalized_sector_name", "sector_name", "today_rank"] if column in today_sector_leaderboard.columns]
        if lookup_columns:
            lookup = today_sector_leaderboard[lookup_columns].copy()
            existing_today_rank = working.get("today_rank", pd.Series(pd.NA, index=working.index))
            if "normalized_sector_name" in lookup.columns and "normalized_sector_name" in working.columns and "today_rank" in lookup.columns:
                normalized_lookup = lookup.drop_duplicates("normalized_sector_name").set_index("normalized_sector_name")
                working["today_rank"] = working["normalized_sector_name"].map(normalized_lookup["today_rank"]).fillna(existing_today_rank)
                if "sector_name" in normalized_lookup.columns:
                    working["sector_name"] = working["sector_name"].where(
                        working["sector_name"].astype(str).str.strip() != "",
                        working["normalized_sector_name"].map(normalized_lookup["sector_name"]).fillna(""),
                    )
                existing_today_rank = working["today_rank"]
            if "sector_name" in lookup.columns and "today_rank" in lookup.columns:
                sector_lookup = lookup.drop_duplicates("sector_name").set_index("sector_name")
                working["today_rank"] = working["sector_name"].map(sector_lookup["today_rank"]).fillna(existing_today_rank)
    for column in SECTOR_LIVE_AGGREGATE_AUDIT_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA
    working["_today_rank_sort"] = _coerce_numeric(working["today_rank"]).fillna(9999.0)
    working = working.sort_values(
        ["_today_rank_sort", "sector_name", "normalized_sector_name"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    return working.drop(columns=["_today_rank_sort"], errors="ignore")[SECTOR_LIVE_AGGREGATE_AUDIT_COLUMNS].reset_index(drop=True)


def _build_sector_summary_bundle(ranking_df: pd.DataFrame, industry_df: pd.DataFrame, base_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    del ranking_df, industry_df, base_df
    empty = pd.DataFrame()
    return {"today": empty, "weekly": empty.copy(), "monthly": empty.copy()}


def _build_stock_candidate_bundles(mode: str, merged: pd.DataFrame, sector_bundle: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    del mode, merged, sector_bundle
    empty = pd.DataFrame()
    return {"center_stocks": empty, "watch_candidates": empty, "buy_candidates": empty}


def _empty_sector_leaderboard() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "today_rank",
            "today_display_rank",
            "today_rank_mode",
            "rank_mode_reason",
            "original_industry_rank_live",
            "anchor_rank_source",
            "present_in_live_industry_table",
            "present_in_sector_summary_before_filter",
            "present_in_today_display_universe",
            "display_eligible",
            "display_excluded_reason",
            "sector_name",
            "normalized_sector_name",
            "representative_stock",
            "representative_stocks",
            "leaders",
            "n",
            "live_turnover_total",
            "leader_live_turnover",
            "median_live_ret",
            "turnover_ratio_median",
            "live_aggregate_observed_count",
            "live_aggregate_ret_count",
            "live_aggregate_turnover_count",
            "live_aggregate_turnover_ratio_count",
            "live_aggregate_status",
            "live_aggregate_reason",
            "live_aggregate_use_for_score",
            "price_block_score",
            "flow_block_score",
            "participation_block_score",
            "sector_confidence",
            "sector_caution",
            "scan_sample_warning_level",
            "scan_sample_warning_reason",
            "industry_up",
            "industry_up_value",
            "industry_up_rank",
            "industry_rank_live",
            "signal_breadth_count",
            "signal_breadth_share",
            "price_up_share_of_sector",
            "turnover_share_of_sector",
            "breadth",
            "sector_constituent_count",
            "scan_member_count",
            "scan_member_count_norm",
            "wide_scan_member_count",
            "wide_scan_member_count_norm",
            "wide_scan_coverage",
            "ranking_confirmed_count",
            "ranking_confirmed_count_norm",
            "ranking_confirmed_share_of_sector",
            "ranking_confirmed_share_of_market",
            "ranking_confirmed_coverage",
            "basket_member_count",
            "ranking_source_breadth_ex_basket",
            "ranking_source_breadth_ex_basket_norm",
            "market_scan_quality_summary",
            "price_up_count",
            "price_up_count_norm",
            "turnover_count",
            "turnover_count_norm",
            "volume_surge_count",
            "volume_surge_count_norm",
            "turnover_surge_count",
            "turnover_surge_count_norm",
            "breadth_sample_count",
            "breadth_reliability",
            "breadth_core_score",
            "participation_block_score_raw",
            "intraday_sector_score_raw",
            "breadth_penalty",
            "concentration_penalty",
            "intraday_penalty_total",
            "intraday_sector_score",
            "today_sector_score",
            "industry_up_anchor_rank",
            "industry_anchor_rank",
            "allowed_shift",
            "max_upshift",
            "max_downshift",
            "upshift_blocked_reason",
            "rank_constraint_applied",
            "final_rank_delta",
            "score_rank",
            "rank_shift_limit",
            "tethered_rank",
        ]
    )


def _sort_today_sector_leaderboard_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else _empty_sector_leaderboard()
    working = frame.copy()
    sort_priority = [
        ("today_display_rank", True),
        ("tethered_rank", True),
        ("preferred_rank_slot", True),
        ("industry_anchor_rank", True),
        ("industry_rank_live", True),
        ("score_rank", True),
        ("today_sector_score", False),
        ("price_block_score", False),
        ("flow_block_score", False),
        ("participation_block_score", False),
        ("sector_name", True),
    ]
    sort_columns = [column for column, _ in sort_priority if column in working.columns]
    sort_ascending = [ascending for column, ascending in sort_priority if column in working.columns]
    if sort_columns:
        working = working.sort_values(sort_columns, ascending=sort_ascending, kind="mergesort").reset_index(drop=True)
    return working


def _is_rank_slot_assignment_feasible(frame: pd.DataFrame, available_slots: list[int]) -> bool:
    if frame.empty:
        return True
    slots = sorted(int(slot) for slot in available_slots)
    working = frame.copy()
    working["rank_floor_bound"] = _coerce_numeric(working["rank_floor_bound"]).fillna(1.0).round().astype(int)
    working["rank_ceiling_bound"] = _coerce_numeric(working["rank_ceiling_bound"]).fillna(float(len(slots))).round().astype(int)
    working = working.sort_values(
        ["rank_ceiling_bound", "rank_floor_bound", "industry_anchor_rank", "score_rank", "sector_name"],
        ascending=[True, True, True, True, True],
        kind="mergesort",
    )
    slot_cursor = 0
    for _, row in working.iterrows():
        floor_bound = int(row.get("rank_floor_bound", 1) or 1)
        ceiling_bound = int(row.get("rank_ceiling_bound", len(slots)) or len(slots))
        while slot_cursor < len(slots) and slots[slot_cursor] < floor_bound:
            slot_cursor += 1
        if slot_cursor >= len(slots):
            return False
        if slots[slot_cursor] > ceiling_bound:
            return False
        slot_cursor += 1
    return True


def _build_upshift_blocked_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    ranking_confirmed_count = int(float(row.get("ranking_confirmed_count", 0.0) or 0.0))
    max_upshift = int(float(row.get("max_upshift", 0.0) or 0.0))
    wide_scan_member_count = int(float(row.get("wide_scan_member_count", row.get("scan_member_count", 0.0)) or 0.0))
    if ranking_confirmed_count <= 0:
        reasons.append("ランキング裏付けなし")
    elif ranking_confirmed_count <= 2:
        reasons.append("ランキング裏付け極少")
    elif ranking_confirmed_count <= 4 and max_upshift <= 0:
        reasons.append("ランキング裏付け薄い")
    if wide_scan_member_count < int(WIDE_SCAN_BASKET_MIN_PER_SECTOR) and max_upshift <= 0:
        reasons.append("wide scan母数不足")
    return _build_sector_caution_tags(reasons)


def _apply_true_rank_shift_limits(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else _empty_sector_leaderboard()
    working = frame.copy().reset_index(drop=True)
    total_rows = len(working)
    fallback_anchor = pd.Series(range(1, total_rows + 1), index=working.index, dtype="float64")
    anchor_rank = _coerce_numeric(working["industry_anchor_rank"]).fillna(fallback_anchor)
    max_upshift = _coerce_numeric(working.get("max_upshift", working.get("rank_shift_limit", pd.Series([0.0] * total_rows, index=working.index)))).fillna(0.0).round().clip(lower=0.0)
    max_downshift = _coerce_numeric(working.get("max_downshift", pd.Series([float(INTRADAY_INDUSTRY_RANK_TETHER["max_downshift"])] * total_rows, index=working.index))).fillna(0.0).round().clip(lower=0.0)
    working["max_upshift"] = max_upshift.astype(int)
    working["max_downshift"] = max_downshift.astype(int)
    working["allowed_shift"] = max_upshift.astype(int)
    working["upshift_blocked_reason"] = working.apply(_build_upshift_blocked_reason, axis=1)
    blocked_upshift = working["upshift_blocked_reason"].astype(str).str.strip() != ""
    anchor_rank_int = anchor_rank.round().astype(int)
    raw_preferred_rank = _coerce_numeric(working["score_rank"]).fillna(_coerce_numeric(working["tethered_rank"])).fillna(anchor_rank).clip(lower=1.0, upper=float(total_rows)).round().astype(int)
    rank_floor = (anchor_rank - max_upshift).clip(lower=1.0, upper=float(total_rows)).round().astype(int)
    rank_floor = rank_floor.where(~blocked_upshift, anchor_rank_int)
    rank_ceiling = (anchor_rank + max_downshift).clip(lower=1.0, upper=float(total_rows)).round().astype(int)
    blocked_preferred_slot = raw_preferred_rank.where(raw_preferred_rank >= anchor_rank_int, anchor_rank_int)
    preferred_slot = raw_preferred_rank.where(~blocked_upshift, blocked_preferred_slot)
    working["rank_floor_bound"] = rank_floor
    working["rank_ceiling_bound"] = rank_ceiling
    working["preferred_rank_slot"] = preferred_slot
    working["_rank_row_id"] = working.index.astype(int)
    preferred_order = _sort_today_sector_leaderboard_rows(working)
    available_slots = list(range(1, total_rows + 1))
    assigned_slots: dict[int, int] = {}
    for _, row in preferred_order.iterrows():
        row_id = int(row.get("_rank_row_id", 0) or 0)
        floor_bound = int(row.get("rank_floor_bound", 1) or 1)
        ceiling_bound = int(row.get("rank_ceiling_bound", total_rows) or total_rows)
        preferred = int(row.get("preferred_rank_slot", floor_bound) or floor_bound)
        candidate_slots = [slot for slot in available_slots if floor_bound <= slot <= ceiling_bound]
        candidate_slots = sorted(candidate_slots, key=lambda slot: (abs(slot - preferred), slot))
        remaining_index = [idx for idx in working["_rank_row_id"].tolist() if idx not in assigned_slots and idx != row_id]
        chosen_slot = None
        for slot in candidate_slots:
            remaining_slots = [value for value in available_slots if value != slot]
            if _is_rank_slot_assignment_feasible(working[working["_rank_row_id"].isin(remaining_index)], remaining_slots):
                chosen_slot = slot
                break
        if chosen_slot is None:
            chosen_slot = candidate_slots[0] if candidate_slots else min(available_slots)
        assigned_slots[row_id] = int(chosen_slot)
        available_slots.remove(chosen_slot)
    working["tethered_rank"] = working["_rank_row_id"].map(assigned_slots).astype("float64")
    working["rank_constraint_applied"] = (
        _coerce_numeric(working["tethered_rank"]).fillna(anchor_rank).round().astype(int) != raw_preferred_rank
    ) | blocked_upshift
    working["final_rank_delta"] = _coerce_numeric(working["tethered_rank"]).fillna(anchor_rank) - anchor_rank
    return working.drop(columns=["rank_floor_bound", "rank_ceiling_bound", "preferred_rank_slot", "_rank_row_id"], errors="ignore")


def _sort_today_sector_leaderboard_for_display(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else _empty_sector_leaderboard()
    working = _sort_today_sector_leaderboard_rows(frame)
    fallback_rank = pd.Series(range(1, len(working) + 1), index=working.index, dtype="int64")
    existing_display_rank = _coerce_numeric(working.get("today_display_rank", pd.Series([pd.NA] * len(working), index=working.index)))
    working["today_display_rank"] = existing_display_rank.fillna(fallback_rank).round().clip(lower=1.0).astype("int64")
    working["today_rank"] = working["today_display_rank"]
    if "industry_anchor_rank" in working.columns:
        working["final_rank_delta"] = _coerce_numeric(working["today_display_rank"]).fillna(0.0) - _coerce_numeric(working["industry_anchor_rank"]).fillna(_coerce_numeric(working["today_display_rank"]).fillna(0.0))
    anchor_only_mask = working.get("today_rank_mode", pd.Series(dtype=str)).astype(str).eq("anchor_only")
    if anchor_only_mask.any():
        working.loc[anchor_only_mask, "final_rank_delta"] = 0.0
    return working


def _empty_persistence_table() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "persistence_rank",
            "sector_name",
            "sector_constituent_count",
            "sector_rs_vs_topix_1w",
            "sector_rs_vs_topix_1m",
            "sector_rs_vs_topix_3m",
            "representative_stock",
            "core_representatives",
            "core_representatives_count",
            "core_representatives_reason",
            "sector_confidence",
            "sector_gate_pass",
            "sector_gate_fail_reason",
            "sector_display_eligible",
            "quality_pass",
            "quality_warn",
            "quality_fail_reason",
            "sector_caution",
        ]
    )


def _build_persistence_quality_payload(row: pd.Series) -> dict[str, Any]:
    required_columns = [
        "sector_rs_vs_topix",
        "sector_constituent_count",
        "sector_positive_ratio",
        "leader_concentration_share",
    ]
    missing_columns = [column for column in required_columns if column not in row.index or pd.isna(row.get(column))]
    warn_tags: list[str] = []
    fail_reasons: list[str] = []
    if missing_columns:
        warn_tags.append("品質列欠損")
        fail_reasons.append(f"missing:{','.join(missing_columns)}")
        return {
            "quality_pass": False,
            "quality_warn": ", ".join(warn_tags),
            "quality_fail_reason": "|".join(fail_reasons),
        }
    sector_rs_vs_topix = float(row.get("sector_rs_vs_topix", 0.0) or 0.0)
    sector_constituent_count = float(row.get("sector_constituent_count", 0.0) or 0.0)
    sector_positive_ratio = float(row.get("sector_positive_ratio", 0.0) or 0.0)
    leader_concentration_share = float(row.get("leader_concentration_share", 0.0) or 0.0)
    if sector_rs_vs_topix <= 0.0:
        warn_tags.append("TOPIXを下回る")
        fail_reasons.append("rs_non_positive")
    if sector_constituent_count < 6.0:
        warn_tags.append("構成少")
        fail_reasons.append("constituents_lt_6")
    if sector_positive_ratio < 0.55:
        warn_tags.append("広がり弱い")
        fail_reasons.append("positive_ratio_lt_0.55")
    if leader_concentration_share > 0.40:
        warn_tags.append("偏重高い")
        fail_reasons.append("leader_concentration_gt_0.40")
    return {
        "quality_pass": not fail_reasons,
        "quality_warn": ", ".join(warn_tags),
        "quality_fail_reason": "|".join(fail_reasons),
    }


def _build_persistence_gate_payload(row: pd.Series, *, horizon: str) -> dict[str, Any]:
    required_columns = [
        "sector_rs_vs_topix",
        "sector_constituent_count",
        "sector_positive_ratio",
        "leader_concentration_share",
    ]
    if str(horizon) == "3m":
        required_columns.append("sector_rs_vs_topix_1m")
    missing_columns = [column for column in required_columns if column not in row.index or pd.isna(row.get(column))]
    if missing_columns:
        reason = f"missing:{','.join(missing_columns)}"
        return {
            "sector_gate_pass": False,
            "sector_gate_fail_reason": reason,
            "sector_display_eligible": False,
        }
    fail_reasons: list[str] = []
    sector_rs_vs_topix = float(row.get("sector_rs_vs_topix", 0.0) or 0.0)
    sector_constituent_count = float(row.get("sector_constituent_count", 0.0) or 0.0)
    sector_positive_ratio = float(row.get("sector_positive_ratio", 0.0) or 0.0)
    leader_concentration_share = float(row.get("leader_concentration_share", 0.0) or 0.0)
    if sector_rs_vs_topix <= 0.0:
        fail_reasons.append("rs_non_positive")
    if sector_constituent_count < 6.0:
        fail_reasons.append("constituents_lt_6")
    if sector_positive_ratio < 0.55:
        fail_reasons.append("positive_ratio_lt_0.55")
    if leader_concentration_share > 0.40:
        fail_reasons.append("leader_concentration_gt_0.40")
    if str(horizon) == "3m":
        sector_rs_vs_topix_1m = float(row.get("sector_rs_vs_topix_1m", 0.0) or 0.0)
        if sector_rs_vs_topix_1m <= -1.0:
            fail_reasons.append("1m_confirmation_broken")
    gate_pass = not fail_reasons
    return {
        "sector_gate_pass": gate_pass,
        "sector_gate_fail_reason": "|".join(fail_reasons),
        "sector_display_eligible": gate_pass,
    }


def _build_persistence_core_representatives(
    sector_names: pd.Series,
    display_base_df: pd.DataFrame,
    *,
    horizon: str,
) -> pd.DataFrame:
    result = pd.DataFrame({"sector_name": pd.Series(sector_names, dtype=str).fillna("").astype(str)})
    result = result[result["sector_name"].str.strip() != ""].drop_duplicates("sector_name").reset_index(drop=True)
    result["core_representatives"] = ""
    result["core_representatives_count"] = 0
    result["core_representatives_reason"] = ""
    result["representative_stocks"] = [[] for _ in range(len(result))]
    if result.empty:
        return result
    horizon_config = {
        "1w": {"required": ["rs_vs_topix_1w", "ret_1w"], "sort_columns": ["rs_vs_topix_1w", "ret_1w", "avg_turnover_20d", "TradingValue_latest", "name"]},
        "1m": {"required": ["rs_vs_topix_1m", "ret_1m", "rs_vs_topix_3m"], "sort_columns": ["rs_vs_topix_1m", "ret_1m", "rs_vs_topix_3m", "avg_turnover_20d", "TradingValue_latest", "name"]},
        "3m": {"required": ["rs_vs_topix_3m", "ret_3m"], "sort_columns": ["rs_vs_topix_3m", "ret_3m", "avg_turnover_20d", "TradingValue_latest", "name"]},
    }.get(str(horizon), {"required": [], "sort_columns": ["avg_turnover_20d", "TradingValue_latest", "name"]})
    required_columns = ["sector_name", "name", "avg_turnover_20d", "TradingValue_latest"] + list(horizon_config["required"])
    missing_columns = [column for column in required_columns if column not in display_base_df.columns]
    if missing_columns:
        reason = f"missing_columns:{','.join(missing_columns)}"
        result["core_representatives_reason"] = reason
        return result
    raw_working = display_base_df.copy()
    raw_working["sector_name"] = raw_working["sector_name"].fillna("").astype(str)
    raw_working["name"] = raw_working["name"].fillna("").astype(str).str.strip()
    numeric_columns = ["avg_turnover_20d", "TradingValue_latest"] + list(horizon_config["required"])
    for column in numeric_columns:
        raw_working[column] = _coerce_numeric(raw_working[column])
    if "code" in raw_working.columns:
        raw_working["code"] = raw_working["code"].fillna("").astype(str)
    working = _exclude_non_corporate_products(raw_working, context=f"persistence_core_representatives_{horizon}")
    available_metadata_columns = [
        column
        for column in [
            "instrument_type",
            "product_category",
            "listing_category",
            "underlying_index",
            "market_code",
            "exchange_name",
            "sector_code",
        ]
        if column in raw_working.columns
    ]
    metadata_non_equity_pattern = re.compile(
        r"ETF|ETN|ETP|REIT|リート|投資法人|不動産投資法人|TRUST|信託|投信|FUND|受益証券|INDEX|指数|連動|COMMODITY|商品|"
        r"BOND|債券|国債|米国債|社債|TREASURY|SPDR|ISHARES|NEXT\s*FUNDS|MAXIS|TRACERS|ONE\s*ETF|"
        r"レバレッジ|ブル|ベア|インバース|ダブルインバース",
        re.IGNORECASE,
    )
    name_non_equity_pattern = re.compile(
        r"ETF|ETN|ETP|REIT|リート|投資法人|不動産投資法人|上場投信|上場信託|投資信託|受益証券|指数連動|商品連動|商品指数|"
        r"債券|国債|米国債|社債|純金|純銀|純プラチナ|金価格|銀価格|プラチナ価格|ゴールド.?シェア|シルバー.?シェア|"
        r"プラチナ.?シェア|SPDR|ISHARES|I.?シェアーズ|NEXT\s*FUNDS|MAXIS|TRACERS|ONE\s*ETF|上場TRACER|"
        r"ブル|ベア|レバレッジ|インバース|ダブルインバース",
        re.IGNORECASE,
    )

    def _is_non_equity_candidate(row: pd.Series) -> bool:
        non_corporate_flag = row.get("is_non_corporate_product", False)
        if pd.notna(non_corporate_flag) and bool(non_corporate_flag):
            return True
        non_corporate_reason = str(row.get("non_corporate_product_reason", "") or "").strip()
        if non_corporate_reason:
            return True
        if _classify_non_corporate_product_row(row):
            return True
        metadata_text = _pick_first_non_empty_text(*(row.get(column, "") for column in available_metadata_columns))
        if metadata_text and metadata_non_equity_pattern.search(metadata_text):
            return True
        normalized_name = _normalize_security_text(row.get("name", ""))
        if normalized_name and name_non_equity_pattern.search(normalized_name):
            return True
        return False

    target_sector_names = set(result["sector_name"].tolist())
    for sector_key in result["sector_name"].tolist():
        sector_key = str(sector_key or "").strip()
        if sector_key == "" or sector_key not in target_sector_names:
            continue
        sector_indices = result.index[result["sector_name"].eq(sector_key)].tolist()
        if not sector_indices:
            continue
        sector_index = sector_indices[0]
        raw_group = raw_working[raw_working["sector_name"].eq(sector_key)].copy()
        group = working[working["sector_name"].eq(sector_key)].copy()
        if group.empty:
            sector_reason: list[str] = []
            if raw_group.empty:
                sector_reason.append("no_sector_rows")
            else:
                if raw_group["name"].fillna("").astype(str).str.strip().eq("").all():
                    sector_reason.append("blank_name_only")
                annotated_raw_group = _annotate_non_corporate_products(raw_group)
                if not annotated_raw_group.empty and annotated_raw_group.apply(_is_non_equity_candidate, axis=1).all():
                    sector_reason.append("non_corporate_products_only")
                for column in numeric_columns:
                    if column == "avg_turnover_20d":
                        if not _coerce_numeric(raw_group[column]).fillna(0.0).gt(0.0).any():
                            sector_reason.append("avg_turnover_20d_unavailable")
                    elif not _coerce_numeric(raw_group[column]).notna().any():
                        sector_reason.append(f"{column}_missing")
            if not sector_reason:
                sector_reason.append("no_eligible_candidates")
            result.at[sector_index, "core_representatives_reason"] = "|".join(dict.fromkeys(sector_reason))
            continue
        eligible = group[group["name"].ne("")].copy()
        for column in numeric_columns:
            if column == "avg_turnover_20d":
                eligible = eligible[eligible[column].gt(0.0)]
            else:
                eligible = eligible[eligible[column].notna()]
        eligible = eligible[~eligible.apply(_is_non_equity_candidate, axis=1)].copy()
        if "code" in eligible.columns:
            eligible = eligible.drop_duplicates("code")
        else:
            eligible = eligible.drop_duplicates("name")
        if eligible.empty:
            sector_reason: list[str] = []
            if raw_group["name"].fillna("").astype(str).str.strip().eq("").all():
                sector_reason.append("blank_name_only")
            elif _annotate_non_corporate_products(raw_group).apply(_is_non_equity_candidate, axis=1).all():
                sector_reason.append("non_corporate_products_only")
            for column in numeric_columns:
                if column == "avg_turnover_20d":
                    if not _coerce_numeric(raw_group[column]).fillna(0.0).gt(0.0).any():
                        sector_reason.append("avg_turnover_20d_unavailable")
                elif not _coerce_numeric(raw_group[column]).notna().any():
                    sector_reason.append(f"{column}_missing")
            if not sector_reason:
                sector_reason.append("no_eligible_candidates")
            result.at[sector_index, "core_representatives_reason"] = "|".join(sector_reason)
            continue
        before_horizon_gate_count = len(eligible)
        eligible = _apply_horizon_representative_gate(eligible, horizon=horizon)
        if eligible.empty:
            result.at[sector_index, "core_representatives_reason"] = f"horizon_gate_no_valid_candidates:{before_horizon_gate_count}"
            continue
        sorted_eligible = eligible.sort_values(
            horizon_config["sort_columns"],
            ascending=[False] * (len(horizon_config["sort_columns"]) - 1) + [True],
            kind="mergesort",
        )
        chosen_rows: list[pd.Series] = []
        for _, candidate_row in sorted_eligible.iterrows():
            if _is_non_equity_candidate(candidate_row):
                continue
            chosen_rows.append(candidate_row)
            if len(chosen_rows) >= 3:
                break
        chosen = pd.DataFrame(chosen_rows)
        if chosen.empty:
            result.at[sector_index, "core_representatives_reason"] = "non_corporate_products_only"
            continue
        chosen_names = chosen["name"].astype(str).tolist()
        representative_records: list[dict[str, Any]] = []
        for representative_rank, (_, chosen_row) in enumerate(chosen.head(3).iterrows(), start=1):
            record_code = _normalize_security_code(chosen_row.get("code", ""))
            record_name = str(chosen_row.get("name", "") or "").strip()
            record_date = _normalize_iso_date_text(chosen_row.get("earnings_announcement_date"))
            representative_records.append(
                {
                    "horizon": str(horizon),
                    "sector_name": sector_key,
                    "representative_rank": int(representative_rank),
                    "code": record_code,
                    "name": record_name,
                    "earnings_announcement_date": record_date,
                    "nikkei_search": _make_nikkei_search_link(record_name, record_code),
                }
            )
        result.at[sector_index, "core_representatives"] = " / ".join(chosen_names)
        result.at[sector_index, "core_representatives_count"] = int(len(chosen_names))
        result.at[sector_index, "representative_stocks"] = representative_records
        if len(chosen_names) < 3:
            result.at[sector_index, "core_representatives_reason"] = f"insufficient_candidates:{len(chosen_names)}"
    return result


def _build_intraday_sector_leaderboard(
    mode: str,
    ranking_df: pd.DataFrame,
    industry_df: pd.DataFrame,
    merged: pd.DataFrame,
    base_df: pd.DataFrame,
    *,
    block_weights: dict[str, dict[str, float]] | None = None,
    breadth_settings_map: dict[str, dict[str, float]] | None = None,
    concentration_settings_map: dict[str, dict[str, float]] | None = None,
) -> pd.DataFrame:
    ranking_df = _ensure_scan_source_type(ranking_df)
    if industry_df.empty or "sector_name" not in industry_df.columns:
        return _empty_sector_leaderboard()
    block_weights = block_weights or INTRADAY_BLOCK_MODE_WEIGHTS
    breadth_settings_map = breadth_settings_map or INTRADAY_BREADTH_SLOT_SETTINGS
    concentration_settings_map = concentration_settings_map or INTRADAY_CONCENTRATION_PENALTY_SETTINGS
    breadth_settings = breadth_settings_map.get(str(mode), breadth_settings_map["now"])
    concentration_settings = concentration_settings_map.get(str(mode), concentration_settings_map["now"])
    del concentration_settings
    industry_base = industry_df[
        [column for column in ["sector_name", "rank_position", "industry_up_value"] if column in industry_df.columns]
    ].copy()
    industry_base["original_sector_name"] = industry_base.get("sector_name", pd.Series(dtype=str)).astype(str).map(str.strip)
    industry_base["sector_name"] = industry_base["original_sector_name"].map(_normalize_industry_name)
    industry_base["normalized_sector_name"] = industry_base["original_sector_name"].map(_normalize_industry_key)
    industry_base = industry_base[industry_base["normalized_sector_name"].astype(str).str.strip() != ""].drop_duplicates("normalized_sector_name").copy()
    if industry_base.empty:
        return _empty_sector_leaderboard()
    if "rank_position" not in industry_base.columns:
        industry_base["rank_position"] = pd.Series(range(1, len(industry_base) + 1), index=industry_base.index, dtype="int64")
    if "industry_up_value" not in industry_base.columns:
        industry_base["industry_up_value"] = pd.NA
    sector_inventory = base_df.copy()
    if "sector_name" not in sector_inventory.columns:
        sector_inventory["sector_name"] = ""
    sector_inventory["original_sector_name"] = sector_inventory.get("sector_name", pd.Series(dtype=str)).astype(str).map(str.strip)
    sector_inventory["normalized_sector_name"] = sector_inventory["original_sector_name"].map(_normalize_industry_key)
    sector_inventory = sector_inventory[sector_inventory["normalized_sector_name"].astype(str).str.strip() != ""].copy()
    if "sector_constituent_count" not in sector_inventory.columns:
        sector_inventory_raw = sector_inventory.groupby("original_sector_name", as_index=False).agg(sector_constituent_count_raw=("code", "nunique"))
        sector_inventory_norm = sector_inventory.groupby("normalized_sector_name", as_index=False).agg(sector_constituent_count_after_normalization=("code", "nunique"))
    else:
        sector_inventory_raw = sector_inventory.groupby("original_sector_name", as_index=False).agg(sector_constituent_count_raw=("sector_constituent_count", "max"))
        sector_inventory_norm = sector_inventory.groupby("normalized_sector_name", as_index=False).agg(sector_constituent_count_after_normalization=("sector_constituent_count", "max"))
    sector_base = industry_base.rename(columns={"rank_position": "industry_rank_live"}).merge(
        sector_inventory_norm,
        on="normalized_sector_name",
        how="left",
    )
    sector_base = sector_base.merge(
        sector_inventory_raw,
        on="original_sector_name",
        how="left",
    )
    sector_base["industry_rank_live"] = _coerce_numeric(sector_base["industry_rank_live"]).fillna(pd.Series(range(1, len(sector_base) + 1), index=sector_base.index, dtype="float64"))
    sector_base["original_industry_rank_live"] = sector_base["industry_rank_live"]
    sector_base["industry_anchor_rank"] = sector_base["original_industry_rank_live"]
    sector_base["anchor_rank_source"] = "industry_up.rank_position"
    sector_base["present_in_live_industry_table"] = True
    sector_base["present_in_sector_summary_before_filter"] = True
    sector_base["present_in_today_display_universe"] = False
    sector_base["display_eligible"] = True
    sector_base["display_excluded_reason"] = ""
    market_scan_member_count = 0.0
    ranking_union_total_count = 0.0
    source_totals: dict[str, float] = {}
    sector_basket_counts: dict[str, int] = {}
    scan = pd.DataFrame()
    if not ranking_df.empty:
        scan = ranking_df.merge(
            base_df[["code", "name", "sector_name", "sector_constituent_count"]],
            on="code",
            how="left",
            suffixes=("", "_base"),
        )
        scan["original_sector_name"] = scan["sector_name"].fillna(scan.get("sector_name_base", "")).fillna("").astype(str).map(str.strip)
        scan["sector_name"] = scan["original_sector_name"].map(_normalize_industry_name)
        scan["normalized_sector_name"] = scan["original_sector_name"].map(_normalize_industry_key)
        scan = scan[scan["normalized_sector_name"].astype(str).str.strip() != ""].copy()
        if not scan.empty:
            market_scan_member_count = float(scan["code"].nunique() or 0.0)
            ranking_union_total_count = float(
                scan[scan.get("ranking_union_member", pd.Series(False, index=scan.index)).fillna(False)]["code"].astype(str).drop_duplicates().shape[0]
            ) if "code" in scan.columns else 0.0
            sector_basket_counts = (
                scan[scan.get("industry_basket_member", pd.Series(False, index=scan.index)).fillna(False)]
                .groupby("normalized_sector_name")["code"]
                .nunique()
                .to_dict()
            ) if "code" in scan.columns else {}
            ranking_only_scan = scan[scan.get("source_type", pd.Series(dtype=str)).astype(str).ne("industry_basket")].copy()
            source_totals = {str(key): float(value or 0.0) for key, value in ranking_only_scan.groupby("source_type")["code"].nunique().to_dict().items()}
            source_counts = (
                ranking_only_scan.groupby(["normalized_sector_name", "source_type"])["code"]
                .nunique()
                .unstack(fill_value=0)
                .reset_index()
            ) if not ranking_only_scan.empty else pd.DataFrame(columns=["normalized_sector_name"])
            member_rows: list[dict[str, Any]] = []
            for normalized_sector_name, group in scan.groupby("normalized_sector_name", dropna=False):
                confirmed_mask = group.get("ranking_union_member", pd.Series(False, index=group.index)).fillna(False)
                basket_mask = group.get("industry_basket_member", pd.Series(False, index=group.index)).fillna(False)
                member_rows.append(
                    {
                        "normalized_sector_name": str(normalized_sector_name or ""),
                        "wide_scan_member_count": int(group["code"].astype(str).drop_duplicates().shape[0]),
                        "ranking_confirmed_count": int(group.loc[confirmed_mask, "code"].astype(str).drop_duplicates().shape[0]),
                        "basket_member_count": int(group.loc[basket_mask, "code"].astype(str).drop_duplicates().shape[0]),
                        "sector_constituent_count_scan": float(_coerce_numeric(group.get("sector_constituent_count", pd.Series(dtype="float64"))).max(skipna=True) or 0.0),
                        "wide_scan_member_codes": _sorted_unique_codes(group["code"]),
                        "ranking_confirmed_codes": _sorted_unique_codes(group.loc[confirmed_mask, "code"]),
                    }
                )
            scan_sector_base = pd.DataFrame(member_rows).merge(source_counts, on="normalized_sector_name", how="left")
            for column in ["price_up", "turnover", "volume_surge", "turnover_surge"]:
                if column not in scan_sector_base.columns:
                    scan_sector_base[column] = 0
            scan_sector_base = scan_sector_base.rename(
                columns={
                    "price_up": "price_up_count",
                    "turnover": "turnover_count",
                    "volume_surge": "volume_surge_count",
                    "turnover_surge": "turnover_surge_count",
                }
            )
            sector_base = sector_base.merge(scan_sector_base, on="normalized_sector_name", how="left")
    if ranking_union_total_count <= 0.0 and not ranking_df.empty and "ranking_union_member" in ranking_df.columns and "code" in ranking_df.columns:
        ranking_union_total_count = float(
            ranking_df[ranking_df["ranking_union_member"].fillna(False)]["code"].astype(str).drop_duplicates().shape[0]
        )
    for column in [
        "wide_scan_member_count",
        "ranking_confirmed_count",
        "basket_member_count",
        "price_up_count",
        "turnover_count",
        "volume_surge_count",
        "turnover_surge_count",
        "sector_constituent_count_raw",
        "sector_constituent_count_after_normalization",
    ]:
        if column not in sector_base.columns:
            sector_base[column] = 0.0
        sector_base[column] = _coerce_numeric(sector_base[column]).fillna(0.0)
    for column in ["wide_scan_member_codes", "ranking_confirmed_codes"]:
        if column not in sector_base.columns:
            sector_base[column] = [[] for _ in range(len(sector_base))]
        sector_base[column] = sector_base[column].apply(lambda value: value if isinstance(value, list) else [])
    scan_source_columns = ["price_up_count", "turnover_count", "volume_surge_count", "turnover_surge_count"]
    market_scan_source_count = float(sum(1 for source_type in ["price_up", "turnover", "volume_surge", "turnover_surge"] if source_totals.get(source_type, 0.0) > 0.0) or 1.0)
    sector_base["ranking_source_breadth_ex_basket"] = 0.0
    for column in scan_source_columns:
        sector_base["ranking_source_breadth_ex_basket"] += _coerce_numeric(sector_base[column]).fillna(0.0).gt(0).astype(float)
    sector_base["signal_breadth_count"] = sector_base["ranking_source_breadth_ex_basket"]
    sector_base["signal_breadth_share"] = _safe_ratio(
        sector_base["ranking_source_breadth_ex_basket"],
        pd.Series([market_scan_source_count] * len(sector_base), index=sector_base.index),
    ).fillna(0.0)
    sector_base["max_scan_source_count"] = _coerce_numeric(sector_base[scan_source_columns].max(axis=1)).fillna(0.0)
    sector_base["breadth_up"] = _coerce_numeric(sector_base["price_up_count"]).fillna(0.0)
    sector_base["breadth_down"] = 0.0
    if "sector_constituent_count_scan" in sector_base.columns:
        sector_base["sector_constituent_count"] = _coerce_numeric(sector_base.get("sector_constituent_count_after_normalization", pd.Series(dtype="float64"))).fillna(_coerce_numeric(sector_base["sector_constituent_count_scan"]))
    else:
        sector_base["sector_constituent_count"] = _coerce_numeric(sector_base.get("sector_constituent_count_after_normalization", pd.Series(dtype="float64")))
    sector_base["sector_constituent_count"] = _coerce_numeric(sector_base["sector_constituent_count"]).fillna(sector_base["wide_scan_member_count"]).clip(lower=1.0)
    sector_live_aggregate_frame, sector_live_aggregate_meta = _build_sector_live_aggregate_frame(merged)
    sector_base, sector_live_aggregate_audit = _apply_sector_live_aggregates(
        sector_base,
        sector_live_aggregate_frame,
        sector_key_col="normalized_sector_name",
        source_meta=sector_live_aggregate_meta,
    )
    sector_base = sector_base.sort_values(["original_industry_rank_live", "sector_name"], ascending=[True, True], kind="mergesort").reset_index(drop=True)
    sector_base["wide_scan_member_count"] = _coerce_numeric(sector_base["wide_scan_member_count"]).fillna(0.0)
    sector_base["scan_member_count"] = sector_base["wide_scan_member_count"]
    sector_base["wide_scan_coverage"] = _safe_ratio(sector_base["wide_scan_member_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["scan_coverage"] = sector_base["wide_scan_coverage"]
    sector_base["ranking_confirmed_share_of_sector"] = _safe_ratio(sector_base["ranking_confirmed_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["ranking_confirmed_coverage"] = sector_base["ranking_confirmed_share_of_sector"]
    sector_base["ranking_confirmed_share_of_market"] = _safe_ratio(
        sector_base["ranking_confirmed_count"],
        pd.Series([ranking_union_total_count] * len(sector_base), index=sector_base.index),
    ).fillna(0.0)
    sector_base["price_up_share_of_sector"] = _safe_ratio(sector_base["price_up_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["price_up_share_of_market_scan"] = _safe_ratio(sector_base["price_up_count"], pd.Series([source_totals.get("price_up", 0.0)] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["turnover_share_of_sector"] = _safe_ratio(sector_base["turnover_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["turnover_share_of_market_scan"] = _safe_ratio(sector_base["turnover_count"], pd.Series([source_totals.get("turnover", 0.0)] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["volume_surge_share_of_sector"] = _safe_ratio(sector_base["volume_surge_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["volume_surge_share_of_market_scan"] = _safe_ratio(sector_base["volume_surge_count"], pd.Series([source_totals.get("volume_surge", 0.0)] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["turnover_surge_share_of_sector"] = _safe_ratio(sector_base["turnover_surge_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["turnover_surge_share_of_market_scan"] = _safe_ratio(sector_base["turnover_surge_count"], pd.Series([source_totals.get("turnover_surge", 0.0)] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["breadth_up_rate"] = sector_base["price_up_share_of_sector"]
    sector_base["breadth_down_rate"] = 0.0
    sector_base["breadth_balance"] = sector_base["breadth_up_rate"] - sector_base["breadth_down_rate"]
    sector_base["breadth_net_rate"] = sector_base["breadth_balance"]
    sector_base["breadth_sample_count"] = _coerce_numeric(sector_base["ranking_confirmed_count"]).fillna(0.0)
    sector_base["breadth_active_coverage"] = sector_base["ranking_confirmed_coverage"]
    sector_base["breadth_reliability"] = _safe_ratio(
        sector_base["breadth_sample_count"],
        pd.Series([float(breadth_settings["reliability_k"])] * len(sector_base), index=sector_base.index),
    ).fillna(0.0).clip(lower=0.0, upper=1.0)
    sector_base["breadth"] = sector_base.apply(lambda row: f"{int(row.get('ranking_source_breadth_ex_basket', 0) or 0)}src/{int(row.get('ranking_confirmed_count', 0) or 0)}rk", axis=1)
    sector_base["median_ret"] = _coerce_numeric(sector_base["median_live_ret"]).where(sector_base["live_aggregate_use_for_score"].fillna(False).astype(bool), pd.NA)
    sector_base["scan_member_share_of_market_scan"] = _safe_ratio(sector_base["scan_member_count"], pd.Series([market_scan_member_count] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["scan_member_count_norm"] = _score_percentile(sector_base["scan_member_count"])
    sector_base["wide_scan_member_count_norm"] = sector_base["scan_member_count_norm"]
    sector_base["ranking_confirmed_count_norm"] = _score_percentile(sector_base["ranking_confirmed_count"])
    sector_base["ranking_source_breadth_ex_basket_norm"] = _safe_ratio(
        sector_base["ranking_source_breadth_ex_basket"],
        pd.Series([market_scan_source_count] * len(sector_base), index=sector_base.index),
    ).fillna(0.0)
    sector_base["price_up_count_norm"] = _score_percentile(sector_base["price_up_count"])
    sector_base["turnover_count_norm"] = _score_percentile(sector_base["turnover_count"])
    sector_base["volume_surge_count_norm"] = _score_percentile(sector_base["volume_surge_count"])
    sector_base["turnover_surge_count_norm"] = _score_percentile(sector_base["turnover_surge_count"])
    sector_base["source_bias_share"] = _safe_ratio(sector_base["max_scan_source_count"], sector_base["ranking_confirmed_count"]).fillna(0.0)
    sector_base["industry_up_rank_norm"] = _score_rank_ascending(sector_base["industry_rank_live"])
    sector_base["leader_concentration_share"] = 0.0
    sector_base["price_up_rate"] = sector_base["price_up_share_of_sector"]
    sector_base["turnover_count_rate"] = sector_base["turnover_share_of_sector"]
    sector_base["volume_surge_rate"] = sector_base["volume_surge_share_of_sector"]
    sector_base["turnover_surge_rate"] = sector_base["turnover_surge_share_of_sector"]
    sector_base["scan_participation_rate"] = sector_base["ranking_confirmed_coverage"]
    sector_base["n"] = sector_base["wide_scan_member_count"].fillna(0.0).astype(int)
    sector_base["price_block_score"] = 0.0
    for column, weight in INTRADAY_BLOCK_COMPONENT_WEIGHTS["price"].items():
        sector_base["price_block_score"] += _coerce_numeric(sector_base[column]).fillna(0.0) * weight
    sector_base["flow_block_score"] = 0.0
    for column, weight in INTRADAY_BLOCK_COMPONENT_WEIGHTS["flow"].items():
        sector_base["flow_block_score"] += _coerce_numeric(sector_base[column]).fillna(0.0) * weight
    sector_base["breadth_core_score"] = 0.0
    for column, weight in INTRADAY_BLOCK_COMPONENT_WEIGHTS["participation"].items():
        sector_base["breadth_core_score"] += _coerce_numeric(sector_base[column]).fillna(0.0) * weight
    sector_base["participation_block_score_raw"] = sector_base["breadth_core_score"]
    sector_base["participation_block_score"] = sector_base["participation_block_score_raw"]
    mode_block_weights = block_weights.get(str(mode), block_weights["now"])
    sector_base["intraday_sector_score_raw"] = (
        sector_base["price_block_score"] * mode_block_weights["price"]
        + sector_base["flow_block_score"] * mode_block_weights["flow"]
        + sector_base["participation_block_score"] * mode_block_weights["participation"]
    )
    sector_base["intraday_total_score"] = sector_base["intraday_sector_score_raw"]
    weak_up = sector_base["ranking_source_breadth_ex_basket"] < 2.0
    weak_balance = sector_base["ranking_confirmed_coverage"] < 0.10
    weak_sample = sector_base["ranking_confirmed_count"] < 5.0
    penalty_hits = weak_up.astype(int) + weak_balance.astype(int) + weak_sample.astype(int)
    sector_base["breadth_penalty_flag"] = penalty_hits.gt(0)
    sector_base["breadth_warning_flag"] = sector_base["breadth_penalty_flag"]
    sector_base["breadth_penalty"] = 0.0
    sector_base["concentration_penalty"] = 0.0
    sector_base["intraday_penalty_total"] = 0.0
    sector_base["intraday_sector_score"] = sector_base["intraday_sector_score_raw"]
    sector_base["today_sector_score"] = sector_base["intraday_sector_score"]
    price_strong_cutoff = _coerce_numeric(sector_base["price_block_score"]).quantile(0.75)
    flow_strong_cutoff = _coerce_numeric(sector_base["flow_block_score"]).quantile(0.75)
    price_weak_cutoff = _coerce_numeric(sector_base["price_block_score"]).quantile(0.35)
    flow_weak_cutoff = _coerce_numeric(sector_base["flow_block_score"]).quantile(0.35)
    if pd.isna(price_strong_cutoff):
        price_strong_cutoff = 0.0
    if pd.isna(flow_strong_cutoff):
        flow_strong_cutoff = 0.0
    if pd.isna(price_weak_cutoff):
        price_weak_cutoff = 0.0
    if pd.isna(flow_weak_cutoff):
        flow_weak_cutoff = 0.0
    strong_price_signal = (
        (_coerce_numeric(sector_base["price_block_score"]).fillna(0.0) > 0.0)
        & (_coerce_numeric(sector_base["price_block_score"]).fillna(0.0) >= float(price_strong_cutoff))
        & (_coerce_numeric(sector_base["industry_up_rank_norm"]).fillna(0.0) >= 0.55)
    )
    strong_flow_signal = (
        (_coerce_numeric(sector_base["flow_block_score"]).fillna(0.0) > 0.0)
        & (_coerce_numeric(sector_base["flow_block_score"]).fillna(0.0) >= float(flow_strong_cutoff))
    )
    weak_price_signal = _coerce_numeric(sector_base["price_block_score"]).fillna(0.0) <= float(price_weak_cutoff)
    weak_flow_signal = _coerce_numeric(sector_base["flow_block_score"]).fillna(0.0) <= float(flow_weak_cutoff)
    sector_base["rank_shift_limit"] = float(INTRADAY_INDUSTRY_RANK_TETHER["base_shift"])
    sector_base["sector_confidence_score"] = 0.0
    sector_base.loc[sector_base["ranking_confirmed_count"] >= 5, "sector_confidence_score"] += 1.0
    sector_base.loc[sector_base["ranking_confirmed_coverage"] >= 0.12, "sector_confidence_score"] += 0.75
    sector_base.loc[sector_base["ranking_source_breadth_ex_basket"] >= 2, "sector_confidence_score"] += 0.75
    sector_base.loc[sector_base["ranking_source_breadth_ex_basket"] >= 3, "sector_confidence_score"] += 0.5
    sector_base.loc[strong_price_signal, "sector_confidence_score"] += 0.5
    sector_base.loc[strong_flow_signal, "sector_confidence_score"] += 0.5
    sector_base.loc[sector_base["industry_up_rank_norm"] >= 0.75, "sector_confidence_score"] += 0.5
    sector_base["sector_confidence"] = sector_base["sector_confidence_score"].apply(_build_sector_confidence)
    scan_sample_warning_level, scan_sample_warning_label, scan_sample_warning_reason = _scan_sample_warning_details(
        sector_base["wide_scan_member_count"],
        sector_base["wide_scan_coverage"],
    )
    sector_base["scan_sample_warning_level"] = scan_sample_warning_level
    sector_base["scan_sample_warning_flag"] = sector_base["scan_sample_warning_level"].astype(str).str.strip() != ""
    sector_base["scan_sample_warning_reason"] = scan_sample_warning_reason
    sector_base["ranking_breadth_warning_flag"] = _coerce_numeric(sector_base["ranking_source_breadth_ex_basket"]).fillna(0.0) <= 1.0
    sector_base["source_bias_warning_flag"] = _coerce_numeric(sector_base["ranking_source_breadth_ex_basket"]).fillna(0.0) == 1.0
    sector_base["severe_caution_flag"] = (
        (_coerce_numeric(sector_base["ranking_confirmed_count"]).fillna(0.0) <= 2.0)
        | (_coerce_numeric(sector_base["wide_scan_member_count"]).fillna(0.0) < float(WIDE_SCAN_BASKET_MIN_PER_SECTOR))
    )
    market_scan_quality = _summarize_market_scan_quality(
        scan_df=scan,
        sector_frame=sector_base,
        ranking_union_count=int(ranking_union_total_count or 0),
        sector_basket_counts={str(key): int(value or 0) for key, value in sector_basket_counts.items()},
    )
    sector_base["today_rank_mode"] = str(market_scan_quality["mode"])
    sector_base["rank_mode_reason"] = str(market_scan_quality["reason"])
    sector_base["market_scan_quality_summary"] = str(market_scan_quality["summary"])
    sector_base["max_upshift"] = 0
    sector_base["max_downshift"] = 0
    if str(market_scan_quality["mode"]) == "anchored_overlay":
        sector_base.loc[
            (_coerce_numeric(sector_base["ranking_confirmed_count"]).fillna(0.0) <= 2.0)
            | weak_price_signal
            | weak_flow_signal,
            "max_downshift",
        ] = 1
        sector_base.loc[
            (_coerce_numeric(sector_base["ranking_confirmed_count"]).fillna(0.0) == 0.0)
            | (weak_price_signal & weak_flow_signal),
            "max_downshift",
        ] = int(INTRADAY_INDUSTRY_RANK_TETHER["max_downshift"])
        plus_one_mask = (
            (_coerce_numeric(sector_base["ranking_confirmed_count"]).fillna(0.0) >= 5.0)
            & (_coerce_numeric(sector_base["ranking_source_breadth_ex_basket"]).fillna(0.0) >= 2.0)
            & strong_price_signal
        )
        plus_two_mask = (
            (_coerce_numeric(sector_base["ranking_confirmed_count"]).fillna(0.0) >= 8.0)
            & (_coerce_numeric(sector_base["ranking_source_breadth_ex_basket"]).fillna(0.0) >= 3.0)
            & strong_price_signal
            & strong_flow_signal
            & ~sector_base["severe_caution_flag"]
        )
        sector_base.loc[plus_one_mask, "max_upshift"] = int(INTRADAY_INDUSTRY_RANK_TETHER["strong_shift"])
        sector_base.loc[plus_two_mask, "max_upshift"] = int(INTRADAY_INDUSTRY_RANK_TETHER["very_strong_shift"])
    sector_base["rank_shift_limit"] = _coerce_numeric(sector_base["max_upshift"]).fillna(0.0)
    sector_base["sector_caution"] = sector_base.apply(
        lambda row: _build_sector_caution_tags(
            [
                "ランキング裏付けなし" if float(row.get("ranking_confirmed_count", 0.0) or 0.0) <= 0.0 else "",
                "ランキング裏付け極少" if 1.0 <= float(row.get("ranking_confirmed_count", 0.0) or 0.0) <= 2.0 else "",
                "ランキング裏付け薄い" if 3.0 <= float(row.get("ranking_confirmed_count", 0.0) or 0.0) <= 4.0 else "",
                "source偏りあり" if bool(row.get("source_bias_warning_flag")) else "",
                "wide scan母数不足" if float(row.get("wide_scan_member_count", 0.0) or 0.0) < float(WIDE_SCAN_BASKET_MIN_PER_SECTOR) else "",
                "業種順位先行" if float(row.get("industry_up_rank_norm", 0.0) or 0.0) >= 0.8 and float(row.get("max_upshift", 0.0) or 0.0) <= 0.0 else "",
            ]
        ),
        axis=1,
    )
    sector_base["present_in_today_display_universe"] = True
    sector_base = sector_base.sort_values(
        ["intraday_sector_score", "price_block_score", "flow_block_score", "participation_block_score"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    sector_base["score_rank"] = range(1, len(sector_base) + 1)
    sector_base["industry_up_rank"] = _rank_display_series(sector_base["industry_rank_live"], fallback=sector_base["score_rank"])
    sector_base["industry_up_anchor_rank"] = _rank_display_series(sector_base["industry_anchor_rank"], fallback=sector_base["industry_up_rank"])
    sector_base["industry_up"] = _coerce_numeric(sector_base["industry_up_value"]).fillna(_coerce_numeric(sector_base["industry_up_rank"]))
    sector_base["tethered_rank"] = _coerce_numeric(sector_base["industry_anchor_rank"]).fillna(0.0)
    anchor_only_mask = sector_base["today_rank_mode"].astype(str).eq("anchor_only")
    if anchor_only_mask.any():
        sector_base = sector_base.sort_values(["industry_anchor_rank", "sector_name"], ascending=[True, True], kind="mergesort").reset_index(drop=True)
        sector_base["score_rank"] = _coerce_numeric(sector_base["industry_anchor_rank"]).fillna(pd.Series(range(1, len(sector_base) + 1), index=sector_base.index, dtype="float64")).round().astype(int)
        sector_base["tethered_rank"] = _coerce_numeric(sector_base["industry_anchor_rank"]).fillna(0.0)
        sector_base["today_display_rank"] = _coerce_numeric(sector_base["original_industry_rank_live"]).fillna(_coerce_numeric(sector_base["industry_anchor_rank"])).round().clip(lower=1.0).astype("int64")
        sector_base["today_rank"] = sector_base["today_display_rank"]
        sector_base["final_rank_delta"] = 0.0
        sector_base["max_upshift"] = 0
        sector_base["max_downshift"] = 0
        sector_base["allowed_shift"] = 0
        sector_base["rank_shift_limit"] = 0
        sector_base["rank_constraint_applied"] = False
        sector_base["upshift_blocked_reason"] = ""
        result = _sort_today_sector_leaderboard_for_display(sector_base)
        result.attrs["market_scan_quality"] = dict(market_scan_quality)
        result.attrs["sector_live_aggregate_audit"] = sector_live_aggregate_audit
        result.attrs["sector_live_aggregate_source_meta"] = sector_live_aggregate_meta
        return result
    sector_base = _apply_true_rank_shift_limits(sector_base)
    result = _sort_today_sector_leaderboard_for_display(sector_base)
    result.attrs["market_scan_quality"] = dict(market_scan_quality)
    result.attrs["sector_live_aggregate_audit"] = sector_live_aggregate_audit
    result.attrs["sector_live_aggregate_source_meta"] = sector_live_aggregate_meta
    return result


def _build_sector_persistence_tables(base_df: pd.DataFrame, *, display_base_df: pd.DataFrame | None = None) -> dict[str, pd.DataFrame]:
    if base_df.empty:
        empty = _empty_persistence_table()
        return {"1w": empty, "1m": empty, "3m": empty}
    display_base_df = display_base_df if display_base_df is not None and not display_base_df.empty else base_df
    liquidity_by_sector = (
        base_df.groupby("sector_name", as_index=False)
        .agg(
            sector_trading_value_total=("TradingValue_latest", "sum"),
            sector_trading_value_top=("TradingValue_latest", "max"),
        )
    )
    representative = (
        display_base_df.sort_values(["sector_name", "TradingValue_latest"], ascending=[True, False])
        .groupby("sector_name")
        .first()
        .reset_index()[["sector_name", "name"]]
        .rename(columns={"name": "representative_stock"})
    )

    def _build(rs_label: str, rank_col: str) -> pd.DataFrame:
        rs_column = f"rs_vs_topix_{rs_label}"
        secondary_sort_columns = {
            "1w": ["persistence_rank", f"sector_rs_vs_topix_{rs_label}", "sector_positive_ratio", "leader_concentration_share"],
            "1m": ["persistence_rank", "sector_positive_ratio", "leader_concentration_share", f"sector_rs_vs_topix_{rs_label}"],
            "3m": ["persistence_rank", f"sector_rs_vs_topix_{rs_label}", "sector_rs_vs_topix_1m", "sector_positive_ratio"],
        }.get(rs_label, ["persistence_rank", f"sector_rs_vs_topix_{rs_label}"])
        secondary_sort_ascending = {
            "1w": [True, False, False, True],
            "1m": [True, False, True, False],
            "3m": [True, False, False, False],
        }.get(rs_label, [True, False])
        core_representatives = _build_persistence_core_representatives(
            base_df.get("sector_name", pd.Series(dtype=str)),
            display_base_df,
            horizon=rs_label,
        )
        frame = (
            base_df.groupby("sector_name", as_index=False)
            .agg(
                sector_constituent_count=("sector_constituent_count", "max"),
                sector_rs_vs_topix_1w=("sector_rs_vs_topix_1w", "median"),
                sector_rs_vs_topix_1m=("sector_rs_vs_topix_1m", "median"),
                sector_rs_vs_topix_3m=("sector_rs_vs_topix_3m", "median"),
                sector_positive_ratio=(rs_column, lambda s: float((_coerce_numeric(s) > 0).mean()) if len(s) else 0.0),
                persistence_rank=(rank_col, "median"),
            )
            .merge(representative, on="sector_name", how="left")
            .merge(liquidity_by_sector, on="sector_name", how="left")
            .merge(core_representatives, on="sector_name", how="left")
            .reset_index(drop=True)
        )
        frame["sector_rs_vs_topix"] = _coerce_numeric(frame[f"sector_rs_vs_topix_{rs_label}"])
        frame["leader_concentration_share"] = _safe_ratio(frame["sector_trading_value_top"], frame["sector_trading_value_total"]).fillna(0.0)
        frame["sector_confidence_score"] = 0.0
        frame.loc[frame["sector_constituent_count"] >= 6, "sector_confidence_score"] += 1.25
        frame.loc[frame["sector_positive_ratio"] >= 0.55, "sector_confidence_score"] += 1.0
        frame.loc[frame["leader_concentration_share"] <= 0.40, "sector_confidence_score"] += 0.75
        frame.loc[frame["sector_rs_vs_topix"].gt(0), "sector_confidence_score"] += 0.5
        frame["sector_confidence"] = frame["sector_confidence_score"].apply(_build_sector_confidence)
        frame["sector_caution"] = frame.apply(
            lambda row: _build_sector_caution_tags(
                [
                    "サンプル少" if float(row.get("sector_constituent_count", 0.0) or 0.0) < 4 else "",
                    "一部銘柄偏重" if float(row.get("leader_concentration_share", 0.0) or 0.0) > 0.55 else "",
                    "広がり弱い" if float(row.get("sector_positive_ratio", 0.0) or 0.0) < 0.45 else "",
                ]
            ),
            axis=1,
        )
        quality_payload = frame.apply(_build_persistence_quality_payload, axis=1, result_type="expand")
        frame["quality_pass"] = quality_payload["quality_pass"].fillna(False).astype(bool)
        frame["quality_warn"] = quality_payload["quality_warn"].fillna("").astype(str)
        frame["quality_fail_reason"] = quality_payload["quality_fail_reason"].fillna("").astype(str)
        gate_payload = frame.apply(lambda row: _build_persistence_gate_payload(row, horizon=rs_label), axis=1, result_type="expand")
        frame["sector_gate_pass"] = gate_payload["sector_gate_pass"].fillna(False).astype(bool)
        frame["sector_gate_fail_reason"] = gate_payload["sector_gate_fail_reason"].fillna("").astype(str)
        frame["sector_display_eligible"] = gate_payload["sector_display_eligible"].fillna(False).astype(bool)
        frame["core_representatives"] = frame.get("core_representatives", pd.Series("", index=frame.index)).fillna("").astype(str)
        frame["core_representatives_count"] = _coerce_numeric(frame.get("core_representatives_count", pd.Series(0, index=frame.index))).fillna(0).astype(int)
        frame["core_representatives_reason"] = frame.get("core_representatives_reason", pd.Series("", index=frame.index)).fillna("").astype(str)
        gate_warn = frame["sector_gate_fail_reason"].apply(_persistence_gate_fail_warn_label)
        frame.loc[frame["quality_warn"].eq("") & gate_warn.ne(""), "quality_warn"] = gate_warn[frame["quality_warn"].eq("") & gate_warn.ne("")]
        frame = frame.sort_values(secondary_sort_columns, ascending=secondary_sort_ascending, kind="mergesort").reset_index(drop=True)
        frame["persistence_rank"] = _coerce_numeric(frame["persistence_rank"]).round().astype("Int64")
        return frame

    return {"1w": _build("1w", "sector_rank_1w"), "1m": _build("1m", "sector_rank_1m"), "3m": _build("3m", "sector_rank_3m")}


def _resolve_persistence_representatives_for_storage(
    frame: pd.DataFrame,
    *,
    horizon: str,
    earnings_announcement_lookup: dict[str, str] | None = None,
    security_reference_lookup: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if frame is None or frame.empty:
        return frame, {
            "source": "missing",
            "resolved_count": 0,
            "unresolved_count": 0,
            "earnings_date_count": 0,
            "unresolved_samples": [],
        }
    working = frame.copy()
    if "representative_stocks" not in working.columns:
        working["representative_stocks"] = [[] for _ in range(len(working))]
    structured_sector_count = 0
    fallback_sector_count = 0
    resolved_count = 0
    unresolved_count = 0
    earnings_date_count = 0
    unresolved_samples: list[dict[str, str]] = []
    by_code = security_reference_lookup.get("by_code", {}) if isinstance(security_reference_lookup, dict) else {}
    for row_index, row in working.iterrows():
        sector_name = str(row.get("sector_name", "") or "").strip()
        raw_items = row.get("representative_stocks", [])
        representative_items: list[dict[str, Any]] = []
        if isinstance(raw_items, list):
            for item in raw_items[:3]:
                if isinstance(item, dict):
                    representative_items.append(dict(item))
        if representative_items:
            structured_sector_count += 1
        else:
            names = _split_ui_stock_names(row.get("core_representatives")) or _split_ui_stock_names(row.get("representative_stock"))
            representative_items = [{"name": name} for name in names[:3] if str(name or "").strip()]
            if representative_items:
                fallback_sector_count += 1
        if not representative_items:
            working.at[row_index, "representative_stocks"] = []
            continue
        reason_label = _persistence_core_representatives_reason_label(row.get("core_representatives_reason"))
        resolved_items: list[dict[str, Any]] = []
        for representative_rank, item in enumerate(representative_items[:3], start=1):
            raw_name = _clean_ui_value(item.get("name")) or _strip_embedded_security_codes(item.get("name"))
            raw_code = _normalize_security_code(item.get("code"))
            resolved_info = {}
            if raw_code and raw_code in by_code:
                resolved_info = by_code.get(raw_code, {}) or {}
            if not resolved_info and raw_name:
                resolved_info = _resolve_security_reference(
                    item.get("name", raw_name),
                    sector_name=sector_name,
                    security_reference_lookup=security_reference_lookup,
                )
            resolved_code = raw_code or _normalize_security_code(resolved_info.get("code"))
            resolved_name = raw_name or _clean_ui_value(resolved_info.get("name")) or _strip_embedded_security_codes(item.get("name"))
            earnings_date = (
                _normalize_iso_date_text(item.get("earnings_announcement_date"))
                or (earnings_announcement_lookup or {}).get(resolved_code, "")
                or _normalize_iso_date_text(resolved_info.get("earnings_announcement_date"))
            )
            representative_reason = _pick_first_non_empty_label(
                item.get("representative_reason"),
                item.get("center_note"),
                _build_persistence_representative_note(horizon, representative_rank, reason_label),
            )
            nikkei_search = str(item.get("nikkei_search", "") or "").strip() or _make_nikkei_search_link(resolved_name, resolved_code)
            if resolved_code:
                resolved_count += 1
            else:
                unresolved_count += 1
                if len(unresolved_samples) < 10:
                    unresolved_samples.append(
                        {
                            "sector_name": sector_name,
                            "name": resolved_name or _clean_ui_value(item.get("name")),
                            "reason": "code_unresolved_after_structured_and_name_lookup",
                        }
                    )
            if earnings_date:
                earnings_date_count += 1
            resolved_items.append(
                {
                    "horizon": str(horizon),
                    "sector_name": sector_name,
                    "representative_rank": int(representative_rank),
                    "code": resolved_code,
                    "name": resolved_name,
                    "representative_reason": representative_reason,
                    "center_note": representative_reason,
                    "earnings_announcement_date": earnings_date,
                    "nikkei_search": nikkei_search,
                }
            )
        working.at[row_index, "representative_stocks"] = resolved_items
        if resolved_items:
            names_joined = " / ".join([str(item.get("name", "") or "").strip() for item in resolved_items if str(item.get("name", "") or "").strip()][:3])
            if "core_representatives" in working.columns and not str(row.get("core_representatives", "") or "").strip():
                working.at[row_index, "core_representatives"] = names_joined
            if "representative_stock" in working.columns and not str(row.get("representative_stock", "") or "").strip():
                working.at[row_index, "representative_stock"] = str(resolved_items[0].get("name", "") or "")
            if "leaders" in working.columns and not str(row.get("leaders", "") or "").strip():
                working.at[row_index, "leaders"] = names_joined
    source_label = "structured" if structured_sector_count > 0 else ("name_fallback" if fallback_sector_count > 0 else "missing")
    return working, {
        "source": source_label,
        "structured_sector_count": int(structured_sector_count),
        "fallback_sector_count": int(fallback_sector_count),
        "resolved_count": int(resolved_count),
        "unresolved_count": int(unresolved_count),
        "earnings_date_count": int(earnings_date_count),
        "unresolved_samples": unresolved_samples,
    }


def _empty_sector_representatives_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "sector_name",
            "today_rank",
            "representative_rank",
            "code",
            "name",
            "live_price",
            "current_price",
            "current_price_unavailable",
            "live_ret_vs_prev_close",
            "live_turnover",
            "live_turnover_value",
            "live_turnover_unavailable",
            "stock_turnover_share_of_sector",
            "selected_horizon",
            "selected_universe",
            "primary_candidate_count",
            "supplemental_candidate_count",
            "final_candidate_count",
            "sector_constituent_count",
            "representative_pool_coverage_rate",
            "candidate_pool_warning",
            "candidate_pool_reason",
            "selected_from_primary_or_supplemental",
            "sector_candidate_count",
            "sector_positive_count",
            "sector_negative_count",
            "sector_positive_rate",
            "sector_median_return",
            "sector_top_quartile_return",
            "sector_bottom_quartile_return",
            "stock_return_percentile_in_sector",
            "stock_return_rank_in_sector",
            "market_positive_rate",
            "market_context",
            "sector_context",
            "sector_live_ret_median",
            "sector_top_positive_count",
            "representative_gate_pass",
            "representative_gate_reason",
            "hard_reject_reason",
            "hard_block_reason",
            "fallback_used",
            "fallback_reason",
            "fallback_blocked_reason",
            "live_ret_from_open",
            "sector_live_ret_pct",
            "sector_today_flow_pct",
            "sector_turnover_share",
            "exclude_spike",
            "exclude_spike_hard_reject",
            "exclude_spike_warning_only",
            "spike_quality",
            "poor_quality_spike",
            "material_supported_breakout",
            "breakout_support_reason",
            "centrality_score",
            "liquidity_score",
            "today_leadership_score",
            "representative_final_score",
            "selected_reason",
            "rep_score_today_strength",
            "rep_score_relative_strength",
            "rep_score_liquidity",
            "representative_score",
            "rep_score_total",
            "rep_score_centrality",
            "rep_score_today_leadership",
            "rep_score_sanity",
            "rep_selected_reason",
            "rep_excluded_reason",
            "rep_fallback_reason",
            "representative_selected_reason",
            "representative_quality_flag",
            "representative_fallback_reason",
            "earnings_today_announcement_flag",
            "earnings_announcement_date",
            "was_in_selected50",
            "was_in_must_have",
            "nikkei_search",
            "material_link",
        ]
    )


def _append_gate_reason(existing: Any, reason: str) -> str:
    parts = [part.strip() for part in str(existing or "").split("|") if part.strip()]
    if reason and reason not in parts:
        parts.append(reason)
    return "|".join(parts)


def _remove_gate_reason(existing: Any, reason: str) -> str:
    return "|".join(part for part in [p.strip() for p in str(existing or "").split("|")] if part and part != reason)


def _apply_today_representative_gate(working: pd.DataFrame) -> pd.DataFrame:
    if working is None or working.empty:
        return working
    result = working.copy()
    if "rep_hard_block" not in result.columns:
        result["rep_hard_block"] = False
    if "rep_excluded_reason" not in result.columns:
        result["rep_excluded_reason"] = ""
    result["rep_hard_block"] = result["rep_hard_block"].fillna(False).astype(bool)
    result["rep_excluded_reason"] = result["rep_excluded_reason"].fillna("").astype(str)
    result["hard_block_reason"] = result.get("hard_block_reason", pd.Series("", index=result.index)).fillna("").astype(str)

    live_ret = _coerce_numeric(result.get("live_ret_vs_prev_close", pd.Series(pd.NA, index=result.index)))
    positive_count = _coerce_numeric(result.get("sector_positive_candidate_count", pd.Series(0, index=result.index))).fillna(0.0)
    positive_rate = _coerce_numeric(result.get("sector_positive_rate", pd.Series(pd.NA, index=result.index))).fillna(
        _safe_ratio(positive_count, _coerce_numeric(result.get("sector_candidate_count", pd.Series(0, index=result.index))).replace(0, pd.NA)).fillna(0.0)
    )
    market_positive_rate = _coerce_numeric(result.get("market_positive_rate", pd.Series(0.5, index=result.index))).fillna(0.5)
    sector_median = _coerce_numeric(result.get("sector_median_return", result.get("sector_live_ret_median", pd.Series(0.0, index=result.index)))).fillna(0.0)
    sector_return_percentile = _coerce_numeric(result.get("stock_return_percentile_in_sector", result.get("sector_live_ret_pct", pd.Series(0.5, index=result.index)))).fillna(0.5)
    sector_return_rank = _coerce_numeric(result.get("stock_return_rank_in_sector", result.get("sector_live_ret_rank_desc", pd.Series(float("inf"), index=result.index)))).fillna(float("inf"))
    live_turnover = _coerce_numeric(result.get("live_turnover", result.get("live_turnover_value", pd.Series(0.0, index=result.index)))).fillna(0.0)
    sector_turnover_share = _coerce_numeric(result.get("stock_turnover_share_of_sector", result.get("sector_turnover_share", pd.Series(0.0, index=result.index)))).fillna(0.0)
    centrality_score = _coerce_numeric(result.get("rep_score_centrality", result.get("centrality_score", pd.Series(0.0, index=result.index)))).fillna(0.0)
    liquidity_ok = result.get("liquidity_ok", pd.Series(True, index=result.index)).fillna(False).astype(bool)
    exclude_spike = result.get("exclude_spike", pd.Series(False, index=result.index)).fillna(False).astype(bool)
    gate_reasons = pd.Series("", index=result.index, dtype=object)
    weak_market = market_positive_rate.lt(0.35)
    weak_sector = positive_rate.lt(0.35) | sector_median.lt(-0.5)
    broad_positive_sector = positive_rate.ge(0.40)
    lower_group = sector_return_percentile.le(0.35)
    materially_below_median = live_ret.lt(sector_median - 2.0)
    event_drop = live_ret.le(-8.0) | (live_ret.le(-5.0) & (lower_group | materially_below_median | broad_positive_sector))
    broad_positive_large_negative = broad_positive_sector & live_ret.lt(0.0) & (live_ret.le(-3.0) | lower_group | materially_below_median)
    not_weak_context_under_median = (~weak_market) & (~weak_sector) & live_ret.lt(0.0) & materially_below_median
    lower_group_under_median = lower_group & live_ret.lt(sector_median)
    material_supported_breakout = result.get("material_supported_breakout", pd.Series(False, index=result.index)).fillna(False).astype(bool)
    material_supported_breakout = material_supported_breakout | (
        exclude_spike
        & live_ret.ge(12.0)
        & (sector_return_percentile.ge(0.75) | sector_return_rank.le(2.0))
        & liquidity_ok
        & (live_turnover.ge(1_000_000_000.0) | sector_turnover_share.ge(0.20))
        & (sector_turnover_share.ge(0.15) | live_turnover.ge(5_000_000_000.0))
        & (~event_drop)
    )
    poor_quality_spike = result.get("poor_quality_spike", pd.Series(False, index=result.index)).fillna(False).astype(bool)
    low_spike_turnover = live_turnover.lt(500_000_000.0)
    low_spike_share = sector_turnover_share.lt(0.05)
    low_spike_centrality = centrality_score.lt(1.25)
    poor_quality_spike = poor_quality_spike | (
        exclude_spike
        & (~material_supported_breakout)
        & ((~liquidity_ok) | (low_spike_turnover & low_spike_share) | (low_spike_centrality & low_spike_share))
    )
    result["material_supported_breakout"] = material_supported_breakout
    result["poor_quality_spike"] = poor_quality_spike
    result["exclude_spike_hard_reject"] = exclude_spike & poor_quality_spike
    result["exclude_spike_warning_only"] = exclude_spike & (~poor_quality_spike)
    result["spike_quality"] = pd.Series("", index=result.index, dtype=object)
    result.loc[exclude_spike & material_supported_breakout, "spike_quality"] = "material_supported_breakout"
    result.loc[exclude_spike & poor_quality_spike, "spike_quality"] = "poor_quality_spike"
    result.loc[exclude_spike & (~material_supported_breakout) & (~poor_quality_spike), "spike_quality"] = "warning_only"
    result["breakout_support_reason"] = ""
    result.loc[exclude_spike & material_supported_breakout, "breakout_support_reason"] = "high_return_top_sector_rank_with_large_turnover_or_sector_share"
    result.loc[exclude_spike & poor_quality_spike, "breakout_support_reason"] = "spike_lacks_turnover_share_liquidity_or_centrality"
    spike_warning_only = exclude_spike & (~poor_quality_spike)
    if spike_warning_only.any():
        for column in ["rep_excluded_reason", "hard_block_reason"]:
            result.loc[spike_warning_only, column] = result.loc[spike_warning_only, column].apply(
                lambda value: _remove_gate_reason(value, "exclude_spike")
            )
        result.loc[spike_warning_only & result["hard_block_reason"].astype(str).eq(""), "rep_hard_block"] = False

    masks = [
        (~liquidity_ok, "liquidity_not_ok", True),
        (exclude_spike & (~poor_quality_spike), "exclude_spike_warning_only", False),
        (poor_quality_spike, "poor_quality_spike", True),
        (lower_group_under_median, "sector_lower_group_under_median", True),
        (materially_below_median, "materially_below_sector_median", True),
        (broad_positive_large_negative, "broad_positive_sector_large_negative", True),
        (not_weak_context_under_median, "not_weak_context_negative_below_median", True),
        (event_drop, "event_like_large_drop", True),
    ]
    for mask, reason, hard in masks:
        mask = mask.fillna(False)
        if mask.any():
            gate_reasons.loc[mask] = gate_reasons.loc[mask].apply(lambda value: _append_gate_reason(value, reason))
            result.loc[mask, "rep_excluded_reason"] = result.loc[mask, "rep_excluded_reason"].apply(lambda value: _append_gate_reason(value, reason))
            if hard:
                result.loc[mask, "hard_block_reason"] = result.loc[mask, "hard_block_reason"].apply(lambda value: _append_gate_reason(value, reason))
                result.loc[mask, "rep_hard_block"] = True

    result["representative_gate_pass"] = ~result["rep_hard_block"].fillna(False)
    result["representative_gate_reason"] = gate_reasons.where(gate_reasons.astype(str).ne(""), "today_gate_pass")
    result["hard_reject_reason"] = result["hard_block_reason"]
    result["fallback_blocked_reason"] = result["hard_block_reason"].where(result["rep_hard_block"].fillna(False), "")
    result["sector_turnover_share"] = sector_turnover_share
    result["centrality_score"] = centrality_score
    result["liquidity_score"] = _coerce_numeric(result.get("rep_score_liquidity", result.get("rep_score_today_flow", pd.Series(0.0, index=result.index)))).fillna(0.0)
    result["today_leadership_score"] = _coerce_numeric(result.get("rep_score_today_leadership", pd.Series(0.0, index=result.index))).fillna(0.0)
    result["representative_final_score"] = _coerce_numeric(result.get("rep_score_total", result.get("representative_score", pd.Series(0.0, index=result.index)))).fillna(0.0)
    result["selected_reason"] = result.get("representative_selected_reason", result.get("rep_selected_reason", pd.Series("", index=result.index))).fillna("").astype(str)
    result["market_context"] = pd.Series(
        ["weak_market" if weak else "normal_or_positive_market" for weak in weak_market.fillna(False).tolist()],
        index=result.index,
    )
    result["sector_context"] = [
        "weak_sector" if weak else ("broad_positive_sector" if broad else "mixed_sector")
        for weak, broad in zip(weak_sector.fillna(False).tolist(), broad_positive_sector.fillna(False).tolist())
    ]
    return result


def _apply_1w_representative_gate(eligible: pd.DataFrame) -> pd.DataFrame:
    if eligible is None or eligible.empty:
        return eligible
    working = eligible.copy()
    live_ret = _coerce_numeric(working.get("live_ret_vs_prev_close", pd.Series(pd.NA, index=working.index)))
    rs_1w = _coerce_numeric(working.get("rs_vs_topix_1w", working.get("ret_1w", pd.Series(pd.NA, index=working.index))))
    sector_positive_count = int(live_ret.gt(0.0).sum()) if live_ret.notna().any() else 0
    sufficient_positive = sector_positive_count >= max(2, math.ceil(len(working) * 0.30))
    mask = live_ret.le(-5.0).fillna(False)
    if sufficient_positive:
        mask = mask | (live_ret.lt(-1.0).fillna(False) & rs_1w.lt(0.0).fillna(False))
    return working.loc[~mask].copy()


def _apply_1m_representative_gate(eligible: pd.DataFrame) -> pd.DataFrame:
    if eligible is None or eligible.empty:
        return eligible
    working = eligible.copy()
    live_ret = _coerce_numeric(working.get("live_ret_vs_prev_close", pd.Series(pd.NA, index=working.index)))
    ret_1m = _coerce_numeric(working.get("ret_1m", pd.Series(pd.NA, index=working.index)))
    rs_1m = _coerce_numeric(working.get("rs_vs_topix_1m", pd.Series(pd.NA, index=working.index)))
    avg_turnover = _coerce_numeric(working.get("avg_turnover_20d", pd.Series(pd.NA, index=working.index)))
    turnover_floor = float(avg_turnover.median(skipna=True) or 0.0) * 0.05
    mask = live_ret.le(-7.0).fillna(False)
    mask = mask | (ret_1m.lt(-12.0).fillna(False) & rs_1m.lt(0.0).fillna(False))
    if turnover_floor > 0:
        mask = mask | avg_turnover.lt(turnover_floor).fillna(False)
    return working.loc[~mask].copy()


def _apply_3m_representative_gate(eligible: pd.DataFrame) -> pd.DataFrame:
    if eligible is None or eligible.empty:
        return eligible
    working = eligible.copy()
    live_ret = _coerce_numeric(working.get("live_ret_vs_prev_close", pd.Series(pd.NA, index=working.index)))
    ret_3m = _coerce_numeric(working.get("ret_3m", pd.Series(pd.NA, index=working.index)))
    rs_3m = _coerce_numeric(working.get("rs_vs_topix_3m", pd.Series(pd.NA, index=working.index)))
    avg_turnover = _coerce_numeric(working.get("avg_turnover_20d", pd.Series(pd.NA, index=working.index)))
    turnover_floor = float(avg_turnover.median(skipna=True) or 0.0) * 0.05
    mask = live_ret.le(-7.0).fillna(False)
    mask = mask | (ret_3m.lt(-18.0).fillna(False) & rs_3m.lt(0.0).fillna(False))
    if turnover_floor > 0:
        mask = mask | avg_turnover.lt(turnover_floor).fillna(False)
    return working.loc[~mask].copy()


def _apply_horizon_representative_gate(eligible: pd.DataFrame, *, horizon: str) -> pd.DataFrame:
    if str(horizon) == "1w":
        return _apply_1w_representative_gate(eligible)
    if str(horizon) == "1m":
        return _apply_1m_representative_gate(eligible)
    if str(horizon) == "3m":
        return _apply_3m_representative_gate(eligible)
    return eligible


def _score_sector_center_candidates(
    merged: pd.DataFrame,
    today_sector_leaderboard: pd.DataFrame,
    display_base_df: pd.DataFrame,
) -> pd.DataFrame:
    if merged.empty or today_sector_leaderboard.empty or display_base_df.empty:
        return pd.DataFrame()
    sorted_today_sector_leaderboard = _sort_today_sector_leaderboard_for_display(today_sector_leaderboard)
    sector_key_col = _sector_key_column(sorted_today_sector_leaderboard, merged, display_base_df)
    market_positive_rate = float((_coerce_numeric(merged.get("live_ret_vs_prev_close", pd.Series(dtype="float64"))).dropna() > 0.0).mean()) if "live_ret_vs_prev_close" in merged.columns and _coerce_numeric(merged.get("live_ret_vs_prev_close", pd.Series(dtype="float64"))).notna().any() else 0.5
    display_sector_keys = set(sorted_today_sector_leaderboard[sector_key_col].astype(str).tolist())
    working = merged[merged[sector_key_col].astype(str).isin(display_sector_keys)].copy()
    if working.empty:
        return working
    sector_scores = sorted_today_sector_leaderboard[
        [sector_key_col, "sector_name", "today_rank", "price_block_score", "flow_block_score", "participation_block_score", "intraday_sector_score"]
    ].drop_duplicates(sector_key_col).rename(columns={"sector_name": "leaderboard_sector_name"})
    working = working.merge(sector_scores, on=sector_key_col, how="left")
    working["sector_name"] = working.get("leaderboard_sector_name", working["sector_name"]).fillna(working["sector_name"])
    full_base = display_base_df.copy()
    full_base["sector_name"] = full_base.get("sector_name", pd.Series(dtype=str)).map(_normalize_industry_name)
    if sector_key_col == "normalized_sector_name":
        full_base["normalized_sector_name"] = full_base.get("original_sector_name", full_base["sector_name"]).map(_normalize_industry_key)
    full_base = full_base[full_base[sector_key_col].astype(str).isin(display_sector_keys)].copy()
    sector_turnover_total_full = full_base.groupby(sector_key_col)["TradingValue_latest"].transform("sum")
    full_base["sector_contribution_full"] = _safe_ratio(full_base["TradingValue_latest"], sector_turnover_total_full).fillna(0.0)
    full_base["contribution_rank_in_sector"] = full_base.groupby(sector_key_col)["sector_contribution_full"].rank(method="dense", ascending=False)
    full_base["turnover_rank_in_sector"] = full_base.groupby(sector_key_col)["avg_turnover_20d"].rank(method="dense", ascending=False)
    global_turnover_floor = float(_coerce_numeric(full_base["avg_turnover_20d"]).median(skipna=True) or 0.0)
    global_volume_floor = float(_coerce_numeric(full_base["avg_volume_20d"]).median(skipna=True) or 0.0)
    sector_turnover_floor = full_base.groupby(sector_key_col)["avg_turnover_20d"].transform(lambda s: float(_coerce_numeric(s).median(skipna=True) or 0.0))
    sector_volume_floor = full_base.groupby(sector_key_col)["avg_volume_20d"].transform(lambda s: float(_coerce_numeric(s).median(skipna=True) or 0.0))
    turnover_threshold = (_coerce_numeric(sector_turnover_floor).fillna(0.0) * 0.20).clip(lower=global_turnover_floor * 0.01)
    volume_threshold = (_coerce_numeric(sector_volume_floor).fillna(0.0) * 0.20).clip(lower=global_volume_floor * 0.01)
    full_base["liquidity_ok"] = (
        _coerce_numeric(full_base["avg_turnover_20d"]).fillna(0.0) >= turnover_threshold
    ) & (
        _coerce_numeric(full_base["avg_volume_20d"]).fillna(0.0) >= volume_threshold
    )
    if "sector_constituent_count" in full_base.columns:
        full_base["sector_constituent_count_for_rep_pool"] = _coerce_numeric(full_base["sector_constituent_count"])
    else:
        full_base["sector_constituent_count_for_rep_pool"] = full_base.groupby(sector_key_col)["code"].transform("nunique")
    base_metrics = full_base[["code", sector_key_col, "sector_contribution_full", "contribution_rank_in_sector", "turnover_rank_in_sector", "liquidity_ok", "sector_constituent_count_for_rep_pool"]].drop_duplicates("code")
    working = working.merge(base_metrics, on=["code", sector_key_col], how="left")
    working["was_in_selected50"] = working.get("was_in_selected50", pd.Series(True, index=working.index)).fillna(True).astype(bool)
    working["was_in_must_have"] = working.get("was_in_must_have", pd.Series(False, index=working.index)).fillna(False).astype(bool)
    working["selected_from_primary_or_supplemental"] = working.get(
        "selected_from_primary_or_supplemental",
        pd.Series(["primary"] * len(working), index=working.index),
    ).fillna("primary").astype(str).replace("", "primary")
    working["sector_live_turnover_total"] = working.groupby(sector_key_col)["live_turnover"].transform("sum")
    working["stock_turnover_share_of_sector"] = _safe_ratio(working["live_turnover"], working["sector_live_turnover_total"]).fillna(0.0)
    intraday_push = (_coerce_numeric(working["live_ret_vs_prev_close"]).fillna(0.0) - _coerce_numeric(working["live_ret_from_open"]).fillna(0.0)).clip(lower=-15.0, upper=15.0)
    working["closing_strength_signal"] = _coerce_numeric(working["high_close_score"]).fillna(0.0) + intraday_push * 0.05
    working["sector_live_ret_median"] = working.groupby(sector_key_col)["live_ret_vs_prev_close"].transform(lambda s: float(_coerce_numeric(s).median(skipna=True) or 0.0))
    working["sector_median_return"] = working["sector_live_ret_median"]
    working["sector_top_quartile_return"] = working.groupby(sector_key_col)["live_ret_vs_prev_close"].transform(lambda s: float(_coerce_numeric(s).quantile(0.75) or 0.0))
    working["sector_bottom_quartile_return"] = working.groupby(sector_key_col)["live_ret_vs_prev_close"].transform(lambda s: float(_coerce_numeric(s).quantile(0.25) or 0.0))
    working["sector_closing_strength_median"] = working.groupby(sector_key_col)["closing_strength_signal"].transform(lambda s: float(_coerce_numeric(s).median(skipna=True) or 0.0))
    working["sector_relative_live_ret"] = _coerce_numeric(working["live_ret_vs_prev_close"]).fillna(0.0) - working["sector_live_ret_median"]
    working["sector_positive_candidate_count"] = working.groupby(sector_key_col)["live_ret_vs_prev_close"].transform(lambda s: int((_coerce_numeric(s).fillna(-999.0) >= 0.0).sum()))
    working["sector_negative_candidate_count"] = working.groupby(sector_key_col)["live_ret_vs_prev_close"].transform(lambda s: int((_coerce_numeric(s).fillna(0.0) < 0.0).sum()))
    working["sector_candidate_count"] = working.groupby(sector_key_col)["code"].transform("size").fillna(0).astype(int)
    working["primary_candidate_count"] = working.groupby(sector_key_col)["selected_from_primary_or_supplemental"].transform(lambda s: int(pd.Series(s).fillna("primary").astype(str).ne("supplemental").sum()))
    working["supplemental_candidate_count"] = working.groupby(sector_key_col)["selected_from_primary_or_supplemental"].transform(lambda s: int(pd.Series(s).fillna("").astype(str).eq("supplemental").sum()))
    working["final_candidate_count"] = working["sector_candidate_count"]
    working["sector_constituent_count"] = working.groupby(sector_key_col)["sector_constituent_count_for_rep_pool"].transform(lambda s: float(_coerce_numeric(s).max(skipna=True) or 0.0))
    working["representative_pool_coverage_rate"] = _safe_ratio(working["final_candidate_count"], working["sector_constituent_count"].replace(0, pd.NA)).fillna(0.0)
    working["candidate_pool_warning"] = ""
    working.loc[_coerce_numeric(working["final_candidate_count"]).fillna(0.0) < float(DEEP_WATCH_REPRESENTATIVE_POOL_MIN_PER_SECTOR), "candidate_pool_warning"] = "representative_pool_too_small"
    working.loc[
        (_coerce_numeric(working["supplemental_candidate_count"]).fillna(0.0) > 0.0)
        & (working["candidate_pool_warning"].astype(str) == ""),
        "candidate_pool_warning",
    ] = "supplemental_candidates_added"
    working["candidate_pool_reason"] = working.apply(
        lambda row: _join_ui_fragments(
            f"primary={int(row.get('primary_candidate_count', 0) or 0)}",
            f"supplemental={int(row.get('supplemental_candidate_count', 0) or 0)}",
            f"final={int(row.get('final_candidate_count', 0) or 0)}",
            f"coverage={float(row.get('representative_pool_coverage_rate', 0.0) or 0.0):.3f}",
            str(row.get("candidate_pool_warning", "") or ""),
        ),
        axis=1,
    )
    working["exclude_spike"] = (
        (_coerce_numeric(working["live_ret_vs_prev_close"]).fillna(0.0) >= 12.0)
        & (_coerce_numeric(working["live_turnover_ratio_20d"]).fillna(0.0) >= 2.5)
        & (
            (_coerce_numeric(working["price_vs_ma20_pct"]).fillna(0.0) >= 18.0)
            | ~working["liquidity_ok"].fillna(False)
            | (_coerce_numeric(working["avg_turnover_20d"]).fillna(0.0) < global_turnover_floor)
        )
    )
    sector_groups = working[sector_key_col]
    working["rep_score_centrality"] = 0.0
    working["rep_score_centrality"] += _score_percentile_within_group(working["sector_contribution_full"], sector_groups) * CENTER_LEADER_CENTRALITY_WEIGHTS["sector_contribution_full"]
    working["rep_score_centrality"] += _score_rank_ascending(working["contribution_rank_in_sector"]) * CENTER_LEADER_CENTRALITY_WEIGHTS["contribution_rank_in_sector"]
    working["rep_score_centrality"] += _score_rank_ascending(working["turnover_rank_in_sector"]) * CENTER_LEADER_CENTRALITY_WEIGHTS["turnover_rank_in_sector"]
    working["rep_score_centrality"] += _score_percentile_within_group(working["avg_turnover_20d"], sector_groups) * CENTER_LEADER_CENTRALITY_WEIGHTS["avg_turnover_20d"]
    working["rep_score_centrality"] += _score_percentile_within_group(working["avg_volume_20d"], sector_groups) * CENTER_LEADER_CENTRALITY_WEIGHTS["avg_volume_20d"]
    working["rep_score_centrality"] += _score_percentile_within_group(working["stock_turnover_share_of_sector"], sector_groups) * CENTER_LEADER_CENTRALITY_WEIGHTS["stock_turnover_share_of_sector"]
    working.loc[working["liquidity_ok"].fillna(False), "rep_score_centrality"] += CENTER_LEADER_CENTRALITY_WEIGHTS["liquidity_ok"]
    working["rep_score_today_flow"] = 0.0
    working["rep_score_today_flow"] += _score_percentile_within_group(working["live_turnover"], sector_groups) * 0.55
    working["rep_score_today_flow"] += _score_percentile_within_group(working["stock_turnover_share_of_sector"], sector_groups) * 0.75
    working["rep_score_today_flow"] += _score_percentile_within_group(working["live_turnover_ratio_20d"], sector_groups) * 0.80
    working["rep_score_today_flow"] += _score_percentile_within_group(working["live_volume_ratio_20d"], sector_groups) * 0.55
    working["rep_score_today_leadership"] = 0.0
    working["rep_score_today_leadership"] += _score_percentile_within_group(working["live_ret_vs_prev_close"], sector_groups) * CENTER_LEADER_TODAY_WEIGHTS["live_ret_vs_prev_close"]
    working["rep_score_today_leadership"] += _score_percentile_within_group(working["live_ret_from_open"], sector_groups) * CENTER_LEADER_TODAY_WEIGHTS["live_ret_from_open"]
    working["rep_score_today_leadership"] += _score_percentile_within_group(working["closing_strength_signal"], sector_groups) * CENTER_LEADER_TODAY_WEIGHTS["closing_strength_signal"]
    working["rep_score_today_leadership"] += _score_percentile_within_group(working["live_turnover_ratio_20d"], sector_groups) * CENTER_LEADER_TODAY_WEIGHTS["live_turnover_ratio_20d"]
    working["rep_score_today_leadership"] += _score_percentile_within_group(working["live_volume_ratio_20d"], sector_groups) * CENTER_LEADER_TODAY_WEIGHTS["live_volume_ratio_20d"]
    working["rep_score_today_leadership"] += _score_percentile_within_group(working["sector_relative_live_ret"], sector_groups) * CENTER_LEADER_TODAY_WEIGHTS["sector_relative_live_ret"]
    working["rep_score_today_leadership"] += working["rep_score_today_flow"] * 0.55
    working["sector_live_ret_rank_desc"] = _group_rank_desc(working["live_ret_vs_prev_close"], sector_groups)
    working["sector_live_ret_pct"] = _score_percentile_within_group(working["live_ret_vs_prev_close"], sector_groups)
    working["stock_return_percentile_in_sector"] = working["sector_live_ret_pct"]
    working["stock_return_rank_in_sector"] = working["sector_live_ret_rank_desc"]
    working["market_positive_rate"] = market_positive_rate
    working["sector_today_flow_pct"] = _score_percentile_within_group(working["rep_score_today_flow"], sector_groups)
    working["sector_today_leadership_pct"] = _score_percentile_within_group(working["rep_score_today_leadership"], sector_groups)
    working["sector_live_ret_top_band_cutoff"] = working["sector_candidate_count"].apply(lambda count: max(1, math.ceil(float(count or 0) * 0.35)))
    working["material_supported_breakout"] = (
        working["exclude_spike"].fillna(False)
        & (_coerce_numeric(working["live_ret_vs_prev_close"]).fillna(0.0) >= 12.0)
        & (
            (_coerce_numeric(working["sector_live_ret_pct"]).fillna(0.0) >= 0.75)
            | (_coerce_numeric(working["sector_live_ret_rank_desc"]).fillna(float("inf")) <= 2.0)
        )
        & working["liquidity_ok"].fillna(False)
        & (
            (_coerce_numeric(working["live_turnover"]).fillna(0.0) >= 1_000_000_000.0)
            | (_coerce_numeric(working["stock_turnover_share_of_sector"]).fillna(0.0) >= 0.20)
        )
        & (
            (_coerce_numeric(working["stock_turnover_share_of_sector"]).fillna(0.0) >= 0.15)
            | (_coerce_numeric(working["live_turnover"]).fillna(0.0) >= 5_000_000_000.0)
        )
    )
    low_spike_turnover = _coerce_numeric(working["live_turnover"]).fillna(0.0) < 500_000_000.0
    low_spike_share = _coerce_numeric(working["stock_turnover_share_of_sector"]).fillna(0.0) < 0.05
    low_spike_centrality = _coerce_numeric(working["rep_score_centrality"]).fillna(0.0) < 1.25
    working["poor_quality_spike"] = (
        working["exclude_spike"].fillna(False)
        & (~working["material_supported_breakout"].fillna(False))
        & ((~working["liquidity_ok"].fillna(False)) | (low_spike_turnover & low_spike_share) | (low_spike_centrality & low_spike_share))
    )
    working["exclude_spike_hard_reject"] = working["exclude_spike"].fillna(False) & working["poor_quality_spike"].fillna(False)
    working["exclude_spike_warning_only"] = working["exclude_spike"].fillna(False) & (~working["poor_quality_spike"].fillna(False))
    working["spike_quality"] = ""
    working.loc[working["exclude_spike"].fillna(False) & working["material_supported_breakout"].fillna(False), "spike_quality"] = "material_supported_breakout"
    working.loc[working["exclude_spike"].fillna(False) & working["poor_quality_spike"].fillna(False), "spike_quality"] = "poor_quality_spike"
    working.loc[
        working["exclude_spike"].fillna(False)
        & (~working["material_supported_breakout"].fillna(False))
        & (~working["poor_quality_spike"].fillna(False)),
        "spike_quality",
    ] = "warning_only"
    working["breakout_support_reason"] = ""
    working.loc[
        working["exclude_spike"].fillna(False) & working["material_supported_breakout"].fillna(False),
        "breakout_support_reason",
    ] = "high_return_top_sector_rank_with_large_turnover_or_sector_share"
    working.loc[
        working["exclude_spike"].fillna(False) & working["poor_quality_spike"].fillna(False),
        "breakout_support_reason",
    ] = "spike_lacks_turnover_share_liquidity_or_centrality"
    working["rep_score_sanity"] = 0.0
    working.loc[working["liquidity_ok"].fillna(False), "rep_score_sanity"] += 1.00
    working.loc[~working["exclude_spike"].fillna(False), "rep_score_sanity"] += 0.80
    working.loc[working["material_supported_breakout"].fillna(False), "rep_score_sanity"] += 0.70
    working.loc[working["poor_quality_spike"].fillna(False), "rep_score_sanity"] -= 1.20
    working.loc[_coerce_numeric(working["live_ret_vs_prev_close"]).fillna(-999.0) >= _coerce_numeric(working["sector_live_ret_median"]).fillna(0.0), "rep_score_sanity"] += 0.60
    working.loc[_coerce_numeric(working["closing_strength_signal"]).fillna(0.0) >= _coerce_numeric(working["sector_closing_strength_median"]).fillna(0.0), "rep_score_sanity"] += 0.45
    working.loc[_coerce_numeric(working["sector_today_flow_pct"]).fillna(0.0) >= 0.67, "rep_score_sanity"] += 0.30
    weak_relative_close_mask = (
        _coerce_numeric(working["live_ret_vs_prev_close"]).fillna(0.0) < _coerce_numeric(working["sector_live_ret_median"]).fillna(0.0)
    ) & (
        _coerce_numeric(working["closing_strength_signal"]).fillna(0.0) < _coerce_numeric(working["sector_closing_strength_median"]).fillna(0.0)
    )
    working.loc[weak_relative_close_mask, "rep_score_sanity"] -= 1.40
    working.loc[
        (_coerce_numeric(working["sector_live_ret_pct"]).fillna(0.0) <= 0.34)
        & (_coerce_numeric(working["sector_today_flow_pct"]).fillna(0.0) <= 0.34),
        "rep_score_sanity",
    ] -= 1.15
    working.loc[
        (_coerce_numeric(working["live_ret_from_open"]).fillna(0.0) < 0.0)
        & (_coerce_numeric(working["sector_today_leadership_pct"]).fillna(0.0) <= 0.50),
        "rep_score_sanity",
    ] -= 0.60
    working.loc[_coerce_numeric(working["high_close_score"]).fillna(1.0) < 0.88, "rep_score_sanity"] -= 0.80
    working["rep_score_total"] = working["rep_score_centrality"] + working["rep_score_today_leadership"] + working["rep_score_sanity"]
    working["rep_hard_block"] = False
    working["rep_excluded_reason"] = ""
    working["hard_block_reason"] = ""
    hard_block_liquidity = ~working["liquidity_ok"].fillna(False)
    hard_block_spike = working["poor_quality_spike"].fillna(False)
    working.loc[hard_block_liquidity, "rep_hard_block"] = True
    working.loc[hard_block_liquidity, "rep_excluded_reason"] = "liquidity_not_ok"
    working.loc[hard_block_liquidity, "hard_block_reason"] = "liquidity_not_ok"
    working.loc[hard_block_spike, "rep_hard_block"] = True
    working.loc[hard_block_spike, "rep_excluded_reason"] = working["rep_excluded_reason"].where(working["rep_excluded_reason"].astype(str) != "", "poor_quality_spike")
    working.loc[hard_block_spike, "hard_block_reason"] = working["hard_block_reason"].where(working["hard_block_reason"].astype(str) != "", "poor_quality_spike")
    working = _apply_today_representative_gate(working)
    working["rep_relative_leadership_pass"] = (
        (_coerce_numeric(working["sector_live_ret_rank_desc"]).fillna(float("inf")) <= _coerce_numeric(working["sector_live_ret_top_band_cutoff"]).fillna(0.0))
        | (_coerce_numeric(working["live_ret_vs_prev_close"]).fillna(-999.0) >= _coerce_numeric(working["sector_live_ret_median"]).fillna(0.0))
        | (_coerce_numeric(working["sector_today_leadership_pct"]).fillna(0.0) >= 0.67)
        | (_coerce_numeric(working["sector_today_flow_pct"]).fillna(0.0) >= 0.67)
    )
    working["rep_centrality_pass"] = (
        (_coerce_numeric(working["rep_score_centrality"]).fillna(0.0) >= 1.70)
        & (_score_percentile_within_group(working["rep_score_centrality"], sector_groups).fillna(0.0) >= 0.40)
    )
    working["rep_sanity_pass"] = _coerce_numeric(working["rep_score_sanity"]).fillna(0.0) >= 0.10
    working["rep_relative_weak"] = (
        (_coerce_numeric(working["sector_live_ret_pct"]).fillna(0.0) <= 0.34)
        & (_coerce_numeric(working["live_ret_vs_prev_close"]).fillna(0.0) < _coerce_numeric(working["sector_live_ret_median"]).fillna(0.0))
        & (_coerce_numeric(working["sector_today_leadership_pct"]).fillna(0.0) <= 0.34)
        & (_coerce_numeric(working["sector_today_flow_pct"]).fillna(0.0) <= 0.45)
    )
    working["rep_quality_pass"] = (
        ~working["rep_hard_block"].fillna(False)
        & working["rep_relative_leadership_pass"].fillna(False)
        & working["rep_centrality_pass"].fillna(False)
        & working["rep_sanity_pass"].fillna(False)
        & (~working["rep_relative_weak"].fillna(False))
    )
    working.loc[
        (~working["rep_hard_block"].fillna(False))
        & (~working["rep_quality_pass"].fillna(False))
        & working["rep_relative_weak"].fillna(False),
        "rep_excluded_reason",
    ] = "relative_leadership_weak"
    working.loc[
        (~working["rep_hard_block"].fillna(False))
        & (~working["rep_quality_pass"].fillna(False))
        & (working["rep_excluded_reason"].astype(str) == "")
        & (~working["rep_relative_leadership_pass"].fillna(False)),
        "rep_excluded_reason",
    ] = "relative_leadership_not_enough"
    working.loc[
        (~working["rep_hard_block"].fillna(False))
        & (~working["rep_quality_pass"].fillna(False))
        & (working["rep_excluded_reason"].astype(str) == "")
        & (~working["rep_centrality_pass"].fillna(False)),
        "rep_excluded_reason",
    ] = "centrality_below_minimum"
    working.loc[
        (~working["rep_hard_block"].fillna(False))
        & (~working["rep_quality_pass"].fillna(False))
        & (working["rep_excluded_reason"].astype(str) == "")
        & (~working["rep_sanity_pass"].fillna(False)),
        "rep_excluded_reason",
    ] = "sanity_below_minimum"
    working["rep_selected_reason"] = ""
    working["rep_fallback_reason"] = ""
    working["representative_selected_reason"] = ""
    working["representative_quality_flag"] = "excluded"
    working["representative_fallback_reason"] = ""
    working["is_sector_center_candidate"] = ~working["rep_hard_block"].fillna(False)
    working["candidate_in_universe"] = True
    working["selected_horizon"] = "today"
    working["selected_universe"] = "stock_merged_deep_watch_selected_sector_pool"
    working["sector_positive_count"] = _coerce_numeric(working["sector_positive_candidate_count"]).fillna(0).astype(int)
    working["sector_negative_count"] = _coerce_numeric(working["sector_negative_candidate_count"]).fillna(0).astype(int)
    sector_total = (working["sector_positive_count"] + working["sector_negative_count"]).replace(0, pd.NA)
    working["sector_positive_rate"] = _safe_ratio(working["sector_positive_count"], sector_total).fillna(0.0)
    working["sector_top_positive_count"] = working.groupby(sector_key_col)["live_ret_vs_prev_close"].transform(
        lambda s: int((_coerce_numeric(s).fillna(-999.0) > 0.0).sum())
    )
    working["rep_score_today_strength"] = working["rep_score_today_leadership"]
    working["rep_score_relative_strength"] = working["stock_return_percentile_in_sector"].fillna(0.0) * 3.0
    working["rep_score_liquidity"] = working["rep_score_today_flow"]
    return working


def _build_sector_representatives(scored_candidates: pd.DataFrame) -> pd.DataFrame:
    if scored_candidates.empty:
        return _empty_sector_representatives_frame()
    working = scored_candidates.copy()
    sector_key_col = _sector_key_column(working)
    representative_frames: list[pd.DataFrame] = []
    for _, group in working.groupby(sector_key_col, dropna=False):
        sector_name_series = group["sector_name"] if "sector_name" in group.columns else pd.Series([""] * len(group), index=group.index)
        sector_name_values = sector_name_series.dropna().astype(str)
        sector_name = str(sector_name_values.iloc[0] if len(sector_name_values) else "")
        today_rank_value = group.get("today_rank", pd.Series(pd.NA, index=group.index)).dropna()
        today_rank = today_rank_value.iloc[0] if len(today_rank_value) else pd.NA
        group_positive_count = int(_coerce_numeric(group.get("sector_positive_candidate_count", pd.Series([0]))).max() or 0)
        group_negative_count = int(_coerce_numeric(group.get("sector_negative_candidate_count", pd.Series([0]))).max() or 0)
        group_total_count = group_positive_count + group_negative_count
        sector_group = group.sort_values(
            ["rep_score_today_leadership", "rep_score_centrality", "rep_score_sanity", "live_ret_vs_prev_close", "stock_turnover_share_of_sector"],
            ascending=[False, False, False, False, False],
            kind="mergesort",
        ).copy()
        quality = sector_group[sector_group["rep_quality_pass"].fillna(False)].copy()
        chosen = pd.DataFrame()
        fallback_reason = "fallback_no_clear_leader"
        fallback_pool = sector_group[
            (~sector_group["rep_hard_block"].fillna(False))
            & (sector_group["rep_centrality_pass"].fillna(False))
            & (_coerce_numeric(sector_group["rep_score_sanity"]).fillna(0.0) >= -0.25)
        ].copy()
        if fallback_pool.empty:
            fallback_reason = "fallback_insufficient_candidates"
            fallback_pool = sector_group[
                (~sector_group["rep_hard_block"].fillna(False))
                & (_coerce_numeric(sector_group["rep_score_sanity"]).fillna(0.0) >= -0.50)
            ].copy()
        if fallback_pool.empty:
            fallback_pool = sector_group[(~sector_group["rep_hard_block"].fillna(False))].copy()
        if not quality.empty:
            chosen = quality.head(3).copy()
            chosen["representative_quality_flag"] = "quality_pass"
            if len(chosen) < 3 and not fallback_pool.empty:
                existing_codes = set(chosen.get("code", pd.Series(dtype=str)).astype(str).tolist())
                supplement = fallback_pool[~fallback_pool.get("code", pd.Series(dtype=str)).astype(str).isin(existing_codes)].head(3 - len(chosen)).copy()
                if not supplement.empty:
                    supplement["rep_selected_reason"] = "center_fallback_leader"
                    supplement["rep_fallback_reason"] = fallback_reason
                    supplement["representative_fallback_reason"] = fallback_reason
                    supplement["representative_quality_flag"] = "quality_warn"
                    chosen = pd.concat([chosen, supplement], ignore_index=True, sort=False)
        else:
            if fallback_pool.empty:
                no_rep = pd.DataFrame(
                    [
                        {
                            "sector_name": sector_name,
                            "today_rank": today_rank,
                            "representative_rank": 1,
                            "code": "",
                            "name": "代表なし",
                            "live_price": pd.NA,
                            "current_price": pd.NA,
                            "current_price_unavailable": True,
                            "live_ret_vs_prev_close": pd.NA,
                            "live_ret_from_open": pd.NA,
                            "live_turnover": pd.NA,
                            "live_turnover_value": pd.NA,
                            "live_turnover_unavailable": True,
                            "stock_turnover_share_of_sector": pd.NA,
                            "selected_horizon": "today",
                            "selected_universe": "stock_merged_deep_watch_selected_sector_pool",
                            "primary_candidate_count": int(_coerce_numeric(sector_group.get("primary_candidate_count", pd.Series([0]))).max() or 0),
                            "supplemental_candidate_count": int(_coerce_numeric(sector_group.get("supplemental_candidate_count", pd.Series([0]))).max() or 0),
                            "final_candidate_count": int(_coerce_numeric(sector_group.get("final_candidate_count", pd.Series([0]))).max() or 0),
                            "sector_constituent_count": int(_coerce_numeric(sector_group.get("sector_constituent_count", pd.Series([0]))).max() or 0),
                            "representative_pool_coverage_rate": float(_coerce_numeric(sector_group.get("representative_pool_coverage_rate", pd.Series([0.0]))).max() or 0.0),
                            "candidate_pool_warning": "no_valid_representative_after_gate",
                            "candidate_pool_reason": str(
                                (
                                    sector_group["candidate_pool_reason"]
                                    if "candidate_pool_reason" in sector_group.columns
                                    else pd.Series([""] * len(sector_group), index=sector_group.index)
                                ).fillna("").astype(str).iloc[0]
                                if len(sector_group)
                                else ""
                            ),
                            "selected_from_primary_or_supplemental": "",
                            "sector_candidate_count": int(_coerce_numeric(sector_group.get("sector_candidate_count", pd.Series([0]))).max() or 0),
                            "sector_positive_count": group_positive_count,
                            "sector_negative_count": group_negative_count,
                            "sector_positive_rate": float(group_positive_count / group_total_count) if group_total_count else 0.0,
                            "sector_median_return": float(_coerce_numeric(sector_group.get("sector_median_return", pd.Series([pd.NA]))).dropna().iloc[0]) if _coerce_numeric(sector_group.get("sector_median_return", pd.Series([pd.NA]))).notna().any() else None,
                            "sector_top_quartile_return": float(_coerce_numeric(sector_group.get("sector_top_quartile_return", pd.Series([pd.NA]))).dropna().iloc[0]) if _coerce_numeric(sector_group.get("sector_top_quartile_return", pd.Series([pd.NA]))).notna().any() else None,
                            "sector_bottom_quartile_return": float(_coerce_numeric(sector_group.get("sector_bottom_quartile_return", pd.Series([pd.NA]))).dropna().iloc[0]) if _coerce_numeric(sector_group.get("sector_bottom_quartile_return", pd.Series([pd.NA]))).notna().any() else None,
                            "stock_return_percentile_in_sector": pd.NA,
                            "stock_return_rank_in_sector": pd.NA,
                            "market_positive_rate": float(_coerce_numeric(sector_group.get("market_positive_rate", pd.Series([pd.NA]))).dropna().iloc[0]) if _coerce_numeric(sector_group.get("market_positive_rate", pd.Series([pd.NA]))).notna().any() else None,
                            "market_context": "",
                            "sector_context": "",
                            "sector_live_ret_median": float(_coerce_numeric(sector_group.get("sector_live_ret_median", pd.Series([pd.NA]))).dropna().iloc[0]) if _coerce_numeric(sector_group.get("sector_live_ret_median", pd.Series([pd.NA]))).notna().any() else None,
                            "sector_top_positive_count": int(_coerce_numeric(sector_group.get("sector_top_positive_count", pd.Series([0]))).max() or 0),
                            "representative_gate_pass": False,
                            "representative_gate_reason": "no_valid_today_representative",
                            "hard_reject_reason": "all_candidates_blocked_by_today_gate",
                            "hard_block_reason": "all_candidates_blocked_by_today_gate",
                            "fallback_used": True,
                            "fallback_reason": "プラス候補または適格候補なし",
                            "fallback_blocked_reason": "all_candidates_blocked_by_today_gate",
                            "representative_score": pd.NA,
                            "rep_score_total": pd.NA,
                            "rep_score_today_strength": pd.NA,
                            "rep_score_relative_strength": pd.NA,
                            "rep_score_centrality": pd.NA,
                            "rep_score_liquidity": pd.NA,
                            "rep_score_today_leadership": pd.NA,
                            "rep_score_sanity": pd.NA,
                            "rep_selected_reason": "当日中心株不在",
                            "rep_excluded_reason": "",
                            "rep_fallback_reason": "プラス候補または適格候補なし",
                            "representative_selected_reason": "当日中心株不在",
                            "representative_quality_flag": "no_valid_today_representative",
                            "representative_fallback_reason": "プラス候補または適格候補なし",
                            "earnings_today_announcement_flag": False,
                            "earnings_announcement_date": "",
                            "was_in_selected50": False,
                            "was_in_must_have": False,
                            "nikkei_search": "",
                            "material_link": "",
                        }
                    ]
                )
                representative_frames.append(no_rep)
                continue
            chosen = fallback_pool.head(3).copy()
            chosen["rep_selected_reason"] = "center_fallback_leader"
            chosen["rep_fallback_reason"] = fallback_reason
            chosen["representative_fallback_reason"] = fallback_reason
            chosen["representative_quality_flag"] = "quality_warn"
        if chosen.empty:
            continue
        chosen = chosen.reset_index(drop=True)
        chosen["representative_rank"] = range(1, len(chosen) + 1)
        if quality.empty:
            chosen["rep_selected_reason"] = "center_fallback_leader"
        else:
            chosen["rep_selected_reason"] = chosen["representative_rank"].map(lambda rank: "center_leader" if int(rank) == 1 else "sector_support_leader")
        chosen["representative_selected_reason"] = chosen["rep_selected_reason"]
        chosen.loc[chosen["representative_quality_flag"].astype(str).isin(["", "excluded", "nan"]), "representative_quality_flag"] = "quality_pass"
        chosen.loc[chosen["rep_fallback_reason"].astype(str).eq(""), "rep_fallback_reason"] = chosen["representative_fallback_reason"]
        chosen["fallback_used"] = chosen["representative_fallback_reason"].fillna("").astype(str).ne("")
        chosen["fallback_reason"] = chosen["representative_fallback_reason"].fillna("").astype(str)
        chosen["representative_score"] = chosen["rep_score_total"]
        chosen["current_price"] = _coerce_numeric(chosen.get("live_price", pd.Series(pd.NA, index=chosen.index)))
        chosen["live_turnover_value"] = _coerce_numeric(chosen.get("live_turnover", pd.Series(pd.NA, index=chosen.index)))
        chosen["current_price_unavailable"] = chosen["current_price"].isna()
        chosen["live_turnover_unavailable"] = chosen["live_turnover_value"].isna()
        for column in [
            "selected_horizon",
            "selected_universe",
            "primary_candidate_count",
            "supplemental_candidate_count",
            "final_candidate_count",
            "sector_constituent_count",
            "representative_pool_coverage_rate",
            "candidate_pool_warning",
            "candidate_pool_reason",
            "selected_from_primary_or_supplemental",
            "sector_candidate_count",
            "sector_positive_count",
            "sector_negative_count",
            "sector_positive_rate",
            "sector_median_return",
            "sector_top_quartile_return",
            "sector_bottom_quartile_return",
            "stock_return_percentile_in_sector",
            "stock_return_rank_in_sector",
            "market_positive_rate",
            "market_context",
            "sector_context",
            "sector_live_ret_median",
            "sector_top_positive_count",
            "representative_gate_pass",
            "representative_gate_reason",
            "hard_reject_reason",
            "hard_block_reason",
            "fallback_used",
            "fallback_reason",
            "fallback_blocked_reason",
            "live_ret_from_open",
            "sector_live_ret_pct",
            "sector_today_flow_pct",
            "sector_turnover_share",
            "exclude_spike",
            "exclude_spike_hard_reject",
            "exclude_spike_warning_only",
            "spike_quality",
            "poor_quality_spike",
            "material_supported_breakout",
            "breakout_support_reason",
            "centrality_score",
            "liquidity_score",
            "today_leadership_score",
            "representative_final_score",
            "selected_reason",
            "rep_score_today_strength",
            "rep_score_relative_strength",
            "rep_score_liquidity",
        ]:
            if column not in chosen.columns:
                chosen[column] = pd.NA
        representative_frames.append(
            chosen[
                [
                    "sector_name",
                    "today_rank",
                    "representative_rank",
                    "code",
                    "name",
                    "live_price",
                    "current_price",
                    "current_price_unavailable",
                    "live_ret_vs_prev_close",
                    "live_turnover",
                    "live_turnover_value",
                    "live_turnover_unavailable",
                    "stock_turnover_share_of_sector",
                    "selected_horizon",
                    "selected_universe",
                    "primary_candidate_count",
                    "supplemental_candidate_count",
                    "final_candidate_count",
                    "sector_constituent_count",
                    "representative_pool_coverage_rate",
                    "candidate_pool_warning",
                    "candidate_pool_reason",
                    "selected_from_primary_or_supplemental",
                    "sector_candidate_count",
                    "sector_positive_count",
                    "sector_negative_count",
                    "sector_positive_rate",
                    "sector_median_return",
                    "sector_top_quartile_return",
                    "sector_bottom_quartile_return",
                    "stock_return_percentile_in_sector",
                    "stock_return_rank_in_sector",
                    "market_positive_rate",
                    "market_context",
                    "sector_context",
                    "sector_live_ret_median",
                    "sector_top_positive_count",
                    "representative_gate_pass",
                    "representative_gate_reason",
                    "hard_reject_reason",
                    "hard_block_reason",
                    "fallback_used",
                    "fallback_reason",
                    "fallback_blocked_reason",
                    "live_ret_from_open",
                    "sector_live_ret_pct",
                    "sector_today_flow_pct",
                    "sector_turnover_share",
                    "exclude_spike",
                    "exclude_spike_hard_reject",
                    "exclude_spike_warning_only",
                    "spike_quality",
                    "poor_quality_spike",
                    "material_supported_breakout",
                    "breakout_support_reason",
                    "centrality_score",
                    "liquidity_score",
                    "today_leadership_score",
                    "representative_final_score",
                    "selected_reason",
                    "rep_score_today_strength",
                    "rep_score_relative_strength",
                    "rep_score_liquidity",
                    "representative_score",
                    "rep_score_total",
                    "rep_score_centrality",
                    "rep_score_today_leadership",
                    "rep_score_sanity",
                    "rep_selected_reason",
                    "rep_excluded_reason",
                    "rep_fallback_reason",
                    "representative_selected_reason",
                    "representative_quality_flag",
                    "representative_fallback_reason",
                    "earnings_today_announcement_flag",
                    "earnings_announcement_date",
                    "was_in_selected50",
                    "was_in_must_have",
                    "nikkei_search",
                    "material_link",
                ]
            ]
        )
    if not representative_frames:
        return _empty_sector_representatives_frame()
    return pd.concat(representative_frames, ignore_index=True, sort=False)


def _join_candidate_tags(tags: list[str]) -> str:
    unique_tags = [str(tag).strip() for tag in tags if str(tag).strip()]
    if not unique_tags:
        return ""
    return " / ".join(dict.fromkeys(unique_tags))


def _build_candidate_commentary(reason_tags: str, risk_tags: str) -> str:
    reason_list = [part.strip() for part in str(reason_tags).split("/") if part.strip()]
    risk_list = [part.strip() for part in str(risk_tags).split("/") if part.strip()]
    if reason_list and risk_list:
        return f"{reason_list[0]}、{risk_list[0]}に注意"
    if reason_list:
        return reason_list[0]
    if risk_list:
        return f"{risk_list[0]}に注意"
    return ""


TODAY_EARNINGS_ANNOUNCEMENT_NOTE = "本日決算発表日"


def _format_stock_name_with_marker(name: Any, *, marked: bool, marker: str = TODAY_EARNINGS_ANNOUNCEMENT_NOTE) -> str:
    text = str(name or "").strip()
    if not text or not marked or marker in text:
        return text
    return f"{text}（{marker}）"


def _append_tag_if(base_text: Any, tag: str, *, enabled: bool) -> str:
    text = str(base_text or "").strip()
    if not enabled:
        return text
    if not text:
        return tag
    if tag in text:
        return text
    return f"{text}, {tag}"


def _entry_fit_sort_priority(label: Any) -> int:
    return {
        "買い候補": 0,
        "監視候補": 1,
        "見送り": 2,
    }.get(str(label or "").strip(), 9)


def _build_sector_confidence(score: float) -> str:
    if score >= 3.0:
        return "高"
    if score >= 1.5:
        return "中"
    return "低"


def _build_sector_caution_tags(tags: list[str]) -> str:
    unique_tags = [str(tag).strip() for tag in tags if str(tag).strip()]
    if not unique_tags:
        return ""
    return ", ".join(dict.fromkeys(unique_tags))


def _add_today_sector_rank_audit_fields(
    today_sector_leaderboard: pd.DataFrame,
    representative_pool_with_selection: pd.DataFrame,
    *,
    sector_key_col: str,
) -> pd.DataFrame:
    if today_sector_leaderboard is None or today_sector_leaderboard.empty:
        return today_sector_leaderboard
    working = today_sector_leaderboard.copy()
    pool = representative_pool_with_selection.copy() if isinstance(representative_pool_with_selection, pd.DataFrame) else pd.DataFrame()
    if not pool.empty and sector_key_col in pool.columns:
        live_ret = _coerce_numeric(pool.get("live_ret_vs_prev_close", pd.Series(pd.NA, index=pool.index)))
        pool = pool.copy()
        pool["_audit_live_ret"] = live_ret
        top_ret_map = pool.groupby(sector_key_col)["_audit_live_ret"].max().to_dict()
        top_positive_map = (
            pool[pool["_audit_live_ret"].gt(0.0)]
            .sort_values([sector_key_col, "_audit_live_ret", "code"], ascending=[True, False, True], kind="mergesort")
            .groupby(sector_key_col)
            .apply(lambda group: [{"code": str(row.get("code", "") or ""), "name": str(row.get("name", "") or ""), "live_ret_vs_prev_close": float(row.get("_audit_live_ret", 0.0) or 0.0)} for _, row in group.head(5).iterrows()])
            .to_dict()
        )
    else:
        top_ret_map = {}
        top_positive_map = {}
    positive_count = _coerce_numeric(working.get("sector_positive_candidate_count", pd.Series(0, index=working.index))).fillna(0.0)
    negative_count = _coerce_numeric(working.get("sector_negative_candidate_count", pd.Series(0, index=working.index))).fillna(0.0)
    total_count = (positive_count + negative_count).replace(0, pd.NA)
    working["sector_rank_score"] = _coerce_numeric(working.get("today_sector_score", working.get("intraday_sector_score", pd.Series(pd.NA, index=working.index))))
    working["sector_live_ret"] = _coerce_numeric(working.get("median_live_ret", pd.Series(pd.NA, index=working.index)))
    working["sector_turnover_score"] = _coerce_numeric(working.get("flow_block_score", pd.Series(pd.NA, index=working.index)))
    working["sector_positive_count"] = positive_count.astype(int)
    working["sector_negative_count"] = negative_count.astype(int)
    working["sector_positive_rate"] = _safe_ratio(positive_count, total_count).fillna(0.0)
    working["sector_live_ret_median"] = _coerce_numeric(working.get("median_live_ret", pd.Series(pd.NA, index=working.index)))
    working["sector_top_stock_ret"] = working[sector_key_col].map(top_ret_map) if sector_key_col in working.columns else pd.NA
    working["sector_top_positive_stocks"] = working[sector_key_col].map(top_positive_map).apply(lambda value: value if isinstance(value, list) else []) if sector_key_col in working.columns else [[] for _ in range(len(working))]
    working["sector_rank_reason"] = working.apply(
        lambda row: _join_ui_fragments(
            f"score={_format_ui_number(row.get('sector_rank_score'))}",
            f"live_ret_median={_format_ui_number(row.get('sector_live_ret_median'))}",
            f"turnover_score={_format_ui_number(row.get('sector_turnover_score'))}",
            f"pos={int(row.get('sector_positive_count', 0) or 0)}",
            f"neg={int(row.get('sector_negative_count', 0) or 0)}",
            f"industry_rank={_format_display_rank_value(row.get('industry_rank_live'))}",
            f"score_rank={_format_display_rank_value(row.get('score_rank'))}",
            str(row.get("rank_mode_reason", "") or ""),
        ),
        axis=1,
    )
    return working


def _build_representative_stocks_map(sector_representatives: pd.DataFrame, *, sector_col: str = "sector_name") -> pd.Series:
    if sector_representatives.empty:
        return pd.Series(dtype=object)
    records_by_sector: dict[str, list[dict[str, Any]]] = {}
    ordered = sector_representatives.sort_values([sector_col, "representative_rank"], kind="mergesort")
    for sector_name, group in ordered.groupby(sector_col):
        rows: list[dict[str, Any]] = []
        for _, row in group.head(3).iterrows():
            rows.append(
                {
                    "representative_rank": int(row.get("representative_rank", 0) or 0),
                    "code": str(row.get("code", "") or ""),
                    "name": str(row.get("name", "") or ""),
                    "current_price": float(row.get("current_price", row.get("live_price", 0.0)) or 0.0) if pd.notna(row.get("current_price", row.get("live_price", 0.0))) else None,
                    "current_price_unavailable": bool(row.get("current_price_unavailable", pd.isna(row.get("current_price", row.get("live_price", pd.NA))))),
                    "live_ret_vs_prev_close": float(row.get("live_ret_vs_prev_close", 0.0) or 0.0) if pd.notna(row.get("live_ret_vs_prev_close")) else None,
                    "live_turnover_value": float(row.get("live_turnover_value", row.get("live_turnover", 0.0)) or 0.0) if pd.notna(row.get("live_turnover_value", row.get("live_turnover", 0.0))) else None,
                    "live_turnover_unavailable": bool(row.get("live_turnover_unavailable", pd.isna(row.get("live_turnover_value", row.get("live_turnover", pd.NA))))),
                    "sector_turnover_share_raw": float(row.get("stock_turnover_share_of_sector", 0.0) or 0.0) if pd.notna(row.get("stock_turnover_share_of_sector")) else None,
                    "rep_score_total": float(row.get("rep_score_total", row.get("representative_score", 0.0)) or 0.0) if pd.notna(row.get("rep_score_total", row.get("representative_score", 0.0))) else None,
                    "rep_score_centrality": float(row.get("rep_score_centrality", 0.0) or 0.0) if pd.notna(row.get("rep_score_centrality")) else None,
                    "rep_score_today_leadership": float(row.get("rep_score_today_leadership", 0.0) or 0.0) if pd.notna(row.get("rep_score_today_leadership")) else None,
                    "rep_score_sanity": float(row.get("rep_score_sanity", 0.0) or 0.0) if pd.notna(row.get("rep_score_sanity")) else None,
                    "representative_selected_reason": str(row.get("representative_selected_reason", row.get("rep_selected_reason", "")) or ""),
                    "representative_quality_flag": str(row.get("representative_quality_flag", "") or ""),
                    "representative_fallback_reason": str(row.get("representative_fallback_reason", row.get("rep_fallback_reason", "")) or ""),
                    "representative_gate_pass": bool(row.get("representative_gate_pass", False)),
                    "representative_gate_reason": str(row.get("representative_gate_reason", "") or ""),
                    "hard_block_reason": str(row.get("hard_block_reason", "") or ""),
                    "fallback_used": bool(row.get("fallback_used", False)),
                    "fallback_reason": str(row.get("fallback_reason", "") or ""),
                    "earnings_today_announcement_flag": bool(row.get("earnings_today_announcement_flag", False)),
                    "earnings_announcement_date": str(row.get("earnings_announcement_date", "") or ""),
                    "was_in_selected50": bool(row.get("was_in_selected50", True)),
                    "was_in_must_have": bool(row.get("was_in_must_have", False)),
                }
            )
        records_by_sector[str(sector_name or "")] = rows
    return pd.Series(records_by_sector, dtype=object)


def _apply_sector_cap(frame: pd.DataFrame, *, sector_col: str, limit_per_sector: int, total_limit: int) -> pd.DataFrame:
    if frame.empty:
        return frame
    working = frame.copy()
    working["_sector_slot"] = working.groupby(sector_col).cumcount()
    capped = working[working["_sector_slot"] < limit_per_sector].copy()
    capped = capped.head(total_limit).drop(columns="_sector_slot")
    return capped.reset_index(drop=True)


def _summarize_sector_rank_table(frame: pd.DataFrame, *, limit: int = 10) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    summary: list[dict[str, Any]] = []
    for _, row in frame.head(limit).iterrows():
        display_rank = int(row.get("today_display_rank", row.get("today_rank", 0)) or 0)
        summary.append(
            {
                "today_rank": display_rank,
                "today_display_rank": display_rank,
                "sector_name": str(row.get("sector_name", "") or ""),
                "breadth": str(row.get("breadth", "") or ""),
                "sector_confidence": str(row.get("sector_confidence", "") or ""),
                "sector_caution": str(row.get("sector_caution", "") or ""),
                "industry_anchor_rank": int(row.get("industry_anchor_rank", 0) or 0),
                "today_rank_mode": str(row.get("today_rank_mode", "") or ""),
            }
        )
    return summary


def _summarize_sector_rank_changes(before: pd.DataFrame, after: pd.DataFrame, *, limit: int = 10) -> list[dict[str, Any]]:
    if before.empty or after.empty:
        return []
    before_map = {str(row.get("sector_name", "") or ""): int(row.get("today_display_rank", row.get("today_rank", 0)) or 0) for _, row in before.iterrows()}
    changes: list[dict[str, Any]] = []
    for _, row in after.head(limit).iterrows():
        sector_name = str(row.get("sector_name", "") or "")
        after_rank = int(row.get("today_display_rank", row.get("today_rank", 0)) or 0)
        before_rank = before_map.get(sector_name)
        if before_rank is None:
            changes.append({"sector_name": sector_name, "before_rank": None, "after_rank": after_rank})
        elif before_rank != after_rank:
            changes.append({"sector_name": sector_name, "before_rank": before_rank, "after_rank": after_rank})
    return changes


def _summarize_industry_anchor_positions(industry_df: pd.DataFrame, today_sector_leaderboard: pd.DataFrame, *, anchor_ranks: list[int] | None = None) -> list[dict[str, Any]]:
    if industry_df.empty:
        return []
    anchor_ranks = anchor_ranks or [2, 3, 4]
    industry_base = industry_df.copy()
    industry_base["sector_name"] = industry_base.get("sector_name", pd.Series(dtype=str)).map(_normalize_industry_name)
    industry_base = industry_base[industry_base["sector_name"].astype(str).str.strip() != ""].copy()
    if "rank_position" not in industry_base.columns:
        industry_base["rank_position"] = pd.Series(range(1, len(industry_base) + 1), index=industry_base.index, dtype="int64")
    industry_base = industry_base.drop_duplicates("sector_name")
    leaderboard_map = today_sector_leaderboard.set_index("sector_name") if not today_sector_leaderboard.empty and "sector_name" in today_sector_leaderboard.columns else pd.DataFrame()
    summary: list[dict[str, Any]] = []
    for anchor_rank in anchor_ranks:
        matched = industry_base[_coerce_numeric(industry_base["rank_position"]).eq(float(anchor_rank))]
        if matched.empty:
            continue
        industry_row = matched.iloc[0]
        sector_name = str(industry_row.get("sector_name", "") or "")
        current_row = leaderboard_map.loc[sector_name] if isinstance(leaderboard_map, pd.DataFrame) and sector_name in leaderboard_map.index else None
        current_today_rank = int(current_row.get("today_display_rank", current_row.get("today_rank", 0)) or 0) if current_row is not None else None
        summary.append(
            {
                "sector_name": sector_name,
                "industry_up_rank": int(anchor_rank),
                "industry_up_value": float(industry_row.get("industry_up_value", 0.0) or 0.0) if pd.notna(industry_row.get("industry_up_value")) else None,
                "today_rank": current_today_rank,
                "today_display_rank": current_today_rank,
                "original_industry_rank_live": int(current_row.get("original_industry_rank_live", anchor_rank) or 0) if current_row is not None and pd.notna(current_row.get("original_industry_rank_live")) else int(anchor_rank),
                "anchor_rank_source": str(current_row.get("anchor_rank_source", "industry_up.rank_position") or "") if current_row is not None else "industry_up.rank_position",
                "rank_delta_vs_industry": (current_today_rank - int(anchor_rank)) if current_today_rank is not None else None,
                "tethered_rank": float(current_row.get("tethered_rank", 0.0) or 0.0) if current_row is not None and pd.notna(current_row.get("tethered_rank")) else None,
                "score_rank": int(current_row.get("score_rank", 0) or 0) if current_row is not None and pd.notna(current_row.get("score_rank")) else None,
                "industry_anchor_rank": int(current_row.get("industry_anchor_rank", 0) or 0) if current_row is not None and pd.notna(current_row.get("industry_anchor_rank")) else int(anchor_rank),
                "allowed_shift": int(current_row.get("allowed_shift", 0) or 0) if current_row is not None and pd.notna(current_row.get("allowed_shift")) else None,
                "rank_shift_limit": int(current_row.get("rank_shift_limit", 0) or 0) if current_row is not None and pd.notna(current_row.get("rank_shift_limit")) else None,
                "today_rank_mode": str(current_row.get("today_rank_mode", "") or "") if current_row is not None else "",
                "rank_mode_reason": str(current_row.get("rank_mode_reason", "") or "") if current_row is not None else "",
                "max_upshift": int(current_row.get("max_upshift", 0) or 0) if current_row is not None and pd.notna(current_row.get("max_upshift")) else None,
                "upshift_blocked_reason": str(current_row.get("upshift_blocked_reason", "") or "") if current_row is not None else "",
                "rank_constraint_applied": bool(current_row.get("rank_constraint_applied", False)) if current_row is not None else False,
                "final_rank_delta": float(current_row.get("final_rank_delta", 0.0) or 0.0) if current_row is not None and pd.notna(current_row.get("final_rank_delta")) else None,
                "scan_member_count": int(current_row.get("scan_member_count", 0) or 0) if current_row is not None else 0,
                "wide_scan_member_count": int(current_row.get("wide_scan_member_count", 0) or 0) if current_row is not None else 0,
                "ranking_confirmed_count": int(current_row.get("ranking_confirmed_count", 0) or 0) if current_row is not None else 0,
                "ranking_source_breadth_ex_basket": int(current_row.get("ranking_source_breadth_ex_basket", 0) or 0) if current_row is not None else 0,
                "present_in_live_industry_table": bool(current_row.get("present_in_live_industry_table", False)) if current_row is not None else False,
                "present_in_sector_summary_before_filter": bool(current_row.get("present_in_sector_summary_before_filter", False)) if current_row is not None else False,
                "present_in_today_display_universe": bool(current_row.get("present_in_today_display_universe", False)) if current_row is not None else False,
                "display_eligible": bool(current_row.get("display_eligible", False)) if current_row is not None else False,
                "display_excluded_reason": str(current_row.get("display_excluded_reason", "") or "") if current_row is not None else "",
                "scan_sample_warning_level": str(current_row.get("scan_sample_warning_level", "") or "") if current_row is not None else "",
                "scan_sample_warning_reason": str(current_row.get("scan_sample_warning_reason", "") or "") if current_row is not None else "",
            }
        )
    return summary


def _summarize_representative_table(frame: pd.DataFrame, *, sector_names: list[str] | None = None, limit_sectors: int = 5) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    working = frame.copy()
    if sector_names:
        working = working[working["sector_name"].astype(str).isin([str(name) for name in sector_names])].copy()
    if working.empty:
        return []
    summary: list[dict[str, Any]] = []
    for sector_name, group in working.sort_values(["sector_name", "representative_rank"]).groupby("sector_name"):
        summary.append(
            {
                "sector_name": str(sector_name or ""),
                "leaders": " / ".join(group["name"].astype(str).head(3).tolist()),
            }
        )
        if len(summary) >= limit_sectors:
            break
    return summary


def _summarize_candidate_table(frame: pd.DataFrame, *, rank_col: str, limit: int = 5) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    summary: list[dict[str, Any]] = []
    for _, row in frame.head(limit).iterrows():
        summary.append(
            {
                rank_col: int(row.get(rank_col, 0) or 0),
                "name": str(row.get("name", "") or ""),
                "sector_name": str(row.get("sector_name", "") or ""),
                "entry_fit": str(row.get("entry_fit", "") or ""),
                "candidate_quality": str(row.get("candidate_quality", "") or ""),
            }
        )
    return summary


def _entry_fit_1w_label(*, candidate_quality: str, belongs_today_sector: bool, sector_confidence: str, flow_ok: bool, rs_ok: bool, liquidity_ok: bool, earnings_risk_flag: bool, extension_flag: bool) -> str:
    if str(candidate_quality) == "低":
        return "見送り"
    if earnings_risk_flag:
        return "見送り"
    if not (belongs_today_sector and flow_ok and rs_ok and liquidity_ok):
        return "見送り"
    if extension_flag:
        return "監視候補"
    if str(candidate_quality) == "高" and str(sector_confidence) == "高":
        return "買い候補"
    return "監視候補"


def _entry_fit_1m_label(*, candidate_quality: str, belongs_persistence_sector: bool, sector_confidence: str, medium_term_rs_ok: bool, liquidity_ok: bool, earnings_risk_flag: bool, extension_flag: bool, finance_risk_flag: bool) -> str:
    if str(candidate_quality) == "低":
        return "見送り"
    if earnings_risk_flag or finance_risk_flag:
        return "見送り"
    if not (belongs_persistence_sector and medium_term_rs_ok and liquidity_ok):
        return "見送り"
    if extension_flag:
        return "監視候補"
    if str(candidate_quality) == "高" and str(sector_confidence) in {"高", "中"}:
        return "買い候補"
    return "監視候補"


def _build_swing_candidate_tables(
    merged: pd.DataFrame,
    today_sector_leaderboard: pd.DataFrame,
    persistence_tables: dict[str, pd.DataFrame],
    *,
    selection_config: dict[str, float] | None = None,
) -> dict[str, pd.DataFrame]:
    if merged.empty:
        empty = pd.DataFrame()
        return {"1w": empty, "1m": empty}
    selection_config = selection_config or SWING_SELECTION_CONFIG
    sorted_today_sector_leaderboard = _sort_today_sector_leaderboard_for_display(today_sector_leaderboard)
    working = merged.copy()
    earnings_days_raw = _coerce_numeric(working.get("earnings_buffer_days", pd.Series([pd.NA] * len(working))))
    earnings_days = earnings_days_raw.fillna(999)
    earnings_data_available = bool(earnings_days_raw.notna().any())
    finance_score_raw = _coerce_numeric(working.get("finance_health_score", pd.Series([pd.NA] * len(working))))
    finance_score = finance_score_raw.fillna(0.0)
    working["earnings_proximity_flag"] = earnings_days_raw.lt(7).fillna(False)
    turnover_floor = float(_coerce_numeric(working["avg_turnover_20d"]).median(skipna=True) or 0.0)
    volume_floor = float(_coerce_numeric(working["avg_volume_20d"]).median(skipna=True) or 0.0)
    top_today_sectors = set(sorted_today_sector_leaderboard.head(6)["sector_name"].astype(str).tolist()) if not sorted_today_sector_leaderboard.empty else set()
    top_1m_sectors = set(persistence_tables.get("1m", pd.DataFrame()).head(8)["sector_name"].astype(str).tolist())
    top_3m_sectors = set(persistence_tables.get("3m", pd.DataFrame()).head(8)["sector_name"].astype(str).tolist())
    today_sector_conf_map = sorted_today_sector_leaderboard.set_index("sector_name")["sector_confidence"] if not sorted_today_sector_leaderboard.empty and "sector_confidence" in sorted_today_sector_leaderboard.columns else pd.Series(dtype=str)
    persistence_conf_frames = [persistence_tables.get(key, pd.DataFrame()) for key in ["1m", "3m"]]
    persistence_conf_source = pd.concat([frame[["sector_name", "sector_confidence"]] for frame in persistence_conf_frames if not frame.empty and "sector_confidence" in frame.columns], ignore_index=True).drop_duplicates("sector_name") if any(not frame.empty and "sector_confidence" in frame.columns for frame in persistence_conf_frames) else pd.DataFrame(columns=["sector_name", "sector_confidence"])
    persistence_sector_conf_map = persistence_conf_source.set_index("sector_name")["sector_confidence"] if not persistence_conf_source.empty else pd.Series(dtype=str)
    working["belongs_today_sector"] = working["sector_name"].astype(str).isin(top_today_sectors)
    working["belongs_persistence_sector"] = working["sector_name"].astype(str).isin(top_1m_sectors | top_3m_sectors)
    working["sector_confidence_1w"] = working["sector_name"].map(today_sector_conf_map).fillna("")
    working["sector_confidence_1m"] = working["sector_name"].map(persistence_sector_conf_map).fillna("")
    working["sector_confidence_priority_1w"] = working["sector_confidence_1w"].map(SECTOR_CONFIDENCE_PRIORITY).fillna(0).astype(int)
    working["sector_confidence_priority_1m"] = working["sector_confidence_1m"].map(SECTOR_CONFIDENCE_PRIORITY).fillna(0).astype(int)
    working["liquidity_ok"] = (_coerce_numeric(working["avg_turnover_20d"]).fillna(0.0) >= turnover_floor) & (_coerce_numeric(working["avg_volume_20d"]).fillna(0.0) >= volume_floor)
    working["liquidity_pass"] = working["liquidity_ok"]
    extension_threshold_1w = float(selection_config.get("extension_threshold_1w", 12.0) or 12.0)
    extension_threshold_1m = float(selection_config.get("extension_threshold_1m", 12.0) or 12.0)
    working["price_vs_ma20_abs"] = _coerce_numeric(working["price_vs_ma20_pct"]).abs()
    working["extension_flag_1w"] = working["price_vs_ma20_abs"].gt(extension_threshold_1w).fillna(False)
    working["extension_flag_1m"] = working["price_vs_ma20_abs"].gt(extension_threshold_1m).fillna(False)
    working["extension_flag"] = working["extension_flag_1w"] | working["extension_flag_1m"]
    working["ma20_band_pass"] = _coerce_numeric(working["price_vs_ma20_pct"]).between(-8.0, 15.0, inclusive="both") | _coerce_numeric(working["price_vs_ma20_pct"]).isna()
    working["earnings_unknown_flag"] = earnings_days_raw.isna() & earnings_data_available
    working["earnings_risk_flag"] = earnings_days_raw.lt(7).fillna(False)
    working["finance_risk_flag"] = finance_score_raw.lt(-1.0).fillna(False)
    working["finance_health_guard"] = ~working["finance_risk_flag"]
    working["finance_health_flag"] = finance_score_raw.apply(lambda value: "不明" if pd.isna(value) else ("無難" if float(value) >= -1.0 else "注意"))
    working["rs_ok"] = _coerce_numeric(working["rs_vs_topix_1w"]).gt(0.0).fillna(False)
    working["flow_ok"] = (_coerce_numeric(working["live_turnover_ratio_20d"]).fillna(0.0) >= 1.2) | (_coerce_numeric(working["live_volume_ratio_20d"]).fillna(0.0) >= 1.2)
    working["swing_pass_1w"] = (
        working["belongs_today_sector"]
        & working["liquidity_ok"]
        & working["flow_ok"]
        & working["rs_ok"]
        & ~working["earnings_risk_flag"]
        & ~working["extension_flag_1w"]
    )
    working["candidate_sector_component_1w"] = working["belongs_today_sector"].astype(float) * 1.0
    working["candidate_turnover_component_1w"] = _score_percentile(working["live_turnover"]) * 1.0
    working["candidate_volume_component_1w"] = _score_percentile(working["live_volume_ratio_20d"]) * 0.95
    working["candidate_price_component_1w"] = _score_percentile(working["live_ret_vs_prev_close"]) * 0.95
    working["candidate_rs_component_1w"] = _score_percentile(working["rs_vs_topix_1w"]) * 1.0
    working["candidate_liquidity_component_1w"] = _score_percentile(working["avg_turnover_20d"]) * 0.7
    working["candidate_earnings_component_1w"] = 0.0
    working["swing_score_1w"] = (
        working["candidate_sector_component_1w"]
        + working["candidate_turnover_component_1w"]
        + working["candidate_volume_component_1w"]
        + working["candidate_price_component_1w"]
        + working["candidate_rs_component_1w"]
        + working["candidate_liquidity_component_1w"]
        + working["candidate_earnings_component_1w"]
    )
    working.loc[working["sector_confidence_1w"].eq("高"), "swing_score_1w"] += float(selection_config.get("sector_confidence_bonus_high_1w", 0.0) or 0.0)
    working.loc[working["sector_confidence_1w"].eq("中"), "swing_score_1w"] += float(selection_config.get("sector_confidence_bonus_mid_1w", 0.0) or 0.0)
    working["medium_term_rs_ok"] = (_coerce_numeric(working["rs_vs_topix_1m"]).gt(0.0) & _coerce_numeric(working["rs_vs_topix_3m"]).gt(0.0)).fillna(False)
    working["swing_pass_1m"] = (
        working["belongs_persistence_sector"]
        & working["medium_term_rs_ok"]
        & working["liquidity_ok"]
        & ~working["earnings_risk_flag"]
        & ~working["extension_flag_1m"]
        & ~working["finance_risk_flag"]
    )
    working["candidate_sector_component_1m"] = working["belongs_persistence_sector"].astype(float) * 1.0
    working["candidate_rs_component_1m"] = _score_percentile(working["rs_vs_topix_1m"]) * 1.0
    working["candidate_rs_component_3m"] = _score_percentile(working["rs_vs_topix_3m"]) * 0.9
    working["candidate_ma20_component_1m"] = (1.0 - _score_percentile(working["price_vs_ma20_abs"])) * float(selection_config.get("candidate_ma20_weight_1m", 0.35) or 0.35)
    working["candidate_liquidity_component_1m"] = _score_percentile(working["avg_turnover_20d"]) * 0.7
    working["candidate_sector_rank_component_1m"] = _score_rank_ascending(working["sector_rank_1m"]) * 0.8
    working["candidate_sector_rank_component_3m"] = _score_rank_ascending(working["sector_rank_3m"]) * 0.8
    working["candidate_earnings_component_1m"] = 0.0
    working["candidate_finance_component_1m"] = 0.0
    working.loc[finance_score >= 0.0, "candidate_finance_component_1m"] = 0.4
    working.loc[finance_score < -1.0, "candidate_finance_component_1m"] = -0.6
    working["swing_score_1m"] = (
        working["candidate_sector_component_1m"]
        + working["candidate_rs_component_1m"]
        + working["candidate_rs_component_3m"]
        + working["candidate_ma20_component_1m"]
        + working["candidate_liquidity_component_1m"]
        + working["candidate_sector_rank_component_1m"]
        + working["candidate_sector_rank_component_3m"]
        + working["candidate_earnings_component_1m"]
        + working["candidate_finance_component_1m"]
    )
    working.loc[working["sector_confidence_1m"].eq("高"), "swing_score_1m"] += float(selection_config.get("sector_confidence_bonus_high_1m", 0.0) or 0.0)
    working.loc[working["sector_confidence_1m"].eq("中"), "swing_score_1m"] += float(selection_config.get("sector_confidence_bonus_mid_1m", 0.0) or 0.0)
    working["selection_reason_1w"] = working.apply(
        lambda row: _join_candidate_tags(
            [
                "今日強セクター" if bool(row.get("belongs_today_sector")) else "",
                "当日資金流入" if bool(row.get("flow_ok")) else "",
                "1週RS強い" if bool(row.get("rs_ok")) else "",
                "流動性十分" if bool(row.get("liquidity_ok")) else "",
            ]
        ),
        axis=1,
    )
    working["risk_note_1w"] = working.apply(
        lambda row: _join_candidate_tags(
            [
                TODAY_EARNINGS_ANNOUNCEMENT_NOTE if bool(row.get("earnings_today_announcement_flag")) else "",
                "伸び過ぎ" if bool(row.get("extension_flag_1w")) else "",
                "流動性弱い" if not bool(row.get("liquidity_ok")) else "",
            ]
        ),
        axis=1,
    )
    working["candidate_commentary_1w"] = working.apply(
        lambda row: _build_candidate_commentary(row.get("selection_reason_1w", ""), row.get("risk_note_1w", "")),
        axis=1,
    )
    working["selection_reason_1m"] = working.apply(
        lambda row: _join_candidate_tags(
            [
                "1か月RS強い" if float(row.get("rs_vs_topix_1m", 0.0) or 0.0) > 0 else "",
                "3か月RS強い" if float(row.get("rs_vs_topix_3m", 0.0) or 0.0) > 0 else "",
                "強セクター所属" if bool(row.get("belongs_persistence_sector")) else "",
                "財務無難" if not bool(row.get("finance_risk_flag")) and str(row.get("finance_health_flag", "")) != "不明" else "",
                "流動性十分" if bool(row.get("liquidity_ok")) else "",
            ]
        ),
        axis=1,
    )
    working["risk_note_1m"] = working.apply(
        lambda row: _join_candidate_tags(
            [
                TODAY_EARNINGS_ANNOUNCEMENT_NOTE if bool(row.get("earnings_today_announcement_flag")) else "",
                "20日線乖離大" if bool(row.get("extension_flag_1m")) else "",
                "財務不安" if bool(row.get("finance_risk_flag")) else "",
                "流動性弱い" if not bool(row.get("liquidity_ok")) else "",
            ]
        ),
        axis=1,
    )
    working["candidate_commentary_1m"] = working.apply(
        lambda row: _build_candidate_commentary(row.get("selection_reason_1m", ""), row.get("risk_note_1m", "")),
        axis=1,
    )
    working["candidate_quality_score_1w"] = 0.0
    working.loc[working["swing_pass_1w"], "candidate_quality_score_1w"] += 2.0
    working.loc[working["flow_ok"], "candidate_quality_score_1w"] += 1.0
    working.loc[working["rs_ok"], "candidate_quality_score_1w"] += 1.0
    working.loc[working["liquidity_ok"], "candidate_quality_score_1w"] += 0.5
    working.loc[working["sector_confidence_1w"].eq("高"), "candidate_quality_score_1w"] += float(selection_config.get("sector_confidence_bonus_high_1w", 0.0) or 0.0)
    working.loc[working["sector_confidence_1w"].eq("中"), "candidate_quality_score_1w"] += float(selection_config.get("sector_confidence_bonus_mid_1w", 0.0) or 0.0)
    working.loc[working["earnings_unknown_flag"], "candidate_quality_score_1w"] -= 0.25
    working.loc[working["earnings_risk_flag"], "candidate_quality_score_1w"] -= 1.0
    working.loc[working["extension_flag_1w"], "candidate_quality_score_1w"] -= 0.75
    working["candidate_quality_1w"] = "低"
    working.loc[working["candidate_quality_score_1w"] >= 4.0, "candidate_quality_1w"] = "高"
    working.loc[(working["candidate_quality_score_1w"] >= 2.5) & (working["candidate_quality_score_1w"] < 4.0), "candidate_quality_1w"] = "中"
    working["candidate_quality_score_1m"] = 0.0
    working.loc[working["swing_pass_1m"], "candidate_quality_score_1m"] += 2.0
    working.loc[working["medium_term_rs_ok"], "candidate_quality_score_1m"] += 1.0
    working.loc[working["liquidity_ok"], "candidate_quality_score_1m"] += 0.5
    working.loc[~working["finance_risk_flag"], "candidate_quality_score_1m"] += 0.5
    working.loc[working["sector_confidence_1m"].eq("高"), "candidate_quality_score_1m"] += float(selection_config.get("sector_confidence_bonus_high_1m", 0.0) or 0.0)
    working.loc[working["sector_confidence_1m"].eq("中"), "candidate_quality_score_1m"] += float(selection_config.get("sector_confidence_bonus_mid_1m", 0.0) or 0.0)
    working.loc[working["earnings_unknown_flag"], "candidate_quality_score_1m"] -= 0.25
    working.loc[working["finance_health_flag"].eq("不明"), "candidate_quality_score_1m"] -= 0.15
    working.loc[working["earnings_risk_flag"], "candidate_quality_score_1m"] -= 1.0
    working.loc[working["extension_flag_1m"], "candidate_quality_score_1m"] -= 0.75
    working.loc[working["finance_risk_flag"], "candidate_quality_score_1m"] -= 1.0
    working["candidate_quality_1m"] = "低"
    working.loc[working["candidate_quality_score_1m"] >= 4.0, "candidate_quality_1m"] = "高"
    working.loc[(working["candidate_quality_score_1m"] >= 2.5) & (working["candidate_quality_score_1m"] < 4.0), "candidate_quality_1m"] = "中"
    working["entry_fit_1w"] = working.apply(
        lambda row: _entry_fit_1w_label(
            candidate_quality=str(row.get("candidate_quality_1w", "")),
            belongs_today_sector=bool(row.get("belongs_today_sector")),
            sector_confidence=str(row.get("sector_confidence_1w", "")),
            flow_ok=bool(row.get("flow_ok")),
            rs_ok=bool(row.get("rs_ok")),
            liquidity_ok=bool(row.get("liquidity_ok")),
            earnings_risk_flag=bool(row.get("earnings_risk_flag")),
            extension_flag=bool(row.get("extension_flag_1w")),
        ),
        axis=1,
    )
    working["entry_fit_1m"] = working.apply(
        lambda row: _entry_fit_1m_label(
            candidate_quality=str(row.get("candidate_quality_1m", "")),
            belongs_persistence_sector=bool(row.get("belongs_persistence_sector")),
            sector_confidence=str(row.get("sector_confidence_1m", "")),
            medium_term_rs_ok=bool(row.get("medium_term_rs_ok")),
            liquidity_ok=bool(row.get("liquidity_ok")),
            earnings_risk_flag=bool(row.get("earnings_risk_flag")),
            extension_flag=bool(row.get("extension_flag_1m")),
            finance_risk_flag=bool(row.get("finance_risk_flag")),
        ),
        axis=1,
    )
    working["candidate_commentary_1w"] = working.apply(
        lambda row: (
            "決算近く様子見" if bool(row.get("earnings_risk_flag")) else
            "強いが伸び過ぎ" if bool(row.get("extension_flag_1w")) else
            "強セクター・資金流入継続" if str(row.get("entry_fit_1w", "")) == "買い候補" else
            "強いが待ち伏せ" if str(row.get("entry_fit_1w", "")) == "監視候補" else
            _build_candidate_commentary(row.get("selection_reason_1w", ""), row.get("risk_note_1w", ""))
        ),
        axis=1,
    )
    working["candidate_commentary_1m"] = working.apply(
        lambda row: (
            "決算近く様子見" if bool(row.get("earnings_risk_flag")) else
            "財務懸念で様子見" if bool(row.get("finance_risk_flag")) else
            "中期強いが乖離大" if bool(row.get("extension_flag_1m")) else
            "中期上昇継続" if str(row.get("entry_fit_1m", "")) == "買い候補" else
            "強いが押し目待ち" if str(row.get("entry_fit_1m", "")) == "監視候補" else
            _build_candidate_commentary(row.get("selection_reason_1m", ""), row.get("risk_note_1m", ""))
        ),
        axis=1,
    )
    working["entry_fit_priority_1w"] = working["entry_fit_1w"].map(_entry_fit_sort_priority)
    working["entry_fit_priority_1m"] = working["entry_fit_1m"].map(_entry_fit_sort_priority)
    swing_1w = (
        working[
            working["candidate_quality_1w"].isin(["高", "中"])
            & working["entry_fit_1w"].isin(["買い候補", "監視候補"])
        ]
        .sort_values(
            ["entry_fit_priority_1w", "sector_confidence_priority_1w", "candidate_quality_score_1w", "price_vs_ma20_abs", "swing_score_1w", "rs_vs_topix_1w", "live_turnover", "live_ret_vs_prev_close"],
            ascending=[True, False, False, True, False, False, False, False],
        )[
            [
                "code",
                "name",
                "sector_name",
                "candidate_quality_1w",
                "entry_fit_1w",
                "selection_reason_1w",
                "risk_note_1w",
                "candidate_commentary_1w",
                "swing_score_1w",
                "rs_vs_topix_1w",
                "live_ret_vs_prev_close",
                "live_turnover",
                "earnings_buffer_days",
                "nikkei_search",
                "material_link",
            ]
        ]
        .reset_index(drop=True)
        .rename(
            columns={
                "candidate_quality_1w": "candidate_quality",
                "entry_fit_1w": "entry_fit",
                "selection_reason_1w": "selection_reason",
                "risk_note_1w": "risk_note",
                "candidate_commentary_1w": "candidate_commentary",
            }
        )
    )
    swing_1w = _apply_sector_cap(swing_1w, sector_col="sector_name", limit_per_sector=2, total_limit=6)
    swing_1m = (
        working[
            working["candidate_quality_1m"].isin(["高", "中"])
            & working["entry_fit_1m"].isin(["買い候補", "監視候補"])
        ]
        .sort_values(
            ["entry_fit_priority_1m", "sector_confidence_priority_1m", "candidate_quality_score_1m", "price_vs_ma20_abs", "swing_score_1m", "rs_vs_topix_1m", "rs_vs_topix_3m", "TradingValue_latest"],
            ascending=[True, False, False, True, False, False, False, False],
        )[
            [
                "code",
                "name",
                "sector_name",
                "candidate_quality_1m",
                "entry_fit_1m",
                "selection_reason_1m",
                "risk_note_1m",
                "candidate_commentary_1m",
                "swing_score_1m",
                "rs_vs_topix_1m",
                "rs_vs_topix_3m",
                "price_vs_ma20_pct",
                "earnings_buffer_days",
                "finance_health_flag",
                "nikkei_search",
                "material_link",
            ]
        ]
        .reset_index(drop=True)
        .rename(
            columns={
                "candidate_quality_1m": "candidate_quality",
                "entry_fit_1m": "entry_fit",
                "selection_reason_1m": "selection_reason",
                "risk_note_1m": "risk_note",
                "candidate_commentary_1m": "candidate_commentary",
            }
        )
    )
    swing_1m = _apply_sector_cap(swing_1m, sector_col="sector_name", limit_per_sector=2, total_limit=6)
    swing_buy_1w = swing_1w[swing_1w["entry_fit"].eq("買い候補")].head(5).reset_index(drop=True) if not swing_1w.empty else pd.DataFrame(columns=swing_1w.columns)
    swing_watch_1w = swing_1w[swing_1w["entry_fit"].eq("監視候補")].head(5).reset_index(drop=True) if not swing_1w.empty else pd.DataFrame(columns=swing_1w.columns)
    swing_buy_1m = swing_1m[swing_1m["entry_fit"].eq("買い候補")].head(5).reset_index(drop=True) if not swing_1m.empty else pd.DataFrame(columns=swing_1m.columns)
    swing_watch_1m = swing_1m[swing_1m["entry_fit"].eq("監視候補")].head(5).reset_index(drop=True) if not swing_1m.empty else pd.DataFrame(columns=swing_1m.columns)
    if not swing_1w.empty:
        swing_1w.insert(0, "candidate_rank_1w", range(1, len(swing_1w) + 1))
    if not swing_buy_1w.empty:
        swing_buy_1w["candidate_rank_1w"] = range(1, len(swing_buy_1w) + 1)
    if not swing_watch_1w.empty:
        swing_watch_1w["candidate_rank_1w"] = range(1, len(swing_watch_1w) + 1)
    if not swing_1m.empty:
        swing_1m.insert(0, "candidate_rank_1m", range(1, len(swing_1m) + 1))
    if not swing_buy_1m.empty:
        swing_buy_1m["candidate_rank_1m"] = range(1, len(swing_buy_1m) + 1)
    if not swing_watch_1m.empty:
        swing_watch_1m["candidate_rank_1m"] = range(1, len(swing_watch_1m) + 1)
    return {
        "1w": swing_1w,
        "1m": swing_1m,
        "buy_1w": swing_buy_1w,
        "watch_1w": swing_watch_1w,
        "buy_1m": swing_buy_1m,
        "watch_1m": swing_watch_1m,
    }


def _entry_fit_1w_label_v2(
    *,
    candidate_quality: str,
    pass_score_gate: bool,
    pass_live_gate: bool,
    pass_trend_gate: bool,
    pass_flow_gate: bool,
    pass_quality_gate: bool,
    hard_block_reason: str,
    extension_flag: bool,
    sector_confidence: str,
    today_not_broken: bool,
    intraday_fade: bool,
    one_week_edge: bool,
    medium_term_not_broken: bool,
    chase_risk: bool,
    live_ret_vs_prev_close: float,
) -> str:
    quality = str(candidate_quality)
    strong_quality = quality == "高"
    medium_quality = quality == "中"
    quality_ok_for_buy = strong_quality or (medium_quality and pass_live_gate and one_week_edge and medium_term_not_broken)
    chase_caution_ok = (not chase_risk) or (strong_quality and float(live_ret_vs_prev_close or 0.0) <= 4.5)
    if str(hard_block_reason).strip():
        return "見送り"
    if quality == "低":
        return "見送り"
    if not (pass_quality_gate and pass_score_gate and pass_trend_gate):
        return "見送り"
    if (
        pass_live_gate
        and pass_trend_gate
        and pass_flow_gate
        and str(sector_confidence) in {"高", "中"}
        and today_not_broken
        and not intraday_fade
        and one_week_edge
        and medium_term_not_broken
        and quality_ok_for_buy
        and chase_caution_ok
    ):
        return "買い候補"
    if pass_quality_gate and pass_score_gate and (pass_live_gate or pass_trend_gate):
        return "監視候補"
    return "見送り"


def _entry_fit_1m_label_v2(
    *,
    candidate_quality: str,
    belongs_persistence_sector: bool,
    pass_live_gate: bool,
    pass_trend_gate: bool,
    pass_flow_gate: bool,
    pass_quality_gate: bool,
    hard_block_reason: str,
    extension_flag: bool,
    sector_confidence: str,
) -> str:
    if str(hard_block_reason).strip():
        return "見送り"
    if str(candidate_quality) == "低":
        return "見送り"
    if not (belongs_persistence_sector and pass_quality_gate and pass_trend_gate):
        return "見送り"
    if pass_trend_gate and pass_flow_gate and str(candidate_quality) == "高" and str(sector_confidence) in {"高", "中"} and not extension_flag:
        return "買い候補"
    if pass_quality_gate and (pass_trend_gate or pass_live_gate):
        return "監視候補"
    return "見送り"


def _entry_fit_3m_label_v2(
    *,
    candidate_quality: str,
    belongs_persistence_sector: bool,
    pass_live_gate: bool,
    pass_trend_gate: bool,
    pass_flow_gate: bool,
    pass_quality_gate: bool,
    hard_block_reason: str,
    extension_flag: bool,
    sector_confidence: str,
) -> str:
    if str(hard_block_reason).strip():
        return "見送り"
    if str(candidate_quality) == "低":
        return "見送り"
    if not (belongs_persistence_sector and pass_quality_gate and pass_trend_gate and pass_live_gate):
        return "見送り"
    if pass_trend_gate and pass_flow_gate and str(candidate_quality) == "高" and str(sector_confidence) in {"高", "中"} and not extension_flag:
        return "買い候補"
    if pass_quality_gate and pass_trend_gate and pass_live_gate:
        return "監視候補"
    return "見送り"


def _join_reason_tags(tags: list[str], *, fallback: str = "") -> str:
    text = _join_candidate_tags(tags)
    return text or str(fallback or "").strip()


def _hard_block_reason_1w_v2(row: pd.Series) -> str:
    reasons: list[str] = []
    if bool(row.get("current_price_unavailable")):
        reasons.append("no_live_price")
    if bool(row.get("severe_extension_flag_1w")):
        reasons.append("severe_extension")
    return "|".join(reasons)


def _hard_block_reason_1m_v2(row: pd.Series) -> str:
    reasons: list[str] = []
    if bool(row.get("finance_risk_flag")):
        reasons.append("finance_risk")
    if bool(row.get("current_price_unavailable")):
        reasons.append("no_live_price")
    if bool(row.get("severe_extension_flag_1m")):
        reasons.append("severe_extension")
    return "|".join(reasons)


def _hard_block_reason_3m_v2(row: pd.Series) -> str:
    reasons: list[str] = []
    if bool(row.get("finance_risk_flag")):
        reasons.append("finance_risk")
    if bool(row.get("current_price_unavailable")):
        reasons.append("no_live_price")
    if bool(row.get("severe_extension_flag_1m")):
        reasons.append("severe_extension")
    return "|".join(dict.fromkeys(reasons))


def _swing_reason_1w_v2(row: pd.Series) -> str:
    tags: list[str] = []
    if str(row.get("sector_tailwind_band_1w", "") or "") in {"strong", "mid"}:
        tags.append("1wセクター強い")
    if bool(row.get("today_not_broken_1w")):
        tags.append("todayも崩れず")
    if bool(row.get("pass_flow_gate_1w")):
        tags.append("出来高あり")
    if bool(row.get("one_week_edge_1w")):
        tags.append("TOPIX比で強い")
    if bool(row.get("medium_term_not_broken_1w")):
        tags.append("1m崩れ小")
    if bool(row.get("intraday_fade_flag_1w")):
        tags.append("押し待ち")
    return _join_reason_tags(tags, fallback="1w条件はあるが追認弱め")


def _swing_reason_1m_v2(row: pd.Series) -> str:
    rs_1m = float(_coerce_numeric(pd.Series([row.get("rs_vs_topix_1m", pd.NA)])).fillna(0.0).iloc[0] or 0.0)
    rs_3m = float(_coerce_numeric(pd.Series([row.get("rs_vs_topix_3m", pd.NA)])).fillna(0.0).iloc[0] or 0.0)
    price_vs_ma20_abs = float(_coerce_numeric(pd.Series([row.get("price_vs_ma20_abs", pd.NA)])).fillna(999.0).iloc[0] or 999.0)
    live_ret = float(_coerce_numeric(pd.Series([row.get("live_ret_vs_prev_close", pd.NA)])).fillna(0.0).iloc[0] or 0.0)
    tags: list[str] = []
    if bool(row.get("pass_trend_gate_1m")) and rs_1m > 0.0 and rs_3m > 0.0:
        tags.append("1か月トレンド継続")
    elif bool(row.get("pass_trend_gate_1m")):
        tags.append("中期トレンド維持")
    if bool(row.get("pass_flow_gate_1m")):
        tags.append("中期の資金流入継続")
    if -5.0 <= float(_coerce_numeric(pd.Series([row.get("price_vs_ma20_pct", pd.NA)])).fillna(0.0).iloc[0] or 0.0) <= 6.0 and live_ret >= 0.0:
        tags.append("押し目後の再加速")
    if str(row.get("sector_tailwind_band_1m", "") or "") in {"strong", "mid"}:
        tags.append("セクター上昇と整合")
    if price_vs_ma20_abs <= 8.0:
        tags.append("中期の値崩れが小さい")
    return _join_reason_tags(tags, fallback="中期条件はあるが押し待ち")


def _swing_reason_3m_v2(row: pd.Series) -> str:
    rs_3m = float(_coerce_numeric(pd.Series([row.get("rs_vs_topix_3m", pd.NA)])).fillna(0.0).iloc[0] or 0.0)
    ret_3m = float(_coerce_numeric(pd.Series([row.get("ret_3m", pd.NA)])).fillna(0.0).iloc[0] or 0.0)
    rs_1m = float(_coerce_numeric(pd.Series([row.get("rs_vs_topix_1m", pd.NA)])).fillna(0.0).iloc[0] or 0.0)
    tags: list[str] = []
    if bool(row.get("pass_trend_gate_3m")) and rs_3m > 0.0 and ret_3m > 0.0:
        tags.append("3か月主導継続")
    elif bool(row.get("pass_trend_gate_3m")):
        tags.append("長めの上昇主導を維持")
    if bool(row.get("pass_live_gate_3m")) and rs_1m > -0.5:
        tags.append("1か月の崩れが小さい")
    if bool(row.get("pass_flow_gate_3m")):
        tags.append("流動性が伴う")
    if str(row.get("sector_tailwind_band_3m", "") or "") in {"strong", "mid"}:
        tags.append("3か月主力セクターと整合")
    return _join_reason_tags(tags, fallback="長めの主導性はあるが確認余地あり")


def _swing_risk_note_1w_v2(row: pd.Series) -> str:
    tags = [
        TODAY_EARNINGS_ANNOUNCEMENT_NOTE if bool(row.get("earnings_today_announcement_flag")) else "",
        "決算近い" if bool(row.get("earnings_risk_flag_1w")) and not bool(row.get("earnings_today_announcement_flag")) else "",
        "追撃は慎重" if bool(row.get("chase_risk_flag_1w")) else ("短期過熱注意" if bool(row.get("moderate_extension_flag_1w")) else ""),
        "today失速" if bool(row.get("intraday_fade_flag_1w")) or bool(row.get("today_breakdown_flag_1w")) else "",
        "流動性注意" if not bool(row.get("liquidity_ok")) else "",
        "出来高弱め" if not bool(row.get("pass_flow_gate_1w")) else "",
        "1m弱め" if not bool(row.get("medium_term_not_broken_1w")) else "",
    ]
    return _join_reason_tags(tags, fallback="")


def _swing_risk_note_1m_v2(row: pd.Series) -> str:
    tags = [
        TODAY_EARNINGS_ANNOUNCEMENT_NOTE if bool(row.get("earnings_today_announcement_flag")) else "",
        "決算近い" if bool(row.get("earnings_risk_flag_1m")) and not bool(row.get("earnings_today_announcement_flag")) else "",
        "20日線乖離大" if bool(row.get("moderate_extension_flag_1m")) else "",
        "財務注意" if bool(row.get("finance_risk_flag")) else "",
        "流動性注意" if not bool(row.get("liquidity_ok")) else "",
    ]
    return _join_reason_tags(tags, fallback="")


def _swing_risk_note_3m_v2(row: pd.Series) -> str:
    tags = [
        TODAY_EARNINGS_ANNOUNCEMENT_NOTE if bool(row.get("earnings_today_announcement_flag")) else "",
        "決算近い" if bool(row.get("earnings_risk_flag_1m")) and not bool(row.get("earnings_today_announcement_flag")) else "",
        "財務注意" if bool(row.get("finance_risk_flag")) else "",
        "1か月側が失速" if bool(row.get("month_confirmation_broken_3m")) else "",
        "流動性注意" if not bool(row.get("liquidity_ok")) else "",
        "20日線乖離大" if bool(row.get("moderate_extension_flag_1m")) else "",
    ]
    return _join_reason_tags(tags, fallback="")


def _tailwind_band_from_rank(rank_value: Any, *, strong_max: int, mid_max: int) -> str:
    numeric = _coerce_numeric(pd.Series([rank_value])).iloc[0]
    if pd.isna(numeric):
        return "none"
    rank_int = int(numeric)
    if rank_int <= strong_max:
        return "strong"
    if rank_int <= mid_max:
        return "mid"
    return "none"


def _entry_stance_payload_1w(row: pd.Series) -> dict[str, str]:
    hard_block = str(row.get("hard_block_reason_raw_1w", "") or "").strip()
    severe_extension = bool(row.get("severe_extension_flag_1w"))
    stretch_penalty = bool(row.get("stretch_penalty_applied_1w"))
    chase_risk = bool(row.get("chase_risk_flag_1w"))
    pass_live = bool(row.get("pass_live_gate_1w"))
    pass_trend = bool(row.get("pass_trend_gate_1w"))
    pass_flow = bool(row.get("pass_flow_gate_1w"))
    today_not_broken = bool(row.get("today_not_broken_1w"))
    medium_term_not_broken = bool(row.get("medium_term_not_broken_1w"))
    intraday_fade = bool(row.get("intraday_fade_flag_1w"))
    live_ret = float(_coerce_numeric(pd.Series([row.get("live_ret_vs_prev_close", pd.NA)])).fillna(0.0).iloc[0] or 0.0)
    quality = str(row.get("candidate_quality_1w", "") or "")
    tailwind_band = str(row.get("sector_tailwind_band_1w", "") or "")
    if hard_block:
        return {
            "entry_stance_raw": "monitor",
            "entry_stance_label": "監視",
            "stretch_caution_label": "短期過熱気味" if severe_extension or stretch_penalty else "",
            "watch_reason_label": "hard block 解除待ち",
        }
    if severe_extension:
        return {
            "entry_stance_raw": "pullback_wait",
            "entry_stance_label": "押し待ち",
            "stretch_caution_label": "短期過熱気味",
            "watch_reason_label": "伸びすぎを落ち着かせたい",
        }
    if not today_not_broken or intraday_fade:
        return {
            "entry_stance_raw": "monitor",
            "entry_stance_label": "監視",
            "stretch_caution_label": "",
            "watch_reason_label": "todayの値動き確認待ち",
        }
    if not medium_term_not_broken:
        return {
            "entry_stance_raw": "monitor",
            "entry_stance_label": "監視",
            "stretch_caution_label": "",
            "watch_reason_label": "1mの下支え確認待ち",
        }
    if stretch_penalty or chase_risk:
        if pass_live and pass_trend and pass_flow and quality == "高" and live_ret <= 4.5:
            return {
                "entry_stance_raw": "follow",
                "entry_stance_label": "追撃候補",
                "stretch_caution_label": "追撃は慎重",
                "watch_reason_label": "押し目待ち推奨",
            }
        return {
            "entry_stance_raw": "pullback_wait",
            "entry_stance_label": "押し待ち",
            "stretch_caution_label": "短期過熱気味",
            "watch_reason_label": "伸びすぎを落ち着かせたい",
        }
    if pass_live and pass_trend and pass_flow and quality in {"高", "中"}:
        return {
            "entry_stance_raw": "follow",
            "entry_stance_label": "追撃候補",
            "stretch_caution_label": "",
            "watch_reason_label": "",
        }
    if pass_trend and quality in {"高", "中"}:
        return {
            "entry_stance_raw": "pullback_wait",
            "entry_stance_label": "押し待ち",
            "stretch_caution_label": "",
            "watch_reason_label": "押し目を待って入りたい",
        }
    if pass_live and pass_flow and tailwind_band == "none":
        return {
            "entry_stance_raw": "monitor",
            "entry_stance_label": "監視",
            "stretch_caution_label": "",
            "watch_reason_label": "セクター追い風の確認待ち",
        }
    return {
        "entry_stance_raw": "monitor",
        "entry_stance_label": "監視",
        "stretch_caution_label": "",
        "watch_reason_label": "短期追認の上積み待ち",
    }


def _entry_stance_payload_1m(row: pd.Series) -> dict[str, str]:
    hard_block = str(row.get("hard_block_reason_raw_1m", "") or "").strip()
    severe_extension = bool(row.get("severe_extension_flag_1m"))
    stretch_penalty = bool(row.get("stretch_penalty_applied_1m"))
    pass_live = bool(row.get("pass_live_gate_1m"))
    pass_trend = bool(row.get("pass_trend_gate_1m"))
    pass_flow = bool(row.get("pass_flow_gate_1m"))
    quality = str(row.get("candidate_quality_1m", "") or "")
    if hard_block:
        return {
            "entry_stance_raw": "monitor",
            "entry_stance_label": "監視",
            "stretch_caution_label": "20日線乖離大" if severe_extension or stretch_penalty else "",
            "watch_reason_label": "条件改善待ち",
        }
    if pass_trend and pass_flow and quality == "高" and not (severe_extension or stretch_penalty):
        return {
            "entry_stance_raw": "medium_term_buy",
            "entry_stance_label": "中期継続で買い検討",
            "stretch_caution_label": "",
            "watch_reason_label": "",
        }
    if pass_trend and quality in {"高", "中"}:
        return {
            "entry_stance_raw": "pullback_wait",
            "entry_stance_label": "押し待ち",
            "stretch_caution_label": "20日線乖離大" if severe_extension or stretch_penalty else "",
            "watch_reason_label": "中期継続はあるが押し目待ち",
        }
    if pass_live and quality in {"高", "中"}:
        return {
            "entry_stance_raw": "monitor",
            "entry_stance_label": "監視",
            "stretch_caution_label": "",
            "watch_reason_label": "中期継続の確度を見極めたい",
        }
    return {
        "entry_stance_raw": "monitor",
        "entry_stance_label": "監視",
        "stretch_caution_label": "",
        "watch_reason_label": "中期継続の裏付け待ち",
    }


def _entry_stance_payload_3m(row: pd.Series) -> dict[str, str]:
    hard_block = str(row.get("hard_block_reason_raw_3m", "") or "").strip()
    severe_extension = bool(row.get("severe_extension_flag_1m"))
    stretch_penalty = bool(row.get("stretch_penalty_applied_3m"))
    pass_live = bool(row.get("pass_live_gate_3m"))
    pass_trend = bool(row.get("pass_trend_gate_3m"))
    pass_flow = bool(row.get("pass_flow_gate_3m"))
    quality = str(row.get("candidate_quality_3m", "") or "")
    if hard_block:
        return {
            "entry_stance_raw": "monitor",
            "entry_stance_label": "監視",
            "stretch_caution_label": "20日線乖離大" if severe_extension or stretch_penalty else "",
            "watch_reason_label": "長期条件の改善待ち",
        }
    if pass_trend and pass_live and pass_flow and quality == "高" and not (severe_extension or stretch_penalty):
        return {
            "entry_stance_raw": "long_term_buy",
            "entry_stance_label": "長期主導で買い検討",
            "stretch_caution_label": "",
            "watch_reason_label": "",
        }
    if pass_trend and pass_live and quality in {"高", "中"}:
        return {
            "entry_stance_raw": "pullback_wait",
            "entry_stance_label": "押し待ち",
            "stretch_caution_label": "20日線乖離大" if severe_extension or stretch_penalty else "",
            "watch_reason_label": "長期主導はあるが押し目待ち",
        }
    return {
        "entry_stance_raw": "monitor",
        "entry_stance_label": "監視",
        "stretch_caution_label": "",
        "watch_reason_label": "長期主導の確度を見極めたい",
    }


def _apply_swing_display_cap(
    frame: pd.DataFrame,
    *,
    sector_col: str,
    total_limit: int,
    limit_per_sector: int,
) -> tuple[pd.DataFrame, set[str]]:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=frame.columns if isinstance(frame, pd.DataFrame) else []), set()
    kept_indices: list[int] = []
    sector_counts: dict[str, int] = {}
    pruned_codes: set[str] = set()
    for idx, row in frame.iterrows():
        sector_name = str(row.get(sector_col, "") or "")
        code = str(row.get("code", "") or "")
        if len(kept_indices) >= total_limit:
            if code:
                pruned_codes.add(code)
            continue
        if sector_counts.get(sector_name, 0) >= limit_per_sector:
            if code:
                pruned_codes.add(code)
            continue
        kept_indices.append(idx)
        sector_counts[sector_name] = sector_counts.get(sector_name, 0) + 1
    kept = frame.loc[kept_indices].copy().reset_index(drop=True)
    return kept, pruned_codes


def _append_warning_note(base_text: Any, warning: str) -> str:
    text = str(base_text or "").strip()
    warn = str(warning or "").strip()
    if not warn:
        return text
    if not text:
        return warn
    if warn in text:
        return text
    return f"{text} / {warn}"


def _fill_swing_display_minimum(
    primary: pd.DataFrame,
    fallback: pd.DataFrame,
    *,
    min_rows: int,
    total_limit: int,
    sector_col: str,
    limit_per_sector: int,
) -> pd.DataFrame:
    if min_rows <= 0:
        return primary.copy() if isinstance(primary, pd.DataFrame) else pd.DataFrame()
    if (primary is None or primary.empty) and (fallback is None or fallback.empty):
        return pd.DataFrame(columns=primary.columns if isinstance(primary, pd.DataFrame) else (fallback.columns if isinstance(fallback, pd.DataFrame) else []))
    result = primary.copy() if isinstance(primary, pd.DataFrame) else pd.DataFrame(columns=fallback.columns)
    if result.empty and isinstance(fallback, pd.DataFrame):
        result = pd.DataFrame(columns=fallback.columns)
    fallback_frame = fallback.copy() if isinstance(fallback, pd.DataFrame) else pd.DataFrame(columns=result.columns)
    existing_codes = set(result.get("code", pd.Series(dtype=str)).astype(str).tolist()) if not result.empty else set()
    sector_counts: dict[str, int] = {}
    if not result.empty and sector_col in result.columns:
        sector_counts = result[sector_col].fillna("").astype(str).value_counts().to_dict()
    deferred_rows: list[pd.Series] = []

    def _try_append(row: pd.Series, *, ignore_sector_cap: bool) -> bool:
        nonlocal result, existing_codes, sector_counts
        if len(result) >= total_limit:
            return False
        code = str(row.get("code", "") or "")
        if code and code in existing_codes:
            return False
        sector_name = str(row.get(sector_col, "") or "")
        if not ignore_sector_cap and sector_counts.get(sector_name, 0) >= limit_per_sector:
            return False
        result = pd.concat([result, pd.DataFrame([row])], ignore_index=True, sort=False)
        if code:
            existing_codes.add(code)
        sector_counts[sector_name] = sector_counts.get(sector_name, 0) + 1
        return True

    for _, row in fallback_frame.iterrows():
        if len(result) >= total_limit:
            break
        code = str(row.get("code", "") or "")
        if code and code in existing_codes:
            continue
        sector_name = str(row.get(sector_col, "") or "")
        if sector_counts.get(sector_name, 0) >= limit_per_sector:
            deferred_rows.append(row)
            continue
        _try_append(row, ignore_sector_cap=False)
        if len(result) >= min_rows:
            break
    if len(result) < min_rows:
        for row in deferred_rows:
            if len(result) >= min_rows or len(result) >= total_limit:
                break
            _try_append(row, ignore_sector_cap=True)
    return result.reset_index(drop=True)


def _empty_swing_candidate_audit_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=SWING_CANDIDATE_AUDIT_COLUMNS)


def _resolve_swing_empty_reason(
    audit_frame: pd.DataFrame,
    *,
    target_sector_col: str,
    hard_block_col: str,
    live_col: str,
    trend_col: str,
    quality_col: str,
    score_col: str,
) -> tuple[str, str]:
    if audit_frame is None or audit_frame.empty:
        return "no_universe_rows", "候補なし（監視ユニバース行なし）"
    target = audit_frame[audit_frame[target_sector_col].fillna(False).astype(bool)].copy() if target_sector_col in audit_frame.columns else audit_frame.copy()
    if target.empty:
        return "no_universe_rows", "候補なし（対象セクター内に観測行なし）"
    hard_block_mask = target.get(hard_block_col, pd.Series([""] * len(target), index=target.index)).astype(str).str.strip() != ""
    if hard_block_mask.all():
        return "all_failed_hard_block", "候補なし（全候補が hard block）"
    eligible = target[~hard_block_mask].copy()
    if live_col in eligible.columns and not eligible[live_col].fillna(False).astype(bool).any():
        return "no_live_support", "候補なし（当日追認が弱い）"
    if trend_col in eligible.columns and not eligible[trend_col].fillna(False).astype(bool).any():
        return "no_trend_support", "候補なし（継続性が弱い）"
    if quality_col in eligible.columns and not eligible[quality_col].fillna(False).astype(bool).any():
        return "all_failed_score_gate", "候補なし（品質 gate 未達）"
    if score_col in eligible.columns and not eligible[score_col].fillna(False).astype(bool).any():
        return "all_failed_score_gate", "候補なし（score gate 未達）"
    return "all_failed_score_gate", "候補なし（最終選定に残りませんでした）"


def _build_swing_candidate_audit_frame(
    frame: pd.DataFrame,
    *,
    horizon: str,
    target_sector_col: str,
    live_col: str,
    trend_col: str,
    flow_col: str,
    quality_col: str,
    hard_block_col: str,
    score_col: str,
    score_total_col: str,
    display_reason_col: str,
    selected_codes: set[str],
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return _empty_swing_candidate_audit_frame()
    working = frame.copy()
    working["code"] = working.get("code", pd.Series(dtype=str)).astype(str)
    working["selected_flag"] = working["code"].isin(selected_codes)
    working["in_candidate_universe"] = True
    working["unselected_reason"] = ""
    working.loc[working[hard_block_col].astype(str).str.strip() != "", "unselected_reason"] = "all_failed_hard_block"
    working.loc[(working["unselected_reason"] == "") & ~working[target_sector_col].fillna(False).astype(bool), "unselected_reason"] = "outside_target_sector"
    working.loc[(working["unselected_reason"] == "") & ~working[live_col].fillna(False).astype(bool), "unselected_reason"] = "no_live_support"
    working.loc[(working["unselected_reason"] == "") & ~working[trend_col].fillna(False).astype(bool), "unselected_reason"] = "no_trend_support"
    working.loc[(working["unselected_reason"] == "") & ~working[flow_col].fillna(False).astype(bool), "unselected_reason"] = "no_flow_support"
    working.loc[(working["unselected_reason"] == "") & ~working[quality_col].fillna(False).astype(bool), "unselected_reason"] = "failed_quality_gate"
    working.loc[(working["unselected_reason"] == "") & ~working[score_col].fillna(False).astype(bool), "unselected_reason"] = "all_failed_score_gate"
    working.loc[(working["unselected_reason"] == "") & ~working["selected_flag"], "unselected_reason"] = "not_selected_after_ranking"
    working.loc[working["selected_flag"], "unselected_reason"] = ""
    score_component_columns = [
        column
        for column in working.columns
        if column.endswith(f"_{horizon}") and ("component" in column or "score" in column) and not column.startswith("score_components_")
    ]
    audit_rows: list[dict[str, Any]] = []
    for _, row in working.iterrows():
        component_payload = {column: (float(row.get(column)) if pd.notna(row.get(column)) else None) for column in score_component_columns}
        audit_rows.append(
            {
                "code": str(row.get("code", "") or ""),
                "name": str(row.get("name", "") or ""),
                "sector_name": str(row.get("sector_name", "") or ""),
                "in_candidate_universe": True,
                "sector_tailwind_band": str(row.get(f"sector_tailwind_band_{horizon}", "") or ""),
                "pass_live_gate": bool(row.get(live_col, False)),
                "pass_trend_gate": bool(row.get(trend_col, False)),
                "pass_flow_gate": bool(row.get(flow_col, False)),
                "pass_quality_gate": bool(row.get(quality_col, False)),
                "pass_score_gate": bool(row.get(score_col, False)),
                "hard_block_reason_raw": str(row.get(hard_block_col, "") or ""),
                "entry_stance_raw": str(row.get(f"entry_stance_raw_{horizon}", "") or ""),
                "entry_stance_label": str(row.get(f"entry_stance_label_{horizon}", "") or ""),
                "stretch_penalty_applied": bool(row.get(f"stretch_penalty_applied_{horizon}", False)),
                "display_sector_cap_pruned": bool(row.get(f"display_sector_cap_pruned_{horizon}", False)),
                "override_selected_outside_top_sector": bool(row.get(f"override_selected_outside_top_sector_{horizon}", False)),
                "empty_reason_label": str(row.get(f"empty_reason_label_{horizon}", "") or ""),
                "score_total_raw": float(row.get(score_total_col, 0.0) or 0.0) if pd.notna(row.get(score_total_col)) else None,
                "score_subcomponents_raw": component_payload,
                "selected_horizon": horizon,
                "buy_score_total": float(row.get(f"buy_score_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"buy_score_{horizon}", pd.NA)) else None,
                "buy_score_1w": float(row.get("buy_score_1w", 0.0) or 0.0) if pd.notna(row.get("buy_score_1w", pd.NA)) else None,
                "buy_score_1m": float(row.get("buy_score_1m", 0.0) or 0.0) if pd.notna(row.get("buy_score_1m", pd.NA)) else None,
                "buy_score_3m": float(row.get("buy_score_3m", 0.0) or 0.0) if pd.notna(row.get("buy_score_3m", pd.NA)) else None,
                "buy_strength_score": float(row.get(f"buy_strength_score_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"buy_strength_score_{horizon}", pd.NA)) else None,
                "buy_strength_score_1w": float(row.get("buy_strength_score_1w", 0.0) or 0.0) if pd.notna(row.get("buy_strength_score_1w", pd.NA)) else None,
                "buy_strength_score_1m": float(row.get("buy_strength_score_1m", 0.0) or 0.0) if pd.notna(row.get("buy_strength_score_1m", pd.NA)) else None,
                "buy_strength_score_3m": float(row.get("buy_strength_score_3m", 0.0) or 0.0) if pd.notna(row.get("buy_strength_score_3m", pd.NA)) else None,
                "entry_timing_adjustment": float(row.get(f"entry_timing_adjustment_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"entry_timing_adjustment_{horizon}", pd.NA)) else None,
                "entry_timing_adjustment_1w": float(row.get("entry_timing_adjustment_1w", 0.0) or 0.0) if pd.notna(row.get("entry_timing_adjustment_1w", pd.NA)) else None,
                "entry_timing_adjustment_1m": float(row.get("entry_timing_adjustment_1m", 0.0) or 0.0) if pd.notna(row.get("entry_timing_adjustment_1m", pd.NA)) else None,
                "entry_timing_adjustment_3m": float(row.get("entry_timing_adjustment_3m", 0.0) or 0.0) if pd.notna(row.get("entry_timing_adjustment_3m", pd.NA)) else None,
                "score_components": row.get(f"score_components_{horizon}", component_payload),
                "sector_strength_score": float(row.get(f"sector_strength_score_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"sector_strength_score_{horizon}", pd.NA)) else None,
                "relative_strength_score": float(row.get(f"relative_strength_score_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"relative_strength_score_{horizon}", pd.NA)) else None,
                "liquidity_score": float(row.get("liquidity_score", 0.0) or 0.0) if pd.notna(row.get("liquidity_score", pd.NA)) else None,
                "earnings_risk_score": float(row.get(f"earnings_risk_score_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"earnings_risk_score_{horizon}", pd.NA)) else None,
                "overheating_penalty": float(row.get(f"overheating_penalty_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"overheating_penalty_{horizon}", pd.NA)) else None,
                "abnormal_event_penalty": float(row.get(f"abnormal_event_penalty_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"abnormal_event_penalty_{horizon}", pd.NA)) else None,
                "fallback_penalty": float(row.get(f"fallback_penalty_{horizon}", 0.0) or 0.0) if pd.notna(row.get(f"fallback_penalty_{horizon}", pd.NA)) else None,
                "selected_reason": str(row.get(display_reason_col, "") or ""),
                "rejected_reason": "" if bool(row.get("selected_flag", False)) else str(row.get("unselected_reason", "") or ""),
                "horizon_fit_reason": str(row.get(f"horizon_fit_reason_{horizon}", "") or ""),
                "entry_caution": str(row.get(f"entry_caution_{horizon}", "") or ""),
                "event_candidate_flag": bool(row.get(f"event_candidate_flag_{horizon}", False)),
                "event_candidate_type": str(row.get(f"event_candidate_type_{horizon}", "") or ""),
                "candidate_bucket": str(row.get(f"candidate_bucket_{horizon}", "") or ""),
                "candidate_bucket_label": str(row.get(f"candidate_bucket_label_{horizon}", "") or ""),
                "event_caution_reason": str(row.get(f"event_caution_reason_{horizon}", "") or ""),
                "fallback_used": bool(row.get(f"fallback_used_{horizon}", False)),
                "selected_flag": bool(row.get("selected_flag", False)),
                "unselected_reason": str(row.get("unselected_reason", "") or ""),
                "display_reason_raw": str(row.get(display_reason_col, "") or ""),
            }
        )
    return pd.DataFrame(audit_rows, columns=SWING_CANDIDATE_AUDIT_COLUMNS)


def _candidate_core_stock_score_series(frame: pd.DataFrame) -> pd.Series:
    if frame is None or frame.empty:
        return pd.Series(dtype="float64")
    index = frame.index
    avg_turnover = _score_percentile(_coerce_numeric(frame.get("avg_turnover_20d", pd.Series(pd.NA, index=index)))).fillna(0.0)
    latest_turnover = _score_percentile(_coerce_numeric(frame.get("TradingValue_latest", pd.Series(pd.NA, index=index)))).fillna(0.0)
    sector_contribution = _score_percentile(_coerce_numeric(frame.get("sector_contribution_full", pd.Series(pd.NA, index=index)))).fillna(0.0)
    turnover_rank = _score_rank_ascending(_coerce_numeric(frame.get("turnover_rank_in_sector", pd.Series(pd.NA, index=index)))).fillna(0.0)
    must_have_rank = _score_rank_ascending(_coerce_numeric(frame.get("must_have_rank_in_sector", pd.Series(pd.NA, index=index)))).fillna(0.0)
    was_must_have = frame.get("was_in_must_have", pd.Series(False, index=index)).fillna(False).astype(bool).astype(float)
    liquidity_ok = frame.get("liquidity_ok", pd.Series(False, index=index)).fillna(False).astype(bool).astype(float)
    score = (
        avg_turnover * 0.26
        + latest_turnover * 0.22
        + sector_contribution * 0.20
        + turnover_rank * 0.14
        + must_have_rank * 0.10
        + was_must_have * 0.05
        + liquidity_ok * 0.03
    )
    return score.clip(lower=0.0, upper=1.0)


def _earnings_risk_score_series(frame: pd.DataFrame, *, horizon: str) -> pd.Series:
    index = frame.index
    raw_days = _coerce_numeric(frame.get("earnings_buffer_days", pd.Series(pd.NA, index=index)))
    days = raw_days.fillna(999.0)
    today = frame.get("earnings_today_announcement_flag", pd.Series(False, index=index)).fillna(False).astype(bool)
    unknown = frame.get("earnings_unknown_flag", pd.Series(False, index=index)).fillna(False).astype(bool)
    score = pd.Series(0.0, index=index)
    before_or_today = days.ge(0)
    score.loc[unknown] -= 0.05
    score.loc[before_or_today & days.le(7)] -= 0.08 if horizon == "1w" else 0.05
    score.loc[before_or_today & days.le(3)] -= 0.10 if horizon == "1w" else 0.07
    score.loc[today | days.eq(0)] -= 0.24 if horizon == "1w" else 0.18
    return score


_CANDIDATE_BUCKET_LABELS = {
    "normal_candidate": "通常候補",
    "event_caution_candidate": "イベント注意候補",
    "chase_caution_candidate": "追いかけ注意",
    "post_earnings_follow_candidate": "決算通過後候補",
    "avoid_or_reject": "見送り寄り",
}


def _candidate_bucket_label(bucket: Any) -> str:
    return _CANDIDATE_BUCKET_LABELS.get(str(bucket or ""), "")


def _event_caution_reason_for_row(row: pd.Series, *, horizon: str) -> str:
    bucket = str(row.get(f"candidate_bucket_{horizon}", "") or "")
    event_type = str(row.get(f"event_candidate_type_{horizon}", "") or "")
    if bucket == "avoid_or_reject":
        return "重大な品質NG。見送り寄り"
    if event_type == "earnings_today_overheated":
        return "本日決算 + 20日線乖離大。追いかけ注意"
    if event_type == "earnings_today":
        return "本日決算。通常候補ではなくイベント注意候補"
    if event_type == "earnings_near_overheated":
        return "決算近い + 20日線乖離大。追いかけ注意"
    if event_type == "earnings_near":
        return "決算近い。強さは高いが値動き急変に注意"
    if event_type == "post_earnings_follow":
        return "決算通過後。中期トレンド継続"
    if bucket == "chase_caution_candidate":
        return "20日線乖離大。追いかけ注意"
    return ""


def _build_horizon_fit_reason(row: pd.Series, *, horizon: str) -> str:
    if horizon == "1w":
        tags = [
            "短期資金流入" if bool(row.get("pass_flow_gate_1w")) else "",
            "today強セクター" if bool(row.get("belongs_today_sector")) else "",
            "セクター上位" if str(row.get("sector_tailwind_band_1w", "")) in {"strong", "mid"} else "",
            "過熱許容範囲" if not bool(row.get("chase_risk_flag_1w")) else "",
        ]
        return _join_reason_tags(tags, fallback="短期条件はあるが確認待ち")
    if horizon == "1m":
        tags = [
            "セクター強さ継続" if str(row.get("sector_tailwind_band_1m", "")) in {"strong", "mid"} else "",
            "中期トレンド良好" if bool(row.get("pass_trend_gate_1m")) else "",
            "資金継続" if bool(row.get("pass_flow_gate_1m")) else "",
            "決算リスク低め" if float(row.get("earnings_risk_score_1m", 0.0) or 0.0) >= -0.25 else "",
        ]
        return _join_reason_tags(tags, fallback="中期継続の確認待ち")
    tags = [
        "中核株" if float(row.get("core_stock_score", 0.0) or 0.0) >= 0.66 else "",
        "業種主導性" if str(row.get("sector_tailwind_band_3m", "")) in {"strong", "mid"} else "",
        "中期資金継続" if bool(row.get("pass_flow_gate_3m")) else "",
        "3か月トレンド良好" if bool(row.get("pass_trend_gate_3m")) else "",
    ]
    return _join_reason_tags(tags, fallback="長めの主導性は確認待ち")


def _apply_horizon_buy_scores(working: pd.DataFrame, *, selection_config: dict[str, float]) -> pd.DataFrame:
    if working is None or working.empty:
        return working
    working = working.copy()
    index = working.index

    def num(column: str, default: float = 0.0) -> pd.Series:
        return _coerce_numeric(working.get(column, pd.Series(pd.NA, index=index))).fillna(default)

    def flag(column: str) -> pd.Series:
        return working.get(column, pd.Series(False, index=index)).fillna(False).astype(bool)

    if "core_stock_score" not in working.columns:
        working["core_stock_score"] = _candidate_core_stock_score_series(working)
    working["liquidity_score"] = (
        _score_percentile(num("avg_turnover_20d")).fillna(0.0) * 0.55
        + _score_percentile(num("live_turnover_value")).fillna(0.0) * 0.30
        + flag("liquidity_ok").astype(float) * 0.15
    ).clip(lower=0.0, upper=1.0)

    working["sector_rank_score_1w"] = _score_rank_ascending(num("persistence_rank_1w", 9999.0)).fillna(0.0)
    working["sector_rank_score_1m"] = (
        _score_rank_ascending(num("persistence_rank_1m", 9999.0)).fillna(0.0) * 0.65
        + _score_rank_ascending(num("persistence_rank_3m", 9999.0)).fillna(0.0) * 0.35
    )
    working["sector_rank_score_3m"] = _score_rank_ascending(num("persistence_rank_3m", 9999.0)).fillna(0.0)
    working["sector_strength_score_1w"] = (
        working["sector_rank_score_1w"] * 0.45
        + flag("belongs_today_sector").astype(float) * 0.35
        + _score_percentile(num("sector_positive_ratio_1w")).fillna(0.0) * 0.20
    )
    working["sector_strength_score_1m"] = (
        working["sector_rank_score_1m"] * 0.50
        + _score_percentile(num("sector_positive_ratio_1m")).fillna(0.0) * 0.30
        + (1.0 - _score_percentile(num("leader_concentration_share_1m")).fillna(0.0)) * 0.20
    )
    working["sector_strength_score_3m"] = (
        working["sector_rank_score_3m"] * 0.55
        + _score_percentile(num("sector_positive_ratio_3m")).fillna(0.0) * 0.25
        + flag("sector_gate_pass_3m").astype(float) * 0.20
    )
    working["relative_strength_score_1w"] = (
        _score_percentile(num("rs_vs_topix_1w")).fillna(0.0) * 0.50
        + _score_percentile(num("ret_1w")).fillna(0.0) * 0.25
        + _score_percentile(num("live_ret_vs_prev_close")).fillna(0.0) * 0.15
        + _score_percentile(num("high_close_score")).fillna(0.0) * 0.10
    )
    working["relative_strength_score_1m"] = (
        _score_percentile(num("rs_vs_topix_1m")).fillna(0.0) * 0.50
        + _score_percentile(num("rs_vs_topix_3m")).fillna(0.0) * 0.25
        + (1.0 - _score_percentile(num("price_vs_ma20_abs")).fillna(0.0)) * 0.15
        + _score_percentile(num("live_turnover_ratio_20d")).fillna(0.0) * 0.10
    )
    working["relative_strength_score_3m"] = (
        _score_percentile(num("rs_vs_topix_3m")).fillna(0.0) * 0.50
        + _score_percentile(num("ret_3m")).fillna(0.0) * 0.30
        + _score_percentile(num("rs_vs_topix_1m")).fillna(0.0) * 0.20
    )

    for horizon in ["1w", "1m", "3m"]:
        working[f"earnings_risk_score_{horizon}"] = _earnings_risk_score_series(working, horizon=horizon)
        working[f"fallback_penalty_{horizon}"] = 0.0

    one_day_spike = num("live_ret_vs_prev_close").gt(5.0) & num("price_vs_ma20_abs").gt(8.0)
    small_short_spike = one_day_spike & working["core_stock_score"].lt(0.62)
    working["overheating_penalty_1w"] = 0.0
    working.loc[flag("moderate_extension_flag_1w"), "overheating_penalty_1w"] -= 0.30
    working.loc[flag("severe_extension_flag_1w"), "overheating_penalty_1w"] -= 0.70
    working.loc[num("live_ret_vs_prev_close").gt(6.0), "overheating_penalty_1w"] -= 0.25
    working["overheating_penalty_1m"] = 0.0
    working.loc[flag("moderate_extension_flag_1m"), "overheating_penalty_1m"] -= 0.45
    working.loc[flag("severe_extension_flag_1m"), "overheating_penalty_1m"] -= 0.85
    working.loc[one_day_spike, "overheating_penalty_1m"] -= 0.45
    working["overheating_penalty_3m"] = 0.0
    working.loc[flag("moderate_extension_flag_1m"), "overheating_penalty_3m"] -= 0.50
    working.loc[flag("severe_extension_flag_1m"), "overheating_penalty_3m"] -= 0.95
    working.loc[small_short_spike, "overheating_penalty_3m"] -= 0.70

    working["abnormal_event_penalty_1w"] = 0.0
    working.loc[flag("today_breakdown_flag_1w") | flag("intraday_fade_flag_1w"), "abnormal_event_penalty_1w"] -= 0.85
    working.loc[num("live_ret_vs_prev_close").lt(-5.0), "abnormal_event_penalty_1w"] -= 0.80
    working["abnormal_event_penalty_1m"] = 0.0
    working.loc[num("live_ret_vs_prev_close").lt(-4.0), "abnormal_event_penalty_1m"] -= 0.65
    working.loc[flag("finance_risk_flag"), "abnormal_event_penalty_1m"] -= 0.60
    working["abnormal_event_penalty_3m"] = 0.0
    working.loc[flag("month_confirmation_broken_3m"), "abnormal_event_penalty_3m"] -= 0.75
    working.loc[num("live_ret_vs_prev_close").lt(-4.0), "abnormal_event_penalty_3m"] -= 0.45
    working.loc[flag("finance_risk_flag"), "abnormal_event_penalty_3m"] -= 0.70

    working["short_momentum_score_1w"] = (
        _score_percentile(num("live_ret_vs_prev_close")).fillna(0.0) * 0.35
        + _score_percentile(num("live_turnover_ratio_20d").where(num("live_turnover_ratio_20d").notna(), num("live_volume_ratio_20d"))).fillna(0.0) * 0.35
        + _score_percentile(num("high_close_score")).fillna(0.0) * 0.30
    )
    working["medium_trend_score_1m"] = (
        flag("pass_trend_gate_1m").astype(float) * 0.45
        + _score_percentile(num("rs_vs_topix_1m")).fillna(0.0) * 0.35
        + _score_percentile(num("rs_vs_topix_3m")).fillna(0.0) * 0.20
    )
    working["long_story_score_3m"] = (
        flag("pass_trend_gate_3m").astype(float) * 0.40
        + working["core_stock_score"] * 0.30
        + _score_percentile(num("ret_3m")).fillna(0.0) * 0.30
    )

    working["buy_strength_score_1w"] = (
        working["short_momentum_score_1w"] * 0.75
        + working["sector_strength_score_1w"] * 0.70
        + working["relative_strength_score_1w"] * 0.65
        + working["liquidity_score"] * 0.45
        + working["sector_rank_score_1w"] * 0.25
    )
    working["entry_timing_adjustment_1w"] = (
        + working["earnings_risk_score_1w"]
        + working["overheating_penalty_1w"]
        + working["abnormal_event_penalty_1w"]
        + working["fallback_penalty_1w"]
    )
    working["buy_score_1w"] = working["buy_strength_score_1w"] + working["entry_timing_adjustment_1w"]
    working["buy_strength_score_1m"] = (
        working["medium_trend_score_1m"] * 0.80
        + working["sector_strength_score_1m"] * 0.75
        + working["relative_strength_score_1m"] * 0.65
        + working["liquidity_score"] * 0.45
        + working["sector_rank_score_1m"] * 0.30
        + working["core_stock_score"] * 0.25
    )
    working["entry_timing_adjustment_1m"] = (
        + working["earnings_risk_score_1m"]
        + working["overheating_penalty_1m"]
        + working["abnormal_event_penalty_1m"]
        + working["fallback_penalty_1m"]
    )
    working["buy_score_1m"] = working["buy_strength_score_1m"] + working["entry_timing_adjustment_1m"]
    working["buy_strength_score_3m"] = (
        working["long_story_score_3m"] * 0.90
        + working["sector_strength_score_3m"] * 0.80
        + working["relative_strength_score_3m"] * 0.70
        + working["liquidity_score"] * 0.45
        + working["sector_rank_score_3m"] * 0.35
        + working["core_stock_score"] * 0.45
    )
    working["entry_timing_adjustment_3m"] = (
        + working["earnings_risk_score_3m"]
        + working["overheating_penalty_3m"]
        + working["abnormal_event_penalty_3m"]
        + working["fallback_penalty_3m"]
    )
    working["buy_score_3m"] = working["buy_strength_score_3m"] + working["entry_timing_adjustment_3m"]

    for horizon in ["1w", "1m", "3m"]:
        earnings_days = num("earnings_buffer_days", 999.0)
        earnings_today = flag("earnings_today_announcement_flag") | earnings_days.eq(0)
        earnings_near = earnings_days.ge(0) & earnings_days.le(3) & ~earnings_today
        post_earnings = earnings_days.lt(0) & earnings_days.ge(-7)
        overheat = flag("moderate_extension_flag_1w" if horizon == "1w" else "moderate_extension_flag_1m")
        severe_or_abnormal = (
            working.get(f"hard_block_reason_raw_{horizon}", pd.Series("", index=index)).fillna("").astype(str).str.strip().ne("")
            | working[f"abnormal_event_penalty_{horizon}"].le(-0.80)
        )
        stable_post = post_earnings & flag(f"pass_trend_gate_{horizon}") & flag(f"pass_flow_gate_{horizon}") & working[f"abnormal_event_penalty_{horizon}"].ge(-0.20)
        event_type = pd.Series("", index=index, dtype=object)
        event_type.loc[earnings_today & overheat] = "earnings_today_overheated"
        event_type.loc[earnings_today & ~overheat] = "earnings_today"
        event_type.loc[earnings_near & overheat] = "earnings_near_overheated"
        event_type.loc[earnings_near & ~overheat] = "earnings_near"
        event_type.loc[stable_post & event_type.eq("")] = "post_earnings_follow"
        event_type.loc[overheat & event_type.eq("")] = "overheat_chase"
        bucket = pd.Series("normal_candidate", index=index, dtype=object)
        bucket.loc[overheat] = "chase_caution_candidate"
        bucket.loc[earnings_today | earnings_near] = "event_caution_candidate"
        bucket.loc[(earnings_today | earnings_near) & overheat] = "chase_caution_candidate"
        bucket.loc[stable_post] = "post_earnings_follow_candidate"
        bucket.loc[severe_or_abnormal] = "avoid_or_reject"
        working[f"event_candidate_type_{horizon}"] = event_type
        working[f"event_candidate_flag_{horizon}"] = event_type.isin(["earnings_today_overheated", "earnings_today", "earnings_near_overheated", "earnings_near", "post_earnings_follow"])
        working[f"candidate_bucket_{horizon}"] = bucket
        working[f"candidate_bucket_label_{horizon}"] = bucket.apply(_candidate_bucket_label)
        working[f"event_caution_reason_{horizon}"] = working.apply(lambda row, h=horizon: _event_caution_reason_for_row(row, horizon=h), axis=1)
        working[f"score_components_{horizon}"] = working.apply(
            lambda row, h=horizon: {
                "buy_strength_score": float(row.get(f"buy_strength_score_{h}", 0.0) or 0.0),
                "entry_timing_adjustment": float(row.get(f"entry_timing_adjustment_{h}", 0.0) or 0.0),
                "sector_strength_score": float(row.get(f"sector_strength_score_{h}", 0.0) or 0.0),
                "sector_rank_score": float(row.get(f"sector_rank_score_{h}", 0.0) or 0.0),
                "relative_strength_score": float(row.get(f"relative_strength_score_{h}", 0.0) or 0.0),
                "liquidity_score": float(row.get("liquidity_score", 0.0) or 0.0),
                "earnings_risk_score": float(row.get(f"earnings_risk_score_{h}", 0.0) or 0.0),
                "overheating_penalty": float(row.get(f"overheating_penalty_{h}", 0.0) or 0.0),
                "abnormal_event_penalty": float(row.get(f"abnormal_event_penalty_{h}", 0.0) or 0.0),
                "fallback_penalty": float(row.get(f"fallback_penalty_{h}", 0.0) or 0.0),
                "core_stock_score": float(row.get("core_stock_score", 0.0) or 0.0),
            },
            axis=1,
        )
        working[f"horizon_fit_reason_{horizon}"] = working.apply(lambda row, h=horizon: _build_horizon_fit_reason(row, horizon=h), axis=1)
        working[f"entry_caution_{horizon}"] = working.apply(lambda row, h=horizon: str(row.get(f"risk_note_{h}", "") or "").strip(), axis=1)
        working.loc[working[f"entry_caution_{horizon}"].astype(str).str.strip().eq(""), f"entry_caution_{horizon}"] = working.loc[
            working[f"entry_caution_{horizon}"].astype(str).str.strip().eq(""), f"event_caution_reason_{horizon}"
        ]
        working[f"rejected_reason_{horizon}"] = ""
        working[f"fallback_used_{horizon}"] = False
    return working


def _build_swing_candidate_tables_v2(
    merged: pd.DataFrame,
    today_sector_leaderboard: pd.DataFrame,
    persistence_tables: dict[str, pd.DataFrame],
    *,
    selection_config: dict[str, float] | None = None,
) -> dict[str, pd.DataFrame]:
    selection_config = selection_config or SWING_SELECTION_CONFIG
    if merged is None or merged.empty:
        empty = pd.DataFrame()
        empty_audit = _empty_swing_candidate_audit_frame()
        return {
            "1w": empty,
            "1m": empty,
            "3m": empty.copy(),
            "buy_1w": empty.copy(),
            "watch_1w": empty.copy(),
            "buy_1m": empty.copy(),
            "watch_1m": empty.copy(),
            "buy_3m": empty.copy(),
            "watch_3m": empty.copy(),
            "audit_1w": empty_audit,
            "audit_1m": empty_audit.copy(),
            "audit_3m": empty_audit.copy(),
            "empty_reason_1w": "候補なし（監視ユニバース行なし）",
            "empty_reason_1m": "候補なし（監視ユニバース行なし）",
            "empty_reason_3m": "候補なし（監視ユニバース行なし）",
            "empty_status_1w": "no_universe_rows",
            "empty_status_1m": "no_universe_rows",
            "empty_status_3m": "no_universe_rows",
        }
    sorted_today_sector_leaderboard = _sort_today_sector_leaderboard_for_display(today_sector_leaderboard)
    working = merged.copy()
    working["code"] = working.get("code", pd.Series(dtype=str)).astype(str)
    working["name"] = working.get("name", pd.Series(dtype=str)).astype(str)
    working["sector_name"] = working.get("sector_name", pd.Series(dtype=str)).astype(str)
    working["current_price"] = _coerce_numeric(working.get("current_price", working.get("live_price", pd.Series(pd.NA, index=working.index))))
    working["live_turnover_value"] = _coerce_numeric(working.get("live_turnover_value", working.get("live_turnover", pd.Series(pd.NA, index=working.index))))
    working["current_price_unavailable"] = working["current_price"].isna()
    working["live_turnover_unavailable"] = working["live_turnover_value"].isna()
    earnings_days_raw = _coerce_numeric(working.get("earnings_buffer_days", pd.Series([pd.NA] * len(working), index=working.index)))
    earnings_days = earnings_days_raw.fillna(999.0)
    earnings_data_available = bool(earnings_days_raw.notna().any())
    finance_score_raw = _coerce_numeric(working.get("finance_health_score", pd.Series([pd.NA] * len(working), index=working.index)))
    finance_score = finance_score_raw.fillna(0.0)
    working["earnings_today_announcement_flag"] = working.get("earnings_today_announcement_flag", pd.Series(False, index=working.index)).fillna(False).astype(bool)
    working["earnings_unknown_flag"] = earnings_days_raw.isna() & earnings_data_available
    working["earnings_risk_flag_1w"] = earnings_days.lt(float(selection_config.get("earnings_hard_block_days_1w", 7) or 7)).fillna(False)
    working["earnings_risk_flag_1m"] = earnings_days.lt(float(selection_config.get("earnings_hard_block_days_1m", 7) or 7)).fillna(False)
    working["finance_risk_flag"] = finance_score_raw.lt(-1.0).fillna(False)
    working["finance_health_flag"] = finance_score_raw.apply(lambda value: "不明" if pd.isna(value) else ("無難" if float(value) >= -1.0 else "注意"))
    turnover_floor = float(_coerce_numeric(working["avg_turnover_20d"]).median(skipna=True) or 0.0)
    volume_floor = float(_coerce_numeric(working["avg_volume_20d"]).median(skipna=True) or 0.0)
    working["liquidity_ok"] = (
        _coerce_numeric(working["avg_turnover_20d"]).fillna(0.0) >= turnover_floor
    ) & (
        _coerce_numeric(working["avg_volume_20d"]).fillna(0.0) >= volume_floor
    )
    extension_threshold_1w = float(selection_config.get("extension_threshold_1w", 12.0) or 12.0)
    extension_threshold_1m = float(selection_config.get("extension_threshold_1m", 12.0) or 12.0)
    working["price_vs_ma20_abs"] = _coerce_numeric(working["price_vs_ma20_pct"]).abs()
    working["live_ret_vs_prev_close"] = _coerce_numeric(working.get("live_ret_vs_prev_close", pd.Series(pd.NA, index=working.index)))
    working["live_ret_from_open"] = _coerce_numeric(working.get("live_ret_from_open", pd.Series(pd.NA, index=working.index)))
    working["gap_pct"] = _coerce_numeric(working.get("gap_pct", pd.Series(pd.NA, index=working.index)))
    working["moderate_extension_flag_1w"] = (
        working["price_vs_ma20_abs"].gt(max(6.0, extension_threshold_1w - 2.0)).fillna(False)
        | working["gap_pct"].fillna(0.0).gt(2.5)
        | working["live_ret_from_open"].fillna(0.0).gt(2.8)
    )
    working["severe_extension_flag_1w"] = (
        working["price_vs_ma20_abs"].gt(extension_threshold_1w + 3.0).fillna(False)
        | (
            working["gap_pct"].fillna(0.0).gt(4.5)
            & working["live_ret_from_open"].fillna(0.0).gt(4.0)
        )
    )
    working["extension_flag_1w"] = working["moderate_extension_flag_1w"]
    working["moderate_extension_flag_1m"] = (
        working["price_vs_ma20_abs"].gt(max(7.5, extension_threshold_1m - 1.5)).fillna(False)
        | working["gap_pct"].fillna(0.0).gt(3.0)
    )
    working["severe_extension_flag_1m"] = (
        working["price_vs_ma20_abs"].gt(extension_threshold_1m + 4.0).fillna(False)
        | (
            working["gap_pct"].fillna(0.0).gt(5.0)
            & working["live_ret_from_open"].fillna(0.0).gt(4.5)
        )
    )
    working["extension_flag_1m"] = working["moderate_extension_flag_1m"]
    working["high_close_score"] = _coerce_numeric(working.get("high_close_score", pd.Series(pd.NA, index=working.index))).fillna(0.0)
    working["live_turnover_ratio_20d"] = _coerce_numeric(working.get("live_turnover_ratio_20d", pd.Series(pd.NA, index=working.index)))
    working["live_volume_ratio_20d"] = _coerce_numeric(working.get("live_volume_ratio_20d", pd.Series(pd.NA, index=working.index)))
    working["live_turnover_rank_norm"] = _score_percentile(working["live_turnover_value"])
    working["closing_hold_component_1w"] = _score_percentile(working["high_close_score"]) * 0.30
    working["intraday_followthrough_component_1w"] = _score_percentile((working["live_ret_vs_prev_close"] - working["live_ret_from_open"]).fillna(0.0)) * 0.25
    top_today_limit = int(selection_config.get("top_today_sector_limit_1w", 6) or 6)
    top_persistence_limit_1w = int(selection_config.get("top_persistence_sector_limit_1w", selection_config.get("top_today_sector_limit_1w", 8)) or 8)
    top_persistence_limit_1m = int(selection_config.get("top_persistence_sector_limit_1m", 8) or 8)
    top_persistence_limit_3m = int(selection_config.get("top_persistence_sector_limit_3m", top_persistence_limit_1m) or top_persistence_limit_1m)
    top_today_sectors = set(sorted_today_sector_leaderboard.head(top_today_limit)["sector_name"].astype(str).tolist()) if not sorted_today_sector_leaderboard.empty else set()
    persistence_1w = persistence_tables.get("1w", pd.DataFrame()).copy()
    persistence_1m = persistence_tables.get("1m", pd.DataFrame()).copy()
    persistence_3m = persistence_tables.get("3m", pd.DataFrame()).copy()
    top_1w_sectors = set(persistence_1w.head(top_persistence_limit_1w)["sector_name"].astype(str).tolist())
    top_1m_sectors = set(persistence_1m.head(top_persistence_limit_1m)["sector_name"].astype(str).tolist())
    top_3m_sectors = set(persistence_3m.head(top_persistence_limit_3m)["sector_name"].astype(str).tolist())
    today_rank_map = (
        sorted_today_sector_leaderboard.set_index("sector_name")["today_rank"]
        if not sorted_today_sector_leaderboard.empty and "today_rank" in sorted_today_sector_leaderboard.columns
        else pd.Series(dtype="float64")
    )
    today_sector_conf_map = (
        sorted_today_sector_leaderboard.set_index("sector_name")["sector_confidence"]
        if not sorted_today_sector_leaderboard.empty and "sector_confidence" in sorted_today_sector_leaderboard.columns
        else pd.Series(dtype=str)
    )
    persistence_conf_1w_map = (
        persistence_1w.set_index("sector_name")["sector_confidence"]
        if not persistence_1w.empty and "sector_confidence" in persistence_1w.columns
        else pd.Series(dtype=str)
    )
    persistence_conf_1m_map = (
        persistence_1m.set_index("sector_name")["sector_confidence"]
        if not persistence_1m.empty and "sector_confidence" in persistence_1m.columns
        else pd.Series(dtype=str)
    )
    persistence_conf_3m_map = (
        persistence_3m.set_index("sector_name")["sector_confidence"]
        if not persistence_3m.empty and "sector_confidence" in persistence_3m.columns
        else pd.Series(dtype=str)
    )
    persistence_gate_1w_map = (
        persistence_1w.set_index("sector_name")["sector_gate_pass"]
        if not persistence_1w.empty and "sector_gate_pass" in persistence_1w.columns
        else pd.Series(dtype=bool)
    )
    persistence_gate_1m_map = (
        persistence_1m.set_index("sector_name")["sector_gate_pass"]
        if not persistence_1m.empty and "sector_gate_pass" in persistence_1m.columns
        else pd.Series(dtype=bool)
    )
    persistence_gate_3m_map = (
        persistence_3m.set_index("sector_name")["sector_gate_pass"]
        if not persistence_3m.empty and "sector_gate_pass" in persistence_3m.columns
        else pd.Series(dtype=bool)
    )
    persistence_positive_ratio_1w_map = (
        persistence_1w.set_index("sector_name")["sector_positive_ratio"]
        if not persistence_1w.empty and "sector_positive_ratio" in persistence_1w.columns
        else pd.Series(dtype="float64")
    )
    persistence_positive_ratio_1m_map = (
        persistence_1m.set_index("sector_name")["sector_positive_ratio"]
        if not persistence_1m.empty and "sector_positive_ratio" in persistence_1m.columns
        else pd.Series(dtype="float64")
    )
    persistence_positive_ratio_3m_map = (
        persistence_3m.set_index("sector_name")["sector_positive_ratio"]
        if not persistence_3m.empty and "sector_positive_ratio" in persistence_3m.columns
        else pd.Series(dtype="float64")
    )
    persistence_concentration_1m_map = (
        persistence_1m.set_index("sector_name")["leader_concentration_share"]
        if not persistence_1m.empty and "leader_concentration_share" in persistence_1m.columns
        else pd.Series(dtype="float64")
    )
    persistence_rank_1m_map = (
        persistence_1m.set_index("sector_name")["persistence_rank"]
        if not persistence_1m.empty and "persistence_rank" in persistence_1m.columns
        else pd.Series(dtype="float64")
    )
    persistence_rank_3m_map = (
        persistence_3m.set_index("sector_name")["persistence_rank"]
        if not persistence_3m.empty and "persistence_rank" in persistence_3m.columns
        else pd.Series(dtype="float64")
    )
    persistence_rank_1w_map = (
        persistence_1w.set_index("sector_name")["persistence_rank"]
        if not persistence_1w.empty and "persistence_rank" in persistence_1w.columns
        else pd.Series(dtype="float64")
    )
    working["today_rank"] = _coerce_numeric(working["sector_name"].map(today_rank_map))
    working["today_sector_confidence"] = working["sector_name"].map(today_sector_conf_map).fillna("")
    working["persistence_rank_1w"] = _coerce_numeric(working["sector_name"].map(persistence_rank_1w_map))
    working["persistence_rank_1m"] = _coerce_numeric(working["sector_name"].map(persistence_rank_1m_map))
    working["persistence_rank_3m"] = _coerce_numeric(working["sector_name"].map(persistence_rank_3m_map))
    working["sector_gate_pass_1w"] = working["sector_name"].map(persistence_gate_1w_map).fillna(False).astype(bool)
    working["sector_gate_pass_1m"] = working["sector_name"].map(persistence_gate_1m_map).fillna(False).astype(bool)
    working["sector_gate_pass_3m"] = working["sector_name"].map(persistence_gate_3m_map).fillna(False).astype(bool)
    working["sector_positive_ratio_1w"] = _coerce_numeric(working["sector_name"].map(persistence_positive_ratio_1w_map))
    working["sector_positive_ratio_1m"] = _coerce_numeric(working["sector_name"].map(persistence_positive_ratio_1m_map))
    working["sector_positive_ratio_3m"] = _coerce_numeric(working["sector_name"].map(persistence_positive_ratio_3m_map))
    working["leader_concentration_share_1m"] = _coerce_numeric(working["sector_name"].map(persistence_concentration_1m_map))
    working["belongs_today_sector"] = working["sector_name"].isin(top_today_sectors)
    working["belongs_persistence_sector_1w"] = working["sector_name"].isin(top_1w_sectors)
    working["belongs_persistence_sector"] = working["sector_name"].isin(top_1m_sectors | top_3m_sectors)
    working["belongs_persistence_sector_3m"] = working["sector_name"].isin(top_3m_sectors)
    working["in_scope_1w"] = working["belongs_persistence_sector_1w"] | working["belongs_today_sector"]
    working["in_scope_1m"] = working["belongs_persistence_sector"]
    working["in_scope_3m"] = working["belongs_persistence_sector_3m"]
    working["core_stock_score"] = _candidate_core_stock_score_series(working)
    core_stock_flag = working["core_stock_score"].ge(0.68)
    working["core_stock_flag"] = core_stock_flag
    working["in_scope_1m"] = (
        working["in_scope_1m"]
        | (
            core_stock_flag
            & (working["belongs_today_sector"] | working["persistence_rank_1m"].le(15).fillna(False) | working["persistence_rank_3m"].le(15).fillna(False))
            & _coerce_numeric(working["rs_vs_topix_1m"]).fillna(-999.0).gt(-5.0)
            & _coerce_numeric(working["ret_1m"]).fillna(-999.0).gt(-8.0)
        )
    )
    working["in_scope_3m"] = (
        working["in_scope_3m"]
        | (
            core_stock_flag
            & (working["belongs_today_sector"] | working["persistence_rank_3m"].le(15).fillna(False))
            & _coerce_numeric(working["rs_vs_topix_3m"]).fillna(-999.0).gt(-3.0)
            & _coerce_numeric(working["ret_3m"]).fillna(-999.0).gt(-5.0)
        )
    )
    working["sector_tailwind_band_1w"] = working["persistence_rank_1w"].apply(lambda value: _tailwind_band_from_rank(value, strong_max=5, mid_max=10))
    working["sector_tailwind_band_1m"] = working.apply(
        lambda row: "strong"
        if _tailwind_band_from_rank(row.get("persistence_rank_1m"), strong_max=5, mid_max=10) == "strong"
        or _tailwind_band_from_rank(row.get("persistence_rank_3m"), strong_max=5, mid_max=10) == "strong"
        else (
            "mid"
            if bool(row.get("belongs_persistence_sector"))
            or _tailwind_band_from_rank(row.get("persistence_rank_1m"), strong_max=5, mid_max=10) == "mid"
            or _tailwind_band_from_rank(row.get("persistence_rank_3m"), strong_max=5, mid_max=10) == "mid"
            else "none"
        ),
        axis=1,
    )
    working["sector_tailwind_band_3m"] = working["persistence_rank_3m"].apply(lambda value: _tailwind_band_from_rank(value, strong_max=5, mid_max=10))
    working["sector_tailwind_bonus_1w"] = 0.0
    working.loc[working["sector_tailwind_band_1w"].eq("strong"), "sector_tailwind_bonus_1w"] = float(selection_config.get("sector_tailwind_bonus_strong_1w", 0.70) or 0.70)
    working.loc[working["sector_tailwind_band_1w"].eq("mid"), "sector_tailwind_bonus_1w"] = float(selection_config.get("sector_tailwind_bonus_mid_1w", 0.28) or 0.28)
    working["today_confirmation_bonus_1w"] = 0.0
    working.loc[working["today_rank"].apply(lambda value: _tailwind_band_from_rank(value, strong_max=8, mid_max=12)).eq("strong"), "today_confirmation_bonus_1w"] = 0.18
    working.loc[working["today_rank"].apply(lambda value: _tailwind_band_from_rank(value, strong_max=8, mid_max=12)).eq("mid"), "today_confirmation_bonus_1w"] = 0.08
    working["sector_tailwind_bonus_1m"] = 0.0
    working.loc[working["sector_tailwind_band_1m"].eq("strong"), "sector_tailwind_bonus_1m"] = float(selection_config.get("sector_tailwind_bonus_strong_1m", 0.85) or 0.85)
    working.loc[working["sector_tailwind_band_1m"].eq("mid"), "sector_tailwind_bonus_1m"] = float(selection_config.get("sector_tailwind_bonus_mid_1m", 0.45) or 0.45)
    working["sector_tailwind_bonus_3m"] = 0.0
    working.loc[working["sector_tailwind_band_3m"].eq("strong"), "sector_tailwind_bonus_3m"] = float(selection_config.get("sector_tailwind_bonus_strong_3m", 0.90) or 0.90)
    working.loc[working["sector_tailwind_band_3m"].eq("mid"), "sector_tailwind_bonus_3m"] = float(selection_config.get("sector_tailwind_bonus_mid_3m", 0.42) or 0.42)
    working["sector_confidence_1w"] = working["sector_name"].map(persistence_conf_1w_map).fillna("")
    working["sector_confidence_1m"] = working["sector_name"].map(persistence_conf_1m_map).fillna("")
    working["sector_confidence_3m"] = working["sector_name"].map(persistence_conf_3m_map).fillna("")
    working["sector_confidence_priority_1w"] = working["sector_confidence_1w"].map(SECTOR_CONFIDENCE_PRIORITY).fillna(0).astype(int)
    working["sector_confidence_priority_1m"] = working["sector_confidence_1m"].map(SECTOR_CONFIDENCE_PRIORITY).fillna(0).astype(int)
    working["sector_confidence_priority_3m"] = working["sector_confidence_3m"].map(SECTOR_CONFIDENCE_PRIORITY).fillna(0).astype(int)
    working["rs_ok"] = _coerce_numeric(working["rs_vs_topix_1w"]).gt(0.0).fillna(False)
    working["medium_term_rs_ok"] = (
        _coerce_numeric(working["rs_vs_topix_1m"]).gt(0.0)
        & _coerce_numeric(working["rs_vs_topix_3m"]).gt(-1.0)
    ).fillna(False)
    working["long_term_rs_ok"] = (
        _coerce_numeric(working["rs_vs_topix_3m"]).gt(0.0)
        & _coerce_numeric(working["ret_3m"]).gt(0.0)
    ).fillna(False)
    working["month_confirmation_ok_3m"] = _coerce_numeric(working["rs_vs_topix_1m"]).gt(-1.0).fillna(False)
    working["month_confirmation_broken_3m"] = ~working["month_confirmation_ok_3m"]
    flow_gate_1w = float(selection_config.get("flow_ratio_gate_1w", 1.2) or 1.2)
    volume_gate_1w = float(selection_config.get("volume_ratio_gate_1w", 1.2) or 1.2)
    flow_gate_1m = float(selection_config.get("flow_ratio_gate_1m", 0.9) or 0.9)
    volume_gate_1m = float(selection_config.get("volume_ratio_gate_1m", 0.9) or 0.9)
    score_gate_3m = float(selection_config.get("score_gate_3m", 3.0) or 3.0)
    working["pass_live_gate_1w"] = (
        working["live_ret_vs_prev_close"].fillna(-999.0).ge(1.0)
        | ((working["live_ret_vs_prev_close"].fillna(-999.0) >= 0.4) & (working["live_ret_from_open"].fillna(-999.0) >= 0.0))
        | ((working["live_ret_vs_prev_close"].fillna(-999.0) > 0.0) & (working["live_turnover_ratio_20d"].fillna(0.0) >= flow_gate_1w))
    )
    working["pass_trend_gate_1w"] = (
        working["rs_ok"]
        | (
            _coerce_numeric(working["rs_vs_topix_1w"]).fillna(-999.0).gt(-0.5)
            & _coerce_numeric(working["price_vs_ma20_pct"]).fillna(-999.0).gt(-5.0)
            & working["live_ret_vs_prev_close"].fillna(-999.0).gt(0.0)
        )
    )
    working["pass_flow_gate_1w"] = (
        (working["live_turnover_ratio_20d"].fillna(0.0) >= flow_gate_1w)
        | (working["live_volume_ratio_20d"].fillna(0.0) >= volume_gate_1w)
        | ((working["live_turnover_rank_norm"].fillna(0.0) >= 0.65) & working["live_ret_vs_prev_close"].fillna(-999.0).gt(0.0))
    )
    working["pass_quality_gate_1w"] = working["liquidity_ok"] & working["in_scope_1w"]
    working["today_breakdown_flag_1w"] = (
        working["live_ret_vs_prev_close"].fillna(-999.0).lt(-2.2)
        | (
            working["live_ret_vs_prev_close"].fillna(-999.0).lt(-1.2)
            & working["live_ret_from_open"].fillna(-999.0).lt(-1.6)
        )
        | (
            working["live_ret_vs_prev_close"].fillna(-999.0).lt(-0.8)
            & working["live_ret_from_open"].fillna(-999.0).lt(-1.4)
            & working["high_close_score"].fillna(1.0).lt(0.76)
        )
    )
    working["today_not_broken_1w"] = ~working["today_breakdown_flag_1w"]
    working["intraday_fade_flag_1w"] = (
        working["live_ret_from_open"].fillna(0.0).lt(-1.5)
        | (
            working["live_ret_vs_prev_close"].fillna(0.0).gt(0.8)
            & working["high_close_score"].fillna(1.0).lt(0.78)
        )
    )
    working["one_week_edge_1w"] = (
        _coerce_numeric(working["rs_vs_topix_1w"]).fillna(-999.0).gt(0.5)
        | _coerce_numeric(working["ret_1w"]).fillna(-999.0).gt(2.0)
    )
    working["medium_term_not_broken_1w"] = (
        _coerce_numeric(working["rs_vs_topix_1m"]).fillna(-999.0).gt(-2.5)
        & _coerce_numeric(working["ret_1m"]).fillna(-999.0).gt(-8.0)
    )
    working["chase_risk_flag_1w"] = (
        working["moderate_extension_flag_1w"]
        | (
            working["live_ret_vs_prev_close"].fillna(0.0).gt(4.5)
            & working["gap_pct"].fillna(0.0).gt(2.0)
        )
    )
    working["hard_block_reason_raw_1w"] = working.apply(_hard_block_reason_1w_v2, axis=1)
    working["pass_live_gate_1m"] = (
        working["live_ret_vs_prev_close"].fillna(-999.0).ge(0.0)
        | (working["live_turnover_ratio_20d"].fillna(0.0) >= flow_gate_1m)
    )
    working["medium_duration_ok_1m"] = (
        _coerce_numeric(working["rs_vs_topix_1m"]).fillna(-999.0).gt(1.0)
        | _coerce_numeric(working["ret_1m"]).fillna(-999.0).gt(3.0)
        | working["core_stock_score"].ge(0.68)
    )
    working["pass_trend_gate_1m"] = working["medium_term_rs_ok"] & working["medium_duration_ok_1m"]
    working["pass_flow_gate_1m"] = (
        (working["live_turnover_ratio_20d"].fillna(0.0) >= flow_gate_1m)
        | (working["live_volume_ratio_20d"].fillna(0.0) >= volume_gate_1m)
        | (_score_percentile(working["avg_turnover_20d"]).fillna(0.0) >= 0.55)
    )
    working["pass_quality_gate_1m"] = working["liquidity_ok"] & ~working["finance_risk_flag"] & working["in_scope_1m"]
    working["hard_block_reason_raw_1m"] = working.apply(_hard_block_reason_1m_v2, axis=1)
    working["pass_live_gate_3m"] = working["month_confirmation_ok_3m"] & working["in_scope_3m"]
    working["pass_trend_gate_3m"] = working["long_term_rs_ok"]
    working["pass_flow_gate_3m"] = (
        working["liquidity_ok"]
        | (_score_percentile(working["avg_turnover_20d"]).fillna(0.0) >= 0.55)
    )
    working["pass_quality_gate_3m"] = working["liquidity_ok"] & ~working["finance_risk_flag"] & working["in_scope_3m"]
    working["sector_gate_fail_3m"] = ~working["sector_gate_pass_3m"]
    working["hard_block_reason_raw_3m"] = working.apply(_hard_block_reason_3m_v2, axis=1)

    working["candidate_sector_component_1w"] = working["sector_tailwind_bonus_1w"]
    working["candidate_today_confirmation_component_1w"] = working["today_confirmation_bonus_1w"]
    working["candidate_live_component_1w"] = _score_percentile(working["live_ret_vs_prev_close"]) * 1.05
    working["candidate_flow_component_1w"] = _score_percentile(working["live_turnover_ratio_20d"].fillna(working["live_volume_ratio_20d"])) * 0.95
    working["candidate_followthrough_component_1w"] = working["closing_hold_component_1w"] + working["intraday_followthrough_component_1w"]
    working["candidate_rs_component_1w"] = _score_percentile(working["rs_vs_topix_1w"]) * 0.90
    working["candidate_ret_component_1w"] = _score_percentile(working["ret_1w"]) * 0.65
    working["candidate_breadth_component_1w"] = _score_percentile(working["sector_positive_ratio_1w"]) * 0.35
    working["candidate_liquidity_component_1w"] = _score_percentile(working["avg_turnover_20d"]) * 0.45
    working["candidate_medium_term_component_1w"] = _score_percentile(_coerce_numeric(working["rs_vs_topix_1m"]).clip(lower=-5.0, upper=8.0)) * 0.35
    working["candidate_today_stability_component_1w"] = 0.0
    working.loc[working["today_not_broken_1w"], "candidate_today_stability_component_1w"] += 0.25
    working.loc[working["today_breakdown_flag_1w"], "candidate_today_stability_component_1w"] -= 0.85
    working.loc[working["intraday_fade_flag_1w"], "candidate_today_stability_component_1w"] -= 0.35
    working["candidate_earnings_component_1w"] = 0.0
    working["swing_score_1w"] = (
        working["candidate_sector_component_1w"]
        + working["candidate_today_confirmation_component_1w"]
        + working["candidate_live_component_1w"]
        + working["candidate_flow_component_1w"]
        + working["candidate_followthrough_component_1w"]
        + working["candidate_rs_component_1w"]
        + working["candidate_ret_component_1w"]
        + working["candidate_breadth_component_1w"]
        + working["candidate_liquidity_component_1w"]
        + working["candidate_medium_term_component_1w"]
        + working["candidate_today_stability_component_1w"]
        + working["candidate_earnings_component_1w"]
    )
    working.loc[working["sector_confidence_1w"].eq("高"), "swing_score_1w"] += float(selection_config.get("sector_confidence_bonus_high_1w", 0.0) or 0.0)
    working.loc[working["sector_confidence_1w"].eq("中"), "swing_score_1w"] += float(selection_config.get("sector_confidence_bonus_mid_1w", 0.0) or 0.0)
    working.loc[working["sector_tailwind_band_1w"].eq("none") & working["pass_live_gate_1w"] & working["pass_trend_gate_1w"] & working["pass_flow_gate_1w"], "swing_score_1w"] += 0.20
    working.loc[~working["medium_term_not_broken_1w"], "swing_score_1w"] -= 0.30
    working["pass_score_gate_1w"] = (
        working["swing_score_1w"].fillna(-999.0).ge(float(selection_config.get("score_gate_1w", 2.3) or 2.3))
        & working["in_scope_1w"]
        & (working["pass_live_gate_1w"] | working["pass_trend_gate_1w"])
    )
    working["stretch_penalty_applied_1w"] = working["moderate_extension_flag_1w"] | working["severe_extension_flag_1w"]
    working.loc[working["stretch_penalty_applied_1w"], "swing_score_1w"] -= 0.45
    working["candidate_quality_score_1w"] = 0.0
    working.loc[working["sector_tailwind_band_1w"].eq("strong"), "candidate_quality_score_1w"] += 0.8
    working.loc[working["sector_tailwind_band_1w"].eq("mid"), "candidate_quality_score_1w"] += 0.4
    working.loc[working["pass_live_gate_1w"], "candidate_quality_score_1w"] += 1.2
    working.loc[working["pass_trend_gate_1w"], "candidate_quality_score_1w"] += 1.0
    working.loc[working["pass_flow_gate_1w"], "candidate_quality_score_1w"] += 0.9
    working.loc[working["liquidity_ok"], "candidate_quality_score_1w"] += 0.5
    working.loc[working["today_not_broken_1w"], "candidate_quality_score_1w"] += 0.45
    working.loc[working["one_week_edge_1w"], "candidate_quality_score_1w"] += 0.35
    working.loc[working["medium_term_not_broken_1w"], "candidate_quality_score_1w"] += 0.35
    working.loc[working["stretch_penalty_applied_1w"], "candidate_quality_score_1w"] -= 0.45
    working.loc[working["intraday_fade_flag_1w"], "candidate_quality_score_1w"] -= 0.65
    working.loc[working["today_breakdown_flag_1w"], "candidate_quality_score_1w"] -= 1.1
    working.loc[working["earnings_unknown_flag"], "candidate_quality_score_1w"] -= 0.2
    working.loc[working["earnings_risk_flag_1w"], "candidate_quality_score_1w"] -= 1.2
    working["candidate_quality_1w"] = "低"
    working.loc[working["candidate_quality_score_1w"] >= 4.1, "candidate_quality_1w"] = "高"
    working.loc[(working["candidate_quality_score_1w"] >= 2.7) & (working["candidate_quality_score_1w"] < 4.1), "candidate_quality_1w"] = "中"

    working["candidate_sector_component_1m"] = working["sector_tailwind_bonus_1m"]
    working["candidate_rs_component_1m"] = _score_percentile(working["rs_vs_topix_1m"]) * 1.05
    working["candidate_rs_component_3m"] = _score_percentile(working["rs_vs_topix_3m"]) * 0.80
    working["candidate_ma20_component_1m"] = (1.0 - _score_percentile(working["price_vs_ma20_abs"])) * float(selection_config.get("candidate_ma20_weight_1m", 0.55) or 0.55)
    working["candidate_flow_component_1m"] = _score_percentile(working["live_turnover_ratio_20d"].fillna(working["live_volume_ratio_20d"])) * 0.45
    working["candidate_live_component_1m"] = _score_percentile(working["live_ret_vs_prev_close"]) * 0.20
    working["candidate_liquidity_component_1m"] = _score_percentile(working["avg_turnover_20d"]) * 0.50
    working["candidate_sector_rank_component_1m"] = _score_rank_ascending(working["sector_rank_1m"]) * 0.80
    working["candidate_sector_rank_component_3m"] = _score_rank_ascending(working["sector_rank_3m"]) * 0.75
    working["candidate_breadth_component_1m"] = _score_percentile(working["sector_positive_ratio_1m"]) * 0.55
    working["candidate_concentration_component_1m"] = (1.0 - _score_percentile(working["leader_concentration_share_1m"])) * 0.45
    working["candidate_earnings_component_1m"] = 0.0
    working["candidate_finance_component_1m"] = 0.0
    working.loc[finance_score >= 0.0, "candidate_finance_component_1m"] = 0.35
    working.loc[finance_score < -1.0, "candidate_finance_component_1m"] = -0.80
    working["swing_score_1m"] = (
        working["candidate_sector_component_1m"]
        + working["candidate_rs_component_1m"]
        + working["candidate_rs_component_3m"]
        + working["candidate_ma20_component_1m"]
        + working["candidate_flow_component_1m"]
        + working["candidate_live_component_1m"]
        + working["candidate_liquidity_component_1m"]
        + working["candidate_sector_rank_component_1m"]
        + working["candidate_sector_rank_component_3m"]
        + working["candidate_breadth_component_1m"]
        + working["candidate_concentration_component_1m"]
        + working["candidate_earnings_component_1m"]
        + working["candidate_finance_component_1m"]
    )
    working.loc[working["sector_confidence_1m"].eq("高"), "swing_score_1m"] += float(selection_config.get("sector_confidence_bonus_high_1m", 0.0) or 0.0)
    working.loc[working["sector_confidence_1m"].eq("中"), "swing_score_1m"] += float(selection_config.get("sector_confidence_bonus_mid_1m", 0.0) or 0.0)
    working["stretch_penalty_applied_1m"] = working["moderate_extension_flag_1m"] | working["severe_extension_flag_1m"]
    working.loc[working["stretch_penalty_applied_1m"], "swing_score_1m"] -= 0.40
    working["pass_score_gate_1m"] = (
        working["swing_score_1m"].fillna(-999.0).ge(float(selection_config.get("score_gate_1m", 2.9) or 2.9))
        & working["in_scope_1m"]
        & working["pass_trend_gate_1m"]
    )
    working["candidate_quality_score_1m"] = 0.0
    working.loc[working["sector_tailwind_band_1m"].eq("strong"), "candidate_quality_score_1m"] += 1.0
    working.loc[working["sector_tailwind_band_1m"].eq("mid"), "candidate_quality_score_1m"] += 0.55
    working.loc[working["pass_trend_gate_1m"], "candidate_quality_score_1m"] += 1.4
    working.loc[working["pass_flow_gate_1m"], "candidate_quality_score_1m"] += 0.8
    working.loc[working["pass_live_gate_1m"], "candidate_quality_score_1m"] += 0.4
    working.loc[working["liquidity_ok"], "candidate_quality_score_1m"] += 0.5
    working.loc[~working["finance_risk_flag"], "candidate_quality_score_1m"] += 0.4
    working.loc[working["stretch_penalty_applied_1m"], "candidate_quality_score_1m"] -= 0.40
    working.loc[working["earnings_unknown_flag"], "candidate_quality_score_1m"] -= 0.2
    working.loc[working["earnings_risk_flag_1m"], "candidate_quality_score_1m"] -= 1.0
    working.loc[working["finance_risk_flag"], "candidate_quality_score_1m"] -= 1.0
    working["candidate_quality_1m"] = "低"
    working.loc[working["candidate_quality_score_1m"] >= 4.0, "candidate_quality_1m"] = "高"
    working.loc[(working["candidate_quality_score_1m"] >= 2.8) & (working["candidate_quality_score_1m"] < 4.0), "candidate_quality_1m"] = "中"
    working["candidate_sector_component_3m"] = working["sector_tailwind_bonus_3m"]
    working["candidate_rs_component_3m_long"] = _score_percentile(working["rs_vs_topix_3m"]) * 1.15
    working["candidate_ret_component_3m"] = _score_percentile(working["ret_3m"]) * 0.90
    working["candidate_confirmation_component_3m"] = _score_percentile(working["rs_vs_topix_1m"]) * 0.55
    working["candidate_liquidity_component_3m"] = _score_percentile(working["avg_turnover_20d"]) * 0.55
    working["candidate_breadth_component_3m"] = _score_percentile(working["sector_positive_ratio_3m"]) * 0.40
    working["candidate_finance_component_3m"] = 0.0
    working.loc[finance_score >= 0.0, "candidate_finance_component_3m"] = 0.20
    working.loc[finance_score < -1.0, "candidate_finance_component_3m"] = -0.80
    working["swing_score_3m"] = (
        working["candidate_sector_component_3m"]
        + working["candidate_rs_component_3m_long"]
        + working["candidate_ret_component_3m"]
        + working["candidate_confirmation_component_3m"]
        + working["candidate_liquidity_component_3m"]
        + working["candidate_breadth_component_3m"]
        + working["candidate_finance_component_3m"]
    )
    working.loc[working["sector_confidence_3m"].eq("高"), "swing_score_3m"] += float(selection_config.get("sector_confidence_bonus_high_3m", 0.25) or 0.25)
    working.loc[working["sector_confidence_3m"].eq("中"), "swing_score_3m"] += float(selection_config.get("sector_confidence_bonus_mid_3m", 0.10) or 0.10)
    working["stretch_penalty_applied_3m"] = working["moderate_extension_flag_1m"] | working["severe_extension_flag_1m"]
    working.loc[working["stretch_penalty_applied_3m"], "swing_score_3m"] -= 0.35
    working["pass_score_gate_3m"] = (
        working["swing_score_3m"].fillna(-999.0).ge(score_gate_3m)
        & working["in_scope_3m"]
        & working["pass_trend_gate_3m"]
        & working["pass_live_gate_3m"]
    )
    working["candidate_quality_score_3m"] = 0.0
    working.loc[working["sector_tailwind_band_3m"].eq("strong"), "candidate_quality_score_3m"] += 1.0
    working.loc[working["sector_tailwind_band_3m"].eq("mid"), "candidate_quality_score_3m"] += 0.55
    working.loc[working["pass_trend_gate_3m"], "candidate_quality_score_3m"] += 1.45
    working.loc[working["pass_live_gate_3m"], "candidate_quality_score_3m"] += 0.85
    working.loc[working["pass_flow_gate_3m"], "candidate_quality_score_3m"] += 0.75
    working.loc[working["liquidity_ok"], "candidate_quality_score_3m"] += 0.45
    working.loc[working["stretch_penalty_applied_3m"], "candidate_quality_score_3m"] -= 0.35
    working.loc[working["earnings_unknown_flag"], "candidate_quality_score_3m"] -= 0.2
    working.loc[working["earnings_risk_flag_1m"], "candidate_quality_score_3m"] -= 1.0
    working.loc[working["finance_risk_flag"], "candidate_quality_score_3m"] -= 0.9
    working.loc[working["month_confirmation_broken_3m"], "candidate_quality_score_3m"] -= 1.2
    working["candidate_quality_3m"] = "低"
    working.loc[working["candidate_quality_score_3m"] >= 4.0, "candidate_quality_3m"] = "高"
    working.loc[(working["candidate_quality_score_3m"] >= 2.8) & (working["candidate_quality_score_3m"] < 4.0), "candidate_quality_3m"] = "中"

    working["selection_reason_1w"] = working.apply(_swing_reason_1w_v2, axis=1)
    working["selection_reason_1m"] = working.apply(_swing_reason_1m_v2, axis=1)
    working["selection_reason_3m"] = working.apply(_swing_reason_3m_v2, axis=1)
    working["risk_note_1w"] = working.apply(_swing_risk_note_1w_v2, axis=1)
    working["risk_note_1m"] = working.apply(_swing_risk_note_1m_v2, axis=1)
    working["risk_note_3m"] = working.apply(_swing_risk_note_3m_v2, axis=1)
    working = _apply_horizon_buy_scores(working, selection_config=selection_config)
    working["swing_score_1w"] = working["buy_score_1w"]
    working["swing_score_1m"] = working["buy_score_1m"]
    working["swing_score_3m"] = working["buy_score_3m"]
    buy_score_gate_1w = float(selection_config.get("buy_score_gate_1w", 1.05) or 1.05)
    buy_score_gate_1m = float(selection_config.get("buy_score_gate_1m", 1.25) or 1.25)
    buy_score_gate_3m = float(selection_config.get("buy_score_gate_3m", 1.85) or 1.85)
    working["pass_score_gate_1w"] = (
        working["buy_score_1w"].fillna(-999.0).ge(buy_score_gate_1w)
        & working["in_scope_1w"]
        & (working["pass_live_gate_1w"] | working["pass_trend_gate_1w"])
    )
    working["pass_score_gate_1m"] = (
        working["buy_score_1m"].fillna(-999.0).ge(buy_score_gate_1m)
        & working["in_scope_1m"]
        & working["pass_trend_gate_1m"]
    )
    working["pass_score_gate_3m"] = (
        working["buy_score_3m"].fillna(-999.0).ge(buy_score_gate_3m)
        & working["in_scope_3m"]
        & working["pass_trend_gate_3m"]
        & working["pass_live_gate_3m"]
    )
    working.loc[working["buy_score_1w"].fillna(-999.0).lt(buy_score_gate_1w - 0.25), "candidate_quality_1w"] = "低"
    working.loc[working["buy_score_1m"].fillna(-999.0).lt(buy_score_gate_1m - 0.25), "candidate_quality_1m"] = "低"
    working.loc[working["buy_score_3m"].fillna(-999.0).lt(buy_score_gate_3m - 0.25), "candidate_quality_3m"] = "低"
    working["candidate_commentary_1w"] = working.apply(
        lambda row: _build_candidate_commentary(row.get("selection_reason_1w", ""), row.get("risk_note_1w", "")),
        axis=1,
    )
    working["candidate_commentary_1m"] = working.apply(
        lambda row: _build_candidate_commentary(row.get("selection_reason_1m", ""), row.get("risk_note_1m", "")),
        axis=1,
    )
    working["candidate_commentary_3m"] = working.apply(
        lambda row: _build_candidate_commentary(row.get("selection_reason_3m", ""), row.get("risk_note_3m", "")),
        axis=1,
    )
    entry_stance_1w = pd.DataFrame(list(working.apply(_entry_stance_payload_1w, axis=1)), index=working.index)
    working["entry_stance_raw_1w"] = entry_stance_1w["entry_stance_raw"]
    working["entry_stance_label_1w"] = entry_stance_1w["entry_stance_label"]
    working["stretch_caution_label_1w"] = entry_stance_1w["stretch_caution_label"]
    working["watch_reason_label_1w"] = entry_stance_1w["watch_reason_label"]
    entry_stance_1m = pd.DataFrame(list(working.apply(_entry_stance_payload_1m, axis=1)), index=working.index)
    working["entry_stance_raw_1m"] = entry_stance_1m["entry_stance_raw"]
    working["entry_stance_label_1m"] = entry_stance_1m["entry_stance_label"]
    working["stretch_caution_label_1m"] = entry_stance_1m["stretch_caution_label"]
    working["watch_reason_label_1m"] = entry_stance_1m["watch_reason_label"]
    entry_stance_3m = pd.DataFrame(list(working.apply(_entry_stance_payload_3m, axis=1)), index=working.index)
    working["entry_stance_raw_3m"] = entry_stance_3m["entry_stance_raw"]
    working["entry_stance_label_3m"] = entry_stance_3m["entry_stance_label"]
    working["stretch_caution_label_3m"] = entry_stance_3m["stretch_caution_label"]
    working["watch_reason_label_3m"] = entry_stance_3m["watch_reason_label"]
    working["entry_fit_1w"] = working.apply(
        lambda row: _entry_fit_1w_label_v2(
            candidate_quality=str(row.get("candidate_quality_1w", "")),
            pass_score_gate=bool(row.get("pass_score_gate_1w")),
            pass_live_gate=bool(row.get("pass_live_gate_1w")),
            pass_trend_gate=bool(row.get("pass_trend_gate_1w")),
            pass_flow_gate=bool(row.get("pass_flow_gate_1w")),
            pass_quality_gate=bool(row.get("pass_quality_gate_1w")),
            hard_block_reason=str(row.get("hard_block_reason_raw_1w", "") or ""),
            extension_flag=bool(row.get("extension_flag_1w")),
            sector_confidence=str(row.get("sector_confidence_1w", "")),
            today_not_broken=bool(row.get("today_not_broken_1w")),
            intraday_fade=bool(row.get("intraday_fade_flag_1w")),
            one_week_edge=bool(row.get("one_week_edge_1w")),
            medium_term_not_broken=bool(row.get("medium_term_not_broken_1w")),
            chase_risk=bool(row.get("chase_risk_flag_1w")),
            live_ret_vs_prev_close=float(_coerce_numeric(pd.Series([row.get("live_ret_vs_prev_close", pd.NA)])).fillna(0.0).iloc[0] or 0.0),
        ),
        axis=1,
    )
    working["entry_fit_1m"] = working.apply(
        lambda row: _entry_fit_1m_label_v2(
            candidate_quality=str(row.get("candidate_quality_1m", "")),
            belongs_persistence_sector=bool(row.get("in_scope_1m")),
            pass_live_gate=bool(row.get("pass_live_gate_1m")),
            pass_trend_gate=bool(row.get("pass_trend_gate_1m")),
            pass_flow_gate=bool(row.get("pass_flow_gate_1m")),
            pass_quality_gate=bool(row.get("pass_quality_gate_1m")),
            hard_block_reason=str(row.get("hard_block_reason_raw_1m", "") or ""),
            extension_flag=bool(row.get("extension_flag_1m")),
            sector_confidence=str(row.get("sector_confidence_1m", "")),
        ),
        axis=1,
    )
    working["entry_fit_3m"] = working.apply(
        lambda row: _entry_fit_3m_label_v2(
            candidate_quality=str(row.get("candidate_quality_3m", "")),
            belongs_persistence_sector=bool(row.get("in_scope_3m")),
            pass_live_gate=bool(row.get("pass_live_gate_3m")),
            pass_trend_gate=bool(row.get("pass_trend_gate_3m")),
            pass_flow_gate=bool(row.get("pass_flow_gate_3m")),
            pass_quality_gate=bool(row.get("pass_quality_gate_3m")),
            hard_block_reason=str(row.get("hard_block_reason_raw_3m", "") or ""),
            extension_flag=bool(row.get("stretch_penalty_applied_3m")),
            sector_confidence=str(row.get("sector_confidence_3m", "")),
        ),
        axis=1,
    )
    working["entry_fit_priority_1w"] = working["entry_fit_1w"].map(_entry_fit_sort_priority)
    working["entry_fit_priority_1m"] = working["entry_fit_1m"].map(_entry_fit_sort_priority)
    working["entry_fit_priority_3m"] = working["entry_fit_3m"].map(_entry_fit_sort_priority)

    def _project_swing_candidates(source: pd.DataFrame, *, horizon: str) -> pd.DataFrame:
        if source is None or source.empty:
            return pd.DataFrame()
        if horizon == "1w":
            gate_col = "sector_gate_pass_1w"
            sort_columns = [gate_col, "entry_fit_priority_1w", "pass_score_gate_1w", "sector_confidence_priority_1w", "today_not_broken_1w", "one_week_edge_1w", "medium_term_not_broken_1w", "candidate_quality_score_1w", "swing_score_1w", "live_turnover_value", "rs_vs_topix_1w", "price_vs_ma20_abs"]
            ascending = [False, True, False, False, False, False, False, False, False, False, False, True]
            selected_columns = ["code", "name", "sector_name", "candidate_quality_1w", "entry_fit_1w", "selection_reason_1w", "earnings_announcement_date", "risk_note_1w", "candidate_commentary_1w", "entry_stance_raw_1w", "entry_stance_label_1w", "stretch_caution_label_1w", "watch_reason_label_1w", "swing_score_1w", "rs_vs_topix_1w", "live_ret_vs_prev_close", "current_price", "current_price_unavailable", "live_turnover_value", "live_turnover_unavailable", "earnings_buffer_days", "nikkei_search", "material_link", gate_col]
            rename_map = {
                "candidate_quality_1w": "candidate_quality",
                "entry_fit_1w": "entry_fit",
                "selection_reason_1w": "selection_reason",
                "risk_note_1w": "risk_note",
                "candidate_commentary_1w": "candidate_commentary",
                "entry_stance_raw_1w": "entry_stance_raw",
                "entry_stance_label_1w": "entry_stance_label",
                "stretch_caution_label_1w": "stretch_caution_label",
                "watch_reason_label_1w": "watch_reason_label",
            }
        elif horizon == "3m":
            gate_col = "sector_gate_pass_3m"
            sort_columns = [gate_col, "entry_fit_priority_3m", "sector_confidence_priority_3m", "candidate_quality_score_3m", "pass_score_gate_3m", "swing_score_3m", "rs_vs_topix_3m", "ret_3m", "avg_turnover_20d"]
            ascending = [False, True, False, False, False, False, False, False, False]
            selected_columns = ["code", "name", "sector_name", "candidate_quality_3m", "entry_fit_3m", "selection_reason_3m", "earnings_announcement_date", "risk_note_3m", "candidate_commentary_3m", "entry_stance_raw_3m", "entry_stance_label_3m", "stretch_caution_label_3m", "watch_reason_label_3m", "swing_score_3m", "rs_vs_topix_3m", "ret_3m", "avg_turnover_20d", "live_ret_vs_prev_close", "current_price", "current_price_unavailable", "live_turnover_value", "live_turnover_unavailable", "earnings_buffer_days", "nikkei_search", "material_link", gate_col]
            rename_map = {
                "candidate_quality_3m": "candidate_quality",
                "entry_fit_3m": "entry_fit",
                "selection_reason_3m": "selection_reason",
                "risk_note_3m": "risk_note",
                "candidate_commentary_3m": "candidate_commentary",
                "entry_stance_raw_3m": "entry_stance_raw",
                "entry_stance_label_3m": "entry_stance_label",
                "stretch_caution_label_3m": "stretch_caution_label",
                "watch_reason_label_3m": "watch_reason_label",
            }
        else:
            gate_col = "sector_gate_pass_1m"
            sort_columns = [gate_col, "entry_fit_priority_1m", "sector_confidence_priority_1m", "candidate_quality_score_1m", "pass_score_gate_1m", "swing_score_1m", "rs_vs_topix_1m", "rs_vs_topix_3m", "price_vs_ma20_abs", "live_turnover_value"]
            ascending = [False, True, False, False, False, False, False, False, True, False]
            selected_columns = ["code", "name", "sector_name", "candidate_quality_1m", "entry_fit_1m", "selection_reason_1m", "earnings_announcement_date", "risk_note_1m", "candidate_commentary_1m", "entry_stance_raw_1m", "entry_stance_label_1m", "stretch_caution_label_1m", "watch_reason_label_1m", "swing_score_1m", "rs_vs_topix_1m", "rs_vs_topix_3m", "live_ret_vs_prev_close", "current_price", "current_price_unavailable", "live_turnover_value", "live_turnover_unavailable", "price_vs_ma20_pct", "earnings_buffer_days", "finance_health_flag", "nikkei_search", "material_link", gate_col]
            rename_map = {
                "candidate_quality_1m": "candidate_quality",
                "entry_fit_1m": "entry_fit",
                "selection_reason_1m": "selection_reason",
                "risk_note_1m": "risk_note",
                "candidate_commentary_1m": "candidate_commentary",
                "entry_stance_raw_1m": "entry_stance_raw",
                "entry_stance_label_1m": "entry_stance_label",
                "stretch_caution_label_1m": "stretch_caution_label",
                "watch_reason_label_1m": "watch_reason_label",
            }
        source = source.copy()
        source["selected_horizon"] = horizon
        source["buy_score_total"] = source.get(f"buy_score_{horizon}", pd.Series(0.0, index=source.index))
        source["buy_strength_score"] = source.get(f"buy_strength_score_{horizon}", pd.Series(0.0, index=source.index))
        source["entry_timing_adjustment"] = source.get(f"entry_timing_adjustment_{horizon}", pd.Series(0.0, index=source.index))
        source["sector_strength_score"] = source.get(f"sector_strength_score_{horizon}", pd.Series(0.0, index=source.index))
        source["relative_strength_score"] = source.get(f"relative_strength_score_{horizon}", pd.Series(0.0, index=source.index))
        source["earnings_risk_score"] = source.get(f"earnings_risk_score_{horizon}", pd.Series(0.0, index=source.index))
        source["overheating_penalty"] = source.get(f"overheating_penalty_{horizon}", pd.Series(0.0, index=source.index))
        source["abnormal_event_penalty"] = source.get(f"abnormal_event_penalty_{horizon}", pd.Series(0.0, index=source.index))
        source["fallback_penalty"] = source.get(f"fallback_penalty_{horizon}", pd.Series(0.0, index=source.index))
        source["score_components"] = source.get(f"score_components_{horizon}", pd.Series([{} for _ in range(len(source))], index=source.index))
        source["selected_reason"] = source.get(f"selection_reason_{horizon}", pd.Series("", index=source.index))
        source["rejected_reason"] = source.get(f"rejected_reason_{horizon}", pd.Series("", index=source.index))
        source["horizon_fit_reason"] = source.get(f"horizon_fit_reason_{horizon}", pd.Series("", index=source.index))
        source["entry_caution"] = source.get(f"entry_caution_{horizon}", pd.Series("", index=source.index))
        source["event_candidate_flag"] = source.get(f"event_candidate_flag_{horizon}", pd.Series(False, index=source.index)).fillna(False).astype(bool)
        source["event_candidate_type"] = source.get(f"event_candidate_type_{horizon}", pd.Series("", index=source.index))
        source["candidate_bucket"] = source.get(f"candidate_bucket_{horizon}", pd.Series("", index=source.index))
        source["candidate_bucket_label"] = source.get(f"candidate_bucket_label_{horizon}", pd.Series("", index=source.index))
        source["event_caution_reason"] = source.get(f"event_caution_reason_{horizon}", pd.Series("", index=source.index))
        source["fallback_used"] = source.get(f"fallback_used_{horizon}", pd.Series(False, index=source.index)).fillna(False).astype(bool)
        common_audit_columns = [
            "selected_horizon",
            "buy_score_total",
            "buy_score_1w",
            "buy_score_1m",
            "buy_score_3m",
            "buy_strength_score",
            "buy_strength_score_1w",
            "buy_strength_score_1m",
            "buy_strength_score_3m",
            "entry_timing_adjustment",
            "entry_timing_adjustment_1w",
            "entry_timing_adjustment_1m",
            "entry_timing_adjustment_3m",
            "score_components",
            "sector_strength_score",
            "relative_strength_score",
            "liquidity_score",
            "earnings_risk_score",
            "overheating_penalty",
            "abnormal_event_penalty",
            "fallback_penalty",
            "selected_reason",
            "rejected_reason",
            "horizon_fit_reason",
            "entry_caution",
            "event_candidate_flag",
            "event_candidate_type",
            "candidate_bucket",
            "candidate_bucket_label",
            "event_caution_reason",
            "fallback_used",
        ]
        selected_columns.extend([column for column in common_audit_columns if column in source.columns and column not in selected_columns])
        ordered = source.sort_values(sort_columns, ascending=ascending, kind="mergesort").copy()
        table = ordered[selected_columns].reset_index(drop=True).rename(columns=rename_map)
        gate_pass = ordered.get(gate_col, pd.Series(True, index=ordered.index)).fillna(False).astype(bool).reset_index(drop=True)
        if "risk_note" in table.columns:
            table["risk_note"] = [
                _append_warning_note(value, "セクターgate未達") if not bool(pass_flag) else str(value or "").strip()
                for value, pass_flag in zip(table["risk_note"], gate_pass)
            ]
        return table.drop(columns=[gate_col], errors="ignore")

    def _finalize_swing_table(primary: pd.DataFrame, fallback: pd.DataFrame, *, total_limit: int, limit_per_sector: int) -> tuple[pd.DataFrame, set[str]]:
        capped_primary, _ = _apply_swing_display_cap(
            primary,
            sector_col="sector_name",
            total_limit=total_limit,
            limit_per_sector=limit_per_sector,
        )
        min_rows = min(3, total_limit)
        final = _fill_swing_display_minimum(
            capped_primary,
            fallback,
            min_rows=min_rows,
            total_limit=total_limit,
            sector_col="sector_name",
            limit_per_sector=limit_per_sector,
        )
        primary_codes = set(capped_primary.get("code", pd.Series(dtype=str)).astype(str).tolist()) if not capped_primary.empty else set()
        all_codes = set(pd.concat([primary, fallback], ignore_index=True).get("code", pd.Series(dtype=str)).astype(str).tolist()) if (not primary.empty or not fallback.empty) else set()
        selected_codes = set(final.get("code", pd.Series(dtype=str)).astype(str).tolist()) if not final.empty else set()
        supplemented_codes = {code for code in selected_codes if code and code not in primary_codes}
        if supplemented_codes and not final.empty:
            supplemented_mask = final.get("code", pd.Series(dtype=str)).astype(str).isin(supplemented_codes)
            if supplemented_mask.any():
                final = final.copy()
                if "entry_fit" in final.columns:
                    final.loc[supplemented_mask, "entry_fit"] = "補完・監視"
                final.loc[supplemented_mask, "entry_stance_label"] = "補完・監視"
                final.loc[supplemented_mask, "watch_reason_label"] = "表示件数不足のため補完"
                if "fallback_used" in final.columns:
                    final.loc[supplemented_mask, "fallback_used"] = True
                if "fallback_penalty" in final.columns:
                    final.loc[supplemented_mask, "fallback_penalty"] = -0.85
                if "entry_timing_adjustment" in final.columns:
                    final.loc[supplemented_mask, "entry_timing_adjustment"] = _coerce_numeric(final.loc[supplemented_mask, "entry_timing_adjustment"]).fillna(0.0) - 0.85
                if "buy_score_total" in final.columns:
                    final.loc[supplemented_mask, "buy_score_total"] = _coerce_numeric(final.loc[supplemented_mask, "buy_score_total"]).fillna(0.0) - 0.85
                for score_horizon in ["1w", "1m", "3m"]:
                    score_column = f"buy_score_{score_horizon}"
                    timing_column = f"entry_timing_adjustment_{score_horizon}"
                    if score_column in final.columns:
                        horizon_mask = supplemented_mask & final.get("selected_horizon", pd.Series("", index=final.index)).astype(str).eq(score_horizon)
                        final.loc[horizon_mask, score_column] = _coerce_numeric(final.loc[horizon_mask, score_column]).fillna(0.0) - 0.85
                    if timing_column in final.columns:
                        horizon_mask = supplemented_mask & final.get("selected_horizon", pd.Series("", index=final.index)).astype(str).eq(score_horizon)
                        final.loc[horizon_mask, timing_column] = _coerce_numeric(final.loc[horizon_mask, timing_column]).fillna(0.0) - 0.85
                if "score_components" in final.columns:
                    final.loc[supplemented_mask, "score_components"] = final.loc[supplemented_mask, "score_components"].apply(
                        lambda value: {**(value if isinstance(value, dict) else {}), "fallback_penalty": -0.85, "entry_timing_adjustment": float((value if isinstance(value, dict) else {}).get("entry_timing_adjustment", 0.0) or 0.0) - 0.85}
                    )
                for column in ["selection_reason", "candidate_commentary"]:
                    if column in final.columns:
                        final.loc[supplemented_mask, column] = final.loc[supplemented_mask, column].apply(
                            lambda value: "表示件数不足のため補完。通常の買い候補より信頼度は低い"
                            if pd.isna(value) or not str(value).strip()
                            else f"表示件数不足のため補完。通常の買い候補より信頼度は低い / {str(value).strip()}"
                        )
                if "selected_reason" in final.columns:
                    final.loc[supplemented_mask, "selected_reason"] = final.loc[supplemented_mask, "selection_reason"] if "selection_reason" in final.columns else "表示件数不足のため補完。通常の買い候補より信頼度は低い"
        return final, {code for code in all_codes if code and code not in selected_codes}

    swing_1w_source = working[
        working["candidate_quality_1w"].isin(["高", "中"])
        & working["entry_fit_1w"].isin(["買い候補", "監視候補"])
        & working["pass_score_gate_1w"]
    ].copy()
    swing_1m_source = working[
        working["candidate_quality_1m"].isin(["高", "中"])
        & working["entry_fit_1m"].isin(["買い候補", "監視候補"])
        & working["pass_score_gate_1m"]
    ].copy()
    swing_3m_source = working[
        working["candidate_quality_3m"].isin(["高", "中"])
        & working["entry_fit_3m"].isin(["買い候補", "監視候補"])
        & working["pass_score_gate_3m"]
    ].copy()
    swing_1w_fallback_source = working[
        working["in_scope_1w"]
        & working["hard_block_reason_raw_1w"].astype(str).str.strip().eq("")
    ].copy()
    swing_1m_fallback_source = working[
        working["in_scope_1m"]
        & working["hard_block_reason_raw_1m"].astype(str).str.strip().eq("")
    ].copy()
    swing_3m_fallback_source = working[
        working["in_scope_3m"]
        & working["hard_block_reason_raw_3m"].astype(str).str.strip().eq("")
    ].copy()
    swing_1w_primary = _project_swing_candidates(swing_1w_source, horizon="1w")
    swing_1m_primary = _project_swing_candidates(swing_1m_source, horizon="1m")
    swing_3m_primary = _project_swing_candidates(swing_3m_source, horizon="3m")
    swing_1w_fallback = _project_swing_candidates(swing_1w_fallback_source, horizon="1w")
    swing_1m_fallback = _project_swing_candidates(swing_1m_fallback_source, horizon="1m")
    swing_3m_fallback = _project_swing_candidates(swing_3m_fallback_source, horizon="3m")
    display_total_limit_1w = int(selection_config.get("display_total_limit_1w", 5) or 5)
    display_total_limit_1m = int(selection_config.get("display_total_limit_1m", 5) or 5)
    display_total_limit_3m = int(selection_config.get("display_total_limit_3m", 5) or 5)
    display_sector_limit_1w = int(selection_config.get("display_sector_limit_1w", 2) or 2)
    display_sector_limit_1m = int(selection_config.get("display_sector_limit_1m", 2) or 2)
    display_sector_limit_3m = int(selection_config.get("display_sector_limit_3m", 2) or 2)
    swing_1w, pruned_codes_1w = _finalize_swing_table(
        swing_1w_primary,
        swing_1w_fallback,
        total_limit=display_total_limit_1w,
        limit_per_sector=display_sector_limit_1w,
    )
    swing_1m, pruned_codes_1m = _finalize_swing_table(
        swing_1m_primary,
        swing_1m_fallback,
        total_limit=display_total_limit_1m,
        limit_per_sector=display_sector_limit_1m,
    )
    swing_3m, pruned_codes_3m = _finalize_swing_table(
        swing_3m_primary,
        swing_3m_fallback,
        total_limit=display_total_limit_3m,
        limit_per_sector=display_sector_limit_3m,
    )
    if not swing_1w.empty:
        swing_1w.insert(0, "candidate_rank_1w", range(1, len(swing_1w) + 1))
    if not swing_1m.empty:
        swing_1m.insert(0, "candidate_rank_1m", range(1, len(swing_1m) + 1))
    if not swing_3m.empty:
        swing_3m.insert(0, "candidate_rank_3m", range(1, len(swing_3m) + 1))
    swing_buy_1w = swing_1w[swing_1w["entry_fit"].eq("買い候補")].head(5).reset_index(drop=True) if not swing_1w.empty else pd.DataFrame(columns=swing_1w.columns)
    swing_watch_1w = swing_1w[swing_1w["entry_fit"].eq("監視候補")].head(5).reset_index(drop=True) if not swing_1w.empty else pd.DataFrame(columns=swing_1w.columns)
    swing_buy_1m = swing_1m[swing_1m["entry_fit"].eq("買い候補")].head(5).reset_index(drop=True) if not swing_1m.empty else pd.DataFrame(columns=swing_1m.columns)
    swing_watch_1m = swing_1m[swing_1m["entry_fit"].eq("監視候補")].head(5).reset_index(drop=True) if not swing_1m.empty else pd.DataFrame(columns=swing_1m.columns)
    swing_buy_3m = swing_3m[swing_3m["entry_fit"].eq("買い候補")].head(5).reset_index(drop=True) if not swing_3m.empty else pd.DataFrame(columns=swing_3m.columns)
    swing_watch_3m = swing_3m[swing_3m["entry_fit"].eq("監視候補")].head(5).reset_index(drop=True) if not swing_3m.empty else pd.DataFrame(columns=swing_3m.columns)
    if not swing_buy_1w.empty:
        swing_buy_1w["candidate_rank_1w"] = range(1, len(swing_buy_1w) + 1)
    if not swing_watch_1w.empty:
        swing_watch_1w["candidate_rank_1w"] = range(1, len(swing_watch_1w) + 1)
    if not swing_buy_1m.empty:
        swing_buy_1m["candidate_rank_1m"] = range(1, len(swing_buy_1m) + 1)
    if not swing_watch_1m.empty:
        swing_watch_1m["candidate_rank_1m"] = range(1, len(swing_watch_1m) + 1)
    if not swing_buy_3m.empty:
        swing_buy_3m["candidate_rank_3m"] = range(1, len(swing_buy_3m) + 1)
    if not swing_watch_3m.empty:
        swing_watch_3m["candidate_rank_3m"] = range(1, len(swing_watch_3m) + 1)

    selected_codes_1w = set(swing_1w.get("code", pd.Series(dtype=str)).astype(str).tolist())
    selected_codes_1m = set(swing_1m.get("code", pd.Series(dtype=str)).astype(str).tolist())
    selected_codes_3m = set(swing_3m.get("code", pd.Series(dtype=str)).astype(str).tolist())
    for horizon, table in [("1w", swing_1w), ("1m", swing_1m), ("3m", swing_3m)]:
        if table is None or table.empty or "fallback_used" not in table.columns:
            continue
        fallback_codes = set(table.loc[table["fallback_used"].fillna(False).astype(bool), "code"].astype(str).tolist())
        if not fallback_codes:
            continue
        fallback_mask = working["code"].astype(str).isin(fallback_codes)
        working.loc[fallback_mask, f"fallback_used_{horizon}"] = True
        working.loc[fallback_mask, f"fallback_penalty_{horizon}"] = -0.85
        if f"entry_timing_adjustment_{horizon}" in working.columns:
            working.loc[fallback_mask, f"entry_timing_adjustment_{horizon}"] = _coerce_numeric(working.loc[fallback_mask, f"entry_timing_adjustment_{horizon}"]).fillna(0.0) - 0.85
        working.loc[fallback_mask, f"buy_score_{horizon}"] = _coerce_numeric(working.loc[fallback_mask, f"buy_score_{horizon}"]).fillna(0.0) - 0.85
        working.loc[fallback_mask, f"swing_score_{horizon}"] = working.loc[fallback_mask, f"buy_score_{horizon}"]
        working.loc[fallback_mask, f"score_components_{horizon}"] = working.loc[fallback_mask, f"score_components_{horizon}"].apply(
            lambda value: {**(value if isinstance(value, dict) else {}), "fallback_penalty": -0.85, "entry_timing_adjustment": float((value if isinstance(value, dict) else {}).get("entry_timing_adjustment", 0.0) or 0.0) - 0.85}
        )
    working["display_sector_cap_pruned_1w"] = working["code"].astype(str).isin(pruned_codes_1w)
    working["display_sector_cap_pruned_1m"] = working["code"].astype(str).isin(pruned_codes_1m)
    working["display_sector_cap_pruned_3m"] = working["code"].astype(str).isin(pruned_codes_3m)
    working["override_selected_outside_top_sector_1w"] = working["code"].astype(str).isin(selected_codes_1w) & working["sector_tailwind_band_1w"].eq("none")
    working["override_selected_outside_top_sector_1m"] = working["code"].astype(str).isin(selected_codes_1m) & working["sector_tailwind_band_1m"].eq("none")
    working["override_selected_outside_top_sector_3m"] = working["code"].astype(str).isin(selected_codes_3m) & working["sector_tailwind_band_3m"].eq("none")
    audit_1w = _build_swing_candidate_audit_frame(
        working,
        horizon="1w",
        target_sector_col="in_scope_1w",
        live_col="pass_live_gate_1w",
        trend_col="pass_trend_gate_1w",
        flow_col="pass_flow_gate_1w",
        quality_col="pass_quality_gate_1w",
        hard_block_col="hard_block_reason_raw_1w",
        score_col="pass_score_gate_1w",
        score_total_col="swing_score_1w",
        display_reason_col="selection_reason_1w",
        selected_codes=selected_codes_1w,
    )
    audit_1m = _build_swing_candidate_audit_frame(
        working,
        horizon="1m",
        target_sector_col="in_scope_1m",
        live_col="pass_live_gate_1m",
        trend_col="pass_trend_gate_1m",
        flow_col="pass_flow_gate_1m",
        quality_col="pass_quality_gate_1m",
        hard_block_col="hard_block_reason_raw_1m",
        score_col="pass_score_gate_1m",
        score_total_col="swing_score_1m",
        display_reason_col="selection_reason_1m",
        selected_codes=selected_codes_1m,
    )
    audit_3m = _build_swing_candidate_audit_frame(
        working,
        horizon="3m",
        target_sector_col="in_scope_3m",
        live_col="pass_live_gate_3m",
        trend_col="pass_trend_gate_3m",
        flow_col="pass_flow_gate_3m",
        quality_col="pass_quality_gate_3m",
        hard_block_col="hard_block_reason_raw_3m",
        score_col="pass_score_gate_3m",
        score_total_col="swing_score_3m",
        display_reason_col="selection_reason_3m",
        selected_codes=selected_codes_3m,
    )
    empty_status_1w, empty_reason_1w = _resolve_swing_empty_reason(
        working,
        target_sector_col="in_scope_1w",
        hard_block_col="hard_block_reason_raw_1w",
        live_col="pass_live_gate_1w",
        trend_col="pass_trend_gate_1w",
        quality_col="pass_quality_gate_1w",
        score_col="pass_score_gate_1w",
    )
    empty_status_1m, empty_reason_1m = _resolve_swing_empty_reason(
        working,
        target_sector_col="in_scope_1m",
        hard_block_col="hard_block_reason_raw_1m",
        live_col="pass_live_gate_1m",
        trend_col="pass_trend_gate_1m",
        quality_col="pass_quality_gate_1m",
        score_col="pass_score_gate_1m",
    )
    empty_status_3m, empty_reason_3m = _resolve_swing_empty_reason(
        working,
        target_sector_col="in_scope_3m",
        hard_block_col="hard_block_reason_raw_3m",
        live_col="pass_live_gate_3m",
        trend_col="pass_trend_gate_3m",
        quality_col="pass_quality_gate_3m",
        score_col="pass_score_gate_3m",
    )
    if not swing_1w.empty:
        empty_status_1w, empty_reason_1w = "observed", ""
    if not swing_1m.empty:
        empty_status_1m, empty_reason_1m = "observed", ""
    if not swing_3m.empty:
        empty_status_3m, empty_reason_3m = "observed", ""
    audit_1w["empty_reason_label"] = empty_reason_1w
    audit_1m["empty_reason_label"] = empty_reason_1m
    audit_3m["empty_reason_label"] = empty_reason_3m
    return {
        "1w": swing_1w,
        "1m": swing_1m,
        "3m": swing_3m,
        "buy_1w": swing_buy_1w,
        "watch_1w": swing_watch_1w,
        "buy_1m": swing_buy_1m,
        "watch_1m": swing_watch_1m,
        "buy_3m": swing_buy_3m,
        "watch_3m": swing_watch_3m,
        "audit_1w": audit_1w,
        "audit_1m": audit_1m,
        "audit_3m": audit_3m,
        "empty_reason_1w": empty_reason_1w,
        "empty_reason_1m": empty_reason_1m,
        "empty_reason_3m": empty_reason_3m,
        "empty_status_1w": empty_status_1w,
        "empty_status_1m": empty_status_1m,
        "empty_status_3m": empty_status_3m,
    }


def _make_nikkei_search_link(name: str, code: str) -> str:
    del code
    name = str(name or "").strip()
    if not name:
        return ""
    # TODO: 必要になったら将来ここで事前ヒット確認を追加する。
    return f"https://www.nikkei.com/search?keyword={quote_plus(name)}"


def _timepoint_meaning(mode: str) -> str:
    return {
        "0915": "寄り付き主導",
        "1130": "前場継続",
        "1530": "引けまでの強さ",
        "now": "随時確認",
    }.get(str(mode), "")


REPRESENTATIVE_SELECTED_REASON_LABELS = {
    "center_leader": "中心株かつ当日牽引",
    "sector_support_leader": "セクター内の当日牽引補完",
    "center_fallback_leader": "中心性を優先した代替選出",
    "当日中心株不在": "当日中心株不在",
}

REPRESENTATIVE_QUALITY_FLAG_LABELS = {
    "quality_pass": "品質基準を満たす",
    "quality_warn": "品質要注意",
    "quality_fail": "品質基準未達",
    "fallback": "品質要注意",
    "excluded": "品質基準未達",
    "no_valid_today_representative": "代表なし",
}

REPRESENTATIVE_FALLBACK_REASON_LABELS = {
    "fallback_no_clear_leader": "明確な当日牽引株がないため代替選出",
    "fallback_insufficient_candidates": "適格候補不足のため代替選出",
    "no_quality_candidate_met_center_leader_gate": "明確な当日牽引株がないため代替選出",
    "no_positive_candidate_in_selected50": "明確な当日牽引株がないため代替選出",
    "filled_remaining_support_slots_with_best_available_nonblocked_candidates": "適格候補不足のため代替選出",
    "プラス候補または適格候補なし": "プラス候補または適格候補なし",
}
DISPLAY_UNAVAILABLE_MARK = "—"


def _representative_display_label(value: Any, mapping: dict[str, str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return mapping.get(raw, "未定義の判定")


def _representative_selected_reason_label(value: Any) -> str:
    return _representative_display_label(value, REPRESENTATIVE_SELECTED_REASON_LABELS)


def _representative_quality_flag_label(value: Any) -> str:
    return _representative_display_label(value, REPRESENTATIVE_QUALITY_FLAG_LABELS)


def _representative_fallback_reason_label(value: Any) -> str:
    return _representative_display_label(value, REPRESENTATIVE_FALLBACK_REASON_LABELS)


DISPLAY_REASON_TEXT_ALIASES = {
    "1wセクター強い": "1週間で強いセクターに属する",
    "todayも崩れず": "今日も大きく下げていない",
    "出来高あり": "出来高あり",
    "TOPIX比で強い": "市場平均より強い",
    "1m崩れ小": "直近1か月も大きく崩れていない",
    "押し待ち": "押し目を待ちたい",
    "1w条件はあるが追認弱め": "1週間の条件はあるが確認待ち",
    "1か月トレンド継続": "1か月の上昇傾向が続いている",
    "中期トレンド維持": "1か月の上昇傾向が続いている",
    "中期の資金流入継続": "1か月以上の買いが続いている",
    "押し目後の再加速": "押し目後に持ち直している",
    "セクター上昇と整合": "1か月で強いセクターに属する",
    "中期の値崩れが小さい": "直近1か月も崩れにくい",
    "中期条件はあるが押し待ち": "中期条件はあるが押し目待ち",
    "3か月主導継続": "3か月の強さが続いている",
    "長めの上昇主導を維持": "3か月の上昇傾向が続いている",
    "1か月の崩れが小さい": "直近1か月も大きく崩れていない",
    "流動性が伴う": "流動性がある",
    "3か月主力セクターと整合": "3か月で強いセクターに属する",
    "長めの主導性はあるが確認余地あり": "3か月の条件はあるが確認待ち",
    "表示件数不足のため補完。通常の買い候補より信頼度は低い": "表示件数を満たすための補完",
    "押し目待ち推奨": "すぐ買うより押し目待ち寄り",
    "todayの値動き確認待ち": "今日の値動き確認待ち",
    "1mの下支え確認待ち": "直近1か月の下支え確認待ち",
    "セクター追い風の確認待ち": "セクターの追い風確認待ち",
    "短期追認の上積み待ち": "短期の追認待ち",
    "条件改善待ち": "条件改善待ち",
    "中期継続の確度を見極めたい": "中期継続の確認待ち",
    "中期継続の裏付け待ち": "中期継続の裏付け待ち",
    "長期条件の改善待ち": "長期条件の改善待ち",
    "長期主導の確度を見極めたい": "長期主導の確認待ち",
}

DISPLAY_RISK_TEXT_ALIASES = {
    "追撃は慎重": "すぐ買うより押し目待ち寄り",
    "短期過熱注意": "短期で上がりすぎ注意",
    "決算近い": "決算直前は買い注意",
    "today失速": "今日は伸びが鈍い",
    "流動性注意": "売買代金がやや少ない",
    "出来高弱め": "出来高がやや弱い",
    "1m弱め": "直近1か月はやや弱い",
    "20日線乖離大": "短期で上がりすぎ注意",
    "財務注意": "財務面は要確認",
    "1か月側が失速": "直近1か月は勢い鈍化",
    "セクターgate未達": "セクター条件は一部未達",
    "当日戻り待ち": "すぐ買うより押し目待ち寄り",
}

TODAY_REPRESENTATIVE_SELECTED_REASON_ALIASES = {
    "center_leader": "当日強い / セクター内上位",
    "中心株かつ当日牽引": "当日強い / セクター内上位",
    "sector_support_leader": "セクター内上位 / 代表候補を補完",
    "セクター内の当日牽引補完": "セクター内上位 / 代表候補を補完",
    "center_fallback_leader": "代表候補を補完",
    "中心性を優先した代替選出": "代表候補を補完",
}


def _normalize_display_tag_text(value: Any, mapping: dict[str, str]) -> str:
    text = _clean_ui_value(value)
    if not text:
        return ""
    normalized = text.replace("、", " / ").replace(",", " / ")
    parts = [part.strip() for part in normalized.split("/") if part.strip()]
    labels = [mapping.get(part, part) for part in parts]
    return " / ".join(dict.fromkeys([label for label in labels if label]))


def _candidate_reason_display_text(row: pd.Series, *, default: str = "") -> str:
    text = _first_ui_value(row.get("selection_reason"), row.get("watch_reason_label"), row.get("candidate_commentary"))
    normalized = _normalize_display_tag_text(text, DISPLAY_REASON_TEXT_ALIASES)
    return normalized or default


def _candidate_risk_display_text(row: pd.Series, *, default: str = "") -> str:
    text = _first_ui_value(row.get("risk_note"), row.get("stretch_caution_label"))
    normalized = _normalize_display_tag_text(text, DISPLAY_RISK_TEXT_ALIASES)
    return normalized or default


def _persistence_core_representatives_reason_label(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    labels: list[str] = []
    for token in [part.strip() for part in raw.split("|") if part.strip()]:
        if token.startswith("insufficient_candidates:"):
            count = token.split(":", 1)[1].strip()
            labels.append(f"候補{count}件")
        elif token.startswith("missing_columns:"):
            labels.append("必要列不足")
        elif token == "non_corporate_products_only":
            labels.append("ETF等を除外")
        elif token == "no_eligible_candidates":
            labels.append("適格候補なし")
        elif token == "blank_name_only":
            labels.append("銘柄名不足")
        elif token.startswith("missing_"):
            labels.append("材料不足")
        else:
            labels.append(token)
    return " / ".join(dict.fromkeys([label for label in labels if label]))


def _is_missing_display_value(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text == "" or text.lower() in {"none", "nan", "null", "<na>", "nat"}


def _normalize_display_text(value: Any, *, missing: str = "") -> str:
    if _is_missing_display_value(value):
        return missing
    return str(value).strip()


def _normalize_saved_representative_label(value: Any, formatter: Any) -> str:
    raw = _normalize_display_text(value, missing="")
    if not raw:
        return ""
    formatted = formatter(raw)
    if formatted and formatted != "未定義の判定":
        return formatted
    return raw


def _format_display_rank_value(value: Any) -> str:
    numeric = _coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(numeric):
        return ""
    return str(int(numeric))


def _format_display_pct_1dp(value: Any) -> str:
    numeric = _coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(numeric):
        return DISPLAY_UNAVAILABLE_MARK
    return f"{float(numeric):.1f}"


def _format_display_turnover_value(value: Any) -> str:
    numeric = _coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(numeric):
        return DISPLAY_UNAVAILABLE_MARK
    return f"{int(round(float(numeric))):,}"


def _format_display_price_value(value: Any, *, unavailable: bool = False) -> str:
    if unavailable:
        return DISPLAY_UNAVAILABLE_MARK
    numeric = _coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(numeric):
        return DISPLAY_UNAVAILABLE_MARK
    numeric_float = float(numeric)
    if math.isclose(numeric_float, round(numeric_float), abs_tol=1e-9):
        return f"{int(round(numeric_float)):,}"
    return f"{numeric_float:,.1f}"


def _format_display_value_or_unavailable(value: Any, *, unavailable: bool = False, formatter: Any | None = None) -> str:
    if unavailable:
        return DISPLAY_UNAVAILABLE_MARK
    numeric = _coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(numeric):
        return DISPLAY_UNAVAILABLE_MARK
    if callable(formatter):
        return str(formatter(numeric))
    return str(numeric)


def _format_display_announcement_date(value: Any) -> str:
    text = _normalize_iso_date_text(value)
    return text or DISPLAY_UNAVAILABLE_MARK


def _render_dataframe_or_reason(title: str, frame: pd.DataFrame, *, reason: str, link_columns: bool = False, note: str = "") -> None:
    st.subheader(title)
    if str(note or "").strip():
        st.caption(str(note).strip())
    if frame.empty:
        st.caption(reason)
        return
    kwargs: dict[str, Any] = {"width": "stretch", "hide_index": True}
    if link_columns:
        kwargs["column_config"] = {
            "日経で検索": st.column_config.LinkColumn("日経で検索", display_text="日経で検索"),
            "日経リンク": st.column_config.LinkColumn("日経リンク", display_text="日経リンク"),
            "材料リンク": st.column_config.LinkColumn("材料リンク", display_text="リンクを開く"),
        }
    st.dataframe(frame.rename(columns=UI_COLUMN_LABELS), **kwargs)


def _safe_link_url(value: Any) -> str:
    text = _normalize_display_text(value, missing="")
    if not text:
        return ""
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return text


def _candidate_table_cell_html(value: Any, *, column_label: str) -> str:
    text = _normalize_display_text(value, missing="")
    if column_label == "日経リンク":
        url = _safe_link_url(text)
        if not url:
            return ""
        return f'<a href="{html.escape(url, quote=True)}" target="_blank" rel="noopener noreferrer">日経リンク</a>'
    return html.escape(text)


def _render_candidate_table_or_reason(title: str, frame: pd.DataFrame, *, reason: str, note: str = "") -> None:
    st.subheader(title)
    if str(note or "").strip():
        st.caption(str(note).strip())
    if frame.empty:
        st.caption(reason)
        return
    display = frame.rename(columns=UI_COLUMN_LABELS)
    labels = [str(column) for column in display.columns]
    width_by_label = {
        "順位": "4.2rem",
        "コード": "5rem",
        "現在値": "6rem",
        "前日終値比(%)": "7rem",
        "決算発表予定日": "8rem",
        "日経リンク": "5.5rem",
        "エントリー判断": "13rem",
        "根拠": "38%",
    }
    colgroup = "".join(
        f'<col style="width:{html.escape(width_by_label.get(label, "auto"), quote=True)}">'
        for label in labels
    )
    header_html = "".join(f"<th>{html.escape(label)}</th>" for label in labels)
    body_rows: list[str] = []
    for _, row in display.iterrows():
        cells: list[str] = []
        for label in labels:
            css_class = "reason-cell" if label == "根拠" else ("entry-cell" if label == "エントリー判断" else "")
            class_attr = f' class="{css_class}"' if css_class else ""
            cells.append(f"<td{class_attr}>{_candidate_table_cell_html(row.get(label, ''), column_label=label)}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    table_html = f"""
<style>
.buy-candidate-table-wrap {{
  width: 100%;
  overflow-x: auto;
}}
.buy-candidate-table {{
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
  font-size: 0.92rem;
  line-height: 1.45;
}}
.buy-candidate-table th,
.buy-candidate-table td {{
  border-bottom: 1px solid rgba(49, 51, 63, 0.14);
  padding: 0.45rem 0.5rem;
  vertical-align: top;
  overflow-wrap: anywhere;
  word-break: break-word;
}}
.buy-candidate-table th {{
  font-weight: 600;
  background: rgba(49, 51, 63, 0.04);
}}
.buy-candidate-table .entry-cell {{
  white-space: pre-line;
}}
.buy-candidate-table .reason-cell {{
  white-space: normal;
}}
</style>
<div class="buy-candidate-table-wrap">
  <table class="buy-candidate-table">
    <colgroup>{colgroup}</colgroup>
    <thead><tr>{header_html}</tr></thead>
    <tbody>{''.join(body_rows)}</tbody>
  </table>
</div>
"""
    st.markdown(table_html, unsafe_allow_html=True)


def _build_earnings_candidate_table_note(base_meta: dict[str, Any] | None) -> str:
    return ""


def _clean_ui_value(value: Any) -> str:
    text = _normalize_display_text(value, missing="")
    return "" if text == DISPLAY_UNAVAILABLE_MARK else text


def _first_ui_value(*values: Any) -> str:
    for value in values:
        text = _clean_ui_value(value)
        if text:
            return text
    return ""


def _join_ui_fragments(*parts: Any) -> str:
    cleaned = [_clean_ui_value(part) for part in parts]
    return " / ".join([part for part in cleaned if part])


def _split_candidate_caution_terms(value: Any) -> list[str]:
    text = _clean_ui_value(value)
    if not text or text in {"通常候補", "特記なし", DISPLAY_UNAVAILABLE_MARK}:
        return []
    normalized = re.sub(r"[、,。／|]+", "/", text)
    terms: list[str] = []
    for part in [item.strip() for item in normalized.split("/") if item.strip()]:
        if part in {"通常候補", "特記なし", DISPLAY_UNAVAILABLE_MARK}:
            continue
        if "本日決算" in part or "決算当日" in part:
            terms.append("本日決算")
        elif "決算近い" in part or "決算直前" in part or "決算3営業日前" in part:
            terms.append("決算近い")
        elif "20日線乖離大" in part:
            terms.append("20日線乖離大")
        elif "追いかけ注意" in part or "追撃は慎重" in part:
            terms.append("追いかけ注意")
        elif "イベント注意" in part:
            terms.append("イベント注意")
        elif "補完" in part:
            terms.append("補完候補")
        elif "流動性注意" in part:
            terms.append("流動性注意")
        elif "決算通過後" in part:
            terms.append("決算通過後候補")
    return terms


def _candidate_entry_caution_terms(row: pd.Series) -> list[str]:
    terms: list[str] = []
    for value in [row.get("entry_caution"), row.get("candidate_bucket_label"), row.get("event_caution_reason")]:
        terms.extend(_split_candidate_caution_terms(value))
    fallback_value = row.get("fallback_used", False)
    fallback_used = False
    if not _is_missing_display_value(fallback_value):
        fallback_used = str(fallback_value).strip().lower() in {"true", "1", "yes"}
    if fallback_used:
        terms.append("補完候補")
    return list(dict.fromkeys([term for term in terms if term]))


def _format_candidate_entry_decision(row: pd.Series) -> str:
    base = _clean_ui_value(row.get("entry_stance_label")) or "監視"
    terms = _candidate_entry_caution_terms(row)
    if not terms:
        return base
    return f"{base}\n注意: {' / '.join(terms)}"


def _shorten_ui_text(text: str, *, limit: int = 84) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _format_ui_number(value: Any, *, digits: int = 1) -> str:
    numeric = _coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(numeric):
        return ""
    return f"{float(numeric):.{digits}f}"


def _split_ui_stock_names(value: Any) -> list[str]:
    if isinstance(value, list):
        names: list[str] = []
        for item in value:
            if isinstance(item, dict):
                name = _clean_ui_value(item.get("name")) or _clean_ui_value(item.get("code"))
            else:
                name = _clean_ui_value(item)
            if name:
                names.append(name)
        return names[:3]
    text = _clean_ui_value(value)
    if not text:
        return []
    return [name.strip() for name in text.split("/") if name.strip()][:3]


def _today_sector_summary_text(row: pd.Series) -> str:
    lead_names = _split_ui_stock_names(
        _first_ui_value(
            row.get("representative_stock"),
            row.get("core_representatives"),
            row.get("leaders_display"),
        )
    )
    caution = _first_ui_value(
        row.get("sector_caution"),
        row.get("quality_warn"),
        row.get("scan_sample_warning_reason"),
    )
    return _shorten_ui_text(
        _first_ui_value(
            row.get("sector_summary"),
            _join_ui_fragments(
                f"信頼度 {_clean_ui_value(row.get('sector_confidence'))}" if _clean_ui_value(row.get("sector_confidence")) else "",
                f"主力 {lead_names[0]}" if lead_names else "",
                f"価格 {_format_ui_number(row.get('price_block_score'))}" if _format_ui_number(row.get("price_block_score")) else "",
                f"資金 {_format_ui_number(row.get('flow_block_score'))}" if _format_ui_number(row.get("flow_block_score")) else "",
                f"注意 {caution}" if caution else "",
            ),
            "当日順位と既存スナップショットを確認",
        ),
        limit=92,
    )


def _extract_sector_rank_lookup(frame: pd.DataFrame, *, rank_col: str) -> dict[str, int]:
    if frame is None or frame.empty or "sector_name" not in frame.columns:
        return {}
    working = frame.copy()
    working["sector_name"] = working["sector_name"].apply(_clean_ui_value)
    working = working[working["sector_name"].ne("")]
    if working.empty:
        return {}
    working["_rank_sort"] = _coerce_numeric(working.get(rank_col, pd.Series(pd.NA, index=working.index))).fillna(9999)
    working = working.sort_values(["_rank_sort", "sector_name"], ascending=[True, True], kind="mergesort").drop_duplicates("sector_name")
    lookup: dict[str, int] = {}
    for index, row in working.iterrows():
        sector_name = str(row.get("sector_name", "") or "").strip()
        if not sector_name:
            continue
        rank_value = _coerce_numeric(pd.Series([row.get(rank_col)])).iloc[0]
        lookup[sector_name] = int(rank_value) if pd.notna(rank_value) else index + 1
    return lookup


def _build_sector_focus_view(frame: pd.DataFrame, *, timeframe: str, limit: int = 5) -> pd.DataFrame:
    if frame is None or frame.empty:
        if timeframe == "today":
            return pd.DataFrame(columns=["today_display_rank", "sector_name", "sector_summary", "industry_anchor_rank"])
        return pd.DataFrame(columns=["axis_rank", "sector_name", "sector_summary"])
    rank_col = "today_display_rank" if timeframe == "today" else "persistence_rank"
    working = frame.copy()
    working["_rank_sort"] = _coerce_numeric(working.get(rank_col, pd.Series(pd.NA, index=working.index))).fillna(9999)
    if "sector_name" not in working.columns:
        working["sector_name"] = ""
    working = working.sort_values(["_rank_sort", "sector_name"], ascending=[True, True], kind="mergesort").head(limit).copy()
    if timeframe == "today":
        working["today_display_rank"] = _coerce_numeric(working.get("today_display_rank", working.get("today_rank", pd.Series(pd.NA, index=working.index)))).round().astype("Int64")
        working["industry_anchor_rank"] = _coerce_numeric(
            working.get(
                "industry_anchor_rank",
                working.get("industry_up_anchor_rank", working.get("industry_rank_live", pd.Series(pd.NA, index=working.index))),
            )
        ).round().astype("Int64")
        display_rank = _coerce_numeric(working["today_display_rank"])
        anchor_rank = _coerce_numeric(working["industry_anchor_rank"])
        delta = _coerce_numeric(working.get("final_rank_delta", pd.Series(pd.NA, index=working.index)))
        working["final_rank_delta"] = delta.where(delta.notna(), display_rank - anchor_rank).round().astype("Int64")
    else:
        working["axis_rank"] = _coerce_numeric(working.get("persistence_rank", pd.Series(pd.NA, index=working.index))).round().astype("Int64")
    if timeframe == "today":
        working["sector_summary"] = working.apply(_today_sector_summary_text, axis=1)
    else:
        working["sector_summary"] = working.apply(
            lambda row: _shorten_ui_text(
                _first_ui_value(
                    row.get("sector_summary"),
                    _join_ui_fragments(
                        f"信頼度 {_clean_ui_value(row.get('sector_confidence'))}" if _clean_ui_value(row.get("sector_confidence")) else "",
                        f"TOPIX比RS {_format_ui_number(row.get('sector_rs_vs_topix'))}" if _format_ui_number(row.get("sector_rs_vs_topix")) else "",
                        f"注意 {_first_ui_value(row.get('quality_warn'), row.get('sector_caution'))}" if _first_ui_value(row.get("quality_warn"), row.get("sector_caution")) else "",
                    ),
                ),
                limit=92,
            ),
            axis=1,
        )
    if timeframe == "today":
        return working.drop(columns=["_rank_sort"], errors="ignore")[["today_display_rank", "sector_name", "sector_summary", "industry_anchor_rank"]].reset_index(drop=True)
    return working.drop(columns=["_rank_sort"], errors="ignore")[["axis_rank", "sector_name", "sector_summary"]].reset_index(drop=True)


def _build_center_stock_focus_view(
    sector_frame: pd.DataFrame,
    *,
    sector_rank_lookup: dict[str, int],
    timeframe: str,
    representative_frame: pd.DataFrame | None = None,
    earnings_announcement_lookup: dict[str, str] | None = None,
    security_reference_lookup: dict[str, Any] | None = None,
) -> pd.DataFrame:
    if timeframe == "today":
        if representative_frame is None or representative_frame.empty:
            return pd.DataFrame(columns=SECTOR_REPRESENTATIVES_FOCUS_COLUMNS)
        working = representative_frame.copy()
        for column in SECTOR_REPRESENTATIVES_FOCUS_COLUMNS:
            if column not in working.columns:
                working[column] = ""
        if "current_price" not in working.columns and "live_price" in working.columns:
            working["current_price"] = working["live_price"]
        if "live_turnover_value" not in working.columns and "live_turnover" in working.columns:
            working["live_turnover_value"] = working["live_turnover"]
        working["sector_name"] = working.get("sector_name", pd.Series("", index=working.index)).apply(_clean_ui_value)
        working = working[working["sector_name"].isin(sector_rank_lookup)]
        if working.empty:
            return pd.DataFrame(columns=SECTOR_REPRESENTATIVES_FOCUS_COLUMNS)
        working["_axis_rank_sort"] = working["sector_name"].map(lambda value: sector_rank_lookup.get(str(value), 9999))
        working["_rep_rank_sort"] = _coerce_numeric(working.get("representative_rank", pd.Series(pd.NA, index=working.index))).fillna(9999)
        working["today_rank"] = working["today_rank"].apply(_format_display_rank_value)
        working["representative_rank"] = working["representative_rank"].apply(_format_display_rank_value)
        working["code"] = working["code"].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
        working["name"] = working["name"].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
        working["live_ret_vs_prev_close"] = working["live_ret_vs_prev_close"].apply(_format_display_pct_1dp)
        working["current_price"] = working["current_price"].apply(_format_today_candidate_price)
        working["live_turnover_value"] = working["live_turnover_value"].apply(_format_display_turnover_value)
        working["representative_selected_reason"] = working.apply(_today_representative_reason_text, axis=1)
        working["earnings_announcement_date"] = _resolve_frame_earnings_announcement_dates(
            working,
            lookup=earnings_announcement_lookup,
        ).apply(_format_display_announcement_date)
        working["representative_quality_flag"] = working.apply(_today_representative_quality_text, axis=1)
        working["representative_fallback_reason"] = working["representative_fallback_reason"].apply(_representative_fallback_reason_label)
        working = working.sort_values(
            ["_axis_rank_sort", "_rep_rank_sort", "sector_name", "code"],
            ascending=[True, True, True, True],
            kind="mergesort",
        )
        return working.drop(columns=["_axis_rank_sort", "_rep_rank_sort", "nikkei_search"], errors="ignore").reindex(columns=SECTOR_REPRESENTATIVES_FOCUS_COLUMNS).reset_index(drop=True)
    columns = PERSISTENCE_REPRESENTATIVES_FOCUS_COLUMNS
    if sector_frame is None or sector_frame.empty:
        return pd.DataFrame(columns=columns)
    working = sector_frame.copy()
    working["sector_name"] = working.get("sector_name", pd.Series("", index=working.index)).apply(_clean_ui_value)
    working = working[working["sector_name"].isin(sector_rank_lookup)]
    if working.empty:
        return pd.DataFrame(columns=columns)
    working["_axis_rank_sort"] = working["sector_name"].map(lambda value: sector_rank_lookup.get(str(value), 9999))
    working = working.sort_values(["_axis_rank_sort", "sector_name"], ascending=[True, True], kind="mergesort")
    rows: list[dict[str, Any]] = []
    for _, row in working.iterrows():
        sector_name = _clean_ui_value(row.get("sector_name"))
        if not sector_name:
            continue
        representative_items: list[dict[str, Any]] = []
        raw_items = row.get("representative_stocks", [])
        if isinstance(raw_items, list):
            for item in raw_items[:3]:
                if not isinstance(item, dict):
                    continue
                representative_items.append(
                    {
                        "code": _normalize_security_code(item.get("code")),
                        "name": _clean_ui_value(item.get("name")),
                        "center_note": _pick_first_non_empty_label(item.get("center_note"), item.get("representative_reason")),
                        "earnings_announcement_date": _normalize_iso_date_text(item.get("earnings_announcement_date")),
                        "nikkei_search": str(item.get("nikkei_search", "") or "").strip(),
                    }
                )
        if not representative_items:
            names = _split_ui_stock_names(row.get("core_representatives")) or _split_ui_stock_names(row.get("representative_stock"))
            representative_items = [{"code": "", "name": name, "center_note": "", "earnings_announcement_date": "", "nikkei_search": ""} for name in names]
        if not representative_items:
            continue
        reason_label = _persistence_core_representatives_reason_label(row.get("core_representatives_reason"))
        for idx, item in enumerate(representative_items[:3], start=1):
            resolved_info = {}
            if not item.get("code"):
                resolved_info = _resolve_security_reference(
                    item.get("name"),
                    sector_name=sector_name,
                    security_reference_lookup=security_reference_lookup,
                )
            code = _normalize_security_code(item.get("code")) or str(resolved_info.get("code", "") or "")
            earnings_date = (
                _normalize_iso_date_text(item.get("earnings_announcement_date"))
                or (earnings_announcement_lookup or {}).get(code, "")
                or str(resolved_info.get("earnings_announcement_date", "") or "")
            )
            resolved_name = _normalize_display_text(
                item.get("name") or resolved_info.get("name"),
                missing=DISPLAY_UNAVAILABLE_MARK,
            )
            center_note = _pick_first_non_empty_label(
                item.get("center_note"),
                _build_persistence_representative_note(timeframe, idx, reason_label),
            )
            nikkei_search = str(item.get("nikkei_search", "") or "").strip() or _make_nikkei_search_link(
                "" if resolved_name == DISPLAY_UNAVAILABLE_MARK else resolved_name,
                code,
            )
            rows.append(
                {
                    "sector_name": sector_name,
                    "code": _normalize_display_text(code, missing=DISPLAY_UNAVAILABLE_MARK),
                    "name": resolved_name,
                    "center_note": _normalize_display_text(center_note, missing=DISPLAY_UNAVAILABLE_MARK),
                    "earnings_announcement_date": _format_display_announcement_date(earnings_date),
                    "nikkei_search": nikkei_search,
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _build_center_reference_map(
    center_overview_view: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    center_map: dict[str, dict[str, Any]] = {}
    if center_overview_view is not None and not center_overview_view.empty:
        working = center_overview_view.copy()
        working["sector_name"] = working.get("sector_name", pd.Series("", index=working.index)).apply(_clean_ui_value)
        working = working[working["sector_name"].ne("")]
        if not working.empty:
            if "code" not in working.columns:
                working["code"] = ""
            if "name" not in working.columns:
                working["name"] = ""
            if "representative_rank" in working.columns:
                working["_rep_rank_sort"] = _coerce_numeric(working.get("representative_rank", pd.Series(pd.NA, index=working.index))).fillna(9999)
                working = working.sort_values(["sector_name", "_rep_rank_sort", "code", "name"], ascending=[True, True, True, True], kind="mergesort")
            else:
                working = working.sort_values(["sector_name"], ascending=[True], kind="mergesort")
            for sector_name, group in working.groupby("sector_name", sort=False):
                names: list[str] = []
                codes: dict[str, str] = {}
                for index_within_group, (_, row) in enumerate(group.head(3).iterrows(), start=1):
                    code = _clean_ui_value(row.get("code"))
                    name = _clean_ui_value(row.get("name"))
                    rep_rank = _clean_ui_value(row.get("representative_rank")) or str(index_within_group)
                    if code and rep_rank:
                        codes[code] = rep_rank
                    if name:
                        names.append(name)
                center_map[sector_name] = {
                    "center_text": " / ".join(names[:3]),
                    "codes": codes,
                    "names": set(names),
                }
                normalized_key = _normalize_industry_key(sector_name)
                if normalized_key and normalized_key != sector_name:
                    center_map[normalized_key] = center_map[sector_name]
    return center_map


def _resolve_candidate_center_note(row: pd.Series, center_reference_map: dict[str, dict[str, Any]]) -> str:
    sector_name = _clean_ui_value(row.get("sector_name"))
    code = _clean_ui_value(row.get("code"))
    name = _clean_ui_value(row.get("name"))
    center_meta = center_reference_map.get(sector_name, center_reference_map.get(_normalize_industry_key(sector_name), {}))
    center_codes = center_meta.get("codes", {})
    center_names = center_meta.get("names", set())
    center_text = _clean_ui_value(center_meta.get("center_text"))
    if code and str(code) in center_codes:
        return f"代表銘柄({center_codes[str(code)]}位)"
    if name and name in center_names:
        return "代表候補3銘柄に含む"
    if center_text:
        return f"代表は {center_text.split('/')[0].strip()}"
    return "代表情報なし"


def _build_candidate_basis_text(
    row: pd.Series,
    center_reference_map: dict[str, dict[str, Any]],
    *,
    default_reason: str = "既存候補ロジック通過",
    default_caution: str = "特記なし",
) -> str:
    del default_caution
    center_note = _resolve_candidate_center_note(row, center_reference_map)
    reason = _candidate_reason_display_text(row, default=default_reason)
    return _join_ui_fragments(
        f"理由 {reason}" if reason else f"理由 {default_reason}",
        f"代表 {center_note}",
    )


def _format_today_candidate_price(value: Any) -> str:
    numeric = _coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(numeric):
        return DISPLAY_UNAVAILABLE_MARK
    if float(numeric).is_integer():
        return f"{int(numeric):,}"
    return f"{float(numeric):,.1f}"


def _format_today_candidate_ret(value: Any) -> str:
    numeric = _coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(numeric):
        return DISPLAY_UNAVAILABLE_MARK
    return f"{float(numeric):.1f}"


def _today_shortterm_candidate_type_label() -> str:
    return "短期注目"


def _today_shortterm_focus_label(value: Any, *, default: str = "監視") -> str:
    raw = str(value or "").strip()
    if not raw or raw == DISPLAY_UNAVAILABLE_MARK:
        return default
    if raw.startswith("短期注目・"):
        raw = raw[len("短期注目・") :].strip()
    elif raw == "短期注目":
        return default
    alias_map = {
        "today専用": "追撃候補",
        "today 専用": "追撃候補",
        "today_only": "追撃候補",
        "today候補": "追撃候補",
        "today監視": "監視",
        "補完・監視": "補完・監視",
        "補完監視": "補完・監視",
    }
    normalized = alias_map.get(raw, raw)
    if not normalized:
        return default
    return normalized


def _today_representative_reason_text(row: pd.Series) -> str:
    selected_reason_raw = str(row.get("representative_selected_reason", "") or "").strip()
    selected_reason = TODAY_REPRESENTATIVE_SELECTED_REASON_ALIASES.get(selected_reason_raw, "")
    reasons = [part.strip() for part in selected_reason.split("/") if part.strip()]
    live_turnover = _coerce_numeric(pd.Series([row.get("live_turnover_value", row.get("live_turnover", pd.NA))])).iloc[0]
    rep_centrality = _coerce_numeric(pd.Series([row.get("rep_score_centrality", pd.NA)])).iloc[0]
    turnover_share = _coerce_numeric(pd.Series([row.get("stock_turnover_share_of_sector", pd.NA)])).iloc[0]
    if pd.notna(live_turnover) and float(live_turnover) >= 1_000_000_000.0:
        reasons.append("出来高あり")
    if (
        (pd.notna(rep_centrality) and float(rep_centrality) >= 4.0)
        or (pd.notna(turnover_share) and float(turnover_share) >= 0.25)
        or bool(row.get("was_in_selected50"))
        or bool(row.get("was_in_must_have"))
    ):
        reasons.append("代表性あり")
    if not reasons:
        reasons.append("代表候補")
    return " / ".join(dict.fromkeys(reasons))


def _today_representative_quality_text(row: pd.Series) -> str:
    quality_raw = str(row.get("representative_quality_flag", "") or "").strip()
    if quality_raw in {"quality_pass", "品質基準を満たす"}:
        return "品質基準を満たす"
    if quality_raw in {"quality_fail", "excluded", "品質基準未達"}:
        return "品質基準未達"
    reasons: list[str] = []
    rep_rank = _coerce_numeric(pd.Series([row.get("representative_rank", pd.NA)])).iloc[0]
    leadership = _coerce_numeric(pd.Series([row.get("rep_score_today_leadership", pd.NA)])).iloc[0]
    centrality = _coerce_numeric(pd.Series([row.get("rep_score_centrality", pd.NA)])).iloc[0]
    sanity = _coerce_numeric(pd.Series([row.get("rep_score_sanity", pd.NA)])).iloc[0]
    live_turnover = _coerce_numeric(pd.Series([row.get("live_turnover_value", row.get("live_turnover", pd.NA))])).iloc[0]
    fallback_reason = str(row.get("representative_fallback_reason", "") or "").strip()
    if pd.notna(rep_rank) and int(rep_rank) >= 2:
        reasons.append("セクター内順位低め")
    if pd.notna(leadership) and float(leadership) < 4.0:
        reasons.append("当日強さ弱め")
    if pd.notna(centrality) and float(centrality) < 4.0:
        reasons.append("代表性弱め")
    if pd.notna(live_turnover):
        if float(live_turnover) < 500_000_000.0:
            reasons.append("流動性弱め")
        elif float(live_turnover) < 1_000_000_000.0:
            reasons.append("出来高弱め")
    if pd.notna(sanity) and float(sanity) < 0.10:
        reasons.append("判定材料不足")
    if fallback_reason in {"fallback_no_clear_leader", "no_quality_candidate_met_center_leader_gate", "no_positive_candidate_in_selected50"}:
        reasons.append("当日強さ弱め")
    if fallback_reason in {"fallback_insufficient_candidates", "filled_remaining_support_slots_with_best_available_nonblocked_candidates"}:
        reasons.append("候補不足")
    if not reasons:
        fallback_text = _representative_fallback_reason_label(fallback_reason) or fallback_reason
        reasons.append(fallback_text or "判定材料不足")
    return f"品質要注意: {' / '.join(dict.fromkeys(reasons))}"


def _build_persistence_representative_note(timeframe: str, representative_rank: int, reason_label: str = "") -> str:
    if timeframe == "1w":
        fragments = [
            "1wで市場平均より強い",
            "セクター内上位" if representative_rank == 1 else "セクター内の代表候補",
            "短期資金が続く",
        ]
    elif timeframe == "1m":
        fragments = [
            "1か月の上昇が続く",
            "中期資金が入っている",
            "直近も大きく崩れていない",
        ]
    else:
        fragments = [
            "3か月でセクターを主導",
            "1か月でも大きく崩れていない",
            "中長期資金の中心",
        ]
    if reason_label and reason_label not in {"ETF等を除外"}:
        fragments.append(reason_label)
    return _shorten_ui_text(" / ".join(dict.fromkeys([fragment for fragment in fragments if fragment])), limit=72)


def _build_candidate_focus_view(
    frame: pd.DataFrame,
    *,
    rank_col: str,
    sector_rank_lookup: dict[str, int],
    center_reference_map: dict[str, dict[str, Any]],
    scope_label: str,
    limit: int = 5,
    restrict_to_sector_scope: bool = False,
    fallback_frame: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str, str]:
    columns = [
        "candidate_rank",
        "sector_name",
        "code",
        "name",
        "entry_stance_label",
        "current_price",
        "live_ret_vs_prev_close",
        "candidate_basis",
        "earnings_announcement_date",
        "nikkei_search",
    ]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns), "", ""
    working = frame.copy()
    working["sector_name"] = working["sector_name"].apply(_clean_ui_value)
    normalized_sector_lookup = {_normalize_industry_key(key): value for key, value in sector_rank_lookup.items() if _normalize_industry_key(key)}
    working["_sector_lookup_key"] = working["sector_name"].map(_normalize_industry_key)
    candidate_note = ""
    if restrict_to_sector_scope:
        filtered = working[working["_sector_lookup_key"].isin(normalized_sector_lookup)].copy()
        if filtered.empty and isinstance(fallback_frame, pd.DataFrame) and not fallback_frame.empty:
            working = fallback_frame.copy()
            working["sector_name"] = working["sector_name"].apply(_clean_ui_value)
            working["_sector_lookup_key"] = working["sector_name"].map(_normalize_industry_key)
            candidate_note = "today強セクター内に該当がないため、1w候補全体から表示しています"
        else:
            working = filtered
    if working.empty:
        return pd.DataFrame(columns=columns), f"{scope_label}に該当する既存候補はありません。", candidate_note
    working["_candidate_rank_sort"] = _coerce_numeric(working.get(rank_col, pd.Series(pd.NA, index=working.index))).fillna(9999)
    if restrict_to_sector_scope and not candidate_note:
        working["_axis_rank_sort"] = working["_sector_lookup_key"].map(lambda value: normalized_sector_lookup.get(str(value), 9999))
        working = working.sort_values(
            ["_axis_rank_sort", "_candidate_rank_sort", "sector_name", "code"],
            ascending=[True, True, True, True],
            kind="mergesort",
        ).head(limit)
    else:
        working = working.sort_values(
            ["_candidate_rank_sort", "sector_name", "code"],
            ascending=[True, True, True],
            kind="mergesort",
        ).head(limit)
    working["candidate_rank"] = working[rank_col]
    if restrict_to_sector_scope:
        working["entry_stance_label"] = working.get("entry_stance_label", pd.Series("", index=working.index)).apply(_today_shortterm_focus_label)

    def _build_candidate_basis(row: pd.Series) -> str:
        center_note = _resolve_candidate_center_note(row, center_reference_map)
        reason = _candidate_reason_display_text(row, default="既存候補ロジック通過")
        return _join_ui_fragments(
            f"理由 {reason}" if reason else "理由 既存候補ロジック通過",
            f"代表 {center_note}",
        )

    working["candidate_basis"] = working.apply(_build_candidate_basis, axis=1)
    working["entry_stance_label"] = working.apply(_format_candidate_entry_decision, axis=1)
    working["earnings_announcement_date"] = working.get("earnings_announcement_date", pd.Series("", index=working.index)).apply(_format_display_announcement_date)
    return working.drop(columns=["_sector_lookup_key"], errors="ignore")[columns].reset_index(drop=True), "", candidate_note


def _build_today_purchase_candidate_view(
    representative_frame: pd.DataFrame,
    *,
    sector_rank_lookup: dict[str, int],
    center_reference_map: dict[str, dict[str, Any]],
    fallback_frame: pd.DataFrame | None = None,
    limit: int = 3,
    earnings_announcement_lookup: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, str, str]:
    columns = [
        "candidate_rank",
        "sector_name",
        "code",
        "name",
        "entry_stance_label",
        "current_price",
        "live_ret_vs_prev_close",
        "candidate_basis",
        "earnings_announcement_date",
        "nikkei_search",
    ]
    normalized_sector_lookup = {_normalize_industry_key(key): value for key, value in sector_rank_lookup.items() if _normalize_industry_key(key)}
    dedicated = pd.DataFrame(columns=columns)
    if isinstance(representative_frame, pd.DataFrame) and not representative_frame.empty and normalized_sector_lookup:
        working = representative_frame.copy()
        for column in [
            "sector_name",
            "code",
            "name",
            "representative_selected_reason",
            "representative_quality_flag",
            "representative_fallback_reason",
            "nikkei_search",
        ]:
            if column not in working.columns:
                working[column] = ""
        working["sector_name"] = working["sector_name"].apply(_clean_ui_value)
        working["code"] = working["code"].apply(_clean_ui_value)
        working["name"] = working["name"].apply(_clean_ui_value)
        working["_sector_lookup_key"] = working["sector_name"].map(_normalize_industry_key)
        working = working[
            working["_sector_lookup_key"].isin(normalized_sector_lookup)
            & working["sector_name"].ne("")
            & working["code"].ne("")
            & working["name"].ne("")
        ].copy()
        if not working.empty:
            working["_sector_rank_sort"] = working["_sector_lookup_key"].map(lambda value: normalized_sector_lookup.get(str(value), 9999))
            working["_rep_rank_sort"] = _coerce_numeric(working.get("representative_rank", pd.Series(pd.NA, index=working.index))).fillna(9999)
            working["_ret_numeric"] = _coerce_numeric(working.get("live_ret_vs_prev_close", pd.Series(pd.NA, index=working.index)))
            working["_turnover_numeric"] = _coerce_numeric(working.get("live_turnover_value", pd.Series(pd.NA, index=working.index)))
            working["_lead_numeric"] = _coerce_numeric(working.get("rep_score_today_leadership", pd.Series(pd.NA, index=working.index))).fillna(0.0)
            working["_centrality_numeric"] = _coerce_numeric(working.get("rep_score_centrality", pd.Series(pd.NA, index=working.index))).fillna(0.0)
            working["_price_numeric"] = _coerce_numeric(working.get("current_price", pd.Series(pd.NA, index=working.index)))
            quality_flag = working["representative_quality_flag"].fillna("").astype(str)
            fallback_reason = working["representative_fallback_reason"].fillna("").astype(str)
            working["_quality_warn"] = quality_flag.eq("quality_warn") | quality_flag.eq("品質要注意")
            working["_quality_blocked"] = quality_flag.isin(["quality_fail", "excluded", "品質基準未達"])
            preferred_mask = (
                working["_ret_numeric"].ge(0.0)
                & working["_turnover_numeric"].ge(100_000_000.0)
                & working["_price_numeric"].notna()
                & ~working["_quality_blocked"]
                & (~working["_quality_warn"] | working["_ret_numeric"].ge(2.0))
            )
            reserve_mask = (
                working["_ret_numeric"].ge(-1.5)
                & working["_turnover_numeric"].ge(50_000_000.0)
                & working["_price_numeric"].notna()
                & ~working["_quality_blocked"]
                & ~(working["_quality_warn"] & working["_ret_numeric"].lt(0.5))
            )
            working["_stage_sort"] = 9
            working.loc[reserve_mask, "_stage_sort"] = 1
            working.loc[preferred_mask, "_stage_sort"] = 0
            working["_quality_sort"] = working["_quality_warn"].astype(int)
            working = working[working["_stage_sort"].lt(9)].copy()
            if not working.empty:
                working["selection_reason"] = working.apply(_today_representative_reason_text, axis=1)
                working["stretch_caution_label"] = working.apply(
                    lambda row: ""
                    if str(row.get("representative_quality_flag", "") or "") in {"quality_pass", "品質基準を満たす"}
                    else _today_representative_quality_text(row),
                    axis=1,
                )
                working["risk_note"] = working.apply(
                    lambda row: _join_ui_fragments(
                        ""
                        if str(row.get("representative_quality_flag", "") or "") in {"quality_pass", "品質基準を満たす"}
                        else _today_representative_quality_text(row),
                        "当日戻り待ち" if _coerce_numeric(pd.Series([row.get("_ret_numeric")])).iloc[0] < 0 else "",
                    ),
                    axis=1,
                )
                working["candidate_commentary"] = working["selection_reason"]
                working = working.sort_values(
                    ["_stage_sort", "_rep_rank_sort", "_sector_rank_sort", "_quality_sort", "_ret_numeric", "_lead_numeric", "_centrality_numeric", "_turnover_numeric", "code"],
                    ascending=[True, True, True, True, False, False, False, False, True],
                    kind="mergesort",
                ).drop_duplicates("code")
                dedicated = working.head(limit).copy()
                if not dedicated.empty:
                    dedicated["candidate_rank"] = range(1, len(dedicated) + 1)
                    dedicated["entry_stance_label"] = dedicated["_stage_sort"].map(
                        lambda value: _today_shortterm_focus_label("追撃候補" if int(value) == 0 else "監視")
                    )
                    dedicated["current_price"] = dedicated["current_price"].apply(_format_today_candidate_price)
                    dedicated["live_ret_vs_prev_close"] = dedicated["live_ret_vs_prev_close"].apply(_format_today_candidate_ret)
                    dedicated["candidate_basis"] = dedicated.apply(
                        lambda row: _build_candidate_basis_text(row, center_reference_map, default_reason="当日強セクターの代表株"),
                        axis=1,
                    )
                    dedicated["earnings_announcement_date"] = _resolve_frame_earnings_announcement_dates(
                        dedicated,
                        lookup=earnings_announcement_lookup,
                    ).apply(_format_display_announcement_date)
                    dedicated["nikkei_search"] = dedicated["nikkei_search"].fillna("").astype(str)
                    dedicated = dedicated.reindex(columns=columns)
    final_frame = dedicated.copy()
    candidate_note = ""
    if len(final_frame) < limit and isinstance(fallback_frame, pd.DataFrame) and not fallback_frame.empty:
        shortage = limit - len(final_frame)
        fallback = fallback_frame.copy()
        fallback["sector_name"] = fallback.get("sector_name", pd.Series("", index=fallback.index)).apply(_clean_ui_value)
        fallback["code"] = fallback.get("code", pd.Series("", index=fallback.index)).apply(_clean_ui_value)
        fallback["_sector_lookup_key"] = fallback["sector_name"].map(_normalize_industry_key)
        existing_codes = set(final_frame.get("code", pd.Series(dtype=str)).astype(str).tolist())
        fallback = fallback[~fallback["code"].isin(existing_codes)].copy()
        if not fallback.empty:
            fallback["_candidate_rank_sort"] = _coerce_numeric(fallback.get("candidate_rank_1w", pd.Series(pd.NA, index=fallback.index))).fillna(9999)
            fallback = fallback.sort_values(["_candidate_rank_sort", "sector_name", "code"], ascending=[True, True, True], kind="mergesort").head(shortage).copy()
            fallback["candidate_rank"] = range(len(final_frame) + 1, len(final_frame) + len(fallback) + 1)
            fallback["entry_stance_label"] = fallback.get("entry_stance_label", pd.Series("", index=fallback.index)).apply(_today_shortterm_focus_label)
            fallback["candidate_basis"] = fallback.apply(lambda row: _build_candidate_basis_text(row, center_reference_map), axis=1)
            fallback["earnings_announcement_date"] = _resolve_frame_earnings_announcement_dates(
                fallback,
                lookup=earnings_announcement_lookup,
            ).apply(_format_display_announcement_date)
            fallback["nikkei_search"] = fallback.get("nikkei_search", pd.Series("", index=fallback.index)).fillna("").astype(str)
            fallback = fallback.reindex(columns=columns)
            final_frame = pd.concat([final_frame, fallback], ignore_index=True)
            candidate_note = "短期注目銘柄が不足したため、不足分を1w候補から補完しています"
    if final_frame.empty:
        return pd.DataFrame(columns=columns), "today 短期注目銘柄を抽出できませんでした。", candidate_note
    final_frame = final_frame.head(limit).reset_index(drop=True)
    final_frame["entry_stance_label"] = final_frame.apply(_format_candidate_entry_decision, axis=1)
    return final_frame, "", candidate_note


def _render_timeframe_panel(
    *,
    timeframe_label: str,
    timeframe_note: str,
    sector_title: str,
    sector_frame: pd.DataFrame,
    sector_reason: str,
    center_frame: pd.DataFrame,
    center_reason: str,
    candidate_frame: pd.DataFrame,
    candidate_reason: str,
    candidate_note: str = "",
    candidate_title: str = "購入候補",
) -> None:
    st.caption(f"{timeframe_label}: {timeframe_note}")
    _render_dataframe_or_reason(sector_title, sector_frame, reason=sector_reason)
    _render_dataframe_or_reason("セクター代表銘柄", center_frame, reason=center_reason)
    _render_candidate_table_or_reason(candidate_title, candidate_frame, reason=candidate_reason, note=candidate_note)


def _persistence_gate_fail_warn_label(reason: Any) -> str:
    raw = str(reason or "").strip()
    if not raw:
        return ""
    tags: list[str] = []
    if "rs_non_positive" in raw:
        tags.append("TOPIXを下回る")
    if "constituents_lt_6" in raw:
        tags.append("構成少")
    if "positive_ratio_lt_0.55" in raw:
        tags.append("広がり弱い")
    if "leader_concentration_gt_0.40" in raw:
        tags.append("偏重高い")
    if "1m_confirmation_broken" in raw:
        tags.append("1か月失速")
    if raw.startswith("missing:"):
        tags.append("判定列不足")
    return " / ".join(dict.fromkeys([tag for tag in tags if tag]))


TODAY_SECTOR_DISPLAY_COLUMNS = [
    "today_display_rank",
    "industry_anchor_rank",
    "final_rank_delta",
    "sector_name",
    "sector_summary",
]

SECTOR_REPRESENTATIVES_FOCUS_COLUMNS = [
    "today_rank",
    "sector_name",
    "code",
    "name",
    "live_ret_vs_prev_close",
    "current_price",
    "live_turnover_value",
    "representative_selected_reason",
    "earnings_announcement_date",
    "representative_quality_flag",
    "representative_fallback_reason",
]

SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS = [
    "today_rank",
    "sector_name",
    "code",
    "name",
    "live_ret_vs_prev_close",
    "current_price",
    "live_turnover_value",
    "representative_selected_reason",
    "earnings_announcement_date",
    "representative_quality_flag",
    "representative_fallback_reason",
    "nikkei_search",
]

PERSISTENCE_REPRESENTATIVES_FOCUS_COLUMNS = [
    "sector_name",
    "code",
    "name",
    "center_note",
    "earnings_announcement_date",
    "nikkei_search",
]

SECTOR_REPRESENTATIVES_AUDIT_COLUMNS = [
    "today_rank",
    "sector_name",
    "rep_candidate_pool_count",
    "rep_with_live_ret_count",
    "rep_with_current_price_count",
    "rep_with_live_turnover_count",
    "rep_pass_relative_leadership_count",
    "rep_pass_centrality_count",
    "rep_pass_sanity_count",
    "rep_selected_count",
    "deep_watch_selected_count",
    "deep_watch_observed_live_count",
    "rep_candidate_pool_status",
    "rep_candidate_pool_reason",
    "code",
    "name",
    "candidate_in_universe",
    "live_ret_vs_prev_close_raw",
    "current_price_raw",
    "live_turnover_raw",
    "sector_turnover_share_raw",
    "selected_horizon",
    "selected_universe",
    "primary_candidate_count",
    "supplemental_candidate_count",
    "final_candidate_count",
    "sector_constituent_count",
    "representative_pool_coverage_rate",
    "candidate_pool_warning",
    "candidate_pool_reason",
    "selected_from_primary_or_supplemental",
    "sector_candidate_count",
    "sector_positive_count",
    "sector_negative_count",
    "sector_positive_rate",
    "sector_median_return",
    "sector_top_quartile_return",
    "sector_bottom_quartile_return",
    "stock_return_percentile_in_sector",
    "stock_return_rank_in_sector",
    "market_positive_rate",
    "market_context",
    "sector_context",
    "sector_live_ret_median",
    "sector_top_positive_count",
    "representative_gate_pass",
    "representative_gate_reason",
    "hard_reject_reason",
    "hard_block_reason",
    "fallback_used",
    "fallback_reason",
    "fallback_blocked_reason",
    "live_ret_from_open",
    "sector_live_ret_pct",
    "sector_today_flow_pct",
    "sector_turnover_share",
    "exclude_spike",
    "exclude_spike_hard_reject",
    "exclude_spike_warning_only",
    "spike_quality",
    "poor_quality_spike",
    "material_supported_breakout",
    "breakout_support_reason",
    "centrality_score",
    "liquidity_score",
    "today_leadership_score",
    "representative_final_score",
    "selected_reason",
    "rep_score_today_strength",
    "rep_score_relative_strength",
    "rep_score_liquidity",
    "rep_score_total_raw",
    "rep_score_centrality_raw",
    "rep_score_today_leadership_raw",
    "rep_score_sanity_raw",
    "relative_leadership_pass",
    "centrality_pass",
    "sanity_pass",
    "selected_for_representative",
    "selected_reason_raw",
    "quality_flag_raw",
    "fallback_reason_raw",
    "was_in_selected50",
    "was_in_must_have",
]

SECTOR_LIVE_AGGREGATE_AUDIT_COLUMNS = [
    "today_rank",
    "sector_name",
    "normalized_sector_name",
    "live_aggregate_observed_count",
    "live_aggregate_ret_count",
    "live_aggregate_turnover_count",
    "live_aggregate_turnover_ratio_count",
    "live_turnover_total_raw",
    "leader_live_turnover_raw",
    "median_live_ret_raw",
    "turnover_ratio_median_raw",
    "live_aggregate_status",
    "live_aggregate_reason",
    "sample_codes",
]

SWING_CANDIDATE_AUDIT_COLUMNS = [
    "code",
    "name",
    "sector_name",
    "in_candidate_universe",
    "sector_tailwind_band",
    "pass_live_gate",
    "pass_trend_gate",
    "pass_flow_gate",
    "pass_quality_gate",
    "pass_score_gate",
    "hard_block_reason_raw",
    "entry_stance_raw",
    "entry_stance_label",
    "stretch_penalty_applied",
    "display_sector_cap_pruned",
    "override_selected_outside_top_sector",
    "empty_reason_label",
    "score_total_raw",
    "score_subcomponents_raw",
    "selected_horizon",
    "buy_score_total",
    "buy_score_1w",
    "buy_score_1m",
    "buy_score_3m",
    "buy_strength_score",
    "buy_strength_score_1w",
    "buy_strength_score_1m",
    "buy_strength_score_3m",
    "entry_timing_adjustment",
    "entry_timing_adjustment_1w",
    "entry_timing_adjustment_1m",
    "entry_timing_adjustment_3m",
    "score_components",
    "sector_strength_score",
    "relative_strength_score",
    "liquidity_score",
    "earnings_risk_score",
    "overheating_penalty",
    "abnormal_event_penalty",
    "fallback_penalty",
    "selected_reason",
    "rejected_reason",
    "horizon_fit_reason",
    "entry_caution",
    "event_candidate_flag",
    "event_candidate_type",
    "candidate_bucket",
    "candidate_bucket_label",
    "event_caution_reason",
    "fallback_used",
    "selected_flag",
    "unselected_reason",
    "display_reason_raw",
]

PERSISTENCE_DISPLAY_COLUMNS = [
    "persistence_rank",
    "sector_name",
    "sector_rs_vs_topix",
    "core_representatives",
    "sector_confidence",
    "quality_warn",
    "sector_caution",
]
SWING_1W_DISPLAY_COLUMNS = [
    "candidate_rank_1w",
    "sector_name",
    "code",
    "name",
    "live_ret_vs_prev_close",
    "current_price",
    "live_turnover_value",
    "candidate_quality",
    "entry_stance_label",
    "stretch_caution_label",
    "watch_reason_label",
    "selection_reason",
    "horizon_fit_reason",
    "entry_caution",
    "candidate_bucket_label",
    "event_caution_reason",
    "earnings_announcement_date",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_1w",
    "earnings_buffer_days",
    "nikkei_search",
]
SWING_1M_DISPLAY_COLUMNS = [
    "candidate_rank_1m",
    "sector_name",
    "code",
    "name",
    "live_ret_vs_prev_close",
    "current_price",
    "live_turnover_value",
    "candidate_quality",
    "entry_stance_label",
    "stretch_caution_label",
    "watch_reason_label",
    "selection_reason",
    "horizon_fit_reason",
    "entry_caution",
    "candidate_bucket_label",
    "event_caution_reason",
    "earnings_announcement_date",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_1m",
    "rs_vs_topix_3m",
    "price_vs_ma20_pct",
    "earnings_buffer_days",
    "finance_health_flag",
    "nikkei_search",
]
SWING_3M_DISPLAY_COLUMNS = [
    "candidate_rank_3m",
    "sector_name",
    "code",
    "name",
    "live_ret_vs_prev_close",
    "current_price",
    "live_turnover_value",
    "candidate_quality",
    "entry_stance_label",
    "stretch_caution_label",
    "watch_reason_label",
    "selection_reason",
    "horizon_fit_reason",
    "entry_caution",
    "candidate_bucket_label",
    "event_caution_reason",
    "earnings_announcement_date",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_3m",
    "ret_3m",
    "avg_turnover_20d",
    "earnings_buffer_days",
    "nikkei_search",
]


def _sort_sector_representatives_display_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS)
    working = frame.copy()
    working["_today_rank_sort"] = _coerce_numeric(working.get("today_rank", pd.Series(pd.NA, index=working.index))).fillna(9999)
    working["_representative_rank_sort"] = _coerce_numeric(working.get("representative_rank", pd.Series(pd.NA, index=working.index))).fillna(9999)
    if "sector_name" not in working.columns:
        working["sector_name"] = ""
    if "code" not in working.columns:
        working["code"] = ""
    working = working.sort_values(
        ["_today_rank_sort", "_representative_rank_sort", "sector_name", "code"],
        ascending=[True, True, True, True],
        kind="mergesort",
    ).copy()
    return working.drop(columns=["_today_rank_sort", "_representative_rank_sort"], errors="ignore")


def _build_empty_representative_display_rows(today_sector_leaderboard: pd.DataFrame, present_sector_names: set[str]) -> pd.DataFrame:
    if today_sector_leaderboard is None or today_sector_leaderboard.empty:
        return pd.DataFrame(columns=SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS)
    missing = today_sector_leaderboard.copy()
    missing["sector_name"] = missing.get("sector_name", pd.Series(dtype=str)).astype(str)
    missing = missing[~missing["sector_name"].isin(present_sector_names)].copy()
    rep_selected_mask = _coerce_numeric(
        missing.get("rep_selected_count", pd.Series([pd.NA] * len(missing), index=missing.index))
    ).fillna(0.0).le(0.0)
    missing = missing.loc[rep_selected_mask].copy()
    if missing.empty:
        return pd.DataFrame(columns=SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS)
    rows: list[dict[str, Any]] = []
    for _, row in missing.iterrows():
        reason = _normalize_display_text(row.get("rep_candidate_pool_reason"), missing="候補母集団なし")
        rows.append(
            {
                "today_rank": _format_display_rank_value(row.get("today_rank")),
                "sector_name": _normalize_display_text(row.get("sector_name"), missing=DISPLAY_UNAVAILABLE_MARK),
                "code": "",
                "name": "代表なし（当日中心株不在）",
                "live_ret_vs_prev_close": DISPLAY_UNAVAILABLE_MARK,
                "current_price": DISPLAY_UNAVAILABLE_MARK,
                "live_turnover_value": DISPLAY_UNAVAILABLE_MARK,
                "representative_selected_reason": "当日中心株不在",
                "earnings_announcement_date": DISPLAY_UNAVAILABLE_MARK,
                "representative_quality_flag": "代表なし",
                "representative_fallback_reason": reason,
                "nikkei_search": "",
            }
        )
    return pd.DataFrame(rows, columns=SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS)


def _resolve_rep_candidate_pool_status(
    *,
    wide_scan_member_count: Any,
    deep_watch_selected_count: Any,
    deep_watch_observed_live_count: Any,
    rep_candidate_pool_count: Any,
) -> tuple[str, str]:
    pool_count = int(_coerce_numeric(pd.Series([rep_candidate_pool_count])).fillna(0.0).iloc[0] or 0)
    selected_count = int(_coerce_numeric(pd.Series([deep_watch_selected_count])).fillna(0.0).iloc[0] or 0)
    live_count = int(_coerce_numeric(pd.Series([deep_watch_observed_live_count])).fillna(0.0).iloc[0] or 0)
    scan_count = int(_coerce_numeric(pd.Series([wide_scan_member_count])).fillna(0.0).iloc[0] or 0)
    if pool_count > 0:
        return "observed", ""
    if selected_count <= 0:
        if scan_count > 0:
            return "outside_deep_watch", "候補母集団なし（監視ユニバース外）"
        return "no_scan_candidates", "候補母集団なし"
    if live_count <= 0:
        return "missing_live_data", "候補母集団なし（板データ未取得）"
    return "empty_after_filtering", "候補母集団なし"


def _prepare_table_view(df: pd.DataFrame, columns: list[str]) -> tuple[pd.DataFrame, list[str]]:
    compatibility_notes: list[str] = []
    if df is None or df.empty:
        return pd.DataFrame(columns=columns), compatibility_notes
    prepared = df.copy()
    string_columns = {
        "sector_name",
        "breadth",
        "leaders",
        "representative_stock",
        "core_representatives",
        "core_representatives_reason",
        "code",
        "name",
        "candidate_quality",
        "entry_fit",
        "entry_stance_label",
        "stretch_caution_label",
        "watch_reason_label",
        "selection_reason",
        "risk_note",
        "candidate_commentary",
        "finance_health_flag",
        "sector_confidence",
        "sector_caution",
        "quality_warn",
        "quality_fail_reason",
        "representative_selected_reason",
        "representative_quality_flag",
        "representative_fallback_reason",
        "earnings_announcement_date",
        "nikkei_search",
    }
    if "core_representatives" in columns and "core_representatives" not in prepared.columns and "representative_stock" in prepared.columns:
        prepared["core_representatives"] = prepared["representative_stock"]
        compatibility_notes.append("core_representatives")
    for column in columns:
        if column not in prepared.columns:
            prepared[column] = "" if column in string_columns else pd.NA
            compatibility_notes.append(column)
    for column in columns:
        if column in string_columns:
            prepared[column] = prepared[column].fillna("").astype(str)
        else:
            prepared[column] = _coerce_numeric(prepared[column])
    return prepared.reindex(columns=columns), compatibility_notes


def _prepare_persistence_sector_view(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    if df is None or df.empty:
        return pd.DataFrame(columns=PERSISTENCE_DISPLAY_COLUMNS), []
    prepared = df.copy()
    gate_warn = prepared.get("sector_gate_fail_reason", pd.Series("", index=prepared.index)).apply(_persistence_gate_fail_warn_label)
    if "quality_warn" not in prepared.columns:
        prepared["quality_warn"] = ""
    prepared["quality_warn"] = prepared["quality_warn"].fillna("").astype(str)
    empty_quality_mask = prepared["quality_warn"].eq("") & gate_warn.ne("")
    prepared.loc[empty_quality_mask, "quality_warn"] = gate_warn[empty_quality_mask]
    if "sector_caution" not in prepared.columns:
        prepared["sector_caution"] = ""
    prepared["sector_caution"] = prepared["sector_caution"].fillna("").astype(str)
    empty_caution_mask = prepared["sector_caution"].eq("") & gate_warn.ne("")
    prepared.loc[empty_caution_mask, "sector_caution"] = gate_warn[empty_caution_mask]
    prepared["_gate_priority"] = prepared.get("sector_gate_pass", pd.Series(False, index=prepared.index)).fillna(False).astype(bool).astype(int) * -1
    prepared["_rank_sort"] = _coerce_numeric(prepared.get("persistence_rank", pd.Series(pd.NA, index=prepared.index))).fillna(9999)
    prepared["_sector_name_sort"] = prepared.get("sector_name", pd.Series("", index=prepared.index)).fillna("").astype(str)
    prepared = prepared.sort_values(
        ["_gate_priority", "_rank_sort", "_sector_name_sort"],
        ascending=[True, True, True],
        kind="mergesort",
    ).drop(columns=["_gate_priority", "_rank_sort", "_sector_name_sort"], errors="ignore")
    return _prepare_table_view(prepared, PERSISTENCE_DISPLAY_COLUMNS)


def _summarize_representative_value(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        names: list[str] = []
        for item in value:
            if isinstance(item, dict):
                name = str(item.get("name", "") or item.get("code", "")).strip()
            else:
                name = str(item).strip()
            if name:
                names.append(name)
        return " / ".join(names[:3])
    return ""


def _stringify_snapshot_cell(value: Any) -> str:
    if isinstance(value, list):
        return _summarize_representative_value(value)
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _coalesce_snapshot_series(frame: pd.DataFrame, candidates: list[str], *, string_output: bool = False) -> pd.Series:
    for column in candidates:
        if column in frame.columns:
            series = frame[column]
            if string_output:
                formatter = _summarize_representative_value if column == "representative_stocks" else _stringify_snapshot_cell
                return series.apply(formatter)
            return series
    if string_output:
        return pd.Series([""] * len(frame), index=frame.index, dtype="object")
    return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="object")


def _coalesce_snapshot_string_priority(frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
    result = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    for column in candidates:
        if column not in frame.columns:
            continue
        formatter = _summarize_representative_value if column == "representative_stocks" else _stringify_snapshot_cell
        candidate_values = frame[column].apply(formatter)
        fill_mask = result.fillna("").astype(str).str.strip().eq("")
        if fill_mask.any():
            result.loc[fill_mask] = candidate_values.loc[fill_mask]
    return result


def _prepare_today_sector_view(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    compatibility_notes: list[str] = []
    if df is None or df.empty:
        return pd.DataFrame(columns=TODAY_SECTOR_DISPLAY_COLUMNS), compatibility_notes
    prepared = pd.DataFrame(index=df.index)
    rank_column = "today_display_rank" if "today_display_rank" in df.columns else "today_rank"
    if rank_column not in df.columns:
        compatibility_notes.append("today_rank")
    prepared["today_display_rank"] = _coalesce_snapshot_series(df, ["today_display_rank", "today_rank"])
    prepared["industry_anchor_rank"] = _coalesce_snapshot_series(df, ["industry_anchor_rank", "industry_up_anchor_rank", "industry_rank_live"])
    prepared["final_rank_delta"] = _coalesce_snapshot_series(df, ["final_rank_delta"])
    prepared["sector_name"] = _coalesce_snapshot_series(df, ["sector_name"], string_output=True)
    prepared["sector_confidence"] = _coalesce_snapshot_series(df, ["sector_confidence"], string_output=True)
    prepared["sector_caution"] = _coalesce_snapshot_series(df, ["sector_caution"], string_output=True)
    prepared["price_block_score"] = _coalesce_snapshot_series(df, ["price_strength_display", "price_block_score"])
    prepared["flow_block_score"] = _coalesce_snapshot_series(df, ["flow_strength_display", "flow_block_score"])
    prepared["ranking_breadth_display"] = _coalesce_snapshot_series(df, ["ranking_breadth_display", "ranking_source_breadth_ex_basket", "participation_block_score"])
    display_rank = _coerce_numeric(prepared["today_display_rank"])
    anchor_rank = _coerce_numeric(prepared["industry_anchor_rank"])
    delta = _coerce_numeric(prepared["final_rank_delta"])
    prepared["final_rank_delta"] = delta.where(delta.notna(), display_rank - anchor_rank)
    prepared["sector_summary"] = prepared.apply(
        lambda row: _shorten_ui_text(
            _join_ui_fragments(
                f"信頼度 {_clean_ui_value(row.get('sector_confidence'))}" if _clean_ui_value(row.get("sector_confidence")) else "",
                f"価格 {_format_ui_number(row.get('price_block_score'))}" if _format_ui_number(row.get("price_block_score")) else "",
                f"資金 {_format_ui_number(row.get('flow_block_score'))}" if _format_ui_number(row.get("flow_block_score")) else "",
                f"広がり {_clean_ui_value(row.get('ranking_breadth_display'))}" if _clean_ui_value(row.get("ranking_breadth_display")) else "",
                f"注意 {_clean_ui_value(row.get('sector_caution'))}" if _clean_ui_value(row.get("sector_caution")) else "",
            ),
            limit=92,
        ),
        axis=1,
    )
    prepared, notes = _prepare_table_view(prepared, TODAY_SECTOR_DISPLAY_COLUMNS)
    prepared["_rank_sort"] = _coerce_numeric(prepared.get("today_display_rank", pd.Series(pd.NA, index=prepared.index))).fillna(9999)
    prepared = prepared.sort_values(["_rank_sort", "sector_name"], ascending=[True, True], kind="mergesort").drop(columns=["_rank_sort"], errors="ignore")
    return prepared, compatibility_notes + notes


def _build_sector_representatives_display_frame(
    sector_representatives: pd.DataFrame,
    *,
    today_sector_leaderboard: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if sector_representatives is None or sector_representatives.empty:
        empty_rows = _build_empty_representative_display_rows(
            today_sector_leaderboard if isinstance(today_sector_leaderboard, pd.DataFrame) else pd.DataFrame(),
            set(),
        )
        if not empty_rows.empty:
            return _sort_sector_representatives_display_rows(empty_rows)[SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS].reset_index(drop=True)
        return pd.DataFrame(columns=SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS)
    working = sector_representatives.copy()
    for column in [
        "today_rank",
        "sector_name",
        "representative_rank",
        "code",
        "name",
        "live_ret_vs_prev_close",
        "current_price",
        "current_price_unavailable",
        "live_turnover_value",
        "live_turnover_unavailable",
        "representative_selected_reason",
        "representative_quality_flag",
        "representative_fallback_reason",
        "earnings_announcement_date",
        "nikkei_search",
    ]:
        if column not in working.columns:
            if column in {"sector_name", "code", "name", "representative_selected_reason", "representative_quality_flag", "representative_fallback_reason", "earnings_announcement_date", "nikkei_search"}:
                working[column] = ""
            elif column in {"current_price_unavailable", "live_turnover_unavailable"}:
                working[column] = True
            else:
                working[column] = pd.NA
    working["current_price"] = working.get("current_price", working.get("live_price", pd.Series(pd.NA, index=working.index)))
    working["live_turnover_value"] = working.get("live_turnover_value", working.get("live_turnover", pd.Series(pd.NA, index=working.index)))
    working["current_price_unavailable"] = working.get("current_price_unavailable", working["current_price"].isna()).fillna(True).astype(bool)
    working["live_turnover_unavailable"] = working.get("live_turnover_unavailable", working["live_turnover_value"].isna()).fillna(True).astype(bool)
    working["today_rank"] = working["today_rank"].apply(_format_display_rank_value)
    working["representative_rank"] = working["representative_rank"].apply(_format_display_rank_value)
    working["code"] = working["code"].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
    working["name"] = [
        _format_stock_name_with_marker(
            "代表なし（当日中心株不在）"
            if str(value or "").strip() == "代表なし"
            else _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK),
            marked=bool(flag),
        )
        for value, flag in zip(
            working["name"],
            working.get("earnings_today_announcement_flag", pd.Series(False, index=working.index)).fillna(False),
        )
    ]
    working["live_ret_vs_prev_close"] = working["live_ret_vs_prev_close"].apply(_format_display_pct_1dp)
    working["current_price"] = [
        _format_display_price_value(value, unavailable=bool(unavailable))
        for value, unavailable in zip(working["current_price"], working["current_price_unavailable"])
    ]
    working["live_turnover_value"] = [
        _format_display_value_or_unavailable(
            value,
            unavailable=bool(unavailable),
            formatter=lambda numeric: f"{int(round(float(numeric))):,}",
        )
        for value, unavailable in zip(working["live_turnover_value"], working["live_turnover_unavailable"])
    ]
    working["representative_selected_reason"] = working["representative_selected_reason"].apply(_representative_selected_reason_label)
    working["earnings_announcement_date"] = working["earnings_announcement_date"].apply(_format_display_announcement_date)
    working["representative_quality_flag"] = working["representative_quality_flag"].apply(_representative_quality_flag_label)
    working["representative_fallback_reason"] = working["representative_fallback_reason"].apply(_representative_fallback_reason_label)
    working["nikkei_search"] = working["nikkei_search"].fillna("").astype(str)
    working = working.drop(columns=["current_price_unavailable", "live_turnover_unavailable"], errors="ignore")
    display = working.copy()
    if isinstance(today_sector_leaderboard, pd.DataFrame) and not today_sector_leaderboard.empty:
        empty_rows = _build_empty_representative_display_rows(
            today_sector_leaderboard,
            set(display.get("sector_name", pd.Series(dtype=str)).astype(str).tolist()),
        )
        if not empty_rows.empty:
            display = pd.concat([display, empty_rows], ignore_index=True, sort=False)
    display = _sort_sector_representatives_display_rows(display)
    return display[SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS].reset_index(drop=True)


def _sort_swing_candidate_display_rows(frame: pd.DataFrame, *, rank_col: str) -> pd.DataFrame:
    if rank_col == "candidate_rank_1w":
        columns = SWING_1W_DISPLAY_COLUMNS
    elif rank_col == "candidate_rank_3m":
        columns = SWING_3M_DISPLAY_COLUMNS
    else:
        columns = SWING_1M_DISPLAY_COLUMNS
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    working = frame.copy()
    working["_rank_sort"] = _coerce_numeric(working.get(rank_col, pd.Series(pd.NA, index=working.index))).fillna(9999)
    if "sector_name" not in working.columns:
        working["sector_name"] = ""
    if "code" not in working.columns:
        working["code"] = ""
    working = working.sort_values(
        ["_rank_sort", "sector_name", "code"],
        ascending=[True, True, True],
        kind="mergesort",
    ).copy()
    return working.drop(columns=["_rank_sort"], errors="ignore")


def _build_swing_candidate_display_frame(
    frame: pd.DataFrame,
    *,
    horizon: str,
) -> pd.DataFrame:
    if horizon == "1w":
        rank_col = "candidate_rank_1w"
        columns = SWING_1W_DISPLAY_COLUMNS
    elif horizon == "3m":
        rank_col = "candidate_rank_3m"
        columns = SWING_3M_DISPLAY_COLUMNS
    else:
        rank_col = "candidate_rank_1m"
        columns = SWING_1M_DISPLAY_COLUMNS
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    working = frame.copy()
    for column in columns:
        if column not in working.columns:
            if column in {"sector_name", "code", "name", "candidate_quality", "entry_fit", "entry_stance_label", "stretch_caution_label", "watch_reason_label", "selection_reason", "horizon_fit_reason", "entry_caution", "candidate_bucket_label", "event_caution_reason", "earnings_announcement_date", "risk_note", "candidate_commentary", "finance_health_flag", "nikkei_search"}:
                working[column] = ""
            else:
                working[column] = pd.NA
    rank_numeric = _coerce_numeric(working.get(rank_col, pd.Series([pd.NA] * len(working), index=working.index)))
    working[rank_col] = rank_numeric.round().astype("Int64")
    working["current_price"] = _coerce_numeric(working.get("current_price", working.get("live_price", pd.Series(pd.NA, index=working.index))))
    working["live_turnover_value"] = _coerce_numeric(working.get("live_turnover_value", working.get("live_turnover", pd.Series(pd.NA, index=working.index))))
    working["current_price_unavailable"] = working.get("current_price_unavailable", working["current_price"].isna()).fillna(True).astype(bool)
    working["live_turnover_unavailable"] = working.get("live_turnover_unavailable", working["live_turnover_value"].isna()).fillna(True).astype(bool)
    working["sector_name"] = working["sector_name"].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
    working["code"] = working["code"].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
    working["name"] = working["name"].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
    for column in ["candidate_quality", "entry_fit", "entry_stance_label", "stretch_caution_label", "watch_reason_label", "selection_reason", "horizon_fit_reason", "entry_caution", "candidate_bucket_label", "event_caution_reason", "risk_note", "candidate_commentary", "finance_health_flag"]:
        if column in working.columns:
            working[column] = working[column].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
    working["live_ret_vs_prev_close"] = working["live_ret_vs_prev_close"].apply(_format_display_pct_1dp)
    working["current_price"] = [
        _format_display_price_value(value, unavailable=bool(unavailable))
        for value, unavailable in zip(working["current_price"], working["current_price_unavailable"])
    ]
    working["live_turnover_value"] = [
        _format_display_value_or_unavailable(
            value,
            unavailable=bool(unavailable),
            formatter=lambda numeric: f"{int(round(float(numeric))):,}",
        )
        for value, unavailable in zip(working["live_turnover_value"], working["live_turnover_unavailable"])
    ]
    for numeric_col in ["rs_vs_topix_1w", "rs_vs_topix_1m", "rs_vs_topix_3m", "ret_3m", "price_vs_ma20_pct"]:
        if numeric_col in working.columns:
            working[numeric_col] = working[numeric_col].apply(_format_display_pct_1dp)
    if "earnings_announcement_date" in working.columns:
        working["earnings_announcement_date"] = working["earnings_announcement_date"].apply(_format_display_announcement_date)
    if "avg_turnover_20d" in working.columns:
        working["avg_turnover_20d"] = working["avg_turnover_20d"].apply(
            lambda value: _format_display_value_or_unavailable(
                value,
                unavailable=bool(pd.isna(_coerce_numeric(pd.Series([value])).iloc[0])),
                formatter=lambda numeric: f"{int(round(float(numeric))):,}",
            )
        )
    if "earnings_buffer_days" in working.columns:
        working["earnings_buffer_days"] = _coerce_numeric(working["earnings_buffer_days"]).round().astype("Int64")
    working["nikkei_search"] = working["nikkei_search"].fillna("").astype(str)
    working = working.drop(columns=["current_price_unavailable", "live_turnover_unavailable"], errors="ignore")
    working = _sort_swing_candidate_display_rows(working, rank_col=rank_col)
    return working[columns].reset_index(drop=True)


def _prepare_swing_candidate_display_view(
    saved_display: pd.DataFrame,
    *,
    columns: list[str],
    raw_fallback: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    compatibility_notes: list[str] = []
    rank_columns = [column for column in ["candidate_rank_1w", "candidate_rank_1m", "candidate_rank_3m", "earnings_buffer_days"] if column in columns]
    auxiliary_columns = ["fallback_used"]

    def _attach_auxiliary_columns(prepared_frame: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(raw_fallback, pd.DataFrame) or raw_fallback.empty or "code" not in prepared_frame.columns or "code" not in raw_fallback.columns:
            return prepared_frame
        output = prepared_frame.copy()
        raw_lookup = raw_fallback.copy()
        raw_lookup["_code_key"] = raw_lookup["code"].astype(str)
        for auxiliary_column in auxiliary_columns:
            if auxiliary_column in output.columns or auxiliary_column not in raw_lookup.columns:
                continue
            lookup = raw_lookup.drop_duplicates("_code_key").set_index("_code_key")[auxiliary_column]
            output[auxiliary_column] = output["code"].astype(str).map(lookup)
        return output

    if isinstance(saved_display, pd.DataFrame) and not saved_display.empty:
        prepared = saved_display.copy()
        for column in columns:
            if column not in prepared.columns:
                prepared[column] = ""
                compatibility_notes.append(column)
        for column in columns:
            if column in rank_columns:
                prepared[column] = _coerce_numeric(prepared[column]).round().astype("Int64")
            elif column == "earnings_announcement_date":
                prepared[column] = prepared[column].apply(_format_display_announcement_date)
            else:
                prepared[column] = prepared[column].fillna("").astype(str)
        prepared = _attach_auxiliary_columns(prepared)
        return prepared.reindex(columns=columns + [column for column in auxiliary_columns if column in prepared.columns]), compatibility_notes
    if isinstance(raw_fallback, pd.DataFrame) and not raw_fallback.empty:
        compatibility_notes.append("スイング候補表は旧 snapshot 互換表示です。raw swing_candidates から表示列だけ抽出しています。")
        horizon = "1w" if "candidate_rank_1w" in columns else ("3m" if "candidate_rank_3m" in columns else "1m")
        prepared = _build_swing_candidate_display_frame(raw_fallback, horizon=horizon)
        for column in columns:
            if column not in prepared.columns:
                prepared[column] = ""
                compatibility_notes.append(column)
        for column in columns:
            if column in rank_columns:
                prepared[column] = _coerce_numeric(prepared[column]).round().astype("Int64")
            elif column == "earnings_announcement_date":
                prepared[column] = prepared[column].apply(_format_display_announcement_date)
            else:
                prepared[column] = prepared[column].fillna("").astype(str)
        prepared = _attach_auxiliary_columns(prepared)
        return prepared.reindex(columns=columns + [column for column in auxiliary_columns if column in prepared.columns]), compatibility_notes
    return pd.DataFrame(columns=columns), compatibility_notes


def _empty_sector_representatives_audit_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=SECTOR_REPRESENTATIVES_AUDIT_COLUMNS)


def _build_sector_representatives_audit_frame(
    today_sector_leaderboard: pd.DataFrame,
    representative_pool_with_selection: pd.DataFrame,
    sector_representatives: pd.DataFrame,
    *,
    sector_key_col: str,
) -> pd.DataFrame:
    if today_sector_leaderboard is None or today_sector_leaderboard.empty:
        return _empty_sector_representatives_audit_frame()
    leaderboard_columns = [
        sector_key_col,
        "sector_name",
        "today_rank",
        "rep_candidate_pool_count",
        "rep_with_live_ret_count",
        "rep_with_current_price_count",
        "rep_with_live_turnover_count",
        "rep_pass_relative_leadership_count",
        "rep_pass_centrality_count",
        "rep_pass_sanity_count",
        "rep_selected_count",
        "deep_watch_selected_count",
        "deep_watch_observed_live_count",
        "rep_candidate_pool_status",
        "rep_candidate_pool_reason",
    ]
    leaderboard = today_sector_leaderboard[[column for column in leaderboard_columns if column in today_sector_leaderboard.columns]].drop_duplicates()
    selected_lookup = (
        sector_representatives[["sector_name", "code", "representative_selected_reason", "representative_quality_flag", "representative_fallback_reason"]].drop_duplicates(["sector_name", "code"])
        if sector_representatives is not None and not sector_representatives.empty
        else pd.DataFrame(columns=["sector_name", "code", "representative_selected_reason", "representative_quality_flag", "representative_fallback_reason"])
    )
    audit_rows: list[dict[str, Any]] = []
    representative_pool = representative_pool_with_selection.copy() if representative_pool_with_selection is not None and not representative_pool_with_selection.empty else pd.DataFrame()
    if not representative_pool.empty:
        representative_pool["code"] = representative_pool["code"].astype(str)
        representative_pool = representative_pool.merge(
            selected_lookup.rename(
                columns={
                    "representative_selected_reason": "selected_reason_from_selected",
                    "representative_quality_flag": "quality_flag_from_selected",
                    "representative_fallback_reason": "fallback_reason_from_selected",
                }
            ),
            on=["sector_name", "code"],
            how="left",
        )
    for _, sector_row in leaderboard.iterrows():
        sector_key = str(sector_row.get(sector_key_col, "") or "")
        sector_name = str(sector_row.get("sector_name", "") or "")
        today_rank = sector_row.get("today_rank", pd.NA)
        sector_meta = {
            "rep_candidate_pool_count": int(sector_row.get("rep_candidate_pool_count", 0) or 0),
            "rep_with_live_ret_count": int(sector_row.get("rep_with_live_ret_count", 0) or 0),
            "rep_with_current_price_count": int(sector_row.get("rep_with_current_price_count", 0) or 0),
            "rep_with_live_turnover_count": int(sector_row.get("rep_with_live_turnover_count", 0) or 0),
            "rep_pass_relative_leadership_count": int(sector_row.get("rep_pass_relative_leadership_count", 0) or 0),
            "rep_pass_centrality_count": int(sector_row.get("rep_pass_centrality_count", 0) or 0),
            "rep_pass_sanity_count": int(sector_row.get("rep_pass_sanity_count", 0) or 0),
            "rep_selected_count": int(sector_row.get("rep_selected_count", 0) or 0),
            "deep_watch_selected_count": int(sector_row.get("deep_watch_selected_count", 0) or 0),
            "deep_watch_observed_live_count": int(sector_row.get("deep_watch_observed_live_count", 0) or 0),
            "rep_candidate_pool_status": str(sector_row.get("rep_candidate_pool_status", "") or ""),
            "rep_candidate_pool_reason": str(sector_row.get("rep_candidate_pool_reason", "") or ""),
        }
        sector_pool = representative_pool[representative_pool[sector_key_col].astype(str) == sector_key].copy() if not representative_pool.empty else pd.DataFrame()
        if sector_pool.empty:
            audit_rows.append(
                {
                    "today_rank": int(today_rank) if pd.notna(today_rank) else None,
                    "sector_name": sector_name,
                    **sector_meta,
                    "code": "",
                    "name": "",
                    "candidate_in_universe": False,
                    "live_ret_vs_prev_close_raw": None,
                    "current_price_raw": None,
                    "live_turnover_raw": None,
                    "sector_turnover_share_raw": None,
                    "selected_horizon": "today",
                    "selected_universe": "",
                    "primary_candidate_count": 0,
                    "supplemental_candidate_count": 0,
                    "final_candidate_count": 0,
                    "sector_constituent_count": 0,
                    "representative_pool_coverage_rate": None,
                    "candidate_pool_warning": "",
                    "candidate_pool_reason": "",
                    "selected_from_primary_or_supplemental": "",
                    "sector_candidate_count": 0,
                    "sector_positive_count": 0,
                    "sector_negative_count": 0,
                    "sector_positive_rate": None,
                    "sector_median_return": None,
                    "sector_top_quartile_return": None,
                    "sector_bottom_quartile_return": None,
                    "stock_return_percentile_in_sector": None,
                    "stock_return_rank_in_sector": None,
                    "market_positive_rate": None,
                    "market_context": "",
                    "sector_context": "",
                    "sector_live_ret_median": None,
                    "sector_top_positive_count": 0,
                    "representative_gate_pass": False,
                    "representative_gate_reason": "no_candidate_in_representative_pool",
                    "hard_reject_reason": "",
                    "hard_block_reason": "",
                    "fallback_used": False,
                    "fallback_reason": "",
                    "fallback_blocked_reason": "",
                    "live_ret_from_open": None,
                    "sector_live_ret_pct": None,
                    "sector_today_flow_pct": None,
                    "sector_turnover_share": None,
                    "exclude_spike": False,
                    "exclude_spike_hard_reject": False,
                    "exclude_spike_warning_only": False,
                    "spike_quality": "",
                    "poor_quality_spike": False,
                    "material_supported_breakout": False,
                    "breakout_support_reason": "",
                    "centrality_score": None,
                    "liquidity_score": None,
                    "today_leadership_score": None,
                    "representative_final_score": None,
                    "selected_reason": "",
                    "rep_score_today_strength": None,
                    "rep_score_relative_strength": None,
                    "rep_score_liquidity": None,
                    "rep_score_total_raw": None,
                    "rep_score_centrality_raw": None,
                    "rep_score_today_leadership_raw": None,
                    "rep_score_sanity_raw": None,
                    "relative_leadership_pass": False,
                    "centrality_pass": False,
                    "sanity_pass": False,
                    "selected_for_representative": False,
                    "selected_reason_raw": "",
                    "quality_flag_raw": "",
                    "fallback_reason_raw": "",
                    "was_in_selected50": False,
                    "was_in_must_have": False,
                }
            )
            continue
        sector_pool = sector_pool.sort_values(
            ["rep_score_today_leadership", "rep_score_centrality", "rep_score_total", "code"],
            ascending=[False, False, False, True],
            kind="mergesort",
        )
        for _, row in sector_pool.iterrows():
            selected_reason_raw = str(row.get("rep_selected_reason", row.get("selected_reason_from_selected", "")) or "")
            quality_flag_raw = str(row.get("representative_quality_flag", row.get("quality_flag_from_selected", "")) or "")
            fallback_reason_raw = str(row.get("representative_fallback_reason", row.get("fallback_reason_from_selected", row.get("rep_fallback_reason", ""))) or "")
            audit_rows.append(
                {
                    "today_rank": int(today_rank) if pd.notna(today_rank) else None,
                    "sector_name": sector_name,
                    **sector_meta,
                    "code": str(row.get("code", "") or ""),
                    "name": str(row.get("name", "") or ""),
                    "candidate_in_universe": bool(row.get("candidate_in_universe", True)),
                    "live_ret_vs_prev_close_raw": float(row.get("live_ret_vs_prev_close", 0.0) or 0.0) if pd.notna(row.get("live_ret_vs_prev_close")) else None,
                    "current_price_raw": float(row.get("live_price", 0.0) or 0.0) if pd.notna(row.get("live_price")) else None,
                    "live_turnover_raw": float(row.get("live_turnover", 0.0) or 0.0) if pd.notna(row.get("live_turnover")) else None,
                    "sector_turnover_share_raw": float(row.get("stock_turnover_share_of_sector", 0.0) or 0.0) if pd.notna(row.get("stock_turnover_share_of_sector")) else None,
                    "selected_horizon": str(row.get("selected_horizon", "today") or "today"),
                    "selected_universe": str(row.get("selected_universe", "") or ""),
                    "primary_candidate_count": int(row.get("primary_candidate_count", 0) or 0) if pd.notna(row.get("primary_candidate_count", pd.NA)) else None,
                    "supplemental_candidate_count": int(row.get("supplemental_candidate_count", 0) or 0) if pd.notna(row.get("supplemental_candidate_count", pd.NA)) else None,
                    "final_candidate_count": int(row.get("final_candidate_count", row.get("sector_candidate_count", 0)) or 0) if pd.notna(row.get("final_candidate_count", row.get("sector_candidate_count", pd.NA))) else None,
                    "sector_constituent_count": int(row.get("sector_constituent_count", 0) or 0) if pd.notna(row.get("sector_constituent_count", pd.NA)) else None,
                    "representative_pool_coverage_rate": float(row.get("representative_pool_coverage_rate", 0.0) or 0.0) if pd.notna(row.get("representative_pool_coverage_rate", pd.NA)) else None,
                    "candidate_pool_warning": str(row.get("candidate_pool_warning", "") or ""),
                    "candidate_pool_reason": str(row.get("candidate_pool_reason", "") or ""),
                    "selected_from_primary_or_supplemental": str(row.get("selected_from_primary_or_supplemental", "") or ""),
                    "sector_candidate_count": int(row.get("sector_candidate_count", 0) or 0) if pd.notna(row.get("sector_candidate_count", pd.NA)) else None,
                    "sector_positive_count": int(row.get("sector_positive_count", row.get("sector_positive_candidate_count", 0)) or 0) if pd.notna(row.get("sector_positive_count", row.get("sector_positive_candidate_count", pd.NA))) else None,
                    "sector_negative_count": int(row.get("sector_negative_count", row.get("sector_negative_candidate_count", 0)) or 0) if pd.notna(row.get("sector_negative_count", row.get("sector_negative_candidate_count", pd.NA))) else None,
                    "sector_positive_rate": float(row.get("sector_positive_rate", 0.0) or 0.0) if pd.notna(row.get("sector_positive_rate", pd.NA)) else None,
                    "sector_median_return": float(row.get("sector_median_return", row.get("sector_live_ret_median", 0.0)) or 0.0) if pd.notna(row.get("sector_median_return", row.get("sector_live_ret_median", pd.NA))) else None,
                    "sector_top_quartile_return": float(row.get("sector_top_quartile_return", 0.0) or 0.0) if pd.notna(row.get("sector_top_quartile_return", pd.NA)) else None,
                    "sector_bottom_quartile_return": float(row.get("sector_bottom_quartile_return", 0.0) or 0.0) if pd.notna(row.get("sector_bottom_quartile_return", pd.NA)) else None,
                    "stock_return_percentile_in_sector": float(row.get("stock_return_percentile_in_sector", row.get("sector_live_ret_pct", 0.0)) or 0.0) if pd.notna(row.get("stock_return_percentile_in_sector", row.get("sector_live_ret_pct", pd.NA))) else None,
                    "stock_return_rank_in_sector": int(row.get("stock_return_rank_in_sector", row.get("sector_live_ret_rank_desc", 0)) or 0) if pd.notna(row.get("stock_return_rank_in_sector", row.get("sector_live_ret_rank_desc", pd.NA))) else None,
                    "market_positive_rate": float(row.get("market_positive_rate", 0.0) or 0.0) if pd.notna(row.get("market_positive_rate", pd.NA)) else None,
                    "market_context": str(row.get("market_context", "") or ""),
                    "sector_context": str(row.get("sector_context", "") or ""),
                    "sector_live_ret_median": float(row.get("sector_live_ret_median", 0.0) or 0.0) if pd.notna(row.get("sector_live_ret_median", pd.NA)) else None,
                    "sector_top_positive_count": int(row.get("sector_top_positive_count", 0) or 0) if pd.notna(row.get("sector_top_positive_count", pd.NA)) else None,
                    "representative_gate_pass": bool(row.get("representative_gate_pass", False)),
                    "representative_gate_reason": str(row.get("representative_gate_reason", "") or ""),
                    "hard_reject_reason": str(row.get("hard_reject_reason", row.get("hard_block_reason", "")) or ""),
                    "hard_block_reason": str(row.get("hard_block_reason", "") or ""),
                    "fallback_used": bool(row.get("fallback_used", False)),
                    "fallback_reason": str(row.get("fallback_reason", "") or ""),
                    "fallback_blocked_reason": str(row.get("fallback_blocked_reason", "") or ""),
                    "live_ret_from_open": float(row.get("live_ret_from_open", 0.0) or 0.0) if pd.notna(row.get("live_ret_from_open", pd.NA)) else None,
                    "sector_live_ret_pct": float(row.get("sector_live_ret_pct", 0.0) or 0.0) if pd.notna(row.get("sector_live_ret_pct", pd.NA)) else None,
                    "sector_today_flow_pct": float(row.get("sector_today_flow_pct", 0.0) or 0.0) if pd.notna(row.get("sector_today_flow_pct", pd.NA)) else None,
                    "sector_turnover_share": float(row.get("sector_turnover_share", row.get("stock_turnover_share_of_sector", 0.0)) or 0.0) if pd.notna(row.get("sector_turnover_share", row.get("stock_turnover_share_of_sector", pd.NA))) else None,
                    "exclude_spike": bool(row.get("exclude_spike", False)),
                    "exclude_spike_hard_reject": bool(row.get("exclude_spike_hard_reject", False)),
                    "exclude_spike_warning_only": bool(row.get("exclude_spike_warning_only", False)),
                    "spike_quality": str(row.get("spike_quality", "") or ""),
                    "poor_quality_spike": bool(row.get("poor_quality_spike", False)),
                    "material_supported_breakout": bool(row.get("material_supported_breakout", False)),
                    "breakout_support_reason": str(row.get("breakout_support_reason", "") or ""),
                    "centrality_score": float(row.get("centrality_score", row.get("rep_score_centrality", 0.0)) or 0.0) if pd.notna(row.get("centrality_score", row.get("rep_score_centrality", pd.NA))) else None,
                    "liquidity_score": float(row.get("liquidity_score", row.get("rep_score_liquidity", 0.0)) or 0.0) if pd.notna(row.get("liquidity_score", row.get("rep_score_liquidity", pd.NA))) else None,
                    "today_leadership_score": float(row.get("today_leadership_score", row.get("rep_score_today_leadership", 0.0)) or 0.0) if pd.notna(row.get("today_leadership_score", row.get("rep_score_today_leadership", pd.NA))) else None,
                    "representative_final_score": float(row.get("representative_final_score", row.get("rep_score_total", 0.0)) or 0.0) if pd.notna(row.get("representative_final_score", row.get("rep_score_total", pd.NA))) else None,
                    "selected_reason": str(row.get("selected_reason", selected_reason_raw) or ""),
                    "rep_score_today_strength": float(row.get("rep_score_today_strength", row.get("rep_score_today_leadership", 0.0)) or 0.0) if pd.notna(row.get("rep_score_today_strength", row.get("rep_score_today_leadership", pd.NA))) else None,
                    "rep_score_relative_strength": float(row.get("rep_score_relative_strength", 0.0) or 0.0) if pd.notna(row.get("rep_score_relative_strength", pd.NA)) else None,
                    "rep_score_liquidity": float(row.get("rep_score_liquidity", row.get("rep_score_today_flow", 0.0)) or 0.0) if pd.notna(row.get("rep_score_liquidity", row.get("rep_score_today_flow", pd.NA))) else None,
                    "rep_score_total_raw": float(row.get("rep_score_total", 0.0) or 0.0) if pd.notna(row.get("rep_score_total")) else None,
                    "rep_score_centrality_raw": float(row.get("rep_score_centrality", 0.0) or 0.0) if pd.notna(row.get("rep_score_centrality")) else None,
                    "rep_score_today_leadership_raw": float(row.get("rep_score_today_leadership", 0.0) or 0.0) if pd.notna(row.get("rep_score_today_leadership")) else None,
                    "rep_score_sanity_raw": float(row.get("rep_score_sanity", 0.0) or 0.0) if pd.notna(row.get("rep_score_sanity")) else None,
                    "relative_leadership_pass": bool(row.get("rep_relative_leadership_pass", False)),
                    "centrality_pass": bool(row.get("rep_centrality_pass", False)),
                    "sanity_pass": bool(row.get("rep_sanity_pass", False)),
                    "selected_for_representative": bool(selected_reason_raw),
                    "selected_reason_raw": selected_reason_raw,
                    "quality_flag_raw": quality_flag_raw,
                    "fallback_reason_raw": fallback_reason_raw,
                    "was_in_selected50": bool(row.get("was_in_selected50", False)),
                    "was_in_must_have": bool(row.get("was_in_must_have", False)),
                }
            )
    if not audit_rows:
        return _empty_sector_representatives_audit_frame()
    audit = pd.DataFrame(audit_rows)
    for column in SECTOR_REPRESENTATIVES_AUDIT_COLUMNS:
        if column not in audit.columns:
            audit[column] = pd.NA
    audit["_today_rank_sort"] = _coerce_numeric(audit["today_rank"]).fillna(9999)
    audit["_selected_sort"] = audit["selected_for_representative"].fillna(False).astype(int) * -1
    audit = audit.sort_values(
        ["_today_rank_sort", "sector_name", "_selected_sort", "rep_score_today_leadership_raw", "code"],
        ascending=[True, True, True, False, True],
        kind="mergesort",
    )
    return audit.drop(columns=["_today_rank_sort", "_selected_sort"], errors="ignore")[SECTOR_REPRESENTATIVES_AUDIT_COLUMNS].reset_index(drop=True)


def _prepare_sector_representatives_display_view(
    frame: pd.DataFrame,
    *,
    display_is_source_of_truth: bool = False,
    earnings_announcement_lookup: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    display_frame = frame.copy() if display_is_source_of_truth else _build_sector_representatives_display_frame(frame)
    compatibility_notes: list[str] = []
    if display_frame is None or display_frame.empty:
        return pd.DataFrame(columns=SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS), compatibility_notes
    prepared = display_frame.copy()
    for column in SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = ""
            compatibility_notes.append(column)
    prepared["today_rank"] = prepared["today_rank"].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
    if "representative_rank" in prepared.columns:
        prepared["representative_rank"] = prepared["representative_rank"].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
    for column in ["sector_name", "code", "name", "live_ret_vs_prev_close", "current_price", "live_turnover_value"]:
        prepared[column] = prepared[column].apply(lambda value: _normalize_display_text(value, missing=DISPLAY_UNAVAILABLE_MARK))
    prepared["representative_selected_reason"] = prepared["representative_selected_reason"].apply(lambda value: _normalize_saved_representative_label(value, _representative_selected_reason_label))
    prepared["earnings_announcement_date"] = _resolve_frame_earnings_announcement_dates(
        prepared,
        lookup=earnings_announcement_lookup,
    ).apply(_format_display_announcement_date)
    prepared["representative_quality_flag"] = prepared["representative_quality_flag"].apply(lambda value: _normalize_saved_representative_label(value, _representative_quality_flag_label))
    prepared["representative_fallback_reason"] = prepared["representative_fallback_reason"].apply(lambda value: _normalize_saved_representative_label(value, _representative_fallback_reason_label))
    prepared["nikkei_search"] = prepared["nikkei_search"].apply(lambda value: _normalize_display_text(value, missing=""))
    prepared = _sort_sector_representatives_display_rows(prepared).reindex(columns=SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS)
    return prepared.reindex(columns=SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS), compatibility_notes


def _trim_sector_summary_for_storage(frame: Any) -> Any:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    drop_columns = [
        "wide_scan_member_codes",
        "ranking_confirmed_codes",
        "selected50_codes_in_sector",
        "sector_center_candidate_codes",
        "representative_candidate_codes",
        "representative_excluded_reason_by_code",
        "representative_trace_top10",
    ]
    keep_columns = [column for column in frame.columns if column not in drop_columns]
    return frame[keep_columns].copy()


def _trim_representatives_for_storage(frame: Any) -> Any:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    keep_columns = [
        column
        for column in [
            "today_rank",
            "sector_name",
            "representative_rank",
            "code",
            "name",
            "live_price",
            "current_price",
            "current_price_unavailable",
            "live_ret_vs_prev_close",
            "live_turnover",
            "live_turnover_value",
            "live_turnover_unavailable",
            "stock_turnover_share_of_sector",
            "selected_horizon",
            "selected_universe",
            "primary_candidate_count",
            "supplemental_candidate_count",
            "final_candidate_count",
            "sector_constituent_count",
            "representative_pool_coverage_rate",
            "candidate_pool_warning",
            "candidate_pool_reason",
            "selected_from_primary_or_supplemental",
            "sector_candidate_count",
            "sector_positive_count",
            "sector_negative_count",
            "sector_positive_rate",
            "sector_median_return",
            "sector_top_quartile_return",
            "sector_bottom_quartile_return",
            "stock_return_percentile_in_sector",
            "stock_return_rank_in_sector",
            "market_positive_rate",
            "market_context",
            "sector_context",
            "sector_live_ret_median",
            "sector_top_positive_count",
            "representative_gate_pass",
            "representative_gate_reason",
            "hard_reject_reason",
            "hard_block_reason",
            "fallback_used",
            "fallback_reason",
            "fallback_blocked_reason",
            "live_ret_from_open",
            "sector_live_ret_pct",
            "sector_today_flow_pct",
            "sector_turnover_share",
            "exclude_spike",
            "exclude_spike_hard_reject",
            "exclude_spike_warning_only",
            "spike_quality",
            "poor_quality_spike",
            "material_supported_breakout",
            "breakout_support_reason",
            "centrality_score",
            "liquidity_score",
            "today_leadership_score",
            "representative_final_score",
            "selected_reason",
            "rep_score_today_strength",
            "rep_score_relative_strength",
            "rep_score_liquidity",
            "representative_score",
            "rep_score_total",
            "rep_score_centrality",
            "rep_score_today_leadership",
            "rep_score_sanity",
            "representative_selected_reason",
            "representative_quality_flag",
            "representative_fallback_reason",
            "earnings_today_announcement_flag",
            "earnings_announcement_date",
            "was_in_selected50",
            "was_in_must_have",
            "nikkei_search",
            "material_link",
        ]
        if column in frame.columns
    ]
    return frame[keep_columns].copy()


def _trim_representatives_display_for_storage(frame: Any) -> Any:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    keep_columns = [column for column in SECTOR_REPRESENTATIVES_DISPLAY_COLUMNS if column in frame.columns]
    return frame[keep_columns].copy()


def _trim_representatives_audit_for_storage(frame: Any) -> Any:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    keep_columns = [column for column in SECTOR_REPRESENTATIVES_AUDIT_COLUMNS if column in frame.columns]
    return frame[keep_columns].copy()


def _trim_sector_live_aggregate_audit_for_storage(frame: Any) -> Any:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    keep_columns = [column for column in SECTOR_LIVE_AGGREGATE_AUDIT_COLUMNS if column in frame.columns]
    trimmed = frame[keep_columns].copy()
    for column in [
        "today_rank",
        "live_aggregate_observed_count",
        "live_aggregate_ret_count",
        "live_aggregate_turnover_count",
        "live_aggregate_turnover_ratio_count",
    ]:
        if column in trimmed.columns:
            numeric = _coerce_numeric(trimmed[column])
            trimmed[column] = numeric.where(numeric.notna(), None)
    for column in [
        "live_turnover_total_raw",
        "leader_live_turnover_raw",
        "median_live_ret_raw",
        "turnover_ratio_median_raw",
    ]:
        if column in trimmed.columns:
            numeric = _coerce_numeric(trimmed[column])
            trimmed[column] = numeric.astype(object).where(numeric.notna(), None)
    return trimmed


def _trim_swing_display_for_storage(frame: Any, *, horizon: str) -> Any:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    display_columns = SWING_1W_DISPLAY_COLUMNS if horizon == "1w" else (SWING_3M_DISPLAY_COLUMNS if horizon == "3m" else SWING_1M_DISPLAY_COLUMNS)
    keep_columns = [
        column
        for column in display_columns
        if column in frame.columns
    ]
    trimmed = frame[keep_columns].copy()
    for column in ["candidate_rank_1w", "candidate_rank_1m", "candidate_rank_3m", "earnings_buffer_days"]:
        if column in trimmed.columns:
            numeric = _coerce_numeric(trimmed[column]).round()
            trimmed[column] = numeric.astype(object).where(numeric.notna(), None)
    return trimmed


def _trim_swing_audit_for_storage(frame: Any) -> Any:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    keep_columns = [column for column in SWING_CANDIDATE_AUDIT_COLUMNS if column in frame.columns]
    return frame[keep_columns].copy()


def _trim_diagnostics_for_storage(diagnostics: Any) -> Any:
    if not isinstance(diagnostics, dict):
        return diagnostics
    trimmed = dict(diagnostics)
    for key in [
        "representative_sector_trace_top10",
        "tuning_compare",
        "non_corporate_products",
        "sector_basket_counts",
        "industry_anchor_watch_before",
        "industry_anchor_watch_after",
        "industry_anchor_presence_watch",
        "top10_before_after_compare",
        "base_meta",
        "ranking",
        "deep_watch",
        "board",
    ]:
        trimmed.pop(key, None)
    return trimmed


def _bundle_for_storage(source_bundle: dict[str, Any]) -> dict[str, Any]:
    storage_bundle: dict[str, Any] = {}
    for key, value in source_bundle.items():
        if key in {
            "today_sector_summary",
            "weekly_sector_summary",
            "monthly_sector_summary",
            "leaders_by_sector",
            "center_stocks",
            "focus_candidates",
            "watch_candidates",
            "buy_candidates",
            "swing_buy_candidates_1w",
            "swing_watch_candidates_1w",
            "swing_buy_candidates_1m",
            "swing_watch_candidates_1m",
            "swing_buy_candidates_3m",
            "swing_watch_candidates_3m",
            "paths",
            "snapshot_source_label",
            "snapshot_backend_name",
            "snapshot_warning_message",
            "snapshot_guard",
            "snapshot_compatibility_notes",
            "today_sector_source_key",
        }:
            continue
        if key in {"sector_summary", "today_sector_leaderboard"}:
            storage_bundle[key] = _trim_sector_summary_for_storage(value)
            continue
        if key == "sector_representatives":
            storage_bundle[key] = _trim_representatives_for_storage(value)
            continue
        if key == "sector_representatives_display":
            storage_bundle[key] = _trim_representatives_display_for_storage(value)
            continue
        if key == "sector_representatives_audit":
            storage_bundle[key] = _trim_representatives_audit_for_storage(value)
            continue
        if key == "sector_live_aggregate_audit":
            storage_bundle[key] = _trim_sector_live_aggregate_audit_for_storage(value)
            continue
        if key == "swing_candidates_1w_display":
            storage_bundle[key] = _trim_swing_display_for_storage(value, horizon="1w")
            continue
        if key == "swing_candidates_1m_display":
            storage_bundle[key] = _trim_swing_display_for_storage(value, horizon="1m")
            continue
        if key == "swing_candidates_3m_display":
            storage_bundle[key] = _trim_swing_display_for_storage(value, horizon="3m")
            continue
        if key in {"swing_1w_candidates_audit", "swing_1m_candidates_audit", "swing_3m_candidates_audit"}:
            storage_bundle[key] = _trim_swing_audit_for_storage(value)
            continue
        if key == "diagnostics":
            storage_bundle[key] = _trim_diagnostics_for_storage(value)
            continue
        storage_bundle[key] = value
    if "today_sector_leaderboard" not in storage_bundle and "sector_summary" in storage_bundle:
        storage_bundle["today_sector_leaderboard"] = _trim_sector_summary_for_storage(storage_bundle["sector_summary"])
    if (
        "sector_representatives_display" not in storage_bundle
        and isinstance(source_bundle.get("sector_representatives"), pd.DataFrame)
        and not source_bundle.get("sector_representatives").empty
    ):
        storage_bundle["sector_representatives_display"] = _trim_representatives_display_for_storage(
            _build_sector_representatives_display_frame(
                source_bundle["sector_representatives"],
                today_sector_leaderboard=source_bundle.get("today_sector_leaderboard", pd.DataFrame()),
            )
        )
    if (
        "swing_candidates_1w_display" not in storage_bundle
        and isinstance(source_bundle.get("swing_candidates_1w"), pd.DataFrame)
        and not source_bundle.get("swing_candidates_1w").empty
    ):
        storage_bundle["swing_candidates_1w_display"] = _trim_swing_display_for_storage(
            _build_swing_candidate_display_frame(source_bundle["swing_candidates_1w"], horizon="1w"),
            horizon="1w",
        )
    if (
        "swing_candidates_1m_display" not in storage_bundle
        and isinstance(source_bundle.get("swing_candidates_1m"), pd.DataFrame)
        and not source_bundle.get("swing_candidates_1m").empty
    ):
        storage_bundle["swing_candidates_1m_display"] = _trim_swing_display_for_storage(
            _build_swing_candidate_display_frame(source_bundle["swing_candidates_1m"], horizon="1m"),
            horizon="1m",
        )
    if (
        "swing_candidates_3m_display" not in storage_bundle
        and isinstance(source_bundle.get("swing_candidates_3m"), pd.DataFrame)
        and not source_bundle.get("swing_candidates_3m").empty
    ):
        storage_bundle["swing_candidates_3m_display"] = _trim_swing_display_for_storage(
            _build_swing_candidate_display_frame(source_bundle["swing_candidates_3m"], horizon="3m"),
            horizon="3m",
        )
    return storage_bundle


def build_live_snapshot(
    mode: str,
    ranking_df: pd.DataFrame,
    industry_df: pd.DataFrame,
    board_df: pd.DataFrame,
    base_df: pd.DataFrame,
    now_ts: datetime,
    deep_watch_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    deep_watch_sector_frame = deep_watch_df.copy() if isinstance(deep_watch_df, pd.DataFrame) else pd.DataFrame()
    merged = base_df.merge(board_df, on="code", how="inner")
    if deep_watch_df is not None and not deep_watch_df.empty:
        deep_watch_meta_cols = [
            column
            for column in [
                "code",
                "was_in_selected50",
                "was_in_must_have",
                "deep_watch_selected_reason",
                "deep_watch_combined_priority",
                "selected_from_primary_or_supplemental",
            ]
            if column in deep_watch_df.columns
        ]
        if deep_watch_meta_cols:
            merged = merged.merge(deep_watch_df[deep_watch_meta_cols].drop_duplicates("code"), on="code", how="left")
    merged["was_in_selected50"] = merged.get("was_in_selected50", pd.Series(True, index=merged.index)).fillna(True).astype(bool)
    merged["was_in_must_have"] = merged.get("was_in_must_have", pd.Series(False, index=merged.index)).fillna(False).astype(bool)
    merged["deep_watch_selected_reason"] = merged.get("deep_watch_selected_reason", pd.Series(["priority_fill"] * len(merged), index=merged.index)).fillna("").replace("", "priority_fill")
    merged["deep_watch_combined_priority"] = _coerce_numeric(merged.get("deep_watch_combined_priority", pd.Series([0.0] * len(merged), index=merged.index))).fillna(0.0)
    merged = _annotate_non_corporate_products(merged)
    merged["live_price"] = _coerce_numeric(merged["CurrentPrice"])
    merged["prev_close"] = _coerce_numeric(merged["PrevClose"])
    merged["open_price"] = _coerce_numeric(merged["Open"])
    merged["high_price"] = _coerce_numeric(merged["High"])
    merged["low_price"] = _coerce_numeric(merged["Low"])
    merged["live_volume"] = _coerce_numeric(merged["Volume"]).fillna(_coerce_numeric(merged["volume_latest"]))
    merged["live_turnover"] = _coerce_numeric(merged["Turnover"]).fillna(_coerce_numeric(merged["turnover_latest"]))
    merged["live_price_time"] = merged["CurrentPriceTime"].astype(str)
    merged["live_ret_vs_prev_close"] = (merged["live_price"] / merged["prev_close"] - 1.0) * 100.0
    merged["live_ret_from_open"] = (merged["live_price"] / merged["open_price"] - 1.0) * 100.0
    merged["gap_pct"] = (merged["open_price"] / merged["prev_close"] - 1.0) * 100.0
    merged["live_volume_ratio_20d"] = _safe_ratio(merged["live_volume"], merged["avg_volume_20d"])
    merged["live_turnover_ratio_20d"] = _safe_ratio(merged["live_turnover"], merged["avg_turnover_20d"])
    merged["price_vs_ma20_pct"] = (_safe_ratio(merged["live_price"], merged["close_ma_20d"]) - 1.0) * 100.0
    merged["morning_strength"] = merged["live_ret_from_open"]
    merged["closing_strength"] = merged["live_ret_vs_prev_close"]
    merged["high_close_score"] = 1 - ((merged["high_price"] - merged["live_price"]) / merged["high_price"].replace(0, pd.NA))
    merged["total_score"] = 0.0
    for column, weight in MODE_SCORE_WEIGHTS[mode].items():
        merged["total_score"] += _score_percentile(merged[column]) * weight
    merged["focus_reason"] = merged.apply(lambda row: ", ".join(filter(None, [f"sector:{row.get('sector_name', '')}" if pd.notna(row.get("sector_name")) else "", "turnover_breakout" if float(row.get("live_turnover_ratio_20d", 0) or 0) >= 1.5 else "", "volume_breakout" if float(row.get("live_volume_ratio_20d", 0) or 0) >= 1.5 else "", "near_20d_high" if bool(row.get("is_near_52w_high")) else ""])) or "live_strength", axis=1)
    merged["nikkei_search"] = merged.apply(lambda row: _make_nikkei_search_link(str(row.get("name", "")), str(row.get("code", ""))), axis=1)
    merged["52w_flag"] = merged.apply(lambda row: "new_20d_high" if bool(row.get("is_new_52w_high")) else ("near_20d_high" if bool(row.get("is_near_52w_high")) else ""), axis=1)
    product_filter_diag: dict[str, Any] = {"non_corporate_products": {}}
    filtered_ranking_df = _exclude_non_corporate_products(ranking_df, product_filter_diag, context="today_market_scan")
    stock_base_df = _exclude_non_corporate_products(base_df, product_filter_diag, context="display_stock_base")
    stock_merged = _exclude_non_corporate_products(merged, product_filter_diag, context="live_stock_pool")
    for frame in [filtered_ranking_df, stock_base_df, stock_merged, deep_watch_sector_frame]:
        if not frame.empty and "sector_name" in frame.columns:
            frame["original_sector_name"] = frame.get("sector_name", pd.Series(dtype=str)).astype(str).map(str.strip)
            frame["normalized_sector_name"] = frame["original_sector_name"].map(_normalize_industry_key)
            frame["sector_name"] = frame["sector_name"].map(_normalize_industry_name)
    baseline_today_sector_leaderboard = _build_intraday_sector_leaderboard(
        mode,
        filtered_ranking_df,
        industry_df,
        stock_merged,
        stock_base_df,
        block_weights=INTRADAY_BLOCK_MODE_WEIGHTS_BASELINE,
        breadth_settings_map=INTRADAY_BREADTH_SLOT_SETTINGS_BASELINE,
        concentration_settings_map=INTRADAY_CONCENTRATION_PENALTY_SETTINGS_BASELINE,
    )
    today_sector_leaderboard = _build_intraday_sector_leaderboard(mode, filtered_ranking_df, industry_df, stock_merged, stock_base_df)
    baseline_sector_live_aggregate_audit = baseline_today_sector_leaderboard.attrs.get("sector_live_aggregate_audit", _empty_sector_live_aggregate_audit_frame())
    baseline_sector_live_aggregate_source_meta = dict(baseline_today_sector_leaderboard.attrs.get("sector_live_aggregate_source_meta", {}))
    sector_live_aggregate_audit = today_sector_leaderboard.attrs.get("sector_live_aggregate_audit", _empty_sector_live_aggregate_audit_frame())
    sector_live_aggregate_source_meta = dict(today_sector_leaderboard.attrs.get("sector_live_aggregate_source_meta", {}))
    baseline_today_sector_leaderboard = _sort_today_sector_leaderboard_for_display(baseline_today_sector_leaderboard)
    today_sector_leaderboard = _sort_today_sector_leaderboard_for_display(today_sector_leaderboard)
    baseline_today_sector_leaderboard.attrs["sector_live_aggregate_audit"] = baseline_sector_live_aggregate_audit
    baseline_today_sector_leaderboard.attrs["sector_live_aggregate_source_meta"] = baseline_sector_live_aggregate_source_meta
    today_sector_leaderboard.attrs["sector_live_aggregate_audit"] = sector_live_aggregate_audit
    today_sector_leaderboard.attrs["sector_live_aggregate_source_meta"] = sector_live_aggregate_source_meta
    baseline_representative_pool = _score_sector_center_candidates(stock_merged, baseline_today_sector_leaderboard, stock_base_df)
    representative_pool = _score_sector_center_candidates(stock_merged, today_sector_leaderboard, stock_base_df)
    baseline_sector_representatives = _build_sector_representatives(baseline_representative_pool)
    sector_representatives = _build_sector_representatives(representative_pool)
    leaderboard_sector_key_col = _sector_key_column(today_sector_leaderboard, stock_merged)
    selected50_codes_by_sector = _group_sector_codes(stock_merged, sector_col=leaderboard_sector_key_col)
    deep_watch_selected_count_by_sector = (
        deep_watch_sector_frame.groupby(leaderboard_sector_key_col)["code"].size().to_dict()
        if not deep_watch_sector_frame.empty and leaderboard_sector_key_col in deep_watch_sector_frame.columns
        else {}
    )
    deep_watch_observed_live_count_by_sector = (
        stock_merged.groupby(leaderboard_sector_key_col)["code"].size().to_dict()
        if not stock_merged.empty and leaderboard_sector_key_col in stock_merged.columns
        else {}
    )
    sector_center_candidate_pool = representative_pool[representative_pool.get("is_sector_center_candidate", pd.Series(False, index=representative_pool.index)).fillna(False)].copy() if not representative_pool.empty else pd.DataFrame()
    representative_candidate_codes_by_sector = _group_sector_codes(sector_center_candidate_pool, sector_col=leaderboard_sector_key_col)
    representative_pool_with_selection = representative_pool.copy()
    if not representative_pool_with_selection.empty:
        representative_pool_with_selection["code"] = representative_pool_with_selection["code"].astype(str)
    if not sector_representatives.empty:
        representative_pool_with_selection = representative_pool_with_selection.merge(
            sector_representatives[
                [
                    "code",
                    "rep_selected_reason",
                    "rep_fallback_reason",
                    "representative_quality_flag",
                    "representative_fallback_reason",
                ]
            ].drop_duplicates("code"),
            on="code",
            how="left",
            suffixes=("", "_selected"),
        )
        representative_pool_with_selection["rep_selected_reason"] = representative_pool_with_selection["rep_selected_reason"].where(
            representative_pool_with_selection["rep_selected_reason"].astype(str) != "",
            representative_pool_with_selection.get("rep_selected_reason_selected", ""),
        )
        representative_pool_with_selection["rep_fallback_reason"] = representative_pool_with_selection["rep_fallback_reason"].where(
            representative_pool_with_selection["rep_fallback_reason"].astype(str) != "",
            representative_pool_with_selection.get("rep_fallback_reason_selected", ""),
        )
        representative_pool_with_selection["representative_quality_flag"] = representative_pool_with_selection["representative_quality_flag"].where(
            representative_pool_with_selection["representative_quality_flag"].astype(str) != "excluded",
            representative_pool_with_selection.get("representative_quality_flag_selected", "excluded"),
        )
        representative_pool_with_selection["representative_fallback_reason"] = representative_pool_with_selection["representative_fallback_reason"].where(
            representative_pool_with_selection["representative_fallback_reason"].astype(str) != "",
            representative_pool_with_selection.get("representative_fallback_reason_selected", ""),
        )
    for column in [
        "rep_selected_reason",
        "rep_fallback_reason",
        "representative_quality_flag",
        "representative_fallback_reason",
    ]:
        if column in representative_pool_with_selection.columns:
            representative_pool_with_selection[column] = representative_pool_with_selection[column].where(
                representative_pool_with_selection[column].notna(),
                "",
            )
    positive_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["sector_positive_candidate_count"].max().to_dict()
        if not representative_pool_with_selection.empty
        else {}
    )
    negative_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["sector_negative_candidate_count"].max().to_dict()
        if not representative_pool_with_selection.empty
        else {}
    )
    rep_candidate_pool_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["code"].size().to_dict()
        if not representative_pool_with_selection.empty
        else {}
    )
    primary_candidate_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["selected_from_primary_or_supplemental"].apply(lambda s: int(pd.Series(s).fillna("primary").astype(str).ne("supplemental").sum())).to_dict()
        if not representative_pool_with_selection.empty and "selected_from_primary_or_supplemental" in representative_pool_with_selection.columns
        else {}
    )
    supplemental_candidate_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["selected_from_primary_or_supplemental"].apply(lambda s: int(pd.Series(s).fillna("").astype(str).eq("supplemental").sum())).to_dict()
        if not representative_pool_with_selection.empty and "selected_from_primary_or_supplemental" in representative_pool_with_selection.columns
        else {}
    )
    final_candidate_count_by_sector = rep_candidate_pool_count_by_sector
    representative_pool_coverage_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["representative_pool_coverage_rate"].max().to_dict()
        if not representative_pool_with_selection.empty and "representative_pool_coverage_rate" in representative_pool_with_selection.columns
        else {}
    )
    candidate_pool_warning_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["candidate_pool_warning"].apply(lambda s: _join_ui_fragments(*pd.Series(s).fillna("").astype(str).drop_duplicates().tolist())).to_dict()
        if not representative_pool_with_selection.empty and "candidate_pool_warning" in representative_pool_with_selection.columns
        else {}
    )
    candidate_pool_reason_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["candidate_pool_reason"].apply(lambda s: str(pd.Series(s).fillna("").astype(str).iloc[0] if len(s) else "")).to_dict()
        if not representative_pool_with_selection.empty and "candidate_pool_reason" in representative_pool_with_selection.columns
        else {}
    )
    rep_with_live_ret_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["live_ret_vs_prev_close"].apply(lambda s: int(pd.Series(s).notna().sum())).to_dict()
        if not representative_pool_with_selection.empty
        else {}
    )
    rep_with_current_price_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["live_price"].apply(lambda s: int(pd.Series(s).notna().sum())).to_dict()
        if not representative_pool_with_selection.empty
        else {}
    )
    rep_with_live_turnover_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["live_turnover"].apply(lambda s: int(pd.Series(s).notna().sum())).to_dict()
        if not representative_pool_with_selection.empty
        else {}
    )
    rep_pass_relative_leadership_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["rep_relative_leadership_pass"].apply(lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())).to_dict()
        if not representative_pool_with_selection.empty and "rep_relative_leadership_pass" in representative_pool_with_selection.columns
        else {}
    )
    rep_pass_centrality_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["rep_centrality_pass"].apply(lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())).to_dict()
        if not representative_pool_with_selection.empty and "rep_centrality_pass" in representative_pool_with_selection.columns
        else {}
    )
    rep_pass_sanity_count_by_sector = (
        representative_pool_with_selection.groupby(leaderboard_sector_key_col)["rep_sanity_pass"].apply(lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())).to_dict()
        if not representative_pool_with_selection.empty and "rep_sanity_pass" in representative_pool_with_selection.columns
        else {}
    )
    rep_selected_count_by_sector = (
        sector_representatives.groupby("sector_name")["code"].size().to_dict()
        if not sector_representatives.empty
        else {}
    )
    representative_trace_by_sector: dict[str, list[dict[str, Any]]] = {}
    representative_excluded_reason_by_sector: dict[str, dict[str, str]] = {}
    if not representative_pool_with_selection.empty:
        trace_source = representative_pool_with_selection.sort_values(
            [leaderboard_sector_key_col, "rep_score_total", "rep_score_centrality", "live_ret_vs_prev_close"],
            ascending=[True, False, False, False],
            kind="mergesort",
        ).copy()
        for sector_key, group in trace_source.groupby(leaderboard_sector_key_col, dropna=False):
            rows: list[dict[str, Any]] = []
            reason_map: dict[str, str] = {}
            for _, row in group.head(CENTER_LEADER_TRACE_LIMIT).iterrows():
                code = str(row.get("code", "") or "")
                selected_reason_raw = row.get("rep_selected_reason", "")
                fallback_reason_raw = row.get("rep_fallback_reason", "")
                excluded_reason_raw = row.get("rep_excluded_reason", "")
                selected_reason = "" if pd.isna(selected_reason_raw) else str(selected_reason_raw or "")
                fallback_reason = "" if pd.isna(fallback_reason_raw) else str(fallback_reason_raw or "")
                excluded_reason = "" if pd.isna(excluded_reason_raw) else str(excluded_reason_raw or "")
                if not excluded_reason and not selected_reason:
                    excluded_reason = "not_selected_after_center_leader_ranking" if bool(row.get("is_sector_center_candidate")) else "not_in_sector_center_candidates"
                if selected_reason:
                    reason_map[code] = selected_reason
                elif excluded_reason:
                    reason_map[code] = excluded_reason
                rows.append(
                    {
                        "code": code,
                        "name": str(row.get("name", "") or ""),
                        "was_in_selected50": bool(row.get("was_in_selected50", True)),
                        "was_in_must_have": bool(row.get("was_in_must_have", False)),
                        "rep_score_total": float(row.get("rep_score_total", 0.0) or 0.0) if pd.notna(row.get("rep_score_total")) else None,
                        "rep_score_centrality": float(row.get("rep_score_centrality", 0.0) or 0.0) if pd.notna(row.get("rep_score_centrality")) else None,
                        "rep_score_today_leadership": float(row.get("rep_score_today_leadership", 0.0) or 0.0) if pd.notna(row.get("rep_score_today_leadership")) else None,
                        "rep_score_sanity": float(row.get("rep_score_sanity", 0.0) or 0.0) if pd.notna(row.get("rep_score_sanity")) else None,
                        "rep_selected_reason": selected_reason,
                        "rep_excluded_reason": excluded_reason,
                        "rep_fallback_reason": fallback_reason,
                        "representative_quality_flag": "" if pd.isna(row.get("representative_quality_flag", "")) else str(row.get("representative_quality_flag", "") or ""),
                        "live_ret_vs_prev_close": float(row.get("live_ret_vs_prev_close", 0.0) or 0.0) if pd.notna(row.get("live_ret_vs_prev_close")) else None,
                        "live_ret_from_open": float(row.get("live_ret_from_open", 0.0) or 0.0) if pd.notna(row.get("live_ret_from_open")) else None,
                        "closing_strength": float(row.get("closing_strength_signal", 0.0) or 0.0) if pd.notna(row.get("closing_strength_signal")) else None,
                        "sector_contribution_full": float(row.get("sector_contribution_full", 0.0) or 0.0) if pd.notna(row.get("sector_contribution_full")) else None,
                        "contribution_rank_in_sector": int(row.get("contribution_rank_in_sector", 0) or 0) if pd.notna(row.get("contribution_rank_in_sector")) else None,
                        "turnover_rank_in_sector": int(row.get("turnover_rank_in_sector", 0) or 0) if pd.notna(row.get("turnover_rank_in_sector")) else None,
                        "liquidity_ok": bool(row.get("liquidity_ok", False)),
                        "exclude_spike": bool(row.get("exclude_spike", False)),
                        "exclude_spike_hard_reject": bool(row.get("exclude_spike_hard_reject", False)),
                        "exclude_spike_warning_only": bool(row.get("exclude_spike_warning_only", False)),
                        "spike_quality": str(row.get("spike_quality", "") or ""),
                        "poor_quality_spike": bool(row.get("poor_quality_spike", False)),
                        "material_supported_breakout": bool(row.get("material_supported_breakout", False)),
                        "breakout_support_reason": str(row.get("breakout_support_reason", "") or ""),
                        "sector_turnover_share": float(row.get("sector_turnover_share", row.get("stock_turnover_share_of_sector", 0.0)) or 0.0) if pd.notna(row.get("sector_turnover_share", row.get("stock_turnover_share_of_sector", pd.NA))) else None,
                    }
                )
            representative_trace_by_sector[str(sector_key or "")] = rows
            representative_excluded_reason_by_sector[str(sector_key or "")] = reason_map
    rep_top1 = sector_representatives[sector_representatives.get("representative_rank", pd.Series(dtype=int)).eq(1)].copy() if not sector_representatives.empty else pd.DataFrame()
    rep_map = rep_top1.set_index("sector_name")["name"] if not rep_top1.empty else pd.Series(dtype=str)
    representative_stocks_map = _build_representative_stocks_map(sector_representatives)
    leaders_map = (
        sector_representatives.sort_values(["sector_name", "representative_rank"])
        .groupby("sector_name")
        .apply(
            lambda group: " / ".join(
                _format_stock_name_with_marker(
                    "代表なし（当日中心株不在）"
                    if str(row.get("name", "") or "").strip() == "代表なし"
                    else row.get("name", ""),
                    marked=bool(row.get("earnings_today_announcement_flag", False)),
                )
                for _, row in group.head(3).iterrows()
                if str(row.get("name", "") or "").strip()
            )
        )
        if not sector_representatives.empty else pd.Series(dtype=str)
    )
    sector_today_earnings_map = (
        sector_representatives.groupby("sector_name")["earnings_today_announcement_flag"].apply(lambda s: bool(s.fillna(False).any()))
        if not sector_representatives.empty and "earnings_today_announcement_flag" in sector_representatives.columns else pd.Series(dtype=bool)
    )
    if not today_sector_leaderboard.empty:
        today_sector_leaderboard = today_sector_leaderboard.copy()
        today_sector_leaderboard["representative_stock"] = today_sector_leaderboard["sector_name"].map(leaders_map).fillna("")
        today_sector_leaderboard["representative_stocks"] = today_sector_leaderboard["sector_name"].map(representative_stocks_map).apply(lambda value: value if isinstance(value, list) else [])
        today_sector_leaderboard["leaders"] = today_sector_leaderboard["sector_name"].map(leaders_map).fillna(today_sector_leaderboard["representative_stock"])
        today_sector_leaderboard["sector_caution"] = [
            _append_tag_if(value, TODAY_EARNINGS_ANNOUNCEMENT_NOTE, enabled=bool(flag))
            for value, flag in zip(
                today_sector_leaderboard.get("sector_caution", pd.Series("", index=today_sector_leaderboard.index)),
                today_sector_leaderboard["sector_name"].map(sector_today_earnings_map).fillna(False),
            )
        ]
        today_sector_leaderboard["selected50_codes_in_sector"] = today_sector_leaderboard[leaderboard_sector_key_col].map(selected50_codes_by_sector).apply(lambda value: value if isinstance(value, list) else [])
        today_sector_leaderboard["sector_center_candidate_codes"] = today_sector_leaderboard[leaderboard_sector_key_col].map(representative_candidate_codes_by_sector).apply(lambda value: value if isinstance(value, list) else [])
        today_sector_leaderboard["representative_candidate_codes"] = today_sector_leaderboard[leaderboard_sector_key_col].map(representative_candidate_codes_by_sector).apply(lambda value: value if isinstance(value, list) else [])
        today_sector_leaderboard["sector_positive_candidate_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(positive_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["sector_negative_candidate_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(negative_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["rep_candidate_pool_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(rep_candidate_pool_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["primary_candidate_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(primary_candidate_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["supplemental_candidate_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(supplemental_candidate_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["final_candidate_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(final_candidate_count_by_sector).fillna(today_sector_leaderboard["rep_candidate_pool_count"]).astype(int)
        today_sector_leaderboard["representative_pool_coverage_rate"] = today_sector_leaderboard[leaderboard_sector_key_col].map(representative_pool_coverage_by_sector).fillna(0.0)
        today_sector_leaderboard["candidate_pool_warning"] = today_sector_leaderboard[leaderboard_sector_key_col].map(candidate_pool_warning_by_sector).fillna("")
        today_sector_leaderboard["candidate_pool_reason"] = today_sector_leaderboard[leaderboard_sector_key_col].map(candidate_pool_reason_by_sector).fillna("")
        today_sector_leaderboard["rep_with_live_ret_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(rep_with_live_ret_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["rep_with_current_price_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(rep_with_current_price_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["rep_with_live_turnover_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(rep_with_live_turnover_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["rep_pass_relative_leadership_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(rep_pass_relative_leadership_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["rep_pass_centrality_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(rep_pass_centrality_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["rep_pass_sanity_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(rep_pass_sanity_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["rep_selected_count"] = today_sector_leaderboard["sector_name"].map(rep_selected_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["deep_watch_selected_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(deep_watch_selected_count_by_sector).fillna(0).astype(int)
        today_sector_leaderboard["deep_watch_observed_live_count"] = today_sector_leaderboard[leaderboard_sector_key_col].map(deep_watch_observed_live_count_by_sector).fillna(0).astype(int)
        rep_pool_status = today_sector_leaderboard.apply(
            lambda row: _resolve_rep_candidate_pool_status(
                wide_scan_member_count=row.get("wide_scan_member_count", 0),
                deep_watch_selected_count=row.get("deep_watch_selected_count", 0),
                deep_watch_observed_live_count=row.get("deep_watch_observed_live_count", 0),
                rep_candidate_pool_count=row.get("rep_candidate_pool_count", 0),
            ),
            axis=1,
        )
        today_sector_leaderboard["rep_candidate_pool_status"] = rep_pool_status.apply(lambda value: value[0])
        today_sector_leaderboard["rep_candidate_pool_reason"] = rep_pool_status.apply(lambda value: value[1])
        today_sector_leaderboard = _add_today_sector_rank_audit_fields(
            today_sector_leaderboard,
            representative_pool_with_selection,
            sector_key_col=leaderboard_sector_key_col,
        )
        today_sector_leaderboard["representative_trace_top10"] = today_sector_leaderboard[leaderboard_sector_key_col].map(representative_trace_by_sector).apply(lambda value: value if isinstance(value, list) else [])
        today_sector_leaderboard["representative_excluded_reason_by_code"] = today_sector_leaderboard.apply(
            lambda row: {
                code: representative_excluded_reason_by_sector.get(str(row.get(leaderboard_sector_key_col, "") or ""), {}).get(
                    code,
                    "selected_representative" if code in {str(item.get('code', '') or '') for item in row.get("representative_stocks", []) if isinstance(item, dict)}
                    else "selected50_not_center_candidate"
                    if code in set(row.get("selected50_codes_in_sector", []))
                    else "not_in_selected50_live_pool"
                )
                for code in sorted(
                    set(row.get("wide_scan_member_codes", []))
                    | set(row.get("selected50_codes_in_sector", []))
                    | set(row.get("representative_candidate_codes", []))
                )
            },
            axis=1,
        )
        if not sector_representatives.empty:
            sector_rank_map = today_sector_leaderboard[["sector_name", "today_rank"]].drop_duplicates("sector_name").set_index("sector_name")["today_rank"]
            sector_representatives = sector_representatives.copy()
            sector_representatives["today_rank"] = sector_representatives["sector_name"].map(sector_rank_map)
    sector_live_aggregate_audit = _finalize_sector_live_aggregate_audit(
        sector_live_aggregate_audit,
        today_sector_leaderboard,
        sector_key_col=leaderboard_sector_key_col if leaderboard_sector_key_col in today_sector_leaderboard.columns else "normalized_sector_name",
    )
    sector_representatives_audit = _build_sector_representatives_audit_frame(
        today_sector_leaderboard,
        representative_pool_with_selection,
        sector_representatives,
        sector_key_col=leaderboard_sector_key_col,
    )
    sector_representatives_display = _build_sector_representatives_display_frame(
        sector_representatives,
        today_sector_leaderboard=today_sector_leaderboard,
    )
    persistence_reference_frames = [stock_base_df, sector_representatives, sector_representatives_display]
    persistence_earnings_lookup = _build_earnings_announcement_lookup(*persistence_reference_frames)
    persistence_security_reference_lookup = _build_security_reference_lookup(
        *persistence_reference_frames,
        earnings_announcement_lookup=persistence_earnings_lookup,
    )
    baseline_persistence_tables = _build_sector_persistence_tables(base_df, display_base_df=stock_base_df)
    persistence_tables = _build_sector_persistence_tables(base_df, display_base_df=stock_base_df)
    rep_key_map = pd.Series(dtype=str)
    leaders_key_map = pd.Series(dtype=str)
    if not today_sector_leaderboard.empty:
        leaderboard_key_lookup = today_sector_leaderboard[[leaderboard_sector_key_col, "sector_name"]].drop_duplicates()
        if not rep_top1.empty:
            rep_key_map = leaderboard_key_lookup.merge(
                rep_top1[["sector_name", "name"]].rename(columns={"sector_name": "leaderboard_sector_name"}),
                left_on="sector_name",
                right_on="leaderboard_sector_name",
                how="left",
            ).dropna(subset=["name"]).drop_duplicates(leaderboard_sector_key_col).set_index(leaderboard_sector_key_col)["name"]
        if not sector_representatives.empty:
            leaders_key_map = leaderboard_key_lookup.merge(
                sector_representatives.sort_values(["sector_name", "representative_rank"]).groupby("sector_name")["name"].apply(lambda s: " / ".join(s.head(3).astype(str))).reset_index().rename(columns={"sector_name": "leaderboard_sector_name"}),
                left_on="sector_name",
                right_on="leaderboard_sector_name",
                how="left",
            ).dropna(subset=["name"]).drop_duplicates(leaderboard_sector_key_col).set_index(leaderboard_sector_key_col)["name"]
    for key in ["1w", "1m", "3m"]:
        if not persistence_tables[key].empty:
            persistence_tables[key] = persistence_tables[key].copy()
            persistence_tables[key]["normalized_sector_name"] = persistence_tables[key].get("sector_name", pd.Series(dtype=str)).map(_normalize_industry_key)
            current_representative = persistence_tables[key]["representative_stock"] if "representative_stock" in persistence_tables[key].columns else pd.Series([""] * len(persistence_tables[key]), index=persistence_tables[key].index)
            persistence_tables[key]["representative_stock"] = current_representative.where(current_representative.astype(str).str.strip() != "", persistence_tables[key]["normalized_sector_name"].map(rep_key_map).fillna(persistence_tables[key]["sector_name"].map(rep_map).fillna("")))
            persistence_tables[key]["leaders"] = persistence_tables[key]["normalized_sector_name"].map(leaders_key_map).fillna(persistence_tables[key]["sector_name"].map(leaders_map).fillna(""))
        if not baseline_persistence_tables[key].empty:
            baseline_persistence_tables[key] = baseline_persistence_tables[key].copy()
            baseline_persistence_tables[key]["normalized_sector_name"] = baseline_persistence_tables[key].get("sector_name", pd.Series(dtype=str)).map(_normalize_industry_key)
            baseline_current_representative = baseline_persistence_tables[key]["representative_stock"] if "representative_stock" in baseline_persistence_tables[key].columns else pd.Series([""] * len(baseline_persistence_tables[key]), index=baseline_persistence_tables[key].index)
            baseline_persistence_tables[key]["representative_stock"] = baseline_current_representative.where(baseline_current_representative.astype(str).str.strip() != "", baseline_persistence_tables[key]["sector_name"].map(rep_map).fillna(""))
            baseline_persistence_tables[key]["leaders"] = baseline_persistence_tables[key]["sector_name"].map(leaders_map).fillna("")
    horizon_representative_diagnostics: dict[str, dict[str, Any]] = {}
    for key in ["1w", "1m", "3m"]:
        baseline_persistence_tables[key], _ = _resolve_persistence_representatives_for_storage(
            baseline_persistence_tables[key],
            horizon=key,
            earnings_announcement_lookup=persistence_earnings_lookup,
            security_reference_lookup=persistence_security_reference_lookup,
        )
        persistence_tables[key], horizon_representative_diagnostics[key] = _resolve_persistence_representatives_for_storage(
            persistence_tables[key],
            horizon=key,
            earnings_announcement_lookup=persistence_earnings_lookup,
            security_reference_lookup=persistence_security_reference_lookup,
        )
    baseline_swing_candidates = _build_swing_candidate_tables_v2(
        stock_merged,
        baseline_today_sector_leaderboard,
        baseline_persistence_tables,
        selection_config=SWING_SELECTION_CONFIG_BASELINE,
    )
    swing_candidates = _build_swing_candidate_tables_v2(stock_merged, today_sector_leaderboard, persistence_tables)
    swing_candidates_1w_display = _build_swing_candidate_display_frame(swing_candidates["1w"], horizon="1w")
    swing_candidates_1m_display = _build_swing_candidate_display_frame(swing_candidates["1m"], horizon="1m")
    swing_candidates_3m_display = _build_swing_candidate_display_frame(swing_candidates["3m"], horizon="3m")
    empty_state = {
        "today_sector_leaderboard": "" if not today_sector_leaderboard.empty else "intraday 条件を満たす本命セクターがありません。",
        "sector_persistence_1w": "" if not persistence_tables["1w"].empty else "TOPIX 比 1週継続性を出せるセクターがありません。",
        "sector_persistence_1m": "" if not persistence_tables["1m"].empty else "TOPIX 比 1か月継続性を出せるセクターがありません。",
        "sector_persistence_3m": "" if not persistence_tables["3m"].empty else "TOPIX 比 3か月継続性を出せるセクターがありません。",
        "swing_candidates_1w": "" if not swing_candidates["1w"].empty else str(swing_candidates.get("empty_reason_1w", "") or "1週間スイング候補の条件を満たす銘柄がありません。"),
        "swing_candidates_1m": "" if not swing_candidates["1m"].empty else str(swing_candidates.get("empty_reason_1m", "") or "1か月スイング候補の条件を満たす銘柄がありません。"),
        "swing_candidates_3m": "" if not swing_candidates["3m"].empty else str(swing_candidates.get("empty_reason_3m", "") or "3か月スイング候補の条件を満たす銘柄がありません。"),
        "swing_candidates_1w_display": "" if not swing_candidates_1w_display.empty else str(swing_candidates.get("empty_reason_1w", "") or "1週間スイング候補の条件を満たす銘柄がありません。"),
        "swing_candidates_1m_display": "" if not swing_candidates_1m_display.empty else str(swing_candidates.get("empty_reason_1m", "") or "1か月スイング候補の条件を満たす銘柄がありません。"),
        "swing_candidates_3m_display": "" if not swing_candidates_3m_display.empty else str(swing_candidates.get("empty_reason_3m", "") or "3か月スイング候補の条件を満たす銘柄がありません。"),
        "swing_buy_candidates_1w": "" if not swing_candidates["buy_1w"].empty else "1週間スイング買い候補はありません。",
        "swing_watch_candidates_1w": "" if not swing_candidates["watch_1w"].empty else "1週間スイング監視候補はありません。",
        "swing_buy_candidates_1m": "" if not swing_candidates["buy_1m"].empty else "1か月スイング買い候補はありません。",
        "swing_watch_candidates_1m": "" if not swing_candidates["watch_1m"].empty else "1か月スイング監視候補はありません。",
        "swing_buy_candidates_3m": "" if not swing_candidates["buy_3m"].empty else "3か月スイング買い候補はありません。",
        "swing_watch_candidates_3m": "" if not swing_candidates["watch_3m"].empty else "3か月スイング監視候補はありません。",
        "sector_representatives": "" if not sector_representatives.empty else "今日の本命セクターに紐づく代表銘柄を抽出できませんでした。",
        "sector_representatives_display": "" if not sector_representatives_display.empty else "今日の本命セクターに紐づく代表銘柄を抽出できませんでした。",
    }
    meta = build_snapshot_meta(mode=mode, generated_at=now_ts, source_profile="local_kabu_jq_yanoshin", includes_kabu=True)
    tuned_top_sector_names = today_sector_leaderboard.head(5)["sector_name"].astype(str).tolist() if not today_sector_leaderboard.empty else []
    filtered_wide_scan_total_count = int(filtered_ranking_df["code"].astype(str).drop_duplicates().shape[0]) if not filtered_ranking_df.empty and "code" in filtered_ranking_df.columns else 0
    filtered_ranking_union_count = int(filtered_ranking_df[filtered_ranking_df.get("ranking_union_member", pd.Series(False, index=filtered_ranking_df.index)).fillna(False)]["code"].astype(str).drop_duplicates().shape[0]) if not filtered_ranking_df.empty and "code" in filtered_ranking_df.columns else 0
    filtered_industry_basket_count = int(filtered_ranking_df[filtered_ranking_df.get("industry_basket_member", pd.Series(False, index=filtered_ranking_df.index)).fillna(False)]["code"].astype(str).drop_duplicates().shape[0]) if not filtered_ranking_df.empty and "code" in filtered_ranking_df.columns else 0
    filtered_sector_basket_counts = (
        filtered_ranking_df[filtered_ranking_df.get("industry_basket_member", pd.Series(False, index=filtered_ranking_df.index)).fillna(False)]
        .groupby("sector_name")["code"]
        .nunique()
        .to_dict()
        if not filtered_ranking_df.empty and "sector_name" in filtered_ranking_df.columns and "code" in filtered_ranking_df.columns
        else {}
    )
    leaderboard_market_scan_quality = today_sector_leaderboard.attrs.get("market_scan_quality", {}) if isinstance(today_sector_leaderboard, pd.DataFrame) else {}
    if isinstance(leaderboard_market_scan_quality, dict) and leaderboard_market_scan_quality:
        today_market_scan_quality = {
            "mode": str(leaderboard_market_scan_quality.get("mode", "") or ""),
            "reason": str(leaderboard_market_scan_quality.get("reason", "") or ""),
            "summary": str(leaderboard_market_scan_quality.get("summary", "") or ""),
            "ranking_union_count": int(leaderboard_market_scan_quality.get("ranking_union_count", 0) or 0),
            "sectors_with_ranking_confirmed_ge5": int(leaderboard_market_scan_quality.get("sectors_with_ranking_confirmed_ge5", 0) or 0),
            "sectors_with_source_breadth_ge2": int(leaderboard_market_scan_quality.get("sectors_with_source_breadth_ge2", 0) or 0),
        }
    else:
        today_market_scan_quality = _summarize_market_scan_quality(
            scan_df=filtered_ranking_df,
            sector_frame=today_sector_leaderboard,
            ranking_union_count=filtered_ranking_union_count,
            sector_basket_counts={str(key): int(value or 0) for key, value in filtered_sector_basket_counts.items()},
        )
    today_sector_scan_mode = str(today_market_scan_quality["mode"])
    industry_anchor_watch_before = _summarize_industry_anchor_positions(industry_df, baseline_today_sector_leaderboard, anchor_ranks=[2, 3, 4])
    industry_anchor_watch_after = _summarize_industry_anchor_positions(industry_df, today_sector_leaderboard, anchor_ranks=[2, 3, 4])
    industry_anchor_presence_watch = _summarize_industry_anchor_positions(industry_df, today_sector_leaderboard, anchor_ranks=[1, 3, 6, 7])
    tuning_compare = {
        "sectors_before": _summarize_sector_rank_table(baseline_today_sector_leaderboard, limit=10),
        "sectors_after": _summarize_sector_rank_table(today_sector_leaderboard, limit=10),
        "sector_rank_changes": _summarize_sector_rank_changes(baseline_today_sector_leaderboard, today_sector_leaderboard, limit=10),
        "industry_anchor_watch_before": industry_anchor_watch_before,
        "industry_anchor_watch_after": industry_anchor_watch_after,
        "representatives_before": _summarize_representative_table(baseline_sector_representatives, sector_names=tuned_top_sector_names, limit_sectors=5),
        "representatives_after": _summarize_representative_table(sector_representatives, sector_names=tuned_top_sector_names, limit_sectors=5),
        "swing_1w_before": _summarize_candidate_table(baseline_swing_candidates["1w"], rank_col="candidate_rank_1w", limit=5),
        "swing_1w_after": _summarize_candidate_table(swing_candidates["1w"], rank_col="candidate_rank_1w", limit=5),
        "swing_1m_before": _summarize_candidate_table(baseline_swing_candidates["1m"], rank_col="candidate_rank_1m", limit=5),
        "swing_1m_after": _summarize_candidate_table(swing_candidates["1m"], rank_col="candidate_rank_1m", limit=5),
        "swing_3m_before": _summarize_candidate_table(baseline_swing_candidates["3m"], rank_col="candidate_rank_3m", limit=5),
        "swing_3m_after": _summarize_candidate_table(swing_candidates["3m"], rank_col="candidate_rank_3m", limit=5),
    }
    representative_sector_trace = (
        today_sector_leaderboard[
            [
                column
                for column in [
                    "sector_name",
                    "original_sector_name",
                    "normalized_sector_name",
                    "sector_constituent_count_raw",
                    "sector_constituent_count_after_normalization",
                    "wide_scan_member_codes",
                    "ranking_confirmed_codes",
                    "selected50_codes_in_sector",
                    "sector_center_candidate_codes",
                    "representative_candidate_codes",
                    "sector_positive_candidate_count",
                    "sector_negative_candidate_count",
                    "representative_stocks",
                    "representative_excluded_reason_by_code",
                    "representative_trace_top10",
                ]
                if column in today_sector_leaderboard.columns
            ]
        ].head(10).to_dict(orient="records")
        if not today_sector_leaderboard.empty
        else []
    )
    representative_rejected_negative_while_positive_peer_exists_count = 0
    representative_rejected_large_drop_count = 0
    representative_fallback_blocked_by_hard_gate_count = 0
    representative_no_valid_today_count = 0
    if isinstance(representative_pool_with_selection, pd.DataFrame) and not representative_pool_with_selection.empty:
        exclusion_text = representative_pool_with_selection.get("rep_excluded_reason", pd.Series("", index=representative_pool_with_selection.index)).fillna("").astype(str)
        hard_text = representative_pool_with_selection.get("hard_block_reason", pd.Series("", index=representative_pool_with_selection.index)).fillna("").astype(str)
        representative_rejected_negative_while_positive_peer_exists_count = int(exclusion_text.str.contains("negative_live_ret_while_positive_peer_exists", regex=False).sum())
        representative_rejected_large_drop_count = int(exclusion_text.str.contains("today_drop_lte_-3|today_hard_drop_lte_-5", regex=True).sum())
        representative_fallback_blocked_by_hard_gate_count = int(hard_text.str.contains("today_hard_drop_lte_-5", regex=False).sum())
    if isinstance(sector_representatives, pd.DataFrame) and not sector_representatives.empty and "representative_quality_flag" in sector_representatives.columns:
        representative_no_valid_today_count = int(sector_representatives["representative_quality_flag"].fillna("").astype(str).eq("no_valid_today_representative").sum())
    return {
        "meta": meta,
        "sector_summary": today_sector_leaderboard,
        "today_sector_summary": today_sector_leaderboard,
        "today_sector_leaderboard": today_sector_leaderboard,
        "weekly_sector_summary": persistence_tables["1w"],
        "monthly_sector_summary": persistence_tables["1m"],
        "sector_persistence_1w": persistence_tables["1w"],
        "sector_persistence_1m": persistence_tables["1m"],
        "sector_persistence_3m": persistence_tables["3m"],
        "leaders_by_sector": sector_representatives,
        "center_stocks": sector_representatives,
        "sector_representatives": sector_representatives,
        "sector_representatives_display": sector_representatives_display,
        "sector_representatives_audit": sector_representatives_audit,
        "sector_live_aggregate_audit": sector_live_aggregate_audit,
        "focus_candidates": swing_candidates["1w"],
        "watch_candidates": swing_candidates["1w"],
        "buy_candidates": swing_candidates["1m"],
        "swing_candidates_1w": swing_candidates["1w"],
        "swing_candidates_1m": swing_candidates["1m"],
        "swing_candidates_3m": swing_candidates["3m"],
        "swing_candidates_1w_display": swing_candidates_1w_display,
        "swing_candidates_1m_display": swing_candidates_1m_display,
        "swing_candidates_3m_display": swing_candidates_3m_display,
        "swing_buy_candidates_1w": swing_candidates["buy_1w"],
        "swing_watch_candidates_1w": swing_candidates["watch_1w"],
        "swing_buy_candidates_1m": swing_candidates["buy_1m"],
        "swing_watch_candidates_1m": swing_candidates["watch_1m"],
        "swing_buy_candidates_3m": swing_candidates["buy_3m"],
        "swing_watch_candidates_3m": swing_candidates["watch_3m"],
        "swing_1w_candidates_audit": swing_candidates["audit_1w"],
        "swing_1m_candidates_audit": swing_candidates["audit_1m"],
        "swing_3m_candidates_audit": swing_candidates["audit_3m"],
        "empty_reasons": empty_state,
        "diagnostics": {
            "mode": mode,
            "generated_at": meta["generated_at"],
            "watch_candidate_count": int(len(swing_candidates["1w"])),
            "buy_candidate_count": int(len(swing_candidates["1m"])),
            "swing_3m_candidate_count": int(len(swing_candidates["3m"])),
            "center_stock_count": int(len(sector_representatives)),
            "horizon_representative_source_1w": str(horizon_representative_diagnostics.get("1w", {}).get("source", "") or ""),
            "horizon_representative_source_1m": str(horizon_representative_diagnostics.get("1m", {}).get("source", "") or ""),
            "horizon_representative_source_3m": str(horizon_representative_diagnostics.get("3m", {}).get("source", "") or ""),
            "horizon_representative_code_resolved_1w": int(horizon_representative_diagnostics.get("1w", {}).get("resolved_count", 0) or 0),
            "horizon_representative_code_resolved_1m": int(horizon_representative_diagnostics.get("1m", {}).get("resolved_count", 0) or 0),
            "horizon_representative_code_resolved_3m": int(horizon_representative_diagnostics.get("3m", {}).get("resolved_count", 0) or 0),
            "horizon_representative_code_unresolved_1w": int(horizon_representative_diagnostics.get("1w", {}).get("unresolved_count", 0) or 0),
            "horizon_representative_code_unresolved_1m": int(horizon_representative_diagnostics.get("1m", {}).get("unresolved_count", 0) or 0),
            "horizon_representative_code_unresolved_3m": int(horizon_representative_diagnostics.get("3m", {}).get("unresolved_count", 0) or 0),
            "horizon_representative_earnings_date_count_1w": int(horizon_representative_diagnostics.get("1w", {}).get("earnings_date_count", 0) or 0),
            "horizon_representative_earnings_date_count_1m": int(horizon_representative_diagnostics.get("1m", {}).get("earnings_date_count", 0) or 0),
            "horizon_representative_earnings_date_count_3m": int(horizon_representative_diagnostics.get("3m", {}).get("earnings_date_count", 0) or 0),
            "horizon_representative_unresolved_samples_1w": horizon_representative_diagnostics.get("1w", {}).get("unresolved_samples", []),
            "horizon_representative_unresolved_samples_1m": horizon_representative_diagnostics.get("1m", {}).get("unresolved_samples", []),
            "horizon_representative_unresolved_samples_3m": horizon_representative_diagnostics.get("3m", {}).get("unresolved_samples", []),
            "sector_live_aggregate_source_of_truth": sector_live_aggregate_source_meta or {"source_frame": "stock_merged_observed_live_rows"},
            "sector_live_aggregate_fail_closed_rule": "live_aggregate_status must be observed before any live aggregate is eligible for score usage",
            "representative_candidate_pool_basis": "representative_pool is built from stock_merged = base_df inner board_df; board_df starts with deep_watch primary names and adds representative_supplemental_lane center/liquidity candidates before live board enrichment",
            "ranking_candidate_count": int(len(filtered_ranking_df)),
            "sector_summary_scope": "full_tse_industry_universe_left_join_wide_scan_adjustments",
            "today_sector_population_basis": "industry_up_full_universe_then_left_join_wide_scan_sector_aggregates",
            "today_sector_population_counts": {
                "industry_universe_count": int(industry_df["sector_name"].astype(str).map(_normalize_industry_name).str.strip().replace("", pd.NA).dropna().nunique()) if not industry_df.empty and "sector_name" in industry_df.columns else 0,
                "wide_scan_aggregated_sector_count": int(_coerce_numeric(today_sector_leaderboard.get("wide_scan_member_count", pd.Series(dtype="float64"))).fillna(0.0).gt(0.0).sum()) if not today_sector_leaderboard.empty else 0,
                "sector_summary_before_filter_count": int(today_sector_leaderboard.get("present_in_sector_summary_before_filter", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not today_sector_leaderboard.empty else 0,
                "today_display_universe_count": int(today_sector_leaderboard.get("present_in_today_display_universe", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not today_sector_leaderboard.empty else 0,
                "display_topn_source_count": int(len(today_sector_leaderboard)),
                "leaderboard_sector_count": int(len(today_sector_leaderboard)),
                "scan_sector_count": int(filtered_ranking_df.get("sector_name", pd.Series(dtype=str)).astype(str).str.strip().replace("", pd.NA).dropna().map(_normalize_industry_name).nunique()) if not filtered_ranking_df.empty else 0,
                "scan_zero_sector_count": int(_coerce_numeric(today_sector_leaderboard.get("scan_member_count", pd.Series(dtype="float64"))).fillna(0.0).eq(0.0).sum()) if not today_sector_leaderboard.empty else 0,
            },
            "wide_scan_total_count": filtered_wide_scan_total_count,
            "ranking_union_count": int(today_market_scan_quality["ranking_union_count"]),
            "industry_basket_count": filtered_industry_basket_count,
            "sector_basket_counts": {str(key): int(value or 0) for key, value in filtered_sector_basket_counts.items()},
            "sectors_with_ranking_confirmed_ge5": int(today_market_scan_quality["sectors_with_ranking_confirmed_ge5"]),
            "sectors_with_source_breadth_ge2": int(today_market_scan_quality["sectors_with_source_breadth_ge2"]),
            "wide_scan_mode": today_sector_scan_mode,
            "today_rank_mode": today_sector_scan_mode,
            "rank_mode_reason": str(today_market_scan_quality["reason"]),
            "market_scan_quality_summary": str(today_market_scan_quality["summary"]),
            "today_upshift_block_rules": [
                "ランキング裏付けなし",
                "ランキング裏付け極少",
                "ランキング裏付け薄い",
                "source偏りあり",
                "wide scan母数不足",
            ],
            "breadth_scope": "wide_scan_based_caution_without_live_sector_inputs",
            "today_sector_primary_rank_column": "today_display_rank",
            "today_sector_display_rank_column": "today_display_rank",
            "today_top_sectors_basis": "today_sector_leaderboard_sorted_by_final_dense_display_rank_after_industry_anchor_overlay",
            "today_rank_rule": "today_display_rank_is_dense_final_rank_and_today_rank_equals_today_display_rank",
            "today_rank_absolute_constraint": "anchor_only_days_keep_industry_anchor_order; anchored_overlay_days_allow_max_upshift<=2_and_max_downshift<=2",
            "today_display_universe_rule": "retain full live industry universe in collector; topN must be sliced only after today_display_rank is assigned",
            "sector_alias_normalization_basis": "collector_uses_normalized_sector_name_for_joins; display_sector_name_keeps_industry_table_label",
            "representative_rejected_negative_while_positive_peer_exists_count": representative_rejected_negative_while_positive_peer_exists_count,
            "representative_rejected_large_drop_count": representative_rejected_large_drop_count,
            "representative_fallback_blocked_by_hard_gate_count": representative_fallback_blocked_by_hard_gate_count,
            "representative_no_valid_today_count": representative_no_valid_today_count,
            "representative_today_hard_gate_rule": "fallback never bypasses liquidity/poor_quality_spike/event-like large drop/relative weakness hard gates; exclude_spike alone is warning unless spike quality is poor",
            "representative_sector_trace_top10": representative_sector_trace,
            "today_sector_removed_live_inputs": [
                "median_live_ret_norm",
                "turnover_ratio_median_norm",
                "live_turnover_total_norm",
                "breadth_up_rate",
                "breadth_balance",
                "breadth_penalty",
                "concentration_penalty",
                "leader_concentration_share",
            ],
            "today_sector_participation_inputs": [
                "ranking_source_breadth_ex_basket",
                "ranking_confirmed_count_norm",
                "ranking_confirmed_share_of_sector",
            ],
            "scan_sample_warning_rules": {
                "items": ["wide_scan_member_count", "wide_scan_coverage"],
                "no_scan": {
                    "wide_scan_member_count_eq": 0,
                    "label": "wide scan母数不足",
                },
                "critical": {
                    "wide_scan_member_count_lt": int(INTRADAY_SCAN_SAMPLE_WARNING_RULES["critical_count"]),
                    "label": "wide scan母数不足",
                },
                "warn": {
                    "wide_scan_member_count_lt": int(INTRADAY_SCAN_SAMPLE_WARNING_RULES["warn_count"]),
                    "wide_scan_coverage_lt": float(INTRADAY_SCAN_SAMPLE_WARNING_RULES["warn_coverage"]),
                    "label": "wide scan母数不足",
                },
                "thin": {
                    "wide_scan_member_count_lt": int(INTRADAY_SCAN_SAMPLE_WARNING_RULES["thin_count"]),
                    "wide_scan_coverage_lt": float(INTRADAY_SCAN_SAMPLE_WARNING_RULES["thin_coverage"]),
                    "label": "wide scan母数不足",
                },
                "note": "today sector caution prioritizes ranking_confirmed support; wide_scan warnings are baseline-only context",
            },
            "today_sector_rank_tether": {
                "anchor": "industry_up",
                "base_shift": int(INTRADAY_INDUSTRY_RANK_TETHER["base_shift"]),
                "strong_shift": int(INTRADAY_INDUSTRY_RANK_TETHER["strong_shift"]),
                "very_strong_shift": int(INTRADAY_INDUSTRY_RANK_TETHER["very_strong_shift"]),
                "max_downshift": int(INTRADAY_INDUSTRY_RANK_TETHER["max_downshift"]),
                "rule": "default_upshift_0; plus1_requires_confirmed5_breadth2_and_strong_price; plus2_requires_confirmed8_breadth3_strong_price_flow_and_no_severe_caution; plus3_removed",
            },
            "industry_anchor_watch_before": industry_anchor_watch_before,
            "industry_anchor_watch_after": industry_anchor_watch_after,
            "industry_anchor_presence_watch": industry_anchor_presence_watch,
            "top10_before_after_compare": {
                "before": tuning_compare["sectors_before"],
                "after": tuning_compare["sectors_after"],
                "changes": tuning_compare["sector_rank_changes"],
            },
            "includes_kabu": True,
            "non_corporate_products": product_filter_diag["non_corporate_products"],
            "tuning_compare": tuning_compare,
            "swing_candidates_1w_source_of_truth": "stock_merged",
            "swing_candidates_1m_source_of_truth": "stock_merged",
            "swing_candidates_1w_empty_status": str(swing_candidates.get("empty_status_1w", "") or ""),
            "swing_candidates_1m_empty_status": str(swing_candidates.get("empty_status_1m", "") or ""),
        },
    }


def write_snapshot_bundle(bundle: dict[str, Any], settings: dict[str, Any], *, write_drive: bool = False) -> dict[str, str]:
    storage_bundle = _bundle_for_storage(bundle)
    markdown_text = bundle_to_markdown(bundle)
    result = write_snapshot_bundle_to_store(
        mode=str(bundle["meta"]["mode"]),
        generated_at=str(bundle["meta"]["generated_at"]),
        json_text=bundle_to_json_text(storage_bundle),
        markdown_text=markdown_text,
        settings=settings,
        root_dir=ROOT_DIR,
        write_drive=write_drive,
    )
    for path in result.paths.values():
        logger.info("snapshot write path %s", path)
    return {
        **result.paths,
        "source_label": result.source_label,
        "backend_name": result.backend_name,
        "warning_message": result.warning_message,
    }


def load_saved_snapshot(
    mode: str,
    settings: dict[str, Any] | None = None,
    *,
    allow_stale_content: bool = True,
) -> dict[str, Any]:
    settings = settings or get_settings()
    if _is_streamlit_cloud():
        cached_payload = _load_saved_snapshot_payload_from_github(mode, settings)
    else:
        snapshot_path = _snapshot_json_path(mode, settings)
        cached_payload = _load_saved_snapshot_payload_cached(mode, str(snapshot_path), _snapshot_mtime_ns(snapshot_path))
    payload = cached_payload["payload"]
    meta = normalize_snapshot_meta(payload.get("meta", {}))
    snapshot_guard = evaluate_snapshot_guard(mode, meta)
    compat_notes: list[str] = []
    today_source_key = "today_sector_leaderboard"
    if "today_sector_leaderboard" in payload:
        today_sector_leaderboard = pd.DataFrame(payload.get("today_sector_leaderboard", []))
    elif "today_sector_summary" in payload:
        today_source_key = "today_sector_summary"
        compat_notes.append("today表は旧 snapshot 互換表示です。today_sector_summary を表示しています。")
        today_sector_leaderboard = pd.DataFrame(payload.get("today_sector_summary", []))
    else:
        today_source_key = "sector_summary"
        compat_notes.append("today表は旧 snapshot 互換表示です。sector_summary を表示しています。")
        today_sector_leaderboard = pd.DataFrame(payload.get("sector_summary", []))
    weekly_sector_summary = pd.DataFrame(payload.get("sector_persistence_1w", payload.get("weekly_sector_summary", [])))
    monthly_sector_summary = pd.DataFrame(payload.get("sector_persistence_1m", payload.get("monthly_sector_summary", [])))
    sector_persistence_3m = pd.DataFrame(payload.get("sector_persistence_3m", []))
    sector_representatives = pd.DataFrame(payload.get("sector_representatives", payload.get("center_stocks", payload.get("leaders_by_sector", []))))
    sector_representatives_display = pd.DataFrame(payload.get("sector_representatives_display", []))
    sector_representatives_audit = pd.DataFrame(payload.get("sector_representatives_audit", []))
    sector_live_aggregate_audit = pd.DataFrame(payload.get("sector_live_aggregate_audit", []))
    if sector_representatives_display.empty and not sector_representatives.empty:
        compat_notes.append("代表銘柄表は旧 snapshot 互換表示です。raw sector_representatives から表示列だけ抽出しています。")
        sector_representatives_display = _build_sector_representatives_display_frame(
            sector_representatives,
            today_sector_leaderboard=today_sector_leaderboard,
        )
    swing_candidates_1w = pd.DataFrame(payload.get("swing_candidates_1w", payload.get("watch_candidates", payload.get("focus_candidates", []))))
    swing_candidates_1m = pd.DataFrame(payload.get("swing_candidates_1m", payload.get("buy_candidates", [])))
    swing_candidates_3m = pd.DataFrame(payload.get("swing_candidates_3m", []))
    swing_candidates_1w_display = pd.DataFrame(payload.get("swing_candidates_1w_display", []))
    swing_candidates_1m_display = pd.DataFrame(payload.get("swing_candidates_1m_display", []))
    swing_candidates_3m_display = pd.DataFrame(payload.get("swing_candidates_3m_display", []))
    swing_buy_candidates_1w = pd.DataFrame(payload.get("swing_buy_candidates_1w", []))
    swing_watch_candidates_1w = pd.DataFrame(payload.get("swing_watch_candidates_1w", []))
    swing_buy_candidates_1m = pd.DataFrame(payload.get("swing_buy_candidates_1m", []))
    swing_watch_candidates_1m = pd.DataFrame(payload.get("swing_watch_candidates_1m", []))
    swing_buy_candidates_3m = pd.DataFrame(payload.get("swing_buy_candidates_3m", []))
    swing_watch_candidates_3m = pd.DataFrame(payload.get("swing_watch_candidates_3m", []))
    swing_1w_candidates_audit = pd.DataFrame(payload.get("swing_1w_candidates_audit", []))
    swing_1m_candidates_audit = pd.DataFrame(payload.get("swing_1m_candidates_audit", []))
    swing_3m_candidates_audit = pd.DataFrame(payload.get("swing_3m_candidates_audit", []))
    if swing_candidates_1w_display.empty and not swing_candidates_1w.empty:
        compat_notes.append("1週間候補表は旧 snapshot 互換表示です。raw swing_candidates_1w から表示列だけ抽出しています。")
        swing_candidates_1w_display = _build_swing_candidate_display_frame(swing_candidates_1w, horizon="1w")
    if swing_candidates_1m_display.empty and not swing_candidates_1m.empty:
        compat_notes.append("1か月候補表は旧 snapshot 互換表示です。raw swing_candidates_1m から表示列だけ抽出しています。")
        swing_candidates_1m_display = _build_swing_candidate_display_frame(swing_candidates_1m, horizon="1m")
    if swing_candidates_3m_display.empty and not swing_candidates_3m.empty:
        compat_notes.append("3か月候補表は旧 snapshot 互換表示です。raw swing_candidates_3m から表示列だけ抽出しています。")
        swing_candidates_3m_display = _build_swing_candidate_display_frame(swing_candidates_3m, horizon="3m")
    if bool(snapshot_guard.get("is_stale")) and not allow_stale_content:
        stale_reason = str(snapshot_guard.get("reason", "")).strip()
        today_sector_leaderboard = pd.DataFrame()
        weekly_sector_summary = pd.DataFrame()
        monthly_sector_summary = pd.DataFrame()
        sector_persistence_3m = pd.DataFrame()
        sector_representatives = pd.DataFrame()
        sector_representatives_display = pd.DataFrame()
        sector_representatives_audit = pd.DataFrame()
        sector_live_aggregate_audit = pd.DataFrame()
        swing_candidates_1w = pd.DataFrame()
        swing_candidates_1m = pd.DataFrame()
        swing_candidates_3m = pd.DataFrame()
        swing_candidates_1w_display = pd.DataFrame()
        swing_candidates_1m_display = pd.DataFrame()
        swing_candidates_3m_display = pd.DataFrame()
        swing_buy_candidates_1w = pd.DataFrame()
        swing_watch_candidates_1w = pd.DataFrame()
        swing_buy_candidates_1m = pd.DataFrame()
        swing_watch_candidates_1m = pd.DataFrame()
        swing_buy_candidates_3m = pd.DataFrame()
        swing_watch_candidates_3m = pd.DataFrame()
        swing_1w_candidates_audit = pd.DataFrame()
        swing_1m_candidates_audit = pd.DataFrame()
        swing_3m_candidates_audit = pd.DataFrame()
        payload_empty_reasons = dict(payload.get("empty_reasons", {}))
        for key in [
            "today_sector_leaderboard",
            "sector_persistence_1w",
            "sector_persistence_1m",
            "sector_persistence_3m",
            "sector_representatives",
            "sector_representatives_display",
            "swing_candidates_1w",
            "swing_candidates_1m",
            "swing_candidates_3m",
            "swing_candidates_1w_display",
            "swing_candidates_1m_display",
            "swing_candidates_3m_display",
            "swing_buy_candidates_1w",
            "swing_watch_candidates_1w",
            "swing_buy_candidates_1m",
            "swing_watch_candidates_1m",
            "swing_buy_candidates_3m",
            "swing_watch_candidates_3m",
            "weekly_sector_summary",
            "monthly_sector_summary",
            "center_stocks",
            "watch_candidates",
            "buy_candidates",
        ]:
            payload_empty_reasons[key] = stale_reason
        payload["empty_reasons"] = payload_empty_reasons
    for frame in [swing_candidates_1w, swing_candidates_1m, swing_candidates_3m, sector_representatives, sector_representatives_display]:
        if not frame.empty and "name" in frame.columns:
            frame["nikkei_search"] = frame.apply(lambda row: _make_nikkei_search_link(str(row.get("name", "")), str(row.get("code", ""))), axis=1)
    return {
        "meta": meta,
        "sector_summary": today_sector_leaderboard,
        "today_sector_summary": today_sector_leaderboard,
        "weekly_sector_summary": weekly_sector_summary,
        "monthly_sector_summary": monthly_sector_summary,
        "today_sector_leaderboard": today_sector_leaderboard,
        "sector_persistence_1w": weekly_sector_summary,
        "sector_persistence_1m": monthly_sector_summary,
        "sector_persistence_3m": sector_persistence_3m,
        "leaders_by_sector": sector_representatives,
        "center_stocks": sector_representatives,
        "sector_representatives": sector_representatives,
        "sector_representatives_display": sector_representatives_display,
        "sector_representatives_audit": sector_representatives_audit,
        "sector_live_aggregate_audit": sector_live_aggregate_audit,
        "focus_candidates": swing_candidates_1w,
        "watch_candidates": swing_candidates_1w,
        "buy_candidates": swing_candidates_1m,
        "swing_candidates_1w": swing_candidates_1w,
        "swing_candidates_1m": swing_candidates_1m,
        "swing_candidates_3m": swing_candidates_3m,
        "swing_candidates_1w_display": swing_candidates_1w_display,
        "swing_candidates_1m_display": swing_candidates_1m_display,
        "swing_candidates_3m_display": swing_candidates_3m_display,
        "swing_buy_candidates_1w": swing_buy_candidates_1w if not swing_buy_candidates_1w.empty else swing_candidates_1w[swing_candidates_1w.get("entry_fit", pd.Series(dtype=str)).eq("買い候補")] if not swing_candidates_1w.empty and "entry_fit" in swing_candidates_1w.columns else pd.DataFrame(),
        "swing_watch_candidates_1w": swing_watch_candidates_1w if not swing_watch_candidates_1w.empty else swing_candidates_1w[swing_candidates_1w.get("entry_fit", pd.Series(dtype=str)).eq("監視候補")] if not swing_candidates_1w.empty and "entry_fit" in swing_candidates_1w.columns else pd.DataFrame(),
        "swing_buy_candidates_1m": swing_buy_candidates_1m if not swing_buy_candidates_1m.empty else swing_candidates_1m[swing_candidates_1m.get("entry_fit", pd.Series(dtype=str)).eq("買い候補")] if not swing_candidates_1m.empty and "entry_fit" in swing_candidates_1m.columns else pd.DataFrame(),
        "swing_watch_candidates_1m": swing_watch_candidates_1m if not swing_watch_candidates_1m.empty else swing_candidates_1m[swing_candidates_1m.get("entry_fit", pd.Series(dtype=str)).eq("監視候補")] if not swing_candidates_1m.empty and "entry_fit" in swing_candidates_1m.columns else pd.DataFrame(),
        "swing_buy_candidates_3m": swing_buy_candidates_3m if not swing_buy_candidates_3m.empty else swing_candidates_3m[swing_candidates_3m.get("entry_fit", pd.Series(dtype=str)).eq("買い候補")] if not swing_candidates_3m.empty and "entry_fit" in swing_candidates_3m.columns else pd.DataFrame(),
        "swing_watch_candidates_3m": swing_watch_candidates_3m if not swing_watch_candidates_3m.empty else swing_candidates_3m[swing_candidates_3m.get("entry_fit", pd.Series(dtype=str)).eq("監視候補")] if not swing_candidates_3m.empty and "entry_fit" in swing_candidates_3m.columns else pd.DataFrame(),
        "swing_1w_candidates_audit": swing_1w_candidates_audit,
        "swing_1m_candidates_audit": swing_1m_candidates_audit,
        "swing_3m_candidates_audit": swing_3m_candidates_audit,
        "empty_reasons": payload.get("empty_reasons", {}),
        "diagnostics": payload.get("diagnostics", {}),
        "snapshot_guard": snapshot_guard,
        "snapshot_compatibility_notes": compat_notes,
        "today_sector_source_key": today_source_key,
        "paths": cached_payload["paths"],
        "snapshot_source_label": cached_payload["source_label"],
        "snapshot_backend_name": cached_payload["backend_name"],
        "snapshot_warning_message": "\n".join(filter(None, [str(cached_payload["warning_message"] or "").strip(), str(snapshot_guard.get("reason", "")).strip()])),
    }


def run_cli(mode: str, write_drive: bool = False, fast_check: bool = False) -> dict[str, Any]:
    logger.info("run_cli start mode=%s fast_check=%s write_drive=%s", mode, fast_check, write_drive)
    settings = get_settings()
    try:
        get_api_key(settings)
        with safe_spinner("Building daily base", enabled=False):
            # Even in now mode, the kabu live snapshot is built on top of the J-Quants daily base.
            base_df, base_meta = build_daily_base_data(fast_check=fast_check)
        with safe_spinner("Building market scan", enabled=False):
            token = kabu_get_token(settings)
            ranking_df, industry_df, ranking_diag = build_market_scan_universe(base_df, settings, token)
        deep_watch_df, deep_watch_diag = select_deep_watch_universe(ranking_df, industry_df, base_df, settings, mode)
        board_df, board_diag = enrich_with_board_snapshot(deep_watch_df, base_df, settings, token, mode=mode)
        bundle = build_live_snapshot(mode, ranking_df, industry_df, board_df, base_df, datetime.now(timezone.utc), deep_watch_df=deep_watch_df)
        bundle["meta"]["earnings_forward_buffer_available"] = bool(base_meta.get("earnings_forward_buffer_available", False))
        bundle["meta"]["earnings_forward_buffer_reason"] = str(base_meta.get("earnings_forward_buffer_reason", "") or "")
        bundle["meta"]["earnings_dataset_status"] = str(base_meta.get("earnings_dataset_status", "") or "")
        bundle["meta"]["earnings_rows_future_window"] = int(base_meta.get("earnings_rows_future_window", 0) or 0)
        bundle["meta"]["earnings_rows_raw"] = int(base_meta.get("earnings_rows_raw", 0) or 0)
        bundle["meta"]["earnings_announcement_status"] = str(base_meta.get("earnings_announcement_status", "") or "")
        bundle["meta"]["earnings_announcement_rows_target_date"] = int(base_meta.get("earnings_announcement_rows_target_date", 0) or 0)
        bundle["meta"]["earnings_announcement_target_date"] = str(base_meta.get("earnings_announcement_target_date", "") or "")
        bundle["meta"]["earnings_announcement_source_path"] = str(base_meta.get("earnings_announcement_source_path", "") or "")
        bundle["meta"]["earnings_announcement_source"] = str(base_meta.get("earnings_announcement_source", "") or "")
        bundle["meta"]["earnings_announcement_source_policy"] = "edinetdb_calendar_only"
        bundle["meta"]["earnings_announcement_cache_path"] = str(base_meta.get("earnings_announcement_cache_path", "") or "")
        bundle["meta"]["earnings_announcement_cache_hit"] = bool(base_meta.get("earnings_announcement_cache_hit", False))
        bundle["meta"]["earnings_announcement_failure_reason"] = str(base_meta.get("earnings_announcement_failure_reason", "") or "")
        bundle["meta"]["earnings_announcement_jquants_fallback_used"] = bool(base_meta.get("earnings_announcement_jquants_fallback_used", False))
        bundle["meta"]["earnings_announcement_uses_jquants"] = False
        bundle["meta"]["edinetdb_calendar_status"] = str(base_meta.get("edinetdb_calendar_status", "") or "")
        bundle["meta"]["edinetdb_calendar_source"] = str(base_meta.get("edinetdb_calendar_source", "") or "")
        bundle["meta"]["edinetdb_calendar_cache_path"] = str(base_meta.get("edinetdb_calendar_cache_path", "") or "")
        bundle["meta"]["edinetdb_calendar_cache_hit"] = bool(base_meta.get("edinetdb_calendar_cache_hit", False))
        bundle["meta"]["edinetdb_calendar_request_mode"] = str(base_meta.get("edinetdb_calendar_request_mode", "") or "")
        bundle["meta"]["edinetdb_calendar_failure_reason"] = str(base_meta.get("edinetdb_calendar_failure_reason", "") or "")
        bundle["meta"]["edinetdb_calendar_jquants_fallback_used"] = bool(base_meta.get("edinetdb_calendar_jquants_fallback_used", False))
        bundle["diagnostics"].update(
            {
                "base_meta": base_meta,
                "ranking": ranking_diag,
                "deep_watch": deep_watch_diag,
                "board": board_diag,
                "earnings_announcement": {
                    "source": str(base_meta.get("earnings_announcement_source", "") or ""),
                    "source_path": str(base_meta.get("earnings_announcement_source_path", "") or ""),
                    "status": str(base_meta.get("earnings_announcement_status", "") or ""),
                    "target_date": str(base_meta.get("earnings_announcement_target_date", "") or ""),
                    "request_mode": str(base_meta.get("earnings_announcement_request_mode", "") or ""),
                    "cache_path": str(base_meta.get("earnings_announcement_cache_path", "") or ""),
                    "cache_hit": bool(base_meta.get("earnings_announcement_cache_hit", False)),
                    "failure_reason": str(base_meta.get("earnings_announcement_failure_reason", "") or ""),
                    "uses_jquants_fallback": bool(base_meta.get("earnings_announcement_jquants_fallback_used", False)),
                    "source_policy": "display_and_earnings_buffer_fields_are_derived_from_edinetdb_calendar_only",
                },
                "write_completed": False,
            }
        )
        bundle["meta"]["today_rank_mode"] = str(bundle["diagnostics"].get("today_rank_mode", "") or "")
        bundle["meta"]["ranking_union_count"] = int(bundle["diagnostics"].get("ranking_union_count", 0) or 0)
        bundle["meta"]["sectors_with_ranking_confirmed_ge5"] = int(bundle["diagnostics"].get("sectors_with_ranking_confirmed_ge5", 0) or 0)
        bundle["meta"]["sectors_with_source_breadth_ge2"] = int(bundle["diagnostics"].get("sectors_with_source_breadth_ge2", 0) or 0)
        bundle["diagnostics"]["write_completed"] = True
        write_result = write_snapshot_bundle(bundle, settings, write_drive=write_drive)
        bundle["snapshot_source_label"] = str(write_result.pop("source_label", ""))
        bundle["snapshot_backend_name"] = str(write_result.pop("backend_name", ""))
        bundle["snapshot_warning_message"] = str(write_result.pop("warning_message", ""))
        bundle["paths"] = write_result
        return bundle
    except JQuantsAuthError as exc:
        logger.error("fail-closed reason: %s", exc)
        logger.debug("authentication exception", exc_info=True)
        raise
    except Exception as exc:
        logger.error("fail-closed reason: %s", exc)
        logger.debug("pipeline exception", exc_info=True)
        raise


def _render_bundle(bundle: dict[str, Any], *, source_label: str, is_saved_snapshot: bool = False) -> None:
    del source_label
    snapshot_source_label = str(bundle.get("snapshot_source_label", "")).strip()
    snapshot_warning_message = str(bundle.get("snapshot_warning_message", "")).strip()
    snapshot_compatibility_notes = [str(note).strip() for note in bundle.get("snapshot_compatibility_notes", []) if str(note).strip()]
    today_sector_source_key = str(bundle.get("today_sector_source_key", "")).strip()
    meta = bundle.get("meta", {})
    generated_at_jst = str(meta.get("generated_at_jst", "") or meta.get("generated_at", ""))
    mode = str(meta.get("mode", ""))
    expected_time_label = str(meta.get("expected_time_label", "")).strip()
    timepoint_meaning = _timepoint_meaning(mode)
    empty_reasons = bundle.get("empty_reasons", {})
    snapshot_guard = bundle.get("snapshot_guard", {})
    warning_text = saved_snapshot_timing_warning(meta) if is_saved_snapshot else ""
    snapshot_security_frames = _snapshot_security_frames(bundle)
    earnings_announcement_lookup = _build_earnings_announcement_lookup(*snapshot_security_frames)
    security_reference_lookup = _build_security_reference_lookup(
        *snapshot_security_frames,
        earnings_announcement_lookup=earnings_announcement_lookup,
    )
    today_sector_view, today_sector_notes = _prepare_today_sector_view(bundle.get("today_sector_leaderboard", pd.DataFrame()))
    sector_representatives_raw = bundle.get("sector_representatives", bundle.get("center_stocks", bundle.get("leaders_by_sector", pd.DataFrame())))
    saved_representatives_display = bundle.get("sector_representatives_display", pd.DataFrame())
    sector_representatives_view, sector_representatives_notes = _prepare_sector_representatives_display_view(
        saved_representatives_display if isinstance(saved_representatives_display, pd.DataFrame) and not saved_representatives_display.empty else sector_representatives_raw,
        display_is_source_of_truth=isinstance(saved_representatives_display, pd.DataFrame) and not saved_representatives_display.empty,
        earnings_announcement_lookup=earnings_announcement_lookup,
    )
    weekly_sector_view, weekly_sector_notes = _prepare_persistence_sector_view(bundle.get("sector_persistence_1w", bundle.get("weekly_sector_summary", pd.DataFrame())))
    monthly_sector_view, monthly_sector_notes = _prepare_persistence_sector_view(bundle.get("sector_persistence_1m", bundle.get("monthly_sector_summary", pd.DataFrame())))
    quarter_sector_view, quarter_sector_notes = _prepare_persistence_sector_view(bundle.get("sector_persistence_3m", pd.DataFrame()))
    swing_1w_display = bundle.get("swing_candidates_1w_display", pd.DataFrame())
    swing_1m_display = bundle.get("swing_candidates_1m_display", pd.DataFrame())
    swing_3m_display = bundle.get("swing_candidates_3m_display", pd.DataFrame())
    swing_1w_view, swing_1w_notes = _prepare_swing_candidate_display_view(
        swing_1w_display if isinstance(swing_1w_display, pd.DataFrame) else pd.DataFrame(),
        columns=SWING_1W_DISPLAY_COLUMNS,
        raw_fallback=bundle.get("swing_candidates_1w", pd.DataFrame()),
    )
    swing_1m_view, swing_1m_notes = _prepare_swing_candidate_display_view(
        swing_1m_display if isinstance(swing_1m_display, pd.DataFrame) else pd.DataFrame(),
        columns=SWING_1M_DISPLAY_COLUMNS,
        raw_fallback=bundle.get("swing_candidates_1m", pd.DataFrame()),
    )
    swing_3m_view, swing_3m_notes = _prepare_swing_candidate_display_view(
        swing_3m_display if isinstance(swing_3m_display, pd.DataFrame) else pd.DataFrame(),
        columns=SWING_3M_DISPLAY_COLUMNS,
        raw_fallback=bundle.get("swing_candidates_3m", pd.DataFrame()),
    )
    base_meta = bundle.get("diagnostics", {}).get("base_meta", {})
    if not isinstance(base_meta, dict) or not base_meta:
        base_meta = {
            "earnings_forward_buffer_available": bool(meta.get("earnings_forward_buffer_available", False)),
            "earnings_forward_buffer_reason": str(meta.get("earnings_forward_buffer_reason", "") or ""),
            "earnings_dataset_status": str(meta.get("earnings_dataset_status", "") or ""),
            "earnings_rows_future_window": int(meta.get("earnings_rows_future_window", 0) or 0),
            "earnings_rows_raw": int(meta.get("earnings_rows_raw", 0) or 0),
        }
    earnings_candidate_note = _build_earnings_candidate_table_note(base_meta)
    sector_compat_notes = sorted(set(today_sector_notes + sector_representatives_notes + weekly_sector_notes + monthly_sector_notes + quarter_sector_notes + swing_1w_notes + swing_1m_notes + swing_3m_notes))
    if generated_at_jst or mode:
        with st.expander("運用状態", expanded=False):
            mode_label = f"{expected_time_label} = {timepoint_meaning}" if expected_time_label and timepoint_meaning else expected_time_label or mode
            st.markdown(
                "### 保存データ情報\n"
                f"- モード: `{mode}`\n"
                f"- 対象時点: `{mode_label}`\n"
                f"- 保存時刻(JST): `{generated_at_jst}`\n"
                f"- 表示の意味: `保存時刻は実保存時刻、対象時点は判定に使う基準時刻`"
            )
            if snapshot_source_label:
                st.caption(f"保存元: {snapshot_source_label}")
            if snapshot_warning_message:
                st.caption(f"保存先補足: {snapshot_warning_message}")
            if bundle.get("paths"):
                st.json(bundle["paths"])
            if warning_text:
                st.caption(warning_text)
            if snapshot_compatibility_notes:
                for note in snapshot_compatibility_notes:
                    st.caption(note)
            elif sector_compat_notes:
                st.caption("不足列があるので旧スナップショット互換で補完表示しています。")
    if bool(snapshot_guard.get("is_stale")):
        st.warning(str(snapshot_guard.get("reason", "")).strip() or f"{mode} は本日データなし / stale です。")
    if is_saved_snapshot and today_sector_source_key and today_sector_source_key != "today_sector_leaderboard":
        st.caption(f"today表は `{today_sector_source_key}` からの旧 snapshot 互換表示です。")
    if earnings_candidate_note:
        st.warning(earnings_candidate_note)
    today_sector_focus_source = bundle.get("today_sector_leaderboard", today_sector_view)
    if not isinstance(today_sector_focus_source, pd.DataFrame) or today_sector_focus_source.empty:
        today_sector_focus_source = today_sector_view
    today_sector_focus = _build_sector_focus_view(today_sector_focus_source, timeframe="today")
    weekly_sector_focus = _build_sector_focus_view(weekly_sector_view, timeframe="1w")
    monthly_sector_focus = _build_sector_focus_view(monthly_sector_view, timeframe="1m")
    quarter_sector_focus = _build_sector_focus_view(quarter_sector_view, timeframe="3m")

    today_sector_lookup = _extract_sector_rank_lookup(today_sector_focus, rank_col="today_display_rank")
    weekly_sector_lookup = _extract_sector_rank_lookup(weekly_sector_focus, rank_col="axis_rank")
    monthly_sector_lookup = _extract_sector_rank_lookup(monthly_sector_focus, rank_col="axis_rank")
    quarter_sector_lookup = _extract_sector_rank_lookup(quarter_sector_focus, rank_col="axis_rank")

    today_center_focus = _build_center_stock_focus_view(
        today_sector_view,
        sector_rank_lookup=today_sector_lookup,
        timeframe="today",
        representative_frame=sector_representatives_raw if isinstance(sector_representatives_raw, pd.DataFrame) and not sector_representatives_raw.empty else sector_representatives_view,
        earnings_announcement_lookup=earnings_announcement_lookup,
        security_reference_lookup=security_reference_lookup,
    )
    weekly_center_focus = _build_center_stock_focus_view(
        bundle.get("sector_persistence_1w", bundle.get("weekly_sector_summary", pd.DataFrame())),
        sector_rank_lookup=weekly_sector_lookup,
        timeframe="1w",
        earnings_announcement_lookup=earnings_announcement_lookup,
        security_reference_lookup=security_reference_lookup,
    )
    monthly_center_focus = _build_center_stock_focus_view(
        bundle.get("sector_persistence_1m", bundle.get("monthly_sector_summary", pd.DataFrame())),
        sector_rank_lookup=monthly_sector_lookup,
        timeframe="1m",
        earnings_announcement_lookup=earnings_announcement_lookup,
        security_reference_lookup=security_reference_lookup,
    )
    quarter_center_focus = _build_center_stock_focus_view(
        bundle.get("sector_persistence_3m", pd.DataFrame()),
        sector_rank_lookup=quarter_sector_lookup,
        timeframe="3m",
        earnings_announcement_lookup=earnings_announcement_lookup,
        security_reference_lookup=security_reference_lookup,
    )

    today_center_map = _build_center_reference_map(today_center_focus)
    weekly_center_map = _build_center_reference_map(weekly_center_focus)
    monthly_center_map = _build_center_reference_map(monthly_center_focus)
    quarter_center_map = _build_center_reference_map(quarter_center_focus)

    today_candidate_focus, today_candidate_reason, today_candidate_note = _build_today_purchase_candidate_view(
        sector_representatives_raw if isinstance(sector_representatives_raw, pd.DataFrame) else pd.DataFrame(),
        sector_rank_lookup=today_sector_lookup,
        center_reference_map=today_center_map,
        fallback_frame=swing_1w_view,
        limit=3,
        earnings_announcement_lookup=earnings_announcement_lookup,
    )
    if isinstance(today_candidate_focus, pd.DataFrame) and not today_candidate_focus.empty:
        today_candidate_focus = today_candidate_focus.drop(columns=["candidate_source_label"], errors="ignore")
    weekly_candidate_focus, weekly_candidate_reason, weekly_candidate_note = _build_candidate_focus_view(
        swing_1w_view,
        rank_col="candidate_rank_1w",
        sector_rank_lookup=weekly_sector_lookup,
        center_reference_map=weekly_center_map,
        scope_label="1週間軸",
    )
    monthly_candidate_focus, monthly_candidate_reason, monthly_candidate_note = _build_candidate_focus_view(
        swing_1m_view,
        rank_col="candidate_rank_1m",
        sector_rank_lookup=monthly_sector_lookup,
        center_reference_map=monthly_center_map,
        scope_label="1か月軸",
    )
    quarter_candidate_focus, quarter_candidate_reason, quarter_candidate_note = _build_candidate_focus_view(
        swing_3m_view,
        rank_col="candidate_rank_3m",
        sector_rank_lookup=quarter_sector_lookup,
        center_reference_map=quarter_center_map,
        scope_label="3か月軸",
    )

    timeframe_tabs = st.tabs(["today", "1w", "1m", "3m"])
    with timeframe_tabs[0]:
        _render_timeframe_panel(
            timeframe_label="today",
            timeframe_note="当日の強さを優先し、セクター順位は表示順位の昇順で見ます。",
            sector_title="セクター順位",
            sector_frame=today_sector_focus,
            sector_reason=str(empty_reasons.get("today_sector_leaderboard", "intraday 条件を満たす本命セクターがありません。")),
            center_frame=today_center_focus,
            center_reason=str(
                empty_reasons.get(
                    "sector_representatives_display",
                    empty_reasons.get("sector_representatives", empty_reasons.get("center_stocks", "中心銘柄がありません。")),
                )
            ),
            candidate_frame=today_candidate_focus,
            candidate_reason=today_candidate_reason or str(empty_reasons.get("swing_candidates_1w_display", empty_reasons.get("swing_candidates_1w", "today 強セクター内の買い候補はありません。"))),
            candidate_note="today 強セクター内で短期的に注目する銘柄です。1w候補をベースに抽出しており、最終的な買い判断ではありません。" if not str(today_candidate_note or "").strip() else f"today 強セクター内で短期的に注目する銘柄です。1w候補をベースに抽出しており、最終的な買い判断ではありません。 / {str(today_candidate_note).strip()}",
            candidate_title="短期注目銘柄",
        )
    with timeframe_tabs[1]:
        _render_timeframe_panel(
            timeframe_label="1w",
            timeframe_note="1週間で強さが続くセクターを確認し、代表銘柄と購入候補だけを軽く見ます。",
            sector_title="セクター順位",
            sector_frame=weekly_sector_focus,
            sector_reason=str(empty_reasons.get("sector_persistence_1w", empty_reasons.get("weekly_sector_summary", ""))),
            center_frame=weekly_center_focus,
            center_reason=str(empty_reasons.get("sector_persistence_1w", empty_reasons.get("weekly_sector_summary", "中心銘柄がありません。"))),
            candidate_frame=weekly_candidate_focus,
            candidate_reason=weekly_candidate_reason or str(empty_reasons.get("swing_candidates_1w_display", empty_reasons.get("swing_candidates_1w", "1週間軸の買い候補はありません。"))),
            candidate_note=weekly_candidate_note,
        )
    with timeframe_tabs[2]:
        _render_timeframe_panel(
            timeframe_label="1m",
            timeframe_note="1か月の継続強度を軸に、セクター順位・代表銘柄・購入候補を上から見られます。",
            sector_title="セクター順位",
            sector_frame=monthly_sector_focus,
            sector_reason=str(empty_reasons.get("sector_persistence_1m", empty_reasons.get("monthly_sector_summary", ""))),
            center_frame=monthly_center_focus,
            center_reason=str(empty_reasons.get("sector_persistence_1m", empty_reasons.get("monthly_sector_summary", "中心銘柄がありません。"))),
            candidate_frame=monthly_candidate_focus,
            candidate_reason=monthly_candidate_reason or str(empty_reasons.get("swing_candidates_1m_display", empty_reasons.get("swing_candidates_1m", "1か月軸の買い候補はありません。"))),
            candidate_note=monthly_candidate_note,
        )
    with timeframe_tabs[3]:
        _render_timeframe_panel(
            timeframe_label="3m",
            timeframe_note="3か月の継続トレンドを俯瞰し、長めの軸で残る候補だけを確認します。",
            sector_title="セクター順位",
            sector_frame=quarter_sector_focus,
            sector_reason=str(empty_reasons.get("sector_persistence_3m", "")),
            center_frame=quarter_center_focus,
            center_reason=str(empty_reasons.get("sector_persistence_3m", "中心銘柄がありません。")),
            candidate_frame=quarter_candidate_focus,
            candidate_reason=quarter_candidate_reason or str(empty_reasons.get("swing_candidates_3m_display", empty_reasons.get("swing_candidates_3m", "3か月軸の買い候補はありません。"))),
            candidate_note=quarter_candidate_note,
        )
    diagnostics = bundle.get("diagnostics", {})
    if diagnostics:
        with st.expander("diagnostics", expanded=False):
            st.json(diagnostics)


def _render_control_plane_status(settings: dict[str, Any], *, current_mode: str = "") -> None:
    if st.button("状態を再読込", key="refresh-control-plane-status"):
        _request_viewer_snapshot_reload()
        st.rerun()
    token = _github_control_token(use_streamlit_secrets=True)
    if not token:
        st.info("更新依頼ボタンは無効、閲覧のみです。Streamlit secret `GITHUB_CONTROL_TOKEN` が未設定です。")
        return
    try:
        status_payload, _ = read_control_plane_status(token, settings)
    except Exception as exc:
        st.warning(f"update_status.json の取得に失敗しました: {exc}")
        status_payload = {"last_run_at": "", "status": "unknown", "message": ""}
    try:
        request_payload, _ = read_control_plane_request(token, settings)
    except Exception as exc:
        st.warning(f"update_request.json の取得に失敗しました: {exc}")
        request_payload = {"request_update": False, "request_mode": "", "requested_at": "", "requested_by": "", "status": "unknown"}
    status_cols = st.columns(3)
    status_cols[0].metric("status", str(status_payload.get("status", "")))
    status_cols[1].metric("last_run_at", str(status_payload.get("last_run_at", "")) or "-")
    status_cols[2].metric("request", "pending" if bool(request_payload.get("request_update")) else "idle")
    message = str(status_payload.get("message", "")).strip()
    if message:
        st.caption(f"message: {message}")
    try:
        status_request_mode = normalize_cloud_viewer_mode(status_payload.get("request_mode", ""), allow_blank=True)
    except ValueError:
        status_request_mode = str(status_payload.get("request_mode", "")).strip()
    if status_request_mode:
        st.caption(f"status request_mode: {status_request_mode}")
    requested_at = str(request_payload.get("requested_at", "")).strip()
    requested_by = str(request_payload.get("requested_by", "")).strip()
    try:
        request_mode = normalize_cloud_viewer_mode(
            request_payload.get("request_mode", ""),
            allow_blank=not bool(request_payload.get("request_update")),
        )
    except ValueError:
        request_mode = DEFAULT_CONTROL_PLANE_REQUEST_MODE
    try:
        current_mode_normalized = normalize_cloud_viewer_mode(current_mode, allow_blank=True)
    except ValueError:
        current_mode_normalized = ""
    if request_payload.get("request_update"):
        st.warning(f"更新依頼は受付中です。request_mode={request_mode} requested_at={requested_at or '-'} requested_by={requested_by or '-'}")
        pending_cols = st.columns(len(CLOUD_VIEWER_MODES))
        for index, mode in enumerate(CLOUD_VIEWER_MODES):
            pending_cols[index].button(f"{mode}を更新", disabled=True, key=f"request-{mode}-disabled", help="すでに依頼済みです")
        return
    action_cols = st.columns(len(CLOUD_VIEWER_MODES))
    for index, mode in enumerate(CLOUD_VIEWER_MODES):
        button_type = "primary" if current_mode_normalized and mode == current_mode_normalized else "secondary"
        if action_cols[index].button(
            f"{mode}を更新",
            type=button_type,
            key=f"request-{mode}",
            help=f"control-plane branch に {mode} 更新依頼を書き込みます",
        ):
            try:
                submitted, updated_request = submit_control_plane_update_request(
                    token,
                    settings,
                    requested_by="streamlit-cloud-viewer",
                    requested_mode=mode,
                )
                if submitted:
                    st.success(f"{mode} 更新依頼を送信しました。requested_at={updated_request.get('requested_at', '')}")
                else:
                    st.info("すでに更新依頼が入っているため、二重依頼は行いませんでした。")
                st.rerun()
            except Exception as exc:
                st.error(f"{mode} 更新依頼の送信に失敗しました: {exc}")


def _render_viewer_only_app(settings: dict[str, Any], runtime_context: dict[str, Any] | None = None) -> None:
    runtime_context = runtime_context or {}
    current_snapshot_tokens: dict[str, dict[str, str]] = {}
    st.caption("Cloud viewer-only モードです。スナップショット表示と更新依頼を扱います。")
    _enable_viewer_auto_refresh(settings)
    _render_snapshot_cache_admin_tools()
    available_modes = _available_viewer_snapshot_modes(settings)
    mode_warnings = _get_viewer_snapshot_mode_warnings()
    st.markdown("### スナップショット")
    for warning_message in mode_warnings:
        st.warning(warning_message)
    if not available_modes:
        st.warning("まだ snapshot がありません")
        st.caption("表示対象: latest_0915.json / latest_1130.json / latest_1530.json / latest_now.json")
        with st.expander("更新依頼 / control-plane", expanded=False):
            _render_control_plane_status(settings)
        _finalize_viewer_snapshot_reload(current_snapshot_tokens)
        _render_runtime_detection_diagnostics(runtime_context)
        return
    if len(available_modes) == 1:
        mode = available_modes[0]
        try:
            bundle = load_saved_snapshot(mode, settings, allow_stale_content=True)
        except Exception as exc:
            st.warning(f"{mode} の snapshot 読み込みに失敗したため、この mode は unavailable 扱いにします: {exc}")
            with st.expander("更新依頼 / control-plane", expanded=False):
                _render_control_plane_status(settings, current_mode=mode)
            _finalize_viewer_snapshot_reload(current_snapshot_tokens)
            _render_runtime_detection_diagnostics(runtime_context)
            return
        current_snapshot_tokens[mode] = _bundle_snapshot_token(bundle)
        _render_bundle(bundle, source_label=f"latest_{mode}.json を表示しました", is_saved_snapshot=True)
        with st.expander("更新依頼 / control-plane", expanded=False):
            _render_control_plane_status(settings, current_mode=mode)
        _finalize_viewer_snapshot_reload(current_snapshot_tokens)
        _render_runtime_detection_diagnostics(runtime_context)
        return
    tabs = st.tabs([f"{mode}" for mode in available_modes])
    for tab, mode in zip(tabs, available_modes):
        with tab:
            try:
                bundle = load_saved_snapshot(mode, settings, allow_stale_content=True)
            except Exception as exc:
                st.warning(f"{mode} の snapshot 読み込みに失敗したため、この mode は unavailable 扱いにします: {exc}")
                continue
            current_snapshot_tokens[mode] = _bundle_snapshot_token(bundle)
            _render_bundle(bundle, source_label=f"latest_{mode}.json を表示しました", is_saved_snapshot=True)
    with st.expander("更新依頼 / control-plane", expanded=False):
        _render_control_plane_status(settings)
    _finalize_viewer_snapshot_reload(current_snapshot_tokens)
    _render_runtime_detection_diagnostics(runtime_context)


def render_app() -> None:
    st.set_page_config(page_title="セクター概況", layout="wide")
    st.title("セクター概況")
    settings = get_settings()
    runtime_context = _streamlit_runtime_context(settings)
    if bool(runtime_context.get("viewer_only")):
        st.caption("Cloud では viewer-only で動作します。スナップショット表示と更新依頼だけを行います。")
        st.info("latest_0915.json / latest_1130.json / latest_1530.json / latest_now.json を優先して読み込みます。Cloud では collector / kabu live 取得は実行しません。")
        _render_viewer_only_app(settings, runtime_context)
        return
    st.caption("J-Quants を土台に、kabu ステーション API のライブデータを重ねてスナップショットを作成・表示します。")
    st.info("過去の任意時点をあとから再取得することはできません。保存済みスナップショットのみ再表示できます。")
    view_mode = st.radio("表示方法", ["A: ライブでスナップショットを作成", "B: 保存済みスナップショットを表示"], index=0)
    mode = st.selectbox("表示モード", list(CLOUD_VIEWER_MODES), index=0)
    if view_mode.startswith("A:"):
        write_drive = st.checkbox("Google Drive 同期フォルダへも保存", value=False)
        fast_check = st.checkbox("簡易チェックで実行", value=False)
        if st.button("スナップショットを作成"):
            try:
                with safe_spinner("スナップショットを作成中...", enabled=True):
                    bundle = run_cli(mode=mode, write_drive=write_drive, fast_check=fast_check)
                _render_bundle(bundle, source_label="スナップショットを作成しました")
            except Exception as exc:
                st.error(str(exc))
    else:
        if st.button("保存済みスナップショットを表示"):
            try:
                bundle = load_saved_snapshot(mode, settings)
                _render_bundle(bundle, source_label="保存済みスナップショットを表示しました", is_saved_snapshot=True)
            except FileNotFoundError as exc:
                st.warning(str(exc))
            except Exception as exc:
                st.error(str(exc))
    _render_runtime_detection_diagnostics(runtime_context)


if __name__ == "__main__":
    render_app()
