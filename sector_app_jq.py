import base64
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
from urllib.parse import quote_plus

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

try:
    from snapshot_bundle import bundle_to_json_text, bundle_to_markdown
    from snapshot_store import read_snapshot_json, write_snapshot_bundle as write_snapshot_bundle_to_store
    from snapshot_time import build_snapshot_meta, normalize_snapshot_meta, saved_snapshot_timing_warning
except ModuleNotFoundError:
    @dataclass
    class _SnapshotStoreResult:
        paths: dict[str, str]
        source_label: str
        backend_name: str
        warning_message: str


    def _snapshot_dir(settings: dict[str, Any], root_dir: Path) -> Path:
        output_dir = str(settings.get("SNAPSHOT_OUTPUT_DIR", "data/snapshots")).strip() or "data/snapshots"
        output_path = Path(output_dir)
        if not output_path.is_absolute():
            output_path = root_dir / output_path
        return output_path


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

def read_snapshot_json(mode: str, settings: dict[str, Any], root_dir: Path) -> tuple[str, _SnapshotStoreResult]:
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
) -> _SnapshotStoreResult:
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
        "industry_up_rank_norm": 1.5,
        "price_up_share_of_sector": 0.95,
        "price_up_share_of_market_scan": 0.8,
    },
    "flow": {
        "turnover_share_of_sector": 0.9,
        "turnover_share_of_market_scan": 0.9,
        "volume_surge_share_of_sector": 0.8,
        "volume_surge_share_of_market_scan": 0.65,
        "turnover_surge_share_of_sector": 0.8,
        "turnover_surge_share_of_market_scan": 0.65,
    },
    "participation": {
        "signal_breadth_share": 1.05,
        "scan_coverage": 0.8,
        "scan_member_count_norm": 0.6,
        "scan_member_share_of_market_scan": 0.45,
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
    "base_shift": 1,
    "strong_shift": 2,
    "very_strong_shift": 3,
}
INTRADAY_SCAN_SAMPLE_WARNING_RULES = {
    "critical_count": 4.0,
    "warn_count": 7.0,
    "warn_coverage": 0.16,
    "thin_count": 10.0,
    "thin_coverage": 0.10,
}
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
SECTOR_CONFIDENCE_PRIORITY = {"高": 2, "中": 1, "低": 0}
SWING_SELECTION_CONFIG_BASELINE = {
    "extension_threshold_1w": 12.0,
    "extension_threshold_1m": 12.0,
    "candidate_ma20_weight_1m": 0.35,
    "sector_confidence_bonus_high_1w": 0.0,
    "sector_confidence_bonus_mid_1w": 0.0,
    "sector_confidence_bonus_high_1m": 0.0,
    "sector_confidence_bonus_mid_1m": 0.0,
}
SWING_SELECTION_CONFIG = {
    "extension_threshold_1w": 10.0,
    "extension_threshold_1m": 10.0,
    "candidate_ma20_weight_1m": 0.55,
    "sector_confidence_bonus_high_1w": 0.35,
    "sector_confidence_bonus_mid_1w": 0.15,
    "sector_confidence_bonus_high_1m": 0.30,
    "sector_confidence_bonus_mid_1m": 0.12,
}
VIEWER_ONLY_SNAPSHOT_MODES = ("1130", "1530", "now")
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
    "persistence_rank": "継続順位",
    "breadth": "scan内当日上昇:下落",
    "median_ret": "当日中央値騰落率",
    "median_live_ret": "当日中央値騰落率",
    "turnover_ratio_median": "売買代金倍率",
    "industry_up": "東証業種順位/上昇率",
    "industry_up_value": "東証業種上昇率",
    "industry_up_rank": "東証業種順位",
    "industry_rank_live": "東証業種順位",
    "industry_up_anchor_rank": "東証アンカー順位",
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
    "live_ret_vs_prev_close": "前日終値比(%)",
    "live_ret_from_open": "始値比(%)",
    "live_turnover": "売買代金",
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
    "finance_health_score": "財務健全度",
    "finance_health_flag": "財務健全フラグ",
    "price_block_score": "価格の強さ",
    "flow_block_score": "資金流入の強さ",
    "participation_block_score": "ランキング広がり",
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
    "candidate_quality": "候補品質",
    "entry_fit": "今の判定",
    "selection_reason": "採用理由",
    "risk_note": "注意点",
    "candidate_commentary": "コメント",
    "sector_confidence": "信頼度",
    "sector_caution": "注意点",
    "scan_sample_warning_level": "scan母数警告レベル",
    "scan_sample_warning_reason": "scan母数警告理由",
    "representative_stock": "代表銘柄",
    "representative_rank": "代表順位",
    "representative_score": "代表銘柄スコア",
    "stock_turnover_share_of_sector": "セクター売買代金寄与率",
    "swing_score_1w": "1週間候補スコア",
    "swing_score_1m": "1か月候補スコア",
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
    "nikkei_search": "日経で検索",
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


def _is_streamlit_cloud() -> bool:
    sharing_mode = str(os.environ.get("STREAMLIT_SHARING_MODE", "")).strip().lower()
    cloud_flag = str(os.environ.get("STREAMLIT_CLOUD", "")).strip().lower()
    if sharing_mode in {"1", "true", "cloud"}:
        return True
    if cloud_flag in {"1", "true", "yes"}:
        return True
    return False


def _snapshot_json_path(mode: str, settings: dict[str, Any] | None = None) -> Path:
    settings = settings or get_settings()
    output_dir = str(settings.get("SNAPSHOT_OUTPUT_DIR", "data/snapshots")).strip() or "data/snapshots"
    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = ROOT_DIR / output_path
    return output_path / f"latest_{mode}.json"


def _available_viewer_snapshot_modes(settings: dict[str, Any] | None = None) -> list[str]:
    settings = settings or get_settings()
    return [mode for mode in VIEWER_ONLY_SNAPSHOT_MODES if _snapshot_json_path(mode, settings).exists()]


def _snapshot_mtime_ns(snapshot_path: Path) -> int:
    try:
        return snapshot_path.stat().st_mtime_ns
    except FileNotFoundError:
        return -1


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
    encoded = str(payload.get("content", "")).replace("\n", "")
    if not encoded:
        raise RuntimeError(f"GitHub read returned empty content for {path}")
    text = base64.b64decode(encoded).decode("utf-8")
    return text, str(payload.get("sha", ""))


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
    requested_mode: str = "1130",
    session: requests.sessions.Session | None = None,
) -> tuple[bool, dict[str, Any]]:
    payload, sha = read_control_plane_request(token, settings, session=session)
    if bool(payload.get("request_update")):
        return False, payload
    mode = str(requested_mode or "1130").strip() or "1130"
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


def get_earnings_buffer_frame(trading_dates: list[str], *, api_key: str | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    meta: dict[str, Any] = {
        "status": "empty_dataset",
        "rows_raw": 0,
        "rows_future_window": 0,
        "valid_code4_count": 0,
        "date_col": "",
        "code_col": "",
        "min_event_date": "",
        "max_event_date": "",
    }
    if not trading_dates:
        meta["status"] = "no_trading_dates"
        return pd.DataFrame(columns=["code", "earnings_buffer_days"]), meta
    latest_date = pd.Timestamp(trading_dates[-1])
    path = "/equities/earnings-calendar"
    params = {"from": latest_date.strftime("%Y-%m-%d"), "to": (latest_date + timedelta(days=90)).strftime("%Y-%m-%d")}
    df = _get_optional_dataset(path, params, dataset_name="earnings_calendar", api_key=api_key)
    meta["rows_raw"] = int(len(df))
    if not df.empty:
        code_col = pick_optional_existing(df, ["Code", "LocalCode", "code"])
        event_col = pick_optional_existing(df, ["AnnouncementDate", "DisclosedDate", "ExpectedDate", "ScheduledDate", "Date", "date"])
        meta["date_col"] = str(event_col or "")
        meta["code_col"] = str(code_col or "")
        if code_col and event_col:
            normalized_codes = df[code_col].map(_normalize_code4)
            event_dates = pd.to_datetime(df[event_col], errors="coerce")
            valid_dates = event_dates.dropna()
            if not valid_dates.empty:
                meta["min_event_date"] = valid_dates.min().strftime("%Y-%m-%d")
                meta["max_event_date"] = valid_dates.max().strftime("%Y-%m-%d")
            meta["valid_code4_count"] = int(normalized_codes.map(_is_code4).sum())
            meta["rows_future_window"] = int(((event_dates >= latest_date) & (event_dates <= latest_date + timedelta(days=90))).fillna(False).sum())
            out = pd.DataFrame({"code": normalized_codes, "event_date": event_dates})
            out = out[out["code"].map(_is_code4)].dropna(subset=["event_date"])
            out = out[(out["event_date"] >= latest_date) & (out["event_date"] <= latest_date + timedelta(days=90))].copy()
            logger.info("earnings calendar endpoint used path=%s rows_raw=%s rows_filtered=%s", path, len(df), len(out))
            if not out.empty:
                out["earnings_buffer_days"] = (out["event_date"] - latest_date).dt.days
                meta["status"] = "ok"
                return out.sort_values(["code", "event_date"]).drop_duplicates("code")[["code", "earnings_buffer_days"]].reset_index(drop=True), meta
            meta["status"] = "no_forward_events_in_dataset" if meta["rows_future_window"] == 0 else "no_usable_rows_after_filter"
            logger.warning(
                "earnings calendar unavailable for forward buffer latest_date=%s rows_raw=%s min_event_date=%s max_event_date=%s future_window_rows=%s",
                latest_date.strftime("%Y-%m-%d"),
                meta["rows_raw"],
                meta["min_event_date"],
                meta["max_event_date"],
                meta["rows_future_window"],
            )
            return pd.DataFrame(columns=["code", "earnings_buffer_days"]), meta
        meta["status"] = "missing_required_columns"
    return pd.DataFrame(columns=["code", "earnings_buffer_days"]), meta


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
    earnings_frame, earnings_meta = get_earnings_buffer_frame(trading_dates, api_key=api_key)
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
    base["material_title"] = ""
    base["material_link"] = ""
    base["material_score"] = 0.0
    base["latest_date"] = pd.to_datetime(base["latest_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    base = _annotate_non_corporate_products(base)
    logger.info("build_daily_base_data end rows=%s", len(base))
    return base, {
        "latest_date": max(trading_dates),
        "trading_date_count": len(trading_dates),
        "lookback_days": lookback_trading_days,
        "fast_check": fast_check,
        "topix_source_rows": int(len(topix_history)),
        "earnings_coverage_count": int(base["earnings_buffer_days"].notna().sum()) if "earnings_buffer_days" in base.columns else 0,
        "earnings_dataset_status": str(earnings_meta.get("status", "")),
        "earnings_rows_raw": int(earnings_meta.get("rows_raw", 0) or 0),
        "earnings_rows_future_window": int(earnings_meta.get("rows_future_window", 0) or 0),
        "earnings_min_event_date": str(earnings_meta.get("min_event_date", "") or ""),
        "earnings_max_event_date": str(earnings_meta.get("max_event_date", "") or ""),
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
    ranking_combo = (
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
    ranking_combo = ranking_combo.merge(
        base_df[base_merge_columns],
        on="code",
        how="left",
        suffixes=("", "_base"),
    )
    ranking_combo["name"] = ranking_combo["name"].fillna(ranking_combo["ranking_name"]).fillna("")
    ranking_combo["sector_name"] = ranking_combo["sector_name"].fillna(ranking_combo["ranking_sector_name"]).fillna("")
    ranking_combo = _exclude_non_corporate_products(ranking_combo, diagnostics, context="market_scan")
    industry_df = fetch_kabu_ranking(settings, token, "industry_up")
    diagnostics["ranking_counts"]["industry_up"] = int(len(industry_df))
    logger.info("build_market_scan_universe end candidates=%s industries=%s", len(ranking_combo), len(industry_df))
    return ranking_combo, industry_df, diagnostics


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


def select_deep_watch_universe(market_scan_df: pd.DataFrame, base_df: pd.DataFrame, settings: dict[str, Any], mode: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Select the 50-name deep-watch universe for board enrichment."""
    logger.info("select_deep_watch_universe start mode=%s", mode)
    register_limit = int(settings.get("KABU_REGISTER_LIMIT", 50))
    diagnostics: dict[str, Any] = {"non_corporate_products": {}}
    market_scan_df = _exclude_non_corporate_products(market_scan_df, diagnostics, context="deep_watch_market_scan")
    deep_candidates = _exclude_non_corporate_products(base_df, diagnostics, context="deep_watch_base")
    deep_candidates["candidate_seed_score"] = 0.0
    deep_candidates.loc[deep_candidates["rel_1w"].rank(ascending=False, method="min") <= 80, "candidate_seed_score"] += 1.0
    deep_candidates.loc[deep_candidates["ret_1w"].rank(ascending=False, method="min") <= 80, "candidate_seed_score"] += 1.0
    deep_candidates.loc[deep_candidates["TradingValue_latest"].rank(ascending=False, method="min") <= 100, "candidate_seed_score"] += 1.2
    deep_candidates.loc[deep_candidates["reversal_candidates"].fillna(False), "candidate_seed_score"] += 0.8
    deep_candidates.loc[deep_candidates["is_near_52w_high"].fillna(False), "candidate_seed_score"] += 0.8
    top_sector_names = base_df[["sector_name", "sector_rank_1w"]].dropna().drop_duplicates().sort_values("sector_rank_1w").head(8)["sector_name"].tolist()
    deep_candidates.loc[deep_candidates["sector_name"].isin(top_sector_names), "candidate_seed_score"] += 0.7
    combined = pd.concat([market_scan_df.head(80).merge(base_df, on="code", how="left"), deep_candidates], ignore_index=True, sort=False)
    combined["combined_priority"] = combined.get("ranking_combo_score", 0).fillna(0) + combined["candidate_seed_score"].fillna(0)
    combined["code"] = combined["code"].astype(str)
    pre_count = len(combined)
    invalid_code_count = int((~combined["code"].map(_is_code4)).sum())
    duplicate_count = int(combined["code"].duplicated().sum())
    combined = combined[combined["code"].map(_is_code4)].copy()
    combined = combined.sort_values(["combined_priority", "TradingValue_latest"], ascending=[False, False]).drop_duplicates("code")
    selected = combined.head(register_limit).copy()
    logger.debug("deep-watch candidate_count=%s selected=%s excluded_duplicate=%s excluded_invalid=%s excluded_market_unknown=%s", pre_count, len(selected), duplicate_count, invalid_code_count, 0)
    logger.info("select_deep_watch_universe end selected=%s", len(selected))
    return selected, {
        "candidate_count": pre_count,
        "selected_count": int(len(selected)),
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


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return _coerce_numeric(numerator) / _coerce_numeric(denominator).replace(0, pd.NA)


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
    label.loc[no_scan_mask] = "scan裏付けなし"
    label.loc[thin_mask] = "scan母数薄い"
    label.loc[warn_mask] = "scan母数少"
    label.loc[critical_mask] = "scan母数極少"
    reason = pd.Series([""] * len(count), index=count.index, dtype="object")
    reason.loc[no_scan_mask] = count.loc[no_scan_mask].map(lambda value: f"scan_member_count={int(value)}") + coverage.loc[no_scan_mask].map(lambda value: f", scan_coverage={value:.3f}")
    reason.loc[thin_mask] = count.loc[thin_mask].map(lambda value: f"scan_member_count={int(value)}") + coverage.loc[thin_mask].map(lambda value: f", scan_coverage={value:.3f}")
    reason.loc[warn_mask] = count.loc[warn_mask].map(lambda value: f"scan_member_count={int(value)}") + coverage.loc[warn_mask].map(lambda value: f", scan_coverage={value:.3f}")
    reason.loc[critical_mask] = count.loc[critical_mask].map(lambda value: f"scan_member_count={int(value)}") + coverage.loc[critical_mask].map(lambda value: f", scan_coverage={value:.3f}")
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
        source_labels = {"price_up", "turnover", "volume_surge", "turnover_surge"}
        expanded = out.copy()
        expanded["source_type"] = expanded["ranking_sources"].fillna("").astype(str).str.split(",")
        expanded = expanded.explode("source_type")
        expanded["source_type"] = expanded["source_type"].fillna("").astype(str).str.strip()
        expanded = expanded[expanded["source_type"].isin(source_labels)].copy()
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
            "sector_name",
            "representative_stock",
            "representative_stocks",
            "leaders",
            "n",
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
            "price_up_count",
            "turnover_count",
            "volume_surge_count",
            "turnover_surge_count",
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
        ("tethered_rank", True),
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
    warning_level = str(row.get("scan_sample_warning_level", "") or "").strip()
    if warning_level == "no_scan":
        reasons.append("scan裏付けなし")
    if warning_level == "critical":
        reasons.append("scan母数極少")
    if bool(row.get("source_bias_warning_flag")):
        reasons.append("source偏りあり")
    if bool(row.get("ranking_breadth_warning_flag")):
        reasons.append("ランキング広がり弱い")
    if str(row.get("sector_confidence", "") or "").strip() == "低":
        reasons.append("信頼度=低")
    return _build_sector_caution_tags(reasons)


def _apply_true_rank_shift_limits(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else _empty_sector_leaderboard()
    working = frame.copy().reset_index(drop=True)
    total_rows = len(working)
    fallback_anchor = pd.Series(range(1, total_rows + 1), index=working.index, dtype="float64")
    anchor_rank = _coerce_numeric(working["industry_anchor_rank"]).fillna(fallback_anchor)
    shift_limit = _coerce_numeric(working["rank_shift_limit"]).fillna(0.0).round().clip(lower=0.0)
    working["allowed_shift"] = shift_limit.astype(int)
    working["upshift_blocked_reason"] = working.apply(_build_upshift_blocked_reason, axis=1)
    blocked_upshift = working["upshift_blocked_reason"].astype(str).str.strip() != ""
    anchor_rank_int = anchor_rank.round().astype(int)
    raw_preferred_rank = _coerce_numeric(working["score_rank"]).fillna(_coerce_numeric(working["tethered_rank"])).fillna(anchor_rank).clip(lower=1.0, upper=float(total_rows)).round().astype(int)
    rank_floor = (anchor_rank - shift_limit).clip(lower=1.0, upper=float(total_rows)).round().astype(int)
    rank_floor = rank_floor.where(~blocked_upshift, anchor_rank_int)
    rank_ceiling = (anchor_rank + shift_limit).clip(lower=1.0, upper=float(total_rows)).round().astype(int)
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
    if "tethered_rank" in working.columns:
        working["today_rank"] = _coerce_numeric(working["tethered_rank"]).fillna(fallback_rank).round().clip(lower=1.0).astype("int64")
    else:
        working["today_rank"] = fallback_rank
    if "industry_anchor_rank" in working.columns:
        working["final_rank_delta"] = _coerce_numeric(working["today_rank"]).fillna(0.0) - _coerce_numeric(working["industry_anchor_rank"]).fillna(_coerce_numeric(working["today_rank"]).fillna(0.0))
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
            "sector_confidence",
            "sector_caution",
        ]
    )


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
    industry_base = industry_df[
        [column for column in ["sector_name", "rank_position", "industry_up_value"] if column in industry_df.columns]
    ].copy()
    industry_base["sector_name"] = industry_base["sector_name"].map(_normalize_industry_name)
    industry_base = industry_base[industry_base["sector_name"].astype(str).str.strip() != ""].drop_duplicates("sector_name").copy()
    if industry_base.empty:
        return _empty_sector_leaderboard()
    if "rank_position" not in industry_base.columns:
        industry_base["rank_position"] = pd.Series(range(1, len(industry_base) + 1), index=industry_base.index, dtype="int64")
    if "industry_up_value" not in industry_base.columns:
        industry_base["industry_up_value"] = pd.NA
    sector_inventory = base_df.copy()
    if "sector_name" not in sector_inventory.columns:
        sector_inventory["sector_name"] = ""
    sector_inventory["sector_name"] = sector_inventory["sector_name"].map(_normalize_industry_name)
    sector_inventory = sector_inventory[sector_inventory["sector_name"].astype(str).str.strip() != ""].copy()
    if "sector_constituent_count" not in sector_inventory.columns:
        sector_inventory = sector_inventory.groupby("sector_name", as_index=False).agg(sector_constituent_count=("code", "nunique"))
    else:
        sector_inventory = sector_inventory.groupby("sector_name", as_index=False).agg(sector_constituent_count=("sector_constituent_count", "max"))
    sector_base = industry_base.rename(columns={"rank_position": "industry_rank_live"}).merge(
        sector_inventory,
        on="sector_name",
        how="left",
    )
    market_scan_member_count = 0.0
    source_totals: dict[str, float] = {}
    live_scan = pd.DataFrame(
        columns=[
            "code",
            "sector_name",
            "name",
            "live_ret_vs_prev_close",
            "live_turnover_ratio_20d",
            "live_volume_ratio_20d",
            "live_turnover",
        ]
    )
    if not ranking_df.empty:
        scan = ranking_df.merge(
            base_df[["code", "name", "sector_name", "sector_constituent_count"]],
            on="code",
            how="left",
            suffixes=("", "_base"),
        )
        scan["sector_name"] = scan["sector_name"].fillna(scan.get("sector_name_base", "")).fillna("").map(_normalize_industry_name)
        scan = scan[scan["sector_name"].astype(str).str.strip() != ""].copy()
        if not scan.empty:
            market_scan_member_count = float(scan["code"].nunique() or 0.0)
            source_totals = {str(key): float(value or 0.0) for key, value in scan.groupby("source_type")["code"].nunique().to_dict().items()}
            source_counts = (
                scan.groupby(["sector_name", "source_type"])["code"]
                .nunique()
                .unstack(fill_value=0)
                .reset_index()
            )
            scan_sector_base = (
                scan.groupby("sector_name", as_index=False)
                .agg(
                    scan_member_count=("code", "nunique"),
                    sector_constituent_count_scan=("sector_constituent_count", "max"),
                )
                .merge(source_counts, on="sector_name", how="left")
            )
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
            sector_base = sector_base.merge(scan_sector_base, on="sector_name", how="left")
            if not merged.empty:
                live_scan = scan[["code", "sector_name"]].drop_duplicates().merge(
                    merged[
                        [
                            "code",
                            "name",
                            "live_ret_vs_prev_close",
                            "live_turnover_ratio_20d",
                            "live_volume_ratio_20d",
                            "live_turnover",
                        ]
                    ],
                    on="code",
                    how="left",
                )
    for column in ["scan_member_count", "price_up_count", "turnover_count", "volume_surge_count", "turnover_surge_count"]:
        if column not in sector_base.columns:
            sector_base[column] = 0.0
        sector_base[column] = _coerce_numeric(sector_base[column]).fillna(0.0)
    scan_source_columns = ["price_up_count", "turnover_count", "volume_surge_count", "turnover_surge_count"]
    market_scan_source_count = float(sum(1 for source_type in ["price_up", "turnover", "volume_surge", "turnover_surge"] if source_totals.get(source_type, 0.0) > 0.0) or 1.0)
    sector_base["signal_breadth_count"] = 0.0
    for column in scan_source_columns:
        sector_base["signal_breadth_count"] += _coerce_numeric(sector_base[column]).fillna(0.0).gt(0).astype(float)
    sector_base["signal_breadth_share"] = _safe_ratio(
        sector_base["signal_breadth_count"],
        pd.Series([market_scan_source_count] * len(sector_base), index=sector_base.index),
    ).fillna(0.0)
    sector_base["max_scan_source_count"] = _coerce_numeric(sector_base[scan_source_columns].max(axis=1)).fillna(0.0)
    live_sector = (
        live_scan.groupby("sector_name", as_index=False)
        .agg(
            breadth_up=("live_ret_vs_prev_close", lambda s: int((_coerce_numeric(s) > 0).sum())),
            breadth_down=("live_ret_vs_prev_close", lambda s: int((_coerce_numeric(s) < 0).sum())),
            median_live_ret=("live_ret_vs_prev_close", "median"),
            turnover_ratio_median=("live_turnover_ratio_20d", "median"),
            live_turnover_total=("live_turnover", "sum"),
        )
    )
    sector_base = sector_base.merge(live_sector, on="sector_name", how="left")
    leader_turnover = live_scan.groupby("sector_name", as_index=False).agg(leader_live_turnover=("live_turnover", "max"))
    sector_base = sector_base.merge(leader_turnover, on="sector_name", how="left")
    for column in ["breadth_up", "breadth_down", "live_turnover_total", "leader_live_turnover"]:
        if column not in sector_base.columns:
            sector_base[column] = 0.0
        sector_base[column] = _coerce_numeric(sector_base[column]).fillna(0.0)
    if "sector_constituent_count_scan" in sector_base.columns:
        sector_base["sector_constituent_count"] = _coerce_numeric(sector_base["sector_constituent_count"]).fillna(_coerce_numeric(sector_base["sector_constituent_count_scan"]))
    sector_base["sector_constituent_count"] = _coerce_numeric(sector_base["sector_constituent_count"]).fillna(sector_base["scan_member_count"]).clip(lower=1.0)
    sector_base["scan_member_count"] = _coerce_numeric(sector_base["scan_member_count"]).fillna(0.0)
    sector_base["scan_coverage"] = _safe_ratio(sector_base["scan_member_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["price_up_share_of_sector"] = _safe_ratio(sector_base["price_up_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["price_up_share_of_market_scan"] = _safe_ratio(sector_base["price_up_count"], pd.Series([source_totals.get("price_up", 0.0)] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["turnover_share_of_sector"] = _safe_ratio(sector_base["turnover_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["turnover_share_of_market_scan"] = _safe_ratio(sector_base["turnover_count"], pd.Series([source_totals.get("turnover", 0.0)] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["volume_surge_share_of_sector"] = _safe_ratio(sector_base["volume_surge_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["volume_surge_share_of_market_scan"] = _safe_ratio(sector_base["volume_surge_count"], pd.Series([source_totals.get("volume_surge", 0.0)] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["turnover_surge_share_of_sector"] = _safe_ratio(sector_base["turnover_surge_count"], sector_base["sector_constituent_count"]).fillna(0.0)
    sector_base["turnover_surge_share_of_market_scan"] = _safe_ratio(sector_base["turnover_surge_count"], pd.Series([source_totals.get("turnover_surge", 0.0)] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["breadth_up_rate"] = _safe_ratio(sector_base["breadth_up"], sector_base["scan_member_count"]).fillna(0.0)
    sector_base["breadth_down_rate"] = _safe_ratio(sector_base["breadth_down"], sector_base["scan_member_count"]).fillna(0.0)
    sector_base["breadth_balance"] = sector_base["breadth_up_rate"] - sector_base["breadth_down_rate"]
    sector_base["breadth_net_rate"] = sector_base["breadth_balance"]
    sector_base["breadth_sample_count"] = (_coerce_numeric(sector_base["breadth_up"]).fillna(0.0) + _coerce_numeric(sector_base["breadth_down"]).fillna(0.0))
    sector_base["breadth_active_coverage"] = _safe_ratio(sector_base["breadth_sample_count"], sector_base["scan_member_count"]).fillna(0.0)
    sector_base["breadth_reliability"] = _safe_ratio(
        sector_base["breadth_sample_count"],
        pd.Series([float(breadth_settings["reliability_k"])] * len(sector_base), index=sector_base.index),
    ).fillna(0.0).clip(lower=0.0, upper=1.0)
    sector_base["breadth"] = sector_base.apply(lambda row: f"{int(row.get('breadth_up', 0) or 0)}:{int(row.get('breadth_down', 0) or 0)}", axis=1)
    sector_base["median_ret"] = _coerce_numeric(sector_base["median_live_ret"])
    sector_base["scan_member_share_of_market_scan"] = _safe_ratio(sector_base["scan_member_count"], pd.Series([market_scan_member_count] * len(sector_base), index=sector_base.index)).fillna(0.0)
    sector_base["scan_member_count_norm"] = _score_percentile(sector_base["scan_member_count"])
    sector_base["source_bias_share"] = _safe_ratio(sector_base["max_scan_source_count"], sector_base["scan_member_count"]).fillna(0.0)
    sector_base["industry_up_rank_norm"] = _score_rank_ascending(sector_base["industry_rank_live"])
    sector_base["leader_concentration_share"] = _safe_ratio(sector_base["leader_live_turnover"], sector_base["live_turnover_total"]).fillna(0.0)
    sector_base["price_up_rate"] = sector_base["price_up_share_of_sector"]
    sector_base["turnover_count_rate"] = sector_base["turnover_share_of_sector"]
    sector_base["volume_surge_rate"] = sector_base["volume_surge_share_of_sector"]
    sector_base["turnover_surge_rate"] = sector_base["turnover_surge_share_of_sector"]
    sector_base["scan_participation_rate"] = sector_base["scan_coverage"]
    sector_base["n"] = sector_base["scan_member_count"].fillna(0.0).astype(int)
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
    weak_up = sector_base["breadth_up_rate"] < float(breadth_settings["penalty_up_rate"])
    weak_balance = sector_base["breadth_balance"] < float(breadth_settings["penalty_balance"])
    weak_sample = sector_base["breadth_sample_count"] < float(breadth_settings["penalty_sample"])
    penalty_hits = weak_up.astype(int) + weak_balance.astype(int) + weak_sample.astype(int)
    sector_base["breadth_penalty_flag"] = penalty_hits.gt(0)
    sector_base["breadth_warning_flag"] = (
        (sector_base["breadth_up_rate"] < float(breadth_settings["warn_up_rate"]))
        | (sector_base["breadth_balance"] < float(breadth_settings["warn_balance"]))
        | (sector_base["breadth_sample_count"] < float(breadth_settings["warn_sample"]))
    )
    sector_base["breadth_penalty"] = 0.0
    sector_base.loc[penalty_hits == 1, "breadth_penalty"] = float(breadth_settings["light_penalty"])
    sector_base.loc[penalty_hits >= 2, "breadth_penalty"] = float(breadth_settings["heavy_penalty"])
    sector_base["concentration_penalty"] = 0.0
    leader_share = _coerce_numeric(sector_base["leader_concentration_share"]).fillna(0.0)
    scan_member_count = _coerce_numeric(sector_base["scan_member_count"]).fillna(0.0)
    sector_base.loc[leader_share >= float(concentration_settings["light_share"]), "concentration_penalty"] = float(concentration_settings["light_penalty"])
    sector_base.loc[leader_share >= float(concentration_settings["heavy_share"]), "concentration_penalty"] = float(concentration_settings["heavy_penalty"])
    micro_sample_mask = (scan_member_count <= 2.0) & (leader_share >= float(concentration_settings["warn_share"]))
    sector_base.loc[micro_sample_mask, "concentration_penalty"] = _coerce_numeric(sector_base.loc[micro_sample_mask, "concentration_penalty"]).fillna(0.0) + float(concentration_settings["micro_sample_penalty"])
    sector_base["intraday_penalty_total"] = 0.0
    sector_base["intraday_sector_score"] = sector_base["intraday_sector_score_raw"]
    sector_base["today_sector_score"] = sector_base["intraday_sector_score"]
    price_strong_cutoff = _coerce_numeric(sector_base["price_block_score"]).quantile(0.75)
    flow_strong_cutoff = _coerce_numeric(sector_base["flow_block_score"]).quantile(0.75)
    participation_strong_cutoff = _coerce_numeric(sector_base["participation_block_score"]).quantile(0.75)
    if pd.isna(price_strong_cutoff):
        price_strong_cutoff = 0.0
    if pd.isna(flow_strong_cutoff):
        flow_strong_cutoff = 0.0
    if pd.isna(participation_strong_cutoff):
        participation_strong_cutoff = 0.0
    strong_price_signal = (
        (_coerce_numeric(sector_base["price_block_score"]).fillna(0.0) > 0.0)
        & (_coerce_numeric(sector_base["price_block_score"]).fillna(0.0) >= float(price_strong_cutoff))
    )
    strong_flow_signal = (
        (_coerce_numeric(sector_base["flow_block_score"]).fillna(0.0) > 0.0)
        & (_coerce_numeric(sector_base["flow_block_score"]).fillna(0.0) >= float(flow_strong_cutoff))
    )
    strong_participation_signal = (
        (_coerce_numeric(sector_base["participation_block_score"]).fillna(0.0) > 0.0)
        & (_coerce_numeric(sector_base["participation_block_score"]).fillna(0.0) >= float(participation_strong_cutoff))
        & (_coerce_numeric(sector_base["signal_breadth_count"]).fillna(0.0) >= 3.0)
        & (_coerce_numeric(sector_base["scan_coverage"]).fillna(0.0) >= 0.30)
    )
    sector_base["rank_shift_limit"] = float(INTRADAY_INDUSTRY_RANK_TETHER["base_shift"])
    sector_base.loc[strong_price_signal & strong_flow_signal, "rank_shift_limit"] = float(INTRADAY_INDUSTRY_RANK_TETHER["strong_shift"])
    sector_base.loc[strong_price_signal & strong_flow_signal & strong_participation_signal, "rank_shift_limit"] = float(INTRADAY_INDUSTRY_RANK_TETHER["very_strong_shift"])
    sector_base["sector_confidence_score"] = 0.0
    sector_base.loc[sector_base["scan_member_count"] >= 5, "sector_confidence_score"] += 1.0
    sector_base.loc[sector_base["scan_coverage"] >= 0.45, "sector_confidence_score"] += 1.0
    sector_base.loc[sector_base["signal_breadth_count"] >= 3, "sector_confidence_score"] += 0.75
    sector_base.loc[sector_base["signal_breadth_share"] >= 0.75, "sector_confidence_score"] += 0.5
    sector_base.loc[strong_price_signal, "sector_confidence_score"] += 0.5
    sector_base.loc[strong_flow_signal, "sector_confidence_score"] += 0.5
    sector_base.loc[(sector_base["industry_up_rank_norm"] >= 0.7) & strong_participation_signal, "sector_confidence_score"] += 0.5
    sector_base["sector_confidence"] = sector_base["sector_confidence_score"].apply(_build_sector_confidence)
    scan_sample_warning_level, scan_sample_warning_label, scan_sample_warning_reason = _scan_sample_warning_details(
        sector_base["scan_member_count"],
        sector_base["scan_coverage"],
    )
    sector_base["scan_sample_warning_level"] = scan_sample_warning_level
    sector_base["scan_sample_warning_flag"] = sector_base["scan_sample_warning_level"].astype(str).str.strip() != ""
    sector_base["scan_sample_warning_reason"] = scan_sample_warning_reason
    sector_base["ranking_breadth_warning_flag"] = (
        (_coerce_numeric(sector_base["signal_breadth_count"]).fillna(0.0) <= 1.0)
        | (_coerce_numeric(sector_base["signal_breadth_share"]).fillna(0.0) < 0.50)
    )
    sector_base["source_bias_warning_flag"] = (
        (_coerce_numeric(sector_base["source_bias_share"]).fillna(0.0) >= 0.80)
        & (_coerce_numeric(sector_base["signal_breadth_count"]).fillna(0.0) <= 2.0)
    ) | (
        (_coerce_numeric(sector_base["scan_member_share_of_market_scan"]).fillna(0.0) < 0.03)
        & (_coerce_numeric(sector_base["scan_member_count"]).fillna(0.0) < 4.0)
    )
    sector_base["sector_caution"] = sector_base.apply(
        lambda row: _build_sector_caution_tags(
            [
                str(scan_sample_warning_label.loc[row.name] or "") if row.name in scan_sample_warning_label.index else "",
                "ランキング広がり弱い" if bool(row.get("ranking_breadth_warning_flag")) else "",
                "source偏りあり" if bool(row.get("source_bias_warning_flag")) else "",
                "業種順位先行" if float(row.get("industry_up_rank_norm", 0.0) or 0.0) >= 0.8 and float(row.get("rank_shift_limit", 0.0) or 0.0) <= float(INTRADAY_INDUSTRY_RANK_TETHER["base_shift"]) else "",
            ]
        ),
        axis=1,
    )
    sector_base = sector_base[sector_base["sector_constituent_count"] >= 3].copy()
    sector_base = sector_base.sort_values(
        ["intraday_sector_score", "price_block_score", "flow_block_score", "participation_block_score"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    sector_base["score_rank"] = range(1, len(sector_base) + 1)
    sector_base["industry_anchor_rank"] = _coerce_numeric(sector_base["industry_rank_live"]).fillna(sector_base["score_rank"])
    sector_base["industry_up_rank"] = _rank_display_series(sector_base["industry_rank_live"], fallback=sector_base["score_rank"])
    rank_floor = sector_base["industry_anchor_rank"] - _coerce_numeric(sector_base["rank_shift_limit"]).fillna(0.0)
    rank_ceiling = sector_base["industry_anchor_rank"] + _coerce_numeric(sector_base["rank_shift_limit"]).fillna(0.0)
    sector_base["tethered_rank"] = _coerce_numeric(sector_base["score_rank"]).fillna(0.0)
    sector_base["tethered_rank"] = sector_base["tethered_rank"].where(sector_base["tethered_rank"] >= rank_floor, rank_floor)
    sector_base["tethered_rank"] = sector_base["tethered_rank"].where(sector_base["tethered_rank"] <= rank_ceiling, rank_ceiling)
    sector_base["industry_up_anchor_rank"] = _rank_display_series(sector_base["industry_anchor_rank"], fallback=sector_base["industry_up_rank"])
    sector_base["industry_up"] = _coerce_numeric(sector_base["industry_up_value"]).fillna(_coerce_numeric(sector_base["industry_up_rank"]))
    sector_base = _apply_true_rank_shift_limits(sector_base)
    return _sort_today_sector_leaderboard_for_display(sector_base)


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
            .sort_values(["persistence_rank", f"sector_rs_vs_topix_{rs_label}"], ascending=[True, False])
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
        frame["persistence_rank"] = range(1, len(frame) + 1)
        return frame

    return {"1w": _build("1w", "sector_rank_1w"), "1m": _build("1m", "sector_rank_1m"), "3m": _build("3m", "sector_rank_3m")}


def _build_sector_representatives(
    merged: pd.DataFrame,
    today_sector_leaderboard: pd.DataFrame,
    *,
    representative_weights_override: dict[str, float] | None = None,
    sort_columns: list[str] | None = None,
    sort_ascending: list[bool] | None = None,
) -> pd.DataFrame:
    if merged.empty or today_sector_leaderboard.empty:
        return pd.DataFrame(columns=["sector_name", "representative_rank", "code", "name", "live_price", "live_ret_vs_prev_close", "live_turnover", "stock_turnover_share_of_sector", "representative_score", "nikkei_search", "material_link"])
    sorted_today_sector_leaderboard = _sort_today_sector_leaderboard_for_display(today_sector_leaderboard)
    top_sector_names = sorted_today_sector_leaderboard.head(10)["sector_name"].astype(str).tolist()
    working = merged[merged["sector_name"].isin(top_sector_names)].copy()
    if working.empty:
        return pd.DataFrame(columns=["sector_name", "representative_rank", "code", "name", "live_price", "live_ret_vs_prev_close", "live_turnover", "stock_turnover_share_of_sector", "representative_score", "nikkei_search", "material_link"])
    sector_scores = sorted_today_sector_leaderboard[["sector_name", "price_block_score", "flow_block_score", "participation_block_score", "intraday_sector_score"]].drop_duplicates("sector_name")
    working = working.merge(sector_scores, on="sector_name", how="left")
    working["sector_live_turnover_total"] = working.groupby("sector_name")["live_turnover"].transform("sum")
    working["stock_turnover_share_of_sector"] = _safe_ratio(working["live_turnover"], working["sector_live_turnover_total"]).fillna(0.0)
    working["representative_score"] = 0.0
    representative_weights = dict(representative_weights_override or REPRESENTATIVE_STOCK_SCORE_WEIGHTS)
    representative_weights.update({
        "stock_turnover_share_of_sector": 1.75 if representative_weights_override is None else representative_weights.get("stock_turnover_share_of_sector", 1.25),
        "flow_block_score": 0.25 if representative_weights_override is None else representative_weights.get("flow_block_score", 0.35),
        "price_block_score": 0.10 if representative_weights_override is None else representative_weights.get("price_block_score", 0.25),
    })
    for column, weight in representative_weights.items():
        working["representative_score"] += _score_percentile(working[column]) * weight
    sort_columns = sort_columns or REPRESENTATIVE_SORT_COLUMNS
    sort_ascending = sort_ascending or REPRESENTATIVE_SORT_ASCENDING
    working = working.sort_values(sort_columns, ascending=sort_ascending).copy()
    working["representative_rank"] = working.groupby("sector_name").cumcount() + 1
    return working[working["representative_rank"] <= 3][["sector_name", "representative_rank", "code", "name", "live_price", "live_ret_vs_prev_close", "live_turnover", "stock_turnover_share_of_sector", "representative_score", "nikkei_search", "material_link"]].reset_index(drop=True)


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


def _build_representative_stocks_map(sector_representatives: pd.DataFrame) -> pd.Series:
    if sector_representatives.empty:
        return pd.Series(dtype=object)
    records_by_sector: dict[str, list[dict[str, Any]]] = {}
    ordered = sector_representatives.sort_values(["sector_name", "representative_rank"], kind="mergesort")
    for sector_name, group in ordered.groupby("sector_name"):
        rows: list[dict[str, Any]] = []
        for _, row in group.head(3).iterrows():
            rows.append(
                {
                    "representative_rank": int(row.get("representative_rank", 0) or 0),
                    "code": str(row.get("code", "") or ""),
                    "name": str(row.get("name", "") or ""),
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
        summary.append(
            {
                "today_rank": int(row.get("today_rank", 0) or 0),
                "sector_name": str(row.get("sector_name", "") or ""),
                "breadth": str(row.get("breadth", "") or ""),
                "sector_confidence": str(row.get("sector_confidence", "") or ""),
                "sector_caution": str(row.get("sector_caution", "") or ""),
            }
        )
    return summary


def _summarize_sector_rank_changes(before: pd.DataFrame, after: pd.DataFrame, *, limit: int = 10) -> list[dict[str, Any]]:
    if before.empty or after.empty:
        return []
    before_map = {str(row.get("sector_name", "") or ""): int(row.get("today_rank", 0) or 0) for _, row in before.iterrows()}
    changes: list[dict[str, Any]] = []
    for _, row in after.head(limit).iterrows():
        sector_name = str(row.get("sector_name", "") or "")
        after_rank = int(row.get("today_rank", 0) or 0)
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
        current_today_rank = int(current_row.get("today_rank", 0) or 0) if current_row is not None else None
        summary.append(
            {
                "sector_name": sector_name,
                "industry_up_rank": int(anchor_rank),
                "industry_up_value": float(industry_row.get("industry_up_value", 0.0) or 0.0) if pd.notna(industry_row.get("industry_up_value")) else None,
                "today_rank": current_today_rank,
                "rank_delta_vs_industry": (current_today_rank - int(anchor_rank)) if current_today_rank is not None else None,
                "tethered_rank": float(current_row.get("tethered_rank", 0.0) or 0.0) if current_row is not None and pd.notna(current_row.get("tethered_rank")) else None,
                "score_rank": int(current_row.get("score_rank", 0) or 0) if current_row is not None and pd.notna(current_row.get("score_rank")) else None,
                "industry_anchor_rank": int(current_row.get("industry_anchor_rank", 0) or 0) if current_row is not None and pd.notna(current_row.get("industry_anchor_rank")) else int(anchor_rank),
                "allowed_shift": int(current_row.get("allowed_shift", 0) or 0) if current_row is not None and pd.notna(current_row.get("allowed_shift")) else None,
                "rank_shift_limit": int(current_row.get("rank_shift_limit", 0) or 0) if current_row is not None and pd.notna(current_row.get("rank_shift_limit")) else None,
                "upshift_blocked_reason": str(current_row.get("upshift_blocked_reason", "") or "") if current_row is not None else "",
                "rank_constraint_applied": bool(current_row.get("rank_constraint_applied", False)) if current_row is not None else False,
                "final_rank_delta": float(current_row.get("final_rank_delta", 0.0) or 0.0) if current_row is not None and pd.notna(current_row.get("final_rank_delta")) else None,
                "scan_member_count": int(current_row.get("scan_member_count", 0) or 0) if current_row is not None else 0,
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
    working.loc[earnings_days >= 7, "candidate_earnings_component_1w"] = 0.2
    working.loc[earnings_days < 3, "candidate_earnings_component_1w"] = -0.8
    working.loc[(earnings_days >= 3) & (earnings_days < 7), "candidate_earnings_component_1w"] = -0.4
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
    working.loc[earnings_days >= 10, "candidate_earnings_component_1m"] = 0.2
    working.loc[earnings_days < 5, "candidate_earnings_component_1m"] = -0.8
    working.loc[(earnings_days >= 5) & (earnings_days < 10), "candidate_earnings_component_1m"] = -0.4
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
                "決算近い" if bool(row.get("earnings_risk_flag")) else "",
                "決算不明" if bool(row.get("earnings_unknown_flag")) else "",
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
                "決算近い" if bool(row.get("earnings_risk_flag")) else "",
                "決算不明" if bool(row.get("earnings_unknown_flag")) else "",
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


def _render_dataframe_or_reason(title: str, frame: pd.DataFrame, *, reason: str, link_columns: bool = False) -> None:
    st.subheader(title)
    if frame.empty:
        st.caption(reason)
        return
    kwargs: dict[str, Any] = {"use_container_width": True, "hide_index": True}
    if link_columns:
        kwargs["column_config"] = {
            "日経で検索": st.column_config.LinkColumn("日経で検索", display_text="日経で検索"),
            "材料リンク": st.column_config.LinkColumn("材料リンク", display_text="リンクを開く"),
        }
    st.dataframe(frame.rename(columns=UI_COLUMN_LABELS), **kwargs)


def _build_earnings_candidate_table_note(base_meta: dict[str, Any] | None) -> str:
    base_meta = base_meta or {}
    status = str(base_meta.get("earnings_dataset_status", "")).strip()
    coverage = int(base_meta.get("earnings_coverage_count", 0) or 0)
    rows_raw = int(base_meta.get("earnings_rows_raw", 0) or 0)
    rows_future = int(base_meta.get("earnings_rows_future_window", 0) or 0)
    min_date = str(base_meta.get("earnings_min_event_date", "") or "").strip()
    max_date = str(base_meta.get("earnings_max_event_date", "") or "").strip()
    if status == "no_forward_events_in_dataset":
        date_part = f" event_date={min_date}" if min_date and min_date == max_date else (f" event_date={min_date}..{max_date}" if min_date or max_date else "")
        return f"決算カレンダーは前方日付を返していません。空欄は全銘柄共通のデータ未提供です。rows_raw={rows_raw} future_window={rows_future}.{date_part}"
    if status == "ok" and coverage > 0:
        return "決算日数の空欄は個別銘柄の未取得です。"
    if status in {"empty_dataset", "missing_required_columns", "bad_request", "network_or_timeout", "auth_or_permission_error", "endpoint_not_found", "unknown_error"}:
        return f"決算カレンダー自体を取得できていません。空欄は全銘柄共通のデータ未提供です。status={status} rows_raw={rows_raw}"
    return ""


TODAY_SECTOR_DISPLAY_COLUMNS = [
    "today_rank",
    "sector_name",
    "leaders",
    "sector_confidence",
    "sector_caution",
    "industry_up_anchor_rank",
    "industry_rank_live",
    "price_block_score",
    "flow_block_score",
    "participation_block_score",
    "scan_member_count",
]

PERSISTENCE_DISPLAY_COLUMNS = [
    "persistence_rank",
    "sector_name",
    "sector_rs_vs_topix",
    "representative_stock",
    "sector_confidence",
    "sector_caution",
]
SWING_1W_DISPLAY_COLUMNS = [
    "candidate_rank_1w",
    "name",
    "sector_name",
    "candidate_quality",
    "entry_fit",
    "selection_reason",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_1w",
    "live_ret_vs_prev_close",
    "live_turnover",
    "earnings_buffer_days",
]
SWING_1M_DISPLAY_COLUMNS = [
    "candidate_rank_1m",
    "name",
    "sector_name",
    "candidate_quality",
    "entry_fit",
    "selection_reason",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_1m",
    "rs_vs_topix_3m",
    "price_vs_ma20_pct",
    "earnings_buffer_days",
    "finance_health_flag",
]


def _prepare_table_view(df: pd.DataFrame, columns: list[str]) -> tuple[pd.DataFrame, list[str]]:
    compatibility_notes: list[str] = []
    if df is None or df.empty:
        return pd.DataFrame(columns=columns), compatibility_notes
    prepared = df.copy()
    string_columns = {"sector_name", "breadth", "leaders", "representative_stock", "name", "candidate_quality", "entry_fit", "selection_reason", "risk_note", "candidate_commentary", "finance_health_flag", "sector_confidence", "sector_caution"}
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


def build_live_snapshot(mode: str, ranking_df: pd.DataFrame, industry_df: pd.DataFrame, board_df: pd.DataFrame, base_df: pd.DataFrame, now_ts: datetime) -> dict[str, Any]:
    merged = base_df.merge(board_df, on="code", how="inner")
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
    for frame in [filtered_ranking_df, stock_base_df, stock_merged]:
        if not frame.empty and "sector_name" in frame.columns:
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
    baseline_today_sector_leaderboard = _sort_today_sector_leaderboard_for_display(baseline_today_sector_leaderboard)
    today_sector_leaderboard = _sort_today_sector_leaderboard_for_display(today_sector_leaderboard)
    baseline_sector_representatives = _build_sector_representatives(
        stock_merged,
        baseline_today_sector_leaderboard,
        representative_weights_override=REPRESENTATIVE_STOCK_SCORE_WEIGHTS_BASELINE,
        sort_columns=REPRESENTATIVE_SORT_COLUMNS_BASELINE,
        sort_ascending=REPRESENTATIVE_SORT_ASCENDING_BASELINE,
    )
    sector_representatives = _build_sector_representatives(stock_merged, today_sector_leaderboard)
    rep_top1 = sector_representatives[sector_representatives.get("representative_rank", pd.Series(dtype=int)).eq(1)].copy() if not sector_representatives.empty else pd.DataFrame()
    rep_map = rep_top1.set_index("sector_name")["name"] if not rep_top1.empty else pd.Series(dtype=str)
    representative_stocks_map = _build_representative_stocks_map(sector_representatives)
    leaders_map = (
        sector_representatives.sort_values(["sector_name", "representative_rank"])
        .groupby("sector_name")["name"]
        .apply(lambda s: " / ".join(s.head(3).astype(str)))
        if not sector_representatives.empty else pd.Series(dtype=str)
    )
    if not today_sector_leaderboard.empty:
        today_sector_leaderboard = today_sector_leaderboard.copy()
        today_sector_leaderboard["representative_stock"] = today_sector_leaderboard["sector_name"].map(rep_map).fillna("")
        today_sector_leaderboard["representative_stocks"] = today_sector_leaderboard["sector_name"].map(representative_stocks_map).apply(lambda value: value if isinstance(value, list) else [])
        today_sector_leaderboard["leaders"] = today_sector_leaderboard["sector_name"].map(leaders_map).fillna(today_sector_leaderboard["representative_stock"])
    baseline_persistence_tables = _build_sector_persistence_tables(base_df, display_base_df=stock_base_df)
    persistence_tables = _build_sector_persistence_tables(base_df, display_base_df=stock_base_df)
    for key in ["1w", "1m", "3m"]:
        if not persistence_tables[key].empty:
            persistence_tables[key] = persistence_tables[key].copy()
            current_representative = persistence_tables[key]["representative_stock"] if "representative_stock" in persistence_tables[key].columns else pd.Series([""] * len(persistence_tables[key]), index=persistence_tables[key].index)
            persistence_tables[key]["representative_stock"] = current_representative.where(current_representative.astype(str).str.strip() != "", persistence_tables[key]["sector_name"].map(rep_map).fillna(""))
            persistence_tables[key]["leaders"] = persistence_tables[key]["sector_name"].map(leaders_map).fillna("")
    baseline_swing_candidates = _build_swing_candidate_tables(
        stock_merged,
        baseline_today_sector_leaderboard,
        baseline_persistence_tables,
        selection_config=SWING_SELECTION_CONFIG_BASELINE,
    )
    swing_candidates = _build_swing_candidate_tables(stock_merged, today_sector_leaderboard, persistence_tables)
    empty_state = {
        "today_sector_leaderboard": "" if not today_sector_leaderboard.empty else "intraday 条件を満たす本命セクターがありません。",
        "sector_persistence_1w": "" if not persistence_tables["1w"].empty else "TOPIX 比 1週継続性を出せるセクターがありません。",
        "sector_persistence_1m": "" if not persistence_tables["1m"].empty else "TOPIX 比 1か月継続性を出せるセクターがありません。",
        "sector_persistence_3m": "" if not persistence_tables["3m"].empty else "TOPIX 比 3か月継続性を出せるセクターがありません。",
        "swing_candidates_1w": "" if not swing_candidates["1w"].empty else "1週間スイング候補の条件を満たす銘柄がありません。",
        "swing_candidates_1m": "" if not swing_candidates["1m"].empty else "1か月スイング候補の条件を満たす銘柄がありません。",
        "swing_buy_candidates_1w": "" if not swing_candidates["buy_1w"].empty else "1週間スイング買い候補はありません。",
        "swing_watch_candidates_1w": "" if not swing_candidates["watch_1w"].empty else "1週間スイング監視候補はありません。",
        "swing_buy_candidates_1m": "" if not swing_candidates["buy_1m"].empty else "1か月スイング買い候補はありません。",
        "swing_watch_candidates_1m": "" if not swing_candidates["watch_1m"].empty else "1か月スイング監視候補はありません。",
        "sector_representatives": "" if not sector_representatives.empty else "今日の本命セクターに紐づく代表銘柄を抽出できませんでした。",
    }
    meta = build_snapshot_meta(mode=mode, generated_at=now_ts, source_profile="local_kabu_jq_yanoshin", includes_kabu=True)
    tuned_top_sector_names = today_sector_leaderboard.head(5)["sector_name"].astype(str).tolist() if not today_sector_leaderboard.empty else []
    industry_anchor_watch_before = _summarize_industry_anchor_positions(industry_df, baseline_today_sector_leaderboard, anchor_ranks=[2, 3, 4])
    industry_anchor_watch_after = _summarize_industry_anchor_positions(industry_df, today_sector_leaderboard, anchor_ranks=[2, 3, 4])
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
    }
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
        "focus_candidates": swing_candidates["1w"],
        "watch_candidates": swing_candidates["1w"],
        "buy_candidates": swing_candidates["1m"],
        "swing_candidates_1w": swing_candidates["1w"],
        "swing_candidates_1m": swing_candidates["1m"],
        "swing_buy_candidates_1w": swing_candidates["buy_1w"],
        "swing_watch_candidates_1w": swing_candidates["watch_1w"],
        "swing_buy_candidates_1m": swing_candidates["buy_1m"],
        "swing_watch_candidates_1m": swing_candidates["watch_1m"],
        "empty_reasons": empty_state,
        "diagnostics": {
            "mode": mode,
            "generated_at": meta["generated_at"],
            "watch_candidate_count": int(len(swing_candidates["1w"])),
            "buy_candidate_count": int(len(swing_candidates["1m"])),
            "center_stock_count": int(len(sector_representatives)),
            "ranking_candidate_count": int(len(filtered_ranking_df)),
            "sector_summary_scope": "full_tse_industry_universe_left_join_scan_adjustments",
            "today_sector_population_basis": "industry_up_full_universe_then_left_join_scan_sector_aggregates",
            "today_sector_population_counts": {
                "industry_universe_count": int(industry_df["sector_name"].astype(str).map(_normalize_industry_name).str.strip().replace("", pd.NA).dropna().nunique()) if not industry_df.empty and "sector_name" in industry_df.columns else 0,
                "leaderboard_sector_count": int(len(today_sector_leaderboard)),
                "scan_sector_count": int(filtered_ranking_df.get("sector_name", pd.Series(dtype=str)).astype(str).str.strip().replace("", pd.NA).dropna().map(_normalize_industry_name).nunique()) if not filtered_ranking_df.empty else 0,
                "scan_zero_sector_count": int(_coerce_numeric(today_sector_leaderboard.get("scan_member_count", pd.Series(dtype="float64"))).fillna(0.0).eq(0.0).sum()) if not today_sector_leaderboard.empty else 0,
            },
            "today_upshift_block_rules": [
                "scan裏付けなし",
                "scan母数極少",
                "source偏りあり",
                "ランキング広がり弱い",
                "信頼度=低",
            ],
            "breadth_scope": "scan_based_caution_live_data_only_for_representatives",
            "today_sector_primary_rank_column": "tethered_rank",
            "today_sector_display_rank_column": "today_rank",
            "today_top_sectors_basis": "today_sector_leaderboard_sorted_by_true_tethered_slot_then_industry_anchor_then_score_rank",
            "today_rank_rule": "today_rank_is_reassigned_as_dense_display_rank_after_final_sector_sort",
            "today_rank_absolute_constraint": "abs(today_rank-industry_anchor_rank)<=allowed_shift and blocked_upshift_sectors_cannot_finish_above_industry_anchor_rank",
            "today_sector_removed_live_inputs": [
                "median_live_ret_norm",
                "turnover_ratio_median_norm",
                "live_turnover_total_norm",
                "breadth_up_rate",
                "breadth_balance",
                "breadth_penalty",
                "concentration_penalty",
            ],
            "today_sector_participation_inputs": [
                "signal_breadth_count",
                "signal_breadth_share",
                "scan_coverage",
                "scan_member_count",
                "scan_member_share_of_market_scan",
            ],
            "scan_sample_warning_rules": {
                "items": ["scan_member_count", "scan_coverage"],
                "no_scan": {
                    "scan_member_count_eq": 0,
                    "label": "scan裏付けなし",
                },
                "critical": {
                    "scan_member_count_lt": int(INTRADAY_SCAN_SAMPLE_WARNING_RULES["critical_count"]),
                    "label": "scan母数極少",
                },
                "warn": {
                    "scan_member_count_lt": int(INTRADAY_SCAN_SAMPLE_WARNING_RULES["warn_count"]),
                    "scan_coverage_lt": float(INTRADAY_SCAN_SAMPLE_WARNING_RULES["warn_coverage"]),
                    "label": "scan母数少",
                },
                "thin": {
                    "scan_member_count_lt": int(INTRADAY_SCAN_SAMPLE_WARNING_RULES["thin_count"]),
                    "scan_coverage_lt": float(INTRADAY_SCAN_SAMPLE_WARNING_RULES["thin_coverage"]),
                    "label": "scan母数薄い",
                },
                "note": "scan_member_count>=10 sectors are not flagged by this rule",
            },
            "today_sector_rank_tether": {
                "anchor": "industry_up",
                "base_shift": int(INTRADAY_INDUSTRY_RANK_TETHER["base_shift"]),
                "strong_shift": int(INTRADAY_INDUSTRY_RANK_TETHER["strong_shift"]),
                "very_strong_shift": int(INTRADAY_INDUSTRY_RANK_TETHER["very_strong_shift"]),
                "rule": "base_plus_minus_1_only_expand_to_2_when_price_and_flow_are_strong_expand_to_3_only_when_price_flow_participation_are_all_strong",
            },
            "industry_anchor_watch_after": industry_anchor_watch_after,
            "includes_kabu": True,
            "non_corporate_products": product_filter_diag["non_corporate_products"],
            "tuning_compare": tuning_compare,
        },
    }


def write_snapshot_bundle(bundle: dict[str, Any], settings: dict[str, Any], *, write_drive: bool = False) -> dict[str, str]:
    markdown_text = bundle_to_markdown(bundle)
    result = write_snapshot_bundle_to_store(
        mode=str(bundle["meta"]["mode"]),
        generated_at=str(bundle["meta"]["generated_at"]),
        json_text=bundle_to_json_text(bundle),
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


def load_saved_snapshot(mode: str, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or get_settings()
    snapshot_path = _snapshot_json_path(mode, settings)
    cached_payload = _load_saved_snapshot_payload_cached(mode, str(snapshot_path), _snapshot_mtime_ns(snapshot_path))
    payload = cached_payload["payload"]
    meta = normalize_snapshot_meta(payload.get("meta", {}))
    snapshot_guard = evaluate_snapshot_guard(mode, meta)
    today_sector_summary = pd.DataFrame(payload.get("today_sector_leaderboard", payload.get("today_sector_summary", payload.get("sector_summary", []))))
    weekly_sector_summary = pd.DataFrame(payload.get("sector_persistence_1w", payload.get("weekly_sector_summary", [])))
    monthly_sector_summary = pd.DataFrame(payload.get("sector_persistence_1m", payload.get("monthly_sector_summary", [])))
    sector_persistence_3m = pd.DataFrame(payload.get("sector_persistence_3m", []))
    sector_representatives = pd.DataFrame(payload.get("sector_representatives", payload.get("center_stocks", payload.get("leaders_by_sector", []))))
    swing_candidates_1w = pd.DataFrame(payload.get("swing_candidates_1w", payload.get("watch_candidates", payload.get("focus_candidates", []))))
    swing_candidates_1m = pd.DataFrame(payload.get("swing_candidates_1m", payload.get("buy_candidates", [])))
    swing_buy_candidates_1w = pd.DataFrame(payload.get("swing_buy_candidates_1w", []))
    swing_watch_candidates_1w = pd.DataFrame(payload.get("swing_watch_candidates_1w", []))
    swing_buy_candidates_1m = pd.DataFrame(payload.get("swing_buy_candidates_1m", []))
    swing_watch_candidates_1m = pd.DataFrame(payload.get("swing_watch_candidates_1m", []))
    if bool(snapshot_guard.get("is_stale")):
        stale_reason = str(snapshot_guard.get("reason", "")).strip()
        today_sector_summary = pd.DataFrame()
        weekly_sector_summary = pd.DataFrame()
        monthly_sector_summary = pd.DataFrame()
        sector_persistence_3m = pd.DataFrame()
        sector_representatives = pd.DataFrame()
        swing_candidates_1w = pd.DataFrame()
        swing_candidates_1m = pd.DataFrame()
        swing_buy_candidates_1w = pd.DataFrame()
        swing_watch_candidates_1w = pd.DataFrame()
        swing_buy_candidates_1m = pd.DataFrame()
        swing_watch_candidates_1m = pd.DataFrame()
        payload_empty_reasons = dict(payload.get("empty_reasons", {}))
        for key in [
            "today_sector_leaderboard",
            "sector_persistence_1w",
            "sector_persistence_1m",
            "sector_persistence_3m",
            "sector_representatives",
            "swing_candidates_1w",
            "swing_candidates_1m",
            "swing_buy_candidates_1w",
            "swing_watch_candidates_1w",
            "swing_buy_candidates_1m",
            "swing_watch_candidates_1m",
            "weekly_sector_summary",
            "monthly_sector_summary",
            "center_stocks",
            "watch_candidates",
            "buy_candidates",
        ]:
            payload_empty_reasons[key] = stale_reason
        payload["empty_reasons"] = payload_empty_reasons
    for frame in [swing_candidates_1w, swing_candidates_1m, sector_representatives]:
        if not frame.empty and "name" in frame.columns:
            frame["nikkei_search"] = frame.apply(lambda row: _make_nikkei_search_link(str(row.get("name", "")), str(row.get("code", ""))), axis=1)
    return {
        "meta": meta,
        "sector_summary": today_sector_summary,
        "today_sector_summary": today_sector_summary,
        "weekly_sector_summary": weekly_sector_summary,
        "monthly_sector_summary": monthly_sector_summary,
        "today_sector_leaderboard": today_sector_summary,
        "sector_persistence_1w": weekly_sector_summary,
        "sector_persistence_1m": monthly_sector_summary,
        "sector_persistence_3m": sector_persistence_3m,
        "leaders_by_sector": sector_representatives,
        "center_stocks": sector_representatives,
        "sector_representatives": sector_representatives,
        "focus_candidates": swing_candidates_1w,
        "watch_candidates": swing_candidates_1w,
        "buy_candidates": swing_candidates_1m,
        "swing_candidates_1w": swing_candidates_1w,
        "swing_candidates_1m": swing_candidates_1m,
        "swing_buy_candidates_1w": swing_buy_candidates_1w if not swing_buy_candidates_1w.empty else swing_candidates_1w[swing_candidates_1w.get("entry_fit", pd.Series(dtype=str)).eq("買い候補")] if not swing_candidates_1w.empty and "entry_fit" in swing_candidates_1w.columns else pd.DataFrame(),
        "swing_watch_candidates_1w": swing_watch_candidates_1w if not swing_watch_candidates_1w.empty else swing_candidates_1w[swing_candidates_1w.get("entry_fit", pd.Series(dtype=str)).eq("監視候補")] if not swing_candidates_1w.empty and "entry_fit" in swing_candidates_1w.columns else pd.DataFrame(),
        "swing_buy_candidates_1m": swing_buy_candidates_1m if not swing_buy_candidates_1m.empty else swing_candidates_1m[swing_candidates_1m.get("entry_fit", pd.Series(dtype=str)).eq("買い候補")] if not swing_candidates_1m.empty and "entry_fit" in swing_candidates_1m.columns else pd.DataFrame(),
        "swing_watch_candidates_1m": swing_watch_candidates_1m if not swing_watch_candidates_1m.empty else swing_candidates_1m[swing_candidates_1m.get("entry_fit", pd.Series(dtype=str)).eq("監視候補")] if not swing_candidates_1m.empty and "entry_fit" in swing_candidates_1m.columns else pd.DataFrame(),
        "empty_reasons": payload.get("empty_reasons", {}),
        "diagnostics": payload.get("diagnostics", {}),
        "snapshot_guard": snapshot_guard,
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
        deep_watch_df, deep_watch_diag = select_deep_watch_universe(ranking_df, base_df, settings, mode)
        board_df, board_diag = enrich_with_board_snapshot(deep_watch_df, base_df, settings, token, mode=mode)
        bundle = build_live_snapshot(mode, ranking_df, industry_df, board_df, base_df, datetime.now(timezone.utc))
        bundle["diagnostics"].update({"base_meta": base_meta, "ranking": ranking_diag, "deep_watch": deep_watch_diag, "board": board_diag, "write_completed": False})
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
    meta = bundle.get("meta", {})
    generated_at_jst = str(meta.get("generated_at_jst", "") or meta.get("generated_at", ""))
    mode = str(meta.get("mode", ""))
    expected_time_label = str(meta.get("expected_time_label", "")).strip()
    timepoint_meaning = _timepoint_meaning(mode)
    empty_reasons = bundle.get("empty_reasons", {})
    snapshot_guard = bundle.get("snapshot_guard", {})
    warning_text = saved_snapshot_timing_warning(meta) if is_saved_snapshot else ""
    today_sector_view, today_sector_notes = _prepare_table_view(bundle.get("today_sector_leaderboard", bundle.get("today_sector_summary", bundle["sector_summary"])), TODAY_SECTOR_DISPLAY_COLUMNS)
    weekly_sector_view, weekly_sector_notes = _prepare_table_view(bundle.get("sector_persistence_1w", bundle.get("weekly_sector_summary", pd.DataFrame())), PERSISTENCE_DISPLAY_COLUMNS)
    monthly_sector_view, monthly_sector_notes = _prepare_table_view(bundle.get("sector_persistence_1m", bundle.get("monthly_sector_summary", pd.DataFrame())), PERSISTENCE_DISPLAY_COLUMNS)
    quarter_sector_view, quarter_sector_notes = _prepare_table_view(bundle.get("sector_persistence_3m", pd.DataFrame()), PERSISTENCE_DISPLAY_COLUMNS)
    swing_1w_view, swing_1w_notes = _prepare_table_view(bundle.get("swing_candidates_1w", pd.DataFrame()), SWING_1W_DISPLAY_COLUMNS)
    swing_1m_view, swing_1m_notes = _prepare_table_view(bundle.get("swing_candidates_1m", pd.DataFrame()), SWING_1M_DISPLAY_COLUMNS)
    base_meta = bundle.get("diagnostics", {}).get("base_meta", {})
    earnings_candidate_note = _build_earnings_candidate_table_note(base_meta)
    sector_compat_notes = sorted(set(today_sector_notes + weekly_sector_notes + monthly_sector_notes + quarter_sector_notes + swing_1w_notes + swing_1m_notes))
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
            if sector_compat_notes:
                st.caption("不足列があるので旧スナップショット互換で補完表示しています。")
    if bool(snapshot_guard.get("is_stale")):
        st.warning(str(snapshot_guard.get("reason", "")).strip() or f"{mode} は本日データなし / stale です。")
    _render_dataframe_or_reason(
        "今日の本命セクター",
        today_sector_view,
        reason=str(empty_reasons.get("today_sector_leaderboard", "intraday 条件を満たす本命セクターがありません。")),
    )
    _render_dataframe_or_reason(
        "今日の本命セクター代表銘柄",
        bundle.get("sector_representatives", bundle.get("center_stocks", bundle["leaders_by_sector"])),
        reason=str(empty_reasons.get("sector_representatives", empty_reasons.get("center_stocks", ""))),
        link_columns=True,
    )
    _render_dataframe_or_reason(
        "1週間主力セクター",
        weekly_sector_view,
        reason=str(empty_reasons.get("sector_persistence_1w", empty_reasons.get("weekly_sector_summary", ""))),
    )
    _render_dataframe_or_reason(
        "1か月主力セクター",
        monthly_sector_view,
        reason=str(empty_reasons.get("sector_persistence_1m", empty_reasons.get("monthly_sector_summary", ""))),
    )
    _render_dataframe_or_reason(
        "3か月主力セクター",
        quarter_sector_view,
        reason=str(empty_reasons.get("sector_persistence_3m", "")),
    )
    if earnings_candidate_note:
        st.caption(earnings_candidate_note)
    _render_dataframe_or_reason(
        "1週間スイング候補銘柄",
        swing_1w_view,
        reason=str(empty_reasons.get("swing_candidates_1w", "")),
        link_columns=True,
    )
    if earnings_candidate_note:
        st.caption(earnings_candidate_note)
    _render_dataframe_or_reason(
        "1か月スイング候補銘柄",
        swing_1m_view,
        reason=str(empty_reasons.get("swing_candidates_1m", "")),
        link_columns=True,
    )
    diagnostics = bundle.get("diagnostics", {})
    if diagnostics:
        with st.expander("diagnostics", expanded=False):
            st.json(diagnostics)


def _render_control_plane_status(settings: dict[str, Any]) -> None:
    st.subheader("更新依頼")
    if st.button("状態を再読込", key="refresh-control-plane-status"):
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
    status_request_mode = str(status_payload.get("request_mode", "")).strip()
    if status_request_mode:
        st.caption(f"status request_mode: {status_request_mode}")
    requested_at = str(request_payload.get("requested_at", "")).strip()
    requested_by = str(request_payload.get("requested_by", "")).strip()
    request_mode = str(request_payload.get("request_mode", "") or "1130").strip() or "1130"
    if request_payload.get("request_update"):
        st.warning(f"更新依頼は受付中です。request_mode={request_mode} requested_at={requested_at or '-'} requested_by={requested_by or '-'}")
        pending_cols = st.columns(2)
        pending_cols[0].button("1130を更新", disabled=True, key="request-1130-disabled", help="すでに依頼済みです")
        pending_cols[1].button("nowを更新", disabled=True, key="request-now-disabled", help="すでに依頼済みです")
        return
    action_cols = st.columns(2)
    if action_cols[0].button("1130を更新", type="primary", key="request-1130", help="control-plane branch に 1130 更新依頼を書き込みます"):
        try:
            submitted, updated_request = submit_control_plane_update_request(token, settings, requested_by="streamlit-cloud-viewer", requested_mode="1130")
            if submitted:
                st.success(f"1130 更新依頼を送信しました。requested_at={updated_request.get('requested_at', '')}")
            else:
                st.info("すでに更新依頼が入っているため、二重依頼は行いませんでした。")
            st.rerun()
        except Exception as exc:
            st.error(f"1130 更新依頼の送信に失敗しました: {exc}")
    if action_cols[1].button("nowを更新", key="request-now", help="control-plane branch に now 更新依頼を書き込みます"):
        try:
            submitted, updated_request = submit_control_plane_update_request(token, settings, requested_by="streamlit-cloud-viewer", requested_mode="now")
            if submitted:
                st.success(f"now 更新依頼を送信しました。requested_at={updated_request.get('requested_at', '')}")
            else:
                st.info("すでに更新依頼が入っているため、二重依頼は行いませんでした。")
            st.rerun()
        except Exception as exc:
            st.error(f"now 更新依頼の送信に失敗しました: {exc}")


def _render_viewer_only_app(settings: dict[str, Any]) -> None:
    st.caption("Cloud viewer-only モードです。保存済み snapshot の表示と更新依頼のみ行います。")
    _enable_viewer_auto_refresh(settings)
    _render_control_plane_status(settings)
    _render_snapshot_cache_admin_tools()
    available_modes = _available_viewer_snapshot_modes(settings)
    if not available_modes:
        st.warning("まだ snapshot がありません")
        st.caption("表示対象: latest_1130.json / latest_1530.json / latest_now.json")
        return
    if len(available_modes) == 1:
        mode = available_modes[0]
        bundle = load_saved_snapshot(mode, settings)
        _render_bundle(bundle, source_label=f"latest_{mode}.json を表示しました", is_saved_snapshot=True)
        return
    tabs = st.tabs([f"{mode}" for mode in available_modes])
    for tab, mode in zip(tabs, available_modes):
        with tab:
            bundle = load_saved_snapshot(mode, settings)
            _render_bundle(bundle, source_label=f"latest_{mode}.json を表示しました", is_saved_snapshot=True)


def render_app() -> None:
    st.set_page_config(page_title="Sector Strength Live", layout="wide")
    st.title("セクター強度ライブ")
    settings = get_settings()
    if _is_streamlit_cloud():
        st.caption("Cloud では viewer-only で動作します。保存済み snapshot の表示と更新依頼だけを行います。")
        st.info("latest_1130.json / latest_1530.json / latest_now.json を優先して読み込みます。Cloud では collector / kabu live 取得は実行しません。")
        _render_viewer_only_app(settings)
        return
    st.caption("J-Quants を土台に、kabu ステーション API のライブデータを重ねてスナップショットを作成・表示します。")
    st.info("過去の任意時点をあとから再取得することはできません。保存済みスナップショットのみ再表示できます。")
    view_mode = st.radio("表示方法", ["A: ライブでスナップショットを作成", "B: 保存済みスナップショットを表示"], index=0)
    mode = st.selectbox("表示モード", ["0915", "1130", "1530", "now"], index=0)
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


if __name__ == "__main__":
    render_app()
