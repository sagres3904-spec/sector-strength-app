import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None


BASE_URL = "https://api.jquants.com/v2"
ROOT_DIR = Path(__file__).resolve().parent
SETTINGS_PATH = ROOT_DIR / "config" / "settings.toml"
MODE_SCORE_WEIGHTS = {
    "0915": {"live_ret_from_open": 1.5, "live_ret_vs_prev_close": 1.2, "gap_pct": 1.4, "live_volume_ratio_20d": 1.1, "live_turnover_ratio_20d": 1.3, "ret_1w": 0.6, "ret_1m": 0.5, "material_score": 0.3},
    "1130": {"live_ret_from_open": 1.4, "live_ret_vs_prev_close": 1.0, "morning_strength": 1.2, "live_volume_ratio_20d": 1.1, "live_turnover_ratio_20d": 1.3, "ret_1w": 0.6, "ret_1m": 0.5, "material_score": 0.3},
    "1530": {"live_ret_from_open": 1.3, "live_ret_vs_prev_close": 1.2, "closing_strength": 1.2, "high_close_score": 1.0, "live_volume_ratio_20d": 1.0, "live_turnover_ratio_20d": 1.2, "ret_1w": 0.7, "ret_1m": 0.6, "material_score": 0.3},
    "now": {"live_ret_from_open": 1.4, "live_ret_vs_prev_close": 1.1, "gap_pct": 1.0, "live_volume_ratio_20d": 1.1, "live_turnover_ratio_20d": 1.3, "ret_1w": 0.6, "ret_1m": 0.5, "material_score": 0.3},
}
FOCUS_CANDIDATE_COLUMNS = [
    "code",
    "name",
    "sector_name",
    "live_price",
    "live_ret_vs_prev_close",
    "live_ret_from_open",
    "live_volume",
    "avg_volume_20d",
    "live_turnover",
    "avg_turnover_20d",
    "live_volume_ratio_20d",
    "live_turnover_ratio_20d",
    "ret_1w",
    "ret_1m",
    "52w_flag",
    "material_title",
    "focus_reason",
    "total_score",
    "nikkei_search",
    "material_link",
]

logger = logging.getLogger("jq_snapshot_core")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class JQuantsAuthError(RuntimeError):
    pass


def _short_body(text: str, limit: int = 160) -> str:
    return " ".join(str(text or "").split())[:limit]


def _read_settings_toml() -> dict[str, Any]:
    if tomllib is None or not SETTINGS_PATH.exists():
        return {}
    with SETTINGS_PATH.open("rb") as fh:
        return tomllib.load(fh)


def get_settings() -> dict[str, Any]:
    settings = {
        "JQUANTS_API_KEY": "",
        "SNAPSHOT_BACKEND": "local",
        "SNAPSHOT_LOCAL_DIR": "data/snapshots",
        "SNAPSHOT_GCS_BUCKET": "",
        "SNAPSHOT_GCS_PREFIX": "sector-app/snapshots",
        "SNAPSHOT_OUTPUT_DIR": "data/snapshots",
    }
    settings.update(_read_settings_toml())
    for key in list(settings.keys()):
        env_value = os.environ.get(key)
        if env_value is not None:
            settings[key] = env_value
    return settings


def get_api_key(settings: dict[str, Any] | None = None) -> str:
    settings = settings or get_settings()
    api_key = str(os.environ.get("JQUANTS_API_KEY", "")).strip() or str(settings.get("JQUANTS_API_KEY", "")).strip()
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


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    response = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout)
    if response.status_code in {401, 403}:
        raise JQuantsAuthError(f"J-Quants authentication failed (401/403). The API key is invalid or expired. body={_short_body(response.text)}")
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP error status={response.status_code} url={response.url} body={_short_body(response.text)}")
    return response.json() if response.text else {}


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


def get_recent_trading_dates(n: int = 260, *, api_key: str | None = None, as_of_date: str | None = None) -> list[str]:
    logger.info("get_recent_trading_dates start n=%s as_of_date=%s", n, as_of_date or "")
    end_date = datetime.fromisoformat(as_of_date).date() if as_of_date else date.today()
    start_date = (end_date - timedelta(days=max(n * 2, 420))).isoformat()
    rows = jquants_get_all("/markets/calendar", {"from": start_date, "to": end_date.isoformat()}, api_key=api_key)
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
    return out.drop_duplicates(subset=["code"]).reset_index(drop=True)


def get_price_df(date_str: str, *, api_key: str | None = None) -> pd.DataFrame:
    df = pd.DataFrame(jquants_get_all("/equities/bars/daily", {"date": date_str}, api_key=api_key))
    if df.empty:
        return pd.DataFrame(columns=["code", "date", "close", "open", "high", "low", "volume", "turnover"])
    df["code"] = df["Code"].astype(str).map(_normalize_code4)
    df = df[df["code"].map(_is_code4)].copy()
    close_col = pick_first_existing(df, ["AdjClose", "AdjustmentClose", "Close", "AdjC", "C"])
    open_col = pick_optional_existing(df, ["AdjOpen", "AdjustmentOpen", "Open", "AdjO", "O"])
    high_col = pick_optional_existing(df, ["AdjHigh", "AdjustmentHigh", "High", "AdjH", "H"])
    low_col = pick_optional_existing(df, ["AdjLow", "AdjustmentLow", "Low", "AdjL", "L"])
    volume_col = pick_optional_existing(df, ["Volume", "Vo", "Vol", "V"])
    turnover_col = pick_optional_existing(df, ["TurnoverValue", "TradingValue", "Va"])
    out = pd.DataFrame(
        {
            "code": df["code"],
            "date": date_str,
            "close": _coerce_numeric(df[close_col]),
            "open": _coerce_numeric(df[open_col]) if open_col else pd.NA,
            "high": _coerce_numeric(df[high_col]) if high_col else pd.NA,
            "low": _coerce_numeric(df[low_col]) if low_col else pd.NA,
            "volume": _coerce_numeric(df[volume_col]) if volume_col else pd.NA,
            "turnover": _coerce_numeric(df[turnover_col]) if turnover_col else pd.NA,
        }
    )
    return out.dropna(subset=["close"]).drop_duplicates(subset=["code"]).reset_index(drop=True)


def get_price_history(trading_dates: list[str], *, api_key: str | None = None, lookback_days: int = 80) -> pd.DataFrame:
    date_list = trading_dates[-lookback_days:]
    frames: list[pd.DataFrame] = []
    for date_str in date_list:
        frames.append(get_price_df(date_str, api_key=api_key))
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["code", "date", "close", "open", "high", "low", "volume", "turnover"])
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out.dropna(subset=["date"]).sort_values(["code", "date"]).reset_index(drop=True)


def build_daily_base_data(*, fast_check: bool = False, as_of_date: str | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    logger.info("build_daily_base_data start fast_check=%s as_of_date=%s", fast_check, as_of_date or "")
    api_key = get_api_key()
    trading_dates = get_recent_trading_dates(n=40 if fast_check else 260, api_key=api_key, as_of_date=as_of_date)
    master_df = get_master_df(trading_dates[-1], api_key=api_key)
    price_history = get_price_history(trading_dates, api_key=api_key, lookback_days=25 if fast_check else 80)
    for column in ["close", "open", "high", "low", "volume", "turnover"]:
        price_history[column] = _coerce_numeric(price_history[column])
    grouped = price_history.groupby("code", group_keys=False)
    price_history["avg_volume_20d"] = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price_history["avg_turnover_20d"] = grouped["turnover"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price_history["high_20d"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).max())
    latest = grouped.tail(1).rename(
        columns={
            "close": "close_latest",
            "open": "open_latest",
            "high": "high_latest",
            "low": "low_latest",
            "volume": "volume_latest",
            "turnover": "turnover_latest",
            "date": "latest_date",
        }
    )
    prev_day = grouped.nth(-2).reset_index()[["code", "close"]].rename(columns={"close": "prev_close"})
    week = grouped.nth(-6).reset_index()[["code", "close"]].rename(columns={"close": "close_1w"})
    month = grouped.nth(-21).reset_index()[["code", "close"]].rename(columns={"close": "close_1m"})
    base = master_df.merge(
        latest[
            [
                "code",
                "close_latest",
                "open_latest",
                "high_latest",
                "low_latest",
                "volume_latest",
                "turnover_latest",
                "latest_date",
                "avg_volume_20d",
                "avg_turnover_20d",
                "high_20d",
            ]
        ],
        on="code",
        how="inner",
    )
    base = base.merge(prev_day, on="code", how="left").merge(week, on="code", how="left").merge(month, on="code", how="left")
    base["ret_1w"] = (base["close_latest"] / base["close_1w"] - 1.0) * 100.0
    base["ret_1m"] = (base["close_latest"] / base["close_1m"] - 1.0) * 100.0
    base["sector_ret_1w"] = base.groupby("sector_name", dropna=False)["ret_1w"].transform("mean")
    base["rel_1w"] = base["ret_1w"] - base["sector_ret_1w"]
    sector_rank = base.groupby("sector_name", dropna=False)["ret_1w"].mean().sort_values(ascending=False).reset_index()
    sector_rank["sector_rank_1w"] = range(1, len(sector_rank) + 1)
    base = base.merge(sector_rank[["sector_name", "sector_rank_1w"]], on="sector_name", how="left")
    base["TradingValue_latest"] = _coerce_numeric(base["turnover_latest"]).fillna(0.0)
    base["is_near_52w_high"] = False if fast_check else (base["close_latest"] >= base["high_20d"] * 0.97)
    base["is_new_52w_high"] = False if fast_check else (base["close_latest"] >= base["high_20d"])
    base["reversal_candidates"] = (base["ret_1w"] > 0) & (base["ret_1m"] < 0)
    base["material_title"] = "未取得"
    base["material_link"] = ""
    base["material_score"] = 0.0
    base["latest_date"] = pd.to_datetime(base["latest_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return base, {"latest_date": max(trading_dates), "trading_date_count": len(trading_dates), "lookback_days": 25 if fast_check else 80, "fast_check": fast_check}


def _score_percentile(series: pd.Series) -> pd.Series:
    numeric = _coerce_numeric(series)
    if numeric.dropna().empty:
        return pd.Series([0.0] * len(series), index=series.index)
    return numeric.rank(pct=True, ascending=True).fillna(0.0)


def _make_nikkei_search_link(name: str) -> str:
    name = str(name or "").strip()
    if not name:
        return ""
    from urllib.parse import quote_plus

    return f"https://www.nikkei.com/search?keyword={quote_plus(name)}"


def build_jq_yanoshin_snapshot(mode: str, base_df: pd.DataFrame, *, now_ts: datetime, source_profile: str, snapshot_backend: str) -> dict[str, Any]:
    if mode not in MODE_SCORE_WEIGHTS:
        raise ValueError(f"unsupported mode: {mode}")
    merged = base_df.copy()
    merged["live_price"] = _coerce_numeric(merged["close_latest"])
    merged["prev_close"] = _coerce_numeric(merged["prev_close"]).fillna(_coerce_numeric(merged["close_latest"]))
    merged["open_price"] = _coerce_numeric(merged["open_latest"]).fillna(merged["live_price"])
    merged["high_price"] = _coerce_numeric(merged["high_latest"]).fillna(merged["live_price"])
    merged["low_price"] = _coerce_numeric(merged["low_latest"]).fillna(merged["live_price"])
    merged["live_volume"] = _coerce_numeric(merged["volume_latest"])
    merged["live_turnover"] = _coerce_numeric(merged["turnover_latest"])
    merged["live_price_time"] = merged["latest_date"].astype(str) + "T15:30:00"
    merged["live_ret_vs_prev_close"] = (merged["live_price"] / merged["prev_close"] - 1.0) * 100.0
    merged["live_ret_from_open"] = (merged["live_price"] / merged["open_price"] - 1.0) * 100.0
    merged["gap_pct"] = (merged["open_price"] / merged["prev_close"] - 1.0) * 100.0
    merged["live_volume_ratio_20d"] = merged["live_volume"] / merged["avg_volume_20d"].replace(0, pd.NA)
    merged["live_turnover_ratio_20d"] = merged["live_turnover"] / merged["avg_turnover_20d"].replace(0, pd.NA)
    merged["morning_strength"] = merged["live_ret_from_open"]
    merged["closing_strength"] = merged["live_ret_vs_prev_close"]
    merged["high_close_score"] = 1 - ((merged["high_price"] - merged["live_price"]) / merged["high_price"].replace(0, pd.NA))
    merged["total_score"] = 0.0
    for column, weight in MODE_SCORE_WEIGHTS[mode].items():
        merged["total_score"] += _score_percentile(merged[column]) * weight
    merged["focus_reason"] = merged.apply(
        lambda row: ", ".join(
            filter(
                None,
                [
                    f"sector:{row.get('sector_name', '')}" if pd.notna(row.get("sector_name")) else "",
                    "daily_close_based",
                    "turnover_breakout" if float(row.get("live_turnover_ratio_20d", 0) or 0) >= 1.5 else "",
                    "volume_breakout" if float(row.get("live_volume_ratio_20d", 0) or 0) >= 1.5 else "",
                    "near_52w_high" if bool(row.get("is_near_52w_high")) else "",
                ],
            )
        )
        or "daily_strength",
        axis=1,
    )
    merged["nikkei_search"] = merged["name"].map(_make_nikkei_search_link)
    sector_summary = (
        merged.groupby("sector_name", as_index=False)
        .agg(
            live_sector_ret=("live_ret_vs_prev_close", "mean"),
            live_sector_turnover_score=("live_turnover_ratio_20d", "mean"),
            sector_rank_1w=("sector_rank_1w", "min"),
            leaders=("name", lambda s: ", ".join(s.head(3))),
        )
        .sort_values(["live_sector_ret", "live_sector_turnover_score"], ascending=[False, False])
        .reset_index(drop=True)
    )
    sector_summary["industry_rank_live"] = pd.NA
    leaders_by_sector = merged.sort_values("total_score", ascending=False).groupby("sector_name", as_index=False).head(3)[["sector_name", "code", "name", "live_price", "live_ret_vs_prev_close", "live_turnover", "total_score"]]
    focus_candidates = merged.sort_values("total_score", ascending=False).copy()
    focus_candidates["52w_flag"] = focus_candidates.apply(lambda row: "new_high" if bool(row.get("is_new_52w_high")) else ("near_high" if bool(row.get("is_near_52w_high")) else ""), axis=1)
    return {
        "meta": {
            "generated_at": now_ts.isoformat(),
            "mode": mode,
            "source_profile": source_profile,
            "includes_kabu": False,
            "snapshot_backend": snapshot_backend,
        },
        "sector_summary": sector_summary,
        "leaders_by_sector": leaders_by_sector,
        "focus_candidates": focus_candidates[FOCUS_CANDIDATE_COLUMNS].head(30).reset_index(drop=True),
        "diagnostics": {
            "mode": mode,
            "generated_at": now_ts.isoformat(),
            "focus_candidate_count": int(len(focus_candidates)),
            "ranking_candidate_count": 0,
            "includes_kabu": False,
        },
    }
