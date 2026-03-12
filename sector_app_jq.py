import os
import re
import time
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import unicodedata
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

import pandas as pd
import requests
import streamlit as st

BASE_URL = "https://api.jquants.com/v2"
JST = ZoneInfo("Asia/Tokyo")
FREE_NEWS_FEEDS = [
    {"source": "JPX", "url": "https://www.jpx.co.jp/rss/news_release.xml"},
    {"source": "PR TIMES", "url": "https://prtimes.jp/companyrdf.php"},
]

st.set_page_config(page_title="日本株セクター強弱（J-Quants版）", layout="wide")
st.title("日本株セクター強弱（J-Quants版）")
st.caption("前日までの土台として、S33業種の1週・1か月強弱、売買代金上位、相対強弱を表示します。")

api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
if not api_key:
    st.error("環境変数 JQUANTS_API_KEY が見つかりません。")
    st.stop()


def pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"想定カラムが見つかりません。存在カラム: {list(df.columns)}")


def pick_optional_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def jquants_get_all(path: str, params: dict) -> list[dict]:
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
    }
    url = f"{BASE_URL}{path}"

    rows_all: list[dict] = []
    pagination_key = None

    while True:
        p = dict(params)
        if pagination_key:
            p["pagination_key"] = pagination_key

        r = requests.get(url, headers=headers, params=p, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(
                f"J-Quants API error: status={r.status_code}, url={r.url}, body={r.text[:500]}"
            )

        js = r.json()
        rows = js.get("data", [])
        if isinstance(rows, list):
            rows_all.extend(rows)

        pagination_key = js.get("pagination_key")
        if not pagination_key:
            break

        time.sleep(0.03)

    return rows_all


@st.cache_data(ttl=3600)
def get_recent_trading_dates(n: int = 260) -> list[str]:
    today_jst = datetime.now(JST).date()
    start_date = (today_jst - timedelta(days=max(n * 2, 420))).isoformat()
    end_date = today_jst.isoformat()

    rows = jquants_get_all("/markets/calendar", {"from": start_date, "to": end_date})
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("markets/calendar から営業日を取得できませんでした。")

    date_col = pick_first_existing(df, ["Date", "date"])
    trading_flag_col = pick_optional_existing(
        df,
        ["HolidayDivisionName", "HolidayDivision", "IsTradingDay", "TradingDay", "isTradingDay"],
    )

    cal = df.copy()
    cal[date_col] = pd.to_datetime(cal[date_col], errors="coerce")
    cal = cal.dropna(subset=[date_col]).copy()
    cal = cal[cal[date_col].dt.dayofweek < 5].copy()

    if trading_flag_col:
        flag = cal[trading_flag_col]
        if pd.api.types.is_bool_dtype(flag):
            cal = cal[flag]
        else:
            normalized = flag.astype(str).str.strip().str.lower()
            trading_tokens = {
                "1", "true", "t", "yes", "y", "trading", "business", "open",
                "営業日", "立会日", "取引日", "平日",
            }
            holiday_tokens = {
                "0", "false", "f", "no", "n", "holiday", "closed",
                "休日", "休場日", "非営業日", "土曜", "日曜", "祝日",
                "saturday", "sunday",
            }
            has_trading_token = normalized.apply(
                lambda s: any(token in s for token in trading_tokens)
            )
            has_holiday_token = normalized.apply(
                lambda s: any(token in s for token in holiday_tokens)
            )
            if has_trading_token.any():
                cal = cal[has_trading_token]
            elif has_holiday_token.any():
                cal = cal[~has_holiday_token]

    trading_dates = (
        cal[date_col]
        .dt.strftime("%Y-%m-%d")
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    if len(trading_dates) < n:
        raise RuntimeError(f"営業日を十分に取得できませんでした。取得件数={len(trading_dates)}")

    return trading_dates[-n:]


@st.cache_data(ttl=3600)
def get_master_df(date_str: str) -> pd.DataFrame:
    rows = jquants_get_all("/equities/master", {"date": date_str})
    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError("銘柄マスターが空です。")

    if "Code" not in df.columns:
        raise RuntimeError(f"銘柄マスターに Code 列がありません。columns={list(df.columns)}")

    df["Code"] = df["Code"].astype(str).str.zfill(5)

    if "MktNm" in df.columns:
        df = df[df["MktNm"].isin(["プライム", "スタンダード", "グロース"])]

    if "S33Nm" in df.columns:
        df = df[df["S33Nm"].notna() & (df["S33Nm"].astype(str).str.strip() != "")]
    else:
        raise RuntimeError("銘柄マスターに S33Nm 列がありません。")

    keep_cols = [c for c in ["Code", "CoName", "S33", "S33Nm", "MktNm", "ScaleCat"] if c in df.columns]
    df = df[keep_cols].drop_duplicates(subset=["Code"]).copy()

    return df


@st.cache_data(ttl=3600)
def get_price_df(date_str: str, allow_empty: bool = False) -> pd.DataFrame:
    rows = jquants_get_all("/equities/bars/daily", {"date": date_str})
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["Code", "Date", "Close", "TradingValue"])

    if "Code" not in df.columns:
        raise RuntimeError(f"株価データに Code 列がありません。columns={list(df.columns)}")

    df["Code"] = df["Code"].astype(str).str.zfill(5)

    close_col = pick_first_existing(df, ["AdjC", "C"])
    value_col = pick_optional_existing(df, ["Va"])

    keep_cols = ["Code", "Date", close_col]
    if value_col:
        keep_cols.append(value_col)

    out = df[keep_cols].copy()
    out = out.rename(columns={close_col: "Close"})
    out["Close"] = pd.to_numeric(out["Close"], errors="coerce")

    if value_col:
        out = out.rename(columns={value_col: "TradingValue"})
        out["TradingValue"] = pd.to_numeric(out["TradingValue"], errors="coerce")
    else:
        out["TradingValue"] = pd.NA

    out = out.dropna(subset=["Close"]).drop_duplicates(subset=["Code"])
    return out


def add_sector_relative_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sector_mean_1w"] = out.groupby("S33Nm")["ret_1w"].transform("mean")
    out["sector_mean_1m"] = out.groupby("S33Nm")["ret_1m"].transform("mean")
    out["rel_1w"] = out["ret_1w"] - out["sector_mean_1w"]
    out["rel_1m"] = out["ret_1m"] - out["sector_mean_1m"]
    return out


def resolve_latest_price_snapshot(
    trading_dates: list[str], max_lookback_days: int = 5
) -> tuple[int, str, pd.DataFrame]:
    lookback = min(max_lookback_days, len(trading_dates))
    search_dates = trading_dates[-lookback:]

    for idx in range(len(search_dates) - 1, -1, -1):
        date_str = search_dates[idx]
        price_df = get_price_df(date_str)
        if not price_df.empty:
            return len(trading_dates) - lookback + idx, date_str, price_df

    raise RuntimeError(
        f"価格配信済み最新営業日を特定できませんでした。探索範囲={search_dates[0]}..{search_dates[-1]}"
    )


def resolve_price_available_base_date(
    trading_dates: list[str],
    candidate_idx: int,
    label: str,
    max_lookback_days: int = 5,
) -> tuple[str, pd.DataFrame]:
    lookback = min(max_lookback_days, candidate_idx + 1)

    for idx in range(candidate_idx, candidate_idx - lookback, -1):
        date_str = trading_dates[idx]
        price_df = get_price_df(date_str, allow_empty=True)
        if not price_df.empty:
            return date_str, price_df

    raise RuntimeError(f"{label}比較の価格配信済み基準日が見つかりません。")


def build_sector_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    trading_dates = get_recent_trading_dates(n=260)
    latest_idx, latest_date, px_latest_raw = resolve_latest_price_snapshot(trading_dates, max_lookback_days=5)

    if latest_idx < 21:
        raise RuntimeError(f"比較に必要な営業日数が不足しています。 latest_idx={latest_idx}")

    week_candidate_idx = latest_idx - 5
    month_candidate_idx = latest_idx - 21
    week_base_date, px_week_raw = resolve_price_available_base_date(
        trading_dates,
        week_candidate_idx,
        "1週",
        max_lookback_days=5,
    )
    month_base_date, px_month_raw = resolve_price_available_base_date(
        trading_dates,
        month_candidate_idx,
        "1か月",
        max_lookback_days=5,
    )

    master = get_master_df(latest_date)

    px_latest = px_latest_raw.rename(
        columns={"Date": "Date_latest", "Close": "Close_latest", "TradingValue": "TradingValue_latest"}
    )
    px_week = px_week_raw.rename(columns={"Close": "Close_week"})
    px_month = px_month_raw.rename(columns={"Close": "Close_month"})

    merged = master.merge(px_latest[["Code", "Date_latest", "Close_latest", "TradingValue_latest"]], on="Code", how="inner")
    merged = merged.merge(px_week[["Code", "Close_week"]], on="Code", how="inner")
    merged = merged.merge(px_month[["Code", "Close_month"]], on="Code", how="inner")

    merged["ret_1w"] = (merged["Close_latest"] / merged["Close_week"] - 1.0) * 100.0
    merged["ret_1m"] = (merged["Close_latest"] / merged["Close_month"] - 1.0) * 100.0
    merged["TradingValue_latest"] = pd.to_numeric(merged["TradingValue_latest"], errors="coerce").fillna(0)

    merged = merged.replace([float("inf"), float("-inf")], pd.NA)
    merged = merged.dropna(subset=["ret_1w", "ret_1m"]).copy()
    merged = add_sector_relative_columns(merged)

    merged["売買代金合計(億円)_tmp"] = merged["TradingValue_latest"] / 100000000

    def summarize(col: str) -> pd.DataFrame:
        return (
            merged.groupby(["S33", "S33Nm"], dropna=False)
            .agg(
                **{
                    "銘柄数": ("Code", "nunique"),
                    "平均騰落率": (col, "mean"),
                    "中央値騰落率": (col, "median"),
                    "上昇銘柄比率": (col, lambda s: (s > 0).mean() * 100.0),
                    "売買代金合計_億円": ("売買代金合計(億円)_tmp", "sum"),
                }
            )
            .reset_index()
            .sort_values(["平均騰落率", "売買代金合計_億円"], ascending=[False, False])
            .reset_index(drop=True)
        )

    week_summary = summarize("ret_1w")
    month_summary = summarize("ret_1m")

    meta = {
        "latest_date": latest_date,
        "week_base_date": week_base_date,
        "month_base_date": month_base_date,
        "fresh_52w_min_date": trading_dates[max(0, latest_idx - 1)],
        "master_count": len(master),
        "merged_count": len(merged),
    }

    return merged, week_summary, month_summary, meta


def format_sector_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["平均騰落率", "中央値騰落率", "上昇銘柄比率", "売買代金合計_億円"]:
        out[c] = out[c].round(2)
    return out


def make_turnover_table(sec: pd.DataFrame) -> pd.DataFrame:
    out = sec[["Code", "CoName", "MktNm", "ScaleCat", "TradingValue_latest", "ret_1w", "ret_1m"]].copy()
    out["売買代金(億円)"] = (out["TradingValue_latest"] / 100000000).round(2)
    out = out.rename(columns={"ret_1w": "1週騰落率", "ret_1m": "1か月騰落率"})
    out["1週騰落率"] = out["1週騰落率"].round(2)
    out["1か月騰落率"] = out["1か月騰落率"].round(2)
    out = out.drop(columns=["TradingValue_latest"])
    out = out.sort_values("売買代金(億円)", ascending=False).reset_index(drop=True)
    return out


def make_relative_table(sec: pd.DataFrame, rel_col: str, ret_col: str, label: str) -> pd.DataFrame:
    out = sec[["Code", "CoName", "MktNm", "ScaleCat", ret_col, rel_col, "TradingValue_latest"]].copy()
    out = out.rename(columns={ret_col: label, rel_col: "セクター差"})
    out[label] = out[label].round(2)
    out["セクター差"] = out["セクター差"].round(2)
    out["売買代金(億円)"] = (out["TradingValue_latest"] / 100000000).round(2)
    out = out.drop(columns=["TradingValue_latest"])
    return out


def build_reversal_candidates(merged: pd.DataFrame, week_summary: pd.DataFrame) -> pd.DataFrame:
    week_sector_rank = (
        week_summary[["S33Nm", "平均騰落率"]]
        .reset_index()
        .rename(columns={"index": "セクター1週順位", "平均騰落率": "セクター1週平均騰落率"})
    )
    week_sector_rank["セクター1週順位"] = week_sector_rank["セクター1週順位"] + 1
    sector_rank_threshold = min(10, len(week_summary))

    out = merged.merge(week_sector_rank, on="S33Nm", how="left")
    out["売買代金(億円)"] = out["TradingValue_latest"] / 100000000

    # 逆行高候補:
    # 1) 上位セクターではない、またはセクター1週平均がマイナス/横ばい
    # 2) 個別の1週騰落率はプラス
    # 3) 1週セクター差が十分大きい
    # 4) 1か月では大きく崩れていない
    # 5) 売買代金は5億円以上
    reversal_mask = (
        ((out["セクター1週順位"] > sector_rank_threshold) | (out["セクター1週平均騰落率"] <= 0))
        & (out["ret_1w"] > 0)
        & (out["rel_1w"] >= 3.0)
        & (out["ret_1m"] >= -10.0)
        & (out["売買代金(億円)"] >= 5.0)
    )
    out = out.loc[reversal_mask].copy()

    out = out.rename(
        columns={
            "ret_1w": "1週騰落率",
            "ret_1m": "1か月騰落率",
            "rel_1w": "1週セクター差",
        }
    )

    out["セクター1週順位"] = out["セクター1週順位"].astype(int)
    out["セクター1週平均騰落率"] = out["セクター1週平均騰落率"].round(2)
    out["1週騰落率"] = out["1週騰落率"].round(2)
    out["1か月騰落率"] = out["1か月騰落率"].round(2)
    out["1週セクター差"] = out["1週セクター差"].round(2)
    out["売買代金(億円)"] = out["売買代金(億円)"].round(2)

    cols = [
        "Code",
        "CoName",
        "S33Nm",
        "MktNm",
        "ScaleCat",
        "セクター1週順位",
        "セクター1週平均騰落率",
        "1週騰落率",
        "1週セクター差",
        "1か月騰落率",
        "売買代金(億円)",
    ]
    return (
        out[cols]
        .sort_values(["1週セクター差", "1週騰落率", "売買代金(億円)"], ascending=[False, False, False])
        .reset_index(drop=True)
    )


def render_sector_spotlight(
    summary_df: pd.DataFrame,
    sec_df: pd.DataFrame,
    title: str,
    ret_col: str,
    rel_col: str,
    ret_label: str,
) -> None:
    st.subheader(title)

    top_sectors = summary_df.head(3)
    for _, row in top_sectors.iterrows():
        sector_name = row["S33Nm"]
        sector_sec = sec_df.loc[sec_df["S33Nm"] == sector_name].copy()
        leaders = make_turnover_table(sector_sec).head(10)
        strong = (
            make_relative_table(sector_sec, rel_col, ret_col, ret_label)
            .sort_values(["セクター差", "売買代金(億円)"], ascending=[False, False])
            .head(10)
            .reset_index(drop=True)
        )

        st.markdown(
            f"**{sector_name}**  "
            f"{ret_label}平均 {row['平均騰落率']:.2f}% / "
            f"上昇銘柄比率 {row['上昇銘柄比率']:.2f}% / "
            f"売買代金合計 {row['売買代金合計_億円']:.2f} 億円"
        )
        col1, col2 = st.columns(2)
        with col1:
            st.caption("主力株（売買代金上位）")
            st.dataframe(leaders, use_container_width=True, height=320)
        with col2:
            st.caption("相対的に強い銘柄")
            st.dataframe(strong, use_container_width=True, height=320)


def make_all_table(sec: pd.DataFrame) -> pd.DataFrame:
    out = sec[[
        "Code", "CoName", "MktNm", "ScaleCat",
        "TradingValue_latest", "ret_1w", "ret_1m", "rel_1w", "rel_1m"
    ]].copy()

    out["売買代金(億円)"] = (out["TradingValue_latest"] / 100000000).round(2)

    out = out.rename(
        columns={
            "ret_1w": "1週騰落率",
            "ret_1m": "1か月騰落率",
            "rel_1w": "1週セクター差",
            "rel_1m": "1か月セクター差",
        }
    )

    for c in ["1週騰落率", "1か月騰落率", "1週セクター差", "1か月セクター差"]:
        out[c] = out[c].round(2)

    out = out.drop(columns=["TradingValue_latest"])
    out = out.sort_values("1週騰落率", ascending=False).reset_index(drop=True)
    return out


def _xml_local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _child_text_by_localnames(elem: ET.Element, localnames: list[str]) -> str:
    wanted = set(localnames)
    for child in list(elem):
        if _xml_local_name(child.tag) in wanted:
            text = "".join(child.itertext()).strip()
            if text:
                return text
    return ""


def _parse_feed_datetime(value: str) -> pd.Timestamp:
    text = (value or "").strip()
    if not text:
        return pd.NaT

    parsed = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.isna(parsed):
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            return pd.NaT
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        return pd.Timestamp(dt).tz_convert(JST)

    return parsed.tz_convert(JST)


def _previous_weekday(value: str | datetime | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(JST)
    else:
        ts = ts.tz_convert(JST)

    prev = ts - pd.Timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= pd.Timedelta(days=1)
    return prev


@st.cache_data(ttl=300, show_spinner=False)
def fetch_free_news_items(max_items_per_feed: int = 40) -> pd.DataFrame:
    headers = {"User-Agent": "sector-strength-app/1.0"}
    rows: list[dict] = []

    for feed in FREE_NEWS_FEEDS:
        try:
            response = requests.get(feed["url"], headers=headers, timeout=15)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception:
            continue

        items: list[ET.Element] = []
        for elem in root.iter():
            if _xml_local_name(elem.tag) in {"item", "entry"}:
                items.append(elem)

        for item in items[:max_items_per_feed]:
            title = _child_text_by_localnames(item, ["title"])
            link = _child_text_by_localnames(item, ["link", "id"])
            summary = _child_text_by_localnames(item, ["description", "summary", "content"])
            published_text = _child_text_by_localnames(
                item,
                ["pubDate", "published", "updated", "dc:date", "date"],
            )

            if not link:
                for child in list(item):
                    if _xml_local_name(child.tag) == "link":
                        href = child.attrib.get("href", "").strip()
                        if href:
                            link = href
                            break

            rows.append(
                {
                    "source": feed["source"],
                    "title": title,
                    "link": link,
                    "published_at": _parse_feed_datetime(published_text),
                    "summary": summary,
                }
            )

    if not rows:
        return pd.DataFrame(columns=["source", "title", "link", "published_at", "summary"])

    out = pd.DataFrame(rows, columns=["source", "title", "link", "published_at", "summary"])
    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce")
    out = out.sort_values("published_at", ascending=False, na_position="last").reset_index(drop=True)
    return out


def select_915_free_news(
    news_df: pd.DataFrame,
    latest_date: str | datetime | pd.Timestamp,
    previous_trading_date: str | datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    if news_df is None:
        return pd.DataFrame(columns=["source", "title", "link", "published_at", "summary"])

    required_cols = ["source", "title", "link", "published_at", "summary"]
    missing_cols = [col for col in required_cols if col not in news_df.columns]
    if missing_cols:
        return news_df.iloc[0:0].copy() if isinstance(news_df, pd.DataFrame) else pd.DataFrame(columns=required_cols)

    if news_df.empty:
        return news_df.iloc[0:0].copy()

    out = news_df[required_cols].copy()
    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce")
    if pd.api.types.is_datetime64tz_dtype(out["published_at"]):
        out["published_at"] = out["published_at"].dt.tz_convert(JST)
    else:
        out["published_at"] = out["published_at"].apply(
            lambda x: x.tz_localize(JST) if pd.notna(x) and x.tzinfo is None else x
        )

    out = out.dropna(subset=["published_at"]).copy()
    if out.empty:
        return news_df.iloc[0:0][required_cols].copy()

    latest_ts = pd.Timestamp(latest_date)
    if latest_ts.tzinfo is None:
        latest_ts = latest_ts.tz_localize(JST)
    else:
        latest_ts = latest_ts.tz_convert(JST)
    latest_day = latest_ts.normalize()

    if previous_trading_date is None:
        previous_day = _previous_weekday(latest_day).normalize()
    else:
        previous_day = pd.Timestamp(previous_trading_date)
        if previous_day.tzinfo is None:
            previous_day = previous_day.tz_localize(JST)
        else:
            previous_day = previous_day.tz_convert(JST)
        previous_day = previous_day.normalize()

    window_start = previous_day + pd.Timedelta(hours=15)
    window_end = latest_day + pd.Timedelta(hours=9, minutes=15)

    out = out.loc[(out["published_at"] >= window_start) & (out["published_at"] <= window_end)].copy()
    if out.empty:
        return news_df.iloc[0:0][required_cols].copy()

    out = out.drop_duplicates(subset=["source", "title", "link"], keep="first")
    out = out.sort_values("published_at", ascending=False).reset_index(drop=True)
    return out


def prepare_915_news_preview(news_df: pd.DataFrame, max_rows: int = 15) -> pd.DataFrame:
    preview_cols = ["時刻", "ソース", "タイトル", "要約", "link"]
    required_cols = ["source", "title", "link", "published_at", "summary"]

    if news_df is None or not isinstance(news_df, pd.DataFrame):
        return pd.DataFrame(columns=preview_cols)

    missing_cols = [col for col in required_cols if col not in news_df.columns]
    if missing_cols or news_df.empty:
        return pd.DataFrame(columns=preview_cols)

    out = news_df[required_cols].copy()
    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce")
    if pd.api.types.is_datetime64tz_dtype(out["published_at"]):
        out["published_at"] = out["published_at"].dt.tz_convert(JST)
    else:
        out["published_at"] = out["published_at"].apply(
            lambda x: x.tz_localize(JST) if pd.notna(x) and x.tzinfo is None else x
        )

    out = out.dropna(subset=["published_at"]).copy()
    if out.empty:
        return pd.DataFrame(columns=preview_cols)

    def _shorten(text: str, limit: int) -> str:
        s = "" if pd.isna(text) else str(text).strip()
        if len(s) <= limit:
            return s
        return s[: max(0, limit - 1)].rstrip() + "…"

    out = out.sort_values("published_at", ascending=False).head(max_rows).copy()
    out["時刻"] = out["published_at"].dt.strftime("%m-%d %H:%M")
    out["ソース"] = out["source"].fillna("").astype(str)
    out["タイトル"] = out["title"].apply(lambda x: _shorten(x, 80))
    out["要約"] = out["summary"].apply(lambda x: _shorten(x, 120))
    out["link"] = out["link"].fillna("").astype(str)
    return out[preview_cols].reset_index(drop=True)


def _normalize_match_text(value: str) -> str:
    text = "" if pd.isna(value) else str(value)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"https?://\S+|www\.\S+", " ", text, flags=re.IGNORECASE)
    text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = " ".join(text.split())
    return text.strip()


def _find_equity_background_candidates(
    news_df: pd.DataFrame,
    merged: pd.DataFrame,
    top_n_per_news: int = 3,
    week_summary: pd.DataFrame | None = None,
    reversal_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    cols = [
        "published_at",
        "source",
        "title",
        "link",
        "candidate_type",
        "candidate_name",
        "candidate_code",
        "candidate_sector",
        "reason",
        "_score",
    ]
    if not isinstance(news_df, pd.DataFrame) or news_df.empty:
        return pd.DataFrame(columns=cols)
    if not isinstance(merged, pd.DataFrame) or merged.empty:
        return pd.DataFrame(columns=cols)

    required_news_cols = ["source", "title", "link", "published_at", "summary"]
    if any(col not in news_df.columns for col in required_news_cols):
        return pd.DataFrame(columns=cols)

    def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
        for name in candidates:
            if name in df.columns:
                return name
        return None

    code_col = _pick_col(merged, ["Code", "code", "Ticker", "銘柄コード"])
    name_col = _pick_col(merged, ["CoName", "CompanyName", "Name", "銘柄名"])
    sector_col = _pick_col(merged, ["S33Nm", "Sector33Name", "SectorName", "33業種", "33業種名"])
    if not code_col or not name_col or not sector_col:
        return pd.DataFrame(columns=cols)

    noisy_company_words = {
        "news",
        "holdings",
        "group",
        "capital",
        "partners",
        "japan",
        "pr",
        "market",
    }

    top_sectors: set[str] = set()
    if isinstance(week_summary, pd.DataFrame) and "S33Nm" in week_summary.columns:
        top_sectors = set(week_summary.head(5)["S33Nm"].dropna().astype(str).tolist())

    reversal_codes: set[str] = set()
    if isinstance(reversal_df, pd.DataFrame):
        reversal_code_col = _pick_col(reversal_df, ["Code", "コード", "candidate_code"])
        if reversal_code_col:
            reversal_codes = set(reversal_df[reversal_code_col].dropna().astype(str).tolist())

    equities = (
        merged[[code_col, name_col, sector_col]]
        .dropna(subset=[name_col])
        .drop_duplicates()
        .copy()
    )
    equities["match_name"] = equities[name_col].map(_normalize_match_text)
    equities["match_name_lower"] = equities["match_name"].str.lower()
    equities["is_ascii_name"] = equities["match_name"].map(lambda s: bool(s) and all(ord(ch) < 128 for ch in s))
    equities = equities[equities["match_name"].str.len() >= 3].copy()
    equities = equities[
        ~(
            equities["is_ascii_name"]
            & equities["match_name_lower"].isin(noisy_company_words)
        )
    ].copy()

    sector_map = (
        merged[[sector_col]]
        .dropna()
        .drop_duplicates()
        .copy()
    )
    sector_map["match_sector"] = sector_map[sector_col].map(_normalize_match_text)
    sector_map = sector_map[sector_map["match_sector"].str.len() >= 2].copy()

    rows: list[dict] = []
    for news in news_df[required_news_cols].itertuples(index=False):
        normalized_title = _normalize_match_text(news.title)
        normalized_summary = _normalize_match_text(news.summary)
        text = f"{normalized_title} {normalized_summary}".strip()
        if not text:
            continue
        text_lower = text.lower()

        news_rows: dict[tuple[str, str, str], dict] = {}

        for eq in equities.to_dict("records"):
            match_name = eq["match_name"]
            if not match_name:
                continue

            matched = False
            if eq["is_ascii_name"]:
                pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(eq['match_name_lower'])}(?![A-Za-z0-9])")
                matched = bool(pattern.search(text_lower))
            else:
                matched = match_name in text

            if matched:
                candidate_code = str(eq[code_col]).strip()
                candidate_sector = str(eq[sector_col]).strip()
                reason_parts = ["社名一致"]
                score = 100.0
                if candidate_sector in top_sectors:
                    reason_parts.append("上位セクター")
                    score += 15.0
                if candidate_code in reversal_codes:
                    reason_parts.append("逆行高候補")
                    score += 12.0
                key = ("社名一致", str(eq[name_col]).strip(), candidate_code)
                news_rows[key] = {
                    "published_at": news.published_at,
                    "source": news.source,
                    "title": news.title,
                    "link": news.link,
                    "candidate_type": "社名一致",
                    "candidate_name": eq[name_col],
                    "candidate_code": candidate_code,
                    "candidate_sector": candidate_sector,
                    "reason": " + ".join(reason_parts),
                    "_score": score,
                }

        for sec in sector_map.to_dict("records"):
            if sec["match_sector"] and sec["match_sector"] in text:
                candidate_sector = str(sec[sector_col]).strip()
                reason_parts = ["業種一致"]
                score = 60.0
                if candidate_sector in top_sectors:
                    reason_parts.append("上位セクター")
                    score += 15.0
                key = ("業種一致", candidate_sector, "")
                if key not in news_rows:
                    news_rows[key] = {
                        "published_at": news.published_at,
                        "source": news.source,
                        "title": news.title,
                        "link": news.link,
                        "candidate_type": "業種一致",
                        "candidate_name": candidate_sector,
                        "candidate_code": "",
                        "candidate_sector": candidate_sector,
                        "reason": " + ".join(reason_parts),
                        "_score": score,
                    }

        if not news_rows:
            continue

        published_ts = pd.to_datetime(news.published_at, errors="coerce")
        if pd.notna(published_ts):
            now_jst = pd.Timestamp.now(tz=JST)
            if published_ts.tzinfo is None:
                published_ts = published_ts.tz_localize(JST)
            else:
                published_ts = published_ts.tz_convert(JST)
            age_hours = max((now_jst - published_ts).total_seconds() / 3600.0, 0.0)
            recency_bonus = max(0.0, 6.0 - min(age_hours, 6.0))
        else:
            recency_bonus = 0.0

        ranked_rows = []
        for row in news_rows.values():
            row["_score"] += recency_bonus
            ranked_rows.append(row)

        ranked_rows.sort(key=lambda row: (-row["_score"], row["candidate_name"]))
        rows.extend(ranked_rows[:top_n_per_news])

    if not rows:
        return pd.DataFrame(columns=cols)

    out = pd.DataFrame(rows, columns=cols)
    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce")
    out = out.drop_duplicates(
        subset=["source", "title", "link", "candidate_type", "candidate_name", "candidate_code"],
        keep="first",
    )
    out = out.sort_values(["_score", "published_at"], ascending=[False, False], na_position="last").reset_index(drop=True)
    return out


def prepare_background_candidates_preview(candidates_df: pd.DataFrame, max_rows: int = 12) -> pd.DataFrame:
    preview_cols = ["時刻", "種別", "候補", "コード", "33業種", "根拠", "タイトル", "link"]
    required_cols = [
        "published_at",
        "source",
        "title",
        "link",
        "candidate_type",
        "candidate_name",
        "candidate_code",
        "candidate_sector",
        "reason",
        "_score",
    ]

    if not isinstance(candidates_df, pd.DataFrame) or candidates_df.empty:
        return pd.DataFrame(columns=preview_cols)
    if any(col not in candidates_df.columns for col in required_cols):
        return pd.DataFrame(columns=preview_cols)

    out = candidates_df[required_cols].copy()
    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce")
    if pd.api.types.is_datetime64tz_dtype(out["published_at"]):
        out["published_at"] = out["published_at"].dt.tz_convert(JST)
    else:
        out["published_at"] = out["published_at"].apply(
            lambda x: x.tz_localize(JST) if pd.notna(x) and x.tzinfo is None else x
        )
    out = out.dropna(subset=["published_at"]).copy()
    if out.empty:
        return pd.DataFrame(columns=preview_cols)

    def _shorten(text: str, limit: int) -> str:
        s = "" if pd.isna(text) else str(text).strip()
        if len(s) <= limit:
            return s
        return s[: max(0, limit - 1)].rstrip() + "…"

    out = out.sort_values("published_at", ascending=False).head(max_rows).copy()
    out["時刻"] = out["published_at"].dt.strftime("%m-%d %H:%M")
    out["種別"] = out["candidate_type"].fillna("").astype(str)
    out["候補"] = out["candidate_name"].fillna("").astype(str)
    out["コード"] = out["candidate_code"].fillna("").astype(str)
    out["33業種"] = out["candidate_sector"].fillna("").astype(str)
    out["根拠"] = out["reason"].fillna("").astype(str)
    out["タイトル"] = out["title"].apply(lambda x: _shorten(x, 80))
    out["link"] = out["link"].fillna("").astype(str)
    return out[preview_cols].reset_index(drop=True)


@st.cache_data(ttl=3600, show_spinner=False)
def load_52w_cache(path: str = "data/sector_52w_cache.csv.gz") -> pd.DataFrame:
    cols = [
        "Code",
        "Date",
        "Close",
        "High",
        "Trailing52wHigh",
        "DistTo52wHighPct",
        "IsNear52wHigh",
        "IsNew52wHigh",
    ]
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols)

    try:
        df = pd.read_csv(path, dtype={"Code": str}, compression="infer", encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=cols)

    if any(col not in df.columns for col in cols):
        return pd.DataFrame(columns=cols)

    out = df[cols].copy()
    out["Code"] = out["Code"].astype(str).str.zfill(5)
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ["Close", "High", "Trailing52wHigh", "DistTo52wHighPct"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["IsNear52wHigh", "IsNew52wHigh"]:
        out[col] = (
            out[col]
            .map(lambda v: str(v).strip().lower() in {"true", "1", "yes"})
            .fillna(False)
        )
    return out.dropna(subset=["Code", "Date"]).reset_index(drop=True)


def prepare_52w_high_candidates(cache_df: pd.DataFrame, merged: pd.DataFrame) -> pd.DataFrame:
    preview_cols = ["コード", "銘柄", "33業種", "終値", "52週高値", "高値乖離率%", "判定"]
    required_cache_cols = [
        "Code",
        "Date",
        "Close",
        "Trailing52wHigh",
        "DistTo52wHighPct",
        "IsNear52wHigh",
        "IsNew52wHigh",
    ]
    if not isinstance(cache_df, pd.DataFrame) or cache_df.empty:
        return pd.DataFrame(columns=preview_cols)
    if any(col not in cache_df.columns for col in required_cache_cols):
        return pd.DataFrame(columns=preview_cols)
    if not isinstance(merged, pd.DataFrame) or merged.empty:
        return pd.DataFrame(columns=preview_cols)
    if any(col not in merged.columns for col in ["Code", "CoName", "S33Nm"]):
        return pd.DataFrame(columns=preview_cols)

    meta_df = merged[["Code", "CoName", "S33Nm"]].drop_duplicates(subset=["Code"]).copy()
    out = cache_df.merge(meta_df, on="Code", how="inner")
    out = out.loc[out["IsNear52wHigh"] | out["IsNew52wHigh"]].copy()
    if out.empty:
        return pd.DataFrame(columns=preview_cols)

    out["判定"] = out["IsNew52wHigh"].map({True: "52週高値更新", False: "52週高値まで3%以内"})
    out["終値"] = pd.to_numeric(out["Close"], errors="coerce").round(2)
    out["52週高値"] = pd.to_numeric(out["Trailing52wHigh"], errors="coerce").round(2)
    out["高値乖離率%"] = pd.to_numeric(out["DistTo52wHighPct"], errors="coerce").round(2)
    out = out.rename(columns={"Code": "コード", "CoName": "銘柄", "S33Nm": "33業種"})
    out = out.sort_values(["IsNew52wHigh", "DistTo52wHighPct"], ascending=[False, False]).reset_index(drop=True)
    return out[preview_cols].head(20)


try:
    merged, week_summary, month_summary, meta = build_sector_tables()
except Exception as e:
    st.exception(e)
    st.stop()

reversal_candidates = build_reversal_candidates(merged, week_summary)

st.subheader("採用した営業日")
st.write({
    "最新営業日": meta["latest_date"],
    "1週比較の基準日": meta["week_base_date"],
    "1か月比較の基準日": meta["month_base_date"],
    "マスター銘柄数": meta["master_count"],
    "比較可能銘柄数": meta["merged_count"],
})
st.info(f"J-Quantsベース部分は価格配信済み最新営業日ベースです。現在の基準日: {meta['latest_date']}")

st.subheader("9:15速報（無料ソース）")
try:
    free_news = fetch_free_news_items()
    news_915 = select_915_free_news(
        free_news,
        latest_date=meta["latest_date"],
        previous_trading_date=None,
    )
    preview_915 = prepare_915_news_preview(news_915, max_rows=12)

    st.caption("対象: 前営業日15:00〜当日9:15")
    st.caption(f"取得件数: {len(free_news)} / 9:15対象件数: {len(news_915)}")
    if preview_915.empty:
        st.caption("該当ニュースはありません")
    else:
        st.dataframe(
            preview_915,
            use_container_width=True,
            height=360,
            hide_index=True,
            column_config={
                "link": st.column_config.LinkColumn("link"),
            },
        )
except Exception:
    st.caption("無料ニュースの取得に失敗しました")

st.subheader("背景材料候補（初期版）")
st.caption("根拠: 社名一致 / 業種名一致。上位セクター・逆行高候補を優先表示")
try:
    background_candidates = _find_equity_background_candidates(
        news_915,
        merged,
        week_summary=week_summary,
        reversal_df=reversal_candidates,
    )
    background_preview = prepare_background_candidates_preview(background_candidates, max_rows=12)
    st.caption(f"背景材料候補件数: {len(background_candidates)}")
    if background_preview.empty:
        st.caption("該当する背景材料候補はありません")
    else:
        st.dataframe(
            background_preview,
            use_container_width=True,
            height=360,
            hide_index=True,
            column_config={
                "link": st.column_config.LinkColumn("link"),
            },
        )
except Exception:
    st.caption("背景材料候補の抽出に失敗しました")

st.subheader("52週高値圏")
try:
    cache_52w = load_52w_cache()
    if cache_52w.empty:
        st.caption("52週高値圏キャッシュがありません")
    else:
        cache_latest_date = pd.to_datetime(cache_52w["Date"], errors="coerce").dropna().max()
        fresh_min_date = pd.Timestamp(meta["fresh_52w_min_date"])
        if pd.isna(cache_latest_date) or cache_latest_date < fresh_min_date:
            st.caption("52週高値圏キャッシュが未更新です")
        else:
            candidates_52w = prepare_52w_high_candidates(cache_52w, merged)
            if candidates_52w.empty:
                st.caption("該当する52週高値圏銘柄はありません")
            else:
                st.dataframe(candidates_52w, use_container_width=True, height=420, hide_index=True)
except Exception:
    st.caption("52週高値圏キャッシュの読込に失敗しました")

top3 = week_summary.head(3)["S33Nm"].tolist()
st.subheader("1週強弱 上位3業種")
st.write(" / ".join(top3))

render_sector_spotlight(
    week_summary,
    merged,
    "1週上位セクターの主力株と相対強銘柄",
    "ret_1w",
    "rel_1w",
    "1週騰落率",
)
render_sector_spotlight(
    month_summary,
    merged,
    "1か月上位セクターの主力株と相対強銘柄",
    "ret_1m",
    "rel_1m",
    "1か月騰落率",
)

st.subheader("逆行高候補（初期版）")
st.caption(
    "上位セクターではない、または弱いセクターの中で、個別が1週で逆行高している候補"
)
st.write(f"候補件数: {len(reversal_candidates)}")
if reversal_candidates.empty:
    st.info("条件に合う逆行高候補はありません。")
else:
    st.dataframe(reversal_candidates.head(30), use_container_width=True, height=420)

tab1, tab2 = st.tabs(["1週強弱", "1か月強弱"])

with tab1:
    st.subheader("S33業種 1週強弱")
    show_week = format_sector_summary(week_summary)
    st.dataframe(show_week, use_container_width=True, height=520)

    sector_list = show_week["S33Nm"].tolist()
    selected_week_sector = st.selectbox("1週で中身を見る業種", sector_list, key="week_sector")

    sec = merged.loc[merged["S33Nm"] == selected_week_sector].copy()

    sub1, sub2, sub3, sub4 = st.tabs(["全銘柄", "売買代金上位", "相対的に強い", "相対的に弱い"])

    with sub1:
        st.dataframe(make_all_table(sec), use_container_width=True, height=520)

    with sub2:
        st.dataframe(make_turnover_table(sec).head(20), use_container_width=True, height=520)

    with sub3:
        strong_1w = make_relative_table(sec, "rel_1w", "ret_1w", "1週騰落率")
        strong_1w = strong_1w.sort_values("セクター差", ascending=False).reset_index(drop=True)
        st.dataframe(strong_1w.head(20), use_container_width=True, height=520)

    with sub4:
        weak_1w = make_relative_table(sec, "rel_1w", "ret_1w", "1週騰落率")
        weak_1w = weak_1w.sort_values("セクター差", ascending=True).reset_index(drop=True)
        st.dataframe(weak_1w.head(20), use_container_width=True, height=520)

with tab2:
    st.subheader("S33業種 1か月強弱")
    show_month = format_sector_summary(month_summary)
    st.dataframe(show_month, use_container_width=True, height=520)

    sector_list = show_month["S33Nm"].tolist()
    selected_month_sector = st.selectbox("1か月で中身を見る業種", sector_list, key="month_sector")

    sec = merged.loc[merged["S33Nm"] == selected_month_sector].copy()

    sub1, sub2, sub3, sub4 = st.tabs(["全銘柄", "売買代金上位", "相対的に強い", "相対的に弱い"])

    with sub1:
        st.dataframe(make_all_table(sec), use_container_width=True, height=520)

    with sub2:
        st.dataframe(make_turnover_table(sec).head(20), use_container_width=True, height=520)

    with sub3:
        strong_1m = make_relative_table(sec, "rel_1m", "ret_1m", "1か月騰落率")
        strong_1m = strong_1m.sort_values("セクター差", ascending=False).reset_index(drop=True)
        st.dataframe(strong_1m.head(20), use_container_width=True, height=520)

    with sub4:
        weak_1m = make_relative_table(sec, "rel_1m", "ret_1m", "1か月騰落率")
        weak_1m = weak_1m.sort_values("セクター差", ascending=True).reset_index(drop=True)
        st.dataframe(weak_1m.head(20), use_container_width=True, height=520)

with st.expander("診断表示"):
    st.write("Python:", os.sys.executable)
    st.write("APIキー先頭4文字:", api_key[:4])
    st.write("APIキー末尾4文字:", api_key[-4:])
