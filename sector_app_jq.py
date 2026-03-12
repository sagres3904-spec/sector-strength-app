import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

BASE_URL = "https://api.jquants.com/v2"
JST = ZoneInfo("Asia/Tokyo")

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
