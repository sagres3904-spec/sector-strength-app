import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

BASE_URL = "https://api.jquants.com/v2"
JST = ZoneInfo("Asia/Tokyo")
OUTPUT_PATH = Path("data/sector_52w_cache.csv.gz")
TRADING_DAYS_WINDOW = 260
LATEST_PRICE_LOOKBACK_DAYS = 5
DEFAULT_PAGE_SLEEP_SECONDS = 0.03
DAILY_BARS_PAGE_SLEEP_SECONDS = 0.08
DAILY_BARS_POST_FETCH_SLEEP_SECONDS = 0.08
DAILY_BARS_MAX_RETRIES = 5
RETRY_BACKOFF_CAP_SECONDS = 16.0


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


def get_retry_delay_seconds(
    response: requests.Response | None,
    attempt: int,
    cap_seconds: float = RETRY_BACKOFF_CAP_SECONDS,
) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After", "").strip()
        if retry_after:
            try:
                return min(max(float(retry_after), 1.0), cap_seconds)
            except ValueError:
                pass
    return min(float(2 ** attempt), cap_seconds)


def jquants_get_all(
    path: str,
    params: dict,
    api_key: str,
    max_retries: int = 3,
    page_sleep_seconds: float = DEFAULT_PAGE_SLEEP_SECONDS,
) -> list[dict]:
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
        "User-Agent": "sector-strength-app-52w-batch/1.0",
    }
    url = f"{BASE_URL}{path}"

    rows_all: list[dict] = []
    pagination_key = None

    while True:
        request_params = dict(params)
        if pagination_key:
            request_params["pagination_key"] = pagination_key

        last_error: Exception | None = None
        response = None
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=request_params, timeout=30)
                if response.status_code == 200:
                    break
                if response.status_code in {429, 500, 502, 503, 504} and attempt < max_retries - 1:
                    time.sleep(get_retry_delay_seconds(response, attempt))
                    continue
                response.raise_for_status()
            except requests.RequestException as exc:
                last_error = exc
                if attempt < max_retries - 1:
                    time.sleep(get_retry_delay_seconds(response, attempt))
                    continue
                raise RuntimeError(f"J-Quants API request failed: path={path}, params={request_params}") from exc

        if response is None:
            raise RuntimeError(f"J-Quants API request failed without response: path={path}, params={request_params}")

        if response.status_code != 200:
            date_str = request_params.get("date")
            message = response.text[:500] if response.text else str(last_error)
            raise RuntimeError(
                f"J-Quants API error: path={path}, date={date_str}, status={response.status_code}, "
                f"url={response.url}, body={message}"
            )

        js = response.json()
        rows = js.get("data", [])
        if isinstance(rows, list):
            rows_all.extend(rows)

        pagination_key = js.get("pagination_key")
        if not pagination_key:
            break

        time.sleep(page_sleep_seconds)

    return rows_all


def get_recent_trading_dates(api_key: str, n: int = 260) -> list[str]:
    today_jst = datetime.now(JST).date()
    start_date = (today_jst - timedelta(days=max(n * 2, 420))).isoformat()
    end_date = today_jst.isoformat()

    rows = jquants_get_all("/markets/calendar", {"from": start_date, "to": end_date}, api_key)
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
            has_trading_token = normalized.apply(lambda s: any(token in s for token in trading_tokens))
            has_holiday_token = normalized.apply(lambda s: any(token in s for token in holiday_tokens))
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


def get_master_df(api_key: str, date_str: str) -> pd.DataFrame:
    rows = jquants_get_all("/equities/master", {"date": date_str}, api_key)
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
    keep_cols = [c for c in ["Code", "CoName", "S33Nm"] if c in df.columns]
    return df[keep_cols].drop_duplicates(subset=["Code"]).copy()


def get_price_df(api_key: str, date_str: str) -> pd.DataFrame:
    rows = jquants_get_all(
        "/equities/bars/daily",
        {"date": date_str},
        api_key,
        max_retries=DAILY_BARS_MAX_RETRIES,
        page_sleep_seconds=DAILY_BARS_PAGE_SLEEP_SECONDS,
    )
    time.sleep(DAILY_BARS_POST_FETCH_SLEEP_SECONDS)
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["Code", "Date", "Close", "High"])
    if "Code" not in df.columns:
        raise RuntimeError(f"株価データに Code 列がありません。columns={list(df.columns)}")

    df["Code"] = df["Code"].astype(str).str.zfill(5)
    close_col = pick_first_existing(df, ["AdjC", "C"])
    high_col = pick_first_existing(df, ["AdjH", "H"])
    out = df[["Code", "Date", close_col, high_col]].copy()
    out = out.rename(columns={close_col: "Close", high_col: "High"})
    out["Close"] = pd.to_numeric(out["Close"], errors="coerce")
    out["High"] = pd.to_numeric(out["High"], errors="coerce")
    out = out.dropna(subset=["Close", "High"]).drop_duplicates(subset=["Code"])
    return out


def resolve_latest_price_date(api_key: str, trading_dates: list[str], lookback_days: int = 5) -> tuple[str, pd.DataFrame]:
    lookback = min(lookback_days, len(trading_dates))
    for date_str in reversed(trading_dates[-lookback:]):
        price_df = get_price_df(api_key, date_str)
        if not price_df.empty:
            return date_str, price_df
    raise RuntimeError("価格配信済み最新営業日を特定できませんでした。")


def trim_trading_dates_to_latest(trading_dates: list[str], latest_date: str, n: int) -> list[str]:
    try:
        latest_index = trading_dates.index(latest_date)
    except ValueError as exc:
        raise RuntimeError(f"latest_date が営業日一覧に存在しません: latest_date={latest_date}") from exc

    trimmed = trading_dates[max(0, latest_index - n + 1): latest_index + 1]
    if len(trimmed) < n:
        raise RuntimeError(
            f"52週高値計算に必要な営業日数が不足しています。latest_date={latest_date}, days={len(trimmed)}"
        )
    return trimmed


def build_52w_cache(api_key: str) -> pd.DataFrame:
    trading_dates = get_recent_trading_dates(
        api_key,
        n=TRADING_DAYS_WINDOW + LATEST_PRICE_LOOKBACK_DAYS - 1,
    )
    latest_date, latest_price_df = resolve_latest_price_date(
        api_key,
        trading_dates,
        lookback_days=LATEST_PRICE_LOOKBACK_DAYS,
    )
    trading_dates = trim_trading_dates_to_latest(trading_dates, latest_date, n=TRADING_DAYS_WINDOW)
    master = get_master_df(api_key, latest_date)
    target_codes = set(master["Code"].astype(str).tolist())

    history_frames: list[pd.DataFrame] = []
    for date_str in trading_dates:
        price_df = latest_price_df if date_str == latest_date else get_price_df(api_key, date_str)
        if price_df.empty:
            continue
        price_df = price_df[price_df["Code"].isin(target_codes)].copy()
        if price_df.empty:
            continue
        price_df["Date"] = pd.to_datetime(price_df["Date"], errors="coerce")
        history_frames.append(price_df[["Code", "Date", "Close", "High"]])

    if not history_frames:
        raise RuntimeError("52週高値キャッシュ用の価格履歴を構築できませんでした。")

    history = pd.concat(history_frames, ignore_index=True)
    history = history.dropna(subset=["Date", "Close", "High"]).copy()
    history["Code"] = history["Code"].astype(str).str.zfill(5)
    history = history.sort_values(["Code", "Date"]).drop_duplicates(subset=["Code", "Date"], keep="last")

    history["Trailing52wHigh"] = (
        history.groupby("Code")["High"]
        .transform(lambda s: s.rolling(window=252, min_periods=1).max())
    )

    latest_rows = history.groupby("Code", as_index=False).tail(1).copy()
    latest_rows["DistTo52wHighPct"] = (
        latest_rows["Close"] / latest_rows["Trailing52wHigh"] - 1.0
    ) * 100.0
    latest_rows["IsNear52wHigh"] = latest_rows["DistTo52wHighPct"] >= -3.0
    latest_rows["IsNew52wHigh"] = latest_rows["High"] >= latest_rows["Trailing52wHigh"]
    latest_rows["Date"] = latest_rows["Date"].dt.strftime("%Y-%m-%d")

    out = latest_rows[
        [
            "Code",
            "Date",
            "Close",
            "High",
            "Trailing52wHigh",
            "DistTo52wHighPct",
            "IsNear52wHigh",
            "IsNew52wHigh",
        ]
    ].copy()
    out["Code"] = out["Code"].astype(str).str.zfill(5)
    return out.sort_values(["IsNew52wHigh", "DistTo52wHighPct", "Code"], ascending=[False, False, True]).reset_index(drop=True)


def main() -> int:
    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("環境変数 JQUANTS_API_KEY が見つかりません。")

    output_df = build_52w_cache(api_key)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = OUTPUT_PATH.with_suffix(".tmp")
    output_df.to_csv(temp_path, index=False, compression="gzip", encoding="utf-8-sig")
    os.replace(temp_path, OUTPUT_PATH)
    print(f"Wrote {len(output_df)} rows to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
