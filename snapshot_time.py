from datetime import datetime, time, timedelta, timezone
from typing import Any


JST = timezone(timedelta(hours=9), name="JST")
EXPECTED_TIME_LABELS = {"0915": "09:15", "1130": "11:30", "1530": "15:30", "now": "随時"}
TRUE_TIMEPOINT_WINDOWS = {
    "0915": (time(9, 10), time(9, 20)),
    "1130": (time(11, 25), time(11, 35)),
    "1530": (time(15, 25), time(15, 35)),
}


def ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_snapshot_datetime(meta: dict[str, Any]) -> datetime:
    raw_value = str(meta.get("generated_at_utc") or meta.get("generated_at") or "").strip()
    if not raw_value:
        raise ValueError("generated_at is missing")
    return ensure_aware_utc(datetime.fromisoformat(raw_value.replace("Z", "+00:00")))


def format_jst_display(value: datetime) -> str:
    return ensure_aware_utc(value).astimezone(JST).strftime("%Y-%m-%d %H:%M:%S JST")


def is_true_timepoint(mode: str, generated_at_utc: datetime) -> bool:
    if mode == "now":
        return True
    if mode not in TRUE_TIMEPOINT_WINDOWS:
        return False
    generated_jst = ensure_aware_utc(generated_at_utc).astimezone(JST)
    window_start, window_end = TRUE_TIMEPOINT_WINDOWS[mode]
    return window_start <= generated_jst.time() <= window_end


def build_snapshot_meta(
    *,
    mode: str,
    generated_at: datetime,
    source_profile: str,
    includes_kabu: bool,
    snapshot_backend: str = "",
) -> dict[str, Any]:
    generated_at_utc = ensure_aware_utc(generated_at)
    meta = {
        "generated_at": generated_at_utc.isoformat(),
        "generated_at_utc": generated_at_utc.isoformat(),
        "generated_at_jst": format_jst_display(generated_at_utc),
        "mode": mode,
        "is_true_timepoint": is_true_timepoint(mode, generated_at_utc),
        "expected_time_label": EXPECTED_TIME_LABELS.get(mode, ""),
        "source_profile": source_profile,
        "includes_kabu": includes_kabu,
    }
    if snapshot_backend:
        meta["snapshot_backend"] = snapshot_backend
    return meta


def normalize_snapshot_meta(meta: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(meta)
    generated_at_utc = parse_snapshot_datetime(normalized)
    normalized.setdefault("generated_at", generated_at_utc.isoformat())
    normalized.setdefault("generated_at_utc", generated_at_utc.isoformat())
    normalized.setdefault("generated_at_jst", format_jst_display(generated_at_utc))
    mode = str(normalized.get("mode", ""))
    normalized.setdefault("expected_time_label", EXPECTED_TIME_LABELS.get(mode, ""))
    normalized.setdefault("is_true_timepoint", is_true_timepoint(mode, generated_at_utc))
    normalized.setdefault("source_profile", "")
    normalized.setdefault("includes_kabu", False)
    return normalized


def saved_snapshot_timing_warning(meta: dict[str, Any]) -> str:
    mode = str(meta.get("mode", ""))
    if mode == "now" or bool(meta.get("is_true_timepoint")):
        return ""
    expected_time_label = str(meta.get("expected_time_label", "")).strip()
    generated_at_jst = str(meta.get("generated_at_jst", "")).strip()
    if not expected_time_label or not generated_at_jst:
        return ""
    return f"これは{expected_time_label}時点に保存されたファイルではありません。{generated_at_jst} に{mode}モードで作成された保存データです。"
