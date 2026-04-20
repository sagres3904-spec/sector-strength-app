import json
from typing import Any

import pandas as pd


def _frame_from_bundle(bundle: dict[str, Any], *keys: str) -> pd.DataFrame:
    for key in keys:
        value = bundle.get(key)
        if isinstance(value, pd.DataFrame):
            return value
        if isinstance(value, list):
            return pd.DataFrame(value)
    return pd.DataFrame()


def _focus_candidates_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    return _frame_from_bundle(
        bundle,
        "focus_candidates",
        "swing_candidates_1w_display",
        "swing_candidates_1w",
    )


def bundle_to_json_ready(bundle: dict[str, Any]) -> dict[str, Any]:
    sector_summary = _frame_from_bundle(bundle, "sector_summary", "today_sector_leaderboard")
    leaders_by_sector = _frame_from_bundle(bundle, "leaders_by_sector", "sector_representatives", "sector_representatives_display")
    focus_candidates = _focus_candidates_frame(bundle)
    return {
        "meta": bundle["meta"],
        "sector_summary": sector_summary.to_dict(orient="records"),
        "leaders_by_sector": leaders_by_sector.to_dict(orient="records"),
        "focus_candidates": focus_candidates.to_dict(orient="records"),
        "diagnostics": bundle.get("diagnostics", {}),
    }


def bundle_to_json_text(bundle: dict[str, Any]) -> str:
    return json.dumps(bundle_to_json_ready(bundle), ensure_ascii=False, indent=2)


def bundle_to_markdown(bundle: dict[str, Any]) -> str:
    sector_summary = _frame_from_bundle(bundle, "sector_summary", "today_sector_leaderboard")
    leaders_by_sector = _frame_from_bundle(bundle, "leaders_by_sector", "sector_representatives", "sector_representatives_display")
    focus_candidates = _focus_candidates_frame(bundle)
    lines = [
        f"# Snapshot {bundle['meta']['mode']}",
        "",
        f"- generated_at: {bundle['meta']['generated_at']}",
        f"- mode: {bundle['meta']['mode']}",
        f"- source_profile: {bundle['meta'].get('source_profile', '')}",
        f"- includes_kabu: {bundle['meta'].get('includes_kabu', '')}",
        f"- snapshot_backend: {bundle['meta'].get('snapshot_backend', '')}",
        "",
        "## 強いセクター",
    ]
    for _, row in sector_summary.head(10).iterrows():
        lines.append(
            f"- {row.get('sector_name', '')}: live_ret={row.get('live_sector_ret', '')} "
            f"turnover_score={row.get('live_sector_turnover_score', '')}"
        )
    lines.extend(["", "## セクター別中心銘柄"])
    for _, row in leaders_by_sector.head(15).iterrows():
        lines.append(f"- {row.get('sector_name', '')}: {row.get('code', '')} {row.get('name', '')} score={row.get('total_score', '')}")
    lines.extend(["", "## 需給ブレイク候補"])
    for _, row in focus_candidates.head(20).iterrows():
        reason = row.get("focus_reason", "") or row.get("candidate_basis", "") or row.get("selection_reason", "")
        lines.append(f"- {row.get('code', '')} {row.get('name', '')}: {reason}")
    lines.extend(["", "## 注意点", "- 過去の任意時点を後から再取得することはできず、保存済み snapshot のみ再表示できます。"])
    return "\n".join(lines) + "\n"
