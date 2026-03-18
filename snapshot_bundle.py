import json
from typing import Any

import pandas as pd


def bundle_to_json_ready(bundle: dict[str, Any]) -> dict[str, Any]:
    return {
        "meta": bundle["meta"],
        "sector_summary": bundle["sector_summary"].to_dict(orient="records"),
        "leaders_by_sector": bundle["leaders_by_sector"].to_dict(orient="records"),
        "focus_candidates": bundle["focus_candidates"].to_dict(orient="records"),
        "diagnostics": bundle["diagnostics"],
    }


def bundle_to_json_text(bundle: dict[str, Any]) -> str:
    return json.dumps(bundle_to_json_ready(bundle), ensure_ascii=False, indent=2)


def bundle_to_markdown(bundle: dict[str, Any]) -> str:
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
    for _, row in bundle["sector_summary"].head(10).iterrows():
        lines.append(
            f"- {row.get('sector_name', '')}: live_ret={row.get('live_sector_ret', '')} "
            f"turnover_score={row.get('live_sector_turnover_score', '')}"
        )
    lines.extend(["", "## セクター別中心銘柄"])
    for _, row in bundle["leaders_by_sector"].head(15).iterrows():
        lines.append(f"- {row.get('sector_name', '')}: {row.get('code', '')} {row.get('name', '')} score={row.get('total_score', '')}")
    lines.extend(["", "## 需給ブレイク候補"])
    for _, row in bundle["focus_candidates"].head(20).iterrows():
        lines.append(f"- {row.get('code', '')} {row.get('name', '')}: {row.get('focus_reason', '')}")
    lines.extend(["", "## 注意点", "- 過去の任意時点を後から再取得することはできず、保存済み snapshot のみ再表示できます。"])
    return "\n".join(lines) + "\n"
