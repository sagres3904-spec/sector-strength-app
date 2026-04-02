import base64
import json
import logging
import math
import os
import re
import time
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
VIEWER_ONLY_SNAPSHOT_MODES = ("0915", "1130", "1530", "now")
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

logger = logging.getLogger("sector_app_jq")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


UI_COLUMN_LABELS = {
    "today_rank": "今日の順位",
    "sector_name": "セクター名",
    "n": "採用銘柄数",
    "breadth": "上昇 : 下落",
    "median_ret": "中央値騰落率",
    "turnover_ratio_median": "売買代金倍率",
    "industry_rank_live": "東証業種順位",
    "sector_rank_1w": "1週順位",
    "sector_rank_1m": "1か月順位",
    "sector_rank_3m": "3か月順位",
    "leaders": "代表銘柄3つ",
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
    "price_vs_ma20_pct": "20日線乖離(%)",
    "52w_flag": "52週高値フラグ",
    "material_title": "材料タイトル",
    "focus_reason": "注目理由",
    "total_score": "総合スコア",
    "center_stock_score": "中心銘柄スコア",
    "watch_score": "監視候補スコア",
    "buyability_score": "買い候補スコア",
    "buyability_label": "買い候補判定",
    "today_sector_score": "本命セクタースコア",
    "price_block_score": "価格の強さ",
    "flow_block_score": "資金流入の強さ",
    "participation_block_score": "参加・広がりブロック",
    "signal_breadth_share": "ランキング広がり",
    "scan_member_count": "scan銘柄数",
    "representative_stock": "代表銘柄",
    "sector_confidence": "信頼度",
    "sector_caution": "注意点",
    "candidate_quality": "候補品質",
    "selection_reason": "採用理由",
    "risk_note": "注意点",
    "candidate_commentary": "コメント",
    "entry_fit": "今の判定",
    "sector_rs_vs_topix": "セクターTOPIX比RS",
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
    if _is_streamlit_runtime() and os.name != "nt":
        return True
    return False


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
    file_name = f"latest_{mode}.json"
    return f"{parent_dir}/{file_name}" if parent_dir else file_name


def _viewer_snapshot_github_token() -> str:
    use_streamlit_secrets = _is_streamlit_cloud()
    token = _github_control_token(use_streamlit_secrets=use_streamlit_secrets)
    if token:
        return token
    if use_streamlit_secrets:
        return _github_control_token(use_streamlit_secrets=False)
    return ""


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


def _available_viewer_snapshot_modes(settings: dict[str, Any] | None = None) -> list[str]:
    settings = settings or get_settings()
    if _is_streamlit_cloud():
        available_modes: list[str] = []
        for mode in VIEWER_ONLY_SNAPSHOT_MODES:
            try:
                _read_github_deploy_snapshot_text(mode, settings)
                available_modes.append(mode)
            except FileNotFoundError:
                continue
        return available_modes
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
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


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


def cancel_control_plane_update_request(
    token: str,
    settings: dict[str, Any] | None = None,
    *,
    session: requests.sessions.Session | None = None,
) -> tuple[bool, dict[str, Any]]:
    payload, sha = read_control_plane_request(token, settings, session=session)
    if not bool(payload.get("request_update")):
        return False, payload
    updated_payload = dict(payload)
    updated_payload.update(
        {
            "request_update": False,
            "status": "cancelled",
        }
    )
    write_control_plane_request(token, updated_payload, settings, sha=sha, session=session, message="Cancel snapshot refresh request from viewer")
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
        if idx == total or idx % 10 == 0:
            logger.info("get_price_df progress %s/%s", idx, total)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["code", "date", "close", "volume", "turnover"])
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out.dropna(subset=["date"]).sort_values(["code", "date"]).reset_index(drop=True)


def build_daily_base_data(*, fast_check: bool = False) -> tuple[pd.DataFrame, dict[str, Any]]:
    logger.info("build_daily_base_data start fast_check=%s", fast_check)
    logger.info("building daily base via J-Quants")
    api_key = get_api_key()
    trading_dates = get_recent_trading_dates(n=40 if fast_check else 260, api_key=api_key)
    master_df = get_master_df(trading_dates[-1], api_key=api_key)
    price_history = get_price_history(trading_dates, api_key=api_key, lookback_days=25 if fast_check else 80)
    price_history["volume"] = _coerce_numeric(price_history["volume"])
    price_history["turnover"] = _coerce_numeric(price_history["turnover"])
    price_history["close"] = _coerce_numeric(price_history["close"])
    grouped = price_history.groupby("code", group_keys=False)
    price_history["avg_volume_20d"] = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price_history["avg_turnover_20d"] = grouped["turnover"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price_history["close_ma_20d"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price_history["high_20d"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).max())
    latest = grouped.tail(1).rename(
        columns={
            "close": "close_latest",
            "volume": "volume_latest",
            "turnover": "turnover_latest",
            "date": "latest_date",
            "close_ma_20d": "close_ma_20d",
        }
    )
    week = grouped.nth(-6).reset_index()[["code", "close"]].rename(columns={"close": "close_1w"})
    month = grouped.nth(-21).reset_index()[["code", "close"]].rename(columns={"close": "close_1m"})
    quarter = grouped.nth(-64).reset_index()[["code", "close"]].rename(columns={"close": "close_3m"})
    base = master_df.merge(
        latest[["code", "close_latest", "volume_latest", "turnover_latest", "latest_date", "avg_volume_20d", "avg_turnover_20d", "close_ma_20d", "high_20d"]],
        on="code",
        how="inner",
    )
    base = base.merge(week, on="code", how="left").merge(month, on="code", how="left").merge(quarter, on="code", how="left")
    base["ret_1w"] = (base["close_latest"] / base["close_1w"] - 1.0) * 100.0
    base["ret_1m"] = (base["close_latest"] / base["close_1m"] - 1.0) * 100.0
    base["ret_3m"] = (base["close_latest"] / base["close_3m"] - 1.0) * 100.0
    sector_rank_1w = _sector_rank_from_returns(base, "ret_1w", "sector_ret_1w", "sector_rank_1w")
    sector_rank_1m = _sector_rank_from_returns(base, "ret_1m", "sector_ret_1m", "sector_rank_1m")
    sector_rank_3m = _sector_rank_from_returns(base, "ret_3m", "sector_ret_3m", "sector_rank_3m")
    base = base.merge(sector_rank_1w, on="sector_name", how="left")
    base = base.merge(sector_rank_1m, on="sector_name", how="left")
    base = base.merge(sector_rank_3m, on="sector_name", how="left")
    base["rel_1w"] = base["ret_1w"] - base["sector_ret_1w"]
    base["rel_1m"] = base["ret_1m"] - base["sector_ret_1m"]
    base["rel_3m"] = base["ret_3m"] - base["sector_ret_3m"]
    base["TradingValue_latest"] = _coerce_numeric(base["turnover_latest"]).fillna(0.0)
    base["is_near_52w_high"] = False if fast_check else (base["close_latest"] >= base["high_20d"] * 0.97)
    base["is_new_52w_high"] = False if fast_check else (base["close_latest"] >= base["high_20d"])
    base["reversal_candidates"] = (base["ret_1w"] > 0) & (base["ret_1m"] < 0)
    base["material_title"] = ""
    base["material_link"] = ""
    base["material_score"] = 0.0
    base["latest_date"] = pd.to_datetime(base["latest_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    logger.info("build_daily_base_data end rows=%s", len(base))
    return base, {"latest_date": max(trading_dates), "trading_date_count": len(trading_dates), "lookback_days": 25 if fast_check else 80, "fast_check": fast_check}


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
            return pd.DataFrame(columns=["sector_name", "source_type", "ranking_type", "rank_position"])
        return pd.DataFrame(columns=["code", "name", "sector_name", "exchange", "source_type", "ranking_type", "rank_position", "rank_score"])
    frame = pd.DataFrame(rows)
    if source_type == "industry_up":
        sector_col = pick_optional_existing(frame, ["CategoryName", "IndustryName", "SectorName", "Name", "symbol_name"]) or frame.columns[0]
        return pd.DataFrame(
            {
                "sector_name": frame[sector_col].map(_normalize_industry_name),
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
    diagnostics: dict[str, Any] = {"ranking_counts": {}}
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
    ranking_combo = ranking_combo.merge(
        base_df[
            [
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
            ]
        ],
        on="code",
        how="left",
        suffixes=("", "_base"),
    )
    ranking_combo["name"] = ranking_combo["name"].fillna(ranking_combo["ranking_name"]).fillna("")
    ranking_combo["sector_name"] = ranking_combo["sector_name"].fillna(ranking_combo["ranking_sector_name"]).fillna("")
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


def _fill_prev_close_from_base(payload: dict[str, Any], base_df: pd.DataFrame, code: str) -> bool:
    if payload.get("PrevClose") not in {None, ""}:
        return True
    base_row = base_df.loc[base_df["code"].astype(str) == str(code)]
    if base_row.empty:
        logger.warning("board prev_close missing and base fallback unavailable code=%s", code)
        return False
    close_value = base_row["close_latest"].iloc[0] if "close_latest" in base_row.columns else None
    latest_date = base_row["latest_date"].iloc[0] if "latest_date" in base_row.columns else ""
    if pd.isna(close_value) or close_value in {None, ""}:
        logger.warning("board prev_close missing and base fallback unavailable code=%s", code)
        return False
    payload["PrevClose"] = close_value
    logger.warning("board prev_close missing; filled from base_df code=%s latest_date=%s", code, latest_date)
    return True


def _board_to_row(code: str, payload: dict[str, Any], request_symbol: str, resolved_exchange: int) -> dict[str, Any]:
    if payload.get("BidPrice") in {None, ""} or payload.get("AskPrice") in {None, ""}:
        logger.warning("board bid/ask missing code=%s request_symbol=%s", code, request_symbol)
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


def _fetch_board_with_exchange_fallback(settings: dict[str, Any], token: str, base_df: pd.DataFrame, row: pd.Series) -> tuple[dict[str, Any] | None, dict[str, Any]]:
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
            has_prev_close = _fill_prev_close_from_base(payload, base_df, code)
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
                has_prev_close = _fill_prev_close_from_base(retry_payload, base_df, code)
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
    deep_candidates = base_df.copy()
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
    return selected, {"candidate_count": pre_count, "selected_count": int(len(selected)), "excluded_invalid_code": invalid_code_count, "excluded_duplicate": duplicate_count, "excluded_market_unknown": 0}


def enrich_with_board_snapshot(quotes_df: pd.DataFrame, base_df: pd.DataFrame, settings: dict[str, Any], token: str) -> tuple[pd.DataFrame, dict[str, Any]]:
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
            payload, attempt_diag = _fetch_board_with_exchange_fallback(settings, token, base_df, row)
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
            rows.append(_board_to_row(code, payload, request_symbol, resolved_exchange))
            continue
        if not has_major_fields:
            register_targets.append({"code": code, "resolved_exchange": resolved_exchange, "request_symbol": request_symbol})
            rows.append(_board_to_row(code, payload, request_symbol, resolved_exchange))
            continue
        excluded_missing_prev_close += 1
        logger.warning("board excluded due to missing prev_close after base fallback code=%s request_symbol=%s", code, request_symbol)
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
            if not _fill_prev_close_from_base(payload, base_df, code):
                excluded_missing_prev_close += 1
                row_map.pop(code, None)
                logger.warning("board excluded due to missing prev_close after retry/base fallback code=%s request_symbol=%s", code, request_symbol)
                continue
            row_map[code] = _board_to_row(code, payload, request_symbol, resolved_exchange)
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
                "median_ret",
                "turnover_ratio_median",
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
                "today_sector_score",
            ]
        )
    return frame.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)


def _build_sector_summary_bundle(ranking_df: pd.DataFrame, industry_df: pd.DataFrame, base_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    sector_columns = [
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
    ]
    scan_df = ranking_df.merge(base_df[sector_columns], on="code", how="left", suffixes=("", "_base"))
    scan_df["name"] = scan_df["name"].fillna(scan_df.get("name_base", "")).fillna("")
    scan_df["sector_name"] = scan_df["sector_name"].fillna(scan_df.get("sector_name_base", "")).fillna("")
    scan_df = scan_df[scan_df["sector_name"].astype(str).str.strip() != ""].copy()
    if scan_df.empty:
        empty = _summarize_sector_frame(pd.DataFrame(), sort_columns=["today_sector_score"], ascending=[False])
        return {"today": empty, "weekly": empty.copy(), "monthly": empty.copy()}

    sector_turnover_baseline = (
        base_df.groupby("sector_name", dropna=False)["TradingValue_latest"]
        .median()
        .reset_index(name="sector_turnover_baseline")
    )
    unique_sector_df = (
        scan_df.sort_values(["sector_name", "TradingValue_latest"], ascending=[True, False])
        .drop_duplicates(["sector_name", "code"])
        .copy()
    )
    unique_sector_df = unique_sector_df.merge(sector_turnover_baseline, on="sector_name", how="left")
    unique_sector_df["stock_turnover_ratio_vs_sector"] = _safe_ratio(unique_sector_df["TradingValue_latest"], unique_sector_df["sector_turnover_baseline"])
    sector_base = unique_sector_df.groupby("sector_name", as_index=False).agg(
        n=("code", "nunique"),
        median_ret=("ret_1w", "median"),
        turnover_ratio_median=("stock_turnover_ratio_vs_sector", "median"),
        sector_rank_1w=("sector_rank_1w", "median"),
        sector_rank_1m=("sector_rank_1m", "median"),
        sector_rank_3m=("sector_rank_3m", "median"),
        sector_turnover_total=("TradingValue_latest", "sum"),
    )
    source_counts = (
        scan_df.groupby(["sector_name", "source_type"])["code"]
        .nunique()
        .unstack(fill_value=0)
        .reset_index()
    )
    sector_base = sector_base.merge(source_counts, on="sector_name", how="left")
    for column in ["price_up", "turnover", "volume_surge", "turnover_surge"]:
        if column not in sector_base.columns:
            sector_base[column] = 0
    sector_base = sector_base.rename(
        columns={
            "price_up": "price_up_count",
            "turnover": "turnover_count",
            "volume_surge": "volume_surge_count",
            "turnover_surge": "turnover_surge_count",
        }
    )
    sector_base["breadth_up"] = sector_base["price_up_count"].fillna(0).astype(int)
    sector_base["breadth_down"] = (sector_base["n"].fillna(0) - sector_base["breadth_up"]).clip(lower=0).astype(int)
    sector_base["breadth"] = sector_base.apply(lambda row: f"{int(row['breadth_up'])}:{int(row['breadth_down'])}", axis=1)
    leaders = (
        unique_sector_df.sort_values(["sector_name", "TradingValue_latest"], ascending=[True, False])
        .groupby("sector_name")["name"]
        .apply(lambda s: ", ".join(str(name) for name in s.head(3)))
        .reset_index(name="leaders")
    )
    sector_base = sector_base.merge(leaders, on="sector_name", how="left")
    top_turnover = (
        unique_sector_df.groupby("sector_name")["TradingValue_latest"]
        .max()
        .reset_index(name="top_stock_turnover")
    )
    sector_base = sector_base.merge(top_turnover, on="sector_name", how="left")
    sector_base["leader_contribution_pct"] = (
        _safe_ratio(sector_base["top_stock_turnover"], sector_base["sector_turnover_total"]).fillna(0.0) * 100.0
    )
    if not industry_df.empty and "sector_name" in industry_df.columns:
        sector_base = sector_base.merge(
            industry_df[["sector_name", "rank_position"]].drop_duplicates("sector_name").rename(columns={"rank_position": "industry_rank_live"}),
            on="sector_name",
            how="left",
        )
    else:
        sector_base["industry_rank_live"] = pd.NA
    sector_base["turnover_ratio_median"] = _coerce_numeric(sector_base["turnover_ratio_median"])
    sector_base["today_sector_score"] = 0.0
    for column, weight in {
        "price_up_count": 1.35,
        "turnover_surge_count": 1.2,
        "volume_surge_count": 1.05,
        "turnover_count": 1.0,
        "median_ret": 0.8,
        "turnover_ratio_median": 0.9,
    }.items():
        sector_base["today_sector_score"] += _score_percentile(sector_base[column]) * weight
    for column, weight in {"industry_rank_live": 1.3, "sector_rank_1w": 0.9, "sector_rank_1m": 1.0, "sector_rank_3m": 1.0}.items():
        sector_base["today_sector_score"] += _score_rank_ascending(sector_base[column]) * weight
    sector_base["today_sector_score"] += _score_percentile(sector_base["breadth_up"] - sector_base["breadth_down"]) * 0.8
    sector_base = sector_base[sector_base["n"].fillna(0) >= 3].copy()
    sector_base["industry_rank_live"] = _coerce_numeric(sector_base["industry_rank_live"])

    today = _summarize_sector_frame(
        sector_base.copy(),
        sort_columns=["today_sector_score", "price_up_count", "turnover_surge_count", "volume_surge_count"],
        ascending=[False, False, False, False],
    )
    weekly = _summarize_sector_frame(
        sector_base.copy(),
        sort_columns=["sector_rank_1w", "today_sector_score"],
        ascending=[True, False],
    )
    monthly = _summarize_sector_frame(
        sector_base.copy(),
        sort_columns=["sector_rank_1m", "today_sector_score"],
        ascending=[True, False],
    )
    display_columns = [
        "sector_name",
        "n",
        "breadth",
        "median_ret",
        "turnover_ratio_median",
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
        "today_sector_score",
    ]
    return {
        "today": today[display_columns].reset_index(drop=True),
        "weekly": weekly[display_columns].reset_index(drop=True),
        "monthly": monthly[display_columns].reset_index(drop=True),
    }


def _build_stock_candidate_bundles(mode: str, merged: pd.DataFrame, sector_bundle: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    if merged.empty:
        empty = pd.DataFrame()
        return {"center_stocks": empty, "watch_candidates": empty, "buy_candidates": empty}

    working = merged.copy()
    top_today_sector_names = sector_bundle.get("today", pd.DataFrame()).head(6).get("sector_name", pd.Series(dtype=str)).tolist()
    working["today_sector_score"] = working["sector_name"].map(
        sector_bundle.get("today", pd.DataFrame()).set_index("sector_name")["today_sector_score"] if not sector_bundle.get("today", pd.DataFrame()).empty else pd.Series(dtype=float)
    )
    working["center_stock_score"] = 0.0
    for column, weight in {
        "total_score": 1.2,
        "live_turnover": 1.0,
        "live_volume_ratio_20d": 0.9,
        "live_ret_vs_prev_close": 1.0,
        "live_ret_from_open": 0.9,
        "rel_1w": 0.7,
        "rel_1m": 0.8,
        "rel_3m": 0.8,
    }.items():
        working["center_stock_score"] += _score_percentile(working[column]) * weight
    center_stocks = (
        working[working["sector_name"].isin(top_today_sector_names)]
        .sort_values(["sector_name", "center_stock_score", "live_turnover"], ascending=[True, False, False])
        .groupby("sector_name", as_index=False)
        .head(3)[
            [
                "sector_name",
                "code",
                "name",
                "live_price",
                "live_ret_vs_prev_close",
                "live_ret_from_open",
                "live_turnover",
                "live_volume_ratio_20d",
                "live_turnover_ratio_20d",
                "ret_1w",
                "ret_1m",
                "ret_3m",
                "rel_1w",
                "rel_1m",
                "rel_3m",
                "center_stock_score",
                "nikkei_search",
                "material_link",
            ]
        ]
        .reset_index(drop=True)
    )
    center_codes = set(center_stocks["code"].astype(str).tolist())

    turnover_floor = float(_coerce_numeric(working["avg_turnover_20d"]).median(skipna=True) or 0.0)
    volume_floor = float(_coerce_numeric(working["avg_volume_20d"]).median(skipna=True) or 0.0)
    working["turnover_floor_pass"] = _coerce_numeric(working["avg_turnover_20d"]).fillna(0.0) >= turnover_floor
    working["volume_floor_pass"] = _coerce_numeric(working["avg_volume_20d"]).fillna(0.0) >= volume_floor
    working["ma20_band_pass"] = _coerce_numeric(working["price_vs_ma20_pct"]).between(-5.0, 18.0, inclusive="both").fillna(False)
    working["trend_guard_pass"] = (_coerce_numeric(working["ret_1m"]).fillna(-999) >= -15.0) & (_coerce_numeric(working["ret_3m"]).fillna(-999) >= -20.0)
    working["sector_guard_pass"] = (_coerce_numeric(working["sector_rank_1m"]).fillna(999) <= 12) | (_coerce_numeric(working["sector_rank_3m"]).fillna(999) <= 12)
    working["flow_guard_pass"] = (_coerce_numeric(working["live_turnover_ratio_20d"]).fillna(0.0) >= 1.0) & (_coerce_numeric(working["live_volume_ratio_20d"]).fillna(0.0) >= 1.0)
    working["earnings_proximity_flag"] = False
    working["atr_pct"] = pd.NA
    working["buyability_score"] = 0.0
    for column, weight in {
        "live_turnover_ratio_20d": 1.0,
        "live_volume_ratio_20d": 0.9,
        "ret_1m": 0.6,
        "ret_3m": 0.7,
        "today_sector_score": 0.8,
        "center_stock_score": 0.6,
    }.items():
        working["buyability_score"] += _score_percentile(working[column]) * weight
    working["buyability_score"] += _score_rank_ascending(working["sector_rank_1m"]) * 0.6
    working["buyability_score"] += _score_rank_ascending(working["sector_rank_3m"]) * 0.7
    working["buy_candidate_flag"] = (
        working["turnover_floor_pass"]
        & working["volume_floor_pass"]
        & working["ma20_band_pass"]
        & working["trend_guard_pass"]
        & working["sector_guard_pass"]
        & working["flow_guard_pass"]
        & (~working["earnings_proximity_flag"])
    )
    working["buyability_label"] = working["buy_candidate_flag"].map({True: "buy_candidate", False: "watch_only"})
    candidate_pool = working[~working["code"].astype(str).isin(center_codes)].copy()
    candidate_pool["watch_score"] = 0.0
    for column, weight in {
        "today_sector_score": 1.0,
        "total_score": 0.9,
        "live_turnover_ratio_20d": 0.8,
        "live_volume_ratio_20d": 0.8,
        "rel_1w": 0.6,
        "rel_1m": 0.5,
    }.items():
        candidate_pool["watch_score"] += _score_percentile(candidate_pool[column]) * weight
    candidate_pool["watch_candidate_flag"] = (
        (~candidate_pool["buy_candidate_flag"])
        & (
            candidate_pool["sector_name"].isin(top_today_sector_names)
            | (_coerce_numeric(candidate_pool["today_sector_score"]).fillna(0.0) >= float(_coerce_numeric(candidate_pool["today_sector_score"]).median(skipna=True) or 0.0))
        )
        & (
            (_coerce_numeric(candidate_pool["live_turnover_ratio_20d"]).fillna(0.0) >= 0.8)
            | (_coerce_numeric(candidate_pool["live_volume_ratio_20d"]).fillna(0.0) >= 0.8)
            | (_coerce_numeric(candidate_pool["total_score"]).fillna(0.0) >= float(_coerce_numeric(candidate_pool["total_score"]).median(skipna=True) or 0.0))
        )
    )
    ordered_columns = [
        "sector_name",
        "code",
        "name",
        "live_price",
        "live_ret_vs_prev_close",
        "live_ret_from_open",
        "live_volume",
        "avg_volume_20d",
        "live_turnover",
        "avg_turnover_20d",
        "live_volume_ratio_20d",
        "live_turnover_ratio_20d",
        "price_vs_ma20_pct",
        "ret_1w",
        "ret_1m",
        "ret_3m",
        "sector_rank_1w",
        "sector_rank_1m",
        "sector_rank_3m",
        "total_score",
        "center_stock_score",
        "watch_score",
        "buyability_score",
        "buyability_label",
        "earnings_proximity_flag",
        "atr_pct",
        "52w_flag",
        "material_title",
        "focus_reason",
        "nikkei_search",
        "material_link",
    ]
    buy_candidates = (
        candidate_pool[candidate_pool["buy_candidate_flag"]]
        .sort_values(["buyability_score", "center_stock_score", "live_turnover"], ascending=[False, False, False])[ordered_columns]
        .head(20)
        .reset_index(drop=True)
    )
    watch_candidates = (
        candidate_pool[candidate_pool["watch_candidate_flag"]]
        .sort_values(["watch_score", "today_sector_score", "live_turnover"], ascending=[False, False, False])[ordered_columns]
        .head(30)
        .reset_index(drop=True)
    )
    return {
        "center_stocks": center_stocks,
        "watch_candidates": watch_candidates,
        "buy_candidates": buy_candidates,
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


TODAY_SECTOR_DISPLAY_COLUMNS = [
    "today_rank",
    "sector_name",
    "representative_stock",
    "sector_confidence",
    "sector_caution",
    "industry_rank_live",
    "price_block_score",
    "flow_block_score",
    "signal_breadth_share",
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
SWING_BUY_1W_DISPLAY_COLUMNS = [
    "candidate_rank_1w",
    "name",
    "sector_name",
    "candidate_quality",
    "selection_reason",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_1w",
    "live_ret_vs_prev_close",
    "live_turnover",
    "earnings_buffer_days",
]
SWING_BUY_1M_DISPLAY_COLUMNS = [
    "candidate_rank_1m",
    "name",
    "sector_name",
    "candidate_quality",
    "selection_reason",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_1m",
    "rs_vs_topix_3m",
    "price_vs_ma20_pct",
    "earnings_buffer_days",
    "finance_health_flag",
]
SWING_WATCH_1W_DISPLAY_COLUMNS = [
    "candidate_rank_1w",
    "name",
    "sector_name",
    "entry_fit",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_1w",
    "earnings_buffer_days",
]
SWING_WATCH_1M_DISPLAY_COLUMNS = [
    "candidate_rank_1m",
    "name",
    "sector_name",
    "entry_fit",
    "risk_note",
    "candidate_commentary",
    "rs_vs_topix_1m",
    "rs_vs_topix_3m",
    "price_vs_ma20_pct",
    "earnings_buffer_days",
]


def _prepare_table_view(df: pd.DataFrame, columns: list[str]) -> tuple[pd.DataFrame, list[str]]:
    compatibility_notes: list[str] = []
    if df is None or df.empty:
        return pd.DataFrame(columns=columns), compatibility_notes

    prepared = df.copy()
    if "today_rank" not in prepared.columns and "sector_name" in prepared.columns and "price_block_score" in prepared.columns:
        if "tethered_rank" in prepared.columns:
            prepared["today_rank"] = _coerce_numeric(prepared["tethered_rank"])
            compatibility_notes.append("today_rank<-tethered_rank")
        elif "score_rank" in prepared.columns:
            prepared["today_rank"] = _coerce_numeric(prepared["score_rank"])
            compatibility_notes.append("today_rank<-score_rank")
        elif "rank" in prepared.columns:
            prepared["today_rank"] = _coerce_numeric(prepared["rank"])
            compatibility_notes.append("today_rank<-rank")
        else:
            prepared["today_rank"] = range(1, len(prepared) + 1)
            compatibility_notes.append("today_rank")
    if "persistence_rank" not in prepared.columns and "sector_name" in prepared.columns and "sector_rs_vs_topix" in prepared.columns:
        prepared["persistence_rank"] = range(1, len(prepared) + 1)
        compatibility_notes.append("persistence_rank")
    if "representative_stock" not in prepared.columns:
        prepared["representative_stock"] = ""
        compatibility_notes.append("representative_stock")
    if "sector_confidence" not in prepared.columns:
        prepared["sector_confidence"] = ""
        compatibility_notes.append("sector_confidence")
    if "sector_caution" not in prepared.columns:
        prepared["sector_caution"] = ""
        compatibility_notes.append("sector_caution")
    if "scan_member_count" not in prepared.columns and "n" in prepared.columns:
        prepared["scan_member_count"] = _coerce_numeric(prepared["n"])
        compatibility_notes.append("scan_member_count<-n")
    if "sector_rs_vs_topix" not in prepared.columns:
        if "sector_rs_vs_topix_1w" in prepared.columns:
            prepared["sector_rs_vs_topix"] = _coerce_numeric(prepared["sector_rs_vs_topix_1w"])
        elif "sector_rs_vs_topix_1m" in prepared.columns:
            prepared["sector_rs_vs_topix"] = _coerce_numeric(prepared["sector_rs_vs_topix_1m"])
        elif "sector_rs_vs_topix_3m" in prepared.columns:
            prepared["sector_rs_vs_topix"] = _coerce_numeric(prepared["sector_rs_vs_topix_3m"])
        else:
            prepared["sector_rs_vs_topix"] = pd.NA
        compatibility_notes.append("sector_rs_vs_topix")
    string_columns = {
        "sector_name",
        "representative_stock",
        "sector_confidence",
        "sector_caution",
        "breadth",
        "name",
        "candidate_quality",
        "entry_fit",
        "selection_reason",
        "risk_note",
        "candidate_commentary",
        "finance_health_flag",
    }
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


def _prepare_today_sector_leaderboard_for_view(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    prepared = df.copy()
    fallback_columns = ["leaders", "representative_stocks_text", "representative_stock"]

    def _normalize_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (list, tuple, set)):
            parts = [str(item).strip() for item in value if str(item).strip() and str(item).strip().lower() != "nan"]
            return " / ".join(parts)
        try:
            if pd.isna(value):
                return ""
        except TypeError:
            pass
        text = str(value).strip()
        return "" if not text or text.lower() == "nan" else text

    def _pick_representative_stocks(row: pd.Series) -> str:
        for column in fallback_columns:
            text = _normalize_text(row.get(column, ""))
            if text:
                return text
        return ""

    prepared["representative_stock"] = prepared.apply(
        _pick_representative_stocks,
        axis=1,
    )
    return prepared


def build_live_snapshot(mode: str, ranking_df: pd.DataFrame, industry_df: pd.DataFrame, board_df: pd.DataFrame, base_df: pd.DataFrame, now_ts: datetime) -> dict[str, Any]:
    merged = base_df.merge(board_df, on="code", how="inner")
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
    merged["focus_reason"] = merged.apply(lambda row: ", ".join(filter(None, [f"sector:{row.get('sector_name', '')}" if pd.notna(row.get("sector_name")) else "", "turnover_breakout" if float(row.get("live_turnover_ratio_20d", 0) or 0) >= 1.5 else "", "volume_breakout" if float(row.get("live_volume_ratio_20d", 0) or 0) >= 1.5 else "", "near_52w_high" if bool(row.get("is_near_52w_high")) else ""])) or "live_strength", axis=1)
    merged["nikkei_search"] = merged.apply(lambda row: _make_nikkei_search_link(str(row.get("name", "")), str(row.get("code", ""))), axis=1)
    merged["52w_flag"] = merged.apply(lambda row: "new_high" if bool(row.get("is_new_52w_high")) else ("near_high" if bool(row.get("is_near_52w_high")) else ""), axis=1)
    sector_bundle = _build_sector_summary_bundle(ranking_df, industry_df, base_df)
    stock_bundle = _build_stock_candidate_bundles(mode, merged, sector_bundle)
    empty_state = {
        "weekly_sector_summary": "" if not sector_bundle["weekly"].empty else "市場横断ランキングと daily base を突合した結果、1週順位を出せる主力セクターがありません。",
        "monthly_sector_summary": "" if not sector_bundle["monthly"].empty else "市場横断ランキングと daily base を突合した結果、1か月順位を出せる主力セクターがありません。",
        "buy_candidates": "" if not stock_bundle["buy_candidates"].empty else "20日平均売買代金・20日平均出来高・20日線乖離・1か月/3か月悪化回避・当日フロー条件を同時に満たす銘柄がありません。",
        "watch_candidates": "" if not stock_bundle["watch_candidates"].empty else "中心銘柄を除いた監視候補がありません。",
        "center_stocks": "" if not stock_bundle["center_stocks"].empty else "今日の本命セクターに紐づく中心銘柄を抽出できませんでした。",
    }
    meta = build_snapshot_meta(mode=mode, generated_at=now_ts, source_profile="local_kabu_jq_yanoshin", includes_kabu=True)
    return {
        "meta": meta,
        "sector_summary": sector_bundle["today"],
        "today_sector_summary": sector_bundle["today"],
        "weekly_sector_summary": sector_bundle["weekly"],
        "monthly_sector_summary": sector_bundle["monthly"],
        "leaders_by_sector": stock_bundle["center_stocks"],
        "center_stocks": stock_bundle["center_stocks"],
        "focus_candidates": stock_bundle["watch_candidates"],
        "watch_candidates": stock_bundle["watch_candidates"],
        "buy_candidates": stock_bundle["buy_candidates"],
        "empty_reasons": empty_state,
        "diagnostics": {
            "mode": mode,
            "generated_at": meta["generated_at"],
            "watch_candidate_count": int(len(stock_bundle["watch_candidates"])),
            "buy_candidate_count": int(len(stock_bundle["buy_candidates"])),
            "center_stock_count": int(len(stock_bundle["center_stocks"])),
            "ranking_candidate_count": int(len(ranking_df)),
            "sector_summary_scope": "market_scan_rankings_plus_daily_base",
            "breadth_scope": "price_up_count vs ranked_non_price_up_count_within_market_scan",
            "includes_kabu": True,
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
    if _is_streamlit_cloud():
        cached_payload = _load_saved_snapshot_payload_from_github(mode, settings)
    else:
        snapshot_path = _snapshot_json_path(mode, settings)
        cached_payload = _load_saved_snapshot_payload_cached(mode, str(snapshot_path), _snapshot_mtime_ns(snapshot_path))
    payload = cached_payload["payload"]
    loaded_snapshot_meta = dict(payload.get("meta", {}))
    loaded_snapshot_diagnostics = dict(payload.get("diagnostics", {}))
    meta = normalize_snapshot_meta(payload.get("meta", {}))
    focus_candidates = pd.DataFrame(payload.get("focus_candidates", []))
    legacy_watch_candidates = pd.DataFrame(payload.get("watch_candidates", payload.get("focus_candidates", [])))
    legacy_buy_candidates = pd.DataFrame(payload.get("buy_candidates", []))
    center_stocks = pd.DataFrame(payload.get("sector_representatives", payload.get("center_stocks", payload.get("leaders_by_sector", []))))
    today_sector_summary = pd.DataFrame(payload.get("today_sector_leaderboard", payload.get("today_sector_summary", payload.get("sector_summary", []))))
    weekly_sector_summary = pd.DataFrame(payload.get("sector_persistence_1w", payload.get("weekly_sector_summary", [])))
    monthly_sector_summary = pd.DataFrame(payload.get("sector_persistence_1m", payload.get("monthly_sector_summary", [])))
    quarter_sector_summary = pd.DataFrame(payload.get("sector_persistence_3m", []))
    swing_candidates_1w = pd.DataFrame(payload.get("swing_candidates_1w", payload.get("watch_candidates", payload.get("focus_candidates", []))))
    swing_candidates_1m = pd.DataFrame(payload.get("swing_candidates_1m", payload.get("buy_candidates", [])))
    swing_buy_candidates_1w = pd.DataFrame(payload.get("swing_buy_candidates_1w", []))
    swing_watch_candidates_1w = pd.DataFrame(payload.get("swing_watch_candidates_1w", []))
    swing_buy_candidates_1m = pd.DataFrame(payload.get("swing_buy_candidates_1m", []))
    swing_watch_candidates_1m = pd.DataFrame(payload.get("swing_watch_candidates_1m", []))
    if swing_buy_candidates_1w.empty and not swing_candidates_1w.empty and "entry_fit" in swing_candidates_1w.columns:
        swing_buy_candidates_1w = swing_candidates_1w[swing_candidates_1w["entry_fit"].astype(str).eq("買い候補")].copy()
    if swing_watch_candidates_1w.empty and not swing_candidates_1w.empty and "entry_fit" in swing_candidates_1w.columns:
        swing_watch_candidates_1w = swing_candidates_1w[swing_candidates_1w["entry_fit"].astype(str).eq("監視候補")].copy()
    if swing_buy_candidates_1m.empty and not swing_candidates_1m.empty and "entry_fit" in swing_candidates_1m.columns:
        swing_buy_candidates_1m = swing_candidates_1m[swing_candidates_1m["entry_fit"].astype(str).eq("買い候補")].copy()
    if swing_watch_candidates_1m.empty and not swing_candidates_1m.empty and "entry_fit" in swing_candidates_1m.columns:
        swing_watch_candidates_1m = swing_candidates_1m[swing_candidates_1m["entry_fit"].astype(str).eq("監視候補")].copy()
    watch_candidates = swing_watch_candidates_1w if not swing_watch_candidates_1w.empty else legacy_watch_candidates
    buy_candidates = swing_buy_candidates_1m if not swing_buy_candidates_1m.empty else legacy_buy_candidates
    for frame in [focus_candidates, legacy_watch_candidates, legacy_buy_candidates, watch_candidates, buy_candidates, swing_candidates_1w, swing_candidates_1m, swing_buy_candidates_1w, swing_watch_candidates_1w, swing_buy_candidates_1m, swing_watch_candidates_1m, center_stocks]:
        if not frame.empty and "name" in frame.columns:
            frame["nikkei_search"] = frame.apply(lambda row: _make_nikkei_search_link(str(row.get("name", "")), str(row.get("code", ""))), axis=1)
    return {
        "meta": meta,
        "sector_summary": today_sector_summary,
        "today_sector_summary": today_sector_summary,
        "weekly_sector_summary": weekly_sector_summary,
        "monthly_sector_summary": monthly_sector_summary,
        "sector_persistence_1w": weekly_sector_summary,
        "sector_persistence_1m": monthly_sector_summary,
        "sector_persistence_3m": quarter_sector_summary,
        "leaders_by_sector": center_stocks,
        "center_stocks": center_stocks,
        "sector_representatives": center_stocks,
        "focus_candidates": focus_candidates,
        "watch_candidates": watch_candidates,
        "buy_candidates": buy_candidates,
        "swing_candidates_1w": swing_candidates_1w,
        "swing_candidates_1m": swing_candidates_1m,
        "swing_buy_candidates_1w": swing_buy_candidates_1w,
        "swing_watch_candidates_1w": swing_watch_candidates_1w,
        "swing_buy_candidates_1m": swing_buy_candidates_1m,
        "swing_watch_candidates_1m": swing_watch_candidates_1m,
        "empty_reasons": payload.get("empty_reasons", {}),
        "diagnostics": loaded_snapshot_diagnostics,
        "loaded_snapshot_meta": loaded_snapshot_meta,
        "loaded_snapshot_diagnostics": loaded_snapshot_diagnostics,
        "paths": cached_payload["paths"],
        "snapshot_source_label": cached_payload["source_label"],
        "snapshot_backend_name": cached_payload["backend_name"],
        "snapshot_warning_message": cached_payload["warning_message"],
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
        board_df, board_diag = enrich_with_board_snapshot(deep_watch_df, base_df, settings, token)
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
    raw_meta = bundle.get("loaded_snapshot_meta", bundle.get("meta", {})) if is_saved_snapshot else bundle.get("meta", {})
    meta = normalize_snapshot_meta(raw_meta)
    generated_at_jst = str(raw_meta.get("generated_at_jst", "")).strip() or str(meta.get("generated_at_jst", "") or meta.get("generated_at", ""))
    mode = str(raw_meta.get("mode", "")).strip() or str(meta.get("mode", ""))
    expected_time_label = str(raw_meta.get("expected_time_label", "")).strip() or str(meta.get("expected_time_label", "")).strip()
    timepoint_meaning = _timepoint_meaning(mode)
    empty_reasons = bundle.get("empty_reasons", {})
    warning_text = saved_snapshot_timing_warning(meta) if is_saved_snapshot else ""
    today_sector_source = _prepare_today_sector_leaderboard_for_view(
        bundle.get("today_sector_leaderboard", bundle.get("today_sector_summary", bundle["sector_summary"]))
    )
    today_sector_view, today_sector_notes = _prepare_table_view(today_sector_source, TODAY_SECTOR_DISPLAY_COLUMNS)
    weekly_sector_view, weekly_sector_notes = _prepare_table_view(bundle.get("sector_persistence_1w", bundle.get("weekly_sector_summary", pd.DataFrame())), PERSISTENCE_DISPLAY_COLUMNS)
    monthly_sector_view, monthly_sector_notes = _prepare_table_view(bundle.get("sector_persistence_1m", bundle.get("monthly_sector_summary", pd.DataFrame())), PERSISTENCE_DISPLAY_COLUMNS)
    quarter_sector_view, quarter_sector_notes = _prepare_table_view(bundle.get("sector_persistence_3m", pd.DataFrame()), PERSISTENCE_DISPLAY_COLUMNS)
    swing_buy_1w_view, swing_buy_1w_notes = _prepare_table_view(bundle.get("swing_buy_candidates_1w", pd.DataFrame()), SWING_BUY_1W_DISPLAY_COLUMNS)
    swing_buy_1m_view, swing_buy_1m_notes = _prepare_table_view(bundle.get("swing_buy_candidates_1m", pd.DataFrame()), SWING_BUY_1M_DISPLAY_COLUMNS)
    swing_watch_1w_view, swing_watch_1w_notes = _prepare_table_view(bundle.get("swing_watch_candidates_1w", pd.DataFrame()), SWING_WATCH_1W_DISPLAY_COLUMNS)
    swing_watch_1m_view, swing_watch_1m_notes = _prepare_table_view(bundle.get("swing_watch_candidates_1m", pd.DataFrame()), SWING_WATCH_1M_DISPLAY_COLUMNS)
    sector_compat_notes = sorted(set(today_sector_notes + weekly_sector_notes + monthly_sector_notes + quarter_sector_notes + swing_buy_1w_notes + swing_buy_1m_notes + swing_watch_1w_notes + swing_watch_1m_notes))
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
    _render_dataframe_or_reason(
        "今日の本命セクター",
        today_sector_view,
        reason="市場横断ランキングと daily base の突合条件を満たす本命セクターがありません。",
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
        reason=str(empty_reasons.get("weekly_sector_summary", "")),
    )
    _render_dataframe_or_reason(
        "1か月主力セクター",
        monthly_sector_view,
        reason=str(empty_reasons.get("monthly_sector_summary", "")),
    )
    _render_dataframe_or_reason(
        "3か月主力セクター",
        quarter_sector_view,
        reason=str(empty_reasons.get("sector_persistence_3m", "")),
    )
    _render_dataframe_or_reason(
        "1週間スイング買い候補",
        swing_buy_1w_view,
        reason=str(empty_reasons.get("swing_buy_candidates_1w", empty_reasons.get("swing_candidates_1w", ""))),
        link_columns=True,
    )
    _render_dataframe_or_reason(
        "1か月スイング買い候補",
        swing_buy_1m_view,
        reason=str(empty_reasons.get("swing_buy_candidates_1m", empty_reasons.get("swing_candidates_1m", empty_reasons.get("buy_candidates", "")))),
        link_columns=True,
    )
    _render_dataframe_or_reason(
        "1週間スイング監視候補",
        swing_watch_1w_view,
        reason=str(empty_reasons.get("swing_watch_candidates_1w", empty_reasons.get("watch_candidates", ""))),
        link_columns=True,
    )
    _render_dataframe_or_reason(
        "1か月スイング監視候補",
        swing_watch_1m_view,
        reason=str(empty_reasons.get("swing_watch_candidates_1m", "")),
        link_columns=True,
    )
    diagnostics = bundle.get("loaded_snapshot_diagnostics", bundle.get("diagnostics", {})) if is_saved_snapshot else bundle.get("diagnostics", {})
    if diagnostics:
        with st.expander("diagnostics", expanded=False):
            st.json(diagnostics)


def _render_control_plane_status(settings: dict[str, Any]) -> None:
    st.subheader("運用状態")
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
    requested_at = str(request_payload.get("requested_at", "")).strip()
    requested_by = str(request_payload.get("requested_by", "")).strip()
    request_mode = str(request_payload.get("request_mode", "") or "1130").strip() or "1130"
    st.caption(f"request_mode: {request_mode}")
    if requested_at or requested_by:
        st.caption(f"requested_at: {requested_at or '-'} requested_by: {requested_by or '-'}")
    with st.expander("更新依頼", expanded=bool(request_payload.get("request_update"))):
        if request_payload.get("request_update"):
            st.warning(f"更新依頼は受付中です。request_mode={request_mode} requested_at={requested_at or '-'} requested_by={requested_by or '-'}")
            pending_cols = st.columns(4)
            pending_cols[0].button("0915を更新", disabled=True, key="request-0915-disabled", help="すでに依頼済みです")
            pending_cols[1].button("1130を更新", disabled=True, key="request-1130-disabled", help="すでに依頼済みです")
            pending_cols[2].button("1530を更新", disabled=True, key="request-1530-disabled", help="すでに依頼済みです")
            pending_cols[3].button("nowを更新", disabled=True, key="request-now-disabled", help="すでに依頼済みです")
            if st.button("更新依頼を取り消す", key="cancel-request", help="control-plane branch の更新依頼を取り消します"):
                try:
                    cancelled, updated_request = cancel_control_plane_update_request(token, settings)
                    if cancelled:
                        st.success(f"更新依頼を取り消しました。request_mode={updated_request.get('request_mode', '')}")
                    else:
                        st.info("取り消す更新依頼はありませんでした。")
                    st.rerun()
                except Exception as exc:
                    st.error(f"更新依頼の取り消しに失敗しました: {exc}")
            return
        action_cols = st.columns(4)
        if action_cols[0].button("0915を更新", key="request-0915", help="control-plane branch に 0915 更新依頼を書き込みます"):
            try:
                submitted, updated_request = submit_control_plane_update_request(token, settings, requested_by="streamlit-cloud-viewer", requested_mode="0915")
                if submitted:
                    st.success(f"0915 更新依頼を送信しました。requested_at={updated_request.get('requested_at', '')}")
                else:
                    st.info("すでに更新依頼が入っているため、二重依頼は行いませんでした。")
                st.rerun()
            except Exception as exc:
                st.error(f"0915 更新依頼の送信に失敗しました: {exc}")
        if action_cols[1].button("1130を更新", key="request-1130", help="control-plane branch に 1130 更新依頼を書き込みます"):
            try:
                submitted, updated_request = submit_control_plane_update_request(token, settings, requested_by="streamlit-cloud-viewer", requested_mode="1130")
                if submitted:
                    st.success(f"1130 更新依頼を送信しました。requested_at={updated_request.get('requested_at', '')}")
                else:
                    st.info("すでに更新依頼が入っているため、二重依頼は行いませんでした。")
                st.rerun()
            except Exception as exc:
                st.error(f"1130 更新依頼の送信に失敗しました: {exc}")
        if action_cols[2].button("1530を更新", key="request-1530", help="control-plane branch に 1530 更新依頼を書き込みます"):
            try:
                submitted, updated_request = submit_control_plane_update_request(token, settings, requested_by="streamlit-cloud-viewer", requested_mode="1530")
                if submitted:
                    st.success(f"1530 更新依頼を送信しました。requested_at={updated_request.get('requested_at', '')}")
                else:
                    st.info("すでに更新依頼が入っているため、二重依頼は行いませんでした。")
                st.rerun()
            except Exception as exc:
                st.error(f"1530 更新依頼の送信に失敗しました: {exc}")
        if action_cols[3].button("nowを更新", key="request-now", help="control-plane branch に now 更新依頼を書き込みます"):
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
    st.header("現在の表示")
    st.caption("表示対象: latest_0915.json / latest_1130.json / latest_1530.json / latest_now.json")
    available_modes = _available_viewer_snapshot_modes(settings)
    if not available_modes:
        st.warning("まだ snapshot がありません")
    elif len(available_modes) == 1:
        mode = available_modes[0]
        bundle = load_saved_snapshot(mode, settings)
        _render_bundle(bundle, source_label=f"latest_{mode}.json を表示しました", is_saved_snapshot=True)
    else:
        tabs = st.tabs([f"{mode}" for mode in available_modes])
        for tab, mode in zip(tabs, available_modes):
            with tab:
                bundle = load_saved_snapshot(mode, settings)
                _render_bundle(bundle, source_label=f"latest_{mode}.json を表示しました", is_saved_snapshot=True)
    st.divider()
    _render_control_plane_status(settings)


def render_app() -> None:
    st.set_page_config(page_title="Sector Strength Live", layout="wide")
    st.title("セクター強度ライブ")
    settings = get_settings()
    if _is_streamlit_cloud():
        st.caption("Cloud では viewer-only で動作します。保存済み snapshot の表示と更新依頼だけを行います。")
        st.info("latest_0915.json / latest_1130.json / latest_1530.json / latest_now.json を優先して読み込みます。Cloud では collector / kabu live 取得は実行しません。")
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
