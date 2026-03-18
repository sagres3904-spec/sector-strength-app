import shutil
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


@dataclass(frozen=True)
class SnapshotStoreResult:
    paths: dict[str, str]
    source_label: str
    backend_name: str
    warning_message: str = ""


class SnapshotStoreError(RuntimeError):
    pass


def _normalize_backend_name(settings: dict[str, Any]) -> str:
    return str(settings.get("SNAPSHOT_BACKEND", "local") or "local").strip().lower()


def resolve_snapshot_backend(settings: dict[str, Any]) -> tuple[str, str]:
    backend_name = _normalize_backend_name(settings)
    if backend_name != "gcs":
        return "local", ""
    try:
        from google.cloud import storage  # noqa: F401
    except ModuleNotFoundError:
        return "local", "google-cloud-storage が未導入のため、共通保存先(GCS)を利用できません。"
    if not _gcs_bucket(settings):
        return "local", "SNAPSHOT_GCS_BUCKET が未設定のため、共通保存先(GCS)を利用できません。"
    return "gcs", ""


def _snapshot_local_dir(settings: dict[str, Any], root_dir: Path) -> Path:
    configured = str(settings.get("SNAPSHOT_LOCAL_DIR") or settings.get("SNAPSHOT_OUTPUT_DIR") or "data/snapshots").strip()
    path = Path(configured)
    return path if path.is_absolute() else root_dir / path


def _snapshot_file_names(mode: str, generated_at: str) -> dict[str, str]:
    generated_dt = datetime.fromisoformat(generated_at)
    if mode == "now":
        archive_stem = generated_dt.strftime("%Y-%m-%d_%H%M%S_now")
        latest_stem = "latest_now"
    else:
        archive_stem = generated_dt.strftime(f"%Y-%m-%d_{mode}")
        latest_stem = f"latest_{mode}"
    return {
        "archive_json": f"{archive_stem}.json",
        "archive_md": f"{archive_stem}.md",
        "latest_json": f"{latest_stem}.json",
        "latest_md": f"{latest_stem}.md",
    }


def _gcs_prefix(settings: dict[str, Any]) -> str:
    return str(settings.get("SNAPSHOT_GCS_PREFIX", "sector-app/snapshots") or "").strip().strip("/")


def _gcs_bucket(settings: dict[str, Any]) -> str:
    return str(settings.get("SNAPSHOT_GCS_BUCKET", "") or "").strip()


def _build_gcs_uri(bucket: str, object_name: str) -> str:
    return f"gs://{bucket}/{object_name}"


def _write_local_snapshot_bundle(
    *,
    mode: str,
    generated_at: str,
    json_text: str,
    markdown_text: str,
    settings: dict[str, Any],
    root_dir: Path,
    write_drive: bool,
    warning_message: str = "",
) -> SnapshotStoreResult:
    output_dir = _snapshot_local_dir(settings, root_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    names = _snapshot_file_names(mode, generated_at)
    paths = {key: output_dir / value for key, value in names.items()}
    for key in ["archive_json", "latest_json"]:
        paths[key].write_text(json_text, encoding="utf-8")
    for key in ["archive_md", "latest_md"]:
        paths[key].write_text(markdown_text, encoding="utf-8")
    drive_dir = str(settings.get("DRIVE_SYNC_DIR", "")).strip()
    if write_drive and drive_dir:
        drive_path = Path(drive_dir)
        drive_path.mkdir(parents=True, exist_ok=True)
        for key in ["latest_json", "latest_md", "archive_json", "archive_md"]:
            shutil.copy2(paths[key], drive_path / paths[key].name)
    source_label = "保存元: ローカル"
    if warning_message:
        source_label = f"{source_label} (共通保存先フォールバック)"
    return SnapshotStoreResult(paths={key: str(value) for key, value in paths.items()}, source_label=source_label, backend_name="local", warning_message=warning_message)


def _write_gcs_snapshot_bundle(
    *,
    mode: str,
    generated_at: str,
    json_text: str,
    markdown_text: str,
    settings: dict[str, Any],
) -> SnapshotStoreResult:
    try:
        from google.cloud import storage
    except ModuleNotFoundError as exc:
        raise SnapshotStoreError("google-cloud-storage が未導入のため、共通保存先(GCS)を利用できません。") from exc
    bucket_name = _gcs_bucket(settings)
    if not bucket_name:
        raise SnapshotStoreError("SNAPSHOT_GCS_BUCKET が未設定のため、共通保存先(GCS)を利用できません。")
    prefix = _gcs_prefix(settings)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    names = _snapshot_file_names(mode, generated_at)
    paths: dict[str, str] = {}
    for key, filename in names.items():
        object_name = str(PurePosixPath(prefix, filename)) if prefix else filename
        blob = bucket.blob(object_name)
        content_type = "application/json; charset=utf-8" if filename.endswith(".json") else "text/markdown; charset=utf-8"
        blob.upload_from_string(json_text if filename.endswith(".json") else markdown_text, content_type=content_type)
        paths[key] = _build_gcs_uri(bucket_name, object_name)
    return SnapshotStoreResult(paths=paths, source_label="保存元: 共通保存先", backend_name="gcs")


def write_snapshot_bundle(
    *,
    mode: str,
    generated_at: str,
    json_text: str,
    markdown_text: str,
    settings: dict[str, Any],
    root_dir: Path,
    write_drive: bool = False,
) -> SnapshotStoreResult:
    backend_name = _normalize_backend_name(settings)
    if backend_name == "gcs":
        try:
            return _write_gcs_snapshot_bundle(
                mode=mode,
                generated_at=generated_at,
                json_text=json_text,
                markdown_text=markdown_text,
                settings=settings,
            )
        except Exception as exc:
            return _write_local_snapshot_bundle(
                mode=mode,
                generated_at=generated_at,
                json_text=json_text,
                markdown_text=markdown_text,
                settings=settings,
                root_dir=root_dir,
                write_drive=write_drive,
                warning_message=str(exc),
            )
    return _write_local_snapshot_bundle(
        mode=mode,
        generated_at=generated_at,
        json_text=json_text,
        markdown_text=markdown_text,
        settings=settings,
        root_dir=root_dir,
        write_drive=write_drive,
    )


def _read_local_snapshot_json(mode: str, settings: dict[str, Any], root_dir: Path) -> SnapshotStoreResult:
    output_dir = _snapshot_local_dir(settings, root_dir)
    filename = "latest_now.json" if mode == "now" else f"latest_{mode}.json"
    snapshot_path = output_dir / filename
    if not snapshot_path.exists():
        raise FileNotFoundError(f"保存済み snapshot がありません。期待したファイル: {snapshot_path.name}")
    return SnapshotStoreResult(paths={"latest_json": str(snapshot_path)}, source_label="保存元: ローカル", backend_name="local")


def _read_gcs_snapshot_json(mode: str, settings: dict[str, Any]) -> tuple[str, SnapshotStoreResult]:
    try:
        from google.cloud import storage
    except ModuleNotFoundError as exc:
        raise SnapshotStoreError("google-cloud-storage が未導入のため、共通保存先(GCS)を利用できません。") from exc
    bucket_name = _gcs_bucket(settings)
    if not bucket_name:
        raise SnapshotStoreError("SNAPSHOT_GCS_BUCKET が未設定のため、共通保存先(GCS)を利用できません。")
    filename = "latest_now.json" if mode == "now" else f"latest_{mode}.json"
    prefix = _gcs_prefix(settings)
    object_name = str(PurePosixPath(prefix, filename)) if prefix else filename
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not blob.exists():
        raise FileNotFoundError(f"保存済み snapshot がありません。期待したファイル: {filename}")
    return blob.download_as_text(encoding="utf-8"), SnapshotStoreResult(paths={"latest_json": _build_gcs_uri(bucket_name, object_name)}, source_label="保存元: 共通保存先", backend_name="gcs")


def read_snapshot_json(mode: str, settings: dict[str, Any], root_dir: Path) -> tuple[str, SnapshotStoreResult]:
    backend_name = _normalize_backend_name(settings)
    if backend_name == "gcs":
        try:
            return _read_gcs_snapshot_json(mode, settings)
        except FileNotFoundError:
            raise
        except Exception as exc:
            try:
                local_result = _read_local_snapshot_json(mode, settings, root_dir)
            except FileNotFoundError as local_exc:
                raise SnapshotStoreError(f"{exc} ローカル保存先にも対象ファイルがありませんでした。") from local_exc
            fallback_result = SnapshotStoreResult(
                paths=local_result.paths,
                source_label="保存元: ローカル (共通保存先フォールバック)",
                backend_name=local_result.backend_name,
                warning_message=str(exc),
            )
            snapshot_path = Path(local_result.paths["latest_json"])
            return snapshot_path.read_text(encoding="utf-8"), fallback_result
    local_result = _read_local_snapshot_json(mode, settings, root_dir)
    snapshot_path = Path(local_result.paths["latest_json"])
    return snapshot_path.read_text(encoding="utf-8"), local_result
