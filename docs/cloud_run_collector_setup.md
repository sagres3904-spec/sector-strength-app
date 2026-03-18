# Cloud Run Collector Setup

## 概要

- `collector_jq_yanoshin.py` は J-Quants ベースで snapshot を生成し、`snapshot_store.py` 経由で `local` または `gcs` に保存します。
- kabu はこの collector では使いません。
- kabu の定時実行は引き続きローカル Windows 側の運用を前提にします。

## 必要な環境変数

- `JQUANTS_API_KEY`
- `SNAPSHOT_BACKEND=local` または `gcs`
- `SNAPSHOT_LOCAL_DIR=data/snapshots`
- `SNAPSHOT_GCS_BUCKET`
- `SNAPSHOT_GCS_PREFIX=sector-app/snapshots`

## ローカル dry-run

```powershell
.\.venv\Scripts\python.exe .\collector_jq_yanoshin.py --mode 0915 --backend local
```

## Cloud Run Job の考え方

- イメージは `Dockerfile` で `collector_jq_yanoshin.py` を entrypoint にしています。
- Job ごとに引数で `--mode` を変えます。
- GCS 保存を使う場合は `SNAPSHOT_BACKEND=gcs` と `SNAPSHOT_GCS_BUCKET` を設定します。
- Cloud Run のサービスアカウントには対象 bucket への書き込み権限を付与します。

## Cloud Scheduler の想定

- `0915` 用 Job
- `1130` 用 Job
- `1530` 用 Job

必要なら `now` 用 Job を別で追加できますが、定時運用の主対象は上の 3 本です。

## 注意

- この collector が書いた snapshot は `sector_app_jq.py` の保存済み snapshot 表示でそのまま読めます。
- `source_profile=cloud_jq_yanoshin` と `includes_kabu=false` を JSON meta に持つので、後から識別できます。
- GCS 設定不足時は local backend にフォールバックし、ログと標準出力で理由を確認できます。
