# Local Scheduler Setup

## この構成にする理由

- 無料構成で継続運用するため、GCP / Cloud Scheduler / GCS / GitHub Actions の定時取得は使いません。
- 家の Windows PC で kabu にログインできる前提を活かし、09:15 / 11:30 / 15:30 の snapshot をローカルで作成します。
- Streamlit Community Cloud には `deploy/streamlit-live` ブランチの `latest_*.json` / `latest_*.md` だけを見せます。

## Streamlit Community Cloud

- 参照ブランチは `deploy/streamlit-live`
- 反映対象は `latest_0915`, `latest_1130`, `latest_1530`, `latest_now` のみ

## 自動実行専用の別クローンを使う理由

- 自動実行専用の別クローンとして `D:\株アプリ\sector-strength-app-deploy` を使います。
- 普段の開発作業ツリーと分けることで、タスクスケジューラ実行中の branch 切替や Git push が開発中の変更に干渉しません。
- `local_capture_and_publish.py` はこの別クローン上で `deploy/streamlit-live` に対して最新 snapshot だけを更新します。

## タスクスケジューラ設定例

- 09:15 実行
  - プログラム: `powershell.exe`
  - 引数: `-ExecutionPolicy Bypass -File D:\株アプリ\sector-strength-app\scripts\capture_and_publish_snapshot.ps1 -Mode 0915`
- 11:30 実行
  - プログラム: `powershell.exe`
  - 引数: `-ExecutionPolicy Bypass -File D:\株アプリ\sector-strength-app\scripts\capture_and_publish_snapshot.ps1 -Mode 1130`
- 15:30 実行
  - プログラム: `powershell.exe`
  - 引数: `-ExecutionPolicy Bypass -File D:\株アプリ\sector-strength-app\scripts\capture_and_publish_snapshot.ps1 -Mode 1530`

## 運用前提

- iPhone + Chrome Remote Desktop で家の Windows PC に入り、必要なタイミングで kabu ログイン状態を維持する前提です。
- J-Quants 側の API キーも自動実行専用クローンに設定しておきます。

## non-true-timepoint を push しない理由

- `0915`, `1130`, `1530` は JST の許容窓内で取れた snapshot だけを publish することで、UI 上の時点意味が崩れにくくなります。
- たとえば 11:30 モードなのに 14:22 JST に作成された snapshot を publish すると、閲覧側が誤認しやすくなります。
- そのため既定では `is_true_timepoint=false` の snapshot は push しません。
- どうしても publish したい場合だけ `--allow-non-true-timepoint true` を使います。
