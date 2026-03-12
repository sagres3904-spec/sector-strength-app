# 52週高値圏 設計メモ

## 現状

- `sector_app_jq.py` は実行時に以下だけを取得している
  - `markets/calendar`
  - `equities/master`
  - `equities/bars/daily` の単日スナップショット
- 価格履歴としてメモリにあるのは実質的に
  - 最新日
  - 1週比較基準日
  - 1か月比較基準日
  の3断面のみ
- そのため、実行時 API を増やさずに「真の52週高値」を算出することはできない

## 採用方針

- 推奨は「別バッチで日次更新したキャッシュファイルを、アプリは読むだけにする」方式
- アプリ実行時の J-Quants API 呼び出しは増やさない
- `app.py` は変更しない

## 推奨キャッシュファイル

- パス: `data/sector_52w_cache.csv.gz`
- 理由:
  - 依存追加なしで読みやすい
  - Streamlit Community Cloud でも扱いやすい
  - Windows 環境でも中身確認が簡単

## 必須列

- `Code`
- `Date`
- `Close`
- `High`
- `Trailing52wHigh`
- `DistTo52wHighPct`
- `IsNear52wHigh`
- `IsNew52wHigh`

## Secrets 名

- GitHub Actions / ローカル共通で `JQUANTS_API_KEY`

## 判定ルール案

- `Trailing52wHigh`: 当日を含む直近252営業日の高値最大値
- `DistTo52wHighPct`: `(Close / Trailing52wHigh - 1) * 100`
- `IsNear52wHigh`: `DistTo52wHighPct >= -3.0`
- `IsNew52wHigh`: `High >= Trailing52wHigh`

## stale data

- アプリ側ではキャッシュの最終 `Date` を確認する
- `meta["latest_date"]` より2営業日以上古い場合は stale 扱い
- stale 時は候補表示を出さず、キャプションで
  - 「52週高値圏キャッシュが未更新です」
  を表示する
- ファイルが無い場合も同様に非表示でよい

## workflow の役割

- `.github/workflows/build_52w_cache.yml`
- GitHub Actions で平日夜に `scripts/build_52w_cache.py` を実行する
- 出力された `data/sector_52w_cache.csv.gz` を、変更があるときだけコミットして push する
- アプリ本体はこのファイルを読むだけにする

## 将来追加する関数名候補

- `load_52w_cache`
- `prepare_52w_flags`
- `build_52w_high_candidates`

## 方式比較メモ

### 案A: ローカル履歴キャッシュファイル方式

- Windows Codex app 上で別スクリプトを手動実行して `data/sector_52w_cache.csv.gz` を更新
- アプリはそのファイルを読むだけ
- 手軽だが、更新漏れが起きやすい

### 案B: GitHub Actions 日次更新方式

- GitHub Actions が日次で J-Quants から履歴を取得し、`data/sector_52w_cache.csv.gz` を更新
- Secrets は GitHub Actions Secrets に置く
- アプリは repo 上の最新ファイルを読むだけ
- Community Cloud と相性がよく、初心者でも運用しやすい

## ローカル手動実行の最小例

PowerShell:

```powershell
$env:JQUANTS_API_KEY="your_api_key"
python scripts/build_52w_cache.py
```

## アプリ側の挙動

- キャッシュが無い場合:
  - `52週高値圏キャッシュがありません`
- stale の場合:
  - `52週高値圏キャッシュが未更新です`
- 正常時:
  - `sector_app_jq.py` はキャッシュを読み、`52週高値更新` と `52週高値まで3%以内` を表示する
