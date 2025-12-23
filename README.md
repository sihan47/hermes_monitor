# Hermes Monitor

功能：
- `get_product.py`：抓 Hermes 分類頁（支援多地區），輸出 `products_*.json`，並寫入 `debug_*.html` 方便除錯。
- `main.py`：循環抓取 + 篩選（關鍵字/可購/是否包款），命中後印出並推送 Telegram / LINE。
- `webhook_users.py`：LINE webhook，收集 userId 並更新 `line_users.json`（含使用者偏好）。

## Quick start
1) 安裝：`pip install -r requirements.txt`
2) 編輯 `config.yaml`（分類 URL、篩選、輪詢、Telegram/LINE）
3) 設定 `.env`（例如 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` / `LINE_CHANNEL_ACCESS_TOKEN`）
4) 抓取+通知：`python main.py`（或 `python main.py -c config.yaml`）
5) 只抓資料：`python get_product.py`

### Conda 環境
- 建立：`conda env create -f environment.yml`
- 啟用：`conda activate hermes-monitor`
- 更新：`conda env update -f environment.yml --prune`

## Config (`config.yaml`)
`main.py` / `get_product.py` 主要使用以下欄位（其餘欄位目前未被程式讀取）：
- `scraper`：分類頁/首頁/存檔位置（主站）。
- `scraper_fr` / `scraper_tw` / `scraper_jp`：各地區分類頁與輸出檔。
- `filter`：include/exclude 關鍵字、是否只要包款、是否只要可購；通知會顯示命中的 include 關鍵字。
- `polling`：查詢間隔秒數區間。
- `telegram`：`enabled`、`bot_token`、`chat_id` / `chat_ids`（亦可用 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID(S)`）。
- `line`：`enabled`、`channel_access_token`、`user_db`（`LINE_CHANNEL_ACCESS_TOKEN` 會覆蓋設定）。

LINE 使用者偏好寫在 `line_users.json`（可依 user 設定關鍵字/地區/可購/只看包款/通知期限）。

## Files
- `main.py` — 篩選 + Telegram/LINE notifier（循環輪詢）。
- `get_product.py` — 抓分類頁，輸出 `products_*.json`。
- `webhook_users.py` — LINE webhook + `line_users.json` 維護。
- `config.yaml` — 設定（scraper/filter/polling/telegram/line）。
- `requirements.txt` — requests、beautifulsoup4、PyYAML、line-bot-sdk。
- `test_parse.py` — debug.html 解析小工具。
- `webhook_debug.py` / `regist_line.py` / `Get_user.py` — LINE 測試/輔助腳本。
