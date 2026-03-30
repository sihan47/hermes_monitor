# Hermes Monitor

功能：
- `get_product.py`：抓 Hermes 分類頁（支援多地區），輸出 `products_*.json`，並寫入 `debug_*.html` 方便除錯。
  `product_history` 只會追加歷史上未寫過的 item，重複 item 不再重複落盤。
- `main.py`：循環抓取 + 篩選（關鍵字/可購/是否包款），命中後印出並推送 Telegram / LINE。
  也支援 discovery 測試，比較不同來源（例如首頁、sitemap、既有商品頁）是否能比分類頁更早發現商品 URL。
- `webhook_users.py`：LINE webhook，收集 userId 並更新 `line_users.json`（含使用者偏好）。
- 內建較保守的輪詢策略：共享 session、失敗退避、偵測到 challenge/rate-limit 時自動延長 cooldown。
- Telegram health alerts：抓取失敗、rate-limit/challenge、空結果會主動告警，恢復時也會推送 recovered 訊息。

## Quick start
1) 安裝：`pip install -r requirements.txt`
2) 編輯 `config.yaml`（分類 URL、篩選、輪詢、Telegram/LINE）
3) 設定 `.env`（例如 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` / `LINE_CHANNEL_ACCESS_TOKEN`）
4) 抓取+通知：`python main.py`（或 `python main.py -c config.yaml`）
5) 只抓資料：`python get_product.py`
6) 測 discovery 來源：`python main.py --discovery-test`
7) 若 `config.yaml` 的 `discovery_test.enabled: true`，平常執行 `python main.py` 也會在背景定期更新 discovery 報表

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
- `politeness`：最低輪詢間隔、失敗退避、challenge/rate-limit cooldown。
- `health_alerts`：故障告警開關、提醒間隔與 `heartbeat_seconds`；故障存在時會抑制一般 heartbeat，heartbeat 與商品通知彼此獨立。
- `discovery_test`：用 baseline 分類頁對照其他來源（`html` / `json` / `sitemap` / `auto` / `saved_products` / `history_products` / `known_product_pages`），可背景定期執行，並輸出最新報表、歷史 JSONL 與累積 summary，方便比較誰先看到目標 URL。
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
