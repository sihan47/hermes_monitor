# Hermes Monitor

功能：
- `hermes_checker.py`：依 targets 判斷商品頁是否可購（支援 warmup、VPN 命令、標記判斷）。
- `get_product.py`：抓 Hermès 分類頁，輸出 `products_all.json`。
- `main.py`：循環抓取，依關鍵字篩選，命中新品時印出並可用 Telegram 推送。

## Quick start
1) 安裝：`pip install -r requirements.txt`
2) 編輯 `config.yaml`（URL、markers、篩選、Telegram）
3) 抓取+通知：`python main.py`（或 `python main.py -c config.yaml`）
4) 只抓資料：`python get_product.py`

### Conda 環境
- 建立：`conda env create -f environment.yml`
- 啟用：`conda activate hermes-monitor`
- 更新：`conda env update -f environment.yml --prune`

## Config (`config.yaml`)
- `targets` / `settings` / `output`：給 `hermes_checker.py` 用（可購偵測）。
- `warmup_urls`：先走導覽路徑（同一 session）再打商品頁，拿 cookies/Referer。
- `scraper`：分類頁、首頁、存檔位置（給 `get_product.py`/`main.py`）。
- `filter`：include/exclude 關鍵字、是否只要包款、是否只要可購；通知內容會顯示命中的 include 關鍵字。
- `polling`：查詢間隔秒數區間（預設 30–75）。
- `telegram`：`enabled` 開關；`bot_token` / `chat_id`（也可用環境變數 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`）。

可購標示預設：`add to cart` / `add to bag` / `ajouter au panier`；阻擋標示預設：`access denied` / `forbidden` / `captcha`。依地區語系調整。

## Files
- `main.py` — 篩選 + Telegram notifier（循環輪詢）。
- `get_product.py` — 抓分類頁，輸出 `products_all.json`。
- `hermes_checker.py` — 商品可購偵測，支援 warmup/VPN。
- `config.yaml` — 共用設定（scraper/filter/polling/telegram + checker）。
- `requirements.txt` — requests、beautifulsoup4、PyYAML。
