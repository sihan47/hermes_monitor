# Hermes Monitor

Check a list of Hermès product URLs to see if they look orderable while staying polite to bot-detection (random user agents, pacing, optional VPN hooks for Surfshark).

## Quick start
1) Install deps: `pip install -r requirements.txt`
2) Edit `config.yaml` with your URLs and markers
3) Run: `python main.py` (or `python main.py -c custom.yaml`)

### Conda 環境
- 建立：`conda env create -f environment.yml`
- 啟用：`conda activate hermes-monitor`
- 更新：`conda env update -f environment.yml --prune`

## Config (`config.yaml`)
- `targets`: list of URLs to check; each can override `available_markers`, `blocked_markers`, headers, cookies, and timeout.
- 可設定 `warmup_urls`：先走一段導覽路徑（同一個 session）再打商品頁，方便取得必要的 cookies/Referer。
- `settings`: pacing (`base_delay_seconds`, `jitter_seconds`), request timeout, optional VPN commands:
  - `use_vpn: true` to enable.
  - `vpn.connect_command` / `vpn.disconnect_command` / `vpn.rotate_command` hold your Surfshark CLI commands.
  - `vpn.rotate_every_checks`: rotate IP every N checks.
- `output`: toggle JSON writing and summary printing.

Defaults use generic availability markers (`add to cart`, `add to bag`, `ajouter au panier`) and block markers (`access denied`, `forbidden`, `captcha`). Tune them for your Hermès locale/product.

## Files
- `main.py` — CLI entry point (`-c` to point to another config).
- `hermes_checker.py` — loads config, runs checks, optional VPN connect/rotate/disconnect, writes summary.
- `config.yaml` — sample config with placeholders.
- `requirements.txt` — requests, beautifulsoup4, PyYAML.
