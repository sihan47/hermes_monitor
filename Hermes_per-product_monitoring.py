import json
import argparse
import logging
import math
import os
import time
import re
from pathlib import Path
from typing import Any, Dict, Optional

from scrapingant_client import ScrapingAntClient

def _load_env(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())

_load_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("HermesMonitor")

class HermesMonitor:
    def __init__(self, sa_token: str, target_sku: str) -> None:
        self.client = ScrapingAntClient(token=sa_token)
        self.sku = target_sku
        self.session_cookies: Optional[Dict[str, str]] = None
        self.xsrf_token: Optional[str] = None

        self.init_url = "https://www.hermes.com/nl/en/cart/"
        self.api_endpoint = "https://bck.hermes.com/add-to-cart"

    def _refresh_session(self) -> bool:
        """Phase 1: 高成本獲取乾淨的 Session 與 XSRF Token (The Ultimate Exploit)"""
        logger.info("🔧 [Phase 1] 啟動 Headless Browser (Residential IP) 進行強制突破...")
        try:
            # 💡 終極武器：模擬人類行為 + 強制 API 握手 + DOM Cookie 輸出
            js_hack = """
            // 1. 模擬真實人類行為以繞過 DataDome 探針
            window.scrollTo({top: 500, behavior: 'smooth'});
            window.dispatchEvent(new MouseEvent('mousemove', {clientX: 250, clientY: 250}));
            await new Promise(r => setTimeout(r, 2000));
            
            // 2. 處理 GDPR 隱私牆
            let btn = document.getElementById('didomi-notice-agree-button');
            if(btn) btn.click();
            await new Promise(r => setTimeout(r, 1500));
            
            // 3. 強制與後端進行握手，索要 XSRF Token
            try {
                // 模擬一個前端的背景請求，強迫後端派發 Session Cookie
                await fetch('https://bck.hermes.com/v1/ping', {credentials: 'include'});
            } catch(e) {}
            await new Promise(r => setTimeout(r, 3000));
            
            // 4. 絕對擷取：將真實 Cookie 寫死在 DOM 裡，防止 SDK 漏接
            let debugDiv = document.createElement('div');
            debugDiv.id = 'ultimate_cookie_dump';
            debugDiv.innerText = "COOKIE_DUMP::" + document.cookie;
            document.body.appendChild(debugDiv);
            """

            # 必須為 True 且使用住宅代理
            res = self.client.general_request(
                self.init_url, 
                browser=True, 
                proxy_type="residential", 
                proxy_country="nl",
                js_snippet=js_hack
            )
            
            self.session_cookies = {}
            html_content = getattr(res, 'content', getattr(res, 'text', ''))
            
            # 策略 A: 從 ScrapingAnt SDK 解析
            for c in getattr(res, 'cookies', []):
                name = c.get('name') if isinstance(c, dict) else getattr(c, 'name', '')
                value = c.get('value') if isinstance(c, dict) else getattr(c, 'value', '')
                if name:
                    self.session_cookies[name.lower()] = value 

            # 策略 B: 從 DOM 暴力提取 (The Ultimate Fallback)
            if "COOKIE_DUMP::" in html_content:
                dump_match = re.search(r'COOKIE_DUMP::([^<]+)', html_content)
                if dump_match:
                    raw_cookies = dump_match.group(1)
                    for pair in raw_cookies.split(';'):
                        if '=' in pair:
                            k, v = pair.strip().split('=', 1)
                            self.session_cookies[k.lower()] = v
            
            self.xsrf_token = self.session_cookies.get('x-xsrf-token')
            available_keys = list(self.session_cookies.keys())
            logger.info(f"🔍 [Diagnostic] 成功取得 {len(available_keys)} 個 Cookies。")

            if not self.xsrf_token:
                if "datadome" in html_content.lower() or "challenge" in html_content.lower():
                    logger.error("🚫 遭遇 DataDome 隱形驗證碼，本次住宅 IP 被識破，等待重試。")
                else:
                    logger.warning("⚠️ 網頁載入成功但無 Token，可能被 Soft Block，等待重試。")
                return False
                
            logger.info(f"✅ 突破成功！取得 XSRF-Token: {self.xsrf_token[:8]}...")
            return True
            
        except Exception as e:
            logger.error(f"❌ Session 獲取發生異常: {e}")
            self.session_cookies = None
            self.xsrf_token = None
            return False

    def test_init_request(self) -> Any:
        return self.client.general_request(self.init_url, browser=True)

    def _execute_cart_probe(self) -> str:
        if not self.session_cookies or not self.xsrf_token:
            return "NEEDS_REFRESH"

        payload = json.dumps(
            {
                "locale": "nl_en",
                "items": [{"category": "direct", "sku": self.sku, "custom_options": {}}],
            }
        )
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Locale": "nl_en",
            "Origin": "https://www.hermes.com",
            "Referer": "https://www.hermes.com/",
            "X-Xsrf-Token": self.xsrf_token,
        }

        logger.info("⚡ [Phase 2] 呼叫 Cart API 測試 SKU=%s (Cost: Low)", self.sku)

        try:
            response = self.client.general_request(
                self.api_endpoint,
                method="POST",
                data=payload,
                headers=headers,
                cookies=self.session_cookies,
                browser=False,
            )
        except Exception as exc:
            logger.error("API 呼叫失敗: %s", exc)
            return "NEEDS_REFRESH"

        if response.status_code in (401, 403):
            logger.warning("🛡️ API 回傳 %s (WAF 阻擋)，憑證已失效。", response.status_code)
            return "NEEDS_REFRESH"

        if response.status_code not in (200, 201):
            logger.info("📉 API 回傳非成功狀態 %s，視為無庫存", response.status_code)
            return "OUT_OF_STOCK"

        try:
            data: Dict[str, Any] = response.json()
        except Exception as exc:
            logger.warning("API 成功但 JSON 解析失敗: %s", exc)
            return "OUT_OF_STOCK"

        items_in_cart = data.get("basket", {}).get("items", [])
        if any(item.get("sku") == self.sku for item in items_in_cart):
            logger.info("🎉 購物車回應中發現目標 SKU！絕對有貨！")
            return "IN_STOCK"

        logger.info("💨 請求通過但購物車無目標 SKU (假成功/秒缺貨)")
        return "OUT_OF_STOCK"

    def run(self, interval_seconds: int = 60) -> None:
        logger.info("🚀 開始監控 SKU=%s", self.sku)

        while True:
            if not self.session_cookies:
                success = self._refresh_session()
                if not success:
                    logger.info("💤 Session 初始化失敗，180 秒後換 IP 重試...")
                    time.sleep(180)
                    continue

            status = self._execute_cart_probe()

            if status == "IN_STOCK":
                logger.info("🚨 偵測到目標 SKU 已加入購物車，系統暫停運行以保留狀態！")
                break

            if status == "NEEDS_REFRESH":
                self.session_cookies = None
                self.xsrf_token = None
                logger.info("🔄 準備重置 Session，5 秒後重新獲取...")
                time.sleep(5)
                continue

            logger.info("⏳ 目前無庫存，%s 秒後再試", interval_seconds)
            time.sleep(interval_seconds)

def _read_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hermes per-product monitoring")
    parser.add_argument("--test-init", action="store_true", help="Only run the initial browser request once")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    token = _read_required_env("SCRAPINGANT_API_TOKEN")
    target_sku = "TESTSKU" if args.test_init else input("Target SKU: ").strip()
    if not target_sku:
        raise RuntimeError("Target SKU is required")

    monitor = HermesMonitor(sa_token=token, target_sku=target_sku)
    if args.test_init:
        response = monitor.test_init_request()
        cookies = getattr(response, "cookies", None) or []
        logger.info("Init request status=%s cookies=%s", getattr(response, "status_code", "?"), len(cookies))
        return

    interval_raw = input("Interval seconds [60]: ").strip()
    if interval_raw:
        try:
            interval_seconds = max(1, int(interval_raw))
        except ValueError:
            interval_seconds = 60
    else:
        interval_seconds = 60
    monitor.run(interval_seconds=interval_seconds)

if __name__ == "__main__":
    main()