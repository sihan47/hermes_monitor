import asyncio
import json
import time
from pathlib import Path
from playwright.async_api import async_playwright

OUTPUT_DIR = Path(".")
COOKIES_FILE = OUTPUT_DIR / "cookies_dump.json"

async def silent_harvest():
    async with async_playwright() as p:
        try:
            print("[INFO] 正在靜默連接到真實 Chrome (Port 9222)...")
            browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
            context = browser.contexts[0]

            # 🛑 關鍵：我們「不」獲取 page，也「不」呼叫 goto。
            # 直接從 Context 底層把記憶體裡的 Cookie 抽出來。
            cookies = await context.cookies()
            
            # 過濾出 Hermes 的憑證
            hermes_cookies = [c for c in cookies if "hermes.com" in c.get("domain", "")]

            output = {
                "timestamp": time.time(),
                "cookie_count": len(hermes_cookies),
                "cookies": hermes_cookies,
            }

            COOKIES_FILE.write_text(
                json.dumps(output, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            
            print(f"[SUCCESS] 靜默收割完成！已無痛取得 {len(hermes_cookies)} 個 Cookies。")
            has_datadome = any(c.get("name") == "datadome" for c in hermes_cookies)
            print(f"[INFO] Datadome 狀態: {'✅ 存在' if has_datadome else '❌ 缺失'}")

        except Exception as exc:
            print(f"[ERROR] 執行失敗: {exc}")

if __name__ == "__main__":
    asyncio.run(silent_harvest())