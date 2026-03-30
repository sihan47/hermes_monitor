import asyncio
import time
from playwright.async_api import async_playwright

async def monitor_product_via_cdp():
    base_url = "https://www.hermes.com/nl/en/product/garden-party-36-bag-H051559CK18/"

    async with async_playwright() as p:
        try:
            print(f"[{time.strftime('%H:%M:%S')}] 連接到真實 Chrome (Port 9222)...")
            browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
            context = browser.contexts[0]
            page = await context.new_page()

            # 🛑 核心修正：移除所有假會的 Cache-Busting，做個正常人
            print(f"[{time.strftime('%H:%M:%S')}] 以最純粹的方式載入單一商品頁面...")
            
            # 直接前往乾淨的原始網址
            await page.goto(base_url, wait_until="networkidle", timeout=60000)
            
            await asyncio.sleep(2)

            print(f"[{time.strftime('%H:%M:%S')}] 進行 DOM 文本量測...")
            page_text = await page.evaluate("document.body.innerText")
            page_text_lower = page_text.lower()
            
            positive_targets = ['garden party', 'add to cart']
            found_positive = [t for t in positive_targets if t in page_text_lower]
            negative_signals = ['oops', 'no longer available', 'out of stock']
            found_negative = [s for s in negative_signals if s in page_text_lower]

            if found_positive and not found_negative:
                print("\n" + "="*50)
                print(f"🚨 [商品存活] 成功讀取目標！抓取到特徵: {', '.join(found_positive).upper()}")
                print("="*50 + "\n")
            else:
                if found_negative:
                    print(f"❌ 商品已下架或無庫存 (偵測到: {', '.join(found_negative).upper()})")
                else:
                    print("❌ 畫面未顯示預期內容 (可能是禁止訪問或結構改變)。")
                    
            await page.screenshot(path="pure_navigation_test.png")
            print("📸 已儲存目前畫面狀態至: pure_navigation_test.png")

        except Exception as e:
            print(f"❌ 發生錯誤: {e}")
        finally:
            if 'page' in locals():
                await page.close()

if __name__ == "__main__":
    asyncio.run(monitor_product_via_cdp())