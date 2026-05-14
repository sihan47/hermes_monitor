"""
測試 playwright-stealth 能不能過 DataDome 商品頁。
在 VPS 上執行前先裝：
  pip install playwright playwright-stealth
  playwright install chromium
  playwright install-deps chromium
"""
import asyncio
import json
import sys
import re
import os

sys.path.insert(0, '.')
from get_product import parse_products_from_html, _looks_like_blocked_page

TARGET   = 'https://www.hermes.com/nl/en/product/hermes-geta-bag-H083052CKBY/'
HOMEPAGE = 'https://www.hermes.com/nl/en/'

async def test():
    try:
        from playwright.async_api import async_playwright
        from playwright_stealth import stealth_async
    except ImportError as e:
        print(f'[MISSING] {e}')
        print('Run: pip install playwright playwright-stealth && playwright install chromium && playwright install-deps chromium')
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',    # VPS 必要
                '--disable-gpu',
                '--window-size=1920,1080',
            ],
        )
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            locale='nl-NL',
            timezone_id='Europe/Amsterdam',
            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        )
        page = await context.new_page()
        await stealth_async(page)

        print(f'[1] Visiting homepage: {HOMEPAGE}')
        await page.goto(HOMEPAGE, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(2)

        # Accept GDPR if present
        try:
            btn = page.locator('#didomi-notice-agree-button')
            if await btn.is_visible(timeout=3000):
                await btn.click()
                await asyncio.sleep(1)
                print('[1] GDPR accepted')
        except Exception:
            pass

        print(f'[2] Visiting product page: {TARGET}')
        resp = await page.goto(TARGET, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(3)

        status = resp.status if resp else '?'
        html   = await page.content()
        title  = await page.title()

        print(f'    status={status}  title="{title}"')
        print(f'    has_hermes_state={"hermes-state" in html}')
        print(f'    blocked={_looks_like_blocked_page(html)}')
        print(f'    has_datadome_challenge={"geo.captcha-delivery.com" in html}')

        if 'hermes-state' in html and not _looks_like_blocked_page(html):
            products = parse_products_from_html(html)
            print(f'    products parsed={len(products)}')
            if products:
                p0 = products[0]
                print(f'    [{p0["name"]}] unavailable={p0["unavailable"]} price={p0["price"]}')
            print('\n>>> SUCCESS: product page accessible via Playwright stealth')
        else:
            print('\n>>> FAILED: product page still blocked')
            # Save screenshot for diagnosis
            await page.screenshot(path='_pw_stealth_test.png')
            print('    Screenshot saved: _pw_stealth_test.png')

        # Also check cookies
        cookies = await context.cookies()
        cookie_names = [c['name'] for c in cookies if 'hermes' in c.get('domain','')]
        xsrf = next((c['value'] for c in cookies if c['name'].lower() == 'x-xsrf-token'), None)
        print(f'    hermes cookies: {cookie_names}')
        print(f'    x-xsrf-token: {"FOUND" if xsrf else "not found"}')

        await browser.close()

asyncio.run(test())
