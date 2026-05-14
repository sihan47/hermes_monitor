"""
測試 Next.js /_next/data/{buildId}/ 端點能否繞過 DataDome 拿到即時商品資料。

流程：
  1. 用現有 curl_cffi 抓分類頁（已知可行）→ 提取 buildId
  2. 組出 product/category 的 _next/data JSON URL
  3. 測試能否直接打這個 JSON 端點
"""
import sys
import json
import re
sys.path.insert(0, '.')

from get_product import (
    create_session, _session_get, parse_products_from_html,
    _looks_like_blocked_page, _extract_products_from_state,
)

CATEGORY_URL = 'https://www.hermes.com/nl/en/category/leather-goods/bags-and-clutches/womens-bags-and-clutches/'
HOMEPAGE_URL = 'https://www.hermes.com/nl/en/'
PRODUCT_SLUG = 'nl/en/product/hermes-geta-bag-H083052CKBY'  # without leading /


def extract_build_id(html: str) -> str:
    """Extract Next.js buildId from __NEXT_DATA__ script or static asset paths."""
    # Primary: __NEXT_DATA__ JSON blob
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>', html, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            build_id = data.get('buildId', '')
            if build_id:
                return build_id
        except Exception:
            pass

    # Fallback: /_next/static/{buildId}/ references in script/link tags
    m = re.search(r'/_next/static/([^/\s"\']+)/', html)
    if m:
        candidate = m.group(1)
        # buildId looks like a hash or 'chunks' — skip known non-buildId segments
        if candidate not in ('chunks', 'css', 'media', 'images'):
            return candidate

    return ''


def test_nextjs_endpoint(session, build_id: str, path: str, label: str) -> dict:
    """
    path: e.g. 'nl/en/product/hermes-geta-bag-H083052CKBY'
    Returns dict with keys: status, has_products, products, raw_preview
    """
    url = f'https://www.hermes.com/_next/data/{build_id}/{path}.json'
    print(f'\n  [{label}] GET {url}')
    try:
        resp = _session_get(session, url, timeout=20)
        body = resp.text
        print(f'  status={resp.status_code}  len={len(body)}  ct={resp.headers.get("content-type","?")}')

        if resp.status_code != 200:
            print(f'  >>> non-200, body preview: {body[:200]}')
            return {'status': resp.status_code, 'has_products': False, 'products': []}

        # Try parsing as JSON
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            print(f'  >>> not valid JSON, preview: {body[:200]}')
            return {'status': resp.status_code, 'has_products': False, 'products': []}

        # Walk the JSON for product records (same logic as hermes-state)
        products = _extract_products_from_state(data)
        print(f'  products_found={len(products)}')
        if products:
            p = products[0]
            print(f'  sample: {p.get("name")} | unavailable={p.get("unavailable")} | price={p.get("price")}')

        # Also show top-level keys for diagnosis
        top_keys = list(data.keys()) if isinstance(data, dict) else []
        print(f'  top-level keys: {top_keys}')

        return {'status': resp.status_code, 'has_products': bool(products), 'products': products, 'data': data}

    except Exception as e:
        print(f'  ERROR: {e}')
        return {'status': None, 'has_products': False, 'products': []}


# ── Step 1: fetch category page to get buildId ────────────────────────────────
print('=== Step 1: fetch category page → extract buildId ===')
session = create_session(homepage_url=HOMEPAGE_URL, impersonate_profiles=['safari_ios', 'safari17_0', 'chrome124'])
resp = _session_get(session, CATEGORY_URL, timeout=25)
cat_html = resp.text
print(f'  category page status={resp.status_code}  blocked={_looks_like_blocked_page(cat_html)}')

build_id = extract_build_id(cat_html)
print(f'  buildId="{build_id}"')

if not build_id:
    print('[ERROR] Could not extract buildId from category page. Stopping.')
    sys.exit(1)

# ── Step 2: test _next/data endpoints ─────────────────────────────────────────
print('\n=== Step 2: test /_next/data/ endpoints ===')

# 2a. Product page JSON
result_product = test_nextjs_endpoint(session, build_id, PRODUCT_SLUG, 'product page JSON')

# 2b. Category page JSON (as control — we know category HTML works, see if JSON also works)
cat_slug = 'nl/en/category/leather-goods/bags-and-clutches/womens-bags-and-clutches'
result_category = test_nextjs_endpoint(session, build_id, cat_slug, 'category page JSON')

# ── Summary ───────────────────────────────────────────────────────────────────
print('\n=== Summary ===')
print(f'  buildId:          {build_id}')
print(f'  product JSON:     status={result_product["status"]}  has_products={result_product["has_products"]}')
print(f'  category JSON:    status={result_category["status"]}  has_products={result_category["has_products"]}')

if result_product['has_products']:
    print('\n>>> SUCCESS: product JSON accessible and parseable — 即時商品資料可取得！')
elif result_product['status'] == 200 and not result_product['has_products']:
    print('\n>>> PARTIAL: product JSON returned 200 but no products parsed — 需要檢查結構')
else:
    print('\n>>> FAILED: product JSON blocked or not available')
