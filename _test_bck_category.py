"""
測試直接打 bck.hermes.com/category/ API 端點。
這是 Hermes SSR 用來抓商品列表的後端 API，如果能直接存取就繞過前端 CDN cache。
"""
import sys, json, re
sys.path.insert(0, '.')
from get_product import create_session, _session_get, _extract_products_from_state

HOMEPAGE_URL = 'https://www.hermes.com/nl/en/'

# 從 hermes-state 看到的 category code
CATEGORY_ENDPOINTS = [
    ('NL bags', 'https://bck.hermes.com/category/WOMEN_BAGS_AND_CLUTCHES', {'locale': 'nl_en'}),
    ('NL bags lang', 'https://bck.hermes.com/category/WOMEN_BAGS_AND_CLUTCHES', {'lang': 'nl_EN', 'locale': 'nl_en'}),
    ('products list', 'https://bck.hermes.com/products', {'locale': 'nl_en'}),
]

session = create_session(homepage_url=HOMEPAGE_URL, impersonate_profiles=['safari_ios', 'safari17_0', 'chrome124'])

for label, url, params in CATEGORY_ENDPOINTS:
    param_str = '&'.join(f'{k}={v}' for k, v in params.items())
    full_url = f'{url}?{param_str}' if params else url
    print(f'\n=== [{label}] ===')
    print(f'  GET {full_url}')
    try:
        resp = _session_get(session, full_url, timeout=20)
        body = resp.text
        ct = resp.headers.get('content-type', '?')
        print(f'  status={resp.status_code}  len={len(body)}  ct={ct}')

        if resp.status_code == 200:
            try:
                data = json.loads(body)
                # Show top-level structure
                if isinstance(data, dict):
                    top = {k: f'{type(v).__name__}[{len(v)}]' if isinstance(v, (list, dict)) else type(v).__name__
                           for k, v in list(data.items())[:10]}
                    print(f'  JSON keys: {top}')
                    # Try parsing products
                    products = _extract_products_from_state(data)
                    print(f'  products parsed: {len(products)}')
                    if products:
                        p = products[0]
                        print(f'  sample: {p.get("name")} | unavail={p.get("unavailable")} | price={p.get("price")}')
                    # Look for product count
                    total = data.get('total') or data.get('count') or data.get('nbProducts')
                    if total:
                        print(f'  total products: {total}')
                elif isinstance(data, list):
                    print(f'  JSON list length: {len(data)}')
                    products = _extract_products_from_state(data)
                    print(f'  products parsed: {len(products)}')
            except json.JSONDecodeError:
                print(f'  not JSON, preview: {body[:200]}')
        else:
            print(f'  body preview: {body[:300]}')
    except Exception as e:
        print(f'  ERROR: {e}')
