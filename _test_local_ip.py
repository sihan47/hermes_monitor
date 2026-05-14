"""
從本機（residential IP）直接用 curl_cffi 打商品頁。
如果這能過，代表只要把家裡的 IP 接給 VPS 用就夠了，不需要真實 Chrome。
"""
import sys
sys.path.insert(0, '.')
from get_product import create_session, _session_get, parse_products_from_html, _looks_like_blocked_page

TARGET   = 'https://www.hermes.com/nl/en/product/hermes-geta-bag-H083052CKBY/'
HOMEPAGE = 'https://www.hermes.com/nl/en/'

print('=== 從本機 IP 直接打商品頁 (curl_cffi, no proxy) ===')
for profile in ['safari_ios', 'safari17_0', 'chrome124']:
    session = create_session(homepage_url=HOMEPAGE, impersonate_profiles=[profile])
    resp = _session_get(session, TARGET, timeout=25)
    html = resp.text
    blocked  = _looks_like_blocked_page(html)
    has_state = 'hermes-state' in html
    products = parse_products_from_html(html) if not blocked else []
    print(f'  profile={profile}  status={resp.status_code}  blocked={blocked}  has_state={has_state}  products={len(products)}')
    if not blocked and has_state:
        if products:
            p = products[0]
            print(f'  >>> SUCCESS: {p["name"]} | unavailable={p["unavailable"]} | price={p["price"]}')
        break
