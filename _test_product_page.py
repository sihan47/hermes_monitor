import sys
sys.path.insert(0, '.')
from get_product import create_session, parse_products_from_html, _looks_like_blocked_page, _session_get
from fetch_providers import fetch_external_html

TARGET = 'https://www.hermes.com/nl/en/product/hermes-geta-bag-H083052CKBY/'
HOMEPAGE = 'https://www.hermes.com/nl/en/'
PROFILES = ['safari_ios', 'safari17_0', 'chrome124']

print('=== TEST 1: curl_cffi direct (each profile gets a fresh session) ===')
curl_success = False
for profile in PROFILES:
    try:
        session = create_session(homepage_url=HOMEPAGE, impersonate_profiles=[profile])
        resp = _session_get(session, TARGET, timeout=25)
        html = resp.text
        is_blocked = _looks_like_blocked_page(html)
        has_state = 'hermes-state' in html
        products = parse_products_from_html(html) if not is_blocked else []
        print(f'  profile={profile} status={resp.status_code} blocked={is_blocked} has_state={has_state} products={len(products)}')
        if not is_blocked and has_state:
            print(f'  >>> curl_cffi SUCCESS with profile={profile}')
            if products:
                p = products[0]
                print(f'      name={p.get("name")} unavailable={p.get("unavailable")} price={p.get("price")}')
            curl_success = True
            break
    except Exception as e:
        print(f'  profile={profile} ERROR: {e}')

print()
print('=== TEST 2: ScrapingAnt fallback ===')
sa_html = fetch_external_html(TARGET)
if sa_html:
    is_blocked = _looks_like_blocked_page(sa_html)
    has_state = 'hermes-state' in sa_html
    products = parse_products_from_html(sa_html) if not is_blocked else []
    print(f'  blocked={is_blocked} has_state={has_state} products={len(products)} html_len={len(sa_html)}')
    if products:
        p = products[0]
        print(f'  name={p.get("name")} unavailable={p.get("unavailable")} price={p.get("price")}')
    if not is_blocked and has_state:
        print('  >>> ScrapingAnt SUCCESS')
else:
    print('  ScrapingAnt returned empty / not configured')
