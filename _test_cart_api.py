import os
import sys
import json
import re
sys.path.insert(0, '.')

os.environ.setdefault('SCRAPINGANT_API_TOKEN', '0d604af1e25246c785bf4a6e3bbeb664')

from scrapingant_client import ScrapingAntClient
from scrapingant_client.errors import ScrapingantClientException, ScrapingantDetectedException
from get_product import _looks_like_blocked_page

HOMEPAGE = 'https://www.hermes.com/nl/en/'
CART_URL  = 'https://www.hermes.com/nl/en/cart/'
BCK_URL   = 'https://bck.hermes.com/add-to-cart'
TEST_SKU  = 'H083052CKBY'
TOKEN     = os.environ['SCRAPINGANT_API_TOKEN']

client = ScrapingAntClient(token=TOKEN)
session_cookies: dict = {}
xsrf_token: str = ''

def collect_cookies(result) -> dict:
    out = {}
    for c in getattr(result, 'cookies', []):
        name  = c.get('name')  if isinstance(c, dict) else getattr(c, 'name',  '')
        value = c.get('value') if isinstance(c, dict) else getattr(c, 'value', '')
        if name:
            out[name] = value
            out[name.lower()] = value
    return out

def cookies_to_header(cdict: dict) -> str:
    """Convert cookie dict to Cookie header string (deduplicated by lowercase key)."""
    seen = set()
    parts = []
    for k, v in cdict.items():
        kl = k.lower()
        if kl not in seen:
            seen.add(kl)
            parts.append(f'{k}={v}')
    return '; '.join(parts)

js_phase1 = """
window.scrollTo({top: 200, behavior: 'smooth'});
window.dispatchEvent(new MouseEvent('mousemove', {clientX: 400, clientY: 300}));
await new Promise(r => setTimeout(r, 2000));

let btn = document.getElementById('didomi-notice-agree-button');
if (btn) { btn.click(); await new Promise(r => setTimeout(r, 1500)); }

let xsrfFromFetch = '';
try {
    let pingResp = await fetch('https://bck.hermes.com/v1/ping', {
        credentials: 'include',
        headers: {'Accept': 'application/json'}
    });
    let pingText = await pingResp.text();
    console.log('ping status:', pingResp.status, 'body:', pingText.slice(0,100));
} catch(e) { console.log('ping failed:', e); }

await new Promise(r => setTimeout(r, 3000));

let dump = {
    docCookies: document.cookie,
    metaXsrf: (document.querySelector('meta[name="xsrf-token"]') ||
               document.querySelector('meta[name="_token"]') || {}).content || '',
    lsKeys: Object.keys(localStorage),
    ssKeys: Object.keys(sessionStorage),
};
for (let k of dump.lsKeys) {
    let v = localStorage.getItem(k) || '';
    if (k.toLowerCase().includes('xsrf') || k.toLowerCase().includes('token') || k.toLowerCase().includes('csrf')) {
        dump['ls_' + k] = v.slice(0, 100);
    }
}
for (let k of dump.ssKeys) {
    let v = sessionStorage.getItem(k) || '';
    if (k.toLowerCase().includes('xsrf') || k.toLowerCase().includes('token')) {
        dump['ss_' + k] = v.slice(0, 100);
    }
}
let div = document.createElement('div');
div.id = 'dump';
div.innerText = JSON.stringify(dump);
document.body.appendChild(div);
"""

# ── Phase 1 ───────────────────────────────────────────────────────────────────
print('=== Phase 1: cart page ===')
try:
    result = client.general_request(
        CART_URL,
        browser=True,
        proxy_type='residential',
        proxy_country='nl',
        js_snippet=js_phase1,
    )
    content = result.content if isinstance(result.content, str) else result.content.decode('utf-8', 'ignore')
    print(f'  status={result.status_code}  len={len(content)}  blocked={_looks_like_blocked_page(content)}')

    session_cookies.update(collect_cookies(result))
    print(f'  SDK cookies: {list(session_cookies.keys())}')

    # DOM dump
    m = re.search(r'id="dump"[^>]*>\s*(\{[^<]+)\s*<', content)
    if m:
        try:
            dump = json.loads(m.group(1))
            print(f'  docCookies: {dump.get("docCookies","")[:200]}')
            print(f'  metaXsrf: {dump.get("metaXsrf","")}')
            print(f'  ls keys: {dump.get("lsKeys",[])}')
            for k, v in dump.items():
                if k.startswith(('ls_', 'ss_')):
                    print(f'  {k} = {v}')
            for pair in dump.get('docCookies', '').split(';'):
                if '=' in pair:
                    k, v = pair.strip().split('=', 1)
                    session_cookies[k.strip()] = v.strip()
                    session_cookies[k.strip().lower()] = v.strip()
        except Exception as e:
            print(f'  dump parse error: {e}')

    # Scan hermes-state for tokens
    state_match = re.search(r'<script[^>]+id="hermes-state"[^>]*>(.*?)</script>', content, re.DOTALL)
    if state_match:
        try:
            state = json.loads(state_match.group(1))
            state_str = json.dumps(state)
            for pat in [r'"xsrf[Tt]oken"\s*:\s*"([^"]{10,})"',
                        r'"csrfToken"\s*:\s*"([^"]{10,})"',
                        r'"token"\s*:\s*"([^"]{20,})"']:
                found = re.findall(pat, state_str)
                if found:
                    print(f'  hermes-state token hit: {found[0][:40]}')
        except Exception:
            pass

    # Broad scan of full HTML
    for pat in [r'["\']x-xsrf-token["\']\s*[=:]\s*["\']([^"\']{10,})',
                r'xsrfToken\s*[=:]\s*["\']([^"\']{10,})',
                r'csrfToken\s*[=:]\s*["\']([^"\']{10,})']:
        hits = re.findall(pat, content, re.IGNORECASE)
        if hits:
            print(f'  HTML scan hit ({pat[:30]}): {hits[0][:50]}')

    xsrf_token = (session_cookies.get('x-xsrf-token')
                  or session_cookies.get('X-Xsrf-Token')
                  or session_cookies.get('xsrf-token')
                  or session_cookies.get('XSRF-TOKEN') or '')
    print(f'\n  XSRF: {"FOUND → " + xsrf_token[:20] + "..." if xsrf_token else "NOT FOUND"}')

except ScrapingantDetectedException as e:
    print(f'  DETECTED: {e}'); sys.exit(1)
except Exception as e:
    import traceback; traceback.print_exc(); sys.exit(1)

# ── Phase 2: bck probe ───────────────────────────────────────────────────────
print(f'\n=== Phase 2: bck.hermes.com/add-to-cart SKU={TEST_SKU} ===')
payload = json.dumps({
    'locale': 'nl_en',
    'items': [{'category': 'direct', 'sku': TEST_SKU, 'custom_options': {}}],
})
extra_headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json, text/plain, */*',
    'Locale': 'nl_en',
    'Origin': 'https://www.hermes.com',
    'Referer': 'https://www.hermes.com/nl/en/cart/',
}
if xsrf_token:
    extra_headers['X-Xsrf-Token'] = xsrf_token

cookie_header = cookies_to_header(session_cookies)
if cookie_header:
    extra_headers['Cookie'] = cookie_header

print(f'  Sending with cookies: {[k for k in session_cookies.keys() if k == k.lower()]}')
print(f'  xsrf in header: {bool(xsrf_token)}')

try:
    resp = client.general_request(
        BCK_URL,
        method='POST',
        data=payload,
        headers=extra_headers,
        browser=False,
    )
    body = resp.content if isinstance(resp.content, str) else resp.content.decode('utf-8', 'ignore')
    print(f'  status={resp.status_code}')
    print(f'  body={body[:500]}')
    if resp.status_code in (200, 201):
        try:
            data = json.loads(body)
            items = data.get('basket', {}).get('items', [])
            matched = [i for i in items if i.get('sku') == TEST_SKU]
            print(f'  basket_items={len(items)}  matched={len(matched)}')
            print('  >>> IN_STOCK' if matched else '  >>> OUT_OF_STOCK / wrong SKU format')
        except Exception:
            print('  JSON parse failed')
except Exception as e:
    import traceback; traceback.print_exc()
