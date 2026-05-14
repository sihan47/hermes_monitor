import sys, re, json
sys.path.insert(0, '.')
from get_product import create_session, _session_get

CATEGORY_URL = 'https://www.hermes.com/nl/en/category/leather-goods/bags-and-clutches/womens-bags-and-clutches/'
HOMEPAGE_URL = 'https://www.hermes.com/nl/en/'

session = create_session(homepage_url=HOMEPAGE_URL, impersonate_profiles=['safari_ios'])
resp = _session_get(session, CATEGORY_URL, timeout=25)
html = resp.text
print(f'status={resp.status_code}  len={len(html)}')

# API endpoints referenced in the page
print('\n--- API / bck endpoints ---')
for pat in [
    r'bck\.hermes\.com[^\s"\'<>]+',
    r'api\.hermes\.com[^\s"\'<>]+',
    r'hermes\.com/api/[^\s"\'<>]+',
    r'"(https?://[^"]*hermes[^"]*(?:api|v\d|graphql|product|stock|inventory)[^"]*)"',
]:
    hits = list(set(re.findall(pat, html, re.IGNORECASE)))[:10]
    if hits:
        print(f'\n  Pattern: {pat[:50]}')
        for h in hits:
            print(f'    {h}')

# JS file references (might reveal API base URLs)
print('\n--- JS files loaded ---')
js_files = re.findall(r'src="([^"]*\.js[^"]*)"', html)
for f in js_files[:15]:
    print(f'  {f}')

# Any fetch/XHR patterns in inline JS
print('\n--- fetch/XHR patterns in inline JS ---')
for pat in [r'fetch\(["\']([^"\']+)["\']', r'axios\.[a-z]+\(["\']([^"\']+)["\']']:
    hits = list(set(re.findall(pat, html)))[:10]
    if hits:
        print(f'  {pat[:30]}: {hits}')

# hermes-state structure top keys
print('\n--- hermes-state top-level structure ---')
m = re.search(r'<script[^>]+id="hermes-state"[^>]*>(.*?)</script>', html, re.DOTALL)
if m:
    try:
        state = json.loads(m.group(1))
        def show_keys(obj, depth=0, prefix=''):
            if depth > 2: return
            if isinstance(obj, dict):
                for k, v in list(obj.items())[:8]:
                    vtype = type(v).__name__
                    vlen = f'[{len(v)}]' if isinstance(v, (list, dict)) else ''
                    print(f'  {"  "*depth}{prefix}{k}: {vtype}{vlen}')
                    if isinstance(v, (dict, list)) and depth < 2:
                        show_keys(v, depth+1)
        show_keys(state)
    except Exception as e:
        print(f'  parse error: {e}')
        print(f'  raw preview: {m.group(1)[:300]}')
else:
    print('  hermes-state not found')
