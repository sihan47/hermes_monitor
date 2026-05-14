import os
import sys
sys.path.insert(0, '.')

os.environ.setdefault('SCRAPINGANT_API_TOKEN', '0d604af1e25246c785bf4a6e3bbeb664')

from scrapingant_client import ScrapingAntClient
from scrapingant_client.errors import ScrapingantClientException, ScrapingantDetectedException
from get_product import parse_products_from_html, _looks_like_blocked_page

TARGET = 'https://www.hermes.com/nl/en/product/hermes-geta-bag-H083052CKBY/'
TOKEN = os.environ['SCRAPINGANT_API_TOKEN']

print('=== TEST: ScrapingAnt browser=True, residential NL ===')
client = ScrapingAntClient(token=TOKEN)
try:
    result = client.general_request(
        TARGET,
        browser=True,
        proxy_type='residential',
        proxy_country='nl',
    )
    content = result.content if isinstance(result.content, str) else result.content.decode('utf-8', 'ignore')
    is_blocked = _looks_like_blocked_page(content)
    has_state = 'hermes-state' in content
    products = parse_products_from_html(content) if not is_blocked else []
    print(f'  status={result.status_code} blocked={is_blocked} has_state={has_state} products={len(products)} html_len={len(content)}')
    if products:
        p = products[0]
        print(f'  name={p.get("name")} unavailable={p.get("unavailable")} price={p.get("price")}')
    if not is_blocked and has_state:
        print('  >>> SA browser SUCCESS')
    else:
        print('  >>> SA browser also failed')
except ScrapingantDetectedException as e:
    print(f'  DETECTED as bot: {e}')
except ScrapingantClientException as e:
    print(f'  SA client error: {e}')
except Exception as e:
    print(f'  ERROR: {e}')
