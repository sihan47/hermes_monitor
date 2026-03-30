import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) "
    "Gecko/20100101 Firefox/130.0"
)


def _read_hermes_state(html_text: str) -> Dict[str, Any]:
    match = re.search(
        r'<script id="hermes-state" type="application/json">(.*?)</script>',
        html_text,
        re.S,
    )
    if not match:
        raise ValueError("hermes-state script not found")
    return json.loads(match.group(1))


def _find_products_source(state: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    for value in state.values():
        if not isinstance(value, dict):
            continue
        if value.get("u") != "https://bck.hermes.com/products":
            continue
        body = value.get("b") or {}
        if not isinstance(body, dict):
            continue
        query = body.get("url")
        if isinstance(query, str) and query:
            return value.get("u"), query
    return None, None


def _infer_accept_language(homepage_url: str) -> str:
    lowered = homepage_url.lower()
    if "/fr/fr/" in lowered:
        return "fr-FR,fr;q=0.9,en;q=0.6"
    if "/de/de/" in lowered:
        return "de-DE,de;q=0.9,en;q=0.6"
    if "/nl/en/" in lowered:
        return "nl-NL,nl;q=0.9,en;q=0.6"
    if "/be/en/" in lowered:
        return "en-BE,en;q=0.9"
    return "en-US,en;q=0.9"


def _build_session(homepage_url: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Language": _infer_accept_language(homepage_url),
            "Referer": homepage_url,
        }
    )
    return session


def main() -> None:
    parser = argparse.ArgumentParser(description="Test direct access to Hermes bck products endpoint")
    parser.add_argument(
        "--html-path",
        default="debug_fr.html",
        help="Local Hermes category HTML snapshot containing hermes-state",
    )
    parser.add_argument(
        "--homepage-url",
        default="https://www.hermes.com/fr/fr/",
        help="Homepage used to warm the session before testing the API",
    )
    parser.add_argument(
        "--products-url",
        default="",
        help="Full bck.hermes.com/products URL. If omitted, extract it from --html-path",
    )
    args = parser.parse_args()

    products_url = args.products_url
    extracted_query = None
    if not products_url:
        html_path = Path(args.html_path)
        html_text = html_path.read_text(encoding="utf-8", errors="ignore")
        state = _read_hermes_state(html_text)
        base_url, query = _find_products_source(state)
        if not base_url or not query:
            raise SystemExit("Could not find bck.hermes.com/products source in hermes-state")
        products_url = f"{base_url}?{query}"
        extracted_query = query

    session = _build_session(args.homepage_url)
    homepage_resp = session.get(args.homepage_url, timeout=20)
    products_resp = session.get(products_url, timeout=20)

    result = {
        "homepage_url": args.homepage_url,
        "products_url": products_url,
        "extracted_query": extracted_query,
        "homepage_status": homepage_resp.status_code,
        "homepage_x_datadome": homepage_resp.headers.get("x-datadome"),
        "homepage_cf_cache_status": homepage_resp.headers.get("cf-cache-status"),
        "products_status": products_resp.status_code,
        "products_content_type": products_resp.headers.get("content-type"),
        "products_x_datadome": products_resp.headers.get("x-datadome"),
        "products_cf_cache_status": products_resp.headers.get("cf-cache-status"),
        "products_is_json": "json" in (products_resp.headers.get("content-type") or "").lower(),
        "products_text_head": products_resp.text[:500],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
