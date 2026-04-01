import json
import math
import sys
from pathlib import Path
from typing import Any

from get_product import parse_products_from_html
from scrapingant_source import fetch_content


def _coerce_html_text(payload: Any) -> str:
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="ignore")
    if isinstance(payload, str):
        return payload
    if payload is None:
        return ""
    return str(payload)


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python test_scrapingant_parse.py <url>")
        return 2

    url = sys.argv[1].strip()
    content = fetch_content(url)
    html = _coerce_html_text(content)
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(content, float) and math.isnan(content):
        print("FETCH_NAN")
        return 1

    if not html:
        print("FETCH_EMPTY")
        return 1

    products = parse_products_from_html(html)
    has_hermes_state = 'id="hermes-state"' in html or "id='hermes-state'" in html

    output_path = output_dir / "scrapingant_parse_test.json"
    output_path.write_text(
        json.dumps(products, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "scrapingant_parse_test.meta.json").write_text(
        json.dumps(
            {
                "url": url,
                "html_chars": len(html),
                "has_hermes_state": has_hermes_state,
                "products_count": len(products),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    if not products:
        (output_dir / "scrapingant_parse_test_snippet.txt").write_text(
            html[:1000],
            encoding="utf-8",
        )

    print(f"HTML_CHARS={len(html)}")
    print(f"HAS_HERMES_STATE={has_hermes_state}")
    print(f"PRODUCTS_COUNT={len(products)}")
    print(f"OUTPUT={output_path}")
    if not products:
        print("SNIPPET=output/scrapingant_parse_test_snippet.txt")
    if products:
        first = products[0]
        print(
            "FIRST="
            + json.dumps(
                {
                    "name": first.get("name"),
                    "color": first.get("color"),
                    "price": first.get("price"),
                    "url": first.get("url"),
                },
                ensure_ascii=False,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
