"""
Scrape Hermès category page and save all products to JSON.
Moved from the original main.py so the new main can focus on filtering/notifications.
"""

from pathlib import Path
import json
import re
from typing import Dict, List, Sequence

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.hermes.com"
CATEGORY_URL = (
    "https://www.hermes.com/be/en/category/women/"
    "bags-and-small-leather-goods/bags-and-clutches/"
)
HOMEPAGE_URL = "https://www.hermes.com/be/en/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) "
        "Gecko/20100101 Firefox/130.0"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def create_session(homepage_url: str = HOMEPAGE_URL) -> requests.Session:
    """Create a session with base headers and prefetch homepage to collect cookies."""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(homepage_url, timeout=20)
    except Exception as exc:  # pragma: no cover - network dependent
        print(f"[WARN] Visit homepage failed: {exc}")
    return session


def fetch_category_soup(
    session: requests.Session,
    category_url: str = CATEGORY_URL,
    debug_path: str | Path = "debug.html",
) -> BeautifulSoup:
    """Fetch category HTML, save raw content for debugging, and return BeautifulSoup."""
    resp = session.get(category_url, timeout=20)
    print(f"[INFO] GET {category_url} -> {resp.status_code}")
    resp.raise_for_status()

    Path(debug_path).write_text(resp.text, encoding=resp.encoding or "utf-8")
    print(f"[INFO] Saved raw HTML to {debug_path}")

    return BeautifulSoup(resp.text, "html.parser")


def is_bag_item(name: str) -> bool:
    """Roughly determine if the item is a bag (exclude straps/other accessories)."""
    n = name.lower()
    if "strap" in n:
        return False
    if "bag" in n or "pouch" in n or "clutch" in n:
        return True
    return False


def _pick_price_line(lines: Sequence[str]) -> str | None:
    for line in lines:
        if re.search(r"\d", line):
            return line
    return None


def extract_products_from_soup(soup: BeautifulSoup) -> List[Dict]:
    """
    Parse anchors with /product/ href and build product records.
    Treat each (name, url) pair as a unique product.
    """
    products: Dict[tuple[str, str], Dict] = {}
    anchors = soup.find_all("a", href=re.compile(r"/product/"))
    print(f"[INFO] Found {len(anchors)} <a> with /product/ href")

    for anchor in anchors:
        name = anchor.get_text(strip=True)
        if not name:
            continue

        href = anchor.get("href")
        if not href:
            continue
        url = href if href.startswith("http") else BASE_URL + href

        key = (name, url)
        if key in products:
            continue

        container = anchor
        for _ in range(4):
            if container.parent is None:
                break
            container = container.parent

        full_text = container.get_text("\n", strip=True)
        lines = [line.strip() for line in full_text.splitlines() if line.strip()]

        color = None
        price = None
        unavailable = False

        for index, line in enumerate(lines):
            if line.startswith("Color") and index + 1 < len(lines):
                color = lines[index + 1]
            if price is None and "€" in line:
                price = line
            if line == "Unavailable":
                unavailable = True

        if price is None:
            price = _pick_price_line(lines)

        products[key] = {
            "name": name,
            "color": color,
            "price": price,
            "unavailable": unavailable,
            "url": url,
            "is_bag": is_bag_item(name),
        }

    print(f"[INFO] Unique products parsed: {len(products)}")
    return list(products.values())


def get_all_products(
    save_path: str | Path = "products_all.json",
    category_url: str = CATEGORY_URL,
    homepage_url: str = HOMEPAGE_URL,
    debug_path: str | Path = "debug.html",
) -> List[Dict]:
    """
    Scrape category page, extract products, save to JSON, and return list of dicts.
    """
    category_url = category_url or CATEGORY_URL
    homepage_url = homepage_url or HOMEPAGE_URL

    session = create_session(homepage_url=homepage_url)
    soup = fetch_category_soup(session, category_url=category_url, debug_path=debug_path)
    products = extract_products_from_soup(soup)

    save_path = Path(save_path)
    save_path.write_text(
        json.dumps(products, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[INFO] Saved {len(products)} products to {save_path}")

    return products


def main() -> None:
    products = get_all_products()

    print("\n=== SAMPLE (first 10) ===")
    for product in products[:10]:
        print(
            f"- {product['name']} | {product['color']} | {product['price']} | "
            f"unavail={product['unavailable']} | is_bag={product['is_bag']}"
        )


if __name__ == "__main__":
    main()
