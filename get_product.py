"""
Scrape Hermès category pages and save all products to JSON.
"""

from pathlib import Path
import json
import re
import time
from typing import Dict, List, Optional, Sequence
from urllib.parse import urlsplit, urlunsplit

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
    category_urls: Sequence[str],
    debug_path: str | Path = "debug.html",
    pause_minutes_on_fail: float = 5.0,
    sleep_on_fail: bool = True,
) -> Optional[BeautifulSoup]:
    """Try category URLs in order; on non-200/exception, try next. If all fail, optionally sleep and return None."""
    last_status = None
    for url in category_urls:
        try:
            resp = session.get(url, timeout=20)
            last_status = resp.status_code
            print(f"[INFO] GET {url} -> {resp.status_code}")
            if resp.status_code != 200:
                print(f"[WARN] Non-200 for {url}, skipping")
                continue
            Path(debug_path).write_text(resp.text, encoding=resp.encoding or "utf-8")
            print(f"[INFO] Saved raw HTML to {debug_path}")
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:  # pragma: no cover - network dependent
            print(f"[WARN] Fetch failed for {url}: {exc}")

    pause_seconds = max(0, int(pause_minutes_on_fail * 60))
    if sleep_on_fail and pause_seconds > 0:
        print(
            f"[WARN] All category URLs failed (last status: {last_status}); "
            f"sleeping {pause_seconds}s before next attempt"
        )
        time.sleep(pause_seconds)
    return None


def is_bag_item(name: str) -> bool:
    """Roughly determine if the item is a bag (multi-lingual keywords)."""
    n = name.lower()
    exclude_tokens = ["strap", "shoulder strap", "bandouliere", "bandoulière", "belt", "ceinture"]
    if any(tok in n for tok in exclude_tokens):
        return False
    bag_tokens = [
        "bag",
        "sac",
        "sacoche",
        "sac à main",
        "sac a main",
        "pouch",
        "pochette",
        "clutch",
        "backpack",
        "sac à dos",
        "sac a dos",
        "cab",
        "hobo",
        "tote",
        "besace",
    ]
    return any(tok in n for tok in bag_tokens)


def _pick_price_line(lines: Sequence[str]) -> str | None:
    for line in lines:
        if re.search(r"\d", line):
            return line
    return None


def _pick_color_line(lines: Sequence[str]) -> Optional[str]:
    """Extract color value using common labels."""
    labels = ["color", "couleur", "farbe", "coloris", "顏色", "颜色", "カラー"]
    for idx, line in enumerate(lines):
        low = line.lower()
        if any(label in low for label in labels):
            if ":" in line:
                tail = line.split(":", 1)[1].strip()
                if tail and tail != ":":
                    return tail
            if idx + 1 < len(lines):
                nxt = lines[idx + 1].strip()
                if nxt and nxt != ":":
                    return nxt
    return None


def _extract_color_from_container(container: BeautifulSoup) -> Optional[str]:
    # Attribute-based hints
    for tag in container.find_all(True):
        # itemprop or data-color
        if tag.get("itemprop") == "color":
            txt = tag.get_text(" ", strip=True)
            if txt:
                return txt
        for attr, val in tag.attrs.items():
            if "color" in attr.lower():
                if isinstance(val, list):
                    val = " ".join(str(v) for v in val)
                if val:
                    cleaned = str(val).strip(" :")
                    if cleaned:
                        return cleaned
        classes = tag.get("class", [])
        if any("color" in cls.lower() for cls in classes):
            txt = tag.get_text(" ", strip=True)
            if txt and txt != ":":
                return txt

    # Text-based fallback
    text = container.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    # Regex across entire text
    match = re.search(r"(?i)(color|couleur|coloris|farbe|顏色|颜色|カラー)\\s*[:：]\\s*([^,;\\n]+)", text)
    if match:
        candidate = match.group(2).strip(" :")
        if candidate:
            return candidate

    # Try line-based
    return _pick_color_line(lines)


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
        lines_lower = [ln.lower() for ln in lines]

        color = _extract_color_from_container(container)
        price = None
        unavailable = False

        unavailable_markers = [
            "unavailable",
            "out of stock",
            "épuisé",
            "epuise",
            "indisponible",
            "momentanément indisponible",
            "currently unavailable",
        ]

        for line, lower in zip(lines, lines_lower):
            if price is None and "€" in line:
                price = line
            if any(marker in lower for marker in unavailable_markers):
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
    category_urls: Optional[Sequence[str]] = None,
    homepage_url: str = HOMEPAGE_URL,
    debug_path: str | Path = "debug.html",
    pause_minutes_on_fail: float = 5.0,
    sleep_on_fail: bool = True,
) -> List[Dict]:
    """
    Scrape category page, extract products, save to JSON, and return list of dicts.
    """
    urls: List[str] = []
    if category_urls:
        urls.extend([u for u in category_urls if u])
    if not urls and category_url:
        urls.append(category_url)
    if not urls:
        urls.append(CATEGORY_URL)

    def derive_homepage(url: str) -> str:
        """Derive homepage as scheme://host/<locale>/<lang>/ from a category URL."""
        try:
            parts = urlsplit(url)
            path_parts = [p for p in parts.path.split("/") if p]
            if "category" in path_parts:
                idx = path_parts.index("category")
                path_parts = path_parts[:idx]
            new_path = "/" + "/".join(path_parts) + "/"
            return urlunsplit((parts.scheme, parts.netloc, new_path, "", ""))
        except Exception:
            return HOMEPAGE_URL

    if homepage_url:
        homepage_final = homepage_url
    else:
        homepage_final = derive_homepage(urls[0]) if urls else HOMEPAGE_URL

    session = create_session(homepage_url=homepage_final)
    soup = fetch_category_soup(
        session,
        category_urls=urls,
        debug_path=debug_path,
        pause_minutes_on_fail=pause_minutes_on_fail,
        sleep_on_fail=sleep_on_fail,
    )
    if soup is None:
        return []
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
