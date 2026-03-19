"""
Scrape Hermès category pages and save all products to JSON.
"""

from pathlib import Path
import json
import hashlib
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence
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

BLOCKED_STATUS_CODES = {403, 429}
CHALLENGE_MARKERS = (
    "captcha",
    "captcha-delivery.com",
    "geo.captcha-delivery.com",
    "verify you are human",
    "unusual traffic",
    "attention required",
    "please enable js",
    "please enable cookies",
    "please enable javascript",
    "robot or human",
    "security check",
)


def _collect_response_markers(resp: requests.Response, html: str) -> Dict[str, str]:
    markers: Dict[str, str] = {}
    for header_name in ("x-datadome", "x-dd-b", "x-datadome-cid", "cf-cache-status", "server", "cf-ray"):
        header_value = resp.headers.get(header_name)
        if header_value:
            markers[header_name] = header_value

    lowered_html = html.lower()
    if "geo.captcha-delivery.com" in lowered_html:
        markers["challenge_host"] = "geo.captcha-delivery.com"
    if "var dd=" in lowered_html:
        markers["challenge_script"] = "var dd"
    if "please enable js and disable any ad blocker" in lowered_html:
        markers["challenge_text"] = "Please enable JS and disable any ad blocker"
    return markers


def _infer_accept_language(homepage_url: str) -> str:
    lowered = (homepage_url or "").lower()
    if "/tw/zh/" in lowered:
        return "zh-TW,zh;q=0.9,en;q=0.6"
    if "/jp/ja/" in lowered:
        return "ja-JP,ja;q=0.9,en;q=0.6"
    if "/fr/fr/" in lowered or "/be/fr/" in lowered or "/ca/fr/" in lowered or "/ch/fr/" in lowered:
        return "fr-FR,fr;q=0.9,en;q=0.6"
    if "/de/de/" in lowered or "/at/de/" in lowered or "/ch/de/" in lowered:
        return "de-DE,de;q=0.9,en;q=0.6"
    if "/nl/en/" in lowered:
        return "nl-NL,nl;q=0.9,en;q=0.6"
    if "/be/en/" in lowered:
        return "en-BE,en;q=0.9"
    return HEADERS["Accept-Language"]


def _format_response_markers(markers: Dict[str, str]) -> str:
    ordered_keys = (
        "x-datadome",
        "x-dd-b",
        "cf-cache-status",
        "server",
        "cf-ray",
        "challenge_host",
        "challenge_script",
        "challenge_text",
    )
    parts = [f"{key}={markers[key]}" for key in ordered_keys if markers.get(key)]
    return "; ".join(parts)


def _classify_block_response(resp: requests.Response, html: str) -> tuple[str, str]:
    markers = _collect_response_markers(resp, html)
    marker_text = _format_response_markers(markers)
    status_text = f"HTTP {resp.status_code}"

    if resp.status_code == 429:
        detail = status_text
        if marker_text:
            detail = f"{detail} | {marker_text}"
        return "RATE_LIMIT", detail

    if markers.get("x-datadome") == "protected" or markers.get("challenge_host"):
        detail = status_text
        if marker_text:
            detail = f"{detail} | {marker_text}"
        return "DATADOME_CHALLENGE", detail

    detail = status_text
    if marker_text:
        detail = f"{detail} | {marker_text}"
    return "BLOCKED", detail


def _session_cookie_summary(session: requests.Session) -> str:
    cookie_names = sorted({cookie.name for cookie in session.cookies})
    if not cookie_names:
        return "count=0"
    preview = ",".join(cookie_names[:10])
    extra = "" if len(cookie_names) <= 10 else f",+{len(cookie_names) - 10} more"
    return f"count={len(cookie_names)} names={preview}{extra}"


def _response_history_summary(resp: requests.Response) -> str:
    if not resp.history:
        return "-"
    parts = []
    for item in resp.history:
        parts.append(f"{item.status_code}:{item.url}")
    return " -> ".join(parts)


def _body_fingerprint(text: str) -> str:
    normalized = text.encode("utf-8", errors="ignore")
    digest = hashlib.sha256(normalized).hexdigest()[:16]
    head_digest = hashlib.sha256(normalized[:1024]).hexdigest()[:16]
    return f"sha256={digest} head1k_sha256={head_digest} bytes={len(normalized)}"


def _session_init_info(session: requests.Session) -> Dict[str, str]:
    info = getattr(session, "_hermes_init_info", None)
    if isinstance(info, dict):
        return info
    return {}


def create_session(homepage_url: str = HOMEPAGE_URL) -> requests.Session:
    """Create a session with base headers and prefetch homepage to collect cookies."""
    session = requests.Session()
    headers = dict(HEADERS)
    headers["Accept-Language"] = _infer_accept_language(homepage_url)
    if homepage_url:
        headers["Referer"] = homepage_url
    session.headers.update(headers)
    print(
        f"[INFO] Session initialized for {homepage_url or '-'} | "
        f"Accept-Language={headers.get('Accept-Language')}"
    )
    init_info: Dict[str, str] = {
        "homepage_url": homepage_url or "",
        "accept_language": headers.get("Accept-Language", ""),
        "referer": headers.get("Referer", ""),
        "user_agent": headers.get("User-Agent", ""),
        "homepage_status": "",
        "homepage_reason": "",
        "homepage_markers": "",
        "homepage_history": "",
        "cookies_after_homepage": "count=0",
        "homepage_error": "",
    }
    try:
        resp = session.get(homepage_url, timeout=20)
        init_info["homepage_status"] = str(resp.status_code)
        init_info["homepage_reason"] = resp.reason or ""
        init_info["homepage_markers"] = _format_response_markers(_collect_response_markers(resp, resp.text))
        init_info["homepage_history"] = _response_history_summary(resp)
        init_info["cookies_after_homepage"] = _session_cookie_summary(session)
    except Exception as exc:  # pragma: no cover - network dependent
        init_info["homepage_error"] = str(exc)
        print(f"[WARN] Visit homepage failed: {exc}")
    setattr(session, "_hermes_init_info", init_info)
    return session


def _looks_like_blocked_page(html: str) -> bool:
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        soup = None

    if soup is None:
        lowered = html.lower()
        return any(marker in lowered for marker in CHALLENGE_MARKERS)

    title_text = soup.title.get_text(" ", strip=True).lower() if soup.title else ""

    for tag in soup(["script", "style"]):
        tag.decompose()

    visible_text = soup.get_text(" ", strip=True).lower()
    visible_excerpt = " ".join(visible_text.split())[:4000]

    strong_title_markers = (
        "access denied",
        "attention required",
        "just a moment",
        "hermes.com",
    )
    if any(marker in title_text for marker in strong_title_markers):
        # Plain "hermes.com" title alone is not enough; require a challenge marker too.
        if "hermes.com" not in title_text:
            return True

    hits = sum(1 for marker in CHALLENGE_MARKERS if marker in visible_excerpt)
    if hits >= 2:
        return True

    lowered_html = html.lower()
    if "geo.captcha-delivery.com" in lowered_html or "var dd=" in lowered_html:
        return True

    return False


def fetch_category_soup(
    session: requests.Session,
    category_urls: Sequence[str],
    debug_path: str | Path = "debug.html",
    pause_minutes_on_fail: float = 5.0,
    sleep_on_fail: bool = True,
) -> tuple[Optional[BeautifulSoup], Dict[str, object]]:
    """Try category URLs in order; on non-200/exception, try next. If all fail, optionally sleep and return None."""
    last_status = None
    blocked = False
    rate_limited = False
    last_error = ""
    last_url = ""
    block_reason = ""
    block_detail = ""
    response_markers: Dict[str, str] = {}
    debug_path = Path(debug_path)

    def save_debug_response(resp: requests.Response, source_url: str) -> None:
        encoding = resp.encoding or "utf-8"
        debug_path.write_text(resp.text, encoding=encoding, errors="ignore")
        meta_path = debug_path.with_suffix(debug_path.suffix + ".meta.txt")
        header_lines = [f"{key}: {value}" for key, value in resp.headers.items()]
        request_header_lines = [f"{key}: {value}" for key, value in resp.request.headers.items()]
        init_info = _session_init_info(session)
        init_lines = [f"{key}: {value}" for key, value in init_info.items() if value]
        marker_text = _format_response_markers(_collect_response_markers(resp, resp.text))
        meta_text = (
            f"URL: {source_url}\n"
            f"Status: {resp.status_code}\n"
            f"Reason: {resp.reason}\n"
            f"Encoding: {encoding}\n\n"
            "Session Init:\n"
            f"{chr(10).join(init_lines) if init_lines else '-'}\n\n"
            "Request:\n"
            f"Method: {resp.request.method}\n"
            f"URL: {resp.request.url}\n"
            f"History: {_response_history_summary(resp)}\n"
            f"Cookies: {_session_cookie_summary(session)}\n"
            f"Body Fingerprint: {_body_fingerprint(resp.text)}\n"
            f"Response Markers: {marker_text or '-'}\n\n"
            "Request Headers:\n"
            f"{chr(10).join(request_header_lines)}\n\n"
            "Headers:\n"
            f"{chr(10).join(header_lines)}\n"
        )
        meta_path.write_text(meta_text, encoding="utf-8")
        print(f"[INFO] Saved debug response to {debug_path}")
        print(f"[INFO] Saved debug metadata to {meta_path}")

    for url in category_urls:
        try:
            resp = session.get(url, timeout=20)
            last_status = resp.status_code
            last_url = url
            print(f"[INFO] GET {url} -> {resp.status_code}")
            save_debug_response(resp, url)
            if resp.status_code in BLOCKED_STATUS_CODES:
                blocked = True
                rate_limited = rate_limited or resp.status_code == 429
                response_markers = _collect_response_markers(resp, resp.text)
                block_reason, block_detail = _classify_block_response(resp, resp.text)
                print(f"[WARN] Possible anti-bot/rate-limit response for {url}, skipping")
                continue
            if resp.status_code != 200:
                print(f"[WARN] Non-200 for {url}, skipping")
                continue
            if _looks_like_blocked_page(resp.text):
                blocked = True
                response_markers = _collect_response_markers(resp, resp.text)
                block_reason, block_detail = _classify_block_response(resp, resp.text)
                print(f"[WARN] Challenge page detected for {url}, skipping")
                continue
            return BeautifulSoup(resp.text, "html.parser"), {
                "blocked": False,
                "rate_limited": False,
                "last_status": resp.status_code,
                "last_error": "",
                "last_url": url,
                "block_reason": "",
                "block_detail": "",
                "response_markers": _collect_response_markers(resp, resp.text),
            }
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = str(exc)
            last_url = url
            print(f"[WARN] Fetch failed for {url}: {exc}")

    pause_seconds = max(0, int(pause_minutes_on_fail * 60))
    if sleep_on_fail and pause_seconds > 0:
        print(
            f"[WARN] All category URLs failed (last status: {last_status}); "
            f"sleeping {pause_seconds}s before next attempt"
        )
        time.sleep(pause_seconds)
    return None, {
        "blocked": blocked,
        "rate_limited": rate_limited,
        "last_status": last_status,
        "last_error": last_error,
        "last_url": last_url,
        "block_reason": block_reason,
        "block_detail": block_detail,
        "response_markers": response_markers,
    }


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


def _normalize_product_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = record.get("url") or record.get("href")
    if not isinstance(url, str) or "/product/" not in url:
        return None
    name = record.get("title") or record.get("name") or record.get("label") or "Unknown"
    if not isinstance(name, str):
        name = str(name)
    if not url.startswith("http"):
        url = BASE_URL + url

    price = record.get("price")
    color = record.get("avgColor") or record.get("color") or record.get("mainColor")

    unavailable = False
    stock = record.get("stock")
    if isinstance(stock, dict):
        if stock.get("displayOnly") is True:
            unavailable = True
        if stock.get("ecom") is False:
            unavailable = True

    return {
        "name": name,
        "color": color,
        "price": price,
        "unavailable": unavailable,
        "url": url,
        "is_bag": is_bag_item(name),
    }


def _extract_products_from_state(state: Any) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "url" in node:
                normalized = _normalize_product_record(node)
                if normalized:
                    found.append(normalized)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(state)
    # Deduplicate by (name, url)
    dedup: Dict[tuple[str, str], Dict[str, Any]] = {}
    for item in found:
        key = (item.get("name", ""), item.get("url", ""))
        if key not in dedup:
            dedup[key] = item
    return list(dedup.values())


def parse_products_from_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    products = extract_products_from_soup(soup)
    if products:
        return products

    script = soup.find("script", id="hermes-state")
    if script is None:
        return products

    payload = script.string or script.get_text()
    if not payload:
        return products

    try:
        state = json.loads(payload)
    except json.JSONDecodeError:
        return products

    return _extract_products_from_state(state)


def parse_products_from_json_data(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        normalized: List[Dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict) and "url" in item:
                normalized_item = _normalize_product_record(item) or item
                normalized.append(normalized_item)
        return normalized
    if isinstance(data, dict):
        return _extract_products_from_state(data)
    return []


def _resolve_history_path(history_path: str | Path, region_name: str) -> Path:
    path_value = str(history_path)
    if "{region}" in path_value:
        path_value = path_value.format(region=region_name)
    return Path(path_value)


def _compute_signature(products: Sequence[Dict]) -> tuple[str, int]:
    keys = [f"{p.get('name', '')}|{p.get('url', '')}" for p in products]
    unique_keys = sorted(set(keys))
    signature = hashlib.sha256("\n".join(unique_keys).encode("utf-8")).hexdigest()
    return signature, len(keys)


def _load_last_snapshot(db_path: Path, region_name: str) -> Optional[Dict]:
    if not db_path.exists():
        return None
    last = None
    with db_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("region") == region_name:
                last = record
    return last


def store_history_if_changed(
    products: Sequence[Dict],
    region_name: str,
    history_path: str | Path,
    enabled: bool = True,
) -> None:
    if not enabled:
        return
    region = region_name or "UNKNOWN"
    db_path = _resolve_history_path(history_path, region)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    signature, count = _compute_signature(products)
    last = _load_last_snapshot(db_path, region)
    if last and last.get("signature") == signature and last.get("count") == count:
        print(f"[INFO] History unchanged for {region}; skip write")
        return

    record = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "region": region,
        "count": count,
        "signature": signature,
        "products": list(products),
    }
    with db_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"[INFO] History updated for {region} -> {db_path}")


def get_all_products(
    save_path: str | Path = "products_all.json",
    category_url: str = CATEGORY_URL,
    category_urls: Optional[Sequence[str]] = None,
    homepage_url: str = HOMEPAGE_URL,
    debug_path: str | Path = "debug.html",
    pause_minutes_on_fail: float = 5.0,
    sleep_on_fail: bool = True,
    history_path: str | Path = "output/product_history.jsonl",
    history_enabled: bool = True,
    region_name: str = "MAIN",
    session: Optional[requests.Session] = None,
    return_metadata: bool = False,
) -> List[Dict] | tuple[List[Dict], Dict[str, object]]:
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

    active_session = session or create_session(homepage_url=homepage_final)
    soup, fetch_meta = fetch_category_soup(
        active_session,
        category_urls=urls,
        debug_path=debug_path,
        pause_minutes_on_fail=pause_minutes_on_fail,
        sleep_on_fail=sleep_on_fail,
    )
    if soup is None:
        products: List[Dict] = []
        if return_metadata:
            return products, dict(fetch_meta)
        return products
    products = parse_products_from_html(str(soup))

    save_path = Path(save_path)
    save_path.write_text(
        json.dumps(products, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[INFO] Saved {len(products)} products to {save_path}")
    store_history_if_changed(
        products=products,
        region_name=region_name,
        history_path=history_path,
        enabled=history_enabled,
    )

    result_meta = dict(fetch_meta)
    result_meta["count"] = len(products)
    if return_metadata:
        return products, result_meta
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
