"""
Scrape Hermès category pages and save all products to JSON.
"""

import json
import hashlib
from pathlib import Path
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree

from bs4 import BeautifulSoup
from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError
from config import SHARED_USER_AGENT
from fetch_providers import fetch_external_html

BASE_URL = "https://www.hermes.com"
CATEGORY_URL = (
    "https://www.hermes.com/be/en/category/women/"
    "bags-and-small-leather-goods/bags-and-clutches/"
)
HOMEPAGE_URL = "https://www.hermes.com/be/en/"

HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": SHARED_USER_AGENT,
}
DEFAULT_IMPERSONATE = "safari_ios"

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
    cookie_names: List[str] = []
    cookies = getattr(session, "cookies", None)
    if cookies is not None:
        if hasattr(cookies, "keys"):
            cookie_names = sorted({str(name) for name in cookies.keys()})
        else:
            cookie_names = sorted(
                {
                    str(getattr(cookie, "name", cookie))
                    for cookie in cookies
                }
            )
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


def _save_fallback_debug(html: str, source_url: str, debug_path: str | Path) -> None:
    debug_path = Path(debug_path)
    fallback_path = debug_path.with_name(f"{debug_path.stem}_fallback{debug_path.suffix}")
    fallback_path.write_text(html, encoding="utf-8", errors="ignore")

    soup = BeautifulSoup(html, "html.parser")
    title_text = soup.title.get_text(" ", strip=True) if soup.title else ""
    has_hermes_state = bool(soup.find("script", id="hermes-state"))
    product_anchor_count = len(soup.find_all("a", href=re.compile(r"/product/")))
    meta_path = fallback_path.with_suffix(fallback_path.suffix + ".meta.txt")
    snippet = html[:500]
    meta_text = (
        f"URL: {source_url}\n"
        f"Title: {title_text or '-'}\n"
        f"Has hermes-state: {has_hermes_state}\n"
        f"Product anchors: {product_anchor_count}\n"
        f"Body Fingerprint: {_body_fingerprint(html)}\n\n"
        "Snippet:\n"
        f"{snippet}\n"
    )
    meta_path.write_text(meta_text, encoding="utf-8")
    print(f"[INFO] Saved fallback debug HTML to {fallback_path}")
    print(f"[INFO] Saved fallback debug metadata to {meta_path}")


def _session_init_info(session: requests.Session) -> Dict[str, str]:
    info = getattr(session, "_hermes_init_info", None)
    if isinstance(info, dict):
        return info
    return {}


def _build_session_headers(homepage_url: str) -> Dict[str, str]:
    headers = dict(HEADERS)
    headers["Accept-Language"] = _infer_accept_language(homepage_url)
    if homepage_url:
        headers["Referer"] = homepage_url
    return headers


def _normalize_impersonation_profiles(
    impersonate: Optional[str] = None,
    impersonate_profiles: Optional[Sequence[str]] = None,
) -> List[str]:
    profiles: List[str] = []
    if impersonate_profiles:
        profiles.extend([str(item).strip() for item in impersonate_profiles if str(item).strip()])
    if impersonate and impersonate.strip():
        profiles.insert(0, impersonate.strip())
    deduped: List[str] = []
    seen: set[str] = set()
    for profile in profiles:
        if profile not in seen:
            seen.add(profile)
            deduped.append(profile)
    return deduped or [DEFAULT_IMPERSONATE]


def _looks_like_retryable_block(resp: requests.Response) -> bool:
    if resp.status_code in BLOCKED_STATUS_CODES:
        return True
    lowered_html = resp.text.lower()
    retryable_markers = (
        "please enable js and disable any ad blocker",
        "geo.captcha-delivery.com",
        "attention required",
        "access denied",
        "just a moment",
        "captcha-delivery.com",
    )
    hits = sum(1 for marker in retryable_markers if marker in lowered_html)
    return hits >= 1


def _session_get(
    session: requests.Session,
    url: str,
    timeout: int = 20,
    headers: Optional[Dict[str, str]] = None,
    proxies: Optional[Dict[str, str]] = None,
    impersonate: Optional[str] = None,
) -> requests.Response:
    effective_proxies = proxies or getattr(session, "_hermes_proxies", None)
    rotation_enabled = bool(getattr(session, "_hermes_rotate_profiles_on_block", True))
    session_profiles = list(getattr(session, "_hermes_impersonation_profiles", [DEFAULT_IMPERSONATE]))

    if impersonate:
        profiles = [impersonate]
    else:
        start_index = int(getattr(session, "_hermes_impersonate_index", 0) or 0)
        if not session_profiles:
            session_profiles = [DEFAULT_IMPERSONATE]
        profiles = session_profiles[start_index:] + session_profiles[:start_index]

    last_response: Optional[requests.Response] = None
    last_error: Optional[RequestsError] = None

    for attempt_index, profile in enumerate(profiles):
        request_kwargs: Dict[str, Any] = {
            "timeout": timeout,
            "impersonate": profile,
        }
        if headers:
            request_kwargs["headers"] = headers
        if effective_proxies:
            request_kwargs["proxies"] = effective_proxies

        try:
            resp = session.get(url, **request_kwargs)
        except RequestsError as exc:
            last_error = exc
            if not rotation_enabled or attempt_index >= len(profiles) - 1:
                raise
            print(f"[WARN] GET {url} failed with impersonate={profile}: {exc}; rotating profile")
            continue

        last_response = resp
        profile_index = session_profiles.index(profile) if profile in session_profiles else 0
        setattr(session, "_hermes_impersonate", profile)
        setattr(session, "_hermes_impersonate_index", profile_index)

        if (
            not rotation_enabled
            or impersonate
            or attempt_index >= len(profiles) - 1
            or not _looks_like_retryable_block(resp)
        ):
            return resp

        print(
            f"[WARN] GET {url} returned retryable block with impersonate={profile} "
            f"(HTTP {resp.status_code}); rotating profile"
        )

    if last_response is not None:
        return last_response
    if last_error is not None:
        raise last_error
    raise RequestsError("No impersonation profiles available")


def create_session(
    homepage_url: str = HOMEPAGE_URL,
    *,
    impersonate: str = DEFAULT_IMPERSONATE,
    impersonate_profiles: Optional[Sequence[str]] = None,
    rotate_profiles_on_block: bool = True,
    proxies: Optional[Dict[str, str]] = None,
    timeout: int = 20,
) -> requests.Session:
    """Create a curl_cffi session with browser impersonation and prefetch homepage cookies."""
    profiles = _normalize_impersonation_profiles(
        impersonate=impersonate,
        impersonate_profiles=impersonate_profiles,
    )
    primary_profile = profiles[0]
    headers = _build_session_headers(homepage_url)
    session = requests.Session(
        headers=headers,
        impersonate=primary_profile,
        proxies=proxies,
        timeout=timeout,
    )
    setattr(session, "_hermes_impersonate", primary_profile)
    setattr(session, "_hermes_impersonation_profiles", profiles)
    setattr(session, "_hermes_impersonate_index", 0)
    setattr(session, "_hermes_rotate_profiles_on_block", rotate_profiles_on_block)
    setattr(session, "_hermes_proxies", dict(proxies or {}))
    print(
        f"[INFO] Session initialized for {homepage_url or '-'} | "
        f"profiles={profiles} | Accept-Language={headers.get('Accept-Language')}"
    )
    init_info: Dict[str, str] = {
        "homepage_url": homepage_url or "",
        "impersonate": primary_profile,
        "impersonation_profiles": ",".join(profiles),
        "rotate_profiles_on_block": str(bool(rotate_profiles_on_block)).lower(),
        "accept_language": headers.get("Accept-Language", ""),
        "referer": headers.get("Referer", ""),
        "user_agent": session.headers.get("User-Agent", ""),
        "proxy_count": str(len(proxies or {})),
        "homepage_status": "",
        "homepage_reason": "",
        "homepage_markers": "",
        "homepage_history": "",
        "cookies_after_homepage": "count=0",
        "homepage_error": "",
    }
    try:
        resp = _session_get(session, homepage_url, timeout=timeout)
        init_info["homepage_status"] = str(resp.status_code)
        init_info["homepage_reason"] = resp.reason or ""
        init_info["homepage_markers"] = _format_response_markers(_collect_response_markers(resp, resp.text))
        init_info["homepage_history"] = _response_history_summary(resp)
        init_info["cookies_after_homepage"] = _session_cookie_summary(session)
    except RequestsError as exc:  # pragma: no cover - network dependent
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


def fetch_category_html(
    session: Optional[requests.Session],
    category_urls: Sequence[str],
    debug_path: str | Path = "debug.html",
    pause_minutes_on_fail: float = 5.0,
    sleep_on_fail: bool = True,
    impersonate_profiles: Optional[Sequence[str]] = None,
    rotate_profiles_on_block: bool = True,
) -> tuple[Optional[str], Dict[str, object]]:
    """Try category URLs in order using a locale-aligned session for each URL."""
    last_status = None
    blocked = False
    rate_limited = False
    last_error = ""
    last_url = ""
    block_reason = ""
    block_detail = ""
    response_markers: Dict[str, str] = {}
    debug_path = Path(debug_path)
    session_pool = _session_pool_for(session)

    def session_for_url(source_url: str) -> requests.Session:
        homepage_url = derive_homepage_from_url(source_url)
        active_session = session_pool.get(homepage_url)
        if active_session is None:
            active_session = create_session(
                homepage_url=homepage_url,
                impersonate_profiles=impersonate_profiles,
                rotate_profiles_on_block=rotate_profiles_on_block,
            )
            session_pool[homepage_url] = active_session
        return active_session

    def save_debug_response(
        resp: requests.Response,
        source_url: str,
        active_session: requests.Session,
    ) -> None:
        encoding = resp.encoding or "utf-8"
        debug_path.write_text(resp.text, encoding=encoding, errors="ignore")
        meta_path = debug_path.with_suffix(debug_path.suffix + ".meta.txt")
        header_lines = [f"{key}: {value}" for key, value in resp.headers.items()]
        request_header_lines = [f"{key}: {value}" for key, value in resp.request.headers.items()]
        init_info = _session_init_info(active_session)
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
            f"Cookies: {_session_cookie_summary(active_session)}\n"
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
            active_session = session_for_url(url)
            resp = _session_get(active_session, url, timeout=20)
            last_status = resp.status_code
            last_url = url
            print(f"[INFO] GET {url} -> {resp.status_code}")
            save_debug_response(resp, url, active_session)
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
            return resp.text, {
                "blocked": False,
                "rate_limited": False,
                "last_status": resp.status_code,
                "last_error": "",
                "last_url": url,
                "block_reason": "",
                "block_detail": "",
                "response_markers": _collect_response_markers(resp, resp.text),
            }
        except RequestsError as exc:  # pragma: no cover - network dependent
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


def _normalize_locale_prefix(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    parts = urlsplit(cleaned)
    path = parts.path or cleaned
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/")


def derive_homepage_from_url(url: str) -> str:
    """Derive scheme://host/<locale>/<lang>/ from a Hermes category-like URL."""
    try:
        parts = urlsplit(url)
        path_parts = [part for part in parts.path.split("/") if part]
        if "category" in path_parts:
            path_parts = path_parts[:path_parts.index("category")]
        elif len(path_parts) >= 2:
            path_parts = path_parts[:2]
        new_path = "/" + "/".join(path_parts) + "/"
        return urlunsplit((parts.scheme, parts.netloc, new_path, "", ""))
    except Exception:
        return HOMEPAGE_URL


def _session_pool_for(session: Optional[requests.Session]) -> Dict[str, requests.Session]:
    if session is None:
        return {}
    pool = getattr(session, "_hermes_session_pool", None)
    if isinstance(pool, dict):
        return pool
    pool = {}
    homepage_url = str(_session_init_info(session).get("homepage_url") or "").strip()
    if homepage_url:
        pool[homepage_url] = session
    setattr(session, "_hermes_session_pool", pool)
    return pool


def _extract_locale_prefix_from_soup(soup: BeautifulSoup) -> str:
    base_tag = soup.find("base", href=True)
    if base_tag is not None:
        locale_prefix = _normalize_locale_prefix(str(base_tag.get("href") or ""))
        if locale_prefix:
            return locale_prefix

    canonical = soup.find("link", rel="canonical", href=True)
    if canonical is not None:
        href = str(canonical.get("href") or "")
        parts = urlsplit(href)
        path_parts = [part for part in parts.path.split("/") if part]
        if len(path_parts) >= 2:
            return f"/{path_parts[0]}/{path_parts[1]}"

    return ""


def _absolute_product_url(url: str, locale_prefix: str = "") -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("//"):
        return f"https:{url}"

    normalized_locale = _normalize_locale_prefix(locale_prefix)
    normalized_url = url.strip()
    if not normalized_url:
        return BASE_URL

    if normalized_url.startswith("/product/") and normalized_locale:
        return f"{BASE_URL}{normalized_locale}{normalized_url}"
    if normalized_url.startswith("product/") and normalized_locale:
        return f"{BASE_URL}{normalized_locale}/{normalized_url}"
    if normalized_url.startswith("/"):
        return f"{BASE_URL}{normalized_url}"
    return f"{BASE_URL}/{normalized_url.lstrip('/')}"


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


def extract_products_from_soup(soup: BeautifulSoup, locale_prefix: str = "") -> List[Dict]:
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
        url = _absolute_product_url(href, locale_prefix=locale_prefix)

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


def _normalize_product_record(record: Dict[str, Any], locale_prefix: str = "") -> Optional[Dict[str, Any]]:
    url = record.get("url") or record.get("href")
    if not isinstance(url, str) or "/product/" not in url:
        return None
    name = record.get("title") or record.get("name") or record.get("label") or "Unknown"
    if not isinstance(name, str):
        name = str(name)
    url = _absolute_product_url(url, locale_prefix=locale_prefix)

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


def _extract_products_from_state(state: Any, locale_prefix: str = "") -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "url" in node:
                normalized = _normalize_product_record(node, locale_prefix=locale_prefix)
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
    locale_prefix = _extract_locale_prefix_from_soup(soup)
    script = soup.find("script", id="hermes-state")
    if script is not None:
        payload = script.string or script.get_text()
        if payload:
            try:
                state = json.loads(payload)
            except json.JSONDecodeError:
                pass
            else:
                products_from_state = _extract_products_from_state(state, locale_prefix=locale_prefix)
                if products_from_state:
                    return products_from_state

    return extract_products_from_soup(soup, locale_prefix=locale_prefix)


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


def _url_only_product(url: str) -> Dict[str, Any]:
    return {
        "name": "",
        "color": None,
        "price": None,
        "unavailable": False,
        "url": url,
        "is_bag": None,
    }


def _dedupe_products_by_url(products: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dedup: Dict[str, Dict[str, Any]] = {}
    for product in products:
        if not isinstance(product, dict):
            continue
        url = product.get("url")
        if not isinstance(url, str) or not url:
            continue
        current = dedup.get(url)
        if current is None:
            dedup[url] = dict(product)
            continue
        if not current.get("name") and product.get("name"):
            dedup[url] = dict(product)
    return list(dedup.values())


def extract_product_urls_from_text(text: str) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    seen: set[str] = set()
    patterns = (
        r"https?://www\.hermes\.com[^\s\"'<>]+/product/[^\s\"'<>]+",
        r"/[^\s\"'<>]*/product/[^\s\"'<>]+",
    )
    for pattern in patterns:
        for match in re.findall(pattern, text):
            url = _absolute_product_url(match)
            if url in seen:
                continue
            seen.add(url)
            found.append(_url_only_product(url))
    return found


def _local_xml_tag(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _extract_sitemap_locs(xml_text: str) -> tuple[List[str], List[str]]:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        urls = [item["url"] for item in extract_product_urls_from_text(xml_text)]
        return [], urls

    sitemap_urls: List[str] = []
    product_urls: List[str] = []
    for elem in root.iter():
        if _local_xml_tag(elem.tag) != "loc":
            continue
        if not elem.text:
            continue
        value = elem.text.strip()
        if not value:
            continue
        if "/product/" in value:
            product_urls.append(value)
        elif value.endswith(".xml") or "sitemap" in value.lower():
            sitemap_urls.append(value)
    return sitemap_urls, product_urls


def discover_products_from_source(
    session: requests.Session,
    source_url: str,
    kind: str,
    timeout: int = 20,
    max_sitemaps: int = 20,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    kind_normalized = (kind or "auto").strip().lower()
    meta: Dict[str, Any] = {
        "kind": kind_normalized,
        "source_url": source_url,
        "status_code": None,
        "error": "",
        "fetched_urls": [],
        "sitemaps_visited": 0,
    }

    if kind_normalized == "sitemap":
        queue: List[str] = [source_url]
        seen_sitemaps: set[str] = set()
        products: List[Dict[str, Any]] = []
        while queue and len(seen_sitemaps) < max_sitemaps:
            sitemap_url = queue.pop(0)
            if sitemap_url in seen_sitemaps:
                continue
            seen_sitemaps.add(sitemap_url)
            meta["fetched_urls"].append(sitemap_url)
            try:
                resp = _session_get(session, sitemap_url, timeout=timeout)
            except RequestsError as exc:
                meta["error"] = str(exc)
                continue
            meta["status_code"] = resp.status_code
            if resp.status_code != 200:
                continue
            child_sitemaps, product_urls = _extract_sitemap_locs(resp.text)
            products.extend([_url_only_product(url) for url in product_urls])
            for child in child_sitemaps:
                if child not in seen_sitemaps:
                    queue.append(child)
        meta["sitemaps_visited"] = len(seen_sitemaps)
        return _dedupe_products_by_url(products), meta

    try:
        resp = _session_get(session, source_url, timeout=timeout)
    except RequestsError as exc:
        meta["error"] = str(exc)
        return [], meta

    meta["status_code"] = resp.status_code
    meta["fetched_urls"].append(source_url)
    if resp.status_code != 200:
        return [], meta

    products: List[Dict[str, Any]] = []
    if kind_normalized in {"html", "auto"}:
        products = parse_products_from_html(resp.text)
        if products:
            return _dedupe_products_by_url(products), meta
        if kind_normalized == "html":
            return extract_product_urls_from_text(resp.text), meta

    if kind_normalized in {"json", "auto"}:
        try:
            data = json.loads(resp.text)
        except json.JSONDecodeError:
            data = None
        if data is not None:
            products = parse_products_from_json_data(data)
            if products:
                return _dedupe_products_by_url(products), meta
            if kind_normalized == "json":
                return extract_product_urls_from_text(resp.text), meta

    return extract_product_urls_from_text(resp.text), meta


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


def _product_history_item_key(product: Dict[str, Any]) -> str:
    return json.dumps(product, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _load_seen_history_item_keys(db_path: Path, region_name: str) -> set[str]:
    seen: set[str] = set()
    if not db_path.exists():
        return seen
    with db_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("region") != region_name:
                continue
            products = record.get("products") or []
            if not isinstance(products, list):
                continue
            for product in products:
                if isinstance(product, dict):
                    seen.add(_product_history_item_key(product))
    return seen


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

    seen_item_keys = _load_seen_history_item_keys(db_path, region)
    filtered_products: List[Dict[str, Any]] = []
    current_item_keys: set[str] = set()
    for product in products:
        if not isinstance(product, dict):
            continue
        item_key = _product_history_item_key(product)
        if item_key in seen_item_keys or item_key in current_item_keys:
            continue
        current_item_keys.add(item_key)
        filtered_products.append(product)

    if not filtered_products:
        print(f"[INFO] History items already recorded for {region}; skip write")
        return

    filtered_signature, filtered_count = _compute_signature(filtered_products)
    record = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "region": region,
        "count": filtered_count,
        "signature": filtered_signature,
        "products": filtered_products,
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
    impersonate_profiles: Optional[Sequence[str]] = None,
    rotate_profiles_on_block: bool = True,
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

    if homepage_url:
        homepage_final = homepage_url
    else:
        homepage_final = derive_homepage_from_url(urls[0]) if urls else HOMEPAGE_URL

    active_session = session or create_session(
        homepage_url=homepage_final,
        impersonate_profiles=impersonate_profiles,
        rotate_profiles_on_block=rotate_profiles_on_block,
    )
    html, fetch_meta = fetch_category_html(
        active_session,
        category_urls=urls,
        debug_path=debug_path,
        pause_minutes_on_fail=pause_minutes_on_fail,
        sleep_on_fail=sleep_on_fail,
        impersonate_profiles=impersonate_profiles,
        rotate_profiles_on_block=rotate_profiles_on_block,
    )
    products: List[Dict] = []
    if html:
        products = parse_products_from_html(html)
    else:
        fallback_url = str(fetch_meta.get("last_url") or "")
        print(f"[INFO] Native fetch failed; trying fallback for {fallback_url or '-'}")
        fallback_html = fetch_external_html(fallback_url)
        if fallback_html:
            print(f"[INFO] Fallback returned HTML ({len(fallback_html)} chars)")
            _save_fallback_debug(fallback_html, fallback_url, debug_path)
            products = parse_products_from_html(fallback_html)
        else:
            print("[WARN] Fallback returned no HTML")

    if not products:
        if return_metadata:
            return products, dict(fetch_meta)
        return products

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
