"""
Filter Hermès products and notify via Telegram on a randomized polling interval.
"""

import argparse
import json
import random
import time
from typing import Any, Dict, Iterable, List, Set, Optional, DefaultDict
import os
from pathlib import Path
import re
from datetime import datetime
from collections import defaultdict
from urllib.parse import quote, urlsplit

import requests
import yaml

from config import SHARED_USER_AGENT
from get_product import (
    create_session,
    get_all_products,
    parse_products_from_html,
    parse_products_from_json_data,
)

LINE_IMPORT_ERROR = None
try:
    from linebot.v3.messaging import (
        ApiClient,
        Configuration,
        MessagingApi,
        PushMessageRequest,
        TextMessage,
    )
    from linebot.v3.messaging.exceptions import ApiException as LineBotApiError
except ImportError as e:  # pragma: no cover - optional dependency
    LINE_IMPORT_ERROR = str(e)
    ApiClient = None  # type: ignore
    Configuration = None  # type: ignore
    MessagingApi = None  # type: ignore
    PushMessageRequest = None  # type: ignore
    TextMessage = None  # type: ignore
    LineBotApiError = Exception  # type: ignore

DEFAULT_MIN_SECONDS = 30
DEFAULT_MAX_SECONDS = 75
DEFAULT_INCLUDE: List[str] = []
DEFAULT_EXCLUDE = ["strap", "belt", "charm", "twilly"]
DEFAULT_MIN_POLL_FLOOR = 20
DEFAULT_FAILURE_COOLDOWN_SECONDS = 45
DEFAULT_BLOCKED_COOLDOWN_SECONDS = 90
DEFAULT_MAX_FAILURE_COOLDOWN_SECONDS = 90
DEFAULT_ALERT_REMINDER_SECONDS = 90
DEFAULT_HEARTBEAT_SECONDS = 1800
SESSION_DIAGNOSTIC_PATH = Path("session_diagnostic.json")


def load_dotenv(path: str = ".env") -> Dict[str, str]:
    """Lightweight .env loader (no external dependency)."""
    env: Dict[str, str] = {}
    env_path = Path(path)
    if not env_path.exists():
        return env
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_line_user_prefs(path: str = "line_users.json") -> List[Dict[str, Any]]:
    """Load LINE user preferences from JSON/YAML list."""
    prefs_path = Path(path)
    if not prefs_path.exists():
        return []
    try:
        data = yaml.safe_load(prefs_path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        return []
    except Exception as exc:  # pragma: no cover - parse errors
        print(f"[WARN] Failed to load LINE user prefs: {exc}")
        return []


def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except FileNotFoundError:
        return {}


def _session_cookie_dict(session: requests.Session) -> Dict[str, str]:
    cookies = getattr(session, "cookies", None)
    if cookies is None:
        return {}
    if hasattr(cookies, "get_dict"):
        try:
            cookie_dict = cookies.get_dict()  # type: ignore[attr-defined]
            if isinstance(cookie_dict, dict):
                return {str(key): str(value) for key, value in cookie_dict.items()}
        except Exception:
            pass
    result: Dict[str, str] = {}
    try:
        iterator = cookies.items() if hasattr(cookies, "items") else []
        for key, value in iterator:
            result[str(key)] = str(value)
    except Exception:
        pass
    return result


def _session_playwright_cookies(
    session: requests.Session,
    homepage_url: str = "",
) -> List[Dict[str, Any]]:
    cookies = getattr(session, "cookies", None)
    fallback_url = homepage_url or "https://www.hermes.com/"
    records: List[Dict[str, Any]] = []
    seen: Set[tuple[str, str, str, str]] = set()

    if cookies is not None:
        try:
            for cookie in cookies:
                name = str(getattr(cookie, "name", "") or "")
                value = str(getattr(cookie, "value", "") or "")
                if not name:
                    continue

                record: Dict[str, Any] = {
                    "name": name,
                    "value": value,
                }
                domain = str(getattr(cookie, "domain", "") or "")
                path = str(getattr(cookie, "path", "") or "/")
                if domain:
                    record["domain"] = domain
                    record["path"] = path
                else:
                    record["url"] = fallback_url
                if getattr(cookie, "secure", False):
                    record["secure"] = True
                expires = getattr(cookie, "expires", None)
                if expires is not None:
                    record["expires"] = expires

                key = (
                    record["name"],
                    record["value"],
                    str(record.get("domain") or record.get("url") or ""),
                    str(record.get("path") or ""),
                )
                if key in seen:
                    continue
                seen.add(key)
                records.append(record)
        except Exception:
            records = []

    if records:
        return records

    for name, value in _session_cookie_dict(session).items():
        records.append(
            {
                "name": name,
                "value": value,
                "url": fallback_url,
            }
        )
    return records


def write_session_diagnostic(
    session: requests.Session,
    region_name: str,
    homepage_url: str = "",
    path: Path = SESSION_DIAGNOSTIC_PATH,
) -> None:
    payload: Dict[str, Any] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                payload = existing
        except Exception:
            payload = {}

    regions = payload.get("regions")
    if not isinstance(regions, dict):
        regions = {}
        payload["regions"] = regions

    cookie_dict = _session_cookie_dict(session)
    playwright_cookies = _session_playwright_cookies(session, homepage_url=homepage_url)
    regions[region_name] = {
        "observed_at": datetime.now().isoformat(timespec="seconds"),
        "homepage_url": homepage_url,
        "user_agent": session.headers.get("User-Agent") or SHARED_USER_AGENT,
        "cookie_count": len(cookie_dict),
        "cookies": cookie_dict,
        "playwright_cookies": playwright_cookies,
        "playwright_storage_state": {
            "cookies": playwright_cookies,
            "origins": [],
        },
    }
    payload["last_updated"] = regions[region_name]["observed_at"]

    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def collect_chat_ids(telegram_cfg: Dict[str, Any], env_values: Dict[str, str]) -> List[str]:
    """Gather chat IDs from env/config; supports TELEGRAM_CHAT_IDS (csv), TELEGRAM_CHAT_ID, TELEGRAM_CHAT_ID1/2, ID1/ID2."""
    ids: List[str] = []

    env_chat_ids = os.getenv("TELEGRAM_CHAT_IDS") or env_values.get("TELEGRAM_CHAT_IDS")
    if env_chat_ids:
        ids.extend([cid.strip() for cid in env_chat_ids.split(",") if cid.strip()])

    for key in ("TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_ID1", "TELEGRAM_CHAT_ID2", "ID1", "ID2"):
        val = os.getenv(key) or env_values.get(key)
        if val:
            ids.append(val.strip())

    ids.extend([str(cid) for cid in telegram_cfg.get("chat_ids", []) if cid])
    if telegram_cfg.get("chat_id"):
        ids.append(str(telegram_cfg["chat_id"]))

    # de-duplicate while preserving order
    seen: Set[str] = set()
    unique_ids: List[str] = []
    for cid in ids:
        if cid not in seen:
            seen.add(cid)
            unique_ids.append(cid)
    return unique_ids


def collect_line_user_ids(line_cfg: Dict[str, Any], env_values: Dict[str, str]) -> List[str]:
    """Gather LINE user IDs from env/config; supports LINE_USER_IDS (csv), LINE_USER_ID/1/2."""
    ids: List[str] = []

    env_ids = os.getenv("LINE_USER_IDS") or env_values.get("LINE_USER_IDS")
    if env_ids:
        ids.extend([cid.strip() for cid in env_ids.split(",") if cid.strip()])

    for key in ("LINE_USER_ID", "LINE_USER_ID1", "LINE_USER_ID2"):
        val = os.getenv(key) or env_values.get(key)
        if val:
            ids.append(val.strip())

    ids.extend([str(cid) for cid in line_cfg.get("user_ids", []) if cid])
    if line_cfg.get("user_id"):
        ids.append(str(line_cfg["user_id"]))

    seen: Set[str] = set()
    unique_ids: List[str] = []
    for cid in ids:
        if cid not in seen:
            seen.add(cid)
            unique_ids.append(cid)
    return unique_ids


def filter_products(
    products: Iterable[Dict[str, Any]],
    include_keywords: List[str],
    exclude_keywords: List[str],
    require_available: bool,
    only_bags: bool,
    allowed_regions: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    results = []
    include_pairs = [(k, k.lower()) for k in include_keywords if k]
    exclude_lower = [k.lower() for k in exclude_keywords if k]
    if allowed_regions is None:
        allowed_regions_lower = None
    else:
        allowed_regions_lower = [r.lower() for r in allowed_regions if r]
        if not allowed_regions_lower:
            return []

    for product in products:
        name_raw = product.get("name") or ""
        name = name_raw.lower()
        region = (product.get("region") or "").lower()
        # Region first
        if allowed_regions_lower is not None:
            if not region or region not in allowed_regions_lower:
                continue

        matched_include = None
        for raw, lowered in include_pairs:
            if lowered in name:
                matched_include = raw
                break
        if include_pairs and matched_include is None:
            continue
        if any(k in name for k in exclude_lower):
            continue
        # only_bags can be overruled by include; require_available cannot
        if only_bags and not product.get("is_bag") and matched_include is None:
            continue
        if require_available and product.get("unavailable"):
            continue

        annotated = dict(product)
        annotated["_matched_include"] = matched_include
        results.append(annotated)
    return results


def send_telegram(bot_token: str, chat_id: str, text: str) -> bool:
    if not bot_token or not chat_id:
        print("[WARN] Telegram not configured; skip send")
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = requests.post(
            url,
            timeout=15,
            data={"chat_id": chat_id, "text": text},
        )
        if resp.status_code != 200:
            print(f"[WARN] Telegram send failed: {resp.status_code} {resp.text}")
            return False
        return True
    except requests.RequestException as exc:
        print(f"[WARN] Telegram send error: {exc}")
        return False


def send_line(line_token: str, user_id: str, text: str) -> bool:
    if not line_token or not user_id:
        print("[WARN] LINE not configured; skip send")
        return False
    print(f"[INFO] Sending LINE to {user_id}")
    try:
        if (
            MessagingApi is not None
            and Configuration is not None
            and ApiClient is not None
            and PushMessageRequest is not None
            and TextMessage is not None
        ):
            config = Configuration(access_token=line_token)
            with ApiClient(config) as api_client:
                api_instance = MessagingApi(api_client)
                request = PushMessageRequest(
                    to=user_id,
                    messages=[TextMessage(text=text[:4000])],
                )
                api_instance.push_message(request)
        else:
            resp = requests.post(
                "https://api.line.me/v2/bot/message/push",
                timeout=15,
                headers={
                    "Authorization": f"Bearer {line_token}",
                    "Content-Type": "application/json",
                },
                json={
                    "to": user_id,
                    "messages": [{"type": "text", "text": text[:4000]}],
                },
            )
            if resp.status_code not in {200, 202}:
                print(f"[WARN] LINE send failed to {user_id}: {resp.status_code} {resp.text}")
                return False
        print(f"[INFO] LINE sent to {user_id}")
        return True
    except (LineBotApiError, requests.RequestException) as exc:
        print(f"[WARN] LINE send failed to {user_id}: {exc}")
        return False


def format_product(product: Dict[str, Any]) -> str:
    def clean_color(value: Any) -> str:
        if not value:
            return "-"
        text = str(value)
        # Grab all explicit "Color: xxx" occurrences
        matches = re.findall(r"(?i)(color|couleur|coloris|farbe)\s*[:：]\s*([^,;\n]+)", text)
        for m in matches:
            candidate = m[1].strip(" :")
            if candidate:
                return candidate
        # Split tokens on comma/semicolon and ignore blanks or ones mentioning 'color'
        tokens = [t.strip(" :") for t in re.split(r"[;,]", text) if t.strip(" :")]
        tokens = [t for t in tokens if t and not re.search(r"(?i)(color|couleur|coloris|farbe|顏色|颜色|カラー)", t)]
        if tokens:
            return tokens[0]
        # Last resort: stripped text, unless it's just a colon
        text = text.strip(" :")
        return text or "-"

    def encode_url(url: str) -> str:
        try:
            return quote(url, safe=":/?&=#%-._~")
        except Exception:
            return url

    name = product.get("name", "Unknown")
    color = clean_color(product.get("color"))
    price = product.get("price") or "-"
    url = product.get("url") or "-"
    encoded_url = encode_url(url) if url else "-"
    matched_include = product.get("_matched_include") or "-"
    availability = "available" if not product.get("unavailable") else "unavailable"
    return (
        f"{name}\n"
        f"Color: {color}\n"
        f"Price: {price}\n"
        f"Availability: {availability}\n"
        f"Matched include: {matched_include}\n"
        f"{encoded_url}"
    )


def _format_product_fixed(product: Dict[str, Any]) -> str:
    def clean_color(value: Any) -> str:
        if not value:
            return "-"
        text = str(value)
        matches = re.findall(r"(?i)(color|couleur|coloris|farbe|顏色|颜色|カラー)\s*[:：]\s*([^,;\n]+)", text)
        for m in matches:
            candidate = m[1].strip(" :")
            if candidate:
                return candidate
        tokens = [t.strip(" :") for t in re.split(r"[;,]", text) if t.strip(" :")]
        tokens = [t for t in tokens if t and not re.search(r"(?i)(color|couleur|coloris|farbe|顏色|颜色|カラー)", t)]
        if tokens:
            return tokens[0]
        text = text.strip(" :")
        return text or "-"

    def encode_url(url: str) -> str:
        try:
            return quote(url, safe=":/?&=#%-._~")
        except Exception:
            return url

    name = product.get("name", "Unknown")
    color = clean_color(product.get("color"))
    price = product.get("price") or "-"
    url = product.get("url") or "-"
    encoded_url = encode_url(url) if url else "-"
    matched_include = product.get("_matched_include") or "-"
    availability = "available" if not product.get("unavailable") else "unavailable"
    return (
        f"{name}\n"
        f"Color: {color}\n"
        f"Price: {price}\n"
        f"Availability: {availability}\n"
        f"Matched include: {matched_include}\n"
        f"{encoded_url}"
    )

# Override broken format_product with fixed version
format_product = _format_product_fixed


def build_session_for_scraper(scraper_kwargs: Dict[str, Any]) -> requests.Session:
    homepage_url = scraper_kwargs.get("homepage_url")
    session_kwargs: Dict[str, Any] = {}
    if scraper_kwargs.get("impersonate_profiles"):
        session_kwargs["impersonate_profiles"] = scraper_kwargs["impersonate_profiles"]
    if "rotate_profiles_on_block" in scraper_kwargs:
        session_kwargs["rotate_profiles_on_block"] = bool(scraper_kwargs["rotate_profiles_on_block"])
    if homepage_url:
        return create_session(homepage_url=homepage_url, **session_kwargs)
    return create_session(**session_kwargs)


def compute_failure_cooldown(
    consecutive_failures: int,
    base_seconds: int,
    max_seconds: int,
) -> int:
    step = max(consecutive_failures - 1, 0)
    cooldown = int(base_seconds * (2 ** step))
    return max(base_seconds, min(cooldown, max_seconds))


def send_telegram_to_all(bot_token: str, chat_ids: List[str], text: str) -> None:
    for chat_id in chat_ids:
        send_telegram(bot_token, chat_id, text)


def notification_round_key(channel: str, target_id: str) -> str:
    return f"{channel}:{target_id}"


def has_identical_round_messages(
    previous_round_messages: Dict[str, List[str]],
    channel: str,
    target_id: str,
    messages: List[str],
) -> bool:
    return previous_round_messages.get(notification_round_key(channel, target_id)) == messages


def get_primary_telegram_target(chat_ids: List[str], env_values: Dict[str, str]) -> Optional[str]:
    return (
        os.getenv("TELEGRAM_CHAT_ID1")
        or env_values.get("TELEGRAM_CHAT_ID1")
        or (chat_ids[0] if chat_ids else None)
    )


def classify_fetch_issue(fetch_meta: Dict[str, Any], product_count: int) -> tuple[str, str]:
    status = fetch_meta.get("last_status")
    error_text = str(fetch_meta.get("last_error") or "").strip()
    if fetch_meta.get("blocked"):
        block_reason = str(fetch_meta.get("block_reason") or "").strip() or "BLOCKED"
        block_detail = str(fetch_meta.get("block_detail") or "").strip()
        if not block_detail:
            if fetch_meta.get("rate_limited"):
                block_detail = f"HTTP {status}" if status else "rate-limit signal"
            else:
                block_detail = f"HTTP {status}" if status else "challenge page detected"
        return block_reason, block_detail
    if product_count > 0:
        return "OK", ""
    if error_text:
        return "NETWORK_ERROR", error_text
    if status and status != 200:
        return "HTTP_ERROR", f"HTTP {status}"
    if status == 200:
        return "EMPTY_RESULT", "HTTP 200 but no products parsed"
    return "UNKNOWN_FAILURE", "No response metadata"


def _infer_region_from_path(path_value: str) -> str:
    name = Path(path_value).stem.lower()
    tokens = set(name.replace("-", "_").split("_"))
    if "fr" in tokens or name.endswith("fr"):
        return "FR"
    if "tw" in tokens or name.endswith("tw"):
        return "TW"
    if "jp" in tokens or name.endswith("jp"):
        return "JP"
    return "EU_MAIN"


def _parse_region_arg(arg_value: str) -> tuple[str, str]:
    if "=" in arg_value:
        region_raw, path_value = arg_value.split("=", 1)
        region = region_raw.strip().upper()
        if region in {"EU_MAIN", "FR", "TW", "JP"}:
            return path_value.strip(), region
    return arg_value, _infer_region_from_path(arg_value)


def _load_products_from_html_args(html_args: List[str]) -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    for arg in html_args:
        path_value, region = _parse_region_arg(arg)
        html = Path(path_value).read_text(encoding="utf-8", errors="ignore")
        parsed = parse_products_from_html(html)
        products.extend([dict(item, region=region) for item in parsed])
    return products


def _load_products_from_json_args(json_args: List[str]) -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    for arg in json_args:
        path_value, region = _parse_region_arg(arg)
        raw_text = Path(path_value).read_text(encoding="utf-8", errors="ignore")
        data = json.loads(raw_text)
        parsed = parse_products_from_json_data(data)
        products.extend([dict(item, region=region) for item in parsed])
    return products


def _coerce_string_list(value: Any) -> List[str]:
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def run_offline(
    config_path: str,
    html_args: List[str],
    json_args: List[str],
    send_test: bool = False,
) -> None:
    config = load_config(config_path)

    filter_cfg = config.get("filter", {})
    include_keywords = filter_cfg.get("include_keywords", DEFAULT_INCLUDE)
    exclude_keywords = filter_cfg.get("exclude_keywords", DEFAULT_EXCLUDE)
    require_available = filter_cfg.get("require_available", True)
    only_bags = filter_cfg.get("only_bags", True)

    telegram_cfg = config.get("telegram", {})
    env_values = load_dotenv()
    bot_token = (
        os.getenv("TELEGRAM_BOT_TOKEN")
        or env_values.get("TELEGRAM_BOT_TOKEN")
        or telegram_cfg.get("bot_token", "")
    )
    chat_ids = collect_chat_ids(telegram_cfg, env_values)
    telegram_enabled = telegram_cfg.get("enabled", False) and bot_token and chat_ids
    send_every_poll = telegram_cfg.get("send_every_poll", False)

    line_cfg = config.get("line", {})
    line_token = (
        os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
        or env_values.get("LINE_CHANNEL_ACCESS_TOKEN")
        or line_cfg.get("channel_access_token", "")
    )
    line_sdk_available = (
        MessagingApi is not None
        and Configuration is not None
        and ApiClient is not None
        and PushMessageRequest is not None
        and TextMessage is not None
    )
    line_base_enabled = (
        line_cfg.get("enabled", False)
        and line_token
    )
    line_user_prefs = load_line_user_prefs(line_cfg.get("user_db", "line_users.json"))
    line_user_ids = [pref.get("user_id") for pref in line_user_prefs if pref.get("user_id")]

    if send_test:
        test_msg = "Hermes monitor test notification"
        if telegram_enabled:
            for chat_id in chat_ids:
                send_telegram(bot_token, chat_id, test_msg)
        if line_base_enabled and line_user_ids:
            for pref in line_user_prefs:
                user_id = pref.get("user_id")
                if user_id:
                    send_line(line_token, user_id, test_msg)

    products = []
    if html_args:
        products.extend(_load_products_from_html_args(html_args))
    if json_args:
        products.extend(_load_products_from_json_args(json_args))

    region_order = ["EU_MAIN", "FR", "TW", "JP"]
    counts = {region: 0 for region in region_order}
    for item in products:
        region = item.get("region") or "EU_MAIN"
        if region in counts:
            counts[region] += 1

    print(
        f"[INFO] Offline products main={counts['EU_MAIN']} fr={counts['FR']} "
        f"tw={counts['TW']} jp={counts['JP']}"
    )

    combined_products = products
    filtered = filter_products(
        combined_products,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        require_available=require_available,
        only_bags=only_bags,
    )
    if not combined_products:
        print("[WARN] No products loaded from offline inputs.")
    elif not filtered:
        print("[INFO] No products matched filters for offline inputs.")

    seen: Set[str] = set()
    to_notify_raw = filtered if send_every_poll else [item for item in filtered if item.get("url") not in seen]
    round_seen_urls: Set[str] = set()
    to_notify: List[Dict[str, Any]] = []
    for item in to_notify_raw:
        url = item.get("url")
        if url and url in round_seen_urls:
            continue
        if url:
            round_seen_urls.add(url)
        to_notify.append(item)

    telegram_messages: List[str] = []
    for item in to_notify:
        if require_available and item.get("unavailable"):
            continue
        if not send_every_poll:
            url = item.get("url")
            if url:
                seen.add(url)
        message = format_product(item)
        telegram_messages.append(message)
        print(f"\n[HIT] {item.get('name')}\n{message}")
    if telegram_enabled:
        for chat_id in chat_ids:
            for message in telegram_messages:
                send_telegram(bot_token, chat_id, message)

    line_round_seen: DefaultDict[str, Set[str]] = defaultdict(set)
    if line_base_enabled and combined_products:
        for pref in line_user_prefs:
            user_id = pref.get("user_id")
            if not user_id:
                continue
            notify_until = pref.get("notify_until")
            if not notify_until:
                continue
            try:
                dt_until = datetime.fromisoformat(str(notify_until))
                if datetime.now() > dt_until:
                    continue
            except Exception:
                continue
            include_kw = pref.get("include_keywords", [])
            exclude_kw = pref.get("exclude_keywords", [])
            req_avail = pref.get("require_available", True)
            only_bag_pref = pref.get("only_bags", True)
            regions_pref = pref.get("regions")

            seen_urls = line_round_seen[user_id]
            filtered_items: List[Dict[str, Any]] = []
            for item in combined_products:
                user_filtered = filter_products(
                    [item],
                    include_keywords=include_kw,
                    exclude_keywords=exclude_kw,
                    require_available=req_avail,
                    only_bags=only_bag_pref,
                    allowed_regions=regions_pref,
                )
                if not user_filtered:
                    continue
                item_filtered = user_filtered[0]
                url = item_filtered.get("url")
                if url and url in seen_urls:
                    continue
                if url:
                    seen_urls.add(url)
                filtered_items.append(item_filtered)

            if not filtered_items:
                continue

            for item_filtered in filtered_items:
                msg = format_product(item_filtered)
                send_line(line_token, user_id, msg)

def run_loop(config_path: str, send_test: bool = False) -> None:
    config = load_config(config_path)
    settings_cfg = config.get("settings", {})

    filter_cfg = config.get("filter", {})
    include_keywords = filter_cfg.get("include_keywords", DEFAULT_INCLUDE)
    exclude_keywords = filter_cfg.get("exclude_keywords", DEFAULT_EXCLUDE)
    require_available = filter_cfg.get("require_available", True)
    only_bags = filter_cfg.get("only_bags", True)

    schedule_cfg = config.get("polling", {})
    min_seconds = max(schedule_cfg.get("min_seconds", DEFAULT_MIN_SECONDS), 1)
    max_seconds = max(schedule_cfg.get("max_seconds", DEFAULT_MAX_SECONDS), min_seconds)
    politeness_cfg = config.get("politeness", {})
    minimum_poll_floor = max(politeness_cfg.get("minimum_poll_seconds", DEFAULT_MIN_POLL_FLOOR), 1)
    failure_cooldown_seconds = max(
        politeness_cfg.get("failure_cooldown_seconds", DEFAULT_FAILURE_COOLDOWN_SECONDS),
        1,
    )
    blocked_cooldown_seconds = max(
        politeness_cfg.get("blocked_cooldown_seconds", DEFAULT_BLOCKED_COOLDOWN_SECONDS),
        failure_cooldown_seconds,
    )
    max_failure_cooldown_seconds = max(
        politeness_cfg.get("max_failure_cooldown_seconds", DEFAULT_MAX_FAILURE_COOLDOWN_SECONDS),
        failure_cooldown_seconds,
    )
    if min_seconds < minimum_poll_floor:
        print(
            f"[WARN] polling.min_seconds={min_seconds} is aggressive; "
            f"raising to safer floor {minimum_poll_floor}s"
        )
        min_seconds = minimum_poll_floor
    max_seconds = max(max_seconds, min_seconds)

    telegram_cfg = config.get("telegram", {})
    env_values = load_dotenv()
    bot_token = (
        os.getenv("TELEGRAM_BOT_TOKEN")
        or env_values.get("TELEGRAM_BOT_TOKEN")
        or telegram_cfg.get("bot_token", "")
    )
    chat_ids = collect_chat_ids(telegram_cfg, env_values)
    telegram_enabled = telegram_cfg.get("enabled", False) and bot_token and chat_ids
    send_every_poll = telegram_cfg.get("send_every_poll", False)
    health_alerts_cfg = config.get("health_alerts", {})
    health_alerts_enabled = health_alerts_cfg.get("enabled", True)
    alert_reminder_seconds = max(
        health_alerts_cfg.get("reminder_seconds", DEFAULT_ALERT_REMINDER_SECONDS),
        60,
    )
    heartbeat_seconds = max(
        health_alerts_cfg.get("heartbeat_seconds", DEFAULT_HEARTBEAT_SECONDS),
        60,
    )

    line_cfg = config.get("line", {})
    line_token = (
        os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
        or env_values.get("LINE_CHANNEL_ACCESS_TOKEN")
        or line_cfg.get("channel_access_token", "")
    )
    line_sdk_available = (
        MessagingApi is not None
        and Configuration is not None
        and ApiClient is not None
        and PushMessageRequest is not None
        and TextMessage is not None
    )
    line_base_enabled = (
        line_cfg.get("enabled", False)
        and line_token
    )
    line_user_prefs = load_line_user_prefs(line_cfg.get("user_db", "line_users.json"))
    line_user_ids = [pref.get("user_id") for pref in line_user_prefs if pref.get("user_id")]

    history_cfg = config.get("history", {})
    history_enabled = history_cfg.get("enabled", True)
    history_path = history_cfg.get("path", "output/product_history.jsonl")
    impersonate_profiles = _coerce_string_list(settings_cfg.get("impersonate_profiles"))
    rotate_profiles_on_block = bool(settings_cfg.get("rotate_profiles_on_block", True))

    scraper_cfg = config.get("scraper", {})

    seen: Set[str] = set()

    print(
        f"[INFO] Start polling every {min_seconds}-{max_seconds}s | "
        f"include={include_keywords or 'ALL'} exclude={exclude_keywords} | "
        f"require_available={require_available} only_bags={only_bags} | "
        f"send_every_poll={send_every_poll}"
    )
    if telegram_enabled:
        print(f"[INFO] Telegram enabled for chat_ids={chat_ids}")
    else:
        print("[INFO] Telegram disabled (set telegram.enabled: true and tokens to enable)")
    if line_base_enabled and line_user_ids:
        print(f"[INFO] LINE enabled for user_ids={line_user_ids}")
    else:
        reason = []
        if not line_cfg.get("enabled", False):
            reason.append("disabled in config")
        if not line_token:
            reason.append("missing token")
        if not line_user_ids:
            reason.append("missing user_ids")
        if not line_sdk_available:
            reason.append(f"line-bot-sdk not available, using HTTP fallback disabled only if token missing ({LINE_IMPORT_ERROR or 'import failed'})")
        reason_str = "; ".join(reason) if reason else "disabled"
        print(f"[INFO] LINE disabled ({reason_str})")

    primary_chat_id = get_primary_telegram_target(chat_ids, env_values)

    if send_test:
        test_msg = "Hermès monitor test notification"
        if telegram_enabled:
            for chat_id in chat_ids:
                send_telegram(bot_token, chat_id, test_msg)
        if line_base_enabled and line_user_ids:
            for pref in line_user_prefs:
                user_id = pref.get("user_id")
                if user_id:
                    send_line(line_token, user_id, test_msg)

    scraper_kwargs: Dict[str, Any] = {
        "save_path": scraper_cfg.get("save_path", "products_all.json"),
        "debug_path": scraper_cfg.get("debug_path", "debug.html"),
        "pause_minutes_on_fail": scraper_cfg.get("pause_minutes_on_fail", 5),
        "sleep_on_fail": False,
        "impersonate_profiles": impersonate_profiles,
        "rotate_profiles_on_block": rotate_profiles_on_block,
    }
    if scraper_cfg.get("category_url"):
        scraper_kwargs["category_url"] = scraper_cfg["category_url"]
    if scraper_cfg.get("category_urls"):
        scraper_kwargs["category_urls"] = scraper_cfg["category_urls"]
    if scraper_cfg.get("homepage_url"):
        scraper_kwargs["homepage_url"] = scraper_cfg["homepage_url"]

    fr_scraper_cfg = config.get("scraper_fr", {})
    fr_kwargs: Dict[str, Any] = {
        "save_path": fr_scraper_cfg.get("save_path", scraper_cfg.get("save_path", "products_all.json")),
        "debug_path": fr_scraper_cfg.get("debug_path", "debug_fr.html"),
        "pause_minutes_on_fail": fr_scraper_cfg.get(
            "pause_minutes_on_fail", scraper_cfg.get("pause_minutes_on_fail", 5)
        ),
        "sleep_on_fail": False,
        "impersonate_profiles": impersonate_profiles,
        "rotate_profiles_on_block": rotate_profiles_on_block,
    }
    if fr_scraper_cfg.get("category_url"):
        fr_kwargs["category_url"] = fr_scraper_cfg["category_url"]
    if fr_scraper_cfg.get("category_urls"):
        fr_kwargs["category_urls"] = fr_scraper_cfg["category_urls"]
    if fr_scraper_cfg.get("homepage_url"):
        fr_kwargs["homepage_url"] = fr_scraper_cfg["homepage_url"]

    tw_scraper_cfg = config.get("scraper_tw", {})
    tw_kwargs: Dict[str, Any] = {
        "save_path": tw_scraper_cfg.get("save_path", scraper_cfg.get("save_path", "products_all.json")),
        "debug_path": tw_scraper_cfg.get("debug_path", "debug_tw.html"),
        "pause_minutes_on_fail": tw_scraper_cfg.get(
            "pause_minutes_on_fail", scraper_cfg.get("pause_minutes_on_fail", 5)
        ),
        "sleep_on_fail": False,
        "impersonate_profiles": impersonate_profiles,
        "rotate_profiles_on_block": rotate_profiles_on_block,
    }
    if tw_scraper_cfg.get("category_url"):
        tw_kwargs["category_url"] = tw_scraper_cfg["category_url"]
    if tw_scraper_cfg.get("category_urls"):
        tw_kwargs["category_urls"] = tw_scraper_cfg["category_urls"]
    if tw_scraper_cfg.get("homepage_url"):
        tw_kwargs["homepage_url"] = tw_scraper_cfg["homepage_url"]

    jp_scraper_cfg = config.get("scraper_jp", {})
    jp_kwargs: Dict[str, Any] = {
        "save_path": jp_scraper_cfg.get("save_path", scraper_cfg.get("save_path", "products_all.json")),
        "debug_path": jp_scraper_cfg.get("debug_path", "debug_jp.html"),
        "pause_minutes_on_fail": jp_scraper_cfg.get(
            "pause_minutes_on_fail", scraper_cfg.get("pause_minutes_on_fail", 5)
        ),
        "sleep_on_fail": False,
        "impersonate_profiles": impersonate_profiles,
        "rotate_profiles_on_block": rotate_profiles_on_block,
    }
    if jp_scraper_cfg.get("category_url"):
        jp_kwargs["category_url"] = jp_scraper_cfg["category_url"]
    if jp_scraper_cfg.get("category_urls"):
        jp_kwargs["category_urls"] = jp_scraper_cfg["category_urls"]
    if jp_scraper_cfg.get("homepage_url"):
        jp_kwargs["homepage_url"] = jp_scraper_cfg["homepage_url"]

    region_order = ["EU_MAIN", "FR", "TW", "JP"]
    region_kwargs_map: Dict[str, Dict[str, Any]] = {
        "EU_MAIN": scraper_kwargs,
        "FR": fr_kwargs,
        "TW": tw_kwargs,
        "JP": jp_kwargs,
    }
    region_enabled: Dict[str, bool] = {
        "EU_MAIN": scraper_cfg.get("enabled", True),
        "FR": fr_scraper_cfg.get("enabled", True),
        "TW": tw_scraper_cfg.get("enabled", True),
        "JP": jp_scraper_cfg.get("enabled", True),
    }
    region_next_allowed: Dict[str, float] = {region: 0.0 for region in region_order}
    region_failures: DefaultDict[str, int] = defaultdict(int)
    region_sessions: Dict[str, requests.Session] = {}
    region_health: Dict[str, Dict[str, Any]] = {}
    region_round_status: Dict[str, str] = {}
    line_round_seen: DefaultDict[str, Set[str]] = defaultdict(set)
    last_round_messages: Dict[str, List[str]] = {}
    next_heartbeat_at = time.time() + heartbeat_seconds

    def fetch_region_products(region_name: str, now: float) -> List[Dict[str, Any]]:
        kwargs = region_kwargs_map[region_name]
        if not region_enabled.get(region_name, True):
            region_round_status[region_name] = "disabled"
            return []
        has_target = kwargs.get("category_url") or kwargs.get("category_urls")
        if not has_target:
            region_round_status[region_name] = "disabled"
            return []
        next_allowed = region_next_allowed[region_name]
        if now < next_allowed:
            remaining = next_allowed - now
            print(f"[INFO] {region_name} fetch on cooldown for {remaining:.0f}s")
            health_state = region_health.get(region_name, {})
            issue_code = health_state.get("issue_code", "COOLDOWN")
            region_round_status[region_name] = f"cooldown({remaining:.0f}s,{issue_code})"
            return []

        session = region_sessions.get(region_name)
        if session is None:
            session = build_session_for_scraper(kwargs)
            region_sessions[region_name] = session

        products_raw, fetch_meta = get_all_products(
            **kwargs,
            history_enabled=history_enabled,
            history_path=history_path,
            region_name=region_name,
            session=session,
            return_metadata=True,
        )
        region_products = [dict(item, region=region_name) for item in products_raw]
        issue_code, issue_detail = classify_fetch_issue(fetch_meta, len(region_products))

        if fetch_meta.get("blocked"):
            region_failures[region_name] += 1
            region_next_allowed[region_name] = now + blocked_cooldown_seconds
            region_sessions.pop(region_name, None)
            print(
                f"[WARN] {region_name} fetch blocked: {issue_code} | "
                f"{issue_detail or '-'} | cooldown {blocked_cooldown_seconds}s"
            )
            region_round_status[region_name] = f"{issue_code.lower()}(retry={blocked_cooldown_seconds}s)"
        elif issue_code != "OK":
            region_failures[region_name] += 1
            cooldown = compute_failure_cooldown(
                region_failures[region_name],
                failure_cooldown_seconds,
                max_failure_cooldown_seconds,
            )
            region_next_allowed[region_name] = now + cooldown
            region_sessions.pop(region_name, None)
            print(
                f"[WARN] {region_name} fetch failed or returned no products; "
                f"cooldown {cooldown}s (failure streak={region_failures[region_name]})"
            )
            region_round_status[region_name] = f"{issue_code.lower()}(retry={cooldown}s)"
        else:
            previous = region_health.get(region_name)
            if previous and previous.get("active") and telegram_enabled and health_alerts_enabled:
                recovered_msg = (
                    f"[recovered] {region_name} is healthy again\n"
                    f"Products: {len(region_products)}\n"
                    f"Recovered at: {time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                if primary_chat_id:
                    send_telegram(bot_token, primary_chat_id, recovered_msg)
            region_failures[region_name] = 0
            region_next_allowed[region_name] = 0.0
            region_health[region_name] = {
                "active": False,
                "issue_code": "OK",
                "issue_detail": "",
                "last_alert_at": 0.0,
            }
            write_session_diagnostic(
                session=session,
                region_name=region_name,
                homepage_url=str(kwargs.get("homepage_url") or ""),
            )
            region_round_status[region_name] = f"ok({len(region_products)})"
            return region_products

        health_state = region_health.get(region_name, {})
        should_alert = False
        if not health_state.get("active"):
            should_alert = True
        elif health_state.get("issue_code") != issue_code or health_state.get("issue_detail") != issue_detail:
            should_alert = True
        elif now - float(health_state.get("last_alert_at") or 0.0) >= alert_reminder_seconds:
            should_alert = True

        if should_alert and telegram_enabled and health_alerts_enabled:
            cooldown_seconds = max(int(region_next_allowed[region_name] - now), 0)
            failure_msg = (
                f"[alert] {region_name} fetch unhealthy\n"
                f"Type: {issue_code}\n"
                f"Detail: {issue_detail or '-'}\n"
                f"Failure streak: {region_failures[region_name]}\n"
                f"Retry in: {cooldown_seconds}s\n"
                f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            if primary_chat_id:
                send_telegram(bot_token, primary_chat_id, failure_msg)

        region_health[region_name] = {
            "active": True,
            "issue_code": issue_code,
            "issue_detail": issue_detail,
            "last_alert_at": now if should_alert else health_state.get("last_alert_at", 0.0),
        }
        return []

    while True:
        line_user_prefs = load_line_user_prefs(line_cfg.get("user_db", "line_users.json"))
        line_user_ids = [pref.get("user_id") for pref in line_user_prefs if pref.get("user_id")]
        line_enabled = line_base_enabled and bool(line_user_ids)
        next_round_messages: Dict[str, List[str]] = {}
        if telegram_enabled:
            for chat_id in chat_ids:
                next_round_messages[notification_round_key("telegram", chat_id)] = []
        if line_enabled:
            for user_id in line_user_ids:
                next_round_messages[notification_round_key("line", user_id)] = []

        line_round_seen.clear()
        region_round_status.clear()
        now = time.time()
        products = fetch_region_products("EU_MAIN", now)
        fr_products = fetch_region_products("FR", now)
        tw_products = fetch_region_products("TW", now)
        jp_products = fetch_region_products("JP", now)
        print(
            f"[INFO] Products main={len(products)} fr={len(fr_products)} tw={len(tw_products)} jp={len(jp_products)} | "
            f"status main={region_round_status.get('EU_MAIN', '-')} "
            f"fr={region_round_status.get('FR', '-')} "
            f"tw={region_round_status.get('TW', '-')} "
            f"jp={region_round_status.get('JP', '-')}"
        )

        combined_products = products + fr_products + tw_products + jp_products

        filtered = filter_products(
            combined_products,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            require_available=require_available,
            only_bags=only_bags,
        )
        if not products:
            print("[WARN] No products fetched this round (category fetch failed).")
        elif not filtered:
            print("[INFO] No products matched filters this round.")
        to_notify_raw = filtered if send_every_poll else [item for item in filtered if item.get("url") not in seen]
        # Deduplicate by URL within this round to avoid double send across mirrored catalogs.
        round_seen_urls: Set[str] = set()
        to_notify: List[Dict[str, Any]] = []
        for item in to_notify_raw:
            url = item.get("url")
            if url and url in round_seen_urls:
                continue
            if url:
                round_seen_urls.add(url)
            to_notify.append(item)

        telegram_messages: List[str] = []
        for item in to_notify:
            if require_available and item.get("unavailable"):
                # Safety guard: skip unavailable items when require_available is true.
                continue
            if not send_every_poll:
                url = item.get("url")
                if url:
                    seen.add(url)
            message = format_product(item)
            telegram_messages.append(message)
            print(f"\n[HIT] {item.get('name')}\n{message}")
        if telegram_enabled:
            for chat_id in chat_ids:
                if not telegram_messages:
                    continue
                if has_identical_round_messages(last_round_messages, "telegram", chat_id, telegram_messages):
                    print(f"[INFO] Telegram skip identical round for {chat_id}", flush=True)
                    next_round_messages[notification_round_key("telegram", chat_id)] = list(telegram_messages)
                    continue
                sent_all = True
                for message in telegram_messages:
                    if not send_telegram(bot_token, chat_id, message):
                        sent_all = False
                if sent_all:
                    next_round_messages[notification_round_key("telegram", chat_id)] = list(telegram_messages)

        # LINE notifications evaluated independently per user on all combined products
        if line_enabled and combined_products:
            for pref in line_user_prefs:
                user_id = pref.get("user_id")
                if not user_id:
                    continue
                notify_until = pref.get("notify_until")
                if not notify_until:
                    continue
                try:
                    dt_until = datetime.fromisoformat(str(notify_until))
                    if datetime.now() > dt_until:
                        continue
                except Exception:
                    continue
                include_kw = pref.get("include_keywords", [])
                exclude_kw = pref.get("exclude_keywords", [])
                req_avail = pref.get("require_available", True)
                only_bag_pref = pref.get("only_bags", True)
                regions_pref = pref.get("regions")

                seen_urls = line_round_seen[user_id]
                filtered_items: List[Dict[str, Any]] = []
                for item in combined_products:
                    user_filtered = filter_products(
                        [item],
                        include_keywords=include_kw,
                        exclude_keywords=exclude_kw,
                        require_available=req_avail,
                        only_bags=only_bag_pref,
                        allowed_regions=regions_pref,
                    )
                    if not user_filtered:
                        continue
                    item_filtered = user_filtered[0]
                    url = item_filtered.get("url")
                    if url and url in seen_urls:
                        continue
                    if url:
                        seen_urls.add(url)
                    filtered_items.append(item_filtered)

                line_messages = [format_product(item_filtered) for item_filtered in filtered_items]
                if not line_messages:
                    continue
                if has_identical_round_messages(last_round_messages, "line", user_id, line_messages):
                    print(f"[INFO] LINE skip identical round for {user_id}", flush=True)
                    next_round_messages[notification_round_key("line", user_id)] = list(line_messages)
                    continue
                sent_all = True
                for msg in line_messages:
                    if not send_line(line_token, user_id, msg):
                        sent_all = False
                if sent_all:
                    next_round_messages[notification_round_key("line", user_id)] = list(line_messages)

        # Heartbeat is independent from product notifications and only suppressed by active incidents.
        has_active_incident = any(state.get("active") for state in region_health.values())
        if telegram_enabled and not has_active_incident and time.time() >= next_heartbeat_at:
            heartbeat_target = primary_chat_id
            if heartbeat_target:
                total_checked = len(products) + len(fr_products) + len(tw_products) + len(jp_products)
                heartbeat = (
                    f"[heartbeat] checked {total_checked} items "
                    f"(main={len(products)} fr={len(fr_products)} tw={len(tw_products)} jp={len(jp_products)}) "
                    f"at {time.strftime('%H:%M:%S')}"
                )
                if send_telegram(bot_token, heartbeat_target, heartbeat):
                    next_heartbeat_at = time.time() + heartbeat_seconds

        # Move current round snapshots to "last" for next round
        last_round_messages = next_round_messages

        ready_regions = [
            region for region in region_order
            if (region_kwargs_map[region].get("category_url") or region_kwargs_map[region].get("category_urls"))
            and region_next_allowed[region] <= time.time()
        ]
        if ready_regions:
            sleep_seconds = random.uniform(min_seconds, max_seconds)
        else:
            next_wakeup = min(
                region_next_allowed[region]
                for region in region_order
                if (region_kwargs_map[region].get("category_url") or region_kwargs_map[region].get("category_urls"))
            )
            sleep_seconds = max(1.0, min(float(max_seconds), next_wakeup - time.time()))
        print(f"[INFO] Sleeping {sleep_seconds:.1f}s")
        time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hermes product filter and notifier")
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to config YAML (default: config.yaml)",
    )
    parser.add_argument(
        "--send-test",
        action="store_true",
        help="Send a test notification to all configured channels on startup",
    )
    parser.add_argument(
        "--from-html",
        action="append",
        default=[],
        help="Read products from local HTML files instead of live fetch. "
        "You can pass multiple values; use REGION=path (e.g., FR=debug_fr.html) "
        "to override region inference.",
    )
    parser.add_argument(
        "--from-json",
        action="append",
        default=[],
        help="Read products from local JSON files (list of products or Hermes state JSON). "
        "You can pass multiple values; use REGION=path to override region inference.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.from_html or args.from_json:
        run_offline(args.config, args.from_html, args.from_json, send_test=args.send_test)
        return
    run_loop(args.config, send_test=args.send_test)


if __name__ == "__main__":
    main()
