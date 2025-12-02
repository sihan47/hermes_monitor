"""
Filter Hermès products and notify via Telegram on a randomized polling interval.
"""

import argparse
import random
import time
from typing import Any, Dict, Iterable, List, Set, Optional, DefaultDict
import os
from pathlib import Path
import re
from datetime import datetime
from collections import defaultdict
from urllib.parse import quote

import requests
import yaml

from get_product import get_all_products

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
    if MessagingApi is None or Configuration is None or ApiClient is None or PushMessageRequest is None or TextMessage is None:
        print("[WARN] LINE SDK not installed; skip send")
        return False
    if not line_token or not user_id:
        print("[WARN] LINE not configured; skip send")
        return False
    print(f"[INFO] Sending LINE to {user_id}")
    try:
        config = Configuration(access_token=line_token)
        with ApiClient(config) as api_client:
            api_instance = MessagingApi(api_client)
            request = PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text=text[:4000])],
            )
            api_instance.push_message(request)
            print(f"[INFO] LINE sent to {user_id}")
            return True
    except LineBotApiError as exc:
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

def run_loop(config_path: str, send_test: bool = False) -> None:
    config = load_config(config_path)

    filter_cfg = config.get("filter", {})
    include_keywords = filter_cfg.get("include_keywords", DEFAULT_INCLUDE)
    exclude_keywords = filter_cfg.get("exclude_keywords", DEFAULT_EXCLUDE)
    require_available = filter_cfg.get("require_available", True)
    only_bags = filter_cfg.get("only_bags", True)

    schedule_cfg = config.get("polling", {})
    min_seconds = max(schedule_cfg.get("min_seconds", DEFAULT_MIN_SECONDS), 1)
    max_seconds = max(schedule_cfg.get("max_seconds", DEFAULT_MAX_SECONDS), min_seconds)

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
        and line_sdk_available
    )
    line_user_prefs = load_line_user_prefs(line_cfg.get("user_db", "line_users.json"))
    line_user_ids = [pref.get("user_id") for pref in line_user_prefs if pref.get("user_id")]

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
            reason.append(f"line-bot-sdk not available ({LINE_IMPORT_ERROR or 'import failed'})")
        reason_str = "; ".join(reason) if reason else "disabled"
        print(f"[INFO] LINE disabled ({reason_str})")

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
    }
    if jp_scraper_cfg.get("category_url"):
        jp_kwargs["category_url"] = jp_scraper_cfg["category_url"]
    if jp_scraper_cfg.get("category_urls"):
        jp_kwargs["category_urls"] = jp_scraper_cfg["category_urls"]
    if jp_scraper_cfg.get("homepage_url"):
        jp_kwargs["homepage_url"] = jp_scraper_cfg["homepage_url"]

    fr_next_allowed = 0.0
    tw_next_allowed = 0.0
    jp_next_allowed = 0.0
    line_round_seen: DefaultDict[str, Set[str]] = defaultdict(set)

    while True:
        line_user_prefs = load_line_user_prefs(line_cfg.get("user_db", "line_users.json"))
        line_user_ids = [pref.get("user_id") for pref in line_user_prefs if pref.get("user_id")]
        line_enabled = line_base_enabled and bool(line_user_ids)

        line_round_seen.clear()
        products = get_all_products(**scraper_kwargs)
        products = [dict(item, region="EU_MAIN") for item in products]

        now = time.time()
        fr_products: List[Dict[str, Any]] = []
        tw_products: List[Dict[str, Any]] = []
        jp_products: List[Dict[str, Any]] = []
        if fr_kwargs.get("category_url") or fr_kwargs.get("category_urls"):
            if now >= fr_next_allowed:
                fr_products = get_all_products(**fr_kwargs)
                fr_products = [dict(item, region="FR") for item in fr_products]
                if not fr_products:
                    fr_next_allowed = now + 60 * fr_kwargs.get("pause_minutes_on_fail", 5)
                    print(
                        f"[WARN] FR fetch failed; will retry after "
                        f"{fr_kwargs.get('pause_minutes_on_fail', 5)} minutes"
                    )
            else:
                remaining = fr_next_allowed - now
                print(f"[INFO] FR fetch on cooldown for {remaining:.0f}s")

        if tw_kwargs.get("category_url") or tw_kwargs.get("category_urls"):
            if now >= tw_next_allowed:
                tw_products = get_all_products(**tw_kwargs)
                tw_products = [dict(item, region="TW") for item in tw_products]
                if not tw_products:
                    tw_next_allowed = now + 60 * tw_kwargs.get("pause_minutes_on_fail", 5)
                    print(
                        f"[WARN] TW fetch failed; will retry after "
                        f"{tw_kwargs.get('pause_minutes_on_fail', 5)} minutes"
                    )
            else:
                remaining = tw_next_allowed - now
                print(f"[INFO] TW fetch on cooldown for {remaining:.0f}s")

        if jp_kwargs.get("category_url") or jp_kwargs.get("category_urls"):
            if now >= jp_next_allowed:
                jp_products = get_all_products(**jp_kwargs)
                jp_products = [dict(item, region="JP") for item in jp_products]
                if not jp_products:
                    jp_next_allowed = now + 60 * jp_kwargs.get("pause_minutes_on_fail", 5)
                    print(
                        f"[WARN] JP fetch failed; will retry after "
                        f"{jp_kwargs.get('pause_minutes_on_fail', 5)} minutes"
                    )
            else:
                remaining = jp_next_allowed - now
                print(f"[INFO] JP fetch on cooldown for {remaining:.0f}s")

        print(f"[INFO] Products main={len(products)} fr={len(fr_products)} tw={len(tw_products)} jp={len(jp_products)}")

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

        for item in to_notify:
            if require_available and item.get("unavailable"):
                # Safety guard: skip unavailable items when require_available is true.
                continue
            if not send_every_poll:
                url = item.get("url")
                if url:
                    seen.add(url)
            message = format_product(item)
            print(f"\n[HIT] {item.get('name')}\n{message}")
            if telegram_enabled:
                for chat_id in chat_ids:
                    send_telegram(bot_token, chat_id, message)

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
                for item in combined_products:
                    url = item.get("url")
                    if url and url in seen_urls:
                        continue
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
                    if url:
                        seen_urls.add(url)
                    msg = format_product(item)
                    send_line(line_token, user_id, msg)

        # Heartbeat to TELEGRAM_CHAT_ID1 even when no hits, to confirm bot is alive.
        if telegram_enabled and not to_notify:
            heartbeat_target = (
                os.getenv("TELEGRAM_CHAT_ID1")
                or env_values.get("TELEGRAM_CHAT_ID1")
                or (chat_ids[0] if chat_ids else None)
            )
            if heartbeat_target:
                total_checked = len(products) + len(fr_products) + len(tw_products) + len(jp_products)
                heartbeat = (
                    f"[heartbeat] checked {total_checked} items "
                    f"(main={len(products)} fr={len(fr_products)} tw={len(tw_products)} jp={len(jp_products)}) "
                    f"at {time.strftime('%H:%M:%S')}"
                )
                send_telegram(bot_token, heartbeat_target, heartbeat)

        sleep_seconds = random.uniform(min_seconds, max_seconds)
        print(f"[INFO] Sleeping {sleep_seconds:.1f}s")
        time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hermès product filter and notifier")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_loop(args.config, send_test=args.send_test)


if __name__ == "__main__":
    main()
