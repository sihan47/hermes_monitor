"""
Filter Hermès products and notify via Telegram on a randomized polling interval.
"""

import argparse
import random
import time
from typing import Any, Dict, Iterable, List, Set
import os
from pathlib import Path

import requests
import yaml

from get_product import get_all_products

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


def filter_products(
    products: Iterable[Dict[str, Any]],
    include_keywords: List[str],
    exclude_keywords: List[str],
    require_available: bool,
    only_bags: bool,
) -> List[Dict[str, Any]]:
    results = []
    include_pairs = [(k, k.lower()) for k in include_keywords if k]
    exclude_lower = [k.lower() for k in exclude_keywords if k]

    for product in products:
        name_raw = product.get("name") or ""
        name = name_raw.lower()
        if only_bags and not product.get("is_bag"):
            continue
        if require_available and product.get("unavailable"):
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


def format_product(product: Dict[str, Any]) -> str:
    name = product.get("name", "Unknown")
    color = product.get("color") or "-"
    price = product.get("price") or "-"
    url = product.get("url") or "-"
    matched_include = product.get("_matched_include") or "-"
    availability = "available" if not product.get("unavailable") else "unavailable"
    return (
        f"{name}\n"
        f"Color: {color}\n"
        f"Price: {price}\n"
        f"Availability: {availability}\n"
        f"Matched include: {matched_include}\n"
        f"{url}"
    )


def run_loop(config_path: str) -> None:
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
        "save_path": scraper_cfg.get("save_path", "products_all.json"),
        "debug_path": scraper_cfg.get("debug_path", "debug_fr.html"),
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

    fr_next_allowed = 0.0

    while True:
        products = get_all_products(**scraper_kwargs)

        now = time.time()
        fr_products: List[Dict[str, Any]] = []
        if fr_kwargs.get("category_url") or fr_kwargs.get("category_urls"):
            if now >= fr_next_allowed:
                fr_products = get_all_products(**fr_kwargs)
                if not fr_products:
                    fr_next_allowed = now + 60 * fr_kwargs.get("pause_minutes_on_fail", 5)
                    print(
                        f"[WARN] FR fetch failed; will retry after "
                        f"{fr_kwargs.get('pause_minutes_on_fail', 5)} minutes"
                    )
            else:
                remaining = fr_next_allowed - now
                print(f"[INFO] FR fetch on cooldown for {remaining:.0f}s")

        filtered = filter_products(
            products + fr_products,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            require_available=require_available,
            only_bags=only_bags,
        )
        if not products:
            print("[WARN] No products fetched this round (category fetch failed).")
        elif not filtered:
            print("[INFO] No products matched filters this round.")
        to_notify = filtered if send_every_poll else [item for item in filtered if item.get("url") not in seen]

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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_loop(args.config)


if __name__ == "__main__":
    main()
