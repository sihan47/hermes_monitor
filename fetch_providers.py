from typing import Any

try:
    from scrapingant_source import fetch_content as _scrapingant_fetch_content
except Exception:  # pragma: no cover - optional dependency
    _scrapingant_fetch_content = None


def coerce_html_text(payload: Any) -> str:
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="ignore")
    if isinstance(payload, str):
        return payload
    if isinstance(payload, float) and payload != payload:
        return ""
    if payload is None:
        return ""
    return str(payload)


def fetch_external_html(url: str) -> str:
    if not callable(_scrapingant_fetch_content) or not url:
        return ""
    try:
        return coerce_html_text(_scrapingant_fetch_content(url))
    except Exception as exc:  # pragma: no cover - external provider dependent
        print(f"[WARN] External fetch failed for {url}: {exc}")
        return ""


def has_external_provider() -> bool:
    return callable(_scrapingant_fetch_content)
