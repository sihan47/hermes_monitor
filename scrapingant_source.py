import os
import math
from pathlib import Path
from typing import Any

from scrapingant_client import ScrapingAntClient


def _load_env(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env()

def _get_client(token_name: str) -> ScrapingAntClient | float:
    token = os.getenv(token_name, "").strip()
    if not token:
        return math.nan
    return ScrapingAntClient(token=token)


def fetch_content(url: str, browser: bool = False) -> Any:
    client = _get_client("SCRAPINGANT_API_TOKEN")
    if isinstance(client, float) and math.isnan(client):
        return math.nan
    result = client.general_request(url, browser=browser)
    token_count = 1
    while result.status_code == 403:
        # Try the second token if the first one is blocked
        client = _get_client("SCRAPINGANT_API_TOKEN" + str(token_count))
        if isinstance(client, float) and math.isnan(client):
            return math.nan
        result = client.general_request(url, browser=browser)
        token_count += 1

    return result.content


if __name__ == "__main__":
    target_url = input("URL: ").strip()
    content = fetch_content(target_url)
