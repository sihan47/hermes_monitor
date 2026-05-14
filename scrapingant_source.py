import os
import math
from pathlib import Path
from typing import Any

from scrapingant_client import ScrapingAntClient
from scrapingant_client.errors import (
    ScrapingantClientException,
    ScrapingantDetectedException,
)


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


def swap_env_values(first_key: str, second_key: str, path: str = ".env") -> None:
    if not first_key or not second_key:
        raise ValueError("Both env variable names are required.")
    if first_key == second_key:
        return

    env_path = Path(path)
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found: {env_path}")

    raw_text = env_path.read_text(encoding="utf-8")
    has_trailing_newline = raw_text.endswith("\n")
    lines = raw_text.splitlines()

    assignments: dict[str, tuple[int, str, str]] = {}
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        left, right = line.split("=", 1)
        key = left.strip()
        if key in {first_key, second_key} and key not in assignments:
            assignments[key] = (index, f"{left}=", right)

    if first_key not in assignments or second_key not in assignments:
        missing = [key for key in (first_key, second_key) if key not in assignments]
        raise KeyError(f"Missing env variable(s): {', '.join(missing)}")

    first_index, first_prefix, first_value = assignments[first_key]
    second_index, second_prefix, second_value = assignments[second_key]

    lines[first_index] = f"{first_prefix}{second_value}"
    lines[second_index] = f"{second_prefix}{first_value}"

    updated_text = "\n".join(lines)
    if has_trailing_newline:
        updated_text += "\n"
    env_path.write_text(updated_text, encoding="utf-8")

    os.environ[first_key] = second_value.strip()
    os.environ[second_key] = first_value.strip()


def swap_process_env_values(first_key: str, second_key: str) -> None:
    if not first_key or not second_key:
        raise ValueError("Both env variable names are required.")
    if first_key == second_key:
        return

    first_value = os.getenv(first_key, "")
    second_value = os.getenv(second_key, "")
    os.environ[first_key] = second_value
    os.environ[second_key] = first_value


def promote_working_token(active_key: str, standby_key: str, path: str = ".env") -> None:
    swap_process_env_values(active_key, standby_key)
    try:
        swap_env_values(active_key, standby_key, path=path)
    except (FileNotFoundError, KeyError, OSError, ValueError) as exc:
        print(f"[WARN] Persisting token order to {path} failed: {exc}")


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
    try:
        result = client.general_request(url, browser=browser)

    except ScrapingantDetectedException as exc:
        print(f"[WARN] ScrapingAnt detected/block for {url}: {exc}")
        return math.nan
    except ScrapingantClientException as exc:
        print(f"[WARN] ScrapingAnt request failed for {url}: {exc}")
        return math.nan
    token_count = 1
    MAX_TOKENS = 9
    while result.status_code == 403 and token_count <= MAX_TOKENS:
        client = _get_client("SCRAPINGANT_API_TOKEN" + str(token_count))
        if isinstance(client, float) and math.isnan(client):
            print(f"[WARN] ScrapingAnt: no more tokens to try after index {token_count}")
            return math.nan
        try:
            result = client.general_request(url, browser=browser)
            if 200 <= result.status_code < 300:
                promote_working_token("SCRAPINGANT_API_TOKEN", "SCRAPINGANT_API_TOKEN" + str(token_count))
        except ScrapingantDetectedException as exc:
            print(f"[WARN] ScrapingAnt detected/block for {url}: {exc}")
            return math.nan
        except ScrapingantClientException as exc:
            print(f"[WARN] ScrapingAnt request failed for {url}: {exc}")
            return math.nan
        token_count += 1

    if result.status_code == 403:
        print(f"[WARN] ScrapingAnt: all tokens returned 403 for {url}")
        return math.nan
    return result.content


if __name__ == "__main__":
    target_url = input("URL: ").strip()
    content = fetch_content(target_url)
