import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json
import re

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


def create_session() -> requests.Session:
    """建立帶 headers 的 session，先逛首頁拿 cookies。"""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(HOMEPAGE_URL, timeout=20)
    except Exception as e:
        print(f"[WARN] Visit homepage failed: {e}")
    return session


def fetch_category_soup(session: requests.Session) -> BeautifulSoup:
    """抓分類頁 HTML，存 debug.html，回傳 soup。"""
    resp = session.get(CATEGORY_URL, timeout=20)
    print(f"[INFO] GET {CATEGORY_URL} -> {resp.status_code}")
    resp.raise_for_status()

    Path("debug.html").write_text(resp.text, encoding=resp.encoding or "utf-8")
    print("[INFO] Saved raw HTML to debug.html")

    return BeautifulSoup(resp.text, "html.parser")


def is_bag_item(name: str) -> bool:
    """粗略判斷是不是「包」，過濾掉 strap 類。"""
    n = name.lower()
    if "strap" in n:
        return False
    if "bag" in n or "pouch" in n or "clutch" in n:
        return True
    return False


def extract_products_from_soup(soup: BeautifulSoup):
    """
    以 <a href="/product/..."> 為中心、往上找卡片容器，
    每個 (name, url) 視為一個商品。
    """
    products = {}
    # 找所有指向商品頁的連結
    anchors = soup.find_all("a", href=re.compile(r"/product/"))
    print(f"[INFO] Found {len(anchors)} <a> with /product/ href")

    for a in anchors:
        name = a.get_text(strip=True)
        if not name:
            continue  # 沒文字的多半是圖片用的連結

        href = a.get("href")
        if not href:
            continue
        url = href if href.startswith("http") else BASE_URL + href

        key = (name, url)
        if key in products:
            # 已經處理過這個商品了
            continue

        # 往上爬幾層，找到稍大的容器當卡片
        container = a
        for _ in range(4):
            if container.parent is None:
                break
            container = container.parent

        full_text = container.get_text("\n", strip=True)
        lines = [l.strip() for l in full_text.splitlines() if l.strip()]

        color = None
        price = None
        unavailable = False

        for i, line in enumerate(lines):
            if line.startswith("Color"):
                if i + 1 < len(lines):
                    color = lines[i + 1]
            if "€" in line and price is None:
                price = line
            if line == "Unavailable":
                unavailable = True

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


def get_all_products(save_path: str | Path = "products_all.json"):
    """
    主功能：
    - 抓分類頁
    - 以每個商品連結為單位抽全部商品
    - 存成 JSON 檔
    - 回傳 list[dict]
    """
    session = create_session()
    soup = fetch_category_soup(session)
    products = extract_products_from_soup(soup)

    save_path = Path(save_path)
    save_path.write_text(
        json.dumps(products, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[INFO] Saved {len(products)} products to {save_path}")

    return products


def main():
    products = get_all_products()

    print("\n=== SAMPLE (first 10) ===")
    for p in products[:10]:
        print(
            f"- {p['name']} | {p['color']} | {p['price']} | "
            f"unavail={p['unavailable']} | is_bag={p['is_bag']}"
        )


if __name__ == "__main__":
    main()
