from curl_cffi import requests

# 1. 這是你剛剛截圖找出來的完美後端 API (荷蘭區)
API_URL = "https://bck.hermes.com/products?urlParams=fh_view_size=48%26country=nl%26fh_refpath=41e19284-0e9f-4f5c-823d-352b3e9f6269%26fh_refview=lister%26fh_reffacet=display_name%26fh_location=%252f%252fcatalog01%252fen_US%252fis_visible%253e%257bnl%257d%252fis_searchable%253e%257bnl%257d%252fis_sellable%253e%257bnl%257d%252fhas_stock%253e%257bnl%257d%252fitem_type%253dproduct%252fcategories%253c%257bcatalog01_leathergoods_leathergoodsbagsandclutches_womenbagsandclutches%257d&category=WOMEN_BAGS_AND_CLUTCHES&sort=relevance&pagesize=48&locale=nl_en"

# 2. 準備對應的 NL 首頁與 Referer 以建立 Context Continuity
HOMEPAGE_URL = "https://www.hermes.com/nl/en/"
REFERER_URL = "https://www.hermes.com/nl/en/category/women/bags-and-small-leather-goods/bags-and-clutches/"

def test_hidden_api():
    # 建立帶有 iOS Safari 指紋的 Session
    session = requests.Session(impersonate="safari_ios", timeout=20)
    
    print("[1] 正在造訪首頁獲取初始 Cookie...")
    session.get(HOMEPAGE_URL)
    
    # 換上 API 專屬迷彩服 (XHR 背景請求特徵)
    api_headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.hermes.com",
        "Referer": REFERER_URL,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.6"
    }
    
    print("[2] 帶著完美 Context 直擊 bck.hermes.com API...")
    resp = session.get(API_URL, headers=api_headers)
    
    print("\n--- 測試結果 ---")
    print(f"HTTP Status: {resp.status_code}")
    
    if resp.status_code == 200:
        print("🎉 成功繞過防護！拿到了零延遲的純淨 JSON。商品節錄：")
        try:
            data = resp.json()
            items = data.get("products", {}).get("items", [])
            for i, item in enumerate(items[:3]):
                print(f" - {item.get('title')}")
        except Exception as e:
            print(f"JSON 解析錯誤: {e}")
    else:
        print("❌ 挑戰失敗。DataDome 依然擋下了請求。")
        print("Response 攔截特徵：")
        for k, v in resp.headers.items():
            if "datadome" in k.lower() or "cf-" in k.lower():
                print(f"   {k}: {v}")

if __name__ == "__main__":
    test_hidden_api()