from pathlib import Path
from bs4 import BeautifulSoup

def main():
    html_path = Path("debug.html")
    if not html_path.exists():
        print("debug.html not found, run main.py once first.")
        return

    html = html_path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")

    text = soup.get_text("\n")
    lines = [l.strip() for l in text.splitlines()]
    lines = [l for l in lines if l]

    # 找 Product list 區段
    try:
        start = lines.index("Product list")
    except ValueError:
        print("Product list not found")
        return

    end = len(lines)
    for i in range(start + 1, len(lines)):
        if lines[i].startswith("On the Shoulder") or "Load more items" in lines[i]:
            end = i
            break

    product_lines = lines[start + 1 : end]

    print("===== DUMP PRODUCTS WITH UNAVAILABLE PATTERN =====")
    i = 0
    n = len(product_lines)
    while i < n:
        # pattern：
        # i:   name
        # i+1: ','
        # i+2: 'Color'
        # i+3: ':'
        # i+4: color
        # i+5: ','
        # i+6: '€xxxx'
        if (
            i + 6 < n
            and product_lines[i + 1] == ","
            and product_lines[i + 2] == "Color"
            and product_lines[i + 3] == ":"
        ):
            name = product_lines[i]
            color = product_lines[i + 4]
            price = product_lines[i + 6]

            prev_unavail = (i - 1 >= 0 and product_lines[i - 1] == "Unavailable")

            # 往後找到下一個商品開始，同時看這段內有沒有 Unavailable
            post_unavail = False
            k = i + 7
            while k < n:
                if (
                    k + 6 < n
                    and product_lines[k + 1] == ","
                    and product_lines[k + 2] == "Color"
                    and product_lines[k + 3] == ":"
                ):
                    break
                if product_lines[k] == "Unavailable":
                    post_unavail = True
                k += 1

            print("----")
            print(f"NAME      : {name}")
            print(f"COLOR     : {color}")
            print(f"PRICE     : {price}")
            print(f"prev_unav : {prev_unavail}")
            print(f"post_unav : {post_unavail}")
            print(f"FLAG(AND) : {prev_unavail and post_unavail}")
            print(f"FLAG(OR)  : {prev_unavail or post_unavail}")

            i = k
        else:
            i += 1

if __name__ == "__main__":
    main()
