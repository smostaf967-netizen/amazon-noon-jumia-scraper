"""
Noon Egypt Scraper — GitHub Actions edition
Playwright headless Chromium + RSC payload parsing

Usage:
  python noon_spider.py --group_index 0 --limit 5000
"""
import argparse
import json
import re
import time
import random
import os
from datetime import datetime

PRODUCT_LIMIT = 5000  # per group — 43 groups × 5000 = up to 215,000 products

# Sort orders — كل sort بيجيب منتجات مختلفة
NOON_SORT_ORDERS = [
    ("popularity",  ""),                          # default
    ("newest",      "?sortBy=new_arrivals"),
    ("price_low",   "?sortBy=price_low"),
    ("price_high",  "?sortBy=price_high"),
]

# ── 43 groups covering all major Noon Egypt categories ────────────────────────
CATEGORY_GROUPS = {
    # Electronics
    "0":  ("Smartphones",           "https://www.noon.com/egypt-en/electronics-and-mobiles/mobiles-and-accessories/mobiles-20905/smartphones/"),
    "1":  ("Laptops",               "https://www.noon.com/egypt-en/electronics-and-mobiles/computers-and-accessories/laptops/"),
    "2":  ("Tablets",               "https://www.noon.com/egypt-en/electronics-and-mobiles/computers-and-accessories/tablets/"),
    "3":  ("TVs",                   "https://www.noon.com/egypt-en/electronics-and-mobiles/television-and-video/televisions/"),
    "4":  ("Audio & Headphones",    "https://www.noon.com/egypt-en/electronics-and-mobiles/audio-and-headphones/"),
    "5":  ("Cameras",               "https://www.noon.com/egypt-en/electronics-and-mobiles/cameras-and-accessories/"),
    "6":  ("Video Games",           "https://www.noon.com/egypt-en/electronics-and-mobiles/video-games/"),
    "7":  ("Wearables",             "https://www.noon.com/egypt-en/electronics-and-mobiles/wearable-technology/"),
    "8":  ("Computer Accessories",  "https://www.noon.com/egypt-en/electronics-and-mobiles/computers-and-accessories/computer-accessories/"),
    "9":  ("Networking",            "https://www.noon.com/egypt-en/electronics-and-mobiles/computers-and-accessories/networking/"),
    "10": ("Smart Home",            "https://www.noon.com/egypt-en/electronics-and-mobiles/smart-home-and-security/"),
    # Home & Kitchen
    "11": ("Large Appliances",      "https://www.noon.com/egypt-en/home-and-kitchen/large-appliances/"),
    "12": ("Home Appliances",       "https://www.noon.com/egypt-en/home-and-kitchen/home-appliances/"),
    "13": ("Kitchen & Dining",      "https://www.noon.com/egypt-en/home-and-kitchen/kitchen-and-dining/"),
    "14": ("Furniture",             "https://www.noon.com/egypt-en/home-and-kitchen/home-decor-and-furniture/furniture/"),
    "15": ("Lighting",              "https://www.noon.com/egypt-en/home-and-kitchen/home-decor-and-furniture/lighting-and-studio/"),
    "16": ("Home Decor",            "https://www.noon.com/egypt-en/home-and-kitchen/home-decor-and-furniture/"),
    "17": ("Bedding & Bath",        "https://www.noon.com/egypt-en/home-and-kitchen/bedding-and-bath/"),
    "18": ("Tools & Hardware",      "https://www.noon.com/egypt-en/home-and-kitchen/tools-and-hardware/"),
    # Sports, Baby, Toys
    "19": ("Sports & Outdoors",     "https://www.noon.com/egypt-en/sports-and-outdoors/"),
    "20": ("Baby Products",         "https://www.noon.com/egypt-en/baby-and-toys/baby/"),
    "21": ("Toys & Games",          "https://www.noon.com/egypt-en/baby-and-toys/toys-and-games/"),
    # Beauty & Health
    "22": ("Beauty",                "https://www.noon.com/egypt-en/beauty-and-health/beauty/"),
    "23": ("Skincare",              "https://www.noon.com/egypt-en/beauty-and-health/beauty/skincare/"),
    "24": ("Haircare",              "https://www.noon.com/egypt-en/beauty-and-health/beauty/haircare/"),
    "25": ("Makeup",                "https://www.noon.com/egypt-en/beauty-and-health/beauty/makeup/"),
    "26": ("Fragrances",            "https://www.noon.com/egypt-en/beauty-and-health/beauty/fragrances/"),
    "27": ("Health & Personal Care","https://www.noon.com/egypt-en/beauty-and-health/health/"),
    # Fashion
    "28": ("Men's Fashion",         "https://www.noon.com/egypt-en/fashion/eg-fashion-men-cat/"),
    "29": ("Men's Shoes",           "https://www.noon.com/egypt-en/fashion/eg-fashion-men-shoes-cat/"),
    "30": ("Men's Watches",         "https://www.noon.com/egypt-en/fashion/eg-fashion-men-watches-cat/"),
    "31": ("Women's Fashion",       "https://www.noon.com/egypt-en/fashion/eg-fashion-women-cat/"),
    "32": ("Women's Shoes",         "https://www.noon.com/egypt-en/fashion/eg-fashion-women-shoes-cat/"),
    "33": ("Women's Bags",          "https://www.noon.com/egypt-en/fashion/eg-fashion-women-bags-cat/"),
    "34": ("Women's Watches",       "https://www.noon.com/egypt-en/fashion/eg-fashion-women-watches-cat/"),
    "35": ("Lingerie",              "https://www.noon.com/egypt-en/fashion/eg-fashion-women-lingerie-cat/"),
    "36": ("Kids Fashion",          "https://www.noon.com/egypt-en/fashion/eg-fashion-kids-cat/"),
    "37": ("Sunglasses",            "https://www.noon.com/egypt-en/fashion/eg-fashion-sunglasses-cat/"),
    # Other
    "38": ("Grocery",               "https://www.noon.com/egypt-en/grocery/"),
    "39": ("Automotive",            "https://www.noon.com/egypt-en/automotive/"),
    "40": ("Stationery & Office",   "https://www.noon.com/egypt-en/stationery-and-office-supplies/"),
    "41": ("Mobile Accessories",    "https://www.noon.com/egypt-en/electronics-and-mobiles/mobiles-and-accessories/mobile-accessories/"),
    "42": ("Pet Supplies",          "https://www.noon.com/egypt-en/pet-supplies/"),
}


# ── RSC decoder ───────────────────────────────────────────────────────────────

def _decode_rsc(html):
    chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
    combined = "\n".join(chunks)
    try:
        return combined.encode().decode("unicode_escape")
    except Exception:
        return combined


def _extract_json_array(text, start_idx):
    depth, in_string, escape = 0, False, False
    i = start_idx
    while i < len(text):
        c = text[i]
        if escape:
            escape = False
        elif c == "\\":
            escape = True
        elif c == '"' and not escape:
            in_string = not in_string
        elif not in_string:
            if c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
                if depth == 0:
                    return text[start_idx: i + 1]
        i += 1
    return None


def extract_hits(html):
    decoded = _decode_rsc(html)
    m = re.search(r'"hits"\s*:\s*\[', decoded)
    if not m:
        return []
    arr_start = decoded.index("[", m.start())
    arr_str = _extract_json_array(decoded, arr_start)
    if not arr_str:
        return []
    try:
        hits = json.loads(arr_str)
        return hits if isinstance(hits, list) else []
    except Exception:
        return []


# ── Product builder ───────────────────────────────────────────────────────────

def build_product(item, cat_name):
    sku  = str(item.get("sku") or "").strip()
    name = str(item.get("name") or "").strip()
    if not sku or not name:
        return None

    sale_price = item.get("sale_price")
    orig_price = item.get("price")
    discount   = ""
    if sale_price and orig_price:
        try:
            sp, op = float(sale_price), float(orig_price)
            if op > sp > 0:
                discount = f"{round((1 - sp / op) * 100)}%"
        except Exception:
            pass

    image = item.get("image_url") or ""
    slug  = item.get("url") or sku

    return {
        "platform":        "noon",
        "sku":             sku,
        "title":           name,
        "brand":           str(item.get("brand") or ""),
        "category":        str(item.get("category_name") or cat_name),
        "current_price":   f"{sale_price} EGP" if sale_price else "",
        "original_price":  f"{orig_price} EGP" if orig_price and str(orig_price) != str(sale_price) else "",
        "discount":        discount,
        "rating":          str(item.get("avg_rating") or ""),
        "reviews_count":   str(item.get("review_count") or ""),
        "availability":    "In Stock" if item.get("is_buyable", True) else "Out of Stock",
        "seller":          str(item.get("seller_name") or "Noon"),
        "noon_express":    "Yes" if item.get("is_noon_express") else "",
        "main_image":      image,
        "all_images":      [image] if image else [],
        "product_url":     f"https://www.noon.com/egypt-en/{slug}/p/?o={sku}",
        "scraped_at":      datetime.now().isoformat(),
    }


# ── Page scraper (Playwright) ─────────────────────────────────────────────────

def _scrape_page(page, url, cat_name):
    from playwright.sync_api import TimeoutError as PWTimeout
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45_000)
    except PWTimeout:
        print(f"  [TIMEOUT] {url}")
    except Exception as e:
        print(f"  [NAV ERROR] {e}")
        return []

    # Wait for content and scroll to trigger lazy loading
    time.sleep(3)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
    time.sleep(1)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(2)

    html = page.content()
    hits = extract_hits(html)

    if not hits:
        body_text = page.evaluate("document.body.innerText")
        if "could not find" in body_text.lower() or "went wrong" in body_text.lower():
            print("  [WARN] Noon returned error page")

    return [build_product(h, cat_name) for h in hits if isinstance(h, dict)]


# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape_group(group_index, limit=PRODUCT_LIMIT):
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    group_key = str(group_index)
    if group_key not in CATEGORY_GROUPS:
        print(f"Unknown group_index: {group_index}")
        return []

    cat_name, base_url = CATEGORY_GROUPS[group_key]
    print(f"\n[Group {group_index}] '{cat_name}'  limit={limit}")

    all_products = []
    seen         = set()

    stealth = Stealth(
        navigator_webdriver=True,
        chrome_runtime=True,
        webgl_vendor=True,
        navigator_languages=True,
        navigator_platform=True,
        navigator_plugins=True,
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,   # False = headed (bypass Cloudflare headless detection)
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
            ],
        )
        ctx = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="Africa/Cairo",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            },
        )
        page = ctx.new_page()
        stealth.apply_stealth_sync(page)

        # Warm up session on homepage
        print("  Warming up on homepage...")
        try:
            page.goto("https://www.noon.com/egypt-en/", wait_until="domcontentloaded", timeout=30_000)
            time.sleep(random.uniform(2, 3))
            print("  Warmup OK")
        except Exception as e:
            print(f"  Warmup failed: {e} (continuing)")

        # Loop through sort orders
        for sort_name, sort_suffix in NOON_SORT_ORDERS:
            if len(all_products) >= limit:
                break

            sort_url = base_url + sort_suffix
            print(f"\n  [Sort: {sort_name}]")
            sort_count = 0

            for page_num in range(1, 200):
                if len(all_products) >= limit:
                    break

                sep = "&" if "?" in sort_suffix else "?"
                url = sort_url if page_num == 1 else f"{sort_url}{sep}page={page_num}"
                print(f"  Page {page_num}: {url}")

                raw = _scrape_page(page, url, cat_name)
                print(f"  RSC hits: {len(raw)}")

                new_count = 0
                for p in raw:
                    if p and p["sku"] not in seen and len(all_products) < limit:
                        seen.add(p["sku"])
                        all_products.append(p)
                        new_count += 1
                        sort_count += 1

                print(f"  +{new_count} new  (total {len(all_products)}/{limit})")

                if new_count == 0:
                    print(f"  Sort '{sort_name}' exhausted at page {page_num}")
                    break

                time.sleep(random.uniform(1.5, 3))

            print(f"  Sort '{sort_name}' done: {sort_count} new products")

        browser.close()

    print(f"[Group {group_index}] Done: {len(all_products)} products")
    return all_products


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group_index", type=int, required=True)
    parser.add_argument("--limit",       type=int, default=PRODUCT_LIMIT)
    args = parser.parse_args()

    products = scrape_group(args.group_index, args.limit)

    out = f"products_{args.group_index}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(products)} products -> {out}")


if __name__ == "__main__":
    main()
