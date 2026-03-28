"""
Noon Egypt Scraper — GitHub Actions edition
curl_cffi + RSC payload parsing (NO Playwright needed)

Usage:
  python noon_spider.py --group_index 0 --limit 99999
"""
import argparse
import json
import re
import time
import random
import sys
import os
from datetime import datetime

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "curl_cffi", "-q"])
    from curl_cffi import requests as curl_requests

try:
    from scrapy import Selector
except ImportError:
    Selector = None

PRODUCT_LIMIT = 99999

# Sort orders
NOON_SORT_ORDERS = [
    ("popularity",  ""),
    ("newest",      "sortBy=new_arrivals"),
    ("price_low",   "sortBy=price_low"),
    ("price_high",  "sortBy=price_high"),
]

# 16 price bands in EGP
NOON_PRICE_BANDS = [
    ("all",          None,    None),
    ("0-25",         0,       25),
    ("25-50",        25,      50),
    ("50-100",       50,      100),
    ("100-150",      100,     150),
    ("150-200",      150,     200),
    ("200-300",      200,     300),
    ("300-500",      300,     500),
    ("500-750",      500,     750),
    ("750-1000",     750,     1000),
    ("1000-1500",    1000,    1500),
    ("1500-2000",    1500,    2000),
    ("2000-3500",    2000,    3500),
    ("3500-5000",    3500,    5000),
    ("5000-10000",   5000,    10000),
    ("10000+",       10000,   None),
]

# 43 groups covering all major Noon Egypt categories
CATEGORY_GROUPS = {
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
    "11": ("Large Appliances",      "https://www.noon.com/egypt-en/home-and-kitchen/large-appliances/"),
    "12": ("Home Appliances",       "https://www.noon.com/egypt-en/home-and-kitchen/home-appliances/"),
    "13": ("Kitchen & Dining",      "https://www.noon.com/egypt-en/home-and-kitchen/kitchen-and-dining/"),
    "14": ("Furniture",             "https://www.noon.com/egypt-en/home-and-kitchen/home-decor-and-furniture/furniture/"),
    "15": ("Lighting",              "https://www.noon.com/egypt-en/home-and-kitchen/home-decor-and-furniture/lighting-and-studio/"),
    "16": ("Home Decor",            "https://www.noon.com/egypt-en/home-and-kitchen/home-decor-and-furniture/"),
    "17": ("Bedding & Bath",        "https://www.noon.com/egypt-en/home-and-kitchen/bedding-and-bath/"),
    "18": ("Tools & Hardware",      "https://www.noon.com/egypt-en/home-and-kitchen/tools-and-hardware/"),
    "19": ("Sports & Outdoors",     "https://www.noon.com/egypt-en/sports-and-outdoors/"),
    "20": ("Baby Products",         "https://www.noon.com/egypt-en/baby-and-toys/baby/"),
    "21": ("Toys & Games",          "https://www.noon.com/egypt-en/baby-and-toys/toys-and-games/"),
    "22": ("Beauty",                "https://www.noon.com/egypt-en/beauty-and-health/beauty/"),
    "23": ("Skincare",              "https://www.noon.com/egypt-en/beauty-and-health/beauty/skincare/"),
    "24": ("Haircare",              "https://www.noon.com/egypt-en/beauty-and-health/beauty/haircare/"),
    "25": ("Makeup",                "https://www.noon.com/egypt-en/beauty-and-health/beauty/makeup/"),
    "26": ("Fragrances",            "https://www.noon.com/egypt-en/beauty-and-health/beauty/fragrances/"),
    "27": ("Health & Personal Care","https://www.noon.com/egypt-en/beauty-and-health/health/"),
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
        "platform":          "noon",
        "product_id":        sku,
        "title":             name,
        "brand":             str(item.get("brand") or ""),
        "category":          str(item.get("category_name") or cat_name),
        "current_price":     f"{sale_price} EGP" if sale_price else "",
        "original_price":    f"{orig_price} EGP" if orig_price and str(orig_price) != str(sale_price) else "",
        "discount":          discount,
        "rating":            str(item.get("avg_rating") or ""),
        "reviews_count":     str(item.get("review_count") or ""),
        "availability":      "In Stock" if item.get("is_buyable", True) else "Out of Stock",
        "seller":            str(item.get("seller_name") or "Noon"),
        "express_delivery":  "Yes" if item.get("is_noon_express") else "",
        "platform_badge":    "",
        "sponsored":         "",
        "main_image":        image,
        "all_images":        image,
        "description":       "",
        "weight":            "",
        "dimensions":        "",
        "model_number":      "",
        "country_of_origin": "",
        "warranty":          "",
        "tech_specs":        "",
        "variations":        "",
        "ships_from":        "",
        "delivery_date":     "",
        "product_url":       f"https://www.noon.com/egypt-en/{slug}/p/?o={sku}",
        "scraped_at":        datetime.now().isoformat(),
    }


# ── HTTP fetch with retry ────────────────────────────────────────────────────

def _fetch(session, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                return r.text
            if r.status_code == 403:
                wait = random.uniform(10, 20)
                print(f"    [403] Blocked — waiting {wait:.0f}s (attempt {attempt+1}/{max_retries})", flush=True)
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            print(f"    [HTTP {r.status_code}] Unexpected — retrying", flush=True)
            time.sleep(random.uniform(3, 6))
        except Exception as e:
            print(f"    [ERROR] {e} — retrying", flush=True)
            time.sleep(random.uniform(5, 10))
    return None


# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape_group(group_index, limit=PRODUCT_LIMIT):
    group_key = str(group_index)
    if group_key not in CATEGORY_GROUPS:
        print(f"Unknown group_index: {group_index}")
        return []

    cat_name, base_url = CATEGORY_GROUPS[group_key]
    print(f"\n[Group {group_index}] '{cat_name}'  limit={limit}", flush=True)

    all_products = []
    seen         = set()
    consecutive_empty_bands = 0

    session = curl_requests.Session(impersonate="chrome131")

    # Warmup: visit homepage first to establish session cookies
    print("  Warming up...", flush=True)
    try:
        session.get("https://www.noon.com/egypt-en/", timeout=20)
        time.sleep(random.uniform(1, 2))
        print("  Warmup OK", flush=True)
    except Exception as e:
        print(f"  Warmup failed: {e} (continuing)", flush=True)

    # Loop through price bands × sort orders
    for band_name, price_min, price_max in NOON_PRICE_BANDS:
        if len(all_products) >= limit:
            break

        if consecutive_empty_bands >= 4 and band_name != "all":
            print(f"\n  [SKIP] Skipping band '{band_name}' — {consecutive_empty_bands} consecutive empty bands", flush=True)
            continue

        # Build price filter
        if price_min is None:
            price_param = ""
        elif price_max is None:
            price_param = f"price[from]={price_min}"
        else:
            price_param = f"price[from]={price_min}&price[to]={price_max}"

        band_before = len(all_products)
        print(f"\n  [Price Band: {band_name} EGP]", flush=True)

        for sort_name, sort_param in NOON_SORT_ORDERS:
            if len(all_products) >= limit:
                break

            # Build URL params
            params = [p for p in [price_param, sort_param] if p]
            query_string = "&".join(params)

            sort_count = 0
            print(f"    [{band_name}/{sort_name}]", flush=True)

            for page_num in range(1, 200):
                if len(all_products) >= limit:
                    break

                # Build full URL
                page_params = list(params)
                if page_num > 1:
                    page_params.append(f"page={page_num}")
                full_query = "&".join(page_params)
                url = f"{base_url}?{full_query}" if full_query else base_url

                html = _fetch(session, url)
                if html is None:
                    print(f"    Page {page_num}: fetch failed — skipping", flush=True)
                    break

                hits = extract_hits(html)

                new_count = 0
                for h in hits:
                    if not isinstance(h, dict):
                        continue
                    p = build_product(h, cat_name)
                    if p and p["product_id"] not in seen and len(all_products) < limit:
                        seen.add(p["product_id"])
                        all_products.append(p)
                        new_count += 1
                        sort_count += 1

                print(f"    Page {page_num}: {len(hits)} hits, +{new_count} new (total {len(all_products)})", flush=True)

                if new_count == 0:
                    break

                # Delay between pages (shorter than Playwright — curl is fast)
                time.sleep(random.uniform(1.5, 3.5))

            print(f"    Sort '{sort_name}' done: {sort_count} new products", flush=True)

        band_new = len(all_products) - band_before
        print(f"  Band '{band_name}' done: {band_new} new products", flush=True)

        if band_new > 0:
            consecutive_empty_bands = 0
        else:
            consecutive_empty_bands += 1

    print(f"\n[Group {group_index}] Done: {len(all_products)} products", flush=True)
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
