#!/usr/bin/env python3
"""
amazon_depts_gh.py — Amazon Egypt Department Scraper (GitHub Actions edition)
يسكرب department browsing pages اللي بتيجي 503 محلياً بسبب الـ IP

Usage:
  python amazon_depts_gh.py --dept_index 0
  python amazon_depts_gh.py --dept_index 5 --limit 99999
"""
import argparse
import json
import re
import time
import random
import sys
from datetime import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "curl_cffi", "-q"])
    from curl_cffi import requests as curl_requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "beautifulsoup4", "lxml", "-q"])
    from bs4 import BeautifulSoup

# ── 30 Departments ────────────────────────────────────────────────────────────
DEPARTMENTS = [
    ("sporting-goods",       "Sporting Goods"),       # 0
    ("electronics",          "Electronics"),           # 1
    ("computers",            "Computers"),             # 2
    ("mobile-phones",        "Mobile Phones"),         # 3
    ("clothing",             "Clothing"),              # 4
    ("home-kitchen",         "Home & Kitchen"),        # 5
    ("beauty",               "Beauty"),                # 6
    ("toys",                 "Toys & Games"),          # 7
    ("books",                "Books"),                 # 8
    ("automotive",           "Automotive"),            # 9
    ("office",               "Office Supplies"),       # 10
    ("pet-supplies",         "Pet Supplies"),          # 11
    ("grocery",              "Grocery"),               # 12
    ("camera",               "Camera & Photo"),        # 13
    ("video-games",          "Video Games"),           # 14
    ("furniture",            "Furniture"),             # 15
    ("musical-instruments",  "Musical Instruments"),   # 16
    ("health",               "Health & Personal Care"),# 17
    ("baby",                 "Baby Products"),         # 18
    ("industrial",           "Industrial"),            # 19
    ("arts-crafts",          "Arts & Crafts"),         # 20
    ("luggage",              "Luggage & Bags"),        # 21
    ("garden",               "Garden & Outdoors"),     # 22
    ("tools",                "Tools & Hardware"),      # 23
    ("shoes",                "Shoes"),                 # 24
    ("watches",              "Watches"),               # 25
    ("jewelry",              "Jewelry"),               # 26
    ("handmade",             "Handmade"),              # 27
    ("software",             "Software"),              # 28
    ("wine",                 "Wine"),                  # 29
]

# Price ranges: (label, min_str, max_str) — Amazon uses cents (x100)
PRICE_RANGES = [
    ("0-500",    "0",    "500"),
    ("500-1000", "500",  "1000"),
    ("1000-2000","1000", "2000"),
    ("2000-5000","2000", "5000"),
    ("5000+",    "5000", ""),
]

MAX_PAGES = 10
BASE_URL  = "https://www.amazon.eg"


def make_session():
    s = curl_requests.Session(impersonate="chrome131")
    s.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
    })
    return s


def build_url(dept_slug, pmin, pmax, page):
    url = f"{BASE_URL}/s?i={dept_slug}"
    if pmin or pmax:
        rh = f"p_36%3A{pmin}00"
        if pmax:
            rh += f"-{pmax}00"
        url += f"&rh={rh}"
    if page > 1:
        url += f"&page={page}"
    return url


def parse_page(html, dept_name, dept_slug):
    soup = BeautifulSoup(html, "lxml")
    products = []
    seen = set()

    for card in soup.select("[data-asin]"):
        asin = card.get("data-asin", "").strip()
        if not asin or len(asin) < 10 or asin in seen:
            continue
        seen.add(asin)

        # Title
        title_el = card.select_one("h2 a span")
        title = title_el.get_text(strip=True) if title_el else f"Product {asin}"

        # Price
        price_els = card.select(".a-price .a-offscreen")
        current_price  = price_els[0].get_text(strip=True) if len(price_els) > 0 else ""
        original_price = price_els[1].get_text(strip=True) if len(price_els) > 1 else ""

        # Discount
        discount_el = card.select_one(".a-color-price, .savingsPercentage")
        discount = discount_el.get_text(strip=True) if discount_el else ""
        if "%" not in discount:
            discount = ""

        # Rating / Reviews
        rating_el = card.select_one(".a-icon-alt")
        rating = ""
        if rating_el:
            m = re.match(r"([\d.]+)", rating_el.get_text(strip=True))
            rating = m.group(1) if m else ""

        reviews_el = card.select_one(".s-underline-text")
        reviews = reviews_el.get_text(strip=True).replace(",", "") if reviews_el else ""

        # Image
        img_el = card.select_one("img.s-image")
        image = img_el.get("src", "") if img_el else ""

        # URL
        link_el = card.select_one("h2 a")
        if link_el and link_el.get("href"):
            product_url = BASE_URL + link_el["href"]
        else:
            product_url = f"{BASE_URL}/dp/{asin}"

        # Brand
        brand_el = card.select_one(".a-size-base-plus.a-color-base")
        brand = brand_el.get_text(strip=True) if brand_el else ""

        products.append({
            "platform":       "amazon",
            "product_id":     asin,
            "title":          title,
            "brand":          brand,
            "category":       dept_name,
            "current_price":  current_price,
            "original_price": original_price,
            "discount":       discount,
            "rating":         rating,
            "reviews_count":  reviews,
            "availability":   "In Stock",
            "main_image":     image,
            "product_url":    product_url,
            "scraped_at":     datetime.utcnow().isoformat(),
        })

    return products


def has_next_page(html):
    return bool(
        re.search(r'aria-label="Next page"', html)
        or 's-pagination-next"' in html
    )


def scrape_dept(dept_index, limit):
    dept_slug, dept_name = DEPARTMENTS[dept_index]
    session = make_session()
    all_products = []
    seen_asins = set()
    total = 0
    req_count = 0

    print(f"Starting dept #{dept_index}: {dept_name} ({dept_slug})", flush=True)

    for price_label, pmin, pmax in PRICE_RANGES:
        if total >= limit:
            break

        for page in range(1, MAX_PAGES + 1):
            if total >= limit:
                break

            url = build_url(dept_slug, pmin, pmax, page)
            req_count += 1

            # Refresh session every 20 requests
            if req_count % 20 == 0:
                session = make_session()

            resp = None
            for attempt in range(3):
                try:
                    resp = session.get(url, timeout=30)
                    if resp.status_code == 200:
                        break
                    elif resp.status_code in (429, 503):
                        wait = random.uniform(25, 55)
                        print(f"  [{resp.status_code}] {dept_name} price:{price_label} p:{page} — backoff {wait:.0f}s", flush=True)
                        time.sleep(wait)
                        session = make_session()
                    else:
                        print(f"  [{resp.status_code}] {dept_name} — stopping this price range", flush=True)
                        resp = None
                        break
                except Exception as e:
                    print(f"  Error: {e}", flush=True)
                    time.sleep(random.uniform(5, 15))

            if resp is None or resp.status_code != 200:
                break

            products = parse_page(resp.text, dept_name, dept_slug)
            new_count = 0
            for p in products:
                if p["product_id"] not in seen_asins:
                    seen_asins.add(p["product_id"])
                    all_products.append(p)
                    new_count += 1
                    total += 1

            print(f"  {dept_name} | {price_label} | p{page} → +{new_count} (total:{total})", flush=True)

            if not products or not has_next_page(resp.text):
                break

            # Smart delay: occasional burst
            if random.random() < 0.2:
                time.sleep(random.uniform(0.1, 0.4))
            else:
                time.sleep(random.uniform(1.5, 3.5))

    print(f"\nDone: {dept_name} → {total} unique products", flush=True)
    return all_products


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dept_index", type=int, required=True,
                        help="Department index 0-29")
    parser.add_argument("--limit", type=int, default=99999,
                        help="Max products to collect")
    args = parser.parse_args()

    if args.dept_index < 0 or args.dept_index >= len(DEPARTMENTS):
        print(f"Error: dept_index must be 0-{len(DEPARTMENTS)-1}", flush=True)
        sys.exit(1)

    products = scrape_dept(args.dept_index, args.limit)

    out_file = f"products_{args.dept_index}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(products)} products → {out_file}", flush=True)


if __name__ == "__main__":
    main()
