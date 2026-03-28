"""
amazon_search_spider.py — Keyword Search Scraper
==================================================
يسكرب Amazon Egypt بكلمات بحث (keywords) عشان يكتشف منتجات
مش ظاهرة في الـ browse nodes أو الـ category groups.

كل keyword بيتسكرب بـ:
  - 9 price bands
  - 4 sort orders
  = 36 combination لكل keyword

Usage:
  python amazon_search_spider.py --keywords-file keywords.json --slice 0 --total-slices 5 --limit 99999
  python amazon_search_spider.py --keyword "samsung galaxy" --limit 500
"""

import argparse
import json
import re
import time
import random
import sys
from datetime import datetime
from pathlib import Path

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "curl-cffi", "-q"])
    from curl_cffi import requests as curl_requests

try:
    from scrapy import Selector
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scrapy", "-q"])
    from scrapy import Selector

# ─────────────────────────────────────────────────────────────────────────────
BASE      = "https://www.amazon.eg"
MAX_PAGES = 7    # Amazon search usually caps at 7 pages

SORT_ORDERS = [
    ("featured",   ""),
    ("price_asc",  "&s=price-asc-rank"),
    ("newest",     "&s=date-desc-rank"),
    ("reviews",    "&s=review-rank"),
]

PRICE_BANDS = [
    ("0-50",         0,       50),
    ("50-100",       50,      100),
    ("100-200",      100,     200),
    ("200-500",      200,     500),
    ("500-1000",     500,     1000),
    ("1000-2000",    1000,    2000),
    ("2000-5000",    2000,    5000),
    ("5000-10000",   5000,    10000),
    ("10000+",       10000,   None),
]

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# ─── Built-in keyword list ───────────────────────────────────────────────────

BRANDS = [
    "Samsung", "Apple", "Xiaomi", "Huawei", "Oppo", "Realme", "Nokia",
    "Sony", "LG", "Philips", "Panasonic", "Toshiba", "Sharp",
    "HP", "Dell", "Lenovo", "Asus", "Acer", "MSI",
    "Nike", "Adidas", "Puma", "New Balance", "Skechers",
    "L'Oreal", "Maybelline", "Nivea", "Dove", "Garnier",
    "Braun", "Oral-B", "Gillette",
    "Pampers", "Huggies", "Johnson",
    "Nescafe", "Lipton", "Nestle",
    "Canon", "Nikon", "GoPro",
    "JBL", "Anker", "Baseus", "Ugreen",
    "Bosch", "Black+Decker", "Karcher",
    "Casio", "Titan", "Fossil",
]

PRODUCT_TYPES = [
    "phone", "mobile", "laptop", "tablet", "TV", "headphones", "earbuds",
    "charger", "cable", "case", "screen protector", "power bank",
    "camera", "speaker", "keyboard", "mouse", "monitor",
    "washing machine", "refrigerator", "air conditioner", "microwave",
    "blender", "mixer", "iron", "vacuum cleaner",
    "shoes", "sneakers", "sandals", "watch", "bag", "backpack",
    "t-shirt", "dress", "jeans", "jacket",
    "perfume", "cream", "shampoo", "makeup", "lipstick",
    "toys", "games", "puzzle", "doll",
    "baby", "diaper", "stroller",
    "protein", "vitamins", "supplements",
    "tools", "drill", "screwdriver",
    "lamp", "curtain", "pillow", "mattress", "blanket",
    "coffee", "tea", "chocolate", "snacks",
    "cat food", "dog food", "pet",
    "book", "notebook", "pen",
    "car accessories", "dash cam", "car charger",
]

ARABIC_KEYWORDS = [
    "موبايل", "لابتوب", "تابلت", "شاشة", "سماعة", "شاحن",
    "ساعة", "حذاء", "شنطة", "عطر", "كريم", "شامبو",
    "غسالة", "ثلاجة", "تكييف", "مكنسة", "خلاط",
    "لعبة اطفال", "حفاضات", "عربة اطفال",
    "بروتين", "فيتامين", "مكمل غذائي",
    "مفرش", "وسادة", "مرتبة", "ستارة",
    "قهوة", "شاي", "شوكولاتة",
    "اكسسوارات سيارة", "كاميرا", "سبيكر",
]


def build_keyword_list():
    """Build comprehensive keyword list from brands x product types + Arabic."""
    keywords = []

    # Brand + product type combinations
    for brand in BRANDS:
        keywords.append(brand)
        for ptype in PRODUCT_TYPES[:15]:
            keywords.append(f"{brand} {ptype}")

    # Product types alone
    for ptype in PRODUCT_TYPES:
        keywords.append(ptype)

    # Arabic keywords
    keywords.extend(ARABIC_KEYWORDS)

    # Common search patterns
    extras = [
        "wireless", "bluetooth", "USB-C", "gaming", "smart",
        "original", "waterproof", "portable", "mini",
        "set", "kit", "bundle", "pack",
        "best seller", "new arrival", "trending",
        "offer", "deal", "sale",
    ]
    keywords.extend(extras)

    # Deduplicate
    seen = set()
    unique = []
    for kw in keywords:
        kw_lower = kw.lower().strip()
        if kw_lower and kw_lower not in seen:
            seen.add(kw_lower)
            unique.append(kw)
    return unique


# ─── Product extractor ───────────────────────────────────────────────────────

def extract_products(html, keyword):
    sel = Selector(text=html)
    products = []

    for card in sel.css("div[data-component-type='s-search-result']"):
        asin = card.attrib.get("data-asin", "").strip()
        if not asin:
            continue

        title = (
            card.css("h2 span.a-size-base-plus::text").get()
            or card.css("h2 span.a-size-medium::text").get()
            or card.css("h2 span::text").get()
            or ""
        ).strip()
        if not title:
            continue

        current_price = (
            card.css("span.a-price[data-a-size='xl'] span.a-offscreen::text").get()
            or card.css("span.a-price[data-a-size='l'] span.a-offscreen::text").get()
            or card.css("span.a-price span.a-offscreen::text").get()
            or ""
        ).strip()

        original_price = (
            card.css("span.a-price.a-text-price span.a-offscreen::text").get() or ""
        ).strip()
        if original_price == current_price:
            original_price = ""

        discount = (
            card.css("span.a-badge-text::text").get()
            or card.css("span.s-coupon-highlight-color::text").get()
            or ""
        ).strip()
        if "%" not in discount:
            discount = ""

        rating_raw = card.css("span.a-icon-alt::text").get(default="")
        m = re.search(r"([\d.]+)\s*out of", rating_raw)
        rating = m.group(1) if m else ""

        reviews = (
            card.css("span.a-size-base.s-underline-text::text").get()
            or card.css("a span.a-size-base::text").get()
            or ""
        ).strip()

        brand = (
            card.css("span.a-size-base-plus.a-color-base::text").get() or ""
        ).strip()

        image = card.css("img.s-image::attr(src)").get(default="")
        product_url = f"{BASE}/dp/{asin}" if asin else ""
        prime = "Yes" if card.css("i.a-icon-prime, span.s-prime").get() else ""

        products.append({
            "platform":       "amazon",
            "asin":           asin,
            "title":          title,
            "brand":          brand,
            "category":       keyword,
            "current_price":  current_price,
            "original_price": original_price,
            "discount":       discount,
            "rating":         rating,
            "reviews_count":  reviews,
            "prime_eligible": prime,
            "amazon_choice":  "",
            "sponsored":      "",
            "main_image":     image,
            "product_url":    product_url,
            "scraped_at":     datetime.now().isoformat(),
        })

    return products


# ─── Main scraper ────────────────────────────────────────────────────────────

def scrape_keyword(session, keyword, seen_asins, limit):
    new_products = []
    kw_encoded = keyword.replace(" ", "+")

    for band_name, price_min, price_max in PRICE_BANDS:
        if len(new_products) >= limit:
            break

        if price_max is None:
            price_param = f"&rh=p_36%3A{price_min}-"
        else:
            price_param = f"&rh=p_36%3A{price_min}-{price_max}"

        for sort_name, sort_param in SORT_ORDERS:
            if len(new_products) >= limit:
                break

            base_url = f"{BASE}/s?k={kw_encoded}{price_param}{sort_param}"

            for page in range(1, MAX_PAGES + 1):
                if len(new_products) >= limit:
                    break

                url = base_url if page == 1 else f"{base_url}&page={page}"

                try:
                    r = session.get(url, headers=HEADERS, timeout=45)
                except Exception as e:
                    print(f"    [{keyword}/{band_name}/{sort_name}] p{page}: ERROR {e}")
                    break

                if r.status_code == 503:
                    time.sleep(random.uniform(45, 90))
                    continue
                if r.status_code != 200:
                    break

                products = extract_products(r.text, keyword)
                if not products:
                    break

                new = 0
                for p in products:
                    if p["asin"] not in seen_asins and len(new_products) < limit:
                        seen_asins.add(p["asin"])
                        new_products.append(p)
                        new += 1

                if new == 0:
                    break

                time.sleep(random.uniform(4, 8))

    return new_products


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", default="",
                        help="Single keyword to search")
    parser.add_argument("--keywords-file", default="",
                        help="JSON file with keyword list")
    parser.add_argument("--slice", type=int, default=0,
                        help="Which slice of keywords (0-based)")
    parser.add_argument("--total-slices", type=int, default=5,
                        help="Total slices (accounts)")
    parser.add_argument("--limit", type=int, default=99999,
                        help="Max total products")
    args = parser.parse_args()

    # Build or load keyword list
    if args.keyword:
        keywords = [args.keyword]
    elif args.keywords_file and Path(args.keywords_file).exists():
        keywords = json.loads(Path(args.keywords_file).read_text(encoding="utf-8"))
    else:
        keywords = build_keyword_list()

    # Slice keywords for this account
    if args.total_slices > 1 and not args.keyword:
        my_keywords = [kw for i, kw in enumerate(keywords)
                       if i % args.total_slices == args.slice]
    else:
        my_keywords = keywords

    print(f"\n{'='*60}")
    print(f"  Amazon Keyword Search Scraper")
    print(f"  Slice: {args.slice}/{args.total_slices}")
    print(f"  Keywords: {len(my_keywords)} (out of {len(keywords)} total)")
    print(f"  Limit: {args.limit}")
    print(f"{'='*60}\n")

    session = curl_requests.Session(impersonate="chrome124")

    # Warmup
    try:
        session.get(f"{BASE}/", headers=HEADERS, timeout=20)
        time.sleep(random.uniform(4, 7))
    except Exception:
        pass

    all_products = []
    seen_asins = set()
    out_file = f"search_products_{args.slice}.json"

    for i, keyword in enumerate(my_keywords):
        if len(all_products) >= args.limit:
            break

        print(f"\n  [{i+1}/{len(my_keywords)}] Keyword: '{keyword}'", flush=True)
        before = len(all_products)

        new_prods = scrape_keyword(
            session, keyword, seen_asins, args.limit - len(all_products)
        )
        all_products.extend(new_prods)

        gained = len(all_products) - before
        if gained:
            print(f"    +{gained} new (total: {len(all_products)})", flush=True)

        # Save after EVERY keyword — zero data loss on timeout/cancellation
        Path(out_file).write_text(
            json.dumps(all_products, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        # Pause between keywords
        time.sleep(random.uniform(5, 10))

    print(f"\n{'='*60}")
    print(f"  DONE: {len(all_products)} unique products")
    print(f"  Saved: {out_file}")
    print(f"{'='*60}\n", flush=True)


if __name__ == "__main__":
    main()
