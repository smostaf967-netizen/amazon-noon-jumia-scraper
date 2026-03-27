"""
Amazon Egypt Scraper — GitHub Actions edition
curl-cffi: mimics real Chrome TLS fingerprint (bypasses Amazon bot detection)

Usage:
  python amazon_spider.py --group_index 0 --limit 2000
"""
import argparse
import json
import re
import time
import random
from datetime import datetime

PRODUCT_LIMIT = 2000  # per group — 43 groups × 2000 = up to 86,000 products

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

# ── 43 groups covering all Amazon Egypt categories ────────────────────────────
CATEGORY_GROUPS = {
    # Electronics
    "0":  ("Mobiles",              "https://www.amazon.eg/s?k=mobile+phones&i=mobile"),
    "1":  ("Computers",           "https://www.amazon.eg/s?k=laptops+computers&i=computers"),
    "2":  ("Tablets",             "https://www.amazon.eg/s?k=tablets&i=computers"),
    "3":  ("Electronics",         "https://www.amazon.eg/s?k=electronics&i=electronics"),
    "4":  ("TVs",                 "https://www.amazon.eg/s?k=television+tv&i=electronics"),
    "5":  ("Cameras",             "https://www.amazon.eg/s?k=cameras+photography&i=photo"),
    "6":  ("Audio & Headphones",  "https://www.amazon.eg/s?k=headphones+speakers&i=electronics"),
    "7":  ("Smart Home",          "https://www.amazon.eg/s?k=smart+home&i=electronics"),
    "8":  ("Video Games",         "https://www.amazon.eg/s?k=video+games&i=videogames"),
    # Home & Kitchen
    "9":  ("Appliances",          "https://www.amazon.eg/s?k=home+appliances&i=appliances"),
    "10": ("Kitchen",             "https://www.amazon.eg/s?k=kitchen+products&i=kitchen"),
    "11": ("Furniture",           "https://www.amazon.eg/s?k=furniture+home&i=furniture"),
    "12": ("Bedding & Bath",      "https://www.amazon.eg/s?k=bedding+bath+towels&i=garden"),
    "13": ("Lighting",            "https://www.amazon.eg/s?k=lighting+lamps&i=lighting"),
    "14": ("Cleaning Supplies",   "https://www.amazon.eg/s?k=cleaning+supplies&i=grocery"),
    "15": ("Tools",               "https://www.amazon.eg/s?k=tools+hardware&i=tools"),
    "16": ("Garden",              "https://www.amazon.eg/s?k=garden+outdoor&i=garden"),
    # Fashion
    "17": ("Men's Fashion",       "https://www.amazon.eg/s?k=mens+clothing&i=fashion-mens-clothing"),
    "18": ("Men's Shoes",         "https://www.amazon.eg/s?k=mens+shoes&i=shoes"),
    "19": ("Women's Fashion",     "https://www.amazon.eg/s?k=womens+clothing&i=fashion-womens-clothing"),
    "20": ("Women's Shoes",       "https://www.amazon.eg/s?k=womens+shoes&i=shoes"),
    "21": ("Girls Fashion",       "https://www.amazon.eg/s?k=girls+clothing&i=fashion-girls-clothing"),
    "22": ("Boys Fashion",        "https://www.amazon.eg/s?k=boys+clothing&i=fashion-boys-clothing"),
    "23": ("Kids' Shoes",         "https://www.amazon.eg/s?k=kids+shoes&i=shoes"),
    "24": ("Luggage",             "https://www.amazon.eg/s?k=luggage+bags&i=luggage"),
    "25": ("Watches",             "https://www.amazon.eg/s?k=watches&i=watches"),
    "26": ("Sunglasses",          "https://www.amazon.eg/s?k=sunglasses+eyewear&i=fashion"),
    "27": ("Jewelry",             "https://www.amazon.eg/s?k=jewelry&i=jewelry"),
    # Beauty & Health
    "28": ("Beauty",              "https://www.amazon.eg/s?k=beauty+skincare&i=beauty"),
    "29": ("Fragrances",          "https://www.amazon.eg/s?k=fragrances+perfume&i=beauty"),
    "30": ("Health & Personal",   "https://www.amazon.eg/s?k=health+personal+care&i=hpc"),
    "31": ("Vitamins",            "https://www.amazon.eg/s?k=vitamins+supplements&i=hpc"),
    # Sports, Toys, Baby
    "32": ("Sports",              "https://www.amazon.eg/s?k=sports+fitness&i=sporting-goods"),
    "33": ("Outdoor & Camping",   "https://www.amazon.eg/s?k=outdoor+camping&i=sporting-goods"),
    "34": ("Toys & Games",        "https://www.amazon.eg/s?k=toys+games&i=toys-and-games"),
    "35": ("Baby Products",       "https://www.amazon.eg/s?k=baby+products&i=baby-products"),
    # Other
    "36": ("Books",               "https://www.amazon.eg/s?k=books&i=stripbooks"),
    "37": ("Automotive",          "https://www.amazon.eg/s?k=automotive+car&i=automotive"),
    "38": ("Office Products",     "https://www.amazon.eg/s?k=office+products&i=office-products"),
    "39": ("Pet Supplies",        "https://www.amazon.eg/s?k=pet+supplies&i=pet-supplies"),
    "40": ("Grocery",             "https://www.amazon.eg/s?k=grocery+food&i=grocery"),
    "41": ("Musical Instruments", "https://www.amazon.eg/s?k=musical+instruments&i=musical-instruments"),
    "42": ("Movies & TV",         "https://www.amazon.eg/s?k=movies+tv&i=movies-tv"),
}


# ── Product extractor from listing page HTML ──────────────────────────────────

def extract_products(html, cat_name):
    """Parse Amazon listing page and return list of product dicts."""
    from scrapy import Selector
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
            card.css("span.a-size-base-plus.a-color-base::text").get()
            or ""
        ).strip()

        image = card.css("img.s-image::attr(src)").get(default="")

        href = card.css("h2 a.a-link-normal::attr(href)").get(default="")
        if href and not href.startswith("http"):
            href = "https://www.amazon.eg" + href
        product_url = f"https://www.amazon.eg/dp/{asin}" if asin else href

        prime = "Yes" if card.css("i.a-icon-prime, span.s-prime").get() else ""
        amazon_choice = "Yes" if card.css("span.s-label-popover-default").get() else ""
        sponsored = "Yes" if card.css("span[data-component-type='s-sponsored-label-info-icon']").get() else ""

        products.append({
            "platform":       "amazon",
            "asin":           asin,
            "title":          title,
            "brand":          brand,
            "category":       cat_name,
            "current_price":  current_price,
            "original_price": original_price,
            "discount":       discount,
            "rating":         rating,
            "reviews_count":  reviews,
            "prime_eligible": prime,
            "amazon_choice":  amazon_choice,
            "sponsored":      sponsored,
            "main_image":     image,
            "product_url":    product_url,
            "scraped_at":     datetime.now().isoformat(),
        })

    return products


# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape_group(group_index, limit=PRODUCT_LIMIT):
    from curl_cffi import requests as curl_requests
    from scrapy import Selector

    group_key = str(group_index)
    if group_key not in CATEGORY_GROUPS:
        print(f"Unknown group_index: {group_index}")
        return []

    cat_name, base_url = CATEGORY_GROUPS[group_key]
    print(f"\n[Group {group_index}] '{cat_name}'  limit={limit}")

    all_products = []
    seen_asins   = set()

    # curl-cffi session impersonating Chrome 124 — real TLS fingerprint
    session = curl_requests.Session(impersonate="chrome124")

    # Warm up on homepage
    print("  Warming up on homepage...")
    try:
        r = session.get("https://www.amazon.eg/", headers=HEADERS, timeout=30)
        print(f"  Warmup OK — status {r.status_code}")
        time.sleep(random.uniform(2, 3))
    except Exception as e:
        print(f"  Warmup failed: {e} (continuing)")

    # Paginate through listing pages
    for page_num in range(1, 400):
        if len(all_products) >= limit:
            break

        url = base_url if page_num == 1 else f"{base_url}&page={page_num}"
        print(f"  Page {page_num}: {url}")

        try:
            r = session.get(url, headers=HEADERS, timeout=45)
        except Exception as e:
            print(f"  [ERROR] {e}")
            break

        print(f"  Status: {r.status_code} | HTML len: {len(r.text)}")

        if r.status_code != 200:
            print(f"  Non-200 response — stopping")
            break

        html     = r.text
        products = extract_products(html, cat_name)
        print(f"  Found {len(products)} cards")

        new_count = 0
        for p in products:
            if len(all_products) >= limit:
                break
            if p["asin"] not in seen_asins:
                seen_asins.add(p["asin"])
                all_products.append(p)
                new_count += 1

        print(f"  +{new_count} new  (total {len(all_products)}/{limit})")

        if new_count == 0:
            if page_num == 1:
                print("  Page 1 returned 0 products — may be blocked or empty.")
            else:
                print(f"  No new products on page {page_num} — category exhausted.")
            break

        time.sleep(random.uniform(2, 4))

    print(f"\n[Group {group_index}] Done: {len(all_products)} products")
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
