"""
amazon_node_spider.py — Phase 3
================================
يسكراب Amazon Egypt browse node واحد بـ 4 sort orders.
كل sort → 20 page × 20 product = 400 product.
بعد dedup بالـ ASIN: يطلع ~400-1600 منتج فريد.

Usage:
  python amazon_node_spider.py --node_id 21832868031 --node_name "Mobile Phones" --limit 99999
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
MAX_PAGES = 20   # Amazon hard cap ~20 pages per sort

SORT_ORDERS = [
    ("featured",   ""),
    ("price_asc",  "&s=price-asc-rank"),
    ("newest",     "&s=date-desc-rank"),
    ("reviews",    "&s=review-rank"),
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


# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT EXTRACTOR (reuses logic from amazon_spider.py)
# ─────────────────────────────────────────────────────────────────────────────

def extract_products(html, category):
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

        href = card.css("h2 a.a-link-normal::attr(href)").get(default="")
        if href and not href.startswith("http"):
            href = BASE + href
        product_url = f"{BASE}/dp/{asin}" if asin else href

        prime         = "Yes" if card.css("i.a-icon-prime, span.s-prime").get() else ""
        amazon_choice = "Yes" if card.css("span.s-label-popover-default").get() else ""
        sponsored     = "Yes" if card.css("span[data-component-type='s-sponsored-label-info-icon']").get() else ""

        products.append({
            "platform":       "amazon",
            "asin":           asin,
            "title":          title,
            "brand":          brand,
            "category":       category,
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


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_node(node_id, node_name, limit):
    session     = curl_requests.Session(impersonate="chrome124")
    all_products = []
    seen_asins  = set()

    print(f"\n{'='*60}")
    print(f"  Node: {node_id}  ({node_name})")
    print(f"  Limit: {limit}")
    print(f"{'='*60}\n")

    # Warm up
    try:
        session.get(f"{BASE}/", headers=HEADERS, timeout=20)
        time.sleep(random.uniform(2, 4))
    except Exception:
        pass

    for sort_name, sort_param in SORT_ORDERS:
        if len(all_products) >= limit:
            break

        base_url = f"{BASE}/s?bbn={node_id}&rh=n%3A{node_id}{sort_param}"
        print(f"\n  [Sort: {sort_name}]  {base_url}")
        sort_count = 0

        for page in range(1, MAX_PAGES + 1):
            if len(all_products) >= limit:
                break

            url = base_url if page == 1 else f"{base_url}&page={page}"

            try:
                r = session.get(url, headers=HEADERS, timeout=45)
            except Exception as e:
                print(f"    Page {page}: ERROR {e}")
                break

            if r.status_code == 503:
                print(f"    Page {page}: 503 — backing off 30s")
                time.sleep(30)
                continue
            if r.status_code != 200:
                print(f"    Page {page}: HTTP {r.status_code} — stopping sort")
                break

            products = extract_products(r.text, node_name)
            if not products:
                print(f"    Page {page}: 0 cards — end of sort")
                break

            new = 0
            for p in products:
                if p["asin"] not in seen_asins and len(all_products) < limit:
                    seen_asins.add(p["asin"])
                    all_products.append(p)
                    new += 1
                    sort_count += 1

            print(f"    Page {page}: {len(products)} cards, {new} new  (total={len(all_products)})")

            if new == 0:
                print(f"    No new ASINs — moving to next sort")
                break

            time.sleep(random.uniform(1.5, 3.0))

        print(f"  Sort '{sort_name}' done: {sort_count} new products")

    return all_products


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_id",   required=True)
    parser.add_argument("--node_name", default="")
    parser.add_argument("--limit",     type=int, default=99999)
    args = parser.parse_args()

    products = scrape_node(args.node_id, args.node_name or args.node_id, args.limit)

    out_file = f"node_{args.node_id}.json"
    Path(out_file).write_text(
        json.dumps(products, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"\n  Saved {len(products)} products → {out_file}")


if __name__ == "__main__":
    main()
