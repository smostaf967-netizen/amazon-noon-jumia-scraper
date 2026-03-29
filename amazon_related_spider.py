"""
Amazon Egypt — Related Products Spider
Visits product pages, extracts product info + discovers related ASINs.
Uses checkpoint system to resume across runs (GitHub Actions 6h limit).

Usage:
  python amazon_related_spider.py --slice 0 --total-slices 4 --limit 99999
"""
import argparse
import csv
import json
import re
import sys
import time
import random
from datetime import datetime
from pathlib import Path

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "curl_cffi", "-q"])
    from curl_cffi import requests as curl_requests

try:
    from scrapy import Selector
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scrapy", "-q"])
    from scrapy import Selector

BASE = "https://www.amazon.eg"
PRODUCT_LIMIT = 99999
MAX_RUNTIME = 19800  # 5.5 hours in seconds (leave 30 min for export+push)
PAGE_DELAY = (2.5, 4.5)  # Safe delays — fast enough but won't get blocked

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


def fetch(session, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r.text
            if r.status_code == 503:
                time.sleep(random.uniform(15, 30))
                continue
            if r.status_code == 404:
                return None
            time.sleep(random.uniform(3, 6))
        except Exception:
            time.sleep(random.uniform(5, 10))
    return None


def extract_from_product_page(html, asin):
    """Extract product info + related ASINs from product page."""
    sel = Selector(text=html)

    title = (sel.css("#productTitle::text").get() or "").strip()

    current_price = (
        sel.css("span.a-price .a-offscreen::text").get()
        or sel.css("#priceblock_ourprice::text").get()
        or ""
    ).strip()

    original_price = (
        sel.css("span.a-price.a-text-price .a-offscreen::text").get() or ""
    ).strip()
    if original_price == current_price:
        original_price = ""

    brand = (
        sel.css("#bylineInfo::text").get() or ""
    ).strip().replace("Visit the ", "").replace(" Store", "")

    rating_raw = sel.css("span.a-icon-alt::text").get(default="")
    m = re.search(r"([\d.]+)\s*out of", rating_raw)
    rating = m.group(1) if m else ""

    reviews_el = sel.css("#acrCustomerReviewCount::text").get(default="")
    reviews = reviews_el.strip().split()[0].replace(",", "") if reviews_el.strip() else ""

    image = sel.css("#landingImage::attr(src)").get(default="")

    breadcrumbs = sel.css("#wayfinding-breadcrumbs_feature_div a::text").getall()
    category = " > ".join(c.strip() for c in breadcrumbs if c.strip())

    prime = "Yes" if sel.css("i.a-icon-prime").get() else ""

    # Related ASINs
    related = set()
    for found in re.findall(r'(?:data-asin="|/dp/)(B[A-Z0-9]{9})', html):
        if found != asin:
            related.add(found)

    product = None
    if title:
        product = {
            "platform": "amazon",
            "asin": asin,
            "title": title,
            "brand": brand,
            "category": category,
            "current_price": current_price,
            "original_price": original_price,
            "discount": "",
            "rating": rating,
            "reviews_count": reviews,
            "prime_eligible": prime,
            "amazon_choice": "",
            "sponsored": "",
            "main_image": image,
            "product_url": f"{BASE}/dp/{asin}",
            "scraped_at": datetime.now().isoformat(),
        }

    return product, related


def collect_seed_asins(session):
    """Collect seed ASINs from existing CSVs or by searching Amazon."""
    asins = set()

    # Try loading from local CSV files (downloaded by workflow)
    for pattern in ["amazon_products*.csv", "node_*.json"]:
        for f in Path(".").glob(pattern):
            try:
                if f.suffix == ".csv":
                    with open(f, encoding="utf-8-sig") as fh:
                        for row in csv.DictReader(fh):
                            a = row.get("asin", "").strip()
                            if a:
                                asins.add(a)
                elif f.suffix == ".json":
                    data = json.loads(f.read_text(encoding="utf-8"))
                    for item in data:
                        a = item.get("asin", "").strip()
                        if a:
                            asins.add(a)
            except Exception:
                continue

    # If no local files, search Amazon for seeds
    if not asins:
        print("  No local ASINs found — searching Amazon for seeds...", flush=True)
        searches = ["electronics", "fashion", "home", "beauty", "phone", "laptop",
                     "toys", "kitchen", "sports", "automotive", "baby", "grocery"]
        for kw in searches:
            html = fetch(session, f"{BASE}/s?k={kw}")
            if html:
                found = set(re.findall(r'data-asin="(B[A-Z0-9]{9})"', html))
                asins.update(found)
                print(f"    '{kw}': +{len(found)} ASINs (total: {len(asins)})", flush=True)
            time.sleep(random.uniform(2, 4))

    return asins


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--slice", type=int, default=0)
    parser.add_argument("--total-slices", type=int, default=1)
    parser.add_argument("--limit", type=int, default=PRODUCT_LIMIT)
    args = parser.parse_args()

    start_time = time.time()
    session = curl_requests.Session(impersonate="chrome131")
    checkpoint_file = f"related_checkpoint_{args.slice}.json"
    out_file = f"related_products_{args.slice}.json"

    # Warmup
    print("Warming up...", flush=True)
    fetch(session, f"{BASE}/")
    time.sleep(2)

    # Load checkpoint if exists
    all_products = []
    visited = set()
    queue = []

    if Path(checkpoint_file).exists():
        try:
            ckpt = json.loads(Path(checkpoint_file).read_text(encoding="utf-8"))
            all_products = ckpt.get("products", [])
            visited = set(ckpt.get("visited", []))
            queue = ckpt.get("queue", [])
            print(f"*** RESUMING: {len(all_products)} products, {len(visited)} visited, {len(queue)} in queue ***", flush=True)
        except Exception:
            pass

    # If no queue (first run), collect seeds
    if not queue and not visited:
        print("\nCollecting seed ASINs...", flush=True)
        seeds = collect_seed_asins(session)
        seed_list = sorted(seeds)
        my_seeds = [a for i, a in enumerate(seed_list) if i % args.total_slices == args.slice]
        queue = my_seeds
        print(f"Seeds: {len(seeds)} total, {len(my_seeds)} for this slice", flush=True)

    known_asins = set(a.get("asin", "") for a in all_products) | visited
    total_discovered = 0

    print(f"\nStarting crawl: queue={len(queue)}, visited={len(visited)}, products={len(all_products)}", flush=True)

    while queue and len(all_products) < args.limit:
        elapsed = time.time() - start_time
        if elapsed > MAX_RUNTIME:
            print(f"\n*** TIME LIMIT ({MAX_RUNTIME}s) — saving checkpoint ***", flush=True)
            break

        asin = queue.pop(0)
        if asin in visited:
            continue
        visited.add(asin)

        html = fetch(session, f"{BASE}/dp/{asin}")
        if html is None:
            continue

        product, related = extract_from_product_page(html, asin)

        # Save product
        if product and asin not in set(p["asin"] for p in all_products):
            all_products.append(product)

        # Add related to queue
        new_count = 0
        for rel in related:
            if rel not in known_asins:
                known_asins.add(rel)
                queue.append(rel)
                new_count += 1
                total_discovered += 1

        if len(visited) % 100 == 0:
            rate = len(all_products) / max(1, elapsed / 3600)
            print(f"  [{len(visited)} visited] products={len(all_products)}, queue={len(queue)}, discovered={total_discovered}, rate={rate:.0f}/hr", flush=True)

            # Save checkpoint every 100 visits
            Path(checkpoint_file).write_text(json.dumps({
                "products": all_products,
                "visited": list(visited),
                "queue": queue[:50000],  # Cap queue size
            }, ensure_ascii=False), encoding="utf-8")

        time.sleep(random.uniform(*PAGE_DELAY))

    # Final save
    Path(out_file).write_text(
        json.dumps(all_products, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\nSaved {len(all_products)} products -> {out_file}", flush=True)

    # Save checkpoint for next run
    Path(checkpoint_file).write_text(json.dumps({
        "products": all_products,
        "visited": list(visited),
        "queue": queue[:50000],
    }, ensure_ascii=False), encoding="utf-8")
    print(f"Checkpoint saved: {len(visited)} visited, {len(queue)} in queue", flush=True)

    # Convert to CSV
    if all_products:
        csv_file = f"amazon_products_related.csv"
        columns = list(all_products[0].keys())
        with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_products)
        print(f"CSV: {csv_file} ({len(all_products)} products)", flush=True)


if __name__ == "__main__":
    main()
