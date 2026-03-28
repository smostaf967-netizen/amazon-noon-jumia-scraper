"""
Noon Egypt Scraper — GitHub Actions edition (v4)
curl_cffi + RSC payload parsing — 407 categories from JSON file

Usage:
  python noon_spider.py --category-url "https://www.noon.com/egypt-en/fashion/" --category-name "Fashion" --limit 99999
  python noon_spider.py --slice 0 --total-slices 4 --limit 99999
"""
import argparse
import json
import re
import time
import random
import sys
import os
from datetime import datetime
from pathlib import Path

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "curl_cffi", "-q"])
    from curl_cffi import requests as curl_requests

PRODUCT_LIMIT = 99999
MAX_PAGES = 60  # Noon caps around 50 pages per category


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
                # Session rotation on repeated 403
                if attempt >= 1:
                    session = curl_requests.Session(impersonate="chrome131")
                    session.get("https://www.noon.com/egypt-en/", timeout=20)
                    time.sleep(1)
                continue
            if r.status_code == 404:
                return None
            print(f"    [HTTP {r.status_code}] — retrying", flush=True)
            time.sleep(random.uniform(3, 6))
        except Exception as e:
            print(f"    [ERROR] {e} — retrying", flush=True)
            time.sleep(random.uniform(5, 10))
    return None


# ── Scrape one category ──────────────────────────────────────────────────────

def scrape_category(session, cat_name, cat_url, limit, seen):
    """Scrape a single category URL. Returns list of new products."""
    new_products = []

    for page_num in range(1, MAX_PAGES + 1):
        if len(new_products) >= limit:
            break

        url = cat_url if page_num == 1 else f"{cat_url}?page={page_num}"
        html = _fetch(session, url)
        if html is None:
            break

        hits = extract_hits(html)
        new_count = 0
        for h in hits:
            if not isinstance(h, dict):
                continue
            p = build_product(h, cat_name)
            if p and p["product_id"] not in seen:
                seen.add(p["product_id"])
                new_products.append(p)
                new_count += 1

        if page_num <= 3 or page_num % 10 == 0 or new_count == 0:
            print(f"    Page {page_num}: {len(hits)} hits, +{new_count} new (total {len(new_products)})", flush=True)

        if new_count == 0:
            break

        time.sleep(random.uniform(2, 4))

    return new_products


# ── Load categories ──────────────────────────────────────────────────────────

def load_categories(slice_idx=0, total_slices=1):
    """Load categories from noon_categories_final.json and return this account's slice."""
    cat_file = Path(__file__).parent / "noon_categories_final.json"
    if not cat_file.exists():
        print(f"ERROR: {cat_file} not found", flush=True)
        return []

    with open(cat_file, encoding="utf-8") as f:
        all_cats = json.load(f)

    # Slice for this account
    my_cats = [c for i, c in enumerate(all_cats) if i % total_slices == slice_idx]
    print(f"  Categories: {len(my_cats)} (slice {slice_idx}/{total_slices}, from {len(all_cats)} total)", flush=True)
    return my_cats


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--category-url",   default="")
    parser.add_argument("--category-name",  default="")
    parser.add_argument("--slice",          type=int, default=0)
    parser.add_argument("--total-slices",   type=int, default=1)
    parser.add_argument("--limit",          type=int, default=PRODUCT_LIMIT)
    args = parser.parse_args()

    session = curl_requests.Session(impersonate="chrome131")

    # Warmup
    print("  Warming up...", flush=True)
    try:
        session.get("https://www.noon.com/egypt-en/", timeout=20)
        time.sleep(random.uniform(1, 2))
        print("  Warmup OK", flush=True)
    except Exception as e:
        print(f"  Warmup failed: {e}", flush=True)

    all_products = []
    seen = set()

    if args.category_url:
        # Single category mode
        categories = [{"name": args.category_name or "Unknown", "url": args.category_url}]
    else:
        # Slice mode — load from JSON
        categories = load_categories(args.slice, args.total_slices)

    for i, cat in enumerate(categories):
        cat_name = cat["name"]
        cat_url = cat["url"]
        remaining = args.limit - len(all_products)
        if remaining <= 0:
            break

        print(f"\n[{i+1}/{len(categories)}] '{cat_name}' — {cat_url}", flush=True)

        new_prods = scrape_category(session, cat_name, cat_url, remaining, seen)
        all_products.extend(new_prods)

        print(f"  => +{len(new_prods)} products (grand total: {len(all_products)})", flush=True)

        # Small pause between categories
        if i < len(categories) - 1:
            time.sleep(random.uniform(1, 3))

    # Save
    out = f"noon_products_{args.slice}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(all_products)} products -> {out}", flush=True)


if __name__ == "__main__":
    main()
