#!/usr/bin/env python3
"""
Jumia Egypt Scraper — GitHub Actions edition
curl_cffi (Chrome 131 TLS fingerprint) + HTML parsing

Usage:
  python jumia_spider.py --mode discover
  python jumia_spider.py --mode scrape --category-url "https://www.jumia.com.eg/phones-tablets/" --category-name "Phones & Tablets" --deep
  python jumia_spider.py --mode export
"""
import argparse
import csv
import json
import logging
import os
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
    from bs4 import BeautifulSoup
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "beautifulsoup4", "-q"])
    from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("jumia")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BASE_URL       = "https://www.jumia.com.eg"
PRODUCT_LIMIT  = 99999
MAX_PAGES      = 200
PAGE_DELAY     = (2, 5)
DETAIL_DELAY   = (1.5, 3.5)

CSV_FIELDS = [
    "platform", "product_id", "title", "brand", "category",
    "current_price", "original_price", "discount",
    "rating", "reviews_count", "availability",
    "seller", "express_delivery", "platform_badge", "sponsored",
    "main_image", "all_images",
    "description", "weight", "dimensions",
    "model_number", "country_of_origin", "warranty",
    "tech_specs", "variations",
    "ships_from", "delivery_date",
    "product_url", "scraped_at",
]

# ── Categories ────────────────────────────────────────────────────────────────
SEED_CATEGORIES = [
    {"name": "Phones & Tablets",     "url": f"{BASE_URL}/phones-tablets/"},
    {"name": "Electronics",          "url": f"{BASE_URL}/electronics/"},
    {"name": "Appliances",           "url": f"{BASE_URL}/appliances/"},
    {"name": "Computing",            "url": f"{BASE_URL}/computing/"},
    {"name": "Men's Fashion",        "url": f"{BASE_URL}/mens-clothing/"},
    {"name": "Women's Fashion",      "url": f"{BASE_URL}/womens-clothing/"},
    {"name": "Health & Beauty",      "url": f"{BASE_URL}/health-beauty/"},
    {"name": "Sporting Goods",       "url": f"{BASE_URL}/sporting-goods/"},
    {"name": "Home & Office",        "url": f"{BASE_URL}/home-office/"},
    {"name": "Baby Products",        "url": f"{BASE_URL}/baby-products/"},
    {"name": "Toys & Games",         "url": f"{BASE_URL}/toys-games/"},
    {"name": "Grocery",              "url": f"{BASE_URL}/grocery/"},
    {"name": "Automobile",           "url": f"{BASE_URL}/automobile/"},
    {"name": "Musical Instruments",  "url": f"{BASE_URL}/musical-instruments/"},
    {"name": "Books",                "url": f"{BASE_URL}/books/"},
    {"name": "Video Games",          "url": f"{BASE_URL}/video-games/"},
    {"name": "Luggage & Bags",       "url": f"{BASE_URL}/bags-luggage/"},
    {"name": "Watches",              "url": f"{BASE_URL}/watches/"},
    {"name": "Pet Supplies",         "url": f"{BASE_URL}/pet-supplies/"},
    {"name": "Office Supplies",      "url": f"{BASE_URL}/office-supplies/"},
    {"name": "Kids' Fashion",        "url": f"{BASE_URL}/kids-fashion/"},
    {"name": "Jewelry",              "url": f"{BASE_URL}/jewelry/"},
]

JUMIA_SORT_ORDERS = [
    ("default",    ""),
    ("price_asc",  "sort[by]=price&sort[dir]=asc"),
    ("price_desc", "sort[by]=price&sort[dir]=desc"),
    ("newest",     "sort[by]=date&sort[dir]=desc"),
    ("top_rated",  "sort[by]=rating&sort[dir]=desc"),
]

JUMIA_PRICE_BANDS = [
    ("0-200",        "price=0-200"),
    ("200-500",      "price=200-500"),
    ("500-1000",     "price=500-1000"),
    ("1000-2000",    "price=1000-2000"),
    ("2000-5000",    "price=2000-5000"),
    ("5000-10000",   "price=5000-10000"),
    ("10000+",       "price=10000-"),
]

_EXCLUDE_RE = re.compile(
    r"/(account|cart|help|seller|vendor|blog|deals|flash-sale|flash-sales|"
    r"new-arrivals|featured|bestsellers|login|register|wishlist|"
    r"about|contact|policy|terms|sitemap|search|checkout|"
    r"membership|loyalty|return|recommended|sp-|category-fashion|"
    r"mlp-)(/|$)|^/mlp-|^/sp-",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP SESSION
# ─────────────────────────────────────────────────────────────────────────────

def create_session():
    return curl_requests.Session(impersonate="chrome131")


def fetch(session, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                if "just a moment" in r.text.lower():
                    log.warning(f"Cloudflare challenge — waiting 15s (attempt {attempt+1})")
                    time.sleep(15)
                    continue
                return r.text
            if r.status_code == 403:
                wait = random.uniform(15, 30)
                log.warning(f"403 Forbidden — waiting {wait:.0f}s (attempt {attempt+1})")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            if r.status_code in (429, 502, 503, 504):
                wait = random.uniform(30, 60)
                log.warning(f"HTTP {r.status_code} — waiting {wait:.0f}s")
                time.sleep(wait)
                continue
            log.warning(f"HTTP {r.status_code}: {url[:80]}")
            return None
        except Exception as e:
            log.warning(f"Fetch error: {e} — retrying")
            time.sleep(random.uniform(5, 10))
    log.error(f"All {max_retries} attempts failed: {url[:80]}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def parse_price(text):
    if not text:
        return ""
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    return f"{cleaned} EGP" if cleaned else ""


def parse_rating(style_str):
    m = re.search(r"width:\s*([\d.]+)%", style_str or "")
    if m:
        return str(round(float(m.group(1)) / 20, 1))
    return ""


def parse_reviews(text):
    if not text:
        return ""
    digits = re.sub(r"[^\d]", "", text)
    return digits if digits else ""


def get_total_pages(soup):
    # Method 1: "1 - 40 of 2,160"
    for el in soup.select("p, div, span"):
        text = el.get_text(strip=True)
        m = re.match(r"1\s*-\s*\d+\s+of\s+([\d,]+)", text)
        if m:
            total = int(m.group(1).replace(",", ""))
            return min((total + 39) // 40, MAX_PAGES)

    # Method 2: aria-label="Page N of Y"
    for a in soup.select("a[aria-label]"):
        m = re.search(r"Page\s+\d+\s+of\s+(\d+)", a.get("aria-label", ""))
        if m:
            return min(int(m.group(1)), MAX_PAGES)

    # Method 3: highest page=N
    max_p = 1
    for a in soup.select("a[href*='page=']"):
        m = re.search(r"page=(\d+)", a.get("href", ""))
        if m:
            max_p = max(max_p, int(m.group(1)))
    if max_p > 1:
        return min(max_p, MAX_PAGES)

    # Method 4: pagination links
    for sel in [".-pag", ".pg", "nav", "[class*='pag']"]:
        pag = soup.select_one(sel)
        if pag:
            for a in pag.select("a"):
                txt = a.get_text(strip=True)
                if txt.isdigit():
                    max_p = max(max_p, int(txt))
    return min(max_p, MAX_PAGES) if max_p > 1 else 1


def parse_listing_page(html, cat_name):
    soup = BeautifulSoup(html, "html.parser")
    products = []

    articles = soup.select("article.prd, article[class*='prd'], div[class*='c-prd']")
    for art in articles:
        try:
            link_el = art.select_one("a.core, a[class*='core']") or art.select_one("a[href]")
            if not link_el:
                continue
            href = link_el.get("href", "")
            product_url = f"{BASE_URL}{href}" if href.startswith("/") else href

            pid = art.get("data-id") or art.get("data-sku") or ""
            if not pid:
                m = re.search(r"-(\d+)\.html", href)
                pid = m.group(1) if m else ""
            if not pid:
                continue

            img_el = art.select_one("img.img, img[class*='img']") or art.select_one("img")
            img_src = ""
            if img_el:
                img_src = img_el.get("data-src") or img_el.get("src") or ""
                if "data:image" in img_src or ".svg" in img_src:
                    img_src = ""

            title_el = art.select_one("h3.name, .name, h3")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            price_el = art.select_one("div.prc, .prc")
            current_price = parse_price(price_el.get_text() if price_el else "")

            old_el = art.select_one("div.old, .old")
            original_price = parse_price(old_el.get_text() if old_el else "")

            disc_el = art.select_one("div.bdg._dsct, div.bdg.-dsct, [class*='_dsct']")
            discount = ""
            if disc_el:
                discount = disc_el.get_text(strip=True).lstrip("-").strip()
            elif current_price and original_price:
                try:
                    cp = float(re.sub(r"[^\d.]", "", current_price))
                    op = float(re.sub(r"[^\d.]", "", original_price))
                    if op > cp > 0:
                        discount = f"{round((1 - cp / op) * 100)}%"
                except Exception:
                    pass

            rating = ""
            star_el = art.select_one("i.s[style], [class*='stars'] i[style]")
            if star_el:
                rating = parse_rating(star_el.get("style", ""))

            reviews = ""
            rev_el = art.select_one(".-rvw, [class*='rvw'], .rev, [class*='rev']")
            if rev_el:
                span = rev_el.select_one("span")
                reviews = parse_reviews(span.get_text() if span else rev_el.get_text())

            express = "Yes" if art.select_one(".xtrs, [class*='xtrs']") else ""

            products.append({
                "platform": "jumia", "product_id": pid, "title": title,
                "brand": "", "category": cat_name,
                "current_price": current_price, "original_price": original_price,
                "discount": discount, "rating": rating, "reviews_count": reviews,
                "availability": "In Stock", "seller": "", "express_delivery": express,
                "platform_badge": "", "sponsored": "",
                "main_image": img_src, "all_images": img_src,
                "description": "", "weight": "", "dimensions": "",
                "model_number": "", "country_of_origin": "", "warranty": "",
                "tech_specs": "", "variations": "",
                "ships_from": "", "delivery_date": "",
                "product_url": product_url,
                "scraped_at": datetime.now().isoformat(),
            })
        except Exception:
            continue

    return products, get_total_pages(soup)


# ─────────────────────────────────────────────────────────────────────────────
# DETAIL PAGE ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────

def enrich_product(session, product):
    url = product.get("product_url", "")
    if not url:
        return product

    html = fetch(session, url, max_retries=2)
    if not html:
        return product

    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if d.get("@type") == "Product"), data[0] if data else {})
            if data.get("@graph"):
                data = next((d for d in data["@graph"] if d.get("@type") == "Product"), data)
            if data.get("@type") != "Product":
                continue

            brand = data.get("brand", "")
            if isinstance(brand, dict):
                brand = brand.get("name", "")
            if brand:
                product["brand"] = str(brand)

            desc = data.get("description", "")
            if desc:
                product["description"] = str(desc)[:2000]

            agg = data.get("aggregateRating", {})
            if agg:
                if agg.get("ratingValue"):
                    product["rating"] = str(agg["ratingValue"])
                if agg.get("reviewCount"):
                    product["reviews_count"] = str(agg["reviewCount"])

            offers = data.get("offers", {})
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            avail = offers.get("availability", "")
            if "InStock" in avail:
                product["availability"] = "In Stock"
            elif "OutOfStock" in avail:
                product["availability"] = "Out of Stock"

            images = data.get("image", [])
            if isinstance(images, list):
                urls = [i.get("contentUrl", i) if isinstance(i, dict) else i for i in images]
                if urls:
                    product["all_images"] = " | ".join(str(u) for u in urls[:10])
        except Exception:
            continue

    # Specs
    specs_ul = soup.select_one("ul.-pvs.-mvxs.-phm.-lsn") or soup.select_one("ul[class*='-pvs'][class*='-lsn']")
    if specs_ul:
        specs = {}
        for li in specs_ul.select("li"):
            key_el = li.select_one("span.-b")
            if key_el:
                key = key_el.get_text(strip=True).rstrip(":")
                val = li.get_text(strip=True).replace(key_el.get_text(strip=True), "").strip().lstrip(":")
                if key and val:
                    specs[key] = val
        if specs:
            product["tech_specs"] = json.dumps(specs, ensure_ascii=False)
            for k, v in specs.items():
                kl = k.lower()
                if "model" in kl and not product["model_number"]:
                    product["model_number"] = v
                if ("country" in kl or "origin" in kl or "production" in kl) and not product["country_of_origin"]:
                    product["country_of_origin"] = v
                if ("warranty" in kl or "guarantee" in kl) and not product["warranty"]:
                    product["warranty"] = v

    # Seller
    for section in soup.select("section.card, section"):
        text = section.get_text(strip=True).lower()
        if "seller information" in text or "sold by" in text:
            seller_el = section.select_one("p.-m.-pbs") or section.select_one("a, p, span")
            if seller_el:
                product["seller"] = seller_el.get_text(strip=True)
            if "jumia mall" in text or "verified" in text:
                product["platform_badge"] = "Yes"
            break

    # Delivery
    for div in soup.select("div.markup.-ptxs, div[class*='delivery']"):
        text = div.get_text(strip=True)
        if "delivery" in text.lower():
            product["delivery_date"] = re.sub(r"if you place.*", "", text, flags=re.IGNORECASE).strip()[:100]
            break

    # Express
    if not product["express_delivery"]:
        if "jumia express" in soup.get_text(strip=True).lower():
            product["express_delivery"] = "Yes"

    # Variations
    variants = []
    for a in soup.select("a[class*='a-si'], a[class*='variant'], [data-value]"):
        v = a.get("title") or a.get("data-value") or a.get("aria-label") or a.get_text(strip=True)
        if v and v not in variants:
            variants.append(v)
    if variants:
        product["variations"] = json.dumps({"options": variants[:20]}, ensure_ascii=False)

    time.sleep(random.uniform(*DETAIL_DELAY))
    return product


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def slug_to_name(slug):
    return slug.strip("/").split("/")[-1].replace("-", " ").title()


def fetch_categories(session):
    categories = list(SEED_CATEGORIES)
    seen = {c["url"].rstrip("/") for c in categories}

    html = fetch(session, BASE_URL)
    if not html:
        log.warning("Could not fetch homepage — using seed categories only")
        return categories

    soup = BeautifulSoup(html, "html.parser")
    for sel in ["nav a[href]", ".nav-holder a[href]", ".-hd-links a[href]",
                "#jm-header a[href]", "header a[href]", "[class*='nav'] a[href]"]:
        links = soup.select(sel)
        if len(links) < 3:
            continue
        for a in links:
            href = a.get("href", "")
            if not href.startswith("/"):
                continue
            path = "/" + href.strip("/") + "/"
            if not re.match(r"^/[a-z][a-z0-9-]+/$", path):
                continue
            if _EXCLUDE_RE.search(path):
                continue
            full_url = f"{BASE_URL}{path}"
            if full_url.rstrip("/") in seen:
                continue
            name = a.get_text(strip=True)
            if not name or len(name) > 40 or name.lower() in ("see all", "view all", "shop now"):
                name = slug_to_name(path)
            seen.add(full_url.rstrip("/"))
            categories.append({"name": name, "url": full_url})
        break

    log.info(f"Discovered {len(categories)} categories")
    return categories


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_category(session, cat_name, cat_url, limit=PRODUCT_LIMIT, deep=False):
    log.info(f"[{cat_name}] Scraping — {cat_url}")

    all_products = []
    seen = set()
    total_failures = 0
    total_combos = len(JUMIA_PRICE_BANDS) * len(JUMIA_SORT_ORDERS)

    def _add(p):
        pid = p.get("product_id", "")
        if pid and pid not in seen and len(all_products) < limit:
            seen.add(pid)
            all_products.append(p)
            return True
        return False

    # Warmup
    log.info(f"[{cat_name}] Warmup...")
    fetch(session, f"{BASE_URL}/phones-tablets/", max_retries=2)
    time.sleep(random.uniform(1, 2))
    fetch(session, cat_url, max_retries=2)
    time.sleep(random.uniform(1, 2))

    for band_name, band_param in JUMIA_PRICE_BANDS:
        if len(all_products) >= limit:
            break

        log.info(f"[{cat_name}] Price band: {band_name} EGP")

        for sort_name, sort_param in JUMIA_SORT_ORDERS:
            if len(all_products) >= limit:
                break

            params = [p for p in [band_param, sort_param] if p]
            query = "&".join(params)
            combo_url = f"{cat_url}?{query}" if query else cat_url

            log.info(f"[{cat_name}] [{band_name}/{sort_name}] — {combo_url}")

            html = fetch(session, combo_url)
            if not html:
                total_failures += 1
                log.warning(f"[{cat_name}] Cannot fetch [{band_name}/{sort_name}] — skipping.")
                continue

            products, total_pages = parse_listing_page(html, cat_name)
            new_count = sum(1 for p in products if _add(p))
            log.info(f"[{cat_name}] [{band_name}/{sort_name}] +{new_count} new (total {len(all_products)})")

            if new_count == 0:
                continue

            for page_num in range(2, min(total_pages + 1, MAX_PAGES + 1)):
                if len(all_products) >= limit:
                    break

                page_url = f"{combo_url}&page={page_num}"
                html = fetch(session, page_url)
                if not html:
                    break

                products, _ = parse_listing_page(html, cat_name)
                new_count = sum(1 for p in products if _add(p))

                if page_num % 5 == 0 or new_count == 0:
                    log.info(f"[{cat_name}] [{band_name}/{sort_name}] page {page_num}: +{new_count} (total {len(all_products)})")

                if new_count == 0:
                    break

                time.sleep(random.uniform(*PAGE_DELAY))

    # Fallback: unfiltered
    if total_failures >= total_combos and len(all_products) == 0:
        log.warning(f"[{cat_name}] All filtered URLs failed — fallback to unfiltered")
        for sort_name, sort_param in JUMIA_SORT_ORDERS:
            url = f"{cat_url}?{sort_param}" if sort_param else cat_url
            html = fetch(session, url)
            if not html:
                continue
            products, total_pages = parse_listing_page(html, cat_name)
            for p in products:
                _add(p)
            for page_num in range(2, min(total_pages + 1, 51)):
                if len(all_products) >= limit:
                    break
                sep = "&" if "?" in url else "?"
                html = fetch(session, f"{url}{sep}page={page_num}")
                if not html:
                    break
                products, _ = parse_listing_page(html, cat_name)
                if sum(1 for p in products if _add(p)) == 0:
                    break
                time.sleep(random.uniform(*PAGE_DELAY))

    log.info(f"[{cat_name}] {len(all_products)} unique products from listings.")

    # Deep: enrich
    if deep and all_products:
        log.info(f"[{cat_name}] Enriching {len(all_products)} products via detail pages...")
        for i, p in enumerate(all_products):
            try:
                all_products[i] = enrich_product(session, p)
            except Exception as e:
                log.debug(f"Detail error for {p.get('product_id')}: {e}")
            if (i + 1) % 50 == 0:
                log.info(f"[{cat_name}] Detail progress: {i+1}/{len(all_products)}")
        log.info(f"[{cat_name}] Detail progress: {len(all_products)}/{len(all_products)}")

    log.info(f"[{cat_name}] Done: {len(all_products)} products")
    return all_products


# ─────────────────────────────────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def merge_json_to_csv():
    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)
    all_products = []
    for jf in Path(".").rglob("products_*.json"):
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
            if isinstance(data, list):
                all_products.extend(data)
                log.info(f"Loaded {len(data)} from {jf}")
        except Exception as e:
            log.warning(f"Failed to load {jf}: {e}")
    if not all_products:
        log.warning("No products to export")
        return
    today = datetime.now().strftime("%Y-%m-%d")
    out_file = out_dir / f"jumia_products_{today}.csv"
    with open(out_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_products)
    log.info(f"Exported {len(all_products)} products -> {out_file}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Jumia Egypt Scraper")
    parser.add_argument("--mode", choices=["discover", "scrape", "export"], required=True)
    parser.add_argument("--category-name",  default="")
    parser.add_argument("--category-url",   default="")
    parser.add_argument("--group-index",    type=int, default=0)
    parser.add_argument("--limit",          type=int, default=PRODUCT_LIMIT)
    parser.add_argument("--deep",           action="store_true")
    args = parser.parse_args()

    session = create_session()

    if args.mode == "discover":
        categories = fetch_categories(session)
        with open("categories.json", "w", encoding="utf-8") as f:
            json.dump(categories, f, ensure_ascii=False, indent=2)
        # Output in GitHub Actions matrix format
        matrix = [
            {"cat_name": c["name"], "cat_url": c["url"], "cat_index": i}
            for i, c in enumerate(categories)
        ]
        print(json.dumps(matrix, ensure_ascii=False))

    elif args.mode == "scrape":
        if not args.category_url:
            log.error("--category-url required for scrape mode")
            sys.exit(1)
        products = scrape_category(
            session, cat_name=args.category_name or "Unknown",
            cat_url=args.category_url, limit=args.limit, deep=args.deep,
        )
        out = f"products_{args.group_index}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
        log.info(f"{args.category_name} -> {out} ({len(products)} products)")

    elif args.mode == "export":
        merge_json_to_csv()


if __name__ == "__main__":
    main()
