"""
Noon Egypt Scraper — GitHub Actions edition
Playwright headless Chromium + RSC payload parsing + Cloudflare bypass

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

PRODUCT_LIMIT = 99999  # unlimited — جيب كل اللي تقدر عليه

# Sort orders — كل sort بيجيب منتجات مختلفة
NOON_SORT_ORDERS = [
    ("popularity",  ""),                          # default
    ("newest",      "?sortBy=new_arrivals"),
    ("price_low",   "?sortBy=price_low"),
    ("price_high",  "?sortBy=price_high"),
]

# Price bands in EGP — Noon uses ?price[from]=X&price[to]=Y
# 16 bands عشان نكتشف منتجات أكتر (خصوصاً الفئات الكبيرة)
NOON_PRICE_BANDS = [
    ("all",          None,    None),      # no price filter first
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


# ── Cloudflare detection keywords ────────────────────────────────────────────
CF_SIGNATURES = [
    "checking your browser",
    "just a moment",
    "cf-browser-verification",
    "challenge-platform",
    "turnstile",
    "ray id",
    "cloudflare",
    "attention required",
    "enable javascript",
    "security check",
]


def _is_cloudflare_page(html_or_text):
    """Check if the page is a Cloudflare challenge/block page."""
    lower = html_or_text.lower()
    return any(sig in lower for sig in CF_SIGNATURES)


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


# ── DOM fallback parser ──────────────────────────────────────────────────────

def _parse_dom_fallback(page, cat_name):
    """Fallback: extract products from visible HTML DOM when RSC parsing fails."""
    products = []
    try:
        cards = page.query_selector_all('[data-qa="product-card"], div[class*="productContainer"], article[class*="product"]')
        if not cards:
            cards = page.query_selector_all('a[href*="/p/"]')
        for card in cards:
            try:
                href = card.get_attribute("href") or ""
                sku_match = re.search(r'/p/([A-Z0-9]+)', href)
                sku = sku_match.group(1) if sku_match else ""
                title_el = card.query_selector('[data-qa="product-name"], h2, span[class*="title"], span[class*="name"]')
                title = title_el.inner_text().strip() if title_el else ""
                price_el = card.query_selector('[data-qa="product-price"], span[class*="price"], strong')
                price_text = price_el.inner_text().strip() if price_el else ""
                price_clean = re.sub(r'[^\d.]', '', price_text.replace(',', ''))
                if not sku or not title:
                    continue
                products.append({
                    "platform": "noon",
                    "product_id": sku,
                    "title": title,
                    "brand": "",
                    "category": cat_name,
                    "current_price": f"{price_clean} EGP" if price_clean else "",
                    "original_price": "",
                    "discount": "",
                    "rating": "",
                    "reviews_count": "",
                    "availability": "In Stock",
                    "seller": "",
                    "express_delivery": "",
                    "platform_badge": "",
                    "sponsored": "",
                    "main_image": "",
                    "all_images": "",
                    "description": "",
                    "weight": "",
                    "dimensions": "",
                    "model_number": "",
                    "country_of_origin": "",
                    "warranty": "",
                    "tech_specs": "",
                    "variations": "",
                    "ships_from": "",
                    "delivery_date": "",
                    "product_url": f"https://www.noon.com{href}" if href.startswith("/") else href,
                    "scraped_at": datetime.now().isoformat(),
                })
            except Exception:
                continue
    except Exception as e:
        print(f"  [DOM fallback error] {e}", flush=True)
    return products


# ── Cloudflare bypass helpers ─────────────────────────────────────────────────

def _human_scroll(page):
    """Simulate human-like scrolling behavior."""
    try:
        # Scroll down in small steps
        for pct in [0.2, 0.4, 0.6, 0.8, 1.0]:
            page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {pct})")
            time.sleep(random.uniform(0.3, 0.8))
        # Scroll back up a bit (human behavior)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.3)")
        time.sleep(random.uniform(0.5, 1.0))
        # Final scroll to bottom
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(random.uniform(1.0, 2.0))
    except Exception:
        pass


def _wait_for_cloudflare(page, max_wait=20):
    """Wait for Cloudflare challenge to resolve. Returns True if resolved."""
    start = time.time()
    while time.time() - start < max_wait:
        try:
            html = page.content()
            if not _is_cloudflare_page(html):
                return True
            # Check if there's a Turnstile checkbox to click
            try:
                checkbox = page.query_selector('input[type="checkbox"], .cf-turnstile iframe')
                if checkbox:
                    checkbox.click()
                    print("    [CF] Clicked Turnstile checkbox", flush=True)
                    time.sleep(3)
            except Exception:
                pass
            time.sleep(2)
        except Exception:
            time.sleep(2)
    return False


def _warmup_session(page):
    """Comprehensive warmup: visit homepage, scroll, dismiss popups."""
    print("  Warming up on homepage...", flush=True)
    try:
        page.goto("https://www.noon.com/egypt-en/", wait_until="domcontentloaded", timeout=45_000)
        time.sleep(random.uniform(3, 5))

        # Check for Cloudflare on homepage
        html = page.content()
        if _is_cloudflare_page(html):
            print("  [CF] Cloudflare challenge on homepage — waiting...", flush=True)
            if _wait_for_cloudflare(page, max_wait=25):
                print("  [CF] Challenge resolved!", flush=True)
            else:
                print("  [CF] Challenge NOT resolved — will retry on category pages", flush=True)

        # Human-like browsing: scroll around
        _human_scroll(page)

        # Try to dismiss cookie/popup dialogs
        for sel in ['button[data-qa="accept-cookies"]', 'button[aria-label="Close"]',
                     'button:has-text("Accept")', 'button:has-text("Got it")']:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(0.5)
            except Exception:
                pass

        # Visit a random category first to establish session
        warmup_urls = [
            "https://www.noon.com/egypt-en/electronics-and-mobiles/",
            "https://www.noon.com/egypt-en/fashion/",
            "https://www.noon.com/egypt-en/beauty-and-health/",
        ]
        warmup_url = random.choice(warmup_urls)
        print(f"  Warmup browse: {warmup_url}", flush=True)
        page.goto(warmup_url, wait_until="domcontentloaded", timeout=45_000)
        time.sleep(random.uniform(4, 7))

        if _is_cloudflare_page(page.content()):
            print("  [CF] Cloudflare on warmup page — waiting...", flush=True)
            _wait_for_cloudflare(page, max_wait=20)

        _human_scroll(page)
        time.sleep(random.uniform(2, 4))
        print("  Warmup OK", flush=True)

    except Exception as e:
        print(f"  Warmup failed: {e} (continuing)", flush=True)


# ── Page scraper (Playwright) with retry ─────────────────────────────────────

def _scrape_page(page, url, cat_name, retry=0, max_retries=3):
    from playwright.sync_api import TimeoutError as PWTimeout

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    except PWTimeout:
        print(f"    [TIMEOUT] {url}", flush=True)
        if retry < max_retries:
            print(f"    [RETRY {retry+1}/{max_retries}] Timeout — retrying after wait...", flush=True)
            time.sleep(random.uniform(8, 15))
            return _scrape_page(page, url, cat_name, retry + 1, max_retries)
        return []
    except Exception as e:
        print(f"    [NAV ERROR] {e}", flush=True)
        return []

    # Check for Cloudflare BEFORE reading content
    time.sleep(random.uniform(2, 4))
    html = page.content()

    if _is_cloudflare_page(html):
        print(f"    [CF] Cloudflare challenge detected — waiting up to 20s...", flush=True)
        resolved = _wait_for_cloudflare(page, max_wait=20)
        if resolved:
            print(f"    [CF] Challenge resolved!", flush=True)
            html = page.content()
        elif retry < max_retries:
            print(f"    [CF-RETRY {retry+1}/{max_retries}] Reloading page...", flush=True)
            time.sleep(random.uniform(5, 10))
            return _scrape_page(page, url, cat_name, retry + 1, max_retries)
        else:
            print(f"    [CF] Challenge NOT resolved after {max_retries} retries — skipping", flush=True)
            return []

    # Wait for content and scroll to trigger lazy loading
    _human_scroll(page)
    time.sleep(random.uniform(1, 2))

    html = page.content()
    hits = extract_hits(html)

    if not hits:
        body_text = ""
        try:
            body_text = page.evaluate("document.body.innerText")
        except Exception:
            pass

        if "could not find" in body_text.lower() or "went wrong" in body_text.lower():
            print("    [WARN] Noon returned error page", flush=True)
        else:
            # Fallback: try parsing products from visible DOM
            dom_products = _parse_dom_fallback(page, cat_name)
            if dom_products:
                print(f"    [DOM fallback] Recovered {len(dom_products)} products from HTML", flush=True)
                return dom_products

            # If page 1 returned 0 and no error detected, might be a soft block — retry
            if retry < max_retries:
                print(f"    [EMPTY-RETRY {retry+1}/{max_retries}] Got 0 products — retrying after wait...", flush=True)
                time.sleep(random.uniform(8, 15))
                # Try a small human interaction before retry
                try:
                    page.goto("https://www.noon.com/egypt-en/", wait_until="domcontentloaded", timeout=30_000)
                    time.sleep(random.uniform(3, 5))
                    _human_scroll(page)
                except Exception:
                    pass
                return _scrape_page(page, url, cat_name, retry + 1, max_retries)

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
    print(f"\n[Group {group_index}] '{cat_name}'  limit={limit}", flush=True)

    all_products = []
    seen         = set()
    consecutive_empty_bands = 0  # Track consecutive empty bands for early exit

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
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="Africa/Cairo",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        page = ctx.new_page()
        stealth.apply_stealth_sync(page)

        # Comprehensive warmup
        _warmup_session(page)

        # Loop through price bands × sort orders
        for band_name, price_min, price_max in NOON_PRICE_BANDS:
            if len(all_products) >= limit:
                break

            # If we got 3+ consecutive empty bands, the category is likely fully blocked
            if consecutive_empty_bands >= 3 and band_name != "all":
                print(f"\n  [SKIP] Skipping band '{band_name}' — {consecutive_empty_bands} consecutive empty bands", flush=True)
                continue

            # Build price filter URL params
            if price_min is None:
                price_suffix = ""
            elif price_max is None:
                price_suffix = f"?price[from]={price_min}"
            else:
                price_suffix = f"?price[from]={price_min}&price[to]={price_max}"

            band_before = len(all_products)
            print(f"\n  [Price Band: {band_name} EGP]", flush=True)

            for sort_name, sort_suffix in NOON_SORT_ORDERS:
                if len(all_products) >= limit:
                    break

                # Combine price + sort params
                if price_suffix and sort_suffix:
                    combined = price_suffix + "&" + sort_suffix.lstrip("?")
                elif price_suffix:
                    combined = price_suffix
                else:
                    combined = sort_suffix

                combo_url = base_url + combined
                print(f"\n    [{band_name}/{sort_name}]", flush=True)
                sort_count = 0

                for page_num in range(1, 200):
                    if len(all_products) >= limit:
                        break

                    sep = "&" if "?" in combined else "?"
                    url = combo_url if page_num == 1 else f"{combo_url}{sep}page={page_num}"
                    print(f"    Page {page_num}: {url}", flush=True)

                    # Use retry only on first page of each combo
                    retries = 3 if page_num == 1 else 1
                    raw = _scrape_page(page, url, cat_name, retry=0, max_retries=retries)
                    print(f"    RSC hits: {len(raw)}", flush=True)

                    new_count = 0
                    for p in raw:
                        if p and p["product_id"] not in seen and len(all_products) < limit:
                            seen.add(p["product_id"])
                            all_products.append(p)
                            new_count += 1
                            sort_count += 1

                    print(f"    +{new_count} new  (total {len(all_products)}/{limit})", flush=True)

                    if new_count == 0:
                        print(f"    Sort '{sort_name}' exhausted at page {page_num}", flush=True)
                        break

                    # Longer delays between pages (4-8 seconds)
                    time.sleep(random.uniform(4, 8))

                print(f"    Sort '{sort_name}' done: {sort_count} new products", flush=True)

            band_new = len(all_products) - band_before
            print(f"  Band '{band_name}' done: {band_new} new products", flush=True)

            if band_new > 0:
                consecutive_empty_bands = 0
            else:
                consecutive_empty_bands += 1

        browser.close()

    print(f"[Group {group_index}] Done: {len(all_products)} products", flush=True)
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
