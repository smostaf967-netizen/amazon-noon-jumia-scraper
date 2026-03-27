"""
noon_subcategory_discovery.py
==============================
يكتشف كل الـ subcategory URLs من Noon Egypt بدون Playwright.
بيستخدم curl-cffi لأن Noon بيعمل RSC server-side (HTML جاهز).

Output: noon_categories.json  (list of {name, url, parent, depth})
"""

import json
import re
import time
import random
import sys
from collections import deque
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "curl-cffi", "-q"])
    from curl_cffi import requests as curl_requests

# ─────────────────────────────────────────────────────────────────────────────
BASE     = "https://www.noon.com/egypt-en"
MAX_CATS = 500
MAX_DEPTH = 4

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}

# المشروع الكبير: 43 category رئيسية
MAIN_CATEGORIES = [
    ("Electronics & Mobiles",    f"{BASE}/electronics-and-mobiles/"),
    ("Home & Kitchen",           f"{BASE}/home-and-kitchen/"),
    ("Sports & Outdoors",        f"{BASE}/sports-and-outdoors/"),
    ("Beauty & Health",          f"{BASE}/beauty-and-health/"),
    ("Baby & Toys",              f"{BASE}/baby-and-toys/"),
    ("Fashion",                  f"{BASE}/fashion/"),
    ("Grocery",                  f"{BASE}/grocery/"),
    ("Automotive",               f"{BASE}/automotive/"),
    ("Stationery & Office",      f"{BASE}/stationery-and-office-supplies/"),
    ("Pet Supplies",             f"{BASE}/pet-supplies/"),
]


def _fetch(session, url, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and len(r.text) > 1000:
                return r.text
            if r.status_code in (429, 503):
                print(f"    [HTTP {r.status_code}] backing off...")
                time.sleep(random.uniform(20, 40))
            else:
                time.sleep(random.uniform(2, 4))
        except Exception as e:
            print(f"    [ERROR] {e}")
            time.sleep(random.uniform(3, 6))
    return None


def _decode_rsc(html):
    """Extract RSC payload from Next.js self.__next_f.push chunks."""
    chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
    combined = "\n".join(chunks)
    try:
        return combined.encode().decode("unicode_escape")
    except Exception:
        return combined


def extract_subcategory_links(html, parent_url):
    """
    Extract subcategory links from Noon category page.
    Tries both HTML <a> tags and RSC JSON payload.
    """
    found = {}
    base_domain = "https://www.noon.com/egypt-en"

    # ── Method 1: HTML <a> tags with /egypt-en/.../ pattern ─────────────────
    for m in re.finditer(
        r'href="(/egypt-en/[a-z0-9][a-z0-9\-/]+/)"',
        html
    ):
        path = m.group(1)
        url  = f"https://www.noon.com{path}"
        # Must be deeper than parent
        if url != parent_url and url.startswith(parent_url) and url != parent_url.rstrip("/") + "/":
            # Extract name from nearby text
            snippet = html[max(0, m.start()-20):m.start()+200]
            nm = re.search(r'>([^<\n]{2,50}?)</(?:a|span|p|div|li)', snippet)
            name = nm.group(1).strip() if nm else path.rstrip("/").split("/")[-1].replace("-", " ").title()
            if url not in found:
                found[url] = name

    # ── Method 2: RSC payload ────────────────────────────────────────────────
    rsc = _decode_rsc(html)
    if rsc:
        for m in re.finditer(
            r'"(?:url|href|link|path)"\s*:\s*"(/egypt-en/[a-z0-9][a-z0-9\-/]+/)"',
            rsc
        ):
            path = m.group(1)
            url  = f"https://www.noon.com{path}"
            if url not in found and url.startswith(parent_url):
                found[url] = path.rstrip("/").split("/")[-1].replace("-", " ").title()

        # Also look for category names + URLs in RSC JSON structures
        for m in re.finditer(
            r'"name"\s*:\s*"([^"]{2,60})"\s*,\s*"[^"]*"\s*:\s*"[^"]*"\s*,\s*"(?:url|link|href)"\s*:\s*"(/egypt-en/[^"]+/)"',
            rsc
        ):
            name, path = m.group(1), m.group(2)
            url = f"https://www.noon.com{path}"
            if url not in found:
                found[url] = name

    return [(url, name) for url, name in found.items()]


def has_products(html):
    """Check if a page has product listings."""
    rsc = _decode_rsc(html)
    if '"hits"' in rsc or '"sku"' in rsc:
        return True
    # Check for product grid in HTML
    if 'data-qa="product-list"' in html or '"productCount"' in html:
        return True
    return False


def get_product_count(html):
    """Try to get total product count from page."""
    rsc = _decode_rsc(html)
    for pat in [
        r'"total(?:Items|Products|Count|Results)"\s*:\s*(\d+)',
        r'"nbHits"\s*:\s*(\d+)',
        r'"totalCount"\s*:\s*(\d+)',
        r'(\d[\d,]+)\s+(?:products|results)',
    ]:
        m = re.search(pat, rsc or html, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                pass
    return -1


def discover():
    session  = curl_requests.Session(impersonate="chrome131")
    visited  = set()
    queue    = deque()
    all_cats = []
    rsc_works = False

    print(f"\n{'='*60}")
    print(f"  Noon Egypt Subcategory Discovery")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    # Warm up on homepage
    print("  Warming up...")
    html = _fetch(session, f"{BASE}/")
    if html:
        # Check if RSC works (curl-cffi enough vs Playwright)
        rsc = _decode_rsc(html)
        if len(rsc) > 500:
            rsc_works = True
            print(f"  RSC payload OK ({len(rsc):,} chars) — curl-cffi is enough!")
        else:
            print(f"  RSC payload small ({len(rsc)} chars) — may need Playwright for scraping")
    else:
        print("  Warmup failed")
    time.sleep(random.uniform(2, 3))

    # Seed from main categories
    for name, url in MAIN_CATEGORIES:
        if url not in visited:
            visited.add(url)
            queue.append((url, name, 1, None))

    # BFS subcategory discovery
    print(f"\n  BFS from {len(queue)} main categories...\n")

    while queue and len(all_cats) < MAX_CATS:
        url, name, depth, parent = queue.popleft()

        print(f"  [d={depth}] {name[:45]:<45} {url.replace(BASE,'')}")
        html = _fetch(session, url)
        if not html:
            continue

        count    = get_product_count(html)
        children = extract_subcategory_links(html, url)
        new_ch   = [(u, n) for u, n in children if u not in visited]

        print(f"    -> count={count if count>0 else '?'}  children={len(new_ch)}")

        is_leaf = len(new_ch) == 0 or depth >= MAX_DEPTH

        all_cats.append({
            "name":    name,
            "url":     url,
            "depth":   depth,
            "parent":  parent,
            "count":   count,
            "is_leaf": is_leaf,
            "rsc_ok":  rsc_works,
        })

        if not is_leaf:
            for u, n in new_ch:
                visited.add(u)
                queue.append((u, n, depth + 1, url))

        time.sleep(random.uniform(1.5, 2.5))

    return all_cats, rsc_works


def main():
    cats, rsc_works = discover()
    leaves = [c for c in cats if c["is_leaf"]]

    print(f"\n{'='*60}")
    print(f"  DISCOVERY COMPLETE")
    print(f"  Total categories : {len(cats)}")
    print(f"  Leaf categories  : {len(leaves)}")
    print(f"  RSC via curl-cffi: {'YES - fast mode possible!' if rsc_works else 'NO - Playwright needed'}")
    print(f"{'='*60}")

    if leaves:
        print("\n  Top 20 leaf categories:")
        for c in sorted(leaves, key=lambda x: x["count"], reverse=True)[:20]:
            print(f"    {c['count']:>7}  {c['name'][:45]:<45} {c['url'].replace(BASE,'')}")

    Path("noon_categories.json").write_text(
        json.dumps(cats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    Path("noon_leaf_categories.json").write_text(
        json.dumps(leaves, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\n  Saved noon_categories.json ({len(cats)} categories)")
    print(f"  Saved noon_leaf_categories.json ({len(leaves)} leaf categories)")


if __name__ == "__main__":
    main()
