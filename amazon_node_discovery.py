"""
amazon_node_discovery.py — Phase 1 (v2)
=========================================
يكتشف browse node IDs من Amazon Egypt عن طريق:
1. Site Directory (gp/site-directory) — قائمة كل الـ categories
2. Homepage navigation links
3. BFS على الـ node URLs المكتشفة

Output: amazon_nodes.json + amazon_leaf_nodes.json
"""

import json
import re
import time
import random
import sys
import argparse
from collections import deque
from datetime import datetime
from pathlib import Path

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "curl-cffi", "-q"])
    from curl_cffi import requests as curl_requests

# ─────────────────────────────────────────────────────────────────────────────
BASE        = "https://www.amazon.eg"
MAX_DEPTH   = 6
MIN_PRODS   = 5
MAX_NODES   = 8000
DELAY_MIN   = 1.5
DELAY_MAX   = 3.0

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# ─────────────────────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────────────────────

def _fetch(session, url, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and len(r.text) > 3000:
                return r.text
            if r.status_code == 503:
                print(f"    [503] backing off 30s...")
                time.sleep(30)
            else:
                print(f"    [HTTP {r.status_code}] retrying...")
                time.sleep(random.uniform(3, 6))
        except Exception as e:
            print(f"    [ERROR] {e}")
            time.sleep(random.uniform(4, 8))
    return None


# ─────────────────────────────────────────────────────────────────────────────
# NODE EXTRACTION — multiple patterns
# ─────────────────────────────────────────────────────────────────────────────

def extract_nodes_from_html(html):
    """
    Extract (node_id, name) pairs from Amazon page HTML.
    Tries multiple patterns to handle different page formats.
    """
    found = {}

    # ── Pattern 1: href with rh=n%3A{NODE} ─────────────────────────────────
    # /s?i=...&rh=n%3A21839921031 or /s?rh=n:21839921031
    for m in re.finditer(
        r'href="[^"]*(?:rh=n(?:%3A|:)|bbn=)(\d{7,13})[^"]*"',
        html
    ):
        node_id = m.group(1)
        if node_id not in found:
            # Try to find a label near this href
            snippet = html[m.start():m.start() + 300]
            nm = re.search(r'<span[^>]*>\s*([A-Za-z][^<\n(]{2,50}?)\s*</span>', snippet)
            if nm:
                found[node_id] = nm.group(1).strip()
            else:
                found[node_id] = f"node_{node_id}"

    # ── Pattern 2: /b?node={NODE} style links ──────────────────────────────
    for m in re.finditer(r'href="[^"]*[?&]node=(\d{7,13})[^"]*"', html):
        node_id = m.group(1)
        if node_id not in found:
            snippet = html[m.start():m.start() + 300]
            nm = re.search(r'>([A-Za-z][^<\n(]{2,50}?)<', snippet)
            found[node_id] = nm.group(1).strip() if nm else f"node_{node_id}"

    # ── Pattern 3: JSON embedded in page — "id":"21839921031","label":"..." ─
    for m in re.finditer(
        r'"(?:id|nodeId|node_id)"\s*:\s*"(\d{7,13})"[^}]{0,200}"(?:label|name|title)"\s*:\s*"([^"]{2,60})"',
        html
    ):
        node_id, name = m.group(1), m.group(2)
        if node_id not in found:
            found[node_id] = name.strip()

    # ── Pattern 4: data-refinement-id attribute ─────────────────────────────
    for m in re.finditer(r'data-(?:refinement-id|node-id)="(\d{7,13})"', html):
        node_id = m.group(1)
        if node_id not in found:
            snippet = html[m.start():m.start() + 300]
            nm = re.search(r'aria-label="([^"]{2,60})"', snippet)
            found[node_id] = nm.group(1).strip() if nm else f"node_{node_id}"

    # ── Pattern 5: site-directory links /gp/browse or /b/ with node= ────────
    for m in re.finditer(
        r'href="(?:https://www\.amazon\.eg)?(?:/gp/browse(?:\.html)?|/b/)[^"]*[?&]node=(\d{7,13})[^"]*"[^>]*>\s*([^<\n]{2,60})',
        html
    ):
        node_id, name = m.group(1), m.group(2).strip()
        if node_id not in found and name:
            found[node_id] = name

    # ── Pattern 6: script tag JSON arrays with nodeId ───────────────────────
    for m in re.finditer(
        r'"nodeId"\s*:\s*(\d{7,13})[^}]{0,100}"(?:displayString|title|name)"\s*:\s*"([^"]{2,60})"',
        html
    ):
        node_id, name = m.group(1), m.group(2).strip()
        if node_id not in found:
            found[node_id] = name

    # ── Pattern 7: /s?n={NODE} or ?bbn={NODE} ──────────────────────────────
    for m in re.finditer(r'href="[^"]*[?&](?:bbn|n)=(\d{7,13})(?:&[^"]*)?"\s*(?:aria-label="([^"]*)")?', html):
        node_id = m.group(1)
        name = m.group(2) or ""
        if node_id not in found:
            if not name:
                snippet = html[m.start():m.start() + 200]
                nm = re.search(r'>([A-Za-z][^<\n(]{2,50}?)<', snippet)
                name = nm.group(1).strip() if nm else f"node_{node_id}"
            found[node_id] = name.strip()

    return [(nid, name) for nid, name in found.items() if nid and len(nid) >= 7]


def get_product_count(html):
    for pat in [
        r'of (?:over )?([\d,]+) results',
        r'"totalResultCount"\s*:\s*(\d+)',
        r'Showing\s+([\d,]+)\s+results',
        r'"totalCount"\s*:\s*(\d+)',
        r'(\d[\d,]+)\s+results for',
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace(',', ''))
            except ValueError:
                pass
    return -1


# ─────────────────────────────────────────────────────────────────────────────
# SEED — discover initial nodes from special pages
# ─────────────────────────────────────────────────────────────────────────────

SEED_PAGES = [
    # Site directory — comprehensive category list
    f"{BASE}/gp/site-directory",
    f"{BASE}/gp/site-directory?ie=UTF8",
    # Homepage — navigation nodes
    f"{BASE}/",
    # Deals/all departments
    f"{BASE}/gp/goldbox",
]

# Department pages — department pages often expose node IDs in their URL structure
DEPT_PAGES = [
    f"{BASE}/s?i=electronics&rh=n%3Aelectronics",
    f"{BASE}/s?i=fashion-mens-clothing",
    f"{BASE}/s?i=fashion-womens-clothing",
    f"{BASE}/s?i=shoes",
    f"{BASE}/s?i=beauty",
    f"{BASE}/s?i=hpc",
    f"{BASE}/s?i=sporting-goods",
    f"{BASE}/s?i=toys-and-games",
    f"{BASE}/s?i=baby-products",
    f"{BASE}/s?i=kitchen",
    f"{BASE}/s?i=appliances",
    f"{BASE}/s?i=tools",
    f"{BASE}/s?i=automotive",
    f"{BASE}/s?i=luggage",
    f"{BASE}/s?i=jewelry",
    f"{BASE}/s?i=watches",
    f"{BASE}/s?i=furniture",
    f"{BASE}/s?i=office-products",
]

# Keyword searches — these pages often have richer sidebar refinements
KEYWORD_PAGES = [
    (f"{BASE}/s?k=a&i=electronics",             "Electronics (kw)"),
    (f"{BASE}/s?k=a&i=fashion-mens-clothing",   "Men Fashion (kw)"),
    (f"{BASE}/s?k=a&i=fashion-womens-clothing", "Women Fashion (kw)"),
    (f"{BASE}/s?k=a&i=beauty",                  "Beauty (kw)"),
    (f"{BASE}/s?k=a&i=kitchen",                 "Kitchen (kw)"),
    (f"{BASE}/s?k=a&i=sporting-goods",          "Sports (kw)"),
    (f"{BASE}/s?k=a&i=hpc",                     "Health (kw)"),
    (f"{BASE}/s?k=a&i=shoes",                   "Shoes (kw)"),
    (f"{BASE}/s?k=a&i=tools",                   "Tools (kw)"),
    (f"{BASE}/s?k=a&i=automotive",              "Auto (kw)"),
    (f"{BASE}/s?k=a&i=luggage",                 "Luggage (kw)"),
    (f"{BASE}/s?k=a&i=watches",                 "Watches (kw)"),
    (f"{BASE}/s?k=a&i=jewelry",                 "Jewelry (kw)"),
    (f"{BASE}/s?k=a&i=furniture",               "Furniture (kw)"),
    (f"{BASE}/s?k=a&i=toys-and-games",          "Toys (kw)"),
    (f"{BASE}/s?k=a&i=baby-products",           "Baby (kw)"),
    (f"{BASE}/s?k=a&i=stripbooks",              "Books (kw)"),
    (f"{BASE}/s?k=a&i=office-products",         "Office (kw)"),
    (f"{BASE}/s?k=a&i=pet-supplies",            "Pets (kw)"),
    (f"{BASE}/s?k=a&i=grocery",                 "Grocery (kw)"),
    (f"{BASE}/s?k=a&i=garden",                  "Garden (kw)"),
    (f"{BASE}/s?k=a&i=videogames",              "VideoGames (kw)"),
    (f"{BASE}/s?k=a&i=musical-instruments",     "Music (kw)"),
    (f"{BASE}/s?k=a&i=computers",               "Computers (kw)"),
    (f"{BASE}/s?k=a&i=mobile",                  "Mobile (kw)"),
]


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def discover(max_depth=MAX_DEPTH, max_nodes=MAX_NODES, debug=False):
    session  = curl_requests.Session(impersonate="chrome124")
    visited  = set()
    queue    = deque()
    all_nodes = []

    print(f"\n{'='*60}")
    print(f"  Amazon Egypt Node Discovery v2")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  max_depth={max_depth} | max_nodes={max_nodes}")
    print(f"{'='*60}\n")

    # Warmup
    print("  Warming up...")
    try:
        session.get(f"{BASE}/", headers=HEADERS, timeout=20)
        time.sleep(random.uniform(2, 3))
        print("  OK\n")
    except Exception as e:
        print(f"  {e}\n")

    # ── 1. Site directory pages ──────────────────────────────────────────────
    print(f"  [Phase A] Site directory + homepage...")
    for url in SEED_PAGES:
        print(f"    {url}")
        html = _fetch(session, url)
        if not html:
            continue
        nodes = extract_nodes_from_html(html)
        new = [(nid, nm) for nid, nm in nodes if nid not in visited]
        print(f"    -> {len(nodes)} nodes found ({len(new)} new)")
        if debug and len(nodes) == 0:
            # Print 2KB of HTML to help debug
            print("    [DEBUG] HTML snippet:")
            print(html[:2000])
        for nid, nm in new:
            visited.add(nid)
            queue.append((nid, nm, 1, None))
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # ── 2. Department + keyword pages ────────────────────────────────────────
    print(f"\n  [Phase B] Department + keyword pages ({len(DEPT_PAGES) + len(KEYWORD_PAGES)} pages)...")
    for url in DEPT_PAGES:
        html = _fetch(session, url)
        if html:
            nodes = extract_nodes_from_html(html)
            new = [(nid, nm) for nid, nm in nodes if nid not in visited]
            if new:
                print(f"    {url.split('?')[1][:40]} -> {len(new)} new nodes")
            for nid, nm in new:
                visited.add(nid)
                queue.append((nid, nm, 1, None))
        time.sleep(random.uniform(1.0, 2.0))

    for url, label in KEYWORD_PAGES:
        html = _fetch(session, url)
        if html:
            nodes = extract_nodes_from_html(html)
            new = [(nid, nm) for nid, nm in nodes if nid not in visited]
            if new:
                print(f"    {label} -> {len(new)} new nodes")
                if debug:
                    for nid, nm in new[:5]:
                        print(f"      {nid}: {nm}")
            for nid, nm in new:
                visited.add(nid)
                queue.append((nid, nm, 1, None))
        time.sleep(random.uniform(1.0, 2.0))

    print(f"\n  Seed done — {len(queue)} nodes in queue | {len(visited)} visited\n{'─'*60}\n")

    # ── 3. BFS ───────────────────────────────────────────────────────────────
    processed = 0
    while queue and len(all_nodes) < max_nodes:
        node_id, name, depth, parent = queue.popleft()
        processed += 1

        url = f"{BASE}/s?bbn={node_id}&rh=n%3A{node_id}"
        print(f"  [{processed}] d={depth} | {name[:40]:<40} ({node_id})")

        html = _fetch(session, url)
        if not html:
            continue

        count    = get_product_count(html)
        children = extract_nodes_from_html(html)
        new_ch   = [(nid, nm) for nid, nm in children if nid not in visited and nid != node_id]

        print(f"    count={count:,} | children={len(new_ch)}")

        is_leaf = len(new_ch) == 0 or depth >= max_depth

        if count >= MIN_PRODS or count == -1:
            all_nodes.append({
                "node_id": node_id,
                "name":    name,
                "depth":   depth,
                "parent":  parent,
                "count":   count,
                "is_leaf": is_leaf,
                "url":     url,
            })

        if not is_leaf:
            for nid, nm in new_ch:
                visited.add(nid)
                queue.append((nid, nm, depth + 1, node_id))

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return all_nodes


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-depth", type=int, default=MAX_DEPTH)
    parser.add_argument("--max-nodes", type=int, default=MAX_NODES)
    parser.add_argument("--debug",     action="store_true")
    args = parser.parse_args()

    nodes = discover(args.max_depth, args.max_nodes, args.debug)

    leaves  = [n for n in nodes if n["is_leaf"]]
    est     = sum(min(n["count"], 400) * 4 for n in leaves if n["count"] > 0)

    print(f"\n{'='*60}")
    print(f"  DISCOVERY COMPLETE")
    print(f"  Total nodes   : {len(nodes):,}")
    print(f"  Leaf nodes    : {len(leaves):,}  <- scraped")
    print(f"  Est. products : {est:,}  (leaves x min(count,400) x 4sorts)")
    print(f"{'='*60}")

    if leaves:
        print(f"\n  Top 20 leaf nodes:")
        for n in sorted(leaves, key=lambda x: x["count"], reverse=True)[:20]:
            print(f"    {n['count']:>8,}  {n['name']:<40} {n['node_id']}")

    Path("amazon_nodes.json").write_text(
        json.dumps(nodes, ensure_ascii=False, indent=2), encoding="utf-8")
    Path("amazon_leaf_nodes.json").write_text(
        json.dumps(leaves, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n  Saved amazon_nodes.json ({len(nodes)} nodes)")
    print(f"  Saved amazon_leaf_nodes.json ({len(leaves)} leaf nodes)\n")


if __name__ == "__main__":
    main()
