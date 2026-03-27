"""
amazon_node_discovery.py — Phase 1
====================================
يكتشف كل browse node IDs في Amazon Egypt عن طريق BFS على الـ category tree.
يبدأ من الـ top-level departments ويمشي recursive لحد الـ leaf nodes.

Output: amazon_nodes.json
  [{"node_id": "21839921031", "name": "Smartphones", "depth": 2, "count": 1234, "parent": "21839920031"}, ...]

Usage:
  python amazon_node_discovery.py
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
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

BASE = "https://www.amazon.eg"

MAX_DEPTH      = 6      # max depth of tree traversal
MIN_PRODUCTS   = 5      # ignore nodes with fewer products
MAX_NODES      = 10000  # safety cap
DELAY_MIN      = 1.5
DELAY_MAX      = 3.5

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
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

# Top-level Amazon Egypt departments — entry points for BFS
SEED_DEPARTMENTS = [
    ("Electronics",          f"{BASE}/s?i=electronics"),
    ("Computers",            f"{BASE}/s?i=computers"),
    ("Mobiles",              f"{BASE}/s?i=mobile"),
    ("Home & Kitchen",       f"{BASE}/s?i=kitchen"),
    ("Appliances",           f"{BASE}/s?i=appliances"),
    ("Men's Fashion",        f"{BASE}/s?i=fashion-mens-clothing"),
    ("Women's Fashion",      f"{BASE}/s?i=fashion-womens-clothing"),
    ("Girls Fashion",        f"{BASE}/s?i=fashion-girls-clothing"),
    ("Boys Fashion",         f"{BASE}/s?i=fashion-boys-clothing"),
    ("Shoes",                f"{BASE}/s?i=shoes"),
    ("Beauty",               f"{BASE}/s?i=beauty"),
    ("Health & Personal",    f"{BASE}/s?i=hpc"),
    ("Sports",               f"{BASE}/s?i=sporting-goods"),
    ("Toys & Games",         f"{BASE}/s?i=toys-and-games"),
    ("Baby Products",        f"{BASE}/s?i=baby-products"),
    ("Books",                f"{BASE}/s?i=stripbooks"),
    ("Automotive",           f"{BASE}/s?i=automotive"),
    ("Office Products",      f"{BASE}/s?i=office-products"),
    ("Pet Supplies",         f"{BASE}/s?i=pet-supplies"),
    ("Grocery",              f"{BASE}/s?i=grocery"),
    ("Tools & Hardware",     f"{BASE}/s?i=tools"),
    ("Garden & Outdoor",     f"{BASE}/s?i=garden"),
    ("Luggage & Bags",       f"{BASE}/s?i=luggage"),
    ("Watches",              f"{BASE}/s?i=watches"),
    ("Jewelry",              f"{BASE}/s?i=jewelry"),
    ("Video Games",          f"{BASE}/s?i=videogames"),
    ("Musical Instruments",  f"{BASE}/s?i=musical-instruments"),
    ("Furniture",            f"{BASE}/s?i=furniture"),
    ("Lighting",             f"{BASE}/s?i=lighting"),
    ("Stationery",           f"{BASE}/s?i=office-products"),
    ("Cleaning",             f"{BASE}/s?i=grocery&rh=n%3A21839921031"),
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _fetch(session, url, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and len(r.text) > 5000:
                return r.text
            if r.status_code == 503:
                print(f"    [503] Waiting 30s...")
                time.sleep(30)
            else:
                time.sleep(random.uniform(3, 6))
        except Exception as e:
            print(f"    [ERROR] {e} (attempt {attempt+1})")
            time.sleep(random.uniform(5, 10))
    return None


def extract_child_nodes(html):
    """
    Extract child node IDs + names + product counts from Amazon sidebar HTML.
    Returns list of (node_id, name, count).
    """
    results = {}

    # Pattern 1: sidebar filter links with rh=n%3A{NODE}
    # Matches: href="/s?i=...&rh=n%3A12345678..." > <span>Name</span> (123)
    for m in re.finditer(
        r'href="(/s\?[^"]*rh=n(?:%3A|:)(\d{7,13})[^"]*)"[^>]*>(?:\s*<[^/][^>]*>)*\s*([^\n<(]{2,60})',
        html
    ):
        href, node_id, raw_name = m.groups()
        name = raw_name.strip().strip('\u200e\u200f')
        if name and not name.startswith('&') and len(name) > 1:
            if node_id not in results:
                results[node_id] = (name, 0)

    # Pattern 2: bbn=NODE_ID in href (branch nodes)
    for m in re.finditer(r'href="(/s\?[^"]*bbn=(\d{7,13})[^"]*)"', html):
        href, node_id = m.groups()
        if node_id not in results:
            # Try to find a name near this href
            snippet = html[max(0, m.start()-100):m.end()+200]
            name_m = re.search(r'<span[^>]*>\s*([A-Za-z][^\n<(]{2,50}?)\s*</span>', snippet)
            name = name_m.group(1).strip() if name_m else f"node_{node_id}"
            results[node_id] = (name, 0)

    # Try to extract counts — look for (N,NNN) near each link
    for node_id in list(results.keys()):
        name, _ = results[node_id]
        # Search for this node in HTML and look for count nearby
        pos = html.find(node_id)
        if pos > 0:
            snippet = html[pos:pos+300]
            count_m = re.search(r'\(([\d,]+)\)', snippet)
            if count_m:
                try:
                    count = int(count_m.group(1).replace(',', ''))
                    results[node_id] = (name, count)
                except ValueError:
                    pass

    return [(nid, name, count) for nid, (name, count) in results.items()]


def get_product_count(html):
    """Extract total product count shown on Amazon page."""
    # "1-16 of over 1,000 results" / "1-16 of 234 results"
    m = re.search(r'of (?:over )?([\d,]+) results', html, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except ValueError:
            pass
    # JSON embedded
    m = re.search(r'"totalResultCount"\s*:\s*(\d+)', html)
    if m:
        return int(m.group(1))
    # "Showing X results"
    m = re.search(r'Showing\s+([\d,]+)\s+results', html, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except ValueError:
            pass
    return -1  # unknown


def node_url(node_id):
    return f"{BASE}/s?bbn={node_id}&rh=n%3A{node_id}"


# ─────────────────────────────────────────────────────────────────────────────
# BFS DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def discover(max_depth=MAX_DEPTH, max_nodes=MAX_NODES):
    session  = curl_requests.Session(impersonate="chrome124")
    visited  = set()   # node IDs already processed
    queue    = deque() # (node_id, name, depth, parent_id)
    all_nodes = []     # final output

    print(f"\n{'='*60}")
    print(f"  Amazon Egypt Node Discovery")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Max depth: {max_depth} | Max nodes: {max_nodes}")
    print(f"{'='*60}\n")

    # ── Warmup ──────────────────────────────────────────────────
    print("  Warming up on Amazon Egypt homepage...")
    try:
        session.get(f"{BASE}/", headers=HEADERS, timeout=20)
        time.sleep(random.uniform(2, 4))
        print("  Warmup OK\n")
    except Exception as e:
        print(f"  Warmup failed: {e}\n")

    # ── Seed: scrape top-level departments ───────────────────────
    print(f"  Scraping {len(SEED_DEPARTMENTS)} seed departments...\n")
    for dept_name, dept_url in SEED_DEPARTMENTS:
        print(f"  [{dept_name}] {dept_url}")
        html = _fetch(session, dept_url)
        if not html:
            print(f"    -> FAILED\n")
            continue

        count = get_product_count(html)
        children = extract_child_nodes(html)
        print(f"    -> {count:,} products | {len(children)} child nodes found")

        for node_id, name, node_count in children:
            if node_id not in visited:
                queue.append((node_id, name, 1, None))
                visited.add(node_id)

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    print(f"\n  Seed done — {len(queue)} nodes in queue\n{'─'*60}\n")

    # ── BFS ─────────────────────────────────────────────────────
    processed = 0
    while queue and len(all_nodes) < max_nodes:
        node_id, name, depth, parent = queue.popleft()
        processed += 1

        url   = node_url(node_id)
        print(f"  [{processed}] depth={depth} | {name} (node {node_id})")

        html = _fetch(session, url)
        if not html:
            print(f"    -> FAILED, skipping\n")
            continue

        count    = get_product_count(html)
        children = extract_child_nodes(html)

        # Filter out already-visited children
        new_children = [(nid, n, c) for nid, n, c in children if nid not in visited]

        print(f"    -> {count:,} products | {len(new_children)} new children")

        # Determine if this is a leaf node:
        # A node is a leaf if it has no new children OR max depth reached
        is_leaf = (len(new_children) == 0) or (depth >= max_depth)

        if count >= MIN_PRODUCTS or count == -1:
            all_nodes.append({
                "node_id":  node_id,
                "name":     name,
                "depth":    depth,
                "parent":   parent,
                "count":    count,
                "is_leaf":  is_leaf,
                "url":      url,
            })
            print(f"    -> {'LEAF' if is_leaf else 'BRANCH'} node saved ({len(all_nodes)} total)")
        else:
            print(f"    -> Skipped (only {count} products)")

        # Enqueue children
        if not is_leaf:
            for nid, n, _ in new_children:
                visited.add(nid)
                queue.append((nid, n, depth + 1, node_id))
                if len(visited) % 100 == 0:
                    print(f"    [Queue: {len(queue)} | Visited: {len(visited)}]")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return all_nodes


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-depth", type=int, default=MAX_DEPTH)
    parser.add_argument("--max-nodes", type=int, default=MAX_NODES)
    args = parser.parse_args()

    nodes = discover(max_depth=args.max_depth, max_nodes=args.max_nodes)

    # Summary
    leaf_nodes   = [n for n in nodes if n["is_leaf"]]
    branch_nodes = [n for n in nodes if not n["is_leaf"]]

    print(f"\n{'='*60}")
    print(f"  DISCOVERY COMPLETE")
    print(f"  Total nodes   : {len(nodes):,}")
    print(f"  Leaf nodes    : {len(leaf_nodes):,}  ← these get scraped")
    print(f"  Branch nodes  : {len(branch_nodes):,}")
    est = sum(min(n['count'], 400) for n in leaf_nodes if n['count'] > 0)
    print(f"  Est. products : {est:,}  (leaf nodes × min(count,400))")
    print(f"{'='*60}\n")

    # Save
    out = Path("amazon_nodes.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(nodes, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {out} ({out.stat().st_size // 1024} KB)\n")

    # Save leaf-only file for workflows
    out_leaf = Path("amazon_leaf_nodes.json")
    with open(out_leaf, "w", encoding="utf-8") as f:
        json.dump(leaf_nodes, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {out_leaf} ({len(leaf_nodes)} leaf nodes)\n")


if __name__ == "__main__":
    main()
