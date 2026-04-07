#!/usr/bin/env python3
"""
amazon_enricher_gh.py  —  GitHub Actions Amazon Detail Enricher
يسكريب صفحة المنتج لكل ASIN ويجيب:
  bullet_points, tech_specs, description, brand, rating, reviews,
  coupon, delivery, availability

Usage:
  python amazon_enricher_gh.py --chunk 0 --total-chunks 20
  python amazon_enricher_gh.py --chunk 0 --total-chunks 20 --input urls.json
"""

import argparse, json, random, re, sys, time
from pathlib import Path
from datetime import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

try:
    from curl_cffi import requests as cr
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "curl-cffi", "-q"])
    from curl_cffi import requests as cr

try:
    from scrapy import Selector
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scrapy", "-q"])
    from scrapy import Selector

# ─── Anti-block ───────────────────────────────────────────────────────────────
IMPERSONATIONS = [
    "chrome131","chrome124","chrome120","chrome110","chrome107","chrome104",
    "safari17_0","safari15_5","safari15_3",
]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]
ACCEPT_LANGS = [
    "en-US,en;q=0.9", "en-GB,en;q=0.9",
    "en-US,en;q=0.9,ar-EG;q=0.8", "en-US,en;q=0.8",
]
REFERERS = [
    "https://www.google.com/",
    "https://www.amazon.eg/",
    "https://www.amazon.eg/s?k=products",
    "https://www.google.com/search?q=amazon+egypt",
]

def new_session():
    s = cr.Session(impersonate=random.choice(IMPERSONATIONS))
    s.headers.update({
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         random.choice(REFERERS),
        "Cache-Control":   random.choice(["no-cache", "max-age=0"]),
        "Sec-Fetch-Dest":  "document",
        "Sec-Fetch-Mode":  "navigate",
        "Sec-Fetch-Site":  random.choice(["none", "same-origin", "cross-site"]),
        "Sec-Fetch-User":  "?1",
        "DNT":             random.choice(["1", "0"]),
    })
    return s

def human_delay():
    r = random.random()
    if r < 0.50:
        d = random.uniform(1.5, 4.0)
    elif r < 0.80:
        d = random.uniform(4.0, 9.0)
    elif r < 0.95:
        d = random.uniform(9.0, 18.0)
    else:
        d = random.uniform(20.0, 45.0)   # 5% pause طويلة
    # Extra random jitter
    d += random.uniform(0, 1.5)
    time.sleep(d)

def fetch(url, retries=4):
    for attempt in range(retries):
        if attempt > 0:
            time.sleep(random.uniform(15, 40) * attempt)
        try:
            s = new_session()
            r = s.get(url, timeout=30)
            if r.status_code == 200 and len(r.text) > 2000:
                if "captcha" in r.text.lower() or "robot" in r.text.lower():
                    print(f"  CAPTCHA detected — waiting {60}s", flush=True)
                    time.sleep(60)
                    continue
                return r.text
            if r.status_code == 404:
                return None
            print(f"  [{r.status_code}] {url[:60]} attempt {attempt+1}", flush=True)
        except Exception as e:
            print(f"  ERR attempt {attempt+1}: {e}", flush=True)
    return None

# ─── Parser ───────────────────────────────────────────────────────────────────
def parse_amazon(html, product_id, product_url):
    sel = Selector(text=html)
    d   = {"product_id": product_id, "product_url": product_url}

    # Title
    d["title"] = sel.css("#productTitle::text").get(default="").strip()

    # Brand
    d["brand"] = (
        sel.css("#bylineInfo span.a-color-secondary::text").get()
        or sel.css("#bylineInfo a::text").get()
        or sel.css("#bylineInfo::text").get() or ""
    ).strip().replace("Visit the","").replace("Brand:","").replace("Store","").strip()

    # Price
    whole = sel.css("span.a-price-whole::text").get(default="").strip().replace(",","").replace(".","")
    frac  = sel.css("span.a-price-fraction::text").get(default="00").strip()
    d["current_price"] = f"{whole}.{frac} EGP" if whole.isdigit() else ""

    orig = sel.css("span.a-price.a-text-price span.a-offscreen::text").get(default="").strip()
    d["original_price"] = orig if orig and orig != d["current_price"] else ""

    # Rating
    rating_raw = sel.css("span[data-hook='rating-out-of-text']::text").get(default="")
    m = re.search(r"([\d.]+)\s*out of", rating_raw)
    d["rating"] = m.group(1) if m else sel.css("#acrPopover span.a-size-medium::text").get(default="").strip()

    # Reviews
    rev_raw = sel.css("#acrCustomerReviewText::text").get(default="")
    m2 = re.search(r"[\d,،]+", rev_raw)
    d["reviews_count"] = m2.group().replace(",","").replace("،","") if m2 else ""

    # Coupon
    coupon_el = (
        sel.css("#couponBadgeRegularVpc::text").get()
        or sel.css("label[id*='coupon']::text").get()
        or sel.css("div[id*='coupon'] span::text").get() or ""
    )
    coupon_all = " ".join(sel.css("[id*='coupon']::text,[class*='coupon']::text").getall()).lower()
    d["has_coupon"]    = 1 if coupon_el.strip() or "coupon" in coupon_all else 0
    d["coupon_detail"] = coupon_el.strip()

    # Deal / Flash
    badge_all = " ".join(sel.css(
        "span[id*='deal']::text,div[id*='deal'] span::text,"
        "span[class*='badge']::text,#apex_offerDisplay_desktop span::text"
    ).getall()).lower()
    d["has_deal"]       = 1 if any(x in badge_all for x in ["limited time","today's deal","save "]) else 0
    d["has_flash_sale"] = 1 if any(x in badge_all for x in ["lightning deal","flash deal"]) else 0

    # Delivery
    delivery = " ".join(t.strip() for t in sel.css(
        "#mir-layout-DELIVERY_BLOCK span::text,"
        "#deliveryBlockMessage span::text,"
        "#ddmDeliveryMessage span::text"
    ).getall() if t.strip())
    d["free_delivery"]    = 1 if "free" in delivery.lower() else 0
    d["express_delivery"] = 1 if sel.css("#primeAccordionRow,#primeLogo,i.a-icon-prime").get() else 0

    # Availability
    d["availability"] = sel.css("#availability span::text").get(default="").strip()

    # Bullet points
    bullets = [b.strip() for b in sel.css("#feature-bullets li span::text").getall()
               if b.strip() and len(b.strip()) > 5 and "list" not in b.lower()]
    d["bullet_points"] = json.dumps(bullets[:10], ensure_ascii=False) if bullets else ""

    # Tech specs
    specs = {}
    for row in sel.css(
        "#productDetails_techSpec_section_1 tr,"
        "#productDetails_detailBullets_sections1 tr,"
        "#productDetails_techSpec_section_2 tr"
    ):
        k = row.css("th::text").get(default="").strip()
        v = row.css("td::text").get(default="").strip()
        if k and v and len(k) < 60:
            specs[k] = v[:200]
    for li in sel.css("#detailBullets_feature_div li"):
        texts = [t.strip().strip(":\u200E\u200e") for t in li.css("span::text").getall() if t.strip()]
        if len(texts) >= 2:
            specs[texts[0]] = " ".join(texts[1:])[:200]
    d["tech_specs"] = json.dumps(specs, ensure_ascii=False) if specs else ""

    # Description
    desc = " ".join(sel.css("#productDescription p::text, #productDescription span::text").getall()).strip()
    d["description"] = desc[:800] if desc else ""

    # Main image
    img_raw = sel.css("#imgBlkFront::attr(data-a-dynamic-image), #landingImage::attr(data-a-dynamic-image)").get(default="")
    if img_raw:
        try:
            imgs = json.loads(img_raw)
            d["main_image"] = max(imgs, key=lambda u: imgs[u][0] * imgs[u][1]) if imgs else ""
        except Exception:
            d["main_image"] = ""

    d["enriched_at"] = datetime.now().isoformat()
    return d


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk",        type=int, required=True, help="Chunk index (0-based)")
    ap.add_argument("--total-chunks", type=int, required=True, help="Total number of chunks")
    ap.add_argument("--input",  default="amazon_enrich_urls.json", help="Input JSON file with [{product_id, product_url}]")
    ap.add_argument("--output", default="",  help="Output JSON file (default: enriched_chunk_N.json)")
    args = ap.parse_args()

    input_file  = Path(args.input)
    output_file = Path(args.output) if args.output else Path(f"enriched_chunk_{args.chunk}.json")

    if not input_file.exists():
        print(f"ERROR: {input_file} not found", flush=True)
        sys.exit(1)

    all_items = json.loads(input_file.read_text(encoding="utf-8"))
    total     = len(all_items)

    # Split into chunks
    chunk_size = (total + args.total_chunks - 1) // args.total_chunks
    start = args.chunk * chunk_size
    end   = min(start + chunk_size, total)
    items = all_items[start:end]

    print(f"Chunk {args.chunk}/{args.total_chunks-1}: items {start}-{end-1} ({len(items)} products)", flush=True)
    print(f"Output: {output_file}", flush=True)

    results  = []
    ok = fail = captcha = 0

    # Random start stagger (avoid all chunks hitting at same time)
    stagger = random.uniform(0, 30)
    print(f"Start stagger: {stagger:.1f}s", flush=True)
    time.sleep(stagger)

    # Shuffle order within chunk for extra randomness
    random.shuffle(items)

    for i, item in enumerate(items):
        pid = item["product_id"]
        url = item["product_url"]

        human_delay()

        html = fetch(url)
        if not html:
            fail += 1
            print(f"  [{i+1}/{len(items)}] FAIL {pid}", flush=True)
            results.append({"product_id": pid, "product_url": url, "_status": "fail"})
            continue

        try:
            data = parse_amazon(html, pid, url)
            has_data = bool(data.get("bullet_points") or data.get("tech_specs") or data.get("description"))
            results.append(data)
            ok += 1
            bp = len(json.loads(data["bullet_points"])) if data.get("bullet_points") else 0
            sp = len(json.loads(data["tech_specs"])) if data.get("tech_specs") else 0
            print(f"  [{i+1}/{len(items)}] OK {pid} | bullets={bp} specs={sp} brand={data.get('brand','')[:20]}", flush=True)
        except Exception as e:
            fail += 1
            print(f"  [{i+1}/{len(items)}] PARSE ERR {pid}: {e}", flush=True)
            results.append({"product_id": pid, "product_url": url, "_status": "parse_err"})

        # Save intermediate every 50 products
        if (i + 1) % 50 == 0:
            output_file.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  Saved {len(results)} results so far (ok={ok} fail={fail})", flush=True)

    # Final save
    output_file.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nDONE: ok={ok} fail={fail} total={len(items)}", flush=True)
    print(f"Output: {output_file} ({output_file.stat().st_size/1024:.0f} KB)", flush=True)


if __name__ == "__main__":
    main()
