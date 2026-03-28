import json, glob, csv, sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
merged = []
for fname in sorted(glob.glob("node_*.json")):
    try:
        data = json.loads(open(fname, encoding="utf-8").read())
        merged.extend(data)
        print(f"{fname}: {len(data)} products")
    except: pass
seen, unique = set(), []
for p in merged:
    asin = p.get("asin", "")
    if asin and asin not in seen:
        seen.add(asin)
        unique.append(p)
print(f"Total raw: {len(merged)}, Unique: {len(unique)}")
columns = ["platform","asin","title","brand","category","current_price","original_price",
           "discount","rating","reviews_count","prime_eligible","amazon_choice",
           "sponsored","main_image","product_url","scraped_at"]
with open("amazon_products_nodes.csv", "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(unique)
print(f"Saved: amazon_products_nodes.csv ({len(unique)} products)")
