#!/usr/bin/env python3
"""
Jumia Egypt Scraper
===================
Dynamically discovers all categories and scrapes products with full details.

Technology:
  - curl-cffi  : Chrome131 TLS fingerprint → bypasses bot detection
  - asyncio    : Concurrent page/detail fetching
  - BeautifulSoup + lxml : HTML parsing

Modes:
  discover   Find all categories → categories.json + CSV
  scrape     Scrape one category → products_N.json   (used by GH Actions matrix)
  export     Merge all products_*.json → jumia_products_DATE.csv

Usage:
  python jumia_spider.py --mode discover
  python jumia_spider.py --mode scrape \\
      --category-name "Phones & Tablets" \\
      --category-url  "https://www.jumia.com.eg/phones-tablets/" \\
      --group-index 0 --limit 5000 --deep
  python jumia_spider.py --mode export
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
import math
from typing import Any, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL         = "https://www.jumia.com.eg"
PRODUCT_LIMIT    = 5000
MAX_PAGES        = 200
PAGE_CONCURRENCY = 8      # concurrent listing pages per category
DETAIL_CONCURRENCY = 8    # concurrent product detail fetches
PAGE_DELAY       = (0.8, 1.8)    # seconds between page requests
DETAIL_DELAY     = (0.2, 0.5)    # seconds between detail requests (Playwright handles its own latency)
RATE_LIMIT_PAUSE = (15, 45)      # seconds to wait on 429
SERVER_ERR_PAUSE = (30, 60)      # seconds to wait on 5xx
REQUEST_TIMEOUT  = 30

# Chrome 131-compatible User-Agents (matches impersonate="chrome131")
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
]

# Fallback categories if dynamic discovery fails
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

# Sort orders — كل sort بيجيب منتجات مختلفة
JUMIA_SORT_ORDERS = [
    ("default",    ""),
    ("price_asc",  "?sort[by]=price&sort[dir]=asc"),
    ("price_desc", "?sort[by]=price&sort[dir]=desc"),
    ("newest",     "?sort[by]=date&sort[dir]=desc"),
    ("top_rated",  "?sort[by]=rating&sort[dir]=desc"),
]

CSV_FIELDS = [
    "product_id", "title", "brand", "category",
    "price_egp", "original_price", "discount_percent",
    "rating", "reviews_count", "availability",
    "jumia_express", "jumia_verified",
    "sold_by", "ships_from", "delivery_date",
    "main_image", "all_images",
    "description", "weight", "dimensions",
    "model_number", "country_of_origin", "warranty",
    "tech_specs", "variations",
    "product_url", "scraped_at",
]

# URL patterns to exclude from category discovery
_EXCLUDE_RE = re.compile(
    r"/(account|cart|help|seller|vendor|blog|deals|flash-sale|flash-sales|"
    r"new-arrivals|featured|bestsellers|login|register|wishlist|"
    r"about|contact|policy|terms|sitemap|search|checkout|"
    r"membership|loyalty|return|recommended|sp-|category-fashion|"
    r"mlp-)(/|$)|^/mlp-|^/sp-",
    re.IGNORECASE,
)

# Generic link texts that should be replaced with slug-derived names
_GENERIC_NAMES = frozenset({
    "see all", "view all", "shop now", "shop all", "more", "all",
    "explore", "browse", "click here", "learn more",
})

# UA pinned when Playwright is active (all requests use the same UA as the browser)
_pinned_ua: Optional[str] = None

# Playwright browser state — set by _init_playwright(), used by _pw_fetch()
_pw_playwright: Optional[Any] = None
_pw_browser:    Optional[Any] = None
_pw_context:    Optional[Any] = None
_pw_semaphore:  Optional[asyncio.Semaphore] = None
_pw_stealth:    Optional[Any] = None   # playwright-stealth async function if available
PW_CONCURRENCY = 12   # max concurrent Playwright tabs (each ~50-100 MB, 7GB RAM avail)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

import io
_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(_stdout)],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def build_headers(referer: str = BASE_URL) -> dict:
    """Build realistic Chrome131 browser headers."""
    ua = _pinned_ua or random.choice(USER_AGENTS)
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "sec-ch-ua": '"Chromium";v="131", "Google Chrome";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-user": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }
    if referer and referer != BASE_URL:
        headers["Referer"] = referer
        headers["sec-fetch-site"] = "same-origin"
    else:
        headers["sec-fetch-site"] = "none"
    return headers


def parse_price(text: Optional[str]) -> Optional[float]:
    """'EGP 1,299.00'  →  1299.0"""
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def parse_rating(style: Optional[str]) -> Optional[float]:
    """'width:60%'  →  3.0  (60 / 20)"""
    if not style:
        return None
    m = re.search(r"width:\s*([\d.]+)%", style)
    if not m:
        return None
    try:
        return round(float(m.group(1)) / 20, 1)
    except ValueError:
        return None


def parse_reviews(text: Optional[str]) -> Optional[int]:
    """'(1,234)'  →  1234"""
    if not text:
        return None
    cleaned = re.sub(r"[^\d]", "", text)
    try:
        return int(cleaned) if cleaned else None
    except ValueError:
        return None


def to_abs_url(href: str) -> str:
    """Relative URL → absolute."""
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return BASE_URL + (href if href.startswith("/") else "/" + href)


def slug_of(url: str) -> str:
    """'/phones-tablets/'  ->  'phones-tablets'"""
    path = url.rstrip("/").split("/")[-1]
    return re.sub(r"[^\w-]", "-", path) or "category"


def slug_to_name(slug: str) -> str:
    """'phones-tablets'  ->  'Phones & Tablets' using common mappings."""
    _MAP = {
        "phones-tablets": "Phones & Tablets",
        "health-beauty": "Health & Beauty",
        "home-office": "Home & Office",
        "sporting-goods": "Sporting Goods",
        "baby-products": "Baby Products",
        "toys-games": "Toys & Games",
        "musical-instruments": "Musical Instruments",
        "pet-supplies": "Pet Supplies",
        "office-supplies": "Office Supplies",
        "bags-luggage": "Bags & Luggage",
        "video-games": "Video Games",
        "mens-clothing": "Men's Fashion",
        "womens-clothing": "Women's Fashion",
        "kids-fashion": "Kids' Fashion",
        "home-kitchen-storage-organization": "Home & Kitchen",
        "electronic-television-video": "Electronics & TV",
        "home-improvement-appliances": "Home Appliances",
        "kitchen-dinning-small-appliances": "Kitchen Appliances",
        "kitchen-utensils-gadgets": "Kitchen & Gadgets",
        "air-conditioning": "Air Conditioning",
        "home-decor": "Home Decor",
        "hair-care-d": "Hair Care",
    }
    if slug in _MAP:
        return _MAP[slug]
    # Generic conversion: replace hyphens with spaces, title-case
    return " ".join(w.capitalize() for w in slug.replace("-d", "").split("-") if w)


# ─────────────────────────────────────────────────────────────────────────────
# PLAYWRIGHT BROWSER  (primary HTTP engine — bypasses Cloudflare Bot Management)
# ─────────────────────────────────────────────────────────────────────────────

async def _init_playwright(target_url: Optional[str] = None) -> bool:
    """
    Start a persistent headless Chromium context with stealth patches.
    All fetch() calls route through this context when it is active.
    target_url: if provided, it is added as the final warmup URL (5 retries)
                so Cloudflare establishes a session for that exact path.
    Returns True on success.
    """
    global _pw_playwright, _pw_browser, _pw_context, _pw_semaphore, _pinned_ua, _pw_stealth

    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("playwright not installed — falling back to curl-cffi (may get 403)")
        return False

    log.info("Initializing Playwright Chromium (stealth) …")
    try:
        _pw_playwright = await async_playwright().start()
        _pw_browser = await _pw_playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
            ],
        )
        _pw_context = await _pw_browser.new_context(
            user_agent=ua,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Africa/Cairo",
        )
        # Apply playwright-stealth for comprehensive anti-fingerprinting
        try:
            from playwright_stealth import stealth_async  # type: ignore
            _pw_stealth = stealth_async
            log.info("playwright-stealth available — will apply stealth to each page")
        except ImportError:
            _pw_stealth = None
            log.warning("playwright-stealth not installed — using basic patches only")

        # Basic fallback stealth patches (used when playwright-stealth is not available)
        await _pw_context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){}, app: {} };
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) =>
                parameters.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(parameters);
        """)
        # Block images/fonts — speeds up page loads without hurting HTML content
        await _pw_context.route(
            "**/*.{png,jpg,jpeg,gif,svg,webp,ico,woff,woff2,ttf,eot}",
            lambda r: r.abort(),
        )
        _pw_semaphore = asyncio.Semaphore(PW_CONCURRENCY)
        _pinned_ua = ua

        # Warmup: load pages to establish a robust Cloudflare session.
        # Each entry: (url, max_attempts, retry_sleep_seconds, referer_for_goto)
        # The target category URL uses BASE_URL as referer (simulates navigating from
        # the homepage) and gets 5 attempts with 20s retry sleep.
        warmup_entries: list[tuple[str, int, int, str]] = [
            (BASE_URL,                          2, 10, ""),
            (f"{BASE_URL}/phones-tablets/",     2, 10, BASE_URL),
            (f"{BASE_URL}/electronics/",        2, 10, BASE_URL),
        ]
        if target_url:
            abs_target = target_url if target_url.startswith("http") else BASE_URL + target_url
            warmup_entries.append((abs_target, 5, 20, BASE_URL))

        for warmup_url, max_wup_attempts, wup_retry_sleep, wup_referer in warmup_entries:
            for wup_attempt in range(max_wup_attempts):
                page = await _pw_context.new_page()
                try:
                    if _pw_stealth:
                        await _pw_stealth(page)
                    goto_kw: dict = {"wait_until": "domcontentloaded", "timeout": 30000}
                    if wup_referer:
                        goto_kw["referer"] = wup_referer
                    resp = await page.goto(warmup_url, **goto_kw)
                    status = resp.status if resp else 0
                    title = await page.title()
                    slug = warmup_url.rstrip("/").split("/")[-1] or "home"

                    # If Cloudflare serves a JS challenge ("Just a moment..."),
                    # poll for up to 90s for Chrome to solve it and redirect.
                    if "just a moment" in title.lower():
                        log.info(f"Warmup [{slug}]: CF challenge — polling 90s for Chrome to solve …")
                        for _ in range(18):  # 18 × 5s = 90s max
                            await asyncio.sleep(5)
                            title = await page.title()
                            if "just a moment" not in title.lower():
                                status = 200
                                log.info(f"Warmup [{slug}] CF challenge solved — title={title[:40]!r}")
                                break
                        else:
                            log.warning(f"Warmup [{slug}] CF challenge not solved after 90s — retrying")

                    if status == 200 and "just a moment" not in title.lower():
                        log.info(f"Warmup [{slug}] OK — HTTP {status}, title={title[:45]!r}")
                        await asyncio.sleep(2.5)
                        break  # success — move to next warmup URL
                    else:
                        log.warning(
                            f"Warmup [{slug}] attempt {wup_attempt+1}/{max_wup_attempts}: "
                            f"HTTP {status}, title={title[:40]!r} — retrying in {wup_retry_sleep}s"
                        )
                        await asyncio.sleep(wup_retry_sleep)
                except Exception as e:
                    log.warning(f"Warmup {warmup_url}: {e}")
                finally:
                    await page.close()

        return True
    except Exception as exc:
        log.error(f"Playwright init failed: {exc}")
        _pw_context = None
        return False


async def _close_playwright() -> None:
    """Shut down the Playwright browser cleanly."""
    global _pw_playwright, _pw_browser, _pw_context
    try:
        if _pw_browser:
            await _pw_browser.close()
        if _pw_playwright:
            await _pw_playwright.stop()
    except Exception:
        pass
    finally:
        _pw_browser = None
        _pw_context = None
        _pw_playwright = None


async def _pw_fetch(
    url: str,
    referer: str = BASE_URL,
    max_attempts: int = 3,
) -> Optional[str]:
    """Fetch a URL through the persistent Playwright context (Cloudflare-safe)."""
    if _pw_context is None or _pw_semaphore is None:
        return None

    for attempt in range(1, max_attempts + 1):
        async with _pw_semaphore:
            page = None
            try:
                await asyncio.sleep(random.uniform(1.0, 2.5))
                page = await _pw_context.new_page()
                if _pw_stealth:
                    await _pw_stealth(page)
                goto_kw: dict = {"wait_until": "domcontentloaded", "timeout": 30000}
                if referer:
                    goto_kw["referer"] = referer
                resp = await page.goto(url, **goto_kw)
                status = resp.status if resp else 0

                # Cloudflare JS challenge: poll for up to 90s for Chrome to solve it.
                title_text = await page.title()
                if "just a moment" in title_text.lower():
                    log.info(f"CF challenge attempt {attempt} — polling 90s  [{url[:60]}]")
                    for _ in range(18):  # 18 × 5s = 90s max
                        await asyncio.sleep(5)
                        title_text = await page.title()
                        if "just a moment" not in title_text.lower():
                            log.info(f"CF challenge solved  [{url[:60]}]")
                            return await page.content()
                    log.warning(f"CF challenge not solved after 90s (attempt {attempt}/{max_attempts})  [{url[:60]}]")
                    if attempt < max_attempts:
                        await asyncio.sleep(5)
                    continue

                if status == 200:
                    return await page.content()
                elif status == 429:
                    pause = random.uniform(*RATE_LIMIT_PAUSE)
                    log.warning(f"429 (PW) → pause {pause:.0f}s  [{url[:60]}]")
                    await asyncio.sleep(pause)
                elif status == 403:
                    log.warning(f"403 (PW, attempt {attempt}/{max_attempts})  [{url[:60]}]")
                    if attempt < max_attempts:
                        await asyncio.sleep(30)
                elif status == 404:
                    return None
                else:
                    log.warning(f"HTTP {status} (PW, attempt {attempt})  [{url[:60]}]")
                    if attempt < max_attempts:
                        await asyncio.sleep(5)
            except Exception as exc:
                log.warning(f"PW fetch error attempt {attempt}: {exc}  [{url[:60]}]")
                await asyncio.sleep(random.uniform(3, 8))
            finally:
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass

    log.error(f"All Playwright attempts failed: {url[:80]}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# HTTP CLIENT  (routes to Playwright when active, curl-cffi as fallback)
# ─────────────────────────────────────────────────────────────────────────────

async def fetch(
    session: AsyncSession,
    url: str,
    referer: str = BASE_URL,
    max_attempts: int = 4,
) -> Optional[str]:
    """
    GET a URL and return HTML text.
    Uses Playwright (Cloudflare-bypassing) when the browser context is active;
    falls back to curl-cffi with adaptive back-off otherwise.
    """
    # ── Playwright path ───────────────────────────────────────────────────────
    if _pw_context is not None:
        return await _pw_fetch(url, referer, max_attempts)

    # ── curl-cffi fallback ────────────────────────────────────────────────────
    consecutive_5xx = 0

    for attempt in range(1, max_attempts + 1):
        try:
            await asyncio.sleep(random.uniform(*PAGE_DELAY))
            resp = await session.get(
                url,
                headers=build_headers(referer),
                timeout=REQUEST_TIMEOUT,
            )

            if resp.status_code == 200:
                consecutive_5xx = 0
                return resp.text

            if resp.status_code == 429:
                pause = random.uniform(*RATE_LIMIT_PAUSE)
                log.warning(f"429 rate-limit → pause {pause:.0f}s  [{url[:60]}]")
                await asyncio.sleep(pause)

            elif resp.status_code in (502, 503, 504):
                consecutive_5xx += 1
                pause = random.uniform(*SERVER_ERR_PAUSE)
                if consecutive_5xx >= 3:
                    log.warning("3 consecutive 5xx — cooling down 120s …")
                    await asyncio.sleep(120)
                    consecutive_5xx = 0
                else:
                    log.warning(f"{resp.status_code} server error → pause {pause:.0f}s  [{url[:60]}]")
                    await asyncio.sleep(pause)

            elif resp.status_code == 403:
                log.warning(f"403 forbidden → pause 30s  [{url[:60]}]")
                await asyncio.sleep(30)

            elif resp.status_code == 404:
                log.debug(f"404 not found: {url[:60]}")
                return None

            else:
                log.warning(f"Unexpected {resp.status_code}  [{url[:60]}]")
                return None

        except Exception as exc:
            log.warning(f"Request error (attempt {attempt}/{max_attempts}): {exc}  [{url[:60]}]")
            await asyncio.sleep(random.uniform(5, 15))

    log.error(f"All {max_attempts} attempts failed: {url[:80]}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_categories(session: AsyncSession) -> list[dict]:
    """
    Discover categories from Jumia's navigation, then merge with SEED_CATEGORIES.
    Dynamic discovery catches new/renamed categories; seeds ensure full coverage.
    """
    log.info("Discovering categories from Jumia homepage …")

    # Start with seed list so we always have full coverage
    seen: set[str] = set()
    categories: list[dict] = []

    for cat in SEED_CATEGORIES:
        seen.add(cat["url"])
        categories.append(cat)

    # Try dynamic discovery to add anything new the nav exposes
    html = await fetch(session, BASE_URL, referer=BASE_URL)
    if html:
        soup = BeautifulSoup(html, "lxml")
        nav_selectors = [
            "nav a[href]",
            ".nav-holder a[href]",
            ".-hd-links a[href]",
            "#jm-header a[href]",
            "header a[href]",
            "[class*='nav'] a[href]",
        ]

        for sel in nav_selectors:
            links = soup.select(sel)
            if len(links) < 3:
                continue

            for a in links:
                href = a.get("href", "").strip()
                if not href:
                    continue

                if href.startswith("http"):
                    if "jumia.com.eg" not in href:
                        continue
                    path = "/" + href.split("jumia.com.eg", 1)[-1].lstrip("/")
                else:
                    path = href

                if not re.match(r"^/[a-z][a-z0-9-]+/$", path):
                    continue
                if _EXCLUDE_RE.search(path):
                    continue

                full_url = BASE_URL + path
                if full_url in seen:
                    continue

                name = a.get_text(strip=True)
                if not name or name.lower() in _GENERIC_NAMES or len(name) > 60:
                    name = slug_to_name(slug_of(full_url))
                if len(name) < 2:
                    continue

                seen.add(full_url)
                categories.append({"name": name, "url": full_url})
            break  # stop after first selector that yields results

    log.info(f"Total categories to scrape: {len(categories)} (seeds + dynamic)")
    return categories


# ─────────────────────────────────────────────────────────────────────────────
# PAGINATION HELPER
# ─────────────────────────────────────────────────────────────────────────────

def get_total_pages(soup: BeautifulSoup) -> int:
    """Extract total page count from Jumia pagination patterns."""

    # ── Pattern 1: "1 – 40 of 2,160" counting string (most reliable) ─────────
    # Jumia shows this in a .-pvS section; commas must be stripped from numbers
    _showing = re.compile(
        r"(\d[\d,]*)\s*[–\-]\s*(\d[\d,]*)\s+of\s+([\d,]+)", re.I
    )
    for text in soup.stripped_strings:
        m = _showing.search(text.replace("\u00a0", " "))
        if m:
            start    = int(m.group(1).replace(",", ""))
            end      = int(m.group(2).replace(",", ""))
            total    = int(m.group(3).replace(",", ""))
            per_page = max(end - start + 1, 1)
            if total > 1:
                return min(math.ceil(total / per_page), MAX_PAGES)

    # ── Pattern 2: aria-label="Page N of Y" on pagination links ─────────────
    for a in soup.select("a[aria-label]"):
        m = re.search(r"page\s+\d+\s+of\s+(\d+)", a.get("aria-label", ""), re.I)
        if m:
            return min(int(m.group(1)), MAX_PAGES)

    # ── Pattern 3: highest page=N in pagination href attributes ─────────────
    for a in soup.find_all("a", href=re.compile(r"[?&]page=(\d+)")):
        pass  # collect all, then pick max
    page_nums = []
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            page_nums.append(int(m.group(1)))
    if page_nums:
        return min(max(page_nums), MAX_PAGES)

    # ── Pattern 4: highest digit-only link text in a pagination container ────
    for sel in [".-pag", ".pg", "nav", "[class*='pag']"]:
        container = soup.select_one(sel)
        if not container:
            continue
        nums = []
        for a in container.find_all("a"):
            t = a.get_text(strip=True).replace(",", "")
            if t.isdigit():
                nums.append(int(t))
        if nums and max(nums) > 1:
            return min(max(nums), MAX_PAGES)

    # ── Pattern 5: data attributes ───────────────────────────────────────────
    for el in soup.select("[data-total], [data-pages], [data-page-count]"):
        val = (
            el.get("data-total") or el.get("data-pages") or el.get("data-page-count", "")
        ).replace(",", "")
        if val.isdigit() and int(val) > 1:
            return min(int(val), MAX_PAGES)

    return 1


# ─────────────────────────────────────────────────────────────────────────────
# LISTING PAGE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_listing_page(html: str, category_name: str) -> list[dict]:
    """Extract product cards from one listing page."""
    soup = BeautifulSoup(html, "lxml")
    products: list[dict] = []
    now = datetime.now().isoformat()

    articles = soup.select("article.prd, article[class*='prd'], div[class*='c-prd']")

    for art in articles:
        try:
            # ── Product ID ────────────────────────────────────────────────────
            product_id = (art.get("data-id") or art.get("data-sku") or "").strip()

            # ── URL ───────────────────────────────────────────────────────────
            link = art.select_one("a.core, a[class*='core']") or art.find("a", href=True)
            href = link.get("href", "") if link else ""
            product_url = to_abs_url(href)

            # Derive ID from URL if missing
            if not product_id and product_url:
                m = re.search(r"-(\d+)\.html", product_url)
                if m:
                    product_id = m.group(1)

            if not product_url:
                continue

            # ── Image ─────────────────────────────────────────────────────────
            img = art.select_one("img.img, img[class*='img']")
            main_image = ""
            if img:
                src = img.get("data-src") or img.get("src") or ""
                if src and "placeholder" not in src and not src.endswith(".svg"):
                    main_image = src

            # ── Title ─────────────────────────────────────────────────────────
            title_el = art.select_one("h3.name, .name, h3")
            title = title_el.get_text(strip=True) if title_el else ""

            # ── Prices ────────────────────────────────────────────────────────
            price_el = art.select_one("div.prc, .prc")
            price_egp = parse_price(price_el.get_text() if price_el else None)

            old_el = art.select_one("div.old, .old")
            original_price = parse_price(old_el.get_text() if old_el else None)

            # ── Discount ──────────────────────────────────────────────────────
            disc_el = art.select_one("div.bdg._dsct, div.bdg.-dsct, [class*='_dsct']")
            discount_percent = ""
            if disc_el:
                discount_percent = disc_el.get_text(strip=True).lstrip("-")  # "-25%" → "25%"
            elif price_egp and original_price and original_price > price_egp > 0:
                pct = round((1 - price_egp / original_price) * 100)
                discount_percent = f"{pct}%"

            # ── Rating ────────────────────────────────────────────────────────
            stars_el = art.select_one("i.s[style], [class*='stars'] i[style]")
            rating = parse_rating(stars_el.get("style") if stars_el else None)

            # ── Reviews ───────────────────────────────────────────────────────
            rvw_wrap = art.select_one(".-rvw, [class*='rvw'], .rev, [class*='rev']")
            reviews_count = None
            if rvw_wrap:
                span = rvw_wrap.find("span")
                if span:
                    reviews_count = parse_reviews(span.get_text(strip=True))

            # ── Jumia Express ─────────────────────────────────────────────────
            xtrs = art.select_one(".xtrs, [class*='xtrs']")
            jumia_express = "Yes" if xtrs else ""

            products.append({
                "product_id":      product_id,
                "title":           title,
                "brand":           "",
                "category":        category_name,
                "price_egp":       price_egp if price_egp is not None else "",
                "original_price":  original_price if original_price is not None else "",
                "discount_percent": discount_percent,
                "rating":          rating if rating is not None else "",
                "reviews_count":   reviews_count if reviews_count is not None else "",
                "availability":    "",
                "jumia_express":   jumia_express,
                "jumia_verified":  "",
                "sold_by":         "",
                "ships_from":      "",
                "delivery_date":   "",
                "main_image":      main_image,
                "all_images":      "",
                "description":     "",
                "weight":          "",
                "dimensions":      "",
                "model_number":    "",
                "country_of_origin": "",
                "warranty":        "",
                "tech_specs":      "",
                "variations":      "",
                "product_url":     product_url,
                "scraped_at":      now,
            })

        except Exception as exc:
            log.debug(f"Card parse error: {exc}")
            continue

    return products


# ─────────────────────────────────────────────────────────────────────────────
# DETAIL PAGE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _extract_jsonld(soup: BeautifulSoup) -> Optional[dict]:
    """Extract the Product node from JSON-LD structured data."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "{}")
        except Exception:
            continue
        # Direct Product type
        if isinstance(data, dict) and data.get("@type") == "Product":
            return data
        # @graph array
        graph = data.get("@graph") if isinstance(data, dict) else None
        if isinstance(graph, list):
            for node in graph:
                if isinstance(node, dict) and node.get("@type") == "Product":
                    return node
    return None


def _parse_specs_ul(soup: BeautifulSoup) -> dict:
    """
    Parse Jumia's specs list.
    Structure: <ul class="-pvs -mvxs -phm -lsn">
                 <li><span class="-b">KEY</span>: VALUE</li>
               </ul>
    """
    specs: dict[str, str] = {}

    # Primary selector observed in live pages
    ul = soup.select_one("ul.-pvs.-mvxs.-phm.-lsn")
    if not ul:
        # Broader fallback
        for candidate in soup.select("ul[class*='-pvs'][class*='-lsn']"):
            ul = candidate
            break

    if not ul:
        return specs

    for li in ul.find_all("li"):
        key_el = li.find("span", class_="-b")
        if not key_el:
            continue
        key = key_el.get_text(strip=True)
        # Text after the key span: "KEY: VALUE" → strip key and leading ": "
        full = li.get_text(strip=True)
        value = full[len(key):].lstrip(": ").strip()
        if key and value:
            specs[key] = value

    return specs


def _spec_val(specs: dict, *keys: str) -> str:
    """Extract a value from specs dict by trying multiple key fragments."""
    for key in keys:
        for k, v in specs.items():
            if key.lower() in k.lower():
                return v
    return ""


def parse_detail_page(html: str, product: dict) -> dict:
    """
    Enrich a product dict with data from its detail page.
    Primary source: JSON-LD structured data (schema.org).
    Secondary:      HTML selectors for seller, delivery, specs.
    """
    soup = BeautifulSoup(html, "lxml")
    p = product.copy()

    # ── 1. JSON-LD — brand, description, rating, images, availability ─────────
    ld = _extract_jsonld(soup)
    if ld:
        # Brand
        brand_obj = ld.get("brand", {})
        if isinstance(brand_obj, dict):
            p["brand"] = brand_obj.get("name", "")
        elif isinstance(brand_obj, str):
            p["brand"] = brand_obj

        # Description
        desc = ld.get("description", "")
        if desc:
            p["description"] = desc[:2000]

        # Rating + reviews
        ar = ld.get("aggregateRating", {})
        if ar:
            p["rating"] = ar.get("ratingValue", "")
            p["reviews_count"] = ar.get("ratingCount", "")

        # Availability
        avail_url = ld.get("offers", {}).get("availability", "")
        if avail_url:
            p["availability"] = "In Stock" if "InStock" in avail_url else "Out of Stock"

        # All images (high-res from contentUrl array)
        img_obj = ld.get("image", {})
        if isinstance(img_obj, dict):
            urls = img_obj.get("contentUrl", [])
            if isinstance(urls, list) and urls:
                p["all_images"] = " | ".join(urls)
                p["main_image"] = urls[0]
        elif isinstance(img_obj, list) and img_obj:
            p["all_images"] = " | ".join(img_obj)
            p["main_image"] = img_obj[0]

    # ── 2. Specs UL — model, country, color, material, SKU ───────────────────
    specs = _parse_specs_ul(soup)
    if specs:
        p["tech_specs"]        = json.dumps(specs, ensure_ascii=False)
        p["model_number"]      = _spec_val(specs, "Model", "model number")
        p["country_of_origin"] = _spec_val(specs, "Production Country", "country", "origin")
        p["warranty"]          = _spec_val(specs, "Warranty", "guarantee")
        # weight/dimensions rarely appear in this specs block; keep empty if not present

    # ── 3. Seller — section.card containing "Seller Information" ─────────────
    for card in soup.select("section.card"):
        card_text = card.get_text(separator=" ", strip=True).lower()
        if "seller information" in card_text:
            # Seller name is the first <p> inside the second div of the card
            seller_p = card.select_one("p.-m.-pbs, p[class*='-pbs']")
            if seller_p:
                p["sold_by"] = seller_p.get_text(strip=True)
            # Jumia Mall / Jumia Verified check
            if "jumia mall" in card_text or "jumia verified" in card_text:
                p["jumia_verified"] = "Yes"
            break

    # ── 4. Delivery date — "Ready for delivery on 29 March" ──────────────────
    for div in soup.select("div.markup.-ptxs, div[class*='markup'][class*='-ptxs']"):
        text = div.get_text(strip=True)
        if re.search(r"ready for (delivery|pickup) on", text, re.I):
            # Strip trailing noise like "if you place your order..."
            clean = re.sub(r"if you place.*", "", text, flags=re.I).strip()
            p["delivery_date"] = clean[:120]
            break

    # ── 5. Ships from / free delivery area ───────────────────────────────────
    for div in soup.select("div.markup.-fs12.-pbs.-gn7.-m, div[class*='markup'][class*='-fs12']"):
        text = div.get_text(strip=True)
        if "delivery" in text.lower() and len(text) < 120:
            p["ships_from"] = text
            break

    # ── 6. Jumia Express — look for the text anywhere in the page ────────────
    if not p.get("jumia_express"):
        page_text_lower = soup.get_text(separator=" ").lower()
        if "jumia express" in page_text_lower:
            p["jumia_express"] = "Yes"

    # ── 7. Variations — look for variant link lists ───────────────────────────
    variations: dict = {}
    seen_vals: set[str] = set()
    variant_items: list[str] = []

    for a in soup.select("a[class*='a-si'], a[class*='variant'], [data-value]")[:30]:
        val = (
            a.get("title")
            or a.get("data-value")
            or a.get("aria-label")
            or a.get_text(strip=True)
        )
        if val and val not in seen_vals and len(val) < 50:
            seen_vals.add(val)
            variant_items.append(val)

    if variant_items:
        variations["options"] = variant_items

    if variations:
        p["variations"] = json.dumps(variations, ensure_ascii=False)

    return p


# ─────────────────────────────────────────────────────────────────────────────
# PAGE FETCH HELPERS  (semaphore-wrapped)
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_listing_page(
    session: AsyncSession,
    sem: asyncio.Semaphore,
    url: str,
    category_name: str,
    referer: str,
) -> list[dict]:
    async with sem:
        html = await fetch(session, url, referer=referer)
        return parse_listing_page(html, category_name) if html else []


async def _fetch_detail_page(
    session: AsyncSession,
    sem: asyncio.Semaphore,
    product: dict,
) -> dict:
    async with sem:
        await asyncio.sleep(random.uniform(*DETAIL_DELAY))
        html = await fetch(session, product["product_url"], referer=BASE_URL)
        return parse_detail_page(html, product) if html else product


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY SCRAPER  (listing  +  optional detail)
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_category(
    session: AsyncSession,
    category_name: str,
    category_url: str,
    group_index: int,
    limit: int,
    deep: bool,
) -> int:
    """
    Full pipeline for one category.
    Returns number of products saved.
    """
    log.info(f"[{category_name}] Scraping → {category_url}")

    # Deduplicated product store across all sort orders
    seen: set[str] = set()
    products: list[dict] = []

    def _add(p: dict):
        key = p.get("product_id") or p.get("product_url")
        if key and key not in seen:
            seen.add(key)
            products.append(p)

    # ── Loop over sort orders ──────────────────────────────────────────────────
    for sort_name, sort_suffix in JUMIA_SORT_ORDERS:
        if len(products) >= limit:
            break

        sort_url = category_url + sort_suffix  # category URLs end in /
        log.info(f"[{category_name}] Sort: {sort_name} → {sort_url}")

        # ── First page: parse products + detect total pages ───────────────────
        first_html = await fetch(session, sort_url, referer=BASE_URL)
        if not first_html:
            log.warning(f"[{category_name}] Cannot fetch first page for sort '{sort_name}' — skipping.")
            continue

        first_soup = BeautifulSoup(first_html, "lxml")
        total_pages = get_total_pages(first_soup)
        log.info(f"[{category_name}] Sort '{sort_name}': {total_pages} pages detected.")

        sort_before = len(products)
        for p in parse_listing_page(first_html, category_name):
            _add(p)

        # ── Remaining pages: concurrent ───────────────────────────────────────
        if total_pages > 1 and len(products) < limit:
            await asyncio.sleep(random.uniform(2.0, 4.0))
            sem = asyncio.Semaphore(PAGE_CONCURRENCY)
            sep = "&" if "?" in sort_url else "?"
            extra_urls = [
                f"{sort_url}{sep}page={n}"
                for n in range(2, min(total_pages + 1, MAX_PAGES + 1))
            ]

            log.info(f"[{category_name}] Fetching {len(extra_urls)} more pages concurrently …")
            tasks = [
                _fetch_listing_page(session, sem, url, category_name, sort_url)
                for url in extra_urls
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for batch in results:
                if isinstance(batch, Exception):
                    log.warning(f"[{category_name}] Page error: {batch}")
                    continue
                for p in batch:
                    _add(p)
                    if len(products) >= limit:
                        break
                if len(products) >= limit:
                    break

        sort_new = len(products) - sort_before
        log.info(f"[{category_name}] Sort '{sort_name}' done: +{sort_new} new (total {len(products)})")

        if sort_new == 0:
            log.info(f"[{category_name}] Sort '{sort_name}' returned no new products — skipping remaining sorts.")
            break

    products = products[:limit]
    log.info(f"[{category_name}] {len(products)} unique products from listings.")

    # ── Deep scrape: product detail pages ─────────────────────────────────────
    if deep and products:
        log.info(f"[{category_name}] Enriching {len(products)} products via detail pages …")
        detail_sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
        enriched: list[dict] = []

        BATCH = 50
        for i in range(0, len(products), BATCH):
            batch = products[i : i + BATCH]
            tasks = [_fetch_detail_page(session, detail_sem, p) for p in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for j, res in enumerate(results):
                enriched.append(res if not isinstance(res, Exception) else batch[j])
            done = min(i + BATCH, len(products))
            log.info(f"[{category_name}] Detail progress: {done}/{len(products)}")

        products = enriched

    # ── Save to JSON ──────────────────────────────────────────────────────────
    out_file = f"products_{group_index}.json"
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(products, fh, ensure_ascii=False, indent=2)

    log.info(f"[{category_name}] ✓ {len(products)} products → {out_file}")
    return len(products)


# ─────────────────────────────────────────────────────────────────────────────
# CSV EXPORT  (merge all products_*.json  →  one CSV)
# ─────────────────────────────────────────────────────────────────────────────

def merge_json_to_csv(output_dir: str = "output") -> str:
    Path(output_dir).mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = os.path.join(output_dir, f"jumia_products_{date_str}.csv")

    # Collect all products_*.json  (also inside downloaded artifact subdirs)
    json_files: list[Path] = []
    for p in sorted(Path(".").rglob("products_*.json")):
        json_files.append(p)

    if not json_files:
        log.error("No products_*.json files found. Run --mode scrape first.")
        return ""

    log.info(f"Merging {len(json_files)} JSON file(s) → {csv_path}")
    total = 0

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()

        for jf in json_files:
            try:
                with open(jf, "r", encoding="utf-8") as f:
                    for product in json.load(f):
                        writer.writerow({field: product.get(field, "") for field in CSV_FIELDS})
                        total += 1
            except Exception as exc:
                log.warning(f"Skipping {jf}: {exc}")

    log.info(f"Export complete — {total:,} products → {csv_path}")
    return csv_path


def save_categories_csv(categories: list[dict], output_dir: str = "output"):
    Path(output_dir).mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = os.path.join(output_dir, f"jumia_categories_{date_str}.csv")
    now = datetime.now().isoformat()

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["name", "url", "discovered_at"])
        writer.writeheader()
        for cat in categories:
            writer.writerow({"name": cat["name"], "url": cat["url"], "discovered_at": now})

    log.info(f"Categories saved → {csv_path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def async_main(args: argparse.Namespace) -> None:

    # For scrape mode: start the persistent Playwright browser (bypasses Cloudflare)
    if args.mode == "scrape":
        await _init_playwright(target_url=args.category_url if args.category_url else None)

    async with AsyncSession(impersonate="chrome131", timeout=REQUEST_TIMEOUT) as session:

        # ── DISCOVER ─────────────────────────────────────────────────────────
        if args.mode == "discover":
            categories = await fetch_categories(session)

            with open("categories.json", "w", encoding="utf-8") as fh:
                json.dump(categories, fh, ensure_ascii=False, indent=2)

            log.info(f"Saved {len(categories)} categories → categories.json")
            save_categories_csv(categories)

            # Print JSON so GitHub Actions can capture it
            print(json.dumps(categories))

        # ── SCRAPE ───────────────────────────────────────────────────────────
        elif args.mode == "scrape":
            if not args.category_url or not args.category_name:
                log.error("--category-url and --category-name are required for scrape mode.")
                sys.exit(1)

            # Playwright warmup was done in _init_playwright(); add a short delay
            await asyncio.sleep(random.uniform(1.0, 2.5))

            count = await scrape_category(
                session=session,
                category_name=args.category_name,
                category_url=args.category_url,
                group_index=args.group_index,
                limit=args.limit,
                deep=args.deep,
            )

            await _close_playwright()

            if count == 0:
                log.warning("No products scraped — check the category URL.")
                sys.exit(1)

        # ── EXPORT ───────────────────────────────────────────────────────────
        elif args.mode == "export":
            csv_path = merge_json_to_csv()
            if not csv_path:
                sys.exit(1)

        else:
            log.error(f"Unknown mode: {args.mode!r}")
            sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Jumia Egypt Scraper — discover · scrape · export",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["discover", "scrape", "export"],
        required=True,
        help="Operation mode",
    )
    parser.add_argument("--category-name",  default="",            help="Category display name (scrape mode)")
    parser.add_argument("--category-url",   default="",            help="Category URL (scrape mode)")
    parser.add_argument("--group-index",    type=int, default=0,   help="Output file index (scrape mode)")
    parser.add_argument("--limit",          type=int, default=PRODUCT_LIMIT, help="Max products to scrape per category")
    parser.add_argument("--deep",           action="store_true",   help="Also scrape individual product detail pages")

    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
