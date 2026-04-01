#!/usr/bin/env python3
"""Precious Metals Aggregator — precious metals price data and dealer scraping."""

import json
import mimetypes
import os
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests as _requests

_SESSION = _requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def fetch_html(url, timeout=20):
    try:
        resp = _SESSION.get(url, timeout=timeout)
        resp.raise_for_status()
        if len(resp.text) > 500:
            return resp.text
    except Exception as e:
        print(f"[fetch_html] {url}: {e}", flush=True)
    return None


def fetch_json(url, timeout=10):
    try:
        resp = _SESSION.get(url, timeout=timeout,
                            headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[fetch_json] {url}: {e}", flush=True)
        return None


def parse_price(text):
    text = text.replace(",", "").strip()
    m = re.search(r"\$?([\d]+\.?\d*)", text)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


# ── Spot price cache (per metal) ──────────────────────────────────────────────

_spot_cache = {
    "gold":     {"price": None, "ts": 0},
    "silver":   {"price": None, "ts": 0},
    "platinum": {"price": None, "ts": 0},
}
METAL_API_SYMBOLS  = {"gold": "XAU",    "silver": "XAG",    "platinum": "XPT"}
METAL_LIVE_NAMES   = {"gold": "gold",   "silver": "silver", "platinum": "platinum"}


def get_spot_price(metal="gold"):
    cache = _spot_cache.get(metal, _spot_cache["gold"])
    now = time.time()
    if cache["price"] and (now - cache["ts"]) < 120:
        return cache["price"]

    symbol = METAL_API_SYMBOLS.get(metal, "XAU")
    data = fetch_json(f"https://api.gold-api.com/price/{symbol}")
    if data and data.get("price"):
        cache["price"] = float(data["price"])
        cache["ts"] = now
        print(f"[spot] {metal}: ${cache['price']:.4f} from gold-api.com")
        return cache["price"]

    live_name = METAL_LIVE_NAMES.get(metal, "gold")
    data = fetch_json(f"https://api.metals.live/v1/spot/{live_name}")
    if data:
        price = None
        if isinstance(data, list) and len(data) > 0:
            price = data[0].get("price")
        elif isinstance(data, dict):
            price = data.get("price")
        if price:
            cache["price"] = float(price)
            cache["ts"] = now
            print(f"[spot] {metal}: ${cache['price']:.4f} from metals.live")

    return cache["price"]


# ── Gold purity detection ──────────────────────────────────────────────────────

KARAT_PATTERNS = [
    (r"\b24\s*[Kk]\b|\.?999\.?9?\s*fine|\bpure\s*gold\b", "24K"),
    (r"\b22\s*[Kk]\b|\.?916\b", "22K"),
    (r"\b18\s*[Kk]\b|\.?750\b", "18K"),
    (r"\b14\s*[Kk]\b|\.?585\b", "14K"),
    (r"\b10\s*[Kk]\b|\.?417\b", "10K"),
]
KARAT_NUMERIC = {"10K": 10, "14K": 14, "18K": 18, "22K": 22, "24K": 24}

GOLD_BULLION_RE = re.compile(
    r"\b(bullion|coins?|bars?|rounds?|eagle|maple|krugerrand|sovereign|panda|philharmonic|"
    r"gold\s+bars?|gold\s+coins?)\b",
    re.IGNORECASE,
)


def detect_karat(title):
    for pattern, label in KARAT_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return label
    if GOLD_BULLION_RE.search(title):
        if re.search(r"\bbars?\b|\brounds?\b", title, re.IGNORECASE):
            return "24K"
        if re.search(r"\bcoins?\b|\beagle\b|\bmaple\b|\bkrugerrand\b", title, re.IGNORECASE):
            return "22K"
    return None


# ── Silver purity detection ────────────────────────────────────────────────────

SILVER_PURITY_PATTERNS = [
    (r"\.9999|9999\s*fine|four\s*nines?",                          ".9999"),
    (r"\.999\b|999\s*fine|three\s*nines?|\bfine\s+silver\b|pure\s+silver", ".999"),
    (r"\.925\b|925\b|\bsterling\b",                                ".925"),
    (r"\.900\b|900\s*silver|coin\s+silver|junk\s+silver|90%\s+silver", ".900"),
    (r"\.800\b|800\s*silver",                                      ".800"),
]
SILVER_PURITY_NUMERIC = {
    ".9999": 0.9999, ".999": 0.999, ".925": 0.925, ".900": 0.900, ".800": 0.800,
}

SILVER_BULLION_RE = re.compile(
    r"\b(bullion|coins?|bars?|rounds?|eagle|maple|libertad|kangaroo|britannia|lunar|"
    r"silver\s+bars?|silver\s+coins?|silver\s+rounds?|morgan|peace\s+dollar|"
    r"\d+\s*oz\s+silver|silver\s+\d+\s*oz)\b",
    re.IGNORECASE,
)


def detect_silver_purity(title):
    for pattern, label in SILVER_PURITY_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return label
    return None


# ── Platinum purity detection ──────────────────────────────────────────────────

PLATINUM_PURITY_PATTERNS = [
    (r"pt\s*950|950\s*pt|plat(?:inum)?\s*950|\.950\b|950\s*plat(?:inum)?", ".950"),
    (r"pt\s*900|900\s*pt|plat(?:inum)?\s*900|\.900\b|900\s*plat(?:inum)?", ".900"),
    (r"pt\s*850|850\s*pt|plat(?:inum)?\s*850|\.850\b|850\s*plat(?:inum)?", ".850"),
]
PLATINUM_PURITY_NUMERIC = {
    ".950": 0.950, ".900": 0.900, ".850": 0.850,
}

PLATINUM_BULLION_RE = re.compile(
    r"\b(bullion|coins?|bars?|rounds?|platinum\s+bars?|platinum\s+coins?)\b",
    re.IGNORECASE,
)


def detect_platinum_purity(title):
    for pattern, label in PLATINUM_PURITY_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return label
    return None


# ── Purity lookup (unified) ────────────────────────────────────────────────────

def get_purity_fraction(label, metal):
    if metal == "gold":
        k = KARAT_NUMERIC.get(label)
        return k / 24.0 if k is not None else None
    elif metal == "silver":
        return SILVER_PURITY_NUMERIC.get(label)
    elif metal == "platinum":
        return PLATINUM_PURITY_NUMERIC.get(label)
    return None


# ── Type detection ─────────────────────────────────────────────────────────────

TYPE_PATTERNS = [
    ("Coins", re.compile(
        r"\b(coins?|eagle|maple|krugerrand|sovereign|panda|philharmonic|libertad|buffalo|"
        r"britannia|lunar|kangaroo|morgan|peace\s+dollar)\b",
        re.IGNORECASE)),
    ("Bars", re.compile(r"\b(bars?|ingots?)\b", re.IGNORECASE)),
    ("Rounds", re.compile(r"\b(rounds?)\b", re.IGNORECASE)),
    ("Jewelry", re.compile(
        r"\b(jewelry|jewellery|necklaces?|bracelets?|pendants?|chains?|rings?|earrings?|"
        r"bangles?|anklets?|brooch|cufflinks?|wedding\s+band)\b",
        re.IGNORECASE)),
    ("Nuggets", re.compile(r"\b(nuggets?|natural\s+(?:gold|silver)|placer)\b", re.IGNORECASE)),
    ("Other", re.compile(
        r"\b(watch|watches|rolex|cartier|omega|scrap|dental|flakes?|dust|lots?)\b",
        re.IGNORECASE)),
]


def detect_type(title):
    for label, pattern in TYPE_PATTERNS:
        if pattern.search(title):
            return label
    return None


# ── Weight detection ───────────────────────────────────────────────────────────

FRAC_OZ = {
    "1/10": 0.1, "1/4": 0.25, "1/2": 0.5, "1/20": 0.05,
    "1/25": 0.04, "2": 2.0, "5": 5.0, "10": 10.0, "50": 50.0, "100": 100.0,
}


def detect_weight_oz(title):
    t = title.lower()

    m = re.search(r"(\d+/\d+)\s*(?:troy\s*)?oz", t)
    if m:
        frac = m.group(1)
        if frac in FRAC_OZ:
            return FRAC_OZ[frac]
        parts = frac.split("/")
        try:
            return float(parts[0]) / float(parts[1])
        except (ValueError, ZeroDivisionError):
            pass

    m = re.search(r"([\d]+(?:\.\d+)?)\s*(?:troy\s*)?oz\b", t)
    if m:
        return float(m.group(1))

    m = re.search(r"([\d]+(?:\.\d+)?)\s*(?:gram|grams|gm|g)\b", t)
    if m:
        return float(m.group(1)) / 31.1035

    m = re.search(r"([\d]+(?:\.\d+)?)\s*dwt\b", t)
    if m:
        return float(m.group(1)) / 20.0

    return None


def enrich_listing(listing, metal="gold"):
    """Add type, weight, purity, and metal content fields to a listing."""
    title = listing.get("title", "")

    if "type" not in listing or listing["type"] is None:
        listing["type"] = detect_type(title)

    weight = detect_weight_oz(title)
    listing["weight_oz"] = weight

    if metal == "gold":
        if "karat" not in listing or listing["karat"] is None:
            listing["karat"] = detect_karat(title)
        purity_label = listing.get("karat")
        purity_frac = (KARAT_NUMERIC.get(purity_label) / 24.0) if purity_label and purity_label in KARAT_NUMERIC else None

    elif metal == "silver":
        purity_label = detect_silver_purity(title)
        if not purity_label and SILVER_BULLION_RE.search(title):
            purity_label = ".999"
        purity_frac = SILVER_PURITY_NUMERIC.get(purity_label) if purity_label else None
        listing["karat"] = purity_label  # alias for UI badge

    elif metal == "platinum":
        purity_label = detect_platinum_purity(title)
        if not purity_label and PLATINUM_BULLION_RE.search(title):
            purity_label = ".950"
        purity_frac = PLATINUM_PURITY_NUMERIC.get(purity_label) if purity_label else None
        listing["karat"] = purity_label  # alias for UI badge

    else:
        purity_label = None
        purity_frac = None

    listing["purity_label"]    = purity_label
    listing["purity_fraction"] = purity_frac

    if weight and purity_frac:
        listing["metal_content_oz"] = round(weight * purity_frac, 6)
    else:
        listing["metal_content_oz"] = None

    # Keep gold_content_oz as alias for backward compatibility
    listing["gold_content_oz"] = listing["metal_content_oz"]

    return listing


# ── SD Bullion scraper ─────────────────────────────────────────────────────────

SD_BASE  = "https://sdbullion.com"
SD_PAGES = {
    "gold":     ["/gold/gold-coins",         "/gold/gold-bars"],
    "silver":   ["/silver/silver-coins",     "/silver/silver-bars"],
    "platinum": ["/platinum/platinum-coins", "/platinum/platinum-bars"],
}


def scrape_sdbullion_page(path, metal="gold"):
    html = fetch_html(SD_BASE + path)
    if not html:
        return []

    listings = []
    seen = set()

    for block in re.finditer(
        r"<form\b[^>]*\bproduct-item\b[^>]*>(.*?)</form>", html, re.DOTALL
    ):
        chunk = block.group(1)

        tm = re.search(
            r'<a\b[^>]*\bproduct-item-link\b[^>]*\bhref="([^"]*)"[^>]*>([\s\S]*?)</a>',
            chunk,
        )
        if not tm:
            continue
        url = tm.group(1)
        title = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", tm.group(2))).strip()
        if not title or url in seen:
            continue
        seen.add(url)

        pm = re.search(
            r'<span\b[^>]+\bid="[A-Z0-9\-]+-\d+"[^>]+data-nfusions-sku="[^"]*"[^>]*>\s*\$([\d,]+\.?\d*)',
            chunk,
        )
        price = parse_price(pm.group(1)) if pm else None
        if not price:
            continue

        im = re.search(r'<img\b[^>]+\bproduct-image-photo\b[^>]+src="([^"]*)"', chunk)

        listings.append(enrich_listing({
            "title": title,
            "price": price,
            "currency": "USD",
            "url": url if url.startswith("http") else SD_BASE + url,
            "image_url": im.group(1) if im else None,
            "source": "SD Bullion",
            "condition": "New",
            "listing_type": "fixed",
            "verified_seller": True,
        }, metal))

    return listings


def fetch_sdbullion(metal="gold"):
    pages = SD_PAGES.get(metal, SD_PAGES["gold"])
    results = []
    with ThreadPoolExecutor(max_workers=2) as ex:
        for fut in as_completed([ex.submit(scrape_sdbullion_page, p, metal) for p in pages]):
            try:
                results.extend(fut.result())
            except Exception as e:
                print(f"[sdbullion] {e}")
    return results


# ── eBay scraper ───────────────────────────────────────────────────────────────

EBAY_QUERIES = {
    "gold": [
        "gold bullion coin bar",
        "gold numismatic coin",
        "gold jewelry 14k",
    ],
    "silver": [
        "silver bullion coin bar",
        "silver numismatic coin",
        "silver sterling jewelry",
        "silver round bar 1oz",
    ],
    "platinum": [
        "platinum bullion coin bar",
        "platinum jewelry ring",
        "platinum watch",
    ],
}

EBAY_MISC_QUERIES = {
    "gold": [
        "gold watch mens",
        "gold wedding ring band",
        "gold scrap lot",
        "gold nugget natural",
        "14k 18k gold chain necklace",
    ],
    "silver": [
        "sterling silver necklace",
        "sterling silver ring",
        "silver scrap lot",
        "junk silver coins 90%",
        "sterling silver bracelet",
    ],
    "platinum": [
        "platinum ring wedding band",
        "platinum necklace bracelet",
        "platinum earrings",
    ],
}


def is_reputable(seller_block):
    pct = re.search(r"([\d.]+)%\s*positive", seller_block, re.IGNORECASE)
    count = re.search(r"\(([\d.,]+[Kk]?)\)", seller_block)
    if pct:
        p = float(pct.group(1))
        c = 0
        if count:
            raw = count.group(1).replace(",", "")
            c = int(float(raw[:-1]) * 1000) if raw.lower().endswith("k") else int(float(raw))
        if p >= 98.0 and c >= 50:
            return True
    if re.search(r"top.rated", seller_block, re.IGNORECASE):
        return True
    return False


def scrape_ebay_query(query, metal="gold"):
    params = urllib.parse.urlencode(
        {"_nkw": query, "_sop": "10", "_ipg": "48", "LH_BIN": "1"}
    )
    html = fetch_html(f"https://www.ebay.com/sch/i.html?{params}")
    if not html:
        return []

    listings = []
    for block in re.finditer(
        r'<li\b[^>]*\bid="item[0-9a-f]+"[^>]*>(.*?)</li>', html, re.DOTALL
    ):
        chunk = block.group(1)

        tm = re.search(
            r'<span\b[^>]*class="su-styled-text primary default"[^>]*>([\s\S]*?)</span>',
            chunk,
        )
        if not tm:
            continue
        title = re.sub(r"<[^>]+>", "", tm.group(1)).strip()
        title = re.sub(r"Opens in a new window.*$", "", title, flags=re.IGNORECASE).strip()
        if not title:
            continue

        pm = re.search(r'<span\b[^>]*\bs-card__price\b[^>]*>([\s\S]*?)</span>', chunk)
        price = parse_price(re.sub(r"<[^>]+>", "", pm.group(1))) if pm else None
        if not price:
            continue

        sm = re.search(
            r'<div\b[^>]*\bsu-card-container__attributes__secondary\b[^>]*>([\s\S]*?)</div>',
            chunk,
        )
        seller_text = re.sub(r"<[^>]+>", "", sm.group(1)).strip() if sm else ""
        if not is_reputable(seller_text):
            continue

        um = re.search(r'<a\b[^>]*\bs-card__link\b[^>]*\bhref="([^"?]*)', chunk)
        url = um.group(1) if um else None

        img_m = re.search(
            r'<img\b[^>]*\bs-card__image\b[^>]*(?:data-defer-load="([^"]+)"|[^>]+src="([^"]+)")',
            chunk,
        )
        image = None
        if img_m:
            image = img_m.group(1) or img_m.group(2)

        listings.append(enrich_listing({
            "title": title,
            "price": price,
            "currency": "USD",
            "url": url,
            "image_url": image,
            "source": "eBay",
            "condition": None,
            "seller": seller_text[:80],
            "listing_type": "buy_it_now",
            "verified_seller": True,
        }, metal))

    return listings


def fetch_ebay(metal="gold", include_misc=False):
    queries = EBAY_QUERIES.get(metal, EBAY_QUERIES["gold"])[:]
    if include_misc:
        queries.extend(EBAY_MISC_QUERIES.get(metal, []))

    all_listings = []
    seen_urls = set()
    with ThreadPoolExecutor(max_workers=4) as ex:
        for fut in as_completed([ex.submit(scrape_ebay_query, q, metal) for q in queries]):
            try:
                for item in fut.result():
                    url = item.get("url")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_listings.append(item)
                    elif not url:
                        all_listings.append(item)
            except Exception as e:
                print(f"[ebay] {e}")
    return all_listings


# ── Craigslist scraper ─────────────────────────────────────────────────────────

CL_CITIES = ["newyork", "losangeles", "chicago", "sfbay", "seattle", "dallas"]

CL_QUERIES = {
    "gold": [
        "14k gold", "18k gold", "24k gold", "gold coin", "gold bar bullion",
    ],
    "silver": [
        ".999 silver", "sterling silver", "silver coin", "silver bar bullion", "fine silver",
    ],
    "platinum": [
        "platinum ring", "platinum jewelry", "platinum coin bar", "pt950",
    ],
}

GOLD_CL_KEYWORDS = [
    "bullion", "gold coin", "gold bar", "gold nugget", "gold round",
    "gold chain", "gold ring", "gold necklace", "gold bracelet",
    "solid gold", "pure gold", "fine gold", ".999", ".585", ".750",
    ".916", "troy oz", "gold eagle", "gold maple", "krugerrand",
]

SILVER_CL_KEYWORDS = [
    "bullion", "silver coin", "silver bar", "silver round", "silver dollar",
    "silver chain", "silver ring", "silver necklace", "silver bracelet",
    "fine silver", "pure silver", "coin silver", "junk silver",
    "troy oz silver", "silver eagle", "silver maple", "morgan dollar", "peace dollar",
]

PLATINUM_CL_KEYWORDS = [
    "platinum ring", "platinum necklace", "platinum bracelet", "platinum watch",
    "platinum coin", "platinum bar", "platinum jewelry", "platinum band",
    "platinum earring",
]

GOLD_EXCLUSIONS_RE = re.compile(
    r"plat(?:ed|ing)|gold.?(?:tone|fill|color|trim|rim|leaf|foil|accent|layered|overlay|vermeil)|gold\s+over\b|"
    r"costume|fashion|fake|faux|replica|hdmi|cable|cd\b|dvd|blu.ray|"
    r"plate\b|porcelain|vase|ceramic|decor|figurine|ornament|frame|"
    r"dipped|crystal|china\b|incense|candle|lamp|pen\b|glasses|"
    r"shot glass|champagne|cordial|belly dance|hip scarf|fountain|"
    r"cream|serum|headphone|speaker|monster\b|racing|nascar|"
    r"tea\s*pot|highland mint|nfl\b|nba\b|mlb\b|champions|trump",
    re.IGNORECASE
)

SILVER_EXCLUSIONS_RE = re.compile(
    r"silver[-\s]?(?:plated?|tone|colored?|finish|fill(?:ed)?|overlay|accent|trim|dipped|leaf|foil)|"
    r"silver\s+over\b|plated\b|silverware\s+set|cutlery\s+set|"
    r"costume|fashion|fake|faux|replica|hdmi|cable|cd\b|dvd|blu.ray|"
    r"porcelain|vase|ceramic|decor|figurine|ornament|frame|"
    r"crystal|incense|candle|lamp|pen\b|glasses|shot\s*glass|champagne|cordial|"
    r"cream|serum|headphone|speaker",
    re.IGNORECASE
)

PLATINUM_EXCLUSIONS_RE = re.compile(
    r"platinum\s+(?:card|plan|member|award|status|record|album|edition|package|service|account|tier|blonde|hair)|"
    r"platinum\s+credit|fake|faux|replica|plated\b|tone|colored?|"
    r"hdmi|cable|cd\b|dvd|blu.ray|porcelain|decor",
    re.IGNORECASE
)


def _cl_is_valid(title_lower, metal):
    if metal == "gold":
        has_mark = bool(re.search(r"\b(10|14|18|22|24)\s*k\b", title_lower))
        has_kw   = any(kw in title_lower for kw in GOLD_CL_KEYWORDS)
        return (has_mark or has_kw) and not GOLD_EXCLUSIONS_RE.search(title_lower)
    elif metal == "silver":
        has_mark = bool(re.search(
            r"sterling|\.925\b|\.999\b|\.9999\b|\.900\b|\.800\b|fine\s+silver|coin\s+silver",
            title_lower
        ))
        has_kw = any(kw in title_lower for kw in SILVER_CL_KEYWORDS)
        return (has_mark or has_kw) and not SILVER_EXCLUSIONS_RE.search(title_lower)
    elif metal == "platinum":
        has_mark = bool(re.search(
            r"pt\s*950|pt\s*900|pt\s*850|\.950\b|\.900\b|\.850\b|platinum\s+\d{3}",
            title_lower
        ))
        has_kw = any(kw in title_lower for kw in PLATINUM_CL_KEYWORDS)
        return (has_mark or has_kw) and not PLATINUM_EXCLUSIONS_RE.search(title_lower)
    return False


def scrape_craigslist_city(city, query, metal="gold"):
    params = urllib.parse.urlencode({"query": query, "sort": "date"})
    url = f"https://{city}.craigslist.org/search/sss?{params}"
    html = fetch_html(url, timeout=12)
    if not html:
        return []

    listings = []
    for block in re.finditer(
        r'<li\b[^>]*class="[^"]*cl-static-search-result[^"]*"[^>]*>(.*?)</li>',
        html, re.DOTALL
    ):
        chunk = block.group(1)

        tm = re.search(
            r'<a\b[^>]*href="([^"]*)"[^>]*>[\s\S]*?<div\s+class="title">(.*?)</div>',
            chunk
        )
        if not tm:
            continue
        item_url = tm.group(1)
        title = re.sub(r"<[^>]+>", "", tm.group(2)).strip()
        title = re.sub(r"&amp;", "&", re.sub(r"&#\d+;", "", title)).strip()
        if not title:
            continue

        pm = re.search(r'<div\s+class="price">\s*\$([\d,]+)', chunk)
        price = parse_price(pm.group(1)) if pm else None
        if not price or price < 25:
            continue

        if not _cl_is_valid(title.lower(), metal):
            continue

        loc_m = re.search(r'<div\s+class="location">\s*(.*?)\s*</div>', chunk)
        location = loc_m.group(1).strip() if loc_m else city

        listings.append(enrich_listing({
            "title": title,
            "price": price,
            "currency": "USD",
            "url": item_url,
            "image_url": None,
            "source": "Craigslist",
            "condition": None,
            "listing_type": "local",
            "verified_seller": False,
            "region": location,
        }, metal))

    return listings


def fetch_craigslist(metal="gold"):
    queries = CL_QUERIES.get(metal, CL_QUERIES["gold"])
    all_listings = []
    seen_urls = set()
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = []
        for city in CL_CITIES:
            for query in queries:
                futs.append(ex.submit(scrape_craigslist_city, city, query, metal))
        for fut in as_completed(futs):
            try:
                for item in fut.result():
                    url = item.get("url")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_listings.append(item)
            except Exception as e:
                print(f"[craigslist] {e}")
    return all_listings


# ── Facebook Marketplace (link-out only) ──────────────────────────────────────

FB_QUERIES = {
    "gold": [
        "gold jewelry", "gold ring", "gold watch",
        "gold chain necklace", "gold coin bar",
    ],
    "silver": [
        "silver jewelry", "sterling silver", "silver ring",
        "silver chain necklace", "silver coin bar bullion",
    ],
    "platinum": [
        "platinum ring", "platinum jewelry",
        "platinum watch", "platinum coin",
    ],
}


def generate_facebook_links(metal="gold"):
    listings = []
    for query in FB_QUERIES.get(metal, FB_QUERIES["gold"]):
        encoded = urllib.parse.quote(query)
        listings.append({
            "title": f"Search Facebook Marketplace: \"{query}\"",
            "price": 0,
            "currency": "USD",
            "url": f"https://www.facebook.com/marketplace/search/?query={encoded}&exact=false",
            "image_url": None,
            "source": "Facebook",
            "condition": None,
            "listing_type": "marketplace_link",
            "verified_seller": False,
            "type": "Other",
            "karat": None,
            "purity_label": None,
            "purity_fraction": None,
            "weight_oz": None,
            "metal_content_oz": None,
            "gold_content_oz": None,
            "is_search_link": True,
        })
    return listings


# ── HTTP handler ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"  {self.address_string()} - {fmt % args}")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        qs     = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._serve_file(os.path.join(STATIC_DIR, "index.html"))
        elif path.startswith("/static/"):
            self._serve_file(os.path.join(STATIC_DIR, path[len("/static/"):]))
        elif path == "/api/listings":
            self._api_listings(qs)
        elif path == "/api/spot":
            self._api_spot(qs)
        else:
            self.send_response(404)
            self.end_headers()

    def _serve_file(self, filepath):
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            mime, _ = mimetypes.guess_type(filepath)
            self.send_response(200)
            self.send_header("Content-Type", mime or "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()

    def _api_spot(self, qs):
        metal = (qs.get("metal", ["gold"])[0] or "gold").lower()
        if metal not in ("gold", "silver", "platinum"):
            metal = "gold"
        price = get_spot_price(metal)
        body = json.dumps({"price": price, "metal": metal}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _api_listings(self, qs):
        metal = (qs.get("metal", ["gold"])[0] or "gold").lower()
        if metal not in ("gold", "silver", "platinum"):
            metal = "gold"

        src           = (qs.get("source", [None])[0] or "").lower().replace(" ", "")
        min_karat_raw = qs.get("min_karat", [None])[0]
        max_karat_raw = qs.get("max_karat", [None])[0]
        item_type     = qs.get("type", [None])[0]
        include_misc  = qs.get("include_misc", ["0"])[0] == "1"
        q             = (qs.get("q", [""])[0] or "").lower()
        min_price_raw = qs.get("min_price", [None])[0]
        max_price_raw = qs.get("max_price", [None])[0]
        min_weight_raw = qs.get("min_weight_oz", [None])[0]
        max_weight_raw = qs.get("max_weight_oz", [None])[0]

        min_purity_frac = get_purity_fraction(min_karat_raw, metal) if min_karat_raw else None
        max_purity_frac = get_purity_fraction(max_karat_raw, metal) if max_karat_raw else None
        min_price  = float(min_price_raw)  if min_price_raw  else None
        max_price  = float(max_price_raw)  if max_price_raw  else None
        min_weight = float(min_weight_raw) if min_weight_raw else None
        max_weight = float(max_weight_raw) if max_weight_raw else None

        spot = get_spot_price(metal)

        listings = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {}
            if not src or src == "ebay":
                futs["ebay"] = ex.submit(fetch_ebay, metal, include_misc)
            if not src or src == "sdbullion":
                futs["sd"] = ex.submit(fetch_sdbullion, metal)
            if include_misc and (not src or src == "craigslist"):
                futs["cl"] = ex.submit(fetch_craigslist, metal)
            if include_misc and (not src or src == "facebook"):
                futs["fb"] = ex.submit(generate_facebook_links, metal)
            for name, fut in futs.items():
                try:
                    listings.extend(fut.result())
                except Exception as e:
                    print(f"[api] {name}: {e}")

        # Filter by type
        if item_type:
            listings = [l for l in listings if l.get("type") == item_type]

        # Filter by purity range
        if min_purity_frac is not None or max_purity_frac is not None:
            filtered = []
            for l in listings:
                pf = l.get("purity_fraction")
                if pf is None:
                    continue
                if min_purity_frac is not None and pf < min_purity_frac:
                    continue
                if max_purity_frac is not None and pf > max_purity_frac:
                    continue
                filtered.append(l)
            listings = filtered

        # Filter by price range
        if min_price is not None:
            listings = [l for l in listings if l.get("price", 0) >= min_price]
        if max_price is not None:
            listings = [l for l in listings if l.get("price", 0) <= max_price]

        # Filter by weight range
        if min_weight is not None:
            listings = [l for l in listings if (l.get("weight_oz") or 0) >= min_weight]
        if max_weight is not None:
            listings = [l for l in listings if l.get("weight_oz") and l["weight_oz"] <= max_weight]

        # Filter by search query
        if q:
            listings = [l for l in listings if q in l.get("title", "").lower()]

        # Exclude listings where weight (and thus metal content) is unknown
        listings = [l for l in listings if l.get("weight_oz") and not l.get("is_search_link")]

        listings.sort(key=lambda x: x["price"])

        body = json.dumps({
            "count":      len(listings),
            "spot_price": spot,
            "metal":      metal,
            "listings":   listings,
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8765))
    httpd = HTTPServer(("0.0.0.0", port), Handler)
    print(f"Precious Metals Aggregator running at http://localhost:{port}")
    httpd.serve_forever()
