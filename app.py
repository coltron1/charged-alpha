"""
Charged Alpha — Unified Flask Server
All investing tools served from one app.

Routes:
  /                              → Homepage
  /screener/                     → Stock Screener
  /screener/api/...              → Stock Screener API
  /etf/                          → ETF Screener
  /etf/api/...                   → ETF Screener API
  /mutual-funds/                 → Mutual Fund Screener
  /mutual-funds/api/...          → Mutual Fund Screener API
  /crypto/                       → Crypto Screener
  /crypto/api/...                → Crypto Screener API
  /options/                      → Options Scanner
  /options/api/...               → Options Scanner API
  /bonds/                        → Bond Dashboard
  /bonds/api/...                 → Bond Dashboard API
  /reits/                        → REIT Screener
  /reits/api/...                 → REIT Screener API
  /forex/                        → Forex Heatmap
  /forex/api/...                 → Forex Heatmap API
  /commodities/                  → Commodities Dashboard
  /commodities/api/...           → Commodities Dashboard API
  /earnings/                     → Earnings Calendar
  /earnings/api/...              → Earnings Calendar API
  /gold/                         → Precious Metals Aggregator
  /gold/api/...                  → Precious Metals API
  /charts/                       → Stock Charts (TradingView)
  /charts/api/...                → Chart save/load API
  /games/                        → Interactive investing games
  /games/api/...                 → Game leaderboards and score API
  /auth/...                      → Authentication (login, register, OAuth)
"""

import json
import os
import re
import time
import threading
import datetime
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urlparse
from xml.sax.saxutils import escape as xml_escape
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

import yfinance as yf
from flask import Flask, abort, render_template, request, jsonify, redirect, Response, url_for
from flask_compress import Compress
from flask_login import LoginManager, current_user, login_required
from werkzeug.middleware.proxy_fix import ProxyFix

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# ── Shared utilities ────────────────────────────────────────────────────────
from yf_utils import (TTLCache, JobStore, fetch_ticker_info, safe_float,
                       normalize_div_yield, fetch_chart, fetch_banner_tickers)
from models import db, User, GameScore
from auth import auth_bp, get_email_updates_subscription, get_public_first_name, init_oauth, public_auth_enabled
from chart_storage import save_chart_state, load_chart_state, list_user_charts, delete_chart_state

# ── Import backend modules ──────────────────────────────────────────────────
from stock_screener import (screen_stocks, get_stock_detail,
                            get_sp500_tickers, get_ticker_sector)
from etf_screener import screen_etfs, get_etf_detail
from mutual_fund_screener import screen_mutual_funds, get_mutual_fund_detail, get_mutual_fund_catalog_rows
from crypto_screener import screen_cryptos, get_crypto_chart
from options_scanner import scan_options
from bond_data import get_yields, get_yield_history, get_bond_etfs
from reit_screener import screen_reits
from forex_data import get_all_pairs, get_pair_chart, get_currency_strength
from commodities_data import get_all_commodities, get_commodity_chart
from earnings_data import get_earnings_week, get_earnings_month, get_stock_earnings_history
from gold_server import get_spot_price, fetch_ebay, fetch_sdbullion, \
    fetch_craigslist, generate_facebook_links, get_purity_fraction

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
BASE_DIR = Path(__file__).resolve().parent
SHOWS_CATALOG_PATH = BASE_DIR / "data" / "shows_catalog.json"
GOOGLE_SITE_VERIFICATION_FILENAME = "google8b17550efa5dc3e4.html"
INTERACTIVE_GAME_BUILD_PATH = BASE_DIR / "static" / "games" / "interactive"
INTERACTIVE_GAME_MANIFEST_PATH = INTERACTIVE_GAME_BUILD_PATH / ".vite" / "manifest.json"

GAME_CATALOG = [
    {
        "slug": "expiration-date",
        "app_slug": "expiration-date",
        "title": "Expiration Date",
        "status": "Playable alpha",
        "playable": True,
        "unlock_after": None,
        "tagline": "Buy the option. Beat the clock. Make the market move enough.",
        "description": (
            "Trade simulated SPX options through real historical market shocks. "
            "Use future knowledge to buy calls, puts, straddles, or sit in the "
            "safety of Treasury bills waiting for the right event, then "
            "see whether the actual S&P 500 move beat premium, volatility, and time decay."
        ),
        "lesson": "Options are paid tickets: direction matters, but size and timing decide the payout.",
        "challenge": "Can you turn future knowledge into a top score before expiration eats the trade?",
        "image": "/static/games/interactive/games/options-fortune/expiration-date-game-image-1.webp",
        "preview_dashboard": "/static/games/interactive/games/previews/expiration-date-dashboard.jpg",
        "preview_action": "/static/games/interactive/games/previews/expiration-date-play.jpg",
        "route": "/games/expiration-date",
    },
    {
        "slug": "front-page-fortune",
        "app_slug": "front-page-fortune",
        "title": "Front Page Fortune",
        "status": "Playable alpha",
        "playable": True,
        "unlock_after": None,
        "tagline": "Know the event. Still call the market.",
        "description": (
            "Play a portfolio across real historical events using S&P 500 stocks, "
            "gold, a custom mix, or sit in the safety of Treasury bills waiting "
            "for the right event. You know what happened next, but the "
            "actual market data decides whether your allocation was genius or a trap."
        ),
        "lesson": "Future knowledge is powerful, but markets price fear, relief, taxes, and timing.",
        "challenge": "Can you turn $100,000 into the biggest fortune without being fooled by the obvious trade?",
        "image": "/static/games/interactive/games/headline-market/how-to-dashboard-desktop.png",
        "preview_dashboard": "/static/games/interactive/games/previews/front-page-fortune-dashboard.jpg",
        "preview_action": "/static/games/interactive/games/previews/front-page-fortune-play.jpg",
        "route": "/games/front-page-fortune",
    },
    {
        "slug": "harvest-ledger",
        "app_slug": "harvest-ledger",
        "title": "Harvest Ledger",
        "status": "Playable alpha",
        "playable": True,
        "unlock_after": None,
        "tagline": "Trade the crop shock before the futures tape settles.",
        "description": (
            "Speculate on real grain-market history with corn, soybean, wheat, or "
            "sit in the safety of Treasury bills waiting for the right event. "
            "Droughts, floods, crop reports, and trade shocks can "
            "make futures contracts surge, but one wrong harvest call can drain the ledger."
        ),
        "lesson": "Futures magnify commodity moves, so contract size and risk control matter.",
        "challenge": "Can you read the crop tape, choose the right contract, and harvest the weekly high score?",
        "image": "/static/games/interactive/games/futures-fortune/grain-ledger-prologue.webp",
        "preview_dashboard": "/static/games/interactive/games/previews/harvest-ledger-dashboard.jpg",
        "preview_action": "/static/games/interactive/games/previews/harvest-ledger-play.jpg",
        "route": "/games/harvest-ledger",
    },
    {
        "slug": "sector-oracle",
        "app_slug": "sector-oracle",
        "title": "Sector Oracle",
        "status": "Playable alpha",
        "playable": True,
        "unlock_after": None,
        "tagline": "Pick the sector that wins after the obvious story breaks.",
        "description": (
            "Rotate through real market eras using a balanced mix, tech, energy, "
            "banks, health care, staples, or sit in the safety of Treasury bills "
            "waiting for the right event. The historical event is known; "
            "the game is spotting which investment actually benefits when prices move."
        ),
        "lesson": "A true event can still point to the wrong sector if the market already priced it in.",
        "challenge": "Can you beat the index by finding the second-order winner before everyone else?",
        "image": "/static/games/interactive/games/sector-oracle/oracle-of-sectors-chapter-1.webp",
        "preview_dashboard": "/static/games/interactive/games/previews/sector-oracle-dashboard.jpg",
        "preview_action": "/static/games/interactive/games/previews/sector-oracle-play.jpg",
        "route": "/games/sector-oracle",
    },
]

GAME_SCORE_RESET_EPOCH_UTC = datetime.datetime(2026, 6, 4, 5, 1, 0)
GAME_SCORE_RESET_TZ = ZoneInfo("America/Chicago") if ZoneInfo else datetime.timezone.utc
GAME_SCORE_NAME_BLOCKLIST = {
    "admin",
    "administrator",
    "asshole",
    "bitch",
    "chargedalpha",
    "cunt",
    "dick",
    "fuck",
    "hitler",
    "moderator",
    "nazi",
    "shit",
    "support",
}
GAME_SCORE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9 .'-]{1,23}$")
GAME_SEEDED_SCORE_NAMES = [
    "Maya",
    "Cal",
    "Avery",
    "Riley",
    "Jordan",
    "Sam",
    "Nora",
    "Theo",
    "Quinn",
    "Casey",
    "Morgan",
    "Elliot",
    "Reese",
    "Logan",
    "Taylor",
    "Parker",
]
GAME_SEEDED_SCORE_RANGES = {
    "expiration-date": (145_000, 225_000),
    "front-page-fortune": (135_000, 190_000),
    "harvest-ledger": (125_000, 180_000),
    "sector-oracle": (130_000, 185_000),
}
GAME_SEEDED_SCORE_BASE = 100_000
GAME_SEEDED_SCORE_STEP = 500


@app.get("/health")
def health_check():
    return jsonify({"status": "ok"}), 200
SITE_URL = os.environ.get("SITE_URL", "https://chargedalpha.com").rstrip("/")
SITE_URL_PARTS = urlparse(SITE_URL)
SITE_SCHEME = SITE_URL_PARTS.scheme or "https"
CANONICAL_HOST = (SITE_URL_PARTS.netloc or SITE_URL_PARTS.path).lower().strip("/")
WWW_CANONICAL_HOST = f"www.{CANONICAL_HOST}"
GOOGLE_ANALYTICS_ID = os.environ.get("GOOGLE_ANALYTICS_ID", "G-HJ72GKBEFW").strip()
DEFAULT_SOCIAL_IMAGE_PATH = "/static/assets/charged-alpha-logo.png"
DEFAULT_SOCIAL_IMAGE_URL = f"{SITE_URL}{DEFAULT_SOCIAL_IMAGE_PATH}"
SHOWS_INITIAL_STOCK_COUNT = 24
PUBLIC_SITEMAP_PATHS = [
    "/",
    "/shows",
    "/screener",
    "/etf",
    "/mutual-funds",
    "/crypto",
    "/options",
    "/bonds",
    "/reits",
    "/forex",
    "/commodities",
    "/earnings",
    "/gold",
    "/charts",
    "/games",
    "/about",
    "/privacy",
]
PUBLIC_ROBOTS_DISALLOW_PATHS = [
    "/auth/",
    "/login",
    "/register",
    "/api/",
    "/screener/api/",
    "/etf/api/",
    "/mutual-funds/api/",
    "/crypto/api/",
    "/options/api/",
    "/bonds/api/",
    "/reits/api/",
    "/forex/api/",
    "/commodities/api/",
    "/earnings/api/",
    "/gold/api/",
    "/charts/api/",
    "/games/api/",
]
SEO_DEFAULTS = {
    "title": "Charged Alpha Stock Earnings Videos & Research Tools",
    "description": (
        "Search Charged Alpha earnings videos by ticker, quarter, YouTube episode, "
        "Spotify podcast, and stock analysis page, plus free investing research tools."
    ),
    "robots": "index,follow,max-image-preview:large",
    "og_type": "website",
    "twitter_card": "summary",
    "og_image": DEFAULT_SOCIAL_IMAGE_URL,
}
SEO_PAGE_META = {
    "/": {
        "title": "Charged Alpha Stock Earnings Videos & Research Tools",
        "description": (
            "Search Charged Alpha earnings videos by ticker, quarter, YouTube "
            "episode, Spotify podcast, and stock analysis page, plus free "
            "investing research tools."
        ),
    },
    "/shows": {
        "title": "Stock Earnings Video Library — Charged Alpha",
        "description": (
            "Browse Charged Alpha earnings videos and podcast episodes by ticker, "
            "company, quarter, and stock analysis page."
        ),
    },
    "/screener": {
        "title": "S&P 500 Stock Screener — Charged Alpha",
        "description": (
            "Screen S&P 500 stocks by valuation, growth, profitability, momentum, "
            "and sector filters inside Charged Alpha."
        ),
    },
    "/etf": {
        "title": "ETF Screener — Charged Alpha",
        "description": (
            "Find ETFs by expense ratio, yield, liquidity, structure, and "
            "performance filters with the Charged Alpha ETF screener."
        ),
    },
    "/mutual-funds": {
        "title": "Mutual Fund Screener — Charged Alpha",
        "description": (
            "Screen mutual funds by expense ratio, AUM, yield, performance, "
            "allocation style, and international exposure with Charged Alpha."
        ),
    },
    "/crypto": {
        "title": "Crypto Screener — Charged Alpha",
        "description": (
            "Screen crypto assets by market cap, volume, price action, and trend "
            "signals with the Charged Alpha crypto screener."
        ),
    },
    "/options": {
        "title": "Options Flow Scanner — Charged Alpha",
        "description": (
            "Scan unusual options activity, premium, expiration, strike, and "
            "sentiment setups with Charged Alpha's options flow scanner."
        ),
    },
    "/bonds": {
        "title": "Bond & Treasury Dashboard — Charged Alpha",
        "description": (
            "Track Treasury yields, curve movement, and bond ETF context in one "
            "Charged Alpha fixed-income dashboard."
        ),
    },
    "/reits": {
        "title": "REIT Screener — Charged Alpha",
        "description": (
            "Screen REITs by yield, valuation, property type, leverage, and price "
            "performance with Charged Alpha."
        ),
    },
    "/forex": {
        "title": "Forex Heatmap — Charged Alpha",
        "description": (
            "Monitor currency strength, pair heatmaps, and FX trend charts with "
            "Charged Alpha's forex dashboard."
        ),
    },
    "/commodities": {
        "title": "Commodities Dashboard — Charged Alpha",
        "description": (
            "Track commodity prices and trend charts across metals, energy, and "
            "other key macro-sensitive markets."
        ),
    },
    "/earnings": {
        "title": "Earnings Calendar — Charged Alpha",
        "description": (
            "Follow upcoming earnings dates, monthly earnings schedules, and prior "
            "report history with Charged Alpha."
        ),
    },
    "/gold": {
        "title": "Precious Metals Aggregator — Charged Alpha",
        "description": (
            "Compare gold and precious metals pricing, spot moves, and marketplace "
            "listings in the Charged Alpha metals hub."
        ),
    },
    "/charts": {
        "title": "Stock Charts — Charged Alpha",
        "description": (
            "Build, save, and revisit chart layouts with Charged Alpha's stock "
            "chart workspace and TradingView-powered analysis tools."
        ),
    },
    "/games": {
        "title": "Interactive Investing Games — Charged Alpha",
        "description": (
            "Play Charged Alpha's open investing story games, chase weekly high "
            "scores, and test market decisions across headlines, crops, sectors, "
            "and options."
        ),
    },
    "/about": {
        "title": "About Colton Thomas — Charged Alpha",
        "description": (
            "Meet Colton Thomas, creator of Charged Alpha and the investing "
            "education content, earnings-analysis videos, and free market games "
            "behind the platform."
        ),
    },
    "/unsubscribe": {
        "title": "Email Preferences — Charged Alpha",
        "description": (
            "Opt out of Charged Alpha marketing outreach or contact Colton with "
            "email preference questions."
        ),
        "robots": "noindex,nofollow,noarchive",
    },
    "/privacy": {
        "title": "Privacy Policy — Charged Alpha Academy",
        "description": (
            "Privacy Policy for Charged Alpha Academy, an offline investing-education "
            "app by Crown Creek Capital LLC d/b/a Charged Alpha."
        ),
    },
    "/games/front-page-fortune": {
        "title": "Front Page Fortune — Historical Market Prediction Game",
        "description": (
            "Play Front Page Fortune, a historical investing game where Jonah knows "
            "future newspaper headlines but still has to decide how stocks, gold, "
            "and bonds will react."
        ),
    },
    "/games/harvest-ledger": {
        "title": "Harvest Ledger — Charged Alpha Game",
        "description": (
            "Play Harvest Ledger, a Charged Alpha investing story game focused on "
            "commodities, futures, inventory, and risk management."
        ),
    },
    "/games/sector-oracle": {
        "title": "Sector Oracle — Charged Alpha Game",
        "description": (
            "Play Sector Oracle, a Charged Alpha investing game about macro "
            "headlines, sector rotation, valuation, and second-order effects."
        ),
    },
    "/games/expiration-date": {
        "title": "Expiration Date — Charged Alpha Game",
        "description": (
            "Play Expiration Date, a Charged Alpha options education game about "
            "option premium, time decay, catalysts, and position sizing."
        ),
    },
    "/account": {
        "title": "Account — Charged Alpha",
        "description": (
            "View your Charged Alpha saved charts and interactive investing game scores."
        ),
        "robots": "noindex,nofollow,noarchive",
    },
    "/auth/login": {
        "title": "Sign In — Charged Alpha",
        "description": (
            "Sign in to Charged Alpha to save chart layouts and access your "
            "personalized investing workspace."
        ),
        "robots": "noindex,nofollow,noarchive",
    },
    "/auth/register": {
        "title": "Create Account — Charged Alpha",
        "description": (
            "Create a Charged Alpha account to save chart layouts and personalize "
            "your research workflow."
        ),
        "robots": "noindex,nofollow,noarchive",
    },
}
NOINDEX_PATH_PREFIXES = (
    "/auth/",
    "/api/",
    "/screener/api/",
    "/etf/api/",
    "/mutual-funds/api/",
    "/crypto/api/",
    "/options/api/",
    "/bonds/api/",
    "/reits/api/",
    "/forex/api/",
    "/commodities/api/",
    "/earnings/api/",
    "/gold/api/",
    "/charts/api/",
    "/games/api/",
)
NOINDEX_EXACT_PATHS = {
    "/login",
    "/register",
    "/health",
    "/account",
    "/unsubscribe",
}


def _is_noindex_path(path):
    normalized = _normalize_path(path)
    return normalized in NOINDEX_EXACT_PATHS or any(normalized.startswith(prefix) for prefix in NOINDEX_PATH_PREFIXES)


def _normalize_path(path):
    if not path or path == "/":
        return "/"
    return "/" + path.strip("/")


def _canonical_url(path):
    return f"{SITE_URL}{_normalize_path(path)}"


def _parse_datetime(value):
    if not value:
        return None
    text = str(value).strip()
    try:
        parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed
    except ValueError:
        return None


def _date_for_sitemap(value):
    parsed = _parse_datetime(value)
    if parsed:
        return parsed.date().isoformat()
    return ""


def _youtube_video_id(url):
    parsed = urlparse(url or "")
    if parsed.netloc.endswith("youtu.be"):
        return parsed.path.strip("/").split("/")[0]
    if "youtube.com" in parsed.netloc:
        query_id = parse_qs(parsed.query).get("v", [""])[0]
        if query_id:
            return query_id
        parts = [part for part in parsed.path.split("/") if part]
        if "embed" in parts:
            idx = parts.index("embed")
            return parts[idx + 1] if idx + 1 < len(parts) else ""
        if "shorts" in parts:
            idx = parts.index("shorts")
            return parts[idx + 1] if idx + 1 < len(parts) else ""
    return ""


def _youtube_embed_url(url):
    video_id = _youtube_video_id(url)
    return f"https://www.youtube.com/embed/{video_id}" if video_id else ""


def _video_upload_date(value):
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else ""


def _latest_catalog_timestamp(shows_data):
    timestamps = [
        ep.get("published_at")
        for ep in shows_data.get("episodes", [])
        if ep.get("published_at")
    ]
    parsed = [ts for ts in timestamps if _parse_datetime(ts)]
    if parsed:
        return max(parsed, key=lambda ts: _parse_datetime(ts))
    return shows_data.get("last_synced_at", "")


def _get_seo_meta(path=None):
    current_path = _normalize_path(path or request.path)
    page_meta = SEO_PAGE_META.get(current_path, {})
    canonical_path = page_meta.get("canonical_path", current_path)
    title = page_meta.get("title", SEO_DEFAULTS["title"])
    description = page_meta.get("description", SEO_DEFAULTS["description"])
    robots = page_meta.get("robots", SEO_DEFAULTS["robots"])

    return {
        "title": title,
        "description": description,
        "canonical_url": _canonical_url(canonical_path),
        "robots": robots,
        "og_title": page_meta.get("og_title", title),
        "og_description": page_meta.get("og_description", description),
        "og_type": page_meta.get("og_type", SEO_DEFAULTS["og_type"]),
        "twitter_card": page_meta.get("twitter_card", SEO_DEFAULTS["twitter_card"]),
        "og_image": page_meta.get("og_image", SEO_DEFAULTS["og_image"]),
        "twitter_image": page_meta.get("twitter_image", page_meta.get("og_image", SEO_DEFAULTS["og_image"])),
    }


def _normalize_game_slug(slug):
    return (slug or "").strip().strip("/").lower()


def _get_game(game_slug):
    normalized = _normalize_game_slug(game_slug)
    return next((game for game in GAME_CATALOG if game["slug"] == normalized), None)


def _completed_game_slugs_for_user(user=None):
    if not getattr(user, "is_authenticated", False):
        return set()

    completed_rows = (
        db.session.query(GameScore.game_slug)
        .filter_by(user_id=user.id)
        .distinct()
        .all()
    )
    return {row[0] for row in completed_rows}


def _hydrate_game_catalog(user=None):
    completed_slugs = _completed_game_slugs_for_user(user)
    hydrated_games = []

    for index, game in enumerate(GAME_CATALOG):
        item = dict(game)
        prerequisite_slug = item.get("unlock_after")
        prerequisite = _get_game(prerequisite_slug)
        is_playable = bool(item.get("playable"))

        item["sequence"] = index + 1
        item["sequence_label"] = f"Game {index + 1}"
        item["prerequisite_title"] = prerequisite["title"] if prerequisite else ""
        item["prerequisite_route"] = prerequisite["route"] if prerequisite else ""
        item["has_completion"] = item["slug"] in completed_slugs
        item["is_playable"] = is_playable
        item["is_unlocked"] = is_playable

        if not item["is_playable"]:
            item["locked_reason"] = "Coming soon. This game will open when it is ready."
        else:
            item["locked_reason"] = ""

        hydrated_games.append(item)

    return hydrated_games


def _hydrate_game(game_slug, user=None):
    normalized = _normalize_game_slug(game_slug)
    return next((game for game in _hydrate_game_catalog(user) if game["slug"] == normalized), None)


def _load_interactive_manifest():
    if not INTERACTIVE_GAME_MANIFEST_PATH.exists():
        return None
    with INTERACTIVE_GAME_MANIFEST_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _get_interactive_entry():
    manifest = _load_interactive_manifest()
    if not manifest:
        return None

    entry = manifest.get("index.html")
    if not entry or not entry.get("file"):
        return None

    return {
        "script": f"/static/games/interactive/{entry['file']}",
        "css": [f"/static/games/interactive/{path}" for path in entry.get("css", [])],
    }


def _serialize_game_score(score):
    return {
        "id": f"score-{score.id}",
        "createdAt": score.created_at.isoformat() + "Z" if score.created_at else "",
        "email": "",
        "name": score.display_name,
        "score": int(score.score or 0),
        "returnPercent": float(score.return_percent or 0),
        "moves": int(score.moves or 0),
        "reallocations": int(score.reallocations or 0),
        "taxPaid": float(score.tax_paid or 0),
    }


def _weekly_seed_int(*parts):
    payload = "|".join(str(part) for part in parts).encode("utf-8")
    return int(hashlib.sha256(payload).hexdigest()[:12], 16)


def _seeded_range_value(game_slug, week_key, label, low, high):
    return low + (_weekly_seed_int(game_slug, week_key, label) % (high - low + 1))


def _round_seed_score(value):
    return int(round(value / GAME_SEEDED_SCORE_STEP) * GAME_SEEDED_SCORE_STEP)


def _seeded_weekly_game_score(game, cutoff):
    game_slug = game["slug"]
    week_key = cutoff.strftime("%Y-%m-%dT%H:%M:%S")
    score_min, score_max = GAME_SEEDED_SCORE_RANGES.get(game_slug, (125_000, 175_000))
    raw_score = _seeded_range_value(game_slug, week_key, "score", score_min, score_max)
    score = _round_seed_score(raw_score)
    name = GAME_SEEDED_SCORE_NAMES[
        _weekly_seed_int(game_slug, week_key, "name") % len(GAME_SEEDED_SCORE_NAMES)
    ]
    created_at = cutoff + datetime.timedelta(
        minutes=_seeded_range_value(game_slug, week_key, "minute", 8, 360)
    )
    tax_paid = _seeded_range_value(game_slug, week_key, "tax", 0, 7_500)
    moves = _seeded_range_value(game_slug, week_key, "moves", 4, 14)
    reallocations = _seeded_range_value(game_slug, week_key, "reallocations", 2, 9)

    return {
        "id": f"seed-{game_slug}-{cutoff.strftime('%Y%m%d')}",
        "createdAt": created_at.isoformat() + "Z",
        "email": "",
        "name": name,
        "score": score,
        "returnPercent": ((score - GAME_SEEDED_SCORE_BASE) / GAME_SEEDED_SCORE_BASE) * 100,
        "moves": moves,
        "reallocations": reallocations,
        "taxPaid": float(tax_paid),
        "seeded": True,
        "gameSlug": game_slug,
        "gameTitle": game["title"],
    }


def _leaderboard_week_start_utc(now=None):
    current_utc = now or datetime.datetime.utcnow()
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=datetime.timezone.utc)
    local_now = current_utc.astimezone(GAME_SCORE_RESET_TZ)
    reset_local = (local_now - datetime.timedelta(days=local_now.weekday())).replace(
        hour=0,
        minute=1,
        second=0,
        microsecond=0,
    )
    if local_now < reset_local:
        reset_local -= datetime.timedelta(days=7)
    return reset_local.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def _leaderboard_cutoff_utc():
    return max(_leaderboard_week_start_utc(), GAME_SCORE_RESET_EPOCH_UTC)


def _prune_old_game_scores():
    cutoff = _leaderboard_cutoff_utc()
    deleted = GameScore.query.filter(GameScore.created_at < cutoff).delete(synchronize_session=False)
    if deleted:
        db.session.commit()
    return cutoff


def _normalize_score_display_name(value):
    cleaned = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:24]


def _score_name_has_blocked_token(value):
    compact = re.sub(r"[^a-z0-9]", "", (value or "").lower())
    tokens = re.findall(r"[a-z0-9]+", (value or "").lower())
    return any(compact == word or word in tokens for word in GAME_SCORE_NAME_BLOCKLIST)


def _validate_score_display_name(value):
    name = _normalize_score_display_name(value)
    if len(name) < 2:
        return None, "Enter a display name."
    if re.search(r"@|https?:|www\.", name, re.I):
        return None, "Use a display name, not an email or link."
    if not GAME_SCORE_NAME_RE.match(name):
        return None, "Names can use letters, numbers, spaces, apostrophes, periods, and hyphens."
    if _score_name_has_blocked_token(name):
        return None, "Choose a different display name."
    return name, None


def _anonymous_score_user_id():
    user = User.query.filter_by(email="weekly-scores@chargedalpha.local").first()
    if not user:
        user = User(
            email="weekly-scores@chargedalpha.local",
            name="Weekly Scores",
            provider="system",
        )
        db.session.add(user)
        db.session.flush()
    return user.id


def _ranked_game_scores(game, limit=None, cutoff=None):
    cutoff = cutoff or _prune_old_game_scores()
    query = (
        GameScore.query.filter_by(game_slug=game["slug"])
        .filter(GameScore.created_at >= cutoff)
        .order_by(GameScore.score.desc(), GameScore.created_at.asc())
    )
    if limit:
        query = query.limit(limit)

    entries = []
    for score in query.all():
        entry = _serialize_game_score(score)
        entry["gameSlug"] = game["slug"]
        entry["gameTitle"] = game["title"]
        entries.append(entry)

    entries.append(_seeded_weekly_game_score(game, cutoff))
    entries.sort(key=lambda entry: (-entry["score"], entry["createdAt"]))
    if limit:
        entries = entries[:limit]
    for rank, entry in enumerate(entries, start=1):
        entry["rank"] = rank
    return entries


def _coerce_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _coerce_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_shows_catalog():
    if not SHOWS_CATALOG_PATH.exists():
        return {"platform_links": {}, "episodes": []}
    with SHOWS_CATALOG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _show_slug(ticker):
    return (ticker or "").upper().replace(".", "-").replace("/", "-").strip()


def _quarter_sort_key(label):
    text = (label or "").upper()
    quarter_match = re.search(r"Q([1-4])", text)
    year_match = re.search(r"(20\d{2})", text)
    quarter = int(quarter_match.group(1)) if quarter_match else 0
    year = int(year_match.group(1)) if year_match else 0
    return (year, quarter, text)


def _episode_sort_key(ep):
    return (*_quarter_sort_key(ep.get("quarter")), ep.get("published_at") or "")


def _episode_has_any_link(ep):
    return any(
        ep.get(key)
        for key in (
            "youtube_url",
            "spotify_url",
            "apple_url",
            "google_url",
            "iheart_url",
            "amazon_url",
            "podbean_url",
        )
    )


def build_show_library(episodes):
    grouped = {}
    quarter_set = set()
    published_episode_count = 0
    youtube_episode_count = 0
    podcast_episode_count = 0

    for ep in episodes or []:
        ticker = (ep.get("ticker") or "").upper().strip()
        if not ticker:
            continue

        slug = _show_slug(ticker)
        quarter = (ep.get("quarter") or "Unknown").strip()
        quarter_set.add(quarter)
        has_youtube = bool(ep.get("youtube_url"))
        has_any_link = bool(ep.get("has_episode") or _episode_has_any_link(ep))
        has_podcast = bool(ep.get("spotify_url") or ep.get("podbean_url") or ep.get("apple_url") or ep.get("amazon_url"))

        if has_any_link:
            published_episode_count += 1
        if has_youtube:
            youtube_episode_count += 1
        if has_podcast:
            podcast_episode_count += 1

        stock = grouped.setdefault(
            slug,
            {
                "slug": slug,
                "ticker": ticker,
                "yf_symbol": ticker.replace(".", "-"),
                "company": ep.get("company") or ticker,
                "sector": ep.get("sector") or "Unclassified",
                "episodes": [],
            },
        )

        stock["episodes"].append(
            {
                "ticker": ticker,
                "quarter": quarter,
                "title": ep.get("title") or ep.get("episode_title") or f"{ticker} {quarter} earnings analysis",
                "episode_number": ep.get("episode_number") or "",
                "published_at": ep.get("published_at") or "",
                "status": ep.get("status") or ("youtube_live" if has_youtube else ("linked_elsewhere" if has_any_link else "planned")),
                "has_episode": has_any_link,
                "has_any_link": has_any_link,
                "youtube_url": ep.get("youtube_url") or "",
                "spotify_url": ep.get("spotify_url") or "",
                "apple_url": ep.get("apple_url") or "",
                "google_url": ep.get("google_url") or "",
                "iheart_url": ep.get("iheart_url") or "",
                "amazon_url": ep.get("amazon_url") or "",
                "podbean_url": ep.get("podbean_url") or "",
            }
        )

    stocks = []
    for stock in grouped.values():
        stock["episodes"].sort(key=_episode_sort_key, reverse=True)
        latest = stock["episodes"][0]
        latest_youtube = next((ep for ep in stock["episodes"] if ep.get("youtube_url")), None)
        latest_spotify = next((ep for ep in stock["episodes"] if ep.get("spotify_url")), None)
        latest_podcast = next((ep for ep in stock["episodes"] if ep.get("podbean_url")), None)
        dated_episodes = [ep for ep in stock["episodes"] if _parse_datetime(ep.get("published_at"))]
        latest_published = max(
            dated_episodes,
            key=lambda ep: _parse_datetime(ep.get("published_at")),
            default=latest,
        )

        stock["quarter_count"] = len(stock["episodes"])
        stock["published_count"] = sum(1 for ep in stock["episodes"] if ep.get("has_any_link"))
        stock["youtube_count"] = sum(1 for ep in stock["episodes"] if ep.get("youtube_url"))
        stock["podcast_count"] = sum(1 for ep in stock["episodes"] if ep.get("podbean_url") or ep.get("spotify_url"))
        stock["latest_quarter"] = latest["quarter"]
        stock["latest_published_at"] = latest_published.get("published_at") or latest.get("published_at") or ""
        stock["latest_video_quarter"] = latest_youtube["quarter"] if latest_youtube else None
        stock["latest_video_title"] = latest_youtube["title"] if latest_youtube else ""
        stock["latest_video_published_at"] = latest_youtube["published_at"] if latest_youtube else ""
        stock["latest_video_thumbnail"] = _youtube_thumbnail_url(latest_youtube.get("youtube_url")) if latest_youtube else DEFAULT_SOCIAL_IMAGE_URL
        stock["latest_youtube_embed_url"] = _youtube_embed_url(latest_youtube.get("youtube_url")) if latest_youtube else ""
        stock["latest_status"] = latest_youtube["status"] if latest_youtube else latest["status"]
        stock["quarter_labels"] = [ep["quarter"] for ep in stock["episodes"]]
        stock["latest_links"] = {
            "youtube": latest_youtube.get("youtube_url") if latest_youtube else "",
            "spotify": latest_spotify.get("spotify_url") if latest_spotify else "",
            "podcast": latest_podcast.get("podbean_url") if latest_podcast else "",
        }
        stock["latest_youtube_url"] = latest_youtube.get("youtube_url") if latest_youtube else ""
        stock["latest_spotify_url"] = latest_spotify.get("spotify_url") if latest_spotify else ""
        stock["latest_podcast_url"] = latest_podcast.get("podbean_url") if latest_podcast else ""
        stock["has_youtube"] = bool(latest_youtube)
        stock["has_podcast"] = bool(latest_spotify or latest_podcast)
        stock["latest_quarter_sort"] = _quarter_sort_key(stock["latest_video_quarter"] or stock["latest_quarter"])
        stocks.append(stock)

    stocks.sort(
        key=lambda stock: (stock["latest_quarter_sort"], stock["published_count"], stock["ticker"]),
        reverse=True,
    )

    quarter_options = sorted(quarter_set, key=_quarter_sort_key, reverse=True)
    sector_options = sorted({stock["sector"] for stock in stocks})

    return {
        "stocks": stocks,
        "quarters": quarter_options,
        "sectors": sector_options,
        "stats": {
            "stock_count": len(stocks),
            "episode_count": len(episodes or []),
            "published_episode_count": published_episode_count,
            "youtube_episode_count": youtube_episode_count,
            "podcast_episode_count": podcast_episode_count,
            "quarter_count": len(quarter_options),
        },
    }


def build_show_client_stocks(stocks):
    client_stocks = []
    for stock in stocks or []:
        search_text = " ".join(
            [
                stock.get("ticker") or "",
                stock.get("company") or "",
                stock.get("sector") or "",
                " ".join(
                    f"{episode.get('title') or ''} {episode.get('quarter') or ''}"
                    for episode in stock.get("episodes", [])
                ),
            ]
        )
        client_stocks.append(
            {
                "slug": stock.get("slug"),
                "ticker": stock.get("ticker"),
                "company": stock.get("company"),
                "sector": stock.get("sector"),
                "quarter_count": stock.get("quarter_count", 0),
                "published_count": stock.get("published_count", 0),
                "youtube_count": stock.get("youtube_count", 0),
                "podcast_count": stock.get("podcast_count", 0),
                "latest_quarter": stock.get("latest_quarter"),
                "latest_published_at": stock.get("latest_published_at"),
                "latest_video_quarter": stock.get("latest_video_quarter"),
                "latest_video_title": stock.get("latest_video_title"),
                "latest_video_published_at": stock.get("latest_video_published_at"),
                "latest_video_thumbnail": stock.get("latest_video_thumbnail"),
                "latest_youtube_embed_url": stock.get("latest_youtube_embed_url"),
                "latest_status": stock.get("latest_status"),
                "quarter_labels": stock.get("quarter_labels", []),
                "latest_links": stock.get("latest_links", {}),
                "latest_youtube_url": stock.get("latest_youtube_url"),
                "latest_spotify_url": stock.get("latest_spotify_url"),
                "latest_podcast_url": stock.get("latest_podcast_url"),
                "has_youtube": stock.get("has_youtube", False),
                "has_podcast": stock.get("has_podcast", False),
                "latest_quarter_sort": stock.get("latest_quarter_sort", (0, 0, "")),
                "search_text": search_text,
            }
        )
    return client_stocks


def flatten_video_sections(video_sections):
    videos = []
    for section in video_sections or []:
        section_title = section.get("title") or "Research Videos"
        for video in section.get("videos") or []:
            item = dict(video)
            item["section_title"] = section_title
            videos.append(item)
    return videos


def _video_object_schema(title, youtube_url, published_at="", description="", thumbnail_url="", page_url=""):
    upload_date = _video_upload_date(published_at)
    if not youtube_url or not upload_date:
        return None
    schema = {
        "@context": "https://schema.org",
        "@type": "VideoObject",
        "name": title,
        "description": description or title,
        "thumbnailUrl": [thumbnail_url or _youtube_thumbnail_url(youtube_url)],
        "uploadDate": upload_date,
        "contentUrl": youtube_url,
        "url": page_url or youtube_url,
        "publisher": {
            "@type": "Organization",
            "name": "Charged Alpha",
            "url": SITE_URL,
            "logo": {
                "@type": "ImageObject",
                "url": DEFAULT_SOCIAL_IMAGE_URL,
            },
        },
    }
    embed_url = _youtube_embed_url(youtube_url)
    if embed_url:
        schema["embedUrl"] = embed_url
    return schema


def _website_schema():
    return {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": "Charged Alpha",
        "url": SITE_URL,
        "potentialAction": {
            "@type": "SearchAction",
            "target": f"{SITE_URL}/shows?search={{search_term_string}}",
            "query-input": "required name=search_term_string",
        },
    }


def _shows_collection_schema(path, show_library):
    page_url = _canonical_url(path)
    stocks = show_library.get("stocks", [])[:24]
    return {
        "@context": "https://schema.org",
        "@type": "CollectionPage",
        "name": "Charged Alpha Stock Earnings Video Library" if path == "/" else "Charged Alpha Shows",
        "description": _get_seo_meta(path)["description"],
        "url": page_url,
        "isPartOf": {
            "@type": "WebSite",
            "name": "Charged Alpha",
            "url": SITE_URL,
        },
        "mainEntity": {
            "@type": "ItemList",
            "numberOfItems": show_library.get("stats", {}).get("stock_count", len(stocks)),
            "itemListElement": [
                {
                    "@type": "ListItem",
                    "position": index + 1,
                    "url": _canonical_url(f"/shows/{stock['slug']}"),
                    "name": f"{stock['company']} ({stock['ticker']})",
                }
                for index, stock in enumerate(stocks)
            ],
        },
    }


def _shows_page_structured_data(path, show_library):
    return [
        _website_schema(),
        _shows_collection_schema(path, show_library),
    ]


def _stock_page_structured_data(show_stock, seo_meta):
    schemas = [
        {
            "@context": "https://schema.org",
            "@type": "CollectionPage",
            "name": seo_meta["title"],
            "description": seo_meta["description"],
            "url": seo_meta["canonical_url"],
            "isPartOf": {
                "@type": "WebSite",
                "name": "Charged Alpha",
                "url": SITE_URL,
            },
            "about": {
                "@type": "Organization",
                "name": show_stock["company"],
                "tickerSymbol": show_stock["ticker"],
            },
            "mainEntity": {
                "@type": "ItemList",
                "numberOfItems": len(show_stock.get("episodes", [])),
                "itemListElement": [
                    {
                        "@type": "ListItem",
                        "position": index + 1,
                        "name": episode.get("title"),
                        "url": episode.get("youtube_url") or episode.get("spotify_url") or episode.get("podbean_url") or seo_meta["canonical_url"],
                    }
                    for index, episode in enumerate(show_stock.get("episodes", [])[:24])
                ],
            },
        }
    ]
    latest_video = next(
        (
            episode
            for episode in show_stock.get("episodes", [])
            if episode.get("youtube_url") and _video_upload_date(episode.get("published_at"))
        ),
        None,
    )
    if latest_video:
        schema = _video_object_schema(
            latest_video.get("title"),
            latest_video.get("youtube_url"),
            latest_video.get("published_at"),
            f"Charged Alpha earnings analysis for {show_stock['company']} ({show_stock['ticker']}) covering {latest_video.get('quarter')}.",
            _youtube_thumbnail_url(latest_video.get("youtube_url")),
            seo_meta["canonical_url"],
        )
        if schema:
            schemas.append(schema)
    return schemas


SHOW_COMPETITOR_MAP = {
    "AAPL": ["MSFT", "GOOGL"],
    "MSFT": ["AAPL", "GOOGL"],
    "GOOGL": ["META", "MSFT"],
    "AMZN": ["WMT", "COST"],
    "NVDA": ["AMD", "AVGO"],
    "META": ["GOOGL", "NFLX"],
    "TSLA": ["GM", "F"],
    "BRK.B": ["JPM", "GS"],
    "JPM": ["BAC", "GS"],
    "BAC": ["JPM", "C"],
    "C": ["JPM", "BAC"],
    "V": ["MA", "AXP"],
    "MA": ["V", "AXP"],
    "XOM": ["CVX", "CAT"],
    "CVX": ["XOM", "CAT"],
    "JNJ": ["MRK", "ABBV"],
    "MRK": ["JNJ", "ABBV"],
    "ABBV": ["JNJ", "MRK"],
    "WMT": ["COST", "AMZN"],
    "COST": ["WMT", "HD"],
    "PG": ["KO", "PEP"],
    "KO": ["PEP", "PG"],
    "PEP": ["KO", "PG"],
    "HD": ["WMT", "COST"],
    "AVGO": ["NVDA", "AMD"],
    "ORCL": ["MSFT", "CSCO"],
    "INTC": ["AMD", "NVDA"],
    "QCOM": ["AMD", "AVGO"],
    "GS": ["JPM", "MS"],
    "MS": ["JPM", "GS"],
    "CAT": ["DE", "GE"],
    "DE": ["CAT", "GE"],
    "NFLX": ["GOOGL", "META"],
    "AMD": ["NVDA", "INTC"],
    "F": ["GM", "TSLA"],
    "GM": ["F", "TSLA"],
}


COMPARE_METRICS = [
    {"key": "market_cap", "label": "Market Cap", "format": "compact_currency", "prefer": "higher", "why": "More scale can mean deeper resources and resilience, although bigger does not automatically mean better upside."},
    {"key": "trailing_pe", "label": "Trailing P/E", "format": "multiple", "prefer": "lower", "why": "Lower trailing P/E can indicate a cheaper valuation relative to trailing earnings, but it may also reflect slower growth or higher perceived risk."},
    {"key": "forward_pe", "label": "Forward P/E", "format": "multiple", "prefer": "lower", "why": "Forward P/E is often a better read on what investors are paying for the next year of earnings power."},
    {"key": "revenue_growth", "label": "Revenue Growth", "format": "percent", "prefer": "higher", "why": "Higher revenue growth usually signals stronger demand, market share gains, or a business still in expansion mode."},
    {"key": "earnings_growth", "label": "Earnings Growth", "format": "percent", "prefer": "higher", "why": "Faster earnings growth matters because it shows management is converting sales momentum into shareholder value."},
    {"key": "operating_margin", "label": "Operating Margin", "format": "percent", "prefer": "higher", "why": "Higher operating margin suggests better operating discipline, pricing power, or a structurally stronger business model."},
    {"key": "gross_margin", "label": "Gross Margin", "format": "percent", "prefer": "higher", "why": "Gross margin helps show how much product-level pricing power and unit economics a company has before overhead."},
    {"key": "profit_margin", "label": "Net Margin", "format": "percent", "prefer": "higher", "why": "Higher net margin means more of each dollar of revenue reaches the bottom line after all costs."},
    {"key": "return_on_equity", "label": "Return on Equity", "format": "percent", "prefer": "higher", "why": "ROE shows how efficiently management turns shareholder capital into profits, though leverage can inflate it."},
    {"key": "fcf_yield", "label": "Free Cash Flow Yield", "format": "percent", "prefer": "higher", "why": "Higher free cash flow yield can indicate a stronger cash return relative to the stock's market value."},
    {"key": "debt_to_equity", "label": "Debt to Equity", "format": "ratio", "prefer": "lower", "why": "Lower leverage usually means less balance-sheet risk, though capital-intensive sectors naturally run higher debt loads."},
    {"key": "current_ratio", "label": "Current Ratio", "format": "ratio", "prefer": "higher", "why": "A stronger current ratio usually signals better short-term liquidity and more room to absorb shocks."},
    {"key": "beta", "label": "Beta", "format": "number", "prefer": "lower", "why": "Lower beta often means lower volatility versus the market, while higher beta usually brings a rougher ride."},
    {"key": "dividend_yield", "label": "Dividend Yield", "format": "percent", "prefer": "higher", "why": "Dividend yield matters for income-focused investors, but a high yield can also reflect a stressed stock price."},
    {"key": "target_upside", "label": "Analyst Upside", "format": "percent", "prefer": "higher", "why": "Higher analyst upside suggests the Street still sees room between current price and consensus fair value."},
]


def _format_compare_value(value, fmt):
    if value is None:
        return "—"
    if fmt == "currency":
        return f"${value:,.2f}"
    if fmt == "compact_currency":
        abs_value = abs(float(value))
        if abs_value >= 1_000_000_000_000:
            return f"${value / 1_000_000_000_000:.2f}T"
        if abs_value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}B"
        if abs_value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        return f"${value:,.0f}"
    if fmt == "multiple":
        return f"{value:.1f}x"
    if fmt == "ratio":
        return f"{value:.2f}x"
    if fmt == "percent":
        return f"{value:.1f}%"
    if fmt == "int":
        return f"{int(round(value)):,}"
    return f"{value:.2f}" if isinstance(value, float) else str(value)


def _comparison_insights(snapshot):
    growth = snapshot.get("revenue_growth")
    earnings = snapshot.get("earnings_growth")
    margin = snapshot.get("operating_margin")
    forward_pe = snapshot.get("forward_pe")
    debt = snapshot.get("debt_to_equity")
    upside = snapshot.get("target_upside")
    beta = snapshot.get("beta")
    fcf_yield = snapshot.get("fcf_yield")

    points = []
    if growth is not None or earnings is not None:
        if (growth or 0) >= 20 or (earnings or 0) >= 20:
            points.append("Growth profile looks strong right now, with above-average top-line and/or earnings momentum.")
        elif (growth or 0) < 5 and (earnings or 0) < 5:
            points.append("Growth profile looks mature or currently muted, which can cap multiple expansion unless execution improves.")
        else:
            points.append("Growth is positive but not explosive, which usually supports a steadier compounding case than a hyper-growth story.")

    if margin is not None or fcf_yield is not None:
        if (margin or 0) >= 30:
            points.append("Profitability is a real strength here, with healthy operating margins helping support resilience through weaker cycles.")
        elif fcf_yield is not None and fcf_yield > 3:
            points.append("Cash generation stands out versus market value, which helps the stock absorb valuation pressure better than weaker cash converters.")
        else:
            points.append("Profitability is serviceable, but it does not obviously dominate peers on margin or cash conversion alone.")

    if forward_pe is not None or upside is not None:
        if forward_pe is not None and forward_pe >= 30:
            points.append("Valuation already asks investors to pay up, so the upside case depends on continued execution staying strong.")
        elif forward_pe is not None and forward_pe <= 18:
            points.append("Valuation looks more grounded than many growth names, which can improve the risk/reward if fundamentals hold up.")
        elif upside is not None and upside >= 20:
            points.append("Consensus analyst targets still imply meaningful upside, suggesting the Street thinks the current price leaves room for appreciation.")
        else:
            points.append("Valuation sits in a middle zone where future upside likely depends more on quarterly execution than on multiple re-rating alone.")

    if debt is not None or beta is not None:
        if debt is not None and debt > 100:
            points.append("Balance-sheet leverage is elevated, so investors should watch refinancing costs and how much flexibility management really has.")
        elif beta is not None and beta >= 1.5:
            points.append("Expect a more volatile ride than the market average; that can amplify upside, but drawdowns can come fast too.")
        else:
            points.append("Risk profile looks relatively manageable compared with many peers, especially if operating execution remains stable.")

    return points[:4]


def _pick_competitor_stocks(show_stock, all_stocks):
    stock_by_ticker = {stock["ticker"]: stock for stock in all_stocks}
    picks = []
    for ticker in SHOW_COMPETITOR_MAP.get(show_stock["ticker"], []) + SHOW_COMPETITOR_MAP.get(show_stock["ticker"].replace("-", "."), []):
        normalized = ticker.replace(".", "-")
        stock = stock_by_ticker.get(ticker) or stock_by_ticker.get(normalized)
        if stock and stock["ticker"] != show_stock["ticker"] and stock not in picks:
            picks.append(stock)
        if len(picks) == 2:
            return picks

    sector_peers = [
        stock for stock in all_stocks
        if stock["ticker"] != show_stock["ticker"] and stock.get("sector") == show_stock.get("sector")
    ]
    sector_peers.sort(key=lambda stock: (stock.get("published_count", 0), stock.get("latest_quarter_sort", (0, 0, "")), stock.get("ticker")), reverse=True)
    for stock in sector_peers:
        if stock not in picks:
            picks.append(stock)
        if len(picks) == 2:
            break
    return picks[:2]


def _number_or_none(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _youtube_thumbnail_url(url):
    video_id = _youtube_video_id(url)
    if not video_id:
        return DEFAULT_SOCIAL_IMAGE_URL
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


def _build_fast_show_stock_detail(symbol):
    t, info = fetch_ticker_info(symbol)
    if not info:
        return {}

    try:
        fast = t.fast_info or {}
    except Exception:
        fast = {}

    def fast_get(key):
        try:
            return fast.get(key)
        except Exception:
            return None

    def pick_number(*values, decimals=2, scale=1.0, as_int=False):
        for value in values:
            num = _number_or_none(value)
            if num is None:
                continue
            num *= scale
            if as_int:
                return int(round(num))
            return round(num, decimals)
        return None

    price = pick_number(fast_get("lastPrice"), info.get("currentPrice"), info.get("regularMarketPrice"), info.get("previousClose"))
    previous_close = pick_number(fast_get("previousClose"), info.get("previousClose"), info.get("regularMarketPreviousClose"))
    market_cap = pick_number(fast_get("marketCap"), info.get("marketCap"), decimals=0)
    volume = pick_number(fast_get("lastVolume"), info.get("volume"), info.get("regularMarketVolume"), as_int=True)
    target_mean_price = pick_number(info.get("targetMeanPrice"))
    free_cashflow = pick_number(info.get("freeCashflow"), decimals=0)

    change = change_pct = target_upside = fcf_yield = None
    if price is not None and previous_close not in (None, 0):
        change = round(price - previous_close, 2)
        change_pct = round((price - previous_close) / previous_close * 100, 2)
    if target_mean_price and price and price > 0:
        target_upside = round((target_mean_price - price) / price * 100, 1)
    if free_cashflow and market_cap and market_cap > 0:
        fcf_yield = round(free_cashflow / market_cap * 100, 2)

    return {
        "symbol": symbol,
        "name": info.get("longName") or info.get("shortName") or symbol,
        "price": price,
        "previous_close": previous_close,
        "change": change,
        "change_pct": change_pct,
        "week_52_high": pick_number(fast_get("yearHigh"), info.get("fiftyTwoWeekHigh")),
        "week_52_low": pick_number(fast_get("yearLow"), info.get("fiftyTwoWeekLow")),
        "trailing_pe": pick_number(info.get("trailingPE")),
        "forward_pe": pick_number(info.get("forwardPE")),
        "eps": pick_number(info.get("trailingEps")),
        "price_to_book": pick_number(info.get("priceToBook")),
        "market_cap": market_cap,
        "beta": pick_number(info.get("beta")),
        "dividend_yield": normalize_div_yield(info.get("trailingAnnualDividendYield") or info.get("dividendYield")),
        "volume": volume,
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "target_mean_price": target_mean_price,
        "revenue_growth": safe_float(info, "revenueGrowth", scale=100),
        "earnings_growth": safe_float(info, "earningsGrowth", scale=100),
        "debt_to_equity": pick_number(info.get("debtToEquity")),
        "current_ratio": pick_number(info.get("currentRatio")),
        "operating_margin": safe_float(info, "operatingMargins", scale=100),
        "gross_margin": safe_float(info, "grossMargins", scale=100),
        "profit_margin": safe_float(info, "profitMargins", scale=100),
        "return_on_equity": safe_float(info, "returnOnEquity", scale=100),
        "free_cashflow": free_cashflow,
        "fcf_yield": fcf_yield,
        "target_upside": target_upside,
        "summary": info.get("longBusinessSummary") or "",
        "website": info.get("website") or "",
        "country": info.get("country") or "",
        "employees": info.get("fullTimeEmployees"),
    }


def _compact_stock_snapshot(show_stock, allow_fetch=False):
    detail_bundle = _cached_show_stock_detail(show_stock["yf_symbol"], allow_fetch=allow_fetch)
    info = detail_bundle.get("info") if detail_bundle else {}
    info = info or {}

    def pick(key, scale=1.0):
        value = info.get(key)
        if value is None:
            return None
        try:
            return round(float(value) * scale, 4)
        except (TypeError, ValueError):
            return None

    market_cap = info.get("market_cap")
    free_cashflow = info.get("free_cashflow")
    price = info.get("price")
    target_mean_price = info.get("target_mean_price")
    fcf_yield = None
    target_upside = None

    try:
        market_cap = float(market_cap) if market_cap is not None else None
    except (TypeError, ValueError):
        market_cap = None
    try:
        free_cashflow = float(free_cashflow) if free_cashflow is not None else None
    except (TypeError, ValueError):
        free_cashflow = None
    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    try:
        target_mean_price = float(target_mean_price) if target_mean_price is not None else None
    except (TypeError, ValueError):
        target_mean_price = None

    if free_cashflow and market_cap and market_cap > 0:
        fcf_yield = round(free_cashflow / market_cap * 100, 2)
    if target_mean_price and price and price > 0:
        target_upside = round((target_mean_price - price) / price * 100, 1)

    return {
        "ticker": show_stock["ticker"],
        "company": show_stock["company"],
        "latest_video_quarter": show_stock.get("latest_video_quarter"),
        "latest_youtube_url": show_stock.get("latest_youtube_url"),
        "latest_youtube_embed_url": show_stock.get("latest_youtube_embed_url"),
        "latest_spotify_url": show_stock.get("latest_spotify_url"),
        "youtube_thumbnail_url": _youtube_thumbnail_url(show_stock.get("latest_youtube_url")),
        "market_cap": market_cap,
        "trailing_pe": pick("trailing_pe"),
        "forward_pe": pick("forward_pe"),
        "revenue_growth": pick("revenue_growth"),
        "earnings_growth": pick("earnings_growth"),
        "operating_margin": pick("operating_margin"),
        "gross_margin": pick("gross_margin"),
        "profit_margin": pick("profit_margin"),
        "return_on_equity": pick("return_on_equity"),
        "fcf_yield": fcf_yield,
        "debt_to_equity": pick("debt_to_equity"),
        "current_ratio": pick("current_ratio"),
        "beta": pick("beta"),
        "dividend_yield": pick("dividend_yield"),
        "target_upside": target_upside,
    }


def build_stock_competitor_analysis(show_stock, primary_snapshot, all_stocks):
    competitor_stocks = _pick_competitor_stocks(show_stock, all_stocks)
    snapshots = []

    primary = dict(primary_snapshot or {})
    primary.update({
        "ticker": show_stock["ticker"],
        "company": show_stock["company"],
        "latest_video_quarter": show_stock.get("latest_video_quarter"),
        "latest_youtube_url": show_stock.get("latest_youtube_url"),
        "latest_youtube_embed_url": show_stock.get("latest_youtube_embed_url"),
        "latest_spotify_url": show_stock.get("latest_spotify_url"),
        "youtube_thumbnail_url": _youtube_thumbnail_url(show_stock.get("latest_youtube_url")),
    })
    snapshots.append(primary)

    if competitor_stocks:
        with ThreadPoolExecutor(max_workers=min(2, len(competitor_stocks))) as ex:
            snapshots.extend(ex.map(lambda stock: _compact_stock_snapshot(stock, allow_fetch=True), competitor_stocks))

    rows = []
    for metric in COMPARE_METRICS:
        values = [snap.get(metric["key"]) for snap in snapshots]
        numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
        best_value = worst_value = None
        if len(numeric_values) >= 2 and metric["prefer"] in ("higher", "lower"):
            best_value = max(numeric_values) if metric["prefer"] == "higher" else min(numeric_values)
            worst_value = min(numeric_values) if metric["prefer"] == "higher" else max(numeric_values)

        entries = []
        for snap in snapshots:
            value = snap.get(metric["key"])
            status = "neutral"
            if isinstance(value, (int, float)) and best_value is not None and worst_value is not None:
                if abs(float(value) - best_value) < 1e-9:
                    status = "best"
                elif abs(float(value) - worst_value) < 1e-9:
                    status = "worst"
                else:
                    status = "middle"
            entries.append({
                "ticker": snap.get("ticker"),
                "company": snap.get("company"),
                "value": value,
                "display": _format_compare_value(value, metric["format"]),
                "status": status,
            })

        rows.append({
            "label": metric["label"],
            "why": metric["why"],
            "entries": entries,
        })

    cards = []
    for snap in snapshots:
        cards.append({
            "ticker": snap.get("ticker"),
            "company": snap.get("company"),
            "latest_video_quarter": snap.get("latest_video_quarter") or "YouTube link pending",
            "latest_youtube_url": snap.get("latest_youtube_url") or "",
            "latest_youtube_embed_url": snap.get("latest_youtube_embed_url") or "",
            "latest_spotify_url": snap.get("latest_spotify_url") or "",
            "youtube_thumbnail_url": snap.get("youtube_thumbnail_url") or _youtube_thumbnail_url(snap.get("latest_youtube_url")),
            "insights": _comparison_insights(snap),
        })

    notes = []
    if show_stock.get("sector") == "Financials":
        notes.append("Financial companies often look unusual on debt and liquidity ratios, so compare those rows more carefully than you would for non-financial businesses.")

    return {
        "stocks": cards,
        "rows": rows,
        "notes": notes,
    }

app.url_map.strict_slashes = False
app.config['MAX_CONTENT_LENGTH'] = 1 * 1024 * 1024  # 1MB max request body
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-change-in-production')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///charged_alpha.db')
app.config['PREFERRED_URL_SCHEME'] = "https" if SITE_URL.startswith("https://") else "http"
# Railway Postgres uses postgres:// but SQLAlchemy needs postgresql://
if app.config['SQLALCHEMY_DATABASE_URI'].startswith('postgres://'):
    app.config['SQLALCHEMY_DATABASE_URI'] = app.config['SQLALCHEMY_DATABASE_URI'].replace(
        'postgres://', 'postgresql://', 1)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    REMEMBER_COOKIE_HTTPONLY=True,
    REMEMBER_COOKIE_SAMESITE="Lax",
)
if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("FLASK_ENV") == "production":
    app.config.update(
        SESSION_COOKIE_SECURE=True,
        REMEMBER_COOKIE_SECURE=True,
    )
Compress(app)

# ── Database + Auth ────────────────────────────────────────────────────────
db.init_app(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


@login_manager.unauthorized_handler
def unauthorized():
    if request.path.startswith("/games/api/"):
        next_url = request.referrer or "/games/front-page-fortune"
        return jsonify({
            "ok": False,
            "error": "Public score posting is temporarily paused.",
            "auth_enabled": public_auth_enabled(),
            "login_url": url_for("auth.login", next=next_url) if public_auth_enabled() else "",
            "register_url": url_for("auth.register", next=next_url) if public_auth_enabled() else "",
        }), 401
    if not public_auth_enabled():
        abort(404)
    return redirect(url_for("auth.login", next=request.full_path if request.query_string else request.path))

init_oauth(app)
app.register_blueprint(auth_bp)

with app.app_context():
    db.create_all()

# ── Convenience redirects for auth ────────────────────────────────────────
@app.route("/login")
def login_redirect():
    if not public_auth_enabled():
        abort(404)
    return redirect("/auth/login" + ("?" + request.query_string.decode() if request.query_string else ""))

@app.route("/register")
def register_redirect():
    if not public_auth_enabled():
        abort(404)
    return redirect("/auth/register" + ("?" + request.query_string.decode() if request.query_string else ""))

# ── Shared job store (auto-cleans after 10 min) ────────────────────────────
job_store = JobStore(ttl=600)

# ── Shared caches ───────────────────────────────────────────────────────────
_detail_cache = TTLCache(default_ttl=300, max_size=500)
_banner_cache = TTLCache(default_ttl=120, max_size=10)
_shows_cache = TTLCache(default_ttl=300, max_size=5)

# ── Market cap range definitions ────────────────────────────────────────────
CAP_RANGES = {
    "micro":  (0,           300_000_000),
    "small":  (300_000_000, 2_000_000_000),
    "mid":    (2_000_000_000, 10_000_000_000),
    "large":  (10_000_000_000, 200_000_000_000),
    "mega":   (200_000_000_000, float("inf")),
}

BANNER_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "BRK-B", "JPM",
    "V", "UNH", "XOM", "JNJ", "WMT", "PG", "MA", "HD", "CVX", "MRK",
    "ABBV", "PEP", "KO", "COST", "BAC", "AVGO", "TMO", "MCD", "CSCO",
    "ACN", "NKE", "ORCL", "CRM", "AMGN", "INTC", "QCOM", "SBUX", "GS",
    "CAT", "BA", "DE", "GE", "IBM", "DIS", "NFLX", "PYPL", "AMD", "T",
    "F", "GM", "DAL",
]


# ── Helper ──────────────────────────────────────────────────────────────────
def _f_body(body, key, default=None):
    v = body.get(key)
    if v in (None, ""):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _extract_list(body, key):
    """Extract a list filter from request body, returning None if empty."""
    val = body.get(key)
    if val and isinstance(val, list):
        return [v for v in val if v] or None
    return None


def _cached_detail(cache_prefix, symbol, fetch_fn):
    """Shared pattern: cache check → fetch → error check → cache set → jsonify."""
    sym = symbol.upper()
    cache_key = f"{cache_prefix}_{sym}"
    cached = _detail_cache.get(cache_key)
    if cached:
        return jsonify(cached)
    data = fetch_fn(sym)
    if not data:
        return jsonify({"error": f"Could not load {cache_prefix} data"}), 404
    _detail_cache.set(cache_key, data)
    return jsonify(data)


def _shows_context():
    cached = _shows_cache.get("shows_context")
    if cached:
        return cached

    shows_data = load_shows_catalog()
    show_library = build_show_library(shows_data.get("episodes", []))
    context = {
        "shows_data": shows_data,
        "show_library": show_library,
        "show_client_stocks": build_show_client_stocks(show_library.get("stocks", [])),
    }
    _shows_cache.set("shows_context", context)
    return context


def _cached_show_stock_detail(symbol, allow_fetch=True):
    sym = symbol.upper()
    cache_key = f"show_stock_detail_{sym}"
    cached = _detail_cache.get(cache_key, ttl=900)
    if cached is not None:
        return cached
    if not allow_fetch:
        return {}

    data = {"info": _build_fast_show_stock_detail(sym), "options": []}
    _detail_cache.set(cache_key, data)
    return data


def _start_job(fn, *args):
    job_id = job_store.create()

    def run():
        try:
            def on_progress(p, t, **kw):
                job_store.set_progress(job_id, p, t, **kw)

            def on_match(m):
                job_store.append_match(job_id, m)

            fn(*args, on_progress=on_progress, on_match=on_match)
            job_store.update(job_id, status="done")
        except Exception as e:
            job_store.update(job_id, status="error", error=str(e))

    threading.Thread(target=run, daemon=True).start()
    return job_id


def _get_job(job_id):
    job = job_store.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


def _chart_helper(symbol, range_key, params_map=None):
    sym = symbol.upper()
    data = fetch_chart(sym, range_key, params_map=params_map)
    if data is None:
        return jsonify({"error": "No price data available"}), 404
    return jsonify(data)


@app.context_processor
def inject_seo_meta():
    return {
        "seo_meta": _get_seo_meta(),
        "google_analytics_id": GOOGLE_ANALYTICS_ID,
        "auth_public_enabled": public_auth_enabled,
    }


@app.before_request
def enforce_canonical_host():
    request_host = request.host.split(":", 1)[0].lower().rstrip(".")
    if request_host != WWW_CANONICAL_HOST:
        return None

    target_path = request.full_path if request.query_string else request.path
    return redirect(f"{SITE_SCHEME}://{CANONICAL_HOST}{target_path}", code=301)


@app.after_request
def apply_seo_headers(response):
    if _is_noindex_path(request.path):
        response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    return response


# ═════════════════════════════════════════════════════════════════════════════
#  SEO DISCOVERY FILES
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/robots.txt")
def robots_txt():
    lines = [
        "User-agent: *",
        "Allow: /",
        *(f"Disallow: {path}" for path in PUBLIC_ROBOTS_DISALLOW_PATHS),
        f"Sitemap: {SITE_URL}/sitemap.xml",
    ]
    return Response("\n".join(lines) + "\n", mimetype="text/plain")


@app.route(f"/{GOOGLE_SITE_VERIFICATION_FILENAME}")
def google_site_verification():
    verification_path = BASE_DIR / GOOGLE_SITE_VERIFICATION_FILENAME
    if not verification_path.exists():
        return Response(status=404)
    return Response(verification_path.read_text(encoding="utf-8"), mimetype="text/html")


@app.route("/sitemap.xml")
def sitemap_xml():
    shows_data = load_shows_catalog()
    show_library = build_show_library(shows_data.get("episodes", []))
    latest_catalog_date = _date_for_sitemap(_latest_catalog_timestamp(shows_data))

    def url_entry(loc, lastmod=""):
        lines = [
            "  <url>",
            f"    <loc>{xml_escape(loc)}</loc>",
        ]
        if lastmod:
            lines.append(f"    <lastmod>{xml_escape(lastmod)}</lastmod>")
        lines.append("  </url>")
        return "\n".join(lines)

    url_entries = []
    for path in ("/", "/shows"):
        if not _is_noindex_path(path):
            url_entries.append(url_entry(f"{SITE_URL}{path}", latest_catalog_date))

    sorted_stocks = sorted(
        show_library["stocks"],
        key=lambda stock: _parse_datetime(stock.get("latest_published_at")) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
        reverse=True,
    )
    for stock in sorted_stocks:
        loc = f"{SITE_URL}/shows/{stock['slug']}"
        lastmod = _date_for_sitemap(stock.get("latest_published_at"))
        url_entries.append(url_entry(loc, lastmod))

    for path in PUBLIC_SITEMAP_PATHS:
        if path in ("/", "/shows") or _is_noindex_path(path):
            continue
        loc = f"{SITE_URL}{path}"
        url_entries.append(
            url_entry(loc)
        )

    for game in GAME_CATALOG:
        path = game.get("route")
        if not path or not game.get("playable") or _is_noindex_path(path):
            continue
        loc = f"{SITE_URL}{_normalize_path(path)}"
        if not any(f"<loc>{xml_escape(loc)}</loc>" in entry for entry in url_entries):
            url_entries.append(url_entry(loc))

    joined_url_entries = "\n".join(url_entries)
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f'{joined_url_entries}\n'
        '</urlset>\n'
    )
    return Response(xml, mimetype="application/xml")


# ═════════════════════════════════════════════════════════════════════════════
#  HEALTH CHECK (Railway)
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


# ═════════════════════════════════════════════════════════════════════════════
#  HOMEPAGE
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/")
def index():
    context = _shows_context()
    shows_data = context["shows_data"]
    show_library = context["show_library"]
    show_stocks = context.get("show_client_stocks", [])
    return render_template(
        "shows.html",
        show_stocks=show_stocks[:SHOWS_INITIAL_STOCK_COUNT],
        show_stats=show_library.get("stats", {}),
        show_quarters=show_library.get("quarters", []),
        show_sectors=show_library.get("sectors", []),
        video_sections=shows_data.get("video_sections", []),
        podcast_platforms=shows_data.get("platform_links", {}),
        structured_data=_shows_page_structured_data("/", show_library),
    )


@app.route("/games")
def games_index():
    games = _hydrate_game_catalog(current_user)
    playable_games = [game for game in games if game["is_playable"]]
    leaderboard_full = {
        game["slug"]: _ranked_game_scores(game, limit=10)
        for game in playable_games
    }
    leaderboard_ticker = sorted(
        [entry for entries in leaderboard_full.values() for entry in entries],
        key=lambda entry: (-entry["score"], entry["createdAt"]),
    )[:16]
    return render_template(
        "games.html",
        games=games,
        leaderboard_ticker=leaderboard_ticker,
        leaderboard_full=leaderboard_full,
        leaderboard_period_start=_leaderboard_cutoff_utc(),
    )


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/privacy")
def privacy_policy():
    return render_template("privacy.html")


@app.route("/privacypolicy")
def privacypolicy_redirect():
    return redirect("/privacy", code=301)


@app.route("/unsubscribe")
def unsubscribe():
    return render_template("unsubscribe.html")


@app.route("/games/<game_slug>")
def game_detail(game_slug):
    game = _hydrate_game(game_slug, current_user)
    if not game:
        return ("Game not found", 404)

    if not game["is_unlocked"] or not game["is_playable"]:
        return render_template(
            "game_locked.html",
            game=game,
            games=_hydrate_game_catalog(current_user),
            seo_meta=_get_seo_meta(game["route"]),
        )

    interactive_entry = _get_interactive_entry()
    return render_template(
        "game_app.html",
        game=game,
        interactive_entry=interactive_entry,
        seo_meta=_get_seo_meta(game["route"]),
    )


@app.route("/games/api/leaderboard/<game_slug>")
def games_leaderboard(game_slug):
    game = _get_game(game_slug)
    if not game:
        return jsonify({"error": "Game not found"}), 404

    cutoff = _prune_old_game_scores()
    return jsonify({
        "entries": _ranked_game_scores(game, limit=25, cutoff=cutoff),
        "periodStart": cutoff.isoformat() + "Z",
        "resets": "weekly",
    })


@app.route("/games/api/scores", methods=["POST"])
def games_save_score():
    body = request.get_json(force=True, silent=True) or {}
    game = _hydrate_game(body.get("game_slug"), current_user)
    if not game:
        return jsonify({"ok": False, "error": "Game not found"}), 404
    if not game["is_playable"]:
        return jsonify({"ok": False, "error": "Game is not accepting scores yet"}), 400
    if not game["is_unlocked"]:
        return jsonify({"ok": False, "error": game["locked_reason"]}), 403

    score_value = _coerce_int(body.get("score"))
    if score_value <= 0:
        return jsonify({"ok": False, "error": "Score is required"}), 400

    fallback_name = get_public_first_name(current_user) if current_user.is_authenticated else ""
    display_name, name_error = _validate_score_display_name(body.get("display_name") or fallback_name)
    if name_error:
        return jsonify({"ok": False, "error": name_error}), 400

    metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
    user_id = current_user.id if current_user.is_authenticated else _anonymous_score_user_id()

    _prune_old_game_scores()
    score = GameScore(
        user_id=user_id,
        game_slug=game["slug"],
        display_name=display_name,
        score=score_value,
        return_percent=_coerce_float(body.get("return_percent")),
        moves=_coerce_int(body.get("moves")),
        reallocations=_coerce_int(body.get("reallocations")),
        tax_paid=_coerce_float(body.get("tax_paid")),
        metadata_json=json.dumps(metadata),
    )
    db.session.add(score)
    db.session.commit()

    return jsonify({"ok": True, "entry": _serialize_game_score(score)})


@app.route("/games/api/progress")
def games_progress():
    games = _hydrate_game_catalog(current_user)
    completed_slugs = _completed_game_slugs_for_user(current_user)
    return jsonify({
        "authenticated": bool(current_user.is_authenticated),
        "completed": sorted(completed_slugs),
        "games": [
            {
                "slug": game["slug"],
                "title": game["title"],
                "sequence": game["sequence"],
                "playable": game["is_playable"],
                "unlocked": game["is_unlocked"],
                "completed": game["has_completion"],
                "unlockAfter": game.get("unlock_after"),
                "route": game["route"],
            }
            for game in games
        ],
    })


@app.route("/account")
@login_required
def account():
    if not public_auth_enabled():
        abort(404)
    scores = (
        GameScore.query.filter_by(user_id=current_user.id)
        .order_by(GameScore.created_at.desc())
        .limit(50)
        .all()
    )
    game_scores = []
    for score in scores:
        serialized = _serialize_game_score(score)
        serialized["gameTitle"] = (_get_game(score.game_slug) or {}).get("title", score.game_slug)
        game_scores.append(serialized)

    return render_template(
        "account.html",
        email_updates_subscription=get_email_updates_subscription(current_user),
        game_scores=game_scores,
        games=_hydrate_game_catalog(current_user),
        saved_charts=list_user_charts(current_user.id),
    )


@app.route("/shows")
def shows():
    context = _shows_context()
    shows_data = context["shows_data"]
    show_library = context["show_library"]
    show_stocks = context.get("show_client_stocks", [])
    return render_template(
        "shows.html",
        show_stocks=show_stocks[:SHOWS_INITIAL_STOCK_COUNT],
        show_stats=show_library.get("stats", {}),
        show_quarters=show_library.get("quarters", []),
        show_sectors=show_library.get("sectors", []),
        video_sections=shows_data.get("video_sections", []),
        podcast_platforms=shows_data.get("platform_links", {}),
        structured_data=_shows_page_structured_data("/shows", show_library),
    )


@app.route("/api/shows/stocks")
def shows_stocks_api():
    context = _shows_context()
    return jsonify({
        "stocks": context.get("show_client_stocks", []),
        "stats": context.get("show_library", {}).get("stats", {}),
    })


@app.route("/shows/<ticker_slug>")
def show_stock_detail_page(ticker_slug):
    context = _shows_context()
    shows_data = context["shows_data"]
    show_library = context["show_library"]
    requested = _show_slug(ticker_slug)
    show_stock = next((stock for stock in show_library["stocks"] if stock["slug"] == requested), None)
    if not show_stock:
        return ("Stock show not found", 404)

    prefetch_stocks = [show_stock] + _pick_competitor_stocks(show_stock, show_library["stocks"])
    with ThreadPoolExecutor(max_workers=min(3, len(prefetch_stocks))) as ex:
        list(ex.map(lambda stock: _cached_show_stock_detail(stock["yf_symbol"]), prefetch_stocks))

    detail_bundle = _cached_show_stock_detail(show_stock["yf_symbol"])
    stock_detail = dict(detail_bundle.get("info") or {})
    if not stock_detail:
        stock_detail = {
            "symbol": show_stock["ticker"],
            "name": show_stock["company"],
            "sector": show_stock["sector"],
        }

    stock_detail.setdefault("summary", "")
    stock_detail.setdefault("website", "")
    stock_detail.setdefault("country", "")
    stock_detail.setdefault("employees", None)

    for key in (
        "price",
        "change",
        "change_pct",
        "trailing_pe",
        "forward_pe",
        "market_cap",
        "volume",
        "price_to_book",
        "beta",
        "week_52_low",
        "week_52_high",
        "eps",
        "target_mean_price",
        "target_upside",
        "industry",
        "revenue_growth",
        "earnings_growth",
        "operating_margin",
        "gross_margin",
        "profit_margin",
        "return_on_equity",
        "debt_to_equity",
        "current_ratio",
        "dividend_yield",
        "fcf_yield",
    ):
        stock_detail.setdefault(key, None)

    competitor_analysis = build_stock_competitor_analysis(show_stock, stock_detail, show_library["stocks"])
    related_videos = [
        video
        for video in flatten_video_sections(shows_data.get("video_sections", []))
        if show_stock["ticker"] in [ticker.upper() for ticker in video.get("tickers", [])]
    ][:6]

    seo_title = f"{show_stock['company']} ({show_stock['ticker']}) Stock Library — Charged Alpha"
    seo_description = (
        f"Track {show_stock['company']} ({show_stock['ticker']}) across Charged Alpha earnings episodes, "
        "with YouTube, podcast, stock metrics, chart context, and competitor comparisons."
    )
    seo_meta = {
        "title": seo_title,
        "description": seo_description,
        "canonical_url": _canonical_url(f"/shows/{show_stock['slug']}"),
        "robots": SEO_DEFAULTS["robots"],
        "og_title": seo_title,
        "og_description": seo_description,
        "og_type": "article",
        "twitter_card": SEO_DEFAULTS["twitter_card"],
        "og_image": show_stock.get("latest_video_thumbnail") or DEFAULT_SOCIAL_IMAGE_URL,
        "twitter_image": show_stock.get("latest_video_thumbnail") or DEFAULT_SOCIAL_IMAGE_URL,
    }

    return render_template(
        "show_stock_detail.html",
        show_stock=show_stock,
        stock_detail=stock_detail,
        competitor_analysis=competitor_analysis,
        related_videos=related_videos,
        chart_symbol=show_stock["yf_symbol"],
        podcast_platforms=shows_data.get("platform_links", {}),
        seo_meta=seo_meta,
        structured_data=_stock_page_structured_data(show_stock, seo_meta),
    )


# ── Market pulse API (homepage ticker) ────────────────────────────────────
_market_pulse_cache = TTLCache(default_ttl=120, max_size=1)

@app.route("/api/market-pulse")
def market_pulse():
    cached = _market_pulse_cache.get("pulse")
    if cached:
        return jsonify(cached)

    symbols = {
        # US indices
        "^GSPC": {"name": "S&P 500", "cat": "US"},
        "^DJI": {"name": "Dow Jones", "cat": "US"},
        "^IXIC": {"name": "Nasdaq", "cat": "US"},
        "^RUT": {"name": "Russell 2000", "cat": "US"},
        "^VIX": {"name": "VIX", "cat": "US"},
        # International
        "^FTSE": {"name": "FTSE 100", "cat": "Intl"},
        "^GDAXI": {"name": "DAX", "cat": "Intl"},
        "^N225": {"name": "Nikkei 225", "cat": "Intl"},
        "^HSI": {"name": "Hang Seng", "cat": "Intl"},
        "000001.SS": {"name": "Shanghai", "cat": "Intl"},
        # Commodities
        "GC=F": {"name": "Gold", "cat": "Cmdty"},
        "SI=F": {"name": "Silver", "cat": "Cmdty"},
        "CL=F": {"name": "Crude Oil", "cat": "Cmdty"},
        "NG=F": {"name": "Natural Gas", "cat": "Cmdty"},
        # Currencies
        "DX-Y.NYB": {"name": "US Dollar", "cat": "FX"},
        "EURUSD=X": {"name": "EUR/USD", "cat": "FX"},
        "GBPUSD=X": {"name": "GBP/USD", "cat": "FX"},
        "JPY=X": {"name": "USD/JPY", "cat": "FX"},
        # Crypto
        "BTC-USD": {"name": "Bitcoin", "cat": "Crypto"},
        "ETH-USD": {"name": "Ethereum", "cat": "Crypto"},
        # Rates
        "^TNX": {"name": "10Y Treasury", "cat": "Rates"},
        "^FVX": {"name": "5Y Treasury", "cat": "Rates"},
    }

    results = []
    try:
        tickers = yf.Tickers(" ".join(symbols.keys()))
        for sym, meta in symbols.items():
            try:
                t = tickers.tickers.get(sym) or tickers.tickers.get(sym.replace(".", "-"))
                if not t:
                    continue
                info = t.fast_info if hasattr(t, "fast_info") else {}
                price = getattr(info, "last_price", None)
                prev = getattr(info, "previous_close", None)
                if price is None or prev is None:
                    hist = t.history(period="2d")
                    if len(hist) >= 1:
                        price = price or float(hist["Close"].iloc[-1])
                    if len(hist) >= 2:
                        prev = prev or float(hist["Close"].iloc[-2])
                if price is None:
                    continue
                change_pct = round((price - prev) / prev * 100, 2) if prev else 0
                # Format price
                if price >= 1000:
                    price_fmt = f"{price:,.0f}"
                elif price >= 1:
                    price_fmt = f"{price:,.2f}"
                else:
                    price_fmt = f"{price:.4f}"
                results.append({
                    "symbol": sym,
                    "name": meta["name"],
                    "cat": meta["cat"],
                    "price": price_fmt,
                    "change": change_pct,
                })
            except Exception:
                continue
    except Exception as e:
        print(f"Market pulse error: {e}")

    _market_pulse_cache.set("pulse", results)
    return jsonify(results)


# ═════════════════════════════════════════════════════════════════════════════
#  STOCK SCREENER  /screener/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/screener")
def screener_index():
    return render_template("stock_screener.html")


@app.route("/screener/api/screen", methods=["POST"])
def screener_start():
    body = request.get_json(force=True)
    _f = lambda k, d=None: _f_body(body, k, d)

    cap_labels = body.get("cap_ranges")
    cap_ranges = None
    if cap_labels and isinstance(cap_labels, list):
        cap_ranges = [CAP_RANGES[k] for k in cap_labels if k in CAP_RANGES]
        if not cap_ranges:
            cap_ranges = None

    sectors = _extract_list(body, "sectors")
    analyst_recs = _extract_list(body, "analyst_recs")

    criteria = {
        "pe_below_historical": bool(body.get("pe_below_historical", False)),
        "pe_min_discount_pct": _f("pe_min_discount_pct", 0),
        "min_price": _f("min_price"), "max_price": _f("max_price"),
        "min_pb": _f("min_pb"), "max_pb": _f("max_pb"),
        "min_div_yield": _f("min_div_yield"), "max_div_yield": _f("max_div_yield"),
        "max_payout_ratio": _f("max_payout_ratio"),
        "min_div_streak": _f("min_div_streak"),
        "ex_div_window": _f("ex_div_window"),
        "min_revenue_growth": _f("min_revenue_growth"),
        "min_eps_growth": _f("min_eps_growth"),
        "min_w52_perf": _f("min_w52_perf"), "max_w52_perf": _f("max_w52_perf"),
        "max_w52_dist_high": _f("max_w52_dist_high"),
        "max_debt_to_equity": _f("max_debt_to_equity"),
        "min_current_ratio": _f("min_current_ratio"),
        "min_fcf_yield": _f("min_fcf_yield"),
        "min_operating_margin": _f("min_operating_margin"),
        "min_put_iv": _f("min_put_iv"), "max_put_iv": _f("max_put_iv"),
        "max_put_spread_pct": _f("max_put_spread_pct"),
        "min_put_oi": _f("min_put_oi"), "min_put_volume": _f("min_put_volume"),
        "sectors": sectors, "cap_ranges": cap_ranges, "analyst_recs": analyst_recs,
        "min_analyst_count": _f("min_analyst_count"),
        "min_target_upside": _f("min_target_upside"),
    }
    job_id = _start_job(screen_stocks, criteria)
    return jsonify({"job_id": job_id})


@app.route("/screener/api/screen/<job_id>")
def screener_status(job_id):
    return _get_job(job_id)


@app.route("/screener/api/stock/<symbol>")
def screener_stock_detail(symbol):
    return _cached_detail("stock", symbol, get_stock_detail)


@app.route("/screener/api/stock/<symbol>/chart")
def screener_stock_chart(symbol):
    return _chart_helper(symbol, request.args.get("range", "1y"))


@app.route("/screener/api/ticker-banner")
def screener_ticker_banner():
    results = fetch_banner_tickers(BANNER_TICKERS, cache_obj=_banner_cache)
    return jsonify(results)


# ═════════════════════════════════════════════════════════════════════════════
#  ETF SCREENER  /etf/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/etf")
def etf_index():
    return render_template("etf_screener.html")


@app.route("/etf/api/screen", methods=["POST"])
def etf_start():
    body = request.get_json(force=True)
    _f = lambda k, d=None: _f_body(body, k, d)

    categories = _extract_list(body, "categories")
    asset_classes = _extract_list(body, "asset_classes")

    criteria = {
        "max_expense_ratio": _f("max_expense_ratio"),
        "min_aum": _f("min_aum"),
        "min_div_yield": _f("min_div_yield"), "max_div_yield": _f("max_div_yield"),
        "min_ytd_return": _f("min_ytd_return"),
        "min_1y_return": _f("min_1y_return"),
        "min_3y_return": _f("min_3y_return"),
        "min_avg_volume": _f("min_avg_volume"),
        "min_w52_perf": _f("min_w52_perf"), "max_w52_perf": _f("max_w52_perf"),
        "max_w52_dist_high": _f("max_w52_dist_high"),
        "categories": categories, "asset_classes": asset_classes,
    }
    job_id = _start_job(screen_etfs, criteria)
    return jsonify({"job_id": job_id})


@app.route("/etf/api/screen/<job_id>")
def etf_status(job_id):
    return _get_job(job_id)


@app.route("/etf/api/etf/<symbol>")
def etf_detail(symbol):
    return _cached_detail("etf", symbol, get_etf_detail)


@app.route("/etf/api/etf/<symbol>/chart")
def etf_chart(symbol):
    return _chart_helper(symbol, request.args.get("range", "1y"))


# ═════════════════════════════════════════════════════════════════════════════
#  MUTUAL FUND SCREENER  /mutual-funds/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/mutual-funds")
def mutual_fund_index():
    return render_template("mutual_fund_screener.html")


@app.route("/mutual-funds/api/screen", methods=["POST"])
def mutual_fund_start():
    body = request.get_json(force=True)
    _f = lambda k, d=None: _f_body(body, k, d)

    categories = _extract_list(body, "categories")
    asset_classes = _extract_list(body, "asset_classes")
    management_styles = _extract_list(body, "management_styles")

    criteria = {
        "max_expense_ratio": _f("max_expense_ratio"),
        "min_aum": _f("min_aum"),
        "min_div_yield": _f("min_div_yield"), "max_div_yield": _f("max_div_yield"),
        "min_ytd_return": _f("min_ytd_return"),
        "min_1y_return": _f("min_1y_return"),
        "min_3y_return": _f("min_3y_return"),
        "min_avg_volume": _f("min_avg_volume"),
        "min_w52_perf": _f("min_w52_perf"), "max_w52_perf": _f("max_w52_perf"),
        "max_w52_dist_high": _f("max_w52_dist_high"),
        "min_morningstar_rating": _f("min_morningstar_rating"),
        "max_morningstar_risk": _f("max_morningstar_risk"),
        "max_beta": _f("max_beta"),
        "max_turnover_pct": _f("max_turnover_pct"),
        "min_years_history": _f("min_years_history"),
        "min_stock_position": _f("min_stock_position"),
        "min_bond_position": _f("min_bond_position"),
        "max_cash_position": _f("max_cash_position"),
        "query": (body.get("query") or "").strip() or None,
        "categories": categories,
        "asset_classes": asset_classes,
        "management_styles": management_styles,
    }
    job_id = _start_job(screen_mutual_funds, criteria)
    return jsonify({"job_id": job_id})


@app.route("/mutual-funds/api/catalog")
def mutual_fund_catalog():
    return jsonify({"funds": get_mutual_fund_catalog_rows()})


@app.route("/mutual-funds/api/screen/<job_id>")
def mutual_fund_status(job_id):
    return _get_job(job_id)


@app.route("/mutual-funds/api/fund/<symbol>")
def mutual_fund_detail(symbol):
    return _cached_detail("mutual_fund", symbol, get_mutual_fund_detail)


@app.route("/mutual-funds/api/fund/<symbol>/chart")
def mutual_fund_chart(symbol):
    return _chart_helper(symbol, request.args.get("range", "1y"))


# ═════════════════════════════════════════════════════════════════════════════
#  CRYPTO SCREENER  /crypto/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/crypto")
def crypto_index():
    return render_template("crypto_screener.html")


@app.route("/crypto/api/screen", methods=["POST"])
def crypto_start():
    body = request.get_json(force=True)
    _f = lambda k, d=None: _f_body(body, k, d)
    criteria = {
        "min_price": _f("min_price"), "max_price": _f("max_price"),
        "min_market_cap": _f("min_market_cap"), "max_market_cap": _f("max_market_cap"),
        "min_change_24h": _f("min_change_24h"), "max_change_24h": _f("max_change_24h"),
        "min_change_7d": _f("min_change_7d"), "max_change_7d": _f("max_change_7d"),
        "min_volume": _f("min_volume"), "max_volume": _f("max_volume"),
    }
    job_id = _start_job(screen_cryptos, criteria)
    return jsonify({"job_id": job_id})


@app.route("/crypto/api/screen/<job_id>")
def crypto_status(job_id):
    return _get_job(job_id)


@app.route("/crypto/api/crypto/<coin_id>/chart")
def crypto_chart_route(coin_id):
    days = request.args.get("days", "30")
    data = get_crypto_chart(coin_id, days)
    if not data:
        return jsonify({"error": "No chart data"}), 404
    return jsonify(data)


# ═════════════════════════════════════════════════════════════════════════════
#  OPTIONS SCANNER  /options/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/options")
def options_index():
    return render_template("options_scanner.html")


@app.route("/options/api/scan", methods=["POST"])
def options_start():
    body = request.get_json(force=True)
    _f = lambda k, d=None: _f_body(body, k, d)

    symbols_raw = body.get("symbols", "")
    if isinstance(symbols_raw, str):
        symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]
    else:
        symbols = symbols_raw

    criteria = {
        "symbols": symbols if symbols else None,
        "option_type": body.get("option_type", "both"),
        "min_oi": _f("min_oi"), "min_volume": _f("min_volume"),
        "max_spread_pct": _f("max_spread_pct"),
        "min_dte": _f("min_dte"), "max_dte": _f("max_dte"),
        "min_vol_oi": _f("min_vol_oi"),
        "unusual_only": bool(body.get("unusual_only", False)),
    }
    job_id = _start_job(scan_options, criteria)
    return jsonify({"job_id": job_id})


@app.route("/options/api/scan/<job_id>")
def options_status(job_id):
    return _get_job(job_id)


# ═════════════════════════════════════════════════════════════════════════════
#  BOND DASHBOARD  /bonds/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/bonds")
def bonds_index():
    return render_template("bond_dashboard.html")


@app.route("/bonds/api/yields")
def bonds_yields():
    return jsonify(get_yields())


@app.route("/bonds/api/yields/history")
def bonds_yield_history():
    ticker = request.args.get("ticker", "^TNX")
    range_key = request.args.get("range", "1y")
    return jsonify(get_yield_history(ticker, range_key))


@app.route("/bonds/api/etfs")
def bonds_etfs():
    return jsonify(get_bond_etfs())


# ═════════════════════════════════════════════════════════════════════════════
#  REIT SCREENER  /reits/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/reits")
def reits_index():
    return render_template("reit_screener.html")


@app.route("/reits/api/screen", methods=["POST"])
def reits_start():
    body = request.get_json(force=True)
    _f = lambda k, d=None: _f_body(body, k, d)

    sectors = _extract_list(body, "sectors")

    criteria = {
        "min_div_yield": _f("min_div_yield"), "max_div_yield": _f("max_div_yield"),
        "min_price": _f("min_price"), "max_price": _f("max_price"),
        "min_pe": _f("min_pe"), "max_pe": _f("max_pe"),
        "max_debt_to_equity": _f("max_debt_to_equity"),
        "min_market_cap": _f("min_market_cap"),
        "min_w52_perf": _f("min_w52_perf"), "max_w52_perf": _f("max_w52_perf"),
        "sectors": sectors,
    }
    job_id = _start_job(screen_reits, criteria)
    return jsonify({"job_id": job_id})


@app.route("/reits/api/screen/<job_id>")
def reits_status(job_id):
    return _get_job(job_id)


@app.route("/reits/api/reit/<symbol>/chart")
def reits_chart(symbol):
    return _chart_helper(symbol, request.args.get("range", "1y"))


# ═════════════════════════════════════════════════════════════════════════════
#  FOREX HEATMAP  /forex/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/forex")
def forex_index():
    return render_template("forex_heatmap.html")


@app.route("/forex/api/pairs")
def forex_pairs():
    tf = request.args.get("timeframe", "1d")
    return jsonify(get_all_pairs(tf))


@app.route("/forex/api/strength")
def forex_strength():
    tf = request.args.get("timeframe", "1d")
    return jsonify(get_currency_strength(tf))


@app.route("/forex/api/pair/<pair>/chart")
def forex_pair_chart(pair):
    range_key = request.args.get("range", "1y")
    data = get_pair_chart(pair, range_key)
    if not data:
        return jsonify({"error": "No data"}), 404
    return jsonify(data)


# ═════════════════════════════════════════════════════════════════════════════
#  COMMODITIES DASHBOARD  /commodities/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/commodities")
def commodities_index():
    return render_template("commodities_dashboard.html")


@app.route("/commodities/api/commodities")
def commodities_data():
    return jsonify(get_all_commodities())


@app.route("/commodities/api/commodity/<path:ticker>/chart")
def commodities_chart(ticker):
    range_key = request.args.get("range", "1y")
    data = get_commodity_chart(ticker, range_key)
    if not data:
        return jsonify({"error": "No data"}), 404
    return jsonify(data)


# ═════════════════════════════════════════════════════════════════════════════
#  EARNINGS CALENDAR  /earnings/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/earnings")
def earnings_index():
    return render_template("earnings_calendar.html")


@app.route("/earnings/api/earnings")
def earnings_data():
    week = request.args.get("week")
    sector = request.args.get("sector")
    return jsonify(get_earnings_week(week, sector))


@app.route("/earnings/api/earnings-month")
def earnings_month_data():
    month = request.args.get("month")
    return jsonify(get_earnings_month(month))


@app.route("/earnings/api/stock/<symbol>/earnings-history")
def earnings_history(symbol):
    return jsonify(get_stock_earnings_history(symbol.upper()))


# ═════════════════════════════════════════════════════════════════════════════
#  PRECIOUS METALS (GOLD)  /gold/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/gold")
def gold_index():
    return render_template("gold.html")


@app.route("/gold/api/spot")
def gold_spot():
    metal = request.args.get("metal", "gold").lower()
    if metal not in ("gold", "silver", "platinum"):
        metal = "gold"
    price = get_spot_price(metal)
    return jsonify({"price": price, "metal": metal})


@app.route("/gold/api/listings")
def gold_listings():
    metal = request.args.get("metal", "gold").lower()
    if metal not in ("gold", "silver", "platinum"):
        metal = "gold"

    src = (request.args.get("source", "") or "").lower().replace(" ", "")
    min_karat_raw = request.args.get("min_karat")
    max_karat_raw = request.args.get("max_karat")
    item_type = request.args.get("type")
    include_misc = request.args.get("include_misc", "0") == "1"
    q = (request.args.get("q", "") or "").lower()
    min_price_raw = request.args.get("min_price")
    max_price_raw = request.args.get("max_price")
    min_weight_raw = request.args.get("min_weight_oz")
    max_weight_raw = request.args.get("max_weight_oz")

    min_purity_frac = get_purity_fraction(min_karat_raw, metal) if min_karat_raw else None
    max_purity_frac = get_purity_fraction(max_karat_raw, metal) if max_karat_raw else None
    min_price = float(min_price_raw) if min_price_raw else None
    max_price = float(max_price_raw) if max_price_raw else None
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
                print(f"[gold api] {name}: {e}")

    # Apply filters in a single pass for efficiency
    def _passes_gold_filter(l):
        if l.get("is_search_link") or not l.get("weight_oz"):
            return False
        if item_type and l.get("type") != item_type:
            return False
        pf = l.get("purity_fraction")
        if (min_purity_frac is not None or max_purity_frac is not None) and pf is None:
            return False
        if min_purity_frac is not None and pf < min_purity_frac:
            return False
        if max_purity_frac is not None and pf > max_purity_frac:
            return False
        price = l.get("price", 0)
        if min_price is not None and price < min_price:
            return False
        if max_price is not None and price > max_price:
            return False
        wt = l.get("weight_oz") or 0
        if min_weight is not None and wt < min_weight:
            return False
        if max_weight is not None and (not wt or wt > max_weight):
            return False
        if q and q not in l.get("title", "").lower():
            return False
        return True

    listings = sorted(
        (l for l in listings if _passes_gold_filter(l)),
        key=lambda x: x["price"]
    )

    return jsonify({
        "count": len(listings),
        "spot_price": spot,
        "metal": metal,
        "listings": listings,
    })


# ═════════════════════════════════════════════════════════════════════════════
#  STOCK CHARTS  /charts/
# ═════════════════════════════════════════════════════════════════════════════
@app.route("/charts")
def charts_index():
    return render_template("stock_charts.html")


@app.route("/charts/api/save", methods=["POST"])
@login_required
def charts_save():
    body = request.get_json(force=True)
    chart_name = body.get("chart_name", "").strip()
    symbol = body.get("symbol", "")
    state_json = body.get("state_json", "{}")
    if not chart_name:
        return jsonify({"ok": False, "error": "Chart name is required"}), 400
    save_chart_state(current_user.id, chart_name, symbol, state_json)
    return jsonify({"ok": True})


@app.route("/charts/api/load")
@login_required
def charts_load():
    chart_name = request.args.get("chart_name", "")
    if not chart_name:
        return jsonify({"error": "chart_name required"}), 400
    data = load_chart_state(current_user.id, chart_name)
    if not data:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data)


@app.route("/charts/api/list")
@login_required
def charts_list():
    return jsonify(list_user_charts(current_user.id))


@app.route("/charts/api/delete", methods=["DELETE"])
@login_required
def charts_delete():
    chart_name = request.args.get("chart_name", "")
    if not chart_name:
        return jsonify({"ok": False, "error": "chart_name required"}), 400
    deleted = delete_chart_state(current_user.id, chart_name)
    return jsonify({"ok": deleted})


# ═════════════════════════════════════════════════════════════════════════════
#  RUN
# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=os.environ.get("FLASK_DEBUG", "0") == "1")
