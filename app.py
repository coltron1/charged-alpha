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
  /auth/...                      → Authentication (login, register, OAuth)
"""

import json
import os
import re
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import yfinance as yf
from flask import Flask, render_template, request, jsonify, redirect, Response
from flask_compress import Compress
from flask_login import LoginManager, current_user, login_required

# ── Shared utilities ────────────────────────────────────────────────────────
from yf_utils import (TTLCache, JobStore, fetch_ticker_info, safe_float,
                       normalize_div_yield, fetch_chart, fetch_banner_tickers)
from models import db, User
from auth import auth_bp, init_oauth
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
BASE_DIR = Path(__file__).resolve().parent
SHOWS_CATALOG_PATH = BASE_DIR / "data" / "shows_catalog.json"


@app.get("/health")
def health_check():
    return jsonify({"status": "ok"}), 200
SITE_URL = os.environ.get("SITE_URL", "https://chargedalpha.com").rstrip("/")
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
]
SEO_DEFAULTS = {
    "title": "Charged Alpha — Data-Driven Investing Tools",
    "description": (
        "Charged Alpha delivers data-driven investing tools, market dashboards, "
        "and stock research workflows for faster investor decision-making."
    ),
    "robots": "index,follow,max-image-preview:large",
    "og_type": "website",
    "twitter_card": "summary",
}
SEO_PAGE_META = {
    "/": {
        "title": "Charged Alpha — Data-Driven Investing Tools",
        "description": (
            "Explore Charged Alpha's data-driven investing tools, stock screens, "
            "market dashboards, and research workflows built for faster analysis."
        ),
    },
    "/shows": {
        "title": "Charged Alpha Stock Library — Quarterly Stock Episodes",
        "description": (
            "Browse Charged Alpha's stock-first library with quarterly episode coverage, "
            "ticker and quarter filters, and detail pages for each company."
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
)
NOINDEX_EXACT_PATHS = {
    "/login",
    "/register",
    "/health",
}


def _normalize_path(path):
    if not path or path == "/":
        return "/"
    return "/" + path.strip("/")


def _canonical_url(path):
    return f"{SITE_URL}{_normalize_path(path)}"


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
    }


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

    for ep in episodes or []:
        ticker = (ep.get("ticker") or "").upper().strip()
        if not ticker:
            continue

        slug = _show_slug(ticker)
        quarter = (ep.get("quarter") or "Unknown").strip()
        quarter_set.add(quarter)
        has_youtube = bool(ep.get("youtube_url"))
        has_any_link = bool(ep.get("has_episode") or _episode_has_any_link(ep))

        if has_youtube:
            published_episode_count += 1
            youtube_episode_count += 1

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
                "status": ep.get("status") or ("youtube_live" if has_youtube else ("linked_elsewhere" if has_any_link else "planned")),
                "has_episode": has_youtube,
                "has_any_link": has_any_link,
                "youtube_url": ep.get("youtube_url") or "",
            }
        )

    stocks = []
    for stock in grouped.values():
        stock["episodes"].sort(key=lambda ep: _quarter_sort_key(ep["quarter"]), reverse=True)
        latest = stock["episodes"][0]
        latest_youtube = next((ep for ep in stock["episodes"] if ep.get("youtube_url")), None)

        stock["quarter_count"] = len(stock["episodes"])
        stock["published_count"] = sum(1 for ep in stock["episodes"] if ep.get("youtube_url"))
        stock["latest_quarter"] = latest["quarter"]
        stock["latest_video_quarter"] = latest_youtube["quarter"] if latest_youtube else None
        stock["latest_status"] = latest_youtube["status"] if latest_youtube else latest["status"]
        stock["quarter_labels"] = [ep["quarter"] for ep in stock["episodes"]]
        stock["latest_links"] = {
            "youtube": latest_youtube.get("youtube_url") if latest_youtube else "",
        }
        stock["latest_youtube_url"] = latest_youtube.get("youtube_url") if latest_youtube else ""
        stock["has_youtube"] = bool(latest_youtube)
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
            "quarter_count": len(quarter_options),
        },
    }


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


def build_stock_competitor_analysis(show_stock, primary_snapshot, all_stocks):
    competitor_stocks = _pick_competitor_stocks(show_stock, all_stocks)
    snapshots = []

    primary = dict(primary_snapshot or {})
    primary.update({
        "ticker": show_stock["ticker"],
        "company": show_stock["company"],
        "latest_video_quarter": show_stock.get("latest_video_quarter"),
        "latest_youtube_url": show_stock.get("latest_youtube_url"),
    })
    snapshots.append(primary)

    for comp_stock in competitor_stocks:
        comp_bundle = get_stock_detail(comp_stock["yf_symbol"], include_options=False) or {}
        comp_info = dict(comp_bundle.get("info") or {})
        comp_info.update({
            "ticker": comp_stock["ticker"],
            "company": comp_stock["company"],
            "latest_video_quarter": comp_stock.get("latest_video_quarter"),
            "latest_youtube_url": comp_stock.get("latest_youtube_url"),
        })
        snapshots.append(comp_info)

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
# Railway Postgres uses postgres:// but SQLAlchemy needs postgresql://
if app.config['SQLALCHEMY_DATABASE_URI'].startswith('postgres://'):
    app.config['SQLALCHEMY_DATABASE_URI'] = app.config['SQLALCHEMY_DATABASE_URI'].replace(
        'postgres://', 'postgresql://', 1)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
Compress(app)

# ── Database + Auth ────────────────────────────────────────────────────────
db.init_app(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

init_oauth(app)
app.register_blueprint(auth_bp)

with app.app_context():
    db.create_all()

# ── Convenience redirects for auth ────────────────────────────────────────
@app.route("/login")
def login_redirect():
    return redirect("/auth/login" + ("?" + request.query_string.decode() if request.query_string else ""))

@app.route("/register")
def register_redirect():
    return redirect("/auth/register" + ("?" + request.query_string.decode() if request.query_string else ""))

# ── Shared job store (auto-cleans after 10 min) ────────────────────────────
job_store = JobStore(ttl=600)

# ── Shared caches ───────────────────────────────────────────────────────────
_detail_cache = TTLCache(default_ttl=300, max_size=500)
_banner_cache = TTLCache(default_ttl=120, max_size=10)

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
    return {"seo_meta": _get_seo_meta()}


@app.after_request
def apply_seo_headers(response):
    path = _normalize_path(request.path)
    if path in NOINDEX_EXACT_PATHS or any(path.startswith(prefix) for prefix in NOINDEX_PATH_PREFIXES):
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
        "Disallow: /auth/",
        "Disallow: /login",
        "Disallow: /register",
        "Disallow: /api/",
        "Disallow: /screener/api/",
        "Disallow: /etf/api/",
        "Disallow: /mutual-funds/api/",
        "Disallow: /crypto/api/",
        "Disallow: /options/api/",
        "Disallow: /bonds/api/",
        "Disallow: /reits/api/",
        "Disallow: /forex/api/",
        "Disallow: /commodities/api/",
        "Disallow: /earnings/api/",
        "Disallow: /gold/api/",
        "Disallow: /charts/api/",
        f"Sitemap: {SITE_URL}/sitemap.xml",
    ]
    return Response("\n".join(lines) + "\n", mimetype="text/plain")


@app.route("/sitemap.xml")
def sitemap_xml():
    url_entries = []
    for path in PUBLIC_SITEMAP_PATHS:
        loc = f"{SITE_URL}{path}"
        url_entries.append(
            "  <url>\n"
            f"    <loc>{loc}</loc>\n"
            "  </url>"
        )

    shows_data = load_shows_catalog()
    show_library = build_show_library(shows_data.get("episodes", []))
    for stock in show_library["stocks"]:
        loc = f"{SITE_URL}/shows/{stock['slug']}"
        url_entries.append(
            "  <url>\n"
            f"    <loc>{loc}</loc>\n"
            "  </url>"
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f'{"\n".join(url_entries)}\n'
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
    shows_data = load_shows_catalog()
    show_library = build_show_library(shows_data.get("episodes", []))
    featured_stocks = [
        stock for stock in show_library["stocks"] if stock.get("published_count")
    ][:6]
    return render_template(
        "index.html",
        podcast_platforms=shows_data.get("platform_links", {}),
        show_stats=show_library.get("stats", {}),
        featured_stocks=featured_stocks,
        show_quarters=show_library.get("quarters", []),
    )


@app.route("/shows")
def shows():
    shows_data = load_shows_catalog()
    show_library = build_show_library(shows_data.get("episodes", []))
    return render_template(
        "shows.html",
        show_stocks=show_library.get("stocks", []),
        show_stats=show_library.get("stats", {}),
        show_quarters=show_library.get("quarters", []),
        show_sectors=show_library.get("sectors", []),
        podcast_platforms=shows_data.get("platform_links", {}),
    )


@app.route("/shows/<ticker_slug>")
def show_stock_detail_page(ticker_slug):
    shows_data = load_shows_catalog()
    show_library = build_show_library(shows_data.get("episodes", []))
    requested = _show_slug(ticker_slug)
    show_stock = next((stock for stock in show_library["stocks"] if stock["slug"] == requested), None)
    if not show_stock:
        return ("Stock show not found", 404)

    detail_bundle = get_stock_detail(show_stock["yf_symbol"], include_options=False) or {}
    stock_detail = dict(detail_bundle.get("info") or {})
    if not stock_detail:
        stock_detail = {
            "symbol": show_stock["ticker"],
            "name": show_stock["company"],
            "sector": show_stock["sector"],
        }

    _, info = fetch_ticker_info(show_stock["yf_symbol"])
    if info:
        stock_detail["summary"] = info.get("longBusinessSummary") or ""
        stock_detail["website"] = info.get("website") or ""
        stock_detail["industry"] = stock_detail.get("industry") or info.get("industry")
        stock_detail["country"] = info.get("country") or ""
        stock_detail["employees"] = info.get("fullTimeEmployees")
    else:
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

    seo_title = f"{show_stock['company']} ({show_stock['ticker']}) Stock Library — Charged Alpha"
    seo_description = (
        f"Track {show_stock['company']} ({show_stock['ticker']}) across quarterly Charged Alpha YouTube coverage, "
        "with live stock metrics, price chart context, competitor comparisons, and video links by quarter."
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
    }

    return render_template(
        "show_stock_detail.html",
        show_stock=show_stock,
        stock_detail=stock_detail,
        competitor_analysis=competitor_analysis,
        chart_symbol=show_stock["yf_symbol"],
        podcast_platforms=shows_data.get("platform_links", {}),
        seo_meta=seo_meta,
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
