"""
Charged Alpha — Unified Flask Server
All investing tools served from one app.

Routes:
  /                              → Homepage
  /screener/                     → Stock Screener
  /screener/api/...              → Stock Screener API
  /etf/                          → ETF Screener
  /etf/api/...                   → ETF Screener API
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
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import yfinance as yf
from flask import Flask, render_template, request, jsonify, redirect
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


def load_shows_catalog():
    if not SHOWS_CATALOG_PATH.exists():
        return {"platform_links": {}, "episodes": []}
    with SHOWS_CATALOG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

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
    return render_template(
        "index.html",
        podcast_platforms=shows_data.get("platform_links", {}),
    )


@app.route("/shows")
def shows():
    shows_data = load_shows_catalog()
    return render_template(
        "shows.html",
        shows_catalog=shows_data.get("episodes", []),
        podcast_platforms=shows_data.get("platform_links", {}),
    )


@app.route("/shows")
def shows():
    return render_template("shows.html")


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
