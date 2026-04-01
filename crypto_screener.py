import time
from datetime import datetime
import requests
from yf_utils import TTLCache

_CG_BASE = "https://api.coingecko.com/api/v3"
_CC_BASE = "https://api.coincap.io/v2"
_CP_BASE = "https://api.coinpaprika.com/v1"
_cache = TTLCache(default_ttl=120, max_size=100)
_chart_cache = TTLCache(default_ttl=300, max_size=200)

def _log(msg):
    print(msg, flush=True)

def _fetch_coins_coingecko():
    """Fetch from CoinGecko (primary source)."""
    url = f"{_CG_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "7d",
    }
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or len(data) == 0:
        raise ValueError(f"CoinGecko returned empty or invalid data: {str(data)[:200]}")
    return data


def _fetch_coins_coincap():
    """Fetch from CoinCap as fallback, normalised to CoinGecko format."""
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    resp = requests.get(f"{_CC_BASE}/assets", params={"limit": 250}, headers=headers, timeout=30)
    resp.raise_for_status()
    raw = resp.json().get("data", [])
    if not raw:
        raise ValueError("CoinCap returned empty data")
    coins = []
    for r in raw:
        price = float(r.get("priceUsd") or 0)
        mcap = float(r.get("marketCapUsd") or 0)
        vol = float(r.get("volumeUsd24Hr") or 0)
        change_24h = float(r["changePercent24Hr"]) if r.get("changePercent24Hr") else None
        supply = float(r.get("supply") or 0)
        max_supply = float(r["maxSupply"]) if r.get("maxSupply") else None
        coins.append({
            "id": (r.get("id") or "").lower(),
            "symbol": (r.get("symbol") or "").lower(),
            "name": r.get("name"),
            "image": None,
            "current_price": price,
            "market_cap": mcap,
            "market_cap_rank": int(r.get("rank") or 0),
            "total_volume": vol,
            "price_change_percentage_24h": change_24h,
            "price_change_percentage_7d_in_currency": None,
            "circulating_supply": supply,
            "total_supply": max_supply,
        })
    return coins


def _fetch_coins_coinpaprika():
    """Fetch from CoinPaprika as third fallback, normalised to CoinGecko format."""
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    resp = requests.get(f"{_CP_BASE}/tickers", headers=headers, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    if not isinstance(raw, list) or not raw:
        raise ValueError("CoinPaprika returned empty data")
    coins = []
    for r in raw[:250]:
        quotes = r.get("quotes", {}).get("USD", {})
        price = quotes.get("price") or 0
        mcap = quotes.get("market_cap") or 0
        vol = quotes.get("volume_24h") or 0
        change_24h = quotes.get("percent_change_24h")
        change_7d = quotes.get("percent_change_7d")
        coins.append({
            "id": (r.get("id") or "").lower(),
            "symbol": (r.get("symbol") or "").lower(),
            "name": r.get("name"),
            "image": None,
            "current_price": price,
            "market_cap": mcap,
            "market_cap_rank": r.get("rank") or 0,
            "total_volume": vol,
            "price_change_percentage_24h": change_24h,
            "price_change_percentage_7d_in_currency": change_7d,
            "circulating_supply": r.get("circulating_supply") or 0,
            "total_supply": r.get("total_supply"),
        })
    return coins


_expired_coins_backup = None  # fallback if all APIs fail

def _fetch_coins():
    global _expired_coins_backup
    cached = _cache.get("coins")
    if cached:
        return cached
    # Try multiple APIs in order
    sources = [
        (_fetch_coins_coingecko, "CoinGecko"),
        (_fetch_coins_coincap, "CoinCap"),
        (_fetch_coins_coinpaprika, "CoinPaprika"),
    ]
    for fetcher, name in sources:
        try:
            data = fetcher()
            _log(f"Crypto data loaded from {name}: {len(data)} coins")
            _cache.set("coins", data)
            _expired_coins_backup = data
            return data
        except Exception as e:
            _log(f"{name} fetch error: {e}")
    # Return last known data if all APIs fail
    if _expired_coins_backup:
        _log("Using expired backup for crypto data")
        return _expired_coins_backup
    return []

def screen_cryptos(criteria, on_progress=None, on_match=None):
    coins = _fetch_coins()
    total = len(coins)
    if on_progress:
        on_progress(0, total)

    for i, c in enumerate(coins):
        price = c.get("current_price") or 0
        mcap = c.get("market_cap") or 0
        change_24h = c.get("price_change_percentage_24h")
        change_7d = c.get("price_change_percentage_7d_in_currency")
        volume = c.get("total_volume") or 0
        circ = c.get("circulating_supply") or 0
        total_supply = c.get("total_supply")
        supply_ratio = round(circ / total_supply * 100, 1) if total_supply and total_supply > 0 else None

        # Apply filters
        cr = criteria
        if cr.get("min_price") is not None and price < cr["min_price"]:
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("max_price") is not None and price > cr["max_price"]:
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("min_market_cap") is not None and mcap < cr["min_market_cap"]:
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("max_market_cap") is not None and mcap > cr["max_market_cap"]:
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("min_change_24h") is not None and (change_24h is None or change_24h < cr["min_change_24h"]):
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("max_change_24h") is not None and (change_24h is None or change_24h > cr["max_change_24h"]):
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("min_change_7d") is not None and (change_7d is None or change_7d < cr["min_change_7d"]):
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("max_change_7d") is not None and (change_7d is None or change_7d > cr["max_change_7d"]):
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("min_volume") is not None and volume < cr["min_volume"]:
            if on_progress: on_progress(i + 1, total)
            continue
        if cr.get("max_volume") is not None and volume > cr["max_volume"]:
            if on_progress: on_progress(i + 1, total)
            continue

        match = {
            "rank": c.get("market_cap_rank"),
            "id": c.get("id"),
            "symbol": (c.get("symbol") or "").upper(),
            "name": c.get("name"),
            "image": c.get("image"),
            "price": round(price, 6) if price < 1 else round(price, 2),
            "market_cap": mcap,
            "change_24h": round(change_24h, 2) if change_24h is not None else None,
            "change_7d": round(change_7d, 2) if change_7d is not None else None,
            "volume": volume,
            "circulating_supply": circ,
            "total_supply": total_supply,
            "supply_ratio": supply_ratio,
        }
        if on_match:
            on_match(match)

        if on_progress:
            on_progress(i + 1, total)

def _chart_coingecko(coin_id, days):
    """Fetch chart from CoinGecko."""
    url = f"{_CG_BASE}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json().get("prices", [])


def _chart_coincap(coin_id, days):
    """Fetch chart from CoinCap. coin_id must be CoinCap-compatible (lowercase name)."""
    interval_map = {"1": "m15", "7": "h1", "30": "h6", "90": "h12", "365": "d1"}
    interval = interval_map.get(str(days), "h6")
    end = int(time.time() * 1000)
    start = end - int(days) * 86400 * 1000
    url = f"{_CC_BASE}/assets/{coin_id}/history"
    params = {"interval": interval, "start": start, "end": end}
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    raw = resp.json().get("data", [])
    return [[int(r["time"]), float(r["priceUsd"])] for r in raw if r.get("priceUsd")]


def get_crypto_chart(coin_id, days="30"):
    cache_key = f"{coin_id}_{days}"
    cached = _chart_cache.get(cache_key)
    if cached:
        return cached
    for fetcher, name in [(_chart_coingecko, "CoinGecko"), (_chart_coincap, "CoinCap")]:
        try:
            prices = fetcher(coin_id, days)
            if not prices:
                continue
            labels = []
            values = []
            for ts, price in prices:
                dt = datetime.utcfromtimestamp(ts / 1000)
                labels.append(dt.strftime("%Y-%m-%d %H:%M") if int(days) <= 1 else dt.strftime("%Y-%m-%d"))
                values.append(round(price, 6) if price < 1 else round(price, 2))
            result = {"labels": labels, "prices": values}
            _chart_cache.set(cache_key, result)
            return result
        except Exception as e:
            _log(f"Chart {name} error for {coin_id}: {e}")
    return None
