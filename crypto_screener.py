import time
from datetime import datetime
import requests

_CG_BASE = "https://api.coingecko.com/api/v3"
_cache = {}
_CACHE_TTL = 120

def _fetch_coins():
    cached = _cache.get("coins")
    if cached and (time.time() - cached[0]) < _CACHE_TTL:
        return cached[1]
    url = f"{_CG_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "7d",
    }
    headers = {"Accept": "application/json"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    _cache["coins"] = (time.time(), data)
    return data

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

def get_crypto_chart(coin_id, days="30"):
    url = f"{_CG_BASE}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        prices = data.get("prices", [])
        labels = []
        values = []
        for ts, price in prices:
            dt = datetime.utcfromtimestamp(ts / 1000)
            labels.append(dt.strftime("%Y-%m-%d %H:%M") if int(days) <= 1 else dt.strftime("%Y-%m-%d"))
            values.append(round(price, 6) if price < 1 else round(price, 2))
        return {"labels": labels, "prices": values}
    except Exception:
        return None
