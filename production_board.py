"""Public episode history and anticipated earnings, with an explicit field allowlist."""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
CHICAGO = ZoneInfo('America/Chicago')
SCHEMA = 'charged-alpha-public-production/1'
PUBLISHED_RESULTS_STATUS = 'Actual results released; issuer/EDGAR source verified'


def text(value, limit=180):
    return ' '.join(str(value or '').split())[:limit]


def timestamp(value):
    try:
        value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return value if value.tzinfo else None
    except ValueError:
        return None


def date_label(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d').strftime('%a, %b %-d, %Y')
    except (ValueError, TypeError):
        return 'Date unconfirmed'


def time_label(value):
    value = timestamp(value)
    return value.astimezone(CHICAGO).strftime('%b %-d, %Y · %-I:%M %p %Z') if value else 'Update pending'


def public_url(value, hosts=None):
    try:
        parts = urlsplit(value or '')
        host = parts.hostname or ''
        if parts.scheme != 'https' or parts.username or parts.password or not re.fullmatch(r'[a-z0-9.-]+\.[a-z]{2,}', host):
            return None
        if hosts and host not in hosts:
            return None
        if any(k in parts.query.lower() for k in ('token=', 'apikey=', 'api_key=')):
            return None
        return value
    except (ValueError, TypeError):
        return None


def project_snapshot(snapshot, catalog):
    if snapshot.get('schema') != 'charged-alpha-production-dashboard/1':
        raise ValueError('Unsupported dashboard snapshot')
    if not timestamp(snapshot.get('generated_at')):
        raise ValueError('Dashboard timestamp missing')
    companies = {r.get('ticker'): r.get('company') for r in catalog.get('episodes', []) if isinstance(r, dict)}
    for ticker, metadata in catalog.get('stock_metadata', {}).items():
        if isinstance(metadata, dict) and metadata.get('company'):
            companies[ticker] = metadata['company']
    known = set(companies)
    history, seen, completed_event_dates = [], set(), set()
    for item in snapshot.get('completed_history', snapshot.get('completed', [])):
        if item.get('evidence') != 'actual_bundle_DONE' or not timestamp(item.get('completed_at')):
            continue
        ticker, period = text(item.get('ticker'), 16), text(item.get('period'), 40)
        if not re.fullmatch(r'[A-Z0-9.\-]{1,16}', ticker) or not period or (ticker, period) in seen:
            continue
        video = public_url(item.get('video_url'), {'youtu.be', 'youtube.com', 'www.youtube.com'})
        short = public_url(item.get('short_url'), {'youtu.be', 'youtube.com', 'www.youtube.com'})
        packet = public_url(item.get('packet_url'), {'chargedalpha.com', 'www.chargedalpha.com'})
        if not (video or short or packet):
            continue
        seen.add((ticker, period))
        report_date = text(item.get('report_date'), 10)
        if date_label(report_date) != 'Date unconfirmed':
            completed_event_dates.add((ticker, report_date))
        history.append({'ticker': ticker, 'company': text(companies.get(ticker) or ticker), 'period': period,
                        'completed_at': item['completed_at'], 'video_url': video, 'short_url': short,
                        'packet_url': packet, 'stock_url': '/shows/' + ticker.lower() if ticker in known else None})
    history.sort(key=lambda r: timestamp(r['completed_at']), reverse=True)
    # A DONE bundle closes only its source-backed ticker/report-date event.
    # Completion time is publication workflow evidence, not earnings timing.
    upcoming, seen = [], set()
    forecast = snapshot.get('forecast', {})
    candidates = [row for day in forecast.get('date_cards', []) for row in day.get('candidates', [])]
    candidates += forecast.get('pending_beyond_horizon', [])
    for item in candidates:
        ticker, report_date = text(item.get('ticker'), 16), text(item.get('report_date'), 10)
        if item.get('screen_status') != 'candidate' or date_label(report_date) == 'Date unconfirmed' or (ticker, report_date) in seen:
            continue
        if item.get('date_status') == PUBLISHED_RESULTS_STATUS:
            continue
        if not re.fullmatch(r'[A-Z0-9.\-]{1,16}', ticker):
            continue
        if (ticker, report_date) in completed_event_dates:
            continue
        seen.add((ticker, report_date))
        exact = timestamp(item.get('report_at'))
        slot = item.get('report_time')
        label = {'amc': 'After U.S. market close', 'bmo': 'Before U.S. market open', 'dmh': 'During U.S. market hours'}.get(slot, 'Time unconfirmed')
        if exact:
            label = exact.astimezone(CHICAGO).strftime('%-I:%M %p %Z')
        upcoming.append({'ticker': ticker, 'company': text(item.get('company') or ticker),
                         'report_date': report_date, 'report_time_label': label,
                         'planned_date': text(item.get('planned_date'), 10),
                         'date_status': 'Issuer-announced' if str(item.get('date_status', '')).startswith('Issuer') else 'Calendar estimate',
                         'source_url': None if item.get('source_url') == 'https://financialmodelingprep.com/stable/earnings-calendar' else public_url(item.get('source_url')),
                         'stock_url': '/shows/' + ticker.lower() if ticker in known else None})
    # Within each report date, retain the planning priority order from the saved forecast.
    upcoming.sort(key=lambda r: r['report_date'])
    health = snapshot.get('source_health', {})
    return {'schema': SCHEMA, 'updated_at': snapshot['generated_at'],
            'calendar_updated_at': health.get('fetched_at'), 'calendar_status': health.get('fetch_status', 'unavailable'),
            'daily_target': forecast.get('daily_target', 10), 'history': history, 'upcoming': upcoming}


def load_board(path=None, now=None):
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(CHICAGO).date()
    try:
        data = json.loads(Path(path or ROOT / 'data/production_dashboard.json').read_text())
        if data.get('schema') != SCHEMA:
            raise ValueError('Unsupported public dashboard')
    except (OSError, ValueError):
        data = {'history': [], 'upcoming': [], 'calendar_status': 'unavailable'}
    history = [{**row, 'completed_label': time_label(row.get('completed_at'))} for row in data.get('history', [])]
    upcoming = []
    for row in data.get('upcoming', []):
        if row.get('report_date', '') < today.isoformat():
            continue
        upcoming.append({**row, 'report_date_label': date_label(row['report_date']),
                         'planned_date_label': date_label(row.get('planned_date'))})
    fetched = timestamp(data.get('calendar_updated_at'))
    stale = data.get('calendar_status') != 'fresh' or not fetched or (now - fetched).total_seconds() > 4 * 3600
    days = []
    for offset in range(7):
        day = (today + timedelta(days=offset)).isoformat()
        days.append({'date_label': date_label(day), 'items': [r for r in upcoming if r['report_date'] == day]})
    return {'updated_label': time_label(data.get('updated_at')),
            'calendar_updated_label': time_label(data.get('calendar_updated_at')),
            'calendar_status': 'stale' if stale else 'fresh', 'daily_target': data.get('daily_target', 10),
            'latest': history[:1], 'upcoming': upcoming[:2], 'history': history,
            'days': days, 'stale': stale}
