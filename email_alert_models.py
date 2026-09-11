"""Additive tables for explicitly confirmed stock alerts, separate from app updates."""
import datetime as dt
import uuid
from models import db


def uid():
    return uuid.uuid4().hex


class StockAlertSubscriber(db.Model):
    __tablename__ = 'stock_alert_subscribers'
    id = db.Column(db.String(32), primary_key=True, default=uid)
    email = db.Column(db.String(254), nullable=False, unique=True)
    state = db.Column(db.String(20), nullable=False, default='pending', index=True)
    tickers_json = db.Column(db.Text, nullable=False, default='[]')
    version = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, nullable=False, default=dt.datetime.utcnow)
    confirmed_at = db.Column(db.DateTime)
    updated_at = db.Column(db.DateTime, nullable=False, default=dt.datetime.utcnow)


class StockAlertRequest(db.Model):
    __tablename__ = 'stock_alert_requests'
    id = db.Column(db.String(32), primary_key=True, default=uid)
    token_hash = db.Column(db.String(64), nullable=False, unique=True)
    subscriber_id = db.Column(db.String(32), db.ForeignKey('stock_alert_subscribers.id'), nullable=False, index=True)
    version = db.Column(db.Integer, nullable=False)
    kind = db.Column(db.String(16), nullable=False)
    tickers_json = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=dt.datetime.utcnow)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    used_at = db.Column(db.DateTime)


class StockAlertSeen(db.Model):
    __tablename__ = 'stock_alert_seen'
    subscriber_id = db.Column(db.String(32), db.ForeignKey('stock_alert_subscribers.id'), primary_key=True)
    edition_key = db.Column(db.String(100), primary_key=True)
    seen_at = db.Column(db.DateTime, nullable=False, default=dt.datetime.utcnow)


class StockAlertMessage(db.Model):
    __tablename__ = 'stock_alert_messages'
    id = db.Column(db.String(32), primary_key=True, default=uid)
    dedupe_key = db.Column(db.String(120), nullable=False, unique=True)
    subscriber_id = db.Column(db.String(32), db.ForeignKey('stock_alert_subscribers.id'), nullable=False, index=True)
    version = db.Column(db.Integer, nullable=False)
    kind = db.Column(db.String(16), nullable=False)
    payload_json = db.Column(db.Text, nullable=False)
    editions_json = db.Column(db.Text, nullable=False, default='[]')
    state = db.Column(db.String(20), nullable=False, default='queued', index=True)
    provider_id = db.Column(db.String(80), unique=True)
    created_at = db.Column(db.DateTime, nullable=False, default=dt.datetime.utcnow)
    first_attempt_at = db.Column(db.DateTime, index=True)
    retry_at = db.Column(db.DateTime, nullable=False, default=dt.datetime.utcnow)
    attempts = db.Column(db.Integer, nullable=False, default=0)
    sent_at = db.Column(db.DateTime)
    outcome = db.Column(db.String(80))


class StockAlertThrottle(db.Model):
    __tablename__ = 'stock_alert_throttles'
    key = db.Column(db.String(64), primary_key=True)
    hits = db.Column(db.Integer, nullable=False, default=0)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)


class StockAlertLease(db.Model):
    __tablename__ = 'stock_alert_leases'
    id = db.Column(db.String(20), primary_key=True)
    owner = db.Column(db.String(32), nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)
