"""
Database models for Charged Alpha user accounts and saved chart layouts.
Uses Flask-SQLAlchemy with PostgreSQL (Railway).
"""

import datetime
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin

db = SQLAlchemy()


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=True)  # null for OAuth-only users
    name = db.Column(db.String(255), nullable=True)
    provider = db.Column(db.String(50), default="local")  # local, google, github
    provider_id = db.Column(db.String(255), nullable=True)  # OAuth provider user ID
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    charts = db.relationship("SavedChart", backref="user", lazy="dynamic",
                             cascade="all, delete-orphan")
    game_scores = db.relationship("GameScore", backref="user", lazy="dynamic",
                                  cascade="all, delete-orphan")
    email_subscriptions = db.relationship("EmailSubscriber", backref="user", lazy="dynamic",
                                          cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User {self.email}>"


class SavedChart(db.Model):
    __tablename__ = "saved_charts"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    chart_name = db.Column(db.String(255), nullable=False)
    symbol = db.Column(db.String(50), nullable=True)
    state_json = db.Column(db.Text, nullable=False)  # TradingView serialized state
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow,
                           onupdate=datetime.datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("user_id", "chart_name", name="uq_user_chart_name"),
    )

    def __repr__(self):
        return f"<SavedChart {self.chart_name} ({self.symbol})>"


class GameScore(db.Model):
    __tablename__ = "game_scores"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True, index=True)
    game_slug = db.Column(db.String(80), nullable=False, index=True)
    display_name = db.Column(db.String(80), nullable=False)
    score = db.Column(db.Integer, nullable=False, index=True)
    return_percent = db.Column(db.Float, nullable=True)
    moves = db.Column(db.Integer, nullable=True)
    reallocations = db.Column(db.Integer, nullable=True)
    tax_paid = db.Column(db.Float, nullable=True)
    metadata_json = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, index=True)

    __table_args__ = (
        db.Index("ix_game_scores_game_score", "game_slug", "score"),
    )

    def __repr__(self):
        return f"<GameScore {self.game_slug} {self.score}>"


class EmailSubscriber(db.Model):
    __tablename__ = "email_subscribers"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True, index=True)
    email = db.Column(db.String(255), nullable=False, unique=True, index=True)
    name = db.Column(db.String(255), nullable=True)
    subscribed = db.Column(db.Boolean, nullable=False, default=True, index=True)
    consent_source = db.Column(db.String(80), nullable=True)
    # Set only when an explicit opt-in occurs. A suppression row created by an
    # unsubscribe request must never look like it was previously subscribed.
    subscribed_at = db.Column(db.DateTime, nullable=True)
    unsubscribed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow,
    )

    def __repr__(self):
        status = "subscribed" if self.subscribed else "unsubscribed"
        return f"<EmailSubscriber {self.email} {status}>"


class AppAnalyticsEvent(db.Model):
    """Privacy-safe product event from the Charged Alpha mobile app.

    ``install_id`` is generated randomly by the app and is not derived from a
    device, advertising, account, email, or store identifier. Event payloads
    are validated against a small allowlist before reaching this table.
    """

    __tablename__ = "app_analytics_events"

    id = db.Column(db.Integer, primary_key=True)
    event_id = db.Column(db.String(36), nullable=False, unique=True, index=True)
    install_id = db.Column(db.String(36), nullable=False, index=True)
    session_id = db.Column(db.String(36), nullable=False, index=True)
    event_name = db.Column(db.String(64), nullable=False, index=True)
    platform = db.Column(db.String(16), nullable=False, index=True)
    app_version = db.Column(db.String(40), nullable=False, index=True)
    app_build = db.Column(db.String(40), nullable=False)
    schema_version = db.Column(db.Integer, nullable=False)
    occurred_at = db.Column(db.DateTime, nullable=False, index=True)
    properties_json = db.Column(db.Text, nullable=False, default="{}")
    received_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=False, index=True)

    __table_args__ = (
        db.Index("ix_app_analytics_name_occurred", "event_name", "occurred_at"),
        db.Index("ix_app_analytics_install_occurred", "install_id", "occurred_at"),
    )

    def __repr__(self):
        return f"<AppAnalyticsEvent {self.event_name} {self.event_id}>"
