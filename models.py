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
