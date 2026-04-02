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
