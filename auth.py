"""
Authentication blueprint — email/password + Google/GitHub OAuth.
"""

import os
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from authlib.integrations.flask_client import OAuth

from models import db, User

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# ── OAuth setup (initialized in init_oauth) ────────────────────────────────
oauth = OAuth()


def init_oauth(app):
    """Call once from app.py after app is configured."""
    oauth.init_app(app)

    # Google
    if os.environ.get("GOOGLE_CLIENT_ID"):
        oauth.register(
            name="google",
            client_id=os.environ["GOOGLE_CLIENT_ID"],
            client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
        )

    # GitHub
    if os.environ.get("GITHUB_CLIENT_ID"):
        oauth.register(
            name="github",
            client_id=os.environ["GITHUB_CLIENT_ID"],
            client_secret=os.environ["GITHUB_CLIENT_SECRET"],
            access_token_url="https://github.com/login/oauth/access_token",
            authorize_url="https://github.com/login/oauth/authorize",
            api_base_url="https://api.github.com/",
            client_kwargs={"scope": "user:email"},
        )


# ── Email / password ───────────────────────────────────────────────────────

@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect("/charts/")

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")

        if not email or not password:
            flash("Email and password are required.", "error")
            return render_template("auth/register.html")
        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template("auth/register.html")
        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return render_template("auth/register.html")

        existing = User.query.filter_by(email=email).first()
        if existing:
            flash("An account with this email already exists.", "error")
            return render_template("auth/register.html")

        user = User(
            email=email,
            name=name or email.split("@")[0],
            password_hash=generate_password_hash(password, method="pbkdf2:sha256"),
            provider="local",
        )
        db.session.add(user)
        db.session.commit()
        login_user(user)
        next_url = request.args.get("next", "/charts/")
        return redirect(next_url)

    return render_template("auth/register.html")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect("/charts/")

    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        user = User.query.filter_by(email=email).first()
        if not user or not user.password_hash:
            flash("Invalid email or password.", "error")
            return render_template("auth/login.html")
        if not check_password_hash(user.password_hash, password):
            flash("Invalid email or password.", "error")
            return render_template("auth/login.html")

        login_user(user)
        next_url = request.args.get("next", "/charts/")
        return redirect(next_url)

    return render_template("auth/login.html")


@auth_bp.route("/logout")
def logout():
    logout_user()
    return redirect("/")


# ── Current user API (for frontend) ───────────────────────────────────────

@auth_bp.route("/api/me")
def me():
    if current_user.is_authenticated:
        return jsonify({
            "authenticated": True,
            "name": current_user.name,
            "email": current_user.email,
        })
    return jsonify({"authenticated": False})


# ── Google OAuth ───────────────────────────────────────────────────────────

@auth_bp.route("/google")
def google_login():
    if not oauth.google:
        flash("Google sign-in is not configured.", "error")
        return redirect(url_for("auth.login"))
    redirect_uri = url_for("auth.google_callback", _external=True)
    return oauth.google.authorize_redirect(redirect_uri)


@auth_bp.route("/google/callback")
def google_callback():
    try:
        token = oauth.google.authorize_access_token()
        userinfo = token.get("userinfo") or oauth.google.userinfo()
    except Exception:
        flash("Google sign-in failed. Please try again.", "error")
        return redirect(url_for("auth.login"))

    email = userinfo.get("email", "").lower()
    if not email:
        flash("Could not retrieve email from Google.", "error")
        return redirect(url_for("auth.login"))

    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(
            email=email,
            name=userinfo.get("name", email.split("@")[0]),
            provider="google",
            provider_id=userinfo.get("sub"),
        )
        db.session.add(user)
        db.session.commit()
    login_user(user)
    return redirect("/charts/")


# ── GitHub OAuth ───────────────────────────────────────────────────────────

@auth_bp.route("/github")
def github_login():
    if not oauth.github:
        flash("GitHub sign-in is not configured.", "error")
        return redirect(url_for("auth.login"))
    redirect_uri = url_for("auth.github_callback", _external=True)
    return oauth.github.authorize_redirect(redirect_uri)


@auth_bp.route("/github/callback")
def github_callback():
    try:
        token = oauth.github.authorize_access_token()
        resp = oauth.github.get("user", token=token)
        profile = resp.json()
    except Exception:
        flash("GitHub sign-in failed. Please try again.", "error")
        return redirect(url_for("auth.login"))

    # GitHub may not return email in profile — fetch from /user/emails
    email = profile.get("email")
    if not email:
        try:
            emails_resp = oauth.github.get("user/emails", token=token)
            emails = emails_resp.json()
            primary = next((e for e in emails if e.get("primary")), None)
            email = primary["email"] if primary else emails[0]["email"]
        except Exception:
            flash("Could not retrieve email from GitHub.", "error")
            return redirect(url_for("auth.login"))

    email = email.lower()
    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(
            email=email,
            name=profile.get("name") or profile.get("login", email.split("@")[0]),
            provider="github",
            provider_id=str(profile.get("id")),
        )
        db.session.add(user)
        db.session.commit()
    login_user(user)
    return redirect("/charts/")
