"""
Authentication blueprint — email/password + Google/GitHub OAuth.
"""

import os
import re
import secrets
import datetime
from urllib.parse import urlparse

from flask import Blueprint, current_app, render_template, request, redirect, url_for, flash, jsonify, session
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from authlib.integrations.flask_client import OAuth

from models import db, EmailSubscriber, User

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# ── OAuth setup (initialized in init_oauth) ────────────────────────────────
oauth = OAuth()

PUBLIC_NAME_BLOCKLIST = {
    "admin",
    "administrator",
    "asshole",
    "bitch",
    "chargedalpha",
    "cunt",
    "dick",
    "fuck",
    "hitler",
    "moderator",
    "nazi",
    "shit",
    "support",
}
COMMON_PASSWORDS = {"password", "password1", "qwerty123", "chargedalpha", "letmein123"}
PUBLIC_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9 .'-]{1,39}$")
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
TRUTHY_VALUES = {"1", "true", "yes", "on"}


def _env_is_set(name):
    return bool(os.environ.get(name, "").strip())


def _form_is_truthy(name):
    return request.values.get(name, "").strip().lower() in TRUTHY_VALUES


def google_oauth_available():
    return _env_is_set("GOOGLE_CLIENT_ID") and _env_is_set("GOOGLE_CLIENT_SECRET")


def github_oauth_available():
    return os.environ.get("ENABLE_GITHUB_AUTH", "").strip().lower() in {"1", "true", "yes"} and _env_is_set("GITHUB_CLIENT_ID") and _env_is_set("GITHUB_CLIENT_SECRET")


def _oauth_redirect_uri(provider):
    configured = os.environ.get(f"{provider.upper()}_REDIRECT_URI", "").strip()
    if configured:
        return configured
    return url_for(f"auth.{provider}_callback", _external=True)


def safe_next_url(raw_next, default="/"):
    """Return a same-site path for post-auth redirects."""
    if not raw_next:
        return default

    parsed = urlparse(raw_next)
    if parsed.scheme or parsed.netloc:
        if parsed.netloc != request.host:
            return default
        path = parsed.path or default
        if parsed.query:
            path += f"?{parsed.query}"
        if parsed.fragment:
            path += f"#{parsed.fragment}"
    else:
        path = raw_next

    if not path.startswith("/") or path.startswith("//"):
        return default

    parsed_path = urlparse(path)
    next_url = parsed_path.path or default
    if parsed_path.query:
        next_url += f"?{parsed_path.query}"
    if parsed_path.fragment:
        next_url += f"#{parsed_path.fragment}"
    return next_url


def _csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


@auth_bp.app_context_processor
def inject_auth_helpers():
    return {
        "csrf_token": _csrf_token,
        "google_oauth_available": google_oauth_available,
        "github_oauth_available": github_oauth_available,
    }


def _validate_csrf_token():
    token = session.get("_csrf_token")
    submitted = request.form.get("csrf_token", "")
    return bool(token and submitted and secrets.compare_digest(token, submitted))


def normalize_public_name(value, fallback=""):
    cleaned = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:40] or fallback


def _contains_blocked_name_token(value):
    compact = re.sub(r"[^a-z0-9]", "", (value or "").lower())
    tokens = re.findall(r"[a-z0-9]+", (value or "").lower())
    return compact in PUBLIC_NAME_BLOCKLIST or any(token in PUBLIC_NAME_BLOCKLIST for token in tokens)


def validate_account_name(value):
    name = normalize_public_name(value)
    if len(name) < 2:
        return None, "Enter your first name."
    if "@" in name or "http" in name.lower() or "www." in name.lower():
        return None, "Use a real first name, not an email address or link."
    if not PUBLIC_NAME_RE.match(name):
        return None, "Names can use letters, numbers, spaces, apostrophes, periods, and hyphens."
    if _contains_blocked_name_token(name):
        return None, "Choose a different display name."
    return name, None


def get_public_first_name(user):
    candidates = []
    if getattr(user, "name", None):
        candidates.append(str(user.name).split()[0])
    if getattr(user, "email", None):
        candidates.append(str(user.email).split("@")[0].replace(".", " ").replace("_", " ").split()[0])

    for candidate in candidates:
        first_name = normalize_public_name(candidate)
        if PUBLIC_NAME_RE.match(first_name) and not _contains_blocked_name_token(first_name):
            return first_name[:24]
    return "Player"


def get_email_updates_subscription(user):
    if not getattr(user, "is_authenticated", False):
        return None
    return EmailSubscriber.query.filter_by(email=(user.email or "").strip().lower()).first()


def set_email_updates_subscription(user, subscribed=True, source="account"):
    email = (getattr(user, "email", "") or "").strip().lower()
    if not email or not EMAIL_RE.match(email):
        return None

    now = datetime.datetime.utcnow()
    subscription = EmailSubscriber.query.filter_by(email=email).first()
    if not subscription:
        subscription = EmailSubscriber(email=email, created_at=now)
        db.session.add(subscription)

    subscription.user_id = getattr(user, "id", None)
    subscription.name = normalize_public_name(getattr(user, "name", "") or get_public_first_name(user))
    subscription.subscribed = bool(subscribed)
    subscription.consent_source = source
    subscription.updated_at = now
    if subscribed:
        subscription.subscribed_at = now
        subscription.unsubscribed_at = None
    else:
        subscription.unsubscribed_at = now
    db.session.commit()
    return subscription


def _validate_password(password, email):
    if len(password) < 10:
        return "Password must be at least 10 characters."
    if not re.search(r"[A-Za-z]", password) or not re.search(r"\d", password):
        return "Password must include at least one letter and one number."
    lowered = password.lower()
    email_prefix = (email or "").split("@")[0].lower()
    if lowered in COMMON_PASSWORDS or (email_prefix and email_prefix in lowered):
        return "Choose a less predictable password."
    return None


def init_oauth(app):
    """Call once from app.py after app is configured."""
    oauth.init_app(app)

    # Google
    if google_oauth_available():
        oauth.register(
            name="google",
            client_id=os.environ["GOOGLE_CLIENT_ID"],
            client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            api_base_url="https://openidconnect.googleapis.com/v1/",
            client_kwargs={"scope": "openid email profile"},
        )

    # GitHub
    if github_oauth_available():
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
        return redirect(safe_next_url(request.args.get("next"), "/"))

    if request.method == "POST":
        if not _validate_csrf_token():
            flash("Your session expired. Please try again.", "error")
            return render_template("auth/register.html")

        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")
        wants_email_updates = _form_is_truthy("email_updates")

        clean_name, name_error = validate_account_name(name)
        if name_error:
            flash(name_error, "error")
            return render_template("auth/register.html")
        if not email or not password:
            flash("Email and password are required.", "error")
            return render_template("auth/register.html")
        if not EMAIL_RE.match(email):
            flash("Enter a valid email address.", "error")
            return render_template("auth/register.html")
        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template("auth/register.html")
        password_error = _validate_password(password, email)
        if password_error:
            flash(password_error, "error")
            return render_template("auth/register.html")

        existing = User.query.filter_by(email=email).first()
        if existing:
            flash("An account with this email already exists.", "error")
            return render_template("auth/register.html")

        user = User(
            email=email,
            name=clean_name,
            password_hash=generate_password_hash(password, method="pbkdf2:sha256"),
            provider="local",
        )
        db.session.add(user)
        db.session.commit()
        if wants_email_updates:
            set_email_updates_subscription(user, True, "register-email")
        login_user(user)
        next_url = safe_next_url(request.args.get("next"), "/")
        return redirect(next_url)

    return render_template("auth/register.html")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(safe_next_url(request.args.get("next"), "/"))

    if request.method == "POST":
        if not _validate_csrf_token():
            flash("Your session expired. Please try again.", "error")
            return render_template("auth/login.html")

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
        next_url = safe_next_url(request.args.get("next"), "/")
        return redirect(next_url)

    return render_template("auth/login.html")


@auth_bp.route("/logout")
def logout():
    logout_user()
    return redirect("/")


# ── Current user API (for frontend) ───────────────────────────────────────

@auth_bp.route("/api/me")
def me():
    next_url = safe_next_url(request.args.get("next") or request.referrer, "/")
    if current_user.is_authenticated:
        first_name = get_public_first_name(current_user)
        return jsonify({
            "authenticated": True,
            "name": current_user.name,
            "email": current_user.email,
            "firstName": first_name,
        })
    return jsonify({
        "authenticated": False,
        "loginUrl": url_for("auth.login", next=next_url),
        "registerUrl": url_for("auth.register", next=next_url),
    })


# ── Google OAuth ───────────────────────────────────────────────────────────

@auth_bp.route("/google")
def google_login():
    next_url = safe_next_url(request.args.get("next"), "/")
    if not hasattr(oauth, 'google') or not google_oauth_available():
        flash("Google sign-in is not configured yet. Use email sign-in for now.", "error")
        return redirect(url_for("auth.login", next=next_url))
    session["auth_next"] = safe_next_url(request.args.get("next"), "/")
    session["email_updates_opt_in"] = _form_is_truthy("email_updates")
    redirect_uri = _oauth_redirect_uri("google")
    return oauth.google.authorize_redirect(redirect_uri, prompt="select_account")


@auth_bp.route("/google/callback")
def google_callback():
    if not hasattr(oauth, 'google') or not google_oauth_available():
        flash("Google sign-in is not configured yet. Use email sign-in for now.", "error")
        return redirect(url_for("auth.login"))

    try:
        token = oauth.google.authorize_access_token()
        userinfo = token.get("userinfo")
        if not userinfo:
            userinfo_response = oauth.google.get("userinfo", token=token)
            userinfo = userinfo_response.json()
    except Exception:
        current_app.logger.exception("Google sign-in failed")
        flash("Google sign-in failed. Please try again.", "error")
        return redirect(url_for("auth.login"))

    email = userinfo.get("email", "").lower()
    if not email:
        flash("Could not retrieve email from Google.", "error")
        return redirect(url_for("auth.login"))

    user = User.query.filter_by(email=email).first()
    if not user:
        profile_name = userinfo.get("given_name") or userinfo.get("name") or email.split("@")[0]
        profile_parts = normalize_public_name(str(profile_name)).split()
        clean_name, _ = validate_account_name(profile_parts[0] if profile_parts else email.split("@")[0])
        user = User(
            email=email,
            name=clean_name or "Player",
            provider="google",
            provider_id=userinfo.get("sub"),
        )
        db.session.add(user)
        db.session.commit()
    if session.pop("email_updates_opt_in", False):
        set_email_updates_subscription(user, True, "register-google")
    login_user(user)
    return redirect(session.pop("auth_next", "/"))


@auth_bp.route("/email-updates", methods=["POST"])
@login_required
def email_updates():
    if not _validate_csrf_token():
        flash("Your session expired. Please try again.", "error")
        return redirect(url_for("account"))

    action = request.form.get("action", "subscribe")
    if action == "unsubscribe":
        set_email_updates_subscription(current_user, False, "account-unsubscribe")
        flash("Email updates are turned off.", "success")
    else:
        set_email_updates_subscription(current_user, True, "account")
        flash("You're on the Charged Alpha email updates list.", "success")
    return redirect(url_for("account"))


# ── GitHub OAuth ───────────────────────────────────────────────────────────

@auth_bp.route("/github")
def github_login():
    next_url = safe_next_url(request.args.get("next"), "/")
    if not hasattr(oauth, 'github') or not github_oauth_available():
        flash("GitHub sign-in is not available yet.", "error")
        return redirect(url_for("auth.login", next=next_url))
    session["auth_next"] = safe_next_url(request.args.get("next"), "/")
    redirect_uri = _oauth_redirect_uri("github")
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
        profile_name = profile.get("name") or profile.get("login") or email.split("@")[0]
        profile_parts = normalize_public_name(str(profile_name)).split()
        clean_name, _ = validate_account_name(profile_parts[0] if profile_parts else email.split("@")[0])
        user = User(
            email=email,
            name=clean_name or "Player",
            provider="github",
            provider_id=str(profile.get("id")),
        )
        db.session.add(user)
        db.session.commit()
    login_user(user)
    return redirect(session.pop("auth_next", "/"))
