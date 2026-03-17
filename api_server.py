#!/usr/bin/env python3
"""Friends Bracket Pool — FastAPI backend with PostgreSQL (Neon)."""
import json
import os
import sys
import time
import base64
import hashlib
import secrets
import logging
import traceback
from contextlib import asynccontextmanager
from urllib.request import urlopen, Request as UrlRequest
from urllib.error import URLError

from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel
from typing import Optional
import psycopg2
import psycopg2.extras

# Set up logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def hash_password(password: str) -> str:
    """Hash a password with a random salt using SHA-256."""
    salt = secrets.token_hex(16)
    h = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
    return f"{salt}:{h}"

def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored salt:hash."""
    if ":" not in stored_hash:
        return False
    salt, h = stored_hash.split(":", 1)
    return hashlib.sha256(f"{salt}:{password}".encode()).hexdigest() == h

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_bkNlfWGCVD95@ep-dawn-lake-am5wefih-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require"
)
logger.info(f"Connecting to PostgreSQL...")

def get_db():
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=30)
    conn.autocommit = False
    return conn

def dict_row(cursor):
    """Convert a cursor row to a dict."""
    if cursor.description is None:
        return None
    columns = [col.name for col in cursor.description]
    def make_dict(row):
        return dict(zip(columns, row)) if row else None
    return make_dict

def fetchall_dict(cursor):
    """Fetch all rows as list of dicts."""
    if cursor.description is None:
        return []
    columns = [col.name for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def fetchone_dict(cursor):
    """Fetch one row as dict."""
    if cursor.description is None:
        return None
    columns = [col.name for col in cursor.description]
    row = cursor.fetchone()
    return dict(zip(columns, row)) if row else None

def init_db(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL DEFAULT '',
            display_name TEXT NOT NULL,
            avatar_data TEXT DEFAULT '',
            is_admin INTEGER DEFAULT 0,
            created_at DOUBLE PRECISION NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS brackets (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            label TEXT NOT NULL DEFAULT 'Bracket 1',
            picks TEXT NOT NULL DEFAULT '{}',
            submitted INTEGER DEFAULT 0,
            submitted_at DOUBLE PRECISION,
            created_at DOUBLE PRECISION NOT NULL,
            updated_at DOUBLE PRECISION NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS site_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT ''
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bets (
            id SERIAL PRIMARY KEY,
            creator_id INTEGER NOT NULL REFERENCES users(id),
            about_user_id INTEGER REFERENCES users(id),
            bet_type TEXT NOT NULL DEFAULT 'friend',
            description TEXT NOT NULL,
            amount DOUBLE PRECISION NOT NULL,
            taker_id INTEGER REFERENCES users(id),
            taken_at DOUBLE PRECISION,
            closed BOOLEAN NOT NULL DEFAULT FALSE,
            closed_at DOUBLE PRECISION,
            created_at DOUBLE PRECISION NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tournament_results (
            game_key TEXT PRIMARY KEY,
            espn_game_id TEXT,
            round INTEGER,
            round_name TEXT,
            region TEXT,
            team1_name TEXT,
            team1_seed INTEGER,
            team1_score INTEGER,
            team2_name TEXT,
            team2_seed INTEGER,
            team2_score INTEGER,
            winner_name TEXT,
            winner_seed INTEGER,
            game_state TEXT DEFAULT 'pre',
            game_date TEXT,
            updated_at DOUBLE PRECISION
        );
    """)
    # Add tiebreaker_score column if missing
    cur.execute("""
        ALTER TABLE brackets ADD COLUMN IF NOT EXISTS tiebreaker_score INTEGER DEFAULT NULL
    """)
    # Add bio column if missing
    cur.execute("""
        ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT DEFAULT ''
    """)
    # Stay info table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stay_info (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            hotel_name TEXT NOT NULL DEFAULT '',
            hotel_link TEXT NOT NULL DEFAULT '',
            check_in TEXT NOT NULL DEFAULT '',
            check_out TEXT NOT NULL DEFAULT '',
            updated_at DOUBLE PRECISION NOT NULL,
            UNIQUE(user_id)
        );
    """)
    # Arrival/departure columns on stay_info
    cur.execute("ALTER TABLE stay_info ADD COLUMN IF NOT EXISTS arrival TEXT DEFAULT ''")
    cur.execute("ALTER TABLE stay_info ADD COLUMN IF NOT EXISTS departure TEXT DEFAULT ''")
    # Closed bet columns
    cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS closed BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS closed_at DOUBLE PRECISION")

    conn.commit()
    cur.close()
    logger.info("DB tables initialized")

def seed_admin(conn):
    """Ensure the admin user exists."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE LOWER(username) = 'paul'")
    row = cur.fetchone()
    if not row:
        now = time.time()
        pw_hash = hash_password("admin2026")
        cur.execute(
            "INSERT INTO users (email, username, password_hash, display_name, is_admin, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
            ('prcummins@gmail.com', 'paul', pw_hash, 'Paul', 1, now)
        )
        conn.commit()
        logger.info("Admin user 'paul' seeded")
    else:
        logger.info("Admin user 'paul' already exists")
    cur.close()

db = None

def ensure_db():
    """Connect to DB with retries. Called at startup and lazily if needed."""
    global db
    for attempt in range(5):
        try:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass
            db = get_db()
            init_db(db)
            seed_admin(db)
            cur = db.cursor()
            cur.execute("SELECT id, username FROM users")
            test_rows = fetchall_dict(cur)
            cur.close()
            logger.info(f"DB setup complete. Users: {test_rows}")
            return
        except Exception as e:
            logger.error(f"DB connect attempt {attempt+1}/5 failed: {e}")
            if attempt < 4:
                import time as _t
                _t.sleep(3)
    logger.error("All DB connection attempts failed")

try:
    ensure_db()
except Exception as e:
    logger.error(f"DB SETUP ERROR (non-fatal): {e}")

# Bet reveal time: Saturday March 21, 2026 at 12:00 PM PDT (UTC-7) = 19:00 UTC
BET_REVEAL_TIMESTAMP = 1774314000  # March 21 2026 12:00 PM PDT

def get_cursor():
    """Get a cursor, reconnecting if needed."""
    global db
    if db is None:
        ensure_db()
    try:
        db.isolation_level
        cur = db.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
    except Exception:
        logger.info("Reconnecting to PostgreSQL...")
        try:
            db.close()
        except Exception:
            pass
        db = get_db()
    return db.cursor()

@asynccontextmanager
async def lifespan(app):
    yield
    try:
        db.close()
    except Exception:
        pass

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---- Models ----
class LoginRequest(BaseModel):
    username: str
    password: str

class SavePicksRequest(BaseModel):
    picks: dict
    tiebreaker_score: Optional[int] = None

class SubmitBracketRequest(BaseModel):
    picks: dict
    tiebreaker_score: Optional[int] = None

class CreateBracketRequest(BaseModel):
    user_id: int

class CreateBetRequest(BaseModel):
    about_user_id: int
    description: str
    amount: float

class TakeBetRequest(BaseModel):
    pass

class AddUserRequest(BaseModel):
    email: str
    username: str
    password: str
    display_name: str
    is_admin: int = 0

class UpdateAvatarRequest(BaseModel):
    user_id: int

class MagicLinkRequest(BaseModel):
    email: str

# ---- Session tokens (in-memory) ----
active_sessions = {}  # token -> user_id
magic_links = {}  # token -> {"user_id": int, "expires": float}

# ---- Auth (username + password) ----
@app.post("/api/login")
def login(req: LoginRequest):
    try:
        username = req.username.strip().lower()
        logger.info(f"Login attempt for: {username}")
        cur = get_cursor()
        cur.execute("SELECT * FROM users WHERE LOWER(username) = %s", (username,))
        user = fetchone_dict(cur)
        cur.close()
        if not user:
            raise HTTPException(status_code=401, detail="Invalid username or password")
        if not verify_password(req.password, user.get("password_hash", "")):
            raise HTTPException(status_code=401, detail="Invalid username or password")
        user.pop("password_hash", None)
        # Create session token
        token = secrets.token_urlsafe(32)
        active_sessions[token] = user["id"]
        return {"user": user, "session_token": token}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"LOGIN ERROR: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/session")
def restore_session(token: str = ""):
    """Restore a session from a token."""
    if not token or token not in active_sessions:
        raise HTTPException(status_code=401, detail="Invalid session")
    user_id = active_sessions[token]
    try:
        cur = get_cursor()
        cur.execute("SELECT id, email, username, display_name, avatar_data, is_admin, COALESCE(bio, '') as bio FROM users WHERE id = %s", (user_id,))
        user = fetchone_dict(cur)
        cur.close()
        if not user:
            del active_sessions[token]
            raise HTTPException(status_code=401, detail="User not found")
        return {"user": user}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"SESSION RESTORE ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ---- Magic Link Login ----
@app.post("/api/magic-link")
def request_magic_link(req: MagicLinkRequest):
    """Generate a magic login link and email it to the user."""
    try:
        email = req.email.strip().lower()
        cur = get_cursor()
        cur.execute("SELECT id, email, display_name FROM users WHERE LOWER(email) = %s", (email,))
        user = fetchone_dict(cur)
        cur.close()
        if not user:
            # Don't reveal if email exists or not
            return {"ok": True, "message": "If that email is registered, a login link has been sent."}
        # Generate token valid for 15 minutes
        token = secrets.token_urlsafe(48)
        magic_links[token] = {"user_id": user["id"], "expires": time.time() + 900}
        # Build the magic link URL
        base_url = os.environ.get("BASE_URL", "https://viva-las-vegas-2026.onrender.com")
        magic_url = f"{base_url}?magic={token}"
        # Send email
        _send_magic_email(user["email"], user["display_name"], magic_url)
        logger.info(f"Magic link generated for {email}")
        return {"ok": True, "message": "If that email is registered, a login link has been sent."}
    except Exception as e:
        logger.error(f"MAGIC LINK ERROR: {e}")
        logger.error(traceback.format_exc())
        # Still return success to not leak info
        return {"ok": True, "message": "If that email is registered, a login link has been sent."}

@app.get("/api/magic-login")
def magic_login(token: str = ""):
    """Validate a magic link token and create a session."""
    if not token or token not in magic_links:
        raise HTTPException(status_code=401, detail="Invalid or expired link")
    link_data = magic_links[token]
    if time.time() > link_data["expires"]:
        del magic_links[token]
        raise HTTPException(status_code=401, detail="Link has expired. Please request a new one.")
    user_id = link_data["user_id"]
    # Clean up used token
    del magic_links[token]
    try:
        cur = get_cursor()
        cur.execute("SELECT id, email, username, display_name, avatar_data, is_admin, COALESCE(bio, '') as bio FROM users WHERE id = %s", (user_id,))
        user = fetchone_dict(cur)
        cur.close()
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        # Create session
        session_token = secrets.token_urlsafe(32)
        active_sessions[session_token] = user["id"]
        return {"user": user, "session_token": session_token}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"MAGIC LOGIN ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _send_magic_email(to_email: str, display_name: str, magic_url: str):
    """Send a magic login link via SMTP."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_from = os.environ.get("SMTP_FROM", smtp_user)
    if not smtp_user or not smtp_pass:
        logger.warning("SMTP not configured — magic link email not sent. Link: " + magic_url)
        return
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Your Viva Las Vegas Login Link"
    msg["From"] = f"Viva Las Vegas 2026 <{smtp_from}>"
    msg["To"] = to_email
    text = f"""Hey {display_name},

Here's your magic login link for Viva Las Vegas 2026:

{magic_url}

This link expires in 15 minutes. If you didn't request this, just ignore it.

- The Pool"""
    html = f"""\
<html><body style="font-family:sans-serif;background:#f1f5f9;padding:20px;">
<div style="max-width:480px;margin:0 auto;background:#fff;border-radius:12px;padding:32px;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
  <h2 style="color:#1a2744;margin-top:0;">&#127922; Viva Las Vegas 2026</h2>
  <p>Hey <strong>{display_name}</strong>,</p>
  <p>Click below to log in — no password needed:</p>
  <a href="{magic_url}" style="display:inline-block;background:#1a2744;color:#fff;padding:14px 28px;border-radius:8px;text-decoration:none;font-weight:bold;margin:16px 0;">Log In Now</a>
  <p style="color:#64748b;font-size:13px;">This link expires in 15 minutes. If you didn't request this, just ignore it.</p>
</div>
</body></html>"""
    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_from, to_email, msg.as_string())
        logger.info(f"Magic link email sent to {to_email}")
    except Exception as e:
        logger.error(f"Failed to send magic link email: {e}")

# ---- Users ----
@app.get("/api/users")
def list_users():
    try:
        cur = get_cursor()
        cur.execute("SELECT id, email, username, display_name, avatar_data, is_admin, COALESCE(bio, '') as bio FROM users ORDER BY display_name")
        rows = fetchall_dict(cur)
        cur.close()
        return rows
    except Exception as e:
        logger.error(f"LIST_USERS ERROR: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/reset-password/{user_id}")
def reset_password(user_id: int, req: dict):
    new_password = req.get("password", "")
    if not new_password:
        raise HTTPException(status_code=400, detail="Password is required")
    pw_hash = hash_password(new_password)
    cur = get_cursor()
    cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (pw_hash, user_id))
    db.commit()
    cur.close()
    return {"ok": True}

@app.post("/api/admin/users")
def add_user(req: AddUserRequest):
    email = req.email.strip().lower()
    username = req.username.strip().lower()
    if not username or not req.password:
        raise HTTPException(status_code=400, detail="Username and password are required")
    cur = get_cursor()
    cur.execute("SELECT id FROM users WHERE LOWER(email) = %s", (email,))
    if cur.fetchone():
        cur.close()
        raise HTTPException(status_code=409, detail="Email already exists")
    cur.execute("SELECT id FROM users WHERE LOWER(username) = %s", (username,))
    if cur.fetchone():
        cur.close()
        raise HTTPException(status_code=409, detail="Username already taken")
    now = time.time()
    pw_hash = hash_password(req.password)
    cur.execute(
        "INSERT INTO users (email, username, password_hash, display_name, is_admin, created_at) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
        (email, username, pw_hash, req.display_name, req.is_admin, now)
    )
    user_id = cur.fetchone()[0]
    db.commit()
    cur.close()
    return {"id": user_id, "email": email, "username": username, "display_name": req.display_name}

@app.delete("/api/admin/users/{user_id}")
def remove_user(user_id: int):
    cur = get_cursor()
    cur.execute("DELETE FROM bets WHERE creator_id = %s OR about_user_id = %s OR taker_id = %s", (user_id, user_id, user_id))
    cur.execute("DELETE FROM brackets WHERE user_id = %s", (user_id,))
    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    db.commit()
    cur.close()
    return {"deleted": user_id}

@app.post("/api/admin/avatar/{user_id}")
async def upload_avatar(user_id: int, file: UploadFile = File(...)):
    data = await file.read()
    if len(data) > 2_000_000:
        raise HTTPException(status_code=400, detail="Image too large (max 2MB)")
    b64 = base64.b64encode(data).decode()
    mime = file.content_type or "image/jpeg"
    data_url = f"data:{mime};base64,{b64}"
    cur = get_cursor()
    cur.execute("UPDATE users SET avatar_data = %s WHERE id = %s", (data_url, user_id))
    db.commit()
    cur.close()
    return {"ok": True}

class UpdateBioRequest(BaseModel):
    bio: str = ''

@app.put("/api/users/{user_id}/bio")
def update_bio(user_id: int, req: UpdateBioRequest):
    try:
        cur = get_cursor()
        cur.execute("UPDATE users SET bio = %s WHERE id = %s", (req.bio.strip()[:200], user_id))
        db.commit()
        cur.close()
        return {"ok": True}
    except Exception as e:
        logger.error(f"UPDATE_BIO ERROR: {e}")
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))

# ---- Brackets ----
@app.get("/api/brackets")
def list_brackets():
    cur = get_cursor()
    cur.execute("""
        SELECT b.id, b.user_id, b.label, b.picks, b.submitted, b.submitted_at, b.tiebreaker_score, u.display_name, u.avatar_data
        FROM brackets b JOIN users u ON b.user_id = u.id
        ORDER BY u.display_name, b.label
    """)
    rows = fetchall_dict(cur)
    cur.close()
    results = []
    for d in rows:
        d["picks"] = json.loads(d["picks"]) if d["picks"] else {}
        d["pick_count"] = len(d["picks"])
        results.append(d)
    return results

@app.get("/api/brackets/{user_id}")
def get_user_brackets(user_id: int):
    cur = get_cursor()
    cur.execute("""
        SELECT b.*, u.display_name, u.avatar_data
        FROM brackets b JOIN users u ON b.user_id = u.id
        WHERE b.user_id = %s
        ORDER BY b.label
    """, (user_id,))
    rows = fetchall_dict(cur)
    cur.close()
    results = []
    for d in rows:
        d["picks"] = json.loads(d["picks"]) if d["picks"] else {}
        results.append(d)
    return results

@app.post("/api/brackets")
def create_bracket(req: CreateBracketRequest):
    cur = get_cursor()
    cur.execute("SELECT COUNT(*) as c FROM brackets WHERE user_id = %s", (req.user_id,))
    count = cur.fetchone()[0]
    label = f"Bracket {count + 1}"
    now = time.time()
    cur.execute(
        "INSERT INTO brackets (user_id, label, picks, created_at, updated_at) VALUES (%s, %s, '{}', %s, %s) RETURNING id",
        (req.user_id, label, now, now)
    )
    bracket_id = cur.fetchone()[0]
    db.commit()
    cur.close()
    return {"id": bracket_id, "label": label}

@app.put("/api/brackets/{bracket_id}/picks")
def save_picks(bracket_id: int, req: SavePicksRequest):
    cur = get_cursor()
    cur.execute("SELECT * FROM brackets WHERE id = %s", (bracket_id,))
    bracket = fetchone_dict(cur)
    if not bracket:
        cur.close()
        raise HTTPException(status_code=404, detail="Bracket not found")
    if bracket["submitted"]:
        cur.close()
        raise HTTPException(status_code=400, detail="Bracket already submitted and locked")
    now = time.time()
    cur.execute("UPDATE brackets SET picks = %s, tiebreaker_score = %s, updated_at = %s WHERE id = %s",
               (json.dumps(req.picks), req.tiebreaker_score, now, bracket_id))
    db.commit()
    cur.close()
    return {"ok": True}

@app.post("/api/brackets/{bracket_id}/submit")
def submit_bracket(bracket_id: int, req: SubmitBracketRequest):
    cur = get_cursor()
    cur.execute("SELECT * FROM brackets WHERE id = %s", (bracket_id,))
    bracket = fetchone_dict(cur)
    if not bracket:
        cur.close()
        raise HTTPException(status_code=404, detail="Bracket not found")
    if bracket["submitted"]:
        cur.close()
        raise HTTPException(status_code=400, detail="Bracket already submitted")
    now = time.time()
    cur.execute("UPDATE brackets SET picks = %s, tiebreaker_score = %s, submitted = 1, submitted_at = %s, updated_at = %s WHERE id = %s",
               (json.dumps(req.picks), req.tiebreaker_score, now, now, bracket_id))
    db.commit()
    cur.close()
    return {"ok": True, "submitted_at": now}

@app.delete("/api/brackets/{bracket_id}")
def delete_bracket(bracket_id: int, viewer_id: int = 0):
    cur = get_cursor()
    cur.execute("SELECT * FROM brackets WHERE id = %s", (bracket_id,))
    bracket = fetchone_dict(cur)
    if not bracket:
        cur.close()
        raise HTTPException(status_code=404, detail="Bracket not found")
    if bracket["user_id"] != viewer_id:
        cur.close()
        raise HTTPException(status_code=403, detail="Can only delete your own brackets")
    if bracket["submitted"]:
        cur.close()
        raise HTTPException(status_code=400, detail="Cannot delete a submitted bracket")
    cur.execute("DELETE FROM brackets WHERE id = %s", (bracket_id,))
    db.commit()
    cur.close()
    return {"deleted": bracket_id}

# ---- Admin bracket management ----
@app.post("/api/admin/brackets/{bracket_id}/reset")
def admin_reset_bracket(bracket_id: int, viewer_id: int = 0):
    """Admin resets a submitted bracket back to draft so user can edit it."""
    cur = get_cursor()
    # verify admin
    cur.execute("SELECT is_admin FROM users WHERE id = %s", (viewer_id,))
    admin_row = fetchone_dict(cur)
    if not admin_row or not admin_row["is_admin"]:
        cur.close()
        raise HTTPException(status_code=403, detail="Admin only")
    cur.execute("SELECT * FROM brackets WHERE id = %s", (bracket_id,))
    bracket = fetchone_dict(cur)
    if not bracket:
        cur.close()
        raise HTTPException(status_code=404, detail="Bracket not found")
    cur.execute("UPDATE brackets SET submitted = 0, submitted_at = NULL, updated_at = %s WHERE id = %s",
               (time.time(), bracket_id))
    db.commit()
    cur.close()
    return {"reset": bracket_id}

@app.delete("/api/admin/brackets/{bracket_id}")
def admin_delete_bracket(bracket_id: int, viewer_id: int = 0):
    """Admin can delete any bracket, even submitted ones."""
    cur = get_cursor()
    cur.execute("SELECT is_admin FROM users WHERE id = %s", (viewer_id,))
    admin_row = fetchone_dict(cur)
    if not admin_row or not admin_row["is_admin"]:
        cur.close()
        raise HTTPException(status_code=403, detail="Admin only")
    cur.execute("SELECT * FROM brackets WHERE id = %s", (bracket_id,))
    bracket = fetchone_dict(cur)
    if not bracket:
        cur.close()
        raise HTTPException(status_code=404, detail="Bracket not found")
    cur.execute("DELETE FROM brackets WHERE id = %s", (bracket_id,))
    db.commit()
    cur.close()
    return {"deleted": bracket_id}

# ---- Bets ----
@app.get("/api/bets")
def list_bets(viewer_id: Optional[int] = None):
    """Return all bets. If viewer_id is provided and bets are not revealed yet,
    hide friend bets that are about the viewer."""
    now = time.time()
    revealed = now >= BET_REVEAL_TIMESTAMP
    
    cur = get_cursor()
    cur.execute("""
        SELECT b.*, 
            creator.display_name as creator_name,
            about.display_name as about_name,
            taker.display_name as taker_name
        FROM bets b
        JOIN users creator ON b.creator_id = creator.id
        LEFT JOIN users about ON b.about_user_id = about.id
        LEFT JOIN users taker ON b.taker_id = taker.id
        ORDER BY b.created_at DESC
    """)
    rows = fetchall_dict(cur)
    
    results = []
    for d in rows:
        if not revealed and viewer_id and d["about_user_id"] == viewer_id:
            continue
        results.append(d)

    # Count active (non-closed) bets on each user (exclude viewer's own count if not revealed)
    cur.execute("SELECT about_user_id, COUNT(*) as c FROM bets WHERE about_user_id IS NOT NULL AND closed = FALSE GROUP BY about_user_id")
    on_rows = fetchall_dict(cur)
    bets_on_count = {r["about_user_id"]: r["c"] for r in on_rows}
    # Hide the viewer's own bet count before reveal so they can't see how many bets are about them
    if not revealed and viewer_id and viewer_id in bets_on_count:
        del bets_on_count[viewer_id]
    cur.close()

    return {"bets": results, "revealed": revealed, "reveal_time": BET_REVEAL_TIMESTAMP, "bets_on_count": bets_on_count}

@app.post("/api/bets")
def create_bet(req: CreateBetRequest, viewer_id: int = 0):
    if viewer_id == 0:
        raise HTTPException(status_code=400, detail="Must be logged in")
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Bet amount must be positive")
    if not req.about_user_id:
        raise HTTPException(status_code=400, detail="Must specify a member")
    if req.about_user_id == viewer_id:
        raise HTTPException(status_code=400, detail="Cannot bet about yourself")

    cur = get_cursor()
    cur.execute("SELECT COUNT(*) as c FROM bets WHERE creator_id = %s AND closed = FALSE", (viewer_id,))
    count = cur.fetchone()[0]
    if count >= 3:
        cur.close()
        raise HTTPException(status_code=400, detail="You can only have 3 active bets")

    cur.execute("SELECT COUNT(*) as c FROM bets WHERE about_user_id = %s AND closed = FALSE", (req.about_user_id,))
    on_count = cur.fetchone()[0]
    if on_count >= 3:
        cur.close()
        raise HTTPException(status_code=400, detail="Max 3 active bets can be placed about this person")

    now = time.time()
    cur.execute(
        "INSERT INTO bets (creator_id, about_user_id, bet_type, description, amount, created_at) VALUES (%s, %s, 'friend', %s, %s, %s) RETURNING id",
        (viewer_id, req.about_user_id, req.description, req.amount, now)
    )
    bet_id = cur.fetchone()[0]
    db.commit()
    cur.close()
    return {"id": bet_id}

@app.post("/api/bets/{bet_id}/take")
def take_bet(bet_id: int, viewer_id: int = 0):
    if viewer_id == 0:
        raise HTTPException(status_code=400, detail="Must be logged in")
    cur = get_cursor()
    cur.execute("SELECT * FROM bets WHERE id = %s", (bet_id,))
    bet = fetchone_dict(cur)
    if not bet:
        cur.close()
        raise HTTPException(status_code=404, detail="Bet not found")
    if bet["taker_id"]:
        cur.close()
        raise HTTPException(status_code=400, detail="Bet already taken")
    if bet["creator_id"] == viewer_id:
        cur.close()
        raise HTTPException(status_code=400, detail="Cannot take your own bet")
    if bet["about_user_id"] and bet["about_user_id"] == viewer_id:
        cur.close()
        raise HTTPException(status_code=400, detail="Cannot take a bet about yourself")
    
    now = time.time()
    cur.execute("UPDATE bets SET taker_id = %s, taken_at = %s WHERE id = %s", (viewer_id, now, bet_id))
    db.commit()
    cur.close()
    return {"ok": True}

@app.post("/api/bets/{bet_id}/close")
def close_bet(bet_id: int, viewer_id: int = 0):
    """Creator can close/settle a bet, freeing up slots for new bets."""
    if viewer_id == 0:
        raise HTTPException(status_code=400, detail="Must be logged in")
    cur = get_cursor()
    # Check if viewer is admin
    cur.execute("SELECT is_admin FROM users WHERE id = %s", (viewer_id,))
    viewer_row = fetchone_dict(cur)
    is_admin = viewer_row and viewer_row.get("is_admin", False)

    cur.execute("SELECT * FROM bets WHERE id = %s", (bet_id,))
    bet = fetchone_dict(cur)
    if not bet:
        cur.close()
        raise HTTPException(status_code=404, detail="Bet not found")
    if bet["creator_id"] != viewer_id and not is_admin:
        cur.close()
        raise HTTPException(status_code=403, detail="Only the bet creator or admin can close a bet")
    if bet["closed"]:
        cur.close()
        raise HTTPException(status_code=400, detail="Bet is already closed")
    now = time.time()
    cur.execute("UPDATE bets SET closed = TRUE, closed_at = %s WHERE id = %s", (now, bet_id))
    db.commit()
    cur.close()
    return {"ok": True}

@app.delete("/api/bets/{bet_id}")
def delete_bet(bet_id: int, viewer_id: int = 0):
    cur = get_cursor()
    # Check if viewer is admin
    cur.execute("SELECT is_admin FROM users WHERE id = %s", (viewer_id,))
    viewer_row = fetchone_dict(cur)
    is_admin = viewer_row and viewer_row.get("is_admin", False)

    cur.execute("SELECT * FROM bets WHERE id = %s", (bet_id,))
    bet = fetchone_dict(cur)
    if not bet:
        cur.close()
        raise HTTPException(status_code=404, detail="Bet not found")
    # Admin can delete any bet; regular users can only delete their own
    if not is_admin and bet["creator_id"] != viewer_id:
        cur.close()
        raise HTTPException(status_code=403, detail="Can only delete your own bets")
    # Regular users cannot delete taken bets; admin can
    if not is_admin and bet["taker_id"]:
        cur.close()
        raise HTTPException(status_code=400, detail="Cannot delete a bet that has been taken")
    cur.execute("DELETE FROM bets WHERE id = %s", (bet_id,))
    db.commit()
    cur.close()
    return {"deleted": bet_id}

@app.get("/api/config")
def get_config():
    cur = get_cursor()
    cur.execute("SELECT key, value FROM site_settings")
    rows = fetchall_dict(cur)
    cur.close()
    settings = {r['key']: r['value'] for r in rows}
    return {
        "bet_reveal_timestamp": BET_REVEAL_TIMESTAMP,
        "entry_fee": 50,
        "max_bets_per_user": 3,
        "banner_position": int(settings.get("banner_position", "30")),
        "group_schedule": settings.get("group_schedule", "")
    }

class UpdateSettingRequest(BaseModel):
    key: str
    value: str

@app.post("/api/admin/settings")
def update_setting(req: UpdateSettingRequest):
    allowed_keys = {"banner_position", "group_schedule"}
    if req.key not in allowed_keys:
        raise HTTPException(400, "Invalid setting key")
    cur = get_cursor()
    cur.execute(
        "INSERT INTO site_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        (req.key, req.value)
    )
    db.commit()
    cur.close()
    return {"ok": True}

# ---- Stay Info ----
class SaveStayRequest(BaseModel):
    hotel_name: str = ''
    hotel_link: str = ''
    check_in: str = ''
    check_out: str = ''
    arrival: str = ''
    departure: str = ''

@app.get("/api/stays")
def list_stays():
    try:
        cur = get_cursor()
        cur.execute("""
            SELECT s.*, u.display_name, u.avatar_data
            FROM stay_info s JOIN users u ON s.user_id = u.id
            ORDER BY u.display_name
        """)
        rows = fetchall_dict(cur)
        cur.close()
        return rows
    except Exception as e:
        logger.error(f"LIST_STAYS ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/stays/{user_id}")
def save_stay(user_id: int, req: SaveStayRequest):
    try:
        now = time.time()
        cur = get_cursor()
        cur.execute("""
            INSERT INTO stay_info (user_id, hotel_name, hotel_link, check_in, check_out, arrival, departure, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                hotel_name = EXCLUDED.hotel_name,
                hotel_link = EXCLUDED.hotel_link,
                check_in = EXCLUDED.check_in,
                check_out = EXCLUDED.check_out,
                arrival = EXCLUDED.arrival,
                departure = EXCLUDED.departure,
                updated_at = EXCLUDED.updated_at
        """, (user_id, req.hotel_name.strip(), req.hotel_link.strip(), req.check_in.strip(), req.check_out.strip(), req.arrival.strip(), req.departure.strip(), now))
        db.commit()
        cur.close()
        return {"ok": True}
    except Exception as e:
        logger.error(f"SAVE_STAY ERROR: {e}")
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/group-schedule")
def get_group_schedule():
    cur = get_cursor()
    cur.execute("SELECT value FROM site_settings WHERE key = 'group_schedule'")
    row = cur.fetchone()
    cur.close()
    return {"schedule": row[0] if row else ""}

# ---- Tournament / ESPN ----
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
TOURNAMENT_DATES = [
    "20260319", "20260320",  # R1
    "20260321", "20260322",  # R2
    "20260327", "20260328",  # Sweet 16
    "20260329", "20260330",  # Elite 8
    "20260404",              # Final Four
    "20260406",              # Championship
]

ROUND_POINTS = {1: 10, 2: 20, 3: 40, 4: 80, 5: 160, 6: 320}

ROUND_NAME_MAP = {
    "1st Round": 1,
    "2nd Round": 2,
    "Sweet 16": 3,
    "Elite Eight": 4,
    "Final Four": 5,
    "National Championship": 6,
}

# Simple in-memory cache for schedule endpoint
_schedule_cache = {"data": None, "ts": 0}

def fetch_espn_scoreboard(date_str):
    """Fetch ESPN scoreboard for a given date string YYYYMMDD."""
    url = f"{ESPN_BASE}?dates={date_str}&groups=100&limit=50"
    try:
        req = UrlRequest(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.error(f"ESPN fetch error for {date_str}: {e}")
        return None

def parse_espn_games(data):
    """Parse ESPN scoreboard JSON into tournament game records."""
    if not data or "events" not in data:
        return []
    games = []
    for event in data["events"]:
        # Check if it's an NCAA tournament game
        notes = ""
        competitions = event.get("competitions", [])
        if not competitions:
            continue
        comp = competitions[0]
        for note in comp.get("notes", []):
            notes += note.get("headline", "") + " "
        if "NCAA" not in notes and "NCAA" not in event.get("name", ""):
            continue

        # Determine round
        round_num = 0
        round_name = ""
        for rn, rnum in ROUND_NAME_MAP.items():
            if rn.lower() in notes.lower():
                round_num = rnum
                round_name = rn
                break

        if round_num == 0:
            continue

        # Parse region from notes
        region = ""
        for reg in ["East", "West", "South", "Midwest"]:
            if reg.lower() in notes.lower():
                region = reg
                break

        # Parse competitors
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        def parse_comp(c):
            team = c.get("team", {})
            name = team.get("shortDisplayName", team.get("displayName", "Unknown"))
            seed = 0
            if "curatedRank" in c and "current" in c["curatedRank"]:
                seed = c["curatedRank"]["current"]
                if seed == 99:
                    seed = 0
            score = int(c.get("score", 0) or 0)
            return name, seed, score

        name1, seed1, score1 = parse_comp(competitors[0])
        name2, seed2, score2 = parse_comp(competitors[1])

        status = comp.get("status", {}).get("type", {})
        state_desc = status.get("description", "Scheduled")
        completed = status.get("completed", False)
        game_state = "final" if completed else ("in" if state_desc == "In Progress" else "pre")

        winner_name = ""
        winner_seed = 0
        if completed:
            if score1 > score2:
                winner_name, winner_seed = name1, seed1
            elif score2 > score1:
                winner_name, winner_seed = name2, seed2

        espn_id = str(event.get("id", ""))
        game_datetime = comp.get("date", "")  # Full ISO datetime e.g. "2026-03-19T16:00Z"
        game_date = game_datetime[:10] if game_datetime else ""

        # Extract status detail (e.g. "1st Half", "Halftime", "3:42 - 2nd Half")
        status_detail = comp.get("status", {}).get("type", {}).get("shortDetail", "")

        game_key = f"espn_{espn_id}"

        games.append({
            "game_key": game_key,
            "espn_game_id": espn_id,
            "round": round_num,
            "round_name": round_name,
            "region": region,
            "team1_name": name1,
            "team1_seed": seed1,
            "team1_score": score1,
            "team2_name": name2,
            "team2_seed": seed2,
            "team2_score": score2,
            "winner_name": winner_name,
            "winner_seed": winner_seed,
            "game_state": game_state,
            "game_date": game_date,
            "game_datetime": game_datetime,
            "status_detail": status_detail,
        })
    return games


@app.get("/api/tournament/results")
def get_tournament_results():
    try:
        cur = get_cursor()
        cur.execute("SELECT * FROM tournament_results ORDER BY round, region, game_key")
        rows = fetchall_dict(cur)
        cur.close()
        return {"results": rows}
    except Exception as e:
        logger.error(f"TOURNAMENT_RESULTS ERROR: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/tournament/refresh")
def refresh_tournament():
    """Fetch latest results from ESPN for all tournament dates and upsert into DB."""
    try:
        all_games = []
        for date_str in TOURNAMENT_DATES:
            data = fetch_espn_scoreboard(date_str)
            if data:
                games = parse_espn_games(data)
                all_games.extend(games)

        cur = get_cursor()
        now = time.time()
        upserted = 0
        for g in all_games:
            cur.execute("""
                INSERT INTO tournament_results
                    (game_key, espn_game_id, round, round_name, region,
                     team1_name, team1_seed, team1_score,
                     team2_name, team2_seed, team2_score,
                     winner_name, winner_seed, game_state, game_date, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (game_key) DO UPDATE SET
                    team1_score = EXCLUDED.team1_score,
                    team2_score = EXCLUDED.team2_score,
                    winner_name = EXCLUDED.winner_name,
                    winner_seed = EXCLUDED.winner_seed,
                    game_state = EXCLUDED.game_state,
                    updated_at = EXCLUDED.updated_at
            """, (
                g["game_key"], g["espn_game_id"], g["round"], g["round_name"], g["region"],
                g["team1_name"], g["team1_seed"], g["team1_score"],
                g["team2_name"], g["team2_seed"], g["team2_score"],
                g["winner_name"], g["winner_seed"], g["game_state"], g["game_date"], now,
            ))
            upserted += 1
        db.commit()
        cur.close()
        logger.info(f"Tournament refresh: {upserted} games upserted")
        return {"ok": True, "games_upserted": upserted}
    except Exception as e:
        logger.error(f"TOURNAMENT_REFRESH ERROR: {e}")
        logger.error(traceback.format_exc())
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tournament/schedule")
def get_tournament_schedule():
    """Fetch today's and upcoming games from ESPN (cached 60s)."""
    global _schedule_cache
    now = time.time()
    if _schedule_cache["data"] is not None and now - _schedule_cache["ts"] < 60:
        return _schedule_cache["data"]

    try:
        import datetime
        today = datetime.date.today()
        dates_to_check = []
        for ds in TOURNAMENT_DATES:
            d = datetime.date(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))
            if d >= today:
                dates_to_check.append(ds)
            elif d == today - datetime.timedelta(days=1):
                dates_to_check.append(ds)

        # Also always include today
        today_str = today.strftime("%Y%m%d")
        if today_str not in dates_to_check:
            dates_to_check.insert(0, today_str)

        all_games = []
        for ds in dates_to_check[:3]:  # Limit to 3 dates max
            data = fetch_espn_scoreboard(ds)
            if data:
                games = parse_espn_games(data)
                all_games.extend(games)

        result = {"games": all_games, "fetched_at": now}
        _schedule_cache = {"data": result, "ts": now}
        return result
    except Exception as e:
        logger.error(f"SCHEDULE ERROR: {e}")
        return {"games": [], "fetched_at": now, "error": str(e)}


def score_bracket(picks, all_results):
    """Score a bracket's picks against tournament results.

    picks: dict of pick keys to team strings (e.g. "East-R0-M0" -> "1 Duke")
    all_results: list of all finished tournament result rows

    Returns dict with total score and per-round breakdown.
    """
    import re
    bracket_round_to_scoring = {0: 1, 1: 2, 2: 3, 3: 4}

    total = 0
    round_scores = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
    correct_picks = []
    wrong_picks = []
    pending_picks = []

    # Precompute lookups from results
    # winners_by_round[round_num] = set of winner names
    winners_by_round = {}
    # losers_by_round[round_num] = set of team names that played but lost
    losers_by_round = {}
    for r in all_results:
        rn = r["round"]
        if rn not in winners_by_round:
            winners_by_round[rn] = set()
            losers_by_round[rn] = set()
        if r["winner_name"]:
            winners_by_round[rn].add(r["winner_name"])
            # The loser is whichever team is not the winner
            if r["team1_name"] != r["winner_name"]:
                losers_by_round[rn].add(r["team1_name"])
            if r["team2_name"] != r["winner_name"]:
                losers_by_round[rn].add(r["team2_name"])

    for key, pick_str in picks.items():
        if not pick_str:
            continue
        m = re.match(r'^(\d+)\s+(.+)$', pick_str)
        picked_name = m.group(2) if m else pick_str

        # Determine scoring round
        scoring_round = 0
        if key.startswith("FF-CHAMP"):
            scoring_round = 6
        elif key.startswith("FF-SF"):
            scoring_round = 5
        else:
            parts = re.match(r'^(.+)-R(\d+)-M(\d+)$', key)
            if parts:
                r_idx = int(parts.group(2))
                scoring_round = bracket_round_to_scoring.get(r_idx, 0)

        if scoring_round == 0:
            continue

        pts = ROUND_POINTS.get(scoring_round, 0)
        round_winners = winners_by_round.get(scoring_round, set())
        round_losers = losers_by_round.get(scoring_round, set())

        if picked_name in round_winners:
            total += pts
            round_scores[scoring_round] += pts
            correct_picks.append(key)
        elif picked_name in round_losers:
            wrong_picks.append(key)
        else:
            pending_picks.append(key)

    return {
        "total": total,
        "round_scores": round_scores,
        "correct_picks": correct_picks,
        "wrong_picks": wrong_picks,
        "pending_picks": pending_picks,
    }


@app.get("/api/leaderboard")
def get_leaderboard():
    """Score all submitted brackets and return ranked leaderboard."""
    try:
        cur = get_cursor()
        cur.execute("""
            SELECT b.id, b.user_id, b.label, b.picks, b.tiebreaker_score, u.display_name, u.avatar_data
            FROM brackets b JOIN users u ON b.user_id = u.id
            WHERE b.submitted = 1
            ORDER BY u.display_name, b.label
        """)
        brackets = fetchall_dict(cur)

        # Get all tournament results
        cur.execute("SELECT * FROM tournament_results WHERE game_state = 'final'")
        results = fetchall_dict(cur)
        cur.close()

        # Get championship combined score for tiebreaker
        champ_combined = None
        for r in results:
            if r["round"] == 6 and r["game_state"] == "final":
                champ_combined = (r["team1_score"] or 0) + (r["team2_score"] or 0)
                break

        entries = []
        for b in brackets:
            picks = json.loads(b["picks"]) if b["picks"] else {}
            scored = score_bracket(picks, results)
            tb = b["tiebreaker_score"]
            tb_diff = abs(tb - champ_combined) if (tb is not None and champ_combined is not None) else None
            entries.append({
                "bracket_id": b["id"],
                "user_id": b["user_id"],
                "display_name": b["display_name"],
                "avatar_data": b["avatar_data"],
                "label": b["label"],
                "score": scored["total"],
                "round_scores": scored["round_scores"],
                "tiebreaker_score": tb,
                "tiebreaker_diff": tb_diff,
                "correct_picks": scored["correct_picks"],
                "wrong_picks": scored["wrong_picks"],
                "pending_picks": scored["pending_picks"],
            })

        # Sort: highest score first; on tie, lowest tiebreaker_diff first
        entries.sort(key=lambda e: (-e["score"], e["tiebreaker_diff"] if e["tiebreaker_diff"] is not None else 99999))

        # Assign ranks
        for i, e in enumerate(entries):
            e["rank"] = i + 1

        return {
            "leaderboard": entries,
            "championship_combined": champ_combined,
            "games_completed": len(results),
        }
    except Exception as e:
        logger.error(f"LEADERBOARD ERROR: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# ---- Serve static files ----
STATIC_DIR = os.path.dirname(os.path.abspath(__file__))

@app.get("/")
def serve_index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"), headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})

@app.get("/{filename}")
def serve_static(filename: str):
    filepath = os.path.join(STATIC_DIR, filename)
    if os.path.isfile(filepath) and filename in ("app.js", "style.css", "index.html"):
        return FileResponse(filepath, headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})
    raise HTTPException(status_code=404, detail="Not found")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
