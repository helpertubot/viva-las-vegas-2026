#!/usr/bin/env python3
"""Friends Bracket Pool — FastAPI backend with SQLite."""
import json
import os
import time
import base64
import hashlib
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import sqlite3

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

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pool.db")

def get_db():
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db

def init_db(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL DEFAULT '',
            display_name TEXT NOT NULL,
            avatar_data TEXT DEFAULT '',
            is_admin INTEGER DEFAULT 0,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS brackets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            label TEXT NOT NULL DEFAULT 'Bracket 1',
            picks TEXT NOT NULL DEFAULT '{}',
            submitted INTEGER DEFAULT 0,
            submitted_at REAL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS site_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            creator_id INTEGER NOT NULL,
            about_user_id INTEGER,
            bet_type TEXT NOT NULL DEFAULT 'friend',
            description TEXT NOT NULL,
            amount REAL NOT NULL,
            taker_id INTEGER,
            taken_at REAL,
            created_at REAL NOT NULL,
            FOREIGN KEY (creator_id) REFERENCES users(id),
            FOREIGN KEY (about_user_id) REFERENCES users(id),
            FOREIGN KEY (taker_id) REFERENCES users(id)
        );
    """)
    db.commit()

def migrate_db(db):
    """Migrate existing brackets table: remove UNIQUE on user_id, add label column."""
    # Check if label column already exists
    cols = [row[1] for row in db.execute("PRAGMA table_info(brackets)").fetchall()]
    if "label" in cols:
        return  # Already migrated

    db.executescript("""
        CREATE TABLE IF NOT EXISTS brackets_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            label TEXT NOT NULL DEFAULT 'Bracket 1',
            picks TEXT NOT NULL DEFAULT '{}',
            submitted INTEGER DEFAULT 0,
            submitted_at REAL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        INSERT INTO brackets_new (id, user_id, label, picks, submitted, submitted_at, created_at, updated_at)
            SELECT id, user_id, 'Bracket 1', picks, submitted, submitted_at, created_at, updated_at FROM brackets;
        DROP TABLE brackets;
        ALTER TABLE brackets_new RENAME TO brackets;
    """)
    db.commit()

db = get_db()
init_db(db)
migrate_db(db)

# Bet reveal time: Saturday March 21, 2026 at 12:00 PM PDT (UTC-7) = 19:00 UTC
BET_REVEAL_TIMESTAMP = 1774314000  # March 21 2026 12:00 PM PDT

@asynccontextmanager
async def lifespan(app):
    yield
    db.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---- Models ----
class LoginRequest(BaseModel):
    username: str
    password: str

class SavePicksRequest(BaseModel):
    picks: dict
    
class SubmitBracketRequest(BaseModel):
    picks: dict

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

# ---- Auth (username + password) ----
@app.post("/api/login")
def login(req: LoginRequest):
    username = req.username.strip().lower()
    row = db.execute("SELECT * FROM users WHERE LOWER(username) = ?", [username]).fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    user = dict(row)
    if not verify_password(req.password, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    # Don't send password_hash to client
    user.pop("password_hash", None)
    return {"user": user}

# ---- Users ----
@app.get("/api/users")
def list_users():
    rows = db.execute("SELECT id, email, username, display_name, avatar_data, is_admin FROM users ORDER BY display_name").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/admin/reset-password/{user_id}")
def reset_password(user_id: int, req: dict):
    new_password = req.get("password", "")
    if not new_password:
        raise HTTPException(status_code=400, detail="Password is required")
    pw_hash = hash_password(new_password)
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", [pw_hash, user_id])
    db.commit()
    return {"ok": True}

@app.post("/api/admin/users")
def add_user(req: AddUserRequest):
    email = req.email.strip().lower()
    username = req.username.strip().lower()
    if not username or not req.password:
        raise HTTPException(status_code=400, detail="Username and password are required")
    existing = db.execute("SELECT id FROM users WHERE LOWER(email) = ?", [email]).fetchone()
    if existing:
        raise HTTPException(status_code=409, detail="Email already exists")
    existing_un = db.execute("SELECT id FROM users WHERE LOWER(username) = ?", [username]).fetchone()
    if existing_un:
        raise HTTPException(status_code=409, detail="Username already taken")
    now = time.time()
    pw_hash = hash_password(req.password)
    cur = db.execute("INSERT INTO users (email, username, password_hash, display_name, is_admin, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                     [email, username, pw_hash, req.display_name, req.is_admin, now])
    db.commit()
    user_id = cur.lastrowid
    return {"id": user_id, "email": email, "username": username, "display_name": req.display_name}

@app.delete("/api/admin/users/{user_id}")
def remove_user(user_id: int):
    db.execute("DELETE FROM bets WHERE creator_id = ? OR about_user_id = ? OR taker_id = ?", [user_id, user_id, user_id])
    db.execute("DELETE FROM brackets WHERE user_id = ?", [user_id])
    db.execute("DELETE FROM users WHERE id = ?", [user_id])
    db.commit()
    return {"deleted": user_id}

@app.post("/api/admin/avatar/{user_id}")
async def upload_avatar(user_id: int, file: UploadFile = File(...)):
    data = await file.read()
    if len(data) > 2_000_000:
        raise HTTPException(status_code=400, detail="Image too large (max 2MB)")
    b64 = base64.b64encode(data).decode()
    mime = file.content_type or "image/jpeg"
    data_url = f"data:{mime};base64,{b64}"
    db.execute("UPDATE users SET avatar_data = ? WHERE id = ?", [data_url, user_id])
    db.commit()
    return {"ok": True}

# ---- Brackets ----
@app.get("/api/brackets")
def list_brackets():
    rows = db.execute("""
        SELECT b.id, b.user_id, b.label, b.picks, b.submitted, b.submitted_at, u.display_name, u.avatar_data
        FROM brackets b JOIN users u ON b.user_id = u.id
        ORDER BY u.display_name, b.label
    """).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        d["picks"] = json.loads(d["picks"]) if d["picks"] else {}
        d["pick_count"] = len(d["picks"])
        results.append(d)
    return results

@app.get("/api/brackets/{user_id}")
def get_user_brackets(user_id: int):
    rows = db.execute("""
        SELECT b.*, u.display_name, u.avatar_data
        FROM brackets b JOIN users u ON b.user_id = u.id
        WHERE b.user_id = ?
        ORDER BY b.label
    """, [user_id]).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        d["picks"] = json.loads(d["picks"]) if d["picks"] else {}
        results.append(d)
    return results

@app.post("/api/brackets")
def create_bracket(req: CreateBracketRequest):
    # Count existing brackets for this user to auto-label
    count = db.execute("SELECT COUNT(*) as c FROM brackets WHERE user_id = ?", [req.user_id]).fetchone()["c"]
    label = f"Bracket {count + 1}"
    now = time.time()
    cur = db.execute(
        "INSERT INTO brackets (user_id, label, picks, created_at, updated_at) VALUES (?, ?, '{}', ?, ?)",
        [req.user_id, label, now, now]
    )
    db.commit()
    return {"id": cur.lastrowid, "label": label}

@app.put("/api/brackets/{bracket_id}/picks")
def save_picks(bracket_id: int, req: SavePicksRequest):
    bracket = db.execute("SELECT * FROM brackets WHERE id = ?", [bracket_id]).fetchone()
    if not bracket:
        raise HTTPException(status_code=404, detail="Bracket not found")
    if bracket["submitted"]:
        raise HTTPException(status_code=400, detail="Bracket already submitted and locked")
    now = time.time()
    db.execute("UPDATE brackets SET picks = ?, updated_at = ? WHERE id = ?",
               [json.dumps(req.picks), now, bracket_id])
    db.commit()
    return {"ok": True}

@app.post("/api/brackets/{bracket_id}/submit")
def submit_bracket(bracket_id: int, req: SubmitBracketRequest):
    bracket = db.execute("SELECT * FROM brackets WHERE id = ?", [bracket_id]).fetchone()
    if not bracket:
        raise HTTPException(status_code=404, detail="Bracket not found")
    if bracket["submitted"]:
        raise HTTPException(status_code=400, detail="Bracket already submitted")
    now = time.time()
    db.execute("UPDATE brackets SET picks = ?, submitted = 1, submitted_at = ?, updated_at = ? WHERE id = ?",
               [json.dumps(req.picks), now, now, bracket_id])
    db.commit()
    return {"ok": True, "submitted_at": now}

@app.delete("/api/brackets/{bracket_id}")
def delete_bracket(bracket_id: int, viewer_id: int = 0):
    bracket = db.execute("SELECT * FROM brackets WHERE id = ?", [bracket_id]).fetchone()
    if not bracket:
        raise HTTPException(status_code=404, detail="Bracket not found")
    if bracket["user_id"] != viewer_id:
        raise HTTPException(status_code=403, detail="Can only delete your own brackets")
    if bracket["submitted"]:
        raise HTTPException(status_code=400, detail="Cannot delete a submitted bracket")
    db.execute("DELETE FROM brackets WHERE id = ?", [bracket_id])
    db.commit()
    return {"deleted": bracket_id}

# ---- Bets ----
@app.get("/api/bets")
def list_bets(viewer_id: Optional[int] = None):
    """Return all bets. If viewer_id is provided and bets are not revealed yet,
    hide friend bets that are about the viewer."""
    now = time.time()
    revealed = now >= BET_REVEAL_TIMESTAMP
    
    rows = db.execute("""
        SELECT b.*, 
            creator.display_name as creator_name,
            about.display_name as about_name,
            taker.display_name as taker_name
        FROM bets b
        JOIN users creator ON b.creator_id = creator.id
        LEFT JOIN users about ON b.about_user_id = about.id
        LEFT JOIN users taker ON b.taker_id = taker.id
        ORDER BY b.created_at DESC
    """).fetchall()
    
    results = []
    for r in rows:
        d = dict(r)
        # If not revealed and this is a friend bet about the viewer, hide it
        if not revealed and viewer_id and d["about_user_id"] == viewer_id:
            continue
        results.append(d)

    # Count bets on each user
    on_rows = db.execute("SELECT about_user_id, COUNT(*) as c FROM bets WHERE about_user_id IS NOT NULL GROUP BY about_user_id").fetchall()
    bets_on_count = {r["about_user_id"]: r["c"] for r in on_rows}

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

    # Check bet count for this user (max 3 created)
    count = db.execute("SELECT COUNT(*) as c FROM bets WHERE creator_id = ?", [viewer_id]).fetchone()["c"]
    if count >= 3:
        raise HTTPException(status_code=400, detail="You can only create 3 bets")

    # Check max 3 bets ON any single person
    on_count = db.execute("SELECT COUNT(*) as c FROM bets WHERE about_user_id = ?", [req.about_user_id]).fetchone()["c"]
    if on_count >= 3:
        raise HTTPException(status_code=400, detail="Max 3 bets can be placed about this person")

    now = time.time()
    cur = db.execute(
        "INSERT INTO bets (creator_id, about_user_id, bet_type, description, amount, created_at) VALUES (?, ?, 'friend', ?, ?, ?)",
        [viewer_id, req.about_user_id, req.description, req.amount, now]
    )
    db.commit()
    return {"id": cur.lastrowid}

@app.post("/api/bets/{bet_id}/take")
def take_bet(bet_id: int, viewer_id: int = 0):
    if viewer_id == 0:
        raise HTTPException(status_code=400, detail="Must be logged in")
    bet = db.execute("SELECT * FROM bets WHERE id = ?", [bet_id]).fetchone()
    if not bet:
        raise HTTPException(status_code=404, detail="Bet not found")
    if bet["taker_id"]:
        raise HTTPException(status_code=400, detail="Bet already taken")
    if bet["creator_id"] == viewer_id:
        raise HTTPException(status_code=400, detail="Cannot take your own bet")
    # For friend bets, you can't take a bet about yourself
    if bet["about_user_id"] and bet["about_user_id"] == viewer_id:
        raise HTTPException(status_code=400, detail="Cannot take a bet about yourself")
    
    now = time.time()
    db.execute("UPDATE bets SET taker_id = ?, taken_at = ? WHERE id = ?", [viewer_id, now, bet_id])
    db.commit()
    return {"ok": True}

@app.delete("/api/bets/{bet_id}")
def delete_bet(bet_id: int, viewer_id: int = 0):
    bet = db.execute("SELECT * FROM bets WHERE id = ?", [bet_id]).fetchone()
    if not bet:
        raise HTTPException(status_code=404, detail="Bet not found")
    if bet["creator_id"] != viewer_id:
        raise HTTPException(status_code=403, detail="Can only delete your own bets")
    if bet["taker_id"]:
        raise HTTPException(status_code=400, detail="Cannot delete a bet that has been taken")
    db.execute("DELETE FROM bets WHERE id = ?", [bet_id])
    db.commit()
    return {"deleted": bet_id}

@app.get("/api/config")
def get_config():
    # Load site settings
    rows = db.execute("SELECT key, value FROM site_settings").fetchall()
    settings = {r['key']: r['value'] for r in rows}
    return {
        "bet_reveal_timestamp": BET_REVEAL_TIMESTAMP,
        "entry_fee": 50,
        "max_bets_per_user": 3,
        "banner_position": int(settings.get("banner_position", "30"))
    }

class UpdateSettingRequest(BaseModel):
    key: str
    value: str

@app.post("/api/admin/settings")
def update_setting(req: UpdateSettingRequest):
    allowed_keys = {"banner_position"}
    if req.key not in allowed_keys:
        raise HTTPException(400, "Invalid setting key")
    db.execute("INSERT OR REPLACE INTO site_settings (key, value) VALUES (?, ?)", (req.key, req.value))
    db.commit()
    return {"ok": True}

# ---- Serve static files ----
STATIC_DIR = os.path.dirname(os.path.abspath(__file__))

@app.get("/")
def serve_index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

@app.get("/{filename}")
def serve_static(filename: str):
    filepath = os.path.join(STATIC_DIR, filename)
    if os.path.isfile(filepath) and filename in ("app.js", "style.css", "index.html"):
        return FileResponse(filepath)
    raise HTTPException(status_code=404, detail="Not found")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
