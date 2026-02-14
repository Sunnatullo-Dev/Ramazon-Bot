import aiosqlite
import logging
import asyncio
from datetime import datetime

DB_NAME = "ramazon.db"
log = logging.getLogger(__name__)

async def init_db(initial_admins: list):
    async with aiosqlite.connect(DB_NAME) as db:
        # Users table with region and active status
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            username TEXT,
            region TEXT,
            joined_at TEXT,
            is_active INTEGER DEFAULT 1,
            last_active TEXT
        )
        """)
        
        # Admins
        await db.execute("CREATE TABLE IF NOT EXISTS admins (admin_id INTEGER PRIMARY KEY)")
        
        # Duolar
        await db.execute("""
        CREATE TABLE IF NOT EXISTS duolar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            text TEXT,
            added_by INTEGER,
            created_at TEXT
        )
        """)
        
        # Ads
        await db.execute("""
        CREATE TABLE IF NOT EXISTS ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT,
            content TEXT,
            meta TEXT,
            expires_at TEXT,
            created_at TEXT,
            sent_count INTEGER DEFAULT 0
        )
        """)
        
        # Duo Stats
        await db.execute("""
        CREATE TABLE IF NOT EXISTS duo_stats (
            name TEXT PRIMARY KEY,
            opens INTEGER DEFAULT 0,
            last_opened TEXT
        )
        """)
        
        # Meta (key-value storage)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        
        await db.commit()
        
        # Seed admins
        for a in initial_admins:
            await db.execute("INSERT OR IGNORE INTO admins (admin_id) VALUES (?)", (a,))
        await db.commit()

# --- User Management ---
async def add_user(uid, first, username=None):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_NAME) as db:
        # Check if exists to preserve joined_at
        async with db.execute("SELECT joined_at FROM users WHERE user_id = ?", (uid,)) as cursor:
            row = await cursor.fetchone()
        
        if row:
            # Update info and mark as active
            await db.execute(
                "UPDATE users SET first_name=?, username=?, is_active=1, last_active=? WHERE user_id=?",
                (first, username, now, uid)
            )
        else:
            # Insert new
            await db.execute(
                "INSERT INTO users (user_id, first_name, username, joined_at, is_active, last_active) VALUES (?, ?, ?, ?, 1, ?)",
                (uid, first, username, now, now)
            )
        await db.commit()

async def set_user_region(uid, region_slug):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET region = ? WHERE user_id = ?", (region_slug, uid))
        await db.commit()

async def set_user_inactive(uid):
    """Mark user as blocked/inactive"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (uid,))
        await db.commit()

async def get_user(uid):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as cursor:
            return await cursor.fetchone()

async def get_all_users():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM users") as cursor:
            return await cursor.fetchall()

async def get_all_user_ids():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            rows = await cursor.fetchall()
            return [r[0] for r in rows]

async def count_users():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            return (await cursor.fetchone())[0]

async def count_active_users():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE is_active=1") as cursor:
            return (await cursor.fetchone())[0]

# --- Admin Management ---
async def get_all_admins():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT admin_id FROM admins") as cursor:
            rows = await cursor.fetchall()
            return [r[0] for r in rows]

async def add_admin(uid):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO admins (admin_id) VALUES (?)", (uid,))
        await db.commit()

async def remove_admin(uid):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM admins WHERE admin_id = ?", (uid,))
        await db.commit()

async def is_admin_db(uid, cached_list=None):
    if cached_list and uid in cached_list:
        return True
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT 1 FROM admins WHERE admin_id = ?", (uid,)) as cursor:
            return await cursor.fetchone() is not None

# --- Duo Management ---
async def add_duo(title, text, added_by):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO duolar (title, text, added_by, created_at) VALUES (?, ?, ?, ?)",
                         (title, text, added_by, now))
        await db.commit()
        await db.execute("INSERT OR IGNORE INTO duo_stats (name, opens) VALUES (?, 0)", (title,))
        await db.commit()

async def list_duos():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, title, text FROM duolar ORDER BY id ASC") as cursor:
            return await cursor.fetchall()

async def delete_duo(duo_id):
    async with aiosqlite.connect(DB_NAME) as db:
        # Get title first for stats
        async with db.execute("SELECT title FROM duolar WHERE id = ?", (duo_id,)) as cursor:
            row = await cursor.fetchone()
        
        if row:
            title = row[0]
            await db.execute("DELETE FROM duolar WHERE id = ?", (duo_id,))
            await db.execute("DELETE FROM duo_stats WHERE name = ?", (title,))
            await db.commit()
            return title
    return None

async def increment_duo_stat(name):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO duo_stats (name, opens) VALUES (?, 0)", (name,))
        await db.execute("UPDATE duo_stats SET opens = opens + 1, last_opened = ? WHERE name = ?", (now, name))
        await db.commit()

async def get_top_duos(limit=5):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, opens FROM duo_stats ORDER BY opens DESC LIMIT ?", (limit,)) as cursor:
            return await cursor.fetchall()

async def get_all_duo_stats():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, opens, last_opened FROM duo_stats ORDER BY opens DESC") as cursor:
            return await cursor.fetchall()

# --- Ads Management ---
async def add_ad(kind, content, meta, expires_at):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "INSERT INTO ads (kind, content, meta, expires_at, created_at) VALUES (?, ?, ?, ?, ?)",
            (kind, content, meta, expires_at, now)
        )
        await db.commit()
        return cursor.lastrowid

async def update_ad_sent_count(ad_id, sent_count, meta=""):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE ads SET sent_count = ?, meta = ? WHERE id = ?", (sent_count, meta, ad_id))
        await db.commit()

async def get_recent_ads(limit=5):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, kind, created_at, sent_count FROM ads ORDER BY created_at DESC LIMIT ?", (limit,)) as cursor:
            return await cursor.fetchall()

# --- Meta ---
async def set_meta(key, value):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
        await db.commit()

async def get_meta(key):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM meta WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None
