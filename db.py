import aiosqlite
from datetime import datetime

DB_NAME = "bot.db"


async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_seen TEXT,
            last_seen TEXT
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            used_at TEXT
        )
        """)

        await db.commit()


# user qo‘shish yoki update qilish
async def add_or_update_user(user_id: int):
    now = datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT user_id FROM users WHERE user_id=?",
            (user_id,)
        )
        row = await cur.fetchone()

        if row:
            await db.execute(
                "UPDATE users SET last_seen=? WHERE user_id=?",
                (now, user_id)
            )
        else:
            await db.execute(
                "INSERT INTO users VALUES (?, ?, ?)",
                (user_id, now, now)
            )

        # har foydalanishni log qilamiz
        await db.execute(
            "INSERT INTO usage_log (user_id, used_at) VALUES (?, ?)",
            (user_id, now)
        )

        await db.commit()


# oylik aktiv userlar
async def get_monthly_users():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("""
        SELECT COUNT(DISTINCT user_id)
        FROM usage_log
        WHERE strftime('%Y-%m', used_at) = strftime('%Y-%m', 'now')
        """)
        return (await cur.fetchone())[0]


# jami userlar
async def get_total_users():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        return (await cur.fetchone())[0]


# Jami user ids listini qaytaradi (broadcast uchun)
async def get_all_user_ids():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT user_id FROM users")
        rows = await cur.fetchall()
        return [row[0] for row in rows]
