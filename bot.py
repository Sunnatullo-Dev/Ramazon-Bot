# ramazon_bot_full_with_videos.py
# Started from main() below


"""
Ramazon bot (to'liq) — token .env dan olinadi.
Funktsiyalar:
- Namoz vaqtlarini islom.uz (fallback: namoz-vaqti.uz, AlAdhan)
- Har kuni Tashkent 00:00 da namozlar yangilanadi
- Ramazon boshlanishi uchun e'lon
- Duolar (inline), duo qo'shish/o'chirish (admin)
- Video qo'shish/o'chirish, video ko'rish
- Admin qo'shish/o'chirish (.env orqali token)
- Reklama (broadcast) va yuborilganlar soni saqlanadi
"""
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime
import aiohttp
import aiosqlite
from openpyxl import Workbook
import os
import json
import re
from typing import Optional, Callable, Dict, Any, Awaitable

from db import (
    init_db, add_user, get_user, get_all_users, get_all_user_ids, 
    count_users, count_active_users, add_admin, remove_admin, 
    get_all_admins, is_admin_db, add_duo, list_duos, delete_duo, 
    increment_duo_stat, get_top_duos, add_ad, update_ad_sent_count, 
    set_meta, get_meta, set_user_region
)
from ramadan_times import get_ramadan_times

from dotenv import load_dotenv
load_dotenv()  # load .env from project root

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, FSInputFile, InputMediaVideo
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

class UserActivityMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user = data.get("event_from_user")
        if user:
            # We just track activity, no heavy updates
            pass 
        return await handler(event, data)

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("ERROR: BOT_TOKEN not set in environment (.env). Please set BOT_TOKEN=...")

# Initial admin IDs (you can keep yours here; DB will be seeded with these)
INITIAL_ADMINS = [7566796449]

# DB_FILE removed, using db.py's internal DB_NAME
RAMADAN_START_DATE = os.getenv("RAMADAN_START_DATE", "2026-02-19")  # YYYY-MM-DD (19-fevral)
PRAYER_SOURCE = "islom.uz"                 # primary source
NAMOZVAQTI_BASE = "https://namoz-vaqti.uz/"
ALADHAN_BASE = "http://api.aladhan.com/v1/timingsByCity"
PRAYER_CACHE_TTL = 7 * 24 * 3600           # 7 kun
BROADCAST_DELAY = 0.05                     # sekund
CACHE_REFRESH_INTERVAL = 24 * 3600         # 24 soat
VIDEO_DATA_FILE = "videos.json"
LONG_THRESHOLD = 120                       # sekund
DEFAULT_DURATION = 8
RAMADAN_CHECK_INTERVAL = 3600              # har soatda tekshiradi

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------- BOT / DP ----------------
bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ---------------- GLOBALS ----------------
ADMINS = list(INITIAL_ADMINS)  # runtime adminlar ro'yxati

# ---------------- CONSTANTS ----------------
REGIONS = [
    ("Qoraqalpog‘iston R.", "nukus"),
    ("Toshkent sh.", "toshkent-shahri"),
    ("Toshkent vil.", "toshkent"),
    ("Andijon vil.", "andijan"),
    ("Buxoro vil.", "bukhara"),
    ("Samarqand vil.", "samarqand"),
    ("Farg‘ona vil.", "fergana"),
    ("Namangan vil.", "namangan"),
    ("Navoiy vil.", "navoiy"),
    ("Qashqadaryo vil.", "qarshi"),
    ("Surxondaryo vil.", "termez"),
    ("Sirdaryo vil.", "gulistan"),
    ("Jizzax vil.", "jizzakh"),
    ("Xorazm vil.", "urgench")
]

BUILTIN_DUOS = {
    "Saharlik duosi": "Navaytu An Asuma Sovma Shahri Ramazona Minal Fajri Ilal Mag'ribi, Xolisan Lillahi Ta'Alaa. Allohu Akbar",
    "Iftorlik duosi":"Allohumma laka Sumtu Va Bika Amantu Va A'layka Tavakkaltu Va A'laa Rizqika Aftortu,  Fag'firliy Ma Qoddamtu Va Maa Axxortu "
}
BUILTIN_DUO_MEANING = {
    "Saharlik duosi": "Ma'nosi: Ramazon Oyining Ro'zasini Tong Otganidan Kun Botgunicha Xolis Alloh Taolo Uchun Tutishni Niyat Qildim , Allohu Akbar ...",
    "Iftorlik duosi": "Ma'nosi: Allohim! Ushbu Ro'zamni Sen Uchun Tutdim ,Va Senga Iymon Keltirdim , Senga Tavakkal Qildim Va Bergan Rizqing Bilan Iftor Qildim. Mening Avvalgi Va Keyingi Gunohlaimni Mag'firat Qilgil. Vallohu A'lam..."
}

# Ramadan times moved to ramadan_times.py

# Oy nomlari (qisqa va to'liq)
MONTH_NAMES_SHORT = {
    1: "yan", 2: "fev", 3: "mar", 4: "apr", 5: "may", 6: "iyn",
    7: "iyl", 8: "avg", 9: "sen", 10: "okt", 11: "noy", 12: "dek"
}
MONTH_NAMES_FULL = {
    1: "yanvar", 2: "fevral", 3: "mart", 4: "aprel", 5: "may", 6: "iyun",
    7: "iyul", 8: "avgust", 9: "sentabr", 10: "oktabr", 11: "noyabr", 12: "dekabr"
}

def format_date_short(date_obj):
    """Format date as '19-fev' style"""
    return f"{date_obj.day}-{MONTH_NAMES_SHORT[date_obj.month]}"

def format_date_full(date_obj):
    """Format date as '19-fevral' style"""
    return f"{date_obj.day}-{MONTH_NAMES_FULL[date_obj.month]}"

_prayer_cache = {}
_prayer_cache_time = {}

# ---------------- FSM STATES ----------------
class StateDuoAdd(StatesGroup):
    waiting_title = State()
    waiting_text = State()

class StateBroadcast(StatesGroup):
    waiting_kind = State()
    waiting_content = State()
    waiting_days = State()
    waiting_confirm = State()

class StateAddVideo(StatesGroup):
    waiting_video = State()

class StateDelVideo(StatesGroup):
    waiting_pos = State()

class StateAdminAdd(StatesGroup):
    waiting_id = State()

# ---------------- VIDEO HELPERS ----------------
if not os.path.exists(VIDEO_DATA_FILE):
    with open(VIDEO_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump([], f)

def load_videos():
    with open(VIDEO_DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_videos(videos):
    with open(VIDEO_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(videos, f, indent=2, ensure_ascii=False)

def classify_kind(duration: Optional[int]) -> str:
    try:
        if duration is not None and int(duration) > LONG_THRESHOLD:
            return "long"
    except:
        pass
    return "short"

def add_video_fileid(fid: str, duration: Optional[int] = None):
    vids = load_videos()
    dur = duration if duration is not None else DEFAULT_DURATION
    kind = classify_kind(dur)
    entry = {"file_id": fid, "duration": int(dur), "kind": kind}
    vids.append(entry)
    save_videos(vids)
    return len(vids), kind

def remove_video_by_pos(pos: int):
    vids = load_videos()
    if pos < 1 or pos > len(vids):
        return False, None
    removed = vids.pop(pos - 1)
    save_videos(vids)
    return True, removed

def get_filtered(kind: str):
    return [v for v in load_videos() if v.get("kind") == kind]

# Video state
AUTO_PLAY = {}       # chat_id -> bool
AUTO_TASKS = {}      # chat_id -> asyncio.Task
CURRENT_INDEX = {}   # chat_id -> index
CURRENT_INFO = {}    # chat_id -> dict

# Message queue system - keeps max 2 messages per user
USER_MSG_QUEUE = {}  # user_id -> [msg_id1, msg_id2]
MAX_USER_MESSAGES = 2

# Debounce system - prevents duplicate callback processing
LAST_CALLBACK = {}   # user_id -> (callback_data, timestamp)
DEBOUNCE_SECONDS = 1.0  # minimum seconds between same callbacks

# Start command debounce - prevents duplicate /start processing
LAST_START = {}  # user_id -> timestamp
START_DEBOUNCE_SECONDS = 2.0  # minimum seconds between /start commands

async def send_queued_message(chat_id: int, user_id: int, text: str, **kwargs):
    """Send message and manage queue - delete oldest if more than MAX_USER_MESSAGES"""
    # Initialize queue for user if not exists
    if user_id not in USER_MSG_QUEUE:
        USER_MSG_QUEUE[user_id] = []
    
    queue = USER_MSG_QUEUE[user_id]
    
    # If queue is full, delete the oldest message
    if len(queue) >= MAX_USER_MESSAGES:
        oldest_msg_id = queue.pop(0)
        try:
            await bot.delete_message(chat_id, oldest_msg_id)
        except Exception:
            pass  # Message might already be deleted
    
    # Send new message
    sent = await bot.send_message(chat_id, text, **kwargs)
    
    # Add new message to queue
    queue.append(sent.message_id)
    
    return sent

def is_duplicate_callback(user_id: int, callback_data: str) -> bool:
    """Check if this is a duplicate callback (same callback within DEBOUNCE_SECONDS)"""
    now = datetime.now().timestamp()
    key = user_id
    
    if key in LAST_CALLBACK:
        last_data, last_time = LAST_CALLBACK[key]
        if last_data == callback_data and (now - last_time) < DEBOUNCE_SECONDS:
            return True
    
    LAST_CALLBACK[key] = (callback_data, now)
    return False

# Video navigation debounce - tracks ANY video button press, not just same button
LAST_VIDEO_NAV = {}  # user_id -> timestamp
VIDEO_NAV_DEBOUNCE = 0.4  # seconds between ANY video navigation

def is_video_nav_spam(user_id: int) -> bool:
    """Check if user is spamming video navigation buttons"""
    now = datetime.now().timestamp()
    
    if user_id in LAST_VIDEO_NAV:
        last_time = LAST_VIDEO_NAV[user_id]
        if (now - last_time) < VIDEO_NAV_DEBOUNCE:
            return True
    
    LAST_VIDEO_NAV[user_id] = now
    return False

# Database logic moved to db.py

# ---------------- PRAYER TIMES (islom.uz parser) ----------------  
LABEL_TO_KEY = {
    "Тонг": "bomdod",
    "Қуёш": "quyosh",
    "Пешин": "peshin",
    "Аср": "asr",
    "Шом": "shom",
    "Хуфтон": "xufton",
}

async def fetch_prayer_from_islom():
    url = "https://islom.uz"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; Bot/1.0)"}
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(url, timeout=12) as resp:
                if resp.status != 200:
                    log.warning("islom.uz returned status %s", resp.status)
                    return None
                text = await resp.text()
    except Exception as e:
        log.exception("islom.uz fetch failed: %s", e)
        return None

    idx = text.find("Намоз вақтлари")
    if idx == -1:
        idx = text.find("Namoz vaqtlari")
    snippet = text[idx: idx + 3500] if idx != -1 else text[:3500]
    s = re.sub(r"\s+", " ", snippet)

    result = {}
    for label in ["Тонг", "Қуёш", "Пешин", "Аср", "Шом", "Хуфтон"]:
        pos = s.find(label)
        if pos == -1:
            pos = s.find(label.capitalize())
        if pos == -1:
            continue
        start = max(0, pos - 120)
        fragment = s[start: pos + len(label) + 40]
        m = re.search(r"(\d{1,2}:\d{2}(?::\d{2})?)", fragment)
        if m:
            time_raw = m.group(1)
            hh, mm, *_ = time_raw.split(":")
            hhmm = f"{int(hh):02d}:{int(mm):02d}"
            key = LABEL_TO_KEY.get(label, label)
            result[key] = hhmm

    # fallback: pick first 6 times if not all found
    if len(result) < 6:
        times = re.findall(r"\b(\d{1,2}:\d{2})(?::\d{2})?\b", s)
        filtered = [t for t in times if t != "00:00"]
        uniq = []
        for t in filtered:
            if t not in uniq:
                uniq.append(t)
            if len(uniq) >= 6:
                break
        need = ['bomdod','quyosh','peshin','asr','shom','xufton']
        if len(uniq) >= 6:
            for i, k in enumerate(need):
                result[k] = uniq[i]

    if result.get('bomdod') and result.get('xufton'):
        return result
    return None

async def fetch_prayer_namozvaqti(region_slug: str, target_date: datetime = None):
    if target_date is None:
        target_date = datetime.now()
    key = f"{region_slug}|{target_date.strftime('%Y-%m-%d')}"
    now = datetime.now()

    # Check fixed Ramadan 2026 times (all regions via ramadan_times.py)
    date_str = target_date.strftime("%Y-%m-%d")
    ramadan_times = get_ramadan_times(region_slug, date_str)
    if ramadan_times:
        _prayer_cache[key] = ramadan_times
        _prayer_cache_time[key] = now
        return ramadan_times

    if key in _prayer_cache and (now - _prayer_cache_time.get(key, now)).total_seconds() < PRAYER_CACHE_TTL:
        return _prayer_cache[key]

    # try primary source: islom.uz (for Tashkent)
    try:
        if PRAYER_SOURCE == "islom.uz" or region_slug in (None, "", "toshkent", "toshkent-shahri"):
            parsed = await fetch_prayer_from_islom()
            if parsed:
                _prayer_cache[key] = parsed
                _prayer_cache_time[key] = now
                return parsed
    except Exception:
        log.exception("islom.uz parsing failed")

    # fallback: namoz-vaqti.uz
    try:
        month_period = target_date.strftime("%Y-%m")
        params = {"format": "json", "region": region_slug, "period": month_period}
        async with aiohttp.ClientSession() as s:
            async with s.get(NAMOZVAQTI_BASE, params=params, timeout=12) as resp:
                if resp.status == 200:
                    j = await resp.json()
                    table = j.get("period_table") or []
                    target_str = target_date.strftime("%d.%m.%Y")
                    for entry in table:
                        if entry.get("date") == target_str:
                            times = entry.get("times")
                            _prayer_cache[key] = times
                            _prayer_cache_time[key] = now
                            return times
                    today = j.get("today")
                    if today and "times" in today:
                        times = today["times"]
                        _prayer_cache[key] = times
                        _prayer_cache_time[key] = now
                        return times
    except Exception as e:
        log.exception("namoz-vaqti.uz failed: %s", e)

    # fallback: AlAdhan  
        params2 = {
            "city": region_slug or "Tashkent",
            "country": "Uzbekistan",
            "method": 2,
            "date": target_date.strftime("%d-%m-%Y")
        }
        async with aiohttp.ClientSession() as s:
            async with s.get(ALADHAN_BASE, params=params2, timeout=12) as resp:
                if resp.status == 200:
                    j = await resp.json()
                    timings = j.get("data", {}).get("timings")
                    if timings:
                        _prayer_cache[key] = timings
                        _prayer_cache_time[key] = now
                        return timings
    except Exception as e:
        log.exception("aladhan failed: %s", e)
    return None

async def refresh_prayer_cache_for_all():
    log.info("Prayer cache yangilanmoqda")
    for _, slug in REGIONS:
        await fetch_prayer_namozvaqti(slug, datetime.now())
    log.info("Prayer cache yangilandi")

# ---------------- KEYBOARDS ----------------
def build_main_inline():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Ramazon taqvimi", callback_data="menu:ramadan")],
        [InlineKeyboardButton(text="🕌 Namoz vaqtlari", callback_data="menu:prayer")],
        [InlineKeyboardButton(text="🤲 Duolar", callback_data="menu:duos")],
        [InlineKeyboardButton(text="🎥 Domlolar / Hadislar", callback_data="menu:videos")],
    ])

def build_admin_reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Statistika"), KeyboardButton(text="📢 Reklama yuborish")],
            [KeyboardButton(text="📥 Excel yuklash"), KeyboardButton(text="📁 Duo Excel")],
            [KeyboardButton(text="➕ Duo qo'shish"), KeyboardButton(text="➕ Admin qo'shish")],
            [KeyboardButton(text="🎬 Video qo'shish"), KeyboardButton(text="🗑 Video o'chirish")],
            [KeyboardButton(text="➖ Admin o'chirish")]
        ],
        resize_keyboard=True
    )

def video_kind_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📹 Qisqa videolar", callback_data="watch:short")],
        [InlineKeyboardButton(text="📼 Uzun videolar", callback_data="watch:long")],
    ])

def video_nav_kb(prev=None, next=None, autoplay=False, kind="short"):
    rows = []
    nav = []
    if prev is not None:
        nav.append(InlineKeyboardButton(text="⬅ Oldingi", callback_data=f"video:{kind}:{prev}"))
    if next is not None:
        nav.append(InlineKeyboardButton(text="➡ Keyingi", callback_data=f"video:{kind}:{next}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⏸ Avto OFF" if autoplay else "▶️ Avto ON", callback_data=f"atoggle:{kind}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------------- VIDEO SEND / EDIT ----------------
async def send_and_track(chat_id: int, file_id: str, prev_idx: Optional[int], next_idx: Optional[int], autoplay: bool, kind: str):
    sent = await bot.send_video(chat_id, video=file_id, reply_markup=video_nav_kb(prev_idx, next_idx, autoplay, kind))
    CURRENT_INFO[chat_id] = {"chat_id": chat_id, "message_id": sent.message_id, "kind": kind}
    return sent

async def edit_tracked(chat_id: int, file_id: str, prev_idx: Optional[int], next_idx: Optional[int], autoplay: bool, kind: str = "short"):
    info = CURRENT_INFO.get(chat_id)
    kb = video_nav_kb(prev_idx, next_idx, autoplay, kind)
    if not info:
        try:
            # If state is lost, we try to rebuild it
            await send_and_track(chat_id, file_id, prev_idx, next_idx, autoplay, kind)
            return True
        except:
            return False
    # Update state just in case
    info["kind"] = kind
    try:
        await bot.edit_message_media(
            chat_id=info["chat_id"],
            message_id=info["message_id"],
            media=InputMediaVideo(media=file_id),
            reply_markup=kb
        )
        return True
    except Exception:
        try:
            await send_and_track(chat_id, file_id, prev_idx, next_idx, autoplay, kind)
            return True
        except:
            return False

async def autoplay_worker(chat_id: int, start_idx: int):
    idx = start_idx
    while AUTO_PLAY.get(chat_id, False):
        info = CURRENT_INFO.get(chat_id)
        if not info:
            break
        kind = info.get("kind", "short")
        playlist = get_filtered(kind)
        if idx >= len(playlist):
            AUTO_PLAY[chat_id] = False
            break
        dur = playlist[idx].get("duration") or DEFAULT_DURATION
        await asyncio.sleep(max(1, int(dur) + 1))
        idx += 1
        if idx >= len(playlist):
            AUTO_PLAY[chat_id] = False
            break
        prev = idx - 1 if idx > 0 else None
        nxt = idx + 1 if idx + 1 < len(playlist) else None
        ok = await edit_tracked(chat_id, playlist[idx]["file_id"], prev, nxt, True, kind)
        if not ok:
            break
        CURRENT_INDEX[chat_id] = idx

# ---------------- RAMADAN ANNOUNCE ----------------
def now_tashkent_date():
    uz_now = datetime.utcnow() + timedelta(hours=5)
    return uz_now.date()

async def announce_ramadan_if_needed():
    try:
        today = now_tashkent_date()
        ramadan_start = datetime.fromisoformat(RAMADAN_START_DATE).date()
        if today < ramadan_start:
            return False
        announced = await get_meta("ramadan_announced")
        if announced == "1":
            return False
        msg = (
            "🌙 *Ramazon boshlandi!* \n\n"
            "Ramazon muborak! Bugun Ramazon boshlandi. Allohimiz ibodatlaringizni qabul qilsin. "
            "Saharlik va iftorlik vaqtlarini tekshiring va duo qiling. \n\n"
            "📌 Taqvim / Namoz vaqtlari uchun bot menyusiga qarang."
        )
        users = await get_all_users()
        sent = failed = 0
        for u in users:
            uid = u[0]
            try:
                await bot.send_message(uid, msg, parse_mode="Markdown")
                sent += 1
                await asyncio.sleep(0.02)
            except Exception:
                failed += 1
        ad_id = await add_ad("ramadan_notice", msg, f"sent:{sent},failed:{failed}", "")
        await update_ad_sent_count(ad_id, sent, f"failed:{failed}")
        await set_meta("ramadan_announced", "1")
        for adm in ADMINS:
            try:
                await bot.send_message(adm, f"Ramazon e'lon qilindi. Xabar yuborildi: {sent}, xato: {failed}")
            except:
                pass
        log.info("Ramadan announcement sent: %s sent, %s failed", sent, failed)
        return True
    except Exception as e:
        log.exception("announce_ramadan_if_needed failed: %s", e)
        return False

async def ramadan_check_loop():
    while True:
        try:
            await announce_ramadan_if_needed()
        except Exception as e:
            log.exception("ramadan_check_loop error: %s", e)
        await asyncio.sleep(RAMADAN_CHECK_INTERVAL)

# ---------------- DAILY NAMAZ UPDATE AT 00:00 TASHKENT ----------------
def seconds_until_next_tashkent_midnight():
    now_utc = datetime.utcnow()
    now_tz = now_utc + timedelta(hours=5)
    tomorrow = (now_tz + timedelta(days=1)).date()
    next_midnight_tz = datetime.combine(tomorrow, dtime.min)
    next_midnight_utc = next_midnight_tz - timedelta(hours=5)
    delta = (next_midnight_utc - now_utc).total_seconds()
    if delta < 0:
        delta += 24 * 3600
    return int(delta)

async def daily_namaz_updater_loop():
    while True:
        sec = seconds_until_next_tashkent_midnight()
        wait_time = sec + 5 # 5 seconds buffer to ensure we are past midnight
        log.info("Next namaz refresh in %s seconds (waiting %s)", sec, wait_time)
        await asyncio.sleep(wait_time)
        try:
            await refresh_prayer_cache_for_all()
            log.info("Daily namaz times refreshed at Tashkent midnight.")
            
            # Broadcast menu to all users
            user_ids = await get_all_user_ids()
            kb = build_main_inline()
            sent_count = 0
            for uid in user_ids:
                try:
                    await bot.send_message(uid, "🌙 Yangi kun muborak! Bugungi menyu:", reply_markup=kb)
                    sent_count += 1
                    await asyncio.sleep(0.05)
                except:
                    pass
            log.info("Daily menu broadcast to %s users", sent_count)
            
        except Exception as e:
            log.exception("daily_namaz_updater_loop error: %s", e)
        # Sleep for 10 minutes to prevent re-triggering approx midnight
        await asyncio.sleep(600)

# ---------------- HANDLERS ----------------
@dp.message(CommandStart())
async def cmd_start(message: Message):
    # Debounce check to prevent duplicate /start processing
    user_id = message.from_user.id
    now = datetime.now().timestamp()
    
    if user_id in LAST_START:
        last_time = LAST_START[user_id]
        if (now - last_time) < START_DEBOUNCE_SECONDS:
            return  # Ignore duplicate /start within debounce period
    
    LAST_START[user_id] = now
    
    first = message.from_user.first_name or "Do'st"
    username = message.from_user.username
    
    first = message.from_user.first_name or "Do'st"
    username = message.from_user.username
    
    # DB updates (New DB)
    await add_user(message.from_user.id, first, username)
    # await add_or_update_user(message.from_user.id) # removed old call
    
    await message.answer(f"Assalomu alaykum, {first}!\n🌙 Ramazon Muborak", reply_markup=build_main_inline())

@dp.callback_query(lambda c: c.data == "menu:ramadan")
async def cb_ramadan(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    rows = []
    for i in range(0, len(REGIONS), 2):
        row = [InlineKeyboardButton(text=REGIONS[i][0], callback_data=f"region:{i}")]
        if i + 1 < len(REGIONS):
            row.append(InlineKeyboardButton(text=REGIONS[i + 1][0], callback_data=f"region:{i+1}"))
        rows.append(row)
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await send_queued_message(c.message.chat.id, c.from_user.id, "📍 Viloyatingizni tanlang:", reply_markup=kb)

@dp.callback_query(F.data.startswith("region:"))
async def cb_region(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    try:
        idx = int(c.data.split(":", 1)[1])
    except:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Xato viloyat.")
    if idx < 0 or idx >= len(REGIONS):
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Noto'g'ri viloyat.")
    display, slug = REGIONS[idx]
    display, slug = REGIONS[idx]
    await set_user_region(c.from_user.id, slug)

    today_date = (datetime.utcnow() + timedelta(hours=5)).date()  # Toshkent vaqti
    ramadan_start = datetime.fromisoformat(RAMADAN_START_DATE).date()
    rows = []
    row = []
    for d in range(1, 31):
        # Haqiqiy sanani hisoblash
        current_date = ramadan_start + timedelta(days=d - 1)
        date_label = format_date_short(current_date)  # "19-fev" formatida
        
        # Bugungi kun bo'lsa yulduz qo'shish
        if current_date == today_date:
            label = f"🌟 {date_label}"
        else:
            label = date_label
        
        row.append(InlineKeyboardButton(text=label, callback_data=f"ramday:{idx}:{d}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await send_queued_message(c.message.chat.id, c.from_user.id, f"📍 {display}\n🌙 Ramazon taqvimi (19-fevral — 20-mart)\nKunni tanlang:", reply_markup=kb)

@dp.callback_query(F.data.startswith("ramday:"))
async def cb_ramday(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    try:
        _, idx_s, day_s = c.data.split(":")
        idx = int(idx_s)
        day = int(day_s)
    except:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Xato ma'lumot.")
    display, slug = REGIONS[idx]
    start = datetime.fromisoformat(RAMADAN_START_DATE).date()
    date = start + timedelta(days=day - 1)
    date_str = format_date_full(date)  # "19-fevral" formatida
    times = await fetch_prayer_namozvaqti(slug, date)
    fajr = times.get('bomdod') or times.get('Fajr') or '—:--' if times else '—:--'
    shom = times.get('shom') or times.get('Maghrib') or '—:--' if times else '—:--'
    fajr = fajr[:5]
    shom = shom[:5]
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"⏰ Saharlik — {fajr}", callback_data=f"time:{idx}:{day}:sahar:{date.strftime('%Y-%m-%d')}"),
                                                InlineKeyboardButton(text=f"🌇 Iftorlik — {shom}", callback_data=f"time:{idx}:{day}:iftor:{date.strftime('%Y-%m-%d')}")]])
    await send_queued_message(c.message.chat.id, c.from_user.id, f"📍 {display}\n🌙 Ramazon {day}-kun ({date_str})\n\n⏰ Saharlik: {fajr}\n🌇 Iftorlik: {shom}\n\n(vaqtlar + 2-3 daqiqa farq qilishi mumkin)\n\nDuo ko'rish uchun tanlang:", reply_markup=kb)

@dp.callback_query(F.data.startswith("time:"))
async def cb_time(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    try:
        _, idx_s, day_s, ttype, date_str = c.data.split(":")
        idx = int(idx_s)
        day = int(day_s)
    except:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Xato.")
    display, slug = REGIONS[idx]
    date = datetime.fromisoformat(date_str)
    times = await fetch_prayer_namozvaqti(slug, date)
    if not times:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Namoz vaqtlari topilmadi.")
    if ttype == 'sahar':
        key = 'Saharlik duosi'
        time_val = (times.get('bomdod') or times.get('Fajr') or '—:--')[:5]
    else:
        key = 'Iftorlik duosi'
        time_val = (times.get('shom') or times.get('Maghrib') or '—:--')[:5]
    duo = BUILTIN_DUOS.get(key, 'Duo topilmadi.')
    meaning = BUILTIN_DUO_MEANING.get(key, '')
    await send_queued_message(c.message.chat.id, c.from_user.id, f"🤲 {key} — {time_val}\n\n{duo}\n\n{meaning}")

@dp.callback_query(lambda c: c.data == "menu:prayer")
async def cb_prayer(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    u = await get_user(c.from_user.id)
    slug = u[3] if u and u[3] else 'toshkent-shahri'
    times = await fetch_prayer_namozvaqti(slug)
    if not times:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Namoz vaqtlari topilmadi.")
    order = ['bomdod', 'quyosh', 'peshin', 'asr', 'shom', 'xufton']
    labels = {'bomdod':'Bomdod','quyosh':'Quyosh','peshin':'Peshin','asr':'Asr','shom':'Shom','xufton':'Xufton'}
    lines = []
    for k in order:
        v = times.get(k) or times.get(k.capitalize()) or times.get(k.upper()) or "—:--"
        lines.append(f"{labels.get(k, k)}: {v[:5]}")
    await send_queued_message(c.message.chat.id, c.from_user.id, "🕌 Namoz vaqtlari (bugun):\n\n" + "\n".join(lines))

@dp.callback_query(lambda c: c.data == "menu:duos")
async def cb_duos(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    is_adm = await is_admin_db(c.from_user.id, ADMINS)
    db_duos = await list_duos()
    items = list(BUILTIN_DUOS.items()) + [(t, tx) for _, t, tx in db_duos]

    rows = []
    if is_adm:
        rows.append([InlineKeyboardButton(text="➕ Duo qo'shish", callback_data="duos:add"),
                     InlineKeyboardButton(text="🗑 Duo o'chirish (admin)", callback_data="duos:admin_delete")])
    for i, (title, _) in enumerate(items):
        label = title if len(title) <= 30 else title[:27] + "..."
        rows.append([InlineKeyboardButton(text=label, callback_data=f"duo_open:{i}")])
    rows.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="duos:back")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await send_queued_message(c.message.chat.id, c.from_user.id, "🤲 Duolar:", reply_markup=kb)

@dp.callback_query(F.data.startswith("duos:"))
async def cb_duos_actions(c: CallbackQuery, state: FSMContext):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    action = c.data.split(":", 1)[1]
    if action == "back":
        await send_queued_message(c.message.chat.id, c.from_user.id, "Orqaga", reply_markup=build_main_inline())
        return
    if action == "add":
        if not await is_admin_db(c.from_user.id, ADMINS):
            return await send_queued_message(c.message.chat.id, c.from_user.id, "Admin emassiz.")
        await state.set_state(StateDuoAdd.waiting_title)
        await send_queued_message(c.message.chat.id, c.from_user.id, "Duo nomini kiriting:")
        return
    if action == "admin_delete":
        if not await is_admin_db(c.from_user.id, ADMINS):
            return await send_queued_message(c.message.chat.id, c.from_user.id, "Admin emassiz.")
        db_duos = await list_duos()
        if not db_duos:
            return await send_queued_message(c.message.chat.id, c.from_user.id, "Bazada duo yo'q.")
        rows = []
        for id_, title, _ in db_duos:
            rows.append([InlineKeyboardButton(text=title[:30], callback_data=f"duo_del:{id_}")])
        rows.append([InlineKeyboardButton(text="🔙 Bekor", callback_data="duo_del:cancel")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
        await send_queued_message(c.message.chat.id, c.from_user.id, "Qaysi duoni o'chirmoqchisiz?", reply_markup=kb)
        return

@dp.callback_query(F.data.startswith("duo_open:"))
async def cb_duo_open(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    try:
        idx = int(c.data.split(":", 1)[1])
    except:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Xato.")
    db_duos = await list_duos()
    items = list(BUILTIN_DUOS.items()) + [(t, tx) for _, t, tx in db_duos]
    if idx >= len(items):
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Duo topilmadi.")
    title, text = items[idx]
    await increment_duo_stat(title)
    await send_queued_message(c.message.chat.id, c.from_user.id, f"🤲 {title}\n\n{text}")

@dp.callback_query(F.data.startswith("duo_del:"))
async def cb_duo_del(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    payload = c.data.split(":", 1)[1]
    if payload == "cancel":
        try:
            await c.message.delete_reply_markup()
        except:
            pass
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Bekor qilindi.")
    try:
        duo_id = int(payload)
    except:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Xato.")
    await delete_duo(duo_id)
    title = f"Duo #{duo_id}" # Simplified feedback
    try:
        await c.message.edit_text(f"✅ Duo '{title}' o'chirildi.")
    except:
        await send_queued_message(c.message.chat.id, c.from_user.id, f"✅ Duo '{title}' o'chirildi.")

@dp.message(StateDuoAdd.waiting_title, F.text)
async def duo_title(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        await state.clear()
        return
    await state.update_data(title=m.text.strip())
    await state.set_state(StateDuoAdd.waiting_text)
    await m.answer("Duo matnini yuboring:")

@dp.message(StateDuoAdd.waiting_text, F.text)
async def duo_text(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        await state.clear()
        return
    # Only accept text, not media
    if not m.text:
        await m.answer("Iltimos, faqat matn yuboring (rasm/video emas).")
        return
    data = await state.get_data()
    title = data.get('title') or "No title"
    data = await state.get_data()
    title = data.get('title') or "No title"
    await add_duo(title, m.text.strip(), m.from_user.id)
    await m.answer("Duo saqlandi ✅")
    await state.clear()

@dp.callback_query(lambda c: c.data == "menu:videos")
async def cb_videos_menu(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    # Clear region selection (dynamic menu requirement)
    await set_user_region(c.from_user.id, None)
    await send_queued_message(c.message.chat.id, c.from_user.id, "Domlolar va Hadislar videolari:", reply_markup=video_kind_kb())

@dp.callback_query(F.data.startswith("watch:"))
async def cb_watch(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    # Check for video navigation spam
    if is_video_nav_spam(c.from_user.id):
        return
    kind = c.data.split(":", 1)[1]
    playlist = get_filtered(kind)
    if not playlist:
        return await send_queued_message(c.message.chat.id, c.from_user.id, f"Bu turda video hali yo'q.")
    idx = 0
    CURRENT_INDEX[c.message.chat.id] = idx
    AUTO_PLAY[c.message.chat.id] = False
    file_id = playlist[idx]["file_id"]
    prev = None
    nxt = 1 if len(playlist) > 1 else None
    await send_and_track(c.message.chat.id, file_id, prev, nxt, False, kind)

@dp.callback_query(F.data.startswith("video:"))
async def cb_video_nav(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    # Check for video navigation spam - prevent rapid button clicks
    if is_video_nav_spam(c.from_user.id):
        return
    parts = c.data.split(":")
    if len(parts) < 3:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Xato ma'lumot.")
    kind = parts[1]
    try:
        idx = int(parts[2])
    except:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Index xatosi.")

    playlist = get_filtered(kind)
    if not playlist:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Video topilmadi.")

    if idx < 0 or idx >= len(playlist):
        return

    prev = idx - 1 if idx > 0 else None
    nxt = idx + 1 if idx + 1 < len(playlist) else None
    autoplay = AUTO_PLAY.get(c.message.chat.id, False)
    
    ok = await edit_tracked(c.message.chat.id, playlist[idx]["file_id"], prev, nxt, autoplay, kind)
    if ok:
        CURRENT_INDEX[c.message.chat.id] = idx

@dp.callback_query(F.data.startswith("atoggle:"))
async def cb_autoplay_toggle(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    parts = c.data.split(":")
    kind = parts[1] if len(parts) > 1 else "short"
    chat_id = c.message.chat.id
    
    current = AUTO_PLAY.get(chat_id, False)
    idx = CURRENT_INDEX.get(chat_id, 0)
    playlist = get_filtered(kind)
    
    # Update or rebuild internal state
    if chat_id not in CURRENT_INFO:
        CURRENT_INFO[chat_id] = {"chat_id": chat_id, "message_id": c.message.message_id, "kind": kind}

    if current:
        AUTO_PLAY[chat_id] = False
        task = AUTO_TASKS.pop(chat_id, None)
        if task:
            task.cancel()
        await bot.edit_message_reply_markup(
            chat_id=chat_id, message_id=c.message.message_id,
            reply_markup=video_nav_kb(idx-1 if idx>0 else None, idx+1 if idx+1<len(playlist) else None, False, kind)
        )
    else:
        AUTO_PLAY[chat_id] = True
        task = asyncio.create_task(autoplay_worker(chat_id, idx))
        AUTO_TASKS[chat_id] = task
        await bot.edit_message_reply_markup(
            chat_id=chat_id, message_id=c.message.message_id,
            reply_markup=video_nav_kb(idx-1 if idx>0 else None, idx+1 if idx+1<len(playlist) else None, True, kind)
        )

# ---------------- ADMIN HANDLERS ----------------
@dp.message(Command("admin"))
async def cmd_admin(m: Message):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return await m.reply("Admin emassiz.")
    await m.answer("🔐 Admin panel", reply_markup=build_admin_reply_kb())

@dp.message(Command("stats"))
async def cmd_stats(m: Message):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    total = await count_users()
    monthly = await count_active_users() # using active here as proxy or update later
    await m.answer(f"📊 Statistika:\n\n👥 Jami userlar: {total}\n📅 Active: {monthly}")


@dp.message(F.text == "📊 Statistika")
async def admin_stats(m: Message):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    total = await count_users()
    users = await get_all_users()
    now = datetime.now()
    try:
        last7 = sum(1 for u in users if datetime.fromisoformat(u[4]) >= now - timedelta(days=7))
        last30 = sum(1 for u in users if datetime.fromisoformat(u[4]) >= now - timedelta(days=30))
    except:
        last7 = last30 = 0
    top = await get_top_duos(5)
    top_text = "\n".join(f"{i}. {n} — {o}" for i, (n, o) in enumerate(top, 1)) or "Hali ma'lumot yo'q"
    # Ad stats from db
    # We can fetch recent ads here, but let's keep it simple for now or implement get_recent_ads in db.py if needed.
    # For now, skipping granular ad logs in text stats to save lines, or implementing query if really needed.
    ads_text = "See DB for ads details." 
    await m.answer(f"Jami: {total}\nOxirgi 7 kun: {last7}\nOxirgi 30 kun: {last30}\n\nEng ko'p ochilgan duolar:\n{top_text}")

@dp.message(F.text == "📥 Excel yuklash")
async def admin_excel_users(m: Message):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    users = await get_all_users()
    wb = Workbook()
    ws = wb.active
    ws.append(["ID", "Ism", "Username", "Viloyat", "Qo'shilgan"])
    for u in users:
        ws.append(u)
    path = "users.xlsx"
    wb.save(path)
    await m.answer_document(FSInputFile(path))
    os.remove(path)

@dp.message(F.text == "📁 Duo Excel")
async def admin_duo_excel(m: Message):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    # Temporarily disabled
    await m.answer("Excel export temporarily disabled during refactor.")

@dp.message(F.text == "➕ Duo qo'shish")
async def admin_duo_add_text_handler(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    await state.set_state(StateDuoAdd.waiting_title)
    await m.answer("Duo nomini kiriting:")

@dp.message(F.text == "📢 Reklama yuborish")
@dp.message(Command("broadcast"))
async def admin_broadcast_start(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        log.warning(f"Unauthorized broadcast attempt by {m.from_user.id}")
        await m.answer("Siz admin emassiz.")
        return
    await state.set_state(StateBroadcast.waiting_kind)
    await m.answer("Reklama turi: matn / rasm / video yozing yoki to'g'ridan media yuboring.")

@dp.message(StateBroadcast.waiting_kind)
async def broadcast_kind(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        await state.clear()
        return
    if m.photo:
        caption = m.caption or ""
        if len(caption) > 1024:
            return await m.answer("Caption juda uzun (max 1024).")
        await state.update_data(kind='photo', content=m.photo[-1].file_id, caption=caption)
        await state.set_state(StateBroadcast.waiting_days)
        await m.answer("Necha kun turishi kerak? (1-30)")
        return
    if m.video:
        caption = m.caption or ""
        if len(caption) > 1024:
            return await m.answer("Caption juda uzun (max 1024).")
        await state.update_data(kind='video', content=m.video.file_id, caption=caption)
        await state.set_state(StateBroadcast.waiting_days)
        await m.answer("Necha kun turishi kerak? (1-30)")
        return
    kind = (m.text or "").strip().lower()
    mapping = {"matn": "text", "text": "text", "rasm": "photo", "foto": "photo", "video": "video"}
    if kind not in mapping:
        return await m.answer("Faqat matn/rasm/video yozing yoki media yuboring.")
    await state.update_data(kind=mapping[kind])
    await state.set_state(StateBroadcast.waiting_content)
    await m.answer("Kontent yuboring (matn yoki media):")

@dp.message(StateBroadcast.waiting_content)
async def broadcast_content(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        await state.clear()
        return
    data = await state.get_data()
    kind = data.get("kind")
    if data.get("content"):
        await state.set_state(StateBroadcast.waiting_days)
        await m.answer("Necha kun turishi kerak? (1-30)")
        return
    if kind == "text":
        if not m.text:
            return await m.answer("Matn yuboring.")
        await state.update_data(content=m.text)
    elif kind == "photo":
        if m.photo:
            caption = m.caption or ""
            if len(caption) > 1024:
                return await m.answer("Caption juda uzun (max 1024).")
            await state.update_data(content=m.photo[-1].file_id, caption=caption)
        else:
            return await m.answer("Rasm yuboring.")
    elif kind == "video":
        if m.video:
            caption = m.caption or ""
            if len(caption) > 1024:
                return await m.answer("Caption juda uzun (max 1024).")
            await state.update_data(content=m.video.file_id, caption=caption)
        else:
            return await m.answer("Video yuboring.")
    await state.set_state(StateBroadcast.waiting_days)
    await m.answer("Necha kun turishi kerak? (1-30)")

@dp.message(StateBroadcast.waiting_days)
async def broadcast_days(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        await state.clear()
        return
    try:
        days = int(m.text.strip())
        if not 1 <= days <= 30:
            raise ValueError
    except:
        return await m.answer("1-30 oralig'ida son kiriting.")
    data = await state.get_data()
    expires = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    data = await state.get_data()
    expires = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    ad_id = await add_ad(data["kind"], data["content"], "", expires)
    await state.update_data(ad_id=ad_id)
    await state.set_state(StateBroadcast.waiting_confirm)
    await m.answer("Reklama saqlandi. Yuborilsinmi? (ha / yo'q)")

@dp.message(StateBroadcast.waiting_confirm)
async def broadcast_confirm(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        await state.clear()
        return
    if m.text.strip().lower() not in ("ha", "yes", "y"):
        await m.answer("Reklama saqlandi, lekin yuborilmadi.")
        await state.clear()
        return
    data = await state.get_data()
    # Use optimized ID fetch
    user_ids = await get_all_user_ids()
    log.info(f"Broadcast boshlandi. Jami userlar: {len(user_ids)}")
    await m.answer(f"Broadcast boshlandi. Jami userlar: {len(user_ids)}")
    
    sent = failed = 0
    
    caption = data.get("caption", "")
    content = data["content"]
    kind = data["kind"]
    
    for uid in user_ids:
        try:
            if kind == "text":
                await bot.send_message(uid, content, parse_mode="HTML")
            elif kind == "photo":
                if caption:
                    await bot.send_photo(uid, photo=content, caption=caption, parse_mode="HTML")
                else:
                    await bot.send_photo(uid, photo=content, parse_mode="HTML")
            elif kind == "video":
                if caption:
                    await bot.send_video(uid, video=content, caption=caption, parse_mode="HTML")
                else:
                    await bot.send_video(uid, video=content, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            log.error(f"Broadcast error for {uid}: {e}")
            print("Broadcast error:", uid, e)
    
    log.info(f"Broadcast tugadi. Sent: {sent}, Failed: {failed}")
    ad_id = data.get("ad_id")
    meta = f"sent:{sent},failed:{failed}"
    if ad_id:
        await update_ad_sent_count(ad_id, sent, meta)
    await m.answer(f"Reklama yuborildi: {sent} ta, xato: {failed} ta")
    await state.clear()

# Video admin handlers (add/remove implemented earlier)
@dp.message(F.text == "🎬 Video qo'shish")
async def video_add_start(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    await state.set_state(StateAddVideo.waiting_video)
    await m.answer("Video yuboring (qisqa yoki uzun bo'lishi avto aniqlanadi)")

@dp.message(StateAddVideo.waiting_video, F.video)
async def video_add(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        await state.clear()
        return
    fid = m.video.file_id
    dur = m.video.duration
    pos, kind = add_video_fileid(fid, dur)
    await m.answer(f"✅ Video saqlandi\nPozitsiya: {pos}\nTuri: {kind}\nDavomiylik: {dur or '?'} soniya")
    await state.clear()

@dp.message(F.video)
async def video_direct_add(m: Message):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    dur = m.video.duration
    pos, kind = add_video_fileid(m.video.file_id, dur)
    await m.answer(f"✅ Avto saqlandi\nPozitsiya: {pos}\nTuri: {kind}")

@dp.message(F.text == "🗑 Video o'chirish")
async def video_del_start(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    vids = load_videos()
    if not vids:
        return await m.answer("Video yo'q.")
    rows = []
    for i, v in enumerate(vids):
        rows.append([InlineKeyboardButton(text=f"{i+1}. {v['kind']} ({v.get('duration','?')}s)", callback_data=f"delvid:{i+1}")])
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data="delvid:cancel")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await m.answer("Qaysi videoni o'chirmoqchisiz?", reply_markup=kb)

@dp.callback_query(F.data.startswith("delvid:"))
async def video_del_callback(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    payload = c.data.split(":", 1)[1]
    if payload == "cancel":
        try:
            await c.message.delete_reply_markup()
        except:
            pass
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Bekor qilindi.")
    try:
        pos = int(payload)
    except:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Xato raqam.")
    ok, rem = remove_video_by_pos(pos)
    if ok:
        try:
            await c.message.edit_text(f"🗑 #{pos} o'chirildi ({rem.get('kind')})")
        except:
            await send_queued_message(c.message.chat.id, c.from_user.id, f"🗑 #{pos} o'chirildi ({rem.get('kind')})")
    else:
        await send_queued_message(c.message.chat.id, c.from_user.id, "Bunday raqam yo'q.")

# ---------------- ADMIN ADD / REMOVE ----------------
@dp.message(F.text == "➕ Admin qo'shish")
async def admin_add_start(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    await state.set_state(StateAdminAdd.waiting_id)
    await m.answer("Yangi adminning Telegram ID sini yuboring (raqam):")

@dp.message(StateAdminAdd.waiting_id)
async def admin_add_receive(m: Message, state: FSMContext):
    if not await is_admin_db(m.from_user.id, ADMINS):
        await state.clear()
        return
    text = (m.text or "").strip()
    if not text.isdigit():
        await m.answer("Iltimos faqat raqam (user ID) yuboring.")
        return
    new_id = int(text)
    await add_admin(new_id)
    await m.answer(f"✅ {new_id} adminlar ro'yxatiga qo'shildi.")
    if new_id not in ADMINS:
        ADMINS.append(new_id)
    try:
        await bot.send_message(new_id, "Siz endi bot adminisiz.")
    except:
        pass
    await state.clear()

@dp.message(F.text == "➖ Admin o'chirish")
async def admin_remove_start(m: Message):
    if not await is_admin_db(m.from_user.id, ADMINS):
        return
    admins = await get_all_admins()
    if not admins:
        return await m.answer("Adminlar ro'yxatida hech kim yo'q.")
    rows = []
    for a in admins:
        rows.append([InlineKeyboardButton(text=str(a), callback_data=f"admin_del:{a}")])
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data="admin_del:cancel")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await m.answer("Qaysi adminni o'chirmoqchisiz? (ID tanlang)", reply_markup=kb)

@dp.callback_query(F.data.startswith("admin_del:"))
async def admin_del_callback(c: CallbackQuery):
    await c.answer()
    if is_duplicate_callback(c.from_user.id, c.data):
        return
    payload = c.data.split(":", 1)[1]
    if payload == "cancel":
        try:
            await c.message.delete_reply_markup()
        except:
            pass
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Bekor qilindi.")
    try:
        aid = int(payload)
    except:
        return await send_queued_message(c.message.chat.id, c.from_user.id, "Xato ID.")
    await remove_admin(aid)
    if aid in ADMINS:
        ADMINS.remove(aid)
    try:
        await c.message.edit_text(f"✅ Admin {aid} o'chirildi.")
    except:
        await send_queued_message(c.message.chat.id, c.from_user.id, f"✅ Admin {aid} o'chirildi.")

# ---------------- STARTUP ----------------
async def periodic_cache():
    while True:
        await refresh_prayer_cache_for_all()
        await asyncio.sleep(CACHE_REFRESH_INTERVAL)

async def on_startup():
    await init_db(INITIAL_ADMINS)
    
    dp.message.middleware(UserActivityMiddleware())
    dp.callback_query.middleware(UserActivityMiddleware())
    
    # Reload admins
    admin_ids = await get_all_admins()
    global ADMINS
    ADMINS = admin_ids if admin_ids else list(INITIAL_ADMINS)
    
    log.info("DB tayyor, admins: %s", ADMINS)
    await refresh_prayer_cache_for_all()
    asyncio.create_task(periodic_cache())
    asyncio.create_task(ramadan_check_loop())
    asyncio.create_task(daily_namaz_updater_loop())

async def main():
    await on_startup()
    log.info("Bot ishga tushdi")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot to'xtatildi")
