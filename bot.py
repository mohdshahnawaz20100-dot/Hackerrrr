import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Clonix is running!")

def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(
        target=server.serve_forever,
        daemon=True
    ).start()
    """
CLONIX — safe Telegram utility bot
---------------------------------
Runs as a Telegram BOT account only. It does not request users' phone numbers,
OTP codes, 2FA passwords, or personal-account session strings.

Required environment variables:
  API_ID, API_HASH, BOT_TOKEN, OWNER_ID

Optional:
  AUTO_DELETE_SECONDS (default 5)
  WEATHER_API_KEY
  GROQ_API_KEY

Install:
  pip install -r requirements.txt
Run:
  python bot.py
"""

import os
import re
import ast
import math
import random
import asyncio
import logging
import operator
import sqlite3
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

from telethon import TelegramClient, events, functions
from telethon.tl.types import ChatBannedRights
from telethon.errors import FloodWaitError, RPCError

try:
    import aiohttp
except ImportError:
    aiohttp = None

# ----------------------------- CONFIG ---------------------------------
def env_int(name: str, default: int = 0) -> int:
    raw = os.getenv(name, "").strip()
    return int(raw) if raw else default

API_ID = env_int("API_ID")
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = env_int("OWNER_ID")
AUTO_DELETE_SECONDS = max(1, min(env_int("AUTO_DELETE_SECONDS", 5), 60))
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()

if API_ID <= 0 or not API_HASH or not BOT_TOKEN or OWNER_ID <= 0:
    raise RuntimeError(
        "Missing environment variables. Set API_ID, API_HASH, BOT_TOKEN and OWNER_ID."
    )

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("clonix")

bot = TelegramClient("clonix_bot_session", API_ID, API_HASH)
_started_at = datetime.now(timezone.utc)
_seen_updates = set()
_seen_lock = asyncio.Lock()
_notes = {}          # in-memory personal notes; reset on restart
_reminders = {}      # in-memory reminder tasks


DATA_DIR = Path(os.getenv("DATA_DIR", ".")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "clonix_data.sqlite3"


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS access_users (
                user_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL CHECK(status IN ('pending', 'approved', 'rejected')),
                requested_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS blocked_users (
                user_id INTEGER PRIMARY KEY,
                blocked_at TEXT NOT NULL,
                blocked_by INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS command_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                display_name TEXT,
                command TEXT NOT NULL,
                chat_id INTEGER,
                chat_title TEXT,
                chat_type TEXT,
                role TEXT,
                outcome TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)


def access_status(user_id: int) -> str:
    if user_id == OWNER_ID:
        return "approved"
    with db_connect() as conn:
        row = conn.execute(
            "SELECT status FROM access_users WHERE user_id = ?", (user_id,)
        ).fetchone()
    return row[0] if row else "none"


def set_access(user_id: int, status: str):
    now = datetime.now(timezone.utc).isoformat()
    with db_connect() as conn:
        conn.execute("""
            INSERT INTO access_users(user_id, status, requested_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET status=excluded.status,
                updated_at=excluded.updated_at
        """, (user_id, status, now, now))


async def require_access(event) -> bool:
    if event.sender_id == OWNER_ID:
        return True
    with db_connect() as conn:
        blocked = conn.execute(
            "SELECT 1 FROM blocked_users WHERE user_id = ?", (event.sender_id,)
        ).fetchone()
    if blocked:
        await safe_reply(event, "⛔ Owner ne tumhara bot access block kiya hai.")
        return False
    if access_status(event.sender_id) == "approved":
        return True
    await safe_reply(
        event,
        "🔒 **Access pending / not approved**\n"
        "Pehle `/request_access` bhejo. Owner approve karega tabhi commands chalengi."
    )
    return False


def display_name(entity) -> str:
    first = getattr(entity, "first_name", "") or ""
    last = getattr(entity, "last_name", "") or ""
    return (first + " " + last).strip() or "Telegram user"


# ----------------------------- HELPERS --------------------------------
async def claim_update(event) -> bool:
    """Best-effort duplicate-update guard for one running process."""
    key = (event.chat_id, event.id)
    async with _seen_lock:
        if key in _seen_updates:
            return False
        _seen_updates.add(key)
        if len(_seen_updates) > 5000:
            _seen_updates.clear()
            _seen_updates.add(key)
    return True


async def safe_reply(event, message: str, **kwargs):
    try:
        return await event.reply(message, **kwargs)
    except FloodWaitError as exc:
        log.warning("Telegram flood wait: %s seconds", exc.seconds)
        return None
    except RPCError as exc:
        log.warning("Telegram API error: %s", exc)
        return None


async def delete_later(message, seconds: int):
    await asyncio.sleep(max(1, min(seconds, 60)))
    try:
        await message.delete()
    except RPCError:
        pass


async def send_auto_delete(event, text: str, seconds: Optional[int] = None):
    msg = await safe_reply(event, text)
    if msg:
        asyncio.create_task(delete_later(msg, seconds or AUTO_DELETE_SECONDS))
    return msg


def args_of(event) -> str:
    text = event.raw_text or ""
    parts = text.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def is_owner(event) -> bool:
    return event.sender_id == OWNER_ID



async def record_command(event, command_name: str, outcome: str = "attempt"):
    """Record command metadata only; never save message text or arguments."""
    try:
        sender = await event.get_sender()
        username = getattr(sender, "username", None)
        display = display_name(sender)
        chat = await event.get_chat()
        chat_title = (
            getattr(chat, "title", None)
            or getattr(chat, "username", None)
            or display_name(chat)
            or ("Private chat" if event.is_private else "Unknown chat")
        )
        if event.is_private:
            chat_type = "private"
        elif getattr(chat, "broadcast", False):
            chat_type = "channel"
        else:
            chat_type = "group"
        role = "private"
        if not event.is_private:
            try:
                permissions = await bot.get_permissions(event.chat_id, event.sender_id)
                role = "owner" if event.sender_id == OWNER_ID else (
                    "admin" if getattr(permissions, "is_admin", False)
                    or getattr(permissions, "is_creator", False) else "member"
                )
            except Exception:
                role = "unknown"
        with db_connect() as conn:
            conn.execute("""
                INSERT INTO command_audit
                (user_id, username, display_name, command, chat_id, chat_title,
                 chat_type, role, outcome, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.sender_id, username, display, command_name,
                event.chat_id, str(chat_title)[:150], chat_type, role,
                outcome, datetime.now(timezone.utc).isoformat()
            ))
    except Exception:
        log.exception("Could not write command audit record")


async def require_group_moderation_rights(event):
    if event.is_private:
        await safe_reply(event, "Ye moderation command group/supergroup mein use karo.")
        return False
    try:
        perms = await bot.get_permissions(event.chat_id, "me")
        if not (getattr(perms, "is_admin", False) or getattr(perms, "is_creator", False)):
            await safe_reply(event, "❌ Bot ko is group mein admin banao.")
            return False
        rights = getattr(perms, "admin_rights", None)
        if rights is not None and not getattr(rights, "ban_users", False):
            await safe_reply(event, "❌ Bot ko Ban Users / Restrict Members permission do.")
            return False
        return True
    except Exception:
        await safe_reply(event, "❌ Bot ke admin permissions verify nahi ho sake.")
        return False


async def resolve_target_user(event):
    reply = await event.get_reply_message()
    if reply and reply.sender_id:
        return reply.sender_id
    raw = args_of(event).split(maxsplit=1)
    if raw and raw[0].lstrip("-").isdigit():
        return int(raw[0])
    return None


def command(pattern: str):
    """Register a dot-command once with common error handling."""
    def decorator(func):
        @bot.on(events.NewMessage(pattern=rf"^\.{re.escape(pattern)}(?:\s+.*)?$", incoming=True))
        async def wrapper(event):
            if not await claim_update(event):
                return
            await record_command(event, f".{pattern}", "attempt")
            if not await require_access(event):
                await record_command(event, f".{pattern}", "denied")
                return
            try:
                await func(event)
                await record_command(event, f".{pattern}", "handled")
            except FloodWaitError as exc:
                await record_command(event, f".{pattern}", "flood_wait")
                await safe_reply(event, f"⏳ Telegram rate limit: {exc.seconds}s. Try later.")
            except Exception:
                await record_command(event, f".{pattern}", "error")
                log.exception("Command .%s failed", pattern)
                await safe_reply(event, f"❌ .{pattern} failed. Check server logs.")
        return wrapper
    return decorator



# ----------------------------- OWNER APPROVAL --------------------------
@bot.on(events.NewMessage(pattern=r"^/request_access(?:@\w+)?$", incoming=True))
async def request_access_handler(event):
    if not await claim_update(event):
        return
    uid = event.sender_id
    status = access_status(uid)
    if status == "approved":
        await safe_reply(event, "✅ Tum already approved ho. `/help` use karo.")
        return
    if status == "pending":
        await safe_reply(event, "⌛ Tumhari request pehle se pending hai.")
        return
    set_access(uid, "pending")
    sender = await event.get_sender()
    name = display_name(sender)
    username = getattr(sender, "username", None)
    try:
        await bot.send_message(
            OWNER_ID,
            f"🔔 **New access request**\nName: {name}\n"
            f"Username: @{username or 'none'}\nUser ID: `{uid}`\n\n"
            f"Approve: `/approve {uid}`\nReject: `/reject {uid}`"
        )
    except RPCError:
        log.exception("Could not notify owner about access request")
    await safe_reply(event, "📨 Request owner ko bhej di gayi hai. Approval ka wait karo.")


@bot.on(events.NewMessage(pattern=r"^/pending(?:@\w+)?$", incoming=True))
async def pending_handler(event):
    if not await claim_update(event):
        return
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT user_id, requested_at FROM access_users WHERE status='pending' ORDER BY requested_at"
        ).fetchall()
    if not rows:
        await safe_reply(event, "📭 No pending access requests.")
        return
    await safe_reply(event, "⌛ **Pending requests**\n" + "\n".join(
        f"• `{uid}` — `/approve {uid}` | `/reject {uid}`" for uid, _ in rows
    ))


async def owner_access_action(event, status: str):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    raw = args_of(event)
    if not raw.isdigit():
        await safe_reply(event, f"Usage: `/{status} USER_ID`")
        return
    uid = int(raw)
    if uid == OWNER_ID:
        await safe_reply(event, "Owner access cannot be changed.")
        return
    set_access(uid, status)
    label = {"approved": "approved ✅", "rejected": "rejected / revoked 🔒"}[status]
    await safe_reply(event, f"User `{uid}` access {label}.")
    try:
        await bot.send_message(
            uid,
            "✅ Owner ne tumhara access approve kar diya. `/help` use karo."
            if status == "approved"
            else "🔒 Tumhara bot access ab available nahi hai."
        )
    except RPCError:
        pass


@bot.on(events.NewMessage(pattern=r"^/approve(?:@\w+)?(?:\s+\d+)?$", incoming=True))
async def approve_handler(event):
    if not await claim_update(event):
        return
    await owner_access_action(event, "approved")


@bot.on(events.NewMessage(pattern=r"^/reject(?:@\w+)?(?:\s+\d+)?$", incoming=True))
async def reject_handler(event):
    if not await claim_update(event):
        return
    await owner_access_action(event, "rejected")


@bot.on(events.NewMessage(pattern=r"^/revoke(?:@\w+)?(?:\s+\d+)?$", incoming=True))
async def revoke_handler(event):
    if not await claim_update(event):
        return
    await owner_access_action(event, "rejected")


@bot.on(events.NewMessage(pattern=r"^/approved(?:@\w+)?$", incoming=True))
async def approved_handler(event):
    if not await claim_update(event):
        return
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT user_id FROM access_users WHERE status='approved' ORDER BY user_id"
        ).fetchall()
    await safe_reply(event, "✅ **Approved users**\n" + (
        "\n".join(f"• `{row[0]}`" for row in rows) if rows else "No approved users yet."
    ))


# ----------------------------- SAFE CALCULATOR -------------------------
_ALLOWED_BINOPS = {
    ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
    ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod, ast.Pow: operator.pow,
}
_ALLOWED_UNARY = {ast.UAdd: operator.pos, ast.USub: operator.neg}


def calculate(expression: str):
    if len(expression) > 120:
        raise ValueError("Expression too long.")
    tree = ast.parse(expression, mode="eval")

    def walk(node):
        if isinstance(node, ast.Expression):
            return walk(node.body)
        if isinstance(node, ast.Constant) and type(node.value) in (int, float):
            return node.value
        if isinstance(node, ast.BinOp) and type(node.op) in _ALLOWED_BINOPS:
            left, right = walk(node.left), walk(node.right)
            if isinstance(node.op, ast.Pow) and abs(right) > 10:
                raise ValueError("Exponent too large.")
            result = _ALLOWED_BINOPS[type(node.op)](left, right)
            if not math.isfinite(float(result)) or abs(result) > 10**100:
                raise ValueError("Result out of range.")
            return result
        if isinstance(node, ast.UnaryOp) and type(node.op) in _ALLOWED_UNARY:
            return _ALLOWED_UNARY[type(node.op)](walk(node.operand))
        raise ValueError("Only numbers and + - * / // % ** brackets are allowed.")

    return walk(tree)


# ----------------------------- BOT COMMANDS ----------------------------
@bot.on(events.NewMessage(pattern=r"^/start(?:@\w+)?$", incoming=True))
async def start_handler(event):
    if not await claim_update(event):
        return
    if access_status(event.sender_id) != "approved":
        await safe_reply(event, "🔒 Access ke liye `/request_access` bhejo.")
        return
    await safe_reply(
        event,
        "⚡ **CLONIX BOT**\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Welcome! This is a bot-account utility service.\n\n"
        "Use `/help` to see commands.\n"
        "Privacy: this bot never asks for your Telegram OTP, 2FA password "
        "or personal-account session string."
    )


@bot.on(events.NewMessage(pattern=r"^/help(?:@\w+)?$", incoming=True))
async def help_handler(event):
    if not await claim_update(event):
        return
    if not await require_access(event):
        await safe_reply(event, "🔒 Commands use karne ke liye `/request_access` bhejo.")
        return
    await safe_reply(
        event,
        "**⚡ CLONIX — COMMANDS**\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "**Basic**\n"
        "`/start` `/help` `/status`\n"
        "`.alive` `.ping` `.id` `.info` `.profile`\n\n"
        "**Utilities**\n"
        "`.calc 12*(4+2)` `.reverse hello` `.flip` `.dice`\n"
        "`.dadjoke` `.text your text` `.banner your text`\n"
        "`.weather Lucknow` (needs WEATHER_API_KEY)\n\n"
        "**Messages & personal tools**\n"
        "`.autodel 5` `.vanish 10` (reply to a message)\n"
        "`.note add title | text` `.note list` `.note get title`\n"
        "`.remind 10m your reminder` `.reminders` `.cancelreminder ID`\n\n"
        "**Owner**\n"
        "`/health` `/broadcast your message`\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Commands that spam chats, raid users, secretly access DMs, "
        "or collect personal-account login codes are intentionally not included."
    )


@bot.on(events.NewMessage(pattern=r"^/status(?:@\w+)?$", incoming=True))
async def status_handler(event):
    if not await claim_update(event):
        return
    if not await require_access(event):
        return
    me = await bot.get_me()
    uptime = datetime.now(timezone.utc) - _started_at
    await safe_reply(
        event,
        f"✅ **Online**\nBot: @{me.username or 'N/A'}\n"
        f"Uptime: {str(uptime).split('.')[0]}\n"
        f"Auto-delete default: {AUTO_DELETE_SECONDS}s"
    )


@bot.on(events.NewMessage(pattern=r"^/health(?:@\w+)?$", incoming=True))
async def health_handler(event):
    if not await claim_update(event):
        return
    if not is_owner(event):
        return
    await safe_reply(event, "💚 Process is running; Telegram connection is active.")


@bot.on(events.NewMessage(pattern=r"^/broadcast(?:@\w+)?(?:\s+[\s\S]+)?$", incoming=True))
async def broadcast_handler(event):
    if not await claim_update(event):
        return
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    text = args_of(event)
    if not text:
        await safe_reply(event, "Usage: `/broadcast your announcement`")
        return
    # Broadcast only to chats the bot has already interacted with is not
    # implemented without an explicit, consent-based recipient registry.
    await safe_reply(event, "Broadcast is disabled until you add an opt-in subscriber list.")


@command("alive")
async def alive(event):
    await safe_reply(event, "⚡ **CLONIX is alive!**")


@command("ping")
async def ping(event):
    started = datetime.now(timezone.utc)
    msg = await safe_reply(event, "🏓 Pong!")
    if msg:
        ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000
        try:
            await msg.edit(f"🏓 **Pong!** `{ms:.0f} ms`")
        except RPCError:
            pass


@command("id")
async def id_cmd(event):
    reply = await event.get_reply_message()
    target = reply.sender_id if reply else event.sender_id
    await safe_reply(event, f"🆔 User ID: `{target}`\nChat ID: `{event.chat_id}`")


@command("info")
async def info_cmd(event):
    reply = await event.get_reply_message()
    target = reply.sender_id if reply else event.sender_id
    entity = await bot.get_entity(target)
    first = getattr(entity, "first_name", "") or ""
    last = getattr(entity, "last_name", "") or ""
    username = getattr(entity, "username", None)
    await safe_reply(
        event,
        f"👤 **{(first + ' ' + last).strip() or 'Unknown'}**\n"
        f"Username: @{username or 'none'}\nID: `{target}`"
    )


@command("profile")
async def profile_cmd(event):
    reply = await event.get_reply_message()
    target = reply.sender_id if reply else event.sender_id
    entity = await bot.get_entity(target)
    first = getattr(entity, "first_name", "") or ""
    last = getattr(entity, "last_name", "") or ""
    username = getattr(entity, "username", None)
    await safe_reply(
        event,
        f"👤 **{(first + ' ' + last).strip() or 'Unknown'}**\\n"
        f"Username: @{username or 'none'}\\nID: `{target}`"
    )


@command("calc")
async def calc_cmd(event):
    expression = args_of(event)
    if not expression:
        await safe_reply(event, "Usage: `.calc 12*(4+2)`")
        return
    try:
        result = calculate(expression)
        await safe_reply(event, f"🧮 `{expression} = {result}`")
    except Exception as exc:
        await safe_reply(event, f"❌ {exc}")


@command("reverse")
async def reverse_cmd(event):
    value = args_of(event)
    if not value:
        await safe_reply(event, "Usage: `.reverse hello`")
        return
    await safe_reply(event, value[::-1])


@command("flip")
async def flip_cmd(event):
    await safe_reply(event, random.choice(["🪙 Heads", "🪙 Tails"]))


@command("dice")
async def dice_cmd(event):
    await safe_reply(event, f"🎲 You rolled **{random.randint(1, 6)}**")


@command("dadjoke")
async def dadjoke_cmd(event):
    jokes = [
        "Why did the computer get cold? It left its Windows open.",
        "I only know 25 letters of the alphabet. I don't know y.",
        "Why was the math book sad? It had too many problems.",
    ]
    await safe_reply(event, random.choice(jokes))


@command("text")
async def text_cmd(event):
    value = args_of(event)
    if not value:
        await safe_reply(event, "Usage: `.text your text`")
        return
    await safe_reply(event, value[:3500])


@command("banner")
async def banner_cmd(event):
    value = args_of(event)
    if not value:
        await safe_reply(event, "Usage: `.banner your text`")
        return
    value = value[:80]
    line = "━" * min(max(len(value), 8), 28)
    await safe_reply(event, f"╭{line}╮\n┃ {value} ┃\n╰{line}╯")


@command("autodel")
async def autodel_cmd(event):
    raw = args_of(event)
    if not raw.isdigit():
        await safe_reply(event, "Usage: `.autodel 5` (1–60 seconds)")
        return
    seconds = int(raw)
    if not 1 <= seconds <= 60:
        await safe_reply(event, "Choose 1–60 seconds.")
        return
    await send_auto_delete(event, f"🗑️ This reply will be deleted in {seconds}s.", seconds)


@command("vanish")
async def vanish_cmd(event):
    raw = args_of(event)
    if not raw.isdigit() or not 1 <= int(raw) <= 60:
        await safe_reply(event, "Reply to your own message and use `.vanish 10` (1–60 seconds).")
        return
    reply = await event.get_reply_message()
    if not reply:
        await safe_reply(event, "Please reply to the message you want to delete.")
        return
    if reply.sender_id != event.sender_id:
        await safe_reply(event, "For safety, vanish can delete only your own message.")
        return
    seconds = int(raw)
    await safe_reply(event, f"⏳ Your replied message will be deleted in {seconds}s.")
    asyncio.create_task(delete_later(reply, seconds))


@command("note")
async def note_cmd(event):
    raw = args_of(event)
    uid = event.sender_id
    user_notes = _notes.setdefault(uid, {})
    if raw == "list":
        if not user_notes:
            await safe_reply(event, "No notes saved.")
        else:
            await safe_reply(event, "📝 **Your notes**\n" + "\n".join(f"• {k}" for k in user_notes))
        return
    if raw.startswith("add ") and "|" in raw:
        title, body = raw[4:].split("|", 1)
        title = title.strip()[:60]
        if not title or not body.strip():
            await safe_reply(event, "Usage: `.note add title | note text`")
            return
        user_notes[title] = body.strip()[:3000]
        await safe_reply(event, f"✅ Saved note: **{title}**")
        return
    if raw.startswith("get "):
        title = raw[4:].strip()
        await safe_reply(event, user_notes.get(title, "Note not found."))
        return
    if raw.startswith("delete "):
        title = raw[7:].strip()
        existed = user_notes.pop(title, None)
        await safe_reply(event, "🗑️ Deleted." if existed else "Note not found.")
        return
    await safe_reply(event, "`.note add title | text` · `.note list` · `.note get title` · `.note delete title`")


def parse_duration(value: str) -> int:
    match = re.fullmatch(r"(\d+)(s|m|h|d)", value.strip().lower())
    if not match:
        raise ValueError("Use 30s, 10m, 2h or 1d.")
    amount = int(match.group(1))
    unit = {"s": 1, "m": 60, "h": 3600, "d": 86400}[match.group(2)]
    seconds = amount * unit
    if not 5 <= seconds <= 7 * 86400:
        raise ValueError("Reminder must be between 5 seconds and 7 days.")
    return seconds


@command("remind")
async def remind_cmd(event):
    raw = args_of(event)
    parts = raw.split(maxsplit=1)
    if len(parts) != 2:
        await safe_reply(event, "Usage: `.remind 10m drink water`")
        return
    try:
        delay = parse_duration(parts[0])
    except ValueError as exc:
        await safe_reply(event, f"❌ {exc}")
        return
    reminder_id = f"{event.sender_id}-{random.randint(1000, 9999)}"
    async def fire():
        await asyncio.sleep(delay)
        try:
            await bot.send_message(event.chat_id, f"⏰ **Reminder**\n{parts[1]}")
        finally:
            _reminders.pop(reminder_id, None)
    task = asyncio.create_task(fire())
    _reminders[reminder_id] = (event.sender_id, task, parts[1])
    await safe_reply(event, f"✅ Reminder set. ID: `{reminder_id}`")


@command("reminders")
async def reminders_cmd(event):
    rows = [
        (rid, item[2]) for rid, item in _reminders.items()
        if item[0] == event.sender_id
    ]
    if not rows:
        await safe_reply(event, "No active reminders.")
        return
    await safe_reply(event, "⏰ **Active reminders**\n" + "\n".join(f"`{rid}` — {label}" for rid, label in rows))


@command("cancelreminder")
async def cancelreminder_cmd(event):
    rid = args_of(event)
    item = _reminders.get(rid)
    if not item or item[0] != event.sender_id:
        await safe_reply(event, "Reminder not found.")
        return
    item[1].cancel()
    _reminders.pop(rid, None)
    await safe_reply(event, "✅ Reminder cancelled.")


@command("weather")
async def weather_cmd(event):
    city = args_of(event)
    if not city:
        await safe_reply(event, "Usage: `.weather Lucknow`")
        return
    if not WEATHER_API_KEY:
        await safe_reply(event, "⚠️ WEATHER_API_KEY is not configured on the host.")
        return
    if aiohttp is None:
        await safe_reply(event, "⚠️ Install aiohttp: `pip install aiohttp`")
        return
    url = "https://api.openweathermap.org/data/2.5/weather"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params={"q": city, "appid": WEATHER_API_KEY, "units": "metric"}, timeout=15) as resp:
            data = await resp.json()
    if resp.status != 200:
        await safe_reply(event, f"❌ Weather lookup failed: {data.get('message', 'unknown error')}")
        return
    weather = data["weather"][0]["description"].title()
    await safe_reply(
        event,
        f"🌤️ **{data['name']}**\n"
        f"🌡️ {data['main']['temp']}°C (feels like {data['main']['feels_like']}°C)\n"
        f"☁️ {weather}\n💧 Humidity: {data['main']['humidity']}%"
    )




# ----------------------------- SECURITY AUDIT & BLOCKLIST ---------------
@command("audit")
async def audit_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    raw = args_of(event)
    limit = int(raw) if raw.isdigit() else 20
    limit = max(1, min(limit, 50))
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT user_id, username, command, chat_title, role, outcome, created_at
            FROM command_audit ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
    if not rows:
        await safe_reply(event, "📭 Abhi koi command audit log nahi hai.")
        return
    lines = []
    for uid, username, cmd, chat, role, outcome, created in rows:
        stamp = created.replace("T", " ")[:19]
        lines.append(
            f"• `{cmd}` | {('@' + username) if username else uid} "
            f"| {chat or 'Unknown'} | {role or 'unknown'} | {outcome} | {stamp}"
        )
    await safe_reply(event, "🛡️ **Recent command activity**\n" + "\n".join(lines))


@command("blockuser")
async def blockuser_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    raw = args_of(event).strip()
    if not raw.lstrip("-").isdigit():
        await safe_reply(event, "Usage: `.blockuser USER_ID`")
        return
    uid = int(raw)
    if uid == OWNER_ID:
        await safe_reply(event, "Owner ko block nahi kar sakte.")
        return
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO blocked_users(user_id, blocked_at, blocked_by) VALUES (?, ?, ?)",
            (uid, datetime.now(timezone.utc).isoformat(), OWNER_ID)
        )
    await safe_reply(event, f"🔒 User `{uid}` blocked from using bot commands.")


@command("unblockuser")
async def unblockuser_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    raw = args_of(event).strip()
    if not raw.lstrip("-").isdigit():
        await safe_reply(event, "Usage: `.unblockuser USER_ID`")
        return
    uid = int(raw)
    with db_connect() as conn:
        cur = conn.execute("DELETE FROM blocked_users WHERE user_id = ?", (uid,))
    await safe_reply(event, "✅ User unblocked." if cur.rowcount else "User block list mein nahi mila.")


@command("blocked")
async def blocked_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    with db_connect() as conn:
        rows = conn.execute("SELECT user_id FROM blocked_users ORDER BY blocked_at DESC").fetchall()
    await safe_reply(event, "🔒 **Blocked users**\n" + (
        "\n".join(f"• `{row[0]}`" for row in rows) if rows else "No blocked users."
    ))


# ----------------------------- GROUP MODERATION ------------------------
@command("ban")
async def ban_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    if not await require_group_moderation_rights(event):
        return
    uid = await resolve_target_user(event)
    if not uid or uid == OWNER_ID:
        await safe_reply(event, "Target member ke message par reply karke `.ban` use karo.")
        return
    try:
        await bot(functions.channels.EditBannedRequest(
            channel=event.chat_id,
            participant=uid,
            banned_rights=ChatBannedRights(until_date=None, view_messages=True)
        ))
        await safe_reply(event, f"🔨 User `{uid}` banned from this group.")
    except Exception as exc:
        await safe_reply(event, f"❌ Ban failed: {type(exc).__name__}. Check bot admin rights.")


@command("unban")
async def unban_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    if not await require_group_moderation_rights(event):
        return
    uid = await resolve_target_user(event)
    if not uid:
        await safe_reply(event, "Usage: `.unban USER_ID`")
        return
    try:
        await bot(functions.channels.EditBannedRequest(
            channel=event.chat_id,
            participant=uid,
            banned_rights=ChatBannedRights(until_date=None)
        ))
        await safe_reply(event, f"✅ User `{uid}` unbanned.")
    except Exception as exc:
        await safe_reply(event, f"❌ Unban failed: {type(exc).__name__}.")


@command("mute")
async def mute_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    if not await require_group_moderation_rights(event):
        return
    reply = await event.get_reply_message()
    uid = reply.sender_id if reply else None
    if not uid or uid == OWNER_ID:
        await safe_reply(event, "Member ke message par reply karke `.mute 10m` use karo.")
        return
    raw = args_of(event).strip() or "10m"
    match = re.fullmatch(r"(\d+)(m|h|d)", raw.lower())
    if not match:
        await safe_reply(event, "Duration: `.mute 10m`, `.mute 2h`, `.mute 1d`")
        return
    amount = int(match.group(1))
    seconds = amount * {"m": 60, "h": 3600, "d": 86400}[match.group(2)]
    if not 1 <= seconds <= 30 * 86400:
        await safe_reply(event, "Mute duration 1 minute se 30 days tak rakho.")
        return
    try:
        await bot(functions.channels.EditBannedRequest(
            channel=event.chat_id,
            participant=uid,
            banned_rights=ChatBannedRights(
                until_date=datetime.now(timezone.utc) + timedelta(seconds=seconds),
                send_messages=True, send_media=True, send_stickers=True,
                send_gifs=True, send_games=True, send_inline=True, embed_links=True
            )
        ))
        await safe_reply(event, f"🔇 User `{uid}` muted for {raw}.")
    except Exception as exc:
        await safe_reply(event, f"❌ Mute failed: {type(exc).__name__}.")


@command("unmute")
async def unmute_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    if not await require_group_moderation_rights(event):
        return
    reply = await event.get_reply_message()
    uid = reply.sender_id if reply else None
    if not uid:
        await safe_reply(event, "Muted member ke message par reply karke `.unmute` use karo.")
        return
    try:
        await bot(functions.channels.EditBannedRequest(
            channel=event.chat_id,
            participant=uid,
            banned_rights=ChatBannedRights(until_date=None)
        ))
        await safe_reply(event, f"🔊 User `{uid}` unmuted.")
    except Exception as exc:
        await safe_reply(event, f"❌ Unmute failed: {type(exc).__name__}.")


# ----------------------------- BACKUP & EXTRA TOOLS --------------------
async def send_owner_file(event, path: Path, caption: str):
    try:
        await bot.send_file(OWNER_ID, str(path), caption=caption)
        await safe_reply(event, "✅ Backup owner ke private chat mein bhej diya.")
    except RPCError:
        log.exception("Backup delivery failed")
        await safe_reply(event, "❌ Backup send nahi hua. Server logs check karo.")


@command("backup")
async def backup_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    source = Path(__file__).resolve()
    with tempfile.TemporaryDirectory() as temp_dir:
        archive = Path(temp_dir) / "clonix_source_backup.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(source, "bot.py")
            for filename in ("requirements.txt", "README_CLONIX.txt"):
                candidate = source.parent / filename
                if candidate.is_file():
                    zf.write(candidate, filename)
        await send_owner_file(event, archive, "🗂️ CLONIX source backup (secrets excluded)")


@command("dbbackup")
async def dbbackup_cmd(event):
    if not is_owner(event):
        await safe_reply(event, "⛔ Owner only.")
        return
    with tempfile.TemporaryDirectory() as temp_dir:
        target = Path(temp_dir) / "clonix_database_backup.sqlite3"
        src_conn = sqlite3.connect(DB_PATH)
        dst_conn = sqlite3.connect(target)
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
            src_conn.close()
        await send_owner_file(event, target, "🗄️ CLONIX approval database backup")


@command("uptime")
async def uptime_cmd(event):
    elapsed = datetime.now(timezone.utc) - _started_at
    await safe_reply(event, f"⏱️ Uptime: `{str(elapsed).split('.')[0]}`")


@command("time")
async def time_cmd(event):
    now = datetime.now().astimezone()
    await safe_reply(event, f"🕒 `{now.strftime('%d %b %Y, %I:%M:%S %p %Z')}`")


@command("wordcount")
async def wordcount_cmd(event):
    value = args_of(event)
    if not value:
        reply = await event.get_reply_message()
        value = reply.raw_text if reply else ""
    if not value:
        await safe_reply(event, "Usage: `.wordcount your text` or reply to a text.")
        return
    words = len(value.split())
    await safe_reply(event, f"📝 Words: `{words}` | Characters: `{len(value)}`")


@command("choose")
async def choose_cmd(event):
    options = [item.strip() for item in args_of(event).split("|") if item.strip()]
    if len(options) < 2:
        await safe_reply(event, "Usage: `.choose tea | coffee | water`")
        return
    await safe_reply(event, "🎯 I choose: **" + random.choice(options)[:250] + "**")


@command("quote")
async def quote_cmd(event):
    quotes = [
        "Small steps every day lead to big changes.",
        "Consistency is more powerful than motivation.",
        "Patience and effort make difficult things possible.",
        "Learn from yesterday, focus on today, prepare for tomorrow.",
    ]
    await safe_reply(event, "✨ " + random.choice(quotes))


# ----------------------------- SAFE FALLBACKS ---------------------------
@command("spray")
async def spray_disabled(event):
    await safe_reply(event, "🚫 `.spray` is disabled because automated mass messaging can spam or harass people.")


@command("dspray")
async def dspray_disabled(event):
    await safe_reply(event, "🚫 `.dspray` is disabled because automated mass messaging can spam or harass people.")


@command("raid")
async def raid_disabled(event):
    await safe_reply(event, "🚫 Raid automation is disabled. Use moderation tools instead.")


# ----------------------------- STARTUP ---------------------------------
async def main():
    init_db()
    log.info("Starting CLONIX bot...")
    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()
    log.info("Connected as @%s (bot id=%s)", me.username, me.id)
    await bot.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by operator.")
