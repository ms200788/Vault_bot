#!/usr/bin/env python3
# bot_full.py
# Full-featured VaultBot with Postgres (asyncpg) persistence and Telegram-channel JSON backups.
# Adapted from your original SQLite implementation; stores data in Postgres (Neon-ready).
# Requires: aiogram, asyncpg, aiohttp, apscheduler

import os
import logging
import asyncio
import json
import tempfile
import secrets
import string
import shutil
import io
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import asyncpg

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, BotCommand
from aiogram.dispatcher.handler import CancelHandler
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.callback_data import CallbackData
from aiogram.utils import executor

from aiogram.utils.exceptions import (
    BotBlocked,
    ChatNotFound,
    RetryAfter,
    BadRequest,
    MessageToDeleteNotFound,
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

import aiohttp
from aiohttp import web

# --------------- Configuration (from env) ----------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID") or 0)
UPLOAD_CHANNEL_ID = int(os.environ.get("UPLOAD_CHANNEL_ID") or 0)
UPLOAD_CHANNEL_ID2 = int(os.environ.get("UPLOAD_CHANNEL_ID2") or 0)
DB_CHANNEL_ID = int(os.environ.get("DB_CHANNEL_ID") or 0)
DB_CHANNEL_ID2 = int(os.environ.get("DB_CHANNEL_ID2") or 0)
DB_PATH = os.environ.get("DB_PATH", "/app/data/database.sqlite3")  # kept for migrate tool if needed
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/app/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", os.environ.get("RENDER_INTERNAL_PORT", "10000") or "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))
AUTO_BACKUP_HOURS = int(os.environ.get("AUTO_BACKUP_HOURS", "6"))
DEBOUNCE_BACKUP_MINUTES = int(os.environ.get("DEBOUNCE_BACKUP_MINUTES", "5"))
MAX_BACKUPS = int(os.environ.get("MAX_BACKUPS", "10"))

NEON_DB_URL = os.environ.get("NEON_DB_URL")
NEON_MAX_BACKUPS = int(os.environ.get("NEON_MAX_BACKUPS", str(MAX_BACKUPS)))

# checks
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if OWNER_ID == 0:
    raise RuntimeError("OWNER_ID is required")
if UPLOAD_CHANNEL_ID == 0:
    raise RuntimeError("UPLOAD_CHANNEL_ID is required")
if DB_CHANNEL_ID == 0:
    raise RuntimeError("DB_CHANNEL_ID is required")
if not NEON_DB_URL:
    raise RuntimeError("NEON_DB_URL (Postgres DSN) is required")

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("vaultbot")

# ---------------- Bot & Dispatcher ----------------
bot = Bot(token=BOT_TOKEN, parse_mode=None)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ---------------- Scheduler ----------------
def _ensure_dir_for_path(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

_ensure_dir_for_path(JOB_DB_PATH)

jobstores = {
    'default': SQLAlchemyJobStore(url=f"sqlite:///{JOB_DB_PATH}")
}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.configure(timezone="UTC")

# ---------------- Callback datas ----------------
cb_choose_protect = CallbackData("protect", "session", "choice")
cb_retry = CallbackData("retry", "session")
cb_help_button = CallbackData("helpbtn", "action")

# ---------------- Postgres schema (strings) ----------------
SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGINT PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        last_seen TIMESTAMPTZ
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id BIGSERIAL PRIMARY KEY,
        owner_id BIGINT,
        created_at TIMESTAMPTZ,
        protect INTEGER DEFAULT 0,
        auto_delete_minutes INTEGER DEFAULT 0,
        title TEXT,
        revoked INTEGER DEFAULT 0,
        header_msg_id BIGINT,
        header_chat_id BIGINT,
        deep_link TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS files (
        id BIGSERIAL PRIMARY KEY,
        session_id BIGINT,
        file_type TEXT,
        file_id TEXT,
        caption TEXT,
        original_msg_id BIGINT,
        vault_msg_id BIGINT,
        FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS delete_jobs (
        id BIGSERIAL PRIMARY KEY,
        session_id BIGINT,
        target_chat_id BIGINT,
        message_ids JSONB,
        run_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ,
        status TEXT DEFAULT 'scheduled'
    );
    """,
    # neon mirrors
    """
    CREATE TABLE IF NOT EXISTS sqlite_backups (
        id BIGSERIAL PRIMARY KEY,
        filename TEXT,
        data bytea,
        created_at timestamptz DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS neon_kv (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at timestamptz DEFAULT now()
    );
    """,
]

# ---------------- Postgres pool ----------------
pg_pool: Optional[asyncpg.pool.Pool] = None

async def init_pg_pool(dsn: str):
    global pg_pool
    if pg_pool:
        return pg_pool
    # strip problematic params (channel_binding) if present
    if "channel_binding" in dsn:
        parts = dsn.split("?")
        if len(parts) == 2:
            qs = parts[1].split("&")
            qs = [p for p in qs if not p.startswith("channel_binding=")]
            dsn = parts[0] + ("?" + "&".join(qs) if qs else "")
    try:
        pg_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        async with pg_pool.acquire() as conn:
            for s in SCHEMA_SQL:
                await conn.execute(s)
        logger.info("Postgres pool initialized and schema ensured")
    except Exception:
        logger.exception("Failed to initialize Postgres pool")
        pg_pool = None
    return pg_pool

async def close_pg_pool():
    global pg_pool
    try:
        if pg_pool:
            await pg_pool.close()
            pg_pool = None
    except Exception:
        logger.exception("Failed to close Postgres pool")

# ---------------- Neon helpers ----------------
async def neon_store_backup(file_path: str):
    if not pg_pool:
        return False
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        fname = os.path.basename(file_path)
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sqlite_backups (filename, data) VALUES ($1, $2)",
                fname, data
            )
            await conn.execute(
                """
                DELETE FROM sqlite_backups
                WHERE id NOT IN (
                    SELECT id FROM sqlite_backups ORDER BY created_at DESC LIMIT $1
                )
                """,
                NEON_MAX_BACKUPS
            )
        logger.info("Stored binary backup in neon table (kept last %s)", NEON_MAX_BACKUPS)
        return True
    except Exception:
        logger.exception("Failed to store backup to neon table")
        return False

async def neon_sync_settings():
    if not pg_pool:
        return False
    try:
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM settings")
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO neon_kv (key, value, updated_at) VALUES ($1, $2, now())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                    """,
                    r["key"], r["value"]
                )
        logger.info("Mirrored settings to neon_kv")
        return True
    except Exception:
        logger.exception("Failed to sync settings to neon")
        return False

# ---------------- DB utility wrappers ----------------
async def db_set(key: str, value: str):
    try:
        async with pg_pool.acquire() as conn:
            await conn.execute("INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", key, value)
    except Exception:
        logger.exception("db_set failed for key %s", key)
        raise

async def db_get(key: str, default=None):
    try:
        async with pg_pool.acquire() as conn:
            r = await conn.fetchrow("SELECT value FROM settings WHERE key=$1", key)
            return r["value"] if r else default
    except Exception:
        logger.exception("db_get failed for key %s", key)
        raise

async def sql_insert_session(owner_id:int, protect:int, auto_delete_minutes:int, title:str, header_chat_id:int, header_msg_id:int, deep_link_token:str)->int:
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO sessions (owner_id,created_at,protect,auto_delete_minutes,title,header_chat_id,header_msg_id,deep_link) VALUES ($1, now(), $2, $3, $4, $5, $6, $7) RETURNING id",
            owner_id, protect, auto_delete_minutes, title, header_chat_id, header_msg_id, deep_link_token
        )
        return int(row["id"])

async def sql_add_file(session_id:int, file_type:str, file_id:str, caption:str, original_msg_id:int, vault_msg_id:int):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO files (session_id,file_type,file_id,caption,original_msg_id,vault_msg_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
            session_id, file_type, file_id, caption, original_msg_id, vault_msg_id
        )
        return int(row["id"])

async def sql_list_sessions(limit=50):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM sessions ORDER BY created_at DESC LIMIT $1", limit)
        return [dict(r) for r in rows]

async def sql_get_session_by_id(session_id:int):
    async with pg_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM sessions WHERE id=$1", session_id)
        return dict(r) if r else None

async def sql_get_session_by_token(token: str):
    async with pg_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM sessions WHERE deep_link=$1", token)
        return dict(r) if r else None

async def sql_get_session_files(session_id:int):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM files WHERE session_id=$1 ORDER BY id", session_id)
        return [dict(r) for r in rows]

async def sql_set_session_revoked(session_id:int, revoked:int=1):
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET revoked=$1 WHERE id=$2", revoked, session_id)

async def sql_add_user(user: types.User):
    async with pg_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (id,username,first_name,last_name,last_seen) VALUES ($1,$2,$3,$4, now()) ON CONFLICT (id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, last_seen=EXCLUDED.last_seen",
                           user.id, user.username or "", user.first_name or "", user.last_name or "")

async def sql_update_user_lastseen(user_id:int, username:str="", first_name:str="", last_name:str=""):
    async with pg_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (id,username,first_name,last_name,last_seen) VALUES ($1,$2,$3,$4, now()) ON CONFLICT (id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, last_seen=EXCLUDED.last_seen",
                           user_id, username or "", first_name or "", last_name or "")

async def sql_remove_user(user_id:int):
    async with pg_pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE id=$1", user_id)

async def sql_stats():
    async with pg_pool.acquire() as conn:
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
        active = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_seen >= (now() - interval '2 days')")
        files = await conn.fetchval("SELECT COUNT(*) FROM files")
        sessions = await conn.fetchval("SELECT COUNT(*) FROM sessions")
        return {"total_users": total_users or 0, "active_2d": active or 0, "files": files or 0, "sessions": sessions or 0}

async def sql_add_delete_job(session_id:int, target_chat_id:int, message_ids:List[int], run_at:datetime):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO delete_jobs (session_id,target_chat_id,message_ids,run_at,created_at) VALUES ($1,$2,$3,$4, now()) RETURNING id",
                                  session_id, target_chat_id, json.dumps(message_ids), run_at)
        return int(row["id"])

async def sql_list_pending_jobs():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM delete_jobs WHERE status='scheduled'")
        return [dict(r) for r in rows]

async def sql_mark_job_done(job_id:int):
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE delete_jobs SET status='done' WHERE id=$1", job_id)

# ---------------- In-memory upload state ----------------
active_uploads: Dict[int, Dict[str, Any]] = {}
def start_upload_session(owner_id:int, exclude_text:bool):
    active_uploads[owner_id] = {
        "messages": [], "exclude_text": exclude_text, "started_at": datetime.utcnow()
    }

def cancel_upload_session(owner_id:int):
    active_uploads.pop(owner_id, None)

def append_upload_message(owner_id:int, msg: types.Message):
    if owner_id not in active_uploads:
        return
    active_uploads[owner_id]["messages"].append(msg)

def get_upload_messages(owner_id:int) -> List[types.Message]:
    return active_uploads.get(owner_id, {}).get("messages", [])

pending_setmessage: Dict[int, Dict[str, Any]] = {}
pending_setimage: Dict[int, Dict[str, Any]] = {}

# ---------------- Helpers for sending/copying ----------------
async def safe_send(chat_id, text=None, **kwargs):
    try:
        if text is None:
            return None
        return await bot.send_message(chat_id, text, **kwargs)
    except BotBlocked:
        logger.warning("Bot blocked by %s", chat_id)
    except ChatNotFound:
        logger.warning("Chat not found: %s", chat_id)
    except RetryAfter as e:
        logger.warning("Flood wait %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_send(chat_id, text, **kwargs)
    except Exception:
        logger.exception("Failed to send message")
    return None

async def safe_copy(to_chat_id:int, from_chat_id:int, message_id:int, **kwargs):
    try:
        return await bot.copy_message(to_chat_id, from_chat_id, message_id, **kwargs)
    except RetryAfter as e:
        logger.warning("RetryAfter copying: %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_copy(to_chat_id, from_chat_id, message_id, **kwargs)
    except Exception:
        logger.exception("safe_copy failed")
        return None

async def resolve_channel_link(link: str) -> Optional[int]:
    link = (link or "").strip()
    if not link:
        return None
    try:
        if link.startswith("-100") or (link.startswith("-") and link[1:].isdigit()):
            return int(link)
        if "t.me" in link:
            base = link.split("?")[0]
            name = base.rstrip("/").split("/")[-1]
            if name:
                if name.startswith("@"):
                    name = name[1:]
                ch = await bot.get_chat(name)
                return ch.id
        if link.startswith("@"):
            name = link[1:]
            ch = await bot.get_chat(name)
            return ch.id
        ch = await bot.get_chat(link)
        return ch.id
    except ChatNotFound:
        logger.warning("resolve_channel_link: chat not found %s", link)
        return None
    except Exception as e:
        logger.warning("resolve_channel_link error %s : %s", link, e)
        return None

def check_sqlite_integrity(path: str) -> bool:
    try:
        import sqlite3
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check;")
        row = cur.fetchone()
        conn.close()
        if row and row[0] == "ok":
            return True
        logger.warning("SQLite integrity_check failed: %s", row)
        return False
    except Exception:
        logger.exception("Failed to run integrity_check on sqlite file")
        return False

# ---------------- Backup / restore ----------------
async def export_db_json_to_file(file_path: str):
    dump = {}
    async with pg_pool.acquire() as conn:
        for t in ("settings", "users", "sessions", "files", "delete_jobs"):
            rows = await conn.fetch(f"SELECT * FROM {t}")
            dump[t] = [dict(r) for r in rows]
    with open(file_path, "wb") as f:
        f.write(json.dumps(dump, default=str).encode())

async def _send_backup_to_channel(channel_id: int) -> Optional[types.Message]:
    try:
        if channel_id == 0:
            return None
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tmpname = tmp.name
        tmp.close()
        try:
            await export_db_json_to_file(tmpname)
            caption = f"DB backup {datetime.utcnow().isoformat()}"
            with open(tmpname, "rb") as f:
                sent = await bot.send_document(channel_id, InputFile(f, filename="db_backup.json"),
                                               caption=caption,
                                               disable_notification=True)
            try:
                await bot.pin_chat_message(channel_id, sent.message_id, disable_notification=True)
            except Exception:
                logger.exception("Failed to pin DB backup in channel (non-fatal)")
            # mirror to neon tables
            try:
                ok_bin = await neon_store_backup(tmpname)
                ok_kv = await neon_sync_settings()
                if not ok_bin:
                    logger.warning("Neon binary store attempted but failed")
                if not ok_kv:
                    logger.warning("Neon kv sync attempted but failed")
            except Exception:
                logger.exception("Neon mirror failed in backup flow")
            return sent
        finally:
            try:
                os.unlink(tmpname)
            except Exception:
                pass
    except Exception:
        logger.exception("backup to channel failed")
        return None

async def backup_db_to_channel():
    try:
        results = []
        sent1 = await _send_backup_to_channel(DB_CHANNEL_ID)
        results.append(sent1)
        if DB_CHANNEL_ID2 and DB_CHANNEL_ID2 != 0:
            sent2 = await _send_backup_to_channel(DB_CHANNEL_ID2)
            results.append(sent2)
        return results
    except Exception:
        logger.exception("backup_db_to_channel failed")
        return None

async def _download_doc_to_tempfile(file_id: str) -> Optional[str]:
    try:
        file = await bot.get_file(file_id)
        file_path = getattr(file, "file_path", None)
        file_bytes = None
        if file_path:
            try:
                file_bytes = await bot.download_file(file_path)
            except Exception:
                try:
                    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(file_url) as resp:
                            if resp.status == 200:
                                file_bytes = await resp.read()
                            else:
                                logger.error("Failed to fetch file from file_url; status %s", resp.status)
                                return None
                except Exception:
                    logger.exception("Failed fallback download of file")
                    return None
        else:
            try:
                fd = await bot.download_file_by_id(file_id)
                file_bytes = fd
            except Exception:
                logger.exception("Failed direct download by id")
                return None

        if not file_bytes:
            return None

        if hasattr(file_bytes, "read"):
            try:
                content = file_bytes.read()
            except Exception:
                try:
                    if isinstance(file_bytes, io.BytesIO):
                        content = file_bytes.getvalue()
                    else:
                        content = None
                except Exception:
                    content = None
        else:
            content = file_bytes

        if content is None:
            return None
        if isinstance(content, str):
            content = content.encode()

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmpname = tmp.name
        tmp.close()
        with open(tmpname, "wb") as out:
            out.write(content)
        return tmpname
    except Exception:
        logger.exception("Failed to download document to tempfile")
        return None

async def _try_restore_from_message(msg: types.Message) -> bool:
    try:
        if not getattr(msg, "document", None):
            return False
        file_id = msg.document.file_id
        tmpname = await _download_doc_to_tempfile(file_id)
        if not tmpname:
            return False
        try:
            # try JSON restoration
            try:
                with open(tmpname, "rb") as f:
                    raw = f.read()
                data = json.loads(raw.decode())
            except Exception:
                data = None
            if data:
                # naive restore: delete existing rows and insert rows from dump
                async with pg_pool.acquire() as conn:
                    for t, rows in data.items():
                        # skip if not list
                        if not isinstance(rows, list):
                            continue
                        # delete existing
                        await conn.execute(f"DELETE FROM {t}")
                        # insert rows
                        for row in rows:
                            columns = list(row.keys())
                            vals = [row[c] for c in columns]
                            placeholders = ",".join(f"${i+1}" for i in range(len(vals)))
                            q = f"INSERT INTO {t} ({', '.join(columns)}) VALUES ({placeholders})"
                            await conn.execute(q, *vals)
                logger.info("DB restored from JSON backup message %s", getattr(msg, "message_id", None))
                return True
            # If not JSON, check if sqlite and save for migration
            ok = check_sqlite_integrity(tmpname)
            if ok:
                outpath = os.path.join(tempfile.gettempdir(), f"uploaded_db_{int(datetime.utcnow().timestamp())}.sqlite3")
                shutil.move(tmpname, outpath)
                logger.info("Saved uploaded sqlite DB to %s (run migration script to import)", outpath)
                return False
            logger.warning("Candidate backup is neither JSON nor valid sqlite")
            return False
        finally:
            try:
                if os.path.exists(tmpname):
                    os.unlink(tmpname)
            except Exception:
                pass
    except Exception:
        logger.exception("Failed restoring from message")
        return False

async def restore_db_from_pinned(force: bool = False) -> bool:
    try:
        # quick check: if settings has bot_username and not forcing, skip restore
        try:
            v = await db_get("bot_username")
            if v and not force:
                logger.info("DB reachable and has bot_username; skipping restore.")
                return True
        except Exception:
            logger.info("DB may be empty or unreachable; attempting restore from channel backups")

        # Check pinned in DB channel(s)
        for ch in (DB_CHANNEL_ID, DB_CHANNEL_ID2):
            try:
                if not ch or ch == 0:
                    continue
                chat = await bot.get_chat(ch)
            except ChatNotFound:
                logger.error("DB channel %s not found during restore", ch)
                continue
            except Exception:
                logger.exception("Error fetching chat for channel %s", ch)
                continue
            pinned = getattr(chat, "pinned_message", None)
            if pinned and getattr(pinned, "document", None):
                logger.info("Found pinned backup in channel %s; attempting restore.", ch)
                ok = await _try_restore_from_message(pinned)
                if ok:
                    return True
                else:
                    logger.warning("Pinned backup in channel %s failed; will try history.", ch)

        # iterate history
        iter_hist = getattr(bot, "iter_history", None)
        if iter_hist:
            for ch in (DB_CHANNEL_ID, DB_CHANNEL_ID2):
                if not ch or ch == 0:
                    continue
                try:
                    logger.info("Scanning recent messages in channel %s for backups", ch)
                    async for msg in bot.iter_history(ch, limit=200):
                        if getattr(msg, "document", None):
                            ok = await _try_restore_from_message(msg)
                            if ok:
                                return True
                    logger.info("No valid backups found in channel %s", ch)
                except Exception:
                    logger.exception("Failed scanning channel %s history", ch)
        logger.error("No valid DB backup found in configured channels")
        return False
    except Exception:
        logger.exception("restore_db_from_pinned failed")
        return False

# ---------------- Delete job execution ----------------
async def execute_delete_job(job_id:int, job_row:Dict[str,Any]):
    try:
        msg_ids = job_row["message_ids"]
        if isinstance(msg_ids, str):
            msg_ids = json.loads(msg_ids)
        target_chat = int(job_row["target_chat_id"])
        for mid in msg_ids:
            try:
                await bot.delete_message(target_chat, int(mid))
            except MessageToDeleteNotFound:
                pass
            except ChatNotFound:
                logger.warning("Chat not found when deleting messages for job %s", job_id)
            except BotBlocked:
                logger.warning("Bot blocked when deleting messages for job %s", job_id)
            except Exception:
                logger.exception("Error deleting message %s in %s", mid, target_chat)
        await sql_mark_job_done(job_id)
        try:
            scheduler.remove_job(f"deljob_{job_id}")
        except Exception:
            pass
        logger.info("Executed delete job %s", job_id)
    except Exception:
        logger.exception("Failed delete job %s", job_id)

async def restore_pending_jobs_and_schedule():
    logger.info("Restoring pending delete jobs")
    pending = await sql_list_pending_jobs()
    for job in pending:
        try:
            run_at = job["run_at"]
            if isinstance(run_at, str):
                run_at = datetime.fromisoformat(run_at)
            now = datetime.utcnow()
            job_id = job["id"]
            if run_at <= now:
                asyncio.create_task(execute_delete_job(job_id, job))
            else:
                scheduler.add_job(execute_delete_job, 'date', run_date=run_at, args=(job_id, job), id=f"deljob_{job_id}")
                logger.info("Scheduled delete job %s at %s", job_id, run_at.isoformat())
        except Exception:
            logger.exception("Failed to restore job %s", job.get("id"))

# ---------------- Web health server ----------------
WEB_RUNNER: Optional[web.AppRunner] = None

async def healthcheck(request):
    return web.Response(text="ok")

async def start_web_server():
    global WEB_RUNNER
    try:
        app = web.Application()
        app.router.add_get('/', healthcheck)
        app.router.add_get('/health', healthcheck)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        WEB_RUNNER = runner
        logger.info("Health endpoint running on 0.0.0.0:%s/health", PORT)
    except Exception:
        logger.exception("Failed to start health server")

async def stop_web_server():
    global WEB_RUNNER
    try:
        if WEB_RUNNER:
            await WEB_RUNNER.cleanup()
            WEB_RUNNER = None
            logger.info("Health endpoint stopped")
    except Exception:
        logger.exception("Failed to stop health server")

# ---------------- Utility & UI helpers ----------------
def is_owner(user_id:int)->bool:
    return user_id == OWNER_ID

def build_channel_buttons(optional_list:List[Dict[str,str]], forced_list:List[Dict[str,str]]):
    kb = InlineKeyboardMarkup()
    for ch in (optional_list or [])[:4]:
        kb.add(InlineKeyboardButton(ch.get("name","Channel"), url=ch.get("link")))
    for ch in (forced_list or [])[:3]:
        kb.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
    kb.add(InlineKeyboardButton("Help", callback_data=cb_help_button.new(action="open")))
    return kb

def generate_token(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# ---------------- Handlers ----------------

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        await sql_add_user(message.from_user)
        args = message.get_args().strip()
        payload = args if args else None

        start_text = await db_get("start_text", "Welcome, {first_name}!")
        start_text = start_text.replace("{username}", message.from_user.username or "").replace("{first_name}", message.from_user.first_name or "")
        optional_json = await db_get("optional_channels", "[]")
        forced_json = await db_get("force_channels", "[]")
        try:
            optional = json.loads(optional_json)
        except Exception:
            optional = []
        try:
            forced = json.loads(forced_json)
        except Exception:
            forced = []
        kb = build_channel_buttons(optional, forced)

        if not payload:
            start_image = await db_get("start_image")
            if start_image:
                try:
                    await bot.send_photo(message.chat.id, start_image, caption=start_text, reply_markup=kb)
                except Exception:
                    await message.answer(start_text, reply_markup=kb)
            else:
                await message.answer(start_text, reply_markup=kb)
            return

        s = None
        try:
            sid = int(payload)
            s = await sql_get_session_by_id(sid)
        except Exception:
            s = await sql_get_session_by_token(payload)

        if not s or s.get("revoked"):
            await message.answer("This session link is invalid or revoked.")
            return

        blocked = False
        unresolved = []
        for ch in forced[:3]:
            link = ch.get("link")
            resolved = await resolve_channel_link(link)
            if resolved:
                try:
                    member = await bot.get_chat_member(resolved, message.from_user.id)
                    if getattr(member, "status", None) in ("left", "kicked"):
                        blocked = True
                        break
                except BadRequest:
                    blocked = True
                    break
                except ChatNotFound:
                    unresolved.append(link)
                except Exception:
                    unresolved.append(link)
            else:
                unresolved.append(link)

        if blocked:
            kb2 = InlineKeyboardMarkup()
            for ch in forced[:3]:
                kb2.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=s["id"])))
            await message.answer("You must join the required channels first.", reply_markup=kb2)
            return

        if unresolved:
            kb2 = InlineKeyboardMarkup()
            for ch in forced[:3]:
                kb2.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=s["id"])))
            await message.answer("Some channels could not be automatically verified. Please join them and press Retry.", reply_markup=kb2)
            return

        files = await sql_get_session_files(s["id"])
        delivered_msg_ids = []
        owner_is_requester = (message.from_user.id == s.get("owner_id"))
        protect_flag = s.get("protect", 0)
        for f in files:
            try:
                if f["file_type"] == "text":
                    m = await bot.send_message(message.chat.id, f.get("caption") or "")
                    delivered_msg_ids.append(m.message_id)
                else:
                    try:
                        m = await bot.copy_message(message.chat.id, UPLOAD_CHANNEL_ID, f["vault_msg_id"],
                                                   caption=f.get("caption") or "",
                                                   protect_content=bool(protect_flag) and not owner_is_requester)
                        delivered_msg_ids.append(m.message_id)
                    except Exception:
                        if f["file_type"] == "photo":
                            sent = await bot.send_photo(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "video":
                            sent = await bot.send_video(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "document":
                            sent = await bot.send_document(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "sticker":
                            try:
                                sent = await bot.send_sticker(message.chat.id, f["file_id"])
                                delivered_msg_ids.append(sent.message_id)
                            except Exception:
                                pass
                        else:
                            sent = await bot.send_message(message.chat.id, f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
            except Exception:
                logger.exception("Error delivering file in session %s", s["id"])

        minutes = int(s.get("auto_delete_minutes", 0) or 0)
        if minutes and delivered_msg_ids:
            run_at = datetime.utcnow() + timedelta(minutes=minutes)
            job_db_id = await sql_add_delete_job(s["id"], message.chat.id, delivered_msg_ids, run_at)
            scheduler.add_job(execute_delete_job, 'date', run_date=run_at,
                              args=(job_db_id, {"id": job_db_id, "message_ids": json.dumps(delivered_msg_ids),
                                                "target_chat_id": message.chat.id, "run_at": run_at.isoformat()}),
                              id=f"deljob_{job_db_id}")
            await message.answer(f"Messages will be auto-deleted in {minutes} minutes.")

        await message.answer("Delivery complete.")
    except Exception:
        logger.exception("Error in /start handler")
        await message.reply("An error occurred while processing your request.", parse_mode=None)

@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip().lower()
    exclude_text = False
    if "exclude_text" in args:
        exclude_text = True
    start_upload_session(OWNER_ID, exclude_text)
    await message.reply("Upload session started. Send media/text you want included. Use /d to finalize, /e to cancel.", parse_mode=None)

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    cancel_upload_session(OWNER_ID)
    await message.reply("Upload canceled.", parse_mode=None)

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    upload = active_uploads.get(OWNER_ID)
    if not upload:
        await message.reply("No active upload session.", parse_mode=None)
        return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Protect ON", callback_data=cb_choose_protect.new(session="pending", choice="1")),
           InlineKeyboardButton("Protect OFF", callback_data=cb_choose_protect.new(session="pending", choice="0")))
    await message.reply("Choose Protect setting:", reply_markup=kb)
    upload["_finalize_requested"] = True

@dp.callback_query_handler(cb_choose_protect.filter())
async def _on_choose_protect(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    try:
        choice = int(callback_data.get("choice", "0"))
        if OWNER_ID not in active_uploads:
            await call.message.answer("Upload session expired.", parse_mode=None)
            return
        active_uploads[OWNER_ID]["_protect_choice"] = choice
        await call.message.answer("Enter auto-delete timer in minutes (0-10080). 0 = no auto-delete. Reply with a number (e.g., 60).", parse_mode=None)
    except Exception:
        logger.exception("Error in choose_protect callback")

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and "_finalize_requested" in active_uploads.get(OWNER_ID, {}), content_types=types.ContentTypes.TEXT)
async def _receive_minutes(m: types.Message):
    try:
        txt = m.text.strip()
        try:
            mins = int(txt)
            if mins < 0 or mins > 10080:
                raise ValueError()
        except Exception:
            await m.reply("Please send a valid integer between 0 and 10080.", parse_mode=None)
            return
        upload = active_uploads.get(OWNER_ID)
        if not upload:
            await m.reply("Upload session missing.", parse_mode=None)
            return
        messages: List[types.Message] = upload.get("messages", [])
        protect = upload.get("_protect_choice", 0)

        try:
            header = await bot.send_message(UPLOAD_CHANNEL_ID, "Uploading session...")
        except ChatNotFound:
            await m.reply("Upload channel not found. Please ensure the bot is in the UPLOAD_CHANNEL.", parse_mode=None)
            logger.error("ChatNotFound uploading to UPLOAD_CHANNEL_ID")
            return
        header_msg_id = header.message_id
        header_chat_id = header.chat.id

        token = generate_token(8)
        attempt = 0
        while await sql_get_session_by_token(token) is not None and attempt < 5:
            token = generate_token(8)
            attempt += 1

        session_temp_id = await sql_insert_session(OWNER_ID, protect, mins, "Untitled", header_chat_id, header_msg_id, token)

        me = await bot.get_me()
        bot_username = me.username or (await db_get("bot_username") or "")
        deep_link = f"https://t.me/{bot_username}?start={token}"

        try:
            await bot.edit_message_text(f"Session {session_temp_id}\n{deep_link}", UPLOAD_CHANNEL_ID, header_msg_id)
        except Exception:
            pass

        for m0 in messages:
            try:
                if m0.text and m0.text.strip().startswith("/"):
                    continue

                if m0.text and (not upload.get("exclude_text")) and not (m0.photo or m0.video or m0.document or m0.sticker or m0.animation):
                    sent = await bot.send_message(UPLOAD_CHANNEL_ID, m0.text)
                    await sql_add_file(session_temp_id, "text", "", m0.text or "", m0.message_id, sent.message_id)
                elif m0.photo:
                    file_id = m0.photo[-1].file_id
                    sent = await bot.send_photo(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    await sql_add_file(session_temp_id, "photo", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif m0.video:
                    file_id = m0.video.file_id
                    sent = await bot.send_video(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    await sql_add_file(session_temp_id, "video", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif m0.document:
                    file_id = m0.document.file_id
                    sent = await bot.send_document(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    await sql_add_file(session_temp_id, "document", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif m0.sticker:
                    file_id = m0.sticker.file_id
                    sent = await bot.send_sticker(UPLOAD_CHANNEL_ID, file_id)
                    await sql_add_file(session_temp_id, "sticker", file_id, "", m0.message_id, sent.message_id)
                elif m0.animation:
                    file_id = m0.animation.file_id
                    sent = await bot.send_animation(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    await sql_add_file(session_temp_id, "animation", file_id, m0.caption or "", m0.message_id, sent.message_id)
                else:
                    try:
                        sent = await bot.copy_message(UPLOAD_CHANNEL_ID, m0.chat.id, m0.message_id)
                        caption = getattr(m0, "caption", None) or getattr(m0, "text", "") or ""
                        await sql_add_file(session_temp_id, "other", "", caption or "", m0.message_id, sent.message_id)
                    except Exception:
                        logger.exception("Failed copying message during finalize")
            except Exception:
                logger.exception("Error copying message during finalize")

        # update deep link info if needed
        await db_set("last_session_token", token)
        if UPLOAD_CHANNEL_ID2 and UPLOAD_CHANNEL_ID2 != 0:
            try:
                await bot.copy_message(UPLOAD_CHANNEL_ID2, UPLOAD_CHANNEL_ID, header_msg_id)
            except Exception:
                logger.exception("Failed mirroring header to UPLOAD_CHANNEL_ID2")

        cancel_upload_session(OWNER_ID)
        await m.reply(f"Session finalized: {deep_link}")
        raise CancelHandler()
    except CancelHandler:
        raise
    except Exception:
        logger.exception("Error finalizing upload")
        await m.reply("An error occurred during finalization.")

@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return

    args_raw = message.get_args().strip().lower()

    if message.reply_to_message and args_raw in ("start", "help"):
        target = args_raw
        if getattr(message.reply_to_message, "text", None):
            await db_set(f"{target}_text", message.reply_to_message.text)
            await message.reply(f"{target} message updated.", parse_mode=None)
            return
        else:
            await message.reply("Replied message has no text to set.", parse_mode=None)
            return

    if message.reply_to_message and not args_raw:
        if getattr(message.reply_to_message, "text", None):
            pending_setmessage[message.from_user.id] = {
                "text": message.reply_to_message.text,
                "from_chat_id": message.chat.id,
                "reply_msg_id": message.reply_to_message.message_id,
            }
            await message.reply("Reply received. Send `start` or `help` (just the word) to choose which message to set.", parse_mode=None)
            return
        else:
            await message.reply("Replied message has no text to set.", parse_mode=None)
            return

    parts = args_raw.split(" ", 1)
    if parts and parts[0] in ("start", "help") and len(parts) > 1:
        target = parts[0]
        txt = parts[1]
        await db_set(f"{target}_text", txt)
        await message.reply(f"{target} message updated.", parse_mode=None)
        return

    await message.reply("Usage:\nReply to a message and use `/setmessage start` or `/setmessage help`\nOr reply and use `/setmessage` then reply with `start` or `help`.", parse_mode=None)

@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return

    args_raw = message.get_args().strip().lower()

    if message.reply_to_message and args_raw in ("start", "help"):
        target = args_raw
        rt = message.reply_to_message
        file_id = None
        if getattr(rt, "photo", None):
            file_id = rt.photo[-1].file_id
        elif getattr(rt, "document", None):
            file_id = rt.document.file_id
        elif getattr(rt, "sticker", None):
            file_id = rt.sticker.file_id
        elif getattr(rt, "animation", None):
            file_id = rt.animation.file_id
        else:
            await message.reply("Replied message must contain a photo, document (image), sticker, or animation.", parse_mode=None)
            return
        if file_id:
            await db_set(f"{target}_image", file_id)
            await message.reply(f"{target} image set.", parse_mode=None)
            return
        else:
            await message.reply("Could not determine file to save.", parse_mode=None)
            return

    if message.reply_to_message and not args_raw:
        rt = message.reply_to_message
        file_id = None
        if getattr(rt, "photo", None):
            file_id = rt.photo[-1].file_id
        elif getattr(rt, "document", None):
            file_id = rt.document.file_id
        elif getattr(rt, "sticker", None):
            file_id = rt.sticker.file_id
        elif getattr(rt, "animation", None):
            file_id = rt.animation.file_id

        if not file_id:
            await message.reply("Replied message must contain a photo, document (image), sticker, or animation.", parse_mode=None)
            return

        pending_setimage[message.from_user.id] = {
            "file_id": file_id,
            "from_chat_id": message.chat.id,
            "reply_msg_id": message.reply_to_message.message_id,
        }
        await message.reply("Media received. Send `start` or `help` (just the word) to choose which image to set.", parse_mode=None)
        return

    await message.reply("Usage:\nReply to a photo/document/sticker/animation and use `/setimage start` or `/setimage help`\nOr reply and use `/setimage` then reply with `start` or `help`.", parse_mode=None)

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and m.text and m.text.strip().lower() in ("start", "help"))
async def _finalize_pending(m: types.Message):
    key = m.from_user.id
    choice = m.text.strip().lower()
    responses = []

    if key in pending_setimage:
        data_img = pending_setimage.pop(key, None)
        if data_img and data_img.get("file_id"):
            try:
                await db_set(f"{choice}_image", data_img["file_id"])
                responses.append(f"{choice} image updated.")
            except Exception:
                logger.exception("Failed to set pending image")
                responses.append(f"Failed to set {choice} image.")
        else:
            responses.append(f"No pending image to set for {choice}.")

    if key in pending_setmessage:
        data_txt = pending_setmessage.pop(key, None)
        if data_txt and data_txt.get("text"):
            try:
                await db_set(f"{choice}_text", data_txt["text"])
                responses.append(f"{choice} message updated.")
            except Exception:
                logger.exception("Failed to set pending message")
                responses.append(f"Failed to set {choice} message.")
        else:
            responses.append(f"No pending message to set for {choice}.")

    if not responses:
        return

    await m.reply("\n".join(responses), parse_mode=None)

@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setchannel <name> <channel_link> OR /setchannel none", parse_mode=None)
        return
    if args.lower() == "none":
        await db_set("force_channels", json.dumps([]))
        await message.reply("Forced channels cleared.", parse_mode=None)
        return
    parts = args.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Provide name and link.", parse_mode=None)
        return
    name, link = parts[0].strip(), parts[1].strip()
    try:
        arr_raw = await db_get("force_channels", "[]")
        arr = json.loads(arr_raw) if arr_raw else []
    except Exception:
        arr = []
    updated = False
    for entry in arr:
        if entry.get("name") == name or entry.get("link") == link:
            entry["name"] = name
            entry["link"] = link
            updated = True
            break
    if not updated:
        if len(arr) >= 3:
            await message.reply("Max 3 forced channels allowed.", parse_mode=None)
            return
        arr.append({"name": name, "link": link})
    await db_set("force_channels", json.dumps(arr))
    await message.reply("Forced channels updated.", parse_mode=None)

@dp.callback_query_handler(cb_help_button.filter())
async def cb_help(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    txt = await db_get("help_text", "Help is not set.")
    img = await db_get("help_image")
    try:
        if img:
            try:
                await bot.send_photo(call.from_user.id, img, caption=txt)
            except Exception:
                try:
                    await bot.send_document(call.from_user.id, img, caption=txt)
                except Exception:
                    try:
                        await bot.send_animation(call.from_user.id, img, caption=txt)
                    except Exception:
                        try:
                            await bot.send_sticker(call.from_user.id, img)
                            await bot.send_message(call.from_user.id, txt)
                        except Exception:
                            await bot.send_message(call.from_user.id, txt)
        else:
            await bot.send_message(call.from_user.id, txt)
    except Exception:
        logger.exception("Failed to send help to user")
        try:
            await call.message.answer("Failed to open help.", parse_mode=None)
        except Exception:
            pass

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    txt = await db_get("help_text", "Help is not set.")
    img = await db_get("help_image")
    if img:
        try:
            await message.reply_photo(img, caption=txt)
        except Exception:
            await message.reply(txt, parse_mode=None)
    else:
        await message.reply(txt, parse_mode=None)

@dp.message_handler(commands=["adminp"])
async def cmd_adminp(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    s = await sql_stats()
    txt = (
        "Owner panel:\n"
        "/upload - start upload session\n"
        "/d - finalize upload (choose protect + minutes)\n"
        "/e - cancel upload\n"
        "/setmessage - set start/help text\n"
        "/setimage - set start/help image (reply to a photo/sticker/document)\n"
        "/setchannel - add forced join channel\n"
        "/stats - show stats\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke a session\n"
        "/broadcast - reply to message to broadcast\n"
        "/backup_db - backup DB to DB channel(s) and Neon\n"
        "/restore_db - restore DB from pinned\n\n"
        f"Stats: Active(2d): {s['active_2d']}  Total users: {s['total_users']}  Files: {s['files']}  Sessions: {s['sessions']}"
    )
    await message.reply(txt, parse_mode=None)

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    s = await sql_stats()
    await message.reply(f"Active(2d): {s['active_2d']}\nTotal users: {s['total_users']}\nTotal files: {s['files']}\nSessions: {s['sessions']}", parse_mode=None)

@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    rows = await sql_list_sessions(200)
    if not rows:
        await message.reply("No sessions.", parse_mode=None)
        return
    out = []
    for r in rows:
        out.append(f"ID:{r['id']} created:{r['created_at']} protect:{r['protect']} auto_min:{r['auto_delete_minutes']} revoked:{r['revoked']} token:{r['deep_link']}")
    msg = "\n".join(out)
    if len(msg) > 4000:
        await message.reply("Too many sessions to display.", parse_mode=None)
    else:
        await message.reply(msg, parse_mode=None)

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /revoke <id>", parse_mode=None)
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id", parse_mode=None)
        return
    await sql_set_session_revoked(sid, 1)
    await message.reply(f"Session {sid} revoked.", parse_mode=None)

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast.", parse_mode=None)
        return

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM users")
        users = [r["id"] for r in rows]
    if not users:
        await message.reply("No users to broadcast to.", parse_mode=None)
        return
    await message.reply(f"Starting broadcast to {len(users)} users.", parse_mode=None)
    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    lock = asyncio.Lock()
    stats = {"success": 0, "failed": 0, "removed": []}

    async def worker(uid):
        nonlocal stats
        async with sem:
            try:
                await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                async with lock:
                    stats["success"] += 1
            except BotBlocked:
                await sql_remove_user(uid)
                async with lock:
                    stats["removed"].append(uid)
            except ChatNotFound:
                await sql_remove_user(uid)
                async with lock:
                    stats["removed"].append(uid)
            except BadRequest:
                async with lock:
                    stats["failed"] += 1
            except RetryAfter as e:
                logger.warning("Broadcast RetryAfter %s seconds", e.timeout)
                await asyncio.sleep(e.timeout + 1)
                try:
                    await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                    async with lock:
                        stats["success"] += 1
                except Exception:
                    async with lock:
                        stats["failed"] += 1
            except Exception:
                async with lock:
                    stats["failed"] += 1

    tasks = [worker(u) for u in users]
    await asyncio.gather(*tasks)
    removed_count = len(stats["removed"])
    await message.reply(f"Broadcast complete. Success: {stats['success']} Failed: {stats['failed']} Removed: {removed_count}")
    if removed_count:
        r_sample = stats["removed"][:10]
        await bot.send_message(OWNER_ID, f"Broadcast removed {removed_count} users (e.g. {r_sample}). These users were removed from DB.")

@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    sent = await backup_db_to_channel()
    if sent:
        await message.reply("DB backed up to channel(s) and Neon (if configured).", parse_mode=None)
    else:
        await message.reply("Backup failed.", parse_mode=None)

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    ok = await restore_db_from_pinned(force=True)
    if ok:
        await message.reply("DB restored.", parse_mode=None)
    else:
        await message.reply("Restore failed.", parse_mode=None)

@dp.message_handler(commands=["del_session"])
async def cmd_del_session(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /del_session <id>", parse_mode=None)
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id", parse_mode=None)
        return
    async with pg_pool.acquire() as conn:
        await conn.execute("DELETE FROM sessions WHERE id=$1", sid)
    await message.reply("Session deleted.", parse_mode=None)

@dp.callback_query_handler(cb_retry.filter())
async def cb_retry_handler(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    session_id = int(callback_data.get("session"))
    await call.message.answer("Please re-open the deep link you received (tap it in chat) to retry delivery. If channels are joined, delivery should proceed.", parse_mode=None)

@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Update handling failed: %s", exception)
    return True

def _upload_or_noncommand_filter(m: types.Message) -> bool:
    if m.from_user and m.from_user.id == OWNER_ID:
        return OWNER_ID in active_uploads
    if getattr(m, "text", None):
        return not m.text.startswith("/")
    return True

@dp.message_handler(_upload_or_noncommand_filter, content_types=types.ContentTypes.ANY)
async def catch_all_store_uploads(message: types.Message):
    try:
        if message.from_user.id != OWNER_ID:
            await sql_update_user_lastseen(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "", message.from_user.last_name or "")
            return
        if OWNER_ID in active_uploads:
            if message.text and message.text.strip().startswith("/"):
                return
            if message.text and active_uploads[OWNER_ID].get("exclude_text"):
                pass
            else:
                append_upload_message(OWNER_ID, message)
                try:
                    await message.reply("Stored in upload session.", parse_mode=None)
                except Exception:
                    pass
    except Exception:
        logger.exception("Error in catch_all_store_uploads")

# ---------------- Debounced & periodic backup jobs ----------------
DB_DIRTY = False
DB_DIRTY_SINCE: Optional[datetime] = None
DB_DIRTY_LOCK = asyncio.Lock()

def mark_db_dirty():
    global DB_DIRTY, DB_DIRTY_SINCE
    DB_DIRTY = True
    DB_DIRTY_SINCE = datetime.utcnow()
    logger.debug("DB marked dirty at %s", DB_DIRTY_SINCE.isoformat())

async def clear_db_dirty():
    global DB_DIRTY, DB_DIRTY_SINCE
    async with DB_DIRTY_LOCK:
        DB_DIRTY = False
        DB_DIRTY_SINCE = None
        logger.debug("DB dirty flag cleared")

async def debounced_backup_job():
    try:
        if DB_DIRTY:
            logger.info("Debounced backup triggered (DB dirty). Starting backup...")
            await backup_db_to_channel()
        else:
            logger.debug("Debounced backup: DB not dirty; skipping upload.")
    except Exception:
        logger.exception("Debounced backup job failed")

async def periodic_safety_backup_job():
    try:
        logger.info("Periodic safety backup triggered.")
        await backup_db_to_channel()
    except Exception:
        logger.exception("Periodic safety backup failed")

# ---------------- Startup / Shutdown ----------------
async def on_startup(dispatcher):
    try:
        await init_pg_pool(NEON_DB_URL)
    except Exception:
        logger.exception("init_pg_pool failed on startup")

    try:
        await restore_db_from_pinned(force=True)
    except Exception:
        logger.exception("restore_db_from_pinned error on startup")

    try:
        scheduler.start()
    except Exception:
        logger.exception("Scheduler start error")

    try:
        await restore_pending_jobs_and_schedule()
    except Exception:
        logger.exception("restore_pending_jobs_and_schedule error")

    try:
        try:
            scheduler.add_job(debounced_backup_job, 'interval', minutes=DEBOUNCE_BACKUP_MINUTES, id="debounced_backup")
        except Exception:
            pass
        try:
            scheduler.add_job(periodic_safety_backup_job, 'interval', hours=AUTO_BACKUP_HOURS, id="periodic_safety_backup")
        except Exception:
            pass
    except Exception:
        logger.exception("Failed scheduling backup jobs")

    try:
        asyncio.create_task(start_web_server())
    except Exception:
        logger.exception("Failed to start health app task")

    # verify channels
    try:
        await bot.get_chat(UPLOAD_CHANNEL_ID)
    except ChatNotFound:
        logger.error("Upload channel not found. Please add the bot to the upload channel.")
    except Exception:
        logger.exception("Error checking upload channel")
    if UPLOAD_CHANNEL_ID2 and UPLOAD_CHANNEL_ID2 != 0:
        try:
            await bot.get_chat(UPLOAD_CHANNEL_ID2)
        except ChatNotFound:
            logger.error("Upload channel 2 not found.")
        except Exception:
            logger.exception("Error checking upload channel 2")
    try:
        await bot.get_chat(DB_CHANNEL_ID)
    except ChatNotFound:
        logger.error("DB channel not found. Please add the bot to the DB channel.")
    except Exception:
        logger.exception("Error checking DB channel")
    if DB_CHANNEL_ID2 and DB_CHANNEL_ID2 != 0:
        try:
            await bot.get_chat(DB_CHANNEL_ID2)
        except ChatNotFound:
            logger.error("DB channel 2 not found.")
        except Exception:
            logger.exception("Error checking DB channel 2")

    me = await bot.get_me()
    try:
        await db_set("bot_username", me.username or "")
    except Exception:
        logger.exception("Failed saving bot_username in settings")

    if await db_get("start_text") is None:
        await db_set("start_text", "Welcome, {first_name}!")
    if await db_get("help_text") is None:
        await db_set("help_text", "This bot delivers sessions.")

    logger.info("on_startup complete")

async def on_shutdown(dispatcher):
    logger.info("Shutting down")
    try:
        if DB_DIRTY:
            logger.info("Final backup on shutdown (DB dirty).")
            await backup_db_to_channel()
    except Exception:
        logger.exception("Final backup failed")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await close_pg_pool()
    except Exception:
        logger.exception("Failed closing Postgres pool")
    try:
        await stop_web_server()
    except Exception:
        logger.exception("Failed stopping web server")
    await bot.close()

if __name__ == "__main__":
    try:
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopped by user")
    except Exception:
        logger.exception("Fatal error")