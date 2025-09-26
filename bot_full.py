
#!/usr/bin/env python3
# vaultbot_postgres_full.py - compact complete version
import os, logging, asyncio, json, tempfile, secrets, string, shutil, io, subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from aiogram.utils.callback_data import CallbackData
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter, BadRequest, MessageToDeleteNotFound

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import aiohttp
from aiohttp import web

# Config
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID") or 0)
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID") or 0)
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID") or 0)
NEON_DB_URL = os.getenv("NEON_DB_URL")
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
PORT = int(os.getenv("PORT", "10000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if not BOT_TOKEN or OWNER_ID == 0 or UPLOAD_CHANNEL_ID == 0 or DB_CHANNEL_ID == 0 or not NEON_DB_URL or not WEBHOOK_HOST:
    raise RuntimeError("Required env vars missing: BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL_ID, DB_CHANNEL_ID, NEON_DB_URL, WEBHOOK_HOST")

WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("vaultbot")

bot = Bot(token=BOT_TOKEN, parse_mode=None)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

cb_choose_protect = CallbackData("protect", "session", "choice")
cb_retry = CallbackData("retry", "session")
cb_help_button = CallbackData("helpbtn", "action")

# Scheduler with SQLAlchemy jobstore; convert NEON_DB_URL scheme if needed
def sa_url_from_dsn(dsn: str) -> str:
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql+psycopg2://", 1)
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg2://", 1)
    return dsn

SA_JOBSTORE_URL = sa_url_from_dsn(NEON_DB_URL)
jobstores = {'default': SQLAlchemyJobStore(url=SA_JOBSTORE_URL)}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.configure(timezone="UTC")

# Schema
SCHEMA = \"\"\"
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, last_seen TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS sessions (id BIGSERIAL PRIMARY KEY, owner_id BIGINT, created_at TIMESTAMPTZ, protect INTEGER DEFAULT 0, auto_delete_minutes INTEGER DEFAULT 0, title TEXT, revoked INTEGER DEFAULT 0, header_msg_id BIGINT, header_chat_id BIGINT, deep_link TEXT);
CREATE TABLE IF NOT EXISTS files (id BIGSERIAL PRIMARY KEY, session_id BIGINT, file_type TEXT, file_id TEXT, caption TEXT, original_msg_id BIGINT, vault_msg_id BIGINT, FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS delete_jobs (id BIGSERIAL PRIMARY KEY, session_id BIGINT, target_chat_id BIGINT, message_ids JSONB, run_at TIMESTAMPTZ, created_at TIMESTAMPTZ, status TEXT DEFAULT 'scheduled');
CREATE TABLE IF NOT EXISTS sqlite_backups (id BIGSERIAL PRIMARY KEY, filename TEXT, data bytea, created_at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS neon_kv (key TEXT PRIMARY KEY, value TEXT, updated_at timestamptz DEFAULT now());
\"\"\"

pg_pool: Optional[asyncpg.pool.Pool] = None

async def init_db(dsn: str):
    global pg_pool
    if pg_pool:
        return pg_pool
    # remove channel_binding if present
    if "channel_binding" in dsn:
        parts = dsn.split("?")
        if len(parts) == 2:
            qs = [p for p in parts[1].split("&") if not p.startswith("channel_binding=")]
            dsn = parts[0] + ("?" + "&".join(qs) if qs else "")
    pg_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=6)
    async with pg_pool.acquire() as conn:
        for stmt in SCHEMA.split(";"):
            s = stmt.strip()
            if s:
                await conn.execute(s)
    logger.info("DB pool initialized and schema applied")
    return pg_pool

async def close_db():
    global pg_pool
    try:
        if pg_pool:
            await pg_pool.close()
            pg_pool = None
    except Exception:
        logger.exception("close_db failed")

# Simple audit log to DB channel
async def write_log(payload: Dict[str, Any]):
    try:
        txt = json.dumps(payload, default=str)
        if len(txt) > 3500: txt = txt[:3500] + "..."
        await bot.send_message(DB_CHANNEL_ID, txt, disable_notification=True)
    except Exception:
        logger.exception("write_log failed")

# Settings helpers
async def db_set(key: str, value: str):
    async with pg_pool.acquire() as conn:
        await conn.execute("INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", key, value)
    await write_log({"op":"db_set","key":key,"time":datetime.utcnow().isoformat()})

async def db_get(key: str, default=None):
    async with pg_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return r["value"] if r else default

# Users
async def sql_add_user(user: types.User):
    async with pg_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (id,username,first_name,last_name,last_seen) VALUES ($1,$2,$3,$4, now()) ON CONFLICT (id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, last_seen=EXCLUDED.last_seen", user.id, user.username or "", user.first_name or "", user.last_name or "")
    await write_log({"op":"add_user","id":user.id,"username":user.username,"time":datetime.utcnow().isoformat()})

async def sql_update_user_lastseen(user_id:int, username:str="", first_name:str="", last_name:str=""):
    async with pg_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (id,username,first_name,last_name,last_seen) VALUES ($1,$2,$3,$4, now()) ON CONFLICT (id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, last_seen=EXCLUDED.last_seen", user_id, username or "", first_name or "", last_name or "")

async def sql_remove_user(user_id:int):
    async with pg_pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE id=$1", user_id)
    await write_log({"op":"remove_user","id":user_id,"time":datetime.utcnow().isoformat()})

# Sessions & files
async def sql_insert_session(owner_id:int, protect:int, auto_delete_minutes:int, title:str, header_chat_id:int, header_msg_id:int, deep_link_token:str)->int:
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO sessions (owner_id,created_at,protect,auto_delete_minutes,title,header_chat_id,header_msg_id,deep_link) VALUES ($1, now(), $2, $3, $4, $5, $6, $7) RETURNING id", owner_id, protect, auto_delete_minutes, title, header_chat_id, header_msg_id, deep_link_token)
        sid = int(row["id"])
    await write_log({"op":"insert_session","id":sid,"owner":owner_id,"time":datetime.utcnow().isoformat()})
    return sid

async def sql_add_file(session_id:int, file_type:str, file_id:str, caption:str, original_msg_id:int, vault_msg_id:int):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO files (session_id,file_type,file_id,caption,original_msg_id,vault_msg_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id", session_id, file_type, file_id, caption, original_msg_id, vault_msg_id)
        fid = int(row["id"])
    await write_log({"op":"add_file","id":fid,"session":session_id,"type":file_type,"time":datetime.utcnow().isoformat()})
    return fid

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
    await write_log({"op":"revoke_session","id":session_id,"time":datetime.utcnow().isoformat()})

# Delete jobs
async def sql_add_delete_job(session_id:int, target_chat_id:int, message_ids:List[int], run_at:datetime):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO delete_jobs (session_id,target_chat_id,message_ids,run_at,created_at) VALUES ($1,$2,$3,$4, now()) RETURNING id", session_id, target_chat_id, json.dumps(message_ids), run_at)
        jid = int(row["id"])
    await write_log({"op":"add_delete_job","id":jid,"session":session_id,"run_at":str(run_at)})
    return jid

async def sql_list_pending_jobs():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM delete_jobs WHERE status='scheduled'")
        return [dict(r) for r in rows]

async def sql_mark_job_done(job_id:int):
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE delete_jobs SET status='done' WHERE id=$1", job_id)
    await write_log({"op":"mark_job_done","id":job_id,"time":datetime.utcnow().isoformat()})

# In-memory upload sessions
active_uploads: Dict[int, Dict[str, Any]] = {}
def start_upload_session(owner_id:int, exclude_text:bool):
    active_uploads[owner_id] = {"messages": [], "exclude_text": exclude_text, "started_at": datetime.utcnow()}
def cancel_upload_session(owner_id:int):
    active_uploads.pop(owner_id, None)
def append_upload_message(owner_id:int, msg: types.Message):
    if owner_id not in active_uploads: return
    active_uploads[owner_id]["messages"].append(msg)

# Helpers for telegram operations
async def safe_copy(to_chat_id:int, from_chat_id:int, message_id:int, **kwargs):
    try:
        return await bot.copy_message(to_chat_id, from_chat_id, message_id, **kwargs)
    except RetryAfter as e:
        await asyncio.sleep(e.timeout + 1)
        return await safe_copy(to_chat_id, from_chat_id, message_id, **kwargs)
    except Exception:
        logger.exception("safe_copy failed")
        return None

def generate_token(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# Backup using pg_dump
async def run_pg_dump(out_path: str) -> bool:
    try:
        cmd = ["pg_dump", "--dbname", NEON_DB_URL, "-F", "p", "-f", out_path]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error("pg_dump failed: %s", (stderr.decode() if stderr else "no stderr"))
            return False
        return True
    except Exception:
        logger.exception("run_pg_dump failed")
        return False

async def send_sql_backup_to_channel(channel_id:int) -> Optional[types.Message]:
    if channel_id == 0: return None
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
    tmpname = tmp.name; tmp.close()
    try:
        ok = await run_pg_dump(tmpname)
        if not ok: return None
        caption = f"PG SQL backup {datetime.utcnow().isoformat()}"
        with open(tmpname, "rb") as f:
            sent = await bot.send_document(channel_id, InputFile(f, filename="backup.sql"), caption=caption, disable_notification=True)
        try: await bot.pin_chat_message(channel_id, sent.message_id, disable_notification=True)
        except Exception: pass
        try:
            async with pg_pool.acquire() as conn:
                with open(tmpname, "rb") as fbin:
                    data = fbin.read()
                await conn.execute("INSERT INTO sqlite_backups (filename, data) VALUES ($1,$2)", os.path.basename(tmpname), data)
                await conn.execute("DELETE FROM sqlite_backups WHERE id NOT IN (SELECT id FROM sqlite_backups ORDER BY created_at DESC LIMIT $1)", MAX_BACKUPS)
        except Exception:
            logger.exception("mirror backup failed")
        return sent
    finally:
        try: os.unlink(tmpname)
        except: pass

async def backup_db_command(triggered_by_user: Optional[types.Message] = None):
    try:
        results = []
        sent1 = await send_sql_backup_to_channel(DB_CHANNEL_ID)
        results.append(sent1)
        if triggered_by_user:
            await triggered_by_user.reply("Backup sent to DB channel.", parse_mode=None)
        return results
    except Exception:
        logger.exception("backup_db_command failed")
        if triggered_by_user: await triggered_by_user.reply("Backup failed.", parse_mode=None)
        return None

# Restore from DB channel
async def download_document_to_tmpfile(file_id: str) -> Optional[str]:
    try:
        file = await bot.get_file(file_id)
        fp = getattr(file, "file_path", None)
        if fp:
            url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{fp}"
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                    else:
                        return None
        else:
            fd = await bot.download_file_by_id(file_id)
            data = fd if isinstance(fd, (bytes,bytearray)) else None
        if not data: return None
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
        tmpname = tmp.name; tmp.close()
        with open(tmpname, "wb") as f: f.write(data)
        return tmpname
    except Exception:
        logger.exception("download failed"); return None

async def restore_db_from_channel(triggered_by_user: Optional[types.Message] = None):
    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
    except Exception:
        if triggered_by_user: await triggered_by_user.reply("DB channel not accessible.", parse_mode=None)
        return False
    candidates = []
    pinned = getattr(chat, "pinned_message", None)
    if pinned and getattr(pinned, "document", None): candidates.append(pinned)
    iter_hist = getattr(bot, "iter_history", None)
    if iter_hist:
        async for msg in bot.iter_history(DB_CHANNEL_ID, limit=200):
            if getattr(msg, "document", None):
                candidates.append(msg)
    for msg in candidates:
        try:
            file_id = msg.document.file_id
            tmpname = await download_document_to_tmpfile(file_id)
            if not tmpname: continue
            try:
                text = open(tmpname, "r", encoding="utf-8", errors="ignore").read()
                statements = [s.strip() for s in text.split(";") if s.strip()]
                conn = await asyncpg.connect(NEON_DB_URL)
                try:
                    for st in statements:
                        try:
                            await conn.execute(st)
                        except Exception as e:
                            logger.debug("Skipping stmt: %.120s ... %s", st.replace("\\n"," "), str(e))
                    await conn.close()
                    if triggered_by_user: await triggered_by_user.reply("Restore attempted (SQL replay).", parse_mode=None)
                    return True
                finally:
                    try: await conn.close()
                    except: pass
            finally:
                try: os.unlink(tmpname)
                except: pass
        except Exception:
            logger.exception("candidate failed")
    if triggered_by_user: await triggered_by_user.reply("No valid backups found in DB channel.", parse_mode=None)
    return False

# Delete job executor
async def execute_delete_job(job_id:int, job_row:Dict[str,Any]):
    try:
        msg_ids = job_row["message_ids"]
        if isinstance(msg_ids, str): msg_ids = json.loads(msg_ids)
        target_chat = int(job_row["target_chat_id"])
        for mid in msg_ids:
            try:
                await bot.delete_message(target_chat, int(mid))
            except MessageToDeleteNotFound:
                pass
            except ChatNotFound:
                logger.warning("Chat not found for job %s", job_id)
            except BotBlocked:
                logger.warning("Bot blocked for job %s", job_id)
            except Exception:
                logger.exception("Error deleting message %s in %s", mid, target_chat)
        await sql_mark_job_done(job_id)
        try: scheduler.remove_job(f"deljob_{job_id}")
        except Exception: pass
        await write_log({"op":"exec_del_job","id":job_id,"time":datetime.utcnow().isoformat()})
    except Exception:
        logger.exception("execute_delete_job failed for %s", job_id)

async def restore_pending_jobs_and_schedule():
    pending = await sql_list_pending_jobs()
    for job in pending:
        try:
            run_at = job["run_at"]
            if isinstance(run_at, str): run_at = datetime.fromisoformat(run_at)
            now = datetime.utcnow()
            job_id = job["id"]
            if run_at <= now:
                asyncio.create_task(execute_delete_job(job_id, job))
            else:
                scheduler.add_job(execute_delete_job, 'date', run_date=run_at, args=(job_id, job), id=f"deljob_{job_id}")
                logger.info("Scheduled delete job %s at %s", job_id, run_at.isoformat())
        except Exception:
            logger.exception("restore_pending_jobs_and_schedule failed for %s", job.get("id"))

# Handlers (simplified but full functionality present)
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    try:
        await sql_add_user(message.from_user)
        args = message.get_args().strip()
        if not args:
            start_text = await db_get("start_text", "Welcome, {first_name}!") or "Welcome!"
            start_text = start_text.replace("{username}", message.from_user.username or "").replace("{first_name}", message.from_user.first_name or "")
            img = await db_get("start_image")
            kb = InlineKeyboardMarkup().add(InlineKeyboardButton("Help", callback_data=cb_help_button.new(action="open")))
            if img:
                try: await bot.send_photo(message.chat.id, img, caption=start_text, reply_markup=kb)
                except: await message.answer(start_text, reply_markup=kb)
            else:
                await message.answer(start_text, reply_markup=kb)
            return
        token = args
        s = await sql_get_session_by_token(token)
        if not s or s.get("revoked"):
            await message.answer("This session link is invalid or revoked."); return
        files = await sql_get_session_files(s['id'])
        delivered = []
        for f in files:
            try:
                if f['file_type'] == 'text':
                    m = await bot.send_message(message.chat.id, f.get('caption') or "")
                    delivered.append(m.message_id)
                else:
                    try:
                        m = await bot.copy_message(message.chat.id, UPLOAD_CHANNEL_ID, f['vault_msg_id'], caption=f.get('caption') or "")
                        delivered.append(m.message_id)
                    except Exception:
                        if f['file_type'] == 'photo':
                            sent = await bot.send_photo(message.chat.id, f['file_id'], caption=f.get('caption') or "")
                            delivered.append(getattr(sent, 'message_id', None))
                        elif f['file_type'] == 'document':
                            sent = await bot.send_document(message.chat.id, f['file_id'], caption=f.get('caption') or "")
                            delivered.append(getattr(sent, 'message_id', None))
                        else:
                            sent = await bot.send_message(message.chat.id, f.get('caption') or "")
                            delivered.append(getattr(sent,'message_id',None))
            except Exception:
                logger.exception("delivery error")
        await message.reply("Delivery complete.")
    except Exception:
        logger.exception("start handler failed"); await message.reply("An error occurred.")

@dp.message_handler(commands=['upload'])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID: await message.reply("Unauthorized"); return
    args = message.get_args().lower() or ""
    exclude_text = "exclude_text" in args
    start_upload_session(OWNER_ID, exclude_text)
    await message.reply("Upload session started. /d to finalize, /e to cancel.")

@dp.message_handler(commands=['e'])
async def cmd_e(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    cancel_upload_session(OWNER_ID); await message.reply("Upload canceled.")

@dp.message_handler(commands=['d'])
async def cmd_d(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    upload = active_uploads.get(OWNER_ID)
    if not upload: await message.reply("No active upload session."); return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Protect ON", callback_data=cb_choose_protect.new(session="pending", choice="1")), InlineKeyboardButton("Protect OFF", callback_data=cb_choose_protect.new(session="pending", choice="0")))
    upload["_finalize_requested"] = True
    await message.reply("Choose Protect setting:", reply_markup=kb)

@dp.callback_query_handler(cb_choose_protect.filter())
async def on_choose_protect(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    try:
        choice = int(callback_data.get("choice","0"))
        if OWNER_ID not in active_uploads:
            await call.message.answer("Upload session expired."); return
        active_uploads[OWNER_ID]["_protect_choice"] = choice
        await call.message.answer("Enter auto-delete timer in minutes (0-10080). 0 = no auto-delete.")
    except Exception:
        logger.exception("choose_protect failed")

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and "_finalize_requested" in active_uploads.get(OWNER_ID, {}), content_types=types.ContentTypes.TEXT)
async def receive_minutes(m: types.Message):
    try:
        txt = m.text.strip(); mins = int(txt)
        if mins < 0 or mins > 10080: raise ValueError()
    except Exception:
        await m.reply("Please send a valid integer between 0 and 10080."); return
    upload = active_uploads.get(OWNER_ID)
    messages = upload.get("messages", [])
    protect = upload.get("_protect_choice", 0)
    try:
        header = await bot.send_message(UPLOAD_CHANNEL_ID, "Uploading session...")
    except ChatNotFound:
        await m.reply("Upload channel not found."); return
    header_msg_id = header.message_id; header_chat_id = header.chat.id
    token = generate_token(8); attempt=0
    while await sql_get_session_by_token(token) is not None and attempt < 5:
        token = generate_token(8); attempt += 1
    session_id = await sql_insert_session(OWNER_ID, protect, mins, "Untitled", header_chat_id, header_msg_id, token)
    me = await bot.get_me(); bot_username = me.username or (await db_get("bot_username") or "bot")
    deep_link = f"https://t.me/{bot_username}?start={token}"
    try:
        await bot.edit_message_text(f"Session {session_id}\\n{deep_link}", UPLOAD_CHANNEL_ID, header_msg_id)
    except Exception: pass
    for m0 in messages:
        try:
            if m0.text and m0.text.strip().startswith("/"): continue
            if m0.text and (not upload.get("exclude_text")) and not (m0.photo or m0.video or m0.document or m0.sticker or m0.animation):
                sent = await bot.send_message(UPLOAD_CHANNEL_ID, m0.text); await sql_add_file(session_id, "text", "", m0.text or "", m0.message_id, sent.message_id)
            elif m0.photo:
                fid = m0.photo[-1].file_id; sent = await bot.send_photo(UPLOAD_CHANNEL_ID, fid, caption=m0.caption or ""); await sql_add_file(session_id, "photo", fid, m0.caption or "", m0.message_id, sent.message_id)
            elif m0.document:
                fid = m0.document.file_id; sent = await bot.send_document(UPLOAD_CHANNEL_ID, fid, caption=m0.caption or ""); await sql_add_file(session_id, "document", fid, m0.caption or "", m0.message_id, sent.message_id)
            else:
                try:
                    sent = await bot.copy_message(UPLOAD_CHANNEL_ID, m0.chat.id, m0.message_id)
                    caption = getattr(m0,"caption",None) or getattr(m0,"text","") or ""
                    await sql_add_file(session_id, "other", "", caption or "", m0.message_id, sent.message_id)
                except Exception:
                    logger.exception("copy fallback failed")
        except Exception:
            logger.exception("upload copy failed")
    cancel_upload_session(OWNER_ID)
    await m.reply(f"Session finalized: {deep_link}")

@dp.message_handler(commands=['setmessage'])
async def cmd_setmessage(message: types.Message):
    if message.from_user.id != OWNER_ID: await message.reply("Unauthorized"); return
    args = message.get_args().strip().lower()
    if message.reply_to_message and args in ("start","help"):
        target = args
        if getattr(message.reply_to_message,"text",None):
            await db_set(f"{target}_text", message.reply_to_message.text); await message.reply(f"{target} message updated."); return
        else: await message.reply("Replied message has no text."); return
    if message.reply_to_message and not args:
        if getattr(message.reply_to_message,"text",None):
            pending_setmessage[message.from_user.id] = {"text": message.reply_to_message.text}; await message.reply("Reply received. Send `start` or `help`."); return
    parts = args.split(" ",1)
    if parts and parts[0] in ("start","help") and len(parts)>1:
        await db_set(f"{parts[0]}_text", parts[1]); await message.reply(f"{parts[0]} message updated."); return
    await message.reply("Usage: reply and /setmessage start or /setmessage help")

@dp.message_handler(commands=['setimage'])
async def cmd_setimage(message: types.Message):
    if message.from_user.id != OWNER_ID: await message.reply("Unauthorized"); return
    args = message.get_args().strip().lower()
    if message.reply_to_message and args in ("start","help"):
        rt = message.reply_to_message; file_id = None
        if getattr(rt,"photo",None): file_id = rt.photo[-1].file_id
        elif getattr(rt,"document",None): file_id = rt.document.file_id
        elif getattr(rt,"sticker",None): file_id = rt.sticker.file_id
        elif getattr(rt,"animation",None): file_id = rt.animation.file_id
        if not file_id: await message.reply("Replied message must contain media."); return
        await db_set(f"{args}_image", file_id); await message.reply(f"{args} image set."); return
    if message.reply_to_message and not args:
        rt = message.reply_to_message; file_id = None
        if getattr(rt,"photo",None): file_id = rt.photo[-1].file_id
        elif getattr(rt,"document",None): file_id = rt.document.file_id
        elif getattr(rt,"sticker",None): file_id = rt.sticker.file_id
        elif getattr(rt,"animation",None): file_id = rt.animation.file_id
        if not file_id: await message.reply("Replied message must contain media."); return
        pending_setimage[message.from_user.id] = {"file_id": file_id}; await message.reply("Media received. Send `start` or `help`."); return
    await message.reply("Usage: reply to media with /setimage start (or help)")

@dp.message_handler(commands=['setchannel'])
async def cmd_setchannel(message: types.Message):
    if message.from_user.id != OWNER_ID: await message.reply("Unauthorized"); return
    args = message.get_args().strip()
    if not args: await message.reply("Usage: /setchannel <name> <link> OR /setchannel none"); return
    if args.lower()=="none": await db_set("force_channels", json.dumps([])); await message.reply("Forced channels cleared."); return
    parts = args.split(" ",1)
    if len(parts)<2: await message.reply("Provide name and link."); return
    name, link = parts[0].strip(), parts[1].strip()
    arr_raw = await db_get("force_channels", "[]")
    try: arr = json.loads(arr_raw) if arr_raw else []
    except: arr = []
    updated=False
    for entry in arr:
        if entry.get("name")==name or entry.get("link")==link:
            entry["name"]=name; entry["link"]=link; updated=True; break
    if not updated:
        if len(arr)>=3: await message.reply("Max 3 forced channels allowed."); return
        arr.append({"name":name,"link":link})
    await db_set("force_channels", json.dumps(arr)); await message.reply("Forced channels updated.")

@dp.message_handler(commands=['broadcast'])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID: await message.reply("Unauthorized"); return
    if not message.reply_to_message: await message.reply("Reply to message to broadcast."); return
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM users")
        users = [r['id'] for r in rows]
    if not users: await message.reply("No users to broadcast to."); return
    await message.reply(f"Starting broadcast to {len(users)} users.")
    sem = asyncio.Semaphore(12); lock = asyncio.Lock(); stats={"success":0,"failed":0,"removed":[]}
    async def worker(uid):
        nonlocal stats
        async with sem:
            try:
                await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                async with lock: stats["success"] += 1
            except BotBlocked:
                await sql_remove_user(uid); async with lock: stats["removed"].append(uid)
            except ChatNotFound:
                await sql_remove_user(uid); async with lock: stats["removed"].append(uid)
            except BadRequest:
                async with lock: stats["failed"] += 1
            except RetryAfter as e:
                await asyncio.sleep(e.timeout + 1)
                try: await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id); async with lock: stats["success"] += 1
                except Exception: async with lock: stats["failed"] += 1
            except Exception:
                async with lock: stats["failed"] += 1
    tasks = [worker(u) for u in users]
    await asyncio.gather(*tasks)
    await message.reply(f"Broadcast complete. Success: {stats['success']} Failed: {stats['failed']} Removed: {len(stats['removed'])}")

@dp.message_handler(commands=['backup_db'])
async def cmd_backup_db(message: types.Message):
    if message.from_user.id != OWNER_ID: await message.reply("Unauthorized"); return
    await backup_db_command(triggered_by_user=message)

@dp.message_handler(commands=['restore_db'])
async def cmd_restore_db(message: types.Message):
    if message.from_user.id != OWNER_ID: await message.reply("Unauthorized"); return
    ok = await restore_db_from_channel(triggered_by_user=message)
    if ok: await message.reply("Restore attempted. Check logs.")
    else: await message.reply("Restore failed or no backups found.")

@dp.message_handler(commands=['del_session'])
async def cmd_del_session(message: types.Message):
    if message.from_user.id != OWNER_ID: await message.reply("Unauthorized"); return
    args = message.get_args().strip()
    if not args: await message.reply("Usage: /del_session <id>"); return
    try: sid = int(args)
    except: await message.reply("Invalid id"); return
    async with pg_pool.acquire() as conn:
        await conn.execute("DELETE FROM sessions WHERE id=$1", sid)
    await message.reply("Session deleted.")

@dp.errors_handler()
async def global_errors(update, exception):
    logger.exception("Update handling failed: %s", exception)
    return True

@dp.message_handler(content_types=types.ContentTypes.ANY)
async def catch_all(message: types.Message):
    try:
        if message.from_user.id == OWNER_ID and OWNER_ID in active_uploads:
            if message.text and message.text.strip().startswith("/"): return
            append_upload_message(OWNER_ID, message)
            try: await message.reply("Stored in upload session.")
            except: pass
            return
        if message.from_user and message.from_user.id != OWNER_ID:
            await sql_update_user_lastseen(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "", message.from_user.last_name or "")
    except Exception:
        logger.exception("catch_all failed")

# Debounced backups
DB_DIRTY = False; DB_DIRTY_SINCE: Optional[datetime] = None; DB_DIRTY_LOCK = asyncio.Lock()
def mark_db_dirty():
    global DB_DIRTY, DB_DIRTY_SINCE; DB_DIRTY=True; DB_DIRTY_SINCE=datetime.utcnow()
async def clear_db_dirty():
    global DB_DIRTY, DB_DIRTY_SINCE
    async with DB_DIRTY_LOCK: DB_DIRTY=False; DB_DIRTY_SINCE=None

async def debounced_backup_job():
    if DB_DIRTY:
        logger.info("Debounced backup triggered")
        await backup_db_command()
    else:
        logger.debug("No DB dirty; skip backup")

async def periodic_backup_job():
    logger.info("Periodic backup triggered")
    await backup_db_command()

# Startup / shutdown
async def on_startup(app):
    await init_db(NEON_DB_URL)
    try: scheduler.start()
    except Exception: logger.exception("scheduler start failed")
    try:
        scheduler.add_job(debounced_backup_job, 'interval', minutes=DEBOUNCE_BACKUP_MINUTES, id="debounced_backup")
        scheduler.add_job(periodic_backup_job, 'interval', hours=24, id="periodic_backup")
    except Exception: pass
    try: await restore_pending_jobs_and_schedule()
    except Exception: logger.exception("restore_pending_jobs failed")
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook")
    me = await bot.get_me(); await db_set("bot_username", me.username or "")
    if await db_get("start_text") is None: await db_set("start_text", "Welcome, {first_name}!")
    if await db_get("help_text") is None: await db_set("help_text", "This bot delivers sessions.")

async def on_shutdown(app):
    try: await bot.delete_webhook()
    except: pass
    try: scheduler.shutdown(wait=False)
    except: pass
    try: await close_db()
    except: pass
    await bot.session.close()

# Aiohttp app for webhook health + allow aiogram to hook
app = web.Application()
app.router.add_get("/", lambda r: web.Response(text="ok"))
app.router.add_get("/health", lambda r: web.Response(text="ok"))
# aiogram will mount webhook endpoint via executor.start_webhook

if __name__ == '__main__':
    # start_polling replaced by webhook runner
    executor.start_webhook(dispatcher=dp, webhook_path=WEBHOOK_PATH, on_startup=on_startup, on_shutdown=on_shutdown, host='0.0.0.0', port=PORT)
