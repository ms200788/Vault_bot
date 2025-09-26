#!/usr/bin/env python3
"""
migrate_sqlite_to_neon.py

Usage:
  NEON_DB_URL="postgresql://user:pass@host:port/dbname" \
  python migrate_sqlite_to_neon.py /path/to/database.sqlite3
"""
import os
import sys
import json
import sqlite3
import psycopg2
import psycopg2.extras

NEON_DB_URL = os.environ.get("NEON_DB_URL")
if not NEON_DB_URL:
    print("Set NEON_DB_URL environment variable")
    sys.exit(1)

if len(sys.argv) < 2:
    print("Usage: python migrate_sqlite_to_neon.py /path/to/database.sqlite3")
    sys.exit(1)

sqlite_path = sys.argv[1]
if not os.path.exists(sqlite_path):
    print("Sqlite file not found:", sqlite_path)
    sys.exit(1)

print("Connecting to Postgres...")
pg = psycopg2.connect(NEON_DB_URL)
cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# -------------------------
# Ensure schema
# -------------------------
def ensure_schema():
    stmts = [
        """CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            last_seen TIMESTAMPTZ
        );""",
        """CREATE TABLE IF NOT EXISTS sessions (
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
        );""",
        """CREATE TABLE IF NOT EXISTS files (
            id BIGSERIAL PRIMARY KEY,
            session_id BIGINT,
            file_type TEXT,
            file_id TEXT,
            caption TEXT,
            original_msg_id BIGINT,
            vault_msg_id BIGINT
        );""",
        """CREATE TABLE IF NOT EXISTS delete_jobs (
            id BIGSERIAL PRIMARY KEY,
            session_id BIGINT,
            target_chat_id BIGINT,
            message_ids JSONB,
            run_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ,
            status TEXT DEFAULT 'scheduled'
        );"""
    ]
    for s in stmts:
        cur.execute(s)
    pg.commit()

# -------------------------
# Migration helpers
# -------------------------
def migrate_table(name, query, insert_sql, convert_row=None):
    try:
        scur.execute(query)
        rows = scur.fetchall()
        for r in rows:
            values = convert_row(r) if convert_row else tuple(r)
            cur.execute(insert_sql, values)
        pg.commit()
        print(f"{name} migrated:", len(rows))
    except Exception as e:
        print(f"Failed migrating {name}: {e}")
        pg.rollback()

# -------------------------
# Start migration
# -------------------------
ensure_schema()
print("Reading sqlite data...")
sconn = sqlite3.connect(sqlite_path)
sconn.row_factory = sqlite3.Row
scur = sconn.cursor()

# Settings
migrate_table(
    "settings",
    "SELECT key, value FROM settings",
    """INSERT INTO settings (key, value)
       VALUES (%s, %s)
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"""
)

# Users
migrate_table(
    "users",
    "SELECT id, username, first_name, last_name, last_seen FROM users",
    """INSERT INTO users (id, username, first_name, last_name, last_seen)
       VALUES (%s,%s,%s,%s,%s)
       ON CONFLICT (id) DO UPDATE
       SET username=EXCLUDED.username,
           first_name=EXCLUDED.first_name,
           last_name=EXCLUDED.last_name,
           last_seen=EXCLUDED.last_seen"""
)

# Sessions
migrate_table(
    "sessions",
    "SELECT id, owner_id, created_at, protect, auto_delete_minutes, title, revoked, header_msg_id, header_chat_id, deep_link FROM sessions",
    """INSERT INTO sessions (id, owner_id, created_at, protect, auto_delete_minutes, title, revoked, header_msg_id, header_chat_id, deep_link)
       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
       ON CONFLICT (id) DO UPDATE
       SET owner_id=EXCLUDED.owner_id,
           created_at=EXCLUDED.created_at,
           protect=EXCLUDED.protect,
           auto_delete_minutes=EXCLUDED.auto_delete_minutes,
           title=EXCLUDED.title,
           revoked=EXCLUDED.revoked,
           header_msg_id=EXCLUDED.header_msg_id,
           header_chat_id=EXCLUDED.header_chat_id,
           deep_link=EXCLUDED.deep_link"""
)

# Files
migrate_table(
    "files",
    "SELECT id, session_id, file_type, file_id, caption, original_msg_id, vault_msg_id FROM files",
    """INSERT INTO files (id, session_id, file_type, file_id, caption, original_msg_id, vault_msg_id)
       VALUES (%s,%s,%s,%s,%s,%s,%s)
       ON CONFLICT (id) DO UPDATE
       SET session_id=EXCLUDED.session_id,
           file_type=EXCLUDED.file_type,
           file_id=EXCLUDED.file_id,
           caption=EXCLUDED.caption,
           original_msg_id=EXCLUDED.original_msg_id,
           vault_msg_id=EXCLUDED.vault_msg_id"""
)

# Delete jobs
def convert_deletejob(r):
    try:
        mids = json.loads(r["message_ids"])
    except Exception:
        mids = []
    return (r["id"], r["session_id"], r["target_chat_id"], json.dumps(mids), r["run_at"], r["created_at"], r["status"])

migrate_table(
    "delete_jobs",
    "SELECT id, session_id, target_chat_id, message_ids, run_at, created_at, status FROM delete_jobs",
    """INSERT INTO delete_jobs (id, session_id, target_chat_id, message_ids, run_at, created_at, status)
       VALUES (%s,%s,%s,%s,%s,%s,%s)
       ON CONFLICT (id) DO UPDATE
       SET session_id=EXCLUDED.session_id,
           target_chat_id=EXCLUDED.target_chat_id,
           message_ids=EXCLUDED.message_ids,
           run_at=EXCLUDED.run_at,
           created_at=EXCLUDED.created_at,
           status=EXCLUDED.status""",
    convert_deletejob
)

print("Migration complete.")
pg.close()
sconn.close()