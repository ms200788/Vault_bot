
#!/usr/bin/env python3
"""
migrate_sqlite_to_neon.py

Usage: set environment variable NEON_DB_URL and path to existing sqlite file,
then run this script to copy data into Postgres.

Example:
NEON_DB_URL="postgresql://user:pass@host:port/dbname" python migrate_sqlite_to_neon.py /path/to/database.sqlite3
"""
import os
import sys
import json
import sqlite3
import psycopg2
import psycopg2.extras
from psycopg2 import sql as psql

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
pg.autocommit = False
cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Ensure schema (simple)
def ensure_schema():
    stmts = []
    stmts.append("""CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);""")
    stmts.append("""CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, last_seen TIMESTAMPTZ);""")
    stmts.append("""CREATE TABLE IF NOT EXISTS sessions (id BIGSERIAL PRIMARY KEY, owner_id BIGINT, created_at TIMESTAMPTZ, protect INTEGER DEFAULT 0, auto_delete_minutes INTEGER DEFAULT 0, title TEXT, revoked INTEGER DEFAULT 0, header_msg_id BIGINT, header_chat_id BIGINT, deep_link TEXT);""")
    stmts.append("""CREATE TABLE IF NOT EXISTS files (id BIGSERIAL PRIMARY KEY, session_id BIGINT, file_type TEXT, file_id TEXT, caption TEXT, original_msg_id BIGINT, vault_msg_id BIGINT);""")
    stmts.append("""CREATE TABLE IF NOT EXISTS delete_jobs (id BIGSERIAL PRIMARY KEY, session_id BIGINT, target_chat_id BIGINT, message_ids JSONB, run_at TIMESTAMPTZ, created_at TIMESTAMPTZ, status TEXT DEFAULT 'scheduled');""")
    for s in stmts:
        cur.execute(s)
    pg.commit()

ensure_schema()
print("Reading sqlite data...")
sconn = sqlite3.connect(sqlite_path)
sconn.row_factory = sqlite3.Row
scur = sconn.cursor()

# Migrate settings
scur.execute("SELECT key, value FROM settings")
rows = scur.fetchall()
for r in rows:
    cur.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (r["key"], r["value"]))
pg.commit()
print("Settings migrated:", len(rows))

# Migrate users
scur.execute("SELECT id, username, first_name, last_name, last_seen FROM users")
rows = scur.fetchall()
for r in rows:
    cur.execute("INSERT INTO users (id, username, first_name, last_name, last_seen) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, last_seen=EXCLUDED.last_seen", (r["id"], r["username"], r["first_name"], r["last_name"], r["last_seen"]))
pg.commit()
print("Users migrated:", len(rows))

# Migrate sessions
scur.execute("SELECT id, owner_id, created_at, protect, auto_delete_minutes, title, revoked, header_msg_id, header_chat_id, deep_link FROM sessions")
rows = scur.fetchall()
for r in rows:
    cur.execute("INSERT INTO sessions (id, owner_id, created_at, protect, auto_delete_minutes, title, revoked, header_msg_id, header_chat_id, deep_link) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET owner_id=EXCLUDED.owner_id", (r["id"], r["owner_id"], r["created_at"], r["protect"], r["auto_delete_minutes"], r["title"], r["revoked"], r["header_msg_id"], r["header_chat_id"], r["deep_link"]))
pg.commit()
print("Sessions migrated:", len(rows))

# Migrate files
scur.execute("SELECT id, session_id, file_type, file_id, caption, original_msg_id, vault_msg_id FROM files")
rows = scur.fetchall()
for r in rows:
    cur.execute("INSERT INTO files (id, session_id, file_type, file_id, caption, original_msg_id, vault_msg_id) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET session_id=EXCLUDED.session_id", (r["id"], r["session_id"], r["file_type"], r["file_id"], r["caption"], r["original_msg_id"], r["vault_msg_id"]))
pg.commit()
print("Files migrated:", len(rows))

# Migrate delete_jobs
scur.execute("SELECT id, session_id, target_chat_id, message_ids, run_at, created_at, status FROM delete_jobs")
rows = scur.fetchall()
for r in rows:
    # message_ids is stored as JSON in sqlite; keep as JSONB
    try:
        mids = json.loads(r["message_ids"])
    except Exception:
        mids = []
    cur.execute("INSERT INTO delete_jobs (id, session_id, target_chat_id, message_ids, run_at, created_at, status) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET session_id=EXCLUDED.session_id", (r["id"], r["session_id"], r["target_chat_id"], json.dumps(mids), r["run_at"], r["created_at"], r["status"]))
pg.commit()
print("Delete jobs migrated:", len(rows))

print("Migration complete.")
pg.close()
sconn.close()
