# db.py
import json
import time
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


def _now_ts() -> int:
    return int(time.time())


class DB:
    def __init__(self, database_url: str):
        self.database_url = (database_url or "").strip()
        if not self.database_url:
            raise ValueError("DATABASE_URL is empty")
        self._init_db()

    @contextmanager
    def _conn(self):
        con = psycopg.connect(self.database_url, autocommit=True, row_factory=dict_row)
        try:
            yield con
        finally:
            con.close()

    def _table_exists(self, cur, table: str) -> bool:
        cur.execute(
            "SELECT to_regclass(%s) AS reg",
            (table,),
        )
        row = cur.fetchone()
        return bool(row and row["reg"])

    def _column_exists(self, cur, table: str, column: str) -> bool:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (table, column),
        )
        return cur.fetchone() is not None

    def _init_db(self):
        with self._conn() as con:
            with con.cursor() as cur:
                # ---------- EVENTS table: create if not exists ----------
                cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id BIGSERIAL PRIMARY KEY,
                    ts BIGINT NOT NULL,
                    event TEXT NOT NULL,
                    meta JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """)

                # ---------- MIGRATION for existing old schema ----------
                # If old table existed without "event" column -> add it
                # and give a default value to old rows.
                if not self._column_exists(cur, "events", "event"):
                    cur.execute("ALTER TABLE events ADD COLUMN event TEXT;")
                    # make it NOT NULL safely
                    cur.execute("UPDATE events SET event = 'unknown' WHERE event IS NULL;")
                    cur.execute("ALTER TABLE events ALTER COLUMN event SET NOT NULL;")

                # If old table existed without "meta" column -> add it
                if not self._column_exists(cur, "events", "meta"):
                    cur.execute("ALTER TABLE events ADD COLUMN meta JSONB NOT NULL DEFAULT '{}'::jsonb;")

                # If old table existed without "ts" column -> add it
                if not self._column_exists(cur, "events", "ts"):
                    cur.execute("ALTER TABLE events ADD COLUMN ts BIGINT;")
                    cur.execute(f"UPDATE events SET ts = {_now_ts()} WHERE ts IS NULL;")
                    cur.execute("ALTER TABLE events ALTER COLUMN ts SET NOT NULL;")

                # Indexes (after ensuring columns exist)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_event ON events(event);")

                # ---------- lawyer bindings ----------
                cur.execute("""
                CREATE TABLE IF NOT EXISTS lawyer_bindings (
                    category TEXT PRIMARY KEY,
                    lawyer_chat_id BIGINT NOT NULL,
                    bound_ts BIGINT NOT NULL
                );
                """)

                # ---------- bind tokens ----------
                cur.execute("""
                CREATE TABLE IF NOT EXISTS bind_tokens (
                    token TEXT PRIMARY KEY,
                    category TEXT NOT NULL,
                    requester_chat_id BIGINT NOT NULL,
                    requester_user_id BIGINT NOT NULL,
                    ts BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING'
                );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_bind_tokens_ts ON bind_tokens(ts);")

    # ---------- analytics ----------
    def log_event(self, event: str, meta: dict | None = None):
        if not event:
            return
        meta = meta or {}
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute(
    "INSERT INTO events(ts, event, meta) VALUES (NOW(), %s, %s::jsonb)",
    (event, json.dumps(meta))
)

    def stats_all_time(self) -> list[dict]:
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute("""
                SELECT event, COUNT(*)::BIGINT AS cnt
                FROM events
                GROUP BY event
                ORDER BY cnt DESC, event ASC;
                """)
                return cur.fetchall()

    # ---------- lawyer bind flow ----------
    def create_bind_token(self, category: str, requester_chat_id: int, requester_user_id: int) -> str:
        token = f"{int(time.time())}{requester_chat_id}{requester_user_id}"
        token = str(abs(hash(token)))[:12]
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute("""
                INSERT INTO bind_tokens(token, category, requester_chat_id, requester_user_id, ts, status)
                VALUES (%s, %s, %s, %s, %s, 'PENDING')
                ON CONFLICT (token) DO NOTHING
                """, (token, category, requester_chat_id, requester_user_id, _now_ts()))
        return token

    def get_bind_token(self, token: str) -> dict | None:
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute("SELECT * FROM bind_tokens WHERE token=%s", (token,))
                return cur.fetchone()

    def mark_bind_token(self, token: str, status: str):
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute("UPDATE bind_tokens SET status=%s WHERE token=%s", (status, token))

    def bind_lawyer(self, category: str, lawyer_chat_id: int):
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute("""
                INSERT INTO lawyer_bindings(category, lawyer_chat_id, bound_ts)
                VALUES (%s, %s, %s)
                ON CONFLICT (category) DO UPDATE SET
                    lawyer_chat_id = EXCLUDED.lawyer_chat_id,
                    bound_ts = EXCLUDED.bound_ts
                """, (category, lawyer_chat_id, _now_ts()))

    def get_lawyer_chat_id(self, category: str) -> int | None:
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute("SELECT lawyer_chat_id FROM lawyer_bindings WHERE category=%s", (category,))
                row = cur.fetchone()
                return int(row["lawyer_chat_id"]) if row else None
