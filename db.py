# db.py
import os
import json
import time
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


def _now_ts() -> int:
    return int(time.time())


class DB:
    def __init__(self, database_url: str):
        self.database_url = database_url.strip()
        if not self.database_url:
            raise ValueError("DATABASE_URL is empty")
        self._init_db()

    @contextmanager
    def _conn(self):
        # autocommit True to avoid "idle in transaction"
        con = psycopg.connect(self.database_url, autocommit=True, row_factory=dict_row)
        try:
            yield con
        finally:
            con.close()

    def _init_db(self):
        with self._conn() as con:
            with con.cursor() as cur:
                # events for analytics (NO personal data required)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id BIGSERIAL PRIMARY KEY,
                    ts BIGINT NOT NULL,
                    event TEXT NOT NULL,
                    meta JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_event ON events(event);")

                # optional: store lawyer bindings (if you use it)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS lawyer_bindings (
                    category TEXT PRIMARY KEY,
                    lawyer_chat_id BIGINT NOT NULL,
                    bound_ts BIGINT NOT NULL
                );
                """)

                # optional: bind tokens (approve/reject)
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
                    "INSERT INTO events(ts, event, meta) VALUES (%s, %s, %s::jsonb)",
                    (_now_ts(), event, json.dumps(meta, ensure_ascii=False)),
                )

    def stats_all_time(self) -> list[dict]:
        # returns list of {"event": "...", "cnt": N}
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute("""
                SELECT event, COUNT(*)::BIGINT AS cnt
                FROM events
                GROUP BY event
                ORDER BY cnt DESC, event ASC;
                """)
                return cur.fetchall()

    # ---------- lawyer bind flow (optional) ----------
    def create_bind_token(self, category: str, requester_chat_id: int, requester_user_id: int) -> str:
        token = f"{int(time.time())}{requester_chat_id}{requester_user_id}"
        # short token (not crypto), good enough for internal flow
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
                row = cur.fetchone()
                return row

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
                if not row:
                    return None
                return int(row["lawyer_chat_id"])
