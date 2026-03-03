import os
import json
import hashlib
import psycopg


class DB:
    """
    Analytics DB V2 (safe):
    - Writes ONLY into events_v2 (new table)
    - Ignores legacy events table (which has old NOT NULL constraints like event_type)
    - Stores only hashed user/chat identifiers (no raw IDs)
    """

    def __init__(self, database_url: str, analytics_salt: str | None = None):
        self.database_url = (database_url or "").strip()
        if not self.database_url:
            raise ValueError("DATABASE_URL is empty")

        self.analytics_salt = (analytics_salt or os.getenv("ANALYTICS_SALT", "")).strip()
        self._init_db()

    def _hash_id(self, raw_id):
        if raw_id is None:
            raw_id = "unknown"
        base = f"{self.analytics_salt}:{raw_id}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    def _init_db(self):
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                # NEW clean table (no legacy constraints)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events_v2 (
                        id BIGSERIAL PRIMARY KEY,
                        ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        user_hash TEXT,
                        chat_hash TEXT,
                        event TEXT NOT NULL,
                        meta JSONB NOT NULL DEFAULT '{}'::jsonb
                    );
                    """
                )

                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_v2_ts ON events_v2(ts);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_v2_event ON events_v2(event);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_v2_user_hash ON events_v2(user_hash);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_v2_chat_hash ON events_v2(chat_hash);")

            conn.commit()

    def log_event(self, event: str, meta: dict | None = None, *, user_id=None, chat_id=None):
        meta = meta or {}
        uhash = self._hash_id(user_id)
        chash = self._hash_id(chat_id)

        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO events_v2 (ts, user_hash, chat_hash, event, meta)
                    VALUES (NOW(), %s, %s, %s, %s::jsonb);
                    """,
                    (uhash, chash, event, json.dumps(meta, ensure_ascii=False)),
                )
            conn.commit()

    def log_event_from_update(self, update, event: str, meta: dict | None = None):
        user_id = None
        chat_id = None

        try:
            if update is not None and getattr(update, "effective_user", None):
                user_id = update.effective_user.id
        except Exception:
            user_id = None

        try:
            if update is not None and getattr(update, "effective_chat", None):
                chat_id = update.effective_chat.id
        except Exception:
            chat_id = None

        self.log_event(event, meta or {}, user_id=user_id, chat_id=chat_id)

    def get_lifetime_stats(self) -> dict:
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM events_v2;")
                total_events = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(DISTINCT user_hash) FROM events_v2 WHERE user_hash IS NOT NULL;")
                unique_users = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(DISTINCT chat_hash) FROM events_v2 WHERE chat_hash IS NOT NULL;")
                unique_chats = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT event, COUNT(*) as cnt
                    FROM events_v2
                    GROUP BY event
                    ORDER BY cnt DESC
                    LIMIT 30;
                    """
                )
                top_events = [{"event": r[0], "count": int(r[1])} for r in cur.fetchall()]

        return {
            "total_events": total_events,
            "unique_users": unique_users,
            "unique_chats": unique_chats,
            "top_events": top_events,
        }
