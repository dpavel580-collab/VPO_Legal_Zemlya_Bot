import os
import json
import hashlib
from datetime import datetime, timezone

import psycopg


class DB:
    """
    Privacy-first analytics DB:
    - stores ONLY hashed user/chat identifiers (no raw IDs)
    - auto-migrates schema to avoid deploy breaks
    - provides lifetime stats (no 2-4h limitation)
    """

    def __init__(self, database_url: str, analytics_salt: str | None = None):
        self.database_url = database_url
        self.analytics_salt = (analytics_salt or os.getenv("ANALYTICS_SALT", "")).strip()
        if not self.database_url:
            raise ValueError("DATABASE_URL is empty")
        self._init_db()

    # -------------------------
    # Hashing (no PII stored)
    # -------------------------
    def _hash_id(self, raw_id: int | str | None) -> str:
        if raw_id is None:
            raw_id = "unknown"
        base = f"{self.analytics_salt}:{raw_id}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    # -------------------------
    # Schema init + migrations
    # -------------------------
    def _init_db(self):
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                # Core events table (keep it simple)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        id BIGSERIAL PRIMARY KEY,
                        ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        user_hash TEXT,
                        chat_hash TEXT,
                        event TEXT NOT NULL,
                        meta JSONB NOT NULL DEFAULT '{}'::jsonb
                    );
                    """
                )

                # Helpful indexes (safe even if some columns are null)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_event ON events(event);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_user_hash ON events(user_hash);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_chat_hash ON events(chat_hash);")

                # MIGRATIONS / SAFETY NET:
                # If earlier schema had NOT NULL constraint on user_hash/chat_hash -> drop it to avoid runtime breaks.
                # (We still fill these fields in code, but this prevents future crashes if something unexpected happens.)
                try:
                    cur.execute("ALTER TABLE events ALTER COLUMN user_hash DROP NOT NULL;")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE events ALTER COLUMN chat_hash DROP NOT NULL;")
                except Exception:
                    pass

                # If someone previously created different column names, we won't delete them, but we ensure ours exist.
                # (No destructive migrations here.)

            conn.commit()

    # -------------------------
    # Logging API
    # -------------------------
    def log_event(self, event: str, meta: dict | None = None, *, user_id=None, chat_id=None):
        meta = meta or {}
        uhash = self._hash_id(user_id)
        chash = self._hash_id(chat_id)

        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO events (ts, user_hash, chat_hash, event, meta)
                    VALUES (NOW(), %s, %s, %s, %s::jsonb);
                    """,
                    (uhash, chash, event, json.dumps(meta, ensure_ascii=False)),
                )
            conn.commit()

    def log_event_from_update(self, update, event: str, meta: dict | None = None):
        """
        Safe helper: extracts IDs from telegram Update without saving raw IDs.
        """
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

    # -------------------------
    # Stats (lifetime)
    # -------------------------
    def get_lifetime_stats(self) -> dict:
        """
        Returns lifetime aggregated counts (no personal data).
        """
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM events;")
                total_events = cur.fetchone()[0]

                cur.execute("SELECT COUNT(DISTINCT user_hash) FROM events WHERE user_hash IS NOT NULL;")
                unique_users = cur.fetchone()[0]

                cur.execute("SELECT COUNT(DISTINCT chat_hash) FROM events WHERE chat_hash IS NOT NULL;")
                unique_chats = cur.fetchone()[0]

                cur.execute(
                    """
                    SELECT event, COUNT(*) as cnt
                    FROM events
                    GROUP BY event
                    ORDER BY cnt DESC
                    LIMIT 30;
                    """
                )
                top_events = [{"event": r[0], "count": r[1]} for r in cur.fetchall()]

        return {
            "total_events": total_events,
            "unique_users": unique_users,
            "unique_chats": unique_chats,
            "top_events": top_events,
        }
