import os
import hashlib
from typing import Optional, Dict, Any

import psycopg


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ANALYTICS_SALT = os.getenv("ANALYTICS_SALT", "").strip()

if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")
if not ANALYTICS_SALT:
    raise RuntimeError("Missing ANALYTICS_SALT")


# ---------- privacy-safe user hash ----------
def user_hash(telegram_user_id: int) -> str:
    raw = f"{ANALYTICS_SALT}:{telegram_user_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# ---------- helpers ----------
def _col_exists(cur, table: str, col: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name=%s AND column_name=%s
        LIMIT 1;
        """,
        (table, col),
    )
    return cur.fetchone() is not None


# ---------- schema + migrations ----------
def init_db() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # 1) Ensure tables exist (fresh install)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
                    user_hash TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    category TEXT NOT NULL
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lawyer_bindings (
                    category TEXT PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_binds (
                    token TEXT PRIMARY KEY,
                    category TEXT NOT NULL,
                    lawyer_chat_id BIGINT NOT NULL,
                    lawyer_user_id BIGINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
                );
                """
            )

            # 2) Migrate "bad" historical schemas for events
            # Sometimes older code used column name `event` instead of `event_type`.
            if _col_exists(cur, "events", "event") and not _col_exists(cur, "events", "event_type"):
                cur.execute('ALTER TABLE events RENAME COLUMN "event" TO event_type;')

            # Ensure required columns exist
            if not _col_exists(cur, "events", "event_type"):
                cur.execute("ALTER TABLE events ADD COLUMN event_type TEXT;")

            if not _col_exists(cur, "events", "category"):
                cur.execute("ALTER TABLE events ADD COLUMN category TEXT;")

            if not _col_exists(cur, "events", "user_hash"):
                cur.execute("ALTER TABLE events ADD COLUMN user_hash TEXT;")

            if not _col_exists(cur, "events", "ts"):
                cur.execute(
                    "ALTER TABLE events ADD COLUMN ts TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc');"
                )

            # Fix possible NULLs from earlier broken inserts
            cur.execute("UPDATE events SET event_type='unknown' WHERE event_type IS NULL;")
            cur.execute("UPDATE events SET category='UNKNOWN' WHERE category IS NULL;")
            cur.execute("UPDATE events SET user_hash='unknown' WHERE user_hash IS NULL;")

            # Enforce NOT NULL (if column was added without constraints)
            # This can fail if there are still NULLs; we updated them above.
            cur.execute("ALTER TABLE events ALTER COLUMN event_type SET NOT NULL;")
            cur.execute("ALTER TABLE events ALTER COLUMN category SET NOT NULL;")
            cur.execute("ALTER TABLE events ALTER COLUMN user_hash SET NOT NULL;")

            # 3) Indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type_cat ON events(event_type, category);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pending_created ON pending_binds(created_at);")

        conn.commit()


# ---------- events ----------
def add_event(u_hash: str, event_type: str, category: str) -> None:
    # Hard safety: never allow null/empty to crash inserts
    u_hash = (u_hash or "unknown").strip()
    event_type = (event_type or "unknown").strip()
    category = (category or "UNKNOWN").strip()

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO events(user_hash, event_type, category) VALUES (%s, %s, %s);",
                (u_hash, event_type, category),
            )
        conn.commit()


def get_stats(days: int = 7) -> Dict[str, Any]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT user_hash)
                FROM events
                WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval;
                """,
                (days,),
            )
            unique_users = int(cur.fetchone()[0])

            def fetch_map(evtype: str) -> Dict[str, int]:
                cur.execute(
                    """
                    SELECT category, COUNT(*)
                    FROM events
                    WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval
                      AND event_type=%s
                    GROUP BY category
                    ORDER BY COUNT(*) DESC;
                    """,
                    (days, evtype),
                )
                return {k: int(v) for k, v in cur.fetchall()}

            return {
                "days": days,
                "unique_users": unique_users,
                "pick_advocate": fetch_map("pick_advocate"),
                "request_click": fetch_map("request_click"),
                "request_sent": fetch_map("request_sent"),
                "asked_question": fetch_map("asked_question"),
            }


# ---------- lawyer binding ----------
def set_lawyer(category: str, chat_id: int) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lawyer_bindings(category, chat_id)
                VALUES (%s, %s)
                ON CONFLICT(category)
                DO UPDATE SET chat_id=EXCLUDED.chat_id, updated_at=(NOW() AT TIME ZONE 'utc');
                """,
                (category, chat_id),
            )
        conn.commit()


def get_lawyer(category: str) -> Optional[int]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id FROM lawyer_bindings WHERE category=%s;", (category,))
            row = cur.fetchone()
            return int(row[0]) if row else None


# ---------- pending binds ----------
def create_pending_bind(token: str, category: str, lawyer_chat_id: int, lawyer_user_id: int) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pending_binds(token, category, lawyer_chat_id, lawyer_user_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(token) DO NOTHING;
                """,
                (token, category, lawyer_chat_id, lawyer_user_id),
            )
        conn.commit()


def get_pending_bind(token: str) -> Optional[Dict[str, Any]]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT token, category, lawyer_chat_id, lawyer_user_id
                FROM pending_binds
                WHERE token=%s;
                """,
                (token,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "token": row[0],
                "category": row[1],
                "lawyer_chat_id": int(row[2]),
                "lawyer_user_id": int(row[3]),
            }


def delete_pending_bind(token: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pending_binds WHERE token=%s;", (token,))
        conn.commit()
