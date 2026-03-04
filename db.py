# db.py
import os
import hashlib
from typing import Optional, Dict, Any, Tuple

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")


# ----------------------------
# helpers
# ----------------------------
_EVENT_COL_CACHE: Optional[str] = None


def _get_event_col(conn) -> str:
    """
    Detects whether events table uses column 'event_type' or 'event'.
    Caches result.
    """
    global _EVENT_COL_CACHE
    if _EVENT_COL_CACHE:
        return _EVENT_COL_CACHE

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='events'
              AND column_name IN ('event_type', 'event')
            ORDER BY CASE column_name WHEN 'event_type' THEN 0 ELSE 1 END
            LIMIT 1;
            """
        )
        row = cur.fetchone()

    # Default to event_type if not found (we will create table with event_type in init_db)
    _EVENT_COL_CACHE = (row[0] if row else "event_type")
    return _EVENT_COL_CACHE


def user_hash(tg_user_id: int) -> str:
    """
    Stable user hash to avoid storing raw telegram IDs.
    """
    s = str(tg_user_id).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


# ----------------------------
# DB init / migrations
# ----------------------------
def init_db() -> None:
    """
    Creates required tables if they do not exist.
    Does NOT drop anything.
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # events: store bot analytics
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
                    user_hash TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    category TEXT,
                    meta JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )

            # lawyers: mapping category -> contacts string (or any payload you want)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lawyers (
                    category TEXT PRIMARY KEY,
                    contact TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
                );
                """
            )

            # pending binds: temporary token -> (category, contact)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_binds (
                    token TEXT PRIMARY KEY,
                    category TEXT,
                    contact TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
                );
                """
            )

        conn.commit()

    # reset cache after possible table creation
    global _EVENT_COL_CACHE
    _EVENT_COL_CACHE = None


# ----------------------------
# events
# ----------------------------
def add_event(u_hash: str, event_type: str, category: str, meta: Optional[Dict[str, Any]] = None) -> None:
    """
    Inserts analytics event.
    Compatible with DB schemas that may use 'event_type' or 'event' column name.
    """
    meta = meta or {}

    with psycopg.connect(DATABASE_URL) as conn:
        ev_col = _get_event_col(conn)
        with conn.cursor() as cur:
            if ev_col == "event_type":
                cur.execute(
                    """
                    INSERT INTO events(user_hash, event_type, category, meta)
                    VALUES (%s, %s, %s, %s::jsonb);
                    """,
                    (u_hash, event_type, category, psycopg.types.json.Json(meta)),
                )
            else:
                # legacy schema with 'event' column
                cur.execute(
                    """
                    INSERT INTO events(user_hash, event, category, meta)
                    VALUES (%s, %s, %s, %s::jsonb);
                    """,
                    (u_hash, event_type, category, psycopg.types.json.Json(meta)),
                )
        conn.commit()


def get_stats(days: int = 7) -> dict:
    """
    Returns:
      unique_users
      pick_advocate/request_click/request_sent/asked_question maps by category
    """
    with psycopg.connect(DATABASE_URL) as conn:
        ev_col = _get_event_col(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT user_hash)
                FROM events
                WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval;
                """,
                (days,),
            )
            unique_users = int(cur.fetchone()[0])

            def fetch_map(evtype: str) -> Dict[str, int]:
                cur.execute(
                    f"""
                    SELECT COALESCE(category, 'UNKNOWN') AS category, COUNT(*)::int
                    FROM events
                    WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval
                      AND {ev_col} = %s
                    GROUP BY COALESCE(category, 'UNKNOWN')
                    ORDER BY COUNT(*) DESC;
                    """,
                    (days, evtype),
                )
                return {k: int(v) for (k, v) in cur.fetchall()}

            return {
                "days": days,
                "unique_users": unique_users,
                "pick_advocate": fetch_map("pick_advocate"),
                "request_click": fetch_map("request_click"),
                "request_sent": fetch_map("request_sent"),
                "asked_question": fetch_map("asked_question"),
            }


# ----------------------------
# lawyers (category -> contact)
# ----------------------------
def set_lawyer(category: str, contact: str) -> None:
    """
    Stores/updates lawyer contact for a category.
    """
    category = (category or "").strip()
    contact = (contact or "").strip()
    if not category:
        raise ValueError("category is required")
    if not contact:
        raise ValueError("contact is required")

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lawyers(category, contact)
                VALUES (%s, %s)
                ON CONFLICT (category)
                DO UPDATE SET contact = EXCLUDED.contact,
                              updated_at = (NOW() AT TIME ZONE 'utc');
                """,
                (category, contact),
            )
        conn.commit()


def get_lawyer(category: str) -> Optional[str]:
    """
    Returns contact string for category or None.
    """
    category = (category or "").strip()
    if not category:
        return None

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT contact FROM lawyers WHERE category=%s;",
                (category,),
            )
            row = cur.fetchone()
            return (row[0] if row else None)


# ----------------------------
# pending binds (token -> category/contact)
# ----------------------------
def create_pending_bind(token: str, category: Optional[str] = None, contact: Optional[str] = None) -> None:
    """
    Creates/overwrites pending bind token.
    bot.py may call it with 1..3 args, so we keep category/contact optional.
    """
    token = (token or "").strip()
    if not token:
        raise ValueError("token is required")

    category = (category or "").strip() if category is not None else None
    contact = (contact or "").strip() if contact is not None else None

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pending_binds(token, category, contact)
                VALUES (%s, %s, %s)
                ON CONFLICT (token)
                DO UPDATE SET category = EXCLUDED.category,
                              contact = EXCLUDED.contact,
                              created_at = (NOW() AT TIME ZONE 'utc');
                """,
                (token, category, contact),
            )
        conn.commit()


def get_pending_bind(token: str) -> Optional[Dict[str, Any]]:
    """
    Returns dict: {token, category, contact, created_at} or None
    """
    token = (token or "").strip()
    if not token:
        return None

    with psycopg.connect(DATABASE_URL) as conn:
        conn.row_factory = dict_row
        with conn.cursor() as cur:
            cur.execute(
                "SELECT token, category, contact, created_at FROM pending_binds WHERE token=%s;",
                (token,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def delete_pending_bind(token: str) -> None:
    """
    Deletes pending bind token (idempotent).
    """
    token = (token or "").strip()
    if not token:
        return

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pending_binds WHERE token=%s;", (token,))
        conn.commit()
