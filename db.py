import os
import logging
import hashlib
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import uuid

import psycopg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
HASH_SALT = os.getenv("HASH_SALT", "").strip()

_EVENT_COL: Optional[str] = None  # event_type або event


# ----------------- users / hashing -----------------

def user_hash(user_id: Optional[int]) -> str:
    base = f"{user_id or 0}:{HASH_SALT}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _db_available() -> bool:
    return bool(DATABASE_URL)


# ----------------- init -----------------

def init_db() -> None:
    """
    1) Визначає як називається колонка події в events: event_type або event.
    2) Створює user_state (вибір адвоката/стан).
    3) Створює pending_binds (тимчасові "прив'язки" — наприклад для платежів/зв’язку).
    """
    global _EVENT_COL

    if not _db_available():
        logger.warning("DATABASE_URL is not set. DB disabled.")
        _EVENT_COL = None
        return

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # визначаємо колонку в events
                try:
                    cur.execute("SELECT event_type FROM events LIMIT 1;")
                    _EVENT_COL = "event_type"
                except Exception:
                    conn.rollback()
                    try:
                        cur.execute("SELECT event FROM events LIMIT 1;")
                        _EVENT_COL = "event"
                    except Exception:
                        conn.rollback()
                        _EVENT_COL = None

                # таблиця стану користувача
                cur.execute("""
                CREATE TABLE IF NOT EXISTS user_state (
                    user_hash TEXT PRIMARY KEY,
                    lawyer_id TEXT,
                    state TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                # pending binds (тимчасові дані/токени)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS pending_binds (
                    token TEXT PRIMARY KEY,
                    user_hash TEXT NOT NULL,
                    bind_type TEXT NOT NULL,
                    payload TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

            conn.commit()

        logger.info("DB ok. events column=%s", _EVENT_COL)
    except Exception as e:
        logger.exception("init_db failed (ignored): %s", e)
        _EVENT_COL = None


# ----------------- events -----------------

def add_event(u_hash: str, event_type: Optional[str], category: Optional[str]) -> None:
    """
    Пише подію в events. НІКОЛИ не валить бота.
    """
    global _EVENT_COL

    if not _db_available():
        return

    safe_event = (event_type or "").strip() or "unknown"
    safe_cat = (category or "").strip() or "UNKNOWN"

    if _EVENT_COL is None:
        init_db()

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                if _EVENT_COL == "event_type":
                    cur.execute(
                        "INSERT INTO events(user_hash, event_type, category) VALUES (%s, %s, %s);",
                        (u_hash, safe_event, safe_cat),
                    )
                elif _EVENT_COL == "event":
                    cur.execute(
                        "INSERT INTO events(user_hash, event, category) VALUES (%s, %s, %s);",
                        (u_hash, safe_event, safe_cat),
                    )
                else:
                    return
            conn.commit()
    except Exception as e:
        logger.exception("add_event failed (ignored): %s", e)


def get_stats(days: int = 7) -> dict:
    empty = {
        "days": days,
        "unique_users": 0,
        "pick_advocate": {},
        "request_click": {},
        "request_sent": {},
        "asked_question": {},
    }

    if not _db_available():
        return empty

    global _EVENT_COL
    if _EVENT_COL is None:
        init_db()
        if _EVENT_COL is None:
            return empty

    try:
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
                unique_users = int(cur.fetchone()[0] or 0)

                def fetch_map(evtype: str):
                    cur.execute(
                        f"""
                        SELECT COALESCE(category, 'UNKNOWN') as category, COUNT(*)
                        FROM events
                        WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval
                          AND {_EVENT_COL} = %s
                        GROUP BY COALESCE(category, 'UNKNOWN')
                        ORDER BY COUNT(*) DESC;
                        """,
                        (days, evtype),
                    )
                    return {str(k): int(v) for k, v in cur.fetchall()}

                return {
                    "days": days,
                    "unique_users": unique_users,
                    "pick_advocate": fetch_map("pick_advocate"),
                    "request_click": fetch_map("request_click"),
                    "request_sent": fetch_map("request_sent"),
                    "asked_question": fetch_map("asked_question"),
                }
    except Exception as e:
        logger.exception("get_stats failed (ignored): %s", e)
        return empty


# ----------------- lawyer selection / user state -----------------

def set_lawyer(u_hash: str, lawyer_id: Optional[str]) -> None:
    """
    bot.py імпортує це.
    """
    if not _db_available() or not u_hash:
        return

    lawyer_id = (str(lawyer_id).strip() if lawyer_id is not None else None) or None

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO user_state(user_hash, lawyer_id, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_hash)
                DO UPDATE SET lawyer_id = EXCLUDED.lawyer_id, updated_at = NOW();
                """, (u_hash, lawyer_id))
            conn.commit()
    except Exception as e:
        logger.exception("set_lawyer failed (ignored): %s", e)


def get_lawyer(u_hash: str) -> Optional[str]:
    if not _db_available() or not u_hash:
        return None

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT lawyer_id FROM user_state WHERE user_hash=%s;", (u_hash,))
                row = cur.fetchone()
                return (row[0] if row else None)
    except Exception as e:
        logger.exception("get_lawyer failed (ignored): %s", e)
        return None


def set_state(u_hash: str, state: Optional[str]) -> None:
    if not _db_available() or not u_hash:
        return

    state = (str(state).strip() if state is not None else None) or None

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO user_state(user_hash, state, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_hash)
                DO UPDATE SET state = EXCLUDED.state, updated_at = NOW();
                """, (u_hash, state))
            conn.commit()
    except Exception as e:
        logger.exception("set_state failed (ignored): %s", e)


def get_state(u_hash: str) -> Optional[str]:
    if not _db_available() or not u_hash:
        return None

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT state FROM user_state WHERE user_hash=%s;", (u_hash,))
                row = cur.fetchone()
                return (row[0] if row else None)
    except Exception as e:
        logger.exception("get_state failed (ignored): %s", e)
        return None


# ----------------- pending bind -----------------

def create_pending_bind(*args, **kwargs) -> str:
    """
    bot.py імпортує це.
    Зроблено максимально сумісно: приймає будь-які аргументи.
    Повертає token (рядок).
    Очікуваний сенс: створити тимчасовий запис прив'язки/платежу/дії.
    """

    # Спроба витягнути u_hash і bind_type з різних варіантів виклику:
    u_hash = kwargs.get("u_hash") or kwargs.get("user_hash")
    bind_type = kwargs.get("bind_type") or kwargs.get("type") or kwargs.get("kind")
    payload = kwargs.get("payload") or kwargs.get("data")

    # Якщо прийшли позиційні:
    # варіант 1: (u_hash, bind_type, payload)
    if len(args) >= 1 and not u_hash:
        u_hash = args[0]
    if len(args) >= 2 and not bind_type:
        bind_type = args[1]
    if len(args) >= 3 and payload is None:
        payload = args[2]

    u_hash = (str(u_hash).strip() if u_hash is not None else "") or ""
    bind_type = (str(bind_type).strip() if bind_type is not None else "") or "generic"
    payload_str = None if payload is None else str(payload)

    token = uuid.uuid4().hex

    if not _db_available():
        return token

    try:
        init_db()
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO pending_binds(token, user_hash, bind_type, payload)
                VALUES (%s, %s, %s, %s);
                """, (token, u_hash, bind_type, payload_str))
            conn.commit()
    except Exception as e:
        logger.exception("create_pending_bind failed (ignored): %s", e)

    return token


def get_pending_bind(token: str) -> Optional[Dict[str, Any]]:
    if not _db_available() or not token:
        return None

    try:
        init_db()
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT token, user_hash, bind_type, payload, created_at
                FROM pending_binds
                WHERE token=%s
                """, (token,))
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "token": row[0],
                    "user_hash": row[1],
                    "bind_type": row[2],
                    "payload": row[3],
                    "created_at": row[4].isoformat() if row[4] else None
                }
    except Exception as e:
        logger.exception("get_pending_bind failed (ignored): %s", e)
        return None


def clear_pending_bind(token: str) -> None:
    if not _db_available() or not token:
        return

    try:
        init_db()
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pending_binds WHERE token=%s;", (token,))
            conn.commit()
    except Exception as e:
        logger.exception("clear_pending_bind failed (ignored): %s", e)
