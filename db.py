import os
import logging
import hashlib
from typing import Optional, Dict, Any

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
    Нічого не мігрує.
    1) Визначає, як називається колонка події в events: event_type або event.
    2) Гарантує існування таблиці user_state (для вибору адвоката/стану).
       Якщо таблиця вже є — просто ок.
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

                # таблиця стану користувача (для адвокатів/кнопок)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS user_state (
                    user_hash TEXT PRIMARY KEY,
                    lawyer_id TEXT,
                    state TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
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
    Пише подію в events. Ніколи не валить бота.
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
                    # якщо events нема/інша схема — просто не пишемо
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
    Запам'ятовує вибраного адвоката для користувача.
    """
    if not _db_available():
        return

    if not u_hash:
        return

    if lawyer_id is not None:
        lawyer_id = str(lawyer_id).strip() or None

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
    """
    Допоміжне: дістати адвоката, якщо bot.py це використовує.
    """
    if not _db_available():
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
    """
    Якщо bot.py веде діалог станами — це теж стане в пригоді.
    """
    if not _db_available():
        return

    if state is not None:
        state = str(state).strip() or None

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
    if not _db_available():
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
