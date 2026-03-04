import os
import logging
import hashlib
from typing import Any, Dict, Optional

import psycopg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
HASH_SALT = os.getenv("HASH_SALT", "").strip()  # можна не ставити, але бажано

# кешимо назву колонки події, щоб не робити перевірку щоразу
_EVENT_COL: Optional[str] = None


def user_hash(user_id: Optional[int]) -> str:
    """
    bot.py це імпортує. Повертає стабільний хеш користувача.
    """
    base = f"{user_id or 0}:{HASH_SALT}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _db_available() -> bool:
    return bool(DATABASE_URL)


def init_db() -> None:
    """
    Нічого не створюємо і не мігруємо.
    Лише визначаємо, як у твоїй таблиці називається колонка події: event_type або event.
    """
    global _EVENT_COL

    if not _db_available():
        logger.warning("DATABASE_URL is not set. DB disabled.")
        _EVENT_COL = None
        return

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT event_type FROM events LIMIT 1;")
                    _EVENT_COL = "event_type"
                except Exception:
                    conn.rollback()
                    cur.execute("SELECT event FROM events LIMIT 1;")
                    _EVENT_COL = "event"

        logger.info("DB ok. Using events.%s", _EVENT_COL)
    except Exception as e:
        logger.exception("init_db failed (ignored): %s", e)
        _EVENT_COL = None


def add_event(u_hash: str, event_type: Optional[str], category: Optional[str]) -> None:
    """
    Пише подію в events.
    ГОЛОВНЕ: не допускає NULL у critical полях і НІКОЛИ не валить бота.
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
                    # якщо не змогли визначити схему — просто не пишемо
                    return
            conn.commit()
    except Exception as e:
        logger.exception("add_event failed (ignored): %s", e)


def get_stats(days: int = 7) -> dict:
    """
    Статистика. Якщо БД/схема не готова — повертає пусте, бот живий.
    """
    global _EVENT_COL

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
