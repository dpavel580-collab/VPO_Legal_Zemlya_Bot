import os
import logging
from typing import Dict, Any

import psycopg

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    logger.warning("DATABASE_URL is not set. DB stats/events will be disabled.")


# --------- internal helpers ---------

def _db_available() -> bool:
    return bool(DATABASE_URL)


def _execute(cur, sql: str, params=()):
    cur.execute(sql, params)


# --------- events ---------

def add_event(u_hash: str, event_type: str, category: str) -> None:
    """
    Writes usage event to DB. Compatible with both schemas:
      - events(user_hash, event_type, category, ts ...)
      - events(user_hash, event,      category, ts ...)
    Never raises outside (doesn't break bot).
    """
    if not _db_available():
        return

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Try new schema first: event_type
                try:
                    _execute(
                        cur,
                        "INSERT INTO events(user_hash, event_type, category) VALUES (%s, %s, %s);",
                        (u_hash, event_type, category),
                    )
                except Exception:
                    # Fallback to old schema: event
                    _execute(
                        cur,
                        "INSERT INTO events(user_hash, event, category) VALUES (%s, %s, %s);",
                        (u_hash, event_type, category),
                    )
            conn.commit()
    except Exception as e:
        # Important: do not crash bot because of stats
        logger.exception("add_event failed: %s", e)


# --------- stats ---------

def get_stats(days: int = 7) -> Dict[str, Any]:
    """
    Returns stats for last N days (UTC). Compatible with both schemas:
      - column event_type
      - column event
    """
    result: Dict[str, Any] = {
        "days": int(days),
        "unique_users": 0,
        "pick_advocate": {},
        "request_click": {},
        "request_sent": {},
        "asked_question": {},
    }

    if not _db_available():
        return result

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Unique users
                _execute(
                    cur,
                    """
                    SELECT COUNT(DISTINCT user_hash)
                    FROM events
                    WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval;
                    """,
                    (days,),
                )
                row = cur.fetchone()
                result["unique_users"] = int(row[0]) if row and row[0] is not None else 0

                def fetch_map(evtype: str) -> Dict[str, int]:
                    # Try event_type first
                    try:
                        _execute(
                            cur,
                            """
                            SELECT category, COUNT(*)
                            FROM events
                            WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval
                              AND event_type = %s
                            GROUP BY category
                            ORDER BY COUNT(*) DESC;
                            """,
                            (days, evtype),
                        )
                        return {k: int(v) for (k, v) in cur.fetchall()}
                    except Exception:
                        # Fallback to old column name: event
                        _execute(
                            cur,
                            """
                            SELECT category, COUNT(*)
                            FROM events
                            WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval
                              AND event = %s
                            GROUP BY category
                            ORDER BY COUNT(*) DESC;
                            """,
                            (days, evtype),
                        )
                        return {k: int(v) for (k, v) in cur.fetchall()}

                result["pick_advocate"] = fetch_map("pick_advocate")
                result["request_click"] = fetch_map("request_click")
                result["request_sent"] = fetch_map("request_sent")
                result["asked_question"] = fetch_map("asked_question")

        return result

    except Exception as e:
        logger.exception("get_stats failed: %s", e)
        return result
