import os
import hashlib
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.rows import dict_row


# Нові назви таблиць, щоб не конфліктувати зі старими "events"
T_EVENTS = "events_v3"
T_LAWYERS = "lawyer_bindings_v1"
T_PENDING = "pending_binds_v1"


def _db_url() -> str:
    url = (os.getenv("DATABASE_URL", "") or "").strip()
    if not url:
        raise RuntimeError("Missing DATABASE_URL (Render Postgres connection string)")
    return url


def _salt() -> str:
    s = (os.getenv("ANALYTICS_SALT", "") or "").strip()
    if not s:
        raise RuntimeError("Missing ANALYTICS_SALT")
    return s


def user_hash(user_id: int | str | None) -> str:
    """
    Анонімний стабільний хеш для статистики.
    НЕ зберігаємо персональні дані.
    """
    base = f"{_salt()}:{user_id if user_id is not None else 'unknown'}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _connect():
    return psycopg.connect(_db_url(), row_factory=dict_row)


def init_db() -> None:
    """
    Створює нові таблиці для статистики/адвокатів/прив'язок.
    Старі таблиці НЕ чіпає, тому конфліктів не буде.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            # 1) Події (аналітика) — тільки хеші
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {T_EVENTS} (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    user_hash TEXT NOT NULL,
                    event TEXT NOT NULL,
                    category TEXT
                );
            """)
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{T_EVENTS}_ts ON {T_EVENTS}(ts);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{T_EVENTS}_event ON {T_EVENTS}(event);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{T_EVENTS}_user_hash ON {T_EVENTS}(user_hash);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{T_EVENTS}_category ON {T_EVENTS}(category);")

            # 2) Прив'язки адвокатів: category -> chat_id
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {T_LAWYERS} (
                    category TEXT PRIMARY KEY,
                    lawyer_chat_id BIGINT NOT NULL,
                    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)

            # 3) Pending bind (адмін підтверджує токен)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {T_PENDING} (
                    token TEXT PRIMARY KEY,
                    category TEXT NOT NULL,
                    lawyer_chat_id BIGINT NOT NULL,
                    lawyer_user_id BIGINT NOT NULL,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{T_PENDING}_created ON {T_PENDING}(created_ts);")

        conn.commit()


def add_event(u_hash: str, event: str, category: str | None = None) -> None:
    """
    Додає подію в аналітику (без персональних даних).
    """
    if not u_hash:
        return
    event = (event or "").strip()
    if not event:
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {T_EVENTS} (ts, user_hash, event, category) VALUES (NOW(), %s, %s, %s);",
                (u_hash, event, category),
            )
        conn.commit()


def set_lawyer(category: str, lawyer_chat_id: int) -> None:
    category = (category or "").strip().upper()
    if not category:
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {T_LAWERS} (category, lawyer_chat_id, updated_ts)
                VALUES (%s, %s, NOW())
                ON CONFLICT (category)
                DO UPDATE SET lawyer_chat_id=EXCLUDED.lawyer_chat_id,
                              updated_ts=NOW();
                """.replace("{T_LAWERS}", T_LAWYERS),  # safe replace
                (category, int(lawyer_chat_id)),
            )
        conn.commit()


def get_lawyer(category: str) -> int | None:
    category = (category or "").strip().upper()
    if not category:
        return None

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT lawyer_chat_id FROM {T_LAWYERS} WHERE category=%s;", (category,))
            row = cur.fetchone()
            if not row:
                return None
            return int(row["lawyer_chat_id"])


def create_pending_bind(token: str, category: str, lawyer_chat_id: int, lawyer_user_id: int) -> None:
    token = (token or "").strip()
    category = (category or "").strip().upper()
    if not token or not category:
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {T_PENDING}(token, category, lawyer_chat_id, lawyer_user_id, created_ts)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (token) DO NOTHING;
                """,
                (token, category, int(lawyer_chat_id), int(lawyer_user_id)),
            )
        conn.commit()


def get_pending_bind(token: str) -> dict | None:
    token = (token or "").strip()
    if not token:
        return None

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT token, category, lawyer_chat_id, lawyer_user_id FROM {T_PENDING} WHERE token=%s;", (token,))
            row = cur.fetchone()
            return dict(row) if row else None


def delete_pending_bind(token: str) -> None:
    token = (token or "").strip()
    if not token:
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {T_PENDING} WHERE token=%s;", (token,))
        conn.commit()


def get_stats(days: int = 7) -> dict:
    """
    Повертає структуру, яку очікує твій stats_cmd у bot.py.
    """
    days = int(days) if days else 7
    since = datetime.now(timezone.utc) - timedelta(days=days)

    out = {
        "days": days,
        "unique_users": 0,
        "asked_question": {},
        "pick_advocate": {},
        "request_click": {},
        "request_sent": {},
        "lawyer_contact_click": {},
    }

    with _connect() as conn:
        with conn.cursor() as cur:
            # Унікальні користувачі за період
            cur.execute(
                f"SELECT COUNT(DISTINCT user_hash) AS cnt FROM {T_EVENTS} WHERE ts >= %s;",
                (since,),
            )
            out["unique_users"] = int(cur.fetchone()["cnt"] or 0)

            # Аггрегація: event + category
            cur.execute(
                f"""
                SELECT event, category, COUNT(*) AS cnt
                FROM {T_EVENTS}
                WHERE ts >= %s
                GROUP BY event, category;
                """,
                (since,),
            )
            rows = cur.fetchall()

    # Розкладаємо у словники як у твоєму bot.py
    for r in rows:
        ev = r["event"]
        cat = r["category"] or "UNKNOWN"
        cnt = int(r["cnt"] or 0)

        if ev == "asked_question":
            out["asked_question"][cat] = out["asked_question"].get(cat, 0) + cnt
        elif ev == "pick_advocate":
            out["pick_advocate"][cat] = out["pick_advocate"].get(cat, 0) + cnt
        elif ev == "request_click":
            out["request_click"][cat] = out["request_click"].get(cat, 0) + cnt
        elif ev == "request_sent":
            out["request_sent"][cat] = out["request_sent"].get(cat, 0) + cnt
        elif ev == "lawyer_contact_click":
            out["lawyer_contact_click"][cat] = out["lawyer_contact_click"].get(cat, 0) + cnt

    return out
