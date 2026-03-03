import os
import hashlib
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

# ---------- schema ----------
def init_db() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # events: only anonymized stats
            cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
                user_hash TEXT NOT NULL,
                event_type TEXT NOT NULL,
                category TEXT NOT NULL
            );
            """)
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_events_type_cat ON events(event_type, category);""")

            # lawyer bindings: category -> chat_id
            cur.execute("""
            CREATE TABLE IF NOT EXISTS lawyer_bindings (
                category TEXT PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
            );
            """)

            # pending binds: token -> lawyer chat_id (expires by cleanup)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS pending_binds (
                token TEXT PRIMARY KEY,
                category TEXT NOT NULL,
                lawyer_chat_id BIGINT NOT NULL,
                lawyer_user_id BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
            );
            """)
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_pending_created ON pending_binds(created_at);""")
        conn.commit()

# ---------- events ----------
def add_event(u_hash: str, event_type: str, category: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO events(user_hash, event_type, category) VALUES (%s, %s, %s);",
                (u_hash, event_type, category)
            )
        conn.commit()

def get_stats(days: int = 7) -> dict:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT COUNT(DISTINCT user_hash)
            FROM events
            WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval;
            """, (days,))
            unique_users = int(cur.fetchone()[0])

            def fetch_map(evtype: str):
                cur.execute("""
                SELECT category, COUNT(*)
                FROM events
                WHERE ts >= (NOW() AT TIME ZONE 'utc') - (%s || ' days')::interval
                  AND event_type=%s
                GROUP BY category
                ORDER BY COUNT(*) DESC;
                """, (days, evtype))
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
            cur.execute("""
            INSERT INTO lawyer_bindings(category, chat_id)
            VALUES (%s, %s)
            ON CONFLICT(category)
            DO UPDATE SET chat_id=EXCLUDED.chat_id, updated_at=(NOW() AT TIME ZONE 'utc');
            """, (category, chat_id))
        conn.commit()

def get_lawyer(category: str):
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id FROM lawyer_bindings WHERE category=%s;", (category,))
            row = cur.fetchone()
            return int(row[0]) if row else None

# ---------- pending binds ----------
def create_pending_bind(token: str, category: str, lawyer_chat_id: int, lawyer_user_id: int) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO pending_binds(token, category, lawyer_chat_id, lawyer_user_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(token) DO NOTHING;
            """, (token, category, lawyer_chat_id, lawyer_user_id))
        conn.commit()

def get_pending_bind(token: str):
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT token, category, lawyer_chat_id, lawyer_user_id
            FROM pending_binds
            WHERE token=%s;
            """, (token,))
            row = cur.fetchone()
            if not row:
                return None
            return {"token": row[0], "category": row[1], "lawyer_chat_id": int(row[2]), "lawyer_user_id": int(row[3])}

def delete_pending_bind(token: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pending_binds WHERE token=%s;", (token,))
        conn.commit()
