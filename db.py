import os
import psycopg
from datetime import datetime, timezone, timedelta

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

def _utcnow():
    return datetime.now(timezone.utc)

def _conn():
    return psycopg.connect(DATABASE_URL)

def ensure_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS analytics_counters (
        k TEXT PRIMARY KEY,
        v BIGINT NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS lawyers (
        category TEXT PRIMARY KEY,
        chat_id BIGINT NOT NULL,
        bound_at TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS lawyer_bind_requests (
        token TEXT PRIMARY KEY,
        category TEXT NOT NULL,
        lawyer_chat_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS user_usage (
        user_id BIGINT PRIMARY KEY,
        window_start TIMESTAMPTZ NOT NULL,
        count INT NOT NULL DEFAULT 0,
        locked_until TIMESTAMPTZ
    );
    """
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

# -------------------------
# analytics
# -------------------------
def inc_counter(k: str, n: int = 1):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics_counters (k, v) VALUES (%s, %s)
                ON CONFLICT (k) DO UPDATE SET v = analytics_counters.v + EXCLUDED.v
                """,
                (k, n)
            )
        conn.commit()

def get_all_counters():
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT k, v FROM analytics_counters ORDER BY v DESC, k ASC")
            return cur.fetchall()

# -------------------------
# lawyers bindings
# -------------------------
def set_lawyer(category: str, chat_id: int):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lawyers (category, chat_id, bound_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (category) DO UPDATE SET chat_id=EXCLUDED.chat_id, bound_at=EXCLUDED.bound_at
                """,
                (category, chat_id, _utcnow())
            )
        conn.commit()

def get_lawyer_chat(category: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id FROM lawyers WHERE category=%s", (category,))
            row = cur.fetchone()
            return row[0] if row else None

def create_bind_request(token: str, category: str, lawyer_chat_id: int):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lawyer_bind_requests (token, category, lawyer_chat_id, created_at, status)
                VALUES (%s, %s, %s, %s, 'pending')
                """,
                (token, category, lawyer_chat_id, _utcnow())
            )
        conn.commit()

def get_bind_request(token: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT token, category, lawyer_chat_id, status FROM lawyer_bind_requests WHERE token=%s",
                (token,)
            )
            return cur.fetchone()

def mark_bind_request(token: str, status: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE lawyer_bind_requests SET status=%s WHERE token=%s",
                (status, token)
            )
        conn.commit()

# -------------------------
# free usage limit (20/24h)
# -------------------------
def get_usage(user_id: int):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT window_start, count, locked_until FROM user_usage WHERE user_id=%s",
                (user_id,)
            )
            return cur.fetchone()

def upsert_usage(user_id: int, window_start, count: int, locked_until):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_usage (user_id, window_start, count, locked_until)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET window_start=EXCLUDED.window_start,
                              count=EXCLUDED.count,
                              locked_until=EXCLUDED.locked_until
                """,
                (user_id, window_start, count, locked_until)
            )
        conn.commit()

def check_free_limit(user_id: int, max_q: int = 20, window_hours: int = 24):
    """
    Returns: (allowed, remaining, locked_until)
    Does NOT increment. Increment separately after successful AI answer.
    """
    now = _utcnow()
    row = get_usage(user_id)

    if row is None:
        upsert_usage(user_id, now, 0, None)
        return True, max_q, None

    window_start, count, locked_until = row

    if locked_until and now < locked_until:
        return False, 0, locked_until

    # reset if window expired
    if now - window_start >= timedelta(hours=window_hours):
        window_start = now
        count = 0
        locked_until = None
        upsert_usage(user_id, window_start, count, locked_until)
        return True, max_q, None

    remaining = max(0, max_q - count)
    if remaining <= 0:
        locked_until = now + timedelta(hours=window_hours)
        upsert_usage(user_id, window_start, count, locked_until)
        return False, 0, locked_until

    return True, remaining, None

def increment_after_answer(user_id: int, max_q: int = 20, window_hours: int = 24):
    now = _utcnow()
    row = get_usage(user_id)
    if row is None:
        upsert_usage(user_id, now, 1, None)
        return 1

    window_start, count, locked_until = row

    if now - window_start >= timedelta(hours=window_hours):
        window_start = now
        count = 0
        locked_until = None

    count += 1

    if count >= max_q:
        locked_until = now + timedelta(hours=window_hours)

    upsert_usage(user_id, window_start, count, locked_until)
    return count
