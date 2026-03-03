import os
import re
import json
import time
import uuid
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.rows import dict_row

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from openai import OpenAI

from db import DB


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("vpo_legal_bot")


# -----------------------------
# ENV
# -----------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "").strip()
ANALYTICS_SALT = os.getenv("ANALYTICS_SALT", "").strip()

WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()  # e.g. https://xxx.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()      # random path segment

LAWYER_BIND_CODE = os.getenv("LAWYER_BIND_CODE", "").strip()

# Contact texts shown to users (no PII stored by bot; these are public contacts)
LAWYER_CONTACT_MIL_PAY = os.getenv("LAWYER_CONTACT_MIL_PAY", "").strip()
LAWYER_CONTACT_MOB = os.getenv("LAWYER_CONTACT_MOB", "").strip()
LAWYER_CONTACT_CIVIL = os.getenv("LAWYER_CONTACT_CIVIL", "").strip()

# Limits
MAX_QUESTIONS_PER_WINDOW = 20
WINDOW_HOURS = 24
SUGGEST_LAWYER_EVERY = 5

# OpenAI
MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()  # you can change if needed


if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")
if not ADMIN_TELEGRAM_ID:
    raise RuntimeError("ADMIN_TELEGRAM_ID is not set")
if not ANALYTICS_SALT:
    raise RuntimeError("ANALYTICS_SALT is not set")
if not LAWYER_BIND_CODE:
    raise RuntimeError("LAWYER_BIND_CODE is not set")


ADMIN_TELEGRAM_ID_INT = int(ADMIN_TELEGRAM_ID)


# -----------------------------
# Helpers
# -----------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def hash_id(raw_id: int | str | None) -> str:
    """Hash ID with salt (no PII stored)."""
    if raw_id is None:
        raw_id = "unknown"
    base = f"{ANALYTICS_SALT}:{raw_id}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def safe_text(s: str, limit: int = 3500) -> str:
    s = (s or "").strip()
    if len(s) > limit:
        return s[:limit] + "…"
    return s


def is_admin(update: Update) -> bool:
    try:
        return update.effective_user and update.effective_user.id == ADMIN_TELEGRAM_ID_INT
    except Exception:
        return False


@dataclass(frozen=True)
class Category:
    key: str
    title: str
    contact_text: str


CATEGORIES = {
    "MIL_PAY": Category(
        key="MIL_PAY",
        title="Військові адвокати - виплати/компенсації",
        contact_text=LAWYER_CONTACT_MIL_PAY or "Контакти для цієї категорії ще не заповнені."
    ),
    "MOB": Category(
        key="MOB",
        title="Військові адвокати - мобілізація/відстрочки",
        contact_text=LAWYER_CONTACT_MOB or "Контакти для цієї категорії ще не заповнені."
    ),
    "CIVIL": Category(
        key="CIVIL",
        title="Цивільні адвокати - пенсії/компенсації/соцвиплати/спадщина/податки",
        contact_text=LAWYER_CONTACT_CIVIL or "Контакти для цієї категорії ще не заповнені."
    ),
}


# -----------------------------
# Postgres: small internal tables (no PII stored)
# -----------------------------
def pg_connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_internal_tables():
    """Create tables used by bot logic."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            # lawyer bindings: category -> lawyer chat_id/user_id (raw IDs here are OK in DB? We DO NOT store raw IDs in analytics,
            # but for routing messages we must store lawyer chat_id. This is operational data, not analytics.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lawyer_bindings (
                    category TEXT PRIMARY KEY,
                    lawyer_chat_id BIGINT NOT NULL,
                    lawyer_user_id BIGINT NOT NULL,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            # pending bind approvals
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_binds (
                    token TEXT PRIMARY KEY,
                    category TEXT NOT NULL,
                    req_chat_id BIGINT NOT NULL,
                    req_user_id BIGINT NOT NULL,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    status TEXT NOT NULL DEFAULT 'PENDING'
                );
                """
            )

            # user quota: hashed user
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_quota (
                    user_hash TEXT PRIMARY KEY,
                    window_start TIMESTAMPTZ NOT NULL,
                    used_count INT NOT NULL
                );
                """
            )

            # contact tokens for lawyer "contact client" button
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS contact_tokens (
                    token TEXT PRIMARY KEY,
                    client_user_id BIGINT NOT NULL,
                    client_username TEXT,
                    category TEXT NOT NULL,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

        conn.commit()


def get_lawyer_chat_id(category: str) -> int | None:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT lawyer_chat_id FROM lawyer_bindings WHERE category=%s;",
                (category,)
            )
            row = cur.fetchone()
            return int(row["lawyer_chat_id"]) if row else None


def set_lawyer_binding(category: str, lawyer_chat_id: int, lawyer_user_id: int):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lawyer_bindings(category, lawyer_chat_id, lawyer_user_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (category)
                DO UPDATE SET lawyer_chat_id=EXCLUDED.lawyer_chat_id,
                              lawyer_user_id=EXCLUDED.lawyer_user_id,
                              created_ts=NOW();
                """,
                (category, lawyer_chat_id, lawyer_user_id)
            )
        conn.commit()


def create_pending_bind(category: str, req_chat_id: int, req_user_id: int) -> str:
    token = uuid.uuid4().hex[:12]
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pending_binds(token, category, req_chat_id, req_user_id)
                VALUES (%s, %s, %s, %s);
                """,
                (token, category, req_chat_id, req_user_id)
            )
        conn.commit()
    return token


def resolve_pending_bind(token: str) -> dict | None:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM pending_binds WHERE token=%s;",
                (token,)
            )
            return cur.fetchone()


def mark_pending_bind(token: str, status: str):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pending_binds SET status=%s WHERE token=%s;",
                (status, token)
            )
        conn.commit()


def quota_get_or_reset(user_hash: str) -> tuple[datetime, int]:
    """
    Returns (window_start, used_count). Resets if window expired.
    """
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT window_start, used_count FROM user_quota WHERE user_hash=%s;", (user_hash,))
            row = cur.fetchone()

            if not row:
                ws = now_utc()
                cur.execute(
                    "INSERT INTO user_quota(user_hash, window_start, used_count) VALUES (%s, %s, %s);",
                    (user_hash, ws, 0)
                )
                conn.commit()
                return ws, 0

            ws = row["window_start"]
            used = int(row["used_count"])

            if ws.tzinfo is None:
                ws = ws.replace(tzinfo=timezone.utc)

            if now_utc() - ws >= timedelta(hours=WINDOW_HOURS):
                ws = now_utc()
                used = 0
                cur.execute(
                    "UPDATE user_quota SET window_start=%s, used_count=%s WHERE user_hash=%s;",
                    (ws, used, user_hash)
                )
                conn.commit()

            return ws, used


def quota_increment(user_hash: str, inc: int = 1) -> int:
    """Increment used_count, return new used_count."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT window_start, used_count FROM user_quota WHERE user_hash=%s;", (user_hash,))
            row = cur.fetchone()
            if not row:
                ws = now_utc()
                used = inc
                cur.execute(
                    "INSERT INTO user_quota(user_hash, window_start, used_count) VALUES (%s, %s, %s);",
                    (user_hash, ws, used)
                )
                conn.commit()
                return used

            used = int(row["used_count"]) + inc
            cur.execute(
                "UPDATE user_quota SET used_count=%s WHERE user_hash=%s;",
                (used, user_hash)
            )
        conn.commit()
    return used


def create_contact_token(client_user_id: int, client_username: str | None, category: str) -> str:
    token = uuid.uuid4().hex[:18]
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO contact_tokens(token, client_user_id, client_username, category)
                VALUES (%s, %s, %s, %s);
                """,
                (token, client_user_id, client_username, category)
            )
        conn.commit()
    return token


def get_contact_token(token: str) -> dict | None:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM contact_tokens WHERE token=%s;", (token,))
            return cur.fetchone()


# -----------------------------
# OpenAI client
# -----------------------------
oai = OpenAI(api_key=OPENAI_API_KEY)


SYSTEM_PROMPT = (
    "Ти - юридичний консультант-бот для ВПО, власників майна та землі на ТОТ України, "
    "військових та військовозобов'язаних, пенсіонерів, людей з інвалідністю та інших категорій громадян.\n\n"
    "Правила відповіді:\n"
    "1) Відповідай українською, коротко і структуровано (кроки/список).\n"
    "2) Якщо не вистачає даних - став 1-2 уточнюючих питання.\n"
    "3) Не збирай і не проси зайві персональні дані. Достатньо загальних обставин.\n"
    "4) Якщо тема ризикована/високі ставки - радь звернутися до адвоката.\n"
    "5) Посилайся на 'чинне законодавство України' без вигадування конкретних номерів статей, "
    "якщо ти не впевнений.\n"
    "6) Наприкінці додавай короткий дисклеймер: 'Це загальна інформація, не індивідуальна правова допомога'."
)


WELCOME_TEXT = (
    "Вітаю! Це юридичний чат-бот від Асоціації розвитку та реконструкції регіонів України, "
    "яка реалізує програму \"Є-ЗЕМЛЯ\".\n\n"
    "Бот надає короткі та структуровані консультації щодо: компенсацій, статусу ВПО, соціальних виплат, "
    "державних програм підтримки, майна в окупації, спадкування, питань мобілізації та виплат "
    "(в межах чинного законодавства).\n\n"
    "Натисніть кнопку нижче, щоб поставити питання. За потреби бот підкаже, як зв'язатися з адвокатом."
)

GOODBYE_TEXT = (
    "Дякую за користування ботом. Лише разом, об'єднано, ми досягнемо результатів у програмі \"Є-ЗЕМЛЯ\".\n"
    "Якщо потрібно - зв'яжіться з адвокатом.\n\n"
    "Будь ласка, поділіться ботом з тими, кому це може допомогти."
)


# -----------------------------
# Keyboards
# -----------------------------
def kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Задати питання", callback_data="ASK")],
        [InlineKeyboardButton("👨‍⚖️ Зв'язатися з адвокатом", callback_data="LAWYER_MENU")],
        [InlineKeyboardButton("❌ Вийти", callback_data="EXIT")],
    ])


def kb_after_block() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Задати нове питання", callback_data="ASK")],
        [InlineKeyboardButton("👨‍⚖️ Зв'язатися з адвокатом", callback_data="LAWYER_MENU")],
        [InlineKeyboardButton("❌ Вийти", callback_data="EXIT")],
    ])


def kb_lawyer_categories() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪖 Виплати/компенсації військовим", callback_data="CAT:MIL_PAY")],
        [InlineKeyboardButton("🪖 Мобілізація/відстрочки", callback_data="CAT:MOB")],
        [InlineKeyboardButton("🏛 Цивільні питання", callback_data="CAT:CIVIL")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="BACK_MAIN")],
    ])


def kb_lawyer_contact(category: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📩 Надіслати запит на зворотній зв'язок", callback_data=f"REQ:{category}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="LAWYER_MENU")],
        [InlineKeyboardButton("❌ Вийти", callback_data="EXIT")],
    ])


# -----------------------------
# Conversation state keys
# -----------------------------
STATE_MODE = "mode"  # "idle" | "asking" | "collect_request"
STATE_REQ_CATEGORY = "req_category"
STATE_REQ_STEP = "req_step"
STATE_Q_IN_BLOCK = "q_in_block"


# -----------------------------
# DB instances
# -----------------------------
db = DB(DATABASE_URL, analytics_salt=ANALYTICS_SALT)


# -----------------------------
# Core handlers
# -----------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.log_event_from_update(update, "start", {"chat_type": update.effective_chat.type})

    context.user_data[STATE_MODE] = "idle"
    context.user_data[STATE_Q_IN_BLOCK] = 0

    await update.message.reply_text(WELCOME_TEXT, reply_markup=kb_main())


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.log_event_from_update(update, "help", {})
    await update.message.reply_text("Натисніть 'Задати питання' або 'Зв'язатися з адвокатом'.", reply_markup=kb_main())


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    # lifetime stats from analytics DB
    s = db.get_lifetime_stats()

    # plus: lawyer binding status (operational)
    bindings = []
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT category, lawyer_chat_id, created_ts FROM lawyer_bindings ORDER BY category;")
            for r in cur.fetchall():
                bindings.append(r)

    text = (
        f"📊 *Статистика (за весь час)*\n\n"
        f"Подій всього: *{s['total_events']}*\n"
        f"Унікальних користувачів (хеш): *{s['unique_users']}*\n"
        f"Унікальних чатів (хеш): *{s['unique_chats']}*\n\n"
        f"*Топ подій:*\n" +
        "\n".join([f"- `{e['event']}`: {e['count']}" for e in s["top_events"][:15]]) +
        "\n\n*Прив'язки адвокатів:*\n" +
        ("\n".join([f"- `{b['category']}` → chat_id {b['lawyer_chat_id']} (з {b['created_ts']})" for b in bindings]) if bindings else "- Немає")
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# -----------------------------
# Lawyer binding flow
# -----------------------------
async def bind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Lawyer runs:
      /bind CIVIL KIV-2026-777-SECRET
      /bind MOB   KIV-2026-777-SECRET
      /bind MIL_PAY KIV-2026-777-SECRET
    Admin approves using /approve <token> or /reject <token>
    """
    db.log_event_from_update(update, "lawyer_bind_cmd", {})

    if not update.message:
        return

    parts = update.message.text.strip().split()
    if len(parts) != 3:
        await update.message.reply_text(
            "Формат: /bind <CIVIL|MOB|MIL_PAY> <SECRET_CODE>\n"
            "Приклад: /bind CIVIL KIV-2026-777-SECRET"
        )
        return

    _, category, code = parts
    category = category.strip().upper()

    if category not in CATEGORIES:
        await update.message.reply_text("Невідома категорія. Доступні: CIVIL, MOB, MIL_PAY")
        return

    if code.strip() != LAWYER_BIND_CODE:
        await update.message.reply_text("Невірний секретний код.")
        return

    req_chat_id = update.effective_chat.id
    req_user_id = update.effective_user.id

    token = create_pending_bind(category, req_chat_id=req_chat_id, req_user_id=req_user_id)
    db.log_event_from_update(update, "lawyer_bind_requested", {"category": category, "token": token})

    # Notify admin
    admin_text = (
        "🧩 *ЗАПИТ НА ПРИВ'ЯЗКУ АДВОКАТА*\n"
        f"Категорія: *{category}* - {CATEGORIES[category].title}\n"
        f"user_id: `{req_user_id}`\n"
        f"chat_id: `{req_chat_id}`\n\n"
        f"Підтвердити: `/approve {token}`\n"
        f"Відхилити: `/reject {token}`"
    )
    await context.bot.send_message(
        chat_id=ADMIN_TELEGRAM_ID_INT,
        text=admin_text,
        parse_mode=ParseMode.MARKDOWN
    )

    await update.message.reply_text("Ок. Запит на прив'язку відправлено адміну. Очікуйте підтвердження.")


async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not update.message:
        return

    parts = update.message.text.strip().split()
    if len(parts) != 2:
        await update.message.reply_text("Формат: /approve <TOKEN>")
        return

    token = parts[1].strip()
    row = resolve_pending_bind(token)
    if not row or row["status"] != "PENDING":
        await update.message.reply_text("Токен не знайдено або вже оброблено.")
        return

    category = row["category"]
    req_chat_id = int(row["req_chat_id"])
    req_user_id = int(row["req_user_id"])

    set_lawyer_binding(category, lawyer_chat_id=req_chat_id, lawyer_user_id=req_user_id)
    mark_pending_bind(token, "APPROVED")

    db.log_event_from_update(update, "lawyer_bind_approved", {"category": category, "token": token})

    await update.message.reply_text(f"✅ Підтверджено. Категорія {category} прив'язана.")
    await context.bot.send_message(
        chat_id=req_chat_id,
        text=f"✅ Вас прив'язано як адвоката для категорії {category} ({CATEGORIES[category].title})."
    )


async def reject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not update.message:
        return

    parts = update.message.text.strip().split()
    if len(parts) != 2:
        await update.message.reply_text("Формат: /reject <TOKEN>")
        return

    token = parts[1].strip()
    row = resolve_pending_bind(token)
    if not row or row["status"] != "PENDING":
        await update.message.reply_text("Токен не знайдено або вже оброблено.")
        return

    mark_pending_bind(token, "REJECTED")
    db.log_event_from_update(update, "lawyer_bind_rejected", {"token": token, "category": row["category"]})

    await update.message.reply_text("❌ Відхилено.")
    await context.bot.send_message(chat_id=int(row["req_chat_id"]), text="❌ Запит на прив'язку відхилено адміністратором.")


# -----------------------------
# Callback handler
# -----------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()
    data = query.data or ""

    # Always log clicks (anonymously)
    db.log_event_from_update(update, "ui_click", {"data": data})

    if data == "ASK":
        context.user_data[STATE_MODE] = "asking"
        await query.message.reply_text("Напишіть ваше питання одним повідомленням.")
        return

    if data == "LAWYER_MENU":
        await query.message.reply_text("Оберіть категорію адвоката:", reply_markup=kb_lawyer_categories())
        return

    if data.startswith("CAT:"):
        cat = data.split(":", 1)[1].strip().upper()
        if cat not in CATEGORIES:
            await query.message.reply_text("Невідома категорія.", reply_markup=kb_lawyer_categories())
            return

        db.log_event_from_update(update, "lawyer_category_select", {"category": cat})
        contact = CATEGORIES[cat].contact_text

        text = (
            f"👨‍⚖️ *{CATEGORIES[cat].title}*\n\n"
            f"{contact}\n\n"
            "Якщо хочете - можете надіслати запит на зворотній зв'язок."
        )
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb_lawyer_contact(cat))
        return

    if data.startswith("REQ:"):
        cat = data.split(":", 1)[1].strip().upper()
        if cat not in CATEGORIES:
            await query.message.reply_text("Невідома категорія.")
            return

        context.user_data[STATE_MODE] = "collect_request"
        context.user_data[STATE_REQ_CATEGORY] = cat
        context.user_data[STATE_REQ_STEP] = 1

        db.log_event_from_update(update, "lawyer_request_flow_start", {"category": cat})

        await query.message.reply_text(
            "Щоб надіслати запит адвокату, напишіть одним повідомленням:\n\n"
            "1) Ім'я та По-батькові\n"
            "2) Контактний номер телефону\n"
            "3) Місто проживання\n"
            "4) Коротко питання\n\n"
            "Приклад:\n"
            "Іван Петрович\n"
            "+380...\n"
            "Київ\n"
            "Потрібна консультація щодо ...",
        )
        return

    if data == "BACK_MAIN":
        await query.message.reply_text("Меню:", reply_markup=kb_main())
        return

    if data == "EXIT":
        context.user_data.clear()
        await query.message.reply_text(GOODBYE_TEXT, reply_markup=kb_main())
        return

    # Lawyer "contact client" button: CONTACT:<token>
    if data.startswith("CONTACT:"):
        token = data.split(":", 1)[1].strip()
        row = get_contact_token(token)
        if not row:
            await query.message.reply_text("Токен зв'язку не знайдено або застарів.")
            return

        # log that lawyer clicked contact
        db.log_event_from_update(update, "lawyer_contact_client_clicked", {"category": row["category"]})

        client_user_id = int(row["client_user_id"])
        client_username = row.get("client_username")

        # Show BOTH: username (if exists) and tg://user?id=... (may not work if privacy blocks, but it's standard)
        txt = (
            "✉️ *ЗВ'ЯЗОК З КЛІЄНТОМ*\n"
            f"Категорія: *{row['category']}* ({CATEGORIES.get(row['category'], Category(row['category'], row['category'], '')).title})\n\n"
        )
        if client_username:
            txt += f"Клієнт: @{client_username}\n"
        txt += f"ID клієнта: `{client_user_id}`\n\n"
        txt += f"Посилання: tg://user?id={client_user_id}\n\n"
        txt += (
            "Якщо посилання не відкривається - клієнт міг обмежити приватні повідомлення. "
            "Тоді попросіть клієнта написати вам напряму або використайте телефон із заявки."
        )
        await query.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)
        return


# -----------------------------
# Message handler
# -----------------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    mode = context.user_data.get(STATE_MODE, "idle")
    text = (update.message.text or "").strip()

    # ---------- Collect lawyer request ----------
    if mode == "collect_request":
        cat = context.user_data.get(STATE_REQ_CATEGORY)
        if not cat or cat not in CATEGORIES:
            context.user_data[STATE_MODE] = "idle"
            await update.message.reply_text("Сталася помилка категорії. Спробуйте ще раз.", reply_markup=kb_main())
            return

        # Minimal validation (no strict parsing; we don't store it in DB)
        payload = safe_text(text, limit=1500)

        db.log_event_from_update(update, "lawyer_request_submitted", {"category": cat})

        lawyer_chat_id = get_lawyer_chat_id(cat)
        if not lawyer_chat_id:
            await update.message.reply_text(
                "Дякую. Запит прийнято.\n\n"
                "На жаль, для цієї категорії адвокат ще не підтверджений. "
                "Спробуйте зв'язатися за контактами або повторіть пізніше.",
                reply_markup=kb_after_block()
            )
            context.user_data[STATE_MODE] = "idle"
            return

        client_user = update.effective_user
        client_user_id = client_user.id if client_user else None
        client_username = client_user.username if client_user else None

        # Create contact token for lawyer button (we need raw client_user_id here operationally)
        contact_token = create_contact_token(
            client_user_id=int(client_user_id),
            client_username=client_username,
            category=cat
        )

        # Send message to lawyer
        header = (
            "🆕 *НОВИЙ ЗАПИТ НА ЗВОРОТНИЙ ЗВ'ЯЗОК*\n"
            f"Категорія: *{cat}* - {CATEGORIES[cat].title}\n"
        )
        if client_username:
            header += f"Клієнт: @{client_username}\n"
        header += f"ID клієнта: `{client_user_id}`\n\n"

        msg = header + safe_text(payload, 2000)

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✉️ ЗВ'ЯЗАТИСЯ З КЛІЄНТОМ", callback_data=f"CONTACT:{contact_token}")]
        ])

        await context.bot.send_message(
            chat_id=lawyer_chat_id,
            text=msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb
        )

        await update.message.reply_text(
            "✅ Дякую! Ваш запит передано адвокату.\n\n"
            "За потреби можете поставити ще питання в боті або знову відкрити меню адвокатів.",
            reply_markup=kb_after_block()
        )
        context.user_data[STATE_MODE] = "idle"
        return

    # ---------- Ask question to AI ----------
    if mode == "asking":
        # quota check
        user = update.effective_user
        user_h = hash_id(user.id if user else None)

        ws, used = quota_get_or_reset(user_h)
        if used >= MAX_QUESTIONS_PER_WINDOW:
            db.log_event_from_update(update, "quota_blocked", {"used": used})
            remain = ws + timedelta(hours=WINDOW_HOURS) - now_utc()
            hours = max(0, int(remain.total_seconds() // 3600))
            mins = max(0, int((remain.total_seconds() % 3600) // 60))

            await update.message.reply_text(
                "⚠️ Ви вже поставили максимальну кількість безкоштовних питань за останні 24 години.\n\n"
                "Радимо зв'язатися з адвокатом.\n"
                f"Спробуйте знову приблизно через {hours} год {mins} хв.",
                reply_markup=kb_after_block()
            )
            context.user_data[STATE_MODE] = "idle"
            return

        # increment quota
        new_used = quota_increment(user_h, 1)
        db.log_event_from_update(update, "ask_question", {"used_24h": new_used})

        question = safe_text(text, 2000)

        # OpenAI call
        try:
            resp = oai.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": question},
                ],
                temperature=0.2,
            )
            answer = resp.choices[0].message.content or ""
        except Exception as e:
            log.exception("OpenAI error")
            db.log_event_from_update(update, "openai_error", {"error": str(e)[:200]})
            await update.message.reply_text(
                "Сталася технічна помилка при генерації відповіді. Спробуйте ще раз пізніше.",
                reply_markup=kb_after_block()
            )
            context.user_data[STATE_MODE] = "idle"
            return

        answer = safe_text(answer, 3500)
        db.log_event_from_update(update, "answer_sent", {})

        await update.message.reply_text(answer)

        # per-block counter (suggest lawyer each 5)
        q_in_block = int(context.user_data.get(STATE_Q_IN_BLOCK, 0))
        q_in_block += 1
        context.user_data[STATE_Q_IN_BLOCK] = q_in_block

        # after each answer: either continue asking or show menu every 5 questions
        if q_in_block % SUGGEST_LAWYER_EVERY == 0:
            await update.message.reply_text(
                "Хочете поставити ще питання чи зв'язатися з адвокатом?",
                reply_markup=kb_after_block()
            )
            context.user_data[STATE_MODE] = "idle"
        else:
            # keep asking mode, but don't spam lawyer
            await update.message.reply_text("Можете написати наступне питання.")
        return

    # ---------- Default idle: show menu ----------
    await update.message.reply_text("Оберіть дію:", reply_markup=kb_main())


async def handle_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.log_event_from_update(update, "non_text_message", {})
    await update.message.reply_text("Поки що бот приймає лише текстові повідомлення.", reply_markup=kb_main())


# -----------------------------
# Main
# -----------------------------
def main():
    init_internal_tables()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # commands
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))

    # lawyer bind flow
    app.add_handler(CommandHandler("bind", bind_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("reject", reject_cmd))

    # callbacks
    app.add_handler(CallbackQueryHandler(on_callback))

    # messages
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, handle_non_text))

    # webhook if configured, else polling
    if WEBHOOK_BASE_URL and WEBHOOK_SECRET:
        # Render provides PORT
        port = int(os.getenv("PORT", "10000"))
        webhook_url = f"{WEBHOOK_BASE_URL.rstrip('/')}/{WEBHOOK_SECRET}"

        log.info("Starting webhook on port=%s url=%s", port, webhook_url)

        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=WEBHOOK_SECRET,
            webhook_url=webhook_url,
            close_loop=False,
        )
    else:
        log.info("Starting polling")
        app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
