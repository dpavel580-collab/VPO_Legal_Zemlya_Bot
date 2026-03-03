# bot.py
import os
import json
import re
import asyncio
from typing import Optional

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
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


# ================== ENV ==================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "").strip()

WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()  # e.g. https://xxx.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()      # random path part
PORT = int(os.getenv("PORT", "10000"))

LAWYER_BIND_CODE = os.getenv("LAWYER_BIND_CODE", "").strip()

LAWYER_CONTACT_MIL_PAY = os.getenv("LAWYER_CONTACT_MIL_PAY", "").strip()
LAWYER_CONTACT_MOB = os.getenv("LAWYER_CONTACT_MOB", "").strip()
LAWYER_CONTACT_CIVIL = os.getenv("LAWYER_CONTACT_CIVIL", "").strip()

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

db = DB(DATABASE_URL)
ai = OpenAI(api_key=OPENAI_API_KEY)


# ================== HELPERS ==================
def is_admin(update: Update) -> bool:
    if not ADMIN_TELEGRAM_ID:
        return False
    try:
        admin_id = int(ADMIN_TELEGRAM_ID)
    except Exception:
        return False
    uid = update.effective_user.id if update.effective_user else None
    return uid == admin_id


def menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Задати питання", callback_data="ASK")],
        [InlineKeyboardButton("⚖️ Зв'язатися з адвокатом", callback_data="LAWYERS")],
        [InlineKeyboardButton("⛔️ Стоп / Вийти", callback_data="STOP")],
    ])


def lawyers_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪖 Військові виплати", callback_data="LAW_MIL_PAY")],
        [InlineKeyboardButton("🪖 Мобілізація / відстрочки", callback_data="LAW_MOB")],
        [InlineKeyboardButton("🏠 Цивільні / пенсії / компенсації", callback_data="LAW_CIVIL")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="BACK_MENU")],
    ])


def back_or_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Меню", callback_data="BACK_MENU")],
    ])


def lawyer_contact_text(category: str) -> str:
    # You can store full text in env values; here we just output env
    if category == "MIL_PAY":
        return LAWYER_CONTACT_MIL_PAY or "Контакти для цієї категорії ще не заповнені."
    if category == "MOB":
        return LAWYER_CONTACT_MOB or "Контакти для цієї категорії ще не заповнені."
    if category == "CIVIL":
        return LAWYER_CONTACT_CIVIL or "Контакти для цієї категорії ще не заповнені."
    return "Контакти не знайдені."


def request_to_lawyer_kb(category: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📨 Надіслати запит на зворотний зв'язок", callback_data=f"REQ_{category}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="LAWYERS")],
    ])


def lawyer_reply_kb(client_user_id: int, client_username: Optional[str]) -> InlineKeyboardMarkup:
    # The main point: to count click stats, the lawyer must press the button
    payload = json.dumps({"id": int(client_user_id), "u": client_username or ""}, ensure_ascii=False)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✉️ Зв'язатися з клієнтом", callback_data=f"CONTACT::{payload}")],
    ])


async def ask_openai(question: str) -> str:
    # very safe: short structured answer
    system = (
        "Ти юридичний помічник. Давай короткі та структуровані відповіді українською. "
        "Посилайся на чинні норми, але якщо не впевнений — скажи, що треба уточнити. "
        "Не збирай персональні дані. Уникай вигадувань."
    )
    resp = ai.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": question},
        ],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()


# ================== STATE (in-memory) ==================
# Note: without collecting personal data, but Telegram user_id is technical identifier.
# If you want persistence across restarts, we can move state to DB later.
STATE = {}  # user_id -> dict


def get_state(user_id: int) -> dict:
    st = STATE.get(user_id)
    if not st:
        st = {"mode": "MENU"}  # MENU | ASK | WAIT_REQ_TEXT
        STATE[user_id] = st
    return st


# ================== COMMANDS ==================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.log_event("start", {"chat_type": update.effective_chat.type})
    await update.message.reply_text(
        "Вітаю! Це юридичний чат-бот. Оберіть дію:",
        reply_markup=menu_kb()
    )


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    rows = db.stats_all_time()
    text = "📊 Статистика за ВЕСЬ ЧАС:\n\n"
    if not rows:
        text += "(поки що немає подій)\n"
    else:
        for r in rows:
            text += f"- {r['event']}: {r['cnt']}\n"
    await update.message.reply_text(text)


# bind flow: lawyers send /bind <CATEGORY> <CODE>
# CATEGORY: MIL_PAY | MOB | CIVIL
async def bind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = (update.message.text or "").strip().split()
    if len(parts) != 3:
        await update.message.reply_text("Формат: /bind MIL_PAY|MOB|CIVIL <код>")
        return
    _, cat, code = parts
    cat = cat.upper()

    if not LAWYER_BIND_CODE or code != LAWYER_BIND_CODE:
        db.log_event("lawyer_bind_wrong_code", {"cat": cat})
        await update.message.reply_text("Невірний код.")
        return

    if cat not in ("MIL_PAY", "MOB", "CIVIL"):
        await update.message.reply_text("Категорія має бути: MIL_PAY або MOB або CIVIL")
        return

    db.bind_lawyer(cat, update.effective_chat.id)
    db.log_event("lawyer_bound", {"cat": cat})

    await update.message.reply_text(f"✅ Вас прив'язано як адвоката для категорії {cat}.")


# ================== CALLBACKS ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    st = get_state(user_id)
    data = q.data or ""

    if data == "BACK_MENU":
        st["mode"] = "MENU"
        db.log_event("menu_open", {})
        await q.message.reply_text("Меню:", reply_markup=menu_kb())
        return

    if data == "STOP":
        st["mode"] = "MENU"
        db.log_event("stop", {})
        await q.message.reply_text("Дякую! Якщо буде потрібно — повертайтесь у меню.", reply_markup=menu_kb())
        return

    if data == "ASK":
        st["mode"] = "ASK"
        db.log_event("ask_start", {})
        await q.message.reply_text("Напишіть ваше питання одним повідомленням:", reply_markup=back_or_menu_kb())
        return

    if data == "LAWYERS":
        st["mode"] = "MENU"
        db.log_event("lawyers_open", {})
        await q.message.reply_text("Оберіть категорію адвоката:", reply_markup=lawyers_kb())
        return

    if data == "LAW_MIL_PAY":
        db.log_event("lawyer_category_click", {"cat": "MIL_PAY"})
        text = "🪖 Військові виплати\n\n" + lawyer_contact_text("MIL_PAY")
        await q.message.reply_text(text, reply_markup=request_to_lawyer_kb("MIL_PAY"))
        return

    if data == "LAW_MOB":
        db.log_event("lawyer_category_click", {"cat": "MOB"})
        text = "🪖 Мобілізація / відстрочки\n\n" + lawyer_contact_text("MOB")
        await q.message.reply_text(text, reply_markup=request_to_lawyer_kb("MOB"))
        return

    if data == "LAW_CIVIL":
        db.log_event("lawyer_category_click", {"cat": "CIVIL"})
        text = "🏠 Цивільні / пенсії / компенсації\n\n" + lawyer_contact_text("CIVIL")
        await q.message.reply_text(text, reply_markup=request_to_lawyer_kb("CIVIL"))
        return

    # User wants to send request to lawyer
    if data.startswith("REQ_"):
        cat = data.replace("REQ_", "").strip().upper()
        if cat not in ("MIL_PAY", "MOB", "CIVIL"):
            return

        st["mode"] = "WAIT_REQ_TEXT"
        st["req_cat"] = cat

        db.log_event("req_start", {"cat": cat})
        await q.message.reply_text(
            "Напишіть одним повідомленням:\n"
            "1) Ім'я та по-батькові\n"
            "2) Контактний номер телефону\n"
            "3) Місто проживання\n"
            "4) Коротко питання\n\n"
            "⚠️ Не пишіть зайвих персональних даних (паспорт, адреса тощо).",
            reply_markup=back_or_menu_kb(),
        )
        return

    # Lawyer presses "contact client" -> we log stats and show tg link / username
    if data.startswith("CONTACT::"):
        payload = data.split("CONTACT::", 1)[1]
        try:
            obj = json.loads(payload)
            cid = int(obj.get("id"))
            uname = (obj.get("u") or "").strip()
        except Exception:
            return

        db.log_event("lawyer_contact_click", {"client_id": cid})

        if uname:
            msg = (
                "ЗВ'ЯЗОК З КЛІЄНТОМ\n\n"
                f"Username клієнта: @{uname}\n\n"
                "Натисніть на username, щоб відкрити чат.\n"
                "Якщо чат не відкривається — клієнт міг обмежити приватні повідомлення."
            )
        else:
            msg = (
                "ЗВ'ЯЗОК З КЛІЄНТОМ\n\n"
                f"ID клієнта: {cid}\n"
                f"Посилання: tg://user?id={cid}\n\n"
                "Якщо посилання не відкривається — клієнт міг обмежити приватні повідомлення. "
                "Тоді попросіть клієнта написати вам напряму або використайте телефон зі заявки."
            )

        await q.message.reply_text(msg)
        return


# ================== TEXT HANDLER ==================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    st = get_state(user_id)
    text = (update.message.text or "").strip()

    # menu via text: if user types /start etc.
    if text.lower() in ("меню", "menu"):
        st["mode"] = "MENU"
        await update.message.reply_text("Меню:", reply_markup=menu_kb())
        return

    if st.get("mode") == "ASK":
        db.log_event("question", {"len": len(text)})
        await update.message.chat.send_action("typing")

        try:
            answer = await ask_openai(text)
        except Exception:
            answer = "Вибачте, зараз сталася технічна помилка. Спробуйте ще раз трохи пізніше."

        await update.message.reply_text(answer)
        await update.message.reply_text(
            "Що далі?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Задати ще питання", callback_data="ASK")],
                [InlineKeyboardButton("⚖️ Зв'язатися з адвокатом", callback_data="LAWYERS")],
                [InlineKeyboardButton("⛔️ Стоп / Вийти", callback_data="STOP")],
            ])
        )
        return

    if st.get("mode") == "WAIT_REQ_TEXT":
        cat = st.get("req_cat", "").upper()
        if cat not in ("MIL_PAY", "MOB", "CIVIL"):
            st["mode"] = "MENU"
            await update.message.reply_text("Меню:", reply_markup=menu_kb())
            return

        # minimal validation (no data storage, just forwarding)
        db.log_event("req_submitted", {"cat": cat})

        lawyer_chat_id = db.get_lawyer_chat_id(cat)
        if not lawyer_chat_id:
            await update.message.reply_text(
                "Поки що адвокат для цієї категорії не підключений. Спробуйте пізніше або оберіть іншу категорію.",
                reply_markup=lawyers_kb(),
            )
            st["mode"] = "MENU"
            return

        username = update.effective_user.username or ""
        client_user_id = update.effective_user.id

        cat_title = {
            "MIL_PAY": "ВІЙСЬКОВІ ВИПЛАТИ",
            "MOB": "МОБІЛІЗАЦІЯ / ВІДСТРОЧКИ",
            "CIVIL": "ЦИВІЛЬНІ / ПЕНСІЇ / КОМПЕНСАЦІЇ",
        }[cat]

        msg_to_lawyer = (
            "НОВИЙ ЗАПИТ НА ЗВОРОТНИЙ ЗВ'ЯЗОК\n"
            f"Категорія: {cat} - {cat_title}\n"
            f"Клієнт: @{username}\n" if username else
            "НОВИЙ ЗАПИТ НА ЗВОРОТНИЙ ЗВ'ЯЗОК\n"
            f"Категорія: {cat} - {cat_title}\n"
        )

        # Always include client id (technical id) so lawyer can open tg link if needed
        msg_to_lawyer += f"ID клієнта: {client_user_id}\n\n{text}"

        try:
            await context.bot.send_message(
                chat_id=lawyer_chat_id,
                text=msg_to_lawyer,
                reply_markup=lawyer_reply_kb(client_user_id, username),
            )
            db.log_event("req_forwarded_to_lawyer", {"cat": cat})
        except Exception:
            await update.message.reply_text("Не вдалося надіслати запит адвокату. Спробуйте пізніше.")
            st["mode"] = "MENU"
            return

        await update.message.reply_text("✅ Запит передано адвокату. Очікуйте зворотний зв'язок.")
        st["mode"] = "MENU"
        return

    # default
    await update.message.reply_text("Оберіть дію в меню:", reply_markup=menu_kb())


async def handle_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ignore files/voice
    db.log_event("non_text", {})
    await update.message.reply_text("Поки що приймаю лише текстові повідомлення. Напишіть питання текстом 🙂")


# ================== MAIN ==================
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # commands
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))  # ALL-TIME stats
    app.add_handler(CommandHandler("bind", bind_cmd))    # lawyer bind

    # callbacks/buttons
    app.add_handler(CallbackQueryHandler(on_callback))

    # text & non-text
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, handle_non_text))

    # webhook if configured, else polling
    if WEBHOOK_BASE_URL and WEBHOOK_SECRET:
        webhook_url = f"{WEBHOOK_BASE_URL.rstrip('/')}/{WEBHOOK_SECRET}"
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_SECRET,
            webhook_url=webhook_url,
            close_loop=False,
        )
    else:
        app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
