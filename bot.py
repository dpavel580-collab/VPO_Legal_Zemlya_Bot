import os
import re
import secrets
from datetime import datetime

from dotenv import load_dotenv

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from openai import OpenAI

from db import (
    ensure_tables,
    inc_counter,
    get_all_counters,
    set_lawyer,
    get_lawyer_chat,
    create_bind_request,
    get_bind_request,
    mark_bind_request,
    check_free_limit,
    increment_after_answer,
)

# ----------------------------
# ENV
# ----------------------------
load_dotenv(override=True)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "").strip()
ANALYTICS_SALT = os.getenv("ANALYTICS_SALT", "").strip()

LAWYER_BIND_CODE = os.getenv("LAWYER_BIND_CODE", "").strip()

LAWYER_CONTACT_CIVIL = os.getenv("LAWYER_CONTACT_CIVIL", "").strip()
LAWYER_CONTACT_MIL_PAY = os.getenv("LAWYER_CONTACT_MIL_PAY", "").strip()
LAWYER_CONTACT_MOB = os.getenv("LAWYER_CONTACT_MOB", "").strip()

WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()  # e.g. https://xxxx.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "tg").strip()    # url path secret, any string
PORT = int(os.getenv("PORT", "10000").strip())

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")
if not ADMIN_TELEGRAM_ID:
    raise RuntimeError("Missing ADMIN_TELEGRAM_ID")
if not DATABASE_URL = os.getenv("DATABASE_URL", "").strip():
    raise RuntimeError("Missing DATABASE_URL")
if not ANALYTICS_SALT:
    # не критично, але краще мати
    ANALYTICS_SALT = "salt"

ADMIN_ID = int(ADMIN_TELEGRAM_ID)

client = OpenAI(api_key=OPENAI_API_KEY)

# ----------------------------
# Категорії адвокатів
# ----------------------------
CAT_CIVIL = "CIVIL"
CAT_MIL_PAY = "MIL_PAY"
CAT_MOB = "MOB"

CAT_LABEL = {
    CAT_CIVIL: "ЦИВІЛЬНІ / ПЕНСІЇ / КОМПЕНСАЦІЇ / СОЦВИПЛАТИ / СПАДЩИНА / ПОДАТКИ",
    CAT_MIL_PAY: "ВІЙСЬКОВІ / ВИПЛАТИ / ПІЛЬГИ / КОМПЕНСАЦІЇ",
    CAT_MOB: "ВІЙСЬКОВІ / МОБІЛІЗАЦІЯ / ВІДСТРОЧКИ",
}

CONTACT_TEXT = {
    CAT_CIVIL: LAWYER_CONTACT_CIVIL,
    CAT_MIL_PAY: LAWYER_CONTACT_MIL_PAY,
    CAT_MOB: LAWYER_CONTACT_MOB,
}

# ----------------------------
# Тексти
# ----------------------------
WELCOME_TEXT = (
    "Вітаю!\n\n"
    "Це юридичний чат-бот від Асоціації розвитку та реконструкції регіонів України, "
    "яка реалізує програму \"Є-ЗЕМЛЯ\".\n\n"
    "Бот допомагає швидко зорієнтуватися в типових юридичних питаннях (ВПО, майно/земля на ТОТ, "
    "соцвиплати, компенсації, спадщина, мобілізація та виплати — в межах чинного законодавства).\n\n"
    "Натисніть кнопку нижче."
)

ABOUT_TEXT = (
    "ПРО БОТА\n\n"
    "Це інформаційний юридичний помічник для ВПО, власників майна/землі на ТОТ, військових, "
    "пенсіонерів та інших категорій громадян.\n\n"
    "Важливо:\n"
    "1) Бот не є адвокатом і не замінює індивідуальну юридичну допомогу.\n"
    "2) Відповіді мають довідковий характер та залежать від повноти даних.\n"
    "3) Якщо ситуація складна/термінова — скористайтеся кнопкою зв'язку з адвокатом.\n\n"
    "Проєкт створено в межах програми \"Є-ЗЕМЛЯ\", яку реалізує Асоціація розвитку та реконструкції регіонів України."
)

ASK_HINT_FIRST = (
    "Напишіть питання одним повідомленням.\n\n"
    "Щоб відповідь була точнішою, коротко вкажіть:\n"
    "- хто ви (ВПО/військовий/пенсіонер/інше)\n"
    "- місто/область (без точної адреси)\n"
    "- що сталося і що саме хочете отримати\n\n"
    "Не надсилайте паспортні дані, ІПН, точні адреси."
)

ASK_HINT_NEXT = (
    "Опишіть наступне питання по суті, коротко, з важливими деталями (без персональних даних)."
)

GOODBYE_TEXT = (
    "Дякую за користування ботом.\n\n"
    "\"Є-ЗЕМЛЯ\" - програма, яку реалізує Асоціація розвитку та реконструкції регіонів України - "
    "шлях для ВПО отримати компенсацію за свою землю та майно, що залишилися в окупації.\n\n"
    "Команда \"Є-ЗЕМЛЯ\" (Павло Деркач, Олег Ситник, Андрій Підставський, Дмитро Овєчкін) "
    "активно працює над впровадженням державних механізмів компенсацій, бо ми також втратили своє майно.\n\n"
    "Будь ласка, за потреби поділіться ботом із знайомими."
)

LIMIT_TEXT_TEMPLATE = (
    "Вибачте, ви вичерпали ліміт безкоштовних питань на 24 години.\n\n"
    "Рекомендуємо зв'язатися з адвокатом для професійної допомоги.\n"
    "{when}"
)

SYSTEM_PROMPT = (
    "Ти - українськомовний юридичний помічник для ВПО, власників майна/землі на ТОТ України, "
    "військових, військовозобов'язаних, пенсіонерів, людей з інвалідністю.\n\n"
    "Стиль:\n"
    "- Пиши людяно, без шаблонних повторів типу 'Ок, уточнюю'.\n"
    "- Уникай зайвої води, але давай корисні деталі і практичні кроки.\n"
    "- НЕ виділяй заголовки зірочками. Заголовок - просто окремим рядком.\n\n"
    "Правила:\n"
    "- Не вигадуй норми/факти. Якщо не впевнений - прямо скажи і попроси уточнення.\n"
    "- Не проси і не зберігай персональні дані (паспорт, ІПН, точні адреси). Ім'я можна.\n"
    "- Якщо для дій потрібні контакти органу (ЦНАП/ПФУ/ТЦК/соцзахист) - попроси місто/область і поясни порядок.\n"
    "- Не давай зразки документів.\n\n"
    "Формат відповіді:\n"
    "Коротко по суті (1-3 речення)\n"
    "Що це означає / що зазвичай передбачено\n"
    "Що робити (кроки 1-6)\n"
    "Які документи можуть знадобитися\n"
    "Коли варто звернутися до адвоката\n"
)

# ----------------------------
# Клавіатури
# ----------------------------
KB_ONLY_START = ReplyKeyboardMarkup(
    [[KeyboardButton("🚀 РОЗПОЧАТИ")]],
    resize_keyboard=True
)

KB_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("❓ ЗАДАТИ ПИТАННЯ"), KeyboardButton("ℹ️ ПРО БОТА")],
        [KeyboardButton("🧑‍⚖️ ЗВ'ЯЗАТИСЯ З АДВОКАТОМ")],
        [KeyboardButton("⛔ СТОП"), KeyboardButton("🚪 ВИЙТИ")],
    ],
    resize_keyboard=True
)

KB_ASKING = ReplyKeyboardMarkup(
    [
        [KeyboardButton("⛔ СТОП"), KeyboardButton("🚪 ВИЙТИ")],
    ],
    resize_keyboard=True
)

KB_AFTER_5 = ReplyKeyboardMarkup(
    [
        [KeyboardButton("➡️ ПРОДОВЖИТИ ДІАЛОГ"), KeyboardButton("🆕 ЗАДАТИ НОВЕ ПИТАННЯ")],
        [KeyboardButton("🧑‍⚖️ ЗВ'ЯЗАТИСЯ З АДВОКАТОМ")],
        [KeyboardButton("⛔ СТОП"), KeyboardButton("🚪 ВИЙТИ")],
    ],
    resize_keyboard=True
)

KB_AFTER_LIMIT = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🧑‍⚖️ ЗВ'ЯЗАТИСЯ З АДВОКАТОМ")],
        [KeyboardButton("⛔ СТОП"), KeyboardButton("🚪 ВИЙТИ")],
    ],
    resize_keyboard=True
)

KB_LAWYER_CATS = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🪖 МОБІЛІЗАЦІЯ / ВІДСТРОЧКИ"), KeyboardButton("💰 ВИПЛАТИ ВІЙСЬКОВИМ")],
        [KeyboardButton("🏛 ЦИВІЛЬНІ / СОЦВИПЛАТИ"),],
        [KeyboardButton("⛔ СТОП"), KeyboardButton("🚪 ВИЙТИ")],
    ],
    resize_keyboard=True
)

KB_LAWYER_ACTIONS = ReplyKeyboardMarkup(
    [
        [KeyboardButton("✍️ НАДІСЛАТИ ЗАПИТ АДВОКАТУ")],
        [KeyboardButton("↩️ НАЗАД"), KeyboardButton("🚪 ВИЙТИ")],
    ],
    resize_keyboard=True
)

# ----------------------------
# Стан (user_data)
# ----------------------------
MODE = "mode"   # menu | asking | after5 | lawyer_menu | lawyer_action | lawyer_form | locked
COUNT_BLOCK = "count_block"   # 0..5 within block
HISTORY = "history"           # list of messages for context
LAW_CAT = "law_cat"           # selected category for lawyer request

MAX_HISTORY_TURNS = 10  # keep last 10 messages (user+assistant)

def _reset_dialog(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[COUNT_BLOCK] = 0
    context.user_data[HISTORY] = []

def _append_history(context: ContextTypes.DEFAULT_TYPE, role: str, content: str):
    hist = context.user_data.get(HISTORY, [])
    hist.append({"role": role, "content": content})
    # trim
    if len(hist) > MAX_HISTORY_TURNS:
        hist = hist[-MAX_HISTORY_TURNS:]
    context.user_data[HISTORY] = hist

def _is_admin(update: Update) -> bool:
    return update.effective_user and update.effective_user.id == ADMIN_ID

# ----------------------------
# Commands
# ----------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    inc_counter("start")
    context.user_data[MODE] = "menu"
    _reset_dialog(context)
    await update.message.reply_text(WELCOME_TEXT, reply_markup=KB_ONLY_START)

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    rows = get_all_counters()
    if not rows:
        await update.message.reply_text("Статистика порожня.")
        return
    lines = ["СТАТИСТИКА (агреговано):"]
    for k, v in rows[:60]:
        lines.append(f"- {k}: {v}")
    await update.message.reply_text("\n".join(lines))

async def bind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /bind <CODE> <CATEGORY>
    CATEGORY: CIVIL | MIL_PAY | MOB
    """
    args = (update.message.text or "").split()
    if len(args) < 3:
        await update.message.reply_text("Формат: /bind <код> <CIVIL|MIL_PAY|MOB>")
        return

    code = args[1].strip()
    cat = args[2].strip().upper()

    if code != LAWYER_BIND_CODE:
        await update.message.reply_text("Невірний код.")
        return
    if cat not in (CAT_CIVIL, CAT_MIL_PAY, CAT_MOB):
        await update.message.reply_text("Невірна категорія. Доступні: CIVIL, MIL_PAY, MOB")
        return

    token = secrets.token_urlsafe(10).replace("-", "").replace("_", "")[:12]
    lawyer_chat_id = update.effective_chat.id

    create_bind_request(token, cat, lawyer_chat_id)
    inc_counter(f"lawyer_bind_requested:{cat}")

    # notify admin
    text_admin = (
        "ЗАПИТ НА ПРИВ'ЯЗКУ АДВОКАТА\n"
        f"Категорія: {cat} - {CAT_LABEL.get(cat,'')}\n"
        f"user_id: {update.effective_user.id}\n"
        f"chat_id: {lawyer_chat_id}\n"
        f"Підтвердити: /approve {token}\n"
        f"Відхилити: /reject {token}"
    )
    await context.bot.send_message(chat_id=ADMIN_ID, text=text_admin)

    await update.message.reply_text("Очікуйте підтвердження.")

async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    args = (update.message.text or "").split()
    if len(args) < 2:
        await update.message.reply_text("Формат: /approve <token>")
        return
    token = args[1].strip()

    row = get_bind_request(token)
    if not row:
        await update.message.reply_text("Токен не знайдено або вже оброблено.")
        return

    _, cat, lawyer_chat_id, status = row
    if status != "pending":
        await update.message.reply_text("Токен не знайдено або вже оброблено.")
        return

    set_lawyer(cat, lawyer_chat_id)
    mark_bind_request(token, "approved")
    inc_counter(f"lawyer_bound:{cat}")

    await context.bot.send_message(
        chat_id=lawyer_chat_id,
        text=f"Вас прив'язано як адвоката для категорії {cat} ({CAT_LABEL.get(cat,'')})."
    )
    await update.message.reply_text("Готово. Адвоката прив'язано.")

async def reject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    args = (update.message.text or "").split()
    if len(args) < 2:
        await update.message.reply_text("Формат: /reject <token>")
        return
    token = args[1].strip()

    row = get_bind_request(token)
    if not row:
        await update.message.reply_text("Токен не знайдено або вже оброблено.")
        return

    _, cat, lawyer_chat_id, status = row
    if status != "pending":
        await update.message.reply_text("Токен не знайдено або вже оброблено.")
        return

    mark_bind_request(token, "rejected")
    inc_counter(f"lawyer_bind_rejected:{cat}")

    await context.bot.send_message(chat_id=lawyer_chat_id, text="Запит на прив'язку відхилено.")
    await update.message.reply_text("Відхилено.")

# ----------------------------
# Helpers (typing effect)
# ----------------------------
async def _send_thinking(update: Update) -> int:
    # "Пише..." як було раніше: одне повідомлення одразу, потім редагуємо крапки
    msg = await update.message.reply_text("Пише.")
    return msg.message_id

async def _update_thinking(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, step: int):
    dots = "." * (step % 3 + 1)
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"Пише{dots}")
    except Exception:
        pass

async def _delete_thinking(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

# ----------------------------
# Main text handler
# ----------------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    user_id = update.effective_user.id

    # global buttons
    if text == "🚀 РОЗПОЧАТИ":
        inc_counter("pressed_start")
        context.user_data[MODE] = "menu"
        _reset_dialog(context)
        await update.message.reply_text("Оберіть дію:", reply_markup=KB_MENU)
        return

    if text == "🚪 ВИЙТИ":
        inc_counter("pressed_exit")
        context.user_data[MODE] = "menu"
        _reset_dialog(context)
        await update.message.reply_text(GOODBYE_TEXT, reply_markup=KB_ONLY_START)
        return

    if text == "⛔ СТОП":
        inc_counter("pressed_stop")
        context.user_data[MODE] = "menu"
        # стоп = повертаємо меню без "про бота" не треба - але залишимо в меню, як у вас було раніше
        await update.message.reply_text("Зупинив діалог. Оберіть дію:", reply_markup=KB_MENU)
        return

    if text == "ℹ️ ПРО БОТА":
        inc_counter("pressed_about")
        context.user_data[MODE] = "menu"
        await update.message.reply_text(ABOUT_TEXT, reply_markup=KB_MENU)
        return

    # Lawyer connect entry
    if text == "🧑‍⚖️ ЗВ'ЯЗАТИСЯ З АДВОКАТОМ":
        inc_counter("pressed_lawyer")
        context.user_data[MODE] = "lawyer_menu"
        await update.message.reply_text("Оберіть напрям:", reply_markup=KB_LAWYER_CATS)
        return

    # lawyer category choice
    if text in ("🪖 МОБІЛІЗАЦІЯ / ВІДСТРОЧКИ", "💰 ВИПЛАТИ ВІЙСЬКОВИМ", "🏛 ЦИВІЛЬНІ / СОЦВИПЛАТИ"):
        if text.startswith("🪖"):
            cat = CAT_MOB
        elif text.startswith("💰"):
            cat = CAT_MIL_PAY
        else:
            cat = CAT_CIVIL

        context.user_data[LAW_CAT] = cat
        context.user_data[MODE] = "lawyer_action"
        inc_counter(f"lawyer_cat_open:{cat}")

        contact = CONTACT_TEXT.get(cat) or "Контакти тимчасово не налаштовані."
        msg = (
            f"АДВОКАТИ: {CAT_LABEL.get(cat,'')}\n\n"
            f"{contact}\n\n"
            "За потреби ви можете надіслати запит на зворотний зв'язок адвокату."
        )
        await update.message.reply_text(msg, reply_markup=KB_LAWYER_ACTIONS)
        return

    if text == "↩️ НАЗАД":
        context.user_data[MODE] = "menu"
        await update.message.reply_text("Оберіть дію:", reply_markup=KB_MENU)
        return

    if text == "✍️ НАДІСЛАТИ ЗАПИТ АДВОКАТУ":
        cat = context.user_data.get(LAW_CAT)
        if cat not in (CAT_CIVIL, CAT_MIL_PAY, CAT_MOB):
            await update.message.reply_text("Оберіть напрям адвоката спочатку.", reply_markup=KB_LAWYER_CATS)
            return

        # check bound lawyer exists
        lawyer_chat = get_lawyer_chat(cat)
        if not lawyer_chat:
            inc_counter(f"lawyer_request_no_lawyer:{cat}")
            await update.message.reply_text(
                "Функція зворотного зв'язку для цього напряму поки не підключена. "
                "Спробуйте інший напрям або напишіть питання боту.",
                reply_markup=KB_MENU
            )
            return

        context.user_data[MODE] = "lawyer_form"
        inc_counter(f"lawyer_request_form:{cat}")

        await update.message.reply_text(
            "Напишіть одним повідомленням:\n"
            "1) Ім'я та по батькові\n"
            "2) Контактний номер телефону\n"
            "3) Місто/область\n"
            "4) Коротко суть питання\n\n"
            "Не надсилайте паспортні дані, ІПН та точні адреси.",
            reply_markup=KB_ASKING
        )
        return

    # Asking mode start
    if text == "❓ ЗАДАТИ ПИТАННЯ":
        inc_counter("pressed_ask")
        context.user_data[MODE] = "asking"
        context.user_data[COUNT_BLOCK] = 0
        context.user_data[HISTORY] = []
        await update.message.reply_text(ASK_HINT_FIRST, reply_markup=KB_ASKING)
        return

    # After-5 options
    if text == "➡️ ПРОДОВЖИТИ ДІАЛОГ":
        inc_counter("after5_continue")
        context.user_data[MODE] = "asking"
        context.user_data[COUNT_BLOCK] = 0
        await update.message.reply_text("Пишіть наступне уточнення/питання.", reply_markup=KB_ASKING)
        return

    if text == "🆕 ЗАДАТИ НОВЕ ПИТАННЯ":
        inc_counter("after5_new")
        context.user_data[MODE] = "asking"
        context.user_data[COUNT_BLOCK] = 0
        context.user_data[HISTORY] = []  # reset context
        await update.message.reply_text(ASK_HINT_NEXT, reply_markup=KB_ASKING)
        return

    # Lawyer form submission
    if context.user_data.get(MODE) == "lawyer_form":
        cat = context.user_data.get(LAW_CAT)
        lawyer_chat = get_lawyer_chat(cat)
        if not lawyer_chat:
            await update.message.reply_text("Адвокат для цього напряму не налаштований.", reply_markup=KB_MENU)
            context.user_data[MODE] = "menu"
            return

        # build payload to lawyer (NO @username visible)
        username = (update.effective_user.username or "").strip().lstrip("@")
        client_id = update.effective_user.id

        payload = (
            "НОВИЙ ЗАПИТ НА ЗВОРОТНИЙ ЗВ'ЯЗОК\n"
            f"Категорія: {cat} - {CAT_LABEL.get(cat,'')}\n"
            f"ID клієнта: {client_id}\n\n"
            f"{text}\n\n"
            f"meta_username: {username}\n"
            f"meta_client_id: {client_id}\n"
        )

        # inline button with callback (stats counted on click)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✉️ ЗВ'ЯЗАТИСЯ З КЛІЄНТОМ", callback_data=f"CONTACT|{cat}")]
        ])

        await context.bot.send_message(chat_id=lawyer_chat, text=payload, reply_markup=kb)
        inc_counter(f"lawyer_request_sent:{cat}")

        await update.message.reply_text(
            "Дякую. Запит передано адвокату. Очікуйте, будь ласка, зворотного зв'язку.",
            reply_markup=KB_MENU
        )
        context.user_data[MODE] = "menu"
        return

    # If not in asking mode - guidance
    mode = context.user_data.get(MODE, "menu")
    if mode not in ("asking",):
        await update.message.reply_text("Оберіть дію кнопками нижче.", reply_markup=KB_MENU)
        return

    # ----------------------------
    # Asking mode: AI answer
    # ----------------------------
    # enforce free limit 20/24h
    allowed, remaining, locked_until = check_free_limit(user_id, max_q=20, window_hours=24)
    if not allowed:
        inc_counter("free_limit_blocked")
        when = ""
        if locked_until:
            # локальний формат без прив'язки до зони
            when = f"Доступ відновиться після: {locked_until.strftime('%d.%m.%Y %H:%M')} (UTC)"
        await update.message.reply_text(
            LIMIT_TEXT_TEMPLATE.format(when=when),
            reply_markup=KB_AFTER_LIMIT
        )
        return

    # typing message
    thinking_id = await _send_thinking(update)

    # small animation while waiting
    # (safe: just 2 edits)
    await _update_thinking(context, update.effective_chat.id, thinking_id, 1)

    try:
        inc_counter("ai_request")

        # build conversation input with history
        history = context.user_data.get(HISTORY, [])
        input_msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
        # include previous history
        for m in history:
            input_msgs.append(m)
        input_msgs.append({"role": "user", "content": text})

        resp = client.responses.create(
            model="gpt-4o-mini",
            input=input_msgs,
            temperature=0.25,
        )

        answer = (resp.output_text or "").strip()
        if not answer:
            answer = "Не зміг сформувати відповідь. Спробуйте переформулювати питання коротко і по суті."

        # delete "Пише..."
        await _delete_thinking(context, update.effective_chat.id, thinking_id)

        await update.message.reply_text(answer, reply_markup=KB_ASKING)

        # update history
        _append_history(context, "user", text)
        _append_history(context, "assistant", answer)

        inc_counter("ai_answer")
        increment_after_answer(user_id, max_q=20, window_hours=24)

        # count per 5-block
        context.user_data[COUNT_BLOCK] = int(context.user_data.get(COUNT_BLOCK, 0)) + 1

        if context.user_data[COUNT_BLOCK] >= 5:
            context.user_data[MODE] = "after5"
            context.user_data[COUNT_BLOCK] = 0
            await update.message.reply_text(
                "Продовжуємо діалог чи починаємо нове питання? За потреби можна зв'язатися з адвокатом.",
                reply_markup=KB_AFTER_5
            )
        else:
            context.user_data[MODE] = "asking"

    except Exception as e:
        await _delete_thinking(context, update.effective_chat.id, thinking_id)
        inc_counter("ai_error")
        await update.message.reply_text(
            f"Помилка AI-запиту: {type(e).__name__}. Спробуйте пізніше.",
            reply_markup=KB_MENU
        )
        context.user_data[MODE] = "menu"

# ----------------------------
# Non-text handler
# ----------------------------
async def handle_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    inc_counter("non_text")
    await update.message.reply_text(
        "Я приймаю лише текстові повідомлення. Будь ласка, напишіть питання текстом.",
        reply_markup=KB_MENU
    )

# ----------------------------
# Callback from lawyer inline button
# ----------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    data = q.data or ""
    parts = data.split("|")
    if len(parts) < 2 or parts[0] != "CONTACT":
        return

    cat = parts[1].strip().upper()
    inc_counter(f"lawyer_contact_click:{cat}")

    # parse meta from message text
    text = q.message.text or ""
    m_u = re.search(r"meta_username:\s*([A-Za-z0-9_]+)", text)
    username = m_u.group(1) if m_u else ""

    m_id = re.search(r"meta_client_id:\s*(\d+)", text)
    client_id = m_id.group(1) if m_id else ""

    if username:
        link = f"https://t.me/{username}"
        msg = (
            "ЗВ'ЯЗОК З КЛІЄНТОМ\n\n"
            f"Клієнт: @{username}\n"
            f"Посилання: {link}\n\n"
            "Якщо чат не відкривається - клієнт міг обмежити приватні повідомлення. "
            "Тоді попросіть клієнта написати вам першим або використайте номер телефону зі заявки."
        )
    else:
        link = f"tg://user?id={client_id}" if client_id else "(ID не знайдено)"
        msg = (
            "ЗВ'ЯЗОК З КЛІЄНТОМ\n\n"
            f"ID клієнта: {client_id}\n"
            f"Спроба відкрити чат: {link}\n\n"
            "Якщо посилання не відкривається - це обмеження Telegram/Android або приватності клієнта. "
            "Попросіть клієнта написати вам першим або використайте номер телефону зі заявки."
        )

    await context.bot.send_message(chat_id=q.message.chat_id, text=msg)

# ----------------------------
# main
# ----------------------------
def main() -> None:
    ensure_tables()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("bind", bind_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("reject", reject_cmd))

    app.add_handler(CallbackQueryHandler(on_callback))

    # text & non-text
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, handle_non_text))

    # webhook if configured, else polling
    if WEBHOOK_BASE_URL:
        webhook_url = f"{WEBHOOK_BASE_URL.rstrip('/')}/{WEBHOOK_SECRET}"
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_SECRET,
            webhook_url=webhook_url,
            close_loop=False
        )
    else:
        app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

