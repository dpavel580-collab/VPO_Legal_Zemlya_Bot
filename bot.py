import os
import re
import secrets
from dotenv import load_dotenv

from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from telegram.constants import ChatAction
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

from openai import OpenAI

from db import (
    init_db, user_hash, add_event, get_stats,
    set_lawyer, get_lawyer,
    create_pending_bind, get_pending_bind, delete_pending_bind
)

# ----------------------------
# ENV
# ----------------------------
load_dotenv(override=True)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "10000"))

ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "").strip()
LAWYER_BIND_CODE = os.getenv("LAWYER_BIND_CODE", "").strip()

LAWYER_CONTACT_MIL_PAY = os.getenv("LAWYER_CONTACT_MIL_PAY", "").strip()
LAWYER_CONTACT_MOB = os.getenv("LAWYER_CONTACT_MOB", "").strip()
LAWYER_CONTACT_CIVIL = os.getenv("LAWYER_CONTACT_CIVIL", "").strip()

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")
if not WEBHOOK_BASE_URL:
    raise RuntimeError("Missing WEBHOOK_BASE_URL")
if not WEBHOOK_SECRET:
    raise RuntimeError("Missing WEBHOOK_SECRET")
if not ADMIN_TELEGRAM_ID:
    raise RuntimeError("Missing ADMIN_TELEGRAM_ID")
if not LAWYER_BIND_CODE:
    raise RuntimeError("Missing LAWYER_BIND_CODE")

client = OpenAI(api_key=OPENAI_API_KEY)

# ----------------------------
# SETTINGS
# ----------------------------
MODEL_NAME = "gpt-4o-mini"
MAX_Q_PER_BLOCK = 5
MAX_HISTORY_TURNS = 24

# ----------------------------
# CATEGORIES (3 адвокати)
# ----------------------------
CAT_MIL_PAY = "MIL_PAY"
CAT_MOB = "MOB"
CAT_CIVIL = "CIVIL"

CAT_LABEL = {
    CAT_MIL_PAY: "ВІЙСЬКОВІ АДВОКАТИ - ВИПЛАТИ",
    CAT_MOB: "ВІЙСЬКОВІ АДВОКАТИ - МОБІЛІЗАЦІЯ ТА ВІДСТРОЧКИ",
    CAT_CIVIL: "ЦИВІЛЬНІ АДВОКАТИ - ПЕНСІЇ, КОМПЕНСАЦІЇ, СОЦВИПЛАТИ, СПАДЩИНА, ПОДАТКИ",
}

def detect_category(q: str) -> str:
    s = (q or "").lower()
    if any(x in s for x in ["тцк", "влк", "повіст", "відстроч", "мобіліз", "бронь", "відстрочка"]):
        return CAT_MOB
    if any(x in s for x in ["виплат", "грошов", "забезпеч", "убд", "поран", "військов", "компенсац військ"]):
        return CAT_MIL_PAY
    return CAT_CIVIL

def contacts_text(cat: str) -> str:
    if cat == CAT_MIL_PAY:
        return LAWYER_CONTACT_MIL_PAY or "Контакти для цього напрямку ще не додані."
    if cat == CAT_MOB:
        return LAWYER_CONTACT_MOB or "Контакти для цього напрямку ще не додані."
    return LAWYER_CONTACT_CIVIL or "Контакти для цього напрямку ще не додані."

# ----------------------------
# BUTTONS
# ----------------------------
BTN_START = "🚀 РОЗПОЧАТИ"
BTN_ASK = "❓ ЗАДАТИ ПИТАННЯ"
BTN_ABOUT = "ℹ️ ПРО БОТ"
BTN_ADV = "👨‍⚖️ ЗВ'ЯЗАТИСЯ З АДВОКАТОМ"
BTN_STOP = "🛑 СТОП"
BTN_EXIT = "👋 ВИЙТИ"

BTN_CAT_MIL_PAY = "⚔️ ВІЙСЬКОВІ - ВИПЛАТИ"
BTN_CAT_MOB = "🪖 ВІЙСЬКОВІ - МОБІЛІЗАЦІЯ/ВІДСТРОЧКИ"
BTN_CAT_CIVIL = "🏛 ЦИВІЛЬНІ - СОЦ/ПЕНСІЇ/КОМПЕНСАЦІЇ"

BTN_SEND_REQ = "✍️ НАДІСЛАТИ ЗАПИТ АДВОКАТУ"
BTN_CANCEL = "❌ СКАСУВАТИ"

BTN_CONTINUE = "💬 ПРОДОВЖИТИ"
BTN_NEW_Q = "🆕 НОВЕ ПИТАННЯ"

STOP_WORDS = {"стоп", "stop", "пауза", "досить"}
EXIT_WORDS = {"вийти", "exit", "вихід"}

# ----------------------------
# TEXTS
# ----------------------------
WELCOME_TEXT = (
    "Вітаю!\n\n"
    "Це юридичний чат-бот від Асоціації Розвитку та Реконструкції Регіонів України.\n\n"
    "Ми реалізуємо програму \"Є-ЗЕМЛЯ\" - ініціативу, спрямовану на захист прав внутрішньо переміщених осіб "
    "та власників майна/землі на тимчасово окупованих територіях України.\n\n"
    "Бот допомагає отримати структуровані юридичні роз'яснення щодо:\n"
    "- компенсацій за майно\n"
    "- статусу ВПО\n"
    "- соціальних та державних виплат\n"
    "- спадкування\n"
    "- майна в окупації\n"
    "- питань мобілізації та військових виплат (в межах чинного законодавства)\n"
    "- пенсійних та соціальних питань\n\n"
    "За потреби ви можете отримати контакти адвокатів для професійної консультації.\n\n"
    f"Натисніть \"{BTN_START}\"."
)

WELCOME_AFTER_START = (
    "Оберіть дію:\n"
    f"- {BTN_ASK}\n"
    f"- {BTN_ADV}\n"
    f"- {BTN_ABOUT}\n\n"
    "Порада: пишіть коротко і по суті. Без паспортних даних/ІПН/точних адрес."
)

ABOUT_TEXT = (
    "ПРО БОТ\n\n"
    "Цей чат-бот створений для:\n"
    "- ВПО\n"
    "- власників майна та землі на ТОТ\n"
    "- військових та військовозобов'язаних\n"
    "- пенсіонерів\n"
    "- осіб з інвалідністю\n"
    "- інших категорій громадян\n\n"
    "Бот надає короткі, структуровані та зрозумілі консультації щодо:\n"
    "- компенсацій\n"
    "- державних програм підтримки\n"
    "- соціальних виплат\n"
    "- спадкування\n"
    "- майна в окупації\n"
    "- мобілізації та військових виплат\n\n"
    "Важливо:\n"
    "- Бот не є адвокатом і не замінює індивідуальну правову допомогу.\n"
    "- Відповіді мають інформаційний характер.\n"
    "- У складних випадках рекомендується звернутися до адвоката.\n\n"
    "Проєкт реалізується в рамках програми \"Є-ЗЕМЛЯ\" Асоціацією розвитку та реконструкції регіонів України."
)

ASK_HINT_FIRST = (
    "Напишіть питання одним повідомленням (лише текст).\n\n"
    "Щоб відповідь була точнішою, додайте 3-5 деталей:\n"
    "1) область і місто/громада (без адреси)\n"
    "2) суть ситуації (1-3 речення)\n"
    "3) що саме потрібно\n"
    "4) якщо була відмова - коротко що написали (без персональних даних)\n"
)

ASK_HINT_NEXT = "Опишіть наступне питання по суті (лише текст) - коротко, але з деталями."

EXIT_TEXT = (
    "Дякую за користування ботом.\n\n"
    "Разом, об'єднуючи зусилля, ми досягнемо реальних результатів у впровадженні механізмів компенсації "
    "за втрачене майно та землю.\n\n"
    "Програму \"Є-ЗЕМЛЯ\" реалізує Асоціація Розвитку та Реконструкції Регіонів України.\n"
    "Над її впровадженням активно працюють Павло Деркач, Олег Ситник, Андрій Підставський, Дмитро Овєчкін.\n\n"
    "Якщо бот був корисним - поділіться ним з тими, кому це може допомогти.\n\n"
    "Бажаю вам сили, витримки та справедливого результату."
)

STOP_TEXT = "Зрозуміло. Діалог зупинено. Можете поставити нове питання або вийти."
AFTER_BLOCK_TEXT = "Ви поставили 5 питань підряд. Продовжуємо, ставимо нове питання чи звертаємось до адвоката?"
NON_TEXT_TEXT = "Я приймаю лише текстові повідомлення. Будь ласка, напишіть питання текстом."

REQUEST_INSTRUCTIONS = (
    "Щоб передати запит адвокату, напишіть одним повідомленням:\n"
    "1) Ім'я та По батькові\n"
    "2) Контактний номер телефону\n"
    "3) Коротко суть питання (1-3 речення)\n\n"
    "Не надсилайте паспортні дані, ІПН або точні адреси.\n"
    "Ці дані будуть передані адвокату для зворотного зв'язку."
)

# ----------------------------
# PROMPT
# ----------------------------
SYSTEM_PROMPT_BASE = (
    "Ти - українськомовний юридичний консультант інформаційного характеру.\n"
    "Аудиторія: ВПО, власники майна на ТОТ, військові, військовозобов'язані, пенсіонери, особи з інвалідністю.\n\n"
    "Стиль:\n"
    "- професійно і людяно, без сухості\n"
    "- без однакових вступів у кожній відповіді\n"
    "- без повторів попередньої інформації слово-в-слово\n"
    "- без markdown (**,#,* не використовувати)\n"
    "- заголовки роби КАПСОМ на окремому рядку\n\n"
    "Структура:\n"
    "СУТЬ ПИТАННЯ\n"
    "КРОКИ ДІЙ (по пунктах)\n"
    "ДОКУМЕНТИ (якщо доречно)\n"
    "КУДИ ЗВЕРНУТИСЯ (тип установи та як знайти контакти у своєму місті)\n"
    "ВАЖЛИВІ НЮАНСИ\n\n"
    "Мобілізація - лише в межах чинного законодавства, без порад щодо ухилення.\n"
    "Не проси паспортні дані, ІПН або точні адреси.\n"
    "Якщо для відповіді потрібне місто/область - постав 1-2 уточнюючі питання.\n"
)

# ----------------------------
# KEYBOARDS
# ----------------------------
KB_ONLY_START = ReplyKeyboardMarkup([[KeyboardButton(BTN_START)]], resize_keyboard=True)

KB_MAIN = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_ASK), KeyboardButton(BTN_ADV)],
     [KeyboardButton(BTN_ABOUT)],
     [KeyboardButton(BTN_STOP), KeyboardButton(BTN_EXIT)]],
    resize_keyboard=True
)

KB_CHAT = ReplyKeyboardMarkup([[KeyboardButton(BTN_STOP), KeyboardButton(BTN_EXIT)]], resize_keyboard=True)

KB_AFTER_BLOCK = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_CONTINUE), KeyboardButton(BTN_NEW_Q)],
     [KeyboardButton(BTN_ADV)],
     [KeyboardButton(BTN_STOP), KeyboardButton(BTN_EXIT)]],
    resize_keyboard=True
)

def kb_advocats():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(BTN_CAT_MIL_PAY)],
         [KeyboardButton(BTN_CAT_MOB)],
         [KeyboardButton(BTN_CAT_CIVIL)],
         [KeyboardButton(BTN_STOP), KeyboardButton(BTN_EXIT)]],
        resize_keyboard=True
    )

def kb_after_contacts():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(BTN_SEND_REQ), KeyboardButton(BTN_CANCEL)],
         [KeyboardButton(BTN_STOP), KeyboardButton(BTN_EXIT)]],
        resize_keyboard=True
    )

# Inline кнопка для адвоката (callback)
def ikb_lawyer_contact(cat: str, client_id: int) -> InlineKeyboardMarkup:
    data = f"CONTACT|{cat}|{client_id}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✉️ ЗВ'ЯЗАТИСЯ З КЛІЄНТОМ", callback_data=data)]
    ])

# ----------------------------
# STATE
# ----------------------------
MODE = "mode"
COUNT = "count"
HISTORY = "history"
ASKED_ONCE = "asked_once"
LAST_CAT = "last_cat"
AWAIT_CAT = "await_cat"

def is_admin(update: Update) -> bool:
    return str(update.effective_user.id) == ADMIN_TELEGRAM_ID

def push_history(context: ContextTypes.DEFAULT_TYPE, role: str, content: str):
    hist = context.user_data.get(HISTORY, [])
    hist.append({"role": role, "content": content})
    if len(hist) > MAX_HISTORY_TURNS:
        hist = hist[-MAX_HISTORY_TURNS:]
    context.user_data[HISTORY] = hist

def sanitize_answer(s: str) -> str:
    if not s:
        return s
    s = s.replace("**", "").replace("*", "").replace("#", "")
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s

# ----------------------------
# COMMANDS
# ----------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.clear()
    context.user_data[MODE] = "menu"
    context.user_data[COUNT] = 0
    await update.message.reply_text(WELCOME_TEXT, reply_markup=KB_ONLY_START)

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    data = get_stats(days=7)

    def get(m, k): return int(m.get(k, 0))

    lines = []
    lines.append(f"СТАТИСТИКА ЗА {data['days']} ДНІВ")
    lines.append(f"УНІКАЛЬНІ КОРИСТУВАЧІ (ЗА ПОДІЯМИ): {data['unique_users']}")
    lines.append("")

    lines.append("ПИТАННЯ (КІЛЬКІСТЬ):")
    for c in [CAT_MOB, CAT_MIL_PAY, CAT_CIVIL]:
        lines.append(f"- {c}: {get(data['asked_question'], c)}")

    lines.append("")
    lines.append("ВИБІР КАТЕГОРІЇ АДВОКАТА (НАТИСКАННЯ):")
    for c in [CAT_MOB, CAT_MIL_PAY, CAT_CIVIL]:
        lines.append(f"- {c}: {get(data['pick_advocate'], c)}")

    lines.append("")
    lines.append("НАТИСНУЛИ 'НАДІСЛАТИ ЗАПИТ' (НАМІР):")
    for c in [CAT_MOB, CAT_MIL_PAY, CAT_CIVIL]:
        lines.append(f"- {c}: {get(data['request_click'], c)}")

    lines.append("")
    lines.append("ЗАПИТ ВІДПРАВЛЕНО АДВОКАТУ (ФАКТ):")
    for c in [CAT_MOB, CAT_MIL_PAY, CAT_CIVIL]:
        sent = get(data['request_sent'], c)
        click = get(data['request_click'], c)
        conv = (sent / click * 100) if click else 0.0
        lines.append(f"- {c}: {sent} (КОНВЕРСІЯ {conv:.1f}%)")

    lines.append("")
    lines.append("АДВОКАТ НАТИСНУВ 'ЗВ'ЯЗАТИСЯ З КЛІЄНТОМ':")
    for c in [CAT_MOB, CAT_MIL_PAY, CAT_CIVIL]:
        lines.append(f"- {c}: {get(data.get('lawyer_contact_click', {}), c)}")

    await update.message.reply_text("\n".join(lines))

# адвокати: /bind CODE CATEGORY
async def bind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    parts = (update.message.text or "").strip().split()
    if len(parts) != 3:
        await update.message.reply_text("Формат: /bind <КОД> <MIL_PAY|MOB|CIVIL>")
        return

    code = parts[1].strip()
    cat = parts[2].strip().upper()

    if code != LAWYER_BIND_CODE:
        await update.message.reply_text("Невірний код прив'язки.")
        return
    if cat not in (CAT_MIL_PAY, CAT_MOB, CAT_CIVIL):
        await update.message.reply_text("Невірна категорія. Доступні: MIL_PAY, MOB, CIVIL")
        return

    token = secrets.token_urlsafe(8)
    create_pending_bind(token, cat, update.effective_chat.id, update.effective_user.id)

    msg = (
        "ЗАПИТ НА ПРИВ'ЯЗКУ АДВОКАТА\n"
        f"Категорія: {cat} - {CAT_LABEL.get(cat, cat)}\n"
        f"user_id: {update.effective_user.id}\n"
        f"chat_id: {update.effective_chat.id}\n"
        f"Підтвердити: /approve {token}\n"
        f"Відхилити: /reject {token}"
    )
    await context.bot.send_message(chat_id=int(ADMIN_TELEGRAM_ID), text=msg)
    await update.message.reply_text("Запит на прив'язку відправлено адміну. Очікуйте підтвердження.")

async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    parts = (update.message.text or "").strip().split()
    if len(parts) != 2:
        await update.message.reply_text("Формат: /approve <TOKEN>")
        return
    token = parts[1].strip()
    data = get_pending_bind(token)
    if not data:
        await update.message.reply_text("Токен не знайдено або вже оброблено.")
        return

    set_lawyer(data["category"], data["lawyer_chat_id"])
    delete_pending_bind(token)

    await update.message.reply_text(f"ОК. Прив'язано {data['category']} -> chat_id {data['lawyer_chat_id']}")
    try:
        await context.bot.send_message(
            chat_id=int(data["lawyer_chat_id"]),
            text=f"Вас прив'язано як адвоката для категорії {data['category']} ({CAT_LABEL.get(data['category'],'')})."
        )
    except Exception:
        pass

async def reject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    parts = (update.message.text or "").strip().split()
    if len(parts) != 2:
        await update.message.reply_text("Формат: /reject <TOKEN>")
        return
    token = parts[1].strip()
    data = get_pending_bind(token)
    if not data:
        await update.message.reply_text("Токен не знайдено або вже оброблено.")
        return
    delete_pending_bind(token)
    await update.message.reply_text("Відхилено.")
    try:
        await context.bot.send_message(chat_id=int(data["lawyer_chat_id"]), text="Запит на прив'язку відхилено адміністратором.")
    except Exception:
        pass

# ----------------------------
# CALLBACK: lawyer presses "contact client"
# ----------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    await q.answer()

    parts = q.data.split("|")
    if len(parts) != 3 or parts[0] != "CONTACT":
        return

    cat = parts[1]
    try:
        client_id = int(parts[2])
    except ValueError:
        return

    # Логуємо: адвокат натиснув "зв'язатися"
    lawyer_id = q.from_user.id
    add_event(user_hash(lawyer_id), "lawyer_contact_click", cat)

    # Надсилаємо адвокату посилання для відкриття приватного чату
    # tg://user?id=... працює в Telegram-клієнтах
    link = f"tg://user?id={client_id}"
    msg = (
        "ЗВ'ЯЗОК З КЛІЄНТОМ\n"
        f"Категорія: {cat} ({CAT_LABEL.get(cat,'')})\n\n"
        f"Натисніть посилання, щоб відкрити приватний чат:\n{link}\n\n"
        "Якщо посилання не відкривається, клієнт міг обмежити приватні повідомлення. "
        "Тоді попросіть клієнта написати вам напряму або використайте номер телефону з заявки."
    )
    await context.bot.send_message(chat_id=q.message.chat_id, text=msg)

# ----------------------------
# HANDLERS
# ----------------------------
async def handle_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(NON_TEXT_TEXT, reply_markup=KB_CHAT)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    low = text.lower()

    if low in STOP_WORDS or text == BTN_STOP:
        context.user_data.clear()
        await update.message.reply_text(STOP_TEXT, reply_markup=KB_MAIN)
        return

    if low in EXIT_WORDS or text == BTN_EXIT:
        context.user_data.clear()
        await update.message.reply_text(EXIT_TEXT, reply_markup=KB_ONLY_START)
        return

    if text == BTN_START:
        context.user_data.clear()
        context.user_data[MODE] = "menu"
        context.user_data[COUNT] = 0
        await update.message.reply_text(WELCOME_AFTER_START, reply_markup=KB_MAIN)
        return

    if text == BTN_ABOUT:
        await update.message.reply_text(ABOUT_TEXT, reply_markup=KB_MAIN)
        return

    if text == BTN_ADV:
        await update.message.reply_text("Оберіть напрямок адвоката:", reply_markup=kb_advocats())
        return

    if text in (BTN_CAT_MIL_PAY, BTN_CAT_MOB, BTN_CAT_CIVIL):
        if text == BTN_CAT_MIL_PAY:
            cat = CAT_MIL_PAY
        elif text == BTN_CAT_MOB:
            cat = CAT_MOB
        else:
            cat = CAT_CIVIL

        context.user_data[LAST_CAT] = cat
        uh = user_hash(update.effective_user.id)
        add_event(uh, "pick_advocate", cat)

        await update.message.reply_text(contacts_text(cat), reply_markup=kb_after_contacts())
        return

    if text == BTN_CANCEL:
        context.user_data[AWAIT_CAT] = None
        await update.message.reply_text("Скасовано. Можете поставити питання або звернутися до адвоката.", reply_markup=KB_MAIN)
        return

    if text == BTN_SEND_REQ:
        cat = context.user_data.get(LAST_CAT)
        if not cat:
            await update.message.reply_text("Спочатку оберіть напрямок адвоката.", reply_markup=kb_advocats())
            return

        if not get_lawyer(cat):
            await update.message.reply_text("Для цього напрямку ще не підключено адвоката для зворотного зв'язку.", reply_markup=kb_advocats())
            return

        uh = user_hash(update.effective_user.id)
        add_event(uh, "request_click", cat)

        context.user_data[AWAIT_CAT] = cat
        await update.message.reply_text(REQUEST_INSTRUCTIONS, reply_markup=KB_CHAT)
        return

    awaiting = context.user_data.get(AWAIT_CAT)
    if awaiting:
        lawyer_chat_id = get_lawyer(awaiting)
        if not lawyer_chat_id:
            context.user_data[AWAIT_CAT] = None
            await update.message.reply_text("Напрямок тимчасово недоступний. Спробуйте пізніше.", reply_markup=KB_MAIN)
            return

        # Формуємо payload адвокату + inline кнопку "зв'язатися"
        client_id = update.effective_user.id
        client_username = update.effective_user.username or ""
        client_line = f"@{client_username}" if client_username else "(username відсутній)"

        payload = (
            "НОВИЙ ЗАПИТ НА ЗВОРОТНИЙ ЗВ'ЯЗОК\n"
            f"Категорія: {awaiting} - {CAT_LABEL.get(awaiting,'')}\n"
            f"Клієнт: {client_line}\n"
            f"ID клієнта: {client_id}\n\n"
            f"{text}"
        )

        try:
            await context.bot.send_message(
                chat_id=int(lawyer_chat_id),
                text=payload,
                reply_markup=ikb_lawyer_contact(awaiting, client_id)
            )
            uh = user_hash(update.effective_user.id)
            add_event(uh, "request_sent", awaiting)

            await update.message.reply_text("Запит передано адвокату. Дякую.", reply_markup=KB_MAIN)
        except Exception:
            await update.message.reply_text("Не вдалося відправити запит. Спробуйте пізніше.", reply_markup=KB_MAIN)

        context.user_data[AWAIT_CAT] = None
        return

    if text == BTN_ASK:
        context.user_data[MODE] = "asking"
        context.user_data[COUNT] = 0
        context.user_data.setdefault(HISTORY, [])
        hint = ASK_HINT_FIRST if not context.user_data.get(ASKED_ONCE, False) else ASK_HINT_NEXT
        context.user_data[ASKED_ONCE] = True
        await update.message.reply_text(hint, reply_markup=KB_CHAT)
        return

    if text == BTN_CONTINUE:
        context.user_data[MODE] = "asking"
        context.user_data[COUNT] = 0
        await update.message.reply_text("Добре. Напишіть наступне питання (текстом).", reply_markup=KB_CHAT)
        return

    if text == BTN_NEW_Q:
        context.user_data[MODE] = "asking"
        context.user_data[COUNT] = 0
        context.user_data[HISTORY] = []
        await update.message.reply_text(ASK_HINT_NEXT, reply_markup=KB_CHAT)
        return

    if context.user_data.get(MODE) != "asking":
        await update.message.reply_text("Оберіть дію кнопками нижче.", reply_markup=KB_MAIN)
        return

    # QUESTION -> AI
    q_text = text
    cat_for_stats = detect_category(q_text)

    uh = user_hash(update.effective_user.id)
    add_event(uh, "asked_question", cat_for_stats)

    context.user_data.setdefault(HISTORY, [])
    push_history(context, "user", q_text)

    input_msgs = [{"role": "system", "content": SYSTEM_PROMPT_BASE}] + context.user_data[HISTORY]

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

     resp = client.chat.completions.create(
    model=MODEL_NAME,
    messages=input_msgs,
    temperature=0.35,
)

        answer = sanitize_answer((resp.choices[0].message.content or "").strip())
        if not answer:
            answer = "Не вийшло сформувати відповідь. Спробуйте переформулювати питання."

        push_history(context, "assistant", answer)
        await update.message.reply_text(answer, reply_markup=KB_CHAT)

        context.user_data[COUNT] = int(context.user_data.get(COUNT, 0)) + 1
        if context.user_data[COUNT] >= MAX_Q_PER_BLOCK:
            context.user_data[COUNT] = 0
            await update.message.reply_text(AFTER_BLOCK_TEXT, reply_markup=KB_AFTER_BLOCK)

    except Exception as e:
        await update.message.reply_text(f"Помилка AI-запиту: {type(e).__name__}. Спробуйте пізніше.", reply_markup=KB_CHAT)

# ----------------------------
# MAIN (webhook)
# ----------------------------
def main() -> None:
    init_db()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))

    app.add_handler(CommandHandler("bind", bind_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("reject", reject_cmd))

    app.add_handler(CallbackQueryHandler(on_callback))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, handle_non_text))

    url_path = f"tg/{WEBHOOK_SECRET}"
    webhook_url = f"{WEBHOOK_BASE_URL.rstrip('/')}/{url_path}"

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=url_path,
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()


