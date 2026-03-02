import os
import re
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from openai import OpenAI

# ----------------------------
# 1) ENV
# ----------------------------
load_dotenv(override=True)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()  # e.g. https://my-bot.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()      # long random string
PORT = int(os.getenv("PORT", "10000"))                        # Render provides PORT

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in env")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY in env")
if not WEBHOOK_BASE_URL:
    raise RuntimeError("Missing WEBHOOK_BASE_URL in env (required for webhook mode)")
if not WEBHOOK_SECRET:
    raise RuntimeError("Missing WEBHOOK_SECRET in env (required for webhook mode)")

client = OpenAI(api_key=OPENAI_API_KEY)

# ----------------------------
# 2) SETTINGS
# ----------------------------
MAX_Q_PER_BLOCK = 5
MAX_HISTORY_TURNS = 28

# ----------------------------
# 3) BUTTONS
# ----------------------------
BTN_START = "🚀 РОЗПОЧАТИ"
BTN_ASK = "❓ ЗАДАТИ ПИТАННЯ"
BTN_ABOUT = "ℹ️ ПРО БОТ"
BTN_STOP = "🛑 СТОП"
BTN_EXIT = "👋 ВИЙТИ"
BTN_CONTINUE = "💬 ПРОДОВЖИТИ ДІАЛОГ"
BTN_NEW_Q = "🆕 ЗАДАТИ НОВЕ ПИТАННЯ"

STOP_WORDS = {"стоп", "stop", "зупинись", "зупинка", "пауза", "досить"}
EXIT_WORDS = {"вийти", "exit", "вихід", "виходжу"}

# ----------------------------
# 4) CONTACTS (generic)
# ----------------------------
CONTACTS_TEXT = (
    "КОРИСНІ ОФІЦІЙНІ КОНТАКТИ:\n"
    "• Безоплатна правнича допомога (БПД): 0 800 213 103\n"
    "• Урядова гаряча лінія: 1545\n"
    "• Консультації щодо виплат для ВПО (гаряча лінія): 0 800 33 18 34\n"
    "• Мінсоцполітики (довідково): (044) 204-72-73\n\n"
    "ЯК ЗНАЙТИ У СВОЄМУ МІСТІ:\n"
    "• ЦНАП/УСЗН: 'ЦНАП <місто>' або 'УСЗН <місто>' у Google/Maps\n"
    "• ПФУ: 'Сервісний центр ПФУ <місто>'\n"
    "• БПД: 'Бюро правничої допомоги <місто>'\n"
)

# ----------------------------
# 5) TEXTS
# ----------------------------
WELCOME_TEXT = (
    "Вітаю!\n\n"
    "Це юридичний чат-бот для внутрішньо переміщених осіб (ВПО) та власників майна "
    "на тимчасово окупованих територіях України.\n\n"
    "\"Є-ЗЕМЛЯ\" - програма, яку реалізує Асоціація Розвитку та Реконструкції Регіонів України.\n"
    "Шлях для ВПО отримати компенсацію за свою землю та майно, яке залишилось в окупації.\n\n"
    f"Натисніть кнопку \"{BTN_START}\"."
)

WELCOME_AFTER_START = (
    "Оберіть дію:\n"
    f"• \"{BTN_ASK}\" - поставити питання\n"
    f"• \"{BTN_ABOUT}\" - про бота та правила\n\n"
    "Порада: пишіть коротко і по суті. Без персональних даних."
)

ABOUT_TEXT = (
    "ПРО БОТ\n\n"
    "Я - юридичний чат-бот для ВПО та власників майна на тимчасово окупованих територіях України.\n"
    "Консультую (довідково) щодо:\n"
    "- статусу ВПО та соціальних виплат\n"
    "- держпрограм підтримки\n"
    "- майна в окупації / втрат майна\n"
    "- спадкування\n"
    "- грошової допомоги та міжнародних програм\n"
    "- соціальних програм\n"
    "- мобілізації (в межах чинного законодавства)\n\n"
    "ВАЖЛИВО:\n"
    "- я не адвокат і не замінюю індивідуальну консультацію\n"
    "- не прошу/не приймаю персональні дані (ПІБ, паспорт, ІПН, точні адреси)\n"
    "- не даю зразків документів\n\n"
    "\"Є-ЗЕМЛЯ\" - програма, яку реалізує Асоціація Розвитку та Реконструкції Регіонів України.\n"
)

ASK_HINT_FIRST = (
    "Напишіть питання одним повідомленням (лише текст).\n\n"
    "Щоб відповідь була точнішою, додайте 3-5 деталей:\n"
    "1) область і місто/громада (без адреси)\n"
    "2) суть ситуації (1-3 речення)\n"
    "3) що саме потрібно (виплата/довідка/спадщина/майно/мобілізація тощо)\n"
    "4) якщо була відмова - коротко що написали (без персональних даних)\n\n"
    "Ім'я можна вказати на початку (не обов'язково):\n"
    "\"Ірина. Підкажіть, будь ласка, ...\""
)

ASK_HINT_NEXT = "Опишіть наступне питання по суті (лише текст) - коротко, але з деталями."

STOP_TEXT = (
    "Зрозуміло. Я зупинив діалог.\n"
    "Можете поставити нове питання або вийти."
)

EXIT_TEXT = (
    "Дякую за звернення.\n"
    "Бажаю вам безпеки, сил і підтримки поруч.\n\n"
    "Команда \"Є-ЗЕМЛЯ\" - Павло Деркач, Олег Ситник, Андрій Підставський, Дмитро Овєчкін - "
    "активно працює над впровадженням на державному рівні механізмів компенсацій за втрачене майно. "
    "Ми також втратили наше майно і землю, тому добре розуміємо ціну цього питання.\n\n"
    "\"Є-ЗЕМЛЯ\" - програма, яку реалізує Асоціація Розвитку та Реконструкції Регіонів України.\n"
    "Шлях для ВПО отримати компенсацію за свою землю та майно, яке залишилось в окупації.\n\n"
    "Якщо бот був корисний - поділіться ним з тими, кому це може допомогти."
)

AFTER_BLOCK_TEXT = (
    f"Ви поставили {MAX_Q_PER_BLOCK} питань підряд.\n"
    "Що робимо далі?"
)

NON_TEXT_TEXT = (
    "Я приймаю лише текстові повідомлення.\n"
    "Будь ласка, напишіть питання текстом (без голосових, файлів, фото чи відео)."
)

# ----------------------------
# 6) PROMPT
# ----------------------------
SYSTEM_PROMPT_BASE = (
    "Ти - юридичний чат-бот для ВПО та власників майна на ТОТ України.\n"
    "Аудиторія часто в стресі/втратах. Тон: людяний, теплий, але професійний.\n\n"
    "СТИЛЬ:\n"
    "- Українською.\n"
    "- Без markdown: НЕ використовуй **, *, #.\n"
    "- Заголовки роби КАПСОМ на окремому рядку.\n"
    "- Відповіді можуть бути ДОВШІ (якщо потрібно), але без води.\n"
    "- Не починай кожну відповідь однаково. НЕ використовуй шаблон 'Ок, уточнюю'.\n\n"
    "АНТИ-ПОВТОРИ:\n"
    "- Якщо користувач уточнює: НЕ переписуй попереднє слово в слово.\n"
    "- Дай нову інформацію + що робити далі.\n\n"
    "ТЕМАТИКА:\n"
    "- ВПО: довідка, виплати, соцпідтримка, державні програми\n"
    "- Майно/втрата/ТОТ: фіксація, докази, захист прав, програми\n"
    "- Спадкування (загально), грошова допомога, міжнародні програми, соціальні програми\n"
    "- Мобілізація: лише в межах чинного законодавства (жодних порад щодо ухилення)\n\n"
    "ОБМЕЖЕННЯ:\n"
    "- Не проси ПІБ/паспорт/ІПН/точні адреси.\n"
    "- НЕ давай зразків документів/шаблонів заяв.\n\n"
    "КОЛИ ТРЕБА ЗВЕРТАТИСЯ ДО ОРГАНУ:\n"
    "- Якщо місто/область не вказані - запитай: 'В якій області та місті/громаді ви зараз?' (без адреси).\n"
    "- Потім дай: куди звернутися (тип установи), порядок дій, документи, як знайти контакти у своєму місті.\n\n"
    "СТРУКТУРА:\n"
    "СУТЬ (1-3 речення)\n"
    "КРОКИ (5-10 пунктів, якщо тема складна)\n"
    "ДОКУМЕНТИ (якщо доречно)\n"
    "КУДИ ЗВЕРНУТИСЯ (якщо доречно)\n"
    "УТОЧНЕННЯ (до 3 питань, лише якщо необхідно)\n"
)

# ----------------------------
# 7) KEYBOARDS
# ----------------------------
KB_ONLY_START = ReplyKeyboardMarkup([[KeyboardButton(BTN_START)]], resize_keyboard=True)

KB_MAIN_TWO = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_ASK), KeyboardButton(BTN_ABOUT)]],
    resize_keyboard=True
)

KB_MAIN_STOP = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_ASK), KeyboardButton(BTN_EXIT)]],
    resize_keyboard=True
)

KB_CHAT_ALWAYS = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_STOP), KeyboardButton(BTN_EXIT)]],
    resize_keyboard=True
)

KB_AFTER_BLOCK = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_CONTINUE)],
        [KeyboardButton(BTN_NEW_Q)],
        [KeyboardButton(BTN_EXIT), KeyboardButton(BTN_STOP)],
    ],
    resize_keyboard=True
)

# ----------------------------
# 8) STATE
# ----------------------------
MODE = "mode"       # "menu" | "asking" | "after_block"
COUNT = "count"
NAME = "name"
HISTORY = "history"
HAS_ASKED_ONCE = "has_asked_once"

def set_menu_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data[MODE] = "menu"
    context.user_data[COUNT] = 0

def set_asking_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data[MODE] = "asking"
    if COUNT not in context.user_data:
        context.user_data[COUNT] = 0
    if HISTORY not in context.user_data:
        context.user_data[HISTORY] = []

def set_after_block_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data[MODE] = "after_block"

def reset_dialog_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data[HISTORY] = []
    context.user_data[COUNT] = 0

def ensure_history(context: ContextTypes.DEFAULT_TYPE) -> None:
    if HISTORY not in context.user_data or not isinstance(context.user_data[HISTORY], list):
        context.user_data[HISTORY] = []

def push_history(context: ContextTypes.DEFAULT_TYPE, role: str, content: str) -> None:
    ensure_history(context)
    context.user_data[HISTORY].append({"role": role, "content": content})
    if len(context.user_data[HISTORY]) > MAX_HISTORY_TURNS:
        context.user_data[HISTORY] = context.user_data[HISTORY][-MAX_HISTORY_TURNS:]

def normalize_cmd(text: str) -> str:
    return (text or "").strip().lower()

# ----------------------------
# 9) NAME OPTIONAL
# ----------------------------
NAME_LINE_RE = re.compile(r"^[A-Za-zА-Яа-яІіЇїЄєҐґ' -]{2,25}$")
NAME_INLINE_RE = re.compile(r"^\s*([A-Za-zА-Яа-яІіЇїЄєҐґ' -]{2,25})[.,!?:\-—]\s+(.+)$")
ME_IS_RE = re.compile(r"\b(мене звати|я)\s+([A-Za-zА-Яа-яІіЇїЄєҐґ' -]{2,25})\b", re.IGNORECASE)

def extract_name_and_clean_question(text: str) -> tuple[str | None, str]:
    t = (text or "").strip()

    if "\n" in t:
        first, rest = t.split("\n", 1)
        first = first.strip()
        rest = rest.strip()
        if rest and NAME_LINE_RE.match(first):
            return first, rest

    m = NAME_INLINE_RE.match(t)
    if m:
        name = m.group(1).strip()
        q = m.group(2).strip()
        if q:
            return name, q

    m2 = ME_IS_RE.search(t)
    if m2:
        name = m2.group(2).strip()
        if NAME_LINE_RE.match(name):
            return name, t

    return None, t

# ----------------------------
# 10) SANITIZE
# ----------------------------
def sanitize_answer(s: str) -> str:
    if not s:
        return s
    s = s.replace("**", "").replace("*", "").replace("#", "")
    s = re.sub(r"^\s*(Ок|ОК|Добре)\s*[,\-:]*\s*(уточнюю|уточню)\.?\s*\n*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s

# ----------------------------
# 11) HANDLERS
# ----------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    set_menu_state(context)
    await update.message.reply_text(WELCOME_TEXT, reply_markup=KB_ONLY_START)

async def handle_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(NON_TEXT_TEXT, reply_markup=KB_CHAT_ALWAYS)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    tnorm = normalize_cmd(text)
    mode = context.user_data.get(MODE, "menu")

    if tnorm in STOP_WORDS or text == BTN_STOP:
        set_menu_state(context)
        await update.message.reply_text(STOP_TEXT, reply_markup=KB_MAIN_STOP)
        return

    if tnorm in EXIT_WORDS or text == BTN_EXIT:
        set_menu_state(context)
        await update.message.reply_text(EXIT_TEXT, reply_markup=KB_ONLY_START)
        return

    if text == BTN_START:
        set_menu_state(context)
        await update.message.reply_text(WELCOME_AFTER_START, reply_markup=KB_MAIN_TWO)
        return

    if text == BTN_ABOUT:
        set_menu_state(context)
        await update.message.reply_text(ABOUT_TEXT, reply_markup=KB_MAIN_TWO)
        return

    if text == BTN_ASK:
        set_asking_state(context)
        context.user_data[COUNT] = 0
        if not context.user_data.get(HAS_ASKED_ONCE, False):
            context.user_data[HAS_ASKED_ONCE] = True
            await update.message.reply_text(ASK_HINT_FIRST, reply_markup=KB_CHAT_ALWAYS)
        else:
            await update.message.reply_text(ASK_HINT_NEXT, reply_markup=KB_CHAT_ALWAYS)
        return

    if text == BTN_CONTINUE:
        set_asking_state(context)
        context.user_data[COUNT] = 0
        await update.message.reply_text("Добре. Напишіть наступне повідомлення (текстом).", reply_markup=KB_CHAT_ALWAYS)
        return

    if text == BTN_NEW_Q:
        saved_name = context.user_data.get(NAME)
        reset_dialog_context(context)
        if saved_name:
            context.user_data[NAME] = saved_name
        set_asking_state(context)
        await update.message.reply_text(ASK_HINT_NEXT, reply_markup=KB_CHAT_ALWAYS)
        return

    if mode != "asking":
        await update.message.reply_text(f"Натисніть \"{BTN_ASK}\" або \"{BTN_ABOUT}\".", reply_markup=KB_MAIN_TWO)
        return

    found_name, clean_question = extract_name_and_clean_question(text)
    if found_name and not context.user_data.get(NAME):
        context.user_data[NAME] = found_name

    ensure_history(context)
    push_history(context, "user", clean_question)

    user_name = context.user_data.get(NAME)
    sys_prompt = SYSTEM_PROMPT_BASE + (f"\nІм'я користувача: {user_name}\n" if user_name else "")
    sys_prompt += "\n\nДОВІДКА (офіційні контакти, якщо треба):\n" + CONTACTS_TEXT

    input_msgs = [{"role": "system", "content": sys_prompt}] + context.user_data[HISTORY]

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

        resp = client.responses.create(
            model="gpt-4o-mini",
            input=input_msgs,
            temperature=0.35,
        )

        answer = (resp.output_text or "").strip() or "Не вийшло сформувати відповідь. Спробуйте переформулювати питання."
        answer = sanitize_answer(answer)

        push_history(context, "assistant", answer)
        await update.message.reply_text(answer, reply_markup=KB_CHAT_ALWAYS)

        context.user_data[COUNT] = int(context.user_data.get(COUNT, 0)) + 1
        if context.user_data[COUNT] >= MAX_Q_PER_BLOCK:
            set_after_block_state(context)
            await update.message.reply_text(AFTER_BLOCK_TEXT, reply_markup=KB_AFTER_BLOCK)
        else:
            set_asking_state(context)

    except Exception as e:
        await update.message.reply_text(
            f"Помилка AI-запиту: {type(e).__name__}. Спробуйте пізніше.",
            reply_markup=KB_CHAT_ALWAYS
        )

def main() -> None:
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, handle_non_text))

    # WEBHOOK ONLY (NO POLLING)
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
