import asyncio
import html
import logging
import os
import re
from io import BytesIO

import aiosqlite
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

# =========================================================
# SOZLAMALAR
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN Railway Variables ichida yo'q!")

PUBLIC_CHANNEL_ID = int(os.getenv("PUBLIC_CHANNEL_ID", "-1004349473919"))
PUBLIC_CHANNEL_USERNAME = os.getenv("PUBLIC_CHANNEL_USERNAME", "@AniJon_Anime")
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", "-1004368926932"))
OWNER_ID = int(os.getenv("OWNER_ID", "8975803698"))
DB_PATH = os.getenv("DB_PATH", "anijon.db")
SITE_URL = os.getenv("SITE_URL", "https://aniworlddub.netlify.app")
BOT_USERNAME = os.getenv("BOT_USERNAME", "AniWorldDub_Bot").lstrip("@")
PORT = int(os.getenv("PORT", "8080"))
# Ixtiyoriy: poster havolalari uchun ochiq manzilni qo'lda berish
API_PUBLIC_URL = os.getenv("API_PUBLIC_URL", "").rstrip("/")

DESCRIPTION_MAX = 700          # rasm ostidagi matn 1024 belgidan oshmasligi uchun
UPLOAD_DEBOUNCE_SECONDS = 3    # videolar ketma-ket kelishini kutish vaqti
POSTER_CACHE_LIMIT = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())

# Admin qismlarni ketma-ket tashlaganda tartib buzilmasligi uchun
upload_buffers: dict[int, dict] = {}
upload_tasks: dict[int, asyncio.Task] = {}
upload_locks: dict[int, asyncio.Lock] = {}

# Poster keshi (Telegramdan qayta-qayta yuklamaslik uchun)
poster_cache: dict[str, bytes] = {}


# =========================================================
# FSM HOLATLARI
# =========================================================

class AnimeAdd(StatesGroup):
    code = State()
    title = State()
    photo = State()
    description = State()


class EpisodeAdd(StatesGroup):
    code = State()
    videos = State()


class AdminAdd(StatesGroup):
    user_id = State()


class AdminRemove(StatesGroup):
    user_id = State()


# =========================================================
# DATABASE
# =========================================================

async def init_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS anime (
                code TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                photo_id TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_code TEXT NOT NULL,
                episode_no INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                file_type TEXT DEFAULT 'video',
                caption TEXT,
                UNIQUE(anime_code, episode_no)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS admin_users (
                user_id INTEGER PRIMARY KEY
            )
        """)

        cursor = await db.execute("PRAGMA table_info(anime)")
        anime_columns = [row[1] for row in await cursor.fetchall()]
        if "photo_id" not in anime_columns:
            await db.execute("ALTER TABLE anime ADD COLUMN photo_id TEXT")

        cursor = await db.execute("PRAGMA table_info(episodes)")
        episode_columns = [row[1] for row in await cursor.fetchall()]
        if "file_type" not in episode_columns:
            await db.execute(
                "ALTER TABLE episodes ADD COLUMN file_type TEXT DEFAULT 'video'"
            )
        if "caption" not in episode_columns:
            await db.execute("ALTER TABLE episodes ADD COLUMN caption TEXT")

        await db.execute(
            "INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)",
            (OWNER_ID,),
        )
        await db.commit()


async def is_admin(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM admin_users WHERE user_id = ?",
            (user_id,),
        )
        return await cursor.fetchone() is not None


async def load_env_admins():
    raw_admins = os.getenv("ADMIN_IDS", "")
    async with aiosqlite.connect(DB_PATH) as db:
        for item in raw_admins.split(","):
            item = item.strip()
            if item.isdigit() and int(item) > 0:
                await db.execute(
                    "INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)",
                    (int(item),),
                )

        await db.execute(
            "INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)",
            (OWNER_ID,),
        )
        await db.commit()


async def get_anime(code: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT code, title, description, photo_id FROM anime WHERE code = ?",
            (code.upper(),),
        )
        return await cursor.fetchone()


async def get_episode_numbers(code: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT episode_no FROM episodes
            WHERE anime_code = ?
            ORDER BY episode_no
            """,
            (code.upper(),),
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


async def get_episode(code: str, number: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT file_id, file_type, caption
            FROM episodes
            WHERE anime_code = ? AND episode_no = ?
            """,
            (code.upper(), number),
        )
        return await cursor.fetchone()


async def next_episode_number(code: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT COALESCE(MAX(episode_no), 0) + 1 FROM episodes WHERE anime_code = ?",
            (code.upper(),),
        )
        row = await cursor.fetchone()
        return int(row[0])


# =========================================================
# YORDAMCHI FUNKSIYALAR
# =========================================================

async def tg_retry(func, *args, **kwargs):
    """Telegram 'juda tez' deb so'rasa, kutib qayta urinadi."""
    for _ in range(3):
        try:
            return await func(*args, **kwargs)
        except TelegramRetryAfter as error:
            await asyncio.sleep(error.retry_after + 1)
    return await func(*args, **kwargs)


def parse_episode_payload(payload: str):
    """'ep_AN001_5' -> ('AN001', 5)"""
    code, number_text = payload[3:].rsplit("_", 1)
    return code.upper(), int(number_text)


def cut(text: str, limit: int) -> str:
    text = text or ""
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


# =========================================================
# TUGMALAR
# =========================================================

def main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Anime qidirish")],
            [KeyboardButton(text="🌐 Saytga kirish")],
        ],
        resize_keyboard=True,
    )


def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Anime qo'shish", callback_data="add_anime")],
        [InlineKeyboardButton(text="🎬 Qism qo'shish", callback_data="add_episode")],
        [InlineKeyboardButton(text="📚 Anime ro'yxati", callback_data="anime_list")],
        [InlineKeyboardButton(text="👥 Admin qo'shish", callback_data="add_admin")],
        [InlineKeyboardButton(text="➖ Adminni o'chirish", callback_data="remove_admin")],
        [InlineKeyboardButton(text="📊 Statistika", callback_data="statistics")],
    ])


def subscription_keyboard(payload: str = ""):
    channel = PUBLIC_CHANNEL_USERNAME.lstrip("@")
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📢 Kanalga obuna bo'lish",
            url=f"https://t.me/{channel}",
        )],
        [InlineKeyboardButton(
            text="✅ Obunani tekshirish",
            callback_data=f"subcheck|{payload}",
        )],
    ])


def episodes_keyboard(code: str, numbers: list[int]):
    rows = []
    row = []
    for number in numbers:
        row.append(InlineKeyboardButton(
            text=str(number),
            callback_data=f"get:{code}:{number}",
        ))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="🌐 Sayt", url=SITE_URL)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================================================
# OBUNA, ANIME KARTASI VA QISM YUBORISH
# =========================================================

async def check_subscription(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(PUBLIC_CHANNEL_ID, user_id)
        if member.status in ("member", "administrator", "creator"):
            return True

        return member.status == "restricted" and bool(
            getattr(member, "is_member", False)
        )
    except Exception:
        logger.exception("Obunani tekshirishda xato")
        return False


async def send_anime_card(chat_id: int, code: str):
    anime = await get_anime(code)
    if not anime:
        await bot.send_message(chat_id, "❌ Anime topilmadi.")
        return

    numbers = await get_episode_numbers(code)
    title = html.escape(anime["title"])

    if not numbers:
        await bot.send_message(
            chat_id,
            f"<b>{title}</b>\n\nHozircha bu anime uchun qismlar qo'shilmagan.",
        )
        return

    description = html.escape(cut(anime["description"] or "", DESCRIPTION_MAX))
    text = (
        f"🎌 <b>{title}</b>\n\n"
        f"{description}\n\n"
        f"🎬 Qismlar: {len(numbers)} ta\n"
        "Kerakli qismni tanlang:"
    )
    keyboard = episodes_keyboard(code.upper(), numbers)

    if anime["photo_id"]:
        try:
            await bot.send_photo(
                chat_id,
                anime["photo_id"],
                caption=text,
                reply_markup=keyboard,
            )
            return
        except TelegramBadRequest:
            logger.exception("Anime rasmini yuborishda xato")

    await bot.send_message(chat_id, text, reply_markup=keyboard)


async def send_episode(chat_id: int, code: str, number: int):
    anime = await get_anime(code)
    episode = await get_episode(code, number)

    if not anime or not episode:
        await bot.send_message(chat_id, "❌ Anime yoki qism topilmadi.")
        return

    file_id, file_type, _saved_caption = episode
    title = html.escape(anime["title"])
    caption = (
        f"<b>{title}</b>\n"
        f"🎬 {number}-qism\n"
        f"🔖 Kod: {html.escape(code.upper())}"
    )

    numbers = await get_episode_numbers(code)
    prev_no = max((n for n in numbers if n < number), default=None)
    next_no = min((n for n in numbers if n > number), default=None)

    nav_row = []
    if prev_no is not None:
        nav_row.append(InlineKeyboardButton(
            text=f"⬅️ {prev_no}-qism",
            callback_data=f"get:{code.upper()}:{prev_no}",
        ))
    if next_no is not None:
        nav_row.append(InlineKeyboardButton(
            text=f"{next_no}-qism ➡️",
            callback_data=f"get:{code.upper()}:{next_no}",
        ))

    rows = []
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton(
        text="📚 Barcha qismlar",
        callback_data=f"list:{code.upper()}",
    )])
    keyboard = InlineKeyboardMarkup(inline_keyboard=rows)

    if file_type == "document":
        await tg_retry(
            bot.send_document, chat_id, file_id,
            caption=caption, reply_markup=keyboard,
        )
    else:
        await tg_retry(
            bot.send_video, chat_id, file_id,
            caption=caption, reply_markup=keyboard,
        )


# =========================================================
# SAYT UCHUN API
# =========================================================

def public_base_url(request: web.Request) -> str:
    if API_PUBLIC_URL:
        return API_PUBLIC_URL

    scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
    host = request.headers.get("X-Forwarded-Host", request.host)
    return f"{scheme}://{host}"


@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        try:
            response = await handler(request)
        except web.HTTPException as error:
            response = error

    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response


async def api_root(request: web.Request):
    return web.json_response({"status": "ok", "service": "AniWorldDub API"})


async def api_anime_list(request: web.Request):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT a.code, a.title, a.description, a.photo_id,
                   COUNT(e.id) AS episode_count
            FROM anime a
            LEFT JOIN episodes e ON e.anime_code = a.code
            GROUP BY a.code
            ORDER BY a.rowid DESC
            """
        )
        rows = await cursor.fetchall()

    base = public_base_url(request)
    result = []

    for row in rows:
        result.append({
            "code": row["code"],
            "title": row["title"],
            "description": row["description"] or "",
            "episode_count": row["episode_count"],
            "poster_url": (
                f"{base}/poster/{row['code']}" if row["photo_id"] else None
            ),
        })

    return web.json_response({"anime": result})


async def api_anime_detail(request: web.Request):
    code = request.match_info["code"].upper()
    anime = await get_anime(code)

    if not anime:
        return web.json_response({"error": "Anime topilmadi"}, status=404)

    numbers = await get_episode_numbers(code)
    base = public_base_url(request)

    return web.json_response({
        "code": code,
        "title": anime["title"],
        "description": anime["description"] or "",
        "poster_url": (
            f"{base}/poster/{code}" if anime["photo_id"] else None
        ),
        "telegram_url": f"https://t.me/{BOT_USERNAME}?start=a_{code}",
        "episodes": [
            {
                "number": number,
                "telegram_url": (
                    f"https://t.me/{BOT_USERNAME}?start=ep_{code}_{number}"
                ),
            }
            for number in numbers
        ],
    })


async def api_poster(request: web.Request):
    code = request.match_info["code"].upper()
    anime = await get_anime(code)

    if not anime or not anime["photo_id"]:
        raise web.HTTPNotFound()

    photo_id = anime["photo_id"]
    data = poster_cache.get(photo_id)

    if data is None:
        try:
            file = await bot.get_file(photo_id)
            image_buffer = BytesIO()
            await bot.download_file(file.file_path, destination=image_buffer)
            data = image_buffer.getvalue()
        except Exception:
            logger.exception("Poster yuklashda xato")
            raise web.HTTPNotFound()

        if len(poster_cache) >= POSTER_CACHE_LIMIT:
            poster_cache.pop(next(iter(poster_cache)))
        poster_cache[photo_id] = data

    return web.Response(
        body=data,
        content_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=86400"},
    )


async def start_api():
    app = web.Application(middlewares=[cors_middleware])

    app.router.add_get("/", api_root)
    app.router.add_get("/api/anime", api_anime_list)
    app.router.add_get("/api/anime/{code}", api_anime_detail)
    app.router.add_get("/poster/{code}", api_poster)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("API server ishga tushdi: port %s", PORT)

    return runner


# =========================================================
# START VA FOYDALANUVCHI
# =========================================================

@dp.message(CommandStart())
async def start_handler(message: Message):
    user_id = message.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id) VALUES (?)",
            (user_id,),
        )
        await db.commit()

    parts = (message.text or "").split(maxsplit=1)
    argument = parts[1].strip() if len(parts) > 1 else ""

    # Saytdan qism bosilganda: ep_KOD_RAQAM
    if argument.startswith("ep_"):
        try:
            code, number = parse_episode_payload(argument)
        except (ValueError, TypeError):
            await message.answer("❌ Qism havolasi noto'g'ri.")
            return

        if not await check_subscription(user_id):
            await message.answer(
                "Avval kanalimizga obuna bo'ling:",
                reply_markup=subscription_keyboard(argument),
            )
            return

        await send_episode(message.chat.id, code, number)
        return

    # Saytdan anime bosilganda: a_KOD
    if argument.startswith("a_"):
        code = argument[2:].upper()

        if not await check_subscription(user_id):
            await message.answer(
                "Avval kanalimizga obuna bo'ling:",
                reply_markup=subscription_keyboard(argument),
            )
            return

        await send_anime_card(message.chat.id, code)
        return

    await message.answer(
        "🎌 <b>AniWorldDub botiga xush kelibsiz!</b>\n\n"
        "Anime kodini yuboring yoki saytdan foydalaning.",
        reply_markup=main_keyboard(),
    )


@dp.message(Command("myid"))
async def myid_command(message: Message):
    await message.answer(
        f"🆔 Sizning Telegram ID raqamingiz:\n"
        f"<code>{message.from_user.id}</code>"
    )


@dp.message(Command("cancel"))
async def cancel_command(message: Message, state: FSMContext):
    current = await state.get_state()

    if current is None:
        await message.answer("Hozir bajarilayotgan jarayon yo'q.")
        return

    user_id = message.from_user.id
    task = upload_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()
    upload_buffers.pop(user_id, None)

    await state.clear()
    await message.answer(
        "❌ Jarayon bekor qilindi.",
        reply_markup=main_keyboard(),
    )


@dp.callback_query(F.data.startswith("subcheck|"))
async def subscription_check_callback(callback: CallbackQuery):
    payload = callback.data.split("|", 1)[1]

    if not await check_subscription(callback.from_user.id):
        await callback.answer(
            "Hali kanalga obuna bo'lmagansiz!",
            show_alert=True,
        )
        return

    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

    chat_id = callback.message.chat.id

    if payload.startswith("ep_"):
        try:
            code, number = parse_episode_payload(payload)
            await send_episode(chat_id, code, number)
        except (ValueError, TypeError):
            await bot.send_message(chat_id, "Qism havolasida xatolik.")

    elif payload.startswith("a_"):
        await send_anime_card(chat_id, payload[2:].upper())

    else:
        await bot.send_message(
            chat_id,
            "✅ Obuna tasdiqlandi!",
            reply_markup=main_keyboard(),
        )

    await callback.answer()


# =========================================================
# ANIME QIDIRISH
# =========================================================

@dp.message(F.text == "🔎 Anime qidirish")
async def search_button(message: Message):
    await message.answer("🔎 Anime kodini yuboring:")


@dp.message(F.text == "🌐 Saytga kirish")
async def site_button(message: Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🌐 AniWorldDub saytini ochish",
            url=SITE_URL,
        )]
    ])

    await message.answer(
        "Saytimizga o'tish uchun tugmani bosing:",
        reply_markup=keyboard,
    )


@dp.message(Command("search"))
async def search_command(message: Message):
    await message.answer("Anime kodini yuboring:")


@dp.message(F.text, StateFilter(None))
async def anime_code_handler(message: Message):
    code = message.text.strip().upper()

    if code.startswith("/"):
        return

    anime = await get_anime(code)

    if not anime:
        await message.answer(
            "❌ Bunday kodli anime topilmadi.\n"
            "Kodni tekshiring yoki saytdan tanlang.",
        )
        return

    if not await check_subscription(message.from_user.id):
        await message.answer(
            "Avval kanalimizga obuna bo'ling:",
            reply_markup=subscription_keyboard(f"a_{code}"),
        )
        return

    await send_anime_card(message.chat.id, code)


@dp.callback_query(F.data.startswith("get:"))
async def get_episode_callback(callback: CallbackQuery):
    try:
        _, code, number_text = callback.data.split(":")
        number = int(number_text)
    except (ValueError, TypeError):
        await callback.answer("Noto'g'ri qism.", show_alert=True)
        return

    if not await check_subscription(callback.from_user.id):
        await callback.message.answer(
            "Avval kanalga obuna bo'ling:",
            reply_markup=subscription_keyboard(f"ep_{code}_{number}"),
        )
        await callback.answer()
        return

    await callback.answer("Qism yuborilmoqda...")
    await send_episode(callback.message.chat.id, code, number)


@dp.callback_query(F.data.startswith("list:"))
async def list_episodes_callback(callback: CallbackQuery):
    code = callback.data.split(":", 1)[1]

    if not await check_subscription(callback.from_user.id):
        await callback.message.answer(
            "Avval kanalga obuna bo'ling:",
            reply_markup=subscription_keyboard(f"a_{code}"),
        )
        await callback.answer()
        return

    await callback.answer()
    await send_anime_card(callback.message.chat.id, code)


# =========================================================
# ADMIN PANEL
# =========================================================

@dp.message(Command("admin"))
async def admin_command(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer("⛔ Siz admin emassiz.")
        return

    await message.answer(
        "🛠 <b>ADMIN PANEL</b>\n"
        "Kerakli bo'limni tanlang:",
        reply_markup=admin_keyboard(),
    )


# =========================================================
# ADMIN: ANIME QO'SHISH (kod -> nom -> rasm -> tavsif -> qismlar)
# =========================================================

@dp.callback_query(F.data == "add_anime")
async def add_anime_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q!", show_alert=True)
        return

    await state.clear()
    await state.set_state(AnimeAdd.code)

    await callback.message.answer(
        "Anime uchun kod kiriting. Masalan: <code>AN001</code>\n"
        "Bekor qilish: /cancel"
    )
    await callback.answer()


@dp.message(AnimeAdd.code, F.text)
async def add_anime_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()

    if not re.fullmatch(r"[A-Z0-9_-]{2,30}", code):
        await message.answer(
            "❌ Kod noto'g'ri. Faqat lotin harflari, raqam, _ va - "
            "ishlating (2-30 belgi)."
        )
        return

    if await get_anime(code):
        await message.answer(
            f"❌ <code>{code}</code> kodi band. Boshqa kod kiriting:"
        )
        return

    await state.update_data(code=code)
    await state.set_state(AnimeAdd.title)
    await message.answer("Anime nomini kiriting:")


@dp.message(AnimeAdd.title, F.text)
async def add_anime_title(message: Message, state: FSMContext):
    title = message.text.strip()

    if not title or len(title) > 150:
        await message.answer("❌ Nom bo'sh yoki juda uzun (150 belgigacha).")
        return

    await state.update_data(title=title)
    await state.set_state(AnimeAdd.photo)
    await message.answer("Anime rasmini (poster) yuboring:")


@dp.message(AnimeAdd.photo, F.photo)
async def add_anime_photo(message: Message, state: FSMContext):
    await state.update_data(photo_id=message.photo[-1].file_id)
    await state.set_state(AnimeAdd.description)
    await message.answer(
        "Anime haqida qisqacha ma'lumot yozing "
        f"({DESCRIPTION_MAX} belgigacha).\n"
        "Tavsifsiz qoldirish uchun <code>-</code> yuboring."
    )


@dp.message(AnimeAdd.photo)
async def add_anime_photo_wrong(message: Message):
    await message.answer("❌ Rasm yuboring (fayl emas, oddiy rasm).")


@dp.message(AnimeAdd.description, F.text)
async def add_anime_description(message: Message, state: FSMContext):
    description = message.text.strip()

    if description == "-":
        description = ""

    if len(description) > DESCRIPTION_MAX:
        await message.answer(
            f"❌ Tavsif juda uzun ({len(description)} belgi). "
            f"{DESCRIPTION_MAX} belgigacha qisqartiring."
        )
        return

    data = await state.get_data()
    code = data["code"]

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO anime (code, title, description, photo_id)
                VALUES (?, ?, ?, ?)
                """,
                (code, data["title"], description, data.get("photo_id")),
            )
            await db.commit()
    except aiosqlite.IntegrityError:
        await message.answer(f"❌ <code>{code}</code> kodi allaqachon bor.")
        await state.clear()
        return

    await state.clear()
    await state.set_state(EpisodeAdd.videos)
    await state.update_data(code=code)

    await message.answer(
        f"✅ <b>{html.escape(data['title'])}</b> saqlandi!\n"
        f"🔖 Kod: <code>{code}</code>\n\n"
        "🎬 Endi qismlarni tartib bilan yuboring (1-qismdan boshlab).\n"
        "Hammasini birdaniga tashlasangiz ham bo'ladi, men o'zim "
        "1, 2, 3... deb taxlab chiqaman.\n\n"
        "Tugagach /done bosing."
    )


@dp.message(AnimeAdd.description)
async def add_anime_description_wrong(message: Message):
    await message.answer("❌ Matn yuboring.")


# =========================================================
# ADMIN: QISM QO'SHISH
# =========================================================

@dp.callback_query(F.data == "add_episode")
async def add_episode_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q!", show_alert=True)
        return

    await state.clear()
    await state.set_state(EpisodeAdd.code)

    await callback.message.answer(
        "Qaysi animega qism qo'shamiz? Anime kodini yuboring:\n"
        "Bekor qilish: /cancel"
    )
    await callback.answer()


@dp.message(EpisodeAdd.code, F.text)
async def add_episode_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    anime = await get_anime(code)

    if not anime:
        await message.answer("❌ Bunday kodli anime topilmadi. Qayta yuboring:")
        return

    numbers = await get_episode_numbers(code)
    next_no = (numbers[-1] + 1) if numbers else 1

    await state.set_state(EpisodeAdd.videos)
    await state.update_data(code=code)

    await message.answer(
        f"🎌 <b>{html.escape(anime['title'])}</b>\n"
        f"Hozir {len(numbers)} ta qism bor. Yangilari "
        f"<b>{next_no}-qismdan</b> boshlanadi.\n\n"
        "Videolarni tartib bilan yuboring. Tugagach /done bosing."
    )


async def flush_uploads(user_id: int):
    """Yig'ilgan videolarni message_id tartibida Omborga yuborib, bazaga yozadi."""
    buffer = upload_buffers.pop(user_id, None)
    if not buffer:
        return

    lock = upload_locks.setdefault(user_id, asyncio.Lock())

    async with lock:
        code = buffer["code"]
        chat_id = buffer["chat_id"]
        messages = sorted(buffer["messages"], key=lambda m: m.message_id)

        saved_numbers = []
        failed = 0

        for msg in messages:
            if msg.video:
                file_id, file_type = msg.video.file_id, "video"
            else:
                file_id, file_type = msg.document.file_id, "document"

            number = await next_episode_number(code)
            storage_caption = f"{code} • {number}-qism"

            try:
                if file_type == "video":
                    sent = await tg_retry(
                        bot.send_video, STORAGE_CHANNEL_ID, file_id,
                        caption=storage_caption,
                    )
                    saved_file_id = sent.video.file_id
                else:
                    sent = await tg_retry(
                        bot.send_document, STORAGE_CHANNEL_ID, file_id,
                        caption=storage_caption,
                    )
                    saved_file_id = sent.document.file_id
            except Exception:
                logger.exception("Omborga yuborishda xato")
                failed += 1
                continue

            try:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        """
                        INSERT INTO episodes
                            (anime_code, episode_no, file_id, file_type, caption)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (code, number, saved_file_id, file_type, msg.caption or ""),
                    )
                    await db.commit()
            except Exception:
                logger.exception("Qismni bazaga yozishda xato")
                failed += 1
                continue

            saved_numbers.append(number)
            await asyncio.sleep(0.4)

        if saved_numbers:
            first, last = saved_numbers[0], saved_numbers[-1]
            span = f"{first}-qism" if first == last else f"{first}–{last}-qism"
            text = f"✅ {len(saved_numbers)} ta qism saqlandi ({span})."
        else:
            text = "❌ Hech qaysi qism saqlanmadi."

        if failed:
            text += (
                f"\n⚠️ {failed} ta video saqlanmadi. Bot Ombor kanalida "
                "admin va xabar yuborish huquqiga egaligini tekshiring, "
                "so'ng o'sha videolarni qayta yuboring."
            )

        text += "\n\nKeyingisini yuboring yoki /done bosing."
        await bot.send_message(chat_id, text)


async def delayed_flush(user_id: int):
    await asyncio.sleep(UPLOAD_DEBOUNCE_SECONDS)
    # Shu joydan keyin yangi videolar alohida partiya bo'ladi
    upload_tasks.pop(user_id, None)
    await flush_uploads(user_id)


@dp.message(EpisodeAdd.videos, Command("done"))
async def add_episode_done(message: Message, state: FSMContext):
    user_id = message.from_user.id

    task = upload_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()

    await flush_uploads(user_id)

    # Davom etayotgan yuklash bo'lsa, tugashini kutamiz
    async with upload_locks.setdefault(user_id, asyncio.Lock()):
        pass

    data = await state.get_data()
    code = data["code"]
    await state.clear()

    numbers = await get_episode_numbers(code)

    await message.answer(
        "🎉 Qismlar qo'shish yakunlandi!\n\n"
        f"🔖 Anime kodi: <code>{code}</code>\n"
        f"🎬 Jami qismlar: {len(numbers)} ta",
        reply_markup=admin_keyboard(),
    )


@dp.message(
    EpisodeAdd.videos,
    F.video | (F.document & F.document.mime_type.startswith("video/")),
)
async def add_episode_file(message: Message, state: FSMContext):
    data = await state.get_data()
    user_id = message.from_user.id

    buffer = upload_buffers.setdefault(
        user_id,
        {"code": data["code"], "chat_id": message.chat.id, "messages": []},
    )
    buffer["messages"].append(message)

    old_task = upload_tasks.get(user_id)
    if old_task and not old_task.done():
        old_task.cancel()

    upload_tasks[user_id] = asyncio.create_task(delayed_flush(user_id))


@dp.message(EpisodeAdd.videos)
async def add_episode_wrong_file(message: Message):
    await message.answer(
        "Video yoki video fayl yuboring. Tugatish uchun /done bosing."
    )


# =========================================================
# ADMIN: ADMIN QO'SHISH
# =========================================================

@dp.callback_query(F.data == "add_admin")
async def add_admin_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer(
            "Faqat bot egasi admin qo'sha oladi.",
            show_alert=True,
        )
        return

    await state.clear()
    await state.set_state(AdminAdd.user_id)

    await callback.message.answer(
        "Yangi adminning Telegram ID raqamini yuboring.\n"
        "ID raqamini bilish uchun u /myid yuborsin."
    )
    await callback.answer()


@dp.message(AdminAdd.user_id, F.text)
async def add_admin_finish(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await state.clear()
        return

    try:
        user_id = int(message.text.strip())
        if user_id <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            "❌ ID faqat musbat raqamlardan iborat bo'lishi kerak."
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)",
            (user_id,),
        )
        await db.commit()

    await state.clear()
    await message.answer(
        f"✅ <code>{user_id}</code> adminlar ro'yxatiga qo'shildi.",
        reply_markup=admin_keyboard(),
    )


# =========================================================
# ADMIN: ADMINNI O'CHIRISH
# =========================================================

@dp.callback_query(F.data == "remove_admin")
async def remove_admin_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer(
            "Faqat bot egasi adminni o'chira oladi.",
            show_alert=True,
        )
        return

    await state.clear()
    await state.set_state(AdminRemove.user_id)

    await callback.message.answer(
        "O'chiriladigan adminning Telegram ID raqamini yuboring:"
    )
    await callback.answer()


@dp.message(AdminRemove.user_id, F.text)
async def remove_admin_finish(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await state.clear()
        return

    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID raqamini to'g'ri kiriting.")
        return

    if user_id == OWNER_ID:
        await message.answer("Bot egasini o'chirib bo'lmaydi.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM admin_users WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()

    await state.clear()
    await message.answer(
        f"✅ <code>{user_id}</code> adminlar ro'yxatidan o'chirildi.",
        reply_markup=admin_keyboard(),
    )


# =========================================================
# ANIME RO'YXATI
# =========================================================

@dp.callback_query(F.data == "anime_list")
async def anime_list_callback(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q!", show_alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT a.code, a.title, COUNT(e.id)
            FROM anime a
            LEFT JOIN episodes e ON e.anime_code = a.code
            GROUP BY a.code
            ORDER BY a.title
            """
        )
        rows = await cursor.fetchall()

    if not rows:
        await callback.message.answer("Hozircha anime yo'q.")
    else:
        lines = ["📚 <b>Anime ro'yxati:</b>\n"]
        for code, title, count in rows:
            lines.append(
                f"• {html.escape(title)} — <code>{code}</code> ({count} qism)"
            )

        text = "\n".join(lines)
        for start in range(0, len(text), 3800):
            await callback.message.answer(text[start:start + 3800])

    await callback.answer()


# =========================================================
# STATISTIKA
# =========================================================

@dp.callback_query(F.data == "statistics")
async def statistics_callback(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q!", show_alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        anime_count = await (await db.execute("SELECT COUNT(*) FROM anime")).fetchone()
        episode_count = await (await db.execute("SELECT COUNT(*) FROM episodes")).fetchone()
        user_count = await (await db.execute("SELECT COUNT(*) FROM users")).fetchone()
        admin_count = await (await db.execute("SELECT COUNT(*) FROM admin_users")).fetchone()

    await callback.message.answer(
        "📊 <b>Bot statistikasi</b>\n\n"
        f"🎌 Anime: {anime_count[0]}\n"
        f"🎬 Qismlar: {episode_count[0]}\n"
        f"👥 Foydalanuvchilar: {user_count[0]}\n"
        f"🛡 Adminlar: {admin_count[0]}"
    )
    await callback.answer()


# =========================================================
# BOTNI ISHGA TUSHIRISH
# =========================================================

async def main():
    await init_db()
    await load_env_admins()

    api_runner = await start_api()

    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("AniWorldDub bot ishga tushdi!")

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
        )
    finally:
        await api_runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
