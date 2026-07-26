import asyncio
import logging
import os
import sqlite3
from typing import Any, Awaitable, Callable, Dict, List, Optional
from datetime import datetime

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ErrorEvent,
    LinkPreviewOptions
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("AnonBot")

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID_RAW = os.getenv("CHANNEL_ID", "0")
ADMINS_RAW = os.getenv("ADMINS", "")

if not BOT_TOKEN:
    raise ValueError("ОШИБКА: Токен бота BOT_TOKEN не найден в .env файле!")

try:
    CHANNEL_ID = int(CHANNEL_ID_RAW)
except ValueError:
    raise ValueError("ОШИБКА: CHANNEL_ID в .env должен быть числом (например, -1001234567890)")

# Parse list of admin user IDs
ADMIN_IDS: List[int] = []
if ADMINS_RAW:
    for item in ADMINS_RAW.split(","):
        item = item.strip()
        if item.isdigit() or (item.startswith("-") and item[1:].isdigit()):
            ADMIN_IDS.append(int(item))

class Database:
    """Синхронная база данных SQLite с оберткой под asyncio."""

    def __init__(self, db_path: str = "bot_database.db"):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        """Инициализация таблиц базы данных."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    is_banned INTEGER DEFAULT 0,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    async def add_or_update_user(self, user_id: int, username: Optional[str], full_name: str) -> None:
        """Добавление или обновление данных пользователя."""
        def _execute():
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO users (user_id, username, full_name, is_banned)
                    VALUES (?, ?, ?, 0)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username=excluded.username,
                        full_name=excluded.full_name
                """, (user_id, username, full_name))
                conn.commit()
        await asyncio.to_thread(_execute)

    async def is_banned(self, user_id: int) -> bool:
        """Проверка заблокирован ли пользователь."""
        def _execute() -> bool:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                return bool(row[0]) if row else False
        return await asyncio.to_thread(_execute)

    async def set_ban_status(self, user_id: int, banned: bool) -> None:
        """Блокировка или разблокировка пользователя."""
        def _execute():
            status = 1 if banned else 0
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (status, user_id))
                conn.commit()
        await asyncio.to_thread(_execute)

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получение данных конкретного пользователя."""
        def _execute() -> Optional[Dict[str, Any]]:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id, username, full_name, is_banned, joined_at FROM users WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                if row:
                    return {
                        "user_id": row[0],
                        "username": row[1],
                        "full_name": row[2],
                        "is_banned": bool(row[3]),
                        "joined_at": row[4]
                    }
                return None
        return await asyncio.to_thread(_execute)

    async def get_all_users(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Получение списка последних зарегистрированных пользователей."""
        def _execute() -> List[Dict[str, Any]]:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT user_id, username, full_name, is_banned, joined_at FROM users ORDER BY joined_at DESC LIMIT ?",
                    (limit,)
                )
                rows = cursor.fetchall()
                return [
                    {
                        "user_id": r[0],
                        "username": r[1],
                        "full_name": r[2],
                        "is_banned": bool(r[3]),
                        "joined_at": r[4]
                    }
                    for r in rows
                ]
        return await asyncio.to_thread(_execute)

    async def get_stats(self) -> Dict[str, int]:
        """Статистика пользователей."""
        def _execute() -> Dict[str, int]:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM users")
                total = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
                banned = cursor.fetchone()[0]
                return {"total": total, "banned": banned}
        return await asyncio.to_thread(_execute)

db = Database()

class ThrottlingMiddleware(BaseMiddleware):
    """Мидлварь для защиты от спама и частых запросов (cooldown)."""

    def __init__(self, cooldown: float = 2.0):
        super().__init__()
        self.cooldown = cooldown
        self.user_timestamps: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        if not isinstance(event, Message) or not event.from_user:
            return await handler(event, data)

        user_id = event.from_user.id
        now = asyncio.get_event_loop().time()

        if user_id in self.user_timestamps:
            delta = now - self.user_timestamps[user_id]
            if delta < self.cooldown:
                # Тихо игнорируем слишком частые сообщения
                return

        self.user_timestamps[user_id] = now
        return await handler(event, data)


class BanCheckMiddleware(BaseMiddleware):
    """Мидлварь для проверки блокировки пользователя."""

    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any]
    ) -> Any:
        user = getattr(event, "from_user", None)
        if user and await db.is_banned(user.id):
            if isinstance(event, CallbackQuery):
                await event.answer("🚫 Вы заблокированы и не можете использовать бота.", show_alert=True)
            elif isinstance(event, Message):
                ban_text = (
                    "🚫 <b>Доступ ограничен</b>\n\n"
                    "Вы заблокированы в системе и больше не можете отправлять материалы в бота.\n\n"
                    "💬 <i>Если вы считаете, что это ошибка, обратитесь к администраторам канала.</i>"
                )
                msg = await event.answer(ban_text)
                asyncio.create_task(delete_message_delayed(msg, 10))
            return

        # Записываем или обновляем профиль
        if user:
            await db.add_or_update_user(user.id, user.username, user.full_name)

        return await handler(event, data)


class IsAdminFilter(BaseFilter):
    """Фильтр проверки прав администратора."""

    async def __call__(self, event: Message | CallbackQuery) -> bool:
        user = event.from_user
        if not user:
            return False
        return user.id in ADMIN_IDS


async def delete_message_delayed(message: Message, delay: int = 5) -> None:
    """Вспомогательная функция для автоудаления служебных сообщений."""
    try:
        await asyncio.sleep(delay)
        await message.delete()
    except (TelegramBadRequest, TelegramForbiddenError, Exception):
        pass


def get_welcome_keyboard() -> InlineKeyboardMarkup:
    """Современная клавиатура приветствия."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➡️ Отправить материал", callback_data="send_material")
        ],
        [
            InlineKeyboardButton(text="🗑 Удалить пост", callback_data="delete_post_info")
        ]
    ])
    return keyboard


def get_cancel_keyboard() -> InlineKeyboardMarkup:
    """Кнопка отмены / возврата в главное меню."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_start")
        ]
    ])
    return keyboard


def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Главное меню администратора."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 Список пользователей", callback_data="admin_users_1"),
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")
        ]
    ])
    return keyboard


def get_users_list_keyboard(users: List[Dict[str, Any]], page: int = 1) -> InlineKeyboardMarkup:
    """Клавиатура со списком пользователей для админ-панели."""
    buttons = []
    for u in users:
        status_icon = "🚫" if u["is_banned"] else "👤"
        name = u["username"] if u["username"] else u["full_name"]
        btn_text = f"{status_icon} {name} ({u['user_id']})"
        buttons.append([InlineKeyboardButton(text=btn_text, callback_data=f"user_info_{u['user_id']}")])

    # Кнопки навигации / назад
    buttons.append([InlineKeyboardButton(text="🔙 Назад в меню", callback_data="admin_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_user_detail_keyboard(user_id: int, is_banned: bool) -> InlineKeyboardMarkup:
    """Клавиатура управления конкретным пользователем."""
    ban_btn_text = "✅ Разблокировать" if is_banned else "🚫 Заблокировать"
    action = "unban" if is_banned else "ban"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=ban_btn_text, callback_data=f"toggle_{action}_{user_id}")],
        [InlineKeyboardButton(text="🔙 К списку пользователей", callback_data="admin_users_1")]
    ])


router = Router()

WELCOME_TEXT = (
    "👋 <b>Добро пожаловать!</b>\n\n"
    "📸 Здесь можно анонимно отправить фотографию или видео с описанием.\n\n"
    "📝 <b>Просто отправьте:</b>\n"
    "• фотографию или видео;\n"
    "• затем подпишите происходящее.\n\n"
    "📢 Ваш материал будет мгновенно опубликован в канале <b>«Позорники Придника»</b>!\n\n"
    "⚠️ <i>Принимаются только фотографии и видео с текстом.</i>"
)


@router.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start."""
    await message.answer(
        text=WELCOME_TEXT,
        reply_markup=get_welcome_keyboard(),
        link_preview_options=LinkPreviewOptions(is_disabled=True)
    )


@router.callback_query(F.data == "send_material")
async def cb_send_material(callback: CallbackQuery):
    """Нажатие кнопки 'Отправить материал' (заменяет сообщение без спама)."""
    await callback.answer()
    prompt_text = (
        "📷 <b>Жду ваш материал!</b>\n\n"
        "Отправьте сюда <b>фотографию</b> или <b>видео</b> обязательно вместе с подписью (описанием).\n\n"
        "⚡ <i>Публикация в канал происходит мгновенно после отправки.</i>"
    )
    await callback.message.edit_text(
        text=prompt_text,
        reply_markup=get_cancel_keyboard()
    )


@router.callback_query(F.data == "back_to_start")
async def cb_back_to_start(callback: CallbackQuery):
    """Возврат к приветственному сообщению."""
    await callback.answer()
    await callback.message.edit_text(
        text=WELCOME_TEXT,
        reply_markup=get_welcome_keyboard(),
        link_preview_options=LinkPreviewOptions(is_disabled=True)
    )


@router.callback_query(F.data == "delete_post_info")
async def cb_delete_post_info(callback: CallbackQuery):
    """Инструкция по удалению опубликованных постов."""
    await callback.answer()
    delete_text = (
        "🗑 <b>Удаление публикации</b>\n\n"
        "По вопросам платного удаления поста свяжитесь с администратором:\n"
        "👉 @sosite_mne_eblany\n\n"
        "<i>При обращении сразу прикладывайте ссылку на опубликованный материал.</i>"
    )
    delete_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💬 Написать администратору", url="https://t.me/sosite_mne_eblany")
        ],
        [
            InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_start")
        ]
    ])
    await callback.message.edit_text(
        text=delete_text,
        reply_markup=delete_keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True)
    )


@router.message(F.photo | F.video)
async def handle_media_submission(message: Message, bot: Bot):
    """Обработка фото и видео."""
    caption = message.caption

    # Проверка наличия описания к медиа
    if not caption or not caption.strip():
        warning = await message.reply("⚠️ <b>Добавьте описание к материалу.</b>")
        asyncio.create_task(delete_message_delayed(warning, 6))
        return

    # Формируем подпись для публикаций в канале по новому шаблону
    channel_caption = (
        f"{caption.strip()}\n\n"
        "#Позорникипридника\n"
        "#Позорникипридника\n"
        "#Позорникипридника\n\n"
        '<a href="https://t.me/pozorpredlochka_bot"><b>предложка | позорник придника</b></a>'
    )

    try:
        if message.photo:
            photo_file_id = message.photo[-1].file_id
            await bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=photo_file_id,
                caption=channel_caption
            )
        elif message.video:
            video_file_id = message.video.file_id
            await bot.send_video(
                chat_id=CHANNEL_ID,
                video=video_file_id,
                caption=channel_caption
            )

        confirm_msg = await message.reply("✅ <b>Ваш материал успешно опубликован в канале!</b>")
        asyncio.create_task(delete_message_delayed(confirm_msg, 8))

        # Уведомление администраторам о том, кто отправил материал
        user = message.from_user
        if user:
            username_str = f"@{user.username}" if user.username else "отсутствует"
            user_mention = f'<a href="tg://user?id={user.id}">{user.full_name}</a>'

            admin_notice = (
                "🔔 <b>Новая публикация в канале!</b>\n\n"
                f"👤 <b>Отправитель:</b> {user_mention}\n"
                f"🔗 <b>Юзернейм:</b> {username_str}\n"
                f"🆔 <b>ID:</b> <code>{user.id}</code>\n\n"
                f"📝 <b>Текст описания:</b>\n{caption.strip()}"
            )

            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(chat_id=admin_id, text=admin_notice)
                except Exception as admin_err:
                    logger.warning(f"Не удалось отправить уведомление админу {admin_id}: {admin_err}")

    except TelegramForbiddenError:
        logger.error(f"Бот не имеет прав для публикации в канале {CHANNEL_ID}. Проверьте права администратора.")
        err_msg = await message.reply("⚠️ <b>Произошла ошибка при публикации. Бот не имеет прав администратора в канале.</b>")
        asyncio.create_task(delete_message_delayed(err_msg, 8))

    except Exception as e:
        logger.error(f"Ошибка публикации в канал: {e}")
        err_msg = await message.reply("⚠️ <b>Не удалось отправить материал. Попробуйте позже.</b>")
        asyncio.create_task(delete_message_delayed(err_msg, 6))


@router.message(Command("admin"), IsAdminFilter())
async def cmd_admin(message: Message):
    """Вход в административную панель."""
    await message.answer(
        "⚙️ <b>Панель администратора</b>\n\n"
        "Выберите необходимое действие в меню ниже:",
        reply_markup=get_admin_keyboard()
    )


@router.message(
    F.content_type.in_({
        "text", "voice", "video_note", "document", "sticker", 
        "audio", "animation", "contact", "location", "poll"
    })
)
async def reject_invalid_content(message: Message):
    """Удаление непринятых типов контента и вывод предупреждения."""
    try:
        await message.delete()
    except (TelegramBadRequest, TelegramForbiddenError):
        pass

    warning = await message.answer("⚠️ <b>Принимаются только фотографии или видео с текстовым описанием.</b>")
    asyncio.create_task(delete_message_delayed(warning, 5))


@router.callback_query(F.data == "admin_main", IsAdminFilter())
async def cb_admin_main(callback: CallbackQuery):
    """Возврат в главное меню админки."""
    await callback.answer()
    await callback.message.edit_text(
        "⚙️ <b>Панель администратора</b>\n\n"
        "Выберите необходимое действие в меню ниже:",
        reply_markup=get_admin_keyboard()
    )


@router.callback_query(F.data == "admin_stats", IsAdminFilter())
async def cb_admin_stats(callback: CallbackQuery):
    """Просмотр статистики."""
    await callback.answer()
    stats = await db.get_stats()
    stats_text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 всего пользователей в базе: <b>{stats['total']}</b>\n"
        f"🚫 заблокированных пользователей: <b>{stats['banned']}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="admin_main")]
    ])
    await callback.message.edit_text(stats_text, reply_markup=kb)


@router.callback_query(F.data.startswith("admin_users_"), IsAdminFilter())
async def cb_admin_users(callback: CallbackQuery):
    """Список зарегистрированных пользователей."""
    await callback.answer()
    users = await db.get_all_users(limit=20)

    if not users:
        await callback.message.edit_text(
            "👥 <b>Список пользователей пуст.</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_main")]
            ])
        )
        return

    await callback.message.edit_text(
        "👥 <b>Выберите пользователя для управления:</b>",
        reply_markup=get_users_list_keyboard(users)
    )


@router.callback_query(F.data.startswith("user_info_"), IsAdminFilter())
async def cb_user_info(callback: CallbackQuery):
    """Информация о конкретном пользователе."""
    await callback.answer()
    user_id = int(callback.data.split("_")[2])
    user = await db.get_user(user_id)

    if not user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    status = "🔴 Заблокирован" if user["is_banned"] else "🟢 Активен"
    username_str = f"@{user['username']}" if user["username"] else "отсутствует"

    text = (
        f"👤 <b>Карточка пользователя</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"📛 Имя: <b>{user['full_name']}</b>\n"
        f"🔗 Username: {username_str}\n"
        f"📌 Статус: {status}\n"
        f"📅 Дата первого визита: <code>{user['joined_at']}</code>"
    )

    await callback.message.edit_text(
        text=text,
        reply_markup=get_user_detail_keyboard(user_id, user["is_banned"])
    )


@router.callback_query(F.data.startswith("toggle_"), IsAdminFilter())
async def cb_toggle_ban(callback: CallbackQuery):
    """Блокировка или разблокировка пользователя."""
    await callback.answer()
    parts = callback.data.split("_")
    action = parts[1]  # ban or unban
    user_id = int(parts[2])

    should_ban = (action == "ban")
    await db.set_ban_status(user_id, should_ban)

    user = await db.get_user(user_id)
    if not user:
        return

    status_str = "заблокирован" if should_ban else "разблокирован"
    await callback.answer(f"Пользователь {status_str}!", show_alert=True)

    status = "🔴 Заблокирован" if user["is_banned"] else "🟢 Активен"
    username_str = f"@{user['username']}" if user["username"] else "отсутствует"

    text = (
        f"👤 <b>Карточка пользователя</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"📛 Имя: <b>{user['full_name']}</b>\n"
        f"🔗 Username: {username_str}\n"
        f"📌 Статус: {status}\n"
        f"📅 Дата первого визита: <code>{user['joined_at']}</code>"
    )

    await callback.message.edit_text(
        text=text,
        reply_markup=get_user_detail_keyboard(user_id, user["is_banned"])
    )


@router.error()
async def error_handler(event: ErrorEvent):
    """Глобальный обработчик ошибок."""
    logger.error(f"Необработанное исключение: {event.exception}", exc_info=True)


async def main():
    """Запуск телеграм бота."""
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    # Подключаем middlewares
    dp.message.middleware(ThrottlingMiddleware(cooldown=2.0))
    dp.message.middleware(BanCheckMiddleware())
    dp.callback_query.middleware(BanCheckMiddleware())

    # Регистрируем роутеры
    dp.include_router(router)

    logger.info("Запуск Telegram-бота...")
    logger.info(f"Настроено администраторов: {len(ADMIN_IDS)}")
    logger.info(f"Целевой канал для публикаций: {CHANNEL_ID}")

    try:
        # Пропускаем накопившиеся обновления при старте
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот успешно остановлен.")