import asyncio

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart

from config import config
from database import db
from llm_handler import llm_handler
from logger import logger

bot = Bot(token=config.TELEGRAM_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    """Обработчик команды /start."""
    welcome_text = """
    Привет! Я бот для аналитики видео.

    Задавайте вопросы на естественном языке, например:
    - Сколько всего видео есть в системе?
    - Сколько видео набрало больше 10 000 просмотров?
    - Сколько всего видео есть в системе?
    - Сколько видео вышло с 1 ноября 2025 по 5 ноября 2025 включительно?
    - На сколько просмотров в сумме выросли все видео 28 ноября 2025?
    - Сколько разных видео получали новые просмотры 27 ноября 2025?

    Я отвечу одним числом — результатом запроса.
    """
    await message.answer(welcome_text)


@dp.message(F.text)
async def handle_message(message: types.Message):
    """Обработчик текстовых сообщений."""
    user_query = message.text.strip()

    if not user_query:
        return await message.answer('Пожалуйста, введите текстовый запрос.')

    await message.chat.do('typing')

    try:
        sql_query = await llm_handler.generate_sql(user_query)
        logger.info(f'Generated SQL: {sql_query}')

        result = await db.execute_query(sql_query)

        await message.answer(str(result))

    except Exception as e:
        logger.error(f'Error processing query: {e}')
        await message.answer(
            'Произошла ошибка при обработке запроса. '
            'Попробуйте сформулировать иначе.'
        )


@dp.message(
    F.content_type.in_({
        types.ContentType.VOICE,
        types.ContentType.AUDIO,
        types.ContentType.PHOTO,
        types.ContentType.VIDEO,
        types.ContentType.DOCUMENT,
        types.ContentType.LOCATION,
        types.ContentType.CONTACT,
        types.ContentType.STICKER,
    })
)
async def reply_to_other_types_of_messages(message: types.Message):
    """Функция обрабатывает сообщение другого вида нежели текстовое."""
    await message.reply(
        'Извините, я могу работать только с текстовыми сообщениями.'
    )


async def main():
    """Основная функция запуска бота."""
    await db.connect()
    logger.info('Connected to database.')

    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
