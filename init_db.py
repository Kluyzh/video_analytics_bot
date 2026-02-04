import asyncio
from database import db
from logger import logger


async def init_database():
    """Инициализация базы данных."""
    try:
        await db.connect()

        json_file = 'videos.json'

        logger.info(f'Loading data from {json_file}...')
        await db.init_database(json_file)
        logger.info('Database initialized successfully!')

    except Exception as e:
        logger.error(f'Error initializing database: {e}')
    finally:
        await db.close()

if __name__ == '__main__':
    asyncio.run(init_database())
