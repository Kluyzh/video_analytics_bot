import asyncio

from database import db


async def load_data():
    """Загрузка данных в базу данных."""
    await db.connect()
    await db.load_data()
    await db.close()

if __name__ == '__main__':
    asyncio.run(load_data())
