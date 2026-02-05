import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')

    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('POSTGRES_DB')
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')

    LLM_API_KEY = os.getenv('LLM_API_KEY')
    LLM_MODEL = os.getenv('LLM_MODEL')
    BASE_URL = os.getenv('BASE_URL')

    @property
    def DATABASE_URL(self):
        return (
            f'postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:'
            f'{self.DB_PORT}/{self.DB_NAME}'
        )


config = Config()
