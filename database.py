import asyncpg
import json
from config import config
from logger import logger
import datetime


class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Установить соединение с базой данных."""
        self.pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=1,
            max_size=10
        )

    async def close(self):
        """Закрыть соединение."""
        if self.pool:
            await self.pool.close()

    async def execute_query(self, query: str):
        """Выполнить SQL запрос и вернуть одно значение (для бота)"""
        async with self.pool.acquire() as connection:
            try:
                result = await connection.fetch(query)

                if not result:
                    return 0

                first_row = result[0]
                if first_row:
                    return first_row[0]
                else:
                    return 0

            except Exception as e:
                raise Exception(f'Database error: {str(e)}')

    def _parse_datetime(self, dt_str):
        """Преобразует строку в datetime объект."""
        if dt_str is None:
            return None

        try:
            if isinstance(dt_str, datetime.datetime):
                if dt_str.tzinfo is not None:
                    return dt_str.astimezone(
                        datetime.timezone.utc
                    ).replace(tzinfo=None)
                return dt_str

            if isinstance(dt_str, str):
                import re
                dt_str = re.sub(r'\.\d+', '', dt_str)

                if dt_str.endswith('Z'):
                    dt_str = dt_str[:-1] + '+00:00'

                dt = datetime.datetime.fromisoformat(dt_str)

                if dt.tzinfo is not None:
                    dt = dt.astimezone(datetime.timezone.utc)
                    dt = dt.replace(tzinfo=None)

                return dt

            return None

        except Exception as e:
            logger.warning(f'Warning: Could not parse datetime {dt_str}: {e}')
            return datetime.datetime.utcnow()

    async def init_database(self, json_file_path: str = 'videos.json'):
        """Инициализировать базу данных и загрузить данные из JSON."""
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS videos (
            id VARCHAR(255) PRIMARY KEY,
            creator_id VARCHAR(255),
            video_created_at TIMESTAMP,
            views_count INTEGER,
            likes_count INTEGER,
            comments_count INTEGER,
            reports_count INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS video_snapshots (
            id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255) REFERENCES videos(id) ON DELETE CASCADE,
            views_count INTEGER,
            likes_count INTEGER,
            comments_count INTEGER,
            reports_count INTEGER,
            delta_views_count INTEGER,
            delta_likes_count INTEGER,
            delta_comments_count INTEGER,
            delta_reports_count INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """

        async with self.pool.acquire() as connection:
            await connection.execute(create_tables_sql)

            count_result = await connection.fetchval(
                'SELECT COUNT(*) FROM videos'
            )

            if count_result > 0:
                logger.info(
                    f'Database already contains {count_result} videos. '
                    'Skipping data load.'
                )
                return

            try:
                with open(json_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                videos_data = data.get('videos', [])

                if not videos_data:
                    logger.warning('Warning: No videos found in JSON data')
                    return

                logger.info(f'Found {len(videos_data)} video entries in JSON')

                inserted_videos = 0
                inserted_snapshots = 0

                for i, video in enumerate(videos_data, 1):
                    try:
                        video_created_at = self._parse_datetime(
                            video.get('video_created_at')
                        )
                        created_at = self._parse_datetime(
                            video.get('created_at')
                        )
                        updated_at = self._parse_datetime(
                            video.get('updated_at')
                        )
                        await connection.execute(
                            """
                            INSERT INTO videos
                            (id, creator_id, video_created_at, views_count,
                            likes_count, comments_count, reports_count,
                            created_at, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (id) DO NOTHING
                            """,
                            video.get('id'),
                            video.get('creator_id'),
                            video_created_at,
                            video.get('views_count', 0),
                            video.get('likes_count', 0),
                            video.get('comments_count', 0),
                            video.get('reports_count', 0),
                            created_at,
                            updated_at
                        )

                        inserted_videos += 1

                        snapshots = video.get('snapshots', [])

                        for snapshot in snapshots:
                            snapshot_created_at = self._parse_datetime(
                                snapshot.get('created_at')
                            )
                            snapshot_updated_at = self._parse_datetime(
                                snapshot.get('updated_at')
                            )
                            await connection.execute(
                                """
                                INSERT INTO video_snapshots
                                (id, video_id, views_count, likes_count,
                                 comments_count, reports_count,
                                 delta_views_count,
                                 delta_likes_count, delta_comments_count,
                                 delta_reports_count, created_at, updated_at)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                                        $10, $11, $12)
                                ON CONFLICT (id) DO NOTHING
                                """,
                                snapshot.get('id'),
                                snapshot.get('video_id', video.get('id')),
                                snapshot.get('views_count', 0),
                                snapshot.get('likes_count', 0),
                                snapshot.get('comments_count', 0),
                                snapshot.get('reports_count', 0),
                                snapshot.get('delta_views_count', 0),
                                snapshot.get('delta_likes_count', 0),
                                snapshot.get('delta_comments_count', 0),
                                snapshot.get('delta_reports_count', 0),
                                snapshot_created_at,
                                snapshot_updated_at
                            )

                            inserted_snapshots += 1

                    except Exception as e:
                        video_id = video.get('id')
                        logger.error(f'Error inserting video {video_id}: {e}')
                        continue

                    if i % 100 == 0:
                        logger.info(f'Processed {i} videos...')

                logger.info(
                    f'Successfully loaded {inserted_videos} videos '
                    f'and {inserted_snapshots} snapshots'
                )
            except Exception as e:
                logger.error(f'Error loading JSON data: {e}')


db = Database()
