import json
import os
from datetime import datetime

import aiomysql
from config.get_config_path import get_config_path


def get_timestamp():
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    return f"{date_string}_{timestamp}"


class DbTools:
    def __init__(self):
        self.db_info = self.load_db_info()
        self.pool = None

    def load_db_info(self):
        db_info_path = os.path.join(get_config_path(), 'db_info.json')
        with open(db_info_path, 'r') as f:
            return json.load(f)

    async def init_pool(self):
        if self.pool is not None:
            return
        self.pool = await aiomysql.create_pool(
            host=self.db_info.get('host'),
            port=int(self.db_info.get('port', 3306)),
            user=self.db_info.get('user'),
            password=self.db_info.get('password'),
            db=self.db_info.get('database') or self.db_info.get('db'),
            charset=self.db_info.get('charset', 'utf8mb4'),
            autocommit=False,
            minsize=1,
            maxsize=10,
        )

    async def close(self):
        if self.pool is None:
            return
        self.pool.close()
        await self.pool.wait_closed()
        self.pool = None

    async def batch_stock_info(self, updates):
        if not updates:
            return

        if self.pool is None:
            await self.init_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                stock_code = updates[0]['stock_code']
                update_dates = [update['date'] for update in updates]

                placeholders = ','.join(['%s'] * len(update_dates))
                query_check = (
                    f"SELECT `date` FROM stock_data WHERE `stock_code` = %s AND `date` IN ({placeholders})"
                )
                await cursor.execute(query_check, [stock_code, *update_dates])
                existing_dates = {str(row[0]) for row in await cursor.fetchall()}

                rows_to_insert = [
                    (
                        update['stock_code'],
                        update['open_price'],
                        update['close_price'],
                        update['high_price'],
                        update['low_price'],
                        update['volume'],
                        update['turnover'],
                        update['amplitude'],
                        update['price_change_rate'],
                        update['price_change_amount'],
                        update['turnover_rate'],
                        update['date'],
                    )
                    for update in updates
                    if str(update['date']) not in existing_dates
                ]

                if not rows_to_insert:
                    return

                query_insert = """
                INSERT INTO stock_data (
                    stock_code,
                    open_price,
                    close_price,
                    high_price,
                    low_price,
                    volume,
                    turnover,
                    amplitude,
                    price_change_rate,
                    price_change_amount,
                    turnover_rate,
                    date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
