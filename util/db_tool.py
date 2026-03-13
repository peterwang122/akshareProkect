import json
import math
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
    FIELD_LIMITS = {
        'open_price': 999999.99,
        'close_price': 999999.99,
        'high_price': 999999.99,
        'low_price': 999999.99,
        'volume': 999999999999.99,
        'turnover': 99999999999999.99,
        'amplitude': 999.99,
        'price_change_rate': 999.99,
        'price_change_amount': 999999.99,
        'turnover_rate': 999.99,
        'pe_ttm': 9999999999.9999,
        'pb': 9999999999.9999,
        'total_market_value': 9999999999999999999999.99,
        'circulating_market_value': 9999999999999999999999.99,
    }

    def __init__(self):
        self.db_info = self.load_db_info()
        self.pool = None

    def load_db_info(self):
        db_info_path = os.path.join(get_config_path(), 'db_info.json')
        with open(db_info_path, 'r') as f:
            return json.load(f)

    def _normalize_numeric(self, field, value):
        if value is None:
            return None

        try:
            num = float(value)
        except (TypeError, ValueError):
            return None

        if not math.isfinite(num):
            return None

        limit = self.FIELD_LIMITS.get(field)
        if limit is not None and abs(num) > limit:
            return None

        return num

    def _sanitize_update(self, update):
        sanitized = dict(update)
        for field in [
            'open_price', 'close_price', 'high_price', 'low_price', 'volume', 'turnover',
            'amplitude', 'price_change_rate', 'price_change_amount', 'turnover_rate'
        ]:
            sanitized[field] = self._normalize_numeric(field, update.get(field))
        sanitized['date'] = str(update.get('date', ''))
        return sanitized

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

        sanitized_updates = [self._sanitize_update(update) for update in updates]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                stock_code = sanitized_updates[0]['stock_code']
                update_dates = [update['date'] for update in sanitized_updates]

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
                    for update in sanitized_updates
                    if update['date'] and str(update['date']) not in existing_dates
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

    async def upsert_stock_basic_info(self, basic_rows):
        """插入 stock_zh_a_spot_em 的代码和名称，插入前先查重。"""
        if not basic_rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                codes = [row['stock_code'] for row in basic_rows if row.get('stock_code')]
                if not codes:
                    return 0

                placeholders = ','.join(['%s'] * len(codes))
                query_existing = f"SELECT stock_code FROM stock_basic_info WHERE stock_code IN ({placeholders})"
                await cursor.execute(query_existing, codes)
                existing_codes = {row[0] for row in await cursor.fetchall()}

                rows_to_insert = [
                    (row['stock_code'], row['stock_name'])
                    for row in basic_rows
                    if row['stock_code'] not in existing_codes
                ]

                if not rows_to_insert:
                    return 0

                query_insert = """
                INSERT INTO stock_basic_info (stock_code, stock_name)
                VALUES (%s, %s)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
                return len(rows_to_insert)

    async def update_stock_data_valuation(self, valuation_rows, spot_date):
        """按 stock_code + date 更新 stock_data 的估值字段。"""
        if not valuation_rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        rows_to_update = []
        for row in valuation_rows:
            rows_to_update.append((
                self._normalize_numeric('pe_ttm', row.get('pe_ttm')),
                self._normalize_numeric('pb', row.get('pb')),
                self._normalize_numeric('total_market_value', row.get('total_market_value')),
                self._normalize_numeric('circulating_market_value', row.get('circulating_market_value')),
                row['stock_code'],
                spot_date,
            ))

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_update = """
                UPDATE stock_data
                SET pe_ttm = %s,
                    pb = %s,
                    total_market_value = %s,
                    circulating_market_value = %s
                WHERE stock_code = %s AND date = %s
                """
                await cursor.executemany(query_update, rows_to_update)
                await conn.commit()
                return cursor.rowcount
