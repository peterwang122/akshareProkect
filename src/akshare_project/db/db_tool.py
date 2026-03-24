import json
import math
import os
from datetime import datetime

import aiomysql

from akshare_project.core.paths import get_config_dir


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
        'settle_price': 999999.99,
        'pre_settle_price': 999999.99,
        'volume': 999999999999.99,
        'open_interest': 999999999999.99,
        'turnover': 99999999999999.99,
        'amplitude': 99999999999999.99,
        'price_change_rate': 99999999999999.99,
        'price_change_amount': 999999.99,
        'turnover_rate': 99999999999999.99,
        'pre_close_price': 999999.99,
        'iopv_realtime': 999999.99,
        'discount_rate': 99999999999999.99,
        'volume_ratio': 99999999999999.99,
        'current_hand': 999999999999.99,
        'bid1_price': 999999.99,
        'ask1_price': 999999.99,
        'outer_volume': 999999999999.99,
        'inner_volume': 999999999999.99,
        'latest_share': 999999999999.99,
        'main_net_inflow': 99999999999999.99,
        'main_net_inflow_ratio': 99999999999999.99,
        'extra_large_net_inflow': 99999999999999.99,
        'extra_large_net_inflow_ratio': 99999999999999.99,
        'large_net_inflow': 99999999999999.99,
        'large_net_inflow_ratio': 99999999999999.99,
        'medium_net_inflow': 99999999999999.99,
        'medium_net_inflow_ratio': 99999999999999.99,
        'small_net_inflow': 99999999999999.99,
        'small_net_inflow_ratio': 99999999999999.99,
        'pe_ttm': 9999999999.9999,
        'pb': 9999999999.9999,
        'total_market_value': 9999999999999999999999.99,
        'circulating_market_value': 9999999999999999999999.99,
    }

    def __init__(self):
        self.db_info = self.load_db_info()
        self.pool = None

    def load_db_info(self):
        db_info_path = os.path.join(get_config_dir(), 'db_info.json')
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

    def _sanitize_index_daily_update(self, update):
        sanitized = dict(update)
        for field in [
            'open_price', 'close_price', 'high_price', 'low_price', 'volume', 'turnover',
            'amplitude', 'price_change_rate', 'price_change_amount', 'turnover_rate'
        ]:
            sanitized[field] = self._normalize_numeric(field, update.get(field))
        sanitized['index_code'] = str(update.get('index_code', '')).strip()
        sanitized['trade_date'] = str(update.get('trade_date', ''))
        sanitized['data_source'] = str(update.get('data_source', 'akshare')).strip() or 'akshare'
        return sanitized

    def _sanitize_cffex_member_ranking(self, row):
        sanitized = dict(row)
        sanitized['product_code'] = str(row.get('product_code', '')).strip().upper()
        sanitized['product_name'] = str(row.get('product_name', '')).strip()
        sanitized['contract_code'] = str(row.get('contract_code', '')).strip().upper()
        sanitized['trade_date'] = str(row.get('trade_date', '')).strip()
        sanitized['rank_no'] = str(row.get('rank_no', '')).strip()
        sanitized['volume_rank'] = str(row.get('volume_rank', '')).strip() or None
        sanitized['volume_member'] = str(row.get('volume_member', '')).strip() or None
        sanitized['volume_value'] = self._normalize_numeric('cffex_volume_value', row.get('volume_value'))
        sanitized['volume_change_value'] = self._normalize_numeric(
            'cffex_volume_change_value',
            row.get('volume_change_value'),
        )
        sanitized['long_rank'] = str(row.get('long_rank', '')).strip() or None
        sanitized['long_member'] = str(row.get('long_member', '')).strip() or None
        sanitized['long_open_interest'] = self._normalize_numeric(
            'cffex_long_open_interest',
            row.get('long_open_interest'),
        )
        sanitized['long_change_value'] = self._normalize_numeric(
            'cffex_long_change_value',
            row.get('long_change_value'),
        )
        sanitized['short_rank'] = str(row.get('short_rank', '')).strip() or None
        sanitized['short_member'] = str(row.get('short_member', '')).strip() or None
        sanitized['short_open_interest'] = self._normalize_numeric(
            'cffex_short_open_interest',
            row.get('short_open_interest'),
        )
        sanitized['short_change_value'] = self._normalize_numeric(
            'cffex_short_change_value',
            row.get('short_change_value'),
        )
        sanitized['source_url'] = str(row.get('source_url', '')).strip() or None
        return sanitized

    def _sanitize_douyin_emotion_row(self, row):
        sanitized = dict(row)
        sanitized['emotion_date'] = str(row.get('emotion_date', '')).strip()
        sanitized['video_id'] = str(row.get('video_id', '')).strip()
        sanitized['account_id'] = str(row.get('account_id', '')).strip() or '1368194981'
        sanitized['account_name'] = str(row.get('account_name', '')).strip() or None
        sanitized['video_title'] = str(row.get('video_title', '')).strip() or None
        sanitized['video_url'] = str(row.get('video_url', '')).strip()
        sanitized['hs300_emotion'] = self._normalize_numeric('douyin_emotion_value', row.get('hs300_emotion'))
        sanitized['zz500_emotion'] = self._normalize_numeric('douyin_emotion_value', row.get('zz500_emotion'))
        sanitized['zz1000_emotion'] = self._normalize_numeric('douyin_emotion_value', row.get('zz1000_emotion'))
        sanitized['sz50_emotion'] = self._normalize_numeric('douyin_emotion_value', row.get('sz50_emotion'))
        sanitized['raw_ocr_text'] = str(row.get('raw_ocr_text', '')).strip() or None
        sanitized['extraction_method'] = str(row.get('extraction_method', '')).strip() or 'ocr'
        sanitized['extraction_status'] = str(row.get('extraction_status', '')).strip() or 'SUCCESS'
        return sanitized

    def _sanitize_forex_daily_row(self, row):
        sanitized = dict(row)
        sanitized['symbol_code'] = str(row.get('symbol_code', '')).strip().upper()
        sanitized['symbol_name'] = str(row.get('symbol_name', '')).strip() or None
        sanitized['trade_date'] = str(row.get('trade_date', '')).strip()
        sanitized['open_price'] = self._normalize_numeric('open_price', row.get('open_price'))
        sanitized['latest_price'] = self._normalize_numeric('close_price', row.get('latest_price'))
        sanitized['high_price'] = self._normalize_numeric('high_price', row.get('high_price'))
        sanitized['low_price'] = self._normalize_numeric('low_price', row.get('low_price'))
        sanitized['amplitude'] = self._normalize_numeric('amplitude', row.get('amplitude'))
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'forex_hist_em'
        return sanitized

    def _sanitize_etf_daily_row(self, row):
        sanitized = dict(row)
        sanitized['etf_code'] = str(row.get('etf_code', '')).strip()
        sanitized['etf_name'] = str(row.get('etf_name', '')).strip() or None
        sanitized['trade_date'] = str(row.get('trade_date', '')).strip()
        for field in [
            'open_price', 'close_price', 'high_price', 'low_price', 'volume', 'turnover',
            'amplitude', 'price_change_rate', 'price_change_amount', 'turnover_rate',
            'pre_close_price', 'iopv_realtime', 'discount_rate', 'volume_ratio',
            'current_hand', 'bid1_price', 'ask1_price', 'outer_volume', 'inner_volume',
            'latest_share', 'circulating_market_value', 'total_market_value',
            'main_net_inflow', 'main_net_inflow_ratio',
            'extra_large_net_inflow', 'extra_large_net_inflow_ratio',
            'large_net_inflow', 'large_net_inflow_ratio',
            'medium_net_inflow', 'medium_net_inflow_ratio',
            'small_net_inflow', 'small_net_inflow_ratio',
        ]:
            sanitized[field] = self._normalize_numeric(field, row.get(field))

        spot_data_date = row.get('spot_data_date')
        sanitized['spot_data_date'] = str(spot_data_date).split(' ')[0].strip() if spot_data_date else None

        spot_update_time = row.get('spot_update_time')
        if hasattr(spot_update_time, 'to_pydatetime'):
            spot_update_time = spot_update_time.to_pydatetime()
        if hasattr(spot_update_time, 'tzinfo') and getattr(spot_update_time, 'tzinfo', None) is not None:
            spot_update_time = spot_update_time.replace(tzinfo=None)
        sanitized['spot_update_time'] = spot_update_time or None

        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'fund_etf_hist_em'
        sanitized['adjust_type'] = str(row.get('adjust_type', '')).strip() or None
        return sanitized

    def _sanitize_futures_daily_row(self, row):
        sanitized = dict(row)
        sanitized['market'] = str(row.get('market', '')).strip().upper() or 'CFFEX'
        sanitized['symbol'] = str(row.get('symbol', '')).strip().upper()
        sanitized['variety'] = str(row.get('variety', '')).strip().upper() or None
        sanitized['trade_date'] = str(row.get('trade_date', '')).strip()
        sanitized['open_price'] = self._normalize_numeric('open_price', row.get('open_price'))
        sanitized['high_price'] = self._normalize_numeric('high_price', row.get('high_price'))
        sanitized['low_price'] = self._normalize_numeric('low_price', row.get('low_price'))
        sanitized['close_price'] = self._normalize_numeric('close_price', row.get('close_price'))
        sanitized['volume'] = self._normalize_numeric('volume', row.get('volume'))
        sanitized['open_interest'] = self._normalize_numeric('open_interest', row.get('open_interest'))
        sanitized['turnover'] = self._normalize_numeric('turnover', row.get('turnover'))
        sanitized['settle_price'] = self._normalize_numeric('settle_price', row.get('settle_price'))
        sanitized['pre_settle_price'] = self._normalize_numeric('pre_settle_price', row.get('pre_settle_price'))
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'futures_hist_em'
        return sanitized

    def _sanitize_excel_emotion_row(self, row):
        sanitized = dict(row)
        sanitized['emotion_date'] = str(row.get('emotion_date', '')).strip()
        sanitized['index_name'] = str(row.get('index_name', '')).strip()
        sanitized['emotion_value'] = self._normalize_numeric('douyin_emotion_value', row.get('emotion_value'))
        sanitized['source_file'] = str(row.get('source_file', '')).strip() or None
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'excel'
        return sanitized

    def _sanitize_option_spot_row(self, row):
        sanitized = dict(row)
        sanitized['index_type'] = str(row.get('index_type', '')).strip().upper()
        sanitized['index_name'] = str(row.get('index_name', '')).strip() or None
        sanitized['product_code'] = str(row.get('product_code', '')).strip().lower() or None
        sanitized['contract_symbol'] = str(row.get('contract_symbol', '')).strip().lower()
        sanitized['strike_price'] = self._normalize_numeric('strike_price', row.get('strike_price'))
        sanitized['call_option_symbol'] = str(row.get('call_option_symbol', '')).strip()
        sanitized['call_buy_volume'] = self._normalize_numeric('volume', row.get('call_buy_volume'))
        sanitized['call_buy_price'] = self._normalize_numeric('close_price', row.get('call_buy_price'))
        sanitized['call_latest_price'] = self._normalize_numeric('close_price', row.get('call_latest_price'))
        sanitized['call_sell_price'] = self._normalize_numeric('close_price', row.get('call_sell_price'))
        sanitized['call_sell_volume'] = self._normalize_numeric('volume', row.get('call_sell_volume'))
        sanitized['call_open_interest'] = self._normalize_numeric('open_interest', row.get('call_open_interest'))
        sanitized['call_change'] = self._normalize_numeric('price_change_amount', row.get('call_change'))
        sanitized['put_option_symbol'] = str(row.get('put_option_symbol', '')).strip()
        sanitized['put_buy_volume'] = self._normalize_numeric('volume', row.get('put_buy_volume'))
        sanitized['put_buy_price'] = self._normalize_numeric('close_price', row.get('put_buy_price'))
        sanitized['put_latest_price'] = self._normalize_numeric('close_price', row.get('put_latest_price'))
        sanitized['put_sell_price'] = self._normalize_numeric('close_price', row.get('put_sell_price'))
        sanitized['put_sell_volume'] = self._normalize_numeric('volume', row.get('put_sell_volume'))
        sanitized['put_open_interest'] = self._normalize_numeric('open_interest', row.get('put_open_interest'))
        sanitized['put_change'] = self._normalize_numeric('price_change_amount', row.get('put_change'))
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'option_spot_sina'
        return sanitized

    def _sanitize_option_daily_row(self, row):
        sanitized = dict(row)
        sanitized['index_type'] = str(row.get('index_type', '')).strip().upper()
        sanitized['index_name'] = str(row.get('index_name', '')).strip() or None
        sanitized['product_code'] = str(row.get('product_code', '')).strip().lower() or None
        sanitized['contract_symbol'] = str(row.get('contract_symbol', '')).strip().lower() or None
        sanitized['option_symbol'] = str(row.get('option_symbol', '')).strip()
        sanitized['option_type'] = str(row.get('option_type', '')).strip().upper() or None
        sanitized['strike_price'] = self._normalize_numeric('strike_price', row.get('strike_price'))
        sanitized['trade_date'] = str(row.get('trade_date', '')).strip()
        sanitized['open_price'] = self._normalize_numeric('open_price', row.get('open_price'))
        sanitized['high_price'] = self._normalize_numeric('high_price', row.get('high_price'))
        sanitized['low_price'] = self._normalize_numeric('low_price', row.get('low_price'))
        sanitized['close_price'] = self._normalize_numeric('close_price', row.get('close_price'))
        sanitized['volume'] = self._normalize_numeric('volume', row.get('volume'))
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'option_daily_sina'
        return sanitized

    def _sanitize_failed_task_row(self, row):
        payload = row.get('payload_json')
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, ensure_ascii=False, default=str)
        sanitized = {
            'task_name': str(row.get('task_name', '')).strip(),
            'task_stage': str(row.get('task_stage', '')).strip() or 'task',
            'task_key': str(row.get('task_key', '')).strip(),
            'payload_json': str(payload or '{}').strip() or '{}',
            'error_message': str(row.get('error_message', '')).strip() or None,
        }
        return sanitized

    async def init_pool(self):
        if self.pool is not None:
            return
        self.pool = await aiomysql.create_pool(
            host=self.db_info.get('host'),
            port=int(self.db_info.get('port', 3306)),
            user=self.db_info.get('user'),
            password=self.db_info.get('passwd'),
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
            return 0

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
                    return 0

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
                    date,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
                return len(rows_to_insert)

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
                    circulating_market_value = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE stock_code = %s AND date = %s
                """
                await cursor.executemany(query_update, rows_to_update)
                await conn.commit()
                return cursor.rowcount

    async def upsert_index_basic_info(self, basic_rows):
        if not basic_rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        deduped_rows = {}
        for row in basic_rows:
            index_code = str(row.get('index_code', '')).strip()
            if not index_code:
                continue
            deduped_rows[index_code] = (
                index_code,
                str(row.get('simple_code', '')).strip() or None,
                str(row.get('market', '')).strip() or None,
                str(row.get('index_name', '')).strip(),
                str(row.get('data_source', 'akshare')).strip() or 'akshare',
            )

        rows_to_upsert = list(deduped_rows.values())
        if not rows_to_upsert:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_basic_info (
                    index_code,
                    simple_code,
                    market,
                    index_name,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    simple_code = VALUES(simple_code),
                    market = VALUES(market),
                    index_name = VALUES(index_name),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query_upsert, rows_to_upsert)
                await conn.commit()
                return len(rows_to_upsert)

    async def batch_index_daily_data(self, updates):
        if not updates:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_updates = [self._sanitize_index_daily_update(update) for update in updates]
        sanitized_updates = [
            update for update in sanitized_updates
            if update['index_code'] and update['trade_date']
        ]
        if not sanitized_updates:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                index_code = sanitized_updates[0]['index_code']
                trade_dates = [update['trade_date'] for update in sanitized_updates]
                placeholders = ','.join(['%s'] * len(trade_dates))
                query_check = (
                    f"SELECT trade_date FROM index_daily_data WHERE index_code = %s "
                    f"AND trade_date IN ({placeholders})"
                )
                await cursor.execute(query_check, [index_code, *trade_dates])
                existing_dates = {str(row[0]) for row in await cursor.fetchall()}

                rows_to_insert = [
                    (
                        update['index_code'],
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
                        update['trade_date'],
                        update['data_source'],
                    )
                    for update in sanitized_updates
                    if update['trade_date'] not in existing_dates
                ]

                if not rows_to_insert:
                    return 0

                query_insert = """
                INSERT INTO index_daily_data (
                    index_code,
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
                    trade_date,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
                return len(rows_to_insert)

    async def upsert_stock_daily_snapshots(self, updates):
        if not updates:
            return 0, 0

        if self.pool is None:
            await self.init_pool()

        sanitized_updates = [self._sanitize_update(update) for update in updates]
        sanitized_updates = [
            update for update in sanitized_updates
            if update.get('stock_code') and update.get('date')
        ]
        if not sanitized_updates:
            return 0, 0

        deduped_updates = {}
        for update in sanitized_updates:
            deduped_updates[(update['stock_code'], update['date'])] = update
        sanitized_updates = list(deduped_updates.values())

        stock_codes = sorted({update['stock_code'] for update in sanitized_updates})
        update_dates = sorted({update['date'] for update in sanitized_updates})

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                stock_placeholders = ','.join(['%s'] * len(stock_codes))
                date_placeholders = ','.join(['%s'] * len(update_dates))
                query_check = (
                    f"SELECT stock_code, `date` FROM stock_data "
                    f"WHERE stock_code IN ({stock_placeholders}) AND `date` IN ({date_placeholders})"
                )
                await cursor.execute(query_check, [*stock_codes, *update_dates])
                existing_keys = {
                    (str(stock_code), str(update_date))
                    for stock_code, update_date in await cursor.fetchall()
                }

                rows_to_insert = []
                rows_to_update = []
                for update in sanitized_updates:
                    insert_values = (
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
                    if (update['stock_code'], update['date']) in existing_keys:
                        rows_to_update.append(insert_values[1:] + (update['stock_code'], update['date']))
                    else:
                        rows_to_insert.append(insert_values)

                if rows_to_insert:
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
                        date,
                        created_at,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """
                    await cursor.executemany(query_insert, rows_to_insert)

                if rows_to_update:
                    query_update = """
                    UPDATE stock_data
                    SET open_price = %s,
                        close_price = %s,
                        high_price = %s,
                        low_price = %s,
                        volume = %s,
                        turnover = %s,
                        amplitude = %s,
                        price_change_rate = %s,
                        price_change_amount = %s,
                        turnover_rate = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE stock_code = %s AND date = %s
                    """
                    await cursor.executemany(query_update, rows_to_update)

                await conn.commit()
                return len(rows_to_insert), len(rows_to_update)

    async def upsert_cffex_member_rankings(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_cffex_member_ranking(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['product_code'] and row['contract_code'] and row['trade_date'] and row['rank_no']
        ]
        if not sanitized_rows:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO cffex_member_rankings (
                    product_code,
                    product_name,
                    contract_code,
                    trade_date,
                    rank_no,
                    volume_rank,
                    volume_member,
                    volume_value,
                    volume_change_value,
                    long_rank,
                    long_member,
                    long_open_interest,
                    long_change_value,
                    short_rank,
                    short_member,
                    short_open_interest,
                    short_change_value,
                    source_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    product_name = VALUES(product_name),
                    volume_rank = VALUES(volume_rank),
                    volume_member = VALUES(volume_member),
                    volume_value = VALUES(volume_value),
                    volume_change_value = VALUES(volume_change_value),
                    long_rank = VALUES(long_rank),
                    long_member = VALUES(long_member),
                    long_open_interest = VALUES(long_open_interest),
                    long_change_value = VALUES(long_change_value),
                    short_rank = VALUES(short_rank),
                    short_member = VALUES(short_member),
                    short_open_interest = VALUES(short_open_interest),
                    short_change_value = VALUES(short_change_value),
                    source_url = VALUES(source_url),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['product_code'],
                        row['product_name'],
                        row['contract_code'],
                        row['trade_date'],
                        row['rank_no'],
                        row['volume_rank'],
                        row['volume_member'],
                        row['volume_value'],
                        row['volume_change_value'],
                        row['long_rank'],
                        row['long_member'],
                        row['long_open_interest'],
                        row['long_change_value'],
                        row['short_rank'],
                        row['short_member'],
                        row['short_open_interest'],
                        row['short_change_value'],
                        row['source_url'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def get_cffex_latest_trade_dates(self, product_codes=None):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT product_code, MAX(trade_date)
        FROM cffex_member_rankings
        """
        params = []

        if product_codes:
            placeholders = ','.join(['%s'] * len(product_codes))
            query += f" WHERE product_code IN ({placeholders})"
            params.extend(product_codes)

        query += " GROUP BY product_code"

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()
                return {
                    str(product_code).strip().upper(): str(latest_date)
                    for product_code, latest_date in rows
                    if product_code and latest_date
                }

    async def upsert_douyin_emotion_daily(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_douyin_emotion_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['emotion_date'] and row['video_id'] and row['video_url']
        ]
        if not sanitized_rows:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO douyin_index_emotion_daily (
                    emotion_date,
                    video_id,
                    account_id,
                    account_name,
                    video_title,
                    video_url,
                    hs300_emotion,
                    zz500_emotion,
                    zz1000_emotion,
                    sz50_emotion,
                    raw_ocr_text,
                    extraction_method,
                    extraction_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    account_name = VALUES(account_name),
                    video_title = VALUES(video_title),
                    video_url = VALUES(video_url),
                    hs300_emotion = VALUES(hs300_emotion),
                    zz500_emotion = VALUES(zz500_emotion),
                    zz1000_emotion = VALUES(zz1000_emotion),
                    sz50_emotion = VALUES(sz50_emotion),
                    raw_ocr_text = VALUES(raw_ocr_text),
                    extraction_method = VALUES(extraction_method),
                    extraction_status = VALUES(extraction_status),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['emotion_date'],
                        row['video_id'],
                        row['account_id'],
                        row['account_name'],
                        row['video_title'],
                        row['video_url'],
                        row['hs300_emotion'],
                        row['zz500_emotion'],
                        row['zz1000_emotion'],
                        row['sz50_emotion'],
                        row['raw_ocr_text'],
                        row['extraction_method'],
                        row['extraction_status'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def get_douyin_latest_emotion_date(self, account_id='1368194981'):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT MAX(emotion_date)
        FROM douyin_index_emotion_daily
        WHERE account_id = %s
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, [account_id])
                row = await cursor.fetchone()
                if not row or not row[0]:
                    return None
                return str(row[0])

    async def upsert_forex_basic_info(self, basic_rows):
        if not basic_rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        deduped_rows = {}
        for row in basic_rows:
            symbol_code = str(row.get('symbol_code', '')).strip().upper()
            if not symbol_code:
                continue
            deduped_rows[symbol_code] = (
                symbol_code,
                str(row.get('symbol_name', '')).strip() or None,
                str(row.get('data_source', '')).strip() or 'forex_spot_em',
            )

        rows_to_upsert = list(deduped_rows.values())
        if not rows_to_upsert:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO forex_basic_info (
                    symbol_code,
                    symbol_name,
                    data_source
                ) VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    symbol_name = VALUES(symbol_name),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query_upsert, rows_to_upsert)
                await conn.commit()
                return len(rows_to_upsert)

    async def upsert_etf_basic_info(self, basic_rows):
        if not basic_rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        deduped_rows = {}
        for row in basic_rows:
            etf_code = str(row.get('etf_code', '')).strip()
            if not etf_code:
                continue
            deduped_rows[etf_code] = (
                etf_code,
                str(row.get('etf_name', '')).strip() or None,
            )

        rows_to_upsert = list(deduped_rows.values())
        if not rows_to_upsert:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO etf_basic_info (
                    etf_code,
                    etf_name
                ) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    etf_name = VALUES(etf_name),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query_upsert, rows_to_upsert)
                await conn.commit()
                return len(rows_to_upsert)

    async def batch_forex_daily_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_forex_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['symbol_code'] and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                symbol_code = sanitized_rows[0]['symbol_code']
                trade_dates = [row['trade_date'] for row in sanitized_rows]
                placeholders = ','.join(['%s'] * len(trade_dates))
                query_check = (
                    f"SELECT trade_date FROM forex_daily_data WHERE symbol_code = %s "
                    f"AND trade_date IN ({placeholders})"
                )
                await cursor.execute(query_check, [symbol_code, *trade_dates])
                existing_dates = {str(row[0]) for row in await cursor.fetchall()}

                rows_to_insert = [
                    (
                        row['symbol_code'],
                        row['symbol_name'],
                        row['trade_date'],
                        row['open_price'],
                        row['latest_price'],
                        row['high_price'],
                        row['low_price'],
                        row['amplitude'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                    if row['trade_date'] not in existing_dates
                ]

                if not rows_to_insert:
                    return 0

                query_insert = """
                INSERT INTO forex_daily_data (
                    symbol_code,
                    symbol_name,
                    trade_date,
                    open_price,
                    latest_price,
                    high_price,
                    low_price,
                    amplitude,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
                return len(rows_to_insert)

    async def batch_option_spot_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_option_spot_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['index_type'] and row['contract_symbol'] and row['strike_price'] is not None
        ]
        if not sanitized_rows:
            return 0

        index_type = sanitized_rows[0]['index_type']
        contract_symbol = sanitized_rows[0]['contract_symbol']

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_check = """
                SELECT strike_price
                FROM option_cffex_spot_data
                WHERE index_type = %s AND contract_symbol = %s
                """
                await cursor.execute(query_check, [index_type, contract_symbol])
                existing_strikes = {float(row[0]) for row in await cursor.fetchall() if row[0] is not None}

                rows_to_insert = [
                    (
                        row['index_type'],
                        row['index_name'],
                        row['product_code'],
                        row['contract_symbol'],
                        row['strike_price'],
                        row['call_option_symbol'],
                        row['call_buy_volume'],
                        row['call_buy_price'],
                        row['call_latest_price'],
                        row['call_sell_price'],
                        row['call_sell_volume'],
                        row['call_open_interest'],
                        row['call_change'],
                        row['put_option_symbol'],
                        row['put_buy_volume'],
                        row['put_buy_price'],
                        row['put_latest_price'],
                        row['put_sell_price'],
                        row['put_sell_volume'],
                        row['put_open_interest'],
                        row['put_change'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                    if row['strike_price'] not in existing_strikes
                ]

                if not rows_to_insert:
                    return 0

                query_insert = """
                INSERT INTO option_cffex_spot_data (
                    index_type,
                    index_name,
                    product_code,
                    contract_symbol,
                    strike_price,
                    call_option_symbol,
                    call_buy_volume,
                    call_buy_price,
                    call_latest_price,
                    call_sell_price,
                    call_sell_volume,
                    call_open_interest,
                    call_change,
                    put_option_symbol,
                    put_buy_volume,
                    put_buy_price,
                    put_latest_price,
                    put_sell_price,
                    put_sell_volume,
                    put_open_interest,
                    put_change,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
                return len(rows_to_insert)

    async def batch_option_daily_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_option_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['option_symbol'] and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        option_symbol = sanitized_rows[0]['option_symbol']

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                trade_dates = [row['trade_date'] for row in sanitized_rows]
                placeholders = ','.join(['%s'] * len(trade_dates))
                query_check = (
                    f"SELECT trade_date FROM option_cffex_daily_data WHERE option_symbol = %s "
                    f"AND trade_date IN ({placeholders})"
                )
                await cursor.execute(query_check, [option_symbol, *trade_dates])
                existing_dates = {str(row[0]) for row in await cursor.fetchall()}

                rows_to_insert = [
                    (
                        row['index_type'],
                        row['index_name'],
                        row['product_code'],
                        row['contract_symbol'],
                        row['option_symbol'],
                        row['option_type'],
                        row['strike_price'],
                        row['trade_date'],
                        row['open_price'],
                        row['high_price'],
                        row['low_price'],
                        row['close_price'],
                        row['volume'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                    if row['trade_date'] not in existing_dates
                ]

                if not rows_to_insert:
                    return 0

                query_insert = """
                INSERT INTO option_cffex_daily_data (
                    index_type,
                    index_name,
                    product_code,
                    contract_symbol,
                    option_symbol,
                    option_type,
                    strike_price,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
                return len(rows_to_insert)

    async def upsert_forex_daily_snapshots(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_forex_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['symbol_code'] and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[(row['symbol_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO forex_daily_data (
                    symbol_code,
                    symbol_name,
                    trade_date,
                    open_price,
                    latest_price,
                    high_price,
                    low_price,
                    amplitude,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    symbol_name = VALUES(symbol_name),
                    open_price = VALUES(open_price),
                    latest_price = VALUES(latest_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    amplitude = VALUES(amplitude),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['symbol_code'],
                        row['symbol_name'],
                        row['trade_date'],
                        row['open_price'],
                        row['latest_price'],
                        row['high_price'],
                        row['low_price'],
                        row['amplitude'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def upsert_etf_daily_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_etf_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['etf_code'] and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[(row['etf_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO etf_daily_data (
                    etf_code,
                    etf_name,
                    trade_date,
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
                    pre_close_price,
                    iopv_realtime,
                    discount_rate,
                    volume_ratio,
                    current_hand,
                    bid1_price,
                    ask1_price,
                    outer_volume,
                    inner_volume,
                    latest_share,
                    circulating_market_value,
                    total_market_value,
                    main_net_inflow,
                    main_net_inflow_ratio,
                    extra_large_net_inflow,
                    extra_large_net_inflow_ratio,
                    large_net_inflow,
                    large_net_inflow_ratio,
                    medium_net_inflow,
                    medium_net_inflow_ratio,
                    small_net_inflow,
                    small_net_inflow_ratio,
                    spot_data_date,
                    spot_update_time,
                    data_source,
                    adjust_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    etf_name = VALUES(etf_name),
                    open_price = VALUES(open_price),
                    close_price = VALUES(close_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    volume = VALUES(volume),
                    turnover = VALUES(turnover),
                    amplitude = VALUES(amplitude),
                    price_change_rate = VALUES(price_change_rate),
                    price_change_amount = VALUES(price_change_amount),
                    turnover_rate = VALUES(turnover_rate),
                    pre_close_price = VALUES(pre_close_price),
                    iopv_realtime = VALUES(iopv_realtime),
                    discount_rate = VALUES(discount_rate),
                    volume_ratio = VALUES(volume_ratio),
                    current_hand = VALUES(current_hand),
                    bid1_price = VALUES(bid1_price),
                    ask1_price = VALUES(ask1_price),
                    outer_volume = VALUES(outer_volume),
                    inner_volume = VALUES(inner_volume),
                    latest_share = VALUES(latest_share),
                    circulating_market_value = VALUES(circulating_market_value),
                    total_market_value = VALUES(total_market_value),
                    main_net_inflow = VALUES(main_net_inflow),
                    main_net_inflow_ratio = VALUES(main_net_inflow_ratio),
                    extra_large_net_inflow = VALUES(extra_large_net_inflow),
                    extra_large_net_inflow_ratio = VALUES(extra_large_net_inflow_ratio),
                    large_net_inflow = VALUES(large_net_inflow),
                    large_net_inflow_ratio = VALUES(large_net_inflow_ratio),
                    medium_net_inflow = VALUES(medium_net_inflow),
                    medium_net_inflow_ratio = VALUES(medium_net_inflow_ratio),
                    small_net_inflow = VALUES(small_net_inflow),
                    small_net_inflow_ratio = VALUES(small_net_inflow_ratio),
                    spot_data_date = VALUES(spot_data_date),
                    spot_update_time = VALUES(spot_update_time),
                    data_source = VALUES(data_source),
                    adjust_type = VALUES(adjust_type),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['etf_code'],
                        row['etf_name'],
                        row['trade_date'],
                        row['open_price'],
                        row['close_price'],
                        row['high_price'],
                        row['low_price'],
                        row['volume'],
                        row['turnover'],
                        row['amplitude'],
                        row['price_change_rate'],
                        row['price_change_amount'],
                        row['turnover_rate'],
                        row['pre_close_price'],
                        row['iopv_realtime'],
                        row['discount_rate'],
                        row['volume_ratio'],
                        row['current_hand'],
                        row['bid1_price'],
                        row['ask1_price'],
                        row['outer_volume'],
                        row['inner_volume'],
                        row['latest_share'],
                        row['circulating_market_value'],
                        row['total_market_value'],
                        row['main_net_inflow'],
                        row['main_net_inflow_ratio'],
                        row['extra_large_net_inflow'],
                        row['extra_large_net_inflow_ratio'],
                        row['large_net_inflow'],
                        row['large_net_inflow_ratio'],
                        row['medium_net_inflow'],
                        row['medium_net_inflow_ratio'],
                        row['small_net_inflow'],
                        row['small_net_inflow_ratio'],
                        row['spot_data_date'],
                        row['spot_update_time'],
                        row['data_source'],
                        row['adjust_type'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def get_forex_rows_pending_history_refresh(self, before_trade_date, selected_symbols=None):
        if self.pool is None:
            await self.init_pool()

        normalized_symbols = [
            str(symbol_code).strip().upper()
            for symbol_code in (selected_symbols or [])
            if str(symbol_code).strip()
        ]

        query = """
        SELECT
            symbol_code,
            symbol_name,
            trade_date,
            data_source,
            created_at,
            updated_at
        FROM forex_daily_data
        WHERE trade_date < %s
          AND data_source = 'forex_spot_em'
        """
        params = [before_trade_date]

        if normalized_symbols:
            placeholders = ','.join(['%s'] * len(normalized_symbols))
            query += f" AND symbol_code IN ({placeholders})"
            params.extend(normalized_symbols)

        query += """
        ORDER BY trade_date ASC, symbol_code ASC
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

        return [
            {
                'symbol_code': str(row[0]).strip().upper(),
                'symbol_name': str(row[1]).strip() if row[1] is not None else None,
                'trade_date': str(row[2]),
                'data_source': str(row[3]).strip() if row[3] is not None else None,
                'created_at': row[4],
                'updated_at': row[5],
            }
            for row in rows
        ]

    async def upsert_index_daily_snapshots(self, updates):
        if not updates:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_updates = [self._sanitize_index_daily_update(update) for update in updates]
        sanitized_updates = [
            update for update in sanitized_updates
            if update['index_code'] and update['trade_date']
        ]
        if not sanitized_updates:
            return 0

        deduped_updates = {}
        for update in sanitized_updates:
            deduped_updates[(update['index_code'], update['trade_date'])] = update
        sanitized_updates = list(deduped_updates.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_daily_data (
                    index_code,
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
                    trade_date,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open_price = VALUES(open_price),
                    close_price = VALUES(close_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    volume = VALUES(volume),
                    turnover = VALUES(turnover),
                    amplitude = VALUES(amplitude),
                    price_change_rate = VALUES(price_change_rate),
                    price_change_amount = VALUES(price_change_amount),
                    turnover_rate = VALUES(turnover_rate),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        update['index_code'],
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
                        update['trade_date'],
                        update['data_source'],
                    )
                    for update in sanitized_updates
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_updates)

    async def batch_futures_daily_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_futures_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['symbol'] and row['trade_date'] and row['market']
        ]
        if not sanitized_rows:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                symbol = sanitized_rows[0]['symbol']
                trade_dates = [row['trade_date'] for row in sanitized_rows]
                placeholders = ','.join(['%s'] * len(trade_dates))
                query_check = (
                    f"SELECT trade_date FROM futures_daily_data WHERE symbol = %s "
                    f"AND trade_date IN ({placeholders})"
                )
                await cursor.execute(query_check, [symbol, *trade_dates])
                existing_dates = {str(row[0]) for row in await cursor.fetchall()}

                rows_to_insert = [
                    (
                        row['market'],
                        row['symbol'],
                        row['variety'],
                        row['trade_date'],
                        row['open_price'],
                        row['high_price'],
                        row['low_price'],
                        row['close_price'],
                        row['volume'],
                        row['open_interest'],
                        row['turnover'],
                        row['settle_price'],
                        row['pre_settle_price'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                    if row['trade_date'] not in existing_dates
                ]

                if not rows_to_insert:
                    return 0

                query_insert = """
                INSERT INTO futures_daily_data (
                    market,
                    symbol,
                    variety,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    open_interest,
                    turnover,
                    settle_price,
                    pre_settle_price,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
                return len(rows_to_insert)

    async def batch_excel_emotion_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_excel_emotion_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['emotion_date'] and row['index_name'] and row['emotion_value'] is not None
        ]
        if not sanitized_rows:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                dates = sorted({row['emotion_date'] for row in sanitized_rows})
                index_names = sorted({row['index_name'] for row in sanitized_rows})

                date_placeholders = ','.join(['%s'] * len(dates))
                index_placeholders = ','.join(['%s'] * len(index_names))
                query_existing = (
                    f"SELECT emotion_date, index_name FROM excel_index_emotion_daily "
                    f"WHERE emotion_date IN ({date_placeholders}) AND index_name IN ({index_placeholders})"
                )
                await cursor.execute(query_existing, [*dates, *index_names])
                existing_keys = {
                    (str(emotion_date), str(index_name))
                    for emotion_date, index_name in await cursor.fetchall()
                }

                rows_to_insert = [
                    (
                        row['emotion_date'],
                        row['index_name'],
                        row['emotion_value'],
                        row['source_file'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                    if (row['emotion_date'], row['index_name']) not in existing_keys
                ]

                if not rows_to_insert:
                    return 0

                query_insert = """
                INSERT INTO excel_index_emotion_daily (
                    emotion_date,
                    index_name,
                    emotion_value,
                    source_file,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s)
                """
                await cursor.executemany(query_insert, rows_to_insert)
                await conn.commit()
                return len(rows_to_insert)

    async def upsert_failed_task(self, row):
        if not row:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized = self._sanitize_failed_task_row(row)
        if not sanitized['task_name'] or not sanitized['task_key']:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                INSERT INTO daily_task_failures (
                    task_name,
                    task_stage,
                    task_key,
                    payload_json,
                    error_message,
                    result_status,
                    status,
                    first_failed_at,
                    last_failed_at
                ) VALUES (%s, %s, %s, %s, %s, 'FAILED', 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    payload_json = VALUES(payload_json),
                    error_message = VALUES(error_message),
                    result_status = 'FAILED',
                    status = 'PENDING',
                    last_failed_at = CURRENT_TIMESTAMP,
                    resolved_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.execute(
                    query,
                    (
                        sanitized['task_name'],
                        sanitized['task_stage'],
                        sanitized['task_key'],
                        sanitized['payload_json'],
                        sanitized['error_message'],
                    ),
                )
                await conn.commit()
                return cursor.rowcount

    async def get_pending_failed_tasks(self, task_name=None, limit=None):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT
            id,
            task_name,
            task_stage,
            task_key,
            payload_json,
            error_message,
            result_status,
            retry_count,
            first_failed_at,
            last_failed_at
        FROM daily_task_failures
        WHERE status = 'PENDING'
        """
        params = []
        if task_name:
            query += " AND task_name = %s"
            params.append(str(task_name).strip())
        query += " ORDER BY last_failed_at ASC, id ASC"
        if limit is not None:
            query += " LIMIT %s"
            params.append(int(limit))

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

        result = []
        for row in rows:
            payload_text = row[4] or '{}'
            try:
                payload = json.loads(payload_text)
            except (TypeError, ValueError):
                payload = {}
            result.append({
                'id': row[0],
                'task_name': row[1],
                'task_stage': row[2],
                'task_key': row[3],
                'payload': payload,
                'payload_json': payload_text,
                'error_message': row[5],
                'result_status': row[6],
                'retry_count': int(row[7] or 0),
                'first_failed_at': row[8],
                'last_failed_at': row[9],
            })
        return result

    async def resolve_failed_task_by_identity(self, task_name, task_stage, task_key):
        if self.pool is None:
            await self.init_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                UPDATE daily_task_failures
                SET status = 'RESOLVED',
                    result_status = 'SUCCESS',
                    error_message = NULL,
                    resolved_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE task_name = %s
                  AND task_stage = %s
                  AND task_key = %s
                  AND status = 'PENDING'
                """
                await cursor.execute(query, (task_name, task_stage, task_key))
                await conn.commit()
                return cursor.rowcount

    async def get_option_symbols_missing_trade_date(self, trade_date, exclude_success_task_name=None):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT
            latest.index_type,
            latest.index_name,
            latest.product_code,
            latest.contract_symbol,
            latest.option_symbol,
            latest.option_type,
            latest.strike_price
        FROM option_cffex_daily_data latest
        INNER JOIN (
            SELECT option_symbol, MAX(trade_date) AS latest_trade_date
            FROM option_cffex_daily_data
            GROUP BY option_symbol
        ) snapshot
            ON latest.option_symbol = snapshot.option_symbol
           AND latest.trade_date = snapshot.latest_trade_date
        LEFT JOIN option_cffex_daily_data target
            ON target.option_symbol = latest.option_symbol
           AND target.trade_date = %s
        """
        params = [trade_date]
        if exclude_success_task_name:
            query += """
        LEFT JOIN daily_task_failures success_marker
            ON success_marker.task_name = %s
           AND success_marker.task_stage = 'daily'
           AND success_marker.task_key = CONCAT(%s, ':', latest.option_symbol)
           AND success_marker.result_status = 'SUCCESS'
           AND success_marker.status = 'RESOLVED'
            """
            params.extend([str(exclude_success_task_name).strip(), trade_date])

        query += """
        WHERE target.option_symbol IS NULL
        """
        if exclude_success_task_name:
            query += " AND success_marker.id IS NULL"

        query += """
        ORDER BY latest.index_type, latest.contract_symbol, latest.option_symbol
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

        return [
            {
                'index_type': str(row[0]).strip(),
                'index_name': str(row[1]).strip() if row[1] is not None else None,
                'product_code': str(row[2]).strip() if row[2] is not None else None,
                'contract_symbol': str(row[3]).strip() if row[3] is not None else None,
                'option_symbol': str(row[4]).strip(),
                'option_type': str(row[5]).strip() if row[5] is not None else None,
                'strike_price': float(row[6]) if row[6] is not None else None,
            }
            for row in rows
        ]

    async def get_existing_stock_codes_on_date(self, trade_date, stock_codes):
        if self.pool is None:
            await self.init_pool()

        normalized_codes = [
            str(stock_code).strip()
            for stock_code in stock_codes
            if str(stock_code).strip()
        ]
        if not normalized_codes:
            return set()

        placeholders = ','.join(['%s'] * len(normalized_codes))
        query = (
            f"SELECT stock_code FROM stock_data "
            f"WHERE date = %s AND stock_code IN ({placeholders})"
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, [trade_date, *normalized_codes])
                rows = await cursor.fetchall()

        return {str(row[0]).strip() for row in rows if row and row[0] is not None}

    async def get_etf_codes_missing_hist_data(self, selected_codes=None, exclude_success_task_name=None):
        if self.pool is None:
            await self.init_pool()

        normalized_codes = [
            str(etf_code).strip()
            for etf_code in (selected_codes or [])
            if str(etf_code).strip()
        ]

        query = """
        SELECT
            basic.etf_code,
            basic.etf_name
        FROM etf_basic_info basic
        LEFT JOIN (
            SELECT etf_code
            FROM etf_daily_data
            WHERE data_source = 'fund_etf_hist_em'
            GROUP BY etf_code
        ) hist
            ON hist.etf_code = basic.etf_code
        """
        params = []

        if exclude_success_task_name:
            query += """
        LEFT JOIN daily_task_failures success_marker
            ON success_marker.task_name = %s
           AND success_marker.task_stage = 'history'
           AND success_marker.task_key = basic.etf_code
           AND success_marker.result_status = 'SUCCESS'
           AND success_marker.status = 'RESOLVED'
            """
            params.append(str(exclude_success_task_name).strip())

        query += """
        WHERE hist.etf_code IS NULL
        """

        if exclude_success_task_name:
            query += " AND success_marker.id IS NULL"

        if normalized_codes:
            placeholders = ','.join(['%s'] * len(normalized_codes))
            query += f" AND basic.etf_code IN ({placeholders})"
            params.extend(normalized_codes)

        query += " ORDER BY basic.etf_code"

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

        return [
            {
                'etf_code': str(row[0]).strip(),
                'etf_name': str(row[1]).strip() if row[1] is not None else None,
            }
            for row in rows
            if row and row[0] is not None
        ]

    async def get_pending_etf_backfill_failures(self, selected_codes=None):
        failed_tasks = await self.get_pending_failed_tasks(task_name='etf_backfill_history')
        normalized_codes = {
            str(etf_code).strip()
            for etf_code in (selected_codes or [])
            if str(etf_code).strip()
        }
        if not normalized_codes:
            return failed_tasks
        return [
            task for task in failed_tasks
            if str(task.get('task_key', '')).strip() in normalized_codes
        ]

    async def upsert_success_task(self, row):
        if not row:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized = self._sanitize_failed_task_row(row)
        if not sanitized['task_name'] or not sanitized['task_key']:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                INSERT INTO daily_task_failures (
                    task_name,
                    task_stage,
                    task_key,
                    payload_json,
                    error_message,
                    result_status,
                    status,
                    first_failed_at,
                    last_failed_at,
                    resolved_at
                ) VALUES (%s, %s, %s, %s, NULL, 'SUCCESS', 'RESOLVED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    payload_json = VALUES(payload_json),
                    error_message = NULL,
                    result_status = 'SUCCESS',
                    status = 'RESOLVED',
                    resolved_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.execute(
                    query,
                    (
                        sanitized['task_name'],
                        sanitized['task_stage'],
                        sanitized['task_key'],
                        sanitized['payload_json'],
                    ),
                )
                await conn.commit()
                return cursor.rowcount

    async def mark_failed_task_retry_result(self, task_id, success, error_message=None):
        if self.pool is None:
            await self.init_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                if success:
                    query = """
                    UPDATE daily_task_failures
                    SET retry_count = retry_count + 1,
                        last_retry_at = CURRENT_TIMESTAMP,
                        result_status = 'SUCCESS',
                        error_message = NULL,
                        status = 'RESOLVED',
                        resolved_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """
                    await cursor.execute(query, (task_id,))
                else:
                    query = """
                    UPDATE daily_task_failures
                    SET retry_count = retry_count + 1,
                        last_retry_at = CURRENT_TIMESTAMP,
                        last_failed_at = CURRENT_TIMESTAMP,
                        result_status = 'FAILED',
                        error_message = %s,
                        status = 'PENDING',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """
                    await cursor.execute(query, (str(error_message or '').strip() or None, task_id))
                await conn.commit()
                return cursor.rowcount
