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
        'emotion_value': 9999999999.9999,
        'main_basis': 9999999999.9999,
        'month_basis': 9999999999.9999,
        'breadth_up_pct': 9999999999.9999,
    }

    def __init__(self):
        self.db_info = self.load_db_info()
        self.session_time_zone = str(self.db_info.get('timezone', '+08:00')).strip() or '+08:00'
        self.pool = None
        self._stock_qfq_change_columns_ready = False

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

    def _serialize_json_field(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    def _sanitize_stock_info_all_row(self, row):
        sanitized = dict(row)
        sanitized['stock_code'] = str(row.get('stock_code', '')).strip()
        sanitized['prefixed_code'] = str(row.get('prefixed_code', '')).strip().lower()
        sanitized['exchange'] = str(row.get('exchange', '')).strip().upper() or None
        sanitized['market_prefix'] = str(row.get('market_prefix', '')).strip().lower() or None
        sanitized['board'] = str(row.get('board', '')).strip() or None
        sanitized['security_type'] = str(row.get('security_type', '')).strip().upper() or None
        sanitized['stock_name'] = str(row.get('stock_name', '')).strip() or None
        sanitized['security_full_name'] = str(row.get('security_full_name', '')).strip() or None
        sanitized['company_abbr'] = str(row.get('company_abbr', '')).strip() or None
        sanitized['company_full_name'] = str(row.get('company_full_name', '')).strip() or None
        list_date = row.get('list_date')
        sanitized['list_date'] = str(list_date).split(' ')[0].strip() if list_date else None
        sanitized['industry'] = str(row.get('industry', '')).strip() or None
        sanitized['region'] = str(row.get('region', '')).strip() or None
        sanitized['total_share_capital'] = self._normalize_numeric('total_share_capital', row.get('total_share_capital'))
        sanitized['circulating_share_capital'] = self._normalize_numeric(
            'circulating_share_capital',
            row.get('circulating_share_capital'),
        )
        sanitized['source_variants_json'] = self._serialize_json_field(row.get('source_variants_json'))
        sanitized['raw_records_json'] = self._serialize_json_field(row.get('raw_records_json'))
        return sanitized

    def _sanitize_stock_daily_data_row(self, row):
        sanitized = dict(row)
        sanitized['stock_code'] = str(row.get('stock_code', '')).strip()
        sanitized['prefixed_code'] = str(row.get('prefixed_code', '')).strip().lower()
        sanitized['stock_name'] = str(row.get('stock_name', '')).strip() or None
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        for field in [
            'open_price', 'close_price', 'high_price', 'low_price', 'latest_price',
            'pre_close_price', 'buy_price', 'sell_price', 'price_change_amount',
            'price_change_rate', 'volume', 'turnover_amount'
        ]:
            sanitized[field] = self._normalize_numeric(field, row.get(field))

        snapshot_time = row.get('snapshot_time')
        if hasattr(snapshot_time, 'to_pydatetime'):
            snapshot_time = snapshot_time.to_pydatetime()
        if hasattr(snapshot_time, 'tzinfo') and getattr(snapshot_time, 'tzinfo', None) is not None:
            snapshot_time = snapshot_time.replace(tzinfo=None)
        sanitized['snapshot_time'] = snapshot_time or None
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'stock_zh_a_spot'
        return sanitized

    def _sanitize_stock_qfq_daily_row(self, row):
        sanitized = dict(row)
        sanitized['stock_code'] = str(row.get('stock_code', '')).strip()
        sanitized['prefixed_code'] = str(row.get('prefixed_code', '')).strip().lower()
        sanitized['stock_name'] = str(row.get('stock_name', '')).strip() or None
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['open_price'] = self._normalize_numeric('open_price', row.get('open_price'))
        sanitized['close_price'] = self._normalize_numeric('close_price', row.get('close_price'))
        sanitized['high_price'] = self._normalize_numeric('high_price', row.get('high_price'))
        sanitized['low_price'] = self._normalize_numeric('low_price', row.get('low_price'))
        sanitized['price_change_amount'] = self._normalize_numeric('price_change_amount', row.get('price_change_amount'))
        sanitized['price_change_rate'] = self._normalize_numeric('price_change_rate', row.get('price_change_rate'))
        sanitized['volume'] = self._normalize_numeric('volume', row.get('volume'))
        sanitized['turnover_amount'] = self._normalize_numeric('turnover_amount', row.get('turnover_amount'))
        sanitized['outstanding_share'] = self._normalize_numeric('outstanding_share', row.get('outstanding_share'))
        sanitized['turnover_rate'] = self._normalize_numeric('turnover_rate', row.get('turnover_rate'))
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'stock_zh_a_daily_qfq'
        request_start_date = row.get('request_start_date')
        request_end_date = row.get('request_end_date')
        sanitized['request_start_date'] = str(request_start_date).split(' ')[0].strip() if request_start_date else None
        sanitized['request_end_date'] = str(request_end_date).split(' ')[0].strip() if request_end_date else None
        sanitized['refresh_batch_id'] = str(row.get('refresh_batch_id', '')).strip() or None
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
        sanitized['sina_symbol'] = str(row.get('sina_symbol', '')).strip().lower() or None
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

    def _sanitize_quant_index_dashboard_row(self, row):
        sanitized = dict(row)
        sanitized['trade_date'] = str(row.get('trade_date', '')).strip()
        sanitized['index_code'] = str(row.get('index_code', '')).strip()
        sanitized['index_name'] = str(row.get('index_name', '')).strip()
        emotion_value = self._normalize_numeric('emotion_value', row.get('emotion_value'))
        main_basis = self._normalize_numeric('main_basis', row.get('main_basis'))
        month_basis = self._normalize_numeric('month_basis', row.get('month_basis'))
        sanitized['breadth_up_count'] = int(row.get('breadth_up_count') or 0)
        sanitized['breadth_total_count'] = int(row.get('breadth_total_count') or 0)

        breadth_up_pct = self._normalize_numeric('breadth_up_pct', row.get('breadth_up_pct'))
        if breadth_up_pct is not None:
            breadth_up_pct = max(0.0, min(100.0, float(breadth_up_pct)))

        # Keep the values aligned with DECIMAL(18, 6) to avoid SQL truncation warnings.
        sanitized['emotion_value'] = round(float(emotion_value), 6) if emotion_value is not None else None
        sanitized['main_basis'] = round(float(main_basis), 6) if main_basis is not None else None
        sanitized['month_basis'] = round(float(month_basis), 6) if month_basis is not None else None
        sanitized['breadth_up_pct'] = round(float(breadth_up_pct), 6) if breadth_up_pct is not None else None
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

    def _sanitize_option_rtj_daily_row(self, row):
        sanitized = dict(row)
        sanitized['index_type'] = str(row.get('index_type', '')).strip().upper()
        sanitized['index_name'] = str(row.get('index_name', '')).strip() or None
        sanitized['product_prefix'] = str(row.get('product_prefix', '')).strip().upper() or None
        sanitized['contract_code'] = str(row.get('contract_code', '')).strip().upper()
        sanitized['contract_month'] = str(row.get('contract_month', '')).strip() or None
        sanitized['option_type'] = str(row.get('option_type', '')).strip().upper() or None
        sanitized['strike_price'] = self._normalize_numeric('strike_price', row.get('strike_price'))
        sanitized['trade_date'] = str(row.get('trade_date', '')).strip()
        sanitized['open_price'] = self._normalize_numeric('open_price', row.get('open_price'))
        sanitized['high_price'] = self._normalize_numeric('high_price', row.get('high_price'))
        sanitized['low_price'] = self._normalize_numeric('low_price', row.get('low_price'))
        sanitized['close_price'] = self._normalize_numeric('close_price', row.get('close_price'))
        sanitized['settle_price'] = self._normalize_numeric('settle_price', row.get('settle_price'))
        sanitized['pre_settle_price'] = self._normalize_numeric('pre_settle_price', row.get('pre_settle_price'))
        sanitized['price_change_close'] = self._normalize_numeric('price_change_amount', row.get('price_change_close'))
        sanitized['price_change_settle'] = self._normalize_numeric('price_change_amount', row.get('price_change_settle'))
        sanitized['volume'] = self._normalize_numeric('volume', row.get('volume'))
        sanitized['turnover'] = self._normalize_numeric('turnover', row.get('turnover'))
        sanitized['open_interest'] = self._normalize_numeric('open_interest', row.get('open_interest'))
        sanitized['open_interest_change'] = self._normalize_numeric('open_interest', row.get('open_interest_change'))
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'cffex_rtj'
        sanitized['source_url'] = str(row.get('source_url', '')).strip() or 'http://www.cffex.com.cn/rtj/'
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
            init_command=f"SET time_zone = '{self.session_time_zone}'",
        )

    async def close(self):
        if self.pool is None:
            return
        self.pool.close()
        await self.pool.wait_closed()
        self.pool = None

    async def ensure_stock_qfq_change_columns(self):
        if self._stock_qfq_change_columns_ready:
            return

        if self.pool is None:
            await self.init_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND table_name = 'stock_qfq_daily_data'
                      AND column_name IN ('price_change_amount', 'price_change_rate')
                    """
                )
                existing_columns = {str(row[0]).strip().lower() for row in await cursor.fetchall()}

                alter_clauses = []
                if 'price_change_amount' not in existing_columns:
                    alter_clauses.append("ADD COLUMN price_change_amount DECIMAL(18, 4) NULL AFTER low_price")
                if 'price_change_rate' not in existing_columns:
                    after_column = 'price_change_amount' if ('price_change_amount' in existing_columns or alter_clauses) else 'low_price'
                    alter_clauses.append(
                        f"ADD COLUMN price_change_rate DECIMAL(18, 4) NULL AFTER {after_column}"
                    )

                if alter_clauses:
                    await cursor.execute(
                        "ALTER TABLE stock_qfq_daily_data " + ", ".join(alter_clauses)
                    )
                    await conn.commit()

        self._stock_qfq_change_columns_ready = True

    async def upsert_stock_info_all(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_stock_info_all_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['stock_code'] and row['prefixed_code']
        ]
        if not sanitized_rows:
            return 0

        deduped = {}
        for row in sanitized_rows:
            deduped[row['prefixed_code']] = row
        sanitized_rows = list(deduped.values())

        values = [
            (
                row['stock_code'],
                row['prefixed_code'],
                row['exchange'],
                row['market_prefix'],
                row['board'],
                row['security_type'],
                row['stock_name'],
                row['security_full_name'],
                row['company_abbr'],
                row['company_full_name'],
                row['list_date'],
                row['industry'],
                row['region'],
                row['total_share_capital'],
                row['circulating_share_capital'],
                row['source_variants_json'],
                row['raw_records_json'],
            )
            for row in sanitized_rows
        ]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                INSERT INTO stock_info_all (
                    stock_code,
                    prefixed_code,
                    exchange,
                    market_prefix,
                    board,
                    security_type,
                    stock_name,
                    security_full_name,
                    company_abbr,
                    company_full_name,
                    list_date,
                    industry,
                    region,
                    total_share_capital,
                    circulating_share_capital,
                    source_variants_json,
                    raw_records_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    stock_code = VALUES(stock_code),
                    exchange = VALUES(exchange),
                    market_prefix = VALUES(market_prefix),
                    board = VALUES(board),
                    security_type = VALUES(security_type),
                    stock_name = VALUES(stock_name),
                    security_full_name = VALUES(security_full_name),
                    company_abbr = VALUES(company_abbr),
                    company_full_name = VALUES(company_full_name),
                    list_date = VALUES(list_date),
                    industry = VALUES(industry),
                    region = VALUES(region),
                    total_share_capital = VALUES(total_share_capital),
                    circulating_share_capital = VALUES(circulating_share_capital),
                    source_variants_json = VALUES(source_variants_json),
                    raw_records_json = VALUES(raw_records_json),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query, values)
                await conn.commit()
                return len(sanitized_rows)

    async def get_stock_info_rows_by_codes(self, stock_codes):
        if self.pool is None:
            await self.init_pool()

        normalized_codes = [
            str(stock_code).strip()
            for stock_code in (stock_codes or [])
            if str(stock_code).strip()
        ]
        if not normalized_codes:
            return []

        placeholders = ','.join(['%s'] * len(normalized_codes))
        query = (
            f"SELECT stock_code, prefixed_code, exchange, market_prefix, board, security_type, "
            f"stock_name, security_full_name, company_abbr, company_full_name, list_date, "
            f"industry, region, total_share_capital, circulating_share_capital "
            f"FROM stock_info_all WHERE stock_code IN ({placeholders}) ORDER BY stock_code ASC"
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, normalized_codes)
                return list(await cursor.fetchall())

    async def get_all_stock_info_rows(self):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT
            stock_code,
            prefixed_code,
            exchange,
            market_prefix,
            board,
            security_type,
            stock_name,
            security_full_name,
            company_abbr,
            company_full_name,
            list_date,
            industry,
            region,
            total_share_capital,
            circulating_share_capital
        FROM stock_info_all
        ORDER BY prefixed_code ASC
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                return list(await cursor.fetchall())

    async def delete_stock_info_all_by_prefixed_codes(self, prefixed_codes, chunk_size=500):
        if self.pool is None:
            await self.init_pool()

        normalized_codes = sorted(
            {
                str(prefixed_code).strip().lower()
                for prefixed_code in (prefixed_codes or [])
                if str(prefixed_code).strip()
            }
        )
        if not normalized_codes:
            return 0

        deleted_rows = 0
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for offset in range(0, len(normalized_codes), chunk_size):
                    chunk = normalized_codes[offset:offset + chunk_size]
                    placeholders = ','.join(['%s'] * len(chunk))
                    query = f"DELETE FROM stock_info_all WHERE prefixed_code IN ({placeholders})"
                    await cursor.execute(query, chunk)
                    deleted_rows += cursor.rowcount
                await conn.commit()
        return deleted_rows

    async def upsert_stock_daily_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_stock_daily_data_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['stock_code'] and row['prefixed_code'] and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        deduped = {}
        for row in sanitized_rows:
            deduped[(row['prefixed_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped.values())

        values = [
            (
                row['stock_code'],
                row['prefixed_code'],
                row['stock_name'],
                row['trade_date'],
                row['open_price'],
                row['close_price'],
                row['high_price'],
                row['low_price'],
                row['latest_price'],
                row['pre_close_price'],
                row['buy_price'],
                row['sell_price'],
                row['price_change_amount'],
                row['price_change_rate'],
                row['volume'],
                row['turnover_amount'],
                row['data_source'],
                row['snapshot_time'],
            )
            for row in sanitized_rows
        ]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                INSERT INTO stock_daily_data (
                    stock_code,
                    prefixed_code,
                    stock_name,
                    trade_date,
                    open_price,
                    close_price,
                    high_price,
                    low_price,
                    latest_price,
                    pre_close_price,
                    buy_price,
                    sell_price,
                    price_change_amount,
                    price_change_rate,
                    volume,
                    turnover_amount,
                    data_source,
                    snapshot_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    stock_code = VALUES(stock_code),
                    stock_name = VALUES(stock_name),
                    open_price = VALUES(open_price),
                    close_price = VALUES(close_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    latest_price = VALUES(latest_price),
                    pre_close_price = VALUES(pre_close_price),
                    buy_price = VALUES(buy_price),
                    sell_price = VALUES(sell_price),
                    price_change_amount = VALUES(price_change_amount),
                    price_change_rate = VALUES(price_change_rate),
                    volume = VALUES(volume),
                    turnover_amount = VALUES(turnover_amount),
                    data_source = VALUES(data_source),
                    snapshot_time = VALUES(snapshot_time),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query, values)
                await conn.commit()
                return len(sanitized_rows)

    async def get_stock_daily_prefixed_codes_by_date(self, trade_date):
        if self.pool is None:
            await self.init_pool()

        normalized_trade_date = str(trade_date or '').split(' ')[0].strip()
        if not normalized_trade_date:
            return []

        query = """
        SELECT prefixed_code
        FROM stock_daily_data
        WHERE trade_date = %s
        ORDER BY prefixed_code ASC
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, (normalized_trade_date,))
                rows = await cursor.fetchall()
                return [str(row[0]).strip().lower() for row in rows if row and row[0]]

    async def get_stock_daily_hist_prefixed_codes(self):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT DISTINCT prefixed_code
        FROM stock_daily_data
        WHERE data_source = 'stock_zh_a_hist_tx'
        ORDER BY prefixed_code ASC
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()
                return [str(row[0]).strip().lower() for row in rows if row and row[0]]

    async def delete_stock_daily_data_by_trade_date_and_prefixed_codes(self, trade_date, prefixed_codes, chunk_size=500):
        if self.pool is None:
            await self.init_pool()

        normalized_trade_date = str(trade_date or '').split(' ')[0].strip()
        normalized_codes = sorted(
            {
                str(prefixed_code).strip().lower()
                for prefixed_code in (prefixed_codes or [])
                if str(prefixed_code).strip()
            }
        )
        if not normalized_trade_date or not normalized_codes:
            return 0

        deleted_rows = 0
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for offset in range(0, len(normalized_codes), chunk_size):
                    chunk = normalized_codes[offset:offset + chunk_size]
                    placeholders = ','.join(['%s'] * len(chunk))
                    query = (
                        f"DELETE FROM stock_daily_data "
                        f"WHERE trade_date = %s AND prefixed_code IN ({placeholders})"
                    )
                    await cursor.execute(query, [normalized_trade_date, *chunk])
                    deleted_rows += cursor.rowcount
                await conn.commit()
        return deleted_rows

    async def get_stock_qfq_request_window(self, prefixed_code):
        if self.pool is None:
            await self.init_pool()

        normalized_prefixed_code = str(prefixed_code or '').strip().lower()
        if not normalized_prefixed_code:
            return None

        query = """
        SELECT prefixed_code, request_start_date, request_end_date, refresh_batch_id
        FROM stock_qfq_daily_data
        WHERE prefixed_code = %s
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (normalized_prefixed_code,))
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    'prefixed_code': row.get('prefixed_code'),
                    'request_start_date': str(row.get('request_start_date')).split(' ')[0] if row.get('request_start_date') else None,
                    'request_end_date': str(row.get('request_end_date')).split(' ')[0] if row.get('request_end_date') else None,
                    'refresh_batch_id': row.get('refresh_batch_id'),
                }

    async def replace_stock_qfq_daily_data(self, prefixed_code, rows):
        if self.pool is None:
            await self.init_pool()
        await self.ensure_stock_qfq_change_columns()

        normalized_prefixed_code = str(prefixed_code or '').strip().lower()
        sanitized_rows = [self._sanitize_stock_qfq_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['stock_code'] and row['prefixed_code'] == normalized_prefixed_code and row['trade_date']
        ]
        if not normalized_prefixed_code:
            return 0, 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM stock_qfq_daily_data WHERE prefixed_code = %s",
                    (normalized_prefixed_code,),
                )
                deleted_rows = int((await cursor.fetchone())[0] or 0)
                await cursor.execute(
                    "DELETE FROM stock_qfq_daily_data WHERE prefixed_code = %s",
                    (normalized_prefixed_code,),
                )

                written_rows = 0
                if sanitized_rows:
                    values = [
                        (
                            row['stock_code'],
                            row['prefixed_code'],
                            row['stock_name'],
                            row['trade_date'],
                            row['open_price'],
                            row['close_price'],
                            row['high_price'],
                            row['low_price'],
                            row['price_change_amount'],
                            row['price_change_rate'],
                            row['volume'],
                            row['turnover_amount'],
                            row['outstanding_share'],
                            row['turnover_rate'],
                            row['data_source'],
                            row['request_start_date'],
                            row['request_end_date'],
                            row['refresh_batch_id'],
                        )
                        for row in sanitized_rows
                    ]
                    query = """
                    INSERT INTO stock_qfq_daily_data (
                        stock_code,
                        prefixed_code,
                        stock_name,
                        trade_date,
                        open_price,
                        close_price,
                        high_price,
                        low_price,
                        price_change_amount,
                        price_change_rate,
                        volume,
                        turnover_amount,
                        outstanding_share,
                        turnover_rate,
                        data_source,
                        request_start_date,
                        request_end_date,
                        refresh_batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    await cursor.executemany(query, values)
                    written_rows = len(values)

                await conn.commit()
                return deleted_rows, written_rows

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
                str(row.get('sina_symbol', '')).strip().lower() or None,
            )

        rows_to_upsert = list(deduped_rows.values())
        if not rows_to_upsert:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO etf_basic_info_sina (
                    etf_code,
                    etf_name,
                    sina_symbol
                ) VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    etf_name = VALUES(etf_name),
                    sina_symbol = VALUES(sina_symbol),
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

    async def batch_option_rtj_daily_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_option_rtj_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['contract_code'] and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[(row['contract_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_insert = """
                INSERT IGNORE INTO option_cffex_rtj_daily_data (
                    index_type,
                    index_name,
                    product_prefix,
                    contract_code,
                    contract_month,
                    option_type,
                    strike_price,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    settle_price,
                    pre_settle_price,
                    price_change_close,
                    price_change_settle,
                    volume,
                    turnover,
                    open_interest,
                    open_interest_change,
                    data_source,
                    source_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = [
                    (
                        row['index_type'],
                        row['index_name'],
                        row['product_prefix'],
                        row['contract_code'],
                        row['contract_month'],
                        row['option_type'],
                        row['strike_price'],
                        row['trade_date'],
                        row['open_price'],
                        row['high_price'],
                        row['low_price'],
                        row['close_price'],
                        row['settle_price'],
                        row['pre_settle_price'],
                        row['price_change_close'],
                        row['price_change_settle'],
                        row['volume'],
                        row['turnover'],
                        row['open_interest'],
                        row['open_interest_change'],
                        row['data_source'],
                        row['source_url'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_insert, values)
                await conn.commit()
                return cursor.rowcount

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
                INSERT INTO etf_daily_data_sina (
                    etf_code,
                    etf_name,
                    sina_symbol,
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
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    etf_name = VALUES(etf_name),
                    sina_symbol = VALUES(sina_symbol),
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
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['etf_code'],
                        row['etf_name'],
                        row['sina_symbol'],
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
                        row['data_source'],
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

    async def get_index_codes_by_names(self, index_names):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return {}

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT index_name, index_code "
            f"FROM index_basic_info "
            f"WHERE index_name IN ({placeholders}) "
            f"ORDER BY id ASC"
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, normalized_names)
                rows = await cursor.fetchall()

        code_by_name = {}
        for index_name, index_code in rows:
            normalized_name = str(index_name or '').strip()
            normalized_code = str(index_code or '').strip()
            if normalized_name and normalized_code and normalized_name not in code_by_name:
                code_by_name[normalized_name] = normalized_code
        return code_by_name

    async def get_latest_quant_index_trade_date(self, index_names):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return None

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT MAX(d.trade_date) "
            f"FROM index_daily_data d "
            f"INNER JOIN index_basic_info b ON b.index_code = d.index_code "
            f"WHERE b.index_name IN ({placeholders})"
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, normalized_names)
                row = await cursor.fetchone()

        if not row or row[0] is None:
            return None
        return str(row[0]).split(' ')[0]

    async def get_latest_quant_index_trade_dates(self, index_names, limit=10):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return []

        try:
            normalized_limit = int(limit)
        except (TypeError, ValueError):
            normalized_limit = 10
        normalized_limit = max(1, normalized_limit)

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT DISTINCT d.trade_date "
            f"FROM index_daily_data d "
            f"INNER JOIN index_basic_info b ON b.index_code = d.index_code "
            f"WHERE b.index_name IN ({placeholders}) "
            f"ORDER BY d.trade_date DESC "
            f"LIMIT %s"
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, [*normalized_names, normalized_limit])
                rows = await cursor.fetchall()

        return [str(row[0]).split(' ')[0] for row in rows if row and row[0] is not None]

    async def get_quant_index_dashboard_trade_dates(self, index_names, start_date=None, end_date=None):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return []

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT DISTINCT d.trade_date "
            f"FROM index_daily_data d "
            f"INNER JOIN index_basic_info b ON b.index_code = d.index_code "
            f"WHERE b.index_name IN ({placeholders})"
        )
        params = [*normalized_names]
        if start_date:
            query += " AND d.trade_date >= %s"
            params.append(str(start_date))
        if end_date:
            query += " AND d.trade_date <= %s"
            params.append(str(end_date))
        query += " ORDER BY d.trade_date ASC"

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

        return [str(row[0]).split(' ')[0] for row in rows if row and row[0] is not None]

    async def get_quant_index_dashboard_index_closes(self, index_names, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return []

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT b.index_name, d.trade_date, d.close_price "
            f"FROM index_daily_data d "
            f"INNER JOIN index_basic_info b ON b.index_code = d.index_code "
            f"WHERE b.index_name IN ({placeholders}) "
            f"AND d.trade_date BETWEEN %s AND %s "
            f"AND d.close_price IS NOT NULL "
            f"ORDER BY d.trade_date ASC, b.index_name ASC"
        )
        params = [*normalized_names, str(start_date), str(end_date)]

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def get_quant_index_dashboard_emotions(self, index_names, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return []

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT emotion_date, index_name, emotion_value "
            f"FROM excel_index_emotion_daily "
            f"WHERE index_name IN ({placeholders}) "
            f"AND emotion_date BETWEEN %s AND %s "
            f"AND emotion_value IS NOT NULL "
            f"ORDER BY emotion_date ASC, index_name ASC"
        )
        params = [*normalized_names, str(start_date), str(end_date)]

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def get_quant_index_dashboard_futures_closes(self, symbols, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        normalized_symbols = [
            str(symbol).strip().upper()
            for symbol in (symbols or [])
            if str(symbol).strip()
        ]
        if not normalized_symbols:
            return []

        placeholders = ','.join(['%s'] * len(normalized_symbols))
        query = (
            f"SELECT trade_date, symbol, close_price, data_source "
            f"FROM futures_daily_data "
            f"WHERE symbol IN ({placeholders}) "
            f"AND trade_date BETWEEN %s AND %s "
            f"AND data_source IN ('get_futures_daily_derived', 'futures_hist_em') "
            f"AND close_price IS NOT NULL "
            f"ORDER BY trade_date ASC, symbol ASC"
        )
        params = [*normalized_symbols, str(start_date), str(end_date)]

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def get_quant_index_dashboard_breadth(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT
            breadth.trade_date,
            SUM(
                CASE
                    WHEN breadth.derived_prev_close IS NOT NULL
                     AND breadth.derived_close IS NOT NULL
                     AND breadth.derived_close > breadth.derived_prev_close
                    THEN 1 ELSE 0
                END
            ) AS breadth_up_count,
            SUM(
                CASE
                    WHEN breadth.derived_prev_close IS NOT NULL
                     AND breadth.derived_close IS NOT NULL
                    THEN 1 ELSE 0
                END
            ) AS breadth_total_count
        FROM (
            SELECT
                current_rows.prefixed_code,
                current_rows.trade_date,
                COALESCE(current_rows.latest_price, current_rows.close_price) AS derived_close,
                COALESCE(
                    current_rows.pre_close_price,
                    (
                        SELECT COALESCE(prev_rows.latest_price, prev_rows.close_price)
                        FROM stock_daily_data prev_rows
                        WHERE prev_rows.prefixed_code = current_rows.prefixed_code
                          AND prev_rows.trade_date < current_rows.trade_date
                        ORDER BY prev_rows.trade_date DESC
                        LIMIT 1
                    )
                ) AS derived_prev_close
            FROM stock_daily_data current_rows
            WHERE current_rows.trade_date BETWEEN %s AND %s
        ) breadth
        GROUP BY breadth.trade_date
        ORDER BY breadth.trade_date ASC
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def upsert_quant_index_dashboard_daily(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_quant_index_dashboard_row(row) for row in rows]
        deduped_rows = {}
        for row in sanitized_rows:
            if not (row['trade_date'] and row['index_code'] and row['index_name']):
                continue
            deduped_rows[(row['index_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped_rows.values())
        if not sanitized_rows:
            return 0

        values = [
            (
                row['trade_date'],
                row['index_code'],
                row['index_name'],
                row['emotion_value'] if row['emotion_value'] is not None else 50,
                row['main_basis'] if row['main_basis'] is not None else 0,
                row['month_basis'] if row['month_basis'] is not None else 0,
                row['breadth_up_count'],
                row['breadth_total_count'],
                row['breadth_up_pct'] if row['breadth_up_pct'] is not None else 0,
            )
            for row in sanitized_rows
        ]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                INSERT INTO quant_index_dashboard_daily (
                    trade_date,
                    index_code,
                    index_name,
                    emotion_value,
                    main_basis,
                    month_basis,
                    breadth_up_count,
                    breadth_total_count,
                    breadth_up_pct
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    index_name = VALUES(index_name),
                    emotion_value = VALUES(emotion_value),
                    main_basis = VALUES(main_basis),
                    month_basis = VALUES(month_basis),
                    breadth_up_count = VALUES(breadth_up_count),
                    breadth_total_count = VALUES(breadth_total_count),
                    breadth_up_pct = VALUES(breadth_up_pct),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query, values)
                await conn.commit()
                return len(sanitized_rows)

    async def batch_futures_daily_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_futures_daily_row(row) for row in rows]
        deduped_rows = {}
        for row in sanitized_rows:
            if not (row['symbol'] and row['trade_date'] and row['market']):
                continue
            deduped_rows[(row['symbol'], row['trade_date'], row['data_source'])] = row
        sanitized_rows = list(deduped_rows.values())
        if not sanitized_rows:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
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
                ]

                if not rows_to_insert:
                    return 0

                query_insert = """
                INSERT IGNORE INTO futures_daily_data (
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
                return cursor.rowcount

    async def batch_excel_emotion_data(self, rows):
        if not rows:
            return {
                'parsed_rows': 0,
                'inserted_rows': 0,
                'updated_rows': 0,
                'affected_dates': [],
            }

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_excel_emotion_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['emotion_date'] and row['index_name'] and row['emotion_value'] is not None
        ]
        if not sanitized_rows:
            return {
                'parsed_rows': 0,
                'inserted_rows': 0,
                'updated_rows': 0,
                'affected_dates': [],
            }

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[(row['emotion_date'], row['index_name'])] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                dates = sorted({row['emotion_date'] for row in sanitized_rows})
                index_names = sorted({row['index_name'] for row in sanitized_rows})

                date_placeholders = ','.join(['%s'] * len(dates))
                index_placeholders = ','.join(['%s'] * len(index_names))
                query_existing = (
                    f"SELECT emotion_date, index_name, emotion_value, source_file, data_source "
                    f"FROM excel_index_emotion_daily "
                    f"WHERE emotion_date IN ({date_placeholders}) AND index_name IN ({index_placeholders})"
                )
                await cursor.execute(query_existing, [*dates, *index_names])
                existing_keys = {
                    (str(row['emotion_date']), str(row['index_name'])): {
                        'emotion_value': row.get('emotion_value'),
                        'source_file': row.get('source_file'),
                        'data_source': row.get('data_source'),
                    }
                    for row in await cursor.fetchall()
                }

                rows_to_upsert = []

                inserted_rows = 0
                updated_rows = 0
                affected_dates = set()
                for row in sanitized_rows:
                    row_key = (row['emotion_date'], row['index_name'])
                    existing_row = existing_keys.get(row_key)
                    if existing_row is None:
                        inserted_rows += 1
                        affected_dates.add(row['emotion_date'])
                        rows_to_upsert.append((
                            row['emotion_date'],
                            row['index_name'],
                            row['emotion_value'],
                            row['source_file'],
                            row['data_source'],
                        ))
                        continue

                    existing_emotion = self._normalize_numeric('douyin_emotion_value', existing_row.get('emotion_value'))
                    existing_source_file = str(existing_row.get('source_file') or '').strip() or None
                    existing_data_source = str(existing_row.get('data_source') or '').strip() or None
                    if (
                        existing_emotion != row['emotion_value']
                        or existing_source_file != row['source_file']
                        or existing_data_source != row['data_source']
                    ):
                        updated_rows += 1
                        affected_dates.add(row['emotion_date'])
                        rows_to_upsert.append((
                            row['emotion_date'],
                            row['index_name'],
                            row['emotion_value'],
                            row['source_file'],
                            row['data_source'],
                        ))

                if not rows_to_upsert:
                    return {
                        'parsed_rows': len(sanitized_rows),
                        'inserted_rows': 0,
                        'updated_rows': 0,
                        'affected_dates': [],
                    }

                query_insert = """
                INSERT INTO excel_index_emotion_daily (
                    emotion_date,
                    index_name,
                    emotion_value,
                    source_file,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    emotion_value = VALUES(emotion_value),
                    source_file = VALUES(source_file),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query_insert, rows_to_upsert)
                await conn.commit()
                return {
                    'parsed_rows': len(sanitized_rows),
                    'inserted_rows': inserted_rows,
                    'updated_rows': updated_rows,
                    'affected_dates': sorted(affected_dates),
                }

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

    async def get_option_rtj_missing_trade_dates(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT ref.trade_date
        FROM (
            SELECT DISTINCT trade_date
            FROM futures_daily_data
            WHERE market = 'CFFEX'
              AND data_source = 'get_futures_daily'
              AND trade_date BETWEEN %s AND %s
        ) ref
        LEFT JOIN (
            SELECT DISTINCT trade_date
            FROM option_cffex_rtj_daily_data
            WHERE trade_date BETWEEN %s AND %s
        ) opt
          ON opt.trade_date = ref.trade_date
        WHERE opt.trade_date IS NULL
        ORDER BY ref.trade_date ASC
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, [start_date, end_date, start_date, end_date])
                rows = await cursor.fetchall()

        return [str(row[0]) for row in rows if row and row[0] is not None]

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
            basic.etf_name,
            basic.sina_symbol
        FROM etf_basic_info_sina basic
        LEFT JOIN (
            SELECT etf_code
            FROM etf_daily_data_sina
            WHERE data_source = 'fund_etf_hist_sina'
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
                'sina_symbol': str(row[2]).strip().lower() if row[2] is not None else None,
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
