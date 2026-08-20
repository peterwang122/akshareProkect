import json
import math
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import aiomysql

from akshare_project.db.config import load_db_info


CFFEX_NET_SHORT_DELTA_WINDOWS = (5, 7, 14, 20, 30, 60, 120)
CFFEX_NET_SHORT_DELTA_SOURCE_PREFIXES = ("top20", "citic")
QUANT_INDEX_CFFEX_NET_SHORT_DELTA_FIELDS = tuple(
    f"cffex_{source_prefix}_net_short_delta_{window}d"
    for source_prefix in CFFEX_NET_SHORT_DELTA_SOURCE_PREFIXES
    for window in CFFEX_NET_SHORT_DELTA_WINDOWS
)
BASIS_DELTA_WINDOWS = CFFEX_NET_SHORT_DELTA_WINDOWS
BASIS_DELTA_KINDS = ("main", "month")
QUANT_INDEX_BASIS_DELTA_FIELDS = tuple(
    f"basis_{basis_kind}_delta_{window}d"
    for basis_kind in BASIS_DELTA_KINDS
    for window in BASIS_DELTA_WINDOWS
)
QUANT_INDEX_MARGIN_TRADING_FIELDS = (
    "margin_financing_balance",
    "margin_securities_lending_balance",
    "margin_total_balance",
    "margin_financing_net_buy_amount",
    "margin_leverage_ratio_pct",
    "margin_total_market_cap_leverage_ratio_pct",
)
MARGIN_FINANCING_NET_BUY_WINDOWS = CFFEX_NET_SHORT_DELTA_WINDOWS
QUANT_INDEX_MARGIN_FINANCING_NET_BUY_SUM_FIELDS = tuple(
    f"margin_financing_net_buy_sum_{window}d"
    for window in MARGIN_FINANCING_NET_BUY_WINDOWS
)
QUANT_INDEX_SELF_SENTIMENT_SCORE_FIELDS = (
    "self_sentiment_score",
    "self_sentiment_core_score",
    "self_sentiment_derivative_score",
)
QUANT_INDEX_RISK_FIELDS = (
    "risk_yellow_vulnerability",
    "risk_yellow_vulnerability_score",
    "risk_red_escalation",
    "risk_red_escalation_score",
    "risk_global_shock",
    "risk_global_shock_score",
    "risk_global_shock_mode",
    "risk_strategy_components_json",
)
QUANT_INDEX_TURNOVER_CONCENTRATION_FIELDS = (
    "turnover_concentration_top5_pct",
    "turnover_concentration_top1_pct",
    "turnover_concentration_top1_raw_pct",
    "turnover_concentration_meta_json",
)


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
        'price_change': 999999.99,
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
        'hs300_close': 9999999999.9999,
        'open_value': 9999999999.9999,
        'high_value': 9999999999.9999,
        'low_value': 9999999999.9999,
        'close_value': 9999999999.9999,
        'fear_greed_value': 9999999999.9999,
        'long_value': 99999999999999.99,
        'short_value': 99999999999999.99,
        'ratio_value': 9999999999.9999,
        'main_basis': 9999999999.9999,
        'month_basis': 9999999999.9999,
        'breadth_up_pct': 9999999999.9999,
        'option_pc_current_month': 9999999999.9999,
        'option_pc_next_month': 9999999999.9999,
        'option_pc_quarter_1': 9999999999.9999,
        'option_pc_quarter_2': 9999999999.9999,
        'option_volume_pc_ratio': 9999999999.9999,
        'option_turnover_pc_ratio': 9999999999.9999,
        **{field: 100.0 for field in QUANT_INDEX_SELF_SENTIMENT_SCORE_FIELDS},
        'risk_yellow_vulnerability_score': 100.0,
        'risk_red_escalation_score': 100.0,
        'risk_global_shock_score': 100.0,
        'fund_purchase_limit_pct': 100.0,
        **{field: 9999999999999999999999.99 for field in QUANT_INDEX_MARGIN_TRADING_FIELDS},
        **{field: 99999999999999.99 for field in QUANT_INDEX_CFFEX_NET_SHORT_DELTA_FIELDS},
        **{field: 99999999999999.99 for field in QUANT_INDEX_BASIS_DELTA_FIELDS},
        **{
            field: 9999999999999999999999999999.99
            for field in QUANT_INDEX_MARGIN_FINANCING_NET_BUY_SUM_FIELDS
        },
    }
    QUANT_INDEX_OPTION_PC_RATIO_FIELDS = (
        'option_pc_current_month',
        'option_pc_next_month',
        'option_pc_quarter_1',
        'option_pc_quarter_2',
    )
    QUANT_INDEX_OPTION_PC_CONTRACT_MONTH_FIELDS = (
        'option_pc_current_month_contract_month',
        'option_pc_next_month_contract_month',
        'option_pc_quarter_1_contract_month',
        'option_pc_quarter_2_contract_month',
    )
    QUANT_INDEX_OPTION_PC_SPECIAL_FLAG_FIELDS = (
        'option_pc_current_month_special_flag',
        'option_pc_next_month_special_flag',
        'option_pc_quarter_1_special_flag',
        'option_pc_quarter_2_special_flag',
    )
    QUANT_INDEX_OPTION_PC_SPECIAL_NOTE_FIELDS = (
        'option_pc_current_month_special_note',
        'option_pc_next_month_special_note',
        'option_pc_quarter_1_special_note',
        'option_pc_quarter_2_special_note',
    )
    QUANT_INDEX_OPTION_FLOW_PC_RATIO_FIELDS = (
        'option_volume_pc_ratio',
        'option_turnover_pc_ratio',
    )
    QUANT_INDEX_CFFEX_NET_SHORT_DELTA_FIELDS = QUANT_INDEX_CFFEX_NET_SHORT_DELTA_FIELDS
    QUANT_INDEX_BASIS_DELTA_FIELDS = QUANT_INDEX_BASIS_DELTA_FIELDS
    QUANT_INDEX_MARGIN_FINANCING_NET_BUY_SUM_FIELDS = QUANT_INDEX_MARGIN_FINANCING_NET_BUY_SUM_FIELDS
    QUANT_INDEX_FUND_PURCHASE_LIMIT_FIELDS = (
        'fund_purchase_limit_count',
        'fund_purchase_limit_total_count',
        'fund_purchase_limit_pct',
    )
    QUANT_INDEX_MARGIN_TRADING_FIELDS = QUANT_INDEX_MARGIN_TRADING_FIELDS
    QUANT_INDEX_SELF_SENTIMENT_SCORE_FIELDS = QUANT_INDEX_SELF_SENTIMENT_SCORE_FIELDS
    QUANT_INDEX_RISK_FIELDS = QUANT_INDEX_RISK_FIELDS
    QUANT_INDEX_TURNOVER_CONCENTRATION_FIELDS = QUANT_INDEX_TURNOVER_CONCENTRATION_FIELDS
    INDEX_BASIC_TABLES = {'index_basic_info', 'index_us_basic_info', 'index_hk_basic_info', 'index_qvix_basic_info'}
    INDEX_DAILY_TABLES = {'index_daily_data', 'index_us_daily_data', 'index_hk_daily_data', 'index_qvix_daily_data'}
    INDEX_FUTURES_CONTRACT_TABLES = {
        'futures_us_index_contract_info',
        'futures_us_index_official_contract_info',
        'futures_hk_index_contract_info',
    }
    INDEX_FUTURES_DAILY_TABLES = {'futures_us_index_daily_data', 'futures_hk_index_daily_data'}

    def __init__(self):
        self.db_info = self.load_db_info()
        self.session_time_zone = str(self.db_info.get('timezone', '+08:00')).strip() or '+08:00'
        self.pool = None
        self._stock_qfq_change_columns_ready = False
        self._stock_hfq_change_columns_ready = False
        self._stock_exchange_official_daily_table_ready = False
        self._exchange_option_tables_ready = False
        self._option_minute_tables_ready = False
        self._cn_risk_free_rate_table_ready = False
        self._cn_macro_tables_ready = False
        self._fund_purchase_limit_daily_table_ready = False
        self._margin_trading_daily_table_ready = False
        self._global_risk_asset_daily_table_ready = False
        self._a_share_turnover_concentration_daily_table_ready = False
        self._quant_index_dashboard_option_pc_columns_ready = False

    def load_db_info(self):
        return load_db_info()

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

    def _sanitize_stock_exchange_official_daily_row(self, row):
        sanitized = dict(row)
        sanitized['exchange'] = str(row.get('exchange', '')).strip().upper()
        sanitized['stock_code'] = str(row.get('stock_code', '')).strip()
        sanitized['prefixed_code'] = str(row.get('prefixed_code', '')).strip().lower()
        sanitized['stock_name'] = str(row.get('stock_name', '')).strip() or None
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        numeric_scales = {
            'open_price': 4,
            'close_price': 4,
            'high_price': 4,
            'low_price': 4,
            'pre_close_price': 4,
            'price_change_amount': 4,
            'price_change_rate': 4,
            'volume': 2,
            'turnover_amount': 2,
            'total_market_value': 2,
            'circulating_market_value': 2,
            'total_share_capital': 2,
            'circulating_share_capital': 2,
            'pe_rate': 4,
            'turnover_rate': 4,
            'amplitude': 4,
        }
        for field, scale in numeric_scales.items():
            numeric_value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = round(numeric_value, scale) if numeric_value is not None else None
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'exchange_official_daily'
        sanitized['raw_trading_json'] = self._serialize_json_field(row.get('raw_trading_json'))
        sanitized['raw_metrics_json'] = self._serialize_json_field(row.get('raw_metrics_json'))
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

    def _sanitize_stock_hfq_daily_row(self, row):
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
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'stock_zh_a_daily_hfq'
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

    def _sanitize_index_basic_row(self, row):
        sanitized = dict(row)
        sanitized['index_code'] = str(row.get('index_code', '')).strip()
        sanitized['simple_code'] = str(row.get('simple_code', '')).strip() or None
        sanitized['market'] = str(row.get('market', '')).strip() or None
        sanitized['index_name'] = str(row.get('index_name', '')).strip()
        sanitized['data_source'] = str(row.get('data_source', 'akshare')).strip() or 'akshare'
        return sanitized

    def _sanitize_index_news_sentiment_scope_row(self, row):
        sanitized = dict(row)
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['sentiment_value'] = self._normalize_numeric('emotion_value', row.get('sentiment_value'))
        sanitized['hs300_close'] = self._normalize_numeric('hs300_close', row.get('hs300_close'))
        sanitized['data_source'] = str(row.get('data_source', 'index_news_sentiment_scope')).strip() or 'index_news_sentiment_scope'
        return sanitized

    def _sanitize_index_us_vix_daily_row(self, row):
        sanitized = dict(row)
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['open_value'] = self._normalize_numeric('open_value', row.get('open_value'))
        sanitized['high_value'] = self._normalize_numeric('high_value', row.get('high_value'))
        sanitized['low_value'] = self._normalize_numeric('low_value', row.get('low_value'))
        sanitized['close_value'] = self._normalize_numeric('close_value', row.get('close_value'))
        sanitized['data_source'] = str(row.get('data_source', 'cboe_vix_history')).strip() or 'cboe_vix_history'
        return sanitized

    def _sanitize_index_us_fear_greed_daily_row(self, row):
        sanitized = dict(row)
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['fear_greed_value'] = self._normalize_numeric('fear_greed_value', row.get('fear_greed_value'))
        sanitized['sentiment_label'] = str(row.get('sentiment_label', '')).strip().upper() or None
        sanitized['data_source'] = str(row.get('data_source', 'cnn_fear_greed_live')).strip() or 'cnn_fear_greed_live'
        return sanitized

    def _sanitize_index_cn_market_fear_greed_daily_row(self, row):
        sanitized = dict(row)
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['fear_greed_value'] = self._normalize_numeric('emotion_value', row.get('fear_greed_value'))
        sanitized['sentiment_label'] = str(row.get('sentiment_label', '')).strip() or None
        sanitized['locked'] = 1 if bool(row.get('locked')) else 0
        sanitized['data_source'] = str(
            row.get('data_source', 'miumiu_market_fear_greed')
        ).strip() or 'miumiu_market_fear_greed'
        raw_json = row.get('raw_json')
        sanitized['raw_json'] = (
            raw_json
            if isinstance(raw_json, str)
            else json.dumps(raw_json or {}, ensure_ascii=False, separators=(',', ':'))
        )
        return sanitized

    def _sanitize_index_cn_baifenwei_fear_greed_daily_row(self, row):
        sanitized = dict(row)
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        for field_name in (
            'fear_greed_value',
            'volatility_score',
            'relative_turnover_score',
            'margin_trading_score',
            'market_breadth_score',
            'rsi_score',
            'limit_up_down_ratio_score',
        ):
            sanitized[field_name] = self._normalize_numeric('emotion_value', row.get(field_name))
        sanitized['market_index_value'] = self._normalize_numeric('close_value', row.get('market_index_value'))
        sanitized['sentiment_label'] = str(row.get('sentiment_label', '')).strip().upper() or None
        sanitized['value_origin'] = str(row.get('value_origin', '')).strip().lower() or 'reconstructed'
        sanitized['data_source'] = str(
            row.get('data_source', 'baifenwei_fear_greed')
        ).strip() or 'baifenwei_fear_greed'
        source_generated_at = str(row.get('source_generated_at') or '').strip()
        sanitized['source_generated_at'] = source_generated_at.replace('T', ' ') or None
        raw_json = row.get('raw_json')
        sanitized['raw_json'] = (
            raw_json
            if isinstance(raw_json, str)
            else json.dumps(raw_json or {}, ensure_ascii=False, separators=(',', ':'))
        )
        return sanitized

    def _sanitize_index_us_hedge_fund_ls_proxy_row(self, row):
        sanitized = dict(row)
        report_date = row.get('report_date')
        release_date = row.get('release_date')
        sanitized['report_date'] = str(report_date).split(' ')[0].strip() if report_date else ''
        sanitized['contract_scope'] = str(row.get('contract_scope', '')).strip().upper()
        sanitized['long_value'] = self._normalize_numeric('long_value', row.get('long_value'))
        sanitized['short_value'] = self._normalize_numeric('short_value', row.get('short_value'))
        sanitized['ratio_value'] = self._normalize_numeric('ratio_value', row.get('ratio_value'))
        sanitized['release_date'] = str(release_date).split(' ')[0].strip() if release_date else None
        sanitized['data_source'] = str(row.get('data_source', 'ofr_tff')).strip() or 'ofr_tff'
        return sanitized

    def _sanitize_index_us_put_call_ratio_row(self, row):
        sanitized = dict(row)
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['total_put_call_ratio'] = self._normalize_numeric(
            'total_put_call_ratio',
            row.get('total_put_call_ratio'),
        )
        sanitized['index_put_call_ratio'] = self._normalize_numeric(
            'index_put_call_ratio',
            row.get('index_put_call_ratio'),
        )
        sanitized['equity_put_call_ratio'] = self._normalize_numeric(
            'equity_put_call_ratio',
            row.get('equity_put_call_ratio'),
        )
        sanitized['etf_put_call_ratio'] = self._normalize_numeric(
            'etf_put_call_ratio',
            row.get('etf_put_call_ratio'),
        )
        sanitized['data_source'] = str(row.get('data_source', 'cboe_market_statistics')).strip() or 'cboe_market_statistics'
        return sanitized

    def _sanitize_index_us_treasury_yield_row(self, row):
        sanitized = dict(row)
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['yield_3m'] = self._normalize_numeric('yield_3m', row.get('yield_3m'))
        sanitized['yield_2y'] = self._normalize_numeric('yield_2y', row.get('yield_2y'))
        sanitized['yield_10y'] = self._normalize_numeric('yield_10y', row.get('yield_10y'))
        sanitized['yield_real_10y'] = self._normalize_numeric('yield_real_10y', row.get('yield_real_10y'))
        sanitized['spread_10y_2y'] = self._normalize_numeric('spread_10y_2y', row.get('spread_10y_2y'))
        sanitized['spread_10y_3m'] = self._normalize_numeric('spread_10y_3m', row.get('spread_10y_3m'))
        sanitized['available_at'] = str(row.get('available_at') or '').strip() or None
        sanitized['data_source'] = str(row.get('data_source', 'fred_public_csv')).strip() or 'fred_public_csv'
        return sanitized

    def _sanitize_index_us_credit_spread_row(self, row):
        sanitized = dict(row)
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['high_yield_oas'] = self._normalize_numeric('high_yield_oas', row.get('high_yield_oas'))
        sanitized['data_source'] = str(row.get('data_source', 'fred_public_csv')).strip() or 'fred_public_csv'
        return sanitized

    def _validate_table_name(self, table_name, allowed_tables):
        normalized_table_name = str(table_name or '').strip()
        if normalized_table_name not in allowed_tables:
            raise ValueError(f'unsupported table name: {normalized_table_name}')
        return normalized_table_name

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

    def _sanitize_index_futures_contract_row(self, row):
        sanitized = dict(row)
        sanitized['root_symbol'] = str(row.get('root_symbol', '')).strip().upper()
        sanitized['source_contract_code'] = str(row.get('source_contract_code', '')).strip().upper()
        sanitized['contract_name'] = str(row.get('contract_name', '')).strip() or None
        sanitized['contract_month'] = str(row.get('contract_month', '')).split(' ')[0].strip() or None
        sanitized['exchange'] = str(row.get('exchange', '')).strip().upper() or None
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or None
        first_seen = row.get('first_seen_trade_date')
        last_seen = row.get('last_seen_trade_date')
        sanitized['first_seen_trade_date'] = str(first_seen).split(' ')[0].strip() if first_seen else None
        sanitized['last_seen_trade_date'] = str(last_seen).split(' ')[0].strip() if last_seen else None
        return sanitized

    def _sanitize_index_futures_daily_row(self, row):
        sanitized = dict(row)
        sanitized['source_contract_code'] = str(row.get('source_contract_code', '')).strip().upper()
        sanitized['root_symbol'] = str(row.get('root_symbol', '')).strip().upper()
        sanitized['contract_name'] = str(row.get('contract_name', '')).strip() or None
        sanitized['contract_month'] = str(row.get('contract_month', '')).split(' ')[0].strip() or None
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        for field in [
            'open_price', 'high_price', 'low_price', 'close_price', 'volume',
            'open_interest', 'settle_price', 'pre_settle_price'
        ]:
            sanitized[field] = self._normalize_numeric(field, row.get(field))
        sanitized['closing_range_raw'] = str(row.get('closing_range_raw', '')).strip() or None
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or None
        return sanitized

    def _sanitize_us_index_official_futures_daily_row(self, row):
        sanitized = self._sanitize_index_futures_daily_row(row)
        sanitized['last_price'] = self._normalize_numeric('close_price', row.get('last_price'))
        sanitized['price_change'] = self._normalize_numeric('price_change', row.get('price_change'))
        sanitized['raw_payload_json'] = self._serialize_json_field(row.get('raw_payload_json'))
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
        for field in self.QUANT_INDEX_OPTION_PC_RATIO_FIELDS:
            value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = round(float(value), 6) if value is not None else None
        for field in self.QUANT_INDEX_OPTION_FLOW_PC_RATIO_FIELDS:
            value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = round(float(value), 6) if value is not None else None
        for field in self.QUANT_INDEX_CFFEX_NET_SHORT_DELTA_FIELDS:
            value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = round(float(value), 6) if value is not None else None
        for field in self.QUANT_INDEX_BASIS_DELTA_FIELDS:
            value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = round(float(value), 6) if value is not None else None
        for field in self.QUANT_INDEX_MARGIN_FINANCING_NET_BUY_SUM_FIELDS:
            value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = round(float(value), 2) if value is not None else None
        for field in ('fund_purchase_limit_count', 'fund_purchase_limit_total_count'):
            raw_value = row.get(field)
            sanitized[field] = int(raw_value) if raw_value is not None else None
        fund_purchase_limit_pct = self._normalize_numeric(
            'fund_purchase_limit_pct',
            row.get('fund_purchase_limit_pct'),
        )
        sanitized['fund_purchase_limit_pct'] = (
            round(max(0.0, min(100.0, float(fund_purchase_limit_pct))), 6)
            if fund_purchase_limit_pct is not None
            else None
        )
        for field in self.QUANT_INDEX_MARGIN_TRADING_FIELDS:
            value = self._normalize_numeric(field, row.get(field))
            precision = 6 if field.endswith("leverage_ratio_pct") else 2
            sanitized[field] = round(float(value), precision) if value is not None else None
        for field in self.QUANT_INDEX_SELF_SENTIMENT_SCORE_FIELDS:
            value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = (
                round(max(0.0, min(100.0, float(value))), 6)
                if value is not None
                else None
            )
        for field in (
            'risk_yellow_vulnerability',
            'risk_red_escalation',
            'risk_global_shock',
        ):
            raw_value = row.get(field)
            sanitized[field] = None if raw_value is None else (1 if raw_value else 0)
        for field in (
            'risk_yellow_vulnerability_score',
            'risk_red_escalation_score',
            'risk_global_shock_score',
        ):
            value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = (
                round(max(0.0, min(100.0, float(value))), 6)
                if value is not None
                else None
            )
        raw_mode = row.get('risk_global_shock_mode')
        sanitized['risk_global_shock_mode'] = (
            str(raw_mode).strip()[:64]
            if raw_mode is not None and str(raw_mode).strip()
            else None
        )
        for field in self.QUANT_INDEX_OPTION_PC_CONTRACT_MONTH_FIELDS:
            raw_value = row.get(field)
            sanitized[field] = str(raw_value).strip() if raw_value is not None and str(raw_value).strip() else None
        for field in self.QUANT_INDEX_OPTION_PC_SPECIAL_FLAG_FIELDS:
            sanitized[field] = 1 if row.get(field) else 0
        for field in self.QUANT_INDEX_OPTION_PC_SPECIAL_NOTE_FIELDS:
            raw_value = row.get(field)
            sanitized[field] = str(raw_value).strip()[:512] if raw_value is not None and str(raw_value).strip() else None
        sanitized['exchange_option_pc_json'] = self._serialize_json_field(
            row.get('exchange_option_pc_json')
        )
        sanitized['option_vix_json'] = self._serialize_json_field(
            row.get('option_vix_json')
        )
        sanitized['self_sentiment_components_json'] = self._serialize_json_field(
            row.get('self_sentiment_components_json')
        )
        sanitized['risk_strategy_components_json'] = self._serialize_json_field(
            row.get('risk_strategy_components_json')
        )
        for field in (
            'turnover_concentration_top5_pct',
            'turnover_concentration_top1_pct',
            'turnover_concentration_top1_raw_pct',
        ):
            value = self._normalize_numeric(field, row.get(field))
            sanitized[field] = (
                round(max(0.0, min(100.0, float(value))), 6)
                if value is not None
                else None
            )
        sanitized['turnover_concentration_meta_json'] = self._serialize_json_field(
            row.get('turnover_concentration_meta_json')
        )
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

    def _sanitize_exchange_option_contract_daily_row(self, row):
        sanitized = dict(row)
        sanitized['exchange'] = str(row.get('exchange', '')).strip().upper()
        sanitized['contract_code'] = str(row.get('contract_code', '')).strip()
        sanitized['contract_trade_code'] = str(row.get('contract_trade_code', '')).strip() or None
        sanitized['contract_name'] = str(row.get('contract_name', '')).strip() or None
        sanitized['underlying_code'] = str(row.get('underlying_code', '')).strip() or None
        sanitized['underlying_name'] = str(row.get('underlying_name', '')).strip() or None
        sanitized['option_type'] = str(row.get('option_type', '')).strip().upper() or None
        sanitized['contract_month'] = str(row.get('contract_month', '')).strip() or None
        sanitized['strike_price'] = self._normalize_numeric('close_price', row.get('strike_price'))
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        for field in (
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'pre_close_price',
            'pre_settle_price',
            'settle_price',
            'volume',
            'turnover',
            'open_interest',
        ):
            sanitized[field] = self._normalize_numeric(field, row.get(field))
        sanitized['pre_settle_source'] = (
            str(row.get('pre_settle_source', '')).strip() or None
        )
        for field in (
            'delta_value',
            'theta_value',
            'gamma_value',
            'vega_value',
            'rho_value',
            'implied_volatility',
        ):
            sanitized[field] = self._normalize_numeric('price_change_rate', row.get(field))
        sanitized['data_source'] = (
            str(row.get('data_source', '')).strip() or 'exchange_official+sina'
        )
        sanitized['source_url'] = str(row.get('source_url', '')).strip() or None
        sanitized['raw_json'] = self._serialize_json_field(row.get('raw_json'))
        return sanitized

    def _sanitize_exchange_option_contract_info_row(self, row):
        sanitized = dict(row)
        sanitized['exchange'] = str(row.get('exchange', '')).strip().upper()
        sanitized['contract_code'] = str(row.get('contract_code', '')).strip()
        sanitized['contract_trade_code'] = str(row.get('contract_trade_code', '')).strip() or None
        sanitized['contract_name'] = str(row.get('contract_name', '')).strip() or None
        sanitized['underlying_code'] = str(row.get('underlying_code', '')).strip() or None
        sanitized['underlying_name'] = str(row.get('underlying_name', '')).strip() or None
        sanitized['option_type'] = str(row.get('option_type', '')).strip().upper() or None
        sanitized['contract_month'] = str(row.get('contract_month', '')).strip() or None
        sanitized['strike_price'] = self._normalize_numeric('close_price', row.get('strike_price'))
        sanitized['contract_unit'] = self._normalize_numeric('volume', row.get('contract_unit'))
        for field in (
            'listed_date',
            'last_trade_date',
            'exercise_date',
            'expire_date',
            'delivery_date',
        ):
            value = row.get(field)
            sanitized[field] = str(value).split(' ')[0].strip() if value else None
        sanitized['listing_reason'] = str(row.get('listing_reason', '')).strip() or None
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'exchange_official'
        sanitized['source_url'] = str(row.get('source_url', '')).strip() or None
        sanitized['raw_json'] = self._serialize_json_field(row.get('raw_json'))
        return sanitized

    def _sanitize_exchange_option_daily_stats_row(self, row):
        sanitized = dict(row)
        sanitized['exchange'] = str(row.get('exchange', '')).strip().upper()
        sanitized['underlying_code'] = str(row.get('underlying_code', '')).strip()
        sanitized['underlying_name'] = str(row.get('underlying_name', '')).strip() or None
        trade_date = row.get('trade_date')
        sanitized['trade_date'] = str(trade_date).split(' ')[0].strip() if trade_date else ''
        sanitized['contract_count'] = self._normalize_numeric('volume', row.get('contract_count'))
        sanitized['turnover_amount'] = self._normalize_numeric('turnover', row.get('turnover_amount'))
        for field in (
            'total_volume',
            'call_volume',
            'put_volume',
            'open_interest',
            'call_open_interest',
            'put_open_interest',
        ):
            sanitized[field] = self._normalize_numeric('volume', row.get(field))
        put_call_volume_ratio = self._normalize_numeric(
            'price_change_rate',
            row.get('put_call_volume_ratio'),
        )
        sanitized['put_call_volume_ratio'] = (
            round(put_call_volume_ratio, 8)
            if put_call_volume_ratio is not None
            else None
        )
        sanitized['data_source'] = str(row.get('data_source', '')).strip() or 'exchange_official'
        sanitized['source_url'] = str(row.get('source_url', '')).strip() or None
        sanitized['raw_json'] = self._serialize_json_field(row.get('raw_json'))
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

    async def ensure_stock_hfq_change_columns(self):
        if self._stock_hfq_change_columns_ready:
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
                      AND table_name = 'stock_hfq_daily_data'
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
                        "ALTER TABLE stock_hfq_daily_data " + ", ".join(alter_clauses)
                    )
                    await conn.commit()

        self._stock_hfq_change_columns_ready = True

    async def ensure_stock_exchange_official_daily_table(self):
        if self._stock_exchange_official_daily_table_ready:
            return

        if self.pool is None:
            await self.init_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stock_exchange_official_daily_data (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        exchange VARCHAR(16) NOT NULL,
                        stock_code VARCHAR(16) NOT NULL,
                        prefixed_code VARCHAR(16) NOT NULL,
                        stock_name VARCHAR(128) NULL,
                        trade_date DATE NOT NULL,
                        open_price DECIMAL(18, 4) NULL,
                        close_price DECIMAL(18, 4) NULL,
                        high_price DECIMAL(18, 4) NULL,
                        low_price DECIMAL(18, 4) NULL,
                        pre_close_price DECIMAL(18, 4) NULL,
                        price_change_amount DECIMAL(18, 4) NULL,
                        price_change_rate DECIMAL(18, 4) NULL,
                        volume DECIMAL(24, 2) NULL,
                        turnover_amount DECIMAL(24, 2) NULL,
                        total_market_value DECIMAL(28, 2) NULL,
                        circulating_market_value DECIMAL(28, 2) NULL,
                        total_share_capital DECIMAL(24, 2) NULL,
                        circulating_share_capital DECIMAL(24, 2) NULL,
                        pe_rate DECIMAL(18, 4) NULL,
                        turnover_rate DECIMAL(18, 4) NULL,
                        amplitude DECIMAL(18, 4) NULL,
                        data_source VARCHAR(64) NOT NULL,
                        raw_trading_json LONGTEXT NULL,
                        raw_metrics_json LONGTEXT NULL,
                        created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY uk_stock_exchange_official_daily (exchange, prefixed_code, trade_date),
                        KEY idx_stock_exchange_official_date_exchange (trade_date, exchange),
                        KEY idx_stock_exchange_official_code_date (prefixed_code, trade_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await conn.commit()

        self._stock_exchange_official_daily_table_ready = True

    async def ensure_exchange_option_tables(self):
        if self._exchange_option_tables_ready:
            return

        if self.pool is None:
            await self.init_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS option_exchange_contract_info (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        exchange VARCHAR(16) NOT NULL,
                        contract_code VARCHAR(32) NOT NULL,
                        contract_trade_code VARCHAR(64) NULL,
                        contract_name VARCHAR(128) NULL,
                        underlying_code VARCHAR(16) NULL,
                        underlying_name VARCHAR(128) NULL,
                        option_type VARCHAR(16) NULL,
                        contract_month VARCHAR(8) NULL,
                        strike_price DECIMAL(18, 6) NULL,
                        contract_unit DECIMAL(24, 2) NULL,
                        listed_date DATE NULL,
                        last_trade_date DATE NULL,
                        exercise_date DATE NULL,
                        expire_date DATE NULL,
                        delivery_date DATE NULL,
                        listing_reason VARCHAR(128) NULL,
                        data_source VARCHAR(64) NOT NULL,
                        source_url VARCHAR(512) NULL,
                        raw_json LONGTEXT NULL,
                        created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY uk_option_exchange_contract_info (
                            exchange, contract_code
                        ),
                        KEY idx_option_exchange_info_underlying (
                            exchange, underlying_code, listed_date
                        ),
                        KEY idx_option_exchange_info_expire (expire_date, exchange)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS option_exchange_contract_daily_data (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        exchange VARCHAR(16) NOT NULL,
                        contract_code VARCHAR(32) NOT NULL,
                        contract_trade_code VARCHAR(64) NULL,
                        contract_name VARCHAR(128) NULL,
                        underlying_code VARCHAR(16) NULL,
                        underlying_name VARCHAR(128) NULL,
                        option_type VARCHAR(16) NULL,
                        contract_month VARCHAR(8) NULL,
                        strike_price DECIMAL(18, 6) NULL,
                        trade_date DATE NOT NULL,
                        open_price DECIMAL(18, 6) NULL,
                        high_price DECIMAL(18, 6) NULL,
                        low_price DECIMAL(18, 6) NULL,
                        close_price DECIMAL(18, 6) NULL,
                        pre_close_price DECIMAL(18, 6) NULL,
                        pre_settle_price DECIMAL(18, 6) NULL,
                        pre_settle_source VARCHAR(64) NULL,
                        settle_price DECIMAL(18, 6) NULL,
                        volume DECIMAL(24, 2) NULL,
                        turnover DECIMAL(24, 2) NULL,
                        open_interest DECIMAL(24, 2) NULL,
                        delta_value DECIMAL(18, 8) NULL,
                        theta_value DECIMAL(18, 8) NULL,
                        gamma_value DECIMAL(18, 8) NULL,
                        vega_value DECIMAL(18, 8) NULL,
                        rho_value DECIMAL(18, 8) NULL,
                        implied_volatility DECIMAL(18, 8) NULL,
                        data_source VARCHAR(64) NOT NULL,
                        source_url VARCHAR(512) NULL,
                        raw_json LONGTEXT NULL,
                        created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY uk_option_exchange_contract_date (
                            exchange, contract_code, trade_date
                        ),
                        KEY idx_option_exchange_date (trade_date, exchange),
                        KEY idx_option_exchange_underlying_date (
                            exchange, underlying_code, trade_date
                        ),
                        KEY idx_option_exchange_trade_code_date (
                            contract_trade_code, trade_date
                        )
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS option_exchange_daily_stats (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        exchange VARCHAR(16) NOT NULL,
                        underlying_code VARCHAR(16) NOT NULL,
                        underlying_name VARCHAR(128) NULL,
                        trade_date DATE NOT NULL,
                        contract_count DECIMAL(18, 2) NULL,
                        turnover_amount DECIMAL(24, 2) NULL,
                        total_volume DECIMAL(24, 2) NULL,
                        call_volume DECIMAL(24, 2) NULL,
                        put_volume DECIMAL(24, 2) NULL,
                        put_call_volume_ratio DECIMAL(18, 8) NULL,
                        open_interest DECIMAL(24, 2) NULL,
                        call_open_interest DECIMAL(24, 2) NULL,
                        put_open_interest DECIMAL(24, 2) NULL,
                        data_source VARCHAR(64) NOT NULL,
                        source_url VARCHAR(512) NULL,
                        raw_json LONGTEXT NULL,
                        created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY uk_option_exchange_stats_date (
                            exchange, underlying_code, trade_date
                        ),
                        KEY idx_option_exchange_stats_date (trade_date, exchange)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await cursor.execute(
                    "SHOW COLUMNS FROM option_exchange_contract_daily_data"
                )
                daily_columns = {
                    str(row[0]).strip() for row in await cursor.fetchall()
                }
                if "pre_settle_price" not in daily_columns:
                    await cursor.execute(
                        """
                        ALTER TABLE option_exchange_contract_daily_data
                        ADD COLUMN pre_settle_price DECIMAL(18, 6) NULL
                        AFTER pre_close_price
                        """
                    )
                if "pre_settle_source" not in daily_columns:
                    await cursor.execute(
                        """
                        ALTER TABLE option_exchange_contract_daily_data
                        ADD COLUMN pre_settle_source VARCHAR(64) NULL
                        AFTER pre_settle_price
                        """
                    )
                await conn.commit()

        self._exchange_option_tables_ready = True

    async def ensure_option_minute_tables(self):
        if self._option_minute_tables_ready:
            return

        if self.pool is None:
            await self.init_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS option_contract_minute_data (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        exchange VARCHAR(16) NOT NULL,
                        contract_code VARCHAR(32) NOT NULL,
                        contract_trade_code VARCHAR(64) NULL,
                        underlying_code VARCHAR(16) NOT NULL,
                        option_type VARCHAR(16) NOT NULL,
                        contract_month VARCHAR(8) NOT NULL,
                        strike_price DECIMAL(18, 6) NOT NULL,
                        expire_date DATE NOT NULL,
                        trade_date DATE NOT NULL,
                        bar_time DATETIME NOT NULL,
                        open_price DECIMAL(18, 8) NULL,
                        high_price DECIMAL(18, 8) NULL,
                        low_price DECIMAL(18, 8) NULL,
                        close_price DECIMAL(18, 8) NULL,
                        bid1_price DECIMAL(18, 8) NULL,
                        bid1_volume DECIMAL(24, 2) NULL,
                        ask1_price DECIMAL(18, 8) NULL,
                        ask1_volume DECIMAL(24, 2) NULL,
                        mid_price DECIMAL(18, 8) NULL,
                        average_price DECIMAL(18, 8) NULL,
                        minute_volume DECIMAL(24, 2) NULL,
                        cumulative_volume DECIMAL(24, 2) NULL,
                        cumulative_turnover DECIMAL(24, 2) NULL,
                        open_interest DECIMAL(24, 2) NULL,
                        quote_count INT NOT NULL DEFAULT 1,
                        price_basis VARCHAR(32) NOT NULL,
                        data_source VARCHAR(64) NOT NULL,
                        source_url VARCHAR(512) NULL,
                        raw_json LONGTEXT NULL,
                        created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY uk_option_contract_minute (
                            exchange, contract_code, bar_time
                        ),
                        KEY idx_option_minute_source_time (
                            exchange, underlying_code, bar_time
                        ),
                        KEY idx_option_minute_trade_date (trade_date, exchange),
                        KEY idx_option_minute_contract_date (
                            contract_code, trade_date
                        )
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS option_vix_minute_data (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        index_name VARCHAR(64) NOT NULL,
                        source_key VARCHAR(64) NOT NULL,
                        exchange VARCHAR(16) NOT NULL,
                        product_code VARCHAR(16) NOT NULL,
                        trade_date DATE NOT NULL,
                        bar_time DATETIME NOT NULL,
                        vix_value DECIMAL(18, 8) NOT NULL,
                        near_contract_month VARCHAR(8) NULL,
                        near_expire_date DATE NULL,
                        near_strike_count INT NULL,
                        next_contract_month VARCHAR(8) NULL,
                        next_expire_date DATE NULL,
                        next_strike_count INT NULL,
                        risk_free_curve_date DATE NULL,
                        near_risk_free_rate DECIMAL(18, 10) NULL,
                        next_risk_free_rate DECIMAL(18, 10) NULL,
                        price_basis VARCHAR(32) NOT NULL,
                        calculation_method VARCHAR(64) NOT NULL,
                        quality_json LONGTEXT NULL,
                        created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY uk_option_vix_minute (source_key, bar_time),
                        KEY idx_option_vix_minute_index_time (
                            index_name, bar_time
                        ),
                        KEY idx_option_vix_minute_trade_date (
                            trade_date, source_key
                        )
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await conn.commit()

        self._option_minute_tables_ready = True

    async def ensure_cn_risk_free_rate_table(self):
        if self._cn_risk_free_rate_table_ready:
            return
        if self.pool is None:
            await self.init_pool()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cn_risk_free_rate_daily (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        trade_date DATE NOT NULL,
                        tenor_code VARCHAR(8) NOT NULL,
                        tenor_days INT NOT NULL,
                        rate_pct DECIMAL(18, 8) NOT NULL,
                        rate_decimal DECIMAL(18, 10) NOT NULL,
                        data_source VARCHAR(64) NOT NULL,
                        source_url VARCHAR(512) NULL,
                        raw_json LONGTEXT NULL,
                        created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY uk_cn_risk_free_rate_date_tenor (
                            trade_date, tenor_code
                        ),
                        KEY idx_cn_risk_free_rate_date (trade_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await conn.commit()
        self._cn_risk_free_rate_table_ready = True

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

    async def upsert_stock_exchange_official_daily_data(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()
        await self.ensure_stock_exchange_official_daily_table()

        sanitized_rows = [self._sanitize_stock_exchange_official_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['exchange'] and row['stock_code'] and row['prefixed_code'] and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        deduped = {}
        for row in sanitized_rows:
            deduped[(row['exchange'], row['prefixed_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped.values())

        values = [
            (
                row['exchange'],
                row['stock_code'],
                row['prefixed_code'],
                row['stock_name'],
                row['trade_date'],
                row['open_price'],
                row['close_price'],
                row['high_price'],
                row['low_price'],
                row['pre_close_price'],
                row['price_change_amount'],
                row['price_change_rate'],
                row['volume'],
                row['turnover_amount'],
                row['total_market_value'],
                row['circulating_market_value'],
                row['total_share_capital'],
                row['circulating_share_capital'],
                row['pe_rate'],
                row['turnover_rate'],
                row['amplitude'],
                row['data_source'],
                row['raw_trading_json'],
                row['raw_metrics_json'],
            )
            for row in sanitized_rows
        ]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                INSERT INTO stock_exchange_official_daily_data (
                    exchange,
                    stock_code,
                    prefixed_code,
                    stock_name,
                    trade_date,
                    open_price,
                    close_price,
                    high_price,
                    low_price,
                    pre_close_price,
                    price_change_amount,
                    price_change_rate,
                    volume,
                    turnover_amount,
                    total_market_value,
                    circulating_market_value,
                    total_share_capital,
                    circulating_share_capital,
                    pe_rate,
                    turnover_rate,
                    amplitude,
                    data_source,
                    raw_trading_json,
                    raw_metrics_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    stock_code = VALUES(stock_code),
                    stock_name = VALUES(stock_name),
                    open_price = VALUES(open_price),
                    close_price = VALUES(close_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    pre_close_price = VALUES(pre_close_price),
                    price_change_amount = VALUES(price_change_amount),
                    price_change_rate = VALUES(price_change_rate),
                    volume = VALUES(volume),
                    turnover_amount = VALUES(turnover_amount),
                    total_market_value = VALUES(total_market_value),
                    circulating_market_value = VALUES(circulating_market_value),
                    total_share_capital = VALUES(total_share_capital),
                    circulating_share_capital = VALUES(circulating_share_capital),
                    pe_rate = VALUES(pe_rate),
                    turnover_rate = VALUES(turnover_rate),
                    amplitude = VALUES(amplitude),
                    data_source = VALUES(data_source),
                    raw_trading_json = VALUES(raw_trading_json),
                    raw_metrics_json = VALUES(raw_metrics_json),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query, values)
                await conn.commit()
                return len(sanitized_rows)

    async def get_stock_exchange_official_daily_coverage_by_date(
        self,
        trade_date,
        exchange,
    ):
        if self.pool is None:
            await self.init_pool()
        await self.ensure_stock_exchange_official_daily_table()

        normalized_trade_date = str(trade_date or '').split(' ')[0].strip()
        normalized_exchange = str(exchange or '').strip().upper()
        if not normalized_trade_date or normalized_exchange not in {'SH', 'SZ'}:
            return {}

        query = """
        SELECT
            prefixed_code,
            CASE
                WHEN close_price IS NOT NULL
                 AND turnover_amount IS NOT NULL
                 AND raw_trading_json IS NOT NULL
                 AND raw_metrics_json IS NOT NULL
                THEN 1 ELSE 0
            END AS is_complete
        FROM stock_exchange_official_daily_data
        WHERE trade_date = %s
          AND exchange = %s
        ORDER BY prefixed_code ASC
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    query,
                    (normalized_trade_date, normalized_exchange),
                )
                rows = await cursor.fetchall()
                return {
                    str(row[0]).strip().lower(): bool(row[1])
                    for row in rows
                    if row and row[0]
                }

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

    async def get_stock_daily_hist_metric_targets(self, stock_codes=None, start_date=None, end_date=None):
        if self.pool is None:
            await self.init_pool()

        normalized_codes = sorted(
            {
                str(stock_code).strip()
                for stock_code in (stock_codes or [])
                if str(stock_code).strip()
            }
        )
        normalized_start_date = str(start_date or '').split(' ')[0].strip()
        normalized_end_date = str(end_date or '').split(' ')[0].strip()

        query = """
        SELECT DISTINCT
            stock_code,
            prefixed_code
        FROM stock_daily_data
        WHERE data_source = 'stock_zh_a_hist_tx'
        """
        params = []

        if normalized_start_date:
            query += " AND trade_date >= %s"
            params.append(normalized_start_date)
        if normalized_end_date:
            query += " AND trade_date <= %s"
            params.append(normalized_end_date)
        if normalized_codes:
            placeholders = ','.join(['%s'] * len(normalized_codes))
            query += f" AND stock_code IN ({placeholders})"
            params.extend(normalized_codes)

        query += " ORDER BY prefixed_code ASC"

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def get_stock_daily_hist_rows_for_metric_repair(self, prefixed_code, end_date=None):
        if self.pool is None:
            await self.init_pool()

        normalized_prefixed_code = str(prefixed_code or '').strip().lower()
        normalized_end_date = str(end_date or '').split(' ')[0].strip()
        if not normalized_prefixed_code:
            return []

        query = """
        SELECT
            stock_code,
            prefixed_code,
            trade_date,
            close_price
        FROM stock_daily_data
        WHERE data_source = 'stock_zh_a_hist_tx'
          AND prefixed_code = %s
        """
        params = [normalized_prefixed_code]

        if normalized_end_date:
            query += " AND trade_date <= %s"
            params.append(normalized_end_date)

        query += " ORDER BY trade_date ASC"

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def update_stock_daily_hist_metrics(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        values = []
        for row in rows:
            prefixed_code = str((row or {}).get('prefixed_code') or '').strip().lower()
            trade_date = str((row or {}).get('trade_date') or '').split(' ')[0].strip()
            if not prefixed_code or not trade_date:
                continue

            values.append((
                self._normalize_numeric('pre_close_price', (row or {}).get('pre_close_price')),
                self._normalize_numeric('price_change_amount', (row or {}).get('price_change_amount')),
                self._normalize_numeric('price_change_rate', (row or {}).get('price_change_rate')),
                prefixed_code,
                trade_date,
            ))

        if not values:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                UPDATE stock_daily_data
                SET pre_close_price = %s,
                    price_change_amount = %s,
                    price_change_rate = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE prefixed_code = %s
                  AND trade_date = %s
                  AND data_source = 'stock_zh_a_hist_tx'
                """
                await cursor.executemany(query, values)
                await conn.commit()
                return cursor.rowcount

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

    async def get_stock_hfq_request_window(self, prefixed_code):
        if self.pool is None:
            await self.init_pool()

        normalized_prefixed_code = str(prefixed_code or '').strip().lower()
        if not normalized_prefixed_code:
            return None

        query = """
        SELECT prefixed_code, request_start_date, request_end_date, refresh_batch_id
        FROM stock_hfq_daily_data
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

    async def replace_stock_hfq_daily_data(self, prefixed_code, rows):
        if self.pool is None:
            await self.init_pool()
        await self.ensure_stock_hfq_change_columns()

        normalized_prefixed_code = str(prefixed_code or '').strip().lower()
        sanitized_rows = [self._sanitize_stock_hfq_daily_row(row) for row in rows]
        sanitized_rows = [
            row for row in sanitized_rows
            if row['stock_code'] and row['prefixed_code'] == normalized_prefixed_code and row['trade_date']
        ]
        if not normalized_prefixed_code:
            return 0, 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM stock_hfq_daily_data WHERE prefixed_code = %s",
                    (normalized_prefixed_code,),
                )
                deleted_rows = int((await cursor.fetchone())[0] or 0)
                await cursor.execute(
                    "DELETE FROM stock_hfq_daily_data WHERE prefixed_code = %s",
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
                    INSERT INTO stock_hfq_daily_data (
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

    async def _upsert_index_basic_info_for_table(self, table_name, basic_rows):
        if not basic_rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        normalized_table_name = self._validate_table_name(table_name, self.INDEX_BASIC_TABLES)

        deduped_rows = {}
        for row in basic_rows:
            sanitized = self._sanitize_index_basic_row(row)
            if not sanitized['index_code']:
                continue
            deduped_rows[sanitized['index_code']] = (
                sanitized['index_code'],
                sanitized['simple_code'],
                sanitized['market'],
                sanitized['index_name'],
                sanitized['data_source'],
            )

        rows_to_upsert = list(deduped_rows.values())
        if not rows_to_upsert:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = f"""
                INSERT INTO {normalized_table_name} (
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

    async def upsert_index_basic_info(self, basic_rows):
        return await self._upsert_index_basic_info_for_table('index_basic_info', basic_rows)

    async def upsert_index_us_basic_info(self, basic_rows):
        return await self._upsert_index_basic_info_for_table('index_us_basic_info', basic_rows)

    async def upsert_index_hk_basic_info(self, basic_rows):
        return await self._upsert_index_basic_info_for_table('index_hk_basic_info', basic_rows)

    async def upsert_index_qvix_basic_info(self, basic_rows):
        return await self._upsert_index_basic_info_for_table('index_qvix_basic_info', basic_rows)

    async def _batch_index_daily_data_for_table(self, table_name, updates):
        if not updates:
            return 0

        if self.pool is None:
            await self.init_pool()

        normalized_table_name = self._validate_table_name(table_name, self.INDEX_DAILY_TABLES)

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
                    f"SELECT trade_date FROM {normalized_table_name} WHERE index_code = %s "
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

                query_insert = f"""
                INSERT INTO {normalized_table_name} (
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

    async def batch_index_daily_data(self, updates):
        return await self._batch_index_daily_data_for_table('index_daily_data', updates)

    async def batch_index_us_daily_data(self, updates):
        return await self._batch_index_daily_data_for_table('index_us_daily_data', updates)

    async def batch_index_hk_daily_data(self, updates):
        return await self._batch_index_daily_data_for_table('index_hk_daily_data', updates)

    async def batch_index_qvix_daily_data(self, updates):
        return await self._batch_index_daily_data_for_table('index_qvix_daily_data', updates)

    async def upsert_index_daily_data(self, updates):
        """Upsert official index rows, including revised turnover fields."""
        if not updates:
            return 0
        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_index_daily_update(update) for update in updates]
        sanitized_rows = [
            row for row in sanitized_rows if row.get('index_code') and row.get('trade_date')
        ]
        deduped = {
            (row['index_code'], row['trade_date']): row for row in sanitized_rows
        }
        rows = list(deduped.values())
        if not rows:
            return 0

        query = """
        INSERT INTO index_daily_data (
            index_code, open_price, close_price, high_price, low_price,
            volume, turnover, amplitude, price_change_rate,
            price_change_amount, turnover_rate, trade_date, data_source
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
                row['index_code'], row['open_price'], row['close_price'],
                row['high_price'], row['low_price'], row['volume'], row['turnover'],
                row['amplitude'], row['price_change_rate'], row['price_change_amount'],
                row['turnover_rate'], row['trade_date'], row['data_source'],
            )
            for row in rows
        ]
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(rows)

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

    async def get_quant_index_dashboard_cffex_net_short_positions(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        start_date = str(start_date or "1900-01-01")
        end_date = str(end_date or "2999-12-31")
        product_codes_sql = "'IH','IF','IC','IM'"
        member_match_sql = """
            ((trade_date < %s AND {member_column} = %s)
             OR (trade_date >= %s AND trade_date < %s AND {member_column} = %s)
             OR (trade_date >= %s AND {member_column} = %s))
        """
        short_member_match_sql = member_match_sql.format(member_column="short_member")
        long_member_match_sql = member_match_sql.format(member_column="long_member")
        query = f"""
        SELECT
            trade_date,
            product_code,
            'top20_institutions' AS source_key,
            SUM(COALESCE(short_open_interest, 0)) AS short_position,
            SUM(COALESCE(long_open_interest, 0)) AS long_position
        FROM cffex_member_rankings
        WHERE product_code IN ({product_codes_sql})
          AND trade_date BETWEEN %s AND %s
          AND rank_no <= 20
          AND volume_member IS NOT NULL
        GROUP BY trade_date, product_code
        UNION ALL
        SELECT
            trade_date,
            product_code,
            'citic_customer' AS source_key,
            SUM(CASE WHEN {short_member_match_sql} THEN COALESCE(short_open_interest, 0) ELSE 0 END) AS short_position,
            SUM(CASE WHEN {long_member_match_sql} THEN COALESCE(long_open_interest, 0) ELSE 0 END) AS long_position
        FROM cffex_member_rankings
        WHERE product_code IN ({product_codes_sql})
          AND trade_date BETWEEN %s AND %s
        GROUP BY trade_date, product_code
        ORDER BY trade_date ASC, source_key ASC, product_code ASC
        """
        params = [
            str(start_date),
            str(end_date),
            "2024-02-26",
            "中信期货",
            "2024-02-26",
            "2024-04-29",
            "中信期货(经纪)",
            "2024-04-29",
            "中信期货(代客)",
            "2024-02-26",
            "中信期货",
            "2024-02-26",
            "2024-04-29",
            "中信期货(经纪)",
            "2024-04-29",
            "中信期货(代客)",
            str(start_date),
            str(end_date),
        ]

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def delete_incomplete_cffex_equity_index_dates(self, start_date, end_date, product_codes):
        if not product_codes:
            return 0

        if self.pool is None:
            await self.init_pool()

        placeholders = ','.join(['%s'] * len(product_codes))
        query = f"""
        DELETE FROM cffex_member_rankings
        WHERE trade_date IN (
            SELECT trade_date FROM (
                SELECT trade_date
                FROM cffex_member_rankings
                WHERE trade_date BETWEEN %s AND %s
                  AND product_code IN ({placeholders})
                GROUP BY trade_date
                HAVING COUNT(DISTINCT product_code) > 0
                   AND COUNT(DISTINCT product_code) < %s
            ) incomplete_dates
        )
          AND product_code IN ({placeholders})
        """
        params = [start_date, end_date, *product_codes, len(product_codes), *product_codes]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                await conn.commit()
                return cursor.rowcount

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
                    emotion_date = VALUES(emotion_date),
                    video_id = VALUES(video_id),
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

    async def get_douyin_emotion_by_video_id(self, video_id):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT
            emotion_date,
            video_id,
            hs300_emotion,
            zz500_emotion,
            zz1000_emotion,
            sz50_emotion,
            raw_ocr_text,
            extraction_status
        FROM douyin_index_emotion_daily
        WHERE video_id = %s
        LIMIT 1
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(video_id).strip()])
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_douyin_emotion_by_date(self, emotion_date):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT
            emotion_date,
            video_id,
            video_title,
            video_url,
            hs300_emotion,
            zz500_emotion,
            zz1000_emotion,
            sz50_emotion,
            raw_ocr_text,
            extraction_status
        FROM douyin_index_emotion_daily
        WHERE emotion_date = %s
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(emotion_date).strip()])
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def batch_douyin_emotion_to_excel(self, emotion_date, video_id, values):
        if self.pool is None:
            await self.init_pool()

        normalized_date = str(emotion_date or "").strip()
        normalized_video_id = str(video_id or "").strip()
        index_value_map = {
            "上证50": self._normalize_numeric("douyin_emotion_value", values.get("sz50_emotion")),
            "沪深300": self._normalize_numeric("douyin_emotion_value", values.get("hs300_emotion")),
            "中证500": self._normalize_numeric("douyin_emotion_value", values.get("zz500_emotion")),
            "中证1000": self._normalize_numeric("douyin_emotion_value", values.get("zz1000_emotion")),
        }
        if not normalized_date or not normalized_video_id or any(value is None for value in index_value_map.values()):
            raise ValueError("douyin emotion normalization requires a date, video id, and four values")

        source_file = f"douyin:{normalized_video_id}"
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    SELECT index_name, data_source
                    FROM excel_index_emotion_daily
                    WHERE emotion_date = %s
                      AND index_name IN ('上证50', '沪深300', '中证500', '中证1000')
                    """,
                    [normalized_date],
                )
                existing_sources = {
                    str(row["index_name"]): str(row.get("data_source") or "").strip().lower()
                    for row in await cursor.fetchall()
                }

                rows_to_upsert = []
                protected_names = []
                for index_name, emotion_value in index_value_map.items():
                    if existing_sources.get(index_name) == "excel":
                        protected_names.append(index_name)
                        continue
                    rows_to_upsert.append(
                        (
                            normalized_date,
                            index_name,
                            round(float(emotion_value), 2),
                            source_file,
                            "douyin_coze",
                        )
                    )

                if rows_to_upsert:
                    await cursor.executemany(
                        """
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
                        """,
                        rows_to_upsert,
                    )

                await cursor.execute(
                    """
                    SELECT COUNT(DISTINCT index_name) AS row_count
                    FROM excel_index_emotion_daily
                    WHERE emotion_date = %s
                      AND index_name IN ('上证50', '沪深300', '中证500', '中证1000')
                      AND emotion_value IS NOT NULL
                    """,
                    [normalized_date],
                )
                result_row = await cursor.fetchone()
                await conn.commit()
                return {
                    "upserted_rows": len(rows_to_upsert),
                    "protected_names": protected_names,
                    "available_rows": int((result_row or {}).get("row_count") or 0),
                }

    async def get_complete_excel_emotion_dates(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT emotion_date
        FROM excel_index_emotion_daily
        WHERE emotion_date BETWEEN %s AND %s
          AND index_name IN ('上证50', '沪深300', '中证500', '中证1000')
          AND emotion_value IS NOT NULL
        GROUP BY emotion_date
        HAVING COUNT(DISTINCT index_name) = 4
        ORDER BY emotion_date
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                rows = await cursor.fetchall()
        return [str(row[0]).split(" ")[0] for row in rows if row and row[0]]

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

    async def batch_exchange_option_contract_daily_data(self, rows):
        if not rows:
            return 0

        await self.ensure_exchange_option_tables()
        sanitized_rows = [
            self._sanitize_exchange_option_contract_daily_row(row)
            for row in rows
        ]
        sanitized_rows = [
            row
            for row in sanitized_rows
            if row['exchange'] in {'SSE', 'SZSE'}
            and row['contract_code']
            and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        deduped = {
            (row['exchange'], row['contract_code'], row['trade_date']): row
            for row in sanitized_rows
        }
        values = [
            (
                row['exchange'],
                row['contract_code'],
                row['contract_trade_code'],
                row['contract_name'],
                row['underlying_code'],
                row['underlying_name'],
                row['option_type'],
                row['contract_month'],
                row['strike_price'],
                row['trade_date'],
                row['open_price'],
                row['high_price'],
                row['low_price'],
                row['close_price'],
                row['pre_close_price'],
                row['pre_settle_price'],
                row['pre_settle_source'],
                row['settle_price'],
                row['volume'],
                row['turnover'],
                row['open_interest'],
                row['delta_value'],
                row['theta_value'],
                row['gamma_value'],
                row['vega_value'],
                row['rho_value'],
                row['implied_volatility'],
                row['data_source'],
                row['source_url'],
                row['raw_json'],
            )
            for row in deduped.values()
        ]
        query = """
            INSERT INTO option_exchange_contract_daily_data (
                exchange,
                contract_code,
                contract_trade_code,
                contract_name,
                underlying_code,
                underlying_name,
                option_type,
                contract_month,
                strike_price,
                trade_date,
                open_price,
                high_price,
                low_price,
                close_price,
                pre_close_price,
                pre_settle_price,
                pre_settle_source,
                settle_price,
                volume,
                turnover,
                open_interest,
                delta_value,
                theta_value,
                gamma_value,
                vega_value,
                rho_value,
                implied_volatility,
                data_source,
                source_url,
                raw_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                contract_trade_code = COALESCE(VALUES(contract_trade_code), contract_trade_code),
                contract_name = COALESCE(VALUES(contract_name), contract_name),
                underlying_code = COALESCE(VALUES(underlying_code), underlying_code),
                underlying_name = COALESCE(VALUES(underlying_name), underlying_name),
                option_type = COALESCE(VALUES(option_type), option_type),
                contract_month = COALESCE(VALUES(contract_month), contract_month),
                strike_price = COALESCE(VALUES(strike_price), strike_price),
                open_price = COALESCE(VALUES(open_price), open_price),
                high_price = COALESCE(VALUES(high_price), high_price),
                low_price = COALESCE(VALUES(low_price), low_price),
                close_price = COALESCE(VALUES(close_price), close_price),
                pre_close_price = COALESCE(VALUES(pre_close_price), pre_close_price),
                pre_settle_price = COALESCE(VALUES(pre_settle_price), pre_settle_price),
                pre_settle_source = COALESCE(VALUES(pre_settle_source), pre_settle_source),
                settle_price = COALESCE(VALUES(settle_price), settle_price),
                volume = COALESCE(VALUES(volume), volume),
                turnover = COALESCE(VALUES(turnover), turnover),
                open_interest = COALESCE(VALUES(open_interest), open_interest),
                delta_value = COALESCE(VALUES(delta_value), delta_value),
                theta_value = COALESCE(VALUES(theta_value), theta_value),
                gamma_value = COALESCE(VALUES(gamma_value), gamma_value),
                vega_value = COALESCE(VALUES(vega_value), vega_value),
                rho_value = COALESCE(VALUES(rho_value), rho_value),
                implied_volatility = COALESCE(VALUES(implied_volatility), implied_volatility),
                data_source = VALUES(data_source),
                source_url = COALESCE(VALUES(source_url), source_url),
                raw_json = COALESCE(VALUES(raw_json), raw_json)
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def backfill_exchange_option_pre_settle_prices(self):
        await self.ensure_exchange_option_tables()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    UPDATE option_exchange_contract_daily_data
                    SET
                        pre_settle_price = close_price - CAST(
                            JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$[5]'))
                            AS DECIMAL(18, 6)
                        ),
                        pre_settle_source = 'szse_official_dayk_change'
                    WHERE exchange = 'SZSE'
                      AND data_source = 'szse_official_dayk'
                      AND pre_settle_price IS NULL
                      AND close_price IS NOT NULL
                      AND JSON_VALID(raw_json)
                      AND JSON_EXTRACT(raw_json, '$[5]') IS NOT NULL
                    """
                )
                szse_exact_rows = cursor.rowcount

                await cursor.execute(
                    """
                    UPDATE option_exchange_contract_daily_data
                    SET
                        pre_settle_price = pre_close_price,
                        pre_settle_source = 'legacy_realtime_pre_settle'
                    WHERE pre_settle_price IS NULL
                      AND pre_close_price IS NOT NULL
                    """
                )
                legacy_exact_rows = cursor.rowcount

                await cursor.execute(
                    """
                    UPDATE option_exchange_contract_daily_data target
                    INNER JOIN (
                        SELECT
                            id,
                            LAG(close_price) OVER (
                                PARTITION BY exchange, contract_code
                                ORDER BY trade_date
                            ) AS previous_close
                        FROM option_exchange_contract_daily_data
                    ) previous
                      ON previous.id = target.id
                    SET
                        target.pre_settle_price = previous.previous_close,
                        target.pre_settle_source = 'derived_previous_close'
                    WHERE target.pre_settle_price IS NULL
                      AND previous.previous_close IS NOT NULL
                    """
                )
                derived_rows = cursor.rowcount
                await conn.commit()
                return {
                    'szse_exact_rows': szse_exact_rows,
                    'legacy_exact_rows': legacy_exact_rows,
                    'derived_rows': derived_rows,
                }

    async def upsert_cn_risk_free_rate_daily(self, rows):
        if not rows:
            return 0
        await self.ensure_cn_risk_free_rate_table()
        values = []
        for row in rows:
            trade_date = str(row.get('trade_date') or '').split(' ')[0].strip()
            tenor_code = str(row.get('tenor_code') or '').strip().upper()
            tenor_days = int(row.get('tenor_days') or 0)
            rate_pct = self._normalize_numeric(
                'price_change_rate',
                row.get('rate_pct'),
            )
            rate_decimal = self._normalize_numeric(
                'price_change_rate',
                row.get('rate_decimal'),
            )
            if (
                not trade_date
                or not tenor_code
                or tenor_days <= 0
                or rate_pct is None
                or rate_decimal is None
            ):
                continue
            values.append(
                (
                    trade_date,
                    tenor_code,
                    tenor_days,
                    round(float(rate_pct), 8),
                    round(float(rate_decimal), 10),
                    str(row.get('data_source') or 'chinamoney_shibor').strip(),
                    str(row.get('source_url') or '').strip() or None,
                    self._serialize_json_field(row.get('raw_json')),
                )
            )
        if not values:
            return 0
        query = """
            INSERT INTO cn_risk_free_rate_daily (
                trade_date,
                tenor_code,
                tenor_days,
                rate_pct,
                rate_decimal,
                data_source,
                source_url,
                raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                tenor_days = VALUES(tenor_days),
                rate_pct = VALUES(rate_pct),
                rate_decimal = VALUES(rate_decimal),
                data_source = VALUES(data_source),
                source_url = VALUES(source_url),
                raw_json = VALUES(raw_json),
                updated_at = CURRENT_TIMESTAMP
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def get_cn_risk_free_rate_rows(self, start_date, end_date):
        await self.ensure_cn_risk_free_rate_table()
        query = """
            SELECT
                trade_date,
                tenor_code,
                tenor_days,
                rate_pct,
                rate_decimal,
                data_source
            FROM cn_risk_free_rate_daily
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date ASC, tenor_days ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (start_date, end_date))
                return list(await cursor.fetchall())

    async def batch_exchange_option_contract_info(self, rows):
        if not rows:
            return 0

        await self.ensure_exchange_option_tables()
        sanitized_rows = [
            self._sanitize_exchange_option_contract_info_row(row)
            for row in rows
        ]
        sanitized_rows = [
            row
            for row in sanitized_rows
            if row['exchange'] in {'SSE', 'SZSE'} and row['contract_code']
        ]
        if not sanitized_rows:
            return 0

        deduped = {
            (row['exchange'], row['contract_code']): row
            for row in sanitized_rows
        }
        values = [
            (
                row['exchange'],
                row['contract_code'],
                row['contract_trade_code'],
                row['contract_name'],
                row['underlying_code'],
                row['underlying_name'],
                row['option_type'],
                row['contract_month'],
                row['strike_price'],
                row['contract_unit'],
                row['listed_date'],
                row['last_trade_date'],
                row['exercise_date'],
                row['expire_date'],
                row['delivery_date'],
                row['listing_reason'],
                row['data_source'],
                row['source_url'],
                row['raw_json'],
            )
            for row in deduped.values()
        ]
        query = """
            INSERT INTO option_exchange_contract_info (
                exchange,
                contract_code,
                contract_trade_code,
                contract_name,
                underlying_code,
                underlying_name,
                option_type,
                contract_month,
                strike_price,
                contract_unit,
                listed_date,
                last_trade_date,
                exercise_date,
                expire_date,
                delivery_date,
                listing_reason,
                data_source,
                source_url,
                raw_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                contract_trade_code = COALESCE(VALUES(contract_trade_code), contract_trade_code),
                contract_name = COALESCE(VALUES(contract_name), contract_name),
                underlying_code = COALESCE(VALUES(underlying_code), underlying_code),
                underlying_name = COALESCE(VALUES(underlying_name), underlying_name),
                option_type = COALESCE(VALUES(option_type), option_type),
                contract_month = COALESCE(VALUES(contract_month), contract_month),
                strike_price = COALESCE(VALUES(strike_price), strike_price),
                contract_unit = COALESCE(VALUES(contract_unit), contract_unit),
                listed_date = COALESCE(VALUES(listed_date), listed_date),
                last_trade_date = COALESCE(VALUES(last_trade_date), last_trade_date),
                exercise_date = COALESCE(VALUES(exercise_date), exercise_date),
                expire_date = COALESCE(VALUES(expire_date), expire_date),
                delivery_date = COALESCE(VALUES(delivery_date), delivery_date),
                listing_reason = COALESCE(VALUES(listing_reason), listing_reason),
                data_source = VALUES(data_source),
                source_url = COALESCE(VALUES(source_url), source_url),
                raw_json = COALESCE(VALUES(raw_json), raw_json)
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def batch_exchange_option_daily_stats(self, rows):
        if not rows:
            return 0

        await self.ensure_exchange_option_tables()
        sanitized_rows = [
            self._sanitize_exchange_option_daily_stats_row(row)
            for row in rows
        ]
        sanitized_rows = [
            row
            for row in sanitized_rows
            if row['exchange'] in {'SSE', 'SZSE'}
            and row['underlying_code']
            and row['trade_date']
        ]
        if not sanitized_rows:
            return 0

        deduped = {
            (row['exchange'], row['underlying_code'], row['trade_date']): row
            for row in sanitized_rows
        }
        values = [
            (
                row['exchange'],
                row['underlying_code'],
                row['underlying_name'],
                row['trade_date'],
                row['contract_count'],
                row['turnover_amount'],
                row['total_volume'],
                row['call_volume'],
                row['put_volume'],
                row['put_call_volume_ratio'],
                row['open_interest'],
                row['call_open_interest'],
                row['put_open_interest'],
                row['data_source'],
                row['source_url'],
                row['raw_json'],
            )
            for row in deduped.values()
        ]
        query = """
            INSERT INTO option_exchange_daily_stats (
                exchange,
                underlying_code,
                underlying_name,
                trade_date,
                contract_count,
                turnover_amount,
                total_volume,
                call_volume,
                put_volume,
                put_call_volume_ratio,
                open_interest,
                call_open_interest,
                put_open_interest,
                data_source,
                source_url,
                raw_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                underlying_name = COALESCE(VALUES(underlying_name), underlying_name),
                contract_count = COALESCE(VALUES(contract_count), contract_count),
                turnover_amount = COALESCE(VALUES(turnover_amount), turnover_amount),
                total_volume = COALESCE(VALUES(total_volume), total_volume),
                call_volume = COALESCE(VALUES(call_volume), call_volume),
                put_volume = COALESCE(VALUES(put_volume), put_volume),
                put_call_volume_ratio = COALESCE(
                    VALUES(put_call_volume_ratio),
                    put_call_volume_ratio
                ),
                open_interest = COALESCE(VALUES(open_interest), open_interest),
                call_open_interest = COALESCE(
                    VALUES(call_open_interest),
                    call_open_interest
                ),
                put_open_interest = COALESCE(
                    VALUES(put_open_interest),
                    put_open_interest
                ),
                data_source = VALUES(data_source),
                source_url = COALESCE(VALUES(source_url), source_url),
                raw_json = COALESCE(VALUES(raw_json), raw_json)
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def list_exchange_option_contract_codes(self):
        await self.ensure_exchange_option_tables()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT exchange, contract_code
                    FROM option_exchange_contract_info
                    ORDER BY exchange, contract_code
                    """
                )
                return [
                    (str(row[0]).strip().upper(), str(row[1]).strip())
                    for row in await cursor.fetchall()
                ]

    async def list_exchange_option_active_contract_rows(self, target_date):
        await self.ensure_exchange_option_tables()
        target_text = str(target_date).split(" ")[0]
        query = """
        SELECT
            info.exchange,
            info.contract_code,
            info.contract_trade_code,
            info.contract_name,
            info.underlying_code,
            info.underlying_name,
            info.option_type,
            info.contract_month,
            info.strike_price,
            daily.open_price,
            daily.high_price,
            daily.low_price,
            daily.close_price,
            daily.volume,
            daily.turnover,
            daily.data_source AS daily_data_source
        FROM option_exchange_contract_info info
        LEFT JOIN option_exchange_contract_daily_data daily
          ON daily.exchange = info.exchange
         AND daily.contract_code = info.contract_code
         AND daily.trade_date = %s
        WHERE info.listed_date <= %s
          AND info.last_trade_date >= %s
          AND info.underlying_code IS NOT NULL
        ORDER BY info.exchange, info.contract_code
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [target_text, target_text, target_text])
                rows = list(await cursor.fetchall())
        return [
            {
                **row,
                'trade_date': target_text,
                'data_source': f"{str(row.get('exchange') or '').strip().lower()}_official_dayk",
                'official_complete': (
                    row.get('close_price') is not None
                    and row.get('volume') is not None
                    and row.get('turnover') is not None
                    and str(row.get('daily_data_source') or '').strip()
                    in {'sse_official_dayk', 'szse_official_dayk'}
                ),
            }
            for row in rows
        ]

    async def list_option_minute_exchange_contract_rows(
        self,
        start_date,
        end_date,
        underlying_codes,
    ):
        await self.ensure_exchange_option_tables()
        normalized_codes = [
            str(code).strip()
            for code in (underlying_codes or [])
            if str(code).strip()
        ]
        if not normalized_codes:
            return []
        placeholders = ",".join(["%s"] * len(normalized_codes))
        query = f"""
        SELECT
            exchange,
            contract_code,
            contract_trade_code,
            contract_name,
            underlying_code,
            underlying_name,
            option_type,
            contract_month,
            strike_price,
            listed_date,
            last_trade_date,
            COALESCE(expire_date, last_trade_date) AS expire_date
        FROM option_exchange_contract_info
        WHERE underlying_code IN ({placeholders})
          AND listed_date <= %s
          AND COALESCE(last_trade_date, expire_date) >= %s
          AND option_type IN ('CALL', 'PUT')
          AND strike_price IS NOT NULL
        ORDER BY exchange, underlying_code, expire_date, strike_price, option_type
        """
        params = [*normalized_codes, str(end_date), str(start_date)]
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def batch_option_contract_minute_data(self, rows):
        if not rows:
            return 0
        await self.ensure_option_minute_tables()
        deduped = {}
        for raw_row in rows:
            exchange = str(raw_row.get("exchange") or "").strip().upper()
            contract_code = str(raw_row.get("contract_code") or "").strip()
            underlying_code = str(raw_row.get("underlying_code") or "").strip().upper()
            option_type = str(raw_row.get("option_type") or "").strip().upper()
            contract_month = str(raw_row.get("contract_month") or "").strip()
            expire_date = str(raw_row.get("expire_date") or "").split(" ")[0]
            trade_date = str(raw_row.get("trade_date") or "").split(" ")[0]
            bar_time = str(raw_row.get("bar_time") or "").strip()
            strike_price = self._normalize_numeric(
                "close_price",
                raw_row.get("strike_price"),
            )
            strike_price = round(strike_price, 6) if strike_price is not None else None
            if (
                exchange not in {"CFFEX", "SSE", "SZSE"}
                or not contract_code
                or not underlying_code
                or option_type not in {"CALL", "PUT"}
                or not contract_month
                or strike_price is None
                or not expire_date
                or not trade_date
                or not bar_time
            ):
                continue
            row = {
                **raw_row,
                "exchange": exchange,
                "contract_code": contract_code,
                "contract_trade_code": str(
                    raw_row.get("contract_trade_code") or ""
                ).strip() or None,
                "underlying_code": underlying_code,
                "option_type": option_type,
                "contract_month": contract_month,
                "strike_price": strike_price,
                "expire_date": expire_date,
                "trade_date": trade_date,
                "bar_time": bar_time,
                "price_basis": str(
                    raw_row.get("price_basis") or "last"
                ).strip() or "last",
                "data_source": str(
                    raw_row.get("data_source") or "unknown"
                ).strip() or "unknown",
                "source_url": str(raw_row.get("source_url") or "").strip() or None,
            }
            for field in (
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "bid1_price",
                "bid1_volume",
                "ask1_price",
                "ask1_volume",
                "mid_price",
                "average_price",
                "minute_volume",
                "cumulative_volume",
                "cumulative_turnover",
                "open_interest",
            ):
                normalize_field = (
                    "volume"
                    if field.endswith("volume") or field == "open_interest"
                    else "turnover"
                    if field == "cumulative_turnover"
                    else "close_price"
                )
                row[field] = self._normalize_numeric(
                    normalize_field,
                    raw_row.get(field),
                )
                if row[field] is not None:
                    row[field] = round(
                        row[field],
                        2 if normalize_field in {"volume", "turnover"} else 8,
                    )
            raw_json = raw_row.get("raw_json")
            row["raw_json"] = (
                json.dumps(raw_json, ensure_ascii=False, default=str)
                if raw_json is not None and not isinstance(raw_json, str)
                else raw_json
            )
            row["quote_count"] = max(1, int(raw_row.get("quote_count") or 1))
            deduped[(exchange, contract_code, bar_time)] = row

        if not deduped:
            return 0
        values = [
            (
                row["exchange"],
                row["contract_code"],
                row["contract_trade_code"],
                row["underlying_code"],
                row["option_type"],
                row["contract_month"],
                row["strike_price"],
                row["expire_date"],
                row["trade_date"],
                row["bar_time"],
                row["open_price"],
                row["high_price"],
                row["low_price"],
                row["close_price"],
                row["bid1_price"],
                row["bid1_volume"],
                row["ask1_price"],
                row["ask1_volume"],
                row["mid_price"],
                row["average_price"],
                row["minute_volume"],
                row["cumulative_volume"],
                row["cumulative_turnover"],
                row["open_interest"],
                row["quote_count"],
                row["price_basis"],
                row["data_source"],
                row["source_url"],
                row["raw_json"],
            )
            for row in deduped.values()
        ]
        query = """
        INSERT INTO option_contract_minute_data (
            exchange, contract_code, contract_trade_code, underlying_code,
            option_type, contract_month, strike_price, expire_date,
            trade_date, bar_time, open_price, high_price, low_price, close_price,
            bid1_price, bid1_volume, ask1_price, ask1_volume, mid_price,
            average_price, minute_volume, cumulative_volume, cumulative_turnover,
            open_interest, quote_count, price_basis, data_source, source_url,
            raw_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            open_price = COALESCE(open_price, VALUES(open_price)),
            high_price = CASE
                WHEN VALUES(high_price) IS NULL THEN high_price
                WHEN high_price IS NULL THEN VALUES(high_price)
                ELSE GREATEST(high_price, VALUES(high_price))
            END,
            low_price = CASE
                WHEN VALUES(low_price) IS NULL THEN low_price
                WHEN low_price IS NULL THEN VALUES(low_price)
                ELSE LEAST(low_price, VALUES(low_price))
            END,
            close_price = COALESCE(VALUES(close_price), close_price),
            bid1_price = COALESCE(VALUES(bid1_price), bid1_price),
            bid1_volume = COALESCE(VALUES(bid1_volume), bid1_volume),
            ask1_price = COALESCE(VALUES(ask1_price), ask1_price),
            ask1_volume = COALESCE(VALUES(ask1_volume), ask1_volume),
            mid_price = COALESCE(VALUES(mid_price), mid_price),
            average_price = COALESCE(VALUES(average_price), average_price),
            minute_volume = COALESCE(VALUES(minute_volume), minute_volume),
            cumulative_volume = COALESCE(VALUES(cumulative_volume), cumulative_volume),
            cumulative_turnover = COALESCE(VALUES(cumulative_turnover), cumulative_turnover),
            open_interest = COALESCE(VALUES(open_interest), open_interest),
            quote_count = GREATEST(quote_count, VALUES(quote_count)),
            price_basis = VALUES(price_basis),
            data_source = VALUES(data_source),
            source_url = COALESCE(VALUES(source_url), source_url),
            raw_json = COALESCE(VALUES(raw_json), raw_json)
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for offset in range(0, len(values), 2000):
                    await cursor.executemany(query, values[offset : offset + 2000])
                await conn.commit()
        return len(values)

    async def get_option_contract_minute_rows(self, start_time, end_time):
        await self.ensure_option_minute_tables()
        query = """
        SELECT
            exchange, contract_code, contract_trade_code, underlying_code,
            option_type, contract_month, strike_price, expire_date,
            trade_date, bar_time, open_price, high_price, low_price, close_price,
            bid1_price, bid1_volume, ask1_price, ask1_volume, mid_price,
            average_price, minute_volume, cumulative_volume, cumulative_turnover,
            open_interest, price_basis, data_source
        FROM option_contract_minute_data
        WHERE bar_time BETWEEN %s AND %s
        ORDER BY bar_time, exchange, underlying_code, expire_date, strike_price,
                 option_type
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (str(start_time), str(end_time)))
                return list(await cursor.fetchall())

    async def batch_option_vix_minute_data(self, rows):
        if not rows:
            return 0
        await self.ensure_option_minute_tables()
        values = []
        for row in rows:
            exchange = str(row.get("exchange") or "").strip().upper()
            product_code = str(row.get("product_code") or "").strip().upper()
            source_key = f"{exchange.lower()}:{product_code}" if exchange and product_code else ""
            vix_value = self._normalize_numeric("close_value", row.get("vix_value"))
            if vix_value is None:
                continue
            vix_value = round(vix_value, 8)
            near_rate = self._normalize_numeric(
                "close_value", row.get("near_risk_free_rate")
            )
            next_rate = self._normalize_numeric(
                "close_value", row.get("next_risk_free_rate")
            )
            quality_json = row.get("quality_json")
            if quality_json is not None and not isinstance(quality_json, str):
                quality_json = json.dumps(
                    quality_json,
                    ensure_ascii=False,
                    default=str,
                )
            values.append(
                (
                    str(row.get("index_name") or "").strip(),
                    source_key,
                    exchange,
                    product_code,
                    str(row.get("trade_date") or "").split(" ")[0],
                    str(row.get("bar_time") or "").strip(),
                    vix_value,
                    row.get("near_contract_month"),
                    row.get("near_expire_date"),
                    row.get("near_strike_count"),
                    row.get("next_contract_month"),
                    row.get("next_expire_date"),
                    row.get("next_strike_count"),
                    row.get("risk_free_curve_date"),
                    round(near_rate, 10) if near_rate is not None else None,
                    round(next_rate, 10) if next_rate is not None else None,
                    str(row.get("price_basis") or "mid_quote").strip(),
                    str(row.get("calculation_method") or "ivix_30d_minute").strip(),
                    quality_json,
                )
            )
        values = [value for value in values if all(value[index] for index in (0, 1, 2, 3, 4, 5))]
        if not values:
            return 0
        query = """
        INSERT INTO option_vix_minute_data (
            index_name, source_key, exchange, product_code, trade_date, bar_time,
            vix_value, near_contract_month, near_expire_date, near_strike_count,
            next_contract_month, next_expire_date, next_strike_count,
            risk_free_curve_date, near_risk_free_rate, next_risk_free_rate,
            price_basis, calculation_method, quality_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            vix_value = VALUES(vix_value),
            near_contract_month = VALUES(near_contract_month),
            near_expire_date = VALUES(near_expire_date),
            near_strike_count = VALUES(near_strike_count),
            next_contract_month = VALUES(next_contract_month),
            next_expire_date = VALUES(next_expire_date),
            next_strike_count = VALUES(next_strike_count),
            risk_free_curve_date = VALUES(risk_free_curve_date),
            near_risk_free_rate = VALUES(near_risk_free_rate),
            next_risk_free_rate = VALUES(next_risk_free_rate),
            price_basis = VALUES(price_basis),
            calculation_method = VALUES(calculation_method),
            quality_json = VALUES(quality_json)
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def get_option_vix_minute_daily_ohlc(self, start_date, end_date):
        await self.ensure_option_minute_tables()
        query = """
        SELECT
            aggregate_rows.trade_date,
            aggregate_rows.source_key,
            aggregate_rows.index_name,
            aggregate_rows.exchange,
            aggregate_rows.product_code,
            open_row.vix_value AS vix_open,
            aggregate_rows.vix_high,
            aggregate_rows.vix_low,
            close_row.vix_value AS vix_close,
            aggregate_rows.minute_count,
            aggregate_rows.mid_quote_count,
            close_row.near_contract_month,
            close_row.near_expire_date,
            close_row.near_strike_count,
            close_row.next_contract_month,
            close_row.next_expire_date,
            close_row.next_strike_count,
            close_row.risk_free_curve_date,
            close_row.near_risk_free_rate,
            close_row.next_risk_free_rate,
            close_row.price_basis,
            close_row.calculation_method
        FROM (
            SELECT
                trade_date,
                source_key,
                MAX(index_name) AS index_name,
                MAX(exchange) AS exchange,
                MAX(product_code) AS product_code,
                MIN(bar_time) AS first_bar_time,
                MAX(bar_time) AS last_bar_time,
                MAX(vix_value) AS vix_high,
                MIN(vix_value) AS vix_low,
                COUNT(*) AS minute_count,
                SUM(price_basis = 'mid_quote') AS mid_quote_count
            FROM option_vix_minute_data
            WHERE trade_date BETWEEN %s AND %s
            GROUP BY trade_date, source_key
        ) AS aggregate_rows
        INNER JOIN option_vix_minute_data AS open_row
          ON open_row.trade_date = aggregate_rows.trade_date
         AND open_row.source_key = aggregate_rows.source_key
         AND open_row.bar_time = aggregate_rows.first_bar_time
        INNER JOIN option_vix_minute_data AS close_row
          ON close_row.trade_date = aggregate_rows.trade_date
         AND close_row.source_key = aggregate_rows.source_key
         AND close_row.bar_time = aggregate_rows.last_bar_time
        ORDER BY aggregate_rows.trade_date, aggregate_rows.source_key
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (str(start_date), str(end_date)))
                return list(await cursor.fetchall())

    async def summarize_option_minute_trade_date(self, trade_date):
        await self.ensure_option_minute_tables()
        query = """
        SELECT
            exchange,
            underlying_code,
            COUNT(*) AS row_count,
            COUNT(DISTINCT contract_code) AS contract_count,
            COUNT(DISTINCT bar_time) AS minute_count,
            SUM(mid_price IS NOT NULL) AS midpoint_count,
            MIN(bar_time) AS first_bar_time,
            MAX(bar_time) AS last_bar_time
        FROM option_contract_minute_data
        WHERE trade_date = %s
        GROUP BY exchange, underlying_code
        ORDER BY exchange, underlying_code
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (str(trade_date),))
                return list(await cursor.fetchall())

    async def count_exchange_option_contract_info(self, exchange):
        await self.ensure_exchange_option_tables()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM option_exchange_contract_info
                    WHERE exchange = %s
                    """,
                    [str(exchange).strip().upper()],
                )
                row = await cursor.fetchone()
                return int(row[0] or 0) if row else 0

    async def list_exchange_option_stats_dates(self, exchange, start_date, end_date):
        await self.ensure_exchange_option_tables()
        normalized_exchange = str(exchange or '').strip().upper()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM option_exchange_daily_stats
                    WHERE exchange = %s
                      AND trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                    """,
                    [normalized_exchange, str(start_date), str(end_date)],
                )
                return {
                    row[0] if isinstance(row[0], date) else datetime.strptime(
                        str(row[0]).split(" ")[0],
                        "%Y-%m-%d",
                    ).date()
                    for row in await cursor.fetchall()
                }

    async def list_exchange_option_risk_dates(self, exchange, start_date, end_date):
        await self.ensure_exchange_option_tables()
        normalized_exchange = str(exchange or '').strip().upper()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM option_exchange_contract_daily_data
                    WHERE exchange = %s
                      AND trade_date BETWEEN %s AND %s
                      AND delta_value IS NOT NULL
                    ORDER BY trade_date
                    """,
                    [normalized_exchange, str(start_date), str(end_date)],
                )
                return {
                    row[0] if isinstance(row[0], date) else datetime.strptime(
                        str(row[0]).split(" ")[0],
                        "%Y-%m-%d",
                    ).date()
                    for row in await cursor.fetchall()
                }

    async def list_cn_trade_dates(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM index_daily_data
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                    """,
                    [str(start_date), str(end_date)],
                )
                return [
                    row[0] if isinstance(row[0], date) else datetime.strptime(
                        str(row[0]).split(" ")[0],
                        "%Y-%m-%d",
                    ).date()
                    for row in await cursor.fetchall()
                ]

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

    async def _upsert_index_daily_snapshots_for_table(self, table_name, updates):
        if not updates:
            return 0

        if self.pool is None:
            await self.init_pool()

        normalized_table_name = self._validate_table_name(table_name, self.INDEX_DAILY_TABLES)

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
                query_upsert = f"""
                INSERT INTO {normalized_table_name} (
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

    async def upsert_index_daily_snapshots(self, updates):
        return await self._upsert_index_daily_snapshots_for_table('index_daily_data', updates)

    async def upsert_index_us_daily_snapshots(self, updates):
        return await self._upsert_index_daily_snapshots_for_table('index_us_daily_data', updates)

    async def upsert_index_hk_daily_snapshots(self, updates):
        return await self._upsert_index_daily_snapshots_for_table('index_hk_daily_data', updates)

    async def upsert_index_qvix_daily_snapshots(self, updates):
        return await self._upsert_index_daily_snapshots_for_table('index_qvix_daily_data', updates)

    async def upsert_index_news_sentiment_scope_daily(self, updates):
        if not updates:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_updates = [self._sanitize_index_news_sentiment_scope_row(update) for update in updates]
        sanitized_updates = [
            update for update in sanitized_updates
            if update['trade_date']
        ]
        if not sanitized_updates:
            return 0

        deduped_updates = {}
        for update in sanitized_updates:
            deduped_updates[update['trade_date']] = update
        sanitized_updates = list(deduped_updates.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_news_sentiment_scope_daily (
                    trade_date,
                    sentiment_value,
                    hs300_close,
                    data_source
                ) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    sentiment_value = VALUES(sentiment_value),
                    hs300_close = VALUES(hs300_close),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        update['trade_date'],
                        update['sentiment_value'],
                        update['hs300_close'],
                        update['data_source'],
                    )
                    for update in sanitized_updates
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_updates)

    async def upsert_index_us_vix_daily(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_index_us_vix_daily_row(row) for row in rows]
        sanitized_rows = [row for row in sanitized_rows if row['trade_date']]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[row['trade_date']] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_us_vix_daily (
                    trade_date,
                    open_value,
                    high_value,
                    low_value,
                    close_value,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open_value = VALUES(open_value),
                    high_value = VALUES(high_value),
                    low_value = VALUES(low_value),
                    close_value = VALUES(close_value),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['trade_date'],
                        row['open_value'],
                        row['high_value'],
                        row['low_value'],
                        row['close_value'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def upsert_index_us_fear_greed_daily(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_index_us_fear_greed_daily_row(row) for row in rows]
        sanitized_rows = [row for row in sanitized_rows if row['trade_date']]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[row['trade_date']] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_us_fear_greed_daily (
                    trade_date,
                    fear_greed_value,
                    sentiment_label,
                    data_source
                ) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    fear_greed_value = VALUES(fear_greed_value),
                    sentiment_label = VALUES(sentiment_label),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['trade_date'],
                        row['fear_greed_value'],
                        row['sentiment_label'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def ensure_index_cn_market_fear_greed_daily_table(self):
        if self.pool is None:
            await self.init_pool()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS index_cn_market_fear_greed_daily (
                        trade_date DATE NOT NULL PRIMARY KEY,
                        fear_greed_value DECIMAL(10,4) NOT NULL,
                        sentiment_label VARCHAR(32) NULL,
                        locked TINYINT(1) NOT NULL DEFAULT 0,
                        data_source VARCHAR(64) NOT NULL DEFAULT 'miumiu_market_fear_greed',
                        raw_json JSON NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY idx_cn_market_fear_greed_source (data_source)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await conn.commit()

    async def upsert_index_cn_market_fear_greed_daily(self, rows):
        if not rows:
            return 0
        if self.pool is None:
            await self.init_pool()
        await self.ensure_index_cn_market_fear_greed_daily_table()
        sanitized_rows = [self._sanitize_index_cn_market_fear_greed_daily_row(row) for row in rows]
        sanitized_rows = [row for row in sanitized_rows if row['trade_date'] and row['fear_greed_value'] is not None]
        sanitized_rows = list({row['trade_date']: row for row in sanitized_rows}.values())
        if not sanitized_rows:
            return 0
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    """
                    INSERT INTO index_cn_market_fear_greed_daily (
                        trade_date, fear_greed_value, sentiment_label, locked, data_source, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        fear_greed_value = VALUES(fear_greed_value),
                        sentiment_label = VALUES(sentiment_label),
                        locked = VALUES(locked),
                        data_source = VALUES(data_source),
                        raw_json = VALUES(raw_json),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [(
                        row['trade_date'], row['fear_greed_value'], row['sentiment_label'],
                        row['locked'], row['data_source'], row['raw_json'],
                    ) for row in sanitized_rows],
                )
                await conn.commit()
                return len(sanitized_rows)

    async def ensure_index_cn_baifenwei_fear_greed_daily_table(self):
        if self.pool is None:
            await self.init_pool()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS index_cn_baifenwei_fear_greed_daily (
                        trade_date DATE NOT NULL PRIMARY KEY,
                        fear_greed_value DECIMAL(10,4) NOT NULL,
                        sentiment_label VARCHAR(32) NULL,
                        volatility_score DECIMAL(10,4) NOT NULL,
                        relative_turnover_score DECIMAL(10,4) NOT NULL,
                        margin_trading_score DECIMAL(10,4) NOT NULL,
                        market_breadth_score DECIMAL(10,4) NOT NULL,
                        rsi_score DECIMAL(10,4) NOT NULL,
                        limit_up_down_ratio_score DECIMAL(10,4) NOT NULL,
                        market_index_value DECIMAL(14,4) NULL,
                        value_origin VARCHAR(32) NOT NULL,
                        data_source VARCHAR(64) NOT NULL DEFAULT 'baifenwei_fear_greed',
                        source_generated_at DATETIME NULL,
                        raw_json JSON NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY idx_cn_baifenwei_fear_greed_origin (value_origin),
                        KEY idx_cn_baifenwei_fear_greed_source (data_source)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await conn.commit()

    async def upsert_index_cn_baifenwei_fear_greed_daily(self, rows):
        if not rows:
            return 0
        if self.pool is None:
            await self.init_pool()
        await self.ensure_index_cn_baifenwei_fear_greed_daily_table()
        sanitized_rows = [self._sanitize_index_cn_baifenwei_fear_greed_daily_row(row) for row in rows]
        required_fields = (
            'fear_greed_value',
            'volatility_score',
            'relative_turnover_score',
            'margin_trading_score',
            'market_breadth_score',
            'rsi_score',
            'limit_up_down_ratio_score',
        )
        sanitized_rows = [
            row for row in sanitized_rows
            if row['trade_date'] and all(row.get(field_name) is not None for field_name in required_fields)
        ]
        sanitized_rows = list({row['trade_date']: row for row in sanitized_rows}.values())
        if not sanitized_rows:
            return 0
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    """
                    INSERT INTO index_cn_baifenwei_fear_greed_daily (
                        trade_date, fear_greed_value, sentiment_label,
                        volatility_score, relative_turnover_score, margin_trading_score,
                        market_breadth_score, rsi_score, limit_up_down_ratio_score,
                        market_index_value, value_origin, data_source, source_generated_at, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        fear_greed_value = VALUES(fear_greed_value),
                        sentiment_label = VALUES(sentiment_label),
                        volatility_score = VALUES(volatility_score),
                        relative_turnover_score = VALUES(relative_turnover_score),
                        margin_trading_score = VALUES(margin_trading_score),
                        market_breadth_score = VALUES(market_breadth_score),
                        rsi_score = VALUES(rsi_score),
                        limit_up_down_ratio_score = VALUES(limit_up_down_ratio_score),
                        market_index_value = VALUES(market_index_value),
                        value_origin = VALUES(value_origin),
                        data_source = VALUES(data_source),
                        source_generated_at = VALUES(source_generated_at),
                        raw_json = VALUES(raw_json),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [(
                        row['trade_date'], row['fear_greed_value'], row['sentiment_label'],
                        row['volatility_score'], row['relative_turnover_score'], row['margin_trading_score'],
                        row['market_breadth_score'], row['rsi_score'], row['limit_up_down_ratio_score'],
                        row['market_index_value'], row['value_origin'], row['data_source'],
                        row['source_generated_at'], row['raw_json'],
                    ) for row in sanitized_rows],
                )
                await conn.commit()
                return len(sanitized_rows)

    async def upsert_index_us_hedge_fund_ls_proxy(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_index_us_hedge_fund_ls_proxy_row(row) for row in rows]
        sanitized_rows = [
            row
            for row in sanitized_rows
            if row['report_date'] and row['contract_scope']
        ]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[(row['report_date'], row['contract_scope'])] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_us_hedge_fund_ls_proxy (
                    report_date,
                    contract_scope,
                    long_value,
                    short_value,
                    ratio_value,
                    release_date,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    long_value = VALUES(long_value),
                    short_value = VALUES(short_value),
                    ratio_value = VALUES(ratio_value),
                    release_date = VALUES(release_date),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['report_date'],
                        row['contract_scope'],
                        row['long_value'],
                        row['short_value'],
                        row['ratio_value'],
                        row['release_date'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def get_latest_index_us_hedge_fund_ls_proxy_dates(self):
        if self.pool is None:
            await self.init_pool()

        query = """
        SELECT contract_scope, MAX(report_date)
        FROM index_us_hedge_fund_ls_proxy
        GROUP BY contract_scope
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()
                return {
                    str(contract_scope).strip().upper(): str(report_date)
                    for contract_scope, report_date in rows
                    if contract_scope and report_date
                }

    async def ensure_index_us_macro_auxiliary_tables(self):
        if self.pool is None:
            await self.init_pool()

        statements = [
            """
            CREATE TABLE IF NOT EXISTS index_us_put_call_ratio_daily (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              trade_date DATE NOT NULL COMMENT 'Trading date',
              total_put_call_ratio DECIMAL(10, 4) NULL COMMENT 'Total put call ratio',
              index_put_call_ratio DECIMAL(10, 4) NULL COMMENT 'Index options put call ratio',
              equity_put_call_ratio DECIMAL(10, 4) NULL COMMENT 'Equity options put call ratio',
              etf_put_call_ratio DECIMAL(10, 4) NULL COMMENT 'ETF options put call ratio',
              data_source VARCHAR(64) NOT NULL DEFAULT 'cboe_market_statistics' COMMENT 'Data source',
              created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY uk_index_us_put_call_ratio_trade_date (trade_date),
              KEY idx_index_us_put_call_ratio_trade_date (trade_date)
            ) COMMENT='Daily US options put call ratio from Cboe'
            """,
            """
            CREATE TABLE IF NOT EXISTS index_us_treasury_yield_daily (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              trade_date DATE NOT NULL COMMENT 'Trading date',
              yield_3m DECIMAL(10, 4) NULL COMMENT 'US Treasury 3 month yield',
              yield_2y DECIMAL(10, 4) NULL COMMENT 'US Treasury 2 year yield',
              yield_10y DECIMAL(10, 4) NULL COMMENT 'US Treasury 10 year yield',
              yield_real_10y DECIMAL(10, 4) NULL COMMENT 'US Treasury 10 year real yield (DFII10)',
              spread_10y_2y DECIMAL(10, 4) NULL COMMENT '10Y minus 2Y spread',
              spread_10y_3m DECIMAL(10, 4) NULL COMMENT '10Y minus 3M spread',
              available_at DATETIME NULL COMMENT 'Publicly available time (Asia/Shanghai)',
              data_source VARCHAR(64) NOT NULL DEFAULT 'fred_public_csv' COMMENT 'Data source',
              created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY uk_index_us_treasury_yield_trade_date (trade_date),
              KEY idx_index_us_treasury_yield_trade_date (trade_date)
            ) COMMENT='Daily US Treasury yield and spread data from FRED'
            """,
            """
            CREATE TABLE IF NOT EXISTS index_us_credit_spread_daily (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              trade_date DATE NOT NULL COMMENT 'Trading date',
              high_yield_oas DECIMAL(10, 4) NULL COMMENT 'US high yield option-adjusted spread',
              data_source VARCHAR(64) NOT NULL DEFAULT 'fred_public_csv' COMMENT 'Data source',
              created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY uk_index_us_credit_spread_trade_date (trade_date),
              KEY idx_index_us_credit_spread_trade_date (trade_date)
            ) COMMENT='Daily US high yield credit spread data from FRED'
            """,
        ]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for statement in statements:
                    await cursor.execute(statement)
                await conn.commit()

    async def upsert_index_us_put_call_ratio_daily(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()
        await self.ensure_index_us_macro_auxiliary_tables()

        sanitized_rows = [self._sanitize_index_us_put_call_ratio_row(row) for row in rows]
        sanitized_rows = [row for row in sanitized_rows if row['trade_date']]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[row['trade_date']] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_us_put_call_ratio_daily (
                    trade_date,
                    total_put_call_ratio,
                    index_put_call_ratio,
                    equity_put_call_ratio,
                    etf_put_call_ratio,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    total_put_call_ratio = VALUES(total_put_call_ratio),
                    index_put_call_ratio = VALUES(index_put_call_ratio),
                    equity_put_call_ratio = VALUES(equity_put_call_ratio),
                    etf_put_call_ratio = VALUES(etf_put_call_ratio),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['trade_date'],
                        row['total_put_call_ratio'],
                        row['index_put_call_ratio'],
                        row['equity_put_call_ratio'],
                        row['etf_put_call_ratio'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def upsert_index_us_treasury_yield_daily(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()
        await self.ensure_index_us_macro_auxiliary_tables()

        sanitized_rows = [self._sanitize_index_us_treasury_yield_row(row) for row in rows]
        sanitized_rows = [row for row in sanitized_rows if row['trade_date']]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[row['trade_date']] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_us_treasury_yield_daily (
                    trade_date,
                    yield_3m,
                    yield_2y,
                    yield_10y,
                    yield_real_10y,
                    spread_10y_2y,
                    spread_10y_3m,
                    available_at,
                    data_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    yield_3m = COALESCE(VALUES(yield_3m), yield_3m),
                    yield_2y = COALESCE(VALUES(yield_2y), yield_2y),
                    yield_10y = COALESCE(VALUES(yield_10y), yield_10y),
                    yield_real_10y = COALESCE(VALUES(yield_real_10y), yield_real_10y),
                    spread_10y_2y = COALESCE(VALUES(spread_10y_2y), spread_10y_2y),
                    spread_10y_3m = COALESCE(VALUES(spread_10y_3m), spread_10y_3m),
                    available_at = COALESCE(VALUES(available_at), available_at),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['trade_date'],
                        row['yield_3m'],
                        row['yield_2y'],
                        row['yield_10y'],
                        row['yield_real_10y'],
                        row['spread_10y_2y'],
                        row['spread_10y_3m'],
                        row['available_at'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

    async def upsert_index_us_credit_spread_daily(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()
        await self.ensure_index_us_macro_auxiliary_tables()

        sanitized_rows = [self._sanitize_index_us_credit_spread_row(row) for row in rows]
        sanitized_rows = [row for row in sanitized_rows if row['trade_date']]
        if not sanitized_rows:
            return 0

        deduped_rows = {}
        for row in sanitized_rows:
            deduped_rows[row['trade_date']] = row
        sanitized_rows = list(deduped_rows.values())

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query_upsert = """
                INSERT INTO index_us_credit_spread_daily (
                    trade_date,
                    high_yield_oas,
                    data_source
                ) VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    high_yield_oas = VALUES(high_yield_oas),
                    data_source = VALUES(data_source),
                    updated_at = CURRENT_TIMESTAMP
                """
                values = [
                    (
                        row['trade_date'],
                        row['high_yield_oas'],
                        row['data_source'],
                    )
                    for row in sanitized_rows
                ]
                await cursor.executemany(query_upsert, values)
                await conn.commit()
                return len(sanitized_rows)

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

    async def get_index_codes_by_names_for_market(self, index_names, market='cn'):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return {}

        normalized_market = str(market or 'cn').strip().lower()
        table_by_market = {
            'cn': 'index_basic_info',
            'hk': 'index_hk_basic_info',
            'us': 'index_us_basic_info',
        }
        table_name = table_by_market.get(normalized_market, 'index_basic_info')

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT index_name, index_code "
            f"FROM {table_name} "
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

    async def get_quant_index_dashboard_trade_dates_for_market(self, index_names, market='cn', start_date=None, end_date=None):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return []

        normalized_market = str(market or 'cn').strip().lower()
        table_by_market = {
            'cn': ('index_daily_data', 'index_basic_info'),
            'hk': ('index_hk_daily_data', 'index_hk_basic_info'),
            'us': ('index_us_daily_data', 'index_us_basic_info'),
        }
        daily_table, basic_table = table_by_market.get(normalized_market, table_by_market['cn'])

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT DISTINCT d.trade_date "
            f"FROM {daily_table} d "
            f"INNER JOIN {basic_table} b ON b.index_code = d.index_code "
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

    async def get_quant_index_dashboard_index_closes_for_market(self, index_names, market, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        normalized_names = [
            str(index_name).strip()
            for index_name in (index_names or [])
            if str(index_name).strip()
        ]
        if not normalized_names:
            return []

        normalized_market = str(market or 'cn').strip().lower()
        table_by_market = {
            'cn': ('index_daily_data', 'index_basic_info'),
            'hk': ('index_hk_daily_data', 'index_hk_basic_info'),
            'us': ('index_us_daily_data', 'index_us_basic_info'),
        }
        daily_table, basic_table = table_by_market.get(normalized_market, table_by_market['cn'])

        placeholders = ','.join(['%s'] * len(normalized_names))
        query = (
            f"SELECT b.index_name, d.trade_date, d.close_price "
            f"FROM {daily_table} d "
            f"INNER JOIN {basic_table} b ON b.index_code = d.index_code "
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

    async def get_quant_index_dashboard_us_index_futures_closes(self, root_symbols, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        normalized_symbols = [
            str(symbol).strip().upper()
            for symbol in (root_symbols or [])
            if str(symbol).strip()
        ]
        if not normalized_symbols:
            return []

        placeholders = ','.join(['%s'] * len(normalized_symbols))
        query = (
            f"SELECT trade_date, root_symbol, source_contract_code, close_price "
            f"FROM futures_us_index_daily_data "
            f"WHERE root_symbol IN ({placeholders}) "
            f"AND trade_date BETWEEN %s AND %s "
            f"AND close_price IS NOT NULL "
            f"ORDER BY trade_date ASC, root_symbol ASC"
        )
        params = [*normalized_symbols, str(start_date), str(end_date)]

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def get_quant_index_dashboard_hk_index_futures_closes(self, root_symbols, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        normalized_symbols = [
            str(symbol).strip().upper()
            for symbol in (root_symbols or [])
            if str(symbol).strip()
        ]
        if not normalized_symbols:
            return []

        placeholders = ','.join(['%s'] * len(normalized_symbols))
        query = (
            f"SELECT trade_date, root_symbol, source_contract_code, contract_month, "
            f"close_price, volume, open_interest "
            f"FROM futures_hk_index_daily_data "
            f"WHERE root_symbol IN ({placeholders}) "
            f"AND trade_date BETWEEN %s AND %s "
            f"AND close_price IS NOT NULL "
            f"ORDER BY trade_date ASC, root_symbol ASC, contract_month ASC"
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

    async def ensure_quant_index_dashboard_option_pc_columns(self):
        if self._quant_index_dashboard_option_pc_columns_ready:
            return
        if self.pool is None:
            await self.init_pool()

        column_definitions = [
            (
                'option_pc_current_month',
                "ADD COLUMN option_pc_current_month DECIMAL(18, 6) NULL COMMENT '当月股指期权Put/Call价格比' AFTER breadth_up_pct",
            ),
            (
                'option_pc_current_month_contract_month',
                "ADD COLUMN option_pc_current_month_contract_month VARCHAR(16) NULL COMMENT '当月股指期权合约月份' AFTER option_pc_current_month",
            ),
            (
                'option_pc_current_month_special_flag',
                "ADD COLUMN option_pc_current_month_special_flag TINYINT(1) NOT NULL DEFAULT 0 COMMENT '当月Put/Call是否使用特殊点位计算' AFTER option_pc_current_month_contract_month",
            ),
            (
                'option_pc_current_month_special_note',
                "ADD COLUMN option_pc_current_month_special_note VARCHAR(512) NULL COMMENT '当月Put/Call特殊点位说明' AFTER option_pc_current_month_special_flag",
            ),
            (
                'option_pc_next_month',
                "ADD COLUMN option_pc_next_month DECIMAL(18, 6) NULL COMMENT '下月股指期权Put/Call价格比' AFTER option_pc_current_month_special_note",
            ),
            (
                'option_pc_next_month_contract_month',
                "ADD COLUMN option_pc_next_month_contract_month VARCHAR(16) NULL COMMENT '下月股指期权合约月份' AFTER option_pc_next_month",
            ),
            (
                'option_pc_next_month_special_flag',
                "ADD COLUMN option_pc_next_month_special_flag TINYINT(1) NOT NULL DEFAULT 0 COMMENT '下月Put/Call是否使用特殊点位计算' AFTER option_pc_next_month_contract_month",
            ),
            (
                'option_pc_next_month_special_note',
                "ADD COLUMN option_pc_next_month_special_note VARCHAR(512) NULL COMMENT '下月Put/Call特殊点位说明' AFTER option_pc_next_month_special_flag",
            ),
            (
                'option_pc_quarter_1',
                "ADD COLUMN option_pc_quarter_1 DECIMAL(18, 6) NULL COMMENT '第一季月股指期权Put/Call价格比' AFTER option_pc_next_month_special_note",
            ),
            (
                'option_pc_quarter_1_contract_month',
                "ADD COLUMN option_pc_quarter_1_contract_month VARCHAR(16) NULL COMMENT '第一季月股指期权合约月份' AFTER option_pc_quarter_1",
            ),
            (
                'option_pc_quarter_1_special_flag',
                "ADD COLUMN option_pc_quarter_1_special_flag TINYINT(1) NOT NULL DEFAULT 0 COMMENT '季月1 Put/Call是否使用特殊点位计算' AFTER option_pc_quarter_1_contract_month",
            ),
            (
                'option_pc_quarter_1_special_note',
                "ADD COLUMN option_pc_quarter_1_special_note VARCHAR(512) NULL COMMENT '季月1 Put/Call特殊点位说明' AFTER option_pc_quarter_1_special_flag",
            ),
            (
                'option_pc_quarter_2',
                "ADD COLUMN option_pc_quarter_2 DECIMAL(18, 6) NULL COMMENT '第二季月股指期权Put/Call价格比' AFTER option_pc_quarter_1_special_note",
            ),
            (
                'option_pc_quarter_2_contract_month',
                "ADD COLUMN option_pc_quarter_2_contract_month VARCHAR(16) NULL COMMENT '第二季月股指期权合约月份' AFTER option_pc_quarter_2",
            ),
            (
                'option_pc_quarter_2_special_flag',
                "ADD COLUMN option_pc_quarter_2_special_flag TINYINT(1) NOT NULL DEFAULT 0 COMMENT '季月2 Put/Call是否使用特殊点位计算' AFTER option_pc_quarter_2_contract_month",
            ),
            (
                'option_pc_quarter_2_special_note',
                "ADD COLUMN option_pc_quarter_2_special_note VARCHAR(512) NULL COMMENT '季月2 Put/Call特殊点位说明' AFTER option_pc_quarter_2_special_flag",
            ),
            (
                'option_volume_pc_ratio',
                "ADD COLUMN option_volume_pc_ratio DECIMAL(18, 6) NULL COMMENT '股指期权成交量Put/Call比' AFTER option_pc_quarter_2_special_note",
            ),
            (
                'option_turnover_pc_ratio',
                "ADD COLUMN option_turnover_pc_ratio DECIMAL(18, 6) NULL COMMENT '股指期权成交额Put/Call比' AFTER option_volume_pc_ratio",
            ),
            (
                'exchange_option_pc_json',
                "ADD COLUMN exchange_option_pc_json JSON NULL COMMENT '沪深交易所期权分产品Put/Call指标' AFTER option_turnover_pc_ratio",
            ),
            (
                'option_vix_json',
                "ADD COLUMN option_vix_json JSON NULL COMMENT '按交易所和期权产品独立计算的30日VIX' AFTER exchange_option_pc_json",
            ),
            (
                'self_sentiment_score',
                "ADD COLUMN self_sentiment_score DECIMAL(18, 6) NULL COMMENT '自建情绪综合分' AFTER option_vix_json",
            ),
            (
                'self_sentiment_core_score',
                "ADD COLUMN self_sentiment_core_score DECIMAL(18, 6) NULL COMMENT '自建情绪价格核心分' AFTER self_sentiment_score",
            ),
            (
                'self_sentiment_derivative_score',
                "ADD COLUMN self_sentiment_derivative_score DECIMAL(18, 6) NULL COMMENT '自建情绪衍生品分' AFTER self_sentiment_core_score",
            ),
            (
                'self_sentiment_components_json',
                "ADD COLUMN self_sentiment_components_json JSON NULL COMMENT '自建情绪分项、原始值和算法版本' AFTER self_sentiment_derivative_score",
            ),
        ]
        source_label_by_prefix = {
            'top20': '前20机构',
            'citic': '中信代客',
        }
        previous_column = 'self_sentiment_components_json'
        for field in self.QUANT_INDEX_CFFEX_NET_SHORT_DELTA_FIELDS:
            parts = field.split('_')
            source_prefix = parts[1] if len(parts) > 1 else ''
            window = parts[-1].removesuffix('d')
            source_label = source_label_by_prefix.get(source_prefix, source_prefix)
            column_definitions.append(
                (
                    field,
                    f"ADD COLUMN {field} DECIMAL(18, 6) NULL COMMENT '{source_label}股指期货净空单{window}交易日增量' AFTER {previous_column}",
                )
            )
            previous_column = field
        risk_columns = (
            ("risk_yellow_vulnerability", "TINYINT(1) NULL COMMENT '中证1000黄色脆弱期状态'"),
            ("risk_yellow_vulnerability_score", "DECIMAL(18, 6) NULL COMMENT '黄色脆弱期条件完成度'"),
            ("risk_red_escalation", "TINYINT(1) NULL COMMENT '中证1000红色风险升级状态'"),
            ("risk_red_escalation_score", "DECIMAL(18, 6) NULL COMMENT '红色风险升级条件完成度'"),
            ("risk_global_shock", "TINYINT(1) NULL COMMENT '中证1000全球冲击状态'"),
            ("risk_global_shock_score", "DECIMAL(18, 6) NULL COMMENT '全球冲击模块完成度'"),
            ("risk_global_shock_mode", "VARCHAR(64) NULL COMMENT '全球冲击命中模式'"),
            ("risk_strategy_components_json", "JSON NULL COMMENT '三套风险策略完整组件审计信息'"),
        )
        for column_name, definition in risk_columns:
            column_definitions.append(
                (column_name, f"ADD COLUMN {column_name} {definition} AFTER {previous_column}")
            )
            previous_column = column_name
        turnover_concentration_columns = (
            (
                "turnover_concentration_top5_pct",
                "DECIMAL(18, 6) NULL COMMENT 'A股成交额前5%个股集中度MA5，百分比'",
            ),
            (
                "turnover_concentration_top1_pct",
                "DECIMAL(18, 6) NULL COMMENT 'A股成交额前1%个股集中度MA5，百分比'",
            ),
            (
                "turnover_concentration_top1_raw_pct",
                "DECIMAL(18, 6) NULL COMMENT 'A股成交额前1%个股集中度原始值，百分比'",
            ),
            (
                "turnover_concentration_meta_json",
                "JSON NULL COMMENT 'A股成交集中度覆盖范围、来源和原始口径'",
            ),
        )
        for column_name, definition in turnover_concentration_columns:
            column_definitions.append(
                (column_name, f"ADD COLUMN {column_name} {definition} AFTER {previous_column}")
            )
            previous_column = column_name
        basis_label_by_kind = {
            'main': '主连',
            'month': '月连',
        }
        for field in self.QUANT_INDEX_BASIS_DELTA_FIELDS:
            parts = field.split('_')
            basis_kind = parts[1] if len(parts) > 1 else ''
            window = parts[-1].removesuffix('d')
            basis_label = basis_label_by_kind.get(basis_kind, basis_kind)
            column_definitions.append(
                (
                    field,
                    f"ADD COLUMN {field} DECIMAL(18, 6) NULL COMMENT '{basis_label}期现差{window}交易日变化' AFTER {previous_column}",
                )
            )
            previous_column = field
        fund_purchase_limit_columns = (
            (
                'fund_purchase_limit_count',
                "BIGINT NULL COMMENT 'A股权益类公募基金大额限购产品数'",
            ),
            (
                'fund_purchase_limit_total_count',
                "BIGINT NULL COMMENT 'A股权益类公募基金产品总数'",
            ),
            (
                'fund_purchase_limit_pct',
                "DECIMAL(18, 6) NULL COMMENT 'A股权益类公募基金大额限购比例'",
            ),
        )
        for column_name, definition in fund_purchase_limit_columns:
            column_definitions.append(
                (column_name, f"ADD COLUMN {column_name} {definition} AFTER {previous_column}")
            )
            previous_column = column_name
        margin_trading_columns = (
            (
                "margin_financing_balance",
                "DECIMAL(30, 2) NULL COMMENT 'A股融资余额，人民币元'",
            ),
            (
                "margin_securities_lending_balance",
                "DECIMAL(30, 2) NULL COMMENT 'A股融券余额，人民币元'",
            ),
            (
                "margin_total_balance",
                "DECIMAL(30, 2) NULL COMMENT 'A股融资融券余额，人民币元'",
            ),
            (
                "margin_financing_net_buy_amount",
                "DECIMAL(30, 2) NULL COMMENT 'A股融资净买入额，人民币元'",
            ),
            (
                "margin_leverage_ratio_pct",
                "DECIMAL(18, 6) NULL COMMENT 'A股融资融券余额占沪深北流通市值比例，百分比'",
            ),
            (
                "margin_total_market_cap_leverage_ratio_pct",
                "DECIMAL(18, 6) NULL COMMENT 'A股融资融券余额占A股总市值比例，百分比'",
            ),
        )
        for column_name, definition in margin_trading_columns:
            column_definitions.append(
                (column_name, f"ADD COLUMN {column_name} {definition} AFTER {previous_column}")
            )
            previous_column = column_name
        for field in self.QUANT_INDEX_MARGIN_FINANCING_NET_BUY_SUM_FIELDS:
            window = field.split('_')[-1].removesuffix('d')
            column_definitions.append(
                (
                    field,
                    f"ADD COLUMN {field} DECIMAL(30, 2) NULL COMMENT '近{window}交易日融资净买入累计额，人民币元' AFTER {previous_column}",
                )
            )
            previous_column = field

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW COLUMNS FROM quant_index_dashboard_daily")
                existing_columns = {str(row[0]).strip() for row in await cursor.fetchall()}
                for column_name, alter_clause in column_definitions:
                    if column_name in existing_columns:
                        continue
                    await cursor.execute(f"ALTER TABLE quant_index_dashboard_daily {alter_clause}")
                    existing_columns.add(column_name)
                await conn.commit()
        self._quant_index_dashboard_option_pc_columns_ready = True

    async def ensure_margin_trading_daily_table(self):
        if self._margin_trading_daily_table_ready:
            return
        if self.pool is None:
            await self.init_pool()
        query = """
        CREATE TABLE IF NOT EXISTS margin_trading_daily_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trade_date DATE NOT NULL,
            exchange VARCHAR(8) NOT NULL,
            financing_balance DECIMAL(30, 2) NULL,
            financing_buy_amount DECIMAL(30, 2) NULL,
            financing_repayment_amount DECIMAL(30, 2) NULL,
            financing_net_buy_amount DECIMAL(30, 2) NULL,
            securities_lending_balance DECIMAL(30, 2) NULL,
            margin_balance DECIMAL(30, 2) NULL,
            securities_lending_sell_volume DECIMAL(30, 2) NULL,
            securities_lending_remaining_volume DECIMAL(30, 2) NULL,
            data_source VARCHAR(64) NOT NULL,
            source_url VARCHAR(512) NOT NULL,
            raw_json JSON NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_margin_trading_date_exchange (trade_date, exchange),
            KEY idx_margin_trading_exchange_date (exchange, trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                await conn.commit()
        self._margin_trading_daily_table_ready = True

    async def get_quant_index_dashboard_self_sentiment_history(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()
        await self.ensure_quant_index_dashboard_option_pc_columns()
        query = """
        SELECT trade_date, index_name, self_sentiment_components_json
        FROM quant_index_dashboard_daily
        WHERE trade_date BETWEEN %s AND %s
          AND index_name IN ('上证50', '沪深300', '中证500', '中证1000')
        ORDER BY trade_date ASC, index_name ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def get_quant_index_dashboard_option_pc_history(self, index_name, start_date, end_date):
        if self.pool is None:
            await self.init_pool()
        await self.ensure_quant_index_dashboard_option_pc_columns()
        query = """
        SELECT trade_date,
               option_pc_current_month,
               option_pc_next_month,
               option_pc_quarter_1,
               option_pc_quarter_2
        FROM quant_index_dashboard_daily
        WHERE index_name = %s
          AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(index_name), str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def ensure_global_risk_asset_daily_table(self):
        if self._global_risk_asset_daily_table_ready:
            return
        if self.pool is None:
            await self.init_pool()
        query = """
        CREATE TABLE IF NOT EXISTS global_risk_asset_daily (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            asset_code VARCHAR(32) NOT NULL,
            asset_name VARCHAR(96) NOT NULL,
            trade_date DATE NOT NULL,
            open_value DECIMAL(24, 8) NULL,
            high_value DECIMAL(24, 8) NULL,
            low_value DECIMAL(24, 8) NULL,
            close_value DECIMAL(24, 8) NOT NULL,
            volume DECIMAL(30, 4) NULL,
            source_date DATE NOT NULL,
            available_at DATETIME NOT NULL,
            data_source VARCHAR(96) NOT NULL,
            source_url VARCHAR(1024) NOT NULL,
            raw_json JSON NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_global_risk_asset_date (asset_code, trade_date),
            KEY idx_global_risk_available (available_at, asset_code),
            KEY idx_global_risk_source_date (source_date, asset_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                await conn.commit()
        self._global_risk_asset_daily_table_ready = True

    async def upsert_global_risk_asset_daily_rows(self, rows):
        if not rows:
            return 0
        await self.ensure_global_risk_asset_daily_table()
        deduped = {}
        for raw in rows:
            asset_code = str(raw.get('asset_code') or '').strip().upper()
            trade_date = str(raw.get('trade_date') or '').split(' ')[0].strip()
            close_value = self._normalize_numeric('close_value', raw.get('close_value'))
            if not asset_code or not trade_date or close_value is None:
                continue
            row = dict(raw)
            row['asset_code'] = asset_code
            row['trade_date'] = trade_date
            row['source_date'] = str(raw.get('source_date') or trade_date).split(' ')[0]
            row['close_value'] = close_value
            deduped[(asset_code, trade_date)] = row
        normalized_rows = list(deduped.values())
        if not normalized_rows:
            return 0

        query = """
        INSERT INTO global_risk_asset_daily (
            asset_code, asset_name, trade_date,
            open_value, high_value, low_value, close_value, volume,
            source_date, available_at, data_source, source_url, raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            asset_name = VALUES(asset_name),
            open_value = VALUES(open_value),
            high_value = VALUES(high_value),
            low_value = VALUES(low_value),
            close_value = VALUES(close_value),
            volume = VALUES(volume),
            source_date = VALUES(source_date),
            available_at = VALUES(available_at),
            data_source = VALUES(data_source),
            source_url = VALUES(source_url),
            raw_json = VALUES(raw_json),
            updated_at = CURRENT_TIMESTAMP
        """
        values = []
        for row in normalized_rows:
            values.append((
                row['asset_code'],
                str(row.get('asset_name') or row['asset_code']).strip(),
                row['trade_date'],
                self._normalize_numeric('open_value', row.get('open_value')),
                self._normalize_numeric('high_value', row.get('high_value')),
                self._normalize_numeric('low_value', row.get('low_value')),
                row['close_value'],
                self._normalize_numeric('volume', row.get('volume')),
                row['source_date'],
                row.get('available_at'),
                str(row.get('data_source') or '').strip(),
                str(row.get('source_url') or '').strip(),
                self._serialize_json_field(row.get('raw_json')),
            ))
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def get_global_risk_asset_daily_rows(self, start_date, end_date, asset_codes=None):
        await self.ensure_global_risk_asset_daily_table()
        params = [str(start_date), str(end_date)]
        code_clause = ''
        normalized_codes = sorted({
            str(code).strip().upper() for code in (asset_codes or []) if str(code).strip()
        })
        if normalized_codes:
            placeholders = ','.join(['%s'] * len(normalized_codes))
            code_clause = f' AND asset_code IN ({placeholders})'
            params.extend(normalized_codes)
        query = f"""
        SELECT asset_code, asset_name, trade_date,
               open_value, high_value, low_value, close_value, volume,
               source_date, available_at, data_source, source_url, raw_json
        FROM global_risk_asset_daily
        WHERE trade_date BETWEEN %s AND %s{code_clause}
        ORDER BY trade_date ASC, asset_code ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def ensure_a_share_turnover_concentration_daily_table(self):
        if self._a_share_turnover_concentration_daily_table_ready:
            return
        if self.pool is None:
            await self.init_pool()
        query = """
        CREATE TABLE IF NOT EXISTS a_share_turnover_concentration_daily (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trade_date DATE NOT NULL,
            top5_pct DECIMAL(18, 6) NULL,
            top1_pct DECIMAL(18, 6) NULL,
            top1_raw_pct DECIMAL(18, 6) NULL,
            stock_count INT NULL,
            top1_stock_count INT NULL,
            total_turnover_amount DECIMAL(30, 2) NULL,
            top1_turnover_amount DECIMAL(30, 2) NULL,
            top5_data_source VARCHAR(96) NULL,
            top1_data_source VARCHAR(96) NULL,
            top5_source_url VARCHAR(1024) NULL,
            source_date DATE NULL,
            available_at DATETIME NULL,
            raw_json JSON NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_a_share_turnover_concentration_date (trade_date),
            KEY idx_a_share_turnover_concentration_source_date (source_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                await conn.commit()
        self._a_share_turnover_concentration_daily_table_ready = True

    async def upsert_a_share_turnover_concentration_daily_rows(self, rows):
        if not rows:
            return 0
        await self.ensure_a_share_turnover_concentration_daily_table()
        deduped = {}
        for raw in rows:
            trade_date = str(raw.get("trade_date") or "").split(" ")[0].strip()
            if not trade_date:
                continue
            current = deduped.setdefault(trade_date, {"trade_date": trade_date})
            for key, value in raw.items():
                if key != "trade_date" and value is not None:
                    current[key] = value
        normalized_rows = list(deduped.values())
        if not normalized_rows:
            return 0
        query = """
        INSERT INTO a_share_turnover_concentration_daily (
            trade_date, top5_pct, top1_pct, top1_raw_pct,
            stock_count, top1_stock_count, total_turnover_amount, top1_turnover_amount,
            top5_data_source, top1_data_source, top5_source_url,
            source_date, available_at, raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            top5_pct = COALESCE(VALUES(top5_pct), top5_pct),
            top1_pct = COALESCE(VALUES(top1_pct), top1_pct),
            top1_raw_pct = COALESCE(VALUES(top1_raw_pct), top1_raw_pct),
            stock_count = COALESCE(VALUES(stock_count), stock_count),
            top1_stock_count = COALESCE(VALUES(top1_stock_count), top1_stock_count),
            total_turnover_amount = COALESCE(VALUES(total_turnover_amount), total_turnover_amount),
            top1_turnover_amount = COALESCE(VALUES(top1_turnover_amount), top1_turnover_amount),
            top5_data_source = COALESCE(VALUES(top5_data_source), top5_data_source),
            top1_data_source = COALESCE(VALUES(top1_data_source), top1_data_source),
            top5_source_url = COALESCE(VALUES(top5_source_url), top5_source_url),
            source_date = COALESCE(VALUES(source_date), source_date),
            available_at = COALESCE(VALUES(available_at), available_at),
            raw_json = JSON_MERGE_PATCH(
                COALESCE(raw_json, JSON_OBJECT()),
                COALESCE(VALUES(raw_json), JSON_OBJECT())
            ),
            updated_at = CURRENT_TIMESTAMP
        """
        values = []
        for row in normalized_rows:
            values.append((
                row["trade_date"],
                self._normalize_numeric("top5_pct", row.get("top5_pct")),
                self._normalize_numeric("top1_pct", row.get("top1_pct")),
                self._normalize_numeric("top1_raw_pct", row.get("top1_raw_pct")),
                int(row["stock_count"]) if row.get("stock_count") is not None else None,
                int(row["top1_stock_count"]) if row.get("top1_stock_count") is not None else None,
                self._normalize_numeric("turnover_amount", row.get("total_turnover_amount")),
                self._normalize_numeric("turnover_amount", row.get("top1_turnover_amount")),
                str(row.get("top5_data_source") or "").strip() or None,
                str(row.get("top1_data_source") or "").strip() or None,
                str(row.get("top5_source_url") or "").strip() or None,
                str(row.get("source_date") or "").split(" ")[0].strip() or None,
                row.get("available_at"),
                self._serialize_json_field(row.get("raw_json")),
            ))
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def get_a_share_turnover_concentration_daily_rows(self, start_date, end_date):
        await self.ensure_a_share_turnover_concentration_daily_table()
        query = """
        SELECT trade_date, top5_pct, top1_pct, top1_raw_pct,
               stock_count, top1_stock_count,
               total_turnover_amount, top1_turnover_amount,
               top5_data_source, top1_data_source, top5_source_url,
               source_date, available_at, raw_json
        FROM a_share_turnover_concentration_daily
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def calculate_a_share_top1_turnover_concentration_rows(self, start_date, end_date):
        """Calculate top-1% concentration only where official per-stock coverage exists."""
        await self.ensure_stock_exchange_official_daily_table()
        query = """
        WITH official_dates AS (
            SELECT trade_date
            FROM stock_exchange_official_daily_data
            WHERE trade_date BETWEEN %s AND %s
              AND exchange IN ('SH', 'SZ', 'SSE', 'SZSE')
              AND turnover_amount IS NOT NULL
              AND turnover_amount > 0
            GROUP BY trade_date
            HAVING COUNT(*) >= 1
        ), eligible AS (
            SELECT o.trade_date, LOWER(o.prefixed_code) AS prefixed_code, o.turnover_amount
            FROM stock_exchange_official_daily_data o
            INNER JOIN official_dates d ON d.trade_date = o.trade_date
            WHERE o.exchange IN ('SH', 'SZ', 'SSE', 'SZSE')
              AND o.turnover_amount IS NOT NULL
              AND o.turnover_amount > 0
              AND (
                LOWER(o.prefixed_code) REGEXP '^sh(60|68)[0-9]{4}$'
                OR LOWER(o.prefixed_code) REGEXP '^sz(00|30)[0-9]{4}$'
              )
            UNION ALL
            SELECT s.trade_date, LOWER(s.prefixed_code) AS prefixed_code, s.turnover_amount
            FROM stock_daily_data s
            INNER JOIN official_dates d ON d.trade_date = s.trade_date
            WHERE s.turnover_amount IS NOT NULL
              AND s.turnover_amount > 0
              AND (
                LOWER(s.prefixed_code) REGEXP '^sh(60|68)[0-9]{4}$'
                OR LOWER(s.prefixed_code) REGEXP '^sz(00|30)[0-9]{4}$'
                OR LOWER(s.prefixed_code) REGEXP '^bj[489][0-9]{5}$'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM stock_exchange_official_daily_data o2
                  WHERE o2.trade_date = s.trade_date
                    AND LOWER(o2.prefixed_code) = LOWER(s.prefixed_code)
                    AND o2.exchange IN ('SH', 'SZ', 'SSE', 'SZSE')
                    AND o2.turnover_amount IS NOT NULL
                    AND o2.turnover_amount > 0
              )
        ), ranked AS (
            SELECT trade_date, prefixed_code, turnover_amount,
                   ROW_NUMBER() OVER (
                       PARTITION BY trade_date ORDER BY turnover_amount DESC, prefixed_code ASC
                   ) AS turnover_rank,
                   COUNT(*) OVER (PARTITION BY trade_date) AS stock_count,
                   SUM(turnover_amount) OVER (PARTITION BY trade_date) AS total_turnover_amount
            FROM eligible
        )
        SELECT trade_date,
               MAX(stock_count) AS stock_count,
               CEIL(MAX(stock_count) * 0.01) AS top1_stock_count,
               MAX(total_turnover_amount) AS total_turnover_amount,
               SUM(CASE
                   WHEN turnover_rank <= CEIL(stock_count * 0.01) THEN turnover_amount
                   ELSE 0
               END) AS top1_turnover_amount
        FROM ranked
        GROUP BY trade_date
        HAVING MAX(stock_count) >= 5000
           AND MAX(total_turnover_amount) >= 100000000000
        ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def get_quant_index_risk_im_contract_rows(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()
        query = """
        SELECT trade_date, symbol, close_price, open_interest, volume, data_source
        FROM futures_daily_data
        WHERE trade_date BETWEEN %s AND %s
          AND symbol REGEXP '^IM[0-9]{4}$'
          AND close_price IS NOT NULL
          AND open_interest IS NOT NULL
        ORDER BY trade_date ASC, open_interest DESC, symbol ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def get_quant_index_risk_us_vix_rows(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()
        query = """
        SELECT trade_date, close_value, data_source
        FROM index_us_vix_daily
        WHERE trade_date BETWEEN %s AND %s
          AND close_value IS NOT NULL
        ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def get_quant_index_risk_us_credit_rows(self, start_date, end_date):
        await self.ensure_index_us_macro_auxiliary_tables()
        query = """
        SELECT trade_date, high_yield_oas, data_source
        FROM index_us_credit_spread_daily
        WHERE trade_date BETWEEN %s AND %s
          AND high_yield_oas IS NOT NULL
        ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def get_quant_index_risk_us_treasury_rows(self, start_date, end_date):
        await self.ensure_index_us_macro_auxiliary_tables()
        query = """
        SELECT
          trade_date,
          yield_3m,
          yield_2y,
          yield_10y,
          yield_real_10y,
          spread_10y_2y,
          spread_10y_3m,
          available_at,
          data_source
        FROM index_us_treasury_yield_daily
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def get_quant_index_risk_csi_tech_turnover_rows(self, start_date, end_date):
        if self.pool is None:
            await self.init_pool()
        query = """
        SELECT index_code, trade_date, turnover, data_source
        FROM index_daily_data
        WHERE index_code IN ('sh000985', 'sh000993')
          AND trade_date BETWEEN %s AND %s
          AND turnover IS NOT NULL
          AND turnover > 0
        ORDER BY trade_date ASC, index_code ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def upsert_margin_trading_daily_rows(self, rows):
        if not rows:
            return 0
        await self.ensure_margin_trading_daily_table()
        deduped = {}
        for raw in rows:
            trade_date = str(raw.get("trade_date") or "").strip()
            exchange = str(raw.get("exchange") or "").strip().upper()
            if not trade_date or exchange not in {"SSE", "SZSE", "BSE"}:
                continue
            row = dict(raw)
            row["trade_date"] = trade_date
            row["exchange"] = exchange
            deduped[(trade_date, exchange)] = row
        def decimal_amount(value):
            if value is None:
                return None
            return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        values = [
            (
                row["trade_date"],
                row["exchange"],
                decimal_amount(row.get("financing_balance")),
                decimal_amount(row.get("financing_buy_amount")),
                decimal_amount(row.get("financing_repayment_amount")),
                decimal_amount(row.get("financing_net_buy_amount")),
                decimal_amount(row.get("securities_lending_balance")),
                decimal_amount(row.get("margin_balance")),
                decimal_amount(row.get("securities_lending_sell_volume")),
                decimal_amount(row.get("securities_lending_remaining_volume")),
                str(row.get("data_source") or "exchange_official"),
                str(row.get("source_url") or ""),
                self._serialize_json_field(row.get("raw_json")),
            )
            for row in deduped.values()
        ]
        if not values:
            return 0
        query = """
        INSERT INTO margin_trading_daily_data (
            trade_date, exchange, financing_balance, financing_buy_amount,
            financing_repayment_amount, financing_net_buy_amount,
            securities_lending_balance, margin_balance,
            securities_lending_sell_volume, securities_lending_remaining_volume,
            data_source, source_url, raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            financing_balance = VALUES(financing_balance),
            financing_buy_amount = VALUES(financing_buy_amount),
            financing_repayment_amount = VALUES(financing_repayment_amount),
            financing_net_buy_amount = COALESCE(
                VALUES(financing_net_buy_amount),
                financing_net_buy_amount
            ),
            securities_lending_balance = VALUES(securities_lending_balance),
            margin_balance = VALUES(margin_balance),
            securities_lending_sell_volume = VALUES(securities_lending_sell_volume),
            securities_lending_remaining_volume = VALUES(securities_lending_remaining_volume),
            data_source = VALUES(data_source),
            source_url = VALUES(source_url),
            raw_json = VALUES(raw_json),
            updated_at = CURRENT_TIMESTAMP
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def get_margin_trading_existing_keys(self, start_date, end_date):
        await self.ensure_margin_trading_daily_table()
        query = """
        SELECT exchange, trade_date
        FROM margin_trading_daily_data
        WHERE trade_date BETWEEN %s AND %s
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return {
                    (str(row["exchange"]), str(row["trade_date"]))
                    for row in await cursor.fetchall()
                }

    async def recompute_margin_trading_net_buy(self, start_date, end_date):
        await self.ensure_margin_trading_daily_table()
        query = """
        UPDATE margin_trading_daily_data current_row
        JOIN (
            SELECT
                id,
                financing_balance
                    - LAG(financing_balance) OVER (
                        PARTITION BY exchange
                        ORDER BY trade_date
                    ) AS computed_net_buy
            FROM margin_trading_daily_data
        ) calculated
          ON calculated.id = current_row.id
        SET current_row.financing_net_buy_amount = calculated.computed_net_buy
        WHERE current_row.trade_date BETWEEN %s AND %s
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                affected = cursor.rowcount
                await conn.commit()
                return affected

    async def get_margin_trading_coverage_summary(self, start_date, end_date):
        await self.ensure_margin_trading_daily_table()
        query = """
        SELECT
            trade_date,
            GROUP_CONCAT(exchange ORDER BY exchange SEPARATOR ',') AS exchanges,
            COUNT(*) AS exchange_count
        FROM margin_trading_daily_data
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date
        ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [str(start_date), str(end_date)])
                return list(await cursor.fetchall())

    async def get_margin_trading_daily_summary(self, start_date, end_date):
        await self.ensure_margin_trading_daily_table()
        await self.ensure_cn_macro_tables()
        query = """
        SELECT
            margin_summary.trade_date,
            margin_summary.margin_financing_balance,
            margin_summary.margin_securities_lending_balance,
            margin_summary.margin_total_balance,
            margin_summary.margin_financing_net_buy_amount,
            CASE
                WHEN market_cap_summary.a_share_circulating_market_cap_cny > 0
                THEN margin_summary.margin_total_balance
                    / market_cap_summary.a_share_circulating_market_cap_cny * 100
                ELSE NULL
            END AS margin_leverage_ratio_pct,
            CASE
                WHEN macro_indicator.a_share_total_market_cap_cny > 0
                THEN margin_summary.margin_total_balance
                    / macro_indicator.a_share_total_market_cap_cny * 100
                ELSE NULL
            END AS margin_total_market_cap_leverage_ratio_pct,
            margin_summary.exchanges,
            margin_summary.exchange_count
        FROM (
            SELECT
                trade_date,
                SUM(financing_balance) AS margin_financing_balance,
                SUM(securities_lending_balance) AS margin_securities_lending_balance,
                SUM(margin_balance) AS margin_total_balance,
                SUM(financing_net_buy_amount) AS margin_financing_net_buy_amount,
                GROUP_CONCAT(exchange ORDER BY exchange SEPARATOR ',') AS exchanges,
                COUNT(*) AS exchange_count
            FROM margin_trading_daily_data
            WHERE trade_date BETWEEN %s AND %s
            GROUP BY trade_date
        ) AS margin_summary
        LEFT JOIN (
            SELECT
                trade_date,
                SUM(circulating_market_cap_cny) AS a_share_circulating_market_cap_cny
            FROM cn_stock_market_cap_daily
            WHERE trade_date BETWEEN %s AND %s
              AND exchange IN ('SSE', 'SZSE', 'BSE')
              AND circulating_market_cap_cny IS NOT NULL
            GROUP BY trade_date
            HAVING COUNT(DISTINCT exchange) = 3
        ) AS market_cap_summary
          ON market_cap_summary.trade_date = margin_summary.trade_date
        LEFT JOIN cn_macro_indicator_daily AS macro_indicator
          ON macro_indicator.trade_date = margin_summary.trade_date
        ORDER BY margin_summary.trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    query,
                    [str(start_date), str(end_date), str(start_date), str(end_date)],
                )
                rows = list(await cursor.fetchall())
        result = []
        bse_start = date(2023, 2, 13)
        for row in rows:
            trade_date_value = row.get("trade_date")
            normalized_date = (
                trade_date_value
                if isinstance(trade_date_value, date)
                else datetime.strptime(str(trade_date_value), "%Y-%m-%d").date()
            )
            expected = {"SSE", "SZSE"}
            if normalized_date >= bse_start:
                expected.add("BSE")
            actual = {
                item
                for item in str(row.get("exchanges") or "").split(",")
                if item
            }
            if expected.issubset(actual):
                result.append(row)
        return result

    async def ensure_fund_purchase_limit_daily_table(self):
        if self._fund_purchase_limit_daily_table_ready:
            return
        if self.pool is None:
            await self.init_pool()
        query = """
        CREATE TABLE IF NOT EXISTS fund_purchase_limit_daily_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trade_date DATE NOT NULL,
            fund_code VARCHAR(16) NOT NULL,
            fund_name VARCHAR(255) NOT NULL,
            product_name VARCHAR(255) NOT NULL,
            product_key CHAR(40) NOT NULL,
            fund_type VARCHAR(64) NOT NULL,
            purchase_status VARCHAR(64) NULL,
            redemption_status VARCHAR(64) NULL,
            limited_flag TINYINT(1) NOT NULL DEFAULT 0,
            limited_large_flag TINYINT(1) NOT NULL DEFAULT 0,
            suspended_purchase_flag TINYINT(1) NOT NULL DEFAULT 0,
            data_source VARCHAR(64) NOT NULL,
            raw_json JSON NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_fund_purchase_limit_date_code (trade_date, fund_code),
            KEY idx_fund_purchase_limit_date_product (trade_date, product_key),
            KEY idx_fund_purchase_limit_date_limited (trade_date, limited_flag)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                await conn.commit()
        self._fund_purchase_limit_daily_table_ready = True

    async def replace_fund_purchase_limit_daily_rows(self, trade_date, rows):
        await self.ensure_fund_purchase_limit_daily_table()
        normalized_date = str(trade_date).split(' ')[0]
        values = [
            (
                normalized_date,
                str(row.get('fund_code') or '').strip(),
                str(row.get('fund_name') or '').strip(),
                str(row.get('product_name') or '').strip(),
                str(row.get('product_key') or '').strip(),
                str(row.get('fund_type') or '').strip(),
                str(row.get('purchase_status') or '').strip() or None,
                str(row.get('redemption_status') or '').strip() or None,
                1 if row.get('limited_flag') else 0,
                1 if row.get('limited_large_flag') else 0,
                1 if row.get('suspended_purchase_flag') else 0,
                str(row.get('data_source') or '').strip(),
                row.get('raw_json'),
            )
            for row in rows
            if str(row.get('fund_code') or '').strip()
            and str(row.get('product_key') or '').strip()
        ]
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "DELETE FROM fund_purchase_limit_daily_data WHERE trade_date = %s",
                    (normalized_date,),
                )
                if values:
                    await cursor.executemany(
                        """
                        INSERT INTO fund_purchase_limit_daily_data (
                            trade_date, fund_code, fund_name, product_name, product_key,
                            fund_type, purchase_status, redemption_status, limited_flag,
                            limited_large_flag, suspended_purchase_flag, data_source, raw_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        values,
                    )
                await conn.commit()
        return len(values)

    async def upsert_fund_purchase_limit_daily_rows(self, rows, batch_size=1000):
        await self.ensure_fund_purchase_limit_daily_table()
        values = [
            (
                str(row.get('trade_date') or '').split(' ')[0],
                str(row.get('fund_code') or '').strip(),
                str(row.get('fund_name') or '').strip(),
                str(row.get('product_name') or '').strip(),
                str(row.get('product_key') or '').strip(),
                str(row.get('fund_type') or '').strip(),
                str(row.get('purchase_status') or '').strip() or None,
                str(row.get('redemption_status') or '').strip() or None,
                1 if row.get('limited_flag') else 0,
                1 if row.get('limited_large_flag') else 0,
                1 if row.get('suspended_purchase_flag') else 0,
                str(row.get('data_source') or '').strip(),
                row.get('raw_json'),
            )
            for row in rows
            if str(row.get('trade_date') or '').strip()
            and str(row.get('fund_code') or '').strip()
            and str(row.get('product_key') or '').strip()
        ]
        if not values:
            return 0

        normalized_batch_size = max(1, int(batch_size or 1000))
        query = """
        INSERT INTO fund_purchase_limit_daily_data (
            trade_date, fund_code, fund_name, product_name, product_key,
            fund_type, purchase_status, redemption_status, limited_flag,
            limited_large_flag, suspended_purchase_flag, data_source, raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            fund_name = IF(
                data_source = 'eastmoney_open_fund_daily',
                fund_name,
                VALUES(fund_name)
            ),
            product_name = IF(
                data_source = 'eastmoney_open_fund_daily',
                product_name,
                VALUES(product_name)
            ),
            product_key = IF(
                data_source = 'eastmoney_open_fund_daily',
                product_key,
                VALUES(product_key)
            ),
            fund_type = IF(
                data_source = 'eastmoney_open_fund_daily',
                fund_type,
                VALUES(fund_type)
            ),
            purchase_status = IF(
                data_source = 'eastmoney_open_fund_daily',
                purchase_status,
                VALUES(purchase_status)
            ),
            redemption_status = IF(
                data_source = 'eastmoney_open_fund_daily',
                redemption_status,
                VALUES(redemption_status)
            ),
            limited_flag = IF(
                data_source = 'eastmoney_open_fund_daily',
                limited_flag,
                VALUES(limited_flag)
            ),
            limited_large_flag = IF(
                data_source = 'eastmoney_open_fund_daily',
                limited_large_flag,
                VALUES(limited_large_flag)
            ),
            suspended_purchase_flag = IF(
                data_source = 'eastmoney_open_fund_daily',
                suspended_purchase_flag,
                VALUES(suspended_purchase_flag)
            ),
            raw_json = IF(
                data_source = 'eastmoney_open_fund_daily',
                raw_json,
                VALUES(raw_json)
            ),
            data_source = IF(
                data_source = 'eastmoney_open_fund_daily',
                data_source,
                VALUES(data_source)
            )
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for offset in range(0, len(values), normalized_batch_size):
                    await cursor.executemany(
                        query,
                        values[offset:offset + normalized_batch_size],
                    )
                await conn.commit()
        return len(values)

    async def get_fund_purchase_limit_daily_summary(self, start_date, end_date):
        await self.ensure_fund_purchase_limit_daily_table()
        normalized_start = str(start_date).split(' ')[0]
        normalized_end = str(end_date).split(' ')[0]
        query = """
        SELECT
            trade_date,
            COUNT(*) AS total_fund_count,
            SUM(product_limited_flag) AS limited_fund_count,
            ROUND(SUM(product_limited_flag) / NULLIF(COUNT(*), 0) * 100, 6) AS limited_fund_pct,
            SUM(product_suspended_flag) AS suspended_fund_count
        FROM (
            SELECT
                fund_rows.trade_date AS trade_date,
                fund_rows.product_key AS product_key,
                MAX(limited_flag) AS product_limited_flag,
                MAX(suspended_purchase_flag) AS product_suspended_flag
            FROM fund_purchase_limit_daily_data fund_rows
            INNER JOIN (
                SELECT DISTINCT trade_date
                FROM index_daily_data
                WHERE trade_date BETWEEN %s AND %s
            ) trade_calendar
                ON trade_calendar.trade_date = fund_rows.trade_date
            WHERE fund_rows.trade_date BETWEEN %s AND %s
            GROUP BY fund_rows.trade_date, fund_rows.product_key
        ) product_rows
        GROUP BY trade_date
        ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                query_start = (
                    date.fromisoformat(normalized_start) - timedelta(days=45)
                ).isoformat()
                await cursor.execute(
                    query,
                    (
                        query_start,
                        normalized_end,
                        query_start,
                        normalized_end,
                    ),
                )
                rows = list(await cursor.fetchall())

        effective_previous = None
        results = []
        for row in rows:
            total_count = int(row.get('total_fund_count') or 0)
            suspended_count = int(row.get('suspended_fund_count') or 0)
            market_wide_pause = (
                total_count >= 100
                and suspended_count / total_count >= 0.08
            )
            if market_wide_pause and effective_previous:
                effective = {
                    **row,
                    'total_fund_count': effective_previous['total_fund_count'],
                    'limited_fund_count': effective_previous['limited_fund_count'],
                    'limited_fund_pct': effective_previous['limited_fund_pct'],
                    'market_wide_pause': True,
                }
            else:
                effective = {
                    **row,
                    'market_wide_pause': market_wide_pause,
                }
            effective_previous = effective
            if str(row.get('trade_date')) >= normalized_start:
                results.append(effective)
        return results

    async def get_previous_fund_purchase_limit_flags(self, trade_date):
        await self.ensure_fund_purchase_limit_daily_table()
        normalized_date = str(trade_date).split(' ')[0]
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    SELECT MAX(trade_date) AS previous_date
                    FROM index_daily_data
                    WHERE trade_date < %s
                    """,
                    (normalized_date,),
                )
                date_row = await cursor.fetchone()
                previous_date = date_row.get('previous_date') if date_row else None
                if not previous_date:
                    return {}
                await cursor.execute(
                    """
                    SELECT fund_code, MAX(limited_flag) AS limited_flag
                    FROM fund_purchase_limit_daily_data
                    WHERE trade_date = %s
                    GROUP BY fund_code
                    """,
                    (previous_date,),
                )
                rows = await cursor.fetchall()
                return {
                    str(row.get('fund_code') or '').strip(): (
                        1 if row.get('limited_flag') else 0
                    )
                    for row in rows
                    if str(row.get('fund_code') or '').strip()
                }

    async def normalize_fund_purchase_limit_indicator_flags(
        self,
        start_date,
        end_date,
        pause_ratio=0.08,
        min_products=100,
    ):
        await self.ensure_fund_purchase_limit_daily_table()
        normalized_start = str(start_date).split(' ')[0]
        normalized_end = str(end_date).split(' ')[0]
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    UPDATE fund_purchase_limit_daily_data
                    SET limited_flag = limited_large_flag
                    WHERE trade_date BETWEEN %s AND %s
                      AND limited_flag <> limited_large_flag
                    """,
                    (normalized_start, normalized_end),
                )
                reset_rows = int(cursor.rowcount or 0)
                await conn.commit()

                await cursor.execute(
                    """
                    SELECT
                        trade_date,
                        COUNT(*) AS total_products,
                        SUM(product_suspended) AS suspended_products
                    FROM (
                        SELECT
                            fund_rows.trade_date AS trade_date,
                            fund_rows.product_key AS product_key,
                            MAX(suspended_purchase_flag) AS product_suspended
                        FROM fund_purchase_limit_daily_data fund_rows
                        INNER JOIN (
                            SELECT DISTINCT trade_date
                            FROM index_daily_data
                            WHERE trade_date BETWEEN %s AND %s
                        ) trade_calendar
                            ON trade_calendar.trade_date = fund_rows.trade_date
                        WHERE fund_rows.trade_date BETWEEN %s AND %s
                        GROUP BY fund_rows.trade_date, fund_rows.product_key
                    ) product_rows
                    GROUP BY trade_date
                    HAVING COUNT(*) >= %s
                       AND SUM(product_suspended) / COUNT(*) >= %s
                    ORDER BY trade_date ASC
                    """,
                    (
                        normalized_start,
                        normalized_end,
                        normalized_start,
                        normalized_end,
                        int(min_products),
                        float(pause_ratio),
                    ),
                )
                pause_dates = [
                    row.get('trade_date')
                    for row in await cursor.fetchall()
                    if row.get('trade_date')
                ]

                carried_rows = 0
                for pause_date in pause_dates:
                    await cursor.execute(
                        """
                        SELECT MAX(trade_date) AS previous_date
                        FROM index_daily_data
                        WHERE trade_date < %s
                        """,
                        (pause_date,),
                    )
                    previous_row = await cursor.fetchone()
                    previous_date = (
                        previous_row.get('previous_date') if previous_row else None
                    )
                    if not previous_date:
                        continue
                    await cursor.execute(
                        """
                        UPDATE fund_purchase_limit_daily_data current_row
                        INNER JOIN fund_purchase_limit_daily_data previous_row
                            ON previous_row.trade_date = %s
                           AND previous_row.fund_code = current_row.fund_code
                        SET current_row.limited_flag = previous_row.limited_flag
                        WHERE current_row.trade_date = %s
                          AND current_row.suspended_purchase_flag = 1
                          AND current_row.limited_large_flag = 0
                          AND current_row.limited_flag <> previous_row.limited_flag
                        """,
                        (previous_date, pause_date),
                    )
                    carried_rows += int(cursor.rowcount or 0)
                    await conn.commit()

                return {
                    'reset_rows': reset_rows,
                    'market_wide_pause_dates': len(pause_dates),
                    'carried_limit_rows': carried_rows,
                }

    async def get_quant_index_dashboard_option_closes(self, product_prefixes, start_date, end_date):
        if self.pool is None:
            await self.init_pool()

        normalized_prefixes = [
            str(product_prefix).strip().upper()
            for product_prefix in (product_prefixes or [])
            if str(product_prefix).strip()
        ]
        if not normalized_prefixes:
            return []

        placeholders = ','.join(['%s'] * len(normalized_prefixes))
        query = (
            f"SELECT trade_date, product_prefix, index_type, contract_month, option_type, "
            f"strike_price, open_price, close_price, settle_price, pre_settle_price, "
            f"volume, turnover "
            f"FROM option_cffex_rtj_daily_data "
            f"WHERE product_prefix IN ({placeholders}) "
            f"AND trade_date BETWEEN %s AND %s "
            f"AND option_type IN ('CALL', 'PUT') "
            f"AND strike_price IS NOT NULL "
            f"ORDER BY trade_date ASC, product_prefix ASC, contract_month ASC, strike_price ASC"
        )
        params = [*normalized_prefixes, str(start_date), str(end_date)]

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def get_quant_index_dashboard_exchange_option_rows(
        self,
        underlying_codes,
        start_date,
        end_date,
    ):
        await self.ensure_exchange_option_tables()
        normalized_codes = [
            str(code).strip()
            for code in (underlying_codes or [])
            if str(code).strip()
        ]
        if not normalized_codes:
            return []

        placeholders = ','.join(['%s'] * len(normalized_codes))
        query = f"""
        SELECT
            daily.trade_date,
            daily.exchange,
            daily.contract_code,
            COALESCE(daily.contract_trade_code, info.contract_trade_code) AS contract_trade_code,
            COALESCE(daily.contract_name, info.contract_name) AS contract_name,
            COALESCE(daily.underlying_code, info.underlying_code) AS underlying_code,
            COALESCE(daily.underlying_name, info.underlying_name) AS underlying_name,
            COALESCE(daily.option_type, info.option_type) AS option_type,
            COALESCE(daily.contract_month, info.contract_month) AS contract_month,
            COALESCE(daily.strike_price, info.strike_price) AS strike_price,
            daily.open_price,
            daily.close_price,
            daily.settle_price,
            daily.pre_settle_price,
            daily.pre_settle_source,
            daily.volume,
            daily.turnover,
            info.last_trade_date,
            info.expire_date
        FROM option_exchange_contract_daily_data daily
        INNER JOIN option_exchange_contract_info info
          ON info.exchange = daily.exchange
         AND info.contract_code = daily.contract_code
        WHERE COALESCE(daily.underlying_code, info.underlying_code) IN ({placeholders})
          AND daily.trade_date BETWEEN %s AND %s
          AND COALESCE(daily.option_type, info.option_type) IN ('CALL', 'PUT')
        ORDER BY
            daily.trade_date ASC,
            daily.exchange ASC,
            COALESCE(daily.underlying_code, info.underlying_code) ASC,
            COALESCE(daily.contract_month, info.contract_month) ASC,
            COALESCE(daily.strike_price, info.strike_price) ASC
        """
        params = [*normalized_codes, str(start_date), str(end_date)]
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def get_quant_index_dashboard_etf_closes(
        self,
        etf_codes,
        start_date,
        end_date,
    ):
        if self.pool is None:
            await self.init_pool()
        normalized_codes = [
            str(code).strip()
            for code in (etf_codes or [])
            if str(code).strip()
        ]
        if not normalized_codes:
            return []

        placeholders = ','.join(['%s'] * len(normalized_codes))
        query = f"""
        SELECT etf_code, trade_date, close_price, data_source
        FROM etf_daily_data_sina
        WHERE etf_code IN ({placeholders})
          AND trade_date BETWEEN %s AND %s
          AND close_price IS NOT NULL
        ORDER BY
            etf_code ASC,
            trade_date ASC,
            CASE data_source
                WHEN 'fund_etf_hist_sina' THEN 0
                WHEN 'fund_etf_category_sina' THEN 1
                ELSE 2
            END ASC
        """
        params = [*normalized_codes, str(start_date), str(end_date)]
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return list(await cursor.fetchall())

    async def upsert_quant_index_dashboard_daily(self, rows):
        if not rows:
            return 0

        if self.pool is None:
            await self.init_pool()
        await self.ensure_quant_index_dashboard_option_pc_columns()

        sanitized_rows = [self._sanitize_quant_index_dashboard_row(row) for row in rows]
        deduped_rows = {}
        for row in sanitized_rows:
            if not (row['trade_date'] and row['index_code'] and row['index_name']):
                continue
            deduped_rows[(row['index_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped_rows.values())
        if not sanitized_rows:
            return 0

        cffex_delta_columns = list(self.QUANT_INDEX_CFFEX_NET_SHORT_DELTA_FIELDS)
        basis_delta_columns = list(self.QUANT_INDEX_BASIS_DELTA_FIELDS)
        fund_purchase_limit_columns = list(self.QUANT_INDEX_FUND_PURCHASE_LIMIT_FIELDS)
        margin_trading_columns = list(self.QUANT_INDEX_MARGIN_TRADING_FIELDS)
        margin_financing_net_buy_sum_columns = list(
            self.QUANT_INDEX_MARGIN_FINANCING_NET_BUY_SUM_FIELDS
        )
        optional_metric_columns = [
            *cffex_delta_columns,
            *basis_delta_columns,
            *fund_purchase_limit_columns,
            *margin_trading_columns,
            *margin_financing_net_buy_sum_columns,
            *self.QUANT_INDEX_RISK_FIELDS,
            *self.QUANT_INDEX_TURNOVER_CONCENTRATION_FIELDS,
        ]
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
                row['option_pc_current_month'],
                row['option_pc_current_month_contract_month'],
                row['option_pc_current_month_special_flag'],
                row['option_pc_current_month_special_note'],
                row['option_pc_next_month'],
                row['option_pc_next_month_contract_month'],
                row['option_pc_next_month_special_flag'],
                row['option_pc_next_month_special_note'],
                row['option_pc_quarter_1'],
                row['option_pc_quarter_1_contract_month'],
                row['option_pc_quarter_1_special_flag'],
                row['option_pc_quarter_1_special_note'],
                row['option_pc_quarter_2'],
                row['option_pc_quarter_2_contract_month'],
                row['option_pc_quarter_2_special_flag'],
                row['option_pc_quarter_2_special_note'],
                row['option_volume_pc_ratio'],
                row['option_turnover_pc_ratio'],
                row['exchange_option_pc_json'],
                row['option_vix_json'],
                row['self_sentiment_score'],
                row['self_sentiment_core_score'],
                row['self_sentiment_derivative_score'],
                row['self_sentiment_components_json'],
                *[row[field] for field in optional_metric_columns],
            )
            for row in sanitized_rows
        ]

        optional_metric_insert_columns = ",\n                    ".join(optional_metric_columns)
        optional_metric_placeholders = ", ".join(["%s"] * len(optional_metric_columns))
        optional_metric_update_assignments = ",\n                    ".join(
            f"{field} = VALUES({field})" for field in optional_metric_columns
        )
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"""
                INSERT INTO quant_index_dashboard_daily (
                    trade_date,
                    index_code,
                    index_name,
                    emotion_value,
                    main_basis,
                    month_basis,
                    breadth_up_count,
                    breadth_total_count,
                    breadth_up_pct,
                    option_pc_current_month,
                    option_pc_current_month_contract_month,
                    option_pc_current_month_special_flag,
                    option_pc_current_month_special_note,
                    option_pc_next_month,
                    option_pc_next_month_contract_month,
                    option_pc_next_month_special_flag,
                    option_pc_next_month_special_note,
                    option_pc_quarter_1,
                    option_pc_quarter_1_contract_month,
                    option_pc_quarter_1_special_flag,
                    option_pc_quarter_1_special_note,
                    option_pc_quarter_2,
                    option_pc_quarter_2_contract_month,
                    option_pc_quarter_2_special_flag,
                    option_pc_quarter_2_special_note,
                    option_volume_pc_ratio,
                    option_turnover_pc_ratio,
                    exchange_option_pc_json,
                    option_vix_json,
                    self_sentiment_score,
                    self_sentiment_core_score,
                    self_sentiment_derivative_score,
                    self_sentiment_components_json,
                    {optional_metric_insert_columns}
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {optional_metric_placeholders})
                ON DUPLICATE KEY UPDATE
                    index_name = VALUES(index_name),
                    emotion_value = VALUES(emotion_value),
                    main_basis = VALUES(main_basis),
                    month_basis = VALUES(month_basis),
                    breadth_up_count = VALUES(breadth_up_count),
                    breadth_total_count = VALUES(breadth_total_count),
                    breadth_up_pct = VALUES(breadth_up_pct),
                    option_pc_current_month = VALUES(option_pc_current_month),
                    option_pc_current_month_contract_month = VALUES(option_pc_current_month_contract_month),
                    option_pc_current_month_special_flag = VALUES(option_pc_current_month_special_flag),
                    option_pc_current_month_special_note = VALUES(option_pc_current_month_special_note),
                    option_pc_next_month = VALUES(option_pc_next_month),
                    option_pc_next_month_contract_month = VALUES(option_pc_next_month_contract_month),
                    option_pc_next_month_special_flag = VALUES(option_pc_next_month_special_flag),
                    option_pc_next_month_special_note = VALUES(option_pc_next_month_special_note),
                    option_pc_quarter_1 = VALUES(option_pc_quarter_1),
                    option_pc_quarter_1_contract_month = VALUES(option_pc_quarter_1_contract_month),
                    option_pc_quarter_1_special_flag = VALUES(option_pc_quarter_1_special_flag),
                    option_pc_quarter_1_special_note = VALUES(option_pc_quarter_1_special_note),
                    option_pc_quarter_2 = VALUES(option_pc_quarter_2),
                    option_pc_quarter_2_contract_month = VALUES(option_pc_quarter_2_contract_month),
                    option_pc_quarter_2_special_flag = VALUES(option_pc_quarter_2_special_flag),
                    option_pc_quarter_2_special_note = VALUES(option_pc_quarter_2_special_note),
                    option_volume_pc_ratio = VALUES(option_volume_pc_ratio),
                    option_turnover_pc_ratio = VALUES(option_turnover_pc_ratio),
                    exchange_option_pc_json = VALUES(exchange_option_pc_json),
                    option_vix_json = VALUES(option_vix_json),
                    self_sentiment_score = VALUES(self_sentiment_score),
                    self_sentiment_core_score = VALUES(self_sentiment_core_score),
                    self_sentiment_derivative_score = VALUES(self_sentiment_derivative_score),
                    self_sentiment_components_json = VALUES(self_sentiment_components_json),
                    {optional_metric_update_assignments},
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

    async def batch_index_futures_contract_info(self, table_name, rows):
        if not rows:
            return 0
        if table_name not in self.INDEX_FUTURES_CONTRACT_TABLES:
            raise ValueError(f"unsupported index futures contract table: {table_name}")
        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_index_futures_contract_row(row) for row in rows]
        deduped_rows = {}
        for row in sanitized_rows:
            if not (row['root_symbol'] and row['source_contract_code']):
                continue
            deduped_rows[row['source_contract_code']] = row
        sanitized_rows = list(deduped_rows.values())
        if not sanitized_rows:
            return 0

        values = [
            (
                row['root_symbol'],
                row['source_contract_code'],
                row['contract_name'],
                row['contract_month'],
                row['exchange'],
                row['data_source'],
                row['first_seen_trade_date'],
                row['last_seen_trade_date'],
            )
            for row in sanitized_rows
        ]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"""
                INSERT INTO {table_name} (
                    root_symbol,
                    source_contract_code,
                    contract_name,
                    contract_month,
                    exchange,
                    data_source,
                    first_seen_trade_date,
                    last_seen_trade_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    root_symbol = VALUES(root_symbol),
                    contract_name = VALUES(contract_name),
                    contract_month = VALUES(contract_month),
                    exchange = VALUES(exchange),
                    data_source = VALUES(data_source),
                    first_seen_trade_date = COALESCE(
                        LEAST(first_seen_trade_date, VALUES(first_seen_trade_date)),
                        VALUES(first_seen_trade_date),
                        first_seen_trade_date
                    ),
                    last_seen_trade_date = COALESCE(
                        GREATEST(last_seen_trade_date, VALUES(last_seen_trade_date)),
                        VALUES(last_seen_trade_date),
                        last_seen_trade_date
                    ),
                    updated_at = CURRENT_TIMESTAMP
                """
                await cursor.executemany(query, values)
                await conn.commit()
                return len(sanitized_rows)

    async def batch_index_futures_daily_data(self, table_name, rows):
        if not rows:
            return 0
        if table_name not in self.INDEX_FUTURES_DAILY_TABLES:
            raise ValueError(f"unsupported index futures daily table: {table_name}")
        if self.pool is None:
            await self.init_pool()

        sanitized_rows = [self._sanitize_index_futures_daily_row(row) for row in rows]
        deduped_rows = {}
        for row in sanitized_rows:
            if not (row['source_contract_code'] and row['trade_date'] and row['root_symbol']):
                continue
            deduped_rows[(row['source_contract_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped_rows.values())
        if not sanitized_rows:
            return 0

        is_us_table = table_name == 'futures_us_index_daily_data'
        if is_us_table:
            values = [
                (
                    row['source_contract_code'], row['root_symbol'], row['contract_name'],
                    row['contract_month'], row['trade_date'], row['open_price'],
                    row['high_price'], row['low_price'], row['close_price'],
                    row['closing_range_raw'], row['volume'], row['open_interest'],
                    row['settle_price'], row['pre_settle_price'], row['data_source'],
                )
                for row in sanitized_rows
            ]
            query = f"""
            INSERT INTO {table_name} (
                source_contract_code, root_symbol, contract_name, contract_month, trade_date,
                open_price, high_price, low_price, close_price, closing_range_raw,
                volume, open_interest, settle_price, pre_settle_price, data_source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                root_symbol = VALUES(root_symbol),
                contract_name = VALUES(contract_name),
                contract_month = VALUES(contract_month),
                open_price = VALUES(open_price),
                high_price = VALUES(high_price),
                low_price = VALUES(low_price),
                close_price = VALUES(close_price),
                closing_range_raw = VALUES(closing_range_raw),
                volume = VALUES(volume),
                open_interest = VALUES(open_interest),
                settle_price = VALUES(settle_price),
                pre_settle_price = VALUES(pre_settle_price),
                data_source = VALUES(data_source),
                updated_at = CURRENT_TIMESTAMP
            """
        else:
            values = [
                (
                    row['source_contract_code'], row['root_symbol'], row['contract_name'],
                    row['contract_month'], row['trade_date'], row['open_price'],
                    row['high_price'], row['low_price'], row['close_price'],
                    row['volume'], row['open_interest'], row['settle_price'],
                    row['pre_settle_price'], row['data_source'],
                )
                for row in sanitized_rows
            ]
            query = f"""
            INSERT INTO {table_name} (
                source_contract_code, root_symbol, contract_name, contract_month, trade_date,
                open_price, high_price, low_price, close_price,
                volume, open_interest, settle_price, pre_settle_price, data_source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                root_symbol = VALUES(root_symbol),
                contract_name = VALUES(contract_name),
                contract_month = VALUES(contract_month),
                open_price = VALUES(open_price),
                high_price = VALUES(high_price),
                low_price = VALUES(low_price),
                close_price = VALUES(close_price),
                volume = VALUES(volume),
                open_interest = VALUES(open_interest),
                settle_price = VALUES(settle_price),
                pre_settle_price = VALUES(pre_settle_price),
                data_source = VALUES(data_source),
                updated_at = CURRENT_TIMESTAMP
            """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
                return len(sanitized_rows)

    async def ensure_us_index_official_futures_tables(self):
        if self.pool is None:
            await self.init_pool()

        statements = [
            """
            CREATE TABLE IF NOT EXISTS futures_us_index_official_contract_info (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              root_symbol VARCHAR(16) NOT NULL COMMENT 'Root symbol, e.g. ES or NQ',
              source_contract_code VARCHAR(32) NOT NULL COMMENT 'Official contract code, e.g. ESM26',
              contract_name VARCHAR(128) NULL COMMENT 'Contract display name',
              contract_month VARCHAR(16) NULL COMMENT 'Contract month in YYYY-MM',
              exchange VARCHAR(32) NULL COMMENT 'Exchange, e.g. CME',
              data_source VARCHAR(64) NOT NULL DEFAULT 'cme_settlements',
              first_seen_trade_date DATE NULL,
              last_seen_trade_date DATE NULL,
              created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY uk_us_index_official_contract_code (source_contract_code),
              KEY idx_us_index_official_root_month (root_symbol, contract_month),
              KEY idx_us_index_official_seen_date (last_seen_trade_date)
            ) COMMENT='Official CME US stock index futures contract registry'
            """,
            """
            CREATE TABLE IF NOT EXISTS futures_us_index_official_daily_data (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              source_contract_code VARCHAR(32) NOT NULL COMMENT 'Official contract code, e.g. ESM26',
              root_symbol VARCHAR(16) NOT NULL COMMENT 'Root symbol, e.g. ES or NQ',
              contract_name VARCHAR(128) NULL COMMENT 'Contract display name',
              contract_month VARCHAR(16) NULL COMMENT 'Contract month in YYYY-MM',
              trade_date DATE NOT NULL COMMENT 'Trading date',
              open_price DECIMAL(18, 6) NULL COMMENT 'Open price',
              high_price DECIMAL(18, 6) NULL COMMENT 'High price',
              low_price DECIMAL(18, 6) NULL COMMENT 'Low price',
              last_price DECIMAL(18, 6) NULL COMMENT 'Last price from CME settlements payload',
              close_price DECIMAL(18, 6) NULL COMMENT 'Close price, using settlement when available',
              settle_price DECIMAL(18, 6) NULL COMMENT 'Settlement price',
              price_change DECIMAL(18, 6) NULL COMMENT 'Daily settlement change',
              volume DECIMAL(20, 2) NULL COMMENT 'Volume',
              open_interest DECIMAL(20, 2) NULL COMMENT 'Open interest',
              data_source VARCHAR(64) NOT NULL DEFAULT 'cme_settlements',
              raw_payload_json JSON NULL COMMENT 'Original CME row payload',
              created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY uk_us_index_official_contract_trade_date (source_contract_code, trade_date),
              KEY idx_us_index_official_root_trade_date (root_symbol, trade_date),
              KEY idx_us_index_official_trade_date (trade_date)
            ) COMMENT='Official CME US stock index futures contract-level daily settlement data'
            """,
        ]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for statement in statements:
                    await cursor.execute(statement)
                await conn.commit()

    async def batch_us_index_official_futures_daily_data(self, rows):
        if not rows:
            return 0
        if self.pool is None:
            await self.init_pool()

        await self.ensure_us_index_official_futures_tables()
        sanitized_rows = [self._sanitize_us_index_official_futures_daily_row(row) for row in rows]
        deduped_rows = {}
        for row in sanitized_rows:
            if not (row['source_contract_code'] and row['trade_date'] and row['root_symbol']):
                continue
            deduped_rows[(row['source_contract_code'], row['trade_date'])] = row
        sanitized_rows = list(deduped_rows.values())
        if not sanitized_rows:
            return 0

        values = [
            (
                row['source_contract_code'], row['root_symbol'], row['contract_name'],
                row['contract_month'], row['trade_date'], row['open_price'],
                row['high_price'], row['low_price'], row['last_price'], row['close_price'],
                row['settle_price'], row['price_change'], row['volume'], row['open_interest'],
                row['data_source'], row['raw_payload_json'],
            )
            for row in sanitized_rows
        ]
        query = """
        INSERT INTO futures_us_index_official_daily_data (
            source_contract_code, root_symbol, contract_name, contract_month, trade_date,
            open_price, high_price, low_price, last_price, close_price, settle_price,
            price_change, volume, open_interest, data_source, raw_payload_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            root_symbol = VALUES(root_symbol),
            contract_name = VALUES(contract_name),
            contract_month = VALUES(contract_month),
            open_price = VALUES(open_price),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            last_price = VALUES(last_price),
            close_price = VALUES(close_price),
            settle_price = VALUES(settle_price),
            price_change = VALUES(price_change),
            volume = VALUES(volume),
            open_interest = VALUES(open_interest),
            data_source = VALUES(data_source),
            raw_payload_json = VALUES(raw_payload_json),
            updated_at = CURRENT_TIMESTAMP
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
                return len(sanitized_rows)

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

    async def ensure_cn_macro_tables(self):
        if self._cn_macro_tables_ready:
            return
        if self.pool is None:
            await self.init_pool()

        statements = [
            """
            CREATE TABLE IF NOT EXISTS cn_index_valuation_daily (
                id BIGINT NOT NULL AUTO_INCREMENT,
                trade_date DATE NOT NULL,
                index_code VARCHAR(16) NOT NULL,
                index_name VARCHAR(64) NOT NULL,
                pe_ttm DECIMAL(20, 8) NULL,
                earnings_yield_pct DECIMAL(20, 8) NULL,
                data_source VARCHAR(64) NOT NULL,
                source_url VARCHAR(512) NULL,
                raw_json LONGTEXT NULL,
                created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_cn_index_valuation_date_code (trade_date, index_code),
                KEY idx_cn_index_valuation_code_date (index_code, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS cn_government_bond_yield_daily (
                id BIGINT NOT NULL AUTO_INCREMENT,
                trade_date DATE NOT NULL,
                tenor_years INT NOT NULL,
                maturity_yield_pct DECIMAL(20, 8) NULL,
                spot_yield_pct DECIMAL(20, 8) NULL,
                forward_yield_pct DECIMAL(20, 8) NULL,
                data_source VARCHAR(64) NOT NULL,
                source_url VARCHAR(512) NULL,
                raw_json LONGTEXT NULL,
                created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_cn_gov_bond_yield_date_tenor (trade_date, tenor_years),
                KEY idx_cn_gov_bond_yield_date (trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS cn_stock_market_cap_daily (
                id BIGINT NOT NULL AUTO_INCREMENT,
                trade_date DATE NOT NULL,
                exchange VARCHAR(16) NOT NULL,
                total_market_cap_cny DECIMAL(30, 2) NULL,
                circulating_market_cap_cny DECIMAL(30, 2) NULL,
                reference_gdp_cny DECIMAL(30, 2) NULL,
                data_source VARCHAR(64) NOT NULL,
                source_url VARCHAR(512) NULL,
                raw_json LONGTEXT NULL,
                created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_cn_market_cap_date_exchange (trade_date, exchange),
                KEY idx_cn_market_cap_date (trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS cn_gdp_quarterly (
                id BIGINT NOT NULL AUTO_INCREMENT,
                period_end DATE NOT NULL,
                year INT NOT NULL,
                quarter INT NOT NULL,
                nominal_gdp_cny DECIMAL(30, 2) NULL,
                cumulative_gdp_cny DECIMAL(30, 2) NULL,
                release_date DATE NULL,
                data_source VARCHAR(64) NOT NULL,
                source_url VARCHAR(512) NULL,
                raw_json LONGTEXT NULL,
                created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_cn_gdp_period_end (period_end),
                KEY idx_cn_gdp_release_date (release_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS cn_household_deposit_monthly (
                id BIGINT NOT NULL AUTO_INCREMENT,
                period_end DATE NOT NULL,
                household_deposit_cny DECIMAL(30, 2) NULL,
                demand_deposit_cny DECIMAL(30, 2) NULL,
                time_other_deposit_cny DECIMAL(30, 2) NULL,
                source_updated_at DATE NULL,
                data_source VARCHAR(64) NOT NULL,
                source_url VARCHAR(512) NULL,
                raw_json LONGTEXT NULL,
                created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_cn_household_deposit_period_end (period_end),
                KEY idx_cn_household_deposit_updated (source_updated_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS cn_macro_indicator_daily (
                id BIGINT NOT NULL AUTO_INCREMENT,
                trade_date DATE NOT NULL,
                hs300_pe_ttm DECIMAL(20, 8) NULL,
                csi1000_pe_ttm DECIMAL(20, 8) NULL,
                cn_gov_bond_10y_yield_pct DECIMAL(20, 8) NULL,
                a_share_total_market_cap_cny DECIMAL(30, 2) NULL,
                trailing_4q_nominal_gdp_cny DECIMAL(30, 2) NULL,
                household_deposit_cny DECIMAL(30, 2) NULL,
                hs300_equity_bond_spread_pp DECIMAL(20, 8) NULL,
                csi1000_equity_bond_spread_pp DECIMAL(20, 8) NULL,
                buffett_indicator_pct DECIMAL(20, 8) NULL,
                household_deposit_market_cap_ratio_pct DECIMAL(20, 8) NULL,
                gdp_period_end DATE NULL,
                deposit_period_end DATE NULL,
                market_cap_source VARCHAR(64) NULL,
                market_cap_adjustment_factor DECIMAL(20, 10) NULL,
                gdp_source VARCHAR(64) NULL,
                data_source VARCHAR(64) NOT NULL,
                created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_cn_macro_indicator_date (trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ]
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for statement in statements:
                    await cursor.execute(statement)
                await cursor.execute(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name IN "
                    "('cn_stock_market_cap_daily', 'cn_macro_indicator_daily')"
                )
                existing_columns = {}
                for table_name, column_name in await cursor.fetchall():
                    existing_columns.setdefault(table_name, set()).add(column_name)
                alter_columns = {
                    'cn_stock_market_cap_daily': {
                        'reference_gdp_cny': (
                            "ADD COLUMN reference_gdp_cny DECIMAL(30, 2) NULL "
                            "AFTER circulating_market_cap_cny"
                        ),
                    },
                    'cn_macro_indicator_daily': {
                        'market_cap_source': (
                            "ADD COLUMN market_cap_source VARCHAR(64) NULL "
                            "AFTER deposit_period_end"
                        ),
                        'market_cap_adjustment_factor': (
                            "ADD COLUMN market_cap_adjustment_factor DECIMAL(20, 10) NULL "
                            "AFTER market_cap_source"
                        ),
                        'gdp_source': (
                            "ADD COLUMN gdp_source VARCHAR(64) NULL "
                            "AFTER market_cap_adjustment_factor"
                        ),
                    },
                }
                for table_name, definitions in alter_columns.items():
                    known = existing_columns.get(table_name, set())
                    for column_name, clause in definitions.items():
                        if column_name not in known:
                            await cursor.execute(f"ALTER TABLE {table_name} {clause}")
                await conn.commit()
        self._cn_macro_tables_ready = True

    async def get_cn_trade_dates(self, start_date, end_date):
        query = """
            SELECT DISTINCT trade_date
            FROM index_daily_data
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date ASC
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, (start_date, end_date))
                return [row[0] for row in await cursor.fetchall()]

    async def _upsert_cn_macro_rows(self, query, values):
        if not values:
            return 0
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)
                await conn.commit()
        return len(values)

    async def upsert_cn_index_valuation_daily(self, rows):
        await self.ensure_cn_macro_tables()
        values = [(
            row.get('trade_date'), row.get('index_code'), row.get('index_name'),
            round(float(row.get('pe_ttm')), 8), round(float(row.get('earnings_yield_pct')), 8), row.get('data_source'),
            row.get('source_url'), self._serialize_json_field(row.get('raw_json')),
        ) for row in rows if row.get('trade_date') and row.get('index_code')]
        return await self._upsert_cn_macro_rows("""
            INSERT INTO cn_index_valuation_daily (
                trade_date, index_code, index_name, pe_ttm, earnings_yield_pct,
                data_source, source_url, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE index_name=VALUES(index_name), pe_ttm=VALUES(pe_ttm),
                earnings_yield_pct=VALUES(earnings_yield_pct), data_source=VALUES(data_source),
                source_url=VALUES(source_url), raw_json=VALUES(raw_json), updated_at=CURRENT_TIMESTAMP
        """, values)

    async def upsert_cn_government_bond_yield_daily(self, rows):
        await self.ensure_cn_macro_tables()
        values = [(
            row.get('trade_date'), row.get('tenor_years'), round(float(row.get('maturity_yield_pct')), 8),
            round(float(row.get('spot_yield_pct')), 8) if row.get('spot_yield_pct') is not None else None,
            round(float(row.get('forward_yield_pct')), 8) if row.get('forward_yield_pct') is not None else None,
            row.get('data_source'),
            row.get('source_url'), self._serialize_json_field(row.get('raw_json')),
        ) for row in rows if row.get('trade_date') and row.get('tenor_years')]
        return await self._upsert_cn_macro_rows("""
            INSERT INTO cn_government_bond_yield_daily (
                trade_date, tenor_years, maturity_yield_pct, spot_yield_pct,
                forward_yield_pct, data_source, source_url, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE maturity_yield_pct=VALUES(maturity_yield_pct),
                spot_yield_pct=VALUES(spot_yield_pct), forward_yield_pct=VALUES(forward_yield_pct),
                data_source=VALUES(data_source), source_url=VALUES(source_url),
                raw_json=VALUES(raw_json), updated_at=CURRENT_TIMESTAMP
        """, values)

    async def upsert_cn_stock_market_cap_daily(self, rows):
        await self.ensure_cn_macro_tables()
        values = [(
            row.get('trade_date'), row.get('exchange'), round(float(row.get('total_market_cap_cny')), 2),
            round(float(row.get('circulating_market_cap_cny')), 2) if row.get('circulating_market_cap_cny') is not None else None,
            round(float(row.get('reference_gdp_cny')), 2) if row.get('reference_gdp_cny') is not None else None,
            row.get('data_source'), row.get('source_url'),
            self._serialize_json_field(row.get('raw_json')),
        ) for row in rows if row.get('trade_date') and row.get('exchange')]
        return await self._upsert_cn_macro_rows("""
            INSERT INTO cn_stock_market_cap_daily (
                trade_date, exchange, total_market_cap_cny, circulating_market_cap_cny,
                reference_gdp_cny, data_source, source_url, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE total_market_cap_cny=VALUES(total_market_cap_cny),
                circulating_market_cap_cny=VALUES(circulating_market_cap_cny),
                reference_gdp_cny=VALUES(reference_gdp_cny),
                data_source=VALUES(data_source), source_url=VALUES(source_url),
                raw_json=VALUES(raw_json), updated_at=CURRENT_TIMESTAMP
        """, values)

    async def upsert_cn_gdp_quarterly(self, rows):
        await self.ensure_cn_macro_tables()
        values = [(
            row.get('period_end'), row.get('year'), row.get('quarter'), round(float(row.get('nominal_gdp_cny')), 2),
            round(float(row.get('cumulative_gdp_cny')), 2) if row.get('cumulative_gdp_cny') is not None else None,
            row.get('release_date'), row.get('data_source'),
            row.get('source_url'), self._serialize_json_field(row.get('raw_json')),
        ) for row in rows if row.get('period_end')]
        return await self._upsert_cn_macro_rows("""
            INSERT INTO cn_gdp_quarterly (
                period_end, year, quarter, nominal_gdp_cny, cumulative_gdp_cny,
                release_date, data_source, source_url, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE year=VALUES(year), quarter=VALUES(quarter),
                nominal_gdp_cny=VALUES(nominal_gdp_cny), cumulative_gdp_cny=VALUES(cumulative_gdp_cny),
                release_date=VALUES(release_date), data_source=VALUES(data_source),
                source_url=VALUES(source_url), raw_json=VALUES(raw_json), updated_at=CURRENT_TIMESTAMP
        """, values)

    async def upsert_cn_household_deposit_monthly(self, rows):
        await self.ensure_cn_macro_tables()
        values = [(
            row.get('period_end'), round(float(row.get('household_deposit_cny')), 2),
            round(float(row.get('demand_deposit_cny')), 2) if row.get('demand_deposit_cny') is not None else None,
            round(float(row.get('time_other_deposit_cny')), 2) if row.get('time_other_deposit_cny') is not None else None,
            str(row.get('source_updated_at') or '')[:10] or None, row.get('data_source'),
            row.get('source_url'), self._serialize_json_field(row.get('raw_json')),
        ) for row in rows if row.get('period_end')]
        return await self._upsert_cn_macro_rows("""
            INSERT INTO cn_household_deposit_monthly (
                period_end, household_deposit_cny, demand_deposit_cny,
                time_other_deposit_cny, source_updated_at, data_source, source_url, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE household_deposit_cny=VALUES(household_deposit_cny),
                demand_deposit_cny=VALUES(demand_deposit_cny),
                time_other_deposit_cny=VALUES(time_other_deposit_cny),
                source_updated_at=VALUES(source_updated_at), data_source=VALUES(data_source),
                source_url=VALUES(source_url), raw_json=VALUES(raw_json), updated_at=CURRENT_TIMESTAMP
        """, values)

    async def upsert_cn_macro_indicator_daily(self, rows):
        await self.ensure_cn_macro_tables()
        fields = (
            'trade_date', 'hs300_pe_ttm', 'csi1000_pe_ttm',
            'cn_gov_bond_10y_yield_pct', 'a_share_total_market_cap_cny',
            'trailing_4q_nominal_gdp_cny', 'household_deposit_cny',
            'hs300_equity_bond_spread_pp', 'csi1000_equity_bond_spread_pp',
            'buffett_indicator_pct', 'household_deposit_market_cap_ratio_pct',
            'gdp_period_end', 'deposit_period_end', 'market_cap_source',
            'market_cap_adjustment_factor', 'gdp_source', 'data_source',
        )
        date_fields = {'trade_date', 'gdp_period_end', 'deposit_period_end'}
        text_fields = {'market_cap_source', 'gdp_source', 'data_source'}
        values = [tuple(
            row.get(field)
            if field in date_fields or field in text_fields or row.get(field) is None
            else round(
                float(row.get(field)),
                10 if field == 'market_cap_adjustment_factor'
                else 8 if field.endswith(('_pct', '_pp', '_ttm'))
                else 2,
            )
            for field in fields
        ) for row in rows if row.get('trade_date')]
        return await self._upsert_cn_macro_rows("""
            INSERT INTO cn_macro_indicator_daily (
                trade_date, hs300_pe_ttm, csi1000_pe_ttm, cn_gov_bond_10y_yield_pct,
                a_share_total_market_cap_cny, trailing_4q_nominal_gdp_cny,
                household_deposit_cny, hs300_equity_bond_spread_pp,
                csi1000_equity_bond_spread_pp, buffett_indicator_pct,
                household_deposit_market_cap_ratio_pct, gdp_period_end,
                deposit_period_end, market_cap_source,
                market_cap_adjustment_factor, gdp_source, data_source
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE hs300_pe_ttm=VALUES(hs300_pe_ttm),
                csi1000_pe_ttm=VALUES(csi1000_pe_ttm),
                cn_gov_bond_10y_yield_pct=VALUES(cn_gov_bond_10y_yield_pct),
                a_share_total_market_cap_cny=VALUES(a_share_total_market_cap_cny),
                trailing_4q_nominal_gdp_cny=VALUES(trailing_4q_nominal_gdp_cny),
                household_deposit_cny=VALUES(household_deposit_cny),
                hs300_equity_bond_spread_pp=VALUES(hs300_equity_bond_spread_pp),
                csi1000_equity_bond_spread_pp=VALUES(csi1000_equity_bond_spread_pp),
                buffett_indicator_pct=VALUES(buffett_indicator_pct),
                household_deposit_market_cap_ratio_pct=VALUES(household_deposit_market_cap_ratio_pct),
                gdp_period_end=VALUES(gdp_period_end), deposit_period_end=VALUES(deposit_period_end),
                market_cap_source=VALUES(market_cap_source),
                market_cap_adjustment_factor=VALUES(market_cap_adjustment_factor),
                gdp_source=VALUES(gdp_source),
                data_source=VALUES(data_source), updated_at=CURRENT_TIMESTAMP
        """, values)

    async def get_cn_official_market_cap_complete_dates(self, start_date, end_date):
        await self.ensure_cn_macro_tables()
        query = """
            SELECT trade_date
            FROM cn_stock_market_cap_daily
            WHERE trade_date BETWEEN %s AND %s
              AND exchange IN ('SSE', 'SZSE', 'BSE')
            GROUP BY trade_date
            HAVING COUNT(DISTINCT exchange) = 3
            ORDER BY trade_date
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, (start_date, end_date))
                return {row[0] for row in await cursor.fetchall()}

    async def get_cn_macro_source_rows(self, start_date, end_date):
        await self.ensure_cn_macro_tables()
        market_cap_start = (
            start_date if isinstance(start_date, date)
            else date.fromisoformat(str(start_date)[:10])
        )
        market_cap_start = min(market_cap_start, date(2022, 1, 4))
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                async def fetch(query, params):
                    await cursor.execute(query, params)
                    return list(await cursor.fetchall())

                trade_dates = await fetch(
                    "SELECT DISTINCT trade_date FROM index_daily_data "
                    "WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date",
                    (start_date, end_date),
                )
                valuations = await fetch(
                    "SELECT * FROM cn_index_valuation_daily WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date",
                    (start_date, end_date),
                )
                yields = await fetch(
                    "SELECT * FROM cn_government_bond_yield_daily WHERE trade_date BETWEEN %s AND %s AND tenor_years=10 ORDER BY trade_date",
                    (start_date, end_date),
                )
                market_caps = await fetch(
                    "SELECT * FROM cn_stock_market_cap_daily WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date, exchange",
                    (market_cap_start, end_date),
                )
                gdp = await fetch(
                    "SELECT * FROM cn_gdp_quarterly WHERE period_end <= %s ORDER BY period_end",
                    (end_date,),
                )
                deposits = await fetch(
                    "SELECT * FROM cn_household_deposit_monthly WHERE period_end <= %s ORDER BY period_end",
                    (end_date,),
                )
        return {
            'trade_dates': [row['trade_date'] for row in trade_dates],
            'valuations': valuations,
            'yields': yields,
            'market_caps': market_caps,
            'gdp': gdp,
            'deposits': deposits,
        }
