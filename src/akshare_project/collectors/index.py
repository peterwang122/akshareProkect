import asyncio
import csv
import io
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
import requests

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.paths import get_cache_dir
from akshare_project.core.progress import ProgressStore
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 5
LOGGER = get_logger('index')
PROGRESS_STORE = ProgressStore('index')
US_OPTION_PRICE_PC_PROGRESS_STORE = ProgressStore('index_us_option_price_pc')

SPECIAL_INDEX_CODE = 'bj899050'
SPECIAL_INDEX_SIMPLE_CODE = '899050'
SPECIAL_INDEX_MARKET = 'bj'
SPECIAL_INDEX_NAME = '北证50'
SPECIAL_INDEX_SOURCE = 'stock_zh_index_daily'

CSI_DIVIDEND_INDEX_CODE = 'sh000922'
CSI_DIVIDEND_SIMPLE_CODE = '000922'
CSI_DIVIDEND_INDEX_NAME = '中证红利'
CSI_DIVIDEND_INDEX_SOURCE = 'csindex_official_index_perf'
CSI_DIVIDEND_INDEX_START_DATE = '20050101'
CSI_INDEX_PERF_URL = 'https://www.csindex.com.cn/csindex-home/perf/index-perf'
CSI_INDEX_PERF_VALUE_FIELDS = (
    'open', 'high', 'low', 'close', 'change', 'changePct', 'tradingVol', 'tradingValue'
)

US_INDEX_SOURCE = 'index_us_stock_sina'
HK_INDEX_SPOT_SOURCE = 'stock_hk_index_spot_sina'
HK_INDEX_DAILY_SOURCE = 'stock_hk_index_daily_sina'
NEWS_SENTIMENT_SOURCE = 'index_news_sentiment_scope'
QVIX_DAILY_CSV_URL = 'http://1.optbbs.com/d/csv/d/k.csv'
US_VIX_SOURCE = 'cboe_vix_history'
US_FEAR_GREED_LIVE_SOURCE = 'cnn_fear_greed_live'
US_FEAR_GREED_HISTORY_SOURCE = 'cnn_fear_greed_history'
US_FEAR_GREED_MIRROR_SOURCE = 'fear_greed_history_mirror'
US_HEDGE_PROXY_SOURCE = 'ofr_tff'
US_PUT_CALL_SOURCE = 'cboe_market_statistics'
US_OPTION_PREMIUM_SOURCE = 'optionomics_public_pulse'
US_OPTION_PRICE_PC_LIVE_SOURCE = 'nasdaq_public_option_chain'
US_OPTION_PRICE_PC_HISTORY_SOURCE = 'options_dataset_hist_mirror'
US_TREASURY_YIELD_SOURCE = 'fred_public_csv'
US_CREDIT_SPREAD_SOURCE = 'fred_public_csv'
CN_MARKET_FEAR_GREED_SOURCE = 'miumiu_market_fear_greed'
CN_BAIFENWEI_FEAR_GREED_SOURCE = 'baifenwei_fear_greed'

US_VIX_HISTORY_URL = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv'
US_PUT_CALL_HISTORY_URLS = {
    'total_put_call_ratio': 'https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/totalpc.csv',
    'index_put_call_ratio': 'https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/indexpc.csv',
    'equity_put_call_ratio': 'https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/equitypc.csv',
    'etf_put_call_ratio': 'https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/etppc.csv',
}
US_PUT_CALL_MARKET_STATS_URL = 'https://www.cboe.com/us/options/market_statistics/market/'
US_OPTION_PREMIUM_URL = 'https://optionomics.ai/pulse'
US_OPTION_PREMIUM_VALUE_BASIS = 'source_display_rounded_0.1m_usd'
US_OPTION_PREMIUM_ROUNDING_UNIT_MILLION_USD = 0.1
US_OPTION_PRICE_PC_NASDAQ_URL = 'https://api.nasdaq.com/api/quote/{symbol}/option-chain'
US_OPTION_PRICE_PC_HISTORY_BASE_URL = (
    'https://raw.githubusercontent.com/anahatsingh-ui/options-dataset-hist/main'
)
US_OPTION_PRICE_PC_HISTORY_REPOSITORY_URL = (
    'https://github.com/anahatsingh-ui/options-dataset-hist'
)
US_OPTION_PRICE_PC_PRODUCTS = {
    'SPY': {
        'index_name': '标普500指数',
        'index_code': '.INX',
        'product_name': 'SPY ETF期权',
        'history_start_year': 2008,
    },
    'QQQ': {
        'index_name': '纳斯达克100指数',
        'index_code': '.NDX',
        'product_name': 'QQQ ETF期权',
        'history_start_year': 2011,
    },
}
US_OPTION_PRICE_PC_HISTORY_END_YEAR = 2025
US_PUT_CALL_DAILY_JSON_URL_TEMPLATE = (
    'https://cdn.cboe.com/data/us/options/market_statistics/daily/{trade_date}_daily_options'
)
US_PUT_CALL_DAILY_JSON_START_DATE = '2019-10-05'
US_FEAR_GREED_CNN_URL = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'
CN_MARKET_FEAR_GREED_HISTORY_URL = 'https://www.miumiudashuju.com/api/index/history'
CN_BAIFENWEI_FEAR_GREED_SERIES_URL = 'https://baifenwei.com/data/fear-greed/series-1.json'
CN_BAIFENWEI_FEAR_GREED_SUBSCORES_URL = 'https://baifenwei.com/data/fear-greed/subscores.json'
US_FEAR_GREED_HISTORY_START_DATE = '2020-09-19'
US_FEAR_GREED_MIRROR_URLS = [
    (
        'https://raw.githubusercontent.com/whit3rabbit/fear-greed-data/main/'
        'datasets/hackingthemarkets_fear_greed_data.csv'
    ),
]
OFR_API_BASE_URL = 'https://data.financialresearch.gov/hf/v1'
FRED_CSV_URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv'
FRED_TREASURY_SERIES = {
    'yield_3m': 'DGS3MO',
    'yield_2y': 'DGS2',
    'yield_10y': 'DGS10',
    'yield_real_10y': 'DFII10',
}
FRED_HIGH_YIELD_OAS_SERIES = 'BAMLH0A0HYM2'
FRED_HIGH_YIELD_OAS_ARCHIVE_URL = (
    'https://raw.githubusercontent.com/maaurocp/Trading_Protocol/'
    'bf64e83fa4c2a6e72c37d3883476dc81bd9d2e31/data/raw/'
    'fred_BAMLH0A0HYM2.csv'
)
US_CREDIT_SPREAD_ARCHIVE_SOURCE = (
    'fred_archive:maaurocp@bf64e83f'
)
US_TREASURY_AVAILABLE_TIMEZONE = ZoneInfo('America/Chicago')
SHANGHAI_TIMEZONE = ZoneInfo('Asia/Shanghai')
US_TREASURY_DAILY_AVAILABLE_HOUR = 16
US_TREASURY_DAILY_SYNC_RECENT_DAYS = 10
US_CREDIT_SPREAD_AVAILABLE_HOUR = 0
US_CREDIT_SPREAD_AVAILABLE_MINUTE = 45
DEFAULT_HTTP_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/135.0.0.0 Safari/537.36'
    ),
    'Accept': '*/*',
}
QVIX_HTTP_HEADERS = {
    **DEFAULT_HTTP_HEADERS,
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Referer': 'http://1.optbbs.com/s/vix.shtml',
}
QVIX_DAILY_RECENT_BACKFILL_ROWS = 30
CNN_HTTP_HEADERS = {
    **DEFAULT_HTTP_HEADERS,
    'Accept': 'application/json,text/plain,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.cnn.com/markets/fear-and-greed',
    'Origin': 'https://www.cnn.com',
}
MIUMIU_HTTP_HEADERS = {
    **DEFAULT_HTTP_HEADERS,
    'Accept': 'application/json,text/plain,*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Referer': 'https://www.miumiudashuju.com/history',
}
BAIFENWEI_HTTP_HEADERS = {
    **DEFAULT_HTTP_HEADERS,
    'Accept': 'application/json,text/plain,*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Referer': 'https://baifenwei.com/indicator/fear-greed/',
}
BAIFENWEI_FEAR_GREED_WEIGHTS = {
    'volatility': 0.25,
    'relative_turnover_rate': 0.20,
    'margin_trading': 0.20,
    'market_breadth': 0.15,
    'rsi': 0.10,
    'limit_up_down_ratio': 0.10,
}
US_HEDGE_PROXY_DEFINITIONS = {
    'ES': {
        'long_mnemonic': 'TFF-LF_SP_LONG_POSITION',
        'short_mnemonic': 'TFF-LF_SP_SHORT_POSITION',
    },
    'NQ': {
        'long_mnemonic': 'TFF-LF_ND_LONG_POSITION',
        'short_mnemonic': 'TFF-LF_ND_SHORT_POSITION',
    },
}

QVIX_DEFINITIONS = [
    {
        'index_code': '50ETF_QVIX',
        'simple_code': '50ETF',
        'market': 'cn',
        'index_name': '50ETF QVIX',
        'data_source': 'index_option_50etf_qvix',
        'callable': ak.index_option_50etf_qvix,
        'daily_columns': [0, 1, 2, 3, 4],
    },
    {
        'index_code': '300ETF_QVIX',
        'simple_code': '300ETF',
        'market': 'cn',
        'index_name': '300ETF QVIX',
        'data_source': 'index_option_300etf_qvix',
        'callable': ak.index_option_300etf_qvix,
        'daily_columns': [0, 9, 10, 11, 12],
    },
    {
        'index_code': '500ETF_QVIX',
        'simple_code': '500ETF',
        'market': 'cn',
        'index_name': '500ETF QVIX',
        'data_source': 'index_option_500etf_qvix',
        'callable': ak.index_option_500etf_qvix,
        'daily_columns': [0, 67, 68, 69, 70],
    },
    {
        'index_code': 'CYB_QVIX',
        'simple_code': 'CYB',
        'market': 'cn',
        'index_name': 'CYB QVIX',
        'data_source': 'index_option_cyb_qvix',
        'callable': ak.index_option_cyb_qvix,
        'daily_columns': [0, 71, 72, 73, 74],
    },
    {
        'index_code': 'KCB_QVIX',
        'simple_code': 'KCB',
        'market': 'cn',
        'index_name': 'KCB QVIX',
        'data_source': 'index_option_kcb_qvix',
        'callable': ak.index_option_kcb_qvix,
        'daily_columns': [0, 83, 84, 85, 86],
    },
]

US_INDEX_DEFINITIONS = [
    {
        'index_code': '.IXIC',
        'simple_code': 'IXIC',
        'market': 'us',
        'index_name': '纳斯达克综合指数',
        'data_source': US_INDEX_SOURCE,
    },
    {
        'index_code': '.DJI',
        'simple_code': 'DJI',
        'market': 'us',
        'index_name': '道琼斯工业平均指数',
        'data_source': US_INDEX_SOURCE,
    },
    {
        'index_code': '.INX',
        'simple_code': 'INX',
        'market': 'us',
        'index_name': '标普500指数',
        'data_source': US_INDEX_SOURCE,
    },
    {
        'index_code': '.NDX',
        'simple_code': 'NDX',
        'market': 'us',
        'index_name': '纳斯达克100指数',
        'data_source': US_INDEX_SOURCE,
    },
]

COL_CODE = '代码'
COL_NAME = '名称'
COL_DATE = '日期'
COL_OPEN = '开盘'
COL_CLOSE = '收盘'
COL_LATEST = '最新价'
COL_HIGH = '最高'
COL_LOW = '最低'
COL_PRE_CLOSE = '昨收'
COL_SPOT_OPEN = '今开'
COL_VOLUME = '成交量'
COL_AMOUNT = '成交额'
COL_AMPLITUDE = '振幅'
COL_CHANGE_RATE = '涨跌幅'
COL_CHANGE_AMOUNT = '涨跌额'
COL_TURNOVER_RATE = '换手率'


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def parse_index_code(raw_code):
    code = str(raw_code or '').strip().lower()
    if not code:
        return '', '', ''

    match = re.match(r'([a-z]+)?(\d+)', code)
    if not match:
        return code, '', ''

    market = match.group(1) or ''
    simple_code = match.group(2)
    return code, simple_code, market


def parse_hk_index_code(raw_code):
    original_code = str(raw_code or '').strip()
    if not original_code:
        return '', '', ''

    # `stock_hk_index_spot_sina` 返回的代码本身已经是历史接口所需 symbol，
    # 例如 `HSI`、`CES100`、`HKL`。只有在极少数情况下传入了带供应商前缀的
    # 小写 `hkHSI` 这类值时，才需要去掉前缀；不能把真正的 `HKL` 误裁成 `L`。
    if original_code.startswith('hk') and len(original_code) > 2:
        normalized_code = original_code[2:].upper()
        return normalized_code, normalized_code, 'hk'

    normalized_code = original_code.upper()
    return normalized_code, normalized_code, 'hk'


def save_progress_batch(progress_lines):
    PROGRESS_STORE.append_lines(progress_lines)


def load_progress():
    return PROGRESS_STORE.load()


def log_error(index_code, trade_date, error_message):
    LOGGER.error('%s,%s,%s', index_code, trade_date, error_message)


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    return shared_fetch_with_retry(
        func,
        *args,
        retries=retries,
        sleep_seconds=sleep_seconds,
        logger=LOGGER,
        caller_name=LOGGER.name,
        **kwargs,
    )


def normalize_trade_date(value):
    if value is None:
        return ''
    return str(value).split(' ')[0]


def first_value(row, candidates):
    for candidate in candidates:
        if candidate in row:
            value = row[candidate]
            if value is not None:
                return value
    return None


def calculate_amplitude(high_price, low_price, pre_close):
    try:
        if high_price is None or low_price is None or pre_close in (None, 0):
            return None
        return round((float(high_price) - float(low_price)) / float(pre_close) * 100, 4)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def calculate_price_change(close_price, pre_close):
    try:
        if close_price is None or pre_close in (None, 0):
            return None, None
        price_change_amount = float(close_price) - float(pre_close)
        price_change_rate = price_change_amount / float(pre_close) * 100
        return round(price_change_amount, 4), round(price_change_rate, 4)
    except (TypeError, ValueError, ZeroDivisionError):
        return None, None


def normalize_http_date(value, fmt):
    normalized_value = str(value or '').strip()
    if not normalized_value:
        return ''
    return datetime.strptime(normalized_value, fmt).strftime('%Y-%m-%d')


def normalize_epoch_date(epoch_ms):
    if epoch_ms in (None, ''):
        return ''
    return datetime.fromtimestamp(float(epoch_ms) / 1000, tz=timezone.utc).strftime('%Y-%m-%d')


def normalize_iso_date(value):
    normalized_value = str(value or '').strip()
    if not normalized_value:
        return ''
    normalized_value = normalized_value.replace('Z', '+00:00')
    return datetime.fromisoformat(normalized_value).date().isoformat()


def to_float(value):
    try:
        if value in (None, ''):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_fear_greed_label(value):
    score = to_float(value)
    if score is None:
        return None
    if score <= 24:
        return 'EXTREME_FEAR'
    if score <= 49:
        return 'FEAR'
    if score == 50:
        return 'NEUTRAL'
    if score <= 74:
        return 'GREED'
    return 'EXTREME_GREED'


def infer_cftc_release_date(report_date):
    normalized_trade_date = normalize_trade_date(report_date)
    if not normalized_trade_date:
        return None
    return (datetime.strptime(normalized_trade_date, '%Y-%m-%d') + timedelta(days=3)).strftime('%Y-%m-%d')


def http_get(url, headers=None, timeout=30, params=None):
    last_error = None
    merged_headers = dict(DEFAULT_HTTP_HEADERS)
    if headers:
        merged_headers.update(headers)

    for attempt in range(API_RETRY_COUNT):
        try:
            response = requests.get(url, timeout=timeout, headers=merged_headers, params=params)
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            if attempt < API_RETRY_COUNT - 1:
                time.sleep(API_RETRY_SLEEP_SECONDS)

    raise last_error


def http_get_text(url, headers=None, timeout=30):
    return http_get(url, headers=headers, timeout=timeout).text


def http_get_json(url, headers=None, timeout=30):
    return http_get(url, headers=headers, timeout=timeout).json()


def fetch_us_vix_history_csv():
    return http_get_text(US_VIX_HISTORY_URL)


def fetch_us_put_call_history_csv(url):
    return http_get_text(url)


def fetch_us_put_call_market_stats_html():
    return http_get_text(US_PUT_CALL_MARKET_STATS_URL)


def fetch_us_option_premium_html():
    return http_get_text(
        US_OPTION_PREMIUM_URL,
        headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://optionomics.ai/',
        },
    )


def fetch_nasdaq_us_option_chain(symbol, from_date='all', to_date='undefined'):
    normalized_symbol = str(symbol or '').strip().upper()
    if normalized_symbol not in US_OPTION_PRICE_PC_PRODUCTS:
        raise ValueError(f'Unsupported US ETF option product: {normalized_symbol}')
    query = urlencode({
        'assetclass': 'etf',
        'limit': 5000,
        'fromdate': from_date,
        'todate': to_date,
        'excode': 'oprac',
        'callput': 'callput',
        'money': 'all',
        'type': 'all',
    })
    return http_get_json(
        f'{US_OPTION_PRICE_PC_NASDAQ_URL.format(symbol=normalized_symbol)}?{query}',
        headers={
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://www.nasdaq.com',
            'Referer': (
                f'https://www.nasdaq.com/market-activity/etf/'
                f'{normalized_symbol.lower()}/option-chain'
            ),
        },
    )


def fetch_us_put_call_daily_options_json(trade_date):
    normalized_trade_date = normalize_trade_date(trade_date)
    if not normalized_trade_date:
        return None
    url = US_PUT_CALL_DAILY_JSON_URL_TEMPLATE.format(trade_date=normalized_trade_date)
    last_error = None
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=30, headers=DEFAULT_HTTP_HEADERS)
            if response.status_code in {403, 404}:
                return None
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(API_RETRY_SLEEP_SECONDS)
    raise last_error


def fetch_fred_series_csv(series_id):
    try:
        return http_get(FRED_CSV_URL, params={'id': series_id}).text
    except Exception as exc:
        print(f'fred requests fetch failed for {series_id}, fallback to curl: {exc}')
        return fetch_fred_series_csv_with_curl(series_id)


def fetch_fred_series_csv_with_curl(series_id):
    url = f'{FRED_CSV_URL}?id={series_id}'
    completed = subprocess.run(
        ['curl', '-L', '--silent', '--show-error', '--max-time', '120', url],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f'curl failed for FRED series {series_id}: {completed.stderr.strip()}'
        )
    text = completed.stdout or ''
    if 'observation_date' not in text[:200]:
        raise RuntimeError(
            f'curl returned unexpected FRED payload for {series_id}: {text[:200]}'
        )
    return text


def fetch_us_credit_spread_archive_csv():
    """Fetch the pinned pre-restriction FRED export used only for old gaps."""
    try:
        return http_get(FRED_HIGH_YIELD_OAS_ARCHIVE_URL).text
    except Exception as exc:
        print(f'FRED HY OAS archive requests fetch failed, fallback to curl: {exc}')
        completed = subprocess.run(
            [
                'curl', '-L', '--silent', '--show-error', '--max-time', '120',
                FRED_HIGH_YIELD_OAS_ARCHIVE_URL,
            ],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f'curl failed for FRED HY OAS archive: {completed.stderr.strip()}'
            )
        text = completed.stdout or ''
        if FRED_HIGH_YIELD_OAS_SERIES not in text[:200]:
            raise ValueError('FRED HY OAS archive returned an unexpected response.')
        return text


def fetch_us_fear_greed_current_payload():
    return http_get_json(US_FEAR_GREED_CNN_URL, headers=CNN_HTTP_HEADERS)


def fetch_us_fear_greed_history_payload():
    history_url = f'{US_FEAR_GREED_CNN_URL}/{US_FEAR_GREED_HISTORY_START_DATE}'
    return http_get_json(history_url, headers=CNN_HTTP_HEADERS)


def fetch_us_fear_greed_history_mirror_csv():
    last_error = None
    for mirror_url in US_FEAR_GREED_MIRROR_URLS:
        try:
            return http_get_text(mirror_url)
        except Exception as exc:
            last_error = exc
    raise last_error


def fetch_qvix_daily_source():
    last_error = None
    session = requests.Session()
    session.trust_env = False
    for attempt in range(API_RETRY_COUNT):
        try:
            response = session.get(
                QVIX_DAILY_CSV_URL,
                params={'_': int(time.time())},
                headers=QVIX_HTTP_HEADERS,
                timeout=60,
            )
            response.raise_for_status()
            break
        except Exception as exc:
            last_error = exc
            if attempt < API_RETRY_COUNT - 1:
                time.sleep(API_RETRY_SLEEP_SECONDS)
    else:
        raise last_error

    text = ''
    for encoding in ('gbk', 'utf-8-sig', 'utf-8'):
        try:
            text = response.content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if not text:
        text = response.text

    source_df = pd.read_csv(io.StringIO(text))
    if source_df.empty:
        raise ValueError('OptBBS QVIX daily csv returned empty dataframe')
    return source_df


def build_qvix_history_from_source(index_row, source_df):
    column_indexes = list(index_row.get('daily_columns') or [])
    if not column_indexes:
        return pd.DataFrame()
    if source_df is None or source_df.empty:
        return pd.DataFrame()
    if max(column_indexes) >= len(source_df.columns):
        raise ValueError(
            f'QVIX source column count mismatch for {index_row["index_code"]}: '
            f'need index {max(column_indexes)}, got {len(source_df.columns)} columns'
        )

    temp_df = source_df.iloc[:, column_indexes].copy()
    temp_df.columns = ['date', 'open', 'high', 'low', 'close']
    temp_df['date'] = pd.to_datetime(temp_df['date'], errors='coerce').dt.date
    for column_name in ('open', 'high', 'low', 'close'):
        temp_df[column_name] = pd.to_numeric(temp_df[column_name], errors='coerce')
    temp_df.dropna(subset=['date'], inplace=True)
    return temp_df


def fetch_ofr_series_full(mnemonic):
    url = f'{OFR_API_BASE_URL}/series/full?mnemonic={mnemonic}&output=highcharts'
    payload = http_get_json(url)
    if mnemonic not in payload:
        raise ValueError(f'No OFR payload returned for {mnemonic}')
    return payload[mnemonic]


def build_calculated_history_rows(
    index_code,
    history_df,
    source_name,
    end_date=None,
    volume_candidates=None,
    turnover_candidates=None,
):
    daily_rows = []
    previous_close = None
    normalized_end_date = normalize_trade_date(end_date) if end_date else ''
    volume_candidates = volume_candidates or ['volume', COL_VOLUME]
    turnover_candidates = turnover_candidates or []

    for _, row in history_df.sort_values('date', ascending=True).iterrows():
        trade_date = normalize_trade_date(first_value(row, ['date', COL_DATE]))
        if not trade_date:
            continue
        if normalized_end_date and trade_date > normalized_end_date:
            continue

        open_price = first_value(row, ['open', COL_OPEN])
        close_price = first_value(row, ['close', COL_CLOSE])
        high_price = first_value(row, ['high', COL_HIGH])
        low_price = first_value(row, ['low', COL_LOW])

        amplitude = calculate_amplitude(high_price, low_price, previous_close)
        price_change_amount, price_change_rate = calculate_price_change(close_price, previous_close)

        daily_rows.append({
            'index_code': index_code,
            'open_price': open_price,
            'close_price': close_price,
            'high_price': high_price,
            'low_price': low_price,
            'volume': first_value(row, volume_candidates),
            'turnover': first_value(row, turnover_candidates),
            'amplitude': amplitude,
            'price_change_rate': price_change_rate,
            'price_change_amount': price_change_amount,
            'turnover_rate': None,
            'trade_date': trade_date,
            'data_source': source_name,
        })

        if close_price is not None:
            previous_close = close_price

    return daily_rows


def build_index_basic_rows(spot_df):
    basic_rows = []
    for _, row in spot_df.iterrows():
        index_code, simple_code, market = parse_index_code(row.get(COL_CODE))
        if not index_code:
            continue
        basic_rows.append({
            'index_code': index_code,
            'simple_code': simple_code,
            'market': market,
            'index_name': str(row.get(COL_NAME, '')).strip(),
            'data_source': 'stock_zh_index_spot_sina',
        })
    return basic_rows


def build_special_index_basic_row():
    return {
        'index_code': SPECIAL_INDEX_CODE,
        'simple_code': SPECIAL_INDEX_SIMPLE_CODE,
        'market': SPECIAL_INDEX_MARKET,
        'index_name': SPECIAL_INDEX_NAME,
        'data_source': SPECIAL_INDEX_SOURCE,
    }


def build_csi_dividend_index_basic_row():
    return {
        'index_code': CSI_DIVIDEND_INDEX_CODE,
        'simple_code': CSI_DIVIDEND_SIMPLE_CODE,
        'market': 'sh',
        'index_name': CSI_DIVIDEND_INDEX_NAME,
        'data_source': CSI_DIVIDEND_INDEX_SOURCE,
    }


def append_csi_dividend_index_basic_row(index_rows):
    deduped = {}
    for row in index_rows or []:
        index_code = str((row or {}).get('index_code', '')).strip().lower()
        if not index_code:
            continue
        deduped[index_code] = row
    deduped[CSI_DIVIDEND_INDEX_CODE] = build_csi_dividend_index_basic_row()
    return list(deduped.values())


def drop_csi_index_perf_boundary_duplicate(source_rows, start_date):
    rows = list(source_rows or [])
    if len(rows) < 2:
        return rows

    normalized_start_date = normalize_trade_date(start_date).replace('-', '')
    first_row = rows[0]
    second_row = rows[1]
    first_date = str((first_row or {}).get('tradeDate', '')).strip()
    second_date = str((second_row or {}).get('tradeDate', '')).strip()
    same_snapshot = all(
        (first_row or {}).get(field) == (second_row or {}).get(field)
        for field in CSI_INDEX_PERF_VALUE_FIELDS
    )
    if first_date == normalized_start_date and first_date != second_date and same_snapshot:
        return rows[1:]
    return rows


def fetch_csi_dividend_index_perf(start_date, end_date):
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        CSI_INDEX_PERF_URL,
        params={
            'indexCode': CSI_DIVIDEND_SIMPLE_CODE,
            'startDate': normalize_trade_date(start_date).replace('-', ''),
            'endDate': normalize_trade_date(end_date).replace('-', ''),
        },
        headers={
            **DEFAULT_HTTP_HEADERS,
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.csindex.com.cn/',
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get('data') if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError('CSI index performance response has no data rows')
    return drop_csi_index_perf_boundary_duplicate(rows, start_date)


def build_csi_dividend_index_daily_rows(source_rows):
    daily_rows = []
    for source_row in source_rows or []:
        if not isinstance(source_row, dict):
            continue
        raw_trade_date = normalize_trade_date(source_row.get('tradeDate'))
        try:
            trade_date = datetime.strptime(raw_trade_date, '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError:
            trade_date = raw_trade_date
        close_price = to_float(source_row.get('close'))
        if not trade_date or close_price is None or close_price <= 0:
            continue
        open_price = to_float(source_row.get('open'))
        high_price = to_float(source_row.get('high'))
        low_price = to_float(source_row.get('low'))
        change_amount = to_float(source_row.get('change'))
        previous_close = close_price - change_amount if change_amount is not None else None
        trading_value = to_float(source_row.get('tradingValue'))
        daily_rows.append({
            'index_code': CSI_DIVIDEND_INDEX_CODE,
            'open_price': open_price,
            'close_price': close_price,
            'high_price': high_price,
            'low_price': low_price,
            'volume': to_float(source_row.get('tradingVol')),
            'turnover': round(trading_value * 100_000_000, 2) if trading_value is not None else None,
            'amplitude': calculate_amplitude(high_price, low_price, previous_close),
            'price_change_rate': to_float(source_row.get('changePct')),
            'price_change_amount': change_amount,
            'turnover_rate': None,
            'trade_date': trade_date,
            'data_source': CSI_DIVIDEND_INDEX_SOURCE,
        })
    return sorted(daily_rows, key=lambda row: row['trade_date'])


def append_special_index_row(index_rows):
    deduped = {}
    for row in index_rows or []:
        index_code = str((row or {}).get('index_code', '')).strip().lower()
        if not index_code:
            continue
        deduped[index_code] = row
    deduped.setdefault(SPECIAL_INDEX_CODE, build_special_index_basic_row())
    return list(deduped.values())


def build_us_index_basic_rows():
    return [dict(index_row) for index_row in US_INDEX_DEFINITIONS]


def build_hk_index_basic_rows(spot_df):
    basic_rows = []
    for _, row in spot_df.iterrows():
        index_code, simple_code, market = parse_hk_index_code(row.get(COL_CODE))
        if not index_code:
            continue
        basic_rows.append({
            'index_code': index_code,
            'simple_code': simple_code,
            'market': market,
            'index_name': str(row.get(COL_NAME, '')).strip(),
            'data_source': HK_INDEX_SPOT_SOURCE,
        })
    return basic_rows


def build_qvix_basic_rows():
    return [
        {
            'index_code': row['index_code'],
            'simple_code': row['simple_code'],
            'market': row['market'],
            'index_name': row['index_name'],
            'data_source': row['data_source'],
        }
        for row in QVIX_DEFINITIONS
    ]


def build_qvix_daily_rows(index_code, history_df, source_name):
    if history_df is None or history_df.empty:
        return []

    normalized_df = history_df.copy()
    normalized_df['date'] = pd.to_datetime(normalized_df['date'], errors='coerce')
    normalized_df.dropna(subset=['date'], inplace=True)
    normalized_df = normalized_df[normalized_df['date'].dt.weekday < 5].copy()
    normalized_df['date'] = normalized_df['date'].dt.date

    for row_index, row in normalized_df.iterrows():
        prices = [
            to_float(row.get(column_name))
            for column_name in ('open', 'high', 'low', 'close')
        ]
        valid_prices = [value for value in prices if value is not None]
        if not valid_prices:
            continue
        normalized_df.at[row_index, 'high'] = max(valid_prices)
        normalized_df.at[row_index, 'low'] = min(valid_prices)

    return build_calculated_history_rows(
        index_code,
        normalized_df,
        source_name,
        volume_candidates=['volume'],
        turnover_candidates=[],
    )


def build_news_sentiment_scope_rows(history_df):
    if history_df is None or history_df.empty:
        return []

    temp_df = history_df.copy()
    if len(temp_df.columns) < 3:
        return []

    temp_df = temp_df.iloc[:, :3].copy()
    temp_df.columns = ['trade_date', 'sentiment_value', 'hs300_close']
    temp_df['trade_date'] = temp_df['trade_date'].astype(str)

    rows = []
    for _, row in temp_df.sort_values('trade_date', ascending=True).iterrows():
        trade_date = normalize_trade_date(row.get('trade_date'))
        if not trade_date:
            continue
        rows.append({
            'trade_date': trade_date,
            'sentiment_value': row.get('sentiment_value'),
            'hs300_close': row.get('hs300_close'),
            'data_source': NEWS_SENTIMENT_SOURCE,
        })
    return rows


def build_us_vix_daily_rows(csv_text):
    rows = []
    csv_reader = csv.DictReader(io.StringIO(csv_text or ''))
    for row in csv_reader:
        trade_date = normalize_http_date(row.get('DATE'), '%m/%d/%Y')
        open_value = to_float(row.get('OPEN'))
        high_value = to_float(row.get('HIGH'))
        low_value = to_float(row.get('LOW'))
        close_value = to_float(row.get('CLOSE'))
        if not trade_date or None in (open_value, high_value, low_value, close_value):
            continue
        rows.append({
            'trade_date': trade_date,
            'open_value': open_value,
            'high_value': high_value,
            'low_value': low_value,
            'close_value': close_value,
            'data_source': US_VIX_SOURCE,
        })
    return rows


def normalize_flexible_date(value):
    normalized_value = str(value or '').strip()
    if not normalized_value:
        return ''

    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%Y/%m/%d', '%b %d, %Y', '%B %d, %Y'):
        try:
            return datetime.strptime(normalized_value, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return ''


def normalize_csv_column(value):
    return re.sub(r'[^a-z0-9]+', '', str(value or '').strip().lower())


def find_csv_date_value(row):
    for key, value in (row or {}).items():
        if 'date' in normalize_csv_column(key):
            trade_date = normalize_flexible_date(value)
            if trade_date:
                return trade_date
    return ''


def find_put_call_ratio_value(row):
    ratio_candidates = []
    for key, value in (row or {}).items():
        normalized_key = normalize_csv_column(key)
        if not normalized_key:
            continue
        if 'ratio' in normalized_key or normalized_key in {'pc', 'pcratio', 'putcall'}:
            ratio_candidates.append(value)

    for value in ratio_candidates:
        numeric_value = to_float(value)
        if numeric_value is not None:
            return numeric_value
    return None


def build_us_put_call_ratio_rows_from_history_csv(csv_text, ratio_key):
    rows = []
    raw_lines = [line for line in str(csv_text or '').splitlines() if line.strip()]
    header_index = 0
    for index, line in enumerate(raw_lines):
        normalized_line = normalize_csv_column(line)
        if 'date' in normalized_line and ('pcratio' in normalized_line or 'putcall' in normalized_line):
            header_index = index
            break
    csv_reader = csv.DictReader(io.StringIO('\n'.join(raw_lines[header_index:])))
    for row in csv_reader:
        trade_date = find_csv_date_value(row)
        ratio_value = find_put_call_ratio_value(row)
        if not trade_date or ratio_value is None:
            continue
        rows.append({
            'trade_date': trade_date,
            ratio_key: ratio_value,
            'data_source': US_PUT_CALL_SOURCE,
        })
    return rows


def merge_us_put_call_ratio_rows(*row_groups):
    merged_rows = {}
    for rows in row_groups:
        for row in rows or []:
            trade_date = normalize_trade_date((row or {}).get('trade_date'))
            if not trade_date:
                continue
            target = merged_rows.setdefault(
                trade_date,
                {
                    'trade_date': trade_date,
                    'total_put_call_ratio': None,
                    'index_put_call_ratio': None,
                    'equity_put_call_ratio': None,
                    'etf_put_call_ratio': None,
                    'data_source': US_PUT_CALL_SOURCE,
                },
            )
            for key in (
                'total_put_call_ratio',
                'index_put_call_ratio',
                'equity_put_call_ratio',
                'etf_put_call_ratio',
            ):
                if key in row and row.get(key) is not None:
                    target[key] = row.get(key)
            if row.get('data_source'):
                target['data_source'] = row['data_source']
    return [merged_rows[trade_date] for trade_date in sorted(merged_rows)]


def build_us_put_call_ratio_row_from_daily_options_json(payload, trade_date):
    normalized_trade_date = normalize_trade_date(trade_date)
    if not normalized_trade_date:
        return None
    ratios = {
        'total_put_call_ratio': None,
        'index_put_call_ratio': None,
        'equity_put_call_ratio': None,
        'etf_put_call_ratio': None,
    }
    for item in (payload or {}).get('ratios', []) or []:
        name = normalize_csv_column((item or {}).get('name'))
        value = to_float((item or {}).get('value'))
        if value is None:
            continue
        if name == 'totalputcallratio':
            ratios['total_put_call_ratio'] = value
        elif name == 'indexputcallratio':
            ratios['index_put_call_ratio'] = value
        elif name == 'equityputcallratio':
            ratios['equity_put_call_ratio'] = value
        elif name == 'exchangetradedproductsputcallratio':
            ratios['etf_put_call_ratio'] = value

    if not any(value is not None for value in ratios.values()):
        return None
    return {
        'trade_date': normalized_trade_date,
        **ratios,
        'data_source': US_PUT_CALL_SOURCE,
    }


def parse_optionomics_option_premium_html(html_text):
    normalized_html = str(html_text or '')
    date_match = re.search(
        r'Live\s+board\s*(?:·|&middot;|&#183;|&#x0*B7;)\s*'
        r'([A-Za-z]+\s+\d{1,2},\s+\d{4})',
        normalized_html,
        flags=re.IGNORECASE,
    )
    total_match = re.search(
        r'\$\s*([\d,]+(?:\.\d+)?)\s*<small[^>]*>\s*M\s*</small>',
        normalized_html,
        flags=re.IGNORECASE,
    )
    call_match = re.search(
        r'\$\s*([\d,]+(?:\.\d+)?)\s*M\s+in\s+calls',
        normalized_html,
        flags=re.IGNORECASE,
    )
    put_match = re.search(
        r'\$\s*([\d,]+(?:\.\d+)?)\s*M\s+in\s+puts',
        normalized_html,
        flags=re.IGNORECASE,
    )
    if not all((date_match, total_match, call_match, put_match)):
        raise ValueError('Optionomics page is missing date or premium display values.')

    try:
        trade_date = datetime.strptime(date_match.group(1), '%B %d, %Y').strftime('%Y-%m-%d')
        total_premium = float(total_match.group(1).replace(',', ''))
        call_premium = float(call_match.group(1).replace(',', ''))
        put_premium = float(put_match.group(1).replace(',', ''))
    except (TypeError, ValueError) as exc:
        raise ValueError(f'Invalid Optionomics premium display values: {exc}') from exc

    if min(total_premium, call_premium, put_premium) <= 0:
        raise ValueError('Optionomics premium display values must be positive.')
    if abs(total_premium - call_premium - put_premium) > 0.2:
        raise ValueError(
            'Optionomics total premium does not match rounded call plus put premium: '
            f'{total_premium} != {call_premium} + {put_premium}'
        )

    return {
        'trade_date': trade_date,
        'total_premium_million_usd': round(total_premium, 1),
        'call_premium_million_usd': round(call_premium, 1),
        'put_premium_million_usd': round(put_premium, 1),
        'premium_put_call_ratio': round(put_premium / call_premium, 6),
        'rounding_unit_million_usd': US_OPTION_PREMIUM_ROUNDING_UNIT_MILLION_USD,
        'value_basis': US_OPTION_PREMIUM_VALUE_BASIS,
        'data_source': US_OPTION_PREMIUM_SOURCE,
        'source_url': US_OPTION_PREMIUM_URL,
        'raw_json': {
            'date_display': date_match.group(1),
            'total_display': f'${total_match.group(1)}M',
            'call_display': f'${call_match.group(1)}M',
            'put_display': f'${put_match.group(1)}M',
        },
    }


def parse_us_option_display_number(value):
    text = str(value or '').strip().replace('$', '').replace(',', '')
    if not text or text.lower() in {'--', 'n/a', 'na', 'null', 'none'}:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def parse_us_option_display_integer(value):
    numeric = parse_us_option_display_number(value)
    if numeric is None or numeric < 0:
        return None
    return int(numeric)


def parse_us_option_expiration(value, reference_date=None):
    text = str(value or '').strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d', '%B %d, %Y', '%b %d, %Y'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    reference_day = reference_date
    if isinstance(reference_day, str):
        try:
            reference_day = datetime.strptime(reference_day[:10], '%Y-%m-%d').date()
        except ValueError:
            reference_day = None
    if reference_day is None:
        return None
    for fmt in ('%B %d', '%b %d'):
        try:
            parsed = datetime.strptime(text, fmt).date().replace(year=reference_day.year)
        except ValueError:
            continue
        if parsed < reference_day - timedelta(days=31):
            parsed = parsed.replace(year=reference_day.year + 1)
        return parsed
    return None


def third_friday_of_us_month(year, month):
    first_day = datetime(int(year), int(month), 1).date()
    first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
    return first_friday + timedelta(days=14)


def is_standard_us_monthly_expiration(expiration):
    expiration_day = parse_us_option_expiration(expiration)
    if expiration_day is None:
        return False
    standard_day = third_friday_of_us_month(expiration_day.year, expiration_day.month)
    day_offset = (expiration_day - standard_day).days
    return -3 <= day_offset <= 1


def select_us_option_pc_expirations(expirations, trade_date):
    trade_day = parse_us_option_expiration(trade_date)
    if trade_day is None:
        raise ValueError(f'Invalid US option trade date: {trade_date}')
    parsed_expirations = sorted({
        expiration_day
        for value in (expirations if expirations is not None else [])
        if (expiration_day := parse_us_option_expiration(value, trade_day)) is not None
        and expiration_day > trade_day
        and is_standard_us_monthly_expiration(expiration_day.isoformat())
    })
    expirations_by_month = {}
    for expiration_day in parsed_expirations:
        expirations_by_month.setdefault(
            (expiration_day.year, expiration_day.month),
            [],
        ).append(expiration_day)
    sorted_expirations = []
    for (year, month), candidates in sorted(expirations_by_month.items()):
        standard_day = third_friday_of_us_month(year, month)
        priority_days = (
            standard_day,
            standard_day + timedelta(days=1),
            standard_day - timedelta(days=1),
            standard_day - timedelta(days=2),
            standard_day - timedelta(days=3),
        )
        selected_day = next(
            (candidate for candidate in priority_days if candidate in candidates),
            None,
        )
        if selected_day is not None:
            sorted_expirations.append(selected_day)
    selected = {
        'current_month': sorted_expirations[0] if len(sorted_expirations) >= 1 else None,
        'next_month': sorted_expirations[1] if len(sorted_expirations) >= 2 else None,
        'quarter_1': None,
        'quarter_2': None,
    }
    regular_expirations = {selected['current_month'], selected['next_month']}
    quarter_expirations = [
        expiration_day
        for expiration_day in sorted_expirations
        if expiration_day not in regular_expirations
        and expiration_day.month in {3, 6, 9, 12}
    ]
    selected['quarter_1'] = quarter_expirations[0] if len(quarter_expirations) >= 1 else None
    selected['quarter_2'] = quarter_expirations[1] if len(quarter_expirations) >= 2 else None
    return selected


def parse_nasdaq_option_chain_last_trade(payload):
    data = (payload or {}).get('data') or {}
    last_trade_text = str(data.get('lastTrade') or '').strip()
    price_match = re.search(r'\$\s*([\d,]+(?:\.\d+)?)', last_trade_text)
    date_match = re.search(
        r'AS\s+OF\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})',
        last_trade_text,
        flags=re.IGNORECASE,
    )
    underlying_close = parse_us_option_display_number(price_match.group(1)) if price_match else None
    trade_date = normalize_flexible_date(date_match.group(1)) if date_match else None
    if underlying_close is None or underlying_close <= 0 or not trade_date:
        raise ValueError(f'Nasdaq option chain has invalid lastTrade: {last_trade_text!r}')
    return trade_date, underlying_close, last_trade_text


def extract_nasdaq_option_expirations(payload):
    trade_date, _underlying_close, _last_trade_text = parse_nasdaq_option_chain_last_trade(payload)
    reference_day = datetime.strptime(trade_date, '%Y-%m-%d').date()
    table = ((payload or {}).get('data') or {}).get('table') or {}
    rows = table.get('rows') or []
    expirations = set()
    current_group_expiration = None
    for row in rows:
        row = row or {}
        group_value = row.get('expirygroup') or row.get('expiryGroup')
        group_expiration = parse_us_option_expiration(group_value, reference_day)
        if group_expiration is not None:
            current_group_expiration = group_expiration
            expirations.add(group_expiration)
        row_expiration = parse_us_option_expiration(
            row.get('expiryDate') or row.get('expirationDate'),
            reference_day,
        )
        if row_expiration is not None:
            expirations.add(row_expiration)
        elif current_group_expiration is not None and row.get('strike') not in (None, ''):
            expirations.add(current_group_expiration)
    return sorted(expirations)


def build_us_option_contract_code(symbol, expiration, option_type, strike_price):
    expiration_day = parse_us_option_expiration(expiration)
    normalized_type = str(option_type or '').strip().upper()
    strike = parse_us_option_display_number(strike_price)
    if expiration_day is None or normalized_type not in {'CALL', 'PUT'} or strike is None:
        return None
    side = 'C' if normalized_type == 'CALL' else 'P'
    return f'{str(symbol).strip().upper()}{expiration_day:%y%m%d}{side}{int(round(strike * 1000)):08d}'


def build_us_option_price_rows_from_nasdaq_payload(payload, symbol, expiration=None):
    normalized_symbol = str(symbol or '').strip().upper()
    product = US_OPTION_PRICE_PC_PRODUCTS.get(normalized_symbol)
    if product is None:
        raise ValueError(f'Unsupported US ETF option product: {normalized_symbol}')
    trade_date, underlying_close, last_trade_text = parse_nasdaq_option_chain_last_trade(payload)
    reference_day = datetime.strptime(trade_date, '%Y-%m-%d').date()
    fixed_expiration = parse_us_option_expiration(expiration, reference_day)
    table = ((payload or {}).get('data') or {}).get('table') or {}
    rows = table.get('rows') or []
    result = []
    current_group_expiration = fixed_expiration
    for source_row in rows:
        source_row = source_row or {}
        group_expiration = parse_us_option_expiration(
            source_row.get('expirygroup') or source_row.get('expiryGroup'),
            reference_day,
        )
        if group_expiration is not None:
            current_group_expiration = group_expiration
        row_expiration = (
            fixed_expiration
            or parse_us_option_expiration(
                source_row.get('expiryDate') or source_row.get('expirationDate'),
                reference_day,
            )
            or current_group_expiration
        )
        strike_price = parse_us_option_display_number(source_row.get('strike'))
        if row_expiration is None or strike_price is None:
            continue
        for option_type, prefix in (('CALL', 'c'), ('PUT', 'p')):
            close_price = parse_us_option_display_number(
                source_row.get(f'{prefix}_Last') or source_row.get(f'{prefix}_last')
            )
            volume = parse_us_option_display_integer(
                source_row.get(f'{prefix}_Volume') or source_row.get(f'{prefix}_volume')
            )
            if close_price is None or close_price <= 0 or volume is None or volume <= 0:
                continue
            open_interest = parse_us_option_display_integer(
                source_row.get(f'{prefix}_Openinterest')
                or source_row.get(f'{prefix}_OpenInterest')
                or source_row.get(f'{prefix}_openinterest')
            )
            result.append({
                'trade_date': trade_date,
                'index_code': product['index_code'],
                'index_name': product['index_name'],
                'underlying_code': normalized_symbol,
                'underlying_name': product['product_name'],
                'underlying_close': underlying_close,
                'contract_code': build_us_option_contract_code(
                    normalized_symbol,
                    row_expiration.isoformat(),
                    option_type,
                    strike_price,
                ),
                'expiration_date': row_expiration.isoformat(),
                'contract_month': row_expiration.strftime('%y%m'),
                'option_type': option_type,
                'strike_price': strike_price,
                'close_price': close_price,
                'volume': volume,
                'open_interest': open_interest,
                'value_basis': 'last_trade_with_positive_daily_volume',
                'data_source': US_OPTION_PRICE_PC_LIVE_SOURCE,
                'source_url': (
                    f'https://www.nasdaq.com/market-activity/etf/'
                    f'{normalized_symbol.lower()}/option-chain'
                ),
                'raw_json': {
                    'last_trade': last_trade_text,
                    'expiration': row_expiration.isoformat(),
                    'strike': source_row.get('strike'),
                    'last': source_row.get(f'{prefix}_Last') or source_row.get(f'{prefix}_last'),
                    'volume': source_row.get(f'{prefix}_Volume') or source_row.get(f'{prefix}_volume'),
                    'open_interest': (
                        source_row.get(f'{prefix}_Openinterest')
                        or source_row.get(f'{prefix}_OpenInterest')
                        or source_row.get(f'{prefix}_openinterest')
                    ),
                },
            })
    return result


def select_adjacent_us_option_price_rows(rows, underlying_close):
    target = parse_us_option_display_number(underlying_close)
    if target is None or target <= 0:
        return []
    selected = []
    grouped = {}
    for row in rows or []:
        expiration_date = normalize_trade_date(row.get('expiration_date'))
        option_type = str(row.get('option_type') or '').strip().upper()
        strike_price = parse_us_option_display_number(row.get('strike_price'))
        close_price = parse_us_option_display_number(row.get('close_price'))
        if (
            not expiration_date
            or option_type not in {'CALL', 'PUT'}
            or strike_price is None
            or close_price is None
            or close_price <= 0
        ):
            continue
        grouped.setdefault((expiration_date, option_type), {})[strike_price] = row

    for point_rows in grouped.values():
        strikes = sorted(point_rows)
        exact = next((strike for strike in strikes if abs(strike - target) < 1e-9), None)
        if exact is not None:
            selected.append(point_rows[exact])
            continue
        lower = next((strike for strike in reversed(strikes) if strike < target), None)
        upper = next((strike for strike in strikes if strike > target), None)
        if lower is not None and upper is not None:
            selected.extend((point_rows[lower], point_rows[upper]))
    return selected


def _normalize_history_option_type(value):
    normalized = str(value or '').strip().upper()
    if normalized in {'C', 'CALL'}:
        return 'CALL'
    if normalized in {'P', 'PUT'}:
        return 'PUT'
    return None


def _history_underlying_close_map(underlying_frame):
    if underlying_frame is None or underlying_frame.empty:
        return {}
    columns = {str(column).strip().lower(): column for column in underlying_frame.columns}
    date_column = columns.get('date') or columns.get('trade_date')
    close_column = columns.get('close')
    if date_column is None or close_column is None:
        raise ValueError('US option history underlying file is missing date or close.')
    result = {}
    for raw_date, raw_close in zip(underlying_frame[date_column], underlying_frame[close_column]):
        trade_date = normalize_trade_date(raw_date)
        close_price = to_float(raw_close)
        if trade_date and close_price is not None and close_price > 0:
            result[trade_date] = close_price
    return result


def build_us_option_price_rows_from_history_frames(
    option_frame,
    underlying_frame,
    symbol,
    start_date=None,
    end_date=None,
    source_url=None,
):
    normalized_symbol = str(symbol or '').strip().upper()
    product = US_OPTION_PRICE_PC_PRODUCTS.get(normalized_symbol)
    if product is None:
        raise ValueError(f'Unsupported US ETF option product: {normalized_symbol}')
    if option_frame is None or option_frame.empty:
        return []
    columns = {str(column).strip().lower(): column for column in option_frame.columns}
    required = ('date', 'expiration', 'strike', 'type', 'last', 'volume')
    missing = [column for column in required if column not in columns]
    if missing:
        raise ValueError('US option history file is missing columns: ' + ', '.join(missing))
    underlying_close_map = _history_underlying_close_map(underlying_frame)
    normalized_start = normalize_trade_date(start_date) if start_date else None
    normalized_end = normalize_trade_date(end_date) if end_date else None

    working = option_frame[[columns[column] for column in required] + [
        column for key, column in columns.items()
        if key in {'contract_id', 'open_interest'} and column not in {columns[item] for item in required}
    ]].copy()
    working['_trade_date'] = working[columns['date']].map(normalize_trade_date)
    working['_expiration_date'] = working[columns['expiration']].map(normalize_trade_date)
    working['_option_type'] = working[columns['type']].map(_normalize_history_option_type)
    working['_strike_price'] = pd.to_numeric(working[columns['strike']], errors='coerce')
    working['_close_price'] = pd.to_numeric(working[columns['last']], errors='coerce')
    working['_volume'] = pd.to_numeric(working[columns['volume']], errors='coerce')
    working = working[
        working['_trade_date'].notna()
        & working['_expiration_date'].notna()
        & working['_option_type'].notna()
        & (working['_close_price'] > 0)
        & (working['_volume'] > 0)
    ]
    if normalized_start:
        working = working[working['_trade_date'] >= normalized_start]
    if normalized_end:
        working = working[working['_trade_date'] <= normalized_end]

    result = []
    contract_id_column = columns.get('contract_id')
    open_interest_column = columns.get('open_interest')
    for trade_date, daily_frame in working.groupby('_trade_date', sort=True):
        underlying_close = underlying_close_map.get(trade_date)
        if underlying_close is None:
            continue
        selected_expirations = select_us_option_pc_expirations(
            daily_frame['_expiration_date'].unique(),
            trade_date,
        )
        expiration_values = {
            expiration.isoformat()
            for expiration in selected_expirations.values()
            if expiration is not None
        }
        if not expiration_values:
            continue
        candidate_rows = []
        for _index, source_row in daily_frame[
            daily_frame['_expiration_date'].isin(expiration_values)
        ].iterrows():
            expiration_date = source_row['_expiration_date']
            option_type = source_row['_option_type']
            strike_price = float(source_row['_strike_price'])
            close_price = float(source_row['_close_price'])
            volume = int(source_row['_volume'])
            open_interest = (
                parse_us_option_display_integer(source_row.get(open_interest_column))
                if open_interest_column is not None
                else None
            )
            contract_code = (
                str(source_row.get(contract_id_column) or '').strip()
                if contract_id_column is not None
                else ''
            ) or build_us_option_contract_code(
                normalized_symbol,
                expiration_date,
                option_type,
                strike_price,
            )
            candidate_rows.append({
                'trade_date': trade_date,
                'index_code': product['index_code'],
                'index_name': product['index_name'],
                'underlying_code': normalized_symbol,
                'underlying_name': product['product_name'],
                'underlying_close': underlying_close,
                'contract_code': contract_code,
                'expiration_date': expiration_date,
                'contract_month': expiration_date[2:4] + expiration_date[5:7],
                'option_type': option_type,
                'strike_price': strike_price,
                'close_price': close_price,
                'volume': volume,
                'open_interest': open_interest,
                'value_basis': 'last_trade_with_positive_daily_volume',
                'data_source': US_OPTION_PRICE_PC_HISTORY_SOURCE,
                'source_url': source_url or US_OPTION_PRICE_PC_HISTORY_REPOSITORY_URL,
                'raw_json': {
                    'date': trade_date,
                    'expiration': expiration_date,
                    'type': source_row.get(columns['type']),
                    'strike': strike_price,
                    'last': close_price,
                    'volume': volume,
                    'open_interest': open_interest,
                },
            })
        result.extend(select_adjacent_us_option_price_rows(candidate_rows, underlying_close))
    return result


def build_weekday_date_strings(start_date, end_date):
    start = datetime.strptime(normalize_trade_date(start_date), '%Y-%m-%d').date()
    end = datetime.strptime(normalize_trade_date(end_date), '%Y-%m-%d').date()
    dates = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    return dates


def extract_current_put_call_ratio_from_html(html_text):
    if not html_text:
        return None

    date_match = re.search(
        r'Market Statistics for\s+[A-Za-z]+,\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})',
        html_text,
        flags=re.IGNORECASE,
    )
    trade_date = normalize_flexible_date(date_match.group(1)) if date_match else datetime.now().strftime('%Y-%m-%d')

    try:
        tables = pd.read_html(io.StringIO(html_text))
    except Exception:
        tables = []

    ratios = {
        'total_put_call_ratio': None,
        'index_put_call_ratio': None,
        'equity_put_call_ratio': None,
        'etf_put_call_ratio': None,
    }

    section_blocks = re.findall(
        r'<h3>\s*([^<]+?)\s*</h3>\s*(<table\b.*?</table>)',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for raw_heading, table_html in section_blocks:
        heading = normalize_csv_column(raw_heading)
        if heading not in {'total', 'indexoptions', 'equityoptions'}:
            continue
        try:
            section_tables = pd.read_html(io.StringIO(table_html))
        except Exception:
            continue
        if not section_tables:
            continue
        table = section_tables[0]
        ratio_value = find_last_table_ratio_value(table)
        if ratio_value is None:
            continue
        if heading == 'total':
            ratios['total_put_call_ratio'] = ratio_value
        elif heading == 'indexoptions':
            ratios['index_put_call_ratio'] = ratio_value
        elif heading == 'equityoptions':
            ratios['equity_put_call_ratio'] = ratio_value

    for table in tables:
        if table is None or table.empty:
            continue
        table_columns = [normalize_csv_column(column) for column in table.columns]
        ratio_columns = [
            index
            for index, column_name in enumerate(table_columns)
            if 'putcall' in column_name or 'pcratio' in column_name or 'ratio' == column_name
        ]
        if not ratio_columns:
            continue

        for _, raw_row in table.iterrows():
            row_values = [str(value or '').strip() for value in raw_row.tolist()]
            row_text = ' '.join(row_values).lower()
            ratio_value = None
            for column_index in ratio_columns:
                if column_index < len(row_values):
                    ratio_value = to_float(row_values[column_index])
                    if ratio_value is not None:
                        break
            if ratio_value is None:
                continue
            if 'equity' in row_text and ratios['equity_put_call_ratio'] is None:
                ratios['equity_put_call_ratio'] = ratio_value
            elif ('index' in row_text or 'idx' in row_text) and ratios['index_put_call_ratio'] is None:
                ratios['index_put_call_ratio'] = ratio_value
            elif ('etf' in row_text or 'etp' in row_text) and ratios['etf_put_call_ratio'] is None:
                ratios['etf_put_call_ratio'] = ratio_value
            elif 'total' in row_text and ratios['total_put_call_ratio'] is None:
                ratios['total_put_call_ratio'] = ratio_value

    if not any(value is not None for value in ratios.values()):
        return None
    return {
        'trade_date': trade_date,
        **ratios,
        'data_source': US_PUT_CALL_SOURCE,
    }


def find_last_table_ratio_value(table):
    if table is None or table.empty:
        return None
    ratio_column = None
    for column in table.columns:
        normalized_column = normalize_csv_column(column)
        if 'pcratio' in normalized_column or 'putcallratio' in normalized_column:
            ratio_column = column
            break
    if ratio_column is None:
        return None
    for value in reversed(table[ratio_column].tolist()):
        ratio_value = to_float(value)
        if ratio_value is not None:
            return ratio_value
    return None


def build_fred_series_points(csv_text, series_id):
    points = {}
    csv_reader = csv.DictReader(io.StringIO(csv_text or ''))
    for row in csv_reader:
        trade_date = normalize_flexible_date(row.get('observation_date') or row.get('DATE') or row.get('date'))
        value = to_float(row.get(series_id) or row.get('value') or row.get('VALUE'))
        if not trade_date:
            continue
        points[trade_date] = value
    return points


def build_us_treasury_yield_rows(series_maps):
    dates = sorted(set().union(*(set(points.keys()) for points in series_maps.values())))
    rows = []
    for trade_date in dates:
        yield_3m = series_maps.get('yield_3m', {}).get(trade_date)
        yield_2y = series_maps.get('yield_2y', {}).get(trade_date)
        yield_10y = series_maps.get('yield_10y', {}).get(trade_date)
        yield_real_10y = series_maps.get('yield_real_10y', {}).get(trade_date)
        spread_10y_2y = round(yield_10y - yield_2y, 4) if yield_10y is not None and yield_2y is not None else None
        spread_10y_3m = round(yield_10y - yield_3m, 4) if yield_10y is not None and yield_3m is not None else None
        if all(value is None for value in (yield_3m, yield_2y, yield_10y, yield_real_10y)):
            continue
        rows.append({
            'trade_date': trade_date,
            'yield_3m': yield_3m,
            'yield_2y': yield_2y,
            'yield_10y': yield_10y,
            'yield_real_10y': yield_real_10y,
            'spread_10y_2y': spread_10y_2y,
            'spread_10y_3m': spread_10y_3m,
            'data_source': US_TREASURY_YIELD_SOURCE,
        })
    return attach_us_treasury_available_at(rows)


def us_treasury_available_at(trade_date: str, next_us_business_date: str) -> str | None:
    """观测日后的下一个美国工作日 16:00 America/Chicago，转换为 Asia/Shanghai。"""
    if not trade_date:
        return None
    if not next_us_business_date:
        next_us_business_date = _next_us_business_day(trade_date, [])
    try:
        publish_at = datetime.combine(
            datetime.strptime(next_us_business_date, '%Y-%m-%d').date(),
            datetime.min.time().replace(hour=US_TREASURY_DAILY_AVAILABLE_HOUR),
            tzinfo=US_TREASURY_AVAILABLE_TIMEZONE,
        )
    except (TypeError, ValueError):
        return None
    return publish_at.astimezone(SHANGHAI_TIMEZONE).isoformat(timespec='seconds')


def _next_us_business_day(trade_date: str, _sorted_dates: list[str] | None = None) -> str | None:
    try:
        cursor = datetime.strptime(trade_date, '%Y-%m-%d').date()
    except (TypeError, ValueError):
        return None
    cursor = cursor + timedelta(days=1)
    while cursor.weekday() >= 5 or cursor in _us_federal_holidays(cursor.year):
        cursor = cursor + timedelta(days=1)
    return cursor.isoformat()


@lru_cache(maxsize=16)
def _us_federal_holidays(year: int) -> set:
    """完整美国联邦假日及观察日，包括跨年落在12月31日的元旦观察日。"""
    calendar = USFederalHolidayCalendar()
    return {
        value.date()
        for value in calendar.holidays(
            start=f"{year}-01-01",
            end=f"{year}-12-31",
        )
    }


def attach_us_treasury_available_at(rows):
    """按观测日后的真实美国联邦工作日计算公开可用时间。"""
    return [
        {
            **row,
            'available_at': us_treasury_available_at(
                row['trade_date'],
                _next_us_business_day(row['trade_date']),
            ),
        }
        for row in rows
        if row.get('trade_date')
    ]


def us_credit_spread_available_at(trade_date: str, next_us_business_date: str) -> str | None:
    """HY OAS 公开可用时间：观测日后的下一个美国工作日 00:45 Asia/Shanghai。"""
    if not trade_date:
        return None
    if not next_us_business_date:
        next_us_business_date = _next_us_business_day(trade_date, [])
    try:
        publish_at = datetime.combine(
            datetime.strptime(next_us_business_date, '%Y-%m-%d').date(),
            datetime.min.time().replace(
                hour=US_CREDIT_SPREAD_AVAILABLE_HOUR,
                minute=US_CREDIT_SPREAD_AVAILABLE_MINUTE,
            ),
            tzinfo=SHANGHAI_TIMEZONE,
        )
    except (TypeError, ValueError):
        return None
    return publish_at.isoformat(timespec='seconds')


def attach_us_credit_spread_available_at(rows):
    return [
        {
            **row,
            'available_at': us_credit_spread_available_at(
                row['trade_date'],
                _next_us_business_day(row['trade_date']),
            ),
        }
        for row in rows
        if row.get('trade_date')
    ]


def build_us_credit_spread_rows(csv_text):
    points = build_fred_series_points(csv_text, FRED_HIGH_YIELD_OAS_SERIES)
    rows = [
        {
            'trade_date': trade_date,
            'high_yield_oas': value,
            'data_source': US_CREDIT_SPREAD_SOURCE,
        }
        for trade_date, value in sorted(points.items())
        if value is not None
    ]
    return attach_us_credit_spread_available_at(rows)


def build_us_credit_spread_archive_rows(csv_text):
    points = build_fred_series_points(csv_text, FRED_HIGH_YIELD_OAS_SERIES)
    rows = [
        {
            'trade_date': trade_date,
            'high_yield_oas': value,
            'data_source': US_CREDIT_SPREAD_ARCHIVE_SOURCE,
        }
        for trade_date, value in sorted(points.items())
        if value is not None
    ]
    return attach_us_credit_spread_available_at(rows)


def build_us_fear_greed_rows_from_mirror(csv_text):
    rows = []
    csv_reader = csv.DictReader(io.StringIO(csv_text or ''))
    for row in csv_reader:
        trade_date = normalize_http_date(row.get('Date'), '%m/%d/%Y')
        fear_greed_value = to_float(row.get('Fear Greed'))
        if not trade_date or fear_greed_value is None:
            continue
        rows.append({
            'trade_date': trade_date,
            'fear_greed_value': fear_greed_value,
            'sentiment_label': infer_fear_greed_label(fear_greed_value),
            'data_source': US_FEAR_GREED_MIRROR_SOURCE,
        })
    return rows


def build_us_fear_greed_rows_from_cnn_history(payload):
    rows = []
    historical_points = (
        (payload or {})
        .get('fear_and_greed_historical', {})
        .get('data', [])
    )
    for point in historical_points:
        trade_date = normalize_epoch_date(point.get('x'))
        fear_greed_value = to_float(point.get('y'))
        if not trade_date or fear_greed_value is None:
            continue
        rows.append({
            'trade_date': trade_date,
            'fear_greed_value': fear_greed_value,
            'sentiment_label': infer_fear_greed_label(fear_greed_value),
            'data_source': US_FEAR_GREED_HISTORY_SOURCE,
        })
    return rows


def build_us_fear_greed_current_row(payload):
    fear_and_greed = (payload or {}).get('fear_and_greed', {})
    trade_date = normalize_iso_date(fear_and_greed.get('timestamp'))
    fear_greed_value = to_float(fear_and_greed.get('score'))
    if not trade_date or fear_greed_value is None:
        raise ValueError('No valid CNN Fear & Greed current row returned')
    return {
        'trade_date': trade_date,
        'fear_greed_value': fear_greed_value,
        'sentiment_label': infer_fear_greed_label(fear_greed_value),
        'data_source': US_FEAR_GREED_LIVE_SOURCE,
    }


def merge_us_fear_greed_rows(*row_groups):
    merged_rows = {}
    for rows in row_groups:
        for row in rows or []:
            trade_date = normalize_trade_date((row or {}).get('trade_date'))
            if not trade_date:
                continue
            merged_rows[trade_date] = dict(row)
            merged_rows[trade_date]['trade_date'] = trade_date
    return [merged_rows[trade_date] for trade_date in sorted(merged_rows)]


def build_us_hedge_fund_ls_proxy_rows(contract_scope, long_series, short_series):
    long_points = {
        normalize_epoch_date(point[0]): to_float(point[1])
        for point in (long_series or [])
        if point and len(point) >= 2
    }
    short_points = {
        normalize_epoch_date(point[0]): to_float(point[1])
        for point in (short_series or [])
        if point and len(point) >= 2
    }

    rows = []
    for report_date in sorted(set(long_points) & set(short_points)):
        long_value = long_points.get(report_date)
        short_value = short_points.get(report_date)
        ratio_value = None
        if short_value not in (None, 0) and long_value is not None:
            ratio_value = long_value / short_value
        rows.append({
            'report_date': report_date,
            'contract_scope': contract_scope,
            'long_value': long_value,
            'short_value': short_value,
            'ratio_value': ratio_value,
            'release_date': infer_cftc_release_date(report_date),
            'data_source': US_HEDGE_PROXY_SOURCE,
        })
    return rows


def build_special_index_daily_rows(history_df, end_date=None):
    return build_calculated_history_rows(
        SPECIAL_INDEX_CODE,
        history_df,
        SPECIAL_INDEX_SOURCE,
        end_date=end_date,
        volume_candidates=['volume', COL_VOLUME],
        turnover_candidates=[],
    )


def build_us_index_daily_rows(index_code, history_df):
    return build_calculated_history_rows(
        index_code,
        history_df,
        US_INDEX_SOURCE,
        volume_candidates=['volume', COL_VOLUME],
        turnover_candidates=['amount', COL_AMOUNT],
    )


def build_hk_index_history_rows(index_code, history_df):
    return build_calculated_history_rows(
        index_code,
        history_df,
        HK_INDEX_DAILY_SOURCE,
        volume_candidates=['volume', COL_VOLUME],
        turnover_candidates=[],
    )


def build_index_daily_rows(index_code, history_df, source_name):
    if str(source_name or '').strip() == SPECIAL_INDEX_SOURCE:
        return build_special_index_daily_rows(history_df)

    daily_rows = []
    for _, row in history_df.iterrows():
        trade_date = normalize_trade_date(first_value(row, [COL_DATE, 'date']))
        if not trade_date:
            continue

        daily_rows.append({
            'index_code': index_code,
            'open_price': first_value(row, [COL_OPEN, 'open']),
            'close_price': first_value(row, [COL_CLOSE, 'close']),
            'high_price': first_value(row, [COL_HIGH, 'high']),
            'low_price': first_value(row, [COL_LOW, 'low']),
            'volume': first_value(row, [COL_VOLUME, 'volume']),
            'turnover': first_value(row, [COL_AMOUNT, 'amount']),
            'amplitude': first_value(row, [COL_AMPLITUDE]),
            'price_change_rate': first_value(row, [COL_CHANGE_RATE]),
            'price_change_amount': first_value(row, [COL_CHANGE_AMOUNT]),
            'turnover_rate': first_value(row, [COL_TURNOVER_RATE]),
            'trade_date': trade_date,
            'data_source': source_name,
        })
    return daily_rows


def build_index_spot_daily_rows(spot_df, trade_date):
    rows = []
    for _, row in spot_df.iterrows():
        index_code, _, _ = parse_index_code(row.get(COL_CODE))
        if not index_code:
            continue

        high_price = row.get(COL_HIGH)
        low_price = row.get(COL_LOW)
        pre_close = row.get(COL_PRE_CLOSE)
        rows.append({
            'index_code': index_code,
            'open_price': row.get(COL_SPOT_OPEN),
            'close_price': row.get(COL_LATEST),
            'high_price': high_price,
            'low_price': low_price,
            'volume': row.get(COL_VOLUME),
            'turnover': row.get(COL_AMOUNT),
            'amplitude': calculate_amplitude(high_price, low_price, pre_close),
            'price_change_rate': row.get(COL_CHANGE_RATE),
            'price_change_amount': row.get(COL_CHANGE_AMOUNT),
            'turnover_rate': None,
            'trade_date': trade_date,
            'data_source': 'stock_zh_index_spot_sina',
        })
    return rows


def build_hk_index_spot_daily_rows(spot_df, trade_date):
    rows = []
    for _, row in spot_df.iterrows():
        index_code, _, _ = parse_hk_index_code(row.get(COL_CODE))
        if not index_code:
            continue

        high_price = row.get(COL_HIGH)
        low_price = row.get(COL_LOW)
        pre_close = row.get(COL_PRE_CLOSE)
        rows.append({
            'index_code': index_code,
            'open_price': row.get(COL_SPOT_OPEN),
            'close_price': row.get(COL_LATEST),
            'high_price': high_price,
            'low_price': low_price,
            'volume': None,
            'turnover': None,
            'amplitude': calculate_amplitude(high_price, low_price, pre_close),
            'price_change_rate': row.get(COL_CHANGE_RATE),
            'price_change_amount': row.get(COL_CHANGE_AMOUNT),
            'turnover_rate': None,
            'trade_date': trade_date,
            'data_source': HK_INDEX_SPOT_SOURCE,
        })
    return rows


def get_all_index_spot():
    return fetch_with_retry(ak.stock_zh_index_spot_sina)


def get_special_index_daily():
    return fetch_with_retry(ak.stock_zh_index_daily, symbol=SPECIAL_INDEX_CODE)


def get_us_index_history(index_code):
    return fetch_with_retry(ak.index_us_stock_sina, symbol=index_code)


def get_hk_index_spot():
    return fetch_with_retry(ak.stock_hk_index_spot_sina)


def get_hk_index_history(simple_code):
    return fetch_with_retry(ak.stock_hk_index_daily_sina, symbol=simple_code)


def get_news_sentiment_scope():
    return fetch_with_retry(ak.index_news_sentiment_scope)


def get_index_history(index_code, simple_code, end_date):
    last_error = None

    if str(index_code or '').strip().lower() == SPECIAL_INDEX_CODE:
        history_df = fetch_with_retry(ak.stock_zh_index_daily, symbol=SPECIAL_INDEX_CODE)
        if history_df is not None and not history_df.empty:
            normalized_end_date = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
            history_df = history_df[history_df['date'].astype(str) <= normalized_end_date]
            return history_df, SPECIAL_INDEX_SOURCE
        raise ValueError(f'No history data returned for {SPECIAL_INDEX_CODE}')

    try:
        history_df = fetch_with_retry(
            ak.index_zh_a_hist,
            symbol=simple_code,
            period='daily',
            start_date='19700101',
            end_date=end_date,
        )
        if history_df is not None and not history_df.empty:
            return history_df, 'index_zh_a_hist'
    except Exception as exc:
        last_error = exc

    try:
        history_df = fetch_with_retry(ak.stock_zh_index_daily_em, symbol=index_code)
        if history_df is not None and not history_df.empty:
            return history_df, 'stock_zh_index_daily_em'
    except Exception as exc:
        last_error = exc

    if last_error is not None:
        raise last_error
    raise ValueError(f'No history data returned for {index_code}')


async def process_index(index_row, processed, db_tools, semaphore, progress_lock, end_date):
    index_code = index_row['index_code']
    simple_code = index_row['simple_code']

    if not simple_code:
        log_error(index_code, 'N/A', 'missing simple index code')
        return

    try:
        async with semaphore:
            history_df, source_name = await asyncio.to_thread(
                get_index_history,
                index_code,
                simple_code,
                end_date,
            )

        if history_df is None or history_df.empty:
            return

        pending_updates = []
        new_progress_lines = []

        for update in build_index_daily_rows(index_code, history_df, source_name):
            progress_key = f'{index_code},{update["trade_date"]}'
            if progress_key in processed:
                continue
            pending_updates.append(update)
            new_progress_lines.append(f'{progress_key}\n')

        if not pending_updates:
            return

        inserted = await db_tools.batch_index_daily_data(pending_updates)
        if inserted <= 0:
            return

        async with progress_lock:
            await asyncio.to_thread(save_progress_batch, new_progress_lines)
            processed.update(line.strip() for line in new_progress_lines)

    except Exception as exc:
        error_message = f'Error processing {index_code}: {exc}'
        print(error_message)
        log_error(index_code, 'N/A', error_message)


async def process_us_index(index_row, db_tools, semaphore):
    index_code = index_row['index_code']

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_us_index_history, index_code)

        if history_df is None or history_df.empty:
            print(f'index us backfill skipped {index_code}: no history data returned')
            return 0

        daily_rows = build_us_index_daily_rows(index_code, history_df)
        if not daily_rows:
            print(f'index us backfill skipped {index_code}: no valid rows built')
            return 0

        inserted = await db_tools.batch_index_us_daily_data(daily_rows)
        print(f'index us backfill {index_code} inserted: {inserted}')
        return inserted
    except Exception as exc:
        error_message = f'index us backfill failed for {index_code}: {exc}'
        print(error_message)
        log_error(index_code, 'N/A', error_message)
        return 0


async def process_hk_index_history(index_row, db_tools, semaphore):
    index_code = index_row['index_code']
    simple_code = index_row['simple_code']

    if not simple_code:
        print(f'index hk backfill skipped {index_code}: missing simple index code')
        return 0

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_hk_index_history, simple_code)

        if history_df is None or history_df.empty:
            print(f'index hk backfill skipped {index_code}: no history data returned')
            return 0

        daily_rows = build_hk_index_history_rows(index_code, history_df)
        if not daily_rows:
            print(f'index hk backfill skipped {index_code}: no valid rows built')
            return 0

        inserted = await db_tools.batch_index_hk_daily_data(daily_rows)
        print(f'index hk backfill {index_code} inserted: {inserted}')
        return inserted
    except Exception as exc:
        error_message = f'index hk backfill failed for {index_code}: {exc}'
        print(error_message)
        log_error(index_code, 'N/A', error_message)
        return 0


async def process_qvix_index(index_row, source_df, db_tools):
    index_code = index_row['index_code']
    source_name = index_row['data_source']

    try:
        history_df = build_qvix_history_from_source(index_row, source_df)

        if history_df is None or history_df.empty:
            print(f'index qvix backfill skipped {index_code}: no history data returned')
            return 0

        daily_rows = build_qvix_daily_rows(index_code, history_df, source_name)
        if not daily_rows:
            print(f'index qvix backfill skipped {index_code}: no valid rows built')
            return 0

        upserted = await db_tools.upsert_index_qvix_daily_snapshots(daily_rows)
        print(f'index qvix backfill {index_code} upserted: {upserted}')
        return upserted
    except Exception as exc:
        error_message = f'index qvix backfill failed for {index_code}: {exc}'
        print(error_message)
        log_error(index_code, 'N/A', error_message)
        return 0


async def backfill_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    processed = load_progress()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    progress_lock = asyncio.Lock()
    end_date = datetime.now().strftime('%Y%m%d')

    try:
        spot_df = await asyncio.to_thread(get_all_index_spot)
        if spot_df is None or spot_df.empty:
            print('No index spot data fetched.')
            return

        index_rows = build_index_basic_rows(spot_df)
        upserted = await db_tools.upsert_index_basic_info(index_rows)
        print(f'index_basic_info upserted: {upserted}')

        tasks = [
            process_index(index_row, processed, db_tools, semaphore, progress_lock, end_date)
            for index_row in index_rows
        ]
        await asyncio.gather(*tasks)
        print('index history backfill finished.')
    finally:
        await db_tools.close()


async def backfill_special_index_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        special_basic_row = build_special_index_basic_row()
        basic_upserted = await db_tools.upsert_index_basic_info([special_basic_row])

        end_date = datetime.now().strftime('%Y%m%d')
        history_df, source_name = await asyncio.to_thread(
            get_index_history,
            SPECIAL_INDEX_CODE,
            SPECIAL_INDEX_SIMPLE_CODE,
            end_date,
        )
        if history_df is None or history_df.empty:
            print(f'index {SPECIAL_INDEX_CODE} history backfill finished: no data returned')
            return 0

        daily_rows = build_index_daily_rows(SPECIAL_INDEX_CODE, history_df, source_name)
        if not daily_rows:
            print(f'index {SPECIAL_INDEX_CODE} history backfill finished: no valid rows built')
            return 0

        inserted = await db_tools.batch_index_daily_data(daily_rows)
        print(
            f'index {SPECIAL_INDEX_CODE} history backfill finished: '
            f'index_basic_info upserted: {basic_upserted}, '
            f'index_daily_data inserted: {inserted}, '
            f'source: {source_name}'
        )
        return inserted
    finally:
        await db_tools.close()


async def backfill_csi_dividend_index_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        end_date = datetime.now().strftime('%Y%m%d')
        source_rows = await asyncio.to_thread(
            fetch_csi_dividend_index_perf,
            CSI_DIVIDEND_INDEX_START_DATE,
            end_date,
        )
        daily_rows = build_csi_dividend_index_daily_rows(source_rows)
        if not daily_rows:
            raise ValueError('CSI Dividend official history returned no valid rows')

        basic_upserted = await db_tools.upsert_index_basic_info([
            build_csi_dividend_index_basic_row()
        ])
        daily_upserted = await db_tools.batch_index_daily_data(daily_rows)
        result = {
            'status': 'SUCCESS',
            'index_code': CSI_DIVIDEND_INDEX_CODE,
            'index_name': CSI_DIVIDEND_INDEX_NAME,
            'basic_upserted': basic_upserted,
            'daily_upserted': daily_upserted,
            'row_count': len(daily_rows),
            'start_date': daily_rows[0]['trade_date'],
            'end_date': daily_rows[-1]['trade_date'],
            'data_source': CSI_DIVIDEND_INDEX_SOURCE,
        }
        print(
            'CSI Dividend history backfill finished, '
            f'rows: {len(daily_rows)}, '
            f'range: {result["start_date"]} -> {result["end_date"]}'
        )
        return result
    finally:
        await db_tools.close()


async def sync_daily_special_index():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        trade_date = datetime.now().strftime('%Y-%m-%d')
        basic_upserted = await db_tools.upsert_index_basic_info([build_special_index_basic_row()])
        special_history_df = await asyncio.to_thread(get_special_index_daily)
        special_daily_rows = build_special_index_daily_rows(special_history_df)
        if len(special_daily_rows) < 2:
            raise ValueError('stock_zh_index_daily returned fewer than 2 valid rows')

        latest_special_row = special_daily_rows[-1]
        latest_trade_date = normalize_trade_date(latest_special_row.get('trade_date'))
        if latest_trade_date != trade_date:
            raise ValueError(
                f'latest trade_date mismatch, expected {trade_date}, got {latest_trade_date or "N/A"}'
            )

        daily_upserted = await db_tools.upsert_index_daily_snapshots([latest_special_row])

        print(
            'index bj50 daily finished, '
            f'index_basic_info upserted: {basic_upserted}, '
            f'index_daily_data upserted: {daily_upserted}'
        )
        return daily_upserted
    finally:
        await db_tools.close()


async def backfill_us_history():
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    try:
        basic_rows = build_us_index_basic_rows()
        basic_upserted = await db_tools.upsert_index_us_basic_info(basic_rows)
        inserted_rows = await asyncio.gather(
            *[process_us_index(index_row, db_tools, semaphore) for index_row in basic_rows]
        )
        total_inserted = sum(inserted_rows)
        print(
            'index us history backfill finished, '
            f'index_us_basic_info upserted: {basic_upserted}, '
            f'index_us_daily_data inserted: {total_inserted}'
        )
        return total_inserted
    finally:
        await db_tools.close()


async def backfill_hk_history():
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    try:
        spot_df = await asyncio.to_thread(get_hk_index_spot)
        if spot_df is None or spot_df.empty:
            print('No hk index spot data fetched.')
            return 0

        basic_rows = build_hk_index_basic_rows(spot_df)
        basic_upserted = await db_tools.upsert_index_hk_basic_info(basic_rows)
        inserted_rows = await asyncio.gather(
            *[process_hk_index_history(index_row, db_tools, semaphore) for index_row in basic_rows]
        )
        total_inserted = sum(inserted_rows)
        print(
            'index hk history backfill finished, '
            f'index_hk_basic_info upserted: {basic_upserted}, '
            f'index_hk_daily_data inserted: {total_inserted}'
        )
        return total_inserted
    finally:
        await db_tools.close()


async def backfill_qvix_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        source_df = await asyncio.to_thread(fetch_qvix_daily_source)
        basic_rows = build_qvix_basic_rows()
        basic_upserted = await db_tools.upsert_index_qvix_basic_info(basic_rows)
        inserted_rows = []
        for index_row in QVIX_DEFINITIONS:
            inserted_rows.append(await process_qvix_index(index_row, source_df, db_tools))
        total_upserted = sum(inserted_rows)
        print(
            'index qvix history backfill finished, '
            f'index_qvix_basic_info upserted: {basic_upserted}, '
            f'index_qvix_daily_data upserted: {total_upserted}'
        )
        return total_upserted
    finally:
        await db_tools.close()


def fetch_cn_market_fear_greed_history(expected_date=None, max_attempts=5):
    expected_date = normalize_trade_date(expected_date) if expected_date else ''
    best_payload = None
    best_latest_date = ''

    for attempt in range(max(1, int(max_attempts))):
        response = requests.get(
            CN_MARKET_FEAR_GREED_HISTORY_URL,
            params={'days': 10000, '_': int(time.time() * 1000) + attempt},
            headers={
                **MIUMIU_HTTP_HEADERS,
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError('MIUMIU fear/greed response must be a JSON object.')

        records = payload.get('records')
        latest_date = max(
            (
                normalize_trade_date(record.get('trade_date'))
                for record in records
                if isinstance(record, dict)
            ),
            default='',
        ) if isinstance(records, list) else ''
        if best_payload is None or latest_date > best_latest_date:
            best_payload = payload
            best_latest_date = latest_date
        if not expected_date or latest_date >= expected_date:
            return payload
        if attempt + 1 < max_attempts:
            LOGGER.warning(
                'MIUMIU fear/greed response is stale: expected %s, latest %s; retry %s/%s',
                expected_date,
                latest_date or '-',
                attempt + 2,
                max_attempts,
            )
            time.sleep(API_RETRY_SLEEP_SECONDS)

    return best_payload


def build_cn_market_fear_greed_rows(payload):
    records = payload.get('records') if isinstance(payload, dict) else None
    if not isinstance(records, list):
        return []
    rows = []
    for record in records:
        if not isinstance(record, dict):
            continue
        trade_date = normalize_trade_date(record.get('trade_date'))
        try:
            value = float(record.get('index_value'))
        except (TypeError, ValueError):
            continue
        if not trade_date or not 0 <= value <= 100:
            continue
        rows.append(
            {
                'trade_date': trade_date,
                'fear_greed_value': value,
                'sentiment_label': str(record.get('status_label') or '').strip(),
                'locked': bool(record.get('locked')),
                'data_source': CN_MARKET_FEAR_GREED_SOURCE,
                'raw_json': record,
            }
        )
    return sorted(rows, key=lambda row: row['trade_date'])


async def backfill_cn_market_fear_greed_history(expected_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        payload = await asyncio.to_thread(
            fetch_cn_market_fear_greed_history,
            expected_date,
        )
        rows = build_cn_market_fear_greed_rows(payload)
        if not rows:
            raise ValueError('No valid MIUMIU market fear/greed rows returned.')
        upserted = await db_tools.upsert_index_cn_market_fear_greed_daily(rows)
        result = {
            'status': 'SUCCESS',
            'upserted': upserted,
            'row_count': len(rows),
            'start_date': rows[0]['trade_date'],
            'end_date': rows[-1]['trade_date'],
            'data_source': CN_MARKET_FEAR_GREED_SOURCE,
        }
        print(
            'index cn market fear/greed backfill finished, '
            f'rows: {len(rows)}, range: {rows[0]["trade_date"]} -> {rows[-1]["trade_date"]}'
        )
        return result
    finally:
        await db_tools.close()


async def sync_daily_cn_market_fear_greed(target_date=None):
    expected_date = normalize_trade_date(target_date) if target_date else ''
    result = await backfill_cn_market_fear_greed_history(expected_date=expected_date)
    if expected_date and result['end_date'] < expected_date:
        raise ValueError(
            'MIUMIU market fear/greed source is not ready, '
            f'expected {expected_date}, latest {result["end_date"]}'
        )
    return result


def _fetch_baifenwei_json(url, attempt=0):
    response = requests.get(
        url,
        params={'_': int(time.time() * 1000) + int(attempt)},
        headers={
            **BAIFENWEI_HTTP_HEADERS,
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f'Baifenwei response must be a JSON object: {url}')
    return payload


def _baifenwei_payload_latest_date(subscores_payload):
    dates = subscores_payload.get('dates') if isinstance(subscores_payload, dict) else None
    if not isinstance(dates, list):
        return ''
    return max((normalize_trade_date(value) for value in dates), default='')


def fetch_cn_baifenwei_fear_greed_payloads(expected_date=None, max_attempts=5):
    expected_date = normalize_trade_date(expected_date) if expected_date else ''
    best_payloads = None
    best_latest_date = ''

    for attempt in range(max(1, int(max_attempts))):
        series_payload = _fetch_baifenwei_json(CN_BAIFENWEI_FEAR_GREED_SERIES_URL, attempt)
        subscores_payload = _fetch_baifenwei_json(CN_BAIFENWEI_FEAR_GREED_SUBSCORES_URL, attempt)
        latest_date = _baifenwei_payload_latest_date(subscores_payload)
        if best_payloads is None or latest_date > best_latest_date:
            best_payloads = (series_payload, subscores_payload)
            best_latest_date = latest_date
        if not expected_date or latest_date >= expected_date:
            return series_payload, subscores_payload
        if attempt + 1 < max_attempts:
            LOGGER.warning(
                'Baifenwei fear/greed response is stale: expected %s, latest %s; retry %s/%s',
                expected_date,
                latest_date or '-',
                attempt + 2,
                max_attempts,
            )
            time.sleep(API_RETRY_SLEEP_SECONDS)

    return best_payloads


def build_cn_baifenwei_fear_greed_rows(series_payload, subscores_payload):
    dates = subscores_payload.get('dates') if isinstance(subscores_payload, dict) else None
    raw_series = subscores_payload.get('series') if isinstance(subscores_payload, dict) else None
    if not isinstance(dates, list) or not isinstance(raw_series, list):
        return []

    component_data = {}
    for item in raw_series:
        if not isinstance(item, dict):
            continue
        key = str(item.get('key') or '').strip()
        data = item.get('data')
        if key in BAIFENWEI_FEAR_GREED_WEIGHTS and isinstance(data, list):
            component_data[key] = data
    if set(component_data) != set(BAIFENWEI_FEAR_GREED_WEIGHTS):
        return []

    published_by_date = {}
    points = series_payload.get('points') if isinstance(series_payload, dict) else None
    if isinstance(points, list):
        for point in points:
            if not isinstance(point, list) or len(point) < 2:
                continue
            trade_date = normalize_trade_date(point[0])
            value = to_float(point[1])
            market_index_value = to_float(point[2]) if len(point) > 2 else None
            if trade_date and value is not None and 0 <= value <= 100:
                published_by_date[trade_date] = (value, market_index_value)

    series_generated_at = str(series_payload.get('generated_at') or '').strip()
    subscores_generated_at = str(subscores_payload.get('generated_at') or '').strip()
    source_generated_at = max(series_generated_at, subscores_generated_at) or None
    rows = []
    for index, raw_date in enumerate(dates):
        trade_date = normalize_trade_date(raw_date)
        if not trade_date:
            continue
        components = {}
        valid = True
        for key in BAIFENWEI_FEAR_GREED_WEIGHTS:
            values = component_data[key]
            value = to_float(values[index]) if index < len(values) else None
            if value is None or not 0 <= value <= 100:
                valid = False
                break
            components[key] = value
        if not valid:
            continue

        reconstructed_value = round(
            sum(components[key] * weight for key, weight in BAIFENWEI_FEAR_GREED_WEIGHTS.items()),
            2,
        )
        published = published_by_date.get(trade_date)
        fear_greed_value = published[0] if published else reconstructed_value
        market_index_value = published[1] if published else None
        value_origin = 'published' if published else 'reconstructed'
        rows.append(
            {
                'trade_date': trade_date,
                'fear_greed_value': fear_greed_value,
                'sentiment_label': infer_fear_greed_label(fear_greed_value),
                'volatility_score': components['volatility'],
                'relative_turnover_score': components['relative_turnover_rate'],
                'margin_trading_score': components['margin_trading'],
                'market_breadth_score': components['market_breadth'],
                'rsi_score': components['rsi'],
                'limit_up_down_ratio_score': components['limit_up_down_ratio'],
                'market_index_value': market_index_value,
                'value_origin': value_origin,
                'data_source': CN_BAIFENWEI_FEAR_GREED_SOURCE,
                'source_generated_at': source_generated_at,
                'raw_json': {
                    'components': components,
                    'published_score': published[0] if published else None,
                    'reconstructed_score': reconstructed_value,
                    'market_index_value': market_index_value,
                    'series_generated_at': series_generated_at,
                    'subscores_generated_at': subscores_generated_at,
                },
            }
        )
    return sorted(rows, key=lambda row: row['trade_date'])


async def backfill_cn_baifenwei_fear_greed_history(expected_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        payloads = await asyncio.to_thread(
            fetch_cn_baifenwei_fear_greed_payloads,
            expected_date,
        )
        if not payloads:
            raise ValueError('No Baifenwei fear/greed payload returned.')
        rows = build_cn_baifenwei_fear_greed_rows(*payloads)
        if not rows:
            raise ValueError('No valid Baifenwei fear/greed rows returned.')
        upserted = await db_tools.upsert_index_cn_baifenwei_fear_greed_daily(rows)
        result = {
            'status': 'SUCCESS',
            'upserted': upserted,
            'row_count': len(rows),
            'start_date': rows[0]['trade_date'],
            'end_date': rows[-1]['trade_date'],
            'published_count': sum(row['value_origin'] == 'published' for row in rows),
            'reconstructed_count': sum(row['value_origin'] == 'reconstructed' for row in rows),
            'data_source': CN_BAIFENWEI_FEAR_GREED_SOURCE,
        }
        print(
            'index cn Baifenwei fear/greed backfill finished, '
            f'rows: {len(rows)}, range: {rows[0]["trade_date"]} -> {rows[-1]["trade_date"]}'
        )
        return result
    finally:
        await db_tools.close()


async def sync_daily_cn_baifenwei_fear_greed(target_date=None):
    expected_date = normalize_trade_date(target_date) if target_date else ''
    result = await backfill_cn_baifenwei_fear_greed_history(expected_date=expected_date)
    if expected_date and result['end_date'] < expected_date:
        raise ValueError(
            'Baifenwei fear/greed source is not ready, '
            f'expected {expected_date}, latest {result["end_date"]}'
        )
    return result


async def sync_daily_qvix():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        source_df = await asyncio.to_thread(fetch_qvix_daily_source)
        basic_rows = build_qvix_basic_rows()
        recent_rows = []
        trade_dates = set()

        for index_row in QVIX_DEFINITIONS:
            history_df = build_qvix_history_from_source(index_row, source_df)
            if history_df is None or history_df.empty:
                raise ValueError(f'No history data returned for {index_row["index_code"]}')

            daily_rows = build_qvix_daily_rows(index_row['index_code'], history_df, index_row['data_source'])
            if not daily_rows:
                raise ValueError(f'No valid rows built for {index_row["index_code"]}')

            rows_to_upsert = daily_rows[-QVIX_DAILY_RECENT_BACKFILL_ROWS:]
            recent_rows.extend(rows_to_upsert)
            trade_dates.update(row['trade_date'] for row in rows_to_upsert)

        basic_upserted = await db_tools.upsert_index_qvix_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_index_qvix_daily_snapshots(recent_rows)
        latest_trade_date = max(trade_dates) if trade_dates else ''
        today = datetime.now().strftime('%Y-%m-%d')
        source_is_behind = bool(latest_trade_date and latest_trade_date < today)
        if source_is_behind:
            print(
                'index qvix daily source is behind local date, '
                f'latest_source_trade_date={latest_trade_date}, local_date={today}'
            )
        print(
            'index qvix daily finished, '
            f'index_qvix_basic_info upserted: {basic_upserted}, '
            f'index_qvix_daily_data upserted: {daily_upserted}, '
            f'recent_backfill_rows_per_index: {QVIX_DAILY_RECENT_BACKFILL_ROWS}, '
            f'trade_dates: {",".join(sorted(trade_dates))}'
        )
        return {
            'upserted': daily_upserted,
            'latest_source_trade_date': latest_trade_date,
            'trade_dates': sorted(trade_dates),
            'local_date': today,
            'source_is_behind': source_is_behind,
        }
    finally:
        await db_tools.close()


async def backfill_news_sentiment_scope_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        history_df = await asyncio.to_thread(get_news_sentiment_scope)
        if history_df is None or history_df.empty:
            print('index news sentiment backfill finished: no data returned')
            return 0

        rows = build_news_sentiment_scope_rows(history_df)
        if not rows:
            print('index news sentiment backfill finished: no valid rows built')
            return 0

        upserted = await db_tools.upsert_index_news_sentiment_scope_daily(rows)
        print(
            'index news sentiment backfill finished, '
            f'index_news_sentiment_scope_daily upserted: {upserted}'
        )
        return upserted
    finally:
        await db_tools.close()


async def sync_daily_news_sentiment_scope():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        history_df = await asyncio.to_thread(get_news_sentiment_scope)
        if history_df is None or history_df.empty:
            raise ValueError('No news sentiment scope data returned.')

        rows = build_news_sentiment_scope_rows(history_df)
        if not rows:
            raise ValueError('No valid news sentiment scope rows built.')

        latest_row = rows[-1]
        upserted = await db_tools.upsert_index_news_sentiment_scope_daily([latest_row])
        print(
            'index news sentiment daily finished, '
            f'index_news_sentiment_scope_daily upserted: {upserted}, '
            f'trade_date: {latest_row["trade_date"]}'
        )
        return upserted
    finally:
        await db_tools.close()


async def backfill_us_vix_history(db_tools):
    csv_text = await asyncio.to_thread(fetch_us_vix_history_csv)
    rows = build_us_vix_daily_rows(csv_text)
    if not rows:
        raise ValueError('No valid VIX OHLC rows built.')

    upserted = await db_tools.upsert_index_us_vix_daily(rows)
    print(
        'index us vix backfill finished, '
        f'index_us_vix_daily upserted: {upserted}, '
        f'range: {rows[0]["trade_date"]} -> {rows[-1]["trade_date"]}'
    )
    return upserted


async def sync_daily_us_vix(db_tools):
    csv_text = await asyncio.to_thread(fetch_us_vix_history_csv)
    rows = build_us_vix_daily_rows(csv_text)
    if not rows:
        raise ValueError('No valid VIX OHLC rows built.')

    latest_row = rows[-1]
    upserted = await db_tools.upsert_index_us_vix_daily([latest_row])
    print(
        'index us vix daily finished, '
        f'index_us_vix_daily upserted: {upserted}, '
        f'trade_date: {latest_row["trade_date"]}'
    )
    return upserted


async def backfill_us_fear_greed_history(db_tools):
    mirror_csv = await asyncio.to_thread(fetch_us_fear_greed_history_mirror_csv)
    cnn_history_payload = await asyncio.to_thread(fetch_us_fear_greed_history_payload)
    cnn_current_payload = await asyncio.to_thread(fetch_us_fear_greed_current_payload)

    rows = merge_us_fear_greed_rows(
        build_us_fear_greed_rows_from_mirror(mirror_csv),
        build_us_fear_greed_rows_from_cnn_history(cnn_history_payload),
        [build_us_fear_greed_current_row(cnn_current_payload)],
    )
    if not rows:
        raise ValueError('No valid Fear & Greed rows built.')

    upserted = await db_tools.upsert_index_us_fear_greed_daily(rows)
    print(
        'index us fear greed backfill finished, '
        f'index_us_fear_greed_daily upserted: {upserted}, '
        f'range: {rows[0]["trade_date"]} -> {rows[-1]["trade_date"]}'
    )
    return upserted


async def sync_daily_us_fear_greed(db_tools):
    current_payload = await asyncio.to_thread(fetch_us_fear_greed_current_payload)
    current_row = build_us_fear_greed_current_row(current_payload)
    upserted = await db_tools.upsert_index_us_fear_greed_daily([current_row])
    print(
        'index us fear greed daily finished, '
        f'index_us_fear_greed_daily upserted: {upserted}, '
        f'trade_date: {current_row["trade_date"]}'
    )
    return upserted


async def backfill_us_hedge_fund_ls_proxy(db_tools):
    all_rows = []
    for contract_scope, definition in US_HEDGE_PROXY_DEFINITIONS.items():
        long_payload = await asyncio.to_thread(fetch_ofr_series_full, definition['long_mnemonic'])
        short_payload = await asyncio.to_thread(fetch_ofr_series_full, definition['short_mnemonic'])
        all_rows.extend(
            build_us_hedge_fund_ls_proxy_rows(
                contract_scope,
                long_payload.get('timeseries', {}).get('aggregation', {}).get('data', []),
                short_payload.get('timeseries', {}).get('aggregation', {}).get('data', []),
            )
        )

    if not all_rows:
        raise ValueError('No valid US hedge fund proxy rows built.')

    upserted = await db_tools.upsert_index_us_hedge_fund_ls_proxy(all_rows)
    print(
        'index us hedge fund proxy backfill finished, '
        f'index_us_hedge_fund_ls_proxy upserted: {upserted}, '
        f'range: {all_rows[0]["report_date"]} -> {all_rows[-1]["report_date"]}'
    )
    return upserted


async def sync_daily_us_hedge_fund_ls_proxy(db_tools):
    latest_rows = []
    for contract_scope, definition in US_HEDGE_PROXY_DEFINITIONS.items():
        long_payload = await asyncio.to_thread(fetch_ofr_series_full, definition['long_mnemonic'])
        short_payload = await asyncio.to_thread(fetch_ofr_series_full, definition['short_mnemonic'])
        rows = build_us_hedge_fund_ls_proxy_rows(
            contract_scope,
            long_payload.get('timeseries', {}).get('aggregation', {}).get('data', []),
            short_payload.get('timeseries', {}).get('aggregation', {}).get('data', []),
        )
        if not rows:
            raise ValueError(f'No valid hedge proxy rows built for {contract_scope}')
        latest_rows.append(rows[-1])

    latest_existing_dates = await db_tools.get_latest_index_us_hedge_fund_ls_proxy_dates()
    rows_to_upsert = [
        row
        for row in latest_rows
        if latest_existing_dates.get(row['contract_scope']) != row['report_date']
    ]

    unchanged_scopes = sorted(
        row['contract_scope']
        for row in latest_rows
        if latest_existing_dates.get(row['contract_scope']) == row['report_date']
    )
    upserted = await db_tools.upsert_index_us_hedge_fund_ls_proxy(rows_to_upsert)
    print(
        'index us hedge fund proxy daily finished, '
        f'index_us_hedge_fund_ls_proxy upserted: {upserted}, '
        f'unchanged_scopes: {",".join(unchanged_scopes) if unchanged_scopes else "-"}'
    )
    return upserted


async def fetch_us_put_call_daily_json_rows(start_date, end_date, concurrency=8):
    candidate_dates = build_weekday_date_strings(start_date, end_date)
    return await fetch_us_put_call_daily_json_rows_for_dates(candidate_dates, concurrency=concurrency)


async def fetch_us_put_call_daily_json_rows_for_dates(candidate_dates, concurrency=8):
    candidate_dates = sorted({
        normalized_date
        for trade_date in candidate_dates or []
        if (normalized_date := normalize_trade_date(trade_date))
    })
    semaphore = asyncio.Semaphore(concurrency)
    rows = []
    skipped = 0
    failures = []

    async def fetch_one(trade_date):
        async with semaphore:
            try:
                payload = await asyncio.to_thread(fetch_us_put_call_daily_options_json, trade_date)
                if not payload:
                    return None, True, None
                row = build_us_put_call_ratio_row_from_daily_options_json(payload, trade_date)
                return row, row is None, None
            except Exception as exc:
                return None, False, f'{trade_date}: {exc}'

    results = await asyncio.gather(*(fetch_one(trade_date) for trade_date in candidate_dates))
    for row, was_skipped, failure in results:
        if row:
            rows.append(row)
        if was_skipped:
            skipped += 1
        if failure:
            failures.append(failure)

    return rows, skipped, failures


async def backfill_us_put_call_ratio(db_tools):
    row_groups = []
    failures = []
    for ratio_key, url in US_PUT_CALL_HISTORY_URLS.items():
        try:
            csv_text = await asyncio.to_thread(fetch_us_put_call_history_csv, url)
            rows = build_us_put_call_ratio_rows_from_history_csv(csv_text, ratio_key)
            row_groups.append(rows)
        except Exception as exc:
            failures.append(f'{ratio_key}: {exc}')
            print(f'index us put call backfill failed for {ratio_key}: {exc}')

    try:
        html_text = await asyncio.to_thread(fetch_us_put_call_market_stats_html)
        current_row = extract_current_put_call_ratio_from_html(html_text)
        if current_row:
            row_groups.append([current_row])
    except Exception as exc:
        failures.append(f'current: {exc}')
        print(f'index us put call current fetch failed: {exc}')

    try:
        daily_rows, skipped_daily, daily_failures = await fetch_us_put_call_daily_json_rows(
            US_PUT_CALL_DAILY_JSON_START_DATE,
            datetime.now().strftime('%Y-%m-%d'),
        )
        if daily_rows:
            row_groups.append(daily_rows)
        if daily_failures:
            failures.extend(daily_failures)
        print(
            'index us put call daily-json backfill scanned, '
            f'valid_rows: {len(daily_rows)}, skipped_dates: {skipped_daily}, '
            f'failed_dates: {len(daily_failures)}'
        )
    except Exception as exc:
        failures.append(f'daily-json: {exc}')
        print(f'index us put call daily-json backfill failed: {exc}')

    rows = merge_us_put_call_ratio_rows(*row_groups)
    if not rows:
        raise ValueError(
            'No valid Cboe Put/Call rows built. '
            f'failures: {"; ".join(failures) if failures else "-"}'
        )

    upserted = await db_tools.upsert_index_us_put_call_ratio_daily(rows)
    print(
        'index us put call ratio backfill finished, '
        f'index_us_put_call_ratio_daily upserted: {upserted}, '
        f'range: {rows[0]["trade_date"]} -> {rows[-1]["trade_date"]}, '
        f'failed_sources: {len(failures)}'
    )
    return upserted


async def sync_daily_us_put_call_ratio(db_tools):
    rows = []
    today = datetime.now().date()
    recent_dates = build_weekday_date_strings(
        (today - timedelta(days=14)).strftime('%Y-%m-%d'),
        today.strftime('%Y-%m-%d'),
    )
    missing_dates = []
    get_missing_dates = getattr(db_tools, 'get_index_us_put_call_missing_trade_dates', None)
    if callable(get_missing_dates):
        try:
            missing_dates = await get_missing_dates(
                US_PUT_CALL_DAILY_JSON_START_DATE,
                today.strftime('%Y-%m-%d'),
                limit=256,
            )
        except Exception as exc:
            print(f'index us put call missing-date lookup failed, continue recent repair: {exc}')

    candidate_dates = sorted(set(recent_dates).union(missing_dates))
    try:
        daily_rows, skipped_daily, daily_failures = await fetch_us_put_call_daily_json_rows_for_dates(
            candidate_dates,
            concurrency=4,
        )
        rows.extend(daily_rows)
        if daily_failures:
            print(f'index us put call daily-json recent failures: {len(daily_failures)}')
        print(
            'index us put call daily-json repair scanned, '
            f'candidate_dates: {len(candidate_dates)}, missing_dates: {len(missing_dates)}, '
            f'valid_rows: {len(daily_rows)}, skipped_dates: {skipped_daily}'
        )
    except Exception as exc:
        print(f'index us put call daily-json fetch failed, fallback to current page: {exc}')

    if not rows:
        try:
            html_text = await asyncio.to_thread(fetch_us_put_call_market_stats_html)
            current_row = extract_current_put_call_ratio_from_html(html_text)
            if current_row:
                rows.append(current_row)
        except Exception as exc:
            print(f'index us put call current fetch failed, fallback to history csv: {exc}')

    if not rows:
        history_groups = []
        for ratio_key, url in US_PUT_CALL_HISTORY_URLS.items():
            csv_text = await asyncio.to_thread(fetch_us_put_call_history_csv, url)
            history_groups.append(build_us_put_call_ratio_rows_from_history_csv(csv_text, ratio_key))
        merged_rows = merge_us_put_call_ratio_rows(*history_groups)
        rows = merged_rows[-1:] if merged_rows else []

    if not rows:
        raise ValueError('No valid Cboe Put/Call daily row built.')

    rows = merge_us_put_call_ratio_rows(rows)
    latest_row = rows[-1]
    latest_date = datetime.strptime(latest_row['trade_date'], '%Y-%m-%d').date()
    if latest_date < (datetime.now().date() - timedelta(days=14)):
        raise ValueError(f'Cboe Put/Call daily row is stale: {latest_row["trade_date"]}')
    upserted = await db_tools.upsert_index_us_put_call_ratio_daily(rows)
    print(
        'index us put call ratio daily finished, '
        f'index_us_put_call_ratio_daily upserted: {upserted}, '
        f'range: {rows[0]["trade_date"]} -> {rows[-1]["trade_date"]}'
    )
    return upserted


async def sync_daily_us_option_premium(db_tools):
    html_text = await asyncio.to_thread(fetch_us_option_premium_html)
    row = parse_optionomics_option_premium_html(html_text)
    trade_date = datetime.strptime(row['trade_date'], '%Y-%m-%d').date()
    age_days = (datetime.now().date() - trade_date).days
    if age_days < 0:
        raise ValueError(f'Optionomics premium date is in the future: {row["trade_date"]}')
    if age_days > 7:
        raise ValueError(f'Optionomics premium page is stale: {row["trade_date"]}')

    upserted = await db_tools.upsert_index_us_option_premium_daily([row])
    print(
        'index us option premium daily finished, '
        f'index_us_option_premium_daily upserted: {upserted}, '
        f'trade_date: {row["trade_date"]}, '
        f'call_musd: {row["call_premium_million_usd"]:.1f}, '
        f'put_musd: {row["put_premium_million_usd"]:.1f}, '
        f'premium_pc: {row["premium_put_call_ratio"]:.6f}, '
        'basis: source display rounded to 0.1M USD'
    )
    return upserted


def _us_option_history_url(symbol, filename):
    return (
        f'{US_OPTION_PRICE_PC_HISTORY_BASE_URL}/'
        f'{str(symbol).strip().lower()}/{str(filename).strip()}'
    )


def _download_us_option_history_file(url, target_path):
    path = Path(target_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return path
    temporary_path = path.with_suffix(path.suffix + '.part')
    response = requests.get(url, headers=DEFAULT_HTTP_HEADERS, timeout=180, stream=True)
    response.raise_for_status()
    with temporary_path.open('wb') as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)
    if temporary_path.stat().st_size <= 0:
        raise ValueError(f'US option history download is empty: {url}')
    temporary_path.replace(path)
    return path


def _read_us_option_history_parquet(path):
    try:
        return pd.read_parquet(path)
    except ImportError as exc:
        raise RuntimeError(
            'Historical US option backfill requires pyarrow; install requirements.txt first.'
        ) from exc


def fetch_live_us_option_price_rows(symbol):
    normalized_symbol = str(symbol or '').strip().upper()
    discovery_payload = fetch_nasdaq_us_option_chain(normalized_symbol)
    trade_date, underlying_close, _last_trade_text = parse_nasdaq_option_chain_last_trade(
        discovery_payload
    )
    selected_expirations = select_us_option_pc_expirations(
        extract_nasdaq_option_expirations(discovery_payload),
        trade_date,
    )
    if any(expiration is None for expiration in selected_expirations.values()):
        missing = [key for key, expiration in selected_expirations.items() if expiration is None]
        raise ValueError(
            f'Nasdaq {normalized_symbol} option chain is missing standard expiries: '
            + ', '.join(missing)
        )

    candidate_rows = []
    for expiration in dict.fromkeys(selected_expirations.values()):
        expiration_text = expiration.isoformat()
        payload = fetch_nasdaq_us_option_chain(
            normalized_symbol,
            from_date=expiration_text,
            to_date=expiration_text,
        )
        payload_trade_date, payload_underlying_close, _payload_last_trade = (
            parse_nasdaq_option_chain_last_trade(payload)
        )
        if payload_trade_date != trade_date:
            raise ValueError(
                f'Nasdaq {normalized_symbol} option chain source dates disagree: '
                f'{trade_date} != {payload_trade_date}'
            )
        if abs(payload_underlying_close - underlying_close) > 1e-6:
            raise ValueError(
                f'Nasdaq {normalized_symbol} underlying closes disagree: '
                f'{underlying_close} != {payload_underlying_close}'
            )
        candidate_rows.extend(
            build_us_option_price_rows_from_nasdaq_payload(
                payload,
                normalized_symbol,
                expiration=expiration_text,
            )
        )

    selected_rows = select_adjacent_us_option_price_rows(candidate_rows, underlying_close)
    coverage = {
        (row['expiration_date'], row['option_type'])
        for row in selected_rows
    }
    incomplete = []
    for bucket, expiration in selected_expirations.items():
        for option_type in ('CALL', 'PUT'):
            if (expiration.isoformat(), option_type) not in coverage:
                incomplete.append(f'{bucket}:{option_type}')
    if incomplete:
        raise ValueError(
            f'Nasdaq {normalized_symbol} option chain lacks traded adjacent strikes: '
            + ', '.join(incomplete)
        )
    return {
        'symbol': normalized_symbol,
        'trade_date': trade_date,
        'underlying_close': underlying_close,
        'selected_expirations': {
            bucket: expiration.isoformat()
            for bucket, expiration in selected_expirations.items()
        },
        'rows': selected_rows,
    }


async def sync_daily_us_option_price_pc(db_tools):
    product_results = []
    for symbol in US_OPTION_PRICE_PC_PRODUCTS:
        product_results.append(
            await asyncio.to_thread(fetch_live_us_option_price_rows, symbol)
        )
    trade_dates = sorted({result['trade_date'] for result in product_results})
    if len(trade_dates) != 1:
        raise ValueError(
            'SPY and QQQ Nasdaq option chains returned different source dates: '
            + ', '.join(trade_dates)
        )
    trade_date = trade_dates[0]
    age_days = (datetime.now().date() - datetime.strptime(trade_date, '%Y-%m-%d').date()).days
    if age_days < 0 or age_days > 7:
        raise ValueError(f'Nasdaq option chain source date is invalid or stale: {trade_date}')
    rows = [row for result in product_results for row in result['rows']]
    if not rows:
        raise ValueError('No valid SPY/QQQ option close rows built from Nasdaq.')
    upserted = await db_tools.upsert_index_us_etf_option_daily(rows)
    result = {
        'status': 'SUCCESS',
        'trade_date': trade_date,
        'trade_dates': [trade_date],
        'upserted': upserted,
        'row_count': len(rows),
        'data_source': US_OPTION_PRICE_PC_LIVE_SOURCE,
        'products': {
            item['symbol']: {
                'underlying_close': item['underlying_close'],
                'selected_expirations': item['selected_expirations'],
                'row_count': len(item['rows']),
            }
            for item in product_results
        },
    }
    print(
        'US ETF option price P/C daily finished, '
        f'trade_date: {trade_date}, rows: {len(rows)}, upserted: {upserted}, '
        'products: SPY,QQQ, basis: last trade with positive daily volume'
    )
    return result


async def backfill_us_option_price_pc(
    db_tools,
    start_date='2008-01-01',
    end_date='2025-12-31',
):
    normalized_start = normalize_trade_date(start_date)
    normalized_end = normalize_trade_date(end_date)
    if not normalized_start or not normalized_end or normalized_start > normalized_end:
        raise ValueError(f'Invalid US option history range: {start_date} -> {end_date}')
    cache_dir = get_cache_dir('us_option_price_pc')
    completed = US_OPTION_PRICE_PC_PROGRESS_STORE.load()
    total_upserted = 0
    processed_parts = 0
    skipped_parts = 0
    failures = []

    for symbol, product in US_OPTION_PRICE_PC_PRODUCTS.items():
        start_year = max(int(normalized_start[:4]), int(product['history_start_year']))
        end_year = min(int(normalized_end[:4]), US_OPTION_PRICE_PC_HISTORY_END_YEAR)
        if start_year > end_year:
            continue
        underlying_url = _us_option_history_url(symbol, 'underlying_prices.parquet')
        underlying_path = cache_dir / symbol.lower() / 'underlying_prices.parquet'
        try:
            await asyncio.to_thread(
                _download_us_option_history_file,
                underlying_url,
                underlying_path,
            )
            underlying_frame = await asyncio.to_thread(
                _read_us_option_history_parquet,
                underlying_path,
            )
        except Exception as exc:
            failures.append(f'{symbol}:underlying:{exc}')
            continue

        for year in range(start_year, end_year + 1):
            part_start = max(normalized_start, f'{year}-01-01')
            part_end = min(normalized_end, f'{year}-12-31')
            progress_key = f'{symbol},{year},{part_start},{part_end}'
            if progress_key in completed:
                skipped_parts += 1
                continue
            filename = f'options_{year}.parquet'
            source_url = _us_option_history_url(symbol, filename)
            source_path = cache_dir / symbol.lower() / filename
            try:
                await asyncio.to_thread(
                    _download_us_option_history_file,
                    source_url,
                    source_path,
                )
                option_frame = await asyncio.to_thread(
                    _read_us_option_history_parquet,
                    source_path,
                )
                rows = await asyncio.to_thread(
                    build_us_option_price_rows_from_history_frames,
                    option_frame,
                    underlying_frame,
                    symbol,
                    part_start,
                    part_end,
                    source_url,
                )
                upserted = await db_tools.upsert_index_us_etf_option_daily(rows)
                total_upserted += upserted
                processed_parts += 1
                await asyncio.to_thread(
                    US_OPTION_PRICE_PC_PROGRESS_STORE.append,
                    progress_key,
                )
                completed.add(progress_key)
                print(
                    'US ETF option price P/C history part finished, '
                    f'symbol: {symbol}, year: {year}, rows: {len(rows)}, '
                    f'upserted: {upserted}'
                )
            except Exception as exc:
                failures.append(f'{symbol}:{year}:{exc}')
                print(f'US ETF option price P/C history failed for {symbol} {year}: {exc}')
            finally:
                if 'option_frame' in locals():
                    del option_frame

    if failures:
        raise RuntimeError(
            'US ETF option price P/C history backfill has failures: '
            + '; '.join(failures[:8])
        )
    result = {
        'status': 'SUCCESS',
        'start_date': normalized_start,
        'end_date': normalized_end,
        'processed_parts': processed_parts,
        'skipped_parts': skipped_parts,
        'upserted': total_upserted,
        'data_source': US_OPTION_PRICE_PC_HISTORY_SOURCE,
    }
    print(
        'US ETF option price P/C history finished, '
        f'range: {normalized_start} -> {normalized_end}, '
        f'processed_parts: {processed_parts}, skipped_parts: {skipped_parts}, '
        f'upserted: {total_upserted}'
    )
    return result


async def backfill_us_treasury_yield(db_tools):
    series_maps = {}
    for field_name, series_id in FRED_TREASURY_SERIES.items():
        csv_text = await asyncio.to_thread(fetch_fred_series_csv, series_id)
        series_maps[field_name] = build_fred_series_points(csv_text, series_id)

    rows = build_us_treasury_yield_rows(series_maps)
    if not rows:
        raise ValueError('No valid FRED US Treasury yield rows built.')

    upserted = await db_tools.upsert_index_us_treasury_yield_daily(rows)
    print(
        'index us treasury yield backfill finished, '
        f'index_us_treasury_yield_daily upserted: {upserted}, '
        f'range: {rows[0]["trade_date"]} -> {rows[-1]["trade_date"]}'
    )
    return upserted


async def sync_daily_us_treasury_yield(db_tools):
    series_maps = {}
    for field_name, series_id in FRED_TREASURY_SERIES.items():
        csv_text = await asyncio.to_thread(fetch_fred_series_csv, series_id)
        series_maps[field_name] = build_fred_series_points(csv_text, series_id)

    rows = build_us_treasury_yield_rows(series_maps)
    if not rows:
        raise ValueError('No valid FRED US Treasury yield rows built.')

    complete_rows = [
        row
        for row in rows
        if (
            row.get('yield_3m') is not None
            and row.get('yield_2y') is not None
            and row.get('yield_10y') is not None
            and row.get('yield_real_10y') is not None
        )
    ]
    if not complete_rows:
        raise ValueError(
            'No complete US Treasury row: DGS10 and DFII10 must both be non-null '
            'on the latest common available trade date.'
        )
    latest_row = complete_rows[-1]
    recent_rows = [
        row for row in rows if row['trade_date'] <= latest_row['trade_date']
    ][-US_TREASURY_DAILY_SYNC_RECENT_DAYS:]
    upserted = await db_tools.upsert_index_us_treasury_yield_daily(recent_rows)
    print(
        'index us treasury yield daily finished, '
        f'index_us_treasury_yield_daily upserted: {upserted}, '
        f'trade_date: {latest_row["trade_date"]}'
    )
    return upserted


async def backfill_us_credit_spread(db_tools):
    csv_text = await asyncio.to_thread(fetch_fred_series_csv, FRED_HIGH_YIELD_OAS_SERIES)
    rows = build_us_credit_spread_rows(csv_text)
    if not rows:
        raise ValueError('No valid FRED US high yield credit spread rows built.')

    archive_text = await asyncio.to_thread(fetch_us_credit_spread_archive_csv)
    archive_rows = build_us_credit_spread_archive_rows(archive_text)
    if not archive_rows:
        raise ValueError('No valid archived FRED US high yield credit spread rows built.')

    live_by_date = {row['trade_date']: row for row in rows}
    archive_by_date = {row['trade_date']: row for row in archive_rows}
    overlap_dates = sorted(set(live_by_date) & set(archive_by_date))
    if not overlap_dates:
        raise ValueError('Archived and live FRED HY OAS rows have no overlap to verify.')
    mismatched_dates = [
        trade_date
        for trade_date in overlap_dates
        if abs(
            float(live_by_date[trade_date]['high_yield_oas'])
            - float(archive_by_date[trade_date]['high_yield_oas'])
        ) > 1e-9
    ]
    if mismatched_dates:
        raise ValueError(
            'Archived FRED HY OAS values disagree with the live official series: '
            + ', '.join(mismatched_dates[:5])
        )

    existing_rows = await db_tools.get_quant_index_risk_us_credit_rows(
        '1900-01-01',
        '2100-12-31',
    )
    legacy_rows = attach_us_credit_spread_available_at(
        [
            {
                'trade_date': normalize_trade_date(row.get('trade_date')),
                'high_yield_oas': row.get('high_yield_oas'),
                'data_source': row.get('data_source') or US_CREDIT_SPREAD_SOURCE,
            }
            for row in existing_rows
            if row.get('available_at') is None
        ]
    )
    # The pinned archive restores the observations that FRED stopped serving in
    # April 2026. Existing rows and the live FRED response always take priority.
    merged_rows = dict(archive_by_date)
    merged_rows.update({row['trade_date']: row for row in legacy_rows})
    merged_rows.update({row['trade_date']: row for row in rows})
    rows = [merged_rows[trade_date] for trade_date in sorted(merged_rows)]

    upserted = await db_tools.upsert_index_us_credit_spread_daily(rows)
    print(
        'index us credit spread backfill finished, '
        f'index_us_credit_spread_daily upserted: {upserted}, '
        f'range: {rows[0]["trade_date"]} -> {rows[-1]["trade_date"]}'
    )
    return upserted


async def sync_daily_us_credit_spread(db_tools):
    csv_text = await asyncio.to_thread(fetch_fred_series_csv, FRED_HIGH_YIELD_OAS_SERIES)
    rows = build_us_credit_spread_rows(csv_text)
    if not rows:
        raise ValueError('No valid FRED US high yield credit spread rows built.')

    latest_row = rows[-1]
    upserted = await db_tools.upsert_index_us_credit_spread_daily([latest_row])
    print(
        'index us credit spread daily finished, '
        f'index_us_credit_spread_daily upserted: {upserted}, '
        f'trade_date: {latest_row["trade_date"]}'
    )
    return upserted


async def backfill_us_market_sentiment():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        component_counts = {
            'vix': 0,
            'fear_greed': 0,
            'hedge_proxy': 0,
        }
        failures = []

        for component_name, worker in [
            ('vix', backfill_us_vix_history),
            ('fear_greed', backfill_us_fear_greed_history),
            ('hedge_proxy', backfill_us_hedge_fund_ls_proxy),
        ]:
            try:
                component_counts[component_name] = await worker(db_tools)
            except Exception as exc:
                failures.append(f'{component_name}: {exc}')
                print(f'index us market sentiment backfill failed for {component_name}: {exc}')

        print(
            'index us market sentiment backfill finished, '
            f'vix_upserted: {component_counts["vix"]}, '
            f'fear_greed_upserted: {component_counts["fear_greed"]}, '
            f'hedge_proxy_upserted: {component_counts["hedge_proxy"]}, '
            f'failed: {len(failures)}'
        )
        if failures:
            print('index us market sentiment backfill failed components: ' + '; '.join(failures))
        return sum(component_counts.values())
    finally:
        await db_tools.close()


async def sync_daily_us_market_sentiment():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        component_counts = {
            'vix': 0,
            'fear_greed': 0,
            'hedge_proxy': 0,
        }
        failures = []

        for component_name, worker in [
            ('vix', sync_daily_us_vix),
            ('fear_greed', sync_daily_us_fear_greed),
            ('hedge_proxy', sync_daily_us_hedge_fund_ls_proxy),
        ]:
            try:
                component_counts[component_name] = await worker(db_tools)
            except Exception as exc:
                failures.append(f'{component_name}: {exc}')
                print(f'index us market sentiment daily failed for {component_name}: {exc}')

        print(
            'index us market sentiment daily finished, '
            f'vix_upserted: {component_counts["vix"]}, '
            f'fear_greed_upserted: {component_counts["fear_greed"]}, '
            f'hedge_proxy_upserted: {component_counts["hedge_proxy"]}, '
            f'failed: {len(failures)}'
        )
        if failures:
            print('index us market sentiment daily failed components: ' + '; '.join(failures))
        return sum(component_counts.values())
    finally:
        await db_tools.close()


async def backfill_us_vix():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await backfill_us_vix_history(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_us_vix_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await sync_daily_us_vix(db_tools)
    finally:
        await db_tools.close()


async def backfill_us_fear_greed():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await backfill_us_fear_greed_history(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_us_fear_greed_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await sync_daily_us_fear_greed(db_tools)
    finally:
        await db_tools.close()


async def backfill_us_hedge_proxy():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await backfill_us_hedge_fund_ls_proxy(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_us_hedge_proxy():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await sync_daily_us_hedge_fund_ls_proxy(db_tools)
    finally:
        await db_tools.close()


async def backfill_us_put_call_ratio_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await backfill_us_put_call_ratio(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_us_put_call_ratio_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await sync_daily_us_put_call_ratio(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_us_option_premium_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await sync_daily_us_option_premium(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_us_option_price_pc_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await sync_daily_us_option_price_pc(db_tools)
    finally:
        await db_tools.close()


async def backfill_us_option_price_pc_only(start_date=None, end_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await backfill_us_option_price_pc(
            db_tools,
            start_date=start_date or '2008-01-01',
            end_date=end_date or '2025-12-31',
        )
    finally:
        await db_tools.close()


async def backfill_us_treasury_yield_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await backfill_us_treasury_yield(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_us_treasury_yield_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await sync_daily_us_treasury_yield(db_tools)
    finally:
        await db_tools.close()


async def backfill_us_credit_spread_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await backfill_us_credit_spread(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_us_credit_spread_only():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        return await sync_daily_us_credit_spread(db_tools)
    finally:
        await db_tools.close()


async def sync_daily_from_spot():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        spot_df = await asyncio.to_thread(get_all_index_spot)
        if spot_df is None or spot_df.empty:
            print('No index spot data fetched.')
            return 0

        trade_date = datetime.now().strftime('%Y-%m-%d')
        basic_rows = build_index_basic_rows(spot_df)
        daily_rows = build_index_spot_daily_rows(spot_df, trade_date)

        basic_upserted = await db_tools.upsert_index_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_index_daily_snapshots(daily_rows)

        print(
            'index daily finished, '
            f'index_basic_info upserted: {basic_upserted}, '
            f'index_daily_data upserted: {daily_upserted}'
        )
        return daily_upserted
    finally:
        await db_tools.close()


async def sync_daily_csi_dividend_index(target_date=None):
    target_date_text = str(target_date or datetime.now().date())[:10]
    target_day = datetime.strptime(target_date_text, '%Y-%m-%d')
    source_rows = await asyncio.to_thread(
        fetch_csi_dividend_index_perf,
        (target_day - timedelta(days=14)).strftime('%Y%m%d'),
        target_day.strftime('%Y%m%d'),
    )
    daily_rows = build_csi_dividend_index_daily_rows(source_rows)
    latest_trade_date = daily_rows[-1]['trade_date'] if daily_rows else None
    target_row_count = sum(
        1 for row in daily_rows if row.get('trade_date') == target_date_text
    )
    if target_row_count < 1:
        return {
            'status': 'SOURCE_NOT_READY',
            'target_date': target_date_text,
            'latest_trade_date': latest_trade_date,
            'index_code': CSI_DIVIDEND_INDEX_CODE,
            'index_name': CSI_DIVIDEND_INDEX_NAME,
            'data_source': CSI_DIVIDEND_INDEX_SOURCE,
        }

    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        basic_upserted = await db_tools.upsert_index_basic_info([
            build_csi_dividend_index_basic_row()
        ])
        daily_upserted = await db_tools.upsert_index_daily_snapshots(daily_rows)
        result = {
            'status': 'SUCCESS',
            'target_date': target_date_text,
            'latest_trade_date': latest_trade_date,
            'index_code': CSI_DIVIDEND_INDEX_CODE,
            'index_name': CSI_DIVIDEND_INDEX_NAME,
            'basic_upserted': basic_upserted,
            'daily_upserted': daily_upserted,
            'row_count': len(daily_rows),
            'data_source': CSI_DIVIDEND_INDEX_SOURCE,
        }
        print(
            'CSI Dividend daily finished, '
            f'target date: {target_date_text}, '
            f'rows: {len(daily_rows)}, '
            f'latest: {latest_trade_date}'
        )
        return result
    finally:
        await db_tools.close()


async def collect_us_indices_daily_for_service():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        basic_rows = build_us_index_basic_rows()
        latest_rows = []
        trade_dates = set()

        for index_row in basic_rows:
            history_df = await asyncio.to_thread(get_us_index_history, index_row['index_code'])
            if history_df is None or history_df.empty:
                raise ValueError(f'No history data returned for {index_row["index_code"]}')

            daily_rows = build_us_index_daily_rows(index_row['index_code'], history_df)
            if not daily_rows:
                raise ValueError(f'No valid rows built for {index_row["index_code"]}')

            latest_row = daily_rows[-1]
            latest_rows.append(latest_row)
            trade_dates.add(latest_row['trade_date'])

        basic_upserted = await db_tools.upsert_index_us_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_index_us_daily_snapshots(latest_rows)
        result = {
            'status': 'SUCCESS',
            'market': 'us',
            'index_count': len(latest_rows),
            'basic_upserted': basic_upserted,
            'daily_upserted': daily_upserted,
            'trade_dates': sorted(trade_dates),
            'data_source': US_INDEX_SOURCE,
        }
        print(
            'index us daily finished, '
            f'index_us_basic_info upserted: {basic_upserted}, '
            f'index_us_daily_data upserted: {daily_upserted}, '
            f'trade_dates: {",".join(result["trade_dates"])}'
        )
        return result
    finally:
        await db_tools.close()


async def collect_hk_indices_daily_for_service():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        spot_df = await asyncio.to_thread(get_hk_index_spot)
        if spot_df is None or spot_df.empty:
            raise ValueError('No hk index spot data fetched.')

        trade_date = datetime.now().strftime('%Y-%m-%d')
        basic_rows = build_hk_index_basic_rows(spot_df)
        daily_rows = build_hk_index_spot_daily_rows(spot_df, trade_date)
        if not daily_rows:
            raise ValueError('No valid hk index spot rows built.')

        basic_upserted = await db_tools.upsert_index_hk_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_index_hk_daily_snapshots(daily_rows)
        result = {
            'status': 'SUCCESS',
            'market': 'hk',
            'index_count': len(daily_rows),
            'basic_upserted': basic_upserted,
            'daily_upserted': daily_upserted,
            'trade_dates': [trade_date],
            'data_source': HK_INDEX_SPOT_SOURCE,
        }
        print(
            'index hk daily finished, '
            f'index_hk_basic_info upserted: {basic_upserted}, '
            f'index_hk_daily_data upserted: {daily_upserted}, '
            f'trade_date: {trade_date}'
        )
        return result
    finally:
        await db_tools.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else 'backfill'

    if command == 'backfill':
        await backfill_history()
        return
    if command == 'backfill-bj899050':
        await backfill_special_index_history()
        return
    if command == 'backfill-csi-dividend':
        await backfill_csi_dividend_index_history()
        return
    if command == 'daily-bj899050':
        await sync_daily_special_index()
        return
    if command == 'backfill-us':
        await backfill_us_history()
        return
    if command == 'backfill-hk':
        await backfill_hk_history()
        return
    if command == 'backfill-qvix':
        await backfill_qvix_history()
        return
    if command == 'daily-qvix':
        await sync_daily_qvix()
        return
    if command == 'backfill-news-sentiment':
        await backfill_news_sentiment_scope_history()
        return
    if command == 'daily-news-sentiment':
        await sync_daily_news_sentiment_scope()
        return
    if command == 'backfill-cn-market-fear-greed':
        await backfill_cn_market_fear_greed_history()
        return
    if command == 'daily-cn-market-fear-greed':
        target_date = sys.argv[2] if len(sys.argv) > 2 else None
        await sync_daily_cn_market_fear_greed(target_date=target_date)
        return
    if command == 'backfill-cn-baifenwei-fear-greed':
        await backfill_cn_baifenwei_fear_greed_history()
        return
    if command == 'daily-cn-baifenwei-fear-greed':
        target_date = sys.argv[2] if len(sys.argv) > 2 else None
        await sync_daily_cn_baifenwei_fear_greed(target_date=target_date)
        return
    if command == 'backfill-us-vix':
        await backfill_us_vix()
        return
    if command == 'daily-us-vix':
        await sync_daily_us_vix_only()
        return
    if command == 'backfill-us-fear-greed':
        await backfill_us_fear_greed()
        return
    if command == 'daily-us-fear-greed':
        await sync_daily_us_fear_greed_only()
        return
    if command == 'backfill-us-hedge-proxy':
        await backfill_us_hedge_proxy()
        return
    if command == 'daily-us-hedge-proxy':
        await sync_daily_us_hedge_proxy()
        return
    if command == 'backfill-us-put-call-ratio':
        await backfill_us_put_call_ratio_only()
        return
    if command == 'daily-us-put-call-ratio':
        await sync_daily_us_put_call_ratio_only()
        return
    if command == 'daily-us-option-premium':
        await sync_daily_us_option_premium_only()
        return
    if command == 'daily-us-option-price-pc':
        await sync_daily_us_option_price_pc_only()
        return
    if command == 'backfill-us-option-price-pc':
        start_date = sys.argv[2] if len(sys.argv) > 2 else None
        end_date = sys.argv[3] if len(sys.argv) > 3 else None
        await backfill_us_option_price_pc_only(start_date=start_date, end_date=end_date)
        return
    if command == 'backfill-us-treasury-yield':
        await backfill_us_treasury_yield_only()
        return
    if command == 'daily-us-treasury-yield':
        await sync_daily_us_treasury_yield_only()
        return
    if command == 'backfill-us-credit-spread':
        await backfill_us_credit_spread_only()
        return
    if command == 'daily-us-credit-spread':
        await sync_daily_us_credit_spread_only()
        return
    if command == 'backfill-us-market-sentiment':
        await backfill_us_market_sentiment()
        return
    if command == 'daily-us-market-sentiment':
        await sync_daily_us_market_sentiment()
        return
    if command == 'daily':
        await sync_daily_from_spot()
        return

    raise ValueError(
        'supported commands: backfill, backfill-bj899050, backfill-csi-dividend, '
        'backfill-us, backfill-hk, '
        'daily-bj899050, '
        'backfill-qvix, daily-qvix, backfill-news-sentiment, daily-news-sentiment, '
        'backfill-cn-market-fear-greed, daily-cn-market-fear-greed, '
        'backfill-cn-baifenwei-fear-greed, daily-cn-baifenwei-fear-greed, '
        'backfill-us-vix, daily-us-vix, backfill-us-fear-greed, daily-us-fear-greed, '
        'backfill-us-hedge-proxy, daily-us-hedge-proxy, '
        'backfill-us-put-call-ratio, daily-us-put-call-ratio, daily-us-option-premium, '
        'backfill-us-option-price-pc, daily-us-option-price-pc, '
        'backfill-us-treasury-yield, daily-us-treasury-yield, '
        'backfill-us-credit-spread, daily-us-credit-spread, '
        'backfill-us-market-sentiment, daily-us-market-sentiment, daily'
    )


if __name__ == '__main__':
    asyncio.run(main())
