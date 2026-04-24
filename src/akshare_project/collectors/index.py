import asyncio
import csv
import io
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import akshare as ak
import pandas as pd
import requests

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 5
LOGGER = get_logger('index')
PROGRESS_STORE = ProgressStore('index')

SPECIAL_INDEX_CODE = 'bj899050'
SPECIAL_INDEX_SIMPLE_CODE = '899050'
SPECIAL_INDEX_MARKET = 'bj'
SPECIAL_INDEX_NAME = '北证50'
SPECIAL_INDEX_SOURCE = 'stock_zh_index_daily'

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

US_VIX_HISTORY_URL = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv'
US_FEAR_GREED_CNN_URL = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'
US_FEAR_GREED_HISTORY_START_DATE = '2020-09-19'
US_FEAR_GREED_MIRROR_URLS = [
    (
        'https://raw.githubusercontent.com/whit3rabbit/fear-greed-data/main/'
        'datasets/hackingthemarkets_fear_greed_data.csv'
    ),
]
OFR_API_BASE_URL = 'https://data.financialresearch.gov/hf/v1'
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
CNN_HTTP_HEADERS = {
    **DEFAULT_HTTP_HEADERS,
    'Accept': 'application/json,text/plain,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.cnn.com/markets/fear-and-greed',
    'Origin': 'https://www.cnn.com',
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
    return build_calculated_history_rows(
        index_code,
        history_df,
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

        inserted = await db_tools.batch_index_qvix_daily_data(daily_rows)
        print(f'index qvix backfill {index_code} inserted: {inserted}')
        return inserted
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
        inserted_rows = await asyncio.gather(
            *[process_qvix_index(index_row, source_df, db_tools) for index_row in QVIX_DEFINITIONS]
        )
        total_inserted = sum(inserted_rows)
        print(
            'index qvix history backfill finished, '
            f'index_qvix_basic_info upserted: {basic_upserted}, '
            f'index_qvix_daily_data inserted: {total_inserted}'
        )
        return total_inserted
    finally:
        await db_tools.close()


async def sync_daily_qvix():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        source_df = await asyncio.to_thread(fetch_qvix_daily_source)
        basic_rows = build_qvix_basic_rows()
        latest_rows = []
        trade_dates = set()

        for index_row in QVIX_DEFINITIONS:
            history_df = build_qvix_history_from_source(index_row, source_df)
            if history_df is None or history_df.empty:
                raise ValueError(f'No history data returned for {index_row["index_code"]}')

            daily_rows = build_qvix_daily_rows(index_row['index_code'], history_df, index_row['data_source'])
            if not daily_rows:
                raise ValueError(f'No valid rows built for {index_row["index_code"]}')

            latest_row = daily_rows[-1]
            latest_rows.append(latest_row)
            trade_dates.add(latest_row['trade_date'])

        basic_upserted = await db_tools.upsert_index_qvix_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_index_qvix_daily_snapshots(latest_rows)
        latest_trade_date = max(trade_dates) if trade_dates else ''
        today = datetime.now().strftime('%Y-%m-%d')
        if latest_trade_date and latest_trade_date < today:
            print(
                'index qvix daily source is behind local date, '
                f'latest_source_trade_date={latest_trade_date}, local_date={today}'
            )
        print(
            'index qvix daily finished, '
            f'index_qvix_basic_info upserted: {basic_upserted}, '
            f'index_qvix_daily_data upserted: {daily_upserted}, '
            f'trade_dates: {",".join(sorted(trade_dates))}'
        )
        return daily_upserted
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
        'supported commands: backfill, backfill-bj899050, backfill-us, backfill-hk, '
        'daily-bj899050, '
        'backfill-qvix, daily-qvix, backfill-news-sentiment, daily-news-sentiment, '
        'backfill-us-vix, daily-us-vix, backfill-us-fear-greed, daily-us-fear-greed, '
        'backfill-us-hedge-proxy, daily-us-hedge-proxy, '
        'backfill-us-market-sentiment, daily-us-market-sentiment, daily'
    )


if __name__ == '__main__':
    asyncio.run(main())
