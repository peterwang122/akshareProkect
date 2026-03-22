import asyncio
import sys
from pathlib import Path

import pandas as pd

from util.db_tool import DbTools

DATE_COLUMN_CANDIDATES = ['日期', 'date', 'Date', 'Unnamed: 0']
SUPPORTED_INDEX_COLUMNS = ['上证50', '沪深300', '中证500', '中证1000']


def normalize_date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ''
    timestamp = pd.to_datetime(value, errors='coerce')
    if pd.isna(timestamp):
        return ''
    return timestamp.strftime('%Y-%m-%d')


def resolve_excel_path():
    if len(sys.argv) > 1:
        return Path(sys.argv[1]).resolve()

    for path in Path('.').glob('*.xlsx'):
        if not path.name.startswith('~$'):
            return path.resolve()
    raise FileNotFoundError('No xlsx file found. Usage: python emotion_excel_import.py <xlsx_path>')


def find_date_column(df):
    for candidate in DATE_COLUMN_CANDIDATES:
        if candidate in df.columns:
            return candidate
    raise ValueError(f'No date column found. Supported candidates: {DATE_COLUMN_CANDIDATES}')


def parse_excel_rows(excel_path):
    raw_df = pd.read_excel(excel_path)
    date_column = find_date_column(raw_df)

    rows = []
    for _, row in raw_df.iterrows():
        emotion_date = normalize_date(row.get(date_column))
        if not emotion_date:
            continue

        for index_name in SUPPORTED_INDEX_COLUMNS:
            if index_name not in raw_df.columns:
                continue
            emotion_value = row.get(index_name)
            if pd.isna(emotion_value):
                continue

            rows.append({
                'emotion_date': emotion_date,
                'index_name': index_name,
                'emotion_value': emotion_value,
                'source_file': excel_path.name,
                'data_source': 'excel',
            })

    return rows


async def run():
    excel_path = resolve_excel_path()
    rows = parse_excel_rows(excel_path)
    if not rows:
        print('No valid emotion rows parsed from xlsx.')
        return

    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        inserted = await db_tools.batch_excel_emotion_data(rows)
        print(f'excel emotion import finished, parsed rows: {len(rows)}, inserted rows: {inserted}')
    finally:
        await db_tools.close()


if __name__ == '__main__':
    asyncio.run(run())
