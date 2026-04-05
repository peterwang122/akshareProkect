import asyncio
import sys
from pathlib import Path

import pandas as pd

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.paths import get_input_dir
from akshare_project.collectors.quant_index import refresh_trade_dates
from akshare_project.db.db_tool import DbTools

DATE_COLUMN_CANDIDATES = ["\u65e5\u671f", "date", "Date", "Unnamed: 0"]
SUPPORTED_INDEX_COLUMNS = [
    "\u4e0a\u8bc150",
    "\u6caa\u6df1300",
    "\u4e2d\u8bc1500",
    "\u4e2d\u8bc11000",
]
LOGGER = get_logger("excel_emotion")


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def normalize_date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return ""
    return timestamp.strftime("%Y-%m-%d")


def resolve_excel_path():
    if len(sys.argv) > 1:
        return Path(sys.argv[1]).resolve()

    for path in get_input_dir().glob("*.xlsx"):
        if not path.name.startswith("~$"):
            return path.resolve()
    for path in Path(".").glob("*.xlsx"):
        if not path.name.startswith("~$"):
            return path.resolve()
    raise FileNotFoundError("No xlsx file found. Usage: python run.py emotion-excel import [xlsx_path]")


def find_date_column(df):
    for candidate in DATE_COLUMN_CANDIDATES:
        if candidate in df.columns:
            return candidate
    raise ValueError(f"No date column found. Supported candidates: {DATE_COLUMN_CANDIDATES}")


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
                "emotion_date": emotion_date,
                "index_name": index_name,
                "emotion_value": emotion_value,
                "source_file": excel_path.name,
                "data_source": "excel",
            })

    return rows


async def run():
    excel_path = resolve_excel_path()
    rows = parse_excel_rows(excel_path)
    if not rows:
        print("No valid emotion rows parsed from xlsx.")
        return

    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        result = await db_tools.batch_excel_emotion_data(rows)
        affected_dates = result.get("affected_dates", [])
        quant_affected = 0
        if affected_dates:
            quant_affected = await refresh_trade_dates(db_tools, affected_dates)
        print(
            "excel emotion import finished, "
            f"parsed rows: {result.get('parsed_rows', 0)}, "
            f"inserted rows: {result.get('inserted_rows', 0)}, "
            f"updated rows: {result.get('updated_rows', 0)}, "
            f"affected dates: {','.join(affected_dates) if affected_dates else 'NONE'}, "
            f"quant refreshed: {quant_affected}"
        )
    finally:
        await db_tools.close()


if __name__ == "__main__":
    asyncio.run(run())
