import asyncio
import re
import sys
from datetime import date, datetime, timedelta

import akshare as ak
import pandas as pd

from akshare_project.core.ak_scheduler_client import (
    SchedulerContext,
    decode_job_result,
    get_job_snapshots,
    submit_registered_job,
)
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 8
BACKFILL_START_DATE = date(2005, 2, 23)
ETF_BACKFILL_TASK_NAME = "etf_backfill_history"
ETF_CATEGORY_SYMBOL = "ETF基金"
LOGGER = get_logger("etf")
PROGRESS_STORE = ProgressStore("etf")

COL_SINA_SYMBOL = "代码"
COL_NAME = "名称"
COL_LATEST = "最新价"
COL_CHANGE_AMOUNT = "涨跌额"
COL_CHANGE_RATE = "涨跌幅"
COL_PRE_CLOSE = "昨收"
COL_OPEN = "今开"
COL_HIGH = "最高"
COL_LOW = "最低"
COL_VOLUME = "成交量"
COL_TURNOVER = "成交额"

COL_HIST_DATE = "date"
COL_HIST_OPEN = "open"
COL_HIST_CLOSE = "close"
COL_HIST_HIGH = "high"
COL_HIST_LOW = "low"
COL_HIST_VOLUME = "volume"


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def save_progress_batch(progress_lines):
    PROGRESS_STORE.append_lines(progress_lines)


def log_error(etf_code, trade_date, error_message):
    LOGGER.error("%s,%s,%s", etf_code, trade_date, error_message)


def fetch_with_retry(
    func,
    *args,
    retries=API_RETRY_COUNT,
    sleep_seconds=API_RETRY_SLEEP_SECONDS,
    scheduler_context=None,
    return_scheduler_meta=False,
    request_key=None,
    **kwargs,
):
    return shared_fetch_with_retry(
        func,
        *args,
        retries=retries,
        sleep_seconds=sleep_seconds,
        logger=LOGGER,
        scheduler_context=scheduler_context,
        return_scheduler_meta=return_scheduler_meta,
        caller_name=LOGGER.name,
        request_key=request_key,
        **kwargs,
    )


def normalize_trade_date(value):
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "date"):
        try:
            return value.date().strftime("%Y-%m-%d")
        except Exception:
            pass
    text = str(value).split(" ")[0].strip()
    if not text:
        return ""
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def normalize_sina_symbol(value):
    text = str(value or "").strip().lower()
    if not text:
        return ""
    matched = re.search(r"(sh|sz)\d{6}$", text)
    if matched:
        return matched.group(0)
    code_match = re.search(r"(\d{6})$", text)
    if not code_match:
        return ""
    code = code_match.group(1)
    prefix = "sh" if code.startswith(("5", "6")) else "sz"
    return f"{prefix}{code}"


def normalize_etf_code(value):
    text = str(value or "").strip().lower()
    matched = re.search(r"(\d{6})$", text)
    return matched.group(1) if matched else ""


def build_etf_backfill_task_payload(etf_code, etf_name, sina_symbol, start_date, end_date):
    return {
        "etf_code": normalize_etf_code(etf_code),
        "etf_name": str(etf_name or "").strip() or None,
        "sina_symbol": normalize_sina_symbol(sina_symbol),
        "start_date": str(start_date),
        "end_date": str(end_date),
    }


def build_etf_hist_request_key(sina_symbol):
    return f"fund_etf_hist_sina:{normalize_sina_symbol(sina_symbol)}"


def get_etf_category_sina(return_scheduler_meta=False):
    return fetch_with_retry(
        ak.fund_etf_category_sina,
        symbol=ETF_CATEGORY_SYMBOL,
        return_scheduler_meta=return_scheduler_meta,
        request_key=f"fund_etf_category_sina:{ETF_CATEGORY_SYMBOL}",
    )


def get_etf_history_sina(sina_symbol, scheduler_context=None, request_key=None):
    return fetch_with_retry(
        ak.fund_etf_hist_sina,
        symbol=normalize_sina_symbol(sina_symbol),
        scheduler_context=scheduler_context,
        request_key=request_key or build_etf_hist_request_key(sina_symbol),
    )


def build_category_records(category_df):
    records = []
    deduped = {}
    for _, row in category_df.iterrows():
        sina_symbol = normalize_sina_symbol(row.get(COL_SINA_SYMBOL))
        etf_code = normalize_etf_code(sina_symbol or row.get(COL_SINA_SYMBOL))
        if not etf_code or not sina_symbol:
            continue
        deduped[etf_code] = {
            "etf_code": etf_code,
            "etf_name": str(row.get(COL_NAME, "")).strip() or None,
            "sina_symbol": sina_symbol,
            "open_price": row.get(COL_OPEN),
            "close_price": row.get(COL_LATEST),
            "high_price": row.get(COL_HIGH),
            "low_price": row.get(COL_LOW),
            "volume": row.get(COL_VOLUME),
            "turnover": row.get(COL_TURNOVER),
            "pre_close_price": row.get(COL_PRE_CLOSE),
            "price_change_amount": row.get(COL_CHANGE_AMOUNT),
            "price_change_rate": row.get(COL_CHANGE_RATE),
        }
    records.extend(deduped.values())
    return records


def filter_category_records(category_records, selected_codes=None):
    normalized_codes = [
        normalize_etf_code(code)
        for code in (selected_codes or [])
        if normalize_etf_code(code)
    ]
    if not normalized_codes:
        return category_records, []

    selected_set = set(normalized_codes)
    filtered = [record for record in category_records if record["etf_code"] in selected_set]
    available_codes = {record["etf_code"] for record in filtered}
    missing_codes = [code for code in normalized_codes if code not in available_codes]
    return filtered, missing_codes


def build_etf_basic_rows(category_records):
    return [
        {
            "etf_code": record["etf_code"],
            "etf_name": record.get("etf_name"),
            "sina_symbol": record.get("sina_symbol"),
        }
        for record in category_records
        if record.get("etf_code") and record.get("sina_symbol")
    ]


def build_etf_daily_rows(category_records):
    trade_date = datetime.now().strftime("%Y-%m-%d")
    rows = []
    for record in category_records:
        rows.append({
            "etf_code": record["etf_code"],
            "etf_name": record.get("etf_name"),
            "sina_symbol": record.get("sina_symbol"),
            "trade_date": trade_date,
            "open_price": record.get("open_price"),
            "close_price": record.get("close_price"),
            "high_price": record.get("high_price"),
            "low_price": record.get("low_price"),
            "volume": record.get("volume"),
            "turnover": record.get("turnover"),
            "amplitude": None,
            "price_change_rate": record.get("price_change_rate"),
            "price_change_amount": record.get("price_change_amount"),
            "turnover_rate": None,
            "pre_close_price": record.get("pre_close_price"),
            "data_source": "fund_etf_category_sina",
        })
    return rows


def build_etf_hist_rows(symbol_row, history_df, start_date, end_date):
    if history_df is None or history_df.empty:
        return []

    rows = []
    temp_df = history_df.copy()
    if COL_HIST_DATE not in temp_df.columns:
        return []
    temp_df = temp_df.sort_values(by=COL_HIST_DATE, ascending=True)
    previous_close = None

    for _, row in temp_df.iterrows():
        trade_date_text = normalize_trade_date(row.get(COL_HIST_DATE))
        if not trade_date_text:
            continue

        trade_date_obj = datetime.strptime(trade_date_text, "%Y-%m-%d").date()
        current_close = row.get(COL_HIST_CLOSE)

        if start_date <= trade_date_obj <= end_date:
            price_change_amount = None
            price_change_rate = None
            amplitude = None
            if previous_close not in (None, 0) and not pd.isna(previous_close):
                try:
                    price_change_amount = float(current_close) - float(previous_close)
                    price_change_rate = price_change_amount / float(previous_close) * 100
                    high_price = row.get(COL_HIST_HIGH)
                    low_price = row.get(COL_HIST_LOW)
                    if high_price is not None and low_price is not None:
                        amplitude = (float(high_price) - float(low_price)) / float(previous_close) * 100
                except (TypeError, ValueError, ZeroDivisionError):
                    price_change_amount = None
                    price_change_rate = None
                    amplitude = None

            rows.append({
                "etf_code": symbol_row["etf_code"],
                "etf_name": symbol_row.get("etf_name"),
                "sina_symbol": symbol_row.get("sina_symbol"),
                "trade_date": trade_date_text,
                "open_price": row.get(COL_HIST_OPEN),
                "close_price": current_close,
                "high_price": row.get(COL_HIST_HIGH),
                "low_price": row.get(COL_HIST_LOW),
                "volume": row.get(COL_HIST_VOLUME),
                "turnover": None,
                "amplitude": amplitude,
                "price_change_rate": price_change_rate,
                "price_change_amount": price_change_amount,
                "turnover_rate": None,
                "pre_close_price": previous_close,
                "data_source": "fund_etf_hist_sina",
            })

        if current_close is not None and not pd.isna(current_close):
            previous_close = current_close

    return rows


async def record_etf_backfill_failure(db_tools, etf_code, etf_name, sina_symbol, start_date, end_date, error_message):
    await db_tools.upsert_failed_task({
        "task_name": ETF_BACKFILL_TASK_NAME,
        "task_stage": "history",
        "task_key": normalize_etf_code(etf_code),
        "payload_json": build_etf_backfill_task_payload(etf_code, etf_name, sina_symbol, start_date, end_date),
        "error_message": str(error_message or "").strip() or None,
    })


async def resolve_etf_backfill_success(db_tools, etf_code, etf_name, sina_symbol, start_date, end_date):
    await db_tools.upsert_success_task({
        "task_name": ETF_BACKFILL_TASK_NAME,
        "task_stage": "history",
        "task_key": normalize_etf_code(etf_code),
        "payload_json": build_etf_backfill_task_payload(etf_code, etf_name, sina_symbol, start_date, end_date),
    })


async def load_selected_etfs(selected_codes=None, return_scheduler_meta=False):
    if return_scheduler_meta:
        category_result = await asyncio.to_thread(get_etf_category_sina, True)
        category_df = category_result.value
    else:
        category_df = await asyncio.to_thread(get_etf_category_sina)
        category_result = None

    if category_df is None or category_df.empty:
        print("No ETF category data fetched from Sina.")
        if return_scheduler_meta:
            return [], [], None
        return [], []

    category_records = build_category_records(category_df)
    filtered_records, missing_codes = filter_category_records(category_records, selected_codes)
    if return_scheduler_meta:
        return filtered_records, missing_codes, category_result
    return filtered_records, missing_codes


async def process_history_symbol(
    symbol_row,
    db_tools,
    semaphore,
    progress_lock,
    progress_lines,
    mode_label,
    start_date,
    end_date,
    record_failures=False,
    swallow_exceptions=True,
    scheduler_context=None,
):
    etf_code = symbol_row["etf_code"]
    etf_name = symbol_row.get("etf_name") or ""
    sina_symbol = symbol_row.get("sina_symbol") or ""

    try:
        async with semaphore:
            if not sina_symbol:
                raise ValueError(f"missing sina_symbol for {etf_code}")
            history_df = await asyncio.to_thread(
                get_etf_history_sina,
                sina_symbol,
                scheduler_context,
                build_etf_hist_request_key(sina_symbol),
            )

        if history_df is None or history_df.empty:
            raise ValueError(f"empty history data for {etf_code}")

        history_rows = build_etf_hist_rows(symbol_row, history_df, start_date, end_date)
        if not history_rows:
            raise ValueError(f"no parsed history rows for {etf_code}")

        upserted = await db_tools.upsert_etf_daily_data(history_rows)
        if record_failures:
            await resolve_etf_backfill_success(db_tools, etf_code, etf_name, sina_symbol, start_date, end_date)

        async with progress_lock:
            progress_lines.append(f"{mode_label},{etf_code},{start_date},{end_date},{upserted}")
            await asyncio.to_thread(save_progress_batch, [progress_lines[-1]])

        print(f"{mode_label} {etf_code}: upserted {upserted}")
        return upserted
    except Exception as exc:
        if record_failures:
            await record_etf_backfill_failure(
                db_tools,
                etf_code,
                etf_name,
                sina_symbol,
                start_date,
                end_date,
                str(exc),
            )
        error_message = f"{mode_label} {etf_code} failed: {exc}"
        print(error_message)
        log_error(etf_code, str(end_date), error_message)
        if swallow_exceptions:
            return 0
        raise


async def sync_history(mode_label, selected_codes=None, record_failures=False):
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    progress_lock = asyncio.Lock()
    progress_lines = []

    try:
        category_records, missing_codes, category_result = await load_selected_etfs(
            selected_codes,
            return_scheduler_meta=True,
        )
        if missing_codes:
            print(f"{mode_label} ignored unknown ETF codes: {', '.join(missing_codes)}")
        if not category_records:
            print(f"No ETF symbols matched for {mode_label}.")
            return 0

        basic_rows = build_etf_basic_rows(category_records)
        basic_upserted = await db_tools.upsert_etf_basic_info(basic_rows)
        print(f"{mode_label} etf_basic_info_sina upserted: {basic_upserted}")

        end_date = datetime.now().date() - timedelta(days=1)
        scheduler_context = None
        if category_result is not None:
            scheduler_context = SchedulerContext(
                parent_job_id=category_result.job_id,
                root_job_id=category_result.root_job_id,
                workflow_name=f"{mode_label}:category_to_hist",
            )

        tasks = [
            process_history_symbol(
                symbol_row,
                db_tools,
                semaphore,
                progress_lock,
                progress_lines,
                mode_label,
                BACKFILL_START_DATE,
                end_date,
                record_failures=record_failures,
                swallow_exceptions=True,
                scheduler_context=scheduler_context,
            )
            for symbol_row in basic_rows
        ]
        results = await asyncio.gather(*tasks)
        total_upserted = sum(results)
        print(
            f"{mode_label} finished, "
            f"symbols={len(basic_rows)}, "
            f"upserted_rows={total_upserted}"
        )
        return total_upserted
    finally:
        await db_tools.close()


async def backfill_history(selected_codes=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        category_records, missing_codes, category_result = await load_selected_etfs(
            selected_codes,
            return_scheduler_meta=True,
        )
        if missing_codes:
            print(f"etf_backfill ignored unknown ETF codes: {', '.join(missing_codes)}")
        if not category_records:
            print("No ETF symbols matched for etf_backfill.")
            return 0

        basic_rows = build_etf_basic_rows(category_records)
        basic_upserted = await db_tools.upsert_etf_basic_info(basic_rows)
        print(f"etf_backfill etf_basic_info_sina upserted: {basic_upserted}")

        end_date = datetime.now().date() - timedelta(days=1)
        scheduler_context = None
        if category_result is not None:
            scheduler_context = SchedulerContext(
                parent_job_id=category_result.job_id,
                root_job_id=category_result.root_job_id,
                workflow_name="etf_backfill:category_to_hist",
            )

        submitted_jobs = []
        for symbol_row in basic_rows:
            sina_symbol = symbol_row["sina_symbol"]
            handle = await asyncio.to_thread(
                submit_registered_job,
                ak.fund_etf_hist_sina,
                scheduler_context=scheduler_context,
                caller_name=LOGGER.name,
                request_key=build_etf_hist_request_key(sina_symbol),
                symbol=sina_symbol,
            )
            submitted_jobs.append({
                **symbol_row,
                "job_id": handle.job_id,
                "root_job_id": handle.root_job_id,
                "initial_status": handle.status,
            })

        deduped_jobs = {job["job_id"]: job for job in submitted_jobs}
        print(
            "etf_backfill submitted all history jobs to scheduler, "
            f"symbols={len(basic_rows)}, "
            f"queued_jobs={len(deduped_jobs)}"
        )

        remaining_jobs = dict(deduped_jobs)
        total_upserted = 0
        last_reported_remaining = None

        while remaining_jobs:
            snapshots = await asyncio.to_thread(get_job_snapshots, list(remaining_jobs.keys()))
            if not snapshots:
                await asyncio.sleep(API_RETRY_SLEEP_SECONDS)
                continue

            completed_any = False
            for snapshot in snapshots:
                symbol_row = remaining_jobs.get(snapshot.job_id)
                if symbol_row is None:
                    continue

                if snapshot.status not in {"SUCCESS", "FAILED", "CANCELLED"}:
                    continue

                completed_any = True
                etf_code = symbol_row["etf_code"]
                etf_name = symbol_row.get("etf_name") or ""
                sina_symbol = symbol_row.get("sina_symbol") or ""

                if snapshot.status == "SUCCESS":
                    try:
                        history_df = decode_job_result(snapshot)
                        if history_df is None or history_df.empty:
                            raise ValueError(f"empty history data for {etf_code}")

                        history_rows = build_etf_hist_rows(symbol_row, history_df, BACKFILL_START_DATE, end_date)
                        if not history_rows:
                            raise ValueError(f"no parsed history rows for {etf_code}")

                        upserted = await db_tools.upsert_etf_daily_data(history_rows)
                        await resolve_etf_backfill_success(
                            db_tools,
                            etf_code,
                            etf_name,
                            sina_symbol,
                            BACKFILL_START_DATE,
                            end_date,
                        )
                        total_upserted += upserted
                        await asyncio.to_thread(
                            save_progress_batch,
                            [f"etf_backfill,{etf_code},{BACKFILL_START_DATE},{end_date},{upserted}"],
                        )
                        print(f"etf_backfill {etf_code}: upserted {upserted}")
                    except Exception as exc:
                        await record_etf_backfill_failure(
                            db_tools,
                            etf_code,
                            etf_name,
                            sina_symbol,
                            BACKFILL_START_DATE,
                            end_date,
                            str(exc),
                        )
                        print(f"etf_backfill {etf_code} failed after success result parse: {exc}")
                        log_error(etf_code, str(end_date), str(exc))
                else:
                    error_message = snapshot.error_message or f"scheduler job {snapshot.status.lower()}"
                    await record_etf_backfill_failure(
                        db_tools,
                        etf_code,
                        etf_name,
                        sina_symbol,
                        BACKFILL_START_DATE,
                        end_date,
                        error_message,
                    )
                    print(f"etf_backfill {etf_code} scheduler failed: {error_message}")
                    log_error(etf_code, str(end_date), error_message)

                remaining_jobs.pop(snapshot.job_id, None)

            if remaining_jobs:
                remaining_count = len(remaining_jobs)
                if last_reported_remaining != remaining_count:
                    print(f"etf_backfill waiting for scheduler jobs, remaining={remaining_count}")
                    last_reported_remaining = remaining_count
                await asyncio.sleep(API_RETRY_SLEEP_SECONDS if completed_any else max(2, API_RETRY_SLEEP_SECONDS))

        print(
            "etf_backfill finished, "
            f"symbols={len(basic_rows)}, "
            f"queued_jobs={len(deduped_jobs)}, "
            f"upserted_rows={total_upserted}"
        )
        return total_upserted
    finally:
        await db_tools.close()


async def weekly_repair(selected_codes=None):
    return await sync_history("etf_weekly_repair", selected_codes, record_failures=False)


async def collect_etf_backfill_targets(db_tools, selected_codes=None):
    pending_failures = await db_tools.get_pending_etf_backfill_failures(selected_codes)
    missing_hist = await db_tools.get_etf_codes_missing_hist_data(selected_codes)

    target_map = {}
    for failure in pending_failures:
        payload = failure.get("payload") or {}
        etf_code = normalize_etf_code(payload.get("etf_code") or failure.get("task_key"))
        if not etf_code:
            continue
        target_map[etf_code] = {
            "etf_code": etf_code,
            "etf_name": str(payload.get("etf_name") or "").strip() or None,
            "sina_symbol": normalize_sina_symbol(payload.get("sina_symbol")),
            "failure_id": failure.get("id"),
        }

    for item in missing_hist:
        etf_code = normalize_etf_code(item.get("etf_code"))
        if not etf_code:
            continue
        current = target_map.get(etf_code, {})
        target_map[etf_code] = {
            "etf_code": etf_code,
            "etf_name": current.get("etf_name") or item.get("etf_name"),
            "sina_symbol": current.get("sina_symbol") or normalize_sina_symbol(item.get("sina_symbol")),
            "failure_id": current.get("failure_id"),
        }

    valid_targets = [target for target in target_map.values() if target.get("sina_symbol")]
    return pending_failures, missing_hist, valid_targets


async def process_repair_backfill_target(
    symbol_row,
    db_tools,
    semaphore,
    progress_lock,
    progress_lines,
    end_date,
):
    failure_id = symbol_row.get("failure_id")
    try:
        inserted = await process_history_symbol(
            symbol_row,
            db_tools,
            semaphore,
            progress_lock,
            progress_lines,
            "etf_repair_backfill",
            BACKFILL_START_DATE,
            end_date,
            record_failures=True,
            swallow_exceptions=False,
        )
        if failure_id:
            await db_tools.mark_failed_task_retry_result(failure_id, success=True)
        return inserted
    except Exception as exc:
        if failure_id:
            await db_tools.mark_failed_task_retry_result(failure_id, success=False, error_message=str(exc))
        return 0


async def repair_backfill_once(selected_codes=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    progress_lock = asyncio.Lock()
    progress_lines = []

    try:
        pending_failures, missing_hist, targets = await collect_etf_backfill_targets(db_tools, selected_codes)
        if not targets:
            print("No ETF backfill repair targets found.")
            return 0

        end_date = datetime.now().date() - timedelta(days=1)
        results = await asyncio.gather(*[
            process_repair_backfill_target(
                symbol_row,
                db_tools,
                semaphore,
                progress_lock,
                progress_lines,
                end_date,
            )
            for symbol_row in targets
        ])
        total_upserted = sum(results)
        print(
            "etf repair-backfill round finished, "
            f"targets={len(targets)}, "
            f"pending_failures={len(pending_failures)}, "
            f"missing_hist={len(missing_hist)}, "
            f"upserted_rows={total_upserted}"
        )
        return total_upserted
    finally:
        await db_tools.close()


async def repair_backfill_until_complete(selected_codes=None):
    round_no = 0
    total_upserted = 0

    while True:
        round_no += 1
        selected_text = ",".join(selected_codes) if selected_codes else "ALL"
        print(f"etf repair-backfill round {round_no} started: symbols={selected_text}")
        total_upserted += await repair_backfill_once(selected_codes)

        db_tools = DbTools()
        await db_tools.init_pool()
        try:
            pending_failures = await db_tools.get_pending_etf_backfill_failures(selected_codes)
            missing_hist = await db_tools.get_etf_codes_missing_hist_data(selected_codes)
        finally:
            await db_tools.close()

        print(
            f"etf repair-backfill round {round_no} finished, "
            f"remaining_missing={len(missing_hist)}, "
            f"pending_failures={len(pending_failures)}"
        )

        if not missing_hist and not pending_failures:
            print(
                "etf repair-backfill completed, "
                f"symbols={selected_text}, "
                f"total_upserted={total_upserted}"
            )
            return total_upserted

        await asyncio.sleep(API_RETRY_SLEEP_SECONDS)


async def sync_daily(selected_codes=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        category_records, missing_codes = await load_selected_etfs(selected_codes)
        if missing_codes:
            print(f"etf_daily ignored unknown ETF codes: {', '.join(missing_codes)}")
        if not category_records:
            print("No ETF symbols matched for daily sync.")
            return 0

        basic_rows = build_etf_basic_rows(category_records)
        daily_rows = build_etf_daily_rows(category_records)
        basic_upserted = await db_tools.upsert_etf_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_etf_daily_data(daily_rows)
        trade_dates = sorted({row["trade_date"] for row in daily_rows if row.get("trade_date")})
        print(
            "etf daily finished, "
            f"etf_basic_info_sina upserted: {basic_upserted}, "
            f"etf_daily_data_sina upserted: {daily_upserted}, "
            f"data_source: fund_etf_category_sina, "
            f"trade_dates: {','.join(trade_dates) if trade_dates else 'NONE'}"
        )
        return daily_upserted
    finally:
        await db_tools.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "backfill"
    selected_codes = sys.argv[2:] if len(sys.argv) > 2 else []

    if command == "backfill":
        await backfill_history(selected_codes)
        return
    if command == "daily":
        await sync_daily(selected_codes)
        return
    if command == "weekly-repair":
        await weekly_repair(selected_codes)
        return
    if command == "repair-backfill":
        await repair_backfill_until_complete(selected_codes)
        return

    raise ValueError(
        "usage: python run.py etf [backfill|daily|weekly-repair|repair-backfill] [ETF_CODE ...]"
    )


if __name__ == "__main__":
    asyncio.run(main())
