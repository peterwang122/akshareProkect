import json
import time
from contextlib import contextmanager
from datetime import datetime, timedelta

import pymysql
from pymysql.cursors import DictCursor

from akshare_project.core.paths import get_config_dir


def load_db_info():
    config_path = get_config_dir() / "db_info.json"
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)


class SchedulerStore:
    def __init__(self):
        self.db_info = load_db_info()
        self.session_time_zone = str(self.db_info.get("timezone", "+08:00")).strip() or "+08:00"

    @staticmethod
    def is_lock_contention_error(exc):
        if not isinstance(exc, pymysql.err.OperationalError):
            return False
        error_code = int(exc.args[0]) if exc.args else 0
        return error_code in {1205, 1213}

    @staticmethod
    def is_empty_dataframe_payload(result_type, result_json):
        if str(result_type or "").strip().lower() != "dataframe":
            return False
        if not result_json:
            return False
        try:
            payload = json.loads(result_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        columns = payload.get("columns")
        records = payload.get("records")
        return isinstance(records, list) and len(records) == 0 and (columns is None or isinstance(columns, list))

    @contextmanager
    def connection(self):
        conn = pymysql.connect(
            host=self.db_info.get("host"),
            port=int(self.db_info.get("port", 3306)),
            user=self.db_info.get("user"),
            password=self.db_info.get("passwd"),
            database=self.db_info.get("database") or self.db_info.get("db"),
            charset=self.db_info.get("charset", "utf8mb4"),
            cursorclass=DictCursor,
            autocommit=False,
            init_command=f"SET time_zone = '{self.session_time_zone}'",
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 3")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def run_with_lock_retry(self, operation, max_attempts=5, base_sleep_seconds=0.05):
        for attempt in range(1, int(max_attempts) + 1):
            try:
                return operation()
            except pymysql.err.OperationalError as exc:
                if not self.is_lock_contention_error(exc) or attempt >= int(max_attempts):
                    raise
                time.sleep(float(base_sleep_seconds) * attempt)

    def submit_job(self, payload):
        request_key = str(payload["request_key"]).strip()
        parent_job_id = payload.get("parent_job_id")
        root_job_id = payload.get("root_job_id")
        status = "PENDING"
        for attempt in range(1, 6):
            now = datetime.now()
            try:
                with self.connection() as conn:
                    with conn.cursor() as cursor:
                        if parent_job_id:
                            cursor.execute(
                                "SELECT id, root_job_id, status FROM ak_request_jobs WHERE id = %s",
                                (parent_job_id,),
                            )
                            parent = cursor.fetchone()
                            if not parent:
                                raise ValueError(f"parent_job_id not found: {parent_job_id}")
                            if root_job_id is None:
                                root_job_id = parent.get("root_job_id") or parent.get("id")
                            if parent.get("status") != "SUCCESS":
                                status = "WAITING_PARENT"

                        query = """
                        INSERT INTO ak_request_jobs (
                            request_key,
                            function_name,
                            source_group,
                            args_json,
                            kwargs_json,
                            status,
                            next_run_at,
                            parent_job_id,
                            root_job_id,
                            workflow_name,
                            caller_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            updated_at = CURRENT_TIMESTAMP
                        """
                        cursor.execute(
                            query,
                            (
                                request_key,
                                payload["function_name"],
                                payload["source_group"],
                                json.dumps(payload.get("args") or [], ensure_ascii=False, default=str),
                                json.dumps(payload.get("kwargs") or {}, ensure_ascii=False, default=str),
                                status,
                                now,
                                parent_job_id,
                                root_job_id,
                                payload.get("workflow_name"),
                                payload.get("caller_name"),
                            ),
                        )
                        # For INSERT ... ON DUPLICATE KEY UPDATE, PyMySQL may still expose
                        # lastrowid for the existing row. Use rowcount semantics instead:
                        # 1 => inserted, 0/2 => duplicate-path reuse.
                        is_new_row = int(cursor.rowcount or 0) == 1
                        if is_new_row:
                            job_id = cursor.lastrowid
                            cursor.execute(
                                "UPDATE ak_request_jobs SET root_job_id = COALESCE(root_job_id, %s) WHERE id = %s",
                                (root_job_id or job_id, job_id),
                            )
                        else:
                            cursor.execute("SELECT id FROM ak_request_jobs WHERE request_key = %s", (request_key,))
                            row = cursor.fetchone()
                            job_id = row["id"]

                        cursor.execute("SELECT * FROM ak_request_jobs WHERE id = %s", (job_id,))
                        row = cursor.fetchone()
                        reused_existing = not is_new_row
                        existing_status = str(row.get("status") or "").strip().upper()
                        was_empty_success = (
                            reused_existing
                            and str(payload.get("function_name") or "").strip().lower() == "stock_zh_a_hist_tx"
                            and existing_status == "SUCCESS"
                            and self.is_empty_dataframe_payload(row.get("result_type"), row.get("result_json"))
                        )
                        if was_empty_success:
                            # Empty dataframe cache from a previous run should not block re-execution.
                            cursor.execute(
                                """
                                UPDATE ak_request_jobs
                                SET status = %s,
                                    attempt_count = 0,
                                    next_run_at = %s,
                                    lease_until = NULL,
                                    error_category = NULL,
                                    error_message = NULL,
                                    result_type = NULL,
                                    result_json = NULL,
                                    started_at = NULL,
                                    finished_at = NULL,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                                """,
                                (status, now, job_id),
                            )
                            cursor.execute("SELECT * FROM ak_request_jobs WHERE id = %s", (job_id,))
                            row = cursor.fetchone()
                            row["_dedupe_reused"] = False
                            row["_dedupe_requeued_empty_success"] = True
                            return row

                        requeue_terminal_status = (
                            reused_existing
                            and existing_status in {"FAILED", "CANCELLED"}
                        )
                        if requeue_terminal_status:
                            cursor.execute(
                                """
                                UPDATE ak_request_jobs
                                SET status = %s,
                                    attempt_count = 0,
                                    next_run_at = %s,
                                    lease_until = NULL,
                                    error_category = NULL,
                                    error_message = NULL,
                                    result_type = NULL,
                                    result_json = NULL,
                                    started_at = NULL,
                                    finished_at = NULL,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                                """,
                                (status, now, job_id),
                            )
                            cursor.execute("SELECT * FROM ak_request_jobs WHERE id = %s", (job_id,))
                            row = cursor.fetchone()
                            row["_dedupe_reused"] = False
                            row["_dedupe_requeued_terminal_status"] = existing_status
                            return row

                        row["_dedupe_reused"] = reused_existing
                        return row
            except pymysql.err.OperationalError as exc:
                error_code = int(exc.args[0]) if exc.args else 0
                if error_code not in {1205, 1213} or attempt >= 5:
                    raise
                time.sleep(0.05 * attempt)

    def get_job(self, job_id):
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM ak_request_jobs WHERE id = %s", (job_id,))
                return cursor.fetchone()

    def get_jobs(self, job_ids):
        normalized_ids = [
            int(job_id)
            for job_id in (job_ids or [])
            if str(job_id).strip()
        ]
        if not normalized_ids:
            return []

        placeholders = ",".join(["%s"] * len(normalized_ids))
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT * FROM ak_request_jobs WHERE id IN ({placeholders}) ORDER BY id ASC",
                    normalized_ids,
                )
                return list(cursor.fetchall())

    def recover_stale_jobs(self, lease_seconds):
        now = datetime.now()
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE ak_request_jobs
                        SET status = 'PENDING',
                            error_category = NULL,
                            error_message = NULL,
                            lease_until = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE status = 'RUNNING'
                          AND lease_until IS NOT NULL
                          AND lease_until < %s
                        """,
                        (now,),
                    )
                    return cursor.rowcount
        except pymysql.err.OperationalError as exc:
            if self.is_lock_contention_error(exc):
                return 0
            raise

    def cleanup_old_results(self, retention_hours):
        threshold = datetime.now() - timedelta(hours=float(retention_hours))
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        DELETE FROM ak_request_jobs
                        WHERE status IN ('SUCCESS', 'FAILED', 'CANCELLED')
                          AND finished_at IS NOT NULL
                          AND finished_at < %s
                        """,
                        (threshold,),
                    )
                    return cursor.rowcount
        except pymysql.err.OperationalError as exc:
            if self.is_lock_contention_error(exc):
                return 0
            raise

    def reconcile_waiting_children(self, cancel_on_parent_failure=True):
        changed = 0
        now = datetime.now()
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE ak_request_jobs child
                        INNER JOIN ak_request_jobs parent ON parent.id = child.parent_job_id
                        SET child.status = 'PENDING',
                            child.next_run_at = %s,
                            child.updated_at = CURRENT_TIMESTAMP
                        WHERE child.status = 'WAITING_PARENT'
                          AND parent.status = 'SUCCESS'
                        """,
                        (now,),
                    )
                    changed += cursor.rowcount

                    if cancel_on_parent_failure:
                        cursor.execute(
                            """
                            UPDATE ak_request_jobs child
                            INNER JOIN ak_request_jobs parent ON parent.id = child.parent_job_id
                            SET child.status = 'CANCELLED',
                                child.error_category = 'parent_failed',
                                child.error_message = CONCAT('parent job failed: ', parent.id),
                                child.finished_at = %s,
                                child.updated_at = CURRENT_TIMESTAMP
                            WHERE child.status = 'WAITING_PARENT'
                              AND parent.status IN ('FAILED', 'CANCELLED')
                            """,
                            (now,),
                        )
                        changed += cursor.rowcount
            return changed
        except pymysql.err.OperationalError as exc:
            if self.is_lock_contention_error(exc):
                return 0
            raise

    def lease_next_job(self, source_group, lease_seconds):
        now = datetime.now()
        lease_until = datetime.now() + timedelta(seconds=int(lease_seconds))
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM ak_request_jobs
                    WHERE source_group = %s
                      AND status = 'PENDING'
                      AND next_run_at <= %s
                    ORDER BY next_run_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (source_group, now),
                )
                row = cursor.fetchone()
                if not row:
                    return None

                cursor.execute(
                    """
                    UPDATE ak_request_jobs
                    SET status = 'RUNNING',
                        attempt_count = attempt_count + 1,
                        started_at = %s,
                        lease_until = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (now, lease_until, row["id"]),
                )
                cursor.execute("SELECT * FROM ak_request_jobs WHERE id = %s", (row["id"],))
                return cursor.fetchone()

    def mark_success(self, job_id, result_type, result_json):
        now = datetime.now()
        def operation():
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE ak_request_jobs
                        SET status = 'SUCCESS',
                            error_category = NULL,
                            error_message = NULL,
                            result_type = %s,
                            result_json = %s,
                            lease_until = NULL,
                            finished_at = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (result_type, result_json, now, job_id),
                    )
                    return cursor.rowcount

        return self.run_with_lock_retry(operation)

    def mark_retry(self, job_id, error_category, error_message, next_run_at):
        def operation():
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE ak_request_jobs
                        SET status = 'PENDING',
                            error_category = %s,
                            error_message = %s,
                            next_run_at = %s,
                            lease_until = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (error_category, error_message, next_run_at, job_id),
                    )
                    return cursor.rowcount

        return self.run_with_lock_retry(operation)

    def mark_failed(self, job_id, error_category, error_message):
        now = datetime.now()
        def operation():
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE ak_request_jobs
                        SET status = 'FAILED',
                            error_category = %s,
                            error_message = %s,
                            lease_until = NULL,
                            finished_at = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (error_category, error_message, now, job_id),
                    )
                    return cursor.rowcount

        return self.run_with_lock_retry(operation)

    def get_queue_stats(self):
        stats = {}
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT source_group, status, COUNT(*) AS total
                    FROM ak_request_jobs
                    GROUP BY source_group, status
                    """
                )
                for row in cursor.fetchall():
                    stats.setdefault(row["source_group"], {})[row["status"]] = int(row["total"])
        return stats

    def count_empty_stock_hist_successes(self):
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM ak_request_jobs
                    WHERE function_name = 'stock_zh_a_hist_tx'
                      AND status = 'SUCCESS'
                      AND result_type = 'dataframe'
                      AND JSON_VALID(result_json) = 1
                      AND JSON_LENGTH(result_json, '$.records') = 0
                    """
                )
                row = cursor.fetchone() or {}
                return int(row.get("total") or 0)

    def get_recent_empty_stock_hist_successes(self, limit=20):
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, request_key, attempt_count, created_at, updated_at
                    FROM ak_request_jobs
                    WHERE function_name = 'stock_zh_a_hist_tx'
                      AND status = 'SUCCESS'
                      AND result_type = 'dataframe'
                      AND JSON_VALID(result_json) = 1
                      AND JSON_LENGTH(result_json, '$.records') = 0
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (int(limit),),
                )
                return list(cursor.fetchall())
