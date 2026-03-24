import json
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
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def submit_job(self, payload):
        request_key = str(payload["request_key"]).strip()
        parent_job_id = payload.get("parent_job_id")
        root_job_id = payload.get("root_job_id")
        status = "PENDING"

        with self.connection() as conn:
            with conn.cursor() as cursor:
                if parent_job_id:
                    cursor.execute("SELECT id, root_job_id, status FROM ak_request_jobs WHERE id = %s", (parent_job_id,))
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
                ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s)
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
                        parent_job_id,
                        root_job_id,
                        payload.get("workflow_name"),
                        payload.get("caller_name"),
                    ),
                )
                if cursor.lastrowid:
                    job_id = cursor.lastrowid
                else:
                    cursor.execute("SELECT id FROM ak_request_jobs WHERE request_key = %s", (request_key,))
                    row = cursor.fetchone()
                    job_id = row["id"]

                cursor.execute(
                    "UPDATE ak_request_jobs SET root_job_id = COALESCE(root_job_id, %s) WHERE id = %s",
                    (root_job_id or job_id, job_id),
                )
                cursor.execute("SELECT * FROM ak_request_jobs WHERE id = %s", (job_id,))
                return cursor.fetchone()

    def get_job(self, job_id):
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM ak_request_jobs WHERE id = %s", (job_id,))
                return cursor.fetchone()

    def recover_stale_jobs(self, lease_seconds):
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
                      AND lease_until < CURRENT_TIMESTAMP
                    """
                )
                return cursor.rowcount

    def cleanup_old_results(self, retention_hours):
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM ak_request_jobs
                    WHERE status IN ('SUCCESS', 'FAILED', 'CANCELLED')
                      AND finished_at IS NOT NULL
                      AND finished_at < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL %s HOUR)
                    """,
                    (int(retention_hours),),
                )
                return cursor.rowcount

    def reconcile_waiting_children(self, cancel_on_parent_failure=True):
        changed = 0
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE ak_request_jobs child
                    INNER JOIN ak_request_jobs parent ON parent.id = child.parent_job_id
                    SET child.status = 'PENDING',
                        child.next_run_at = CURRENT_TIMESTAMP,
                        child.updated_at = CURRENT_TIMESTAMP
                    WHERE child.status = 'WAITING_PARENT'
                      AND parent.status = 'SUCCESS'
                    """
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
                            child.finished_at = CURRENT_TIMESTAMP,
                            child.updated_at = CURRENT_TIMESTAMP
                        WHERE child.status = 'WAITING_PARENT'
                          AND parent.status IN ('FAILED', 'CANCELLED')
                        """
                    )
                    changed += cursor.rowcount
        return changed

    def lease_next_job(self, source_group, lease_seconds):
        lease_until = datetime.now() + timedelta(seconds=int(lease_seconds))
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM ak_request_jobs
                    WHERE source_group = %s
                      AND status = 'PENDING'
                      AND next_run_at <= CURRENT_TIMESTAMP
                    ORDER BY next_run_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (source_group,),
                )
                row = cursor.fetchone()
                if not row:
                    return None

                cursor.execute(
                    """
                    UPDATE ak_request_jobs
                    SET status = 'RUNNING',
                        attempt_count = attempt_count + 1,
                        started_at = CURRENT_TIMESTAMP,
                        lease_until = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (lease_until, row["id"]),
                )
                cursor.execute("SELECT * FROM ak_request_jobs WHERE id = %s", (row["id"],))
                return cursor.fetchone()

    def mark_success(self, job_id, result_type, result_json):
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
                        finished_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (result_type, result_json, job_id),
                )
                return cursor.rowcount

    def mark_retry(self, job_id, error_category, error_message, next_run_at):
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

    def mark_failed(self, job_id, error_category, error_message):
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE ak_request_jobs
                    SET status = 'FAILED',
                        error_category = %s,
                        error_message = %s,
                        lease_until = NULL,
                        finished_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (error_category, error_message, job_id),
                )
                return cursor.rowcount

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
