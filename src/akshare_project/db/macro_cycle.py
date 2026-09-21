"""Append-only official release vintages, separate from daily market wide tables."""

from datetime import datetime
from decimal import Decimal
import hashlib

import aiomysql


def vintage_identity(release, latest, *, force_new=False):
    if latest and latest["content_hash"] == release["content_hash"] and not force_new:
        return latest["release_id"], latest["revision_number"], False
    revision = int(latest["revision_number"] if latest else 0) + 1
    key = f"{release['source_key']}:{release['content_hash']}:{revision}"
    return hashlib.sha256(key.encode()).hexdigest(), revision, True


class MacroCycleStore:
    def __init__(self, db):
        self.db = db

    async def ensure_tables(self):
        await self.db.init_pool()
        statements = (
            """CREATE TABLE IF NOT EXISTS cn_macro_release (
                release_id CHAR(64) PRIMARY KEY, source_key CHAR(64) NOT NULL,
                content_hash CHAR(64) NOT NULL, title VARCHAR(512) NOT NULL,
                category VARCHAR(32) NOT NULL, source_url VARCHAR(1500) NOT NULL,
                published_at DATETIME NULL, available_at DATETIME NOT NULL,
                replay_available_at DATETIME NOT NULL,
                publication_precision VARCHAR(16) NOT NULL, replay_eligible BOOLEAN NOT NULL,
                vintage_status VARCHAR(40) NOT NULL, revision_number INT NOT NULL,
                first_seen_at DATETIME NOT NULL, last_seen_at DATETIME NOT NULL,
                raw_response LONGTEXT NOT NULL, parser_version VARCHAR(40) NOT NULL,
                KEY ix_macro_source (source_key, revision_number),
                KEY ix_macro_available (available_at),
                KEY ix_macro_replay (replay_available_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
            """CREATE TABLE IF NOT EXISTS cn_macro_observation (
                id BIGINT AUTO_INCREMENT PRIMARY KEY, release_id CHAR(64) NOT NULL,
                series_key VARCHAR(80) NOT NULL, period_end DATE NOT NULL,
                period_kind VARCHAR(16) NOT NULL, basis_version VARCHAR(40) NOT NULL,
                value DECIMAL(30,8) NOT NULL, unit VARCHAR(20) NOT NULL,
                raw_value TEXT NOT NULL, raw_unit VARCHAR(20) NOT NULL,
                observed_at DATETIME NOT NULL,
                UNIQUE KEY uk_macro_obs (release_id,series_key,period_end,period_kind,basis_version),
                KEY ix_macro_series (series_key,period_end),
                CONSTRAINT fk_macro_release FOREIGN KEY (release_id)
                    REFERENCES cn_macro_release(release_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
            """CREATE TABLE IF NOT EXISTS cn_macro_collection_checkpoint (
                source_key CHAR(64) PRIMARY KEY, source_url VARCHAR(1500) NOT NULL,
                status VARCHAR(16) NOT NULL, checked_at DATETIME NOT NULL,
                error_message TEXT NULL, parser_version VARCHAR(40) NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        )
        async with self.db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for sql in statements:
                    await cursor.execute(sql)
                await cursor.execute("SHOW COLUMNS FROM cn_macro_observation LIKE 'observed_at'")
                if not await cursor.fetchone():
                    await cursor.execute("ALTER TABLE cn_macro_observation ADD COLUMN observed_at DATETIME NULL")
                    await cursor.execute("UPDATE cn_macro_observation o JOIN cn_macro_release r USING(release_id) SET o.observed_at=r.first_seen_at WHERE o.observed_at IS NULL")
            await conn.commit()

    async def save(self, release, observations):
        async with self.db.pool.acquire() as conn:
            try:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT GET_LOCK('cn_macro_release_writer', 20) AS locked")
                    if (await cursor.fetchone())["locked"] != 1:
                        raise RuntimeError("Macro release writer is busy")
                    await cursor.execute("SELECT release_id,content_hash,revision_number,parser_version FROM cn_macro_release WHERE source_key=%s ORDER BY revision_number DESC LIMIT 1", (release["source_key"],))
                    latest = await cursor.fetchone()
                    release_id, revision, is_new = vintage_identity(release, latest)
                    parser_correction = False
                    existing = {}
                    if not is_new:
                        await cursor.execute("SELECT series_key,period_end,period_kind,basis_version,value FROM cn_macro_observation WHERE release_id=%s", (release_id,))
                        existing = {(r['series_key'], str(r['period_end']), r['period_kind'], r['basis_version']): r['value'] for r in await cursor.fetchall()}
                        for row in observations:
                            key = tuple(str(row[k]) for k in ('series_key', 'period_end', 'period_kind', 'basis_version'))
                            if key in existing and Decimal(str(existing[key])) != Decimal(row['value']):
                                if latest['parser_version'] == release['parser_version']:
                                    raise ValueError(f"Same parser produced conflicting observation: {key}")
                                parser_correction = True
                        if parser_correction:
                            release_id, revision, is_new = vintage_identity(release, latest, force_new=True)
                    release = {**release, "release_id": release_id}
                    observations = [{**row, "release_id": release_id} for row in observations]
                    if not is_new:
                        await cursor.execute("UPDATE cn_macro_release SET last_seen_at=%s WHERE release_id=%s", (release["fetched_at"], release["release_id"]))
                        inserted = 0
                        for row in observations:
                            key = tuple(str(row[k]) for k in ('series_key', 'period_end', 'period_kind', 'basis_version'))
                            if key in existing:
                                if Decimal(str(existing[key])) != Decimal(row['value']):
                                    raise ValueError(f"Reparse conflicts with retained official observation: {key}")
                                continue
                            await cursor.execute("""INSERT INTO cn_macro_observation
                                (release_id,series_key,period_end,period_kind,basis_version,value,unit,raw_value,raw_unit,observed_at)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                tuple(row[k] for k in ("release_id", "series_key", "period_end", "period_kind", "basis_version", "value", "unit", "raw_value", "raw_unit")) + (release["fetched_at"],))
                            inserted += cursor.rowcount
                        await conn.commit()
                        return inserted
                    # An edited old URL is a new vintage, never retroactively visible.
                    available = max(release["available_at"], release["fetched_at"]) if revision > 1 else release["available_at"]
                    replay_at = max(available, release["fetched_at"])
                    await cursor.execute("""INSERT INTO cn_macro_release
                        (release_id,source_key,content_hash,title,category,source_url,published_at,
                         available_at,replay_available_at,publication_precision,replay_eligible,
                         vintage_status,revision_number,first_seen_at,last_seen_at,raw_response,parser_version)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (release["release_id"], release["source_key"], release["content_hash"],
                         release["title"], release["category"], release["source_url"], release["published_at"],
                         available, replay_at, release["publication_precision"], release["replay_eligible"],
                         "parser_correction" if parser_correction else ("observed_revision" if revision > 1 else "first_observed_archive"), revision,
                         release["fetched_at"], release["fetched_at"], release["raw_response"], release["parser_version"]))
                    await cursor.executemany("""INSERT INTO cn_macro_observation
                        (release_id,series_key,period_end,period_kind,basis_version,value,unit,raw_value,raw_unit,observed_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [tuple(row[k] for k in ("release_id", "series_key", "period_end", "period_kind", "basis_version", "value", "unit", "raw_value", "raw_unit")) + (release["fetched_at"],) for row in observations])
                    await conn.commit()
                    return len(observations)
            except Exception:
                await conn.rollback()
                raise
            finally:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT RELEASE_LOCK('cn_macro_release_writer')")

    async def checkpoint(self, url, status, error=None, parser_version=None):
        async with self.db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""INSERT INTO cn_macro_collection_checkpoint
                    (source_key,source_url,status,checked_at,error_message,parser_version)
                    VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
                    status=VALUES(status),checked_at=VALUES(checked_at),error_message=VALUES(error_message),
                    parser_version=VALUES(parser_version)""",
                    (hashlib.sha256(url.encode()).hexdigest(), url, status, datetime.now(), error, parser_version))
            await conn.commit()

    async def completed_urls(self, parser_version):
        async with self.db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT source_url FROM cn_macro_collection_checkpoint WHERE status='success' AND parser_version=%s", (parser_version,))
                return {row[0] for row in await cursor.fetchall()}

    async def coverage(self):
        async with self.db.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""SELECT o.series_key,o.period_kind,o.basis_version,o.unit,
                    MIN(o.period_end) AS first_period,MAX(o.period_end) AS last_period,
                    COUNT(DISTINCT o.period_end) AS periods,COUNT(*) AS vintage_observations,
                    MIN(r.replay_available_at) AS strict_history_available_from
                    FROM cn_macro_observation o JOIN cn_macro_release r USING(release_id)
                    GROUP BY o.series_key,o.period_kind,o.basis_version,o.unit ORDER BY o.series_key,o.period_kind""")
                return list(await cursor.fetchall())
