"""Cache (InterSystems IRIS/Caché) source connector.

Two load modes:

Mirror mode    — set ``CacheStreamConfig.table``.  ``discover()`` introspects
                 INFORMATION_SCHEMA; ``read()`` builds CDC SQL automatically.

SQL-file mode  — set ``CacheStreamConfig.sql_file``.  The user provides a
                 ``.sql`` file whose result set is streamed to the destination.
                 Incremental cursor injection uses the ``{{cursor_value}}``
                 placeholder (see ADR-0009).
"""

from __future__ import annotations

import logging
import os
import random
import subprocess
import time
from pathlib import Path
from typing import Iterator, Optional

from pydantic import BaseModel, model_validator

from fflow.common.schema import (
    Column,
    IncrementalConfig,
    Schema,
    Stream,
)
from fflow.common.type_map import cache_type_to_column

logger = logging.getLogger(__name__)

_SHUTTLE_BATCH_SIZE = 100_000
_CURSOR_PLACEHOLDER = "{{cursor_value}}"
_ATTR_ERROR_MAX_RETRIES = 3

_COLUMNS_SQL = (
    "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
    "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION "
    "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? "
    "ORDER BY ORDINAL_POSITION"
)
_PK_SQL = (
    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
    "WHERE TABLE_NAME = ? AND CONSTRAINT_NAME = 'RowIDBasedIDKeyIndex'"
)


# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------


class CacheConnectionConfig(BaseModel):
    url: str
    user: str
    password: str
    jdbc_jar: str
    jdbc_driver: str = "com.intersystems.jdbc.IRISDriver"
    connect_attempts: int = 3
    connect_backoff: float = 1.0


class CacheStreamConfig(BaseModel):
    table: Optional[str] = None
    sql_file: Optional[str] = None
    incremental: IncrementalConfig = IncrementalConfig()
    use_shuttle: bool = False
    shuttle_target_table: Optional[str] = None
    chunk_size: int = 1000

    @model_validator(mode="after")
    def _validate(self) -> "CacheStreamConfig":
        if bool(self.table) == bool(self.sql_file):
            raise ValueError("Exactly one of 'table' or 'sql_file' must be set")
        if self.use_shuttle:
            if not self.table:
                raise ValueError("'use_shuttle' requires 'table'; not supported in SQL-file mode")
            if not self.shuttle_target_table:
                raise ValueError("'shuttle_target_table' is required when 'use_shuttle=True'")
        return self


# ---------------------------------------------------------------------------
# Internal connection wrapper
# ---------------------------------------------------------------------------


class _CacheConnection:
    """Wraps a jaydebeapi JDBC connection with exponential-backoff connect
    and AttributeError retry on first-batch fetch (Cache 2018.1 driver bug).
    """

    def __init__(self, config: CacheConnectionConfig) -> None:
        self._cfg = config
        self._conn = None

    def connect(self) -> "_CacheConnection":
        import jaydebeapi  # optional dep — imported here so unit tests can stub

        last_exc: Optional[Exception] = None
        for attempt in range(1, self._cfg.connect_attempts + 1):
            try:
                logger.info(
                    "CacheSource: connecting (attempt %d/%d) to %s",
                    attempt, self._cfg.connect_attempts, self._cfg.url,
                )
                conn = jaydebeapi.connect(
                    jclassname=self._cfg.jdbc_driver,
                    url=self._cfg.url,
                    driver_args=[self._cfg.user, self._cfg.password],
                    jars=[self._cfg.jdbc_jar],
                )
                cur = conn.cursor()
                try:
                    cur.execute("SELECT 1")
                    try:
                        cur.fetchone()
                    except AttributeError:
                        pass  # known driver bug on ping; connection still usable
                finally:
                    cur.close()
                self._conn = conn
                logger.info("CacheSource: connected OK")
                return self
            except Exception as exc:
                last_exc = exc
                logger.warning("CacheSource: connect attempt %d failed: %s", attempt, exc)
                if self._conn:
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                self._conn = None
                if attempt < self._cfg.connect_attempts:
                    sleep = self._cfg.connect_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                    time.sleep(sleep)
        raise RuntimeError(
            f"CacheSource: all {self._cfg.connect_attempts} connect attempts failed"
        ) from last_exc

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _cursor(self):
        return self._conn.cursor()

    def execute_one(self, sql: str, params=None):
        cur = self._cursor()
        try:
            cur.execute(sql, params) if params else cur.execute(sql)
            return cur.fetchone()
        finally:
            cur.close()

    def execute_all(self, sql: str, params=None):
        cur = self._cursor()
        try:
            cur.execute(sql, params) if params else cur.execute(sql)
            return cur.fetchall() or []
        finally:
            cur.close()

    def get_col_names(self, sql: str) -> list[str]:
        """Return column names for *sql* by executing a zero-row wrapper."""
        wrapper = f"SELECT * FROM ({sql}) _q WHERE 1=0"
        cur = self._cursor()
        try:
            cur.execute(wrapper)
            try:
                return [d[0] for d in (cur.description or [])]
            except AttributeError:
                time.sleep(0.5)
                cur2 = self._cursor()
                try:
                    cur2.execute(wrapper)
                    return [d[0] for d in (cur2.description or [])]
                finally:
                    cur2.close()
        finally:
            cur.close()

    def fetch_iter(self, sql: str, chunk_size: int) -> Iterator[tuple[list, list[str]]]:
        """Execute *sql* and yield ``(rows, col_names)`` chunks.

        Retries up to ``_ATTR_ERROR_MAX_RETRIES`` times on the first batch only
        to handle the Cache 2018.1 JDBC ``AttributeError`` on
        ``cursor.description``.  After the first batch succeeds, subsequent
        batches are not retried.
        """
        cur = None
        first_rows: list = []
        col_names: list[str] = []

        for attempt in range(1, _ATTR_ERROR_MAX_RETRIES + 1):
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            cur = self._cursor()
            try:
                cur.execute(sql)
                first_rows = cur.fetchmany(chunk_size)
                if not first_rows:
                    cur.close()
                    return
                col_names = [d[0] for d in (cur.description or [])]
                break  # first batch + col names acquired — proceed to yield
            except AttributeError:
                if attempt < _ATTR_ERROR_MAX_RETRIES:
                    logger.warning(
                        "CacheSource: AttributeError on first batch (attempt %d/%d); retrying",
                        attempt, _ATTR_ERROR_MAX_RETRIES,
                    )
                    time.sleep(0.5 * attempt)
                    continue
                try:
                    cur.close()
                except Exception:
                    pass
                raise
        else:
            raise RuntimeError("CacheSource: fetch_iter exhausted retries on AttributeError")

        try:
            yield first_rows, col_names
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    return
                yield rows, col_names
        finally:
            try:
                cur.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# SQL builders
# ---------------------------------------------------------------------------


def _build_incremental_sql(
    cache_table: str,
    col_names: list[str],
    pk_col: str,
    start_cid: int,
    cid_column: str = "STTRCID",
    trigger_column: str = "STTRCTRIGGER",
) -> str:
    """UNION ALL CDC query for mirror-mode incremental loads.

    Ported from ``artiva_etl/etl/loader.py:481``.  ``start_cid`` is embedded
    as a literal integer — no JDBC parameters — because Cache 2018.1 has a
    known JDBC parameter bug on complex queries.
    """
    ui_cols = ", ".join(f"{cache_table}.{c}" for c in col_names)

    d_parts = []
    for c in col_names:
        if c == pk_col:
            d_parts.append(f"STTRCKEY AS {c}")
        else:
            d_parts.append(f"NULL AS {c}")
    d_cols = ", ".join(d_parts)

    return (
        f"SELECT {ui_cols}, sttrc.{cid_column}, 'U' AS {trigger_column}\n"
        f"FROM {cache_table}\n"
        f"JOIN (\n"
        f"      SELECT MAX({cid_column}) AS {cid_column}, STTRCKEY\n"
        f"      FROM STTRACKCHANGE\n"
        f"      WHERE STTRCTABLE = '{cache_table}'\n"
        f"        AND {cid_column} >= {start_cid}\n"
        f"        AND {trigger_column} != 'D'\n"
        f"      GROUP BY STTRCKEY\n"
        f"     ) sttrc\n"
        f"ON sttrc.STTRCKEY = {pk_col}\n"
        f"\nUNION ALL\n\n"
        f"SELECT {d_cols}, MAX({cid_column}) AS {cid_column}, 'D' AS {trigger_column}\n"
        f"FROM STTRACKCHANGE\n"
        f"WHERE STTRCTABLE = '{cache_table}'\n"
        f"  AND {trigger_column} = 'D'\n"
        f"  AND {cid_column} >= {start_cid}\n"
        f"GROUP BY STTRCKEY"
    )


def _run_shuttle(
    *,
    shuttle_jar: str,
    cache_url: str,
    cache_user: str,
    cache_password: str,
    sql: str,
    target_table: str,
) -> None:
    """Invoke ``cache-shuttle.jar`` for a first-run bulk snapshot.

    Credentials are passed via env vars — the JAR reads them at startup.
    Raises ``RuntimeError`` on non-zero exit.
    """
    if not shuttle_jar or not os.path.isfile(shuttle_jar):
        raise RuntimeError(
            f"Shuttle JAR not found at '{shuttle_jar}'. "
            "Set shuttle_jar on CacheSource or build cache-shuttle."
        )

    mssql_jdbc_url = os.environ.get("SHUTTLE_MSSQL_JDBC_URL") or os.environ.get("MSSQL_JDBC_URL")
    mssql_user = os.environ.get("MSSQL_USER")
    mssql_password = os.environ.get("MSSQL_PASSWORD")
    if not mssql_jdbc_url or not mssql_user or not mssql_password:
        raise RuntimeError(
            "Shuttle requires SHUTTLE_MSSQL_JDBC_URL (or MSSQL_JDBC_URL), "
            "MSSQL_USER, and MSSQL_PASSWORD env vars."
        )

    env = os.environ.copy()
    env["CACHE_JDBC_URL"] = cache_url
    env["CACHE_USER"] = cache_user
    env["CACHE_PASSWORD"] = cache_password
    env["MSSQL_JDBC_URL"] = mssql_jdbc_url
    env["MSSQL_USER"] = mssql_user
    env["MSSQL_PASSWORD"] = mssql_password

    cmd = [
        "java", "-jar", shuttle_jar,
        "--sql", sql,
        "--target-table", target_table,
        "--batch-size", str(_SHUTTLE_BATCH_SIZE),
    ]
    logger.info("CacheSource: shuttle starting -> target=%s", target_table)
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Shuttle exited with code {result.returncode}")
    logger.info("CacheSource: shuttle completed -> target=%s", target_table)


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


class CacheSource:
    """InterSystems Cache/IRIS source connector.

    Parameters
    ----------
    conn_config:
        JDBC connection parameters.
    stream_configs:
        Mapping of stream name → ``CacheStreamConfig``.
    shuttle_jar:
        Path to ``cache-shuttle.jar``.  Required only for streams with
        ``use_shuttle=True``.
    """

    def __init__(
        self,
        conn_config: CacheConnectionConfig,
        stream_configs: dict[str, CacheStreamConfig],
        shuttle_jar: Optional[str] = None,
    ) -> None:
        self._conn_cfg = conn_config
        self._stream_cfgs = stream_configs
        self._shuttle_jar = shuttle_jar
        self._schema: Optional[Schema] = None

    def _new_conn(self) -> _CacheConnection:
        return _CacheConnection(self._conn_cfg).connect()

    # --- Source protocol ---

    def check(self) -> None:
        """Raise if the JDBC connection cannot be established."""
        conn = self._new_conn()
        conn.close()

    def discover(self) -> Schema:
        """Return the schema for all configured streams.

        Result is cached — subsequent calls return the same ``Schema`` without
        reconnecting.
        """
        if self._schema is not None:
            return self._schema
        conn = self._new_conn()
        try:
            streams = []
            for name, cfg in self._stream_cfgs.items():
                if cfg.table:
                    stream = self._discover_mirror(conn, name, cfg)
                else:
                    stream = self._discover_sql_file(conn, name, cfg)
                streams.append(stream)
        finally:
            conn.close()
        self._schema = Schema(streams=streams)
        return self._schema

    def read(self, stream: str, state: dict) -> Iterator[dict]:
        """Yield rows for *stream*, updating *state* in-place as rows flow."""
        cfg = self._stream_cfgs.get(stream)
        if cfg is None:
            raise ValueError(f"CacheSource: unknown stream '{stream}'")
        if cfg.table:
            return self._read_mirror(stream, cfg, state)
        return self._read_sql_file(stream, cfg, state)

    # --- Discovery helpers ---

    def _discover_mirror(
        self, conn: _CacheConnection, name: str, cfg: CacheStreamConfig
    ) -> Stream:
        col_rows = conn.execute_all(_COLUMNS_SQL, [cfg.table])
        pk_row = conn.execute_one(_PK_SQL, [cfg.table])
        pk_col = pk_row[0] if pk_row else None

        columns = []
        for row in col_rows:
            col_name, data_type, max_len, precision, scale, is_nullable, _ = row
            columns.append(
                cache_type_to_column(
                    name=col_name,
                    data_type=data_type,
                    precision=precision,
                    scale=scale,
                    max_length=max_len,
                    nullable=(is_nullable == "YES"),
                    primary_key=(col_name == pk_col),
                )
            )
        return Stream(name=name, columns=columns, incremental=cfg.incremental)

    def _discover_sql_file(
        self, conn: _CacheConnection, name: str, cfg: CacheStreamConfig
    ) -> Stream:
        sql_text = Path(cfg.sql_file).read_text()
        if _CURSOR_PLACEHOLDER in sql_text:
            sql_text = sql_text.replace(_CURSOR_PLACEHOLDER, "0")
        col_names = conn.get_col_names(sql_text)
        columns = [Column(name=c) for c in col_names]
        return Stream(name=name, columns=columns, incremental=cfg.incremental)

    # --- Read helpers ---

    def _get_table_meta(
        self, conn: _CacheConnection, table: str
    ) -> tuple[list[str], Optional[str]]:
        """Return ``(col_names, pk_col_or_None)`` for *table*."""
        col_rows = conn.execute_all(_COLUMNS_SQL, [table])
        pk_row = conn.execute_one(_PK_SQL, [table])
        col_names = [row[0] for row in col_rows]
        pk_col = pk_row[0] if pk_row else None
        return col_names, pk_col

    def _read_mirror(
        self, stream_name: str, cfg: CacheStreamConfig, state: dict
    ) -> Iterator[dict]:
        is_incr = cfg.incremental.cursor_type != "none"
        cursor_field: Optional[str] = cfg.incremental.cursor_field if is_incr else None
        current_cursor = state.get(cursor_field) if cursor_field else None

        conn = self._new_conn()
        try:
            col_names, pk_col = self._get_table_meta(conn, cfg.table)

            # ── First run + shuttle ────────────────────────────────────────
            if is_incr and current_cursor is None and cfg.use_shuttle:
                pre_max_row = conn.execute_one(
                    f"SELECT MAX({cursor_field}) FROM STTRACKCHANGE WHERE STTRCTABLE = ?",
                    [cfg.table],
                )
                max_cid = int(pre_max_row[0]) if pre_max_row and pre_max_row[0] is not None else 0
                logger.info(
                    "CacheSource: %s shuttle snapshot -> pre_max_%s=%s target=%s",
                    stream_name, cursor_field, max_cid, cfg.shuttle_target_table,
                )
                select_sql = f"SELECT {', '.join(col_names)} FROM {cfg.table}"
                _run_shuttle(
                    shuttle_jar=self._shuttle_jar,
                    cache_url=self._conn_cfg.url,
                    cache_user=self._conn_cfg.user,
                    cache_password=self._conn_cfg.password,
                    sql=select_sql,
                    target_table=cfg.shuttle_target_table,
                )
                state[cursor_field] = max_cid
                return  # 0 rows yielded

            # ── First run, full SELECT ─────────────────────────────────────
            if is_incr and current_cursor is None:
                pre_max_row = conn.execute_one(
                    f"SELECT MAX({cursor_field}) FROM STTRACKCHANGE WHERE STTRCTABLE = ?",
                    [cfg.table],
                )
                pre_max_cid = int(pre_max_row[0]) if pre_max_row and pre_max_row[0] is not None else 0
                logger.info(
                    "CacheSource: %s full load (first run) -> pre_max_%s=%s",
                    stream_name, cursor_field, pre_max_cid,
                )
                sql = f"SELECT {', '.join(col_names)} FROM {cfg.table}"
                for rows, col_hdrs in conn.fetch_iter(sql, cfg.chunk_size):
                    for row_tuple in rows:
                        yield dict(zip(col_hdrs, row_tuple))
                state[cursor_field] = pre_max_cid
                return

            # ── Incremental CDC run ────────────────────────────────────────
            if is_incr and current_cursor is not None:
                effective_pk = pk_col or col_names[0]
                sql = _build_incremental_sql(
                    cache_table=cfg.table,
                    col_names=col_names,
                    pk_col=effective_pk,
                    start_cid=int(current_cursor),
                    cid_column=cursor_field,
                )
                logger.info(
                    "CacheSource: %s incremental -> %s >= %s",
                    stream_name, cursor_field, current_cursor,
                )
                max_seen: Optional[int] = None
                for rows, col_hdrs in conn.fetch_iter(sql, cfg.chunk_size):
                    for row_tuple in rows:
                        row = dict(zip(col_hdrs, row_tuple))
                        cid_val = row.get(cursor_field)
                        if cid_val is not None:
                            cid_int = int(cid_val)
                            max_seen = cid_int if max_seen is None else max(max_seen, cid_int)
                        yield row
                if max_seen is not None:
                    state[cursor_field] = max_seen
                return

            # ── Full refresh (cursor_type="none") ──────────────────────────
            sql = f"SELECT {', '.join(col_names)} FROM {cfg.table}"
            for rows, col_hdrs in conn.fetch_iter(sql, cfg.chunk_size):
                for row_tuple in rows:
                    yield dict(zip(col_hdrs, row_tuple))

        finally:
            conn.close()

    def _read_sql_file(
        self, stream_name: str, cfg: CacheStreamConfig, state: dict
    ) -> Iterator[dict]:
        is_incr = cfg.incremental.cursor_type != "none"
        cursor_field: Optional[str] = cfg.incremental.cursor_field if is_incr else None
        current_cursor = state.get(cursor_field) if cursor_field else None

        sql_text = Path(cfg.sql_file).read_text()

        if is_incr and current_cursor is not None:
            if _CURSOR_PLACEHOLDER not in sql_text:
                raise ValueError(
                    f"CacheSource: stream '{stream_name}' has incremental set but "
                    f"'{cfg.sql_file}' does not contain '{_CURSOR_PLACEHOLDER}'. "
                    "Add the placeholder where the cursor filter belongs (ADR-0009)."
                )
            sql_text = sql_text.replace(_CURSOR_PLACEHOLDER, str(int(current_cursor)))
            logger.info(
                "CacheSource: %s SQL-file incremental -> %s = %s",
                stream_name, cursor_field, current_cursor,
            )
        elif _CURSOR_PLACEHOLDER in sql_text:
            # First run — substitute 0 to load all rows
            sql_text = sql_text.replace(_CURSOR_PLACEHOLDER, "0")
            logger.info("CacheSource: %s SQL-file first run -> substituting cursor=0", stream_name)

        conn = self._new_conn()
        try:
            max_seen: Optional[int] = None
            for rows, col_names in conn.fetch_iter(sql_text, cfg.chunk_size):
                for row_tuple in rows:
                    row = dict(zip(col_names, row_tuple))
                    if is_incr and cursor_field:
                        cid_val = row.get(cursor_field)
                        if cid_val is not None:
                            cid_int = int(cid_val)
                            max_seen = cid_int if max_seen is None else max(max_seen, cid_int)
                    yield row
            if is_incr and cursor_field and max_seen is not None:
                state[cursor_field] = max_seen
        finally:
            conn.close()
