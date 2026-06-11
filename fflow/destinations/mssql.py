"""MSSQL destination connector.

Writes data to SQL Server using pyodbc with ``fast_executemany`` bulk inserts.

Write dispositions
------------------
append  — insert-only; rows accumulate in the destination table.
replace — TRUNCATE then insert in a single transaction.
merge   — upsert via a connection-scoped temporary staging table.
          CDC delete rows (``STTRCTRIGGER='D'``) are applied as pure deletes;
          ``STTRCID`` ordering ensures the latest CDC event wins for any given
          merge key.

Schema contract
---------------
evolve  — ALTER TABLE ADD COLUMN for new source columns (default).
freeze  — raise SchemaContractViolation on any schema change.
discard — silently drop source columns absent from the destination table.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

import pyodbc
from pydantic import BaseModel

from fflow.common.config import SchemaContract, StreamConfig
from fflow.common.exceptions import SchemaContractViolation
from fflow.common.schema import Column, Stream
from fflow.common.type_map import column_to_mssql_ddl

logger = logging.getLogger(__name__)

# CDC metadata columns produced by CacheSource.read().  Used for merge
# ordering and delete handling; must not be written to the destination table.
_CDC_COLS: frozenset[str] = frozenset({"STTRCID", "STTRCTRIGGER"})


class MSSQLConnectionConfig(BaseModel):
    connection_string: str
    dest_schema: str = "ODS"
    staging_schema: str = "ODS"  # reserved for future use; merge uses #temp tables


class MSSQLStreamConfig(BaseModel):
    target_table: str  # unqualified; destination prepends schema


def _q(identifier: str) -> str:
    """Bracket-quote an MSSQL identifier."""
    return f"[{identifier.replace(']', ']]')}]"


def _fqn(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


@dataclass
class _StreamBuffer:
    schema: Stream
    config: StreamConfig
    target_fqn: str
    target_schema: str
    target_table: str
    dest_columns: list[str]      # ordered list of columns written to target
    conn: pyodbc.Connection
    rows: list[dict] = field(default_factory=list)
    has_cdc: bool = False        # True if any buffered row has a non-null STTRCID


class MSSQLDestination:
    """Destination connector for SQL Server.

    Parameters
    ----------
    connection:
        MSSQL connection settings.
    streams:
        Per-stream write config keyed by stream name.
    contract:
        Schema-change policy applied during ``prepare_stream()``.
    """

    # TODO: Refactor MSSQLDestination to use SQLAlchemy instead of raw pyodbc.
    # The connection leak was patched in-place (close() + pop() in finally blocks).
    # Full refactor: extend SQLDestination base class, drop pyodbc dependency.
    # Blocked on: validating SQLAlchemy's fast_executemany support for MSSQL.

    def __init__(
        self,
        connection: MSSQLConnectionConfig,
        streams: dict[str, MSSQLStreamConfig],
        contract: SchemaContract = SchemaContract(),
    ) -> None:
        self._conn_cfg = connection
        self._stream_cfgs = streams
        self._contract = contract
        self._buffers: dict[str, _StreamBuffer] = {}

    # ------------------------------------------------------------------
    # Destination Protocol
    # ------------------------------------------------------------------

    def check(self) -> None:
        """Raise if the connection string is invalid or the server unreachable."""
        conn = pyodbc.connect(self._conn_cfg.connection_string)
        conn.close()

    def prepare_stream(self, stream: str, schema: Stream, config: StreamConfig, run_id: str = "") -> None:
        """Create/alter the destination table and initialise per-stream state.

        DDL operations run with ``autocommit=True`` so schema changes are
        durable even if a subsequent data write fails.  After DDL the
        connection switches to ``autocommit=False`` for the data transaction.
        """
        stream_cfg = self._stream_cfgs[stream]
        target_schema = self._conn_cfg.dest_schema
        target_table = stream_cfg.target_table
        target = _fqn(target_schema, target_table)

        conn: pyodbc.Connection = pyodbc.connect(self._conn_cfg.connection_string)
        conn.autocommit = True

        # Exclude CDC metadata columns — they must not land in the target table.
        source_cols = [c for c in schema.columns if c.name not in _CDC_COLS]
        existing_cols = self._get_existing_columns(conn, target_schema, target_table)

        if not existing_cols:
            col_defs = ",\n    ".join(
                f"{_q(c.name)} {column_to_mssql_ddl(c)} "
                f"{'NULL' if c.nullable else 'NOT NULL'}"
                for c in source_cols
            )
            conn.execute(f"CREATE TABLE {target} (\n    {col_defs}\n)")
            dest_columns = [c.name for c in source_cols]
        else:
            dest_columns = self._apply_contract(conn, target, source_cols, existing_cols)

        conn.autocommit = False

        self._buffers[stream] = _StreamBuffer(
            schema=schema,
            config=config,
            target_fqn=target,
            target_schema=target_schema,
            target_table=target_table,
            dest_columns=dest_columns,
            conn=conn,
        )

    def write(self, stream: str, rows: Iterable[dict]) -> None:
        """Buffer rows for *stream*; may be called multiple times per stream."""
        buf = self._buffers[stream]
        for row in rows:
            if row.get("STTRCID") is not None:
                buf.has_cdc = True
            buf.rows.append(row)

    def commit(self, stream: str) -> None:
        """Write buffered rows to the destination and commit the transaction."""
        buf = self._buffers[stream]
        try:
            disposition = buf.config.write_disposition
            if disposition == "append":
                self._commit_append(buf)
            elif disposition == "replace":
                self._commit_replace(buf)
            else:
                self._commit_merge(buf)
        except Exception:
            buf.conn.rollback()
            buf.rows.clear()
            buf.has_cdc = False
            raise
        finally:
            buf.conn.close()
            self._buffers.pop(stream, None)

    def rollback(self, stream: str) -> None:
        """Discard pending writes and roll back the transaction."""
        buf = self._buffers.pop(stream, None)
        if buf is None:
            return
        try:
            buf.conn.rollback()
        finally:
            buf.conn.close()
        buf.rows.clear()
        buf.has_cdc = False

    # ------------------------------------------------------------------
    # Commit strategies
    # ------------------------------------------------------------------

    def _commit_append(self, buf: _StreamBuffer) -> None:
        if buf.rows:
            self._bulk_insert(buf.conn, buf.target_fqn, buf.dest_columns, buf.rows)
        buf.conn.commit()
        buf.rows.clear()
        buf.has_cdc = False

    def _commit_replace(self, buf: _StreamBuffer) -> None:
        buf.conn.execute(f"TRUNCATE TABLE {buf.target_fqn}")
        if buf.rows:
            self._bulk_insert(buf.conn, buf.target_fqn, buf.dest_columns, buf.rows)
        buf.conn.commit()
        buf.rows.clear()
        buf.has_cdc = False

    def _commit_merge(self, buf: _StreamBuffer) -> None:
        if not buf.rows:
            buf.conn.commit()
            return

        merge_keys = buf.config.merge_key
        dest_cols = buf.dest_columns
        cursor_field = buf.schema.incremental.cursor_field

        # Build ORDER BY for dedup.
        # STTRCID wins for CDC streams (ensures latest event per key).
        # cursor_field is a secondary sort for non-CDC incremental streams.
        order_parts: list[str] = []
        if buf.has_cdc:
            order_parts.append("COALESCE([STTRCID], 0) DESC")
        if cursor_field and cursor_field in dest_cols:
            order_parts.append(f"{_q(cursor_field)} DESC")
        order_by = ", ".join(order_parts) if order_parts else "(SELECT NULL)"

        # Map source column names to DDL types for staging table creation.
        col_type_map = {
            c.name: column_to_mssql_ddl(c)
            for c in buf.schema.columns
            if c.name not in _CDC_COLS
        }

        # Staging temp table: dest columns + CDC metadata for ordering/delete logic.
        # Connection-scoped (#) so concurrent pipeline runs never collide.
        staging_col_defs = (
            [f"{_q(c)} {col_type_map[c]} NULL" for c in dest_cols]
            + ["[STTRCID] BIGINT NULL", "[STTRCTRIGGER] NVARCHAR(1) NULL"]
        )
        staging_cols = dest_cols + ["STTRCID", "STTRCTRIGGER"]

        buf.conn.execute(
            "IF OBJECT_ID('tempdb..#fflow_staging') IS NOT NULL "
            "DROP TABLE #fflow_staging"
        )
        buf.conn.execute(
            "CREATE TABLE #fflow_staging (\n    "
            + ",\n    ".join(staging_col_defs)
            + "\n)"
        )

        self._bulk_insert(buf.conn, "#fflow_staging", staging_cols, buf.rows)

        # Dedup: keep one row per merge key (latest event wins).
        partition = ", ".join(_q(k) for k in merge_keys)
        buf.conn.execute(
            f"WITH _deduped AS (\n"
            f"    SELECT *, ROW_NUMBER() OVER (\n"
            f"        PARTITION BY {partition}\n"
            f"        ORDER BY {order_by}\n"
            f"    ) AS _rn\n"
            f"    FROM #fflow_staging\n"
            f")\n"
            f"DELETE FROM _deduped WHERE _rn > 1"
        )

        # Delete from target for every merge key present in staging.
        # This covers both pure-delete CDC rows (D) and upsert candidates.
        join_cond = " AND ".join(
            f"t.{_q(k)} = s.{_q(k)}" for k in merge_keys
        )
        buf.conn.execute(
            f"DELETE t FROM {buf.target_fqn} t "
            f"INNER JOIN #fflow_staging s ON {join_cond}"
        )

        # Insert surviving (non-delete) rows into target; exclude CDC columns.
        col_list = ", ".join(_q(c) for c in dest_cols)
        buf.conn.execute(
            f"INSERT INTO {buf.target_fqn} ({col_list}) "
            f"SELECT {col_list} FROM #fflow_staging "
            f"WHERE [STTRCTRIGGER] != 'D' OR [STTRCTRIGGER] IS NULL"
        )

        buf.conn.execute("DROP TABLE #fflow_staging")
        buf.conn.commit()
        buf.rows.clear()
        buf.has_cdc = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _coerce_value(self, v: Any) -> Any:
        """JSON-serialize dict/list for NVARCHAR(MAX) JSON columns.

        Mirrors dlt's escape_mssql_literal behaviour for nested types.
        """
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return v

    def _bulk_insert(
        self,
        conn: pyodbc.Connection,
        table: str,
        columns: list[str],
        rows: list[dict],
    ) -> None:
        if not rows:
            return
        col_list = ", ".join(_q(c) for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        values = [[self._coerce_value(row.get(c)) for c in columns] for row in rows]
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(sql, values)

    def _get_existing_columns(
        self, conn: pyodbc.Connection, schema: str, table: str
    ) -> list[str]:
        """Return column names in ordinal order, or [] if the table does not exist."""
        cur = conn.execute(
            "SELECT c.name FROM sys.columns c "
            "JOIN sys.objects o ON o.object_id = c.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "WHERE o.name = ? AND s.name = ? ORDER BY c.column_id",
            (table, schema),
        )
        return [row[0] for row in cur.fetchall()]

    def _apply_contract(
        self,
        conn: pyodbc.Connection,
        target_fqn: str,
        source_cols: list[Column],
        existing_cols: list[str],
    ) -> list[str]:
        """Apply schema contract; return the ordered list of columns to write.

        Raises
        ------
        SchemaContractViolation
            When ``freeze`` is configured and any schema change is detected.
        """
        existing_set = set(existing_cols)
        source_names = [c.name for c in source_cols]
        source_set = set(source_names)

        # New columns: in source but not in target.
        new_cols = [c for c in source_cols if c.name not in existing_set]
        if new_cols:
            if self._contract.on_new_column == "freeze":
                raise SchemaContractViolation(
                    f"New columns detected in source: {[c.name for c in new_cols]}"
                )
            if self._contract.on_new_column == "evolve":
                for col in new_cols:
                    conn.execute(
                        f"ALTER TABLE {target_fqn} ADD {_q(col.name)} "
                        f"{column_to_mssql_ddl(col)} NULL"
                    )
                    logger.info(
                        "Schema evolved: added column %s to %s", col.name, target_fqn
                    )
            # discard: new cols excluded from dest_columns (see return below)

        # Dropped columns: in target but not in source.
        dropped_cols = [c for c in existing_cols if c not in source_set]
        if dropped_cols and self._contract.on_dropped_column == "freeze":
            raise SchemaContractViolation(
                f"Columns dropped from source: {dropped_cols}"
            )

        # discard: only write columns present in both source and target.
        if self._contract.on_new_column == "discard":
            return [c.name for c in source_cols if c.name in existing_set]
        return source_names
