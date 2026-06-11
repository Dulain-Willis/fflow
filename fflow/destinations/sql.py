"""Generic SQLAlchemy-backed destination base class.

Subclasses inherit ``check()``, ``prepare_stream()`` (DDL), and
``_commit_append()`` for free.  They must implement the abstract methods
``_commit_replace()`` and ``_commit_merge()``.

Write disposition dispatch::

    append  → _commit_append()   (generic; subclasses may override)
    replace → _commit_replace()  (abstract; subclass owns)
    merge   → _commit_merge()    (abstract; subclass owns)

Design:
- DDL runs in a short-lived autocommit connection so schema changes are
  durable even if a subsequent data write fails (same pattern as MSSQL).
- Data operations use a per-stream connection stored in the buffer; the
  connection lives until commit() or rollback() closes it.
- Subclasses may access the underlying DBAPI connection via
  ``buf.conn.connection`` for driver-specific features (e.g. fast_executemany).
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from pydantic import BaseModel, model_validator
from sqlalchemy import MetaData, Table, create_engine, inspect, text
from sqlalchemy.exc import NoSuchTableError

from fflow.common.config import SchemaContract, StreamConfig
from fflow.common.exceptions import SchemaContractViolation
from fflow.common.schema import Column, Stream
from fflow.common.type_map import column_to_generic_sql_ddl

logger = logging.getLogger(__name__)


class SQLConnectionConfig(BaseModel):
    connection_url: str
    dest_schema: str = "public"
    staging_schema: str = ""

    @model_validator(mode="after")
    def _default_staging_schema(self) -> "SQLConnectionConfig":
        if not self.staging_schema:
            self.staging_schema = f"{self.dest_schema}_staging"
        return self


@dataclass
class _SQLStreamBuffer:
    schema: Stream
    config: StreamConfig
    run_id: str
    target_schema: str
    staging_schema: str
    target_table: str
    dest_columns: list[str]
    conn: object  # SQLAlchemy Connection (kept open until commit/rollback)
    rows: list[dict] = field(default_factory=list)


class SQLDestination(ABC):
    """Abstract SQLAlchemy-backed destination.

    Parameters
    ----------
    connection:
        Connection settings including ``connection_url`` (SQLAlchemy URL).
    contract:
        Schema-change policy applied during ``prepare_stream()``.
    """

    def __init__(
        self,
        connection: SQLConnectionConfig,
        contract: SchemaContract = SchemaContract(),
    ) -> None:
        self._conn_cfg = connection
        self._contract = contract
        self._engine = create_engine(connection.connection_url)
        self._buffers: dict[str, _SQLStreamBuffer] = {}

    # ------------------------------------------------------------------
    # Destination Protocol
    # ------------------------------------------------------------------

    def check(self) -> None:
        """Raise if the database is unreachable."""
        with self._engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    def prepare_stream(
        self,
        stream: str,
        schema: Stream,
        config: StreamConfig,
        run_id: str = "",
    ) -> None:
        """Create/alter the destination table; open the per-stream data connection."""
        target_schema = self._conn_cfg.dest_schema
        staging_schema = self._conn_cfg.staging_schema
        target_table = self._get_target_table(stream, config)
        target_fqn = self._fqn(target_schema, target_table)

        source_cols = self._filter_source_cols(schema.columns)

        with self._engine.execution_options(
            isolation_level="AUTOCOMMIT"
        ).connect() as ddl_conn:
            self._ensure_schema(ddl_conn, target_schema)
            existing_cols = self._reflect_table(ddl_conn, target_schema, target_table)
            if existing_cols is None:
                col_defs = self._build_col_defs(source_cols)
                ddl_conn.execute(
                    text(f"CREATE TABLE {target_fqn} (\n    {col_defs}\n)")
                )
                dest_columns = [c.name for c in source_cols]
            else:
                dest_columns = self._apply_contract(
                    ddl_conn, target_fqn, source_cols, existing_cols
                )

            # For merge streams, mirror dlt's staging dataset pattern:
            # - Persistent staging schema; staging table is created once and evolved.
            # - New columns are ALTERed in (same as dlt's _execute_schema_update_sql).
            # - Staging table is TRUNCATED here (before data is loaded) so _commit_merge
            #   never needs to truncate. Type changes are not handled automatically —
            #   they will fail at INSERT time, matching dlt's behaviour.
            if config.write_disposition == "merge":
                self._ensure_schema(ddl_conn, staging_schema)
                staging_fqn = self._fqn(staging_schema, target_table)
                existing_staging = self._reflect_table(ddl_conn, staging_schema, target_table)
                if existing_staging is None:
                    col_defs = self._build_col_defs(source_cols)
                    ddl_conn.execute(text(f"CREATE TABLE {staging_fqn} (\n    {col_defs}\n)"))
                else:
                    # Add any new columns (dlt: ALTER TABLE ADD COLUMN for each).
                    existing_set = set(existing_staging)
                    new_cols = [c for c in source_cols if c.name not in existing_set]
                    for col in new_cols:
                        ddl_conn.execute(
                            text(
                                f"ALTER TABLE {staging_fqn} ADD "
                                f"{self._quote(col.name)} {self._col_ddl(col)} NULL"
                            )
                        )
                    # Truncate before loading so each run starts with an empty staging table.
                    ddl_conn.execute(text(f"TRUNCATE TABLE {staging_fqn}"))

        data_conn = self._engine.connect()
        self._buffers[stream] = _SQLStreamBuffer(
            schema=schema,
            config=config,
            run_id=run_id,
            target_schema=target_schema,
            staging_schema=staging_schema,
            target_table=target_table,
            dest_columns=dest_columns,
            conn=data_conn,
        )

    def write(self, stream: str, rows: Iterable[dict]) -> None:
        buf = self._buffers[stream]
        buf.rows.extend(rows)

    def commit(self, stream: str) -> None:
        """Dispatch to the appropriate write strategy and commit."""
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
            raise
        finally:
            buf.conn.close()
            self._buffers.pop(stream, None)

    def rollback(self, stream: str) -> None:
        buf = self._buffers.get(stream)
        if buf is None:
            return
        try:
            buf.conn.rollback()
        finally:
            buf.conn.close()
            self._buffers.pop(stream, None)
        buf.rows.clear()

    # ------------------------------------------------------------------
    # Shared commit strategy
    # ------------------------------------------------------------------

    def _commit_append(self, buf: _SQLStreamBuffer) -> None:
        if buf.rows:
            target_fqn = self._fqn(buf.target_schema, buf.target_table)
            self._bulk_insert(buf.conn, target_fqn, buf.dest_columns, buf.rows)
        buf.conn.commit()
        buf.rows.clear()

    # ------------------------------------------------------------------
    # Abstract commit strategies (dialect-specific)
    # ------------------------------------------------------------------

    @abstractmethod
    def _commit_replace(self, buf: _SQLStreamBuffer) -> None: ...

    def _commit_merge(self, buf: _SQLStreamBuffer) -> None:
        """Merge via persistent staging table (mirrors dlt's staging dataset pattern).

        Flow: load rows into staging (already truncated by prepare_stream) →
        DELETE matching keys from target → INSERT from staging into target.
        """
        if not buf.rows:
            buf.conn.commit()
            return

        merge_keys = buf.config.merge_key
        if not merge_keys:
            raise ValueError("merge_key required for write_disposition='merge'")

        target = self._fqn(buf.target_schema, buf.target_table)
        staging = self._fqn(buf.staging_schema, buf.target_table)

        self._load_staging(buf, staging)

        join_cond = " AND ".join(f't."{k}" = s."{k}"' for k in merge_keys)
        buf.conn.execute(text(f"DELETE FROM {target} t USING {staging} s WHERE {join_cond}"))

        col_list = ", ".join(f'"{c}"' for c in buf.dest_columns)
        buf.conn.execute(text(
            f"INSERT INTO {target} ({col_list}) SELECT {col_list} FROM {staging}"
        ))

        buf.conn.commit()
        buf.rows.clear()

    # ------------------------------------------------------------------
    # DDL helpers
    # ------------------------------------------------------------------

    def _fqn(self, schema: str, table: str) -> str:
        """Return a quoted fully-qualified name.  Subclasses may override."""
        return f'"{schema}"."{table}"'

    def _filter_source_cols(self, columns: list[Column]) -> list[Column]:
        """Strip any source-side metadata columns before DDL/writes.

        Subclasses override to exclude CDC columns (e.g. STTRCID).
        """
        return list(columns)

    def _get_target_table(self, stream: str, config: StreamConfig) -> str:
        """Return the destination table name for *stream*.

        Default: stream name.  Subclasses override when stream configs carry
        an explicit ``target_table`` field.
        """
        return stream

    def _build_col_defs(self, cols: list[Column]) -> str:
        return ",\n    ".join(
            f"{self._quote(c.name)} {self._col_ddl(c)} "
            f"{'NULL' if c.nullable else 'NOT NULL'}"
            for c in cols
        )

    def _col_ddl(self, col: Column) -> str:
        """Return the DDL type fragment for *col*.  Default uses generic ANSI SQL types."""
        return column_to_generic_sql_ddl(col)

    def _quote(self, identifier: str) -> str:
        """Double-quote an identifier.  Subclasses override for dialect."""
        return f'"{identifier}"'

    def _ensure_schema(self, conn: object, schema: str) -> None:
        """Create schema if it does not exist. Subclasses may override for dialect."""
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self._quote(schema)}"))

    def _load_staging(self, buf: _SQLStreamBuffer, staging_fqn: str) -> None:
        """Populate the staging table. Default: plain INSERT. Override for COPY strategies."""
        self._bulk_insert(buf.conn, staging_fqn, buf.dest_columns, buf.rows)

    def _reflect_table(
        self, conn: object, schema: str, table: str
    ) -> list[str] | None:
        """Return existing column names for *(schema, table)*, or None if absent."""
        insp = inspect(conn)
        try:
            cols = insp.get_columns(table, schema=schema)
            return [c["name"] for c in cols]
        except NoSuchTableError:
            return None

    def _apply_contract(
        self,
        conn: object,
        target_fqn: str,
        source_cols: list[Column],
        existing_cols: list[str],
    ) -> list[str]:
        """Apply schema contract; return ordered column list to write."""
        existing_set = set(existing_cols)
        source_names = [c.name for c in source_cols]
        source_set = set(source_names)

        new_cols = [c for c in source_cols if c.name not in existing_set]
        dropped_cols = [c for c in existing_cols if c not in source_set]

        if new_cols:
            if self._contract.on_new_column == "freeze":
                raise SchemaContractViolation(
                    f"Schema contract freeze: new columns {[c.name for c in new_cols]}"
                )
            if self._contract.on_new_column == "evolve":
                for col in new_cols:
                    conn.execute(
                        text(
                            f"ALTER TABLE {target_fqn} ADD "
                            f"{self._quote(col.name)} {self._col_ddl(col)} NULL"
                        )
                    )
            # discard: skip new cols; they won't be in dest_columns

        if dropped_cols and self._contract.on_dropped_column == "freeze":
            raise SchemaContractViolation(
                f"Schema contract freeze: dropped columns {dropped_cols}"
            )

        # dest_columns = intersection of existing (in order) + evolved new cols
        dest = [c for c in existing_cols if c in source_set]
        if self._contract.on_new_column == "evolve":
            dest += [c.name for c in new_cols]
        return dest

    # ------------------------------------------------------------------
    # Bulk insert helper
    # ------------------------------------------------------------------

    def _coerce_value(self, v: Any) -> Any:
        """Coerce a Python value to a DB-safe type before binding.

        Mirrors dlt's per-destination escape_literal functions.
        Default: JSON-serialize dict/list (stored as TEXT/JSONB/SUPER).
        Subclasses override for dialect-specific behaviour.
        """
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return v

    def _bulk_insert(
        self,
        conn: object,
        table: str,
        columns: list[str],
        rows: list[dict],
    ) -> None:
        if not rows:
            return
        col_list = ", ".join(self._quote(c) for c in columns)
        placeholders = ", ".join([":p" + str(i) for i in range(len(columns))])
        sql = text(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})")
        params = [
            {"p" + str(i): self._coerce_value(row.get(col)) for i, col in enumerate(columns)}
            for row in rows
        ]
        conn.execute(sql, params)
