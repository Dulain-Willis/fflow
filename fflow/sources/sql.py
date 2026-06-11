"""Generic SQLAlchemy-backed source connector.

Works with any database supported by SQLAlchemy: PostgreSQL, MySQL, SQLite,
Redshift, Snowflake, BigQuery (via dialect adapters), and others.

Two extraction modes (same as CacheSource):
- **Mirror mode**: ``table`` set, no ``sql_file``.  ``discover()`` reflects the
  table schema; ``read()`` SELECT * (with optional incremental WHERE clause).
- **SQL-file mode**: ``sql_file`` set, no ``table``.  ``discover()`` returns an
  empty schema; ``read()`` executes the file's SQL, optionally substituting
  ``{{cursor_value}}`` with the current watermark.

Incremental cursors (same ``IncrementalConfig`` as all other connectors):
- ``cursor_type="none"``      → full refresh every run
- ``cursor_type="integer"``   → WHERE {cursor_field} > {last_value}
- ``cursor_type="timestamp"`` → WHERE {cursor_field} > '{last_value}'
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Optional

from pydantic import BaseModel, model_validator
from sqlalchemy import create_engine, inspect, text

from fflow.common.schema import Column, IncrementalConfig, Schema, Stream
from fflow.common.type_map import sqlalchemy_type_to_column

logger = logging.getLogger(__name__)


class SQLConnectionConfig(BaseModel):
    connection_url: str


class SQLStreamConfig(BaseModel):
    table: Optional[str] = None
    schema_: Optional[str] = None  # source schema (avoid shadowing Pydantic's schema())
    sql_file: Optional[str] = None
    incremental: IncrementalConfig = IncrementalConfig()
    chunk_size: int = 1000

    @model_validator(mode="after")
    def _exactly_one_of_table_or_sql_file(self) -> "SQLStreamConfig":
        if bool(self.table) == bool(self.sql_file):
            raise ValueError("Exactly one of 'table' or 'sql_file' must be set")
        return self


class SQLSource:
    """Source connector that reads from any SQLAlchemy-compatible database.

    Parameters
    ----------
    connection:
        Connection settings including a SQLAlchemy URL.
    streams:
        Per-stream extraction config keyed by stream name.
    """

    def __init__(
        self,
        connection: SQLConnectionConfig,
        streams: dict[str, SQLStreamConfig],
    ) -> None:
        self._conn_cfg = connection
        self._stream_cfgs = streams
        self._engine = create_engine(connection.connection_url)

    # ------------------------------------------------------------------
    # Source Protocol
    # ------------------------------------------------------------------

    def check(self) -> None:
        """Raise ConnectorError if the database is unreachable."""
        with self._engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    def discover(self) -> Schema:
        """Return schema for all configured streams.

        Mirror-mode streams: schema reflected from the source table.
        SQL-file mode streams: empty column list (schema unknown until query runs).
        """
        streams: list[Stream] = []
        with self._engine.connect() as conn:
            for stream_name, cfg in self._stream_cfgs.items():
                if cfg.sql_file:
                    streams.append(
                        Stream(name=stream_name, columns=[], incremental=cfg.incremental)
                    )
                else:
                    columns = self._reflect_columns(conn, cfg.table, cfg.schema_)
                    streams.append(
                        Stream(
                            name=stream_name,
                            columns=columns,
                            incremental=cfg.incremental,
                        )
                    )
        return Schema(streams=streams)

    def read(self, stream: str, state: dict) -> Iterator[dict]:
        """Yield rows for *stream*, updating *state* in-place as rows flow.

        State key: ``cursor_value`` — the latest seen watermark value.
        """
        cfg = self._stream_cfgs[stream]
        incremental = cfg.incremental
        current_cursor = state.get("cursor_value")

        with self._engine.connect() as conn:
            if cfg.sql_file:
                yield from self._read_sql_file(conn, cfg, incremental, current_cursor, state)
            else:
                yield from self._read_table(conn, cfg, incremental, current_cursor, state)

    # ------------------------------------------------------------------
    # Internal: table read
    # ------------------------------------------------------------------

    def _read_table(
        self,
        conn: object,
        cfg: SQLStreamConfig,
        incremental: IncrementalConfig,
        current_cursor,
        state: dict,
    ) -> Iterator[dict]:
        schema_prefix = f'"{cfg.schema_}".' if cfg.schema_ else ""
        table_ref = f'{schema_prefix}"{cfg.table}"'

        where_clause = self._build_where_clause(incremental, current_cursor)
        sql_str = f"SELECT * FROM {table_ref}"
        if where_clause:
            sql_str += f" WHERE {where_clause}"

        max_cursor = current_cursor
        chunk: list[dict] = []
        result = conn.execute(text(sql_str))
        keys = list(result.keys())
        for row in result:
            record = dict(zip(keys, row))
            if incremental.cursor_type != "none" and incremental.cursor_field:
                val = record.get(incremental.cursor_field)
                if val is not None and (max_cursor is None or val > max_cursor):
                    max_cursor = val
            chunk.append(record)
            if len(chunk) >= cfg.chunk_size:
                yield from chunk
                chunk.clear()

        if chunk:
            yield from chunk

        if incremental.cursor_type != "none" and max_cursor is not None:
            state["cursor_value"] = max_cursor

    # ------------------------------------------------------------------
    # Internal: SQL-file read
    # ------------------------------------------------------------------

    def _read_sql_file(
        self,
        conn: object,
        cfg: SQLStreamConfig,
        incremental: IncrementalConfig,
        current_cursor,
        state: dict,
    ) -> Iterator[dict]:
        sql_text = Path(cfg.sql_file).read_text(encoding="utf-8")

        if "{{cursor_value}}" in sql_text:
            if incremental.cursor_type == "none" or current_cursor is None:
                cursor_str = "0"
            else:
                cursor_str = str(current_cursor)
            sql_text = sql_text.replace("{{cursor_value}}", cursor_str)
        elif incremental.cursor_type != "none" and current_cursor is not None:
            raise ValueError(
                f"SQL file '{cfg.sql_file}' is incremental but missing "
                "'{{cursor_value}}' placeholder"
            )

        max_cursor = current_cursor
        chunk: list[dict] = []
        result = conn.execute(text(sql_text))
        keys = list(result.keys())
        for row in result:
            record = dict(zip(keys, row))
            if incremental.cursor_type != "none" and incremental.cursor_field:
                val = record.get(incremental.cursor_field)
                if val is not None and (max_cursor is None or val > max_cursor):
                    max_cursor = val
            chunk.append(record)
            if len(chunk) >= cfg.chunk_size:
                yield from chunk
                chunk.clear()

        if chunk:
            yield from chunk

        if incremental.cursor_type != "none" and max_cursor is not None:
            state["cursor_value"] = max_cursor

    # ------------------------------------------------------------------
    # Internal: schema reflection
    # ------------------------------------------------------------------

    def _reflect_columns(
        self, conn: object, table: str, schema: Optional[str]
    ) -> list[Column]:
        insp = inspect(conn)
        pk_cols = set(insp.get_pk_constraint(table, schema=schema).get("constrained_columns", []))
        raw_cols = insp.get_columns(table, schema=schema)
        columns: list[Column] = []
        for rc in raw_cols:
            col = sqlalchemy_type_to_column(
                name=rc["name"],
                sa_type=rc["type"],
                nullable=rc.get("nullable", True),
                primary_key=rc["name"] in pk_cols,
            )
            columns.append(col)
        return columns

    @staticmethod
    def _build_where_clause(incremental: IncrementalConfig, current_cursor) -> str:
        if incremental.cursor_type == "none" or current_cursor is None:
            return ""
        field = f'"{incremental.cursor_field}"'
        if incremental.cursor_type == "integer":
            return f"{field} > {int(current_cursor)}"
        # timestamp
        return f"{field} > '{current_cursor}'"
