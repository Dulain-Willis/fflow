"""Redshift destination connector.

Extends :class:`~fflow.destinations.sql.SQLDestination`.  Bulk loads
use Redshift's ``COPY`` command (Parquet from S3 staging) instead of
row-by-row INSERT — the standard approach for Redshift throughput.

Load flow per stream:
1. ``prepare_stream()`` — DDL (CREATE/ALTER TABLE) via SQLAlchemy, same as base.
2. ``write()`` — buffer rows in-memory.
3. ``commit()`` — write buffer as Parquet to S3 staging path, then execute
   ``COPY {table} FROM 's3://...' ... FORMAT AS PARQUET``.

Write dispositions:
- ``append``  — COPY into target.
- ``replace`` — TRUNCATE then COPY.
- ``merge``   — COPY into a staging table, DELETE matching keys from target,
                INSERT non-delete rows from staging.

S3 staging path: ``{s3_staging_prefix}/{stream}/{run_id}.parquet``

Dependencies: ``sqlalchemy``, ``pyarrow``, ``boto3``.
"""

from __future__ import annotations

import io
import json
import logging
import re
from datetime import date, datetime, time
from typing import Any, Optional

from pydantic import BaseModel
from sqlalchemy import text

from fflow.common.config import StreamConfig
from fflow.common.schema import Stream
from fflow.common.type_map import column_to_generic_sql_ddl
from fflow.destinations.sql import SQLConnectionConfig, SQLDestination, _SQLStreamBuffer
from fflow.destinations.s3 import S3Destination as _S3Helper, S3ConnectionConfig, S3StreamConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Literal escaping — copied from dlt's escape_redshift_literal (escape.py).
# Redshift keeps \ as escape character (pre-9 postgres behaviour), so we use
# a plain 'quoted' string rather than E'extended' strings.
# ---------------------------------------------------------------------------
_SQL_ESCAPE_DICT = {"'": "''", "\\": "\\\\", "\n": "\\n", "\r": "\\r"}
_SQL_ESCAPE_RE = re.compile(
    "|".join(re.escape(k) for k in sorted(_SQL_ESCAPE_DICT, key=len, reverse=True)),
    flags=re.DOTALL,
)

# Redshift's max single-statement size is 16 MB. We chunk at half that to
# leave headroom for the INSERT header (mirrors dlt's insert_job_client.py).
_MAX_QUERY_BYTES = 8 * 1024 * 1024  # 8 MB


def _escape_redshift_literal(v: Any) -> str:
    """Serialize *v* to a Redshift SQL literal string.

    Mirrors dlt's escape_redshift_literal (dlt/common/data_writers/escape.py).
    json/dict/list values use json_parse() so they land in SUPER columns.
    """
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        escaped = _SQL_ESCAPE_RE.sub(lambda m: _SQL_ESCAPE_DICT[m.group(0)], v)
        return f"'{escaped}'"
    if isinstance(v, bytes):
        return f"from_hex('{v.hex()}')"
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (dict, list)):
        # SUPER column — wrap in json_parse() so Redshift ingests as semi-structured
        raw = json.dumps(v)
        escaped = _SQL_ESCAPE_RE.sub(lambda m: _SQL_ESCAPE_DICT[m.group(0)], raw)
        return f"json_parse('{escaped}')"
    # fallback: cast to string
    escaped = _SQL_ESCAPE_RE.sub(lambda m: _SQL_ESCAPE_DICT[m.group(0)], str(v))
    return f"'{escaped}'"


class RedshiftConnectionConfig(BaseModel):
    connection_url: str                         # redshift+psycopg2://... or postgresql+psycopg2://...
    dest_schema: str = "public"
    s3_staging_prefix: Optional[str] = None    # if omitted, plain INSERT is used instead of COPY
    iam_role: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region: str = "us-east-1"


class RedshiftDestination(SQLDestination):
    """Destination connector for Amazon Redshift.

    Uses ``COPY`` from S3 for bulk loads when ``s3_staging_prefix`` is set.
    Falls back to plain SQLAlchemy INSERT when it is omitted — suitable for
    low-volume streams where S3 staging is not worth the overhead.

    Parameters
    ----------
    connection:
        Redshift connection settings. Set ``s3_staging_prefix`` to enable
        S3-backed COPY; omit it for direct INSERT.
    streams:
        Per-stream write config keyed by stream name (optional; controls
        ``write_disposition`` and ``merge_key``).
    contract:
        Schema-change policy applied during ``prepare_stream()``.
    """

    def __init__(
        self,
        connection: RedshiftConnectionConfig,
        **kwargs,
    ) -> None:
        sa_cfg = SQLConnectionConfig(
            connection_url=connection.connection_url,
            dest_schema=connection.dest_schema,
        )
        super().__init__(connection=sa_cfg, **kwargs)
        self._rs_cfg = connection

        if connection.s3_staging_prefix:
            bucket, prefix = self._parse_s3_prefix(connection.s3_staging_prefix)
            s3_cfg = S3ConnectionConfig(
                bucket=bucket,
                prefix=prefix,
                region=connection.region,
                aws_access_key_id=connection.aws_access_key_id,
                aws_secret_access_key=connection.aws_secret_access_key,
            )
            self._s3: Optional[_S3Helper] = _S3Helper(connection=s3_cfg)
        else:
            self._s3 = None

    # ------------------------------------------------------------------
    # Commit overrides — append/replace use COPY from S3 when available
    # ------------------------------------------------------------------

    def _load_staging(self, buf: _SQLStreamBuffer, staging_fqn: str) -> None:
        """Populate staging via S3 COPY when available, plain INSERT otherwise."""
        if self._s3 is not None:
            s3_key = self._stage_to_s3(buf)
            s3_uri = f"s3://{self._s3._conn_cfg.bucket}/{s3_key}"
            self._copy_from_s3(buf.conn, staging_fqn, s3_uri)
        else:
            super()._load_staging(buf, staging_fqn)

    def _commit_append(self, buf: _SQLStreamBuffer) -> None:
        if not buf.rows:
            buf.conn.commit()
            return
        if self._s3 is None:
            super()._commit_append(buf)
            return
        s3_key = self._stage_to_s3(buf)
        s3_uri = f"s3://{self._s3._conn_cfg.bucket}/{s3_key}"
        target = self._fqn(buf.target_schema, buf.target_table)
        self._copy_from_s3(buf.conn, target, s3_uri)
        buf.conn.commit()
        buf.rows.clear()

    def _commit_replace(self, buf: _SQLStreamBuffer) -> None:
        if self._s3 is None:
            super()._commit_replace(buf)
            return
        target = self._fqn(buf.target_schema, buf.target_table)
        buf.conn.execute(text(f"TRUNCATE TABLE {target}"))
        if buf.rows:
            s3_key = self._stage_to_s3(buf)
            s3_uri = f"s3://{self._s3._conn_cfg.bucket}/{s3_key}"
            self._copy_from_s3(buf.conn, target, s3_uri)
        buf.conn.commit()
        buf.rows.clear()

    def _commit_merge(self, buf: _SQLStreamBuffer) -> None:
        """Merge via staging table — Redshift-compatible DELETE/INSERT.

        Redshift does not support table aliases in DELETE FROM ... USING.
        Mirrors dlt's gen_delete_from_sql: DELETE WHERE key IN (SELECT key FROM staging).
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

        # DELETE WHERE (k1, k2) IN (SELECT k1, k2 FROM staging)
        # Single key: simpler IN subquery. Composite: tuple comparison.
        if len(merge_keys) == 1:
            k = f'"{merge_keys[0]}"'
            buf.conn.execute(text(
                f"DELETE FROM {target} WHERE {k} IN (SELECT {k} FROM {staging})"
            ))
        else:
            key_cols = ", ".join(f'"{k}"' for k in merge_keys)
            buf.conn.execute(text(
                f"DELETE FROM {target} WHERE ({key_cols}) IN "
                f"(SELECT {key_cols} FROM {staging})"
            ))

        col_list = ", ".join(f'"{c}"' for c in buf.dest_columns)
        buf.conn.execute(text(
            f"INSERT INTO {target} ({col_list}) SELECT {col_list} FROM {staging}"
        ))

        buf.conn.commit()
        buf.rows.clear()

    # ------------------------------------------------------------------
    # Dialect overrides
    # ------------------------------------------------------------------

    def _col_ddl(self, col) -> str:
        from fflow.common.schema import ColumnType
        # Mirrors dlt's Redshift type map (factory.py):
        #   text  -> VARCHAR(MAX)   (TEXT silently becomes VARCHAR(256) in Redshift)
        #   json  -> SUPER          (Redshift native semi-structured type)
        if col.type == ColumnType.string and col.max_length is None:
            return "VARCHAR(MAX)"
        if col.type == ColumnType.json:
            return "SUPER"
        return column_to_generic_sql_ddl(col)

    def _fqn(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'

    def _quote(self, identifier: str) -> str:
        return f'"{identifier}"'

    def _bulk_insert(
        self,
        conn: object,
        table: str,
        columns: list[str],
        rows: list[dict],
    ) -> None:
        """Single-statement multi-row INSERT — mirrors dlt's InsertValuesWriter.

        Builds ``INSERT INTO t(cols) VALUES (r1),(r2),...;`` as a plain SQL
        string using escaped literals (no bind params). Chunks by
        _MAX_QUERY_BYTES so we never exceed Redshift's 16 MB statement limit.
        """
        if not rows:
            return

        col_list = ",".join(f'"{c}"' for c in columns)
        header = f"INSERT INTO {table}({col_list})\nVALUES\n"

        def _row_literal(row: dict) -> str:
            vals = ",".join(
                _escape_redshift_literal(row.get(c)) for c in columns
            )
            return f"({vals})"

        # Build row literals; flush a statement whenever we'd exceed the limit.
        pending: list[str] = []
        pending_bytes = 0

        for row in rows:
            literal = _row_literal(row)
            row_bytes = len(literal.encode())

            if pending and pending_bytes + row_bytes > _MAX_QUERY_BYTES:
                sql = header + ",\n".join(pending) + ";"
                conn.execute(text(sql))
                pending = []
                pending_bytes = 0

            pending.append(literal)
            pending_bytes += row_bytes

        if pending:
            sql = header + ",\n".join(pending) + ";"
            conn.execute(text(sql))

    # ------------------------------------------------------------------
    # S3 staging helpers
    # ------------------------------------------------------------------

    def _stage_to_s3(self, buf: _SQLStreamBuffer) -> str:
        """Write *buf.rows* as Parquet to S3; return the S3 key."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "pyarrow is required for RedshiftDestination. "
                "Install with: pip install pyarrow"
            ) from exc
        table = pa.Table.from_pylist(buf.rows)
        raw = io.BytesIO()
        pq.write_table(table, raw)
        s3_key = self._s3._s3_key(buf.target_table, buf.run_id, "parquet")
        self._s3._s3.put_object(
            Bucket=self._s3._conn_cfg.bucket,
            Key=s3_key,
            Body=raw.getvalue(),
            ContentType="application/octet-stream",
        )
        return s3_key

    def _copy_from_s3(self, conn, target: str, s3_uri: str) -> None:
        """Execute Redshift COPY from *s3_uri* into *target*."""
        credentials = self._build_credentials()
        conn.execute(text(
            f"COPY {target} FROM '{s3_uri}' "
            f"{credentials} "
            f"FORMAT AS PARQUET"
        ))

    def _build_credentials(self) -> str:
        cfg = self._rs_cfg
        if cfg.iam_role:
            return f"IAM_ROLE '{cfg.iam_role}'"
        if cfg.aws_access_key_id and cfg.aws_secret_access_key:
            return (
                f"CREDENTIALS 'aws_access_key_id={cfg.aws_access_key_id};"
                f"aws_secret_access_key={cfg.aws_secret_access_key}'"
            )
        return "IAM_ROLE DEFAULT"

    @staticmethod
    def _parse_s3_prefix(s3_prefix: str) -> tuple[str, str]:
        """Parse ``s3://bucket/prefix`` → ``(bucket, prefix)``."""
        if not s3_prefix.startswith("s3://"):
            raise ValueError(f"s3_staging_prefix must start with 's3://': {s3_prefix}")
        without_scheme = s3_prefix[5:]
        parts = without_scheme.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix


# ---------------------------------------------------------------------------
# Decorator-friendly factory
# ---------------------------------------------------------------------------

def redshift(
    url: str,
    schema: str = "public",
    s3_staging_prefix: Optional[str] = None,
    iam_role: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region: str = "us-east-1",
) -> RedshiftDestination:
    """Create a :class:`RedshiftDestination` for use with the ``@pipeline`` decorator.

    Example::

        @pipeline(
            source=...,
            destination=redshift(url=os.environ["REDSHIFT_URL"], schema="raw_data"),
        )
        def my_pipeline(): ...
    """
    return RedshiftDestination(
        connection=RedshiftConnectionConfig(
            connection_url=url,
            dest_schema=schema,
            s3_staging_prefix=s3_staging_prefix,
            iam_role=iam_role,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region=region,
        )
    )
