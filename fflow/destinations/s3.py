"""S3 destination connector.

Writes rows as Parquet (default) or JSONL to Amazon S3.

File path pattern: ``{prefix}/{stream}/{run_id}.parquet``

Write disposition:
- ``append``:  write a new file per run (run_id ensures uniqueness).
- ``replace``: same as append — S3 has no TRUNCATE; the consumer is
  responsible for replacing its view of the data.
- ``merge``:   not supported; raises ``NotImplementedError``.

Parquet output via ``pyarrow``; JSONL output via stdlib ``json``.
S3 operations via ``boto3``.

Dependencies: ``pyarrow``, ``boto3`` (optional extras in pyproject.toml).
"""

from __future__ import annotations

import io
import json
import logging
from typing import Iterable, Literal, Optional

from pydantic import BaseModel

from fflow.common.config import StreamConfig
from fflow.common.schema import Stream

logger = logging.getLogger(__name__)

OutputFormat = Literal["parquet", "jsonl"]


class S3ConnectionConfig(BaseModel):
    bucket: str
    prefix: str = ""
    region: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None


class S3StreamConfig(BaseModel):
    format: OutputFormat = "parquet"


class _S3StreamBuffer:
    __slots__ = ("config", "stream_cfg", "run_id", "rows")

    def __init__(self, config: StreamConfig, stream_cfg: S3StreamConfig, run_id: str):
        self.config = config
        self.stream_cfg = stream_cfg
        self.run_id = run_id
        self.rows: list[dict] = []


class S3Destination:
    """Destination connector that writes rows to Amazon S3.

    Parameters
    ----------
    connection:
        S3 connection settings (bucket, prefix, credentials).
    streams:
        Per-stream output format config keyed by stream name.
    """

    def __init__(
        self,
        connection: S3ConnectionConfig,
        streams: dict[str, S3StreamConfig] | None = None,
    ) -> None:
        self._conn_cfg = connection
        self._stream_cfgs = streams or {}
        self._buffers: dict[str, _S3StreamBuffer] = {}
        self._s3 = self._build_client()

    # ------------------------------------------------------------------
    # Destination Protocol
    # ------------------------------------------------------------------

    def check(self) -> None:
        """Raise if the bucket is inaccessible."""
        self._s3.head_bucket(Bucket=self._conn_cfg.bucket)

    def prepare_stream(
        self,
        stream: str,
        schema: Stream,
        config: StreamConfig,
        run_id: str = "",
    ) -> None:
        """Validate bucket access and initialise the per-stream buffer."""
        stream_cfg = self._stream_cfgs.get(stream, S3StreamConfig())
        if config.write_disposition == "merge":
            raise NotImplementedError(
                "S3Destination does not support write_disposition='merge'"
            )
        self._buffers[stream] = _S3StreamBuffer(config, stream_cfg, run_id)

    def write(self, stream: str, rows: Iterable[dict]) -> None:
        """Buffer rows in-memory until commit."""
        self._buffers[stream].rows.extend(rows)

    def commit(self, stream: str) -> None:
        """Write buffered rows to S3 and clear the buffer."""
        buf = self._buffers.pop(stream)
        try:
            if buf.rows:
                self._flush(stream, buf)
        except Exception:
            raise

    def rollback(self, stream: str) -> None:
        """Discard buffered rows (S3 writes are atomic — nothing to undo)."""
        self._buffers.pop(stream, None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flush(self, stream: str, buf: _S3StreamBuffer) -> None:
        ext = buf.stream_cfg.format
        key = self._s3_key(stream, buf.run_id, ext)
        body = self._serialise(buf.rows, buf.stream_cfg.format)
        content_type = (
            "application/octet-stream" if ext == "parquet" else "application/x-ndjson"
        )
        self._s3.put_object(
            Bucket=self._conn_cfg.bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )
        logger.info(
            "S3Destination: wrote %d rows to s3://%s/%s",
            len(buf.rows),
            self._conn_cfg.bucket,
            key,
        )

    def _s3_key(self, stream: str, run_id: str, ext: str) -> str:
        prefix = self._conn_cfg.prefix.rstrip("/")
        parts = [p for p in [prefix, stream, f"{run_id}.{ext}"] if p]
        return "/".join(parts)

    @staticmethod
    def _serialise(rows: list[dict], fmt: OutputFormat) -> bytes:
        if fmt == "parquet":
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
            except ImportError as exc:
                raise ImportError(
                    "pyarrow is required for S3Destination parquet output. "
                    "Install with: pip install pyarrow"
                ) from exc
            table = pa.Table.from_pylist(rows)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            return buf.getvalue()

        # JSONL
        lines = "\n".join(json.dumps(row, default=str) for row in rows)
        return lines.encode("utf-8")

    def _build_client(self):
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for S3Destination. "
                "Install with: pip install boto3"
            ) from exc
        kwargs: dict = {"region_name": self._conn_cfg.region}
        if self._conn_cfg.aws_access_key_id:
            kwargs["aws_access_key_id"] = self._conn_cfg.aws_access_key_id
        if self._conn_cfg.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self._conn_cfg.aws_secret_access_key
        return boto3.client("s3", **kwargs)

    @property
    def s3_key_for(self):
        """Expose _s3_key for use by RedshiftDestination."""
        return self._s3_key
