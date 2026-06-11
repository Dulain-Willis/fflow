# tests/unit/test_s3_destination.py
#
# S3Destination unit tests — boto3 and pyarrow mocked.

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from fflow.common.config import StreamConfig
from fflow.common.schema import Stream
from fflow.destinations.s3 import S3ConnectionConfig, S3Destination, S3StreamConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _conn_cfg(bucket: str = "my-bucket", prefix: str = "data") -> S3ConnectionConfig:
    return S3ConnectionConfig(bucket=bucket, prefix=prefix)


def _stream(name: str = "orders") -> Stream:
    return Stream(name=name, columns=[])


def _build_dest(bucket="my-bucket", prefix="data") -> S3Destination:
    mock_s3 = MagicMock()
    with patch("fflow.destinations.s3.S3Destination._build_client", return_value=mock_s3):
        dest = S3Destination(connection=_conn_cfg(bucket, prefix))
    return dest


# ---------------------------------------------------------------------------
# check()
# ---------------------------------------------------------------------------


class TestCheck:
    def test_check_calls_head_bucket(self):
        dest = _build_dest()
        dest.check()
        dest._s3.head_bucket.assert_called_once_with(Bucket="my-bucket")

    def test_check_raises_on_failure(self):
        dest = _build_dest()
        dest._s3.head_bucket.side_effect = RuntimeError("403 Forbidden")
        with pytest.raises(RuntimeError, match="403"):
            dest.check()


# ---------------------------------------------------------------------------
# prepare_stream()
# ---------------------------------------------------------------------------


class TestPrepareStream:
    def test_prepare_stream_stores_buffer(self):
        dest = _build_dest()
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"), run_id="run1")
        assert "orders" in dest._buffers
        assert dest._buffers["orders"].run_id == "run1"

    def test_merge_disposition_raises(self):
        dest = _build_dest()
        with pytest.raises(NotImplementedError):
            dest.prepare_stream(
                "orders", _stream(),
                StreamConfig(name="orders", write_disposition="merge", merge_key=["id"]),
            )


# ---------------------------------------------------------------------------
# write()
# ---------------------------------------------------------------------------


class TestWrite:
    def test_rows_buffered(self):
        dest = _build_dest()
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"), run_id="r1")
        dest.write("orders", [{"id": 1}, {"id": 2}])
        assert len(dest._buffers["orders"].rows) == 2


# ---------------------------------------------------------------------------
# commit() — Parquet
# ---------------------------------------------------------------------------


class TestCommitParquet:
    def _setup(self, run_id: str = "run-abc"):
        dest = _build_dest()
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"), run_id=run_id)
        return dest

    def test_commit_calls_put_object(self):
        dest = self._setup("run-123")
        dest.write("orders", [{"id": 1, "amount": 9.99}])

        mock_pa = MagicMock()
        mock_pq = MagicMock()
        mock_table = MagicMock()
        mock_pa.Table.from_pylist.return_value = mock_table

        import sys
        with patch.dict(sys.modules, {"pyarrow": mock_pa, "pyarrow.parquet": mock_pq}):
            dest.commit("orders")

        dest._s3.put_object.assert_called_once()
        call_kwargs = dest._s3.put_object.call_args.kwargs or dict(
            zip(
                ["Bucket", "Key", "Body", "ContentType"],
                dest._s3.put_object.call_args.args,
            )
        )
        assert call_kwargs.get("Bucket") == "my-bucket"
        key = dest._s3.put_object.call_args[1].get("Key") or dest._s3.put_object.call_args[0][1]
        assert "run-123.parquet" in key

    def test_s3_key_contains_stream_and_run_id(self):
        dest = self._setup("myrunid")
        key = dest._s3_key("orders", "myrunid", "parquet")
        assert "orders" in key
        assert "myrunid.parquet" in key

    def test_buffer_cleared_after_commit(self):
        dest = self._setup()
        dest.write("orders", [{"id": 1}])

        import sys
        mock_pa = MagicMock()
        mock_pq = MagicMock()
        with patch.dict(sys.modules, {"pyarrow": mock_pa, "pyarrow.parquet": mock_pq}):
            dest.commit("orders")

        assert "orders" not in dest._buffers


# ---------------------------------------------------------------------------
# commit() — JSONL
# ---------------------------------------------------------------------------


class TestCommitJsonl:
    def test_jsonl_serialise(self):
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        body = S3Destination._serialise(rows, "jsonl")
        lines = body.decode("utf-8").strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == rows[0]


# ---------------------------------------------------------------------------
# rollback()
# ---------------------------------------------------------------------------


class TestRollback:
    def test_rollback_clears_buffer(self):
        dest = _build_dest()
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"), run_id="r1")
        dest.write("orders", [{"id": 1}])
        dest.rollback("orders")
        assert "orders" not in dest._buffers

    def test_rollback_unknown_stream_is_noop(self):
        dest = _build_dest()
        dest.rollback("nonexistent")  # must not raise


# ---------------------------------------------------------------------------
# S3 key generation
# ---------------------------------------------------------------------------


class TestS3Key:
    def _dest(self, prefix):
        return _build_dest(prefix=prefix)

    def test_key_with_prefix(self):
        dest = self._dest("my-prefix")
        key = dest._s3_key("orders", "run1", "parquet")
        assert key == "my-prefix/orders/run1.parquet"

    def test_key_no_prefix(self):
        dest = self._dest("")
        key = dest._s3_key("orders", "run1", "parquet")
        assert key == "orders/run1.parquet"

    def test_key_strips_trailing_slash(self):
        dest = self._dest("prefix/")
        key = dest._s3_key("orders", "run1", "parquet")
        assert key == "prefix/orders/run1.parquet"
