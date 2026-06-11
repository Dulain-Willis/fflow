"""Unit tests for fflow.common.schema."""

import pytest
from pydantic import ValidationError

from fflow.common.schema import (
    Column,
    ColumnType,
    IncrementalConfig,
    Schema,
    Stream,
)


class TestColumnType:
    def test_all_types_valid(self):
        for t in ColumnType:
            Column(name="x", type=t)

    def test_defaults_to_unknown(self):
        col = Column(name="x")
        assert col.type == ColumnType.unknown


class TestIncrementalConfig:
    def test_none_cursor_is_default(self):
        cfg = IncrementalConfig()
        assert cfg.cursor_type == "none"
        assert cfg.cursor_field is None

    def test_integer_requires_cursor_field(self):
        with pytest.raises(ValidationError, match="cursor_field is required"):
            IncrementalConfig(cursor_type="integer")

    def test_timestamp_requires_cursor_field(self):
        with pytest.raises(ValidationError, match="cursor_field is required"):
            IncrementalConfig(cursor_type="timestamp")

    def test_valid_integer_cursor(self):
        cfg = IncrementalConfig(cursor_type="integer", cursor_field="id")
        assert cfg.cursor_field == "id"


class TestStream:
    def test_primary_keys_extracted(self):
        stream = Stream(
            name="phone",
            columns=[
                Column(name="id", type=ColumnType.integer, primary_key=True),
                Column(name="number", type=ColumnType.string),
            ],
        )
        assert stream.primary_keys == ["id"]

    def test_get_column_found(self):
        stream = Stream(
            name="phone",
            columns=[Column(name="id", type=ColumnType.integer)],
        )
        assert stream.get_column("id") is not None

    def test_get_column_missing(self):
        stream = Stream(name="phone")
        assert stream.get_column("nonexistent") is None


class TestSchema:
    def test_get_stream_found(self):
        schema = Schema(streams=[Stream(name="phone")])
        assert schema.get_stream("phone") is not None

    def test_get_stream_missing(self):
        schema = Schema(streams=[Stream(name="phone")])
        assert schema.get_stream("account") is None

    def test_stream_names(self):
        schema = Schema(streams=[Stream(name="a"), Stream(name="b")])
        assert schema.stream_names == ["a", "b"]
