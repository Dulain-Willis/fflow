"""Unit tests for metadata column injection."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from fflow.common.config import StreamConfig
from fflow.common.metadata import (
    apply_metadata_columns,
    build_metadata_columns,
    check_metadata_column_clashes,
)
from fflow.common.schema import Column, ColumnType, Schema, Stream
from fflow.pipeline.pipeline import Pipeline

_UTC = timezone.utc
_TS = datetime(2026, 6, 9, 18, 0, 0, tzinfo=_UTC)


# ---------------------------------------------------------------------------
# build_metadata_columns
# ---------------------------------------------------------------------------


class TestBuildMetadataColumns:
    def test_loaded_at_only(self):
        cols = build_metadata_columns(loaded_at=True, extra_timezones=[])
        assert len(cols) == 1
        assert cols[0].name == "_fflow_loaded_at"
        assert cols[0].type == ColumnType.timestamp
        assert cols[0].nullable is False

    def test_extra_timezone_columns(self):
        cols = build_metadata_columns(
            loaded_at=True,
            extra_timezones=[("central", "America/Chicago"), ("eastern", "America/New_York")],
        )
        names = [c.name for c in cols]
        assert names == ["_fflow_loaded_at", "_fflow_loaded_at_central", "_fflow_loaded_at_eastern"]

    def test_loaded_at_false_no_base_column(self):
        cols = build_metadata_columns(loaded_at=False, extra_timezones=[])
        assert cols == []

    def test_loaded_at_false_with_extra(self):
        cols = build_metadata_columns(
            loaded_at=False,
            extra_timezones=[("central", "America/Chicago")],
        )
        names = [c.name for c in cols]
        assert "_fflow_loaded_at" not in names
        assert "_fflow_loaded_at_central" in names


# ---------------------------------------------------------------------------
# apply_metadata_columns
# ---------------------------------------------------------------------------


class TestApplyMetadataColumns:
    def test_loaded_at_injected(self):
        chunk = [{"id": 1, "name": "Alice"}]
        result = apply_metadata_columns(chunk, _TS, loaded_at=True, extra_timezones=[])
        assert result[0]["_fflow_loaded_at"] == _TS
        assert result[0]["id"] == 1

    def test_extra_timezone_column_injected(self):
        chunk = [{"id": 1}]
        result = apply_metadata_columns(
            chunk, _TS, loaded_at=True,
            extra_timezones=[("central", "America/Chicago")],
        )
        assert "_fflow_loaded_at_central" in result[0]
        central_ts = result[0]["_fflow_loaded_at_central"]
        assert central_ts.tzinfo is not None
        assert central_ts.utcoffset().total_seconds() in (-6 * 3600, -5 * 3600)  # CDT or CST

    def test_both_loaded_at_false_noop(self):
        chunk = [{"id": 1}]
        result = apply_metadata_columns(chunk, _TS, loaded_at=False, extra_timezones=[])
        assert result == chunk

    def test_original_rows_not_mutated(self):
        original = [{"id": 1}]
        apply_metadata_columns(original, _TS, loaded_at=True, extra_timezones=[])
        assert "_fflow_loaded_at" not in original[0]

    def test_all_rows_get_same_timestamp(self):
        chunk = [{"id": 1}, {"id": 2}, {"id": 3}]
        result = apply_metadata_columns(chunk, _TS, loaded_at=True, extra_timezones=[])
        timestamps = [r["_fflow_loaded_at"] for r in result]
        assert len(set(timestamps)) == 1


# ---------------------------------------------------------------------------
# check_metadata_column_clashes
# ---------------------------------------------------------------------------


class TestCheckMetadataColumnClashes:
    def test_no_clash_passes(self):
        cols = build_metadata_columns(loaded_at=True, extra_timezones=[])
        check_metadata_column_clashes("tickets", {"id", "subject", "email"}, cols)

    def test_clash_raises_with_clear_message(self):
        cols = build_metadata_columns(loaded_at=True, extra_timezones=[])
        with pytest.raises(ValueError, match="_fflow_loaded_at"):
            check_metadata_column_clashes("tickets", {"id", "_fflow_loaded_at"}, cols)

    def test_clash_message_names_the_stream(self):
        cols = build_metadata_columns(loaded_at=True, extra_timezones=[])
        with pytest.raises(ValueError, match="tickets"):
            check_metadata_column_clashes("tickets", {"id", "_fflow_loaded_at"}, cols)

    def test_extra_tz_clash_raises(self):
        cols = build_metadata_columns(
            loaded_at=True, extra_timezones=[("central", "America/Chicago")]
        )
        with pytest.raises(ValueError, match="_fflow_loaded_at_central"):
            check_metadata_column_clashes("users", {"id", "_fflow_loaded_at_central"}, cols)


# ---------------------------------------------------------------------------
# Pipeline integration — loaded_at
# ---------------------------------------------------------------------------


class _StubStateStore:
    def __init__(self):
        self._data: dict = {}

    def get(self, pipeline, stream):
        return dict(self._data.get((pipeline, stream), {}))

    def set(self, pipeline, stream, state):
        self._data[(pipeline, stream)] = dict(state)


class _StubSource:
    def __init__(self, schema: Schema, rows: dict[str, list[dict]]):
        self._schema = schema
        self._rows = rows

    def check(self):
        pass

    def discover(self):
        return self._schema

    def read(self, stream: str, state: dict):
        yield from self._rows.get(stream, [])


class _StubDestination:
    def __init__(self):
        self.written: dict[str, list] = {}

    def check(self):
        pass

    def prepare_stream(self, stream, schema, config, run_id=""):
        pass

    def write(self, stream, rows):
        self.written.setdefault(stream, []).extend(rows)

    def commit(self, stream):
        pass

    def rollback(self, stream):
        pass


def _schema(stream_name: str, *col_names: str) -> Schema:
    return Schema(
        streams=[
            Stream(
                name=stream_name,
                columns=[Column(name=c, type=ColumnType.string) for c in col_names],
            )
        ]
    )


class TestPipelineMetadata:
    def test_loaded_at_present_by_default(self):
        schema = _schema("tickets", "id", "subject")
        source = _StubSource(schema, {"tickets": [{"id": "1", "subject": "hello"}]})
        dest = _StubDestination()

        Pipeline("p", source, dest, _StubStateStore()).run()

        row = dest.written["tickets"][0]
        assert "_fflow_loaded_at" in row
        assert isinstance(row["_fflow_loaded_at"], datetime)
        assert row["_fflow_loaded_at"].tzinfo == _UTC

    def test_loaded_at_false_not_injected(self):
        schema = _schema("tickets", "id")
        source = _StubSource(schema, {"tickets": [{"id": "1"}]})
        dest = _StubDestination()

        Pipeline("p", source, dest, _StubStateStore(), loaded_at=False).run()

        assert "_fflow_loaded_at" not in dest.written["tickets"][0]

    def test_extra_timezone_column_injected(self):
        schema = _schema("tickets", "id")
        source = _StubSource(schema, {"tickets": [{"id": "1"}]})
        dest = _StubDestination()

        Pipeline(
            "p", source, dest, _StubStateStore(),
            loaded_at_extra_timezones=[("central", "America/Chicago")],
        ).run()

        row = dest.written["tickets"][0]
        assert "_fflow_loaded_at" in row
        assert "_fflow_loaded_at_central" in row

    def test_all_rows_in_run_share_loaded_at(self):
        schema = _schema("tickets", "id")
        source = _StubSource(schema, {"tickets": [{"id": "1"}, {"id": "2"}, {"id": "3"}]})
        dest = _StubDestination()

        Pipeline("p", source, dest, _StubStateStore()).run()

        timestamps = [r["_fflow_loaded_at"] for r in dest.written["tickets"]]
        assert len(set(timestamps)) == 1

    def test_source_column_named_loaded_at_raises(self):
        schema = _schema("tickets", "id", "_fflow_loaded_at")
        source = _StubSource(schema, {"tickets": [{"id": "1", "_fflow_loaded_at": "2020-01-01"}]})
        dest = _StubDestination()

        p = Pipeline("p", source, dest, _StubStateStore())
        with pytest.raises(ValueError, match="_fflow_loaded_at"):
            p.run()

    def test_hash_rename_and_metadata_combined(self):
        """Hash rename + metadata injection work together correctly."""
        schema = _schema("patients", "id", "ssn")
        source = _StubSource(schema, {"patients": [{"id": "1", "ssn": "123-45-6789"}]})
        dest = _StubDestination()

        Pipeline(
            "p", source, dest, _StubStateStore(),
            streams=[StreamConfig(name="patients", hash_fields=["ssn"])],
            hash_key="test-key",
        ).run()

        row = dest.written["patients"][0]
        assert "ssn_hash" in row
        assert "ssn" not in row
        assert "_fflow_loaded_at" in row
