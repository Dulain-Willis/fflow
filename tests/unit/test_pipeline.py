"""Unit tests for fflow.pipeline.pipeline.Pipeline."""

import pytest

from fflow.common.config import StreamConfig
from fflow.common.exceptions import PipelineRunError
from fflow.common.schema import Column, ColumnType, IncrementalConfig, Schema, Stream
from fflow.pipeline.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Stub implementations
# ---------------------------------------------------------------------------


class StubStateStore:
    def __init__(self):
        self._data: dict[tuple[str, str], dict] = {}

    def get(self, pipeline, stream):
        return dict(self._data.get((pipeline, stream), {}))

    def set(self, pipeline, stream, state):
        self._data[(pipeline, stream)] = dict(state)


class StubSource:
    def __init__(self, schema: Schema, rows: dict[str, list[dict]] | None = None):
        self._schema = schema
        self._rows = rows or {}
        self.checked = False

    def check(self):
        self.checked = True

    def discover(self):
        return self._schema

    def read(self, stream: str, state: dict):
        for row in self._rows.get(stream, []):
            yield row
        if self._rows.get(stream):
            state["last_id"] = self._rows[stream][-1].get("id", 0)


class StubDestination:
    def __init__(self, fail_streams: list[str] | None = None, fail_prepare: list[str] | None = None):
        self.prepared: list[str] = []
        self.written: dict[str, list] = {}
        self.committed: list[str] = []
        self.rolled_back: list[str] = []
        self._fail_streams = set(fail_streams or [])
        self._fail_prepare = set(fail_prepare or [])
        self.checked = False

    def check(self):
        self.checked = True

    def prepare_stream(self, stream, schema, config, run_id=""):
        if stream in self._fail_prepare:
            raise RuntimeError(f"prepare failed for {stream}")
        self.prepared.append(stream)

    def write(self, stream, rows):
        if stream in self._fail_streams:
            raise RuntimeError(f"write failed for {stream}")
        self.written.setdefault(stream, []).extend(rows)

    def commit(self, stream):
        self.committed.append(stream)

    def rollback(self, stream):
        self.rolled_back.append(stream)


def _simple_schema(*stream_names: str) -> Schema:
    return Schema(
        streams=[
            Stream(
                name=n,
                columns=[Column(name="id", type=ColumnType.integer, primary_key=True)],
            )
            for n in stream_names
        ]
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPipelineCheck:
    def test_check_calls_source_and_destination(self):
        schema = _simple_schema("phone")
        source = StubSource(schema)
        dest = StubDestination()
        pipeline = Pipeline("test", source, dest, StubStateStore())

        pipeline.check()

        assert source.checked
        assert dest.checked


class TestPipelineRun:
    def test_runs_all_streams_by_default(self):
        schema = _simple_schema("phone", "account")
        rows = {
            "phone": [{"id": 1}],
            "account": [{"id": 2}],
        }
        source = StubSource(schema, rows)
        dest = StubDestination()

        Pipeline("p", source, dest, StubStateStore(), loaded_at=False).run()

        assert set(dest.prepared) == {"phone", "account"}
        assert set(dest.committed) == {"phone", "account"}
        assert dest.written["phone"] == [{"id": 1}]
        assert dest.written["account"] == [{"id": 2}]

    def test_streams_filter_by_name(self):
        schema = _simple_schema("phone", "account", "call_log")
        source = StubSource(schema, {"phone": [{"id": 1}]})
        dest = StubDestination()

        Pipeline("p", source, dest, StubStateStore()).run(streams=["phone"])

        assert dest.prepared == ["phone"]

    def test_streams_glob_pattern(self):
        schema = _simple_schema("account_a", "account_b", "phone")
        source = StubSource(schema)
        dest = StubDestination()

        Pipeline("p", source, dest, StubStateStore()).run(streams=["account*"])

        assert set(dest.prepared) == {"account_a", "account_b"}
        assert "phone" not in dest.prepared

    def test_state_persisted_after_run(self):
        schema = Schema(
            streams=[
                Stream(
                    name="phone",
                    columns=[Column(name="id", type=ColumnType.integer)],
                    incremental=IncrementalConfig(cursor_type="integer", cursor_field="id"),
                )
            ]
        )
        rows = {"phone": [{"id": 1}, {"id": 2}, {"id": 3}]}
        source = StubSource(schema, rows)
        store = StubStateStore()

        Pipeline("p", source, dest := StubDestination(), store).run()

        # State should have been updated by the source during read().
        assert store.get("p", "phone") == {"last_id": 3}

    def test_full_refresh_ignores_state(self):
        schema = _simple_schema("phone")
        rows = {"phone": [{"id": 1}]}
        source = StubSource(schema, rows)
        store = StubStateStore()
        store.set("p", "phone", {"last_id": 999})

        dest = StubDestination()
        Pipeline("p", source, dest, store, loaded_at=False).run(full_refresh=True)

        # Rows were still written (source ignores state in stub).
        assert dest.written["phone"] == [{"id": 1}]

    def test_continue_on_stream_error_adr0006(self):
        """ADR-0006: failed streams don't block successful ones."""
        schema = _simple_schema("good", "bad")
        rows = {"good": [{"id": 1}], "bad": [{"id": 2}]}
        source = StubSource(schema, rows)
        dest = StubDestination()

        class FailingSource(StubSource):
            def read(self, stream, state):
                if stream == "bad":
                    raise RuntimeError("bad stream exploded")
                yield from super().read(stream, state)

        source = FailingSource(schema, rows)

        with pytest.raises(PipelineRunError) as exc_info:
            Pipeline("p", source, dest, StubStateStore()).run()

        # One error collected.
        assert len(exc_info.value.errors) == 1
        assert exc_info.value.errors[0].stream == "bad"
        # Good stream still written and committed.
        assert "good" in dest.written

    def test_no_matching_streams_does_nothing(self):
        schema = _simple_schema("phone")
        source = StubSource(schema)
        dest = StubDestination()

        Pipeline("p", source, dest, StubStateStore()).run(streams=["nonexistent"])

        assert dest.prepared == []
        assert dest.committed == []

    def test_prepare_stream_failure_collected(self):
        schema = _simple_schema("phone", "account")
        source = StubSource(schema, {"account": [{"id": 1}]})
        dest = StubDestination(fail_prepare=["phone"])

        with pytest.raises(PipelineRunError) as exc_info:
            Pipeline("p", source, dest, StubStateStore()).run()

        err_streams = [e.stream for e in exc_info.value.errors]
        assert "phone" in err_streams
        # account still ran.
        assert "account" in dest.committed
