"""Unit tests for fflow.extract.pipe_iterator."""

import time
import threading

import pytest

from fflow.extract.pipe_iterator import PipeIterator


# ---------------------------------------------------------------------------
# Helpers — in-memory source and state store stubs
# ---------------------------------------------------------------------------


class StubStateStore:
    def __init__(self):
        self._data = {}

    def get(self, pipeline, stream):
        return self._data.get((pipeline, stream), {})

    def set(self, pipeline, stream, state):
        self._data[(pipeline, stream)] = dict(state)


class StubSource:
    """Yields N rows per stream."""

    def __init__(self, streams_rows: dict[str, list[dict]]):
        self._data = streams_rows

    def check(self): pass
    def discover(self): pass

    def read(self, stream: str, state: dict):
        rows = self._data.get(stream, [])
        for row in rows:
            yield row
        # Update state cursor to last id seen.
        if rows:
            state["last_id"] = rows[-1].get("id", 0)


class ErrorSource:
    """A source where one stream raises an exception."""

    def check(self): pass
    def discover(self): pass

    def read(self, stream: str, state: dict):
        if stream == "bad":
            raise RuntimeError("extraction failed")
        yield {"id": 1}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPipeIterator:
    def test_yields_all_rows_single_stream(self):
        rows = [{"id": i} for i in range(5)]
        source = StubSource({"phone": rows})
        store = StubStateStore()

        collected = []
        with PipeIterator(source, ["phone"], store, "test", chunk_size=2) as pipe:
            for stream_name, chunk in pipe:
                assert stream_name == "phone"
                collected.extend(chunk)

        assert collected == rows

    def test_yields_all_rows_multiple_streams(self):
        source = StubSource({
            "phone": [{"id": i} for i in range(3)],
            "account": [{"id": i} for i in range(4)],
        })
        store = StubStateStore()

        results: dict[str, list] = {"phone": [], "account": []}
        with PipeIterator(source, ["phone", "account"], store, "test") as pipe:
            for stream_name, chunk in pipe:
                results[stream_name].extend(chunk)

        assert len(results["phone"]) == 3
        assert len(results["account"]) == 4

    def test_respects_chunk_size(self):
        rows = [{"id": i} for i in range(10)]
        source = StubSource({"phone": rows})
        store = StubStateStore()

        chunks = []
        with PipeIterator(source, ["phone"], store, "test", chunk_size=3) as pipe:
            for _, chunk in pipe:
                chunks.append(len(chunk))

        # Last chunk may be smaller
        assert all(c <= 3 for c in chunks)
        assert sum(chunks) == 10

    def test_state_updated_after_iteration(self):
        rows = [{"id": i} for i in range(5)]
        source = StubSource({"phone": rows})
        store = StubStateStore()

        with PipeIterator(source, ["phone"], store, "test") as pipe:
            for _ in pipe:
                pass
            state = pipe.get_state("phone")

        assert state.get("last_id") == 4

    def test_worker_error_surfaces_after_iteration(self):
        from fflow.extract.pipe_iterator import _WorkerErrors

        source = ErrorSource()
        store = StubStateStore()

        with pytest.raises(_WorkerErrors) as exc_info:
            with PipeIterator(source, ["bad", "good"], store, "test") as pipe:
                for _ in pipe:
                    pass

        err_streams = [e.stream for e in exc_info.value.errors]
        assert "bad" in err_streams

    def test_empty_source_yields_nothing(self):
        source = StubSource({"phone": []})
        store = StubStateStore()

        items = []
        with PipeIterator(source, ["phone"], store, "test") as pipe:
            for item in pipe:
                items.append(item)

        assert items == []

    def test_no_streams_yields_nothing(self):
        source = StubSource({})
        store = StubStateStore()

        items = []
        with PipeIterator(source, [], store, "test") as pipe:
            for item in pipe:
                items.append(item)

        assert items == []
