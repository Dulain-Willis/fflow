"""Unit tests for fflow.common.state."""

import json
import threading
from pathlib import Path

import pytest

from fflow.common.state import FileStateStore, SqlStateStore, _safe_path_segment


# ---------------------------------------------------------------------------
# FileStateStore
# ---------------------------------------------------------------------------


class TestFileStateStore:
    def test_get_returns_empty_on_missing(self, tmp_path):
        store = FileStateStore(tmp_path)
        assert store.get("my_pipeline", "phone") == {}

    def test_set_then_get(self, tmp_path):
        store = FileStateStore(tmp_path)
        state = {"cursor": 42, "sttrcid": 1234}
        store.set("my_pipeline", "phone", state)
        assert store.get("my_pipeline", "phone") == state

    def test_overwrite_existing(self, tmp_path):
        store = FileStateStore(tmp_path)
        store.set("p", "s", {"cursor": 1})
        store.set("p", "s", {"cursor": 2})
        assert store.get("p", "s") == {"cursor": 2}

    def test_path_sanitization(self, tmp_path):
        store = FileStateStore(tmp_path)
        store.set("my/pipeline", "my:stream", {"x": 1})
        # Must not create subdirectories from the slash.
        assert store.get("my/pipeline", "my:stream") == {"x": 1}

    def test_concurrent_writes_safe(self, tmp_path):
        store = FileStateStore(tmp_path)
        errors = []

        def write(n):
            try:
                store.set("p", "s", {"n": n})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        state = store.get("p", "s")
        assert isinstance(state.get("n"), int)


class TestSafePathSegment:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("simple", "simple"),
            ("my/stream", "my_stream"),
            ("my:stream", "my_stream"),
            ("a.b.c", "a_b_c"),
            ("with space", "with_space"),
        ],
    )
    def test_sanitizes(self, name, expected):
        assert _safe_path_segment(name) == expected


# ---------------------------------------------------------------------------
# SqlStateStore — SQLite in-memory
# ---------------------------------------------------------------------------


@pytest.fixture()
def sqlite_engine():
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool
    # StaticPool: single shared connection across threads — required for
    # in-memory SQLite so all threads see the same database.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    yield engine
    engine.dispose()


class TestSqlStateStore:
    def test_get_empty_on_first_run(self, sqlite_engine):
        store = SqlStateStore(sqlite_engine)
        store.initialize()
        assert store.get("pipe", "stream") == {}

    def test_set_then_get(self, sqlite_engine):
        store = SqlStateStore(sqlite_engine)
        store.initialize()
        store.set("pipe", "stream", {"cursor": 99})
        assert store.get("pipe", "stream") == {"cursor": 99}

    def test_upsert_updates_existing(self, sqlite_engine):
        store = SqlStateStore(sqlite_engine)
        store.initialize()
        store.set("pipe", "s", {"cursor": 1})
        store.set("pipe", "s", {"cursor": 2})
        assert store.get("pipe", "s") == {"cursor": 2}

    def test_independent_streams(self, sqlite_engine):
        store = SqlStateStore(sqlite_engine)
        store.initialize()
        store.set("pipe", "s1", {"cursor": 1})
        store.set("pipe", "s2", {"cursor": 2})
        assert store.get("pipe", "s1") == {"cursor": 1}
        assert store.get("pipe", "s2") == {"cursor": 2}

    def test_independent_pipelines(self, sqlite_engine):
        store = SqlStateStore(sqlite_engine)
        store.initialize()
        store.set("p1", "s", {"cursor": 1})
        store.set("p2", "s", {"cursor": 2})
        assert store.get("p1", "s") == {"cursor": 1}
        assert store.get("p2", "s") == {"cursor": 2}

    def test_initialize_is_idempotent(self, sqlite_engine):
        """Calling initialize() multiple times must not raise."""
        store = SqlStateStore(sqlite_engine)
        store.initialize()
        store.initialize()  # second call — table already exists, no error

    def test_initialize_creates_table(self, sqlite_engine):
        from sqlalchemy import inspect as sa_inspect
        store = SqlStateStore(sqlite_engine)
        assert "fflow_state" not in sa_inspect(sqlite_engine).get_table_names()
        store.initialize()
        assert "fflow_state" in sa_inspect(sqlite_engine).get_table_names()

    def test_concurrent_writes_safe(self, sqlite_engine):
        store = SqlStateStore(sqlite_engine)
        store.initialize()  # main-thread init before workers start
        errors = []

        def write(n):
            try:
                store.set("pipe", f"stream_{n}", {"n": n})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
