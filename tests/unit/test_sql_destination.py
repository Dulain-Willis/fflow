# tests/unit/test_sql_destination.py
#
# SQLDestination base class unit tests — all SQLAlchemy calls mocked.

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest
from sqlalchemy.exc import NoSuchTableError

from fflow.common.config import SchemaContract, StreamConfig
from fflow.common.schema import Column, ColumnType, Stream
from fflow.destinations.sql import SQLConnectionConfig, SQLDestination, _SQLStreamBuffer


# ---------------------------------------------------------------------------
# Concrete subclass for testing abstract class
# ---------------------------------------------------------------------------


class _ConcreteDest(SQLDestination):
    def _commit_replace(self, buf: _SQLStreamBuffer) -> None:
        buf.conn.execute("TRUNCATE")
        buf.conn.commit()
        buf.rows.clear()

    def _commit_merge(self, buf: _SQLStreamBuffer) -> None:
        raise NotImplementedError


def _cfg() -> SQLConnectionConfig:
    return SQLConnectionConfig(connection_url="sqlite:///:memory:", dest_schema="public")


def _stream(name="orders", cols=None):
    if cols is None:
        cols = [
            Column(name="id", type=ColumnType.integer, primary_key=True),
            Column(name="amount", type=ColumnType.decimal),
        ]
    return Stream(name=name, columns=cols)


# ---------------------------------------------------------------------------
# Fixtures: mock SQLAlchemy engine
# ---------------------------------------------------------------------------


def _make_engine_mocks():
    """Return (engine, ddl_conn, data_conn)."""
    engine = MagicMock()

    ddl_conn = MagicMock()
    engine.execution_options.return_value.connect.return_value.__enter__.return_value = ddl_conn
    engine.execution_options.return_value.connect.return_value.__exit__.return_value = None

    data_conn = MagicMock()
    engine.connect.return_value = data_conn

    return engine, ddl_conn, data_conn


def _make_inspect_side(existing_cols: list[str] | None):
    """Return a mock inspector.

    Pass None to simulate a missing table (raises NoSuchTableError).
    Pass a list of column names to simulate an existing table.
    """
    def _build_insp(_conn):
        insp = MagicMock()
        if existing_cols is None:
            insp.get_columns.side_effect = NoSuchTableError("table")
        else:
            insp.get_columns.return_value = [{"name": c} for c in existing_cols]
        insp.get_pk_constraint.return_value = {"constrained_columns": []}
        return insp
    return _build_insp


# ---------------------------------------------------------------------------
# prepare_stream()
# ---------------------------------------------------------------------------


class TestPrepareStream:
    def test_creates_table_when_not_exists(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side(None)):
            dest = _ConcreteDest(_cfg())
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

        sqls = [str(c.args[0]) for c in ddl_conn.execute.call_args_list]
        assert any("CREATE TABLE" in s for s in sqls)

    def test_no_create_when_table_exists(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side(["id", "amount"])):
            dest = _ConcreteDest(_cfg())
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

        sqls = [str(c.args[0]) for c in ddl_conn.execute.call_args_list]
        assert not any("CREATE TABLE" in s for s in sqls)

    def test_alters_table_for_new_column(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side(["id"])):
            dest = _ConcreteDest(_cfg(), contract=SchemaContract(on_new_column="evolve"))
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

        sqls = [str(c.args[0]) for c in ddl_conn.execute.call_args_list]
        assert any("ALTER TABLE" in s for s in sqls)
        assert any("amount" in s for s in sqls)

    def test_freeze_raises_on_new_column(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side(["id"])):
            dest = _ConcreteDest(_cfg(), contract=SchemaContract(on_new_column="freeze"))
            from fflow.common.exceptions import SchemaContractViolation
            with pytest.raises(SchemaContractViolation):
                dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

    def test_run_id_stored_in_buffer(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side([])):
            dest = _ConcreteDest(_cfg())
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"), run_id="abc-123")

        assert dest._buffers["orders"].run_id == "abc-123"


# ---------------------------------------------------------------------------
# write()
# ---------------------------------------------------------------------------


class TestWrite:
    def _setup(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side([])):
            dest = _ConcreteDest(_cfg())
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))
        return dest

    def test_rows_buffered(self):
        dest = self._setup()
        dest.write("orders", [{"id": 1}, {"id": 2}])
        assert len(dest._buffers["orders"].rows) == 2

    def test_multiple_write_calls_accumulate(self):
        dest = self._setup()
        dest.write("orders", [{"id": 1}])
        dest.write("orders", [{"id": 2}])
        assert len(dest._buffers["orders"].rows) == 2


# ---------------------------------------------------------------------------
# commit() — append
# ---------------------------------------------------------------------------


class TestCommitAppend:
    def _setup(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side([])):
            dest = _ConcreteDest(_cfg())
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))
        return dest, data_conn

    def test_inserts_rows_and_commits(self):
        dest, data_conn = self._setup()
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")
        data_conn.execute.assert_called()
        data_conn.commit.assert_called_once()

    def test_buffer_popped_after_commit(self):
        dest, _ = self._setup()
        dest.commit("orders")
        assert "orders" not in dest._buffers

    def test_zero_rows_still_commits(self):
        dest, data_conn = self._setup()
        dest.commit("orders")
        data_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# commit() — replace
# ---------------------------------------------------------------------------


class TestCommitReplace:
    def _setup(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side([])):
            dest = _ConcreteDest(_cfg())
            dest.prepare_stream(
                "orders", _stream(),
                StreamConfig(name="orders", write_disposition="replace")
            )
        return dest, data_conn

    def test_truncates_then_commits(self):
        dest, data_conn = self._setup()
        dest.commit("orders")
        sqls = [str(c.args[0]) for c in data_conn.execute.call_args_list]
        assert any("TRUNCATE" in s for s in sqls)
        data_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# rollback()
# ---------------------------------------------------------------------------


class TestRollback:
    def _setup(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        with patch("fflow.destinations.sql.create_engine", return_value=engine), \
             patch("fflow.destinations.sql.inspect", side_effect=_make_inspect_side([])):
            dest = _ConcreteDest(_cfg())
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))
        return dest, data_conn

    def test_rollback_calls_conn_rollback(self):
        dest, data_conn = self._setup()
        dest.write("orders", [{"id": 1}])
        dest.rollback("orders")
        data_conn.rollback.assert_called_once()

    def test_buffer_popped_after_rollback(self):
        dest, _ = self._setup()
        dest.rollback("orders")
        assert "orders" not in dest._buffers

    def test_rollback_unknown_stream_is_noop(self):
        dest, _ = self._setup()
        dest.rollback("nonexistent")  # must not raise


# ---------------------------------------------------------------------------
# check()
# ---------------------------------------------------------------------------


class TestCheck:
    def test_check_success(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        with patch("fflow.destinations.sql.create_engine", return_value=engine):
            dest = _ConcreteDest(_cfg())
            dest.check()
        conn.execute.assert_called_once()

    def test_check_failure_propagates(self):
        engine, ddl_conn, data_conn = _make_engine_mocks()
        conn = MagicMock()
        conn.execute.side_effect = RuntimeError("DB offline")
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        with patch("fflow.destinations.sql.create_engine", return_value=engine):
            dest = _ConcreteDest(_cfg())
            with pytest.raises(RuntimeError, match="DB offline"):
                dest.check()
