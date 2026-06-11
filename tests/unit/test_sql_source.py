# tests/unit/test_sql_source.py
#
# SQLSource unit tests — all DB calls mocked via patch on create_engine.

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from fflow.common.schema import ColumnType, IncrementalConfig
from fflow.sources.sql import SQLConnectionConfig, SQLSource, SQLStreamConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _conn_cfg() -> SQLConnectionConfig:
    return SQLConnectionConfig(connection_url="sqlite:///:memory:")


def _table_cfg(table: str = "orders", schema_: str | None = None, **kwargs) -> SQLStreamConfig:
    return SQLStreamConfig(table=table, schema_=schema_, **kwargs)


def _sql_file_cfg(sql_file: str = "query.sql", **kwargs) -> SQLStreamConfig:
    return SQLStreamConfig(sql_file=sql_file, **kwargs)


def _mock_engine():
    """Return a (engine, conn_ctx) pair where conn_ctx is the mock SA connection."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    engine.connect.return_value.__exit__.return_value = None
    return engine, conn


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestSQLStreamConfig:
    def test_table_only_valid(self):
        cfg = SQLStreamConfig(table="orders")
        assert cfg.table == "orders"

    def test_sql_file_only_valid(self):
        cfg = SQLStreamConfig(sql_file="q.sql")
        assert cfg.sql_file == "q.sql"

    def test_neither_raises(self):
        with pytest.raises(ValueError):
            SQLStreamConfig()

    def test_both_raises(self):
        with pytest.raises(ValueError):
            SQLStreamConfig(table="orders", sql_file="q.sql")


# ---------------------------------------------------------------------------
# check()
# ---------------------------------------------------------------------------


class TestCheck:
    def test_check_success(self):
        engine, conn = _mock_engine()
        with patch("fflow.sources.sql.create_engine", return_value=engine):
            src = SQLSource(_conn_cfg(), {"orders": _table_cfg()})
            src.check()
            conn.execute.assert_called_once()

    def test_check_failure_propagates(self):
        engine, conn = _mock_engine()
        conn.execute.side_effect = RuntimeError("DB offline")
        with patch("fflow.sources.sql.create_engine", return_value=engine):
            src = SQLSource(_conn_cfg(), {"orders": _table_cfg()})
            with pytest.raises(RuntimeError, match="DB offline"):
                src.check()


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------


class TestDiscover:
    def _mock_inspect(self, cols, pk_cols=None):
        insp = MagicMock()
        insp.get_columns.return_value = cols
        insp.get_pk_constraint.return_value = {
            "constrained_columns": pk_cols or []
        }
        return insp

    def test_discovers_table_columns(self):
        from sqlalchemy.sql import sqltypes
        engine, conn = _mock_engine()
        insp = self._mock_inspect(
            [
                {"name": "id", "type": sqltypes.Integer(), "nullable": False},
                {"name": "amount", "type": sqltypes.Numeric(precision=18, scale=4), "nullable": True},
            ],
            pk_cols=["id"],
        )
        with patch("fflow.sources.sql.create_engine", return_value=engine), \
             patch("fflow.sources.sql.inspect", return_value=insp):
            src = SQLSource(_conn_cfg(), {"orders": _table_cfg()})
            schema = src.discover()

        assert schema.stream_names == ["orders"]
        stream = schema.get_stream("orders")
        assert len(stream.columns) == 2
        id_col = stream.get_column("id")
        assert id_col.type == ColumnType.integer
        assert id_col.primary_key is True

    def test_sql_file_stream_has_empty_columns(self):
        engine, conn = _mock_engine()
        with patch("fflow.sources.sql.create_engine", return_value=engine), \
             patch("fflow.sources.sql.inspect", return_value=MagicMock()):
            src = SQLSource(_conn_cfg(), {"orders": _sql_file_cfg()})
            schema = src.discover()

        assert schema.get_stream("orders").columns == []


# ---------------------------------------------------------------------------
# read() — table mode
# ---------------------------------------------------------------------------


class TestReadTableMode:
    def _setup(self, rows, incremental=None, **table_kwargs):
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None

        result = MagicMock()
        result.keys.return_value = list(rows[0].keys()) if rows else ["id"]
        result.__iter__.return_value = iter([tuple(r.values()) for r in rows])
        conn.execute.return_value = result

        inc = incremental or IncrementalConfig()
        cfg = SQLStreamConfig(
            table="orders",
            incremental=inc,
            **table_kwargs,
        )
        src = SQLSource(_conn_cfg(), {"orders": cfg})
        src._engine = engine  # bypass the real engine created at init
        return engine, src

    def test_read_yields_all_rows(self):
        rows = [{"id": 1}, {"id": 2}]
        engine, src = self._setup(rows)
        with patch("fflow.sources.sql.create_engine", return_value=engine):
            result = list(src.read("orders", {}))
        assert result == rows

    def test_read_with_integer_cursor_injects_where(self):
        rows = [{"id": 5}]
        inc = IncrementalConfig(cursor_type="integer", cursor_field="id")
        engine, src = self._setup(rows, incremental=inc)
        state = {"cursor_value": 3}
        with patch("fflow.sources.sql.create_engine", return_value=engine):
            list(src.read("orders", state))

        sql_arg = str(engine.connect.return_value.__enter__.return_value.execute.call_args[0][0])
        assert 'WHERE "id" > 3' in sql_arg

    def test_read_updates_state_with_max_cursor(self):
        rows = [{"id": 5}, {"id": 8}]
        inc = IncrementalConfig(cursor_type="integer", cursor_field="id")
        engine, src = self._setup(rows, incremental=inc)

        result_mock = MagicMock()
        result_mock.keys.return_value = ["id"]
        result_mock.__iter__.return_value = iter([(5,), (8,)])
        engine.connect.return_value.__enter__.return_value.execute.return_value = result_mock

        state = {}
        with patch("fflow.sources.sql.create_engine", return_value=engine):
            list(src.read("orders", state))
        assert state["cursor_value"] == 8

    def test_read_respects_chunk_size(self):
        rows = [{"id": i} for i in range(5)]
        engine, src = self._setup(rows, chunk_size=2)
        result_mock = MagicMock()
        result_mock.keys.return_value = ["id"]
        result_mock.__iter__.return_value = iter([(i,) for i in range(5)])
        engine.connect.return_value.__enter__.return_value.execute.return_value = result_mock

        with patch("fflow.sources.sql.create_engine", return_value=engine):
            result = list(src.read("orders", {}))
        assert len(result) == 5  # all rows yielded regardless of chunk size


# ---------------------------------------------------------------------------
# read() — SQL-file mode
# ---------------------------------------------------------------------------


class TestReadSqlFileMode:
    def test_sql_file_executed(self, tmp_path: Path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT * FROM orders", encoding="utf-8")

        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        result_mock = MagicMock()
        result_mock.keys.return_value = ["id"]
        result_mock.__iter__.return_value = iter([(1,)])
        conn.execute.return_value = result_mock

        cfg = SQLStreamConfig(sql_file=str(sql_file))
        with patch("fflow.sources.sql.create_engine", return_value=engine):
            src = SQLSource(_conn_cfg(), {"q": cfg})
            rows = list(src.read("q", {}))

        assert rows == [{"id": 1}]
        executed_sql = str(conn.execute.call_args[0][0])
        assert "SELECT * FROM orders" in executed_sql

    def test_cursor_substituted_in_sql_file(self, tmp_path: Path):
        sql_file = tmp_path / "inc.sql"
        sql_file.write_text("SELECT * FROM orders WHERE id > {{cursor_value}}", encoding="utf-8")

        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        result_mock = MagicMock()
        result_mock.keys.return_value = ["id"]
        result_mock.__iter__.return_value = iter([])
        conn.execute.return_value = result_mock

        inc = IncrementalConfig(cursor_type="integer", cursor_field="id")
        cfg = SQLStreamConfig(sql_file=str(sql_file), incremental=inc)
        with patch("fflow.sources.sql.create_engine", return_value=engine):
            src = SQLSource(_conn_cfg(), {"q": cfg})
            list(src.read("q", {"cursor_value": 42}))

        executed_sql = str(conn.execute.call_args[0][0])
        assert "42" in executed_sql
        assert "{{cursor_value}}" not in executed_sql

    def test_missing_placeholder_raises_when_incremental(self, tmp_path: Path):
        sql_file = tmp_path / "bad.sql"
        sql_file.write_text("SELECT * FROM orders", encoding="utf-8")

        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = MagicMock()
        engine.connect.return_value.__exit__.return_value = None

        inc = IncrementalConfig(cursor_type="integer", cursor_field="id")
        cfg = SQLStreamConfig(sql_file=str(sql_file), incremental=inc)
        with patch("fflow.sources.sql.create_engine", return_value=engine):
            src = SQLSource(_conn_cfg(), {"q": cfg})
            with pytest.raises(ValueError, match="cursor_value"):
                list(src.read("q", {"cursor_value": 10}))
