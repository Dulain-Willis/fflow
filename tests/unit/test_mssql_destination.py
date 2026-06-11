# tests/unit/test_mssql_destination.py
#
# All pyodbc calls are intercepted by conftest.py which stubs the module with
# MagicMock() before any import.  Each test sets up its own mock_conn via the
# patch_pyodbc fixture.

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from fflow.common.config import SchemaContract, StreamConfig
from fflow.common.schema import Column, ColumnType, IncrementalConfig, Stream
from fflow.destinations.mssql import (
    MSSQLConnectionConfig,
    MSSQLDestination,
    MSSQLStreamConfig,
    SchemaContractViolation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stream(
    name: str = "orders",
    cols: list[Column] | None = None,
    cursor_field: str | None = None,
) -> Stream:
    if cols is None:
        cols = [
            Column(name="id", type=ColumnType.integer, primary_key=True),
            Column(name="amount", type=ColumnType.decimal),
        ]
    incremental = IncrementalConfig(
        cursor_type="integer" if cursor_field else "none",
        cursor_field=cursor_field,
    )
    return Stream(name=name, columns=cols, incremental=incremental)


def _dest(
    conn_cfg: MSSQLConnectionConfig | None = None,
    stream_cfgs: dict | None = None,
    contract: SchemaContract | None = None,
) -> MSSQLDestination:
    if conn_cfg is None:
        conn_cfg = MSSQLConnectionConfig(connection_string="DSN=test", dest_schema="ODS")
    if stream_cfgs is None:
        stream_cfgs = {"orders": MSSQLStreamConfig(target_table="orders_tbl")}
    if contract is None:
        contract = SchemaContract()
    return MSSQLDestination(connection=conn_cfg, streams=stream_cfgs, contract=contract)


def _cursor_rows(rows: list) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    return cur


def _make_execute_side(existing_cols: list[str]):
    """Return a side_effect fn: sys.columns query → rows; DDL → plain mock."""

    def _side(sql, *args):
        if "sys.columns" in sql:
            return _cursor_rows([(c,) for c in existing_cols])
        return MagicMock()

    return _side


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_conn() -> MagicMock:
    conn = MagicMock()
    conn.autocommit = False
    conn.cursor.return_value = MagicMock()
    return conn


@pytest.fixture(autouse=True)
def patch_pyodbc(mock_conn: MagicMock):
    import pyodbc  # noqa: PLC0415 (already a MagicMock from conftest)

    pyodbc.connect.reset_mock()
    pyodbc.connect.side_effect = None
    pyodbc.connect.return_value = mock_conn
    yield pyodbc
    pyodbc.connect.reset_mock()
    pyodbc.connect.side_effect = None


# ---------------------------------------------------------------------------
# check()
# ---------------------------------------------------------------------------


class TestCheck:
    def test_success(self, mock_conn: MagicMock) -> None:
        _dest().check()
        mock_conn.close.assert_called_once()

    def test_failure(self, patch_pyodbc: MagicMock) -> None:
        patch_pyodbc.connect.side_effect = RuntimeError("ODBC error")
        with pytest.raises(RuntimeError, match="ODBC error"):
            _dest().check()


# ---------------------------------------------------------------------------
# prepare_stream()
# ---------------------------------------------------------------------------


class TestPrepareStream:
    def test_creates_table_when_not_exists(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

        calls = [str(c) for c in mock_conn.execute.call_args_list]
        create_calls = [c for c in calls if "CREATE TABLE" in c]
        assert len(create_calls) == 1
        assert "orders_tbl" in create_calls[0]

    def test_dest_columns_set_correctly_new_table(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

        assert dest._buffers["orders"].dest_columns == ["id", "amount"]

    def test_cdc_cols_excluded_from_ddl(self, mock_conn: MagicMock) -> None:
        cols = [
            Column(name="id", type=ColumnType.integer, primary_key=True),
            Column(name="STTRCID", type=ColumnType.integer),
            Column(name="STTRCTRIGGER", type=ColumnType.string),
        ]
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest(stream_cfgs={"orders": MSSQLStreamConfig(target_table="orders_tbl")})
        dest.prepare_stream("orders", _stream(cols=cols), StreamConfig(name="orders"))

        calls = [str(c) for c in mock_conn.execute.call_args_list]
        create_calls = [c for c in calls if "CREATE TABLE" in c]
        assert "STTRCID" not in create_calls[0]
        assert "STTRCTRIGGER" not in create_calls[0]

    def test_evolve_alters_table_for_new_column(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side(["id"])  # 'amount' is new
        dest = _dest(contract=SchemaContract(on_new_column="evolve"))
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

        calls = [str(c) for c in mock_conn.execute.call_args_list]
        alter_calls = [c for c in calls if "ALTER TABLE" in c]
        assert len(alter_calls) == 1
        assert "amount" in alter_calls[0]

    def test_evolve_includes_new_col_in_dest_columns(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side(["id"])
        dest = _dest(contract=SchemaContract(on_new_column="evolve"))
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

        assert "amount" in dest._buffers["orders"].dest_columns

    def test_freeze_raises_on_new_column(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side(["id"])
        dest = _dest(contract=SchemaContract(on_new_column="freeze"))
        with pytest.raises(SchemaContractViolation, match="amount"):
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

    def test_discard_excludes_new_column(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side(["id"])
        dest = _dest(contract=SchemaContract(on_new_column="discard"))
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

        buf = dest._buffers["orders"]
        assert "amount" not in buf.dest_columns
        assert "id" in buf.dest_columns

    def test_freeze_raises_on_dropped_column(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side(["id", "amount", "old_col"])
        dest = _dest(contract=SchemaContract(on_dropped_column="freeze"))
        with pytest.raises(SchemaContractViolation, match="old_col"):
            dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))

    def test_evolve_dropped_col_does_not_raise(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side(["id", "amount", "old_col"])
        dest = _dest(contract=SchemaContract(on_dropped_column="evolve"))
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))  # no raise

    def test_autocommit_true_during_ddl_false_after(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))
        # After prepare, autocommit should be False for DML phase.
        assert mock_conn.autocommit is False


# ---------------------------------------------------------------------------
# write()
# ---------------------------------------------------------------------------


class TestWrite:
    def _prepared(self, mock_conn: MagicMock, **kwargs) -> MSSQLDestination:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream("orders", _stream(**kwargs), StreamConfig(name="orders"))
        mock_conn.execute.reset_mock()
        return dest

    def test_buffers_rows(self, mock_conn: MagicMock) -> None:
        dest = self._prepared(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}, {"id": 2, "amount": 14.5}])
        assert len(dest._buffers["orders"].rows) == 2

    def test_accumulates_across_multiple_write_calls(self, mock_conn: MagicMock) -> None:
        dest = self._prepared(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.write("orders", [{"id": 2, "amount": 14.5}])
        assert len(dest._buffers["orders"].rows) == 2

    def test_sets_has_cdc_when_sttrcid_present(self, mock_conn: MagicMock) -> None:
        dest = self._prepared(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99, "STTRCID": 100, "STTRCTRIGGER": "I"}])
        assert dest._buffers["orders"].has_cdc is True

    def test_has_cdc_false_without_sttrcid(self, mock_conn: MagicMock) -> None:
        dest = self._prepared(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        assert dest._buffers["orders"].has_cdc is False

    def test_has_cdc_false_when_sttrcid_is_none(self, mock_conn: MagicMock) -> None:
        dest = self._prepared(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99, "STTRCID": None}])
        assert dest._buffers["orders"].has_cdc is False


# ---------------------------------------------------------------------------
# commit() — append
# ---------------------------------------------------------------------------


class TestCommitAppend:
    def _setup(self, mock_conn: MagicMock) -> MSSQLDestination:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream(
            "orders", _stream(), StreamConfig(name="orders", write_disposition="append")
        )
        mock_conn.execute.reset_mock()
        return dest

    def test_inserts_rows(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        cur = mock_conn.cursor.return_value
        cur.executemany.assert_called_once()
        sql, _ = cur.executemany.call_args[0]
        assert "INSERT INTO" in sql
        assert "orders_tbl" in sql
        assert cur.fast_executemany is True
        mock_conn.commit.assert_called_once()

    def test_commit_no_rows_still_commits(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.commit("orders")
        mock_conn.commit.assert_called_once()
        mock_conn.cursor.return_value.executemany.assert_not_called()

    def test_cdc_cols_excluded_from_insert(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99, "STTRCID": 100, "STTRCTRIGGER": "I"}])
        dest.commit("orders")

        cur = mock_conn.cursor.return_value
        sql, values = cur.executemany.call_args[0]
        assert "STTRCID" not in sql
        assert "STTRCTRIGGER" not in sql
        # values use row.get() so STTRCID/STTRCTRIGGER are absent from columns
        assert len(values[0]) == 2  # id, amount only

    def test_rows_cleared_after_commit(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")
        assert "orders" not in dest._buffers


# ---------------------------------------------------------------------------
# commit() — replace
# ---------------------------------------------------------------------------


class TestCommitReplace:
    def _setup(self, mock_conn: MagicMock) -> MSSQLDestination:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream(
            "orders", _stream(), StreamConfig(name="orders", write_disposition="replace")
        )
        mock_conn.execute.reset_mock()
        return dest

    def test_truncates_then_inserts(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        first_call = str(mock_conn.execute.call_args_list[0])
        assert "TRUNCATE TABLE" in first_call
        assert "orders_tbl" in first_call
        mock_conn.cursor.return_value.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_truncate_with_no_rows_still_commits(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.commit("orders")

        first_call = str(mock_conn.execute.call_args_list[0])
        assert "TRUNCATE TABLE" in first_call
        mock_conn.commit.assert_called_once()
        mock_conn.cursor.return_value.executemany.assert_not_called()

    def test_rows_cleared_after_commit(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")
        assert "orders" not in dest._buffers


# ---------------------------------------------------------------------------
# commit() — merge
# ---------------------------------------------------------------------------


class TestCommitMerge:
    def _setup(
        self,
        mock_conn: MagicMock,
        stream: Stream | None = None,
        merge_key: list[str] | None = None,
    ) -> MSSQLDestination:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream(
            "orders",
            stream or _stream(),
            StreamConfig(
                name="orders",
                write_disposition="merge",
                merge_key=merge_key or ["id"],
            ),
        )
        mock_conn.execute.reset_mock()
        return dest

    def _exec_sqls(self, mock_conn: MagicMock) -> list[str]:
        return [str(c) for c in mock_conn.execute.call_args_list]

    def test_creates_staging_table(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        assert any("CREATE TABLE #fflow_staging" in s for s in sqls)

    def test_dedup_sql_present(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        assert any("ROW_NUMBER" in s for s in sqls)

    def test_deletes_from_target_before_insert(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        delete_calls = [s for s in sqls if "DELETE" in s and "orders_tbl" in s]
        insert_calls = [s for s in sqls if "INSERT INTO" in s and "orders_tbl" in s]
        assert len(delete_calls) >= 1
        assert len(insert_calls) >= 1

        # DELETE must appear before INSERT
        delete_idx = next(i for i, s in enumerate(sqls) if "DELETE" in s and "orders_tbl" in s)
        insert_idx = next(i for i, s in enumerate(sqls) if "INSERT INTO" in s and "orders_tbl" in s)
        assert delete_idx < insert_idx

    def test_cdc_delete_row_not_inserted(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99, "STTRCID": 101, "STTRCTRIGGER": "D"}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        insert_calls = [s for s in sqls if "INSERT INTO" in s and "orders_tbl" in s]
        assert len(insert_calls) == 1
        # INSERT must exclude D rows
        assert "STTRCTRIGGER" in insert_calls[0]
        assert "'D'" in insert_calls[0]

    def test_cdc_sttrcid_used_in_dedup_order(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [
            {"id": 1, "amount": 9.99, "STTRCID": 100, "STTRCTRIGGER": "U"},
            {"id": 1, "amount": 0.0, "STTRCID": 101, "STTRCTRIGGER": "D"},
        ])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        dedup_calls = [s for s in sqls if "ROW_NUMBER" in s]
        assert len(dedup_calls) == 1
        assert "STTRCID" in dedup_calls[0]

    def test_no_cdc_uses_cursor_field_for_ordering(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn, stream=_stream(cursor_field="amount"))
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        dedup_calls = [s for s in sqls if "ROW_NUMBER" in s]
        assert len(dedup_calls) == 1
        assert "amount" in dedup_calls[0]
        assert "STTRCID" not in dedup_calls[0]

    def test_no_cursor_no_cdc_dedup_uses_select_null(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        dedup_calls = [s for s in sqls if "ROW_NUMBER" in s]
        assert len(dedup_calls) == 1
        assert "SELECT NULL" in dedup_calls[0]

    def test_empty_rows_commits_without_staging(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        assert not any("CREATE TABLE #fflow_staging" in s for s in sqls)
        mock_conn.commit.assert_called_once()

    def test_staging_dropped_after_commit(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        assert any("DROP TABLE #fflow_staging" in s for s in sqls)

    def test_rows_cleared_after_commit(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")
        assert "orders" not in dest._buffers

    def test_staging_includes_cdc_cols(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        create_staging = next(s for s in sqls if "CREATE TABLE #fflow_staging" in s)
        assert "STTRCID" in create_staging
        assert "STTRCTRIGGER" in create_staging

    def test_bulk_insert_to_staging_includes_cdc_cols(self, mock_conn: MagicMock) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99, "STTRCID": 5, "STTRCTRIGGER": "I"}])
        dest.commit("orders")

        cur = mock_conn.cursor.return_value
        sql, values = cur.executemany.call_args[0]
        assert "#fflow_staging" in sql
        assert "STTRCID" in sql
        assert "STTRCTRIGGER" in sql

    def test_target_insert_excludes_cdc_cols_from_column_list(
        self, mock_conn: MagicMock
    ) -> None:
        dest = self._setup(mock_conn)
        dest.write("orders", [{"id": 1, "amount": 9.99, "STTRCID": 5, "STTRCTRIGGER": "I"}])
        dest.commit("orders")

        sqls = self._exec_sqls(mock_conn)
        target_insert = next(s for s in sqls if "INSERT INTO" in s and "orders_tbl" in s)
        # CDC cols must not appear in the SELECT column list.
        # They may appear in the WHERE clause (STTRCTRIGGER filter).
        assert "STTRCID" not in target_insert
        # Extract the SELECT column list (between SELECT and FROM)
        import re
        m = re.search(r"SELECT (.+?) FROM #fflow_staging", target_insert)
        assert m is not None
        select_cols = m.group(1)
        assert "STTRCID" not in select_cols
        assert "STTRCTRIGGER" not in select_cols


# ---------------------------------------------------------------------------
# rollback()
# ---------------------------------------------------------------------------


class TestRollback:
    def test_rollback_calls_conn_rollback_and_clears_buffer(
        self, mock_conn: MagicMock
    ) -> None:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream("orders", _stream(), StreamConfig(name="orders"))
        dest.write("orders", [{"id": 1, "amount": 9.99}])

        dest.rollback("orders")

        mock_conn.rollback.assert_called_once()
        assert "orders" not in dest._buffers
        # buffer removed — connection closed via finally in rollback()

    def test_rollback_unknown_stream_is_noop(self, mock_conn: MagicMock) -> None:
        _dest().rollback("nonexistent")  # must not raise


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


class TestCommitFailureHandling:
    def test_insert_failure_triggers_rollback_and_clears_buffer(
        self, mock_conn: MagicMock
    ) -> None:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream(
            "orders", _stream(), StreamConfig(name="orders", write_disposition="append")
        )

        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = RuntimeError("Insert failed")
        mock_conn.cursor.return_value = mock_cursor

        dest.write("orders", [{"id": 1, "amount": 9.99}])

        with pytest.raises(RuntimeError, match="Insert failed"):
            dest.commit("orders")

        mock_conn.rollback.assert_called_once()
        assert "orders" not in dest._buffers

    def test_merge_failure_triggers_rollback(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = _make_execute_side([])
        dest = _dest()
        dest.prepare_stream(
            "orders",
            _stream(),
            StreamConfig(name="orders", write_disposition="merge", merge_key=["id"]),
        )
        mock_conn.execute.reset_mock()

        # Fail on the DELETE step (4th execute call in merge)
        call_count = {"n": 0}
        original_side = _make_execute_side([])

        def failing_execute(sql, *args):
            call_count["n"] += 1
            if "DELETE" in sql and "orders_tbl" in sql:
                raise RuntimeError("Delete failed")
            return original_side(sql, *args)

        mock_conn.execute.side_effect = failing_execute

        dest.write("orders", [{"id": 1, "amount": 9.99}])

        with pytest.raises(RuntimeError, match="Delete failed"):
            dest.commit("orders")

        mock_conn.rollback.assert_called_once()
        assert "orders" not in dest._buffers
