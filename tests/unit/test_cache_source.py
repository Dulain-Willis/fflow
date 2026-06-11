"""Unit tests for CacheSource.

All external I/O (jaydebeapi, subprocess, filesystem) is mocked.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, call, patch

import pytest

from fflow.sources.cache import (
    CacheConnectionConfig,
    CacheSource,
    CacheStreamConfig,
    _CacheConnection,
    _build_incremental_sql,
    _run_shuttle,
)
from fflow.common.schema import IncrementalConfig, Schema, Stream


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CONN_CFG = CacheConnectionConfig(
    url="jdbc:IRIS://host:1972/RECPROD",
    user="user",
    password="pass",
    jdbc_jar="/opt/cache.jar",
)


def _mirror_cfg(**kwargs) -> CacheStreamConfig:
    return CacheStreamConfig(table="ARPHONE", **kwargs)


def _sql_file_cfg(path: str, **kwargs) -> CacheStreamConfig:
    return CacheStreamConfig(sql_file=path, **kwargs)


def _make_source(stream_cfgs: dict, shuttle_jar=None) -> CacheSource:
    return CacheSource(CONN_CFG, stream_cfgs, shuttle_jar=shuttle_jar)


def _mock_conn() -> MagicMock:
    """Return a mock _CacheConnection."""
    conn = MagicMock(spec=_CacheConnection)
    conn.connect.return_value = conn
    return conn


# ---------------------------------------------------------------------------
# CacheStreamConfig validation
# ---------------------------------------------------------------------------


class TestCacheStreamConfig:
    def test_table_and_sql_file_both_set_raises(self):
        with pytest.raises(ValueError, match="Exactly one"):
            CacheStreamConfig(table="T", sql_file="f.sql")

    def test_neither_set_raises(self):
        with pytest.raises(ValueError, match="Exactly one"):
            CacheStreamConfig()

    def test_use_shuttle_without_table_raises(self):
        with pytest.raises(ValueError, match="use_shuttle.*table"):
            CacheStreamConfig(sql_file="f.sql", use_shuttle=True, shuttle_target_table="ODS.T")

    def test_use_shuttle_without_target_raises(self):
        with pytest.raises(ValueError, match="shuttle_target_table"):
            CacheStreamConfig(table="T", use_shuttle=True)

    def test_valid_mirror(self):
        cfg = CacheStreamConfig(table="ARPHONE")
        assert cfg.table == "ARPHONE"

    def test_valid_sql_file(self):
        cfg = CacheStreamConfig(sql_file="q.sql")
        assert cfg.sql_file == "q.sql"

    def test_valid_shuttle(self):
        cfg = CacheStreamConfig(table="T", use_shuttle=True, shuttle_target_table="ODS.T")
        assert cfg.use_shuttle is True


# ---------------------------------------------------------------------------
# _build_incremental_sql
# ---------------------------------------------------------------------------


class TestBuildIncrementalSql:
    def test_contains_union_all(self):
        sql = _build_incremental_sql("ARPHONE", ["ID", "NAME"], "ID", 100)
        assert "UNION ALL" in sql

    def test_start_cid_embedded_as_literal(self):
        sql = _build_incremental_sql("ARPHONE", ["ID", "NAME"], "ID", 12345)
        assert "12345" in sql
        assert "?" not in sql  # no JDBC params

    def test_d_branch_nulls_non_pk(self):
        sql = _build_incremental_sql("ARPHONE", ["ID", "NAME", "PHONE"], "ID", 1)
        assert "NULL AS NAME" in sql
        assert "NULL AS PHONE" in sql
        assert "STTRCKEY AS ID" in sql

    def test_delete_filter_present(self):
        sql = _build_incremental_sql("ARPHONE", ["ID"], "ID", 50)
        assert "'D'" in sql
        assert "STTRCTRIGGER" in sql

    def test_custom_cid_and_trigger_columns(self):
        sql = _build_incremental_sql(
            "T", ["ID"], "ID", 1,
            cid_column="MY_CID", trigger_column="MY_TRIG",
        )
        assert "MY_CID" in sql
        assert "MY_TRIG" in sql


# ---------------------------------------------------------------------------
# _run_shuttle
# ---------------------------------------------------------------------------


class TestRunShuttle:
    def test_raises_if_jar_missing(self):
        with pytest.raises(RuntimeError, match="JAR not found"):
            _run_shuttle(
                shuttle_jar="/no/such/file.jar",
                cache_url="url", cache_user="u", cache_password="p",
                sql="SELECT 1", target_table="ODS.T",
            )

    def test_raises_if_env_vars_missing(self, tmp_path):
        jar = tmp_path / "shuttle.jar"
        jar.write_bytes(b"fake")
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(RuntimeError, match="MSSQL_JDBC_URL"):
                _run_shuttle(
                    shuttle_jar=str(jar),
                    cache_url="url", cache_user="u", cache_password="p",
                    sql="SELECT 1", target_table="ODS.T",
                )

    def test_raises_on_nonzero_exit(self, tmp_path, monkeypatch):
        jar = tmp_path / "shuttle.jar"
        jar.write_bytes(b"fake")
        monkeypatch.setenv("MSSQL_JDBC_URL", "jdbc:sqlserver://host")
        monkeypatch.setenv("MSSQL_USER", "sa")
        monkeypatch.setenv("MSSQL_PASSWORD", "pw")

        failed = MagicMock()
        failed.returncode = 1
        with patch("subprocess.run", return_value=failed):
            with pytest.raises(RuntimeError, match="exitcode 1|code 1"):
                _run_shuttle(
                    shuttle_jar=str(jar),
                    cache_url="url", cache_user="u", cache_password="p",
                    sql="SELECT 1", target_table="ODS.T",
                )

    def test_success_calls_java_with_correct_args(self, tmp_path, monkeypatch):
        jar = tmp_path / "shuttle.jar"
        jar.write_bytes(b"fake")
        monkeypatch.setenv("MSSQL_JDBC_URL", "jdbc:sqlserver://host")
        monkeypatch.setenv("MSSQL_USER", "sa")
        monkeypatch.setenv("MSSQL_PASSWORD", "pw")

        ok = MagicMock()
        ok.returncode = 0
        with patch("subprocess.run", return_value=ok) as mock_run:
            _run_shuttle(
                shuttle_jar=str(jar),
                cache_url="url", cache_user="u", cache_password="p",
                sql="SELECT ID FROM T",
                target_table="ODS.ARPHONE",
            )
        cmd = mock_run.call_args[0][0]
        assert "--sql" in cmd
        assert "SELECT ID FROM T" in cmd
        assert "--target-table" in cmd
        assert "ODS.ARPHONE" in cmd


# ---------------------------------------------------------------------------
# CacheSource.check()
# ---------------------------------------------------------------------------


class TestCacheSourceCheck:
    def test_check_connects_and_closes(self):
        src = _make_source({"phone": _mirror_cfg()})
        conn = _mock_conn()
        with patch.object(src, "_new_conn", return_value=conn):
            src.check()
        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# CacheSource.discover() — mirror mode
# ---------------------------------------------------------------------------


SAMPLE_COL_ROWS = [
    ("ID", "INTEGER", None, 10, 0, "NO", 1),
    ("NAME", "VARCHAR", 100, None, None, "YES", 2),
]
SAMPLE_PK_ROW = ("ID",)


class TestDiscoverMirror:
    def _patched_source(self) -> tuple[CacheSource, MagicMock]:
        src = _make_source({"phone": _mirror_cfg()})
        conn = _mock_conn()
        conn.execute_all.return_value = SAMPLE_COL_ROWS
        conn.execute_one.return_value = SAMPLE_PK_ROW
        return src, conn

    def test_returns_schema_with_correct_stream_name(self):
        src, conn = self._patched_source()
        with patch.object(src, "_new_conn", return_value=conn):
            schema = src.discover()
        assert schema.stream_names == ["phone"]

    def test_column_types_mapped(self):
        src, conn = self._patched_source()
        with patch.object(src, "_new_conn", return_value=conn):
            schema = src.discover()
        stream = schema.get_stream("phone")
        cols = {c.name: c for c in stream.columns}
        assert cols["ID"].type.value == "integer"
        assert cols["NAME"].type.value == "string"

    def test_primary_key_flagged(self):
        src, conn = self._patched_source()
        with patch.object(src, "_new_conn", return_value=conn):
            schema = src.discover()
        stream = schema.get_stream("phone")
        pk_cols = [c.name for c in stream.columns if c.primary_key]
        assert pk_cols == ["ID"]

    def test_result_cached(self):
        src, conn = self._patched_source()
        with patch.object(src, "_new_conn", return_value=conn) as mock_new:
            src.discover()
            src.discover()
        assert mock_new.call_count == 1  # only one connection opened


# ---------------------------------------------------------------------------
# CacheSource.discover() — SQL-file mode
# ---------------------------------------------------------------------------


class TestDiscoverSqlFile:
    def test_returns_unknown_type_columns(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT ID, NAME FROM T")
        src = _make_source({"q": _sql_file_cfg(str(sql_file))})
        conn = _mock_conn()
        conn.get_col_names.return_value = ["ID", "NAME"]
        with patch.object(src, "_new_conn", return_value=conn):
            schema = src.discover()
        stream = schema.get_stream("q")
        assert [c.name for c in stream.columns] == ["ID", "NAME"]

    def test_cursor_placeholder_substituted_for_discovery(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT ID, STTRCID FROM T WHERE STTRCID > {{cursor_value}}")
        src = _make_source({"q": _sql_file_cfg(str(sql_file))})
        conn = _mock_conn()
        conn.get_col_names.return_value = ["ID", "STTRCID"]
        with patch.object(src, "_new_conn", return_value=conn):
            src.discover()
        actual_sql = conn.get_col_names.call_args[0][0]
        assert "{{cursor_value}}" not in actual_sql
        assert "0" in actual_sql


# ---------------------------------------------------------------------------
# CacheSource.read() — unknown stream
# ---------------------------------------------------------------------------


def test_read_unknown_stream_raises():
    src = _make_source({"phone": _mirror_cfg()})
    with pytest.raises(ValueError, match="unknown stream"):
        list(src.read("nope", {}))


# ---------------------------------------------------------------------------
# CacheSource.read() — mirror, full refresh (cursor_type="none")
# ---------------------------------------------------------------------------


class TestReadMirrorFullRefresh:
    def _rows(self) -> Iterator:
        yield [("1", "Alice"), ("2", "Bob")], ["ID", "NAME"]

    def test_yields_all_rows_as_dicts(self):
        src = _make_source({"phone": _mirror_cfg()})
        conn = _mock_conn()
        conn.execute_all.return_value = [
            ("ID", "INTEGER", None, 10, 0, "NO", 1),
            ("NAME", "VARCHAR", 100, None, None, "YES", 2),
        ]
        conn.execute_one.return_value = ("ID",)
        conn.fetch_iter.return_value = self._rows()
        with patch.object(src, "_new_conn", return_value=conn):
            rows = list(src.read("phone", {}))
        assert rows == [{"ID": "1", "NAME": "Alice"}, {"ID": "2", "NAME": "Bob"}]

    def test_state_unchanged_on_full_refresh(self):
        src = _make_source({"phone": _mirror_cfg()})
        conn = _mock_conn()
        conn.execute_all.return_value = [("ID", "INTEGER", None, 10, 0, "NO", 1)]
        conn.execute_one.return_value = ("ID",)
        conn.fetch_iter.return_value = iter([([("1",)], ["ID"])])
        state: dict = {}
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("phone", state))
        assert state == {}


# ---------------------------------------------------------------------------
# CacheSource.read() — mirror, first run (integer cursor, no shuttle)
# ---------------------------------------------------------------------------


class TestReadMirrorFirstRun:
    def test_seeds_state_with_pre_max_cid(self):
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"phone": _mirror_cfg(incremental=incr)})
        conn = _mock_conn()
        conn.execute_all.return_value = [("ID", "INTEGER", None, 10, 0, "NO", 1)]
        conn.execute_one.side_effect = [("ID",), (99,)]  # PK, then MAX(STTRCID)
        conn.fetch_iter.return_value = iter([([("1",)], ["ID"])])
        state: dict = {}
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("phone", state))
        assert state["STTRCID"] == 99

    def test_state_zero_when_sttrackchange_empty(self):
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"phone": _mirror_cfg(incremental=incr)})
        conn = _mock_conn()
        conn.execute_all.return_value = [("ID", "INTEGER", None, 10, 0, "NO", 1)]
        conn.execute_one.side_effect = [("ID",), (None,)]  # MAX returns NULL
        conn.fetch_iter.return_value = iter([])
        state: dict = {}
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("phone", state))
        assert state["STTRCID"] == 0


# ---------------------------------------------------------------------------
# CacheSource.read() — mirror, first run with shuttle
# ---------------------------------------------------------------------------


class TestReadMirrorShuttle:
    def test_yields_zero_rows_and_seeds_state(self, tmp_path):
        jar = tmp_path / "shuttle.jar"
        jar.write_bytes(b"fake")
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        cfg = CacheStreamConfig(
            table="ARPHONE",
            use_shuttle=True,
            shuttle_target_table="ODS.ARPHONE",
            incremental=incr,
        )
        src = _make_source({"phone": cfg}, shuttle_jar=str(jar))
        conn = _mock_conn()
        conn.execute_all.return_value = [("ID", "INTEGER", None, 10, 0, "NO", 1)]
        conn.execute_one.side_effect = [("ID",), (500,)]  # PK, MAX(STTRCID)
        state: dict = {}
        ok = MagicMock()
        ok.returncode = 0
        with patch.object(src, "_new_conn", return_value=conn):
            with patch.dict("os.environ", {
                "MSSQL_JDBC_URL": "jdbc:sqlserver://x",
                "MSSQL_USER": "sa",
                "MSSQL_PASSWORD": "pw",
            }):
                with patch("subprocess.run", return_value=ok):
                    rows = list(src.read("phone", state))
        assert rows == []
        assert state["STTRCID"] == 500


# ---------------------------------------------------------------------------
# CacheSource.read() — mirror, incremental CDC run
# ---------------------------------------------------------------------------


class TestReadMirrorIncremental:
    def test_advances_state_to_max_cid_in_rows(self):
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"phone": _mirror_cfg(incremental=incr)})
        conn = _mock_conn()
        conn.execute_all.return_value = [("ID", "INTEGER", None, 10, 0, "NO", 1)]
        conn.execute_one.return_value = ("ID",)
        cdc_rows = [
            {"ID": "1", "STTRCID": 101, "STTRCTRIGGER": "U"},
            {"ID": "2", "STTRCID": 150, "STTRCTRIGGER": "U"},
        ]
        raw_rows = [tuple(r.values()) for r in cdc_rows]
        col_hdrs = list(cdc_rows[0].keys())
        conn.fetch_iter.return_value = iter([(raw_rows, col_hdrs)])
        state = {"STTRCID": 100}
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("phone", state))
        assert state["STTRCID"] == 150

    def test_state_unchanged_if_no_rows(self):
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"phone": _mirror_cfg(incremental=incr)})
        conn = _mock_conn()
        conn.execute_all.return_value = [("ID", "INTEGER", None, 10, 0, "NO", 1)]
        conn.execute_one.return_value = ("ID",)
        conn.fetch_iter.return_value = iter([])
        state = {"STTRCID": 100}
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("phone", state))
        assert state["STTRCID"] == 100  # unchanged

    def test_cdc_sql_passed_to_fetch_iter(self):
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"phone": _mirror_cfg(incremental=incr)})
        conn = _mock_conn()
        conn.execute_all.return_value = [("ID", "INTEGER", None, 10, 0, "NO", 1)]
        conn.execute_one.return_value = ("ID",)
        conn.fetch_iter.return_value = iter([])
        state = {"STTRCID": 200}
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("phone", state))
        sql_used = conn.fetch_iter.call_args[0][0]
        assert "UNION ALL" in sql_used
        assert "200" in sql_used


# ---------------------------------------------------------------------------
# CacheSource.read() — SQL-file mode
# ---------------------------------------------------------------------------


class TestReadSqlFile:
    def test_full_refresh_yields_rows(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT ID FROM T")
        src = _make_source({"q": _sql_file_cfg(str(sql_file))})
        conn = _mock_conn()
        conn.fetch_iter.return_value = iter([([("1",), ("2",)], ["ID"])])
        with patch.object(src, "_new_conn", return_value=conn):
            rows = list(src.read("q", {}))
        assert rows == [{"ID": "1"}, {"ID": "2"}]

    def test_first_run_substitutes_zero(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT ID, STTRCID FROM T WHERE STTRCID > {{cursor_value}}")
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"q": _sql_file_cfg(str(sql_file), incremental=incr)})
        conn = _mock_conn()
        conn.fetch_iter.return_value = iter([])
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("q", {}))
        sql_used = conn.fetch_iter.call_args[0][0]
        assert "{{cursor_value}}" not in sql_used
        assert "> 0" in sql_used

    def test_incremental_substitutes_cursor(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT ID, STTRCID FROM T WHERE STTRCID > {{cursor_value}}")
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"q": _sql_file_cfg(str(sql_file), incremental=incr)})
        conn = _mock_conn()
        conn.fetch_iter.return_value = iter([])
        state = {"STTRCID": 300}
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("q", state))
        sql_used = conn.fetch_iter.call_args[0][0]
        assert "> 300" in sql_used

    def test_missing_placeholder_raises_when_cursor_set(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT ID FROM T")  # no {{cursor_value}}
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"q": _sql_file_cfg(str(sql_file), incremental=incr)})
        conn = _mock_conn()
        state = {"STTRCID": 100}
        with patch.object(src, "_new_conn", return_value=conn):
            with pytest.raises(ValueError, match="cursor_value"):
                list(src.read("q", state))

    def test_advances_state_to_max_cid_in_rows(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT ID, STTRCID FROM T WHERE STTRCID > {{cursor_value}}")
        incr = IncrementalConfig(cursor_type="integer", cursor_field="STTRCID")
        src = _make_source({"q": _sql_file_cfg(str(sql_file), incremental=incr)})
        conn = _mock_conn()
        conn.fetch_iter.return_value = iter([
            ([("1", 201), ("2", 250)], ["ID", "STTRCID"])
        ])
        state = {"STTRCID": 200}
        with patch.object(src, "_new_conn", return_value=conn):
            list(src.read("q", state))
        assert state["STTRCID"] == 250
