"""Unit tests for fflow.cli."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from fflow.cli import (
    _build_parser,
    _cmd_check,
    _cmd_list,
    _cmd_run,
    _cmd_state,
    _get_pipeline,
    _load_registry,
    _resolve_config_spec,
)
from fflow.common.config import StreamConfig
from fflow.pipeline.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pipeline(name: str, streams: list[str] | None = None) -> Pipeline:
    source = MagicMock()
    dest = MagicMock()
    state_store = MagicMock()
    stream_cfgs = [StreamConfig(name=s) for s in (streams or [])]
    return Pipeline(
        name=name,
        source=source,
        destination=dest,
        state_store=state_store,
        streams=stream_cfgs,
    )


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = dict(
        command="run",
        config=None,
        pipeline="test",
        stream=None,
        full_refresh=False,
        workers=5,
        chunk_size=1000,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# _resolve_config_spec
# ---------------------------------------------------------------------------

class TestResolveConfigSpec:
    def test_flag_takes_precedence(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CAPIO_FLOW_CONFIG", "env_module")
        (tmp_path / "pipelines.py").write_text("")
        monkeypatch.chdir(tmp_path)
        assert _resolve_config_spec("flag_module") == "flag_module"

    def test_env_var_fallback(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CAPIO_FLOW_CONFIG", "env_module")
        (tmp_path / "pipelines.py").write_text("")
        monkeypatch.chdir(tmp_path)
        assert _resolve_config_spec(None) == "env_module"

    def test_cwd_fallback(self, monkeypatch, tmp_path):
        monkeypatch.delenv("CAPIO_FLOW_CONFIG", raising=False)
        (tmp_path / "pipelines.py").write_text("")
        monkeypatch.chdir(tmp_path)
        result = _resolve_config_spec(None)
        assert result == str(tmp_path / "pipelines.py")

    def test_returns_none_when_nothing(self, monkeypatch, tmp_path):
        monkeypatch.delenv("CAPIO_FLOW_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)
        assert _resolve_config_spec(None) is None

    def test_flag_wins_over_env_and_cwd(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CAPIO_FLOW_CONFIG", "env_module")
        (tmp_path / "pipelines.py").write_text("")
        monkeypatch.chdir(tmp_path)
        assert _resolve_config_spec("explicit") == "explicit"


# ---------------------------------------------------------------------------
# _load_registry
# ---------------------------------------------------------------------------

class TestLoadRegistry:
    def _write_config(self, tmp_path: Path, content: str) -> str:
        p = tmp_path / "pipelines.py"
        p.write_text(content)
        return str(p)

    def _make_module(self, **pipeline_attrs):
        """Return a real module object with Pipeline instances as globals."""
        import types
        mod = types.ModuleType("test_pipelines")
        for name, value in pipeline_attrs.items():
            setattr(mod, name, value)
        return mod

    def test_loads_pipeline_instance(self, tmp_path):
        p = _make_pipeline("my_pipeline", ["s1", "s2"])
        config_path = self._write_config(tmp_path, "")
        module_mock = self._make_module(my_pipeline=p)
        with patch("importlib.util.spec_from_file_location") as mock_spec_from, \
             patch("importlib.util.module_from_spec") as mock_module_from:
            spec_mock = MagicMock()
            mock_spec_from.return_value = spec_mock
            mock_module_from.return_value = module_mock
            result = _load_registry(config_path)
        assert result == {"my_pipeline": p}

    def test_loads_multiple_pipelines(self, tmp_path):
        p1 = _make_pipeline("pipe_a")
        p2 = _make_pipeline("pipe_b")
        config_path = self._write_config(tmp_path, "")
        module_mock = self._make_module(pipe_a=p1, pipe_b=p2)
        with patch("importlib.util.spec_from_file_location") as mock_spec_from, \
             patch("importlib.util.module_from_spec") as mock_module_from:
            spec_mock = MagicMock()
            mock_spec_from.return_value = spec_mock
            mock_module_from.return_value = module_mock
            result = _load_registry(config_path)
        assert set(result.keys()) == {"pipe_a", "pipe_b"}

    def test_dies_on_none_spec(self):
        with pytest.raises(SystemExit):
            _load_registry(None)

    def test_dies_when_no_pipeline_instances(self, tmp_path):
        config_path = self._write_config(tmp_path, "")
        import types
        module_mock = types.ModuleType("empty_module")
        with patch("importlib.util.spec_from_file_location") as mock_spec_from, \
             patch("importlib.util.module_from_spec") as mock_module_from:
            spec_mock = MagicMock()
            mock_spec_from.return_value = spec_mock
            mock_module_from.return_value = module_mock
            with pytest.raises(SystemExit):
                _load_registry(config_path)

    def test_loads_via_dotted_module_name(self):
        p = _make_pipeline("mod_pipeline")
        import types
        module_mock = types.ModuleType("some.module")
        module_mock.mod_pipeline = p
        with patch("importlib.import_module") as mock_import:
            mock_import.return_value = module_mock
            result = _load_registry("some.module")
        assert "mod_pipeline" in result


# ---------------------------------------------------------------------------
# _get_pipeline
# ---------------------------------------------------------------------------

class TestGetPipeline:
    def test_returns_pipeline(self):
        p = _make_pipeline("alpha")
        registry = {"alpha": p}
        assert _get_pipeline(registry, "alpha") is p

    def test_dies_on_missing(self):
        with pytest.raises(SystemExit):
            _get_pipeline({"alpha": _make_pipeline("alpha")}, "beta")


# ---------------------------------------------------------------------------
# _cmd_list
# ---------------------------------------------------------------------------

class TestCmdList:
    def test_empty_registry(self, capsys):
        _cmd_list(_make_args(command="list"), {})
        out = capsys.readouterr().out
        assert "No pipelines" in out

    def test_shows_pipeline_and_streams(self, capsys):
        p = _make_pipeline("my_pipe", ["stream_a", "stream_b"])
        _cmd_list(_make_args(command="list"), {"my_pipe": p})
        out = capsys.readouterr().out
        assert "my_pipe" in out
        assert "stream_a" in out
        assert "stream_b" in out

    def test_shows_no_streams_message(self, capsys):
        p = _make_pipeline("empty_pipe")
        _cmd_list(_make_args(command="list"), {"empty_pipe": p})
        out = capsys.readouterr().out
        assert "none" in out.lower()

    def test_sorted_by_name(self, capsys):
        reg = {
            "zzz": _make_pipeline("zzz"),
            "aaa": _make_pipeline("aaa"),
        }
        _cmd_list(_make_args(command="list"), reg)
        out = capsys.readouterr().out
        assert out.index("aaa") < out.index("zzz")


# ---------------------------------------------------------------------------
# _cmd_run
# ---------------------------------------------------------------------------

class TestCmdRun:
    def test_calls_pipeline_run_no_error(self):
        # Real Pipeline with mock source/dest — verifies no exception raised
        p = _make_pipeline("pipe1", ["s1"])
        p._source.discover.return_value = MagicMock(stream_names=["s1"])
        p._source.read.return_value = iter([])
        p._destination.prepare_stream.return_value = None
        p._destination.write.return_value = None
        p._destination.commit.return_value = None
        p._state_store.get.return_value = {}
        p._state_store.set.return_value = None
        p._state_store.initialize = MagicMock()
        # Should not raise
        _cmd_run(_make_args(pipeline="pipe1"), {"pipe1": p})

    def test_run_all_streams(self):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        _cmd_run(_make_args(pipeline="pipe1", stream=None), {"pipe1": p})
        p.run.assert_called_once_with(
            streams=None, full_refresh=False, workers=5, chunk_size=1000
        )

    def test_run_specific_streams(self):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        _cmd_run(
            _make_args(pipeline="pipe1", stream=["s1", "s2"]),
            {"pipe1": p},
        )
        p.run.assert_called_once_with(
            streams=["s1", "s2"], full_refresh=False, workers=5, chunk_size=1000
        )

    def test_full_refresh_flag(self):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        _cmd_run(
            _make_args(pipeline="pipe1", full_refresh=True),
            {"pipe1": p},
        )
        p.run.assert_called_once_with(
            streams=None, full_refresh=True, workers=5, chunk_size=1000
        )

    def test_custom_workers_and_chunk_size(self):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        _cmd_run(
            _make_args(pipeline="pipe1", workers=10, chunk_size=500),
            {"pipe1": p},
        )
        p.run.assert_called_once_with(
            streams=None, full_refresh=False, workers=10, chunk_size=500
        )

    def test_missing_pipeline_exits(self):
        with pytest.raises(SystemExit):
            _cmd_run(_make_args(pipeline="nope"), {})


# ---------------------------------------------------------------------------
# _cmd_check
# ---------------------------------------------------------------------------

class TestCmdCheck:
    def test_check_success(self, capsys):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        p.check.return_value = None
        _cmd_check(_make_args(command="check", pipeline="pipe1"), {"pipe1": p})
        out = capsys.readouterr().out
        assert "✓" in out
        assert "Source OK" in out
        assert "Destination OK" in out

    def test_check_failure_exits(self, capsys):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        p.check.side_effect = RuntimeError("connection refused")
        with pytest.raises(SystemExit) as exc_info:
            _cmd_check(_make_args(command="check", pipeline="pipe1"), {"pipe1": p})
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert "✗" in out
        assert "connection refused" in out

    def test_missing_pipeline_exits(self):
        with pytest.raises(SystemExit):
            _cmd_check(_make_args(command="check", pipeline="nope"), {})


# ---------------------------------------------------------------------------
# _cmd_state
# ---------------------------------------------------------------------------

class TestCmdState:
    def test_state_for_specific_stream(self, capsys):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        p.get_state.return_value = {"STTRCID": 42}
        _cmd_state(
            _make_args(command="state", pipeline="pipe1", stream="phone"),
            {"pipe1": p},
        )
        p.get_state.assert_called_once_with("phone")
        out = capsys.readouterr().out
        assert "phone" in out
        assert "42" in out

    def test_state_all_streams_via_list_state(self, capsys):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        p.list_state.return_value = {
            "phone": {"STTRCID": 10},
            "account": {"STTRCID": 20},
        }
        _cmd_state(
            _make_args(command="state", pipeline="pipe1", stream=None),
            {"pipe1": p},
        )
        out = capsys.readouterr().out
        assert "phone" in out
        assert "account" in out
        assert "10" in out
        assert "20" in out

    def test_state_falls_back_to_configured_streams(self, capsys):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        p.list_state.return_value = {}
        p.configured_streams = ["alpha", "beta"]
        p.get_state.return_value = {}
        _cmd_state(
            _make_args(command="state", pipeline="pipe1", stream=None),
            {"pipe1": p},
        )
        out = capsys.readouterr().out
        assert "alpha" in out
        assert "beta" in out

    def test_state_no_data_message(self, capsys):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        p.list_state.return_value = {}
        p.configured_streams = []
        _cmd_state(
            _make_args(command="state", pipeline="pipe1", stream=None),
            {"pipe1": p},
        )
        out = capsys.readouterr().out
        assert "No state" in out

    def test_state_sorted_alphabetically(self, capsys):
        p = MagicMock(spec=Pipeline)
        p.name = "pipe1"
        p.list_state.return_value = {"zzz": {}, "aaa": {}}
        _cmd_state(
            _make_args(command="state", pipeline="pipe1", stream=None),
            {"pipe1": p},
        )
        out = capsys.readouterr().out
        assert out.index("aaa") < out.index("zzz")

    def test_missing_pipeline_exits(self):
        with pytest.raises(SystemExit):
            _cmd_state(_make_args(command="state", pipeline="nope"), {})


# ---------------------------------------------------------------------------
# Pipeline.configured_streams / get_state / list_state
# ---------------------------------------------------------------------------

class TestPipelinePublicAPI:
    def test_configured_streams_empty(self):
        p = _make_pipeline("p", [])
        assert p.configured_streams == []

    def test_configured_streams_returns_names(self):
        p = _make_pipeline("p", ["s1", "s2", "s3"])
        assert p.configured_streams == ["s1", "s2", "s3"]

    def test_get_state_delegates_to_store(self):
        p = _make_pipeline("p", ["s1"])
        p._state_store.get.return_value = {"cursor": 99}
        assert p.get_state("s1") == {"cursor": 99}
        p._state_store.get.assert_called_once_with("p", "s1")

    def test_list_state_uses_list_streams(self):
        p = _make_pipeline("p", ["s1", "s2"])
        p._state_store.list_streams = MagicMock(return_value=["s1", "s2"])
        p._state_store.get.side_effect = lambda pipe, s: {"k": s}
        result = p.list_state()
        p._state_store.list_streams.assert_called_once_with("p")
        assert result == {"s1": {"k": "s1"}, "s2": {"k": "s2"}}

    def test_list_state_returns_empty_without_list_streams(self):
        p = _make_pipeline("p", ["s1"])
        # state_store has no list_streams attr
        del p._state_store.list_streams
        assert p.list_state() == {}


# ---------------------------------------------------------------------------
# Argparser smoke test
# ---------------------------------------------------------------------------

class TestBuildParser:
    def test_run_parses_correctly(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["--config", "my.module", "run", "--pipeline", "pipe1",
             "--stream", "s1", "--stream", "s2", "--full-refresh",
             "--workers", "8", "--chunk-size", "200"]
        )
        assert args.config == "my.module"
        assert args.command == "run"
        assert args.pipeline == "pipe1"
        assert args.stream == ["s1", "s2"]
        assert args.full_refresh is True
        assert args.workers == 8
        assert args.chunk_size == 200

    def test_check_parses_correctly(self):
        parser = _build_parser()
        args = parser.parse_args(["check", "--pipeline", "mypipe"])
        assert args.command == "check"
        assert args.pipeline == "mypipe"

    def test_state_parses_correctly(self):
        parser = _build_parser()
        args = parser.parse_args(["state", "--pipeline", "mypipe", "--stream", "phone"])
        assert args.command == "state"
        assert args.stream == "phone"

    def test_list_parses_correctly(self):
        parser = _build_parser()
        args = parser.parse_args(["list"])
        assert args.command == "list"

    def test_no_command_exits(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])
