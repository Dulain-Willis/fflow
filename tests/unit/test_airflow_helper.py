"""Unit tests for fflow.helpers.airflow.

Airflow is not installed in the dev environment so we mock the entire
``airflow.*`` module tree via ``sys.modules`` before importing the helper.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock, call

import pytest

# ─── Fake Airflow classes ──────────────────────────────────────────────────────


class _FakeTaskGroup:
    """Minimal TaskGroup stand-in that tracks ``with`` block entry/exit."""

    _stack: list["_FakeTaskGroup"] = []

    def __init__(self, group_id: str, **kw: Any) -> None:
        self.group_id = group_id

    def __enter__(self) -> "_FakeTaskGroup":
        _FakeTaskGroup._stack.append(self)
        return self

    def __exit__(self, *_: Any) -> None:
        _FakeTaskGroup._stack.pop()


class _FakePythonOperator:
    """Records every instantiation so tests can inspect created tasks."""

    registry: list["_FakePythonOperator"] = []

    def __init__(self, task_id: str, python_callable: Any, **kw: Any) -> None:
        self.task_id = task_id
        self.python_callable = python_callable
        self.init_kw = kw
        _FakePythonOperator.registry.append(self)


# Install fake modules into sys.modules *before* importing the helper.
_AIRFLOW_MOCKS: dict[str, Any] = {
    "airflow": MagicMock(),
    "airflow.utils": MagicMock(),
    "airflow.utils.task_group": MagicMock(TaskGroup=_FakeTaskGroup),
    "airflow.operators": MagicMock(),
    "airflow.operators.python": MagicMock(PythonOperator=_FakePythonOperator),
}
for _mod_name, _mod in _AIRFLOW_MOCKS.items():
    sys.modules.setdefault(_mod_name, _mod)

# Force a fresh module load now that the fakes are in place.
for _k in list(sys.modules):
    if _k.startswith("fflow.helpers.airflow"):
        del sys.modules[_k]

from fflow.helpers.airflow import (  # noqa: E402
    DEFAULT_RETRY_BACKOFF,
    DEFAULT_RETRY_NO_RETRY,
    PipelineTaskGroup,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_registry():
    _FakePythonOperator.registry.clear()
    yield


@pytest.fixture
def pipeline(mocker):
    p = MagicMock()
    p.name = "my_pipeline"
    return p


# ─── Construction ─────────────────────────────────────────────────────────────


class TestCreation:
    def test_one_operator_per_stream(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["phone", "account", "call_log"])
        assert len(_FakePythonOperator.registry) == 3

    def test_task_ids_match_streams(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["phone", "account"])
        ids = [op.task_id for op in _FakePythonOperator.registry]
        assert ids == ["phone", "account"]

    def test_group_id_defaults_to_pipeline_name(self, pipeline):
        group = PipelineTaskGroup(pipeline=pipeline, streams=["phone"])
        assert group.group_id == "my_pipeline"

    def test_group_id_override(self, pipeline):
        group = PipelineTaskGroup(pipeline=pipeline, streams=["phone"], group_id="custom")
        assert group.group_id == "custom"

    def test_task_kwargs_forwarded(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["phone"], retries=3)
        assert _FakePythonOperator.registry[0].init_kw.get("retries") == 3

    def test_tasks_attribute_populated(self, pipeline):
        group = PipelineTaskGroup(pipeline=pipeline, streams=["a", "b"])
        assert len(group._tasks) == 2

    def test_empty_streams_creates_no_operators(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=[])
        assert _FakePythonOperator.registry == []


# ─── Task ID sanitisation ─────────────────────────────────────────────────────


class TestTaskIdSanitization:
    def test_dots_replaced_with_underscores(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["a.b.c"])
        assert _FakePythonOperator.registry[0].task_id == "a_b_c"

    def test_spaces_replaced(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["my stream"])
        assert _FakePythonOperator.registry[0].task_id == "my_stream"

    def test_collision_raises(self, pipeline):
        # "a.b" and "a_b" both sanitise to "a_b"
        with pytest.raises(ValueError, match="collides"):
            PipelineTaskGroup(pipeline=pipeline, streams=["a.b", "a_b"])


# ─── Callable behaviour ───────────────────────────────────────────────────────


class TestCallable:
    def test_calls_pipeline_run_with_correct_stream(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["phone"])
        _FakePythonOperator.registry[0].python_callable()
        pipeline.run.assert_called_once_with(
            streams=["phone"],
            full_refresh=False,
            workers=5,
            chunk_size=1000,
        )

    def test_full_refresh_forwarded(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["phone"], full_refresh=True)
        _FakePythonOperator.registry[0].python_callable()
        pipeline.run.assert_called_once_with(
            streams=["phone"],
            full_refresh=True,
            workers=5,
            chunk_size=1000,
        )

    def test_workers_and_chunk_size_forwarded(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["phone"], workers=10, chunk_size=500)
        _FakePythonOperator.registry[0].python_callable()
        pipeline.run.assert_called_once_with(
            streams=["phone"],
            full_refresh=False,
            workers=10,
            chunk_size=500,
        )

    def test_each_stream_runs_its_own_stream(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["phone", "account"])
        _FakePythonOperator.registry[0].python_callable()
        _FakePythonOperator.registry[1].python_callable()
        assert pipeline.run.call_args_list == [
            call(streams=["phone"], full_refresh=False, workers=5, chunk_size=1000),
            call(streams=["account"], full_refresh=False, workers=5, chunk_size=1000),
        ]

    def test_callable_name_reflects_stream(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["call_log"])
        fn = _FakePythonOperator.registry[0].python_callable
        assert fn.__name__ == "run_call_log"


# ─── Factory support ──────────────────────────────────────────────────────────


class TestFactory:
    def test_factory_called_per_task_execution(self):
        fresh = MagicMock()
        factory = MagicMock(return_value=fresh)
        pipeline_stub = MagicMock()
        pipeline_stub.name = "stub"
        PipelineTaskGroup(
            pipeline=pipeline_stub,
            streams=["phone"],
            pipeline_factory=factory,
            group_id="grp",
        )
        fn = _FakePythonOperator.registry[0].python_callable
        fn()
        factory.assert_called_once()
        fresh.run.assert_called_once_with(
            streams=["phone"],
            full_refresh=False,
            workers=5,
            chunk_size=1000,
        )

    def test_factory_group_id_explicit(self):
        pipeline_stub = MagicMock()
        pipeline_stub.name = "stub"
        factory = MagicMock(return_value=MagicMock())
        group = PipelineTaskGroup(
            pipeline=pipeline_stub, streams=["x"], pipeline_factory=factory, group_id="my_grp"
        )
        assert group.group_id == "my_grp"

    def test_factory_group_id_defaults_to_pipeline_literal(self):
        pipeline_stub = MagicMock()
        pipeline_stub.name = "stub"
        factory = MagicMock(return_value=MagicMock())
        group = PipelineTaskGroup(pipeline=pipeline_stub, streams=["x"], pipeline_factory=factory)
        assert group.group_id == "pipeline"


# ─── Retry policy ─────────────────────────────────────────────────────────────


class TestRetryPolicy:
    def test_retry_policy_copy_called_per_invocation(self, pipeline):
        mock_attempt = MagicMock()
        mock_attempt.__enter__ = MagicMock(return_value=None)
        mock_attempt.__exit__ = MagicMock(return_value=False)
        mock_policy = MagicMock()
        mock_policy.copy.return_value = [mock_attempt]

        PipelineTaskGroup(pipeline=pipeline, streams=["phone"], retry_policy=mock_policy)
        fn = _FakePythonOperator.registry[0].python_callable
        fn()

        mock_policy.copy.assert_called_once_with(reraise=True)
        pipeline.run.assert_called_once()

    def test_no_retry_policy_calls_run_directly(self, pipeline):
        PipelineTaskGroup(pipeline=pipeline, streams=["phone"])
        _FakePythonOperator.registry[0].python_callable()
        pipeline.run.assert_called_once()


# ─── Module-level constants ───────────────────────────────────────────────────


class TestConstants:
    def test_default_retry_constants_exported(self):
        # Tenacity may or may not be installed; just check the names exist.
        import fflow.helpers.airflow as m

        assert hasattr(m, "DEFAULT_RETRY_NO_RETRY")
        assert hasattr(m, "DEFAULT_RETRY_BACKOFF")
