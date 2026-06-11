"""Airflow integration helper for fflow pipelines.

This is the **only** module in ``fflow`` that imports from Airflow.
All other modules must have zero Airflow imports.

Usage::

    from fflow.helpers.airflow import PipelineTaskGroup

    with dag:
        PipelineTaskGroup(
            pipeline=cache_recprod_to_mssql,
            streams=["phone", "account", "call_log"],
        )

Each stream becomes one Airflow ``PythonOperator`` task.  All tasks are
grouped into an Airflow ``TaskGroup`` named after the pipeline.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Callable, Optional, Sequence

try:
    from airflow.utils.task_group import TaskGroup
    from airflow.operators.python import PythonOperator
except ImportError as exc:
    raise ImportError(
        "apache-airflow is required to use fflow.helpers.airflow. "
        "Install it with: pip install apache-airflow>=2.5"
    ) from exc

try:
    from tenacity import Retrying, stop_after_attempt, wait_exponential

    _HAS_TENACITY = True
except ImportError:
    _HAS_TENACITY = False
    Retrying = None  # type: ignore[assignment,misc]

from fflow.pipeline.pipeline import Pipeline

logger = logging.getLogger(__name__)

if _HAS_TENACITY:
    DEFAULT_RETRY_NO_RETRY: Optional[Any] = Retrying(stop=stop_after_attempt(1), reraise=True)
    DEFAULT_RETRY_BACKOFF: Optional[Any] = Retrying(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.5, min=4, max=10),
        reraise=True,
    )
else:
    DEFAULT_RETRY_NO_RETRY = None
    DEFAULT_RETRY_BACKOFF = None

_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9_\-]")


def _sanitize_task_id(name: str) -> str:
    """Replace characters that are illegal in Airflow task IDs with underscores."""
    return _UNSAFE_CHARS.sub("_", name)


class PipelineTaskGroup(TaskGroup):
    """One Airflow ``TaskGroup`` with one ``PythonOperator`` per stream.

    Because ``PipelineTaskGroup`` extends ``TaskGroup``, it participates in
    Airflow dependency wiring::

        start >> group >> end

    Parameters
    ----------
    pipeline:
        A ``Pipeline`` instance.  Wrapped in a ``lambda`` so each task
        execution calls ``factory()`` — in standard Airflow deployments the
        DAG file is re-imported per worker, giving a fresh instance anyway.
    streams:
        Concrete stream names.  Each becomes one Airflow task.  Glob
        patterns are not supported here; use ``Pipeline.run()`` directly
        for glob stream selection.
    pipeline_factory:
        Optional zero-argument callable ``() -> Pipeline``.  Overrides
        ``pipeline`` when provided.  Pass this for CeleryExecutor /
        KubernetesExecutor deployments where the pipeline holds live
        connections — the factory is called fresh on each task execution,
        ensuring isolation.  When provided, ``group_id`` must also be set
        explicitly (there is no pipeline object to derive a name from).
    group_id:
        Airflow ``TaskGroup`` ID.  Defaults to ``pipeline.name``.
    full_refresh:
        Forwarded to ``Pipeline.run(full_refresh=...)``.
    workers:
        Forwarded to ``Pipeline.run(workers=...)``.
    chunk_size:
        Forwarded to ``Pipeline.run(chunk_size=...)``.
    retry_policy:
        Optional tenacity ``Retrying`` instance applied inside the task
        callable.  ``DEFAULT_RETRY_BACKOFF`` gives 5 attempts with
        exponential back-off.  Note: Airflow's own ``retries=`` parameter
        also retries the task; keep only one layer active to avoid
        multiplying total attempts unexpectedly.
    **task_kwargs:
        Extra keyword arguments forwarded to every ``PythonOperator``
        (e.g. ``retries=``, ``execution_timeout=``, ``on_failure_callback=``).
    """

    def __init__(
        self,
        pipeline: Pipeline,
        streams: Sequence[str],
        *,
        pipeline_factory: Optional[Callable[[], Pipeline]] = None,
        group_id: Optional[str] = None,
        full_refresh: bool = False,
        workers: int = 5,
        chunk_size: int = 1000,
        retry_policy: Optional[Any] = None,
        **task_kwargs: Any,
    ) -> None:
        if pipeline_factory is not None:
            self._pipeline_factory: Callable[[], Pipeline] = pipeline_factory
            effective_group_id = group_id or "pipeline"
        else:
            _captured = pipeline
            self._pipeline_factory = lambda: _captured  # noqa: E731
            effective_group_id = group_id or pipeline.name

        self._full_refresh = full_refresh
        self._workers = workers
        self._chunk_size = chunk_size
        self._retry_policy = retry_policy

        super().__init__(group_id=effective_group_id)

        seen: set[str] = set()
        self._tasks: list[PythonOperator] = []

        with self:
            for stream in streams:
                task_id = _sanitize_task_id(stream)
                if task_id in seen:
                    raise ValueError(
                        f"Stream {stream!r} sanitizes to task ID {task_id!r}, "
                        f"which collides with a previously added stream."
                    )
                seen.add(task_id)
                op = PythonOperator(
                    task_id=task_id,
                    python_callable=self._make_callable(stream),
                    **task_kwargs,
                )
                self._tasks.append(op)

    def _make_callable(self, stream: str) -> Callable[..., None]:
        """Return the callable that Airflow invokes for this stream's task."""
        factory = self._pipeline_factory
        full_refresh = self._full_refresh
        workers = self._workers
        chunk_size = self._chunk_size
        retry_policy = self._retry_policy

        def _run(**context: Any) -> None:
            p = factory()
            if retry_policy is not None:
                for attempt in retry_policy.copy(reraise=True):
                    with attempt:
                        p.run(
                            streams=[stream],
                            full_refresh=full_refresh,
                            workers=workers,
                            chunk_size=chunk_size,
                        )
            else:
                p.run(
                    streams=[stream],
                    full_refresh=full_refresh,
                    workers=workers,
                    chunk_size=chunk_size,
                )

        _run.__name__ = f"run_{_sanitize_task_id(stream)}"
        return _run
