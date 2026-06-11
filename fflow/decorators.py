"""Decorator API for defining pipelines and streams.

Provides ``@pipeline`` and ``@stream()`` — a declarative, Airflow-inspired
interface for authoring pipeline definitions without instantiating
``Pipeline``, ``RestSource``, or ``StreamConfig`` directly.

Usage::

    from fflow import pipeline, stream
    from fflow.sources.rest import rest, RestStreamConfig, JSONLinkPaginator
    from fflow.destinations.redshift import redshift

    @pipeline(
        source=rest("https://api.example.com/v2", auth=...),
        destination=redshift(url=os.environ["REDSHIFT_URL"], schema="raw_data"),
        hash_key=os.environ["HASH_KEY"],
    )
    def my_pipeline():

        @stream()
        def records():
            return RestStreamConfig(
                endpoint="/records.json",
                data_path="records",
                merge_key=["id"],
            )

The decorated function ``my_pipeline`` becomes a ``Pipeline`` instance.
Its name (``"my_pipeline"``) is used as the pipeline name and as the
``FileStateStore`` base path (``.state/my_pipeline``).
"""

from __future__ import annotations

import threading
from typing import Any, Callable

from fflow.common.config import SchemaContract, StreamConfig
from fflow.common.state import FileStateStore
from fflow.pipeline.pipeline import Pipeline

# Thread-local stack so nested @pipeline calls (if ever) don't collide.
_context = threading.local()


def _get_collection() -> list[StreamConfig] | None:
    stack: list[list[StreamConfig]] = getattr(_context, "stack", [])
    return stack[-1] if stack else None


def _push_collection() -> list[StreamConfig]:
    if not hasattr(_context, "stack"):
        _context.stack = []
    col: list[StreamConfig] = []
    _context.stack.append(col)
    return col


def _pop_collection() -> list[StreamConfig]:
    return _context.stack.pop()


def stream() -> Callable:
    """Decorator that registers a stream config with the enclosing ``@pipeline``.

    The decorated function must return a :class:`~fflow.common.config.StreamConfig`
    subclass (e.g. :class:`~fflow.sources.rest.RestStreamConfig`).
    The function name becomes the stream's ``name``.

    Must be used inside a ``@pipeline``-decorated function body.
    """
    def decorator(fn: Callable) -> StreamConfig:
        col = _get_collection()
        if col is None:
            raise RuntimeError(
                f"@stream() '{fn.__name__}' must be defined inside a @pipeline function body."
            )
        cfg: StreamConfig = fn()
        if not isinstance(cfg, StreamConfig):
            raise TypeError(
                f"@stream() '{fn.__name__}' must return a StreamConfig instance; "
                f"got {type(cfg).__name__}."
            )
        # Inject function name as stream name.
        cfg = cfg.model_copy(update={"name": fn.__name__})
        col.append(cfg)
        return cfg
    return decorator


def pipeline(
    source: Any,
    destination: Any,
    name: str | None = None,
    hash_key: str | None = None,
    loaded_at: bool = True,
    loaded_at_extra_timezones: list[tuple[str, str]] | None = None,
    schema_contract: SchemaContract | None = None,
    state_store: Any = None,
) -> Callable:
    """Decorator that builds a :class:`~fflow.pipeline.Pipeline` from a
    class body of ``@stream()``-decorated functions.

    The decorated function's name is used as the pipeline name (and default
    ``FileStateStore`` path) unless *name* is provided explicitly.

    Parameters
    ----------
    source:
        A source connector instance.  For REST sources, use :func:`~fflow.sources.rest.rest`.
        Streams are wired in automatically from ``@stream()`` definitions.
    destination:
        A destination connector instance.
    name:
        Pipeline name.  Defaults to the decorated function's ``__name__``.
    hash_key:
        HMAC-SHA256 key for field hashing.  Required when any stream declares
        ``hash_fields``.
    loaded_at:
        Inject ``_fflow_loaded_at`` UTC metadata column (default ``True``).
    loaded_at_extra_timezones:
        Extra timezone columns, e.g. ``[("central", "America/Chicago")]``.
    schema_contract:
        Schema-change policy.  Defaults to ``evolve``.
    state_store:
        Override the default ``FileStateStore``.  Defaults to
        ``FileStateStore(base_path=".state/{pipeline_name}")``.
    """
    def decorator(fn: Callable) -> Pipeline:
        pipeline_name = name or fn.__name__

        # Collect @stream() configs by calling the function body inside a context.
        col = _push_collection()
        try:
            fn()
        finally:
            _pop_collection()

        stream_configs: list[StreamConfig] = col

        # Wire stream configs into the source if it supports configure_streams().
        if hasattr(source, "configure_streams"):
            source.configure_streams(stream_configs)

        resolved_state_store = state_store or FileStateStore(
            base_path=f".state/{pipeline_name}"
        )

        return Pipeline(
            name=pipeline_name,
            source=source,
            destination=destination,
            state_store=resolved_state_store,
            streams=stream_configs,
            schema_contract=schema_contract,
            hash_key=hash_key,
            loaded_at=loaded_at,
            loaded_at_extra_timezones=loaded_at_extra_timezones or [],
        )

    return decorator
