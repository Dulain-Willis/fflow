"""Streaming engine for concurrent multi-stream extraction.

``PipeIterator`` submits each stream's ``source.read()`` call to a
``ThreadPoolExecutor``.  I/O-bound JDBC/ODBC calls release the GIL so threads
genuinely parallelise.  The main thread consumes ``(stream_name, chunk)``
tuples from a bounded queue, providing backpressure so fast sources cannot
OOM slow destinations.

Design rationale: ADR-0007 — threads over asyncio for JDBC I/O.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from fflow.common.protocols import Source, StateStore

# Sentinel objects placed on the queue to signal stream completion or error.
_STREAM_DONE = object()


class _StreamError:
    """Wraps an exception raised inside a worker thread."""

    def __init__(self, stream: str, exc: BaseException) -> None:
        self.stream = stream
        self.exc = exc


class PipeIterator:
    """Drives concurrent extraction of multiple streams from a single source.

    Yields ``(stream_name, chunk)`` tuples where each *chunk* is a
    ``list[dict]`` of at most *chunk_size* rows.

    Workers push chunks onto a bounded queue; if the destination is slow the
    workers block on ``put``, limiting memory usage.

    Usage::

        with PipeIterator(source, streams, state_store, "my_pipeline") as pipe:
            for stream_name, chunk in pipe:
                destination.write(stream_name, chunk)

    Thread safety: only the main thread should consume from ``__iter__``.
    Worker threads only write to the queue.
    """

    def __init__(
        self,
        source: "Source",
        streams: list[str],
        state_store: "StateStore",
        pipeline_name: str,
        *,
        workers: int = 5,
        chunk_size: int = 1000,
        queue_maxsize: int = 200,
    ) -> None:
        self._source = source
        self._streams = list(streams)
        self._state_store = state_store
        self._pipeline_name = pipeline_name
        self._workers = min(workers, len(streams)) if streams else 1
        self._chunk_size = chunk_size
        # Bounded queue: blocks workers when the destination is slow.
        self._queue: Queue = Queue(maxsize=queue_maxsize)
        self._executor: ThreadPoolExecutor | None = None
        # Tracks per-stream states so the pipeline can persist them after commit.
        self._states: dict[str, dict] = {}
        self._states_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "PipeIterator":
        self._executor = ThreadPoolExecutor(
            max_workers=self._workers,
            thread_name_prefix="fflow-extract",
        )
        for stream in self._streams:
            self._executor.submit(self._extract_stream, stream)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._executor:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[tuple[str, list[dict]]]:
        remaining = len(self._streams)
        errors: list[_StreamError] = []

        while remaining > 0:
            try:
                item = self._queue.get(timeout=0.1)
            except Empty:
                continue

            if item is _STREAM_DONE:
                remaining -= 1
            elif isinstance(item, _StreamError):
                remaining -= 1
                errors.append(item)
            else:
                stream_name, chunk = item
                yield stream_name, chunk

        if errors:
            # Surface worker errors to the pipeline for per-stream handling.
            raise _WorkerErrors(errors)

    def get_state(self, stream: str) -> dict:
        """Return the (possibly updated) state dict for *stream*.

        Call this after the stream's generator has been fully consumed to
        retrieve the watermark the source wrote into the state dict.
        """
        with self._states_lock:
            return dict(self._states.get(stream, {}))

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    def _extract_stream(self, stream: str) -> None:
        try:
            state = self._state_store.get(self._pipeline_name, stream)
            with self._states_lock:
                self._states[stream] = state

            chunk: list[dict] = []
            for row in self._source.read(stream, state):
                chunk.append(row)
                if len(chunk) >= self._chunk_size:
                    self._queue.put((stream, chunk))
                    chunk = []
            if chunk:
                self._queue.put((stream, chunk))

            # After the generator exhausts, state has been updated in-place
            # by the source.  Snapshot it now so the main thread can persist it.
            with self._states_lock:
                self._states[stream] = state

            self._queue.put(_STREAM_DONE)
        except Exception as exc:  # noqa: BLE001
            self._queue.put(_StreamError(stream, exc))


class _WorkerErrors(Exception):
    """Internal — carries one or more _StreamError from worker threads."""

    def __init__(self, errors: list[_StreamError]) -> None:
        self.errors = errors
