"""Pipeline — the top-level run unit.

A pipeline pairs one source connector with one destination connector and
drives the extract → load loop for every configured stream.

Design decisions:
- ADR-0006: pipeline.run() continues on individual stream failures; all errors
  are collected and raised together at the end as a PipelineRunError.
- ADR-0007: concurrent extraction via PipeIterator (ThreadPoolExecutor).
- ADR-0008: schema contract checked per-stream before writing.

Usage::

    pipeline = Pipeline(
        name="cache_recprod_to_mssql",
        source=CacheSource(...),
        destination=MSSQLDestination(...),
        state_store=SqlStateStore(...),
        streams=[
            StreamConfig(name="phone", write_disposition="merge", merge_key=["phone_id"]),
            StreamConfig(name="account", write_disposition="merge", merge_key=["account_id"]),
        ],
    )
    pipeline.run()
    pipeline.run(streams=["phone"])
    pipeline.run(streams=["account.*"])  # glob matching
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from fnmatch import fnmatch
from typing import TYPE_CHECKING

from fflow.common.config import PipelineRunConfig, SchemaContract, StreamConfig
from fflow.common.exceptions import PipelineRunError, StreamError
from fflow.common.hashing import apply_field_hashing, validate_hash_fields
from fflow.common.metadata import (
    apply_metadata_columns,
    build_metadata_columns,
    check_metadata_column_clashes,
)
from fflow.common.schema import Column, ColumnType, Stream
from fflow.extract.pipe_iterator import PipeIterator, _WorkerErrors

if TYPE_CHECKING:
    from fflow.common.protocols import Destination, Source, StateStore

logger = logging.getLogger(__name__)


class Pipeline:
    """Drives the extract → load loop for one source / destination pair.

    Parameters
    ----------
    name:
        Unique pipeline identifier.  Used as the ``pipeline`` key in the state
        store.
    source:
        A connector that implements ``Source`` protocol.
    destination:
        A connector that implements ``Destination`` protocol.
    state_store:
        A connector that implements ``StateStore`` protocol.
    streams:
        Per-stream configuration (write_disposition, merge_key, hash_fields, etc.).
        Streams absent from this list default to ``append`` disposition.
    schema_contract:
        Controls how the destination reacts to schema changes discovered at
        runtime.  Defaults to ``evolve`` (auto-ALTER).
    hash_key:
        Secret key used for HMAC-SHA256 field hashing.  Required when any
        stream declares ``hash_fields``.  Load from an environment variable —
        never hardcode.  See ADR-0011.
    loaded_at:
        When ``True`` (default), fflow injects a ``loaded_at`` UTC
        TIMESTAMP column into every row before writing to the destination.
        The value is captured once at the start of each pipeline run so all
        rows in a run share the same timestamp.
    loaded_at_extra_timezones:
        Additional loader-managed timestamp columns in user-specified timezones.
        Each entry is ``(label, tz_string)`` where ``tz_string`` is a valid
        IANA timezone name (e.g. ``"America/Chicago"``).  Produces a column
        named ``loaded_at_{label}`` (e.g. ``loaded_at_central``).
    """

    def __init__(
        self,
        name: str,
        source: "Source",
        destination: "Destination",
        state_store: "StateStore",
        streams: list[StreamConfig] | None = None,
        schema_contract: SchemaContract | None = None,
        hash_key: str | None = None,
        loaded_at: bool = True,
        loaded_at_extra_timezones: list[tuple[str, str]] | None = None,
    ) -> None:
        self.name = name
        self._source = source
        self._destination = destination
        self._state_store = state_store
        self._stream_configs: dict[str, StreamConfig] = {
            s.name: s for s in (streams or [])
        }
        self._schema_contract = schema_contract or SchemaContract()
        self._hash_key = hash_key
        self._loaded_at = loaded_at
        self._loaded_at_extra_timezones: list[tuple[str, str]] = loaded_at_extra_timezones or []

        streams_needing_hash = [s.name for s in (streams or []) if s.hash_fields]
        if streams_needing_hash and not hash_key:
            raise ValueError(
                f"Pipeline '{name}': streams {streams_needing_hash} declare "
                "hash_fields but hash_key is not set. "
                "Set hash_key=os.environ['HASH_KEY']."
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def configured_streams(self) -> list[str]:
        """Stream names explicitly configured by the user (no live connection)."""
        return list(self._stream_configs.keys())

    def get_state(self, stream: str) -> dict:
        """Return the persisted state dict for *stream*."""
        return self._state_store.get(self.name, stream)

    def list_state(self) -> dict[str, dict]:
        """Return state for all streams that have persisted state.

        Uses ``state_store.list_streams()`` if available.  Returns an empty
        dict if the state store does not implement that method (custom stores).
        """
        if hasattr(self._state_store, "list_streams"):
            streams = self._state_store.list_streams(self.name)
            return {s: self._state_store.get(self.name, s) for s in streams}
        return {}

    def check(self) -> None:
        """Verify both source and destination connections.

        Raises ``ConnectorError`` (from the connector) on failure.
        """
        self._source.check()
        self._destination.check()

    def run(
        self,
        streams: list[str] | None = None,
        full_refresh: bool = False,
        workers: int = 5,
        chunk_size: int = 1000,
    ) -> None:
        """Run the pipeline.

        Parameters
        ----------
        streams:
            Whitelist of stream names to run.  Supports ``fnmatch`` glob
            patterns (e.g. ``["account.*"]``).  ``None`` means run all
            streams discovered from the source.
        full_refresh:
            If ``True``, ignore the current state and reload all rows.
        workers:
            Number of concurrent extraction threads.
        chunk_size:
            Rows per chunk yielded by PipeIterator.
        """
        run_cfg = PipelineRunConfig(
            streams=streams,
            full_refresh=full_refresh,
            workers=workers,
            chunk_size=chunk_size,
        )

        run_id = str(uuid.uuid4())
        logger.info("Pipeline '%s': run_id=%s", self.name, run_id)
        schema = self._source.discover()

        # Initialize state store on the main thread before workers start.
        # Mirrors dlt's initialize_storage() pattern — no lazy DDL in worker threads.
        if hasattr(self._state_store, "initialize"):
            self._state_store.initialize()

        target_streams = self._resolve_streams(schema.stream_names, run_cfg.streams)
        if not target_streams:
            logger.warning("Pipeline '%s': no streams matched — nothing to do", self.name)
            return

        logger.info(
            "Pipeline '%s': running %d stream(s): %s",
            self.name,
            len(target_streams),
            target_streams,
        )

        # Build metadata columns and capture the run-start UTC timestamp once.
        # All rows in this run share the same loaded_at value — mirrors dlt's
        # per-load _dlt_load_id pattern.
        metadata_cols = build_metadata_columns(
            self._loaded_at, self._loaded_at_extra_timezones
        )
        loaded_at_ts: datetime | None = datetime.now(timezone.utc) if metadata_cols else None

        # Validate hash_fields against discovered schema for mirror-mode streams.
        # SQL-file mode streams have no columns at this point — deferred to first chunk.
        _hash_defer_validation: set[str] = set()
        if self._hash_key:
            for stream_name in target_streams:
                cfg = self._stream_configs.get(stream_name, StreamConfig(name=stream_name))
                if not cfg.hash_fields:
                    continue
                stream_schema = schema.get_stream(stream_name)
                if stream_schema and stream_schema.columns:
                    known = {c.name for c in stream_schema.columns}
                    validate_hash_fields(stream_name, cfg.hash_fields, known)
                else:
                    _hash_defer_validation.add(stream_name)

        # Prepare state: honour full_refresh by resetting state to empty.
        state_overrides: dict[str, dict] = {}
        if full_refresh:
            state_overrides = {s: {} for s in target_streams}

        # Prepare destination for each stream (DDL, truncate for replace, etc.)
        # The effective schema passed to prepare_stream has hash fields renamed to
        # {field}_hash and metadata columns appended — matching what rows look like
        # after all transforms in the extract loop below.
        stream_errors: list[StreamError] = []
        streams_ready: list[str] = []
        for stream_name in target_streams:
            stream_schema = schema.get_stream(stream_name)
            stream_cfg = self._stream_configs.get(
                stream_name, StreamConfig(name=stream_name)
            )
            effective_schema = self._build_effective_schema(
                stream_name, stream_schema, stream_cfg, metadata_cols
            )
            try:
                self._destination.prepare_stream(stream_name, effective_schema, stream_cfg, run_id)
                streams_ready.append(stream_name)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Pipeline '%s': stream '%s' prepare_stream failed: %s",
                    self.name, stream_name, exc,
                )
                stream_errors.append(StreamError(stream_name, exc))

        if not streams_ready:
            raise PipelineRunError(stream_errors)

        # Override state store with empty dicts for full_refresh streams.
        effective_state_store = (
            _OverrideStateStore(self._state_store, self.name, state_overrides)
            if state_overrides
            else self._state_store
        )

        # Run extraction + load.
        with PipeIterator(
            self._source,
            streams_ready,
            effective_state_store,
            self.name,
            workers=run_cfg.workers,
            chunk_size=run_cfg.chunk_size,
        ) as pipe:
            try:
                _hash_validated: set[str] = set()
                for stream_name, chunk in pipe:
                    stream_cfg = self._stream_configs.get(
                        stream_name, StreamConfig(name=stream_name)
                    )
                    if stream_cfg.hash_fields and self._hash_key:
                        # First-chunk validation for SQL-file mode streams.
                        if (
                            stream_name in _hash_defer_validation
                            and stream_name not in _hash_validated
                        ):
                            if chunk:
                                validate_hash_fields(
                                    stream_name,
                                    stream_cfg.hash_fields,
                                    set(chunk[0].keys()),
                                )
                            _hash_validated.add(stream_name)
                        chunk = apply_field_hashing(
                            chunk, set(stream_cfg.hash_fields), self._hash_key
                        )
                    if metadata_cols and loaded_at_ts is not None:
                        chunk = apply_metadata_columns(
                            chunk,
                            loaded_at_ts,
                            self._loaded_at,
                            self._loaded_at_extra_timezones,
                        )
                    self._destination.write(stream_name, chunk)

            except _WorkerErrors as worker_exc:
                for err in worker_exc.errors:
                    logger.error(
                        "Pipeline '%s': stream '%s' extraction failed: %s",
                        self.name, err.stream, err.exc,
                    )
                    try:
                        self._destination.rollback(err.stream)
                    except Exception:  # noqa: BLE001
                        pass
                    stream_errors.append(StreamError(err.stream, err.exc))
            except Exception:  # noqa: BLE001
                # Unexpected error — rollback all prepared streams.
                for s in streams_ready:
                    try:
                        self._destination.rollback(s)
                    except Exception:  # noqa: BLE001
                        pass
                raise

            # Post-loop: commit every stream that succeeded, including zero-row
            # streams (e.g. shuttle-loaded). Mirrors dlt's post-extract commit.
            errored_streams = {e.stream for e in stream_errors}
            for stream_name in streams_ready:
                if stream_name not in errored_streams:
                    try:
                        self._commit_stream(pipe, stream_name)
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "Pipeline '%s': stream '%s' commit failed: %s",
                            self.name, stream_name, exc,
                        )
                        stream_errors.append(StreamError(stream_name, exc))

        if stream_errors:
            raise PipelineRunError(stream_errors)

        logger.info("Pipeline '%s': run complete", self.name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_effective_schema(
        self,
        stream_name: str,
        stream_schema: "Stream | None",
        stream_cfg: StreamConfig,
        metadata_cols: list["Column"],
    ) -> "Stream":
        """Return the schema that the destination should use for DDL and writes.

        Applies two transforms to the discovered schema:

        1. Hash-field rename: columns in ``stream_cfg.hash_fields`` are renamed
           from ``{field}`` to ``{field}_hash`` (string, max 64 chars) so the
           destination table column name matches the transformed row keys.
        2. Metadata column append: loader-managed columns (``loaded_at`` etc.)
           are appended so they are included in DDL and ``dest_columns``.

        Raises ``ValueError`` if any metadata column name collides with a
        (post-rename) source column name.
        """
        if stream_schema is None:
            stream_schema = Stream(name=stream_name)

        cols = list(stream_schema.columns)

        # Rename hash fields: email → email_hash (type=string, max_length=64)
        if stream_cfg.hash_fields:
            hash_set = set(stream_cfg.hash_fields)
            cols = [
                col.model_copy(update={
                    "name": f"{col.name}_hash",
                    "type": ColumnType.string,
                    "max_length": 64,
                })
                if col.name in hash_set else col
                for col in cols
            ]

        # Clash check and metadata append
        if metadata_cols:
            dest_names = {c.name for c in cols}
            check_metadata_column_clashes(stream_name, dest_names, metadata_cols)
            cols = cols + metadata_cols

        return stream_schema.model_copy(update={"columns": cols})

    def _commit_stream(self, pipe: PipeIterator, stream: str) -> None:
        """Commit destination writes and persist state for *stream*."""
        self._destination.commit(stream)
        new_state = pipe.get_state(stream)
        self._state_store.set(self.name, stream, new_state)
        logger.debug(
            "Pipeline '%s': stream '%s' committed; state=%s",
            self.name, stream, new_state,
        )

    @staticmethod
    def _resolve_streams(
        available: list[str],
        requested: list[str] | None,
    ) -> list[str]:
        """Filter *available* streams by *requested* glob patterns.

        Returns *available* unchanged if *requested* is ``None``.
        """
        if requested is None:
            return list(available)
        result = []
        for name in available:
            if any(fnmatch(name, pat) for pat in requested):
                result.append(name)
        return result


class _OverrideStateStore:
    """Wraps a real StateStore and returns empty dicts for overridden streams.

    Used to implement ``full_refresh`` without mutating real state until commit.
    """

    def __init__(
        self,
        inner: "StateStore",
        pipeline: str,
        overrides: dict[str, dict],
    ) -> None:
        self._inner = inner
        self._pipeline = pipeline
        self._overrides = overrides

    def get(self, pipeline: str, stream: str) -> dict:
        if pipeline == self._pipeline and stream in self._overrides:
            return {}
        return self._inner.get(pipeline, stream)

    def set(self, pipeline: str, stream: str, state: dict) -> None:
        self._inner.set(pipeline, stream, state)
