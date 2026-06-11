"""Pipeline metadata column injection.

Injects loader-managed timestamp columns into every row before it reaches
the destination.  Applied in the pipeline's extract→load loop, after field
hashing and before destination.write().

``loaded_at`` (UTC TIMESTAMP) is injected automatically when enabled (default).
Optional additional columns in user-specified timezones are declared as
``loaded_at_extra_timezones=[(label, tz_string)]`` and produce columns
named ``loaded_at_{label}`` (e.g. ``loaded_at_central``).

All timestamp values are derived from a single ``datetime`` captured once
at the start of the pipeline run — every row in a run shares the same value,
matching dlt's ``_dlt_load_id`` per-run pattern.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fflow.common.schema import Column, ColumnType

_LOADED_AT = "_fflow_loaded_at"


def build_metadata_columns(
    loaded_at: bool,
    extra_timezones: list[tuple[str, str]],
) -> list[Column]:
    """Return ``Column`` objects for all metadata columns to be injected.

    Parameters
    ----------
    loaded_at:
        Whether to inject the primary ``loaded_at`` (UTC) column.
    extra_timezones:
        Additional timezone columns as ``[(label, tz_string)]``.
        ``tz_string`` must be a valid IANA timezone name (e.g. ``"America/Chicago"``).
        Produces columns named ``loaded_at_{label}``.
    """
    cols: list[Column] = []
    if loaded_at:
        cols.append(Column(name=_LOADED_AT, type=ColumnType.timestamp, nullable=False))
    for label, _ in extra_timezones:
        cols.append(
            Column(name=f"{_LOADED_AT}_{label}", type=ColumnType.timestamp, nullable=False)
        )
    return cols


def check_metadata_column_clashes(
    stream: str,
    dest_col_names: set[str],
    metadata_cols: list[Column],
) -> None:
    """Raise ``ValueError`` if any metadata column name collides with a destination column.

    A collision means fflow would silently overwrite a source value with
    a loader-managed timestamp.  We always fail loud so the user can rename
    the source column or adjust the pipeline config.
    """
    for col in metadata_cols:
        if col.name in dest_col_names:
            raise ValueError(
                f"Stream '{stream}': source column '{col.name}' conflicts with the "
                f"fflow metadata column '{col.name}'. "
                f"fflow automatically injects '{col.name}' into every row as a "
                f"loader-managed timestamp — it cannot be a source column name. "
                f"Rename the source column to resolve this conflict."
            )


def apply_metadata_columns(
    chunk: list[dict],
    loaded_at_ts: datetime,
    loaded_at: bool,
    extra_timezones: list[tuple[str, str]],
) -> list[dict]:
    """Return *chunk* with loader-managed timestamp columns injected into each row.

    Parameters
    ----------
    chunk:
        Rows to augment.
    loaded_at_ts:
        UTC datetime captured once at pipeline run start.
    loaded_at:
        Whether to inject ``loaded_at`` (UTC).
    extra_timezones:
        Additional timezone columns as ``[(label, tz_string)]``.
    """
    if not loaded_at and not extra_timezones:
        return chunk
    additions: dict = {}
    if loaded_at:
        additions[_LOADED_AT] = loaded_at_ts
    for label, tz_str in extra_timezones:
        additions[f"{_LOADED_AT}_{label}"] = loaded_at_ts.astimezone(ZoneInfo(tz_str))
    return [{**row, **additions} for row in chunk]
