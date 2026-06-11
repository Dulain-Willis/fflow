"""Runtime pipeline configuration models.

These are distinct from ``fflow.common.schema``, which describes the
*source-side* schema discovered at runtime.  Config models here describe
*how* the pipeline should behave (write disposition, merge keys, etc.) and
are authored by the user in their pipeline definition file.
"""

from __future__ import annotations

from typing import Literal, Optional, List

from pydantic import BaseModel, model_validator

WriteDisposition = Literal["append", "replace", "merge"]


class StreamConfig(BaseModel):
    """Per-stream pipeline configuration.

    ``write_disposition`` controls how rows land at the destination:
    - ``append``  — insert-only; rows are never updated
    - ``replace`` — truncate the destination table then reload all rows
    - ``merge``   — upsert: insert new rows, update existing rows by
                    ``merge_key``; requires ``merge_key`` to be non-empty
    """

    name: str = ""
    write_disposition: WriteDisposition = "append"
    merge_key: list[str] = []
    hash_fields: list[str] = []
    """Column names whose values are replaced by their HMAC-SHA256 digest before
    landing at the destination.  Requires ``hash_key`` to be set on the Pipeline.
    Use for PHI/PII fields (SSN, DOB, name, etc.) to comply with HIPAA.
    """

    @model_validator(mode="after")
    def _validate(self) -> "StreamConfig":
        if self.write_disposition == "merge" and not self.merge_key:
            raise ValueError(
                f"Stream '{self.name}': merge_key must be set when "
                "write_disposition='merge'"
            )
        return self


class SchemaContract(BaseModel):
    """Controls how the destination reacts to schema changes.

    - ``evolve``  — auto-ALTER the destination table when new columns appear
    - ``freeze``  — raise an error on any schema change; the pipeline fails
    - ``discard`` — silently drop columns not present in the destination table
    """

    on_new_column: Literal["evolve", "freeze", "discard"] = "evolve"
    on_dropped_column: Literal["evolve", "freeze", "discard"] = "evolve"


class PipelineRunConfig(BaseModel):
    """Top-level run-time config for a ``Pipeline.run()`` call.

    Not required for normal use — ``Pipeline.run()`` accepts these as keyword
    arguments and constructs this model internally.
    """

    streams: Optional[list[str]] = None  # None means all streams
    full_refresh: bool = False
    workers: int = 5
    chunk_size: int = 1000
