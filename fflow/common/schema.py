"""Pydantic models for source-side schema discovery.

These types are returned by ``Source.discover()`` and consumed by the pipeline
and destination for DDL generation, cursor advancement, and schema-contract
enforcement.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, model_validator


class ColumnType(str, Enum):
    string = "string"
    integer = "integer"
    float_ = "float"
    decimal = "decimal"
    boolean = "boolean"
    timestamp = "timestamp"
    date = "date"
    json = "json"
    unknown = "unknown"


class Column(BaseModel):
    name: str
    type: ColumnType = ColumnType.unknown
    nullable: bool = True
    primary_key: bool = False
    precision: Optional[int] = None
    scale: Optional[int] = None
    max_length: Optional[int] = None


CursorType = Literal["integer", "timestamp", "none"]


class IncrementalConfig(BaseModel):
    """Declares how a stream advances its watermark between runs.

    ``cursor_type="none"`` means full refresh — no cursor is tracked and
    ``state`` passed into ``read()`` will always be empty on each run.
    """

    cursor_type: CursorType = "none"
    cursor_field: Optional[str] = None  # column name used as watermark

    @model_validator(mode="after")
    def cursor_field_required_when_incremental(self) -> "IncrementalConfig":
        if self.cursor_type != "none" and not self.cursor_field:
            raise ValueError(
                "cursor_field is required when cursor_type is not 'none'"
            )
        return self


class Stream(BaseModel):
    """A single logical table or endpoint within a source."""

    name: str
    columns: list[Column] = []
    incremental: IncrementalConfig = IncrementalConfig()

    def get_column(self, name: str) -> Optional[Column]:
        return next((c for c in self.columns if c.name == name), None)

    @property
    def primary_keys(self) -> list[str]:
        return [c.name for c in self.columns if c.primary_key]


class Schema(BaseModel):
    """The full schema returned by ``Source.discover()``."""

    streams: list[Stream] = []

    def get_stream(self, name: str) -> Optional[Stream]:
        return next((s for s in self.streams if s.name == name), None)

    @property
    def stream_names(self) -> list[str]:
        return [s.name for s in self.streams]
