"""fflow — agnostic data loader: any source → any destination."""

from fflow.pipeline.pipeline import Pipeline
from fflow.common.config import StreamConfig, SchemaContract, WriteDisposition
from fflow.common.schema import Schema, Stream, Column, ColumnType, IncrementalConfig
from fflow.common.state import SqlStateStore, FileStateStore
from fflow.common.exceptions import (
    FflowError,
    ConnectorError,
    PipelineRunError,
    StreamError,
)
from fflow.decorators import pipeline, stream

__version__ = "0.1.0"

__all__ = [
    "Pipeline",
    "StreamConfig",
    "SchemaContract",
    "WriteDisposition",
    "Schema",
    "Stream",
    "Column",
    "ColumnType",
    "IncrementalConfig",
    "SqlStateStore",
    "FileStateStore",
    "FflowError",
    "ConnectorError",
    "PipelineRunError",
    "StreamError",
    "pipeline",
    "stream",
]
