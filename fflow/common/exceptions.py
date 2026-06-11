"""fflow exception hierarchy."""

from __future__ import annotations


class FflowError(Exception):
    """Base class for all fflow errors."""


class ConnectorError(FflowError):
    """Raised when a source or destination ``check()`` fails."""


class StreamError(FflowError):
    """Raised when a single stream fails during ``pipeline.run()``.

    The pipeline catches this, collects it, and continues to the next stream.
    All collected errors are re-raised together at the end as a
    ``PipelineRunError``.
    """

    def __init__(self, stream: str, cause: BaseException) -> None:
        self.stream = stream
        self.cause = cause
        super().__init__(f"Stream '{stream}' failed: {cause}")


class PipelineRunError(FflowError):
    """Raised at the end of ``pipeline.run()`` when one or more streams failed.

    Contains all ``StreamError`` instances collected during the run.
    """

    def __init__(self, errors: list[StreamError]) -> None:
        self.errors = errors
        summary = ", ".join(f"'{e.stream}'" for e in errors)
        super().__init__(
            f"{len(errors)} stream(s) failed: {summary}\n"
            + "\n".join(f"  {e}" for e in errors)
        )


class SchemaContractViolation(FflowError):
    """Raised when a discovered schema change violates the schema contract."""


class StateStoreError(FflowError):
    """Raised when the state store cannot be read or written."""
