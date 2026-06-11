"""StateStore implementations.

Both implementations satisfy the ``StateStore`` protocol defined in
``fflow.common.protocols``.
"""

from __future__ import annotations

import json
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

_TABLE_NAME = "fflow_state"

_CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
    pipeline  VARCHAR(255) NOT NULL,
    stream    VARCHAR(255) NOT NULL,
    state     TEXT         NOT NULL DEFAULT '{{}}',
    updated_at TIMESTAMP   NOT NULL,
    PRIMARY KEY (pipeline, stream)
)
"""

# SQL Server uses NVARCHAR / DATETIME2 and doesn't support IF NOT EXISTS
_CREATE_TABLE_SQL_MSSQL = f"""
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = '{_TABLE_NAME}'
)
CREATE TABLE {_TABLE_NAME} (
    pipeline   NVARCHAR(255)  NOT NULL,
    stream     NVARCHAR(255)  NOT NULL,
    state      NVARCHAR(MAX)  NOT NULL,
    updated_at DATETIME2      NOT NULL,
    CONSTRAINT PK_{_TABLE_NAME} PRIMARY KEY (pipeline, stream)
)
"""


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


class SqlStateStore:
    """Persists state to a ``fflow_state`` table via SQLAlchemy.

    Works with any SQLAlchemy-supported database (SQLite, PostgreSQL,
    SQL Server, etc.).  Uses a plain UPDATE-then-INSERT upsert to stay
    portable across dialects.

    Call ``initialize()`` once on the main thread before any concurrent
    reads or writes (mirrors dlt's ``initialize_storage()`` pattern).
    ``Pipeline.run()`` does this automatically.
    """

    def __init__(self, engine: "Engine") -> None:
        self._engine = engine
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """Create the state table if it does not exist.

        Must be called once on the main thread before ``get()`` or ``set()``
        are called from worker threads.  Safe to call multiple times.
        """
        dialect = self._engine.dialect.name
        ddl = _CREATE_TABLE_SQL_MSSQL if dialect == "mssql" else _CREATE_TABLE_SQL
        with self._engine.begin() as conn:
            conn.execute(_text(ddl))

    def get(self, pipeline: str, stream: str) -> dict:
        with self._engine.connect() as conn:
            result = conn.execute(
                _text(f"SELECT state FROM {_TABLE_NAME} WHERE pipeline = :p AND stream = :s"),
                {"p": pipeline, "s": stream},
            )
            row = result.fetchone()
        return json.loads(row[0]) if row else {}

    def list_streams(self, pipeline: str) -> list[str]:
        """Return all stream names that have persisted state for *pipeline*."""
        with self._engine.connect() as conn:
            result = conn.execute(
                _text(
                    f"SELECT stream FROM {_TABLE_NAME} "
                    "WHERE pipeline = :p ORDER BY stream"
                ),
                {"p": pipeline},
            )
            return [row[0] for row in result.fetchall()]

    def set(self, pipeline: str, stream: str, state: dict) -> None:
        state_json = json.dumps(state)
        now = _now_utc()

        with self._lock:
            with self._engine.begin() as conn:
                result = conn.execute(
                    _text(
                        f"UPDATE {_TABLE_NAME} "
                        "SET state = :state, updated_at = :ts "
                        "WHERE pipeline = :p AND stream = :s"
                    ),
                    {"state": state_json, "ts": now, "p": pipeline, "s": stream},
                )
                if result.rowcount == 0:
                    conn.execute(
                        _text(
                            f"INSERT INTO {_TABLE_NAME} "
                            "(pipeline, stream, state, updated_at) "
                            "VALUES (:p, :s, :state, :ts)"
                        ),
                        {"p": pipeline, "s": stream, "state": state_json, "ts": now},
                    )



def _text(sql: str):
    """Lazy import of sqlalchemy.text to avoid hard import at module load."""
    from sqlalchemy import text
    return text(sql)


# ---------------------------------------------------------------------------
# FileStateStore
# ---------------------------------------------------------------------------

_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_\-]")


def _safe_path_segment(name: str) -> str:
    """Replace filesystem-unsafe characters with underscores."""
    return _SAFE_NAME_RE.sub("_", name)


class FileStateStore:
    """Persists state as JSON files under *base_path*.

    Files are stored at ``{base_path}/{pipeline}/{stream}.json``.
    Pipeline and stream names are sanitized for filesystem safety.
    """

    def __init__(self, base_path: str | Path) -> None:
        self._base = Path(base_path)
        self._lock = threading.Lock()

    def get(self, pipeline: str, stream: str) -> dict:
        path = self._path_for(pipeline, stream)
        if not path.exists():
            return {}
        with open(path) as fh:
            return json.load(fh)

    def list_streams(self, pipeline: str) -> list[str]:
        """Return all stream names that have persisted state for *pipeline*."""
        dir_ = self._base / _safe_path_segment(pipeline)
        if not dir_.exists():
            return []
        return sorted(p.stem for p in dir_.glob("*.json"))

    def set(self, pipeline: str, stream: str, state: dict) -> None:
        path = self._path_for(pipeline, stream)
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            with open(tmp, "w") as fh:
                json.dump(state, fh)
            tmp.replace(path)  # atomic rename on POSIX; best-effort on Windows

    def _path_for(self, pipeline: str, stream: str) -> Path:
        return (
            self._base
            / _safe_path_segment(pipeline)
            / f"{_safe_path_segment(stream)}.json"
        )
