"""Field-level HMAC-SHA256 hashing for PHI/PII compliance.

Applied in the pipeline's extract→load loop after extraction but before
writing to the destination.  PHI never lands at the destination.

Design decisions: ADR-0011
"""

from __future__ import annotations

import hashlib
import hmac as _hmac
from typing import Any


def hash_field(value: Any, key: str) -> str | None:
    """Return HMAC-SHA256 hex digest of ``str(value)``, or ``None`` if value is ``None``."""
    if value is None:
        return None
    return _hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()


def apply_field_hashing(chunk: list[dict], fields: set[str], key: str) -> list[dict]:
    """Return *chunk* with *fields* hashed and renamed to ``{field}_hash``.

    The original key is replaced: ``email`` → ``email_hash`` containing a
    64-character HMAC-SHA256 hex digest.  Non-hashed fields are unchanged.
    """
    return [
        {
            (f"{k}_hash" if k in fields else k): (hash_field(v, key) if k in fields else v)
            for k, v in row.items()
        }
        for row in chunk
    ]


def validate_hash_fields(stream: str, fields: list[str], known: set[str]) -> None:
    """Raise ``ValueError`` if any field in *fields* is absent from *known*.

    A missing hash field means PHI could land at the destination unhashed —
    always fail loud rather than silently skip.
    """
    missing = [f for f in fields if f not in known]
    if missing:
        raise ValueError(
            f"Stream '{stream}': hash_fields references unknown field(s): {missing}. "
            "Check for typos — a misconfigured hash_field means PHI lands unhashed."
        )
