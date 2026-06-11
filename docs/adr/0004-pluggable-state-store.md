# ADR 0004 — Pluggable StateStore (not Airflow XCom)

## Status

Accepted

## Context

Incremental loads require persisting a cursor (watermark) between runs so each run picks up where the previous left off. Options for state persistence:

1. Airflow XCom — store watermarks in Airflow's metadata DB
2. File on disk / S3 — JSON files per pipeline/stream
3. Dedicated SQL table in the destination DB
4. Protocol-based `StateStore` with swappable implementations

## Decision

State is owned by `fflow`, not the orchestrator. A `StateStore` protocol defines the interface:

```python
class StateStore(Protocol):
    def get(self, pipeline: str, stream: str) -> dict: ...
    def set(self, pipeline: str, stream: str, state: dict) -> None: ...
```

Two implementations ship out of the box:
- `SqlStateStore` — persists to a `fflow_state` table in any SQLAlchemy-compatible DB (default)
- `FileStateStore` — persists JSON to a local path or S3 key (for destinations without SQL)

## Consequences

- Pipelines are portable across orchestrators (Airflow, Prefect, cron, manual).
- Watermarks survive Airflow metadata DB wipes or migrations.
- State can be inspected and reset with `cf state --pipeline ...` without touching Airflow.
- Adding a new `StateStore` backend (e.g., Redis) requires only implementing the protocol.
- XCom is unavailable to non-Airflow callers — the `StateStore` approach has no such dependency.
