# ADR 0002 — Copy dlt's package layout

## Status

Accepted

## Context

We need a package structure for `fflow`. Options considered:

1. Flat layout — all modules at top level
2. Domain-grouped — e.g., `connectors/`, `core/`, `runner/`
3. Copy dlt's layout — `common/`, `extract/`, `load/`, `pipeline/`, `sources/`, `destinations/`, `helpers/`

## Decision

Copy dlt's package layout exactly. dlt is a production-grade EL library solving the same problem. Its structure has been battle-tested and reflects the natural domain decomposition of a data loader.

```
fflow/
  common/        # protocols, schema types, state store
  extract/       # streaming engine, pipe iterator
  load/          # write disposition, batch writer
  pipeline/      # Pipeline class
  sources/       # connector impls: CacheSource, RestSource, SQLSource
  destinations/  # connector impls: MSSQLDestination, S3Destination
  helpers/       # airflow.py (optional integration)
  cli.py         # CLI entry point
```

## Consequences

- Developers familiar with dlt can navigate `fflow` immediately.
- We can copy dlt source files directly and adapt (not rewrite from scratch).
- The boundary between core (`common/`, `extract/`, `load/`, `pipeline/`) and connectors (`sources/`, `destinations/`) is structurally enforced.
