# ADR 0001 — Agnostic loader: decouple from Cache and MSSQL

## Status

Accepted

## Context

`artiva_etl` is tightly coupled to InterSystems Cache as the only source and SQL Server as the only destination. Every abstraction leaks these specifics: connection logic, JDBC quirks, ODBC bulk-copy patterns, and namespace config are all baked into the same modules. Adding a new source or destination requires forking core logic.

The business already uses Cache, REST APIs, and SQL Server simultaneously. S3 and Redshift are on the near horizon. A hardcoded two-connector system cannot scale.

## Decision

`fflow` is built as a generic EL loader. Core modules (`common/`, `extract/`, `load/`, `pipeline/`) contain zero references to Cache, SQL Server, or any specific technology. All technology-specific logic lives in connector implementations under `sources/` and `destinations/`, which satisfy a shared protocol.

## Consequences

- Any source or destination that implements the protocol (`check`, `discover`, `read` / `write`, `commit`) works with the pipeline engine.
- Migrating from Cache/MSSQL to a different stack requires only swapping connector classes — the pipeline, state, and streaming engine are unchanged.
- `CacheSource` and `MSSQLDestination` become first-party connectors rather than the foundation.
- `artiva_etl` remains in production until each pipeline is migrated and validated.
