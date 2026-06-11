# ADR 0003 — Python-as-config with Pydantic (no YAML)

## Status

Accepted

## Context

`artiva_etl` uses YAML files (`namespaces.yml`, etc.) for pipeline configuration. YAML has well-known limitations: no type validation, no IDE completion, no conditional logic, verbose syntax for nested structures, and runtime parse errors instead of import-time errors.

Options considered:
1. YAML (status quo)
2. TOML
3. Python + Pydantic

## Decision

Pipeline configs are Python files using Pydantic models. No YAML files anywhere in `fflow` or `fflow-loader`.

```python
# fflow-loader/pipelines/cache_recprod.py
from fflow.pipeline import Pipeline
from fflow.sources.cache import CacheSource
from fflow.destinations.mssql import MSSQLDestination

pipeline = Pipeline(
    name="cache_recprod_to_mssql",
    source=CacheSource(url="jdbc:Cache://...", ...),
    destination=MSSQLDestination(conn_str="..."),
)
```

## Consequences

- Full Python language available in config (conditionals, env lookups, computed values).
- Pydantic validates config at import time — misconfigured pipelines fail immediately, not mid-run.
- IDE provides completion and type checking on config objects.
- No new config format to learn or document.
- Config is code — version controlled, reviewable, testable.
