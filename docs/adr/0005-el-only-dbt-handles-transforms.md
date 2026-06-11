# ADR 0005 — EL only; dbt handles transforms

## Status

Accepted

## Context

`artiva_etl` occasionally performs light transformations during load (column renames, type coercions, derived columns). This couples business logic to the loader and makes it harder to audit, test, or reuse transforms independently.

The Modern Data Stack separates Extract+Load from Transform:
- EL tools (Fivetran, dlt, Airbyte) move raw data to the destination unchanged
- Transformation tools (dbt, SQLMesh) run SQL against the destination after load

## Decision

`fflow` is EL only. No transforms in the loader.

- Sources extract data as-is. The only source-side SQL is to select which rows to extract (incremental cursor), not to reshape data.
- Destinations write raw rows. Schema evolution (new columns) is handled automatically. Data reshaping is not.
- All business-logic transforms live in dbt models in the `fflow-loader` repo or a dedicated dbt project.

SQL-file mode is still EL: the `.sql` file defines *what to extract* from the source (e.g., a join or a filter), not a transform on the destination side. The result rows are written verbatim.

## Consequences

- Loader stays simple and testable — no business logic to unit test.
- Transforms are visible in SQL, version-controlled, and independently testable via dbt.
- Data lineage is cleaner: raw layer populated by `fflow`, transformed layer populated by dbt.
- Any transformation currently in `artiva_etl` must be migrated to dbt before the pipeline can be cut over.
