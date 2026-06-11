# fflow

An agnostic data loader: any source → any destination. Modeled on dlt and Airbyte. Airflow orchestrates but does not own. Replaces `artiva_etl` incrementally.

## Language

**Pipeline**:
A named unit of work pairing one source connector to one destination connector. Contains one or more streams. The top level of the run hierarchy.
_Avoid_: job, task, ETL job

**Source**:
A connector that implements `check()`, `discover()`, and `read()`. Yields rows from a data system (database, API, file store). Never writes.
_Avoid_: extractor, reader, origin

**Destination**:
A connector that implements `check()`, `write()`, and `commit()`. Receives rows and persists them. Never reads source data.
_Avoid_: target, sink, loader

**Stream**:
One logical table or endpoint within a source. The atomic unit a pipeline runs. A pipeline may contain many streams; Airflow maps one task to one stream.
_Avoid_: table, query, resource (use "resource" only when referring to dlt internals)

**Mirror mode**:
Load mode where `discover()` infers the schema automatically and all rows are copied 1:1 from source to destination. No SQL file required. Equivalent to Fivetran's default behavior.
_Avoid_: auto-discover (legacy term from artiva_etl), full load, schema discovery

**SQL-file mode**:
Load mode where a `.sql` file defines the source-side SELECT query. The result set is streamed to the destination. SQL runs on the source, never on the destination.
_Avoid_: custom query mode, transform mode

**State store**:
The component that persists incremental cursors (watermarks) between runs. Owned by the loader, not the orchestrator. Default implementation writes to a SQL table.
_Avoid_: checkpoint table, watermark store

**Snapshot**:
The initial full-load of a Cache source table into its SQL Server landing table. Occurs on the first run of an auto-discover incremental query when no CDC checkpoint exists.
_Avoid_: full load, initial load, seed

**Shuttle**:
The Java process (`cache-shuttle.jar`) that streams rows from Cache to SQL Server via `SQLServerBulkCopy`, bypassing the Python/JPype bridge. Maps result-set columns to destination columns by name.
_Avoid_: Java loader, bulk copy tool

**Auto-discover**:
Runtime introspection of a Cache table's schema that generates the landing DDL and SELECT SQL automatically. No hand-written `.sql` file is required.
_Avoid_: schema discovery, dynamic DDL

**Metadata columns**:
The ETL-managed columns appended to every auto-discover landing table, populated by the pipeline, not sourced from Cache. Regular (non-auto-discover) tables carry a subset. The full set for auto-discover tables is: `load_dttm`, `update_dttm` (Central time, `DATETIME2`), `loaded_at_utc`, `updated_at_utc` (UTC, `DATETIMEOFFSET`), `namespace`, `sid`.
_Avoid_: audit columns, system columns

**Batch-open timestamp**:
A server-side timestamp generated once per namespace at the start of Phase 3, used to backfill `load_dttm`/`update_dttm` and `loaded_at_utc`/`updated_at_utc` for all rows in that snapshot batch. Central and UTC values are generated in the same server-side `UPDATE` from `SYSDATETIME()` and `SYSUTCDATETIME()` respectively.
_Avoid_: ingestion time, row timestamp

**Checkpoint**:
A persisted CDC watermark (`MAX(STTRCID)`) written after a successful snapshot or incremental run. Its presence is what distinguishes a first-run namespace (needs snapshot) from an already-bootstrapped one (routes to incremental).
_Avoid_: watermark (use only when referring to the raw value, not the concept)

**Namespace**:
A logical Cache database instance (e.g. `RECPROD`, `ACPROD`). Each namespace maps to its own JDBC connection and contributes a distinct `sid` value to the landing table.
_Avoid_: environment, site, tenant

**Write disposition**:
The per-stream strategy for how rows land at the destination. One of `append` (insert only), `replace` (truncate + full reload), or `merge` (upsert on primary keys). Declared in pipeline config. Copied from dlt.
_Avoid_: sync mode, write mode, load strategy

**Field hashing**:
Irreversible HMAC-SHA256 transformation applied to specific columns before rows reach the destination. PHI/PII values are replaced with a 64-character hex digest and the column is renamed from `{field}` to `{field}_hash` (e.g. `email` → `email_hash`). The original value never lands at the destination. Requires a secret key (`hash_key`) set on the Pipeline. Declared per-stream via `hash_fields`.
_Avoid_: masking (that term means replace-with-stars), anonymization, encryption (reversible)

## Example dialogue

> **Dev:** Why does the landing table have a `namespace` column if we already know which Cache instance we pulled from?
>
> **Domain expert:** Because multiple namespaces load into the same target table — each row needs to carry its origin so queries and incremental MERGEs can scope to one namespace at a time.
>
> **Dev:** And `load_dttm` — is that when the specific row landed or when the batch started?
>
> **Domain expert:** It's the batch-open timestamp. Every row in a snapshot gets the same value: the moment Phase 3 opened. Incremental runs diverge `load_dttm` (set once on insert) from `update_dttm` (refreshed on each MERGE). The same insert-once vs refresh-on-change pattern applies to `loaded_at_utc` and `updated_at_utc` respectively.
