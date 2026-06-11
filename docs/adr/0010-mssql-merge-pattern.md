# ADR-0010 — MSSQLDestination Merge Pattern

**Date:** 2026-06-07  
**Status:** Accepted

---

## Context

`MSSQLDestination` must support three write dispositions: `append`, `replace`, and `merge`.
The `merge` disposition is the most complex: it must upsert rows by merge key and correctly
handle CDC delete events produced by `CacheSource`.

Two design decisions required explicit choice:

1. **How to handle CDC delete rows** (`STTRCTRIGGER='D'`) from `CacheSource.read()`.
2. **How to ensure the latest event wins** when a sync window contains multiple events for
   the same merge key (e.g., `U@100`, `D@101` — the delete must win).

---

## Decision

### Staging as connection-scoped temp table

Merge writes through a connection-scoped MSSQL local temp table (`#fflow_staging`).
The table is created at the start of `commit()` and dropped before `conn.commit()`.

Using `#temp` (not a persistent staging table) means:
- Concurrent pipeline runs targeting the same destination table never collide.
- The temp table is automatically reclaimed if the connection closes unexpectedly.
- No explicit schema/naming conventions needed for staging.

### Staging schema

The staging table always includes:
- All destination columns (`dest_columns`)
- `STTRCID BIGINT NULL` — CDC sequence number from `CacheSource`
- `STTRCTRIGGER NVARCHAR(1) NULL` — CDC event type from `CacheSource`

These CDC columns are written to staging but **never** to the destination table.

### Dedup ordering

All buffered rows are inserted to staging, including CDC delete rows.  A single
`ROW_NUMBER() OVER (PARTITION BY merge_keys ORDER BY ...)` dedup collapses each
merge key to its latest event before any DML touches the target.

Order-of-precedence for `ORDER BY`:
1. `COALESCE([STTRCID], 0) DESC` — if any buffered row has a non-null `STTRCID`
2. `[cursor_field] DESC` — if the stream declares an incremental cursor field
3. `(SELECT NULL)` — deterministic key assumption (idempotent sources)

This ensures `D@101` beats `U@100` for the same key: the delete survives dedup
and the row is not re-inserted.

### Delete-then-insert upsert

After dedup:

```sql
-- 1. Remove all merge-key matches from target (covers both upsert keys and CDC D keys).
DELETE t FROM [ODS].[target] t
INNER JOIN #fflow_staging s ON t.[pk] = s.[pk]

-- 2. Insert surviving non-delete rows.
INSERT INTO [ODS].[target] ([col1], [col2], ...)
SELECT [col1], [col2], ... FROM #fflow_staging
WHERE [STTRCTRIGGER] != 'D' OR [STTRCTRIGGER] IS NULL
```

CDC D rows are deleted from target (step 1) but excluded from the re-insert (step 2).
Non-CDC rows follow the same delete-then-reinsert upsert path.

### Transaction boundaries

- DDL in `prepare_stream()` runs with `autocommit=True` so schema changes are durable
  even if the subsequent data write fails.
- Each stream gets its own `pyodbc.Connection` so `commit(stream_a)` and
  `rollback(stream_b)` are fully isolated.
- All merge DML (`CREATE #staging`, `INSERT`, `DELETE`, dedup, `DROP #staging`) runs
  in a single transaction; any failure triggers `conn.rollback()`.

---

## Consequences

- `MSSQLDestination` is not stateless between `write()` and `commit()` — rows are
  buffered in Python memory. For very large streams, memory pressure is a known trade-off.
- `STTRCID` and `STTRCTRIGGER` are treated as reserved CDC metadata column names.
  A source that legitimately produces columns with these names will have them silently
  excluded from the destination table.
- The merge pattern relies on MSSQL supporting transactional DDL for local temp tables
  (`CREATE TABLE #x` inside an explicit transaction can be rolled back).
