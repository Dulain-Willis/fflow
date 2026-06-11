# SQL-file mode cursor injection via `{{cursor_value}}` literal substitution

In SQL-file mode with incremental loading enabled, the source must inject the cursor value (e.g. the last-seen `STTRCID`) into the user-provided SQL before executing it. We use a `{{cursor_value}}` placeholder in the SQL file that `CacheSource.read()` replaces with the literal integer value before the query runs.

## Considered Options

**JDBC `?` parameter** — pass the cursor value as a bound parameter. Rejected: Cache 2018.1 has a known JDBC parameter bug on complex queries (also documented in `artiva_etl`'s fallback logic at `loader.py:1904`).

**Template substitution with literal value (chosen)** — replace `{{cursor_value}}` with the integer literal. Avoids the JDBC bug entirely, consistent with `artiva_etl`'s `{{CID_FILTER}}` / `{{DATE_FILTER}}` pattern, and simple to implement.

**SQL-file mode always full-refresh** — no cursor injection at all. Rejected: would make SQL-file mode impractical for any large incremental table.

## Consequences

- SQL files intended for incremental use **must** contain `{{cursor_value}}` where the filter belongs. `CacheSource.read()` raises `ValueError` if `incremental` is set but the token is absent.
- SQL files that are not incremental (or are first-run full loads) are executed as-is — no substitution occurs.
- dlt has no equivalent: it builds queries programmatically via SQLAlchemy and passes `incremental.last_value` through a query adapter callback. fflow's SQL-file approach is a deliberate departure to support hand-written Cache SQL that SQLAlchemy cannot generate.
