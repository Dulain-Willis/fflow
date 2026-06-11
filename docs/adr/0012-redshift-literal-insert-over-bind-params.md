# Redshift bulk INSERT uses escaped SQL literals, not SQLAlchemy bind params

Redshift is pathologically slow with `executemany` over psycopg2 — inserting ~5k rows
via parameterized queries takes 10+ minutes and effectively hangs the pipeline.
We switched to building a single multi-row `INSERT INTO t(cols) VALUES (r1),(r2),...;`
string using manually escaped literals, chunked at 8 MB to stay under Redshift's 16 MB
statement limit. This mirrors dlt's `InsertValuesWriter` + `escape_redshift_literal`
pattern exactly.

Do not "fix" this back to `executemany` or SQLAlchemy `execute(sql, params)` — the
performance regression is severe. If S3 COPY is later configured, that path supersedes
this one for large loads.
