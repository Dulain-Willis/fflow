# Field hashing via HMAC-SHA256 for PHI compliance

fflow loads health data that contains PHI (SSN, DOB, name, etc.). To meet HIPAA requirements we must ensure PHI never lands at the destination in plaintext. We chose HMAC-SHA256 applied in the pipeline's extract→load loop, replacing specified column values in-place before any write occurs.

We chose HMAC-SHA256 over simpler approaches for two reasons: it is cryptographically irreversible (unlike dlt's `mask_columns` replace-with-stars, which is lossy but not cryptographic), and it is deterministic under a fixed key (unlike nullification), which preserves the ability to JOIN and GROUP on hashed identifiers across loads.

A secret key (`hash_key`) is required at the Pipeline level and must be supplied via environment variable. Fields are declared per-stream in `StreamConfig.hash_fields`. A missing or misspelled field name raises an error at startup (mirror-mode streams) or on the first chunk (SQL-file mode streams) — never silently passes — because a silent skip would leave PHI unhashed at the destination.

## Considered Options

- **dlt `mask_columns`** — replaces with a static string; consistent within a run but `"alice@example.com"` and `"bob@example.com"` both become `"******"`, destroying JOIN ability.
- **Nullification** — sets field to NULL; destroys all analytical value.
- **Destination-side hashing (SQL)** — requires PHI to transit through an intermediate system (e.g. MSSQL) first, which is what this feature exists to eliminate.
