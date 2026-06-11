# dlt schema_contract for schema evolution

We copy dlt's `schema_contract` pattern for handling schema drift between source and destination. Default mode is `evolve`: new columns are auto-added via `ALTER TABLE ADD COLUMN` (using Alembic). Pipeline config can set `freeze` to fail on any schema change.

The simpler alternative is always auto-evolve with no configuration (Airbyte's behavior). We chose configurable strictness because fflow is a library, not a managed platform — users may want to assert that a production pipeline never silently adds columns. The `evolve` default matches Airbyte behavior, so there is no regression for users who don't set `freeze`.
