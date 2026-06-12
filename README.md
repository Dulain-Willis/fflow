# fflow

An agnostic data loader: any source → any destination. Modeled on dlt and Airbyte. Airflow orchestrates but does not own.

## Pipeline

```python
from fflow.pipeline import Pipeline
from fflow.sources.sql import SQLSource
from fflow.destinations.mssql import MSSQLDestination
from fflow.common.config import StreamConfig
from fflow.common.state import SqlStateStore

pipeline = Pipeline(
    name="postgres_to_mssql",
    source=SQLSource(connection_string="postgresql://user:pass@host/db"),
    destination=MSSQLDestination(connection_string="mssql+pyodbc://..."),
    state_store=SqlStateStore(connection_string="mssql+pyodbc://..."),
    streams=[
        StreamConfig(name="orders", write_disposition="merge", merge_key=["order_id"]),
        StreamConfig(name="customers", write_disposition="append"),
    ],
)

pipeline.run()
```

## Quickstart

```bash
pip install fflow

# Optional extras
pip install "fflow[s3]"       # S3 destination
pip install "fflow[redshift]" # Redshift destination
pip install "fflow[cache]"    # InterSystems Cache source
```

## Sources & Destinations

| Type | Connector | Notes |
|------|-----------|-------|
| Source | `SQLSource` | Any SQLAlchemy-supported DB |
| Source | `CacheSource` | InterSystems Cache via JDBC |
| Source | `RESTSource` | Generic HTTP/REST APIs |
| Destination | `MSSQLDestination` | SQL Server — append / replace / merge |
| Destination | `S3Destination` | Parquet files on S3 |
| Destination | `RedshiftDestination` | Redshift via S3 COPY |

## Architecture Decisions

All ADRs are in [`docs/adr/`](docs/adr/).
