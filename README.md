# fflow

An agnostic data loader: any source → any destination. Modeled on dlt and Airbyte. Airflow orchestrates but does not own.

## Pipeline

```python
import os
from fflow import pipeline, stream
from fflow.sources.rest import rest, RestStreamConfig, BearerTokenAuth
from fflow.destinations.redshift import redshift

@pipeline(
    source=rest(
        "https://api.example.com/v1",
        auth=BearerTokenAuth(token=os.environ["API_TOKEN"]),
    ),
    destination=redshift(
        url=os.environ["REDSHIFT_URL"],
        schema="raw_data",
    ),
)
def my_pipeline():

    @stream()
    def orders():
        return RestStreamConfig(
            endpoint="/orders",
            data_path="orders",
            write_disposition="merge",
            merge_key=["order_id"],
        )

    @stream()
    def customers():
        return RestStreamConfig(
            endpoint="/customers",
            data_path="customers",
        )


my_pipeline.run()
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
