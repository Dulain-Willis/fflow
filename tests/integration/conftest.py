# tests/integration/conftest.py
#
# Session-scoped fixtures that open real connections.
# Requires VPN + internal network access.
# Gated on env vars: CACHE_{NS}_USER / CACHE_{NS}_PASSWORD, MSSQL_DSN / MSSQL_CONN_STR.
# All integration tests must be marked @pytest.mark.integration.

import os
import pytest
from dotenv import load_dotenv

load_dotenv()

JDBC_JAR = os.getenv("CACHE_JDBC_JAR", "./cache-jdbc-2.0.0.jar")
JDBC_DRIVER = os.getenv("CACHE_JDBC_DRIVER", "com.intersys.jdbc.CacheDriver")

NAMESPACE_URLS = {
    "RECPROD": "jdbc:Cache://PLN01PARTREC01:1972/RECPROD",
    "ACPROD":  "jdbc:Cache://PLN01PARTAC01:1972/ACPROD",
    "AGYPROD": "jdbc:Cache://PLN01PARTCAP01:1972/AGYPROD",
    "CCPROD":  "jdbc:Cache://PLN01PARTCAP04:1972/CCPROD",
    "DBPROD":  "jdbc:Cache://10.1.5.74:1972/DBPROD",
}

NAMESPACE_CREDS = {
    ns: (
        os.getenv(f"CACHE_{ns}_USER", ""),
        os.getenv(f"CACHE_{ns}_PASSWORD", ""),
    )
    for ns in NAMESPACE_URLS
}
