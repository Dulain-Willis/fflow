# dlt PipeIterator + FuturesPool as the streaming engine

We copy dlt's `PipeIterator` + `FuturesPool` (Python `ThreadPoolExecutor`) as the runtime engine for moving data from source to destination. Source runs in worker threads; destination consumes concurrently from the pipe.

asyncio was the alternative. We chose threads because our I/O is dominated by JDBC (Cache) and ODBC (MSSQL) — both are blocking C extensions that release the GIL, so threads genuinely parallelize. asyncio would require async-compatible drivers for every connector, which don't exist for JDBC/ODBC. dlt made the same choice for the same reasons.
