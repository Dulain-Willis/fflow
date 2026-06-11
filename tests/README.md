# Tests

## Structure

```
tests/unit/
  conftest.py       # stubs pyodbc/jaydebeapi; exposes fake_mssql fixture
  fakes.py          # FakeMsSqlClient — in-memory stand-in, no real DB needed
  test_loader.py    # business logic tests (priority)
  test_merge.py     # merge SQL generation tests
  test_mssql_client.py
  test_cache_client.py
```

## Conventions

- **Use `fake_mssql`** fixture instead of patching pyodbc — tests exercise real loader logic
- Group tests in classes by function under test (`class TestLoadOneChunk`)
- Bug fixes: write a failing test first, then fix, leave it as a regression test
- All unit tests should run in seconds — no subprocess, no real DB, no network
