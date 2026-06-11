# Known Issues

## Auto state store path double-nests pipeline name

**Symptom:** When using `@pipeline` without an explicit `state_store`, state files land at
`.state/{name}/{name}/{stream}.json` instead of `.state/{name}/{stream}.json`.

**Root cause:** `FileStateStore` appends `/{pipeline}/{stream}` to its `base_path`.
The `@pipeline` decorator sets `base_path=".state/{pipeline_name}"`, which
causes the pipeline name to appear twice in the path.

**Fix:** Change default in `fflow/decorators.py`:
```python
# current (wrong)
FileStateStore(base_path=f".state/{pipeline_name}")

# fix
FileStateStore(base_path=".state")
```

**Workaround:** Pass `state_store=FileStateStore(base_path=".state")` explicitly to `@pipeline`.
