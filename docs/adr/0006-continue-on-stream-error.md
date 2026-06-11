# Continue on stream error in pipeline.run()

`pipeline.run()` attempts all streams regardless of individual stream failures. Errors are collected and surfaced at the end. A failed stream does not persist state, so it retries fully on the next run.

The natural alternative — fail-fast on first error — would block all remaining streams whenever one fails. For a pipeline with 20 tables, one bad stream would stall 19 others until the next scheduled run. That's worse than partial success. When running via Airflow (task-per-stream), this behavior is already implicit — a task failure never blocks sibling tasks.
