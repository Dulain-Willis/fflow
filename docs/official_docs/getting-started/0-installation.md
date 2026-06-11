# Getting Started

## Prerequisites

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/getting-started/installation/) or pip

---

## Installation

`fflow` is installed from GitHub. It ships with a CLI — `cf` — that you use to run, check, and inspect pipelines.

### Using uv

**Starting a new project:**

```bash
uv add "fflow @ git+https://github.com/Capio-DecisionScience/fflow.git"
```

This adds `fflow` to your `pyproject.toml`, updates `uv.lock`, and installs it — all in one step.

**Cloning an existing project (e.g. `fflow-loader`):**

```bash
uv sync
```

Reproduces the exact locked environment from `uv.lock`. Run this after `git clone` or `git pull`.

**Verify:**

```bash
uv run cf --help
```

---

### Using pip

**Direct install:**

```bash
pip install "fflow @ git+https://github.com/Capio-DecisionScience/fflow.git"
```

**Via `requirements.txt`:**

Add this line to your `requirements.txt`:

```
fflow @ git+https://github.com/Capio-DecisionScience/fflow.git
```

Then install:

```bash
pip install -r requirements.txt
```

**Via `pyproject.toml`:**

Add to your `dependencies`:

```toml
[project]
dependencies = [
    "fflow @ git+https://github.com/Capio-DecisionScience/fflow.git",
]
```

Then install:

```bash
pip install -e .
```

**Verify:**

```bash
cf --help
```

---

## Optional extras

The base install covers REST and SQL sources. For other connectors, install the relevant extra:

| Extra | What it enables | Command |
|---|---|---|
| `cache` | InterSystems Cache source | `uv add "fflow[cache] @ git+..."` |
| `s3` | S3 destination (Parquet/JSONL) | `uv add "fflow[s3] @ git+..."` |
| `redshift` | Redshift destination | `uv add "fflow[redshift] @ git+..."` |

You don't need to decide upfront — `fflow` will tell you which extra to install if you try to use a connector that requires one.
