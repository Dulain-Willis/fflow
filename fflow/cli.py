"""fflow CLI — cf / fflow commands.

Pipeline discovery (resolution order):
  1. ``--config MODULE_OR_FILE`` flag
  2. ``CAPIO_FLOW_CONFIG`` environment variable
  3. ``pipelines.py`` in the current working directory

The config module must expose::

    PIPELINES: dict[str, Pipeline | Callable[[], Pipeline]]

Keys must match ``Pipeline.name``.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from fflow.pipeline.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Discovery + registry loading
# ---------------------------------------------------------------------------

def _resolve_config_spec(override: str | None) -> str | None:
    """Return config spec from flag > env var > CWD convention."""
    if override:
        return override
    env = os.environ.get("CAPIO_FLOW_CONFIG")
    if env:
        return env
    cwd_path = Path.cwd() / "pipelines.py"
    if cwd_path.exists():
        return str(cwd_path)
    return None


def _load_registry(spec: str | None) -> dict[str, "Pipeline"]:
    """Import config module and return normalized ``{name: Pipeline}`` registry.

    Accepts both eager Pipeline instances and lazy ``Callable[[], Pipeline]``
    factories.  Validates that every registry key matches ``pipeline.name``.
    """
    if spec is None:
        _die(
            "No pipeline config found.\n"
            "  Use --config MODULE, set CAPIO_FLOW_CONFIG env var, "
            "or place pipelines.py in the current directory."
        )

    is_path = spec.endswith(".py") or os.sep in spec or "/" in spec
    if is_path:
        path = Path(spec).resolve()
        if not path.exists():
            _die(f"Config file not found: {path}")
        spec_obj = importlib.util.spec_from_file_location(path.stem, path)
        if spec_obj is None or spec_obj.loader is None:
            _die(f"Could not load config from: {path}")
        module = importlib.util.module_from_spec(spec_obj)
        spec_obj.loader.exec_module(module)  # type: ignore[union-attr]
        origin = str(path)
    else:
        try:
            module = importlib.import_module(spec)
        except ImportError as exc:
            _die(f"Could not import config module {spec!r}: {exc}")
        origin = spec

    from fflow.pipeline.pipeline import Pipeline as PipelineClass

    # Auto-discovery: scan module globals for Pipeline instances.
    # Import pipeline modules in your config module to trigger @pipeline registration.
    registry: dict[str, Pipeline] = {}
    for attr_name, value in vars(module).items():
        if isinstance(value, PipelineClass):
            registry[value.name] = value

    if not registry:
        _die(
            f"Config {origin!r} has no Pipeline instances. "
            "Import your @pipeline-decorated modules to register them."
        )

    print(f"[fflow] config: {origin}", file=sys.stderr)
    return registry


def _get_pipeline(registry: dict[str, "Pipeline"], name: str) -> "Pipeline":
    if name not in registry:
        available = ", ".join(sorted(registry)) or "(none)"
        _die(f"Pipeline {name!r} not found. Available: {available}")
    return registry[name]


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

def _cmd_list(args: argparse.Namespace, registry: dict[str, "Pipeline"]) -> None:
    if not registry:
        print("No pipelines configured.")
        return

    rows = [(name, pipeline.configured_streams) for name, pipeline in sorted(registry.items())]
    col1 = max(len(r[0]) for r in rows)
    col1 = max(col1, len("pipeline"))
    header = f"{'pipeline':<{col1}}  configured streams"
    print(header)
    print(f"{'─' * col1}  {'─' * 40}")
    for name, streams in rows:
        stream_str = (
            ", ".join(streams)
            if streams
            else "(none — add StreamConfig entries to the pipeline definition)"
        )
        print(f"{name:<{col1}}  {stream_str}")


def _cmd_run(args: argparse.Namespace, registry: dict[str, "Pipeline"]) -> None:
    pipeline = _get_pipeline(registry, args.pipeline)
    streams = args.stream or None  # action="append" -> None when not provided
    label = f"streams={streams}" if streams else "all streams"
    flags = " [full-refresh]" if args.full_refresh else ""
    print(f"Running '{pipeline.name}' ({label}){flags} ...", file=sys.stderr)
    pipeline.run(
        streams=streams,
        full_refresh=args.full_refresh,
        workers=args.workers,
        chunk_size=args.chunk_size,
    )
    print("Done.", file=sys.stderr)


def _cmd_check(args: argparse.Namespace, registry: dict[str, "Pipeline"]) -> None:
    pipeline = _get_pipeline(registry, args.pipeline)
    print(f"Checking '{pipeline.name}' ...", file=sys.stderr)
    try:
        pipeline.check()
        print("  ✓ Source OK")
        print("  ✓ Destination OK")
    except Exception as exc:
        print(f"  ✗ {exc}")
        sys.exit(1)


def _cmd_state(args: argparse.Namespace, registry: dict[str, "Pipeline"]) -> None:
    pipeline = _get_pipeline(registry, args.pipeline)

    if args.stream:
        state_data = {args.stream: pipeline.get_state(args.stream)}
    else:
        state_data = pipeline.list_state()
        if not state_data:
            # Fall back to configured streams (shows empty state on first run)
            state_data = {s: {} for s in pipeline.configured_streams}

    if not state_data:
        print(f"No state found for '{pipeline.name}'. Run it first.")
        return

    col1 = max(len(s) for s in state_data)
    col1 = max(col1, len("stream"))
    print(f"{'stream':<{col1}}  state")
    print(f"{'─' * col1}  {'─' * 40}")
    for stream, state in sorted(state_data.items()):
        print(f"{stream:<{col1}}  {json.dumps(state)}")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cf",
        description="fflow — agnostic data loader",
    )
    parser.add_argument(
        "--config",
        metavar="MODULE_OR_FILE",
        help=(
            "Pipeline config: dotted module name (fflow_loader.pipelines) "
            "or path to a .py file. Overrides CAPIO_FLOW_CONFIG env var."
        ),
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # list
    sub.add_parser("list", help="Show configured pipelines and streams.")

    # run
    p_run = sub.add_parser("run", help="Run a pipeline.")
    p_run.add_argument("--pipeline", required=True, metavar="NAME")
    p_run.add_argument(
        "--stream",
        action="append",
        metavar="NAME",
        help="Stream to run (repeat for multiple). Default: all.",
    )
    p_run.add_argument("--full-refresh", action="store_true", help="Ignore state; reload all rows.")
    p_run.add_argument("--workers", type=int, default=5, metavar="N")
    p_run.add_argument("--chunk-size", type=int, default=1000, metavar="N")

    # check
    p_check = sub.add_parser("check", help="Verify source and destination connections.")
    p_check.add_argument("--pipeline", required=True, metavar="NAME")

    # state
    p_state = sub.add_parser("state", help="Show incremental state (watermarks) for a pipeline.")
    p_state.add_argument("--pipeline", required=True, metavar="NAME")
    p_state.add_argument("--stream", metavar="NAME", help="Specific stream to inspect.")

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Load .env from CWD (or any parent) before anything else so credentials
    # are in os.environ when pipeline factories run.  Mirrors how Django, FastAPI,
    # and other Python CLIs handle .env — the user never needs to source it manually.
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # python-dotenv optional; silently skip if absent

    parser = _build_parser()
    args = parser.parse_args()

    spec = _resolve_config_spec(args.config)
    registry = _load_registry(spec)

    dispatch = {
        "list": _cmd_list,
        "run": _cmd_run,
        "check": _cmd_check,
        "state": _cmd_state,
    }
    dispatch[args.command](args, registry)


def _die(msg: str, exit_code: int = 1) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
