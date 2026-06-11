"""Tests for the @pipeline and @stream() decorator API."""

from __future__ import annotations

import pytest

from fflow import pipeline, stream
from fflow.common.state import FileStateStore
from fflow.pipeline.pipeline import Pipeline
from fflow.sources.rest import (
    JSONLinkPaginator,
    JSONResponseCursorPaginator,
    RestSource,
    RestStreamConfig,
    rest,
)


# ---------------------------------------------------------------------------
# Minimal fake destination for testing without a real DB
# ---------------------------------------------------------------------------

class _FakeDestination:
    def check(self): pass
    def prepare_stream(self, *a, **kw): pass
    def write(self, *a, **kw): pass
    def commit(self, *a, **kw): pass
    def rollback(self, *a, **kw): pass


# ---------------------------------------------------------------------------
# @stream() tests
# ---------------------------------------------------------------------------

def test_stream_outside_pipeline_raises():
    with pytest.raises(RuntimeError, match="must be defined inside a @pipeline"):
        @stream()
        def orphan():
            return RestStreamConfig(endpoint="/x.json")


def test_stream_bad_return_type_raises():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    with pytest.raises(TypeError, match="must return a StreamConfig instance"):
        @pipeline(source=src, destination=dest)
        def bad():
            @stream()
            def tickets():
                return {"endpoint": "/tickets.json"}  # dict, not StreamConfig


# ---------------------------------------------------------------------------
# @pipeline tests
# ---------------------------------------------------------------------------

def test_pipeline_returns_pipeline_instance():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest)
    def my_pipeline():
        @stream()
        def records():
            return RestStreamConfig(endpoint="/records.json")

    assert isinstance(my_pipeline, Pipeline)


def test_pipeline_name_from_function():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest)
    def zendesk():
        @stream()
        def tickets():
            return RestStreamConfig(endpoint="/tickets.json")

    assert zendesk.name == "zendesk"


def test_pipeline_name_explicit_override():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest, name="custom_name")
    def my_fn():
        @stream()
        def records():
            return RestStreamConfig(endpoint="/records.json")

    assert my_fn.name == "custom_name"


def test_pipeline_auto_file_state_store():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest)
    def my_pipeline():
        @stream()
        def records():
            return RestStreamConfig(endpoint="/records.json")

    assert isinstance(my_pipeline._state_store, FileStateStore)
    assert str(my_pipeline._state_store._base) == ".state/my_pipeline"


def test_pipeline_state_store_override():
    src = rest("https://api.example.com")
    dest = _FakeDestination()
    custom_store = FileStateStore(base_path=".state/custom")

    @pipeline(source=src, destination=dest, state_store=custom_store)
    def my_pipeline():
        @stream()
        def records():
            return RestStreamConfig(endpoint="/records.json")

    assert my_pipeline._state_store is custom_store


def test_pipeline_streams_wired_to_source():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest)
    def my_pipeline():
        @stream()
        def tickets():
            return RestStreamConfig(endpoint="/tickets.json", data_path="tickets")

        @stream()
        def users():
            return RestStreamConfig(endpoint="/users.json", data_path="users")

    assert "tickets" in src._stream_cfgs
    assert "users" in src._stream_cfgs
    assert src._stream_cfgs["tickets"].endpoint == "/tickets.json"
    assert src._stream_cfgs["users"].endpoint == "/users.json"


def test_pipeline_stream_name_from_function():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest)
    def my_pipeline():
        @stream()
        def my_stream():
            return RestStreamConfig(endpoint="/data.json")

    cfg = src._stream_cfgs["my_stream"]
    assert cfg.name == "my_stream"


def test_write_disposition_must_be_explicit():
    """No inference — write_disposition defaults to 'append' even with merge_key set."""
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest)
    def my_pipeline():
        @stream()
        def records():
            return RestStreamConfig(endpoint="/records.json", merge_key=["id"])

    cfg = src._stream_cfgs["records"]
    # merge_key present but write_disposition not set → stays "append" (not inferred)
    assert cfg.write_disposition == "append"


def test_write_disposition_explicit_replace():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest)
    def my_pipeline():
        @stream()
        def records():
            return RestStreamConfig(endpoint="/records.json", write_disposition="replace")

    cfg = src._stream_cfgs["records"]
    assert cfg.write_disposition == "replace"


def test_hash_fields_on_stream_config():
    src = rest("https://api.example.com")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest, hash_key="x" * 64)
    def my_pipeline():
        @stream()
        def users():
            return RestStreamConfig(
                endpoint="/users.json",
                write_disposition="merge",
                merge_key=["id"],
                hash_fields=["email", "phone"],
            )

    cfg = src._stream_cfgs["users"]
    assert cfg.hash_fields == ["email", "phone"]
    assert my_pipeline._hash_key == "x" * 64


def test_full_zendesk_shape():
    """Smoke test: zendesk-shaped pipeline builds without error."""
    src = rest("https://buoyfi.zendesk.com/api/v2")
    dest = _FakeDestination()

    @pipeline(source=src, destination=dest, hash_key="a" * 64,
              loaded_at_extra_timezones=[("central", "America/Chicago")])
    def zendesk():

        @stream()
        def tickets():
            return RestStreamConfig(
                endpoint="/tickets.json",
                data_path="tickets",
                paginator=JSONLinkPaginator(next_url_path="next_page"),
                params={"per_page": 100},
                write_disposition="merge",
                merge_key=["id"],
                hash_fields=["subject", "raw_subject", "description"],
            )

        @stream()
        def users():
            return RestStreamConfig(
                endpoint="/users.json",
                data_path="users",
                paginator=JSONResponseCursorPaginator(
                    cursor_path="after_cursor",
                    cursor_param="page[after]",
                ),
                params={"per_page": 100, "role": "end-user"},
                write_disposition="merge",
                merge_key=["id"],
                hash_fields=["name", "email", "phone"],
            )

    assert isinstance(zendesk, Pipeline)
    assert zendesk.name == "zendesk"
    assert set(src._stream_cfgs.keys()) == {"tickets", "users"}
    assert src._stream_cfgs["tickets"].write_disposition == "merge"
    assert src._stream_cfgs["users"].write_disposition == "merge"
