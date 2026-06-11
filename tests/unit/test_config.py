"""Unit tests for fflow.common.config."""

import pytest
from pydantic import ValidationError

from fflow.common.config import SchemaContract, StreamConfig


class TestStreamConfig:
    def test_default_append(self):
        cfg = StreamConfig(name="phone")
        assert cfg.write_disposition == "append"
        assert cfg.merge_key == []

    def test_merge_requires_key(self):
        with pytest.raises(ValidationError, match="merge_key must be set"):
            StreamConfig(name="phone", write_disposition="merge")

    def test_merge_with_key_ok(self):
        cfg = StreamConfig(name="phone", write_disposition="merge", merge_key=["id"])
        assert cfg.merge_key == ["id"]

    def test_replace_no_key_ok(self):
        cfg = StreamConfig(name="lookup", write_disposition="replace")
        assert cfg.write_disposition == "replace"


class TestSchemaContract:
    def test_defaults_to_evolve(self):
        sc = SchemaContract()
        assert sc.on_new_column == "evolve"
        assert sc.on_dropped_column == "evolve"
