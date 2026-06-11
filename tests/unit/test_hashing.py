"""Unit tests for field hashing (ADR-0011)."""

from __future__ import annotations

import hashlib
import hmac

import pytest

from fflow.common.config import StreamConfig
from fflow.common.hashing import apply_field_hashing, hash_field, validate_hash_fields
from fflow.common.schema import Column, ColumnType, Schema, Stream
from fflow.pipeline.pipeline import Pipeline

KEY = "test-secret-key"


def _hmac_hex(value: str, key: str = KEY) -> str:
    return hmac.new(key.encode(), value.encode(), hashlib.sha256).hexdigest()


# ---------------------------------------------------------------------------
# hash_field
# ---------------------------------------------------------------------------


class TestHashField:
    def test_string_value(self):
        result = hash_field("123-45-6789", KEY)
        assert result == _hmac_hex("123-45-6789")

    def test_none_returns_none(self):
        assert hash_field(None, KEY) is None

    def test_non_string_coerced(self):
        assert hash_field(42, KEY) == _hmac_hex("42")

    def test_same_input_same_output(self):
        assert hash_field("alice@example.com", KEY) == hash_field("alice@example.com", KEY)

    def test_different_inputs_different_outputs(self):
        assert hash_field("alice@example.com", KEY) != hash_field("bob@example.com", KEY)

    def test_different_keys_different_outputs(self):
        assert hash_field("ssn", "key-a") != hash_field("ssn", "key-b")

    def test_result_is_64_char_hex(self):
        result = hash_field("test", KEY)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)


# ---------------------------------------------------------------------------
# apply_field_hashing
# ---------------------------------------------------------------------------


class TestApplyFieldHashing:
    def test_target_fields_hashed_and_renamed(self):
        chunk = [{"ssn": "123-45-6789", "name": "Alice", "id": 1}]
        result = apply_field_hashing(chunk, {"ssn"}, KEY)
        assert "ssn_hash" in result[0]
        assert "ssn" not in result[0]
        assert result[0]["ssn_hash"] == _hmac_hex("123-45-6789")

    def test_non_target_fields_unchanged(self):
        chunk = [{"ssn": "123-45-6789", "name": "Alice", "id": 1}]
        result = apply_field_hashing(chunk, {"ssn"}, KEY)
        assert result[0]["name"] == "Alice"
        assert result[0]["id"] == 1

    def test_multiple_fields(self):
        chunk = [{"ssn": "123", "dob": "1990-01-01", "id": 1}]
        result = apply_field_hashing(chunk, {"ssn", "dob"}, KEY)
        assert result[0]["ssn_hash"] == _hmac_hex("123")
        assert result[0]["dob_hash"] == _hmac_hex("1990-01-01")
        assert "ssn" not in result[0]
        assert "dob" not in result[0]

    def test_none_value_preserved_as_none(self):
        chunk = [{"ssn": None, "id": 1}]
        result = apply_field_hashing(chunk, {"ssn"}, KEY)
        assert result[0]["ssn_hash"] is None
        assert "ssn" not in result[0]

    def test_empty_chunk_returns_empty(self):
        assert apply_field_hashing([], {"ssn"}, KEY) == []

    def test_original_chunk_not_mutated(self):
        original = [{"ssn": "123", "id": 1}]
        apply_field_hashing(original, {"ssn"}, KEY)
        assert original[0]["ssn"] == "123"
        assert "ssn_hash" not in original[0]


# ---------------------------------------------------------------------------
# validate_hash_fields
# ---------------------------------------------------------------------------


class TestValidateHashFields:
    def test_all_fields_present_ok(self):
        validate_hash_fields("patients", ["ssn", "dob"], {"id", "ssn", "dob", "name"})

    def test_missing_field_raises(self):
        with pytest.raises(ValueError, match="unknown field"):
            validate_hash_fields("patients", ["ssnn"], {"id", "ssn"})

    def test_error_message_contains_stream_name(self):
        with pytest.raises(ValueError, match="patients"):
            validate_hash_fields("patients", ["typo"], {"id", "ssn"})

    def test_error_message_contains_missing_field(self):
        with pytest.raises(ValueError, match="typo"):
            validate_hash_fields("patients", ["typo"], {"id", "ssn"})

    def test_multiple_missing_fields_all_reported(self):
        with pytest.raises(ValueError, match="typo1"):
            validate_hash_fields("patients", ["typo1", "typo2"], {"id"})


# ---------------------------------------------------------------------------
# Pipeline integration
# ---------------------------------------------------------------------------

# Stubs reused from test_pipeline.py pattern


class _StubStateStore:
    def __init__(self):
        self._data: dict = {}

    def get(self, pipeline, stream):
        return dict(self._data.get((pipeline, stream), {}))

    def set(self, pipeline, stream, state):
        self._data[(pipeline, stream)] = dict(state)


class _StubSource:
    def __init__(self, schema: Schema, rows: dict[str, list[dict]]):
        self._schema = schema
        self._rows = rows

    def check(self):
        pass

    def discover(self):
        return self._schema

    def read(self, stream: str, state: dict):
        yield from self._rows.get(stream, [])


class _StubDestination:
    def __init__(self):
        self.written: dict[str, list] = {}

    def check(self):
        pass

    def prepare_stream(self, stream, schema, config, run_id=""):
        pass

    def write(self, stream, rows):
        self.written.setdefault(stream, []).extend(rows)

    def commit(self, stream):
        pass

    def rollback(self, stream):
        pass


def _schema_with_columns(stream_name: str, *col_names: str) -> Schema:
    return Schema(
        streams=[
            Stream(
                name=stream_name,
                columns=[Column(name=c, type=ColumnType.string) for c in col_names],
            )
        ]
    )


def _empty_schema(stream_name: str) -> Schema:
    return Schema(streams=[Stream(name=stream_name, columns=[])])


class TestPipelineFieldHashing:
    def test_hash_fields_applied_before_write(self):
        schema = _schema_with_columns("patients", "id", "ssn", "name")
        source = _StubSource(schema, {"patients": [{"id": "1", "ssn": "123-45-6789", "name": "Alice"}]})
        dest = _StubDestination()

        Pipeline(
            "p", source, dest, _StubStateStore(),
            streams=[StreamConfig(name="patients", hash_fields=["ssn"])],
            hash_key=KEY,
            loaded_at=False,
        ).run()

        row = dest.written["patients"][0]
        assert row["ssn_hash"] == _hmac_hex("123-45-6789")
        assert "ssn" not in row
        assert row["name"] == "Alice"

    def test_unhashed_fields_pass_through(self):
        schema = _schema_with_columns("patients", "id", "ssn")
        source = _StubSource(schema, {"patients": [{"id": "1", "ssn": "123"}]})
        dest = _StubDestination()

        Pipeline(
            "p", source, dest, _StubStateStore(),
            streams=[StreamConfig(name="patients", hash_fields=["ssn"])],
            hash_key=KEY,
            loaded_at=False,
        ).run()

        assert dest.written["patients"][0]["id"] == "1"

    def test_hash_fields_without_hash_key_raises_at_init(self):
        schema = _schema_with_columns("patients", "ssn")
        source = _StubSource(schema, {})
        dest = _StubDestination()

        with pytest.raises(ValueError, match="hash_key is not set"):
            Pipeline(
                "p", source, dest, _StubStateStore(),
                streams=[StreamConfig(name="patients", hash_fields=["ssn"])],
                hash_key=None,
            )

    def test_unknown_hash_field_raises_at_startup_mirror_mode(self):
        schema = _schema_with_columns("patients", "id", "ssn")
        source = _StubSource(schema, {"patients": [{"id": "1", "ssn": "123"}]})
        dest = _StubDestination()

        p = Pipeline(
            "p", source, dest, _StubStateStore(),
            streams=[StreamConfig(name="patients", hash_fields=["ssnn"])],  # typo
            hash_key=KEY,
            loaded_at=False,
        )
        with pytest.raises(ValueError, match="unknown field"):
            p.run()

    def test_unknown_hash_field_raises_on_first_chunk_sql_file_mode(self):
        """SQL-file mode: empty schema from discover(), validation deferred to first chunk."""
        schema = _empty_schema("patients")
        source = _StubSource(schema, {"patients": [{"id": "1", "ssn": "123"}]})
        dest = _StubDestination()

        p = Pipeline(
            "p", source, dest, _StubStateStore(),
            streams=[StreamConfig(name="patients", hash_fields=["ssnn"])],  # typo
            hash_key=KEY,
            loaded_at=False,
        )
        with pytest.raises(ValueError, match="unknown field"):
            p.run()

    def test_no_hash_fields_pipeline_unaffected(self):
        schema = _schema_with_columns("tickets", "id", "email")
        source = _StubSource(schema, {"tickets": [{"id": "1", "email": "a@b.com"}]})
        dest = _StubDestination()

        Pipeline("p", source, dest, _StubStateStore(), loaded_at=False).run()

        assert dest.written["tickets"][0]["email"] == "a@b.com"

    def test_multiple_streams_different_hash_fields(self):
        schema = Schema(
            streams=[
                Stream(name="patients", columns=[
                    Column(name="id", type=ColumnType.string),
                    Column(name="ssn", type=ColumnType.string),
                ]),
                Stream(name="claims", columns=[
                    Column(name="id", type=ColumnType.string),
                    Column(name="member_id", type=ColumnType.string),
                ]),
            ]
        )
        source = _StubSource(schema, {
            "patients": [{"id": "1", "ssn": "123"}],
            "claims":   [{"id": "2", "member_id": "M99"}],
        })
        dest = _StubDestination()

        Pipeline(
            "p", source, dest, _StubStateStore(),
            streams=[
                StreamConfig(name="patients", hash_fields=["ssn"]),
                StreamConfig(name="claims",   hash_fields=["member_id"]),
            ],
            hash_key=KEY,
            loaded_at=False,
        ).run()

        assert dest.written["patients"][0]["ssn_hash"] == _hmac_hex("123")
        assert dest.written["claims"][0]["member_id_hash"] == _hmac_hex("M99")
