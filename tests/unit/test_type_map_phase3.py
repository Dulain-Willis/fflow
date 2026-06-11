# tests/unit/test_type_map_phase3.py
#
# Tests for sqlalchemy_type_to_column() and column_to_generic_sql_ddl().

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from fflow.common.schema import ColumnType
from fflow.common.type_map import column_to_generic_sql_ddl, sqlalchemy_type_to_column
from fflow.common.schema import Column


# ---------------------------------------------------------------------------
# sqlalchemy_type_to_column()
# ---------------------------------------------------------------------------


def _sa_type(cls_name: str, **kwargs):
    """Build a mock SQLAlchemy type that isinstance-checks correctly."""
    from sqlalchemy.sql import sqltypes

    type_map = {
        "Integer": sqltypes.Integer,
        "BigInteger": sqltypes.BigInteger,
        "SmallInteger": sqltypes.SmallInteger,
        "String": sqltypes.String,
        "Text": sqltypes.Text,
        "Numeric": sqltypes.Numeric,
        "Float": sqltypes.Float,
        "Boolean": sqltypes.Boolean,
        "DateTime": sqltypes.DateTime,
        "Date": sqltypes.Date,
        "JSON": sqltypes.JSON,
        "NullType": sqltypes.NullType,
    }
    cls = type_map.get(cls_name)
    if cls is None:
        pytest.skip(f"SQLAlchemy type {cls_name} not found")
    return cls(**kwargs)


class TestSqlalchemyTypeToColumn:
    def test_integer_maps_to_integer(self):
        col = sqlalchemy_type_to_column("id", _sa_type("Integer"))
        assert col.type == ColumnType.integer
        assert col.name == "id"

    def test_biginteger_maps_to_integer(self):
        col = sqlalchemy_type_to_column("id", _sa_type("BigInteger"))
        assert col.type == ColumnType.integer

    def test_string_with_length(self):
        col = sqlalchemy_type_to_column("name", _sa_type("String", length=255))
        assert col.type == ColumnType.string
        assert col.max_length == 255

    def test_text_maps_to_string(self):
        col = sqlalchemy_type_to_column("body", _sa_type("Text"))
        assert col.type == ColumnType.string

    def test_numeric_decimal_asdecimal_true(self):
        col = sqlalchemy_type_to_column("price", _sa_type("Numeric", precision=18, scale=4))
        assert col.type == ColumnType.decimal
        assert col.precision == 18
        assert col.scale == 4

    def test_numeric_float_asdecimal_false(self):
        col = sqlalchemy_type_to_column("rate", _sa_type("Numeric", asdecimal=False))
        assert col.type == ColumnType.float_

    def test_float_maps_to_float(self):
        col = sqlalchemy_type_to_column("score", _sa_type("Float"))
        assert col.type == ColumnType.float_

    def test_boolean_maps_to_boolean(self):
        col = sqlalchemy_type_to_column("active", _sa_type("Boolean"))
        assert col.type == ColumnType.boolean

    def test_datetime_maps_to_timestamp(self):
        col = sqlalchemy_type_to_column("created_at", _sa_type("DateTime"))
        assert col.type == ColumnType.timestamp

    def test_date_maps_to_date(self):
        col = sqlalchemy_type_to_column("dob", _sa_type("Date"))
        assert col.type == ColumnType.date

    def test_json_maps_to_json(self):
        col = sqlalchemy_type_to_column("meta", _sa_type("JSON"))
        assert col.type == ColumnType.json

    def test_unknown_type_maps_to_unknown(self):
        from sqlalchemy.sql import sqltypes
        col = sqlalchemy_type_to_column("x", sqltypes.NullType())
        assert col.type == ColumnType.unknown

    def test_nullable_and_primary_key_propagated(self):
        col = sqlalchemy_type_to_column(
            "id", _sa_type("Integer"), nullable=False, primary_key=True
        )
        assert col.nullable is False
        assert col.primary_key is True


# ---------------------------------------------------------------------------
# column_to_generic_sql_ddl()
# ---------------------------------------------------------------------------


class TestColumnToGenericSqlDdl:
    def _col(self, t, **kwargs):
        return Column(name="x", type=t, **kwargs)

    def test_string_no_length(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.string)) == "TEXT"

    def test_string_with_length(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.string, max_length=100)) == "VARCHAR(100)"

    def test_integer(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.integer)) == "BIGINT"

    def test_decimal_defaults(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.decimal)) == "NUMERIC(18, 4)"

    def test_decimal_custom(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.decimal, precision=10, scale=2)) == "NUMERIC(10, 2)"

    def test_float(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.float_)) == "DOUBLE PRECISION"

    def test_boolean(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.boolean)) == "BOOLEAN"

    def test_timestamp(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.timestamp)) == "TIMESTAMP"

    def test_date(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.date)) == "DATE"

    def test_json(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.json)) == "TEXT"

    def test_unknown_fallback(self):
        assert column_to_generic_sql_ddl(self._col(ColumnType.unknown)) == "TEXT"
