"""Type mapping utilities.

Two functions:

``cache_type_to_column`` — converts a row from Cache ``INFORMATION_SCHEMA.COLUMNS``
into a :class:`~fflow.common.schema.Column`.

``column_to_mssql_ddl`` — converts a :class:`~fflow.common.schema.Column`
into the MSSQL DDL type fragment (e.g. ``"NVARCHAR(255)"``).  Used by
``MSSQLDestination`` when auto-creating or altering landing tables.

``sqlalchemy_type_to_column`` — converts a SQLAlchemy column type object into a
:class:`~fflow.common.schema.Column`.  Used by ``SQLSource.discover()`` when
reflecting table schemas via SQLAlchemy's ``MetaData.reflect()``.

Cache reports SQL-92 type names from INFORMATION_SCHEMA.  The mapping below
is derived from the Cache JDBC type set and validated against production
tables in RECPROD / ACPROD / DBPROD.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fflow.common.schema import Column, ColumnType

if TYPE_CHECKING:
    pass

# VARCHAR columns wider than this are mapped to NVARCHAR(MAX).
_NVARCHAR_MAX_THRESHOLD = 4000


def cache_type_to_column(
    name: str,
    data_type: str,
    *,
    precision: int | None = None,
    scale: int | None = None,
    max_length: int | None = None,
    nullable: bool = True,
    primary_key: bool = False,
) -> Column:
    """Map a Cache INFORMATION_SCHEMA column row to a :class:`Column`.

    Parameters match the columns returned by::

        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?
    """
    dt = data_type.upper().strip()

    if dt in ("VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "LONGVARCHAR", "CHARACTER VARYING"):
        return Column(
            name=name,
            type=ColumnType.string,
            nullable=nullable,
            primary_key=primary_key,
            max_length=max_length,
        )

    if dt in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"):
        return Column(name=name, type=ColumnType.integer, nullable=nullable, primary_key=primary_key)

    if dt in ("NUMERIC", "DECIMAL"):
        return Column(
            name=name,
            type=ColumnType.decimal,
            nullable=nullable,
            primary_key=primary_key,
            precision=precision,
            scale=scale,
        )

    if dt in ("FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL"):
        return Column(name=name, type=ColumnType.float_, nullable=nullable, primary_key=primary_key)

    if dt in ("BIT", "BOOLEAN"):
        return Column(name=name, type=ColumnType.boolean, nullable=nullable, primary_key=primary_key)

    if dt in ("TIMESTAMP", "DATETIME", "POSIXTIME"):
        return Column(name=name, type=ColumnType.timestamp, nullable=nullable, primary_key=primary_key)

    if dt == "DATE":
        return Column(name=name, type=ColumnType.date, nullable=nullable, primary_key=primary_key)

    if dt == "TIME":
        # No ColumnType.time — map to string; destination writes TIME(0).
        return Column(name=name, type=ColumnType.string, nullable=nullable, primary_key=primary_key, max_length=8)

    # Unknown type — destination will emit NVARCHAR(MAX) as a safe fallback.
    return Column(name=name, type=ColumnType.unknown, nullable=nullable, primary_key=primary_key)


def column_to_generic_sql_ddl(col: Column) -> str:
    """Return a standard SQL DDL type fragment for *col*.

    Uses ANSI-compatible types that work with PostgreSQL, Redshift, SQLite,
    and most ANSI-SQL databases.  Destinations with dialect-specific needs
    (e.g. MSSQL, BigQuery) should override with their own mapping.
    """
    if col.type == ColumnType.string:
        if col.max_length is None:
            return "TEXT"
        return f"VARCHAR({col.max_length})"

    if col.type == ColumnType.integer:
        return "BIGINT"

    if col.type == ColumnType.decimal:
        p = col.precision if col.precision is not None else 18
        s = col.scale if col.scale is not None else 4
        return f"NUMERIC({p}, {s})"

    if col.type == ColumnType.float_:
        return "DOUBLE PRECISION"

    if col.type == ColumnType.boolean:
        return "BOOLEAN"

    if col.type == ColumnType.timestamp:
        return "TIMESTAMP"

    if col.type == ColumnType.date:
        return "DATE"

    if col.type == ColumnType.json:
        return "TEXT"

    return "TEXT"


def column_to_mssql_ddl(col: Column) -> str:
    """Return the MSSQL DDL type fragment for *col*.

    Examples: ``"NVARCHAR(255)"``, ``"NUMERIC(18, 4)"``, ``"BIGINT"``.
    """
    if col.type == ColumnType.string:
        if col.max_length is None or col.max_length > _NVARCHAR_MAX_THRESHOLD:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({col.max_length})"

    if col.type == ColumnType.integer:
        return "BIGINT"

    if col.type == ColumnType.decimal:
        p = col.precision if col.precision is not None else 18
        s = col.scale if col.scale is not None else 4
        return f"NUMERIC({p}, {s})"

    if col.type == ColumnType.float_:
        return "FLOAT"

    if col.type == ColumnType.boolean:
        return "BIT"

    if col.type == ColumnType.timestamp:
        return "DATETIME2(7)"

    if col.type == ColumnType.date:
        return "DATE"

    if col.type == ColumnType.json:
        return "NVARCHAR(MAX)"

    # unknown — safe fallback
    return "NVARCHAR(MAX)"


def sqlalchemy_type_to_column(
    name: str,
    sa_type: Any,
    *,
    nullable: bool = True,
    primary_key: bool = False,
) -> Column:
    """Map a SQLAlchemy column type to a :class:`Column`.

    Used by ``SQLSource.discover()`` after ``MetaData.reflect()``.  The
    mapping follows the same type hierarchy as dlt's ``schema_types.py``.

    ``sa_type`` is the resolved SQLAlchemy type object from
    ``column.type`` (e.g. ``sqltypes.Integer()``, ``sqltypes.String(255)``).
    """
    try:
        from sqlalchemy.sql import sqltypes
    except ImportError as exc:
        raise ImportError("sqlalchemy required for sqlalchemy_type_to_column") from exc

    # Numeric must come before Integer — some Oracle numeric types subclass both.
    if isinstance(sa_type, sqltypes.Numeric) and not isinstance(sa_type, sqltypes.Integer):
        if sa_type.asdecimal is False:
            return Column(name=name, type=ColumnType.float_, nullable=nullable, primary_key=primary_key)
        return Column(
            name=name,
            type=ColumnType.decimal,
            nullable=nullable,
            primary_key=primary_key,
            precision=sa_type.precision,
            scale=sa_type.scale,
        )

    if isinstance(sa_type, sqltypes.Integer):
        return Column(name=name, type=ColumnType.integer, nullable=nullable, primary_key=primary_key)

    if isinstance(sa_type, sqltypes.String):
        return Column(
            name=name,
            type=ColumnType.string,
            nullable=nullable,
            primary_key=primary_key,
            max_length=sa_type.length,
        )

    if isinstance(sa_type, sqltypes.DateTime):
        return Column(name=name, type=ColumnType.timestamp, nullable=nullable, primary_key=primary_key)

    if isinstance(sa_type, sqltypes.Date):
        return Column(name=name, type=ColumnType.date, nullable=nullable, primary_key=primary_key)

    if isinstance(sa_type, sqltypes.Boolean):
        return Column(name=name, type=ColumnType.boolean, nullable=nullable, primary_key=primary_key)

    if isinstance(sa_type, sqltypes.JSON):
        return Column(name=name, type=ColumnType.json, nullable=nullable, primary_key=primary_key)

    if isinstance(sa_type, sqltypes.Float):
        return Column(name=name, type=ColumnType.float_, nullable=nullable, primary_key=primary_key)

    return Column(name=name, type=ColumnType.unknown, nullable=nullable, primary_key=primary_key)
