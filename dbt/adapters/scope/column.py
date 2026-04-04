"""ScopeColumn — column metadata with SCOPE type mapping."""

from __future__ import annotations

from typing import ClassVar

from dbt.adapters.base.column import Column

# Mapping from SCOPE types to dbt canonical categories
_SCOPE_TO_DBT: dict[str, str] = {
    "string": "text",
    "bool": "boolean",
    "sbyte": "integer",
    "short": "integer",
    "int": "integer",
    "long": "integer",
    "float": "float",
    "double": "float",
    "decimal": "numeric",
    "DateTime": "datetime",
    "byte[]": "text",
}


class ScopeColumn(Column):
    """Represents a column in a SCOPE Delta table."""

    TYPE_LABELS: ClassVar[dict[str, str]] = {
        v.upper(): k
        for k, v in {
            "integer": "INTEGER",
            "float": "FLOAT",
            "numeric": "NUMERIC",
            "text": "TEXT",
            "boolean": "BOOLEAN",
            "datetime": "TIMESTAMP",
        }.items()
    }

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        """Map a SCOPE SQL type to a dbt category."""
        base = dtype.rstrip("?").strip()
        return _SCOPE_TO_DBT.get(base, "text")

    @classmethod
    def from_scope_type(cls, name: str, scope_type: str) -> ScopeColumn:
        """Create a ``ScopeColumn`` from a SCOPE type declaration."""
        return cls(column=name, dtype=scope_type)

    @property
    def is_scope_nullable(self) -> bool:
        return self.dtype.endswith("?")

    @property
    def scope_type(self) -> str:
        """Return the SCOPE type string (e.g. ``string``, ``long``, ``DateTime``)."""
        return self.dtype
