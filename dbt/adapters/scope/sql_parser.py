"""Abstract SQL parser for the SCOPE dbt adapter.

The adapter code programs against this ABC so that the concrete parser
(currently sqlglot) can be swapped without touching any call-sites.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class SqlParser(ABC):
    """Interface that every SQL parser backend must satisfy.

    SCOPE SQL contains non-standard syntax (``@variables``,
    ``.ToString()`` method calls) that standard SQL parsers cannot
    handle directly.  Implementations are expected to preprocess model
    SQL before parsing and to fall back gracefully when parsing fails.

    Instances are lightweight and stateless; sharing a single instance
    across threads is safe.
    """

    @abstractmethod
    def has_top_level_where(self, sql: str) -> bool:
        """Return ``True`` if *sql* contains a top-level ``WHERE`` clause.

        Must distinguish top-level ``WHERE`` from ``WHERE`` inside
        subqueries, string literals, or parenthesized expressions.
        Must handle SCOPE-specific syntax (``@data``,
        ``_date.ToString("yyyyMMdd")``) without raising.

        :param sql: Raw model SQL text (may contain SCOPE extensions).
        :return: ``True`` if a top-level ``WHERE`` is present.
        """
