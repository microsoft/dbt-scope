"""sqlglot-powered SQL parser for the SCOPE dbt adapter.

Implements :class:`~dbt.adapters.scope.sql_parser.SqlParser` using
`sqlglot <https://sqlglot.com>`_ with SCOPE-specific preprocessing.

SCOPE SQL contains non-standard syntax (``.ToString()`` method calls,
``@variable`` references) that sqlglot cannot parse natively.  Before
parsing, :meth:`_normalize_scope_sql` replaces these constructs with
SQL-safe alternatives.  The original SQL is never modified — sqlglot is
used only for AST inspection.  A regex-based fallback handles edge
cases where sqlglot still cannot produce a usable AST.
"""

from __future__ import annotations

import re

import sqlglot
from dbt.adapters.events.logging import AdapterLogger
from sqlglot import exp

from dbt.adapters.scope.sql_parser import SqlParser

log = AdapterLogger("scope")

# -- SCOPE preprocessing ----------------------------------------------------


def _normalize_scope_sql(sql: str) -> str:
    """Replace SCOPE-specific syntax so sqlglot can parse the SQL.

    Transformations (applied only for parsing — original SQL is never modified):
      - ``.MethodCall(...)`` -> stripped (e.g. ``.ToString("yyyyMMdd")`` -> empty)
      - ``@identifier``     -> ``_scope_identifier``
      - ``==``              -> ``=``  (SCOPE uses C#-style equality)
    """
    # Strip .MethodCall(...) — handles simple non-nested parentheses
    normalized = re.sub(r"\.\w+\([^)]*\)", "", sql)
    # Replace @identifier with _scope_identifier
    normalized = re.sub(r"@(\w+)", r"_scope_\1", normalized)
    # SCOPE uses == for equality (C#-style); normalise to SQL =
    normalized = normalized.replace("==", "=")
    return normalized


# -- Regex fallback ----------------------------------------------------------


def _regex_has_where(sql: str) -> bool:
    """Detect a top-level ``WHERE`` keyword using parenthesis-depth tracking.

    Skips ``WHERE`` inside string literals (single/double quotes) and
    parenthesized sub-expressions (subqueries).
    """
    # Strip string literal contents to avoid false positives
    cleaned = re.sub(r'"[^"]*"', '""', sql)
    cleaned = re.sub(r"'[^']*'", "''", cleaned)

    depth = 0
    tokens = re.split(r"(\(|\)|\bWHERE\b)", cleaned, flags=re.IGNORECASE)
    for token in tokens:
        if token == "(":
            depth += 1
        elif token == ")":
            depth = max(depth - 1, 0)
        elif token.upper() == "WHERE" and depth == 0:
            return True
    return False


# -- SqlglotParser -----------------------------------------------------------


class SqlglotParser(SqlParser):
    """Concrete :class:`SqlParser` backed by sqlglot.

    All methods use the AST only for **inspection** — original SQL text
    is always preserved verbatim so that SCOPE-specific syntax
    (e.g. ``_date.ToString("yyyyMMdd")``, ``@data``) is never mangled
    by sqlglot's code generator.

    Instances are lightweight and stateless; sharing a single instance
    across threads is safe.
    """

    # -- SqlParser interface ------------------------------------------------

    def has_top_level_where(self, sql: str) -> bool:
        """Detect a top-level ``WHERE`` in SCOPE model SQL.

        Preprocesses the SQL to remove SCOPE-specific syntax, then
        parses with sqlglot.  If sqlglot confirms a ``WHERE`` node,
        returns ``True`` immediately.  If sqlglot says *no* WHERE, a
        regex fallback double-checks — SCOPE syntax may have confused
        the AST even after preprocessing.

        :param sql: Raw model SQL (may contain ``@data``, ``.ToString()``, etc.).
        :return: ``True`` if the outermost ``SELECT`` has a ``WHERE`` clause.
        """
        normalized = _normalize_scope_sql(sql)
        try:
            parsed = sqlglot.parse_one(normalized, error_level=sqlglot.ErrorLevel.IGNORE)
            if (
                parsed is not None
                and isinstance(parsed, exp.Select)
                and parsed.args.get("where") is not None
            ):
                return True
            # sqlglot may have missed WHERE due to residual SCOPE syntax —
            # fall through to regex rather than returning False.
        except Exception:
            log.debug(f"sqlglot parse failed, falling back to regex for: {sql:.80s}...")

        # Fallback: regex-based detection on the original (un-normalized) SQL
        result = _regex_has_where(sql)
        log.debug(f"has_top_level_where regex fallback → {result} for: {sql:.80s}...")
        return result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

parser: SqlParser = SqlglotParser()
"""Constructed parser instance.

Import as::

    from dbt.adapters.scope.sqlglot_parser import parser
"""
