"""ScopeRelation — relation model for SCOPE Delta tables."""

from __future__ import annotations

from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import (
    Policy,
)


@dataclass(frozen=True, eq=False, repr=False)
class ScopeRelation(BaseRelation):
    """Represents a Delta table managed by SCOPE on ADLS.

    Mapping:
      - database → storage account
      - schema   → container / path prefix
      - identifier → table name
    """

    quote_character: str = ""
    include_policy: Policy = field(
        default_factory=lambda: Policy(database=False, schema=False, identifier=True)
    )
    quote_policy: Policy = field(
        default_factory=lambda: Policy(database=False, schema=False, identifier=False)
    )

    # SCOPE does not support CREATE OR REPLACE or RENAME
    renameable_relations: frozenset = frozenset()
    replaceable_relations: frozenset = frozenset()

    def render(self) -> str:
        """Render the relation as a bare identifier (table name only)."""
        if self.identifier:
            return str(self.identifier)
        return ""

    def delta_location(
        self,
        storage_account: str,
        container: str,
        delta_base_path: str = "delta",
    ) -> str:
        """Build the ``abfss://`` path for this relation's Delta table."""
        return (
            f"abfss://{container}@{storage_account}"
            f".dfs.core.windows.net/{delta_base_path}/{self.identifier}"
        )
