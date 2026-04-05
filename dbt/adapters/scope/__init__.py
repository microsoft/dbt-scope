from dbt.adapters.base import AdapterPlugin

from dbt.adapters.scope.column import ScopeColumn  # noqa: F401
from dbt.adapters.scope.connections import ScopeConnectionManager  # noqa: F401
from dbt.adapters.scope.credentials import ScopeCredentials
from dbt.adapters.scope.impl import ScopeAdapter
from dbt.adapters.scope.relation import ScopeRelation  # noqa: F401

Plugin = AdapterPlugin(
    adapter=ScopeAdapter,
    credentials=ScopeCredentials,
    include_path="dbt/include/scope",
)
