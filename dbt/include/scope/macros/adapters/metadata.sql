{# ============================================================
   metadata.sql — Introspection macros for SCOPE
   ============================================================ #}

{% macro scope__list_relations_without_caching(schema_relation) -%}
    {# SCOPE has no catalog — return empty list #}
    {{ return([]) }}
{%- endmacro %}

{% macro scope__get_columns_in_relation(relation) -%}
    {# Column info comes from sources.yml, not introspection #}
    {{ return([]) }}
{%- endmacro %}

{% macro scope__information_schema_name(database) -%}
    {# SCOPE has no information_schema #}
    {{ return(none) }}
{%- endmacro %}

{% macro scope__current_timestamp() -%}
    DateTime.UtcNow
{%- endmacro %}
