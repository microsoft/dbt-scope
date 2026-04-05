{# ============================================================
   schema.sql — Schema DDL macros for SCOPE
   SCOPE has no schema concept — these are no-ops.
   ============================================================ #}

{% macro scope__create_schema(relation) -%}
    {# No-op: SCOPE has no schemas #}
{%- endmacro %}

{% macro scope__drop_schema(relation) -%}
    {# No-op: SCOPE has no schemas #}
{%- endmacro %}

{% macro scope__list_schemas(database) -%}
    {# Returns the container name as the single "schema" #}
    {{ return([target.schema]) }}
{%- endmacro %}
