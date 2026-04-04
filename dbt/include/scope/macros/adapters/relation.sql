{# ============================================================
   relation.sql — Relation DDL macros for SCOPE
   ============================================================ #}

{% macro scope__drop_relation(relation) -%}
    {# SCOPE Delta tables are not casually dropped — no-op for safety #}
    -- dbt-scope: drop_relation is a no-op (Delta table: {{ relation }})
{%- endmacro %}

{% macro scope__rename_relation(from_relation, to_relation) -%}
    {{ exceptions.raise_compiler_error(
        "dbt-scope does not support renaming relations. Use --full-refresh."
    ) }}
{%- endmacro %}

{% macro scope__create_table_as(temporary, relation, compiled_code, language='sql') -%}
    {# This is handled by the table/incremental materializations directly #}
    {{ compiled_code }}
{%- endmacro %}

{% macro scope__truncate_relation(relation) -%}
    -- dbt-scope: truncate_relation is a no-op (Delta table: {{ relation }})
{%- endmacro %}

{% macro scope__get_or_create_relation(database, schema, identifier, type) -%}
    {%- set target_relation = adapter.get_relation(
        database=database, schema=schema, identifier=identifier
    ) -%}
    {%- if target_relation is none -%}
        {%- set target_relation = api.Relation.create(
            database=database, schema=schema, identifier=identifier, type=type
        ) -%}
    {%- endif -%}
    {{ return(target_relation) }}
{%- endmacro %}
