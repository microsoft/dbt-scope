{# ============================================================
   utils.sql — Utility macros for dbt-scope
   ============================================================ #}

{# -- SCOPE type mapping from dbt/SQL types -- #}
{% macro scope__type_string() -%}
    string
{%- endmacro %}

{% macro scope__type_int() -%}
    int
{%- endmacro %}

{% macro scope__type_bigint() -%}
    long
{%- endmacro %}

{% macro scope__type_float() -%}
    double
{%- endmacro %}

{% macro scope__type_boolean() -%}
    bool
{%- endmacro %}

{% macro scope__type_timestamp() -%}
    DateTime
{%- endmacro %}

{# -- Generate a SCOPE-compatible column list from delta_table_columns config -- #}
{% macro scope__render_column_list(delta_table_columns) -%}
{%- for col in delta_table_columns -%}
    {{ col.name }}{{ ", " if not loop.last }}
{%- endfor -%}
{%- endmacro %}

{# -- Render column definitions for CREATE TABLE -- #}
{% macro scope__render_column_defs(delta_table_columns) -%}
{%- for col in delta_table_columns -%}
    {{ col.name }} {{ col.type }}{{ "," if not loop.last }}
{%- endfor -%}
{%- endmacro %}
