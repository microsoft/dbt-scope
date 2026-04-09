{# ============================================================
   defaults.sql — single source of truth for config defaults
   in the Jinja layer.

   Mirrors dbt/adapters/scope/constants.py on the Python side.
   ============================================================ #}

{% macro scope__config_defaults() %}
    {% do return({
        "max_files_per_trigger": 50,
        "max_bytes_per_trigger": 107374182400,
        "safety_buffer_seconds": 30,
        "source_compaction_interval": 10,
        "source_retention_files": 100,
    }) %}
{% endmacro %}
