{# ============================================================
   incremental.sql — File-based incremental materialization for SCOPE

   Each microbatch iteration processes one batch of maxFilesPerTrigger
   files from the ADLS Gen1 source. The adapter tracks progress via
   a watermark checkpoint (_checkpoint/watermark.json) alongside
   the Delta table's _delta_log.

   Flow per iteration:
     1. Read watermark from checkpoint
     2. LIST files on ADLS Gen1, filter by regex + watermark
     3. Take up to maxFilesPerTrigger files
     4. Build SCOPE script with explicit file list
     5. Submit SCOPE job
     6. On success, update checkpoint with new watermark

   Full refresh: delete checkpoint → process all files in batches.
   ============================================================ #}

{% materialization incremental, adapter='scope' %}
    {%- set identifier = model['alias'] -%}
    {%- set existing_relation = adapter.get_relation(
        database=database, schema=schema, identifier=identifier
    ) -%}
    {%- set target_relation = api.Relation.create(
        database=database, schema=schema, identifier=identifier, type='table'
    ) -%}

    {# -- Pull config values -- #}
    {%- set strategy = config.get('incremental_strategy', 'microbatch') -%}
    {%- set delta_location = config.get('delta_location', '') -%}
    {%- set source_root = config.get('source_root', '') -%}
    {%- set source_pattern = config.get('source_pattern', '.*\\.ss$') -%}
    {%- set max_files_per_trigger = config.get('max_files_per_trigger', 50) | int -%}
    {%- set safety_buffer_seconds = config.get('safety_buffer_seconds', 30) | int -%}
    {%- set source_compaction_interval = config.get('source_compaction_interval', 10) | int -%}
    {%- set source_retention_files = config.get('source_retention_files', 100) | int -%}
    {%- set partition_by = config.get('partition_by', none) -%}
    {%- set scope_settings = config.get('scope_settings', {}) -%}
    {%- set scope_columns = config.get('scope_columns', []) -%}
    {%- set feature_previews = config.get('scope_feature_previews', 'EnableDeltaTableDynamicInsert:on') -%}

    {# -- Determine if this is a full refresh -- #}
    {%- set full_refresh_mode = (should_full_refresh()) -%}

    {%- if full_refresh_mode -%}
        {# -- Full refresh: delete checkpoint, then discover all files -- #}
        {% do adapter.delete_checkpoint(delta_location) %}

        {%- set file_batch = adapter.discover_files(
            source_root, source_pattern, max_files_per_trigger, delta_location, safety_buffer_seconds
        ) -%}

        {%- if file_batch | length == 0 -%}
            {{ log("SCOPE: No files found for full-refresh of " ~ identifier, info=True) }}
            {%- call statement('main') -%}
                -- no-op: no source files found for full refresh
            {%- endcall -%}
        {%- else -%}
            {%- set scope_script = scope__build_file_based_script(
                identifier,
                delta_location,
                partition_by,
                scope_settings,
                scope_columns,
                feature_previews,
                sql,
                file_batch,
                is_full_refresh=true
            ) -%}

            {{ log("SCOPE: Full refresh for " ~ identifier ~ " (" ~ file_batch | length ~ " files)", info=True) }}

            {% do adapter.set_next_job_name(identifier ~ "_full-refresh") %}
            {%- call statement('main') -%}
                {{ scope_script }}
            {%- endcall -%}

            {# -- Update checkpoint after successful job -- #}
            {% do adapter.update_checkpoint(delta_location, source_root, source_pattern, file_batch, source_compaction_interval, source_retention_files) %}
        {%- endif -%}

    {%- else -%}
        {# -- Incremental run: discover unprocessed files -- #}

        {%- if strategy in ('microbatch', 'append') -%}

            {%- set file_batch = adapter.discover_files(
                source_root, source_pattern, max_files_per_trigger, delta_location, safety_buffer_seconds
            ) -%}

            {%- if file_batch | length == 0 -%}
                {{ log("SCOPE: No unprocessed files for " ~ identifier ~ " — skipping", info=True) }}
                {%- call statement('main') -%}
                    -- no-op: no unprocessed files found
                {%- endcall -%}
            {%- else -%}
                {%- set scope_script = scope__build_file_based_script(
                    identifier,
                    delta_location,
                    partition_by,
                    scope_settings,
                    scope_columns,
                    feature_previews,
                    sql,
                    file_batch,
                    is_incremental=true
                ) -%}

                {{ log("SCOPE: Incremental " ~ identifier ~ " (" ~ file_batch | length ~ " files)", info=True) }}

                {% do adapter.set_next_job_name(identifier ~ "_incremental_" ~ file_batch | length ~ "files") %}
                {%- call statement('main') -%}
                    {{ scope_script }}
                {%- endcall -%}

                {# -- Update checkpoint after successful job -- #}
                {% do adapter.update_checkpoint(delta_location, source_root, source_pattern, file_batch, source_compaction_interval, source_retention_files) %}
            {%- endif -%}

        {%- else -%}
            {{ exceptions.raise_compiler_error(
                "Invalid incremental strategy '" ~ strategy ~ "' for dbt-scope. "
                "Supported: microbatch, append"
            ) }}
        {%- endif -%}
    {%- endif -%}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}


{# ============================================================
   Strategy validation
   ============================================================ #}
{% macro scope__validate_get_incremental_strategy(raw_strategy) %}
    {%- set valid = ['microbatch', 'append'] -%}
    {%- if raw_strategy not in valid -%}
        {{ exceptions.raise_compiler_error(
            "Invalid incremental strategy '" ~ raw_strategy ~ "' for dbt-scope. "
            "Valid strategies: " ~ valid | join(', ')
        ) }}
    {%- endif -%}
    {{ return(raw_strategy) }}
{% endmacro %}
