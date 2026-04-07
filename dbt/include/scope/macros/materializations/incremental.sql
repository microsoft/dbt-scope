{# ============================================================
   incremental.sql — File-based incremental materialization for SCOPE

   Uses the ``append`` strategy with an internal batching loop:
   the macro keeps discovering and processing files until no
   unprocessed files remain, processing up to max_files_per_trigger
   per SCOPE job.

   Flow per dbt run:
     loop:
       1. Read watermark from _checkpoint/watermark.json
       2. LIST files on ADLS Gen1, filter by regex + watermark
       3. Take up to max_files_per_trigger files
       4. Build SCOPE script with explicit file list
       5. Submit SCOPE job
       6. On success, update checkpoint with new watermark
       7. If more files remain, repeat from step 1
     end loop

   Full refresh (--full-refresh): delete checkpoint → process all files.
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
    {%- set delta_location = config.get('delta_location', '') -%}
    {%- set source_roots = config.get('source_roots', []) -%}
    {%- set source_patterns = config.get('source_patterns', ['.*\\.ss$']) -%}
    {%- set max_files_per_trigger = config.get('max_files_per_trigger', 50) | int -%}
    {%- set safety_buffer_seconds = config.get('safety_buffer_seconds', 30) | int -%}
    {%- set source_compaction_interval = config.get('source_compaction_interval', 10) | int -%}
    {%- set source_retention_files = config.get('source_retention_files', 100) | int -%}
    {%- set starting_timestamp = config.get('starting_timestamp', none) -%}
    {%- set partition_by = config.get('partition_by', none) -%}
    {%- set scope_settings = config.get('scope_settings', {}) -%}
    {%- set scope_columns = config.get('scope_columns', []) -%}
    {%- set feature_previews = config.get('scope_feature_previews', 'EnableDeltaTableDynamicInsert:on') -%}

    {# -- Determine if this is a full refresh -- #}
    {%- set full_refresh_mode = (should_full_refresh()) -%}

    {%- if full_refresh_mode -%}
        {% do adapter.delete_checkpoint(delta_location) %}
    {%- endif -%}

    {# -- Batching loop: discover → submit → checkpoint → repeat -- #}
    {%- set ns = namespace(
        batch_num=0,
        total_files=0,
        file_batch=adapter.discover_files(
            source_roots, source_patterns, max_files_per_trigger, delta_location, safety_buffer_seconds, starting_timestamp
        )
    ) -%}

    {%- if ns.file_batch | length == 0 -%}
        {{ log("SCOPE: No " ~ ("" if full_refresh_mode else "unprocessed ") ~ "files for " ~ identifier ~ " — skipping", info=True) }}
        {%- call statement('main') -%}
            -- no-op: no source files found
        {%- endcall -%}
    {%- else -%}
        {%- for _ in range(1000) -%}
            {%- if ns.file_batch | length == 0 -%}
                {# Break out of loop — Jinja has no while, so we use for + break guard #}
            {%- else -%}
                {%- set ns.batch_num = ns.batch_num + 1 -%}
                {%- set ns.total_files = ns.total_files + ns.file_batch | length -%}

                {# Only DELETE on the first batch of a full refresh #}
                {%- set is_first_full_refresh_batch = (full_refresh_mode and ns.batch_num == 1) -%}

                {%- set scope_script = scope__build_file_based_script(
                    identifier,
                    delta_location,
                    partition_by,
                    scope_settings,
                    scope_columns,
                    feature_previews,
                    sql,
                    ns.file_batch,
                    is_full_refresh=is_first_full_refresh_batch,
                    is_incremental=(not is_first_full_refresh_batch)
                ) -%}

                {%- set mode_label = "full-refresh" if full_refresh_mode else "incremental" -%}
                {{ log("SCOPE: " ~ mode_label ~ " " ~ identifier ~ " batch " ~ ns.batch_num ~ " (" ~ ns.file_batch | length ~ " files)", info=True) }}

                {%- set job_suffix = mode_label ~ "_batch" ~ ns.batch_num ~ "_" ~ ns.file_batch | length ~ "files" -%}
                {% do adapter.set_next_job_name(identifier ~ "_" ~ job_suffix) %}
                {%- call statement('main') -%}
                    {{ scope_script }}
                {%- endcall -%}

                {% do adapter.update_checkpoint(delta_location, source_roots, source_patterns, ns.file_batch, source_compaction_interval, source_retention_files) %}

                {# -- Discover next batch (watermark advanced, so new files are eligible) -- #}
                {%- set ns.file_batch = adapter.discover_files(
                    source_roots, source_patterns, max_files_per_trigger, delta_location, safety_buffer_seconds, starting_timestamp
                ) -%}
            {%- endif -%}
        {%- endfor -%}

        {{ log("SCOPE: " ~ identifier ~ " complete — " ~ ns.batch_num ~ " batches, " ~ ns.total_files ~ " files total", info=True) }}
    {%- endif -%}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
