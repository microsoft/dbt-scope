{# ============================================================
   incremental.sql — File-based incremental materialization for SCOPE

   Uses the ``append`` strategy with an internal batching loop:
   the macro keeps discovering and processing files until no
   unprocessed files remain, processing up to max_files_per_trigger
   per SCOPE job.

   Supports two trigger modes (modeled after Spark Structured Streaming):
     - available_now (default): process all available files, then exit
     - processing_time: continuously loop — discover → batch → sleep → repeat

   Flow per dbt run:
     loop:
       1. Read watermark from _checkpoint/watermark.json
       2. LIST files on ADLS Gen1, filter by regex + watermark
       3. Take up to max_files_per_trigger files
       4. Build SCOPE script with explicit file list
       5. Submit SCOPE job
       6. On success, update checkpoint with new watermark
       7. If more files remain, repeat from step 1
       8. (processing_time only) If no files remain, sleep for interval, then repeat from step 1
     end loop

   Full refresh (--full-refresh): delete checkpoint → process all files.
   ============================================================ #}

{% materialization incremental, adapter='scope' %}
    {%- set identifier = model['alias'] -%}
    {%- set job_identifier = config.get('job_tag') or identifier -%}
    {%- set existing_relation = adapter.get_relation(
        database=database, schema=schema, identifier=identifier
    ) -%}
    {%- set target_relation = api.Relation.create(
        database=database, schema=schema, identifier=identifier, type='table'
    ) -%}

    {# -- Pull config values -- #}
    {%- set defaults = scope__config_defaults() -%}
    {%- set delta_location = config.get('delta_location', '') -%}
    {%- set source_roots = config.get('source_roots', []) -%}
    {%- set source_patterns = config.get('source_patterns', ['.*\\.ss$']) -%}
    {%- set max_files_per_trigger = config.get('max_files_per_trigger', target.max_files_per_trigger) | int -%}
    {%- set max_bytes_per_trigger = config.get('max_bytes_per_trigger', target.max_bytes_per_trigger) | int -%}
    {%- set safety_buffer_seconds = config.get('safety_buffer_seconds', defaults.safety_buffer_seconds) | int -%}
    {%- set source_compaction_interval = config.get('source_compaction_interval', defaults.source_compaction_interval) | int -%}
    {%- set source_retention_files = config.get('source_retention_files', defaults.source_retention_files) | int -%}
    {%- set starting_timestamp = config.get('starting_timestamp', none) -%}
    {%- set partition_by = config.get('partition_by', none) -%}
    {%- set scope_settings = config.get('scope_settings', {}) -%}
    {%- set delta_table_columns = config.get('delta_table_columns', []) -%}
    {%- set extract_columns = config.get('extract_columns', []) -%}
    {%- set feature_previews = config.get('scope_feature_previews', 'EnableDeltaTableDynamicInsert:on') -%}
    {%- set delta_lake_commit_condition = config.get('delta_lake_commit_condition', target.delta_lake_commit_condition | default(defaults.delta_lake_commit_condition, true)) -%}

    {# -- Parse trigger config -- #}
    {%- set trigger = adapter.parse_trigger(config.get('trigger', none)) -%}
    {%- set is_processing_time = (trigger.type == 'processing_time') -%}

    {# -- Auto-set large timeout for processing_time unless user overrides -- #}
    {%- if is_processing_time and not config.get('job_timeout_seconds') -%}
        {% do adapter.set_next_job_timeout_seconds(adapter.get_processing_time_timeout()) %}
    {%- endif -%}

    {# -- Determine if this is a full refresh -- #}
    {%- set full_refresh_mode = (should_full_refresh()) -%}

    {%- if full_refresh_mode -%}
        {% do adapter.delete_checkpoint(delta_location) %}
    {%- endif -%}

    {# -- Register model name for orphan cancellation + related metadata -- #}
    {% do adapter.set_next_job_model_name(job_identifier) %}

    {# -- Reset cycle counter for processing_time models -- #}
    {%- if is_processing_time -%}
        {% do adapter.reset_cycle_count() %}
    {%- endif -%}

    {# -- Loop cap: 99999 for processing_time (under Jinja sandbox MAX_RANGE=100000), 1000 for available_now -- #}
    {%- set loop_cap = 99999 if is_processing_time else 1000 -%}

    {# -- Batching loop: discover → submit → checkpoint → repeat -- #}
    {%- set ns = namespace(
        batch_num=0,
        total_files=0,
        cycle_files=0,
        keep_running=true,
        file_batch=adapter.discover_files(
            source_roots, source_patterns, max_files_per_trigger, delta_location, safety_buffer_seconds, starting_timestamp, max_bytes_per_trigger
        )
    ) -%}

    {%- if ns.file_batch | length == 0 and not is_processing_time -%}
        {{ log("SCOPE: No " ~ ("" if full_refresh_mode else "unprocessed ") ~ "files for " ~ identifier ~ " — skipping", info=True) }}
        {%- call statement('main') -%}
            -- no-op: no source files found
        {%- endcall -%}
    {%- else -%}
        {%- set total_batches = adapter.get_total_batches() -%}
        {%- for _ in range(loop_cap) -%}
            {%- if not ns.keep_running -%}
                {# Shutdown or max_cycles reached — exit loop #}
            {%- elif ns.file_batch | length == 0 -%}
                {%- if is_processing_time -%}
                    {# -- No files: sleep and re-discover (processing_time mode) -- #}
                    {{ log("SCOPE: " ~ identifier ~ " — no unprocessed files, waiting for next cycle", info=True) }}
                    {% do adapter.clear_file_discovery_cache() %}
                    {%- set continue_loop = adapter.wait_for_next_cycle(
                        trigger.interval_seconds, trigger.max_cycles
                    ) -%}
                    {%- if not continue_loop -%}
                        {%- set ns.keep_running = false -%}
                    {%- else -%}
                        {%- set ns.cycle_files = 0 -%}
                        {%- set ns.file_batch = adapter.discover_files(
                            source_roots, source_patterns, max_files_per_trigger, delta_location, safety_buffer_seconds, starting_timestamp, max_bytes_per_trigger
                        ) -%}
                        {%- set total_batches = adapter.get_total_batches() -%}
                    {%- endif -%}
                {%- else -%}
                    {# available_now: no more files — exit #}
                {%- endif -%}
            {%- else -%}
                {%- set ns.batch_num = ns.batch_num + 1 -%}
                {%- set ns.total_files = ns.total_files + ns.file_batch | length -%}
                {%- set ns.cycle_files = ns.cycle_files + ns.file_batch | length -%}

                {# Only DELETE on the first batch of a full refresh #}
                {%- set is_first_full_refresh_batch = (full_refresh_mode and ns.batch_num == 1) -%}

                {%- set scope_script = scope__build_file_based_script(
                    identifier,
                    delta_location,
                    partition_by,
                    scope_settings,
                    delta_table_columns,
                    extract_columns,
                    feature_previews,
                    sql,
                    ns.file_batch,
                    is_full_refresh=is_first_full_refresh_batch,
                    is_incremental=(not is_first_full_refresh_batch),
                    delta_lake_commit_condition=delta_lake_commit_condition
                ) -%}

                {%- set mode_label = "full-refresh" if full_refresh_mode else "incremental" -%}
                {{ log("SCOPE: " ~ mode_label ~ " " ~ identifier ~ " batch " ~ ns.batch_num ~ " of " ~ total_batches ~ " (" ~ ns.file_batch | length ~ " files)", info=True) }}

                {%- set job_suffix = mode_label ~ "_batch_" ~ ns.batch_num ~ "_of_" ~ total_batches ~ "_files_" ~ ns.file_batch | length -%}
                {% do adapter.set_next_job_name(job_identifier ~ "_" ~ job_suffix) %}
                {% if config.get('au') %}{% do adapter.set_next_job_au(config.get('au') | int) %}{% endif %}
                {% if config.get('priority') %}{% do adapter.set_next_job_priority(config.get('priority') | int) %}{% endif %}
                {% if config.get('job_timeout_seconds') %}{% do adapter.set_next_job_timeout_seconds(config.get('job_timeout_seconds') | int) %}{% endif %}
                {%- call statement('main') -%}
                    {{ scope_script }}
                {%- endcall -%}

                {% do adapter.update_checkpoint(delta_location, source_roots, source_patterns, ns.file_batch, source_compaction_interval, source_retention_files) %}

                {# -- Discover next batch (watermark advanced, so new files are eligible) -- #}
                {%- set ns.file_batch = adapter.discover_files(
                    source_roots, source_patterns, max_files_per_trigger, delta_location, safety_buffer_seconds, starting_timestamp, max_bytes_per_trigger
                ) -%}

                {# -- If this batch exhausted files AND processing_time: sleep + re-discover -- #}
                {%- if ns.file_batch | length == 0 and is_processing_time -%}
                    {{ log("SCOPE: " ~ identifier ~ " cycle complete — " ~ ns.cycle_files ~ " files processed, waiting for next cycle", info=True) }}
                    {% do adapter.clear_file_discovery_cache() %}
                    {%- set continue_loop = adapter.wait_for_next_cycle(
                        trigger.interval_seconds, trigger.max_cycles
                    ) -%}
                    {%- if not continue_loop -%}
                        {%- set ns.keep_running = false -%}
                    {%- else -%}
                        {%- set ns.cycle_files = 0 -%}
                        {%- set ns.file_batch = adapter.discover_files(
                            source_roots, source_patterns, max_files_per_trigger, delta_location, safety_buffer_seconds, starting_timestamp, max_bytes_per_trigger
                        ) -%}
                        {%- set total_batches = adapter.get_total_batches() -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}

        {{ log("SCOPE: " ~ identifier ~ " complete — " ~ ns.batch_num ~ " batches, " ~ ns.total_files ~ " files total", info=True) }}
    {%- endif -%}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
