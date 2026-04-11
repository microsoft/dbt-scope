{# ============================================================
   table.sql — Full-refresh materialization for SCOPE Delta tables

   File-based processing: Discovers all source files, processes
   them in batches of max_files_per_trigger, and writes to Delta.
   The first batch DELETEs existing data; subsequent batches INSERT
   only (same batching loop as incremental.sql).
   ============================================================ #}

{% materialization table, adapter='scope' %}
    {%- set identifier = model['alias'] -%}
    {%- set job_identifier = config.get('job_tag') or identifier -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
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

    {# -- Delete checkpoint for full refresh -- #}
    {% do adapter.delete_checkpoint(delta_location) %}

    {# -- Register model name for orphan cancellation + related metadata -- #}
    {% do adapter.set_next_job_model_name(job_identifier) %}

    {# -- Batching loop: discover → submit → checkpoint → repeat -- #}
    {# NOTE: file_batch MUST live in the namespace — see incremental.sql for details. #}
    {%- set ns = namespace(
        batch_num=0,
        total_files=0,
        file_batch=adapter.discover_files(
            source_roots, source_patterns, max_files_per_trigger, delta_location, safety_buffer_seconds, starting_timestamp, max_bytes_per_trigger
        )
    ) -%}

    {%- if ns.file_batch | length == 0 -%}
        {{ log("SCOPE: No files found for full-refresh of " ~ identifier, info=True) }}
        {%- call statement('main') -%}
            -- no-op: no source files found
        {%- endcall -%}
    {%- else -%}
        {%- set total_batches = adapter.get_total_batches() -%}
        {%- for _ in range(1000) -%}
            {%- if ns.file_batch | length == 0 -%}
                {# Break out of loop #}
            {%- else -%}
                {%- set ns.batch_num = ns.batch_num + 1 -%}
                {%- set ns.total_files = ns.total_files + ns.file_batch | length -%}

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
                    is_full_refresh=(ns.batch_num == 1)
                ) -%}

                {{ log("SCOPE: full-refresh " ~ identifier ~ " batch " ~ ns.batch_num ~ " of " ~ total_batches ~ " (" ~ ns.file_batch | length ~ " files)", info=True) }}

                {%- set job_suffix = "full-refresh_batch_" ~ ns.batch_num ~ "_of_" ~ total_batches ~ "_files_" ~ ns.file_batch | length -%}
                {% do adapter.set_next_job_name(job_identifier ~ "_" ~ job_suffix) %}
                {% if config.get('au') %}{% do adapter.set_next_job_au(config.get('au') | int) %}{% endif %}
                {% if config.get('priority') %}{% do adapter.set_next_job_priority(config.get('priority') | int) %}{% endif %}
                {% if config.get('job_timeout_seconds') %}{% do adapter.set_next_job_timeout_seconds(config.get('job_timeout_seconds') | int) %}{% endif %}
                {%- call statement('main') -%}
                    {{ scope_script }}
                {%- endcall -%}

                {% do adapter.update_checkpoint(delta_location, source_roots, source_patterns, ns.file_batch, source_compaction_interval, source_retention_files) %}

                {# -- Discover next batch (watermark advanced) -- #}
                {%- set ns.file_batch = adapter.discover_files(
                    source_roots, source_patterns, max_files_per_trigger, delta_location, safety_buffer_seconds, starting_timestamp, max_bytes_per_trigger
                ) -%}
            {%- endif -%}
        {%- endfor -%}

        {{ log("SCOPE: " ~ identifier ~ " full-refresh complete — " ~ ns.batch_num ~ " batches, " ~ ns.total_files ~ " files total", info=True) }}
    {%- endif -%}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}


{# ============================================================
   Macro: build a file-based SCOPE script
   ============================================================ #}
{% macro scope__build_file_based_script(
    table_name,
    delta_location,
    partition_by,
    scope_settings,
    delta_table_columns,
    extract_columns,
    feature_previews,
    model_sql,
    source_files,
    is_full_refresh=false,
    is_incremental=false
) %}
{# -- Normalize partition_by to a list -- #}
{%- set partition_cols = partition_by if partition_by is iterable and partition_by is not string else ([partition_by] if partition_by else []) -%}

{# -- Header -- #}
// ============================================================
// Generated by dbt-scope adapter
// Model: {{ table_name }}
// Strategy: {{ 'full-refresh' if is_full_refresh else 'incremental' }} ({{ source_files | length }} files)
// ============================================================

SET @@FeaturePreviews = "{{ feature_previews }}";
{% if is_incremental %}
SET @@DeltaLakeCommitCondition = "FailIfPartitionConflict";
{% endif %}

#DECLARE @deltaPath string = "{{ delta_location }}";

{# -- CREATE TABLE IF NOT EXISTS (uses delta_table_columns) -- #}
CREATE TABLE IF NOT EXISTS @target (
{%- for col in delta_table_columns %}
    {{ col.name }} {{ col.type }}{{ "," if not loop.last }}
{%- endfor %}
)
{%- if partition_cols %}
PARTITIONED BY ({{ partition_cols | join(', ') }})
{%- endif %}
LOCATION @deltaPath
OPTIONS (LAYOUT = DELTA);

{# -- ALTER TABLE SET TBLPROPERTIES (declarative) -- #}
{%- if scope_settings %}
ALTER TABLE @target SET TBLPROPERTIES (
{%- for key, value in scope_settings.items() %}
    "{{ key }}" = {{ scope__quote_property(value) }}{{ "," if not loop.last }}
{%- endfor %}
);
{%- endif %}

{# -- DELETE existing data for full-refresh idempotency -- #}
{%- if is_full_refresh %}
DECLARE TABLE @target_rw
LOCATION @deltaPath
OPTIONS (LAYOUT = DELTA);

DELETE FROM @target_rw WHERE true;
{%- endif %}

{# -- EXTRACT from explicit file list (uses extract_columns) -- #}
{# Map of virtual column names to SCOPE FILE.* functions #}
{%- set virtual_map = {
    'source_file_uri': 'FILE.URI()',
    'source_file_length': 'FILE.LENGTH()',
    'source_file_created': 'FILE.CREATED()',
    'source_file_modified': 'FILE.MODIFIED()'
} -%}
@data =
    EXTRACT
{%- for col in extract_columns %}
{%-   if col.name in virtual_map %}
        {{ col.name }} = {{ virtual_map[col.name] }}{{ "," if not loop.last }}
{%-   else %}
        {{ col.name }} : {{ col.type }}{{ "," if not loop.last }}
{%-   endif %}
{%- endfor %}
    FROM {{ source_files | map('tojson') | join(',\n         ') }}
    USING Extractors.SStream();

{# -- User's transformation + INSERT -- #}
@batch_data =
    {{ model_sql }};

INSERT INTO @target
SELECT {{ delta_table_columns | map(attribute='name') | join(', ') }} FROM @batch_data;

{% endmacro %}


{# -- Helper: quote a TBLPROPERTIES value -- #}
{% macro scope__quote_property(value) %}
{%- if value is string -%}
"{{ value }}"
{%- else -%}
{{ value }}
{%- endif -%}
{% endmacro %}
