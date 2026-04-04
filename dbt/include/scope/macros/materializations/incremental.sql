{# ============================================================
   incremental.sql — Microbatch incremental materialization for SCOPE

   Generates a SCOPE script per microbatch window that:
     1. Creates the Delta table (idempotent)
     2. Sets table properties
     3. DELETEs the batch partition range (idempotent replace)
     4. EXTRACTs SS files in the batch date range
     5. INSERTs the transformed batch data

   Supports strategies: microbatch, append, delete+insert
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
    {%- set ss_source_path = config.get('ss_source_path', '') -%}
    {%- set partition_by = config.get('partition_by', none) -%}
    {%- set scope_settings = config.get('scope_settings', {}) -%}
    {%- set scope_columns = config.get('scope_columns', []) -%}
    {%- set feature_previews = config.get('scope_feature_previews', 'EnableDeltaTableDynamicInsert:on') -%}
    {%- set event_time = config.get('event_time', partition_by) -%}
    {%- set delete_before_insert = config.get('delete_before_insert', false) -%}

    {# -- Determine if this is a first run or full refresh -- #}
    {%- set is_first_run = existing_relation is none -%}
    {%- set full_refresh_mode = (should_full_refresh()) -%}

    {%- if is_first_run or full_refresh_mode -%}
        {# -- First run or full refresh: just CREATE + INSERT (no DELETE) -- #}
        {%- set scope_script = scope__build_full_refresh_script(
            identifier,
            delta_location,
            ss_source_path,
            partition_by,
            scope_settings,
            scope_columns,
            feature_previews,
            sql
        ) -%}

        {{ log("SCOPE: Full refresh for " ~ identifier, info=True) }}

        {%- call statement('main') -%}
            {{ scope_script }}
        {%- endcall -%}

    {%- else -%}
        {# -- Incremental run -- #}
        {%- if strategy == 'microbatch' -%}
            {# -- Microbatch: DELETE + INSERT per batch window -- #}
            {%- set batch = model.get('batch', {}) -%}
            {%- set batch_start = batch.get('event_time_start', '') -%}
            {%- set batch_end = batch.get('event_time_end', '') -%}

            {%- if batch_start and batch_end -%}
                {%- set scope_script = scope__build_incremental_script(
                    identifier,
                    delta_location,
                    ss_source_path,
                    partition_by,
                    scope_settings,
                    scope_columns,
                    feature_previews,
                    sql,
                    batch_start,
                    batch_end,
                    delete_before_insert
                ) -%}

                {{ log("SCOPE: Microbatch " ~ batch_start ~ " → " ~ batch_end ~ " for " ~ identifier, info=True) }}

                {%- call statement('main') -%}
                    {{ scope_script }}
                {%- endcall -%}
            {%- else -%}
                {{ exceptions.raise_compiler_error(
                    "dbt-scope microbatch requires batch start/end times. "
                    "Ensure event_time, batch_size, and begin are configured."
                ) }}
            {%- endif -%}

        {%- elif strategy == 'append' -%}
            {# -- Append: INSERT without DELETE -- #}
            {%- set scope_script = scope__build_full_refresh_script(
                identifier,
                delta_location,
                ss_source_path,
                partition_by,
                scope_settings,
                scope_columns,
                feature_previews,
                sql
            ) -%}

            {{ log("SCOPE: Append for " ~ identifier, info=True) }}

            {%- call statement('main') -%}
                {{ scope_script }}
            {%- endcall -%}

        {%- elif strategy == 'delete+insert' -%}
            {# -- delete+insert: uses partition_by as the delete key -- #}
            {{ exceptions.raise_compiler_error(
                "dbt-scope delete+insert strategy requires microbatch config. "
                "Use incremental_strategy='microbatch' with event_time and batch_size."
            ) }}

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
   Macro: build the incremental (microbatch) SCOPE script
   ============================================================ #}
{% macro scope__build_incremental_script(
    table_name,
    delta_location,
    ss_source_path,
    partition_by,
    scope_settings,
    scope_columns,
    feature_previews,
    model_sql,
    batch_start,
    batch_end,
    delete_before_insert
) %}

// ============================================================
// Generated by dbt-scope adapter
// Model: {{ table_name }}
// Batch: {{ batch_start }} to {{ batch_end }}
// Strategy: microbatch ({{ 'delete+insert' if delete_before_insert else 'append' }})
// ============================================================

SET @@FeaturePreviews = "{{ feature_previews }}";
SET @@DeltaLakeCommitCondition = "FailIfPartitionConflict";

#DECLARE @deltaPath string = "{{ delta_location }}";
#DECLARE @ssBase string = "{{ ss_source_path }}";
#DECLARE @startDate string = "{{ batch_start }}";
#DECLARE @endDate string = "{{ batch_end }}";

{# -- CREATE TABLE IF NOT EXISTS -- #}
CREATE TABLE IF NOT EXISTS @target (
{%- for col in scope_columns %}
    {{ col.name }} {{ col.type }}{{ "," if not loop.last }}
{%- endfor %}
)
{%- if partition_by %}
PARTITIONED BY ({{ partition_by }})
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

{# -- DELETE existing batch partition data (only if delete_before_insert is true) -- #}
{%- if delete_before_insert and partition_by %}
DECLARE TABLE @target_rw
LOCATION @deltaPath
OPTIONS (LAYOUT = DELTA);

DELETE FROM @target_rw
WHERE {{ partition_by }} >= @startDate.Replace("-", "")
  AND {{ partition_by }} < @endDate.Replace("-", "");
{%- endif %}

{# -- EXTRACT from SS files -- #}
@data =
    EXTRACT
{%- for col in scope_columns %}
{%-   if partition_by is none or col.name != partition_by %}
        {{ col.name }} : {{ col.type }},
{%-   endif %}
{%- endfor %}
        _date : DateTime,
        _serial : int,
        _source_file = FILE.URI()
    FROM @ssBase + "/{_date:yyyy}/{_date:MM}/{_date:dd}/{_date:yyyy}{_date:MM}{_date:dd}_{*}_{_serial}.ss"
    USING Extractors.SStream();

{# -- User's transformation + date filter + INSERT -- #}
@batch_data =
    {{ model_sql }}
    WHERE _date >= DateTime.Parse(@startDate)
      AND _date < DateTime.Parse(@endDate);

INSERT INTO @target
SELECT * FROM @batch_data;

{% endmacro %}


{# ============================================================
   Strategy validation
   ============================================================ #}
{% macro scope__validate_get_incremental_strategy(raw_strategy) %}
    {%- set valid = ['microbatch', 'append', 'delete+insert'] -%}
    {%- if raw_strategy not in valid -%}
        {{ exceptions.raise_compiler_error(
            "Invalid incremental strategy '" ~ raw_strategy ~ "' for dbt-scope. "
            "Valid strategies: " ~ valid | join(', ')
        ) }}
    {%- endif -%}
    {{ return(raw_strategy) }}
{% endmacro %}
