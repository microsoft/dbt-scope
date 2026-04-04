{# ============================================================
   incremental.sql — Microbatch incremental materialization for SCOPE

   Generates a SCOPE script per microbatch window that:
     1. Creates the Delta table (idempotent)
     2. Sets table properties
     3. DELETEs the batch partition range (idempotent replace)
     4. EXTRACTs SS files in the batch date range
     5. INSERTs the transformed batch data

   Supports strategies: microbatch, append, delete+insert

   days_per_batch: When > 1, the macro widens the batch window
   so that a single SCOPE job covers multiple days. dbt's microbatch
   loop still fires once per day; the macro emits a real script only
   on every Nth call and a no-op on the rest.
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
    {%- set days_per_batch = config.get('days_per_batch', 1) | int -%}

    {# -- Determine if this is a first run or full refresh -- #}
    {# Note: For the SCOPE adapter, is_first_run is unreliable because
       list_relations_without_caching returns [] (SCOPE has no catalog).
       The CREATE TABLE IF NOT EXISTS in every script handles first-run safely.
       We only use full_refresh_mode (--full-refresh flag) to decide. #}
    {%- set full_refresh_mode = (should_full_refresh()) -%}

    {# For microbatch with full refresh, only the first batch does work.
       Subsequent batches must be no-ops to avoid duplicating data. #}
    {%- set batch = model.get('batch', {}) -%}
    {%- set batch_start_raw = batch.get('event_time_start', '') -%}
    {%- set batch_end_raw = batch.get('event_time_end', '') -%}
    {# Normalize datetime objects to strings #}
    {%- set batch_start = batch_start_raw.strftime('%Y-%m-%d') if batch_start_raw is not string and batch_start_raw else batch_start_raw -%}
    {%- set batch_end = batch_end_raw.strftime('%Y-%m-%d') if batch_end_raw is not string and batch_end_raw else batch_end_raw -%}

    {%- if full_refresh_mode and strategy == 'microbatch' and batch_start -%}
        {# Microbatch full refresh: only batch 1 (offset 0) does the actual work #}
        {%- set begin_raw = config.get('begin', batch_start[:10]) -%}
        {%- set begin_str = begin_raw.strftime('%Y-%m-%d') if begin_raw is not string else begin_raw[:10] -%}
        {%- set batch_start_dt = modules.datetime.datetime.strptime(batch_start[:10], '%Y-%m-%d') -%}
        {%- set begin_dt = modules.datetime.datetime.strptime(begin_str, '%Y-%m-%d') -%}
        {%- set batch_offset = (batch_start_dt - begin_dt).days -%}

        {%- if batch_offset == 0 -%}
            {# First batch — run the actual full refresh #}
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
            {# Subsequent batch — skip to avoid duplicating data #}
            {{ log("SCOPE: Skipping batch " ~ batch_start ~ " (full refresh already loaded all data)", info=True) }}
            {%- call statement('main') -%}
                -- no-op: full refresh already loaded all data in batch 1
            {%- endcall -%}
        {%- endif -%}

    {%- elif full_refresh_mode -%}
        {# Non-microbatch full refresh #}
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

            {%- if batch_start and batch_end -%}
                {# -- days_per_batch: widen the window -- #}
                {%- if days_per_batch > 1 -%}
                    {%- set batch_start_dt = modules.datetime.datetime.strptime(batch_start[:10], '%Y-%m-%d') -%}
                    {%- set begin_raw = config.get('begin', batch_start[:10]) -%}
                    {%- set begin_str = begin_raw.strftime('%Y-%m-%d') if begin_raw is not string else begin_raw[:10] -%}
                    {%- set begin_dt = modules.datetime.datetime.strptime(begin_str, '%Y-%m-%d') -%}
                    {%- set day_offset = (batch_start_dt - begin_dt).days -%}
                    {%- if day_offset % days_per_batch != 0 -%}
                        {# -- Not the Nth day: emit a no-op -- #}
                        {{ log("SCOPE: Skipping batch " ~ batch_start ~ " (days_per_batch=" ~ days_per_batch ~ ", offset=" ~ day_offset ~ ")", info=True) }}
                        {%- call statement('main') -%}
                            -- no-op: days_per_batch={{ days_per_batch }}, waiting for batch alignment
                        {%- endcall -%}
                    {%- else -%}
                        {# -- Nth day: widen the end to cover days_per_batch days -- #}
                        {%- set widened_end_dt = batch_start_dt + modules.datetime.timedelta(days=days_per_batch) -%}
                        {%- set widened_end = widened_end_dt.strftime('%Y-%m-%d') -%}

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
                            widened_end,
                            delete_before_insert
                        ) -%}

                        {{ log("SCOPE: Microbatch " ~ batch_start ~ " → " ~ widened_end ~ " (days_per_batch=" ~ days_per_batch ~ ") for " ~ identifier, info=True) }}

                        {%- call statement('main') -%}
                            {{ scope_script }}
                        {%- endcall -%}
                    {%- endif -%}
                {%- else -%}
                    {# -- Standard single-day batch -- #}
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
                {%- endif -%}
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
{# -- Normalize partition_by to a list -- #}
{%- set partition_cols = partition_by if partition_by is iterable and partition_by is not string else ([partition_by] if partition_by else []) -%}
{# Only the first partition column is date-derived and excluded from EXTRACT #}
{%- set derived_col = partition_cols[0] if partition_cols else none -%}

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

{# -- DELETE existing batch partition data (only if delete_before_insert is true) -- #}
{%- if delete_before_insert and partition_cols %}
DECLARE TABLE @target_rw
LOCATION @deltaPath
OPTIONS (LAYOUT = DELTA);

DELETE FROM @target_rw
WHERE {{ partition_cols[0] }} >= @startDate.Replace("-", "")
  AND {{ partition_cols[0] }} < @endDate.Replace("-", "");
{%- endif %}

{# -- EXTRACT from SS files -- #}
@data =
    EXTRACT
{%- for col in scope_columns %}
{%-   if col.name != derived_col %}
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
