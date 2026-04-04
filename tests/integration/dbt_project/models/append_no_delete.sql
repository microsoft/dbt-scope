{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='event_year_date',
        batch_size='day',
        begin=var('datagen_start_date', '2026-02-01'),
        lookback=1,
        partition_by='event_year_date',
        delta_location=var('delta_location'),
        ss_source_path=var('ss_source_path'),
        au=10,
        priority=1,
        scope_columns=[
            {'name': 'logical_server_name', 'type': 'string'},
            {'name': 'logical_database_name', 'type': 'string'},
            {'name': 'edition', 'type': 'string'},
            {'name': 'state', 'type': 'string'},
            {'name': 'region_name', 'type': 'string'},
            {'name': 'max_size_bytes', 'type': 'long'},
            {'name': 'event_year_date', 'type': 'string'}
        ]
    )
}}

SELECT
    logical_server_name,
    logical_database_name,
    edition,
    state,
    region_name,
    max_size_bytes,
    _date.ToString("yyyyMMdd") AS event_year_date
FROM @data
