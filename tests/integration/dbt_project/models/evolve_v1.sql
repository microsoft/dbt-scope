{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        partition_by='event_year_date',
        delta_location=var('delta_location_evolve'),
        job_tag=var('job_tag', none),
        source_roots=var('source_roots'),
        source_patterns=var('source_patterns', ['.*\\.ss$']),
        max_files_per_trigger=var('max_files_per_trigger', 500),
        starting_timestamp='1900-01-01T00:00:00+00:00',
        safety_buffer_seconds=0,
        source_compaction_interval=1,
        source_retention_files=100,
        au=4,
        priority=1,
        delta_table_columns=[
            {'name': 'logical_server_name', 'type': 'string'},
            {'name': 'edition', 'type': 'string'},
            {'name': 'max_size_bytes', 'type': 'long'},
            {'name': 'source_file_uri', 'type': 'string'},
            {'name': 'event_year_date', 'type': 'string'}
        ],
        extract_columns=[
            {'name': 'logical_server_name', 'type': 'string'},
            {'name': 'edition', 'type': 'string'},
            {'name': 'max_size_bytes', 'type': 'long'},
            {'name': 'source_file_uri', 'type': 'string'}
        ]
    )
}}

SELECT
    logical_server_name,
    edition,
    max_size_bytes,
    source_file_uri,
    DateTime.UtcNow.ToString("yyyyMMdd") AS event_year_date
FROM @data
