{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        partition_by='event_year_date',
        delta_location=var('delta_location_filtered'),
        source_roots=var('source_roots'),
        source_patterns=var('source_patterns', ['.*\\.ss$']),
        max_files_per_trigger=var('max_files_per_trigger', 50),
        safety_buffer_seconds=0,
        source_compaction_interval=1,
        source_retention_files=100,
        au=4,
        priority=1,
        scope_columns=[
            {'name': 'logical_server_name', 'type': 'string'},
            {'name': 'logical_database_name', 'type': 'string'},
            {'name': 'edition', 'type': 'string'},
            {'name': 'state', 'type': 'string'},
            {'name': 'region_name', 'type': 'string'},
            {'name': 'max_size_bytes', 'type': 'long'},
            {'name': 'source_file_uri', 'type': 'string'},
            {'name': 'source_file_length', 'type': 'long'},
            {'name': 'source_file_created', 'type': 'DateTime'},
            {'name': 'source_file_modified', 'type': 'DateTime'},
            {'name': 'event_year_date', 'type': 'string', 'extract': false}
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
    source_file_uri,
    (long)(source_file_length ?? 0L) AS source_file_length,
    (DateTime)(source_file_created ?? DateTime.MinValue) AS source_file_created,
    (DateTime)(source_file_modified ?? DateTime.MinValue) AS source_file_modified,
    DateTime.UtcNow.ToString("yyyyMMdd") AS event_year_date
FROM @data
WHERE edition == "Standard"
