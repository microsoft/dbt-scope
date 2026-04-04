<!-- PROJECT LOGO -->
<p align="center">
  <img src="https://rakirahman.blob.core.windows.net/public/images/Misc/dbt-scope.png" alt="Logo" width="30%">
  <h3 align="center">ADLA - dbt</h3>
  <p align="center">
    Incremental data transformation for ADLA using dbt.
    <br />
    <br />
    <a href="https://docs.getdbt.com/">dbt Docs</a>
    ·
    <a href="https://azure.microsoft.com/en-us/products/data-lake-analytics">Azure Data Lake Analytics docs</a>
  </p>
</p>

---

- **Clean SQL models** — write `SELECT ... FROM @data`; macros generate `#DECLARE`, `EXTRACT`, `INSERT INTO`
- **Microbatch incremental** — `DELETE+INSERT` per date partition, with dbt retry/backfill built in. `DELETE` is optional if the target can handle deduplication 
- **Declarative table properties** — compression, checkpoint intervals via `scope_settings`

## How it works

SS files live on ADLS in date-partitioned directories. `dbt run` computes which date batches need processing, generates a SCOPE script per batch, and submits each as an ADLA job. Each job reads only its date range (FileSet partition elimination), optionally deletes the target partition in Delta (for idempotent re-runs without duplicates), and inserts into a Delta table on ADLS.

```mermaid
flowchart TB
    subgraph ADLS["ADLS -- SS source files"]
        direction LR
        SS1["📂 /2026/04/01/<br/>20260401_*.ss"]
        SS2["📂 /2026/04/02/<br/>20260402_*.ss"]
        SS3["📂 /2026/04/03/<br/>20260403_*.ss"]
        SSN["📂 ..."]
    end

    subgraph DBT["dbt-scope adapter"]
        direction TB
        Model["model.sql<br/><i>SELECT ... FROM @data</i>"]
        Micro["dbt microbatch<br/>splits into daily batches"]
        Model --> Micro

        subgraph Batch1["Batch: 04-01 to 04-02"]
            Gen1["Jinja macros generate<br/>complete SCOPE script"]
            S1["SET @@FeaturePreviews<br/>#DECLARE @startDate='2026-04-01'<br/>#DECLARE @endDate='2026-04-02'"]
            DDL1["CREATE TABLE IF NOT EXISTS<br/>PARTITIONED BY (event_year_date)<br/>OPTIONS (LAYOUT = DELTA)"]
            DEL1["DELETE FROM @target<br/>WHERE partition in batch range"]
            EXT1["EXTRACT ... FROM .../{_date:yyyy}/.../*.ss<br/>USING Extractors.SStream()<br/>WHERE _date in batch range"]
            INS1["INSERT INTO @target<br/>SELECT * FROM @batch_data"]
            Gen1 --> S1 --> DDL1 --> DEL1 --> EXT1 --> INS1
        end

        Micro -- "batch 1" --> Gen1
    end

    subgraph ADLA["ADLA"]
        Job1["SCOPE job<br/>compile + execute<br/>(couple mins)"]
    end

    subgraph ADLS["ADLS Gen2 -- Delta table"]
        direction LR
        P1["📂 event_year_date=20260401/<br/>part-*.parquet"]
        P2["📂 event_year_date=20260402/<br/>part-*.parquet"]
        P3["📂 event_year_date=20260403/<br/>part-*.parquet"]
        DL["📄 _delta_log/"]
    end

    INS1 -- "REST API<br/>submit + poll" --> Job1
    Job1 -- "reads only<br/>04-01 files" --> SS1
    Job1 -- "writes<br/>partition" --> P1

    Micro -. "batch 2: same flow, reads 04-02, writes P2" .-> SS2
    Micro -. "batch 3: same flow, reads 04-03, writes P3" .-> SS3

    style DEL1 fill:#fee,stroke:#c00
    style Batch1 fill:#f0f7ff,stroke:#369
```

On **full refresh**, all batches run and there is no `DELETE` step.
On **incremental**, dbt only runs batches after the last checkpoint (`MAX(event_time)` from the target). The `DELETE` (highlighted red) makes each batch idempotent — re-running the same batch replaces the partition.

## Install

```powershell
pip install -e ".[dev]"    # Python 3.10+, dbt-core ~1.9
```

## Configure

All sensitive values live in `.env` (see `.env.example`). The profile references them via `env_var()`:

```yaml
# profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: scope
      database: "{{ env_var('SCOPE_STORAGE_ACCOUNT') }}"
      schema: "{{ env_var('SCOPE_CONTAINER') }}"
      adla_account: "{{ env_var('SCOPE_ADLA_ACCOUNT') }}"
      storage_account: "{{ env_var('SCOPE_STORAGE_ACCOUNT') }}"
      container: "{{ env_var('SCOPE_CONTAINER') }}"
      delta_base_path: delta
      au: 100
      priority: 1
```

| dbt concept   | SCOPE concept                                |
| ------------- | -------------------------------------------- |
| `database`    | Storage account name                         |
| `schema`      | ADLS container                               |
| `table`       | Full-refresh: `CREATE TABLE` + `INSERT INTO` |
| `incremental` | Microbatch: `DELETE` partition + `INSERT`    |
| model SQL     | `SELECT` from `@data` (extracted SS rowset)  |

## Usage

### Full refresh

```sql
{{ config(
    materialized='table',
    delta_location='abfss://ctr@acct.dfs.core.windows.net/delta/my_table',
    ss_source_path='/my/cosmos/path/to/MyStream',
    partition_by='event_year_date',
    scope_columns=[
        {'name': 'server_name', 'type': 'string'},
        {'name': 'event_year_date', 'type': 'string'}
    ],
    scope_settings={'microsoft.scope.compression': 'vorder:zstd#11'}
) }}

SELECT logical_server_name_DT_String AS server_name,
       _date.ToString("yyyyMMdd") AS event_year_date
FROM @data
```

### Incremental (microbatch)

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_year_date',
    batch_size='day',
    begin='2026-04-01',
    lookback=1,
    partition_by='event_year_date',
    delta_location='abfss://ctr@acct.dfs.core.windows.net/delta/my_model',
    ss_source_path='/my/cosmos/path/to/MyStream',
    scope_columns=[
        {'name': 'server_name', 'type': 'string'},
        {'name': 'event_year_date', 'type': 'string'}
    ]
) }}

SELECT logical_server_name_DT_String AS server_name,
       _date.ToString("yyyyMMdd") AS event_year_date
FROM @data
```

`dbt retry` re-runs failed batches. `dbt run --event-time-start/end` backfills a range.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
