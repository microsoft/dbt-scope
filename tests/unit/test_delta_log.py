"""Unit tests for Delta log reading — commit parsing and mocked ADLS listing."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from azure.core.exceptions import ResourceNotFoundError

from dbt.adapters.scope.delta_lake import (
    DeltaCommit,
    DuckDbDeltaLakeClient,
    physical_column_name,
)

DELTA_LOC = "abfss://ctr@acct.dfs.core.windows.net/delta/my_table"
LOG = "delta/my_table/_delta_log"

SCHEMA = json.dumps(
    {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "long",
                "nullable": True,
                "metadata": {"delta.columnMapping.physicalName": "col-a1b2"},
            },
            {
                "name": "source_file_uri",
                "type": "string",
                "nullable": True,
                "metadata": {"delta.columnMapping.physicalName": "col-c3d4"},
            },
            {"name": "Event_Date", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
)


def _commit_text(*actions: dict) -> str:
    return "\n".join(json.dumps(a) for a in actions)


def _scope_commit(version: int, token: str) -> DeltaCommit:
    return DeltaCommit.parse(
        version,
        _commit_text(
            {
                "commitInfo": {
                    "timestamp": 1775018672000,
                    "operation": "SET TBLPROPERTIES",
                    "operationParameters": {
                        "properties": json.dumps({"dbt_scope.batch_token": token})
                    },
                    "engineInfo": "ScopeEngine/1.1.0 Delta-Lake/3.1.0",
                    "txnId": "f0e1",
                }
            },
            {
                "metaData": {
                    "schemaString": SCHEMA,
                    "configuration": {"dbt_scope.batch_token": token},
                }
            },
            {"protocol": {"minReaderVersion": 3, "minWriterVersion": 7}},
            {"add": {"path": "part-0001.parquet", "dataChange": True, "modificationTime": 5}},
            {"add": {"path": "part-0002.parquet", "dataChange": True, "modificationTime": 6}},
        ),
    )


def _client() -> DuckDbDeltaLakeClient:
    credential = MagicMock()
    credential.get_token.return_value = SimpleNamespace(token="storage-token")
    return DuckDbDeltaLakeClient(credential=credential, connection_factory=MagicMock())


def _paths(*names: str, directories: tuple[str, ...] = ()) -> list[SimpleNamespace]:
    return [SimpleNamespace(name=f"{LOG}/{n}", is_directory=False) for n in names] + [
        SimpleNamespace(name=f"{LOG}/{d}", is_directory=True) for d in directories
    ]


class TestDeltaCommit:
    def test_parses_scope_engine_commit(self):
        commit = _scope_commit(7, "tok-1")
        assert commit.version == 7
        assert commit.is_scope_engine
        assert commit.commit_info["operation"] == "SET TBLPROPERTIES"
        assert commit.configuration == {"dbt_scope.batch_token": "tok-1"}
        assert commit.schema_string == SCHEMA
        assert commit.added_data_paths == ["part-0001.parquet", "part-0002.parquet"]
        assert commit.timestamp_ms == 1775018672000

    def test_skips_blank_and_unparseable_lines(self):
        commit = DeltaCommit.parse(1, '\n{"add": {"path": "a.parquet"}}\nnot json\n[1]\n')
        assert commit.added_data_paths == ["a.parquet"]

    def test_carries_property_via_configuration_or_operation_parameters(self):
        assert _scope_commit(1, "tok-1").carries_property("dbt_scope.batch_token", "tok-1")
        assert not _scope_commit(1, "tok-1").carries_property("dbt_scope.batch_token", "tok-2")
        params_only = DeltaCommit.parse(
            2,
            _commit_text(
                {
                    "commitInfo": {
                        "engineInfo": "ScopeEngine/1.1.0",
                        "operationParameters": {"properties": '{"dbt_scope.batch_token":"tok-3"}'},
                    }
                }
            ),
        )
        assert params_only.carries_property("dbt_scope.batch_token", "tok-3")
        assert not params_only.carries_property("dbt_scope.batch_token", "tok-1")

    def test_spark_maintenance_is_not_scope_engine(self):
        commit = DeltaCommit.parse(
            3,
            _commit_text(
                {"commitInfo": {"engineInfo": "Apache-Spark/3.5.1 Delta-Lake/3.2.0"}},
                {"add": {"path": "compacted.parquet", "dataChange": False}},
                {"remove": {"path": "part-0001.parquet", "dataChange": False}},
            ),
        )
        assert not commit.is_scope_engine
        assert commit.added_data_paths == []

    def test_table_creating_commit_without_commit_info_is_scope_engine(self):
        commit = DeltaCommit.parse(0, _commit_text({"metaData": {"configuration": {}}}))
        assert commit.is_scope_engine

    def test_timestamp_falls_back_to_add_times_then_file_time(self):
        adds = DeltaCommit.parse(
            1,
            _commit_text(
                {"add": {"path": "a", "modificationTime": 10}},
                {"add": {"path": "b", "modificationTime": 30}},
            ),
            last_modified_ms=99,
        )
        assert adds.timestamp_ms == 30
        assert DeltaCommit.parse(2, "", last_modified_ms=99).timestamp_ms == 99


class TestPhysicalColumnName:
    def test_uses_column_mapping_physical_name(self):
        assert physical_column_name(SCHEMA, "source_file_uri") == "col-c3d4"

    def test_falls_back_to_logical_name_and_case_insensitive_match(self):
        assert physical_column_name(SCHEMA, "event_date") == "Event_Date"

    def test_absent_column_or_schema(self):
        assert physical_column_name(SCHEMA, "missing") is None
        assert physical_column_name(None, "source_file_uri") is None
        assert physical_column_name("not json", "source_file_uri") is None


class TestLogVersions:
    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_lists_json_commits_and_latest_version(self, mock_service):
        fs = mock_service.return_value.get_file_system_client.return_value
        fs.get_paths.return_value = _paths(
            "00000000000000000010.checkpoint.parquet",
            "00000000000000000011.json",
            "00000000000000000012.json",
            "_last_checkpoint",
            directories=("_staged_commits",),
        )
        assert _client().log_versions(DELTA_LOC) == ([11, 12], 12)
        fs.get_paths.assert_called_once_with(path=LOG, recursive=False)

    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_latest_version_from_checkpoint_when_json_commits_are_gone(self, mock_service):
        fs = mock_service.return_value.get_file_system_client.return_value
        fs.get_paths.return_value = _paths(
            "00000000000000000020.checkpoint.0000000001.0000000002.parquet",
            "00000000000000000020.checkpoint.0000000002.0000000002.parquet",
        )
        assert _client().log_versions(DELTA_LOC) == ([], 20)
        assert _client().latest_version(DELTA_LOC) == 20

    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_missing_log_is_version_minus_one(self, mock_service):
        fs = mock_service.return_value.get_file_system_client.return_value
        fs.get_paths.side_effect = ResourceNotFoundError("missing")
        assert _client().log_versions(DELTA_LOC) == ([], -1)


class TestReadCommit:
    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_reads_and_parses_commit(self, mock_service):
        fs = mock_service.return_value.get_file_system_client.return_value
        downloader = MagicMock()
        downloader.readall.return_value = _commit_text({"add": {"path": "a.parquet"}}).encode()
        downloader.properties.last_modified = datetime(2026, 4, 1, tzinfo=timezone.utc)
        fs.get_file_client.return_value.download_file.return_value = downloader

        commit = _client().read_commit(DELTA_LOC, 5)

        fs.get_file_client.assert_called_once_with(f"{LOG}/00000000000000000005.json")
        assert commit.version == 5
        assert commit.added_data_paths == ["a.parquet"]
        assert commit.last_modified_ms == 1775001600000

    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_read_commits_skips_missing_commits(self, mock_service):
        fs = mock_service.return_value.get_file_system_client.return_value

        def file_client(path):
            client = MagicMock()
            if path.endswith("00000000000000000002.json"):
                client.download_file.side_effect = ResourceNotFoundError("gone")
            else:
                client.download_file.return_value.readall.return_value = b"{}"
                client.download_file.return_value.properties = None
            return client

        fs.get_file_client.side_effect = file_client

        commits = _client().read_commits(DELTA_LOC, [3, 1, 2])

        assert [c.version for c in commits] == [1, 3]

    def test_latest_metadata_commit_skips_commits_without_metadata(self):
        client = _client()
        data_only = DeltaCommit.parse(9, _commit_text({"add": {"path": "a"}}))
        with (
            patch.object(client, "_list_log", return_value=([7, 8, 9], {})),
            patch.object(
                client,
                "read_commit",
                side_effect=lambda _loc, v: data_only if v == 9 else _scope_commit(v, f"t{v}"),
            ),
        ):
            assert client.latest_metadata_commit(DELTA_LOC).version == 8

    def test_latest_metadata_commit_reads_checkpoint_when_newer_commits_have_none(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (
            SCHEMA,
            [{"key": "dbt_scope.batch_token", "value": "tok-9"}],
        )
        credential = MagicMock()
        credential.get_token.return_value = SimpleNamespace(token="storage-token")
        client = DuckDbDeltaLakeClient(credential=credential, connection_factory=lambda: conn)
        data_only = DeltaCommit.parse(11, _commit_text({"add": {"path": "a"}}))
        checkpoint = f"{LOG}/00000000000000000010.checkpoint.parquet"
        with (
            patch.object(client, "_list_log", return_value=([9, 10, 11], {10: [checkpoint]})),
            patch.object(client, "read_commit", return_value=data_only) as read_commit,
        ):
            commit = client.latest_metadata_commit(DELTA_LOC)

        read_commit.assert_called_once_with(DELTA_LOC, 11)
        assert commit.version == 10
        assert commit.configuration == {"dbt_scope.batch_token": "tok-9"}
        assert commit.schema_string == SCHEMA
        query = conn.execute.call_args_list[-1].args[0]
        assert f"'abfss://ctr@acct.dfs.core.windows.net/{checkpoint}'" in query


class TestDistinctColumnValues:
    def test_reads_added_files_by_physical_column(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [("/shares/a.ss",), (None,)]
        credential = MagicMock()
        credential.get_token.return_value = SimpleNamespace(token="storage-token")
        client = DuckDbDeltaLakeClient(credential=credential, connection_factory=lambda: conn)

        values = client.distinct_column_values(
            DELTA_LOC,
            [
                "event_date=2026-04-01/part%20one.parquet",
                "abfss://ctr@acct.dfs.core.windows.net/delta/my_table/abs.parquet",
            ],
            "col-c3d4",
        )

        assert values == {"/shares/a.ss"}
        query = conn.execute.call_args_list[-1].args[0]
        assert 'SELECT DISTINCT "col-c3d4"' in query
        assert (
            "'abfss://ctr@acct.dfs.core.windows.net/delta/my_table/"
            "event_date=2026-04-01/part one.parquet'"
        ) in query
        assert "'abfss://ctr@acct.dfs.core.windows.net/delta/my_table/abs.parquet'" in query

    def test_no_files_is_empty(self):
        conn = MagicMock()
        client = DuckDbDeltaLakeClient(credential=MagicMock(), connection_factory=lambda: conn)
        assert client.distinct_column_values(DELTA_LOC, [], "c") == set()
        conn.execute.assert_not_called()
