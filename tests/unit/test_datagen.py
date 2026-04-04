"""Unit tests for datagen — no ADLA needed, tests script generation only."""

import sys
from pathlib import Path

# Add integration dir to path so we can import datagen
sys.path.insert(0, str(Path(__file__).parent.parent / "integration"))

from datagen import (
    ScopeColumn,
    ScopeDataset,
    dataset_to_records,
    generate_scope_script,
    make_default_dataset,
)


class TestGenerateScopeScript:
    def _make_dataset(self, days=3, files_per_day=2) -> ScopeDataset:
        return ScopeDataset(
            stream_name="test_stream",
            ss_root="/local/testuser/ss",
            columns=[
                ScopeColumn("name", "string"),
                ScopeColumn("value", "long"),
                ScopeColumn("PreciseTimeStamp", "DateTime"),
            ],
            rows=[
                ('"hello"', "42L", None),
                ('"world"', "99L", None),
            ],
            start_date="2026-03-01",
            days=days,
            files_per_day=files_per_day,
        )

    def test_output_count_matches_days_times_files(self):
        ds = self._make_dataset(days=5, files_per_day=3)
        script = generate_scope_script(ds)
        assert script.count("OUTPUT") == 5 * 3

    def test_output_count_single_file_per_day(self):
        ds = self._make_dataset(days=10, files_per_day=1)
        script = generate_scope_script(ds)
        assert script.count("OUTPUT") == 10

    def test_contains_stream_path(self):
        ds = self._make_dataset()
        script = generate_scope_script(ds)
        assert "/local/testuser/ss/test_stream/" in script

    def test_contains_date_directories(self):
        ds = self._make_dataset(days=3)
        script = generate_scope_script(ds)
        assert "/2026/03/01/" in script
        assert "/2026/03/02/" in script
        assert "/2026/03/03/" in script

    def test_contains_column_names(self):
        ds = self._make_dataset()
        script = generate_scope_script(ds)
        assert "name, value, PreciseTimeStamp" in script

    def test_contains_values(self):
        ds = self._make_dataset()
        script = generate_scope_script(ds)
        assert '"hello"' in script
        assert "42L" in script

    def test_timestamp_placeholder_filled(self):
        ds = self._make_dataset()
        script = generate_scope_script(ds)
        assert "__TIMESTAMP__" not in script
        assert 'DateTime.Parse("2026-03-01 19:00:00")' in script

    def test_stream_expiry(self):
        ds = self._make_dataset()
        script = generate_scope_script(ds)
        assert "STREAMEXPIRY" in script
        assert "TimeSpan.FromDays(7)" in script

    def test_ss_file_naming_pattern(self):
        ds = self._make_dataset(days=1, files_per_day=2)
        script = generate_scope_script(ds)
        assert "20260301_190000_0.ss" in script
        assert "20260301_200000_1.ss" in script

    def test_header_comment(self):
        ds = self._make_dataset(days=5, files_per_day=2)
        script = generate_scope_script(ds)
        assert "5 days x 2 files = 10 SS files" in script


class TestMakeDefaultDataset:
    def test_creates_with_defaults(self):
        ds = make_default_dataset(ss_root="/local/test/ss")
        assert ds.ss_root == "/local/test/ss"
        assert ds.days == 60
        assert ds.files_per_day == 2
        assert len(ds.columns) == 7
        assert len(ds.rows) == 4

    def test_custom_stream_name(self):
        ds = make_default_dataset(ss_root="/local/test/ss", stream_name="my_stream")
        assert ds.stream_name == "my_stream"
        assert ds.ss_base_path == "/local/test/ss/my_stream"

    def test_date_range(self):
        ds = make_default_dataset(ss_root="/local/test/ss", start_date="2026-01-01", days=5)
        dates = ds.date_range
        assert len(dates) == 5
        assert str(dates[0]) == "2026-01-01"
        assert str(dates[-1]) == "2026-01-05"

    def test_auto_stream_name(self):
        ds = make_default_dataset(ss_root="/local/test/ss")
        assert ds.stream_name.startswith("dbt_test_")


class TestExpectedRowsPerPartition:
    """Tests for per-partition row count tracking."""

    def _make_dataset(self, days=3, files_per_day=2, rows=None) -> ScopeDataset:
        return ScopeDataset(
            stream_name="test_stream",
            ss_root="/local/testuser/ss",
            columns=[
                ScopeColumn("name", "string"),
                ScopeColumn("value", "long"),
                ScopeColumn("PreciseTimeStamp", "DateTime"),
            ],
            rows=rows
            or [
                ('"hello"', "42L", None),
                ('"world"', "99L", None),
            ],
            start_date="2026-03-01",
            days=days,
            files_per_day=files_per_day,
        )

    def test_partition_manifest_keys(self):
        ds = self._make_dataset(days=3, files_per_day=2)
        manifest = ds.expected_rows_per_partition()
        assert set(manifest.keys()) == {"20260301", "20260302", "20260303"}

    def test_partition_manifest_values(self):
        ds = self._make_dataset(days=2, files_per_day=3)
        # 2 rows * 3 files_per_day = 6 rows per partition
        manifest = ds.expected_rows_per_partition()
        assert manifest["20260301"] == 6
        assert manifest["20260302"] == 6

    def test_total_expected_rows(self):
        ds = self._make_dataset(days=5, files_per_day=2)
        # 2 rows * 2 files * 5 days = 20
        assert ds.total_expected_rows == 20

    def test_manifest_single_file_per_day(self):
        ds = self._make_dataset(days=2, files_per_day=1)
        manifest = ds.expected_rows_per_partition()
        # 2 rows * 1 file = 2 rows per partition
        assert manifest["20260301"] == 2
        assert manifest["20260302"] == 2

    def test_manifest_many_rows(self):
        rows = [(f'"server-{i:03d}"', f"{i}L", None) for i in range(10)]
        ds = self._make_dataset(days=1, files_per_day=2, rows=rows)
        manifest = ds.expected_rows_per_partition()
        # 10 rows * 2 files = 20 rows
        assert manifest["20260301"] == 20
        assert ds.total_expected_rows == 20


class TestDatasetToRecords:
    """Tests for dataset_to_records — converts ScopeDataset to Python dicts."""

    def test_record_count_matches_total(self):
        ds = make_default_dataset(ss_root="/local/test/ss", days=3, files_per_day=2)
        records = dataset_to_records(ds)
        assert len(records) == ds.total_expected_rows

    def test_scope_string_quoting_stripped(self):
        ds = make_default_dataset(ss_root="/local/test/ss", days=1, files_per_day=1)
        records = dataset_to_records(ds)
        # First row has logical_server_name = '"server-001"' → 'server-001'
        assert records[0]["logical_server_name"] == "server-001"

    def test_long_suffix_stripped(self):
        ds = make_default_dataset(ss_root="/local/test/ss", days=1, files_per_day=1)
        records = dataset_to_records(ds)
        # First row has max_size_bytes = '1073741824L' → 1073741824
        assert records[0]["max_size_bytes"] == 1073741824

    def test_event_year_date_computed(self):
        ds = make_default_dataset(
            ss_root="/local/test/ss", start_date="2026-03-15", days=1, files_per_day=1
        )
        records = dataset_to_records(ds)
        assert records[0]["event_year_date"] == "20260315"

    def test_edition_values_present(self):
        ds = make_default_dataset(ss_root="/local/test/ss", days=1, files_per_day=1)
        records = dataset_to_records(ds)
        editions = {r["edition"] for r in records}
        assert editions == {"Standard", "Premium", "Basic"}

    def test_filter_by_edition(self):
        ds = make_default_dataset(ss_root="/local/test/ss", days=5, files_per_day=2)
        records = dataset_to_records(ds)
        standard = [r for r in records if r["edition"] == "Standard"]
        # 2 Standard rows out of 4, x 2 files x 5 days = 20
        assert len(standard) == 20
