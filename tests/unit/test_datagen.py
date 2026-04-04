"""Unit tests for datagen — no ADLA needed, tests script generation only."""

import sys
from pathlib import Path

# Add integration dir to path so we can import datagen
sys.path.insert(0, str(Path(__file__).parent.parent / "integration"))

from datagen import (
    ScopeColumn,
    ScopeDataset,
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
