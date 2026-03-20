"""Tests for taxonomy module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from taxonomy import TaxonomyResult


class TestTaxonomyResultMerge:
    """TaxonomyResult.merge() — two results summed correctly."""

    @pytest.mark.parametrize(
        ("a", "b", "expected"),
        [
            (
                TaxonomyResult(success=3, skipped=1, errored=0),
                TaxonomyResult(success=2, skipped=0, errored=1),
                TaxonomyResult(success=5, skipped=1, errored=1),
            ),
            (
                TaxonomyResult(),
                TaxonomyResult(),
                TaxonomyResult(),
            ),
            (
                TaxonomyResult(success=10, skipped=5, errored=2),
                TaxonomyResult(success=0, skipped=0, errored=0),
                TaxonomyResult(success=10, skipped=5, errored=2),
            ),
        ],
        ids=["normal", "zeros", "one-side-zero"],
    )
    def test_merge(self, a: TaxonomyResult, b: TaxonomyResult, expected: TaxonomyResult) -> None:
        result = a.merge(b)
        assert result.success == expected.success
        assert result.skipped == expected.skipped
        assert result.errored == expected.errored

    def test_merge_returns_new_instance(self) -> None:
        a = TaxonomyResult(success=1)
        b = TaxonomyResult(success=2)
        result = a.merge(b)
        assert result is not a
        assert result is not b

    def test_merge_is_associative(self) -> None:
        a = TaxonomyResult(success=1, skipped=2, errored=3)
        b = TaxonomyResult(success=4, skipped=5, errored=6)
        c = TaxonomyResult(success=7, skipped=8, errored=9)
        left = a.merge(b).merge(c)
        right = a.merge(b.merge(c))
        assert left.success == right.success
        assert left.skipped == right.skipped
        assert left.errored == right.errored


class TestTaxonomyResultWrite:
    """TaxonomyResult.write() — JSON file creation and content verification."""

    def test_write_creates_file(self, tmp_path: Path) -> None:
        result = TaxonomyResult(success=10, skipped=3, errored=1)
        result.write(tmp_path)
        path = tmp_path / "count_summary.json"
        assert path.exists()

    def test_write_content(self, tmp_path: Path) -> None:
        result = TaxonomyResult(success=10, skipped=3, errored=1)
        result.write(tmp_path)
        data = json.loads((tmp_path / "count_summary.json").read_text())
        assert data["success"] == 10
        assert data["skipped"] == 3
        assert data["errored"] == 1
        assert data["total"] == 14

    def test_write_creates_parent_dirs(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b" / "c"
        result = TaxonomyResult(success=1)
        result.write(nested)
        assert (nested / "count_summary.json").exists()
