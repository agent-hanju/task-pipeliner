"""Tests for result module — W-02."""

from __future__ import annotations

import json
from pathlib import Path

from result import FilterResult


def test_merge_counters() -> None:
    a = FilterResult(kept=3, removed=1, removed_reasons={"length/empty": 1})
    b = FilterResult(kept=2, removed=2, removed_reasons={"length/empty": 1, "pii/phone": 1})
    merged = a.merge(b)
    assert merged.kept == 5
    assert merged.removed == 3
    assert merged.removed_reasons == {"length/empty": 2, "pii/phone": 1}


def test_merge_empty() -> None:
    a = FilterResult(kept=1, removed=0, removed_reasons={})
    b = FilterResult(kept=0, removed=0, removed_reasons={})
    merged = a.merge(b)
    assert merged.kept == 1
    assert merged.removed == 0
    assert merged.removed_reasons == {}


def test_write_creates_json(tmp_path: Path) -> None:
    r = FilterResult(kept=10, removed=5, removed_reasons={"pii/email": 3, "length/empty": 2})
    r.write(tmp_path)

    stats_path = tmp_path / "stats.json"
    assert stats_path.exists()

    data = json.loads(stats_path.read_text(encoding="utf-8"))
    assert data["kept"] == 10
    assert data["removed"] == 5
    assert data["removed_reasons"]["pii/email"] == 3
    assert data["total"] == 15
