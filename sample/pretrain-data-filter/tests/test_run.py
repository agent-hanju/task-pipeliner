"""Tests for run.py — W-15."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.dummy_data import NORMAL_TEXT, SAMPLE_NORMAL_ITEM, SAMPLE_SHORT_ITEM


def _write_jsonl(path: Path, items: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    return path


def _read_jsonl(path: Path) -> list[dict]:
    results = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                results.append(json.loads(stripped))
    return results


@pytest.mark.timeout(60)
def test_pipeline_produces_output(tmp_path: Path) -> None:
    """Basic pipeline run: normal + short item → kept + removed files."""
    from run import main

    input_path = _write_jsonl(
        tmp_path / "input.jsonl",
        [SAMPLE_NORMAL_ITEM, SAMPLE_SHORT_ITEM],
    )
    output_dir = tmp_path / "output"

    main(
        input_paths=[input_path],
        output_dir=output_dir,
        no_minhash=False,
    )

    kept_path = output_dir / "kept.jsonl"
    removed_path = output_dir / "removed.jsonl"

    assert kept_path.exists() or removed_path.exists(), "At least one output file should exist"

    if kept_path.exists():
        kept = _read_jsonl(kept_path)
        assert len(kept) >= 1

    if removed_path.exists():
        removed = _read_jsonl(removed_path)
        assert len(removed) >= 1
        # Short item should be removed
        assert any("length" in r.get("_removed_reason", "") for r in removed)


@pytest.mark.timeout(60)
def test_pipeline_no_minhash(tmp_path: Path) -> None:
    """Pipeline with --no-minhash should still work."""
    from run import main

    items = [{"id": f"doc_{i}", "text": NORMAL_TEXT + f" variation {i}"} for i in range(3)]
    input_path = _write_jsonl(tmp_path / "input.jsonl", items)
    output_dir = tmp_path / "output"

    main(
        input_paths=[input_path],
        output_dir=output_dir,
        no_minhash=True,
    )

    kept_path = output_dir / "kept.jsonl"
    assert kept_path.exists()
    kept = _read_jsonl(kept_path)
    assert len(kept) >= 1
