"""Tests for io module (W-05)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from task_pipeliner.io import JsonlReader, JsonlWriter


class TestJsonlReader:
    @pytest.mark.parametrize("n", [0, 1, 100])
    def test_single_file_read_count(self, tmp_path: Path, n: int) -> None:
        f = tmp_path / "data.jsonl"
        f.write_text("\n".join(json.dumps({"i": i}) for i in range(n)) + ("\n" if n else ""))
        reader = JsonlReader([f])
        items = list(reader.read())
        assert len(items) == n

    def test_directory_glob(self, tmp_path: Path) -> None:
        for name in ["a.jsonl", "b.jsonl"]:
            (tmp_path / name).write_text(json.dumps({"x": 1}) + "\n")
        (tmp_path / "c.txt").write_text("ignored\n")
        reader = JsonlReader([tmp_path])
        items = list(reader.read())
        assert len(items) == 2

    def test_empty_file(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.jsonl"
        f.write_text("")
        reader = JsonlReader([f])
        assert list(reader.read()) == []


class TestJsonlWriter:
    @pytest.mark.parametrize("n", [0, 1, 100])
    def test_write_kept_count(self, tmp_path: Path, n: int) -> None:
        writer = JsonlWriter(tmp_path)
        with writer:
            for i in range(n):
                writer.write_kept({"i": i})
        kept = tmp_path / "kept.jsonl"
        if n == 0:
            lines = [x for x in kept.read_text(encoding="utf-8").splitlines() if x.strip()]
            assert len(lines) == 0
        else:
            lines = kept.read_text(encoding="utf-8").strip().splitlines()
            assert len(lines) == n

    def test_write_removed_reason(self, tmp_path: Path) -> None:
        writer = JsonlWriter(tmp_path)
        with writer:
            writer.write_removed({"val": 1}, reason="filter_step")
        removed = tmp_path / "removed.jsonl"
        data = json.loads(removed.read_text(encoding="utf-8").strip())
        assert data["reason"] == "filter_step"
        assert data["item"]["val"] == 1

    def test_creates_output_directory(self, tmp_path: Path) -> None:
        out = tmp_path / "sub" / "dir"
        writer = JsonlWriter(out)
        with writer:
            writer.write_kept({"x": 1})
        assert (out / "kept.jsonl").exists()

    def test_overwrite_existing(self, tmp_path: Path) -> None:
        kept = tmp_path / "kept.jsonl"
        kept.write_text('{"old": true}\n')
        writer = JsonlWriter(tmp_path)
        with writer:
            writer.write_kept({"new": True})
        lines = kept.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        assert json.loads(lines[0])["new"] is True
