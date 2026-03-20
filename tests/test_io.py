"""Tests for io module (W-05, W-R06)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from task_pipeliner.io import JsonlReader, JsonlSourceStep, JsonlWriter


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
    def test_write_count(self, tmp_path: Path, n: int) -> None:
        out_file = tmp_path / "output.jsonl"
        writer = JsonlWriter(out_file)
        with writer:
            for i in range(n):
                writer.write({"i": i})
        if n == 0:
            lines = [x for x in out_file.read_text(encoding="utf-8").splitlines() if x.strip()]
            assert len(lines) == 0
        else:
            lines = out_file.read_text(encoding="utf-8").strip().splitlines()
            assert len(lines) == n

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        out_file = tmp_path / "sub" / "dir" / "output.jsonl"
        writer = JsonlWriter(out_file)
        with writer:
            writer.write({"x": 1})
        assert out_file.exists()

    def test_overwrite_existing(self, tmp_path: Path) -> None:
        out_file = tmp_path / "output.jsonl"
        out_file.write_text('{"old": true}\n')
        writer = JsonlWriter(out_file)
        with writer:
            writer.write({"new": True})
        lines = out_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        assert json.loads(lines[0])["new"] is True


class TestJsonlSourceStep:
    def test_items_yields_from_jsonl(self, tmp_path: Path) -> None:
        f = tmp_path / "data.jsonl"
        f.write_text("\n".join(json.dumps({"i": i}) for i in range(5)) + "\n")
        step = JsonlSourceStep(paths=[str(f)])
        items = list(step.items())
        assert len(items) == 5
        assert items[0] == {"i": 0}

    def test_step_type_is_source(self) -> None:
        from task_pipeliner.base import StepType

        step = JsonlSourceStep()
        assert step.step_type == StepType.SOURCE

    def test_process_raises(self) -> None:
        step = JsonlSourceStep()
        with pytest.raises(NotImplementedError):
            step.process({}, None, lambda x: None)
