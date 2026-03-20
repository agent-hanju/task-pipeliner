"""Tests for loader module."""

from __future__ import annotations

from pathlib import Path

import orjson
import pytest
from loader import (
    detect_format,
    extract_date_prefix,
    load_items,
    load_json_file,
    load_jsonl_file,
    resolve_paths,
)

BOM = b"\xef\xbb\xbf"


class TestDetectFormat:
    """detect_format — JSON array vs JSONL detection."""

    def test_json_array(self, tmp_path: Path) -> None:
        p = tmp_path / "data.json"
        p.write_bytes(orjson.dumps([{"a": 1}, {"a": 2}]))
        assert detect_format(p) == "json"

    def test_jsonl(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        p.write_text('{"a": 1}\n{"a": 2}\n', encoding="utf-8")
        assert detect_format(p) == "jsonl"

    def test_json_with_bom(self, tmp_path: Path) -> None:
        p = tmp_path / "bom.json"
        p.write_bytes(BOM + orjson.dumps([{"x": 1}]))
        assert detect_format(p) == "json"

    def test_single_json_object(self, tmp_path: Path) -> None:
        p = tmp_path / "single.json"
        p.write_bytes(orjson.dumps({"key": "value"}))
        # Single object starting with '{' → first line is a complete JSON object → jsonl
        assert detect_format(p) == "jsonl"


class TestExtractDatePrefix:
    """extract_date_prefix — filename date extraction."""

    @pytest.mark.parametrize(
        ("filename", "expected"),
        [
            ("20240115_naver.json", "20240115"),
            ("20231201.jsonl", "20231201"),
            ("nodate.json", "00000000"),
            ("abc_20240115.json", "00000000"),
        ],
        ids=["with-suffix", "date-only", "no-date", "date-not-prefix"],
    )
    def test_extract(self, filename: str, expected: str) -> None:
        assert extract_date_prefix(Path(filename)) == expected


class TestLoadJsonFile:
    """load_json_file — JSON array, wrapper, and BOM handling."""

    def test_json_array(self, tmp_path: Path) -> None:
        p = tmp_path / "arr.json"
        p.write_bytes(orjson.dumps([{"a": 1}, {"b": 2}]))
        result = load_json_file(p)
        assert len(result) == 2
        assert result[0]["a"] == 1

    def test_data_wrapper(self, tmp_path: Path) -> None:
        p = tmp_path / "wrap.json"
        p.write_bytes(orjson.dumps({"data": [{"x": 10}]}))
        result = load_json_file(p)
        assert len(result) == 1
        assert result[0]["x"] == 10

    def test_single_object(self, tmp_path: Path) -> None:
        p = tmp_path / "single.json"
        p.write_bytes(orjson.dumps({"key": "val"}))
        result = load_json_file(p)
        assert len(result) == 1
        assert result[0]["key"] == "val"

    def test_bom_handling(self, tmp_path: Path) -> None:
        p = tmp_path / "bom.json"
        p.write_bytes(BOM + orjson.dumps([{"bom": True}]))
        result = load_json_file(p)
        assert len(result) == 1
        assert result[0]["bom"] is True


class TestLoadJsonlFile:
    """load_jsonl_file — line-by-line streaming."""

    def test_multiple_lines(self, tmp_path: Path) -> None:
        p = tmp_path / "data.jsonl"
        lines = ['{"a":1}', '{"a":2}', '{"a":3}']
        p.write_text("\n".join(lines) + "\n", encoding="utf-8")
        result = list(load_jsonl_file(p))
        assert len(result) == 3

    def test_skips_empty_lines(self, tmp_path: Path) -> None:
        p = tmp_path / "gaps.jsonl"
        p.write_text('{"a":1}\n\n\n{"a":2}\n', encoding="utf-8")
        result = list(load_jsonl_file(p))
        assert len(result) == 2


class TestResolvePaths:
    """resolve_paths — directory expansion and file passthrough."""

    def test_directory_expansion(self, tmp_path: Path) -> None:
        (tmp_path / "a.json").write_text("[]")
        (tmp_path / "b.jsonl").write_text("")
        (tmp_path / "c.txt").write_text("")  # should be ignored
        result = resolve_paths([tmp_path])
        assert len(result) == 2
        names = [p.name for p in result]
        assert "a.json" in names
        assert "b.jsonl" in names
        assert "c.txt" not in names

    def test_file_passthrough(self, tmp_path: Path) -> None:
        f = tmp_path / "data.json"
        f.write_text("[]")
        result = resolve_paths([f])
        assert result == [f]

    def test_sorted_output(self, tmp_path: Path) -> None:
        (tmp_path / "z.json").write_text("[]")
        (tmp_path / "a.json").write_text("[]")
        result = resolve_paths([tmp_path])
        assert result[0].name == "a.json"
        assert result[1].name == "z.json"


class TestLoadItems:
    """load_items — end-to-end loading with _date_prefix injection."""

    def test_mixed_formats(self, tmp_path: Path) -> None:
        # JSON array file
        json_file = tmp_path / "20240115_news.json"
        json_file.write_bytes(orjson.dumps([{"text": "article1"}, {"text": "article2"}]))
        # JSONL file
        jsonl_file = tmp_path / "20240201_data.jsonl"
        jsonl_file.write_text('{"text":"article3"}\n{"text":"article4"}\n', encoding="utf-8")

        items = list(load_items([tmp_path]))
        assert len(items) == 4

    def test_date_prefix_injection(self, tmp_path: Path) -> None:
        f = tmp_path / "20240115_news.json"
        f.write_bytes(orjson.dumps([{"text": "hello"}]))
        items = list(load_items([f]))
        assert items[0]["_date_prefix"] == "20240115"

    def test_no_date_prefix(self, tmp_path: Path) -> None:
        f = tmp_path / "nodate.json"
        f.write_bytes(orjson.dumps([{"text": "hello"}]))
        items = list(load_items([f]))
        assert items[0]["_date_prefix"] == "00000000"

    def test_bom_json_loads(self, tmp_path: Path) -> None:
        f = tmp_path / "20240101_bom.json"
        f.write_bytes(BOM + orjson.dumps([{"val": 42}]))
        items = list(load_items([f]))
        assert len(items) == 1
        assert items[0]["val"] == 42
        assert items[0]["_date_prefix"] == "20240101"
