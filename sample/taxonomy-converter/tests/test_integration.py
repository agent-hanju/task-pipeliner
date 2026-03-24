"""Integration tests: end-to-end pipeline execution."""

from __future__ import annotations

import json
from pathlib import Path

import orjson
import pytest
from run import main


def _write_json_input(path: Path, items: list[dict]) -> Path:
    """Write items as a JSON array file."""
    path.write_bytes(orjson.dumps(items))
    return path


def _write_jsonl_input(path: Path, items: list[dict]) -> Path:
    """Write items as a JSONL file."""
    with path.open("wb") as f:
        for item in items:
            f.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE))
    return path


def _read_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file into a list of dicts."""
    results = []
    with path.open("rb") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                results.append(orjson.loads(stripped))
    return results


def _make_article(index: int, text: str | None = None) -> dict:
    """Create a minimal valid article dict."""
    return {
        "index": index,
        "title": f"테스트 기사 제목 {index}",
        "text": text
        or (
            f"이것은 테스트 기사 {index}의 본문입니다.\n"
            "한국 경제가 올해 성장세를 이어갈 것으로 전망된다.\n"
            "전문가들은 수출 증가와 내수 회복을 주요 요인으로 꼽았다.\n"
            "정부는 관련 정책을 지속적으로 추진할 방침이다.\n"
            "시장 참여자들은 긍정적인 반응을 보이고 있다.\n"
            "하반기에도 이러한 추세가 이어질 것으로 예상된다."
        ),
        "date": "20240301",
        "_date_prefix": "20240301",
    }


@pytest.mark.timeout(60)
class TestEndToEnd:
    """End-to-end pipeline integration tests."""

    def test_json_input_produces_output(self, tmp_path: Path) -> None:
        """JSON array input → pipeline → kept.jsonl exists with valid JSON."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        items = [_make_article(i) for i in range(3)]
        _write_json_input(input_dir / "20240301_news.json", items)

        main(input_dir, output_dir)

        kept = output_dir / "kept.jsonl"
        assert kept.exists()
        results = _read_jsonl(kept)
        assert len(results) == 3

    def test_output_has_taxonomy_fields(self, tmp_path: Path) -> None:
        """Each output item has all required TaxonomyDict fields."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        _write_json_input(input_dir / "20240301_test.json", [_make_article(1)])
        main(input_dir, output_dir)

        results = _read_jsonl(output_dir / "kept.jsonl")
        assert len(results) == 1
        item = results[0]

        required_fields = [
            "dataset_name",
            "id",
            "text",
            "language",
            "type",
            "method",
            "category",
            "industrial_field",
            "template",
            "metadata",
        ]
        for field in required_fields:
            assert field in item, f"Missing field: {field}"

        meta = item["metadata"]
        meta_fields = [
            "source",
            "published_date",
            "collected_date",
            "token_len",
            "quality_level",
            "author",
        ]
        for field in meta_fields:
            assert field in meta, f"Missing metadata field: {field}"

    def test_duplicate_removal(self, tmp_path: Path) -> None:
        """Duplicate items are removed (same id + same text)."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        # Same article twice → should produce only 1 output
        article = _make_article(1)
        _write_json_input(input_dir / "20240301_dup.json", [article, article])

        main(input_dir, output_dir)

        results = _read_jsonl(output_dir / "kept.jsonl")
        assert len(results) == 1

    def test_short_article_skipped(self, tmp_path: Path) -> None:
        """Articles too short are skipped — appear in removed.jsonl, not in kept.jsonl."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        short = {
            "index": 1,
            "title": "짧은 기사",
            "text": "너무 짧은 본문.",
            "date": "20240301",
            "_date_prefix": "20240301",
        }
        normal = _make_article(2)
        _write_json_input(input_dir / "20240301_mix.json", [short, normal])

        main(input_dir, output_dir)

        kept = _read_jsonl(output_dir / "kept.jsonl")
        assert len(kept) == 1
        removed = _read_jsonl(output_dir / "removed.jsonl")
        assert len(removed) == 1

    def test_stats_json_written(self, tmp_path: Path) -> None:
        """stats.json is created after pipeline run."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        _write_json_input(input_dir / "20240301_stats.json", [_make_article(1)])
        main(input_dir, output_dir)

        stats_path = output_dir / "stats.json"
        assert stats_path.exists()
        stats = json.loads(stats_path.read_text())
        assert isinstance(stats, list)
        assert len(stats) >= 3  # preprocess, convert, deduplicate

    def test_directory_input(self, tmp_path: Path) -> None:
        """Directory path → all JSON/JSONL files processed."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        _write_json_input(input_dir / "20240301_a.json", [_make_article(1)])
        _write_json_input(input_dir / "20240302_b.json", [_make_article(2)])

        main(input_dir, output_dir)

        results = _read_jsonl(output_dir / "kept.jsonl")
        assert len(results) == 2

    def test_mixed_json_jsonl_input(self, tmp_path: Path) -> None:
        """Mixed JSON array + JSONL input → all items processed."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        _write_json_input(input_dir / "20240301_array.json", [_make_article(1)])
        _write_jsonl_input(
            input_dir / "20240302_lines.jsonl",
            [_make_article(2), _make_article(3)],
        )

        main(input_dir, output_dir)

        results = _read_jsonl(output_dir / "kept.jsonl")
        assert len(results) == 3

    def test_stats_json_written(self, tmp_path: Path) -> None:
        """stats.json is written by StatsCollector after pipeline run."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        _write_json_input(
            input_dir / "20240301_count.json",
            [_make_article(1), _make_article(2)],
        )
        main(input_dir, output_dir)

        stats_path = output_dir / "stats.json"
        assert stats_path.exists()
        data = json.loads(stats_path.read_text())
        assert isinstance(data, list)
        assert len(data) > 0
