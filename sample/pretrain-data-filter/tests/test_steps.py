"""Tests for pipeline steps — W-10, W-11b, W-11c, W-11d, W-11e, W-12."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tests.dummy_data import (
    SAMPLE_DUPLICATE_ITEMS,
    SAMPLE_NORMAL_ITEM,
    SAMPLE_PII_ITEM,
    SAMPLE_SHORT_ITEM,
)


class _Collector:
    """Captures emitted items for assertion."""

    def __init__(self) -> None:
        self.items: list[tuple[dict, str]] = []

    def __call__(self, item: dict, tag: str) -> None:
        self.items.append((item, tag))


# ---------------------------------------------------------------------------
# W-10: QualityFilterStep
# ---------------------------------------------------------------------------


class TestQualityFilterStep:
    def test_normal_item_kept(self) -> None:
        from steps import QualityFilterStep

        step = QualityFilterStep()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_NORMAL_ITEM)
        result = step.process(item, None, collector)
        assert len(collector.items) == 1
        assert collector.items[0][1] == "kept"
        assert result.kept == 1

    def test_short_text_removed(self) -> None:
        from steps import QualityFilterStep

        step = QualityFilterStep()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_SHORT_ITEM)
        result = step.process(item, None, collector)
        assert len(collector.items) == 1
        assert collector.items[0][1] == "removed"
        assert result.removed == 1
        assert "length" in list(result.removed_reasons.keys())[0]

    def test_pii_removed(self) -> None:
        from steps import QualityFilterStep

        step = QualityFilterStep()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_PII_ITEM)
        result = step.process(item, None, collector)
        assert len(collector.items) == 1
        assert collector.items[0][1] == "removed"
        assert result.removed == 1
        assert "pii" in list(result.removed_reasons.keys())[0]

    def test_filter_disabled(self) -> None:
        """Disabled filter should be skipped."""
        from steps import QualityFilterStep

        step = QualityFilterStep(filters={"length": {}, "pii": {}})
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_SHORT_ITEM)
        step.process(item, None, collector)
        # length filter still active, should remove
        assert collector.items[0][1] == "removed"


# ---------------------------------------------------------------------------
# W-11b: HashComputeStep
# ---------------------------------------------------------------------------


class TestHashComputeStep:
    def test_adds_hash_field(self) -> None:
        from steps import HashComputeStep

        step = HashComputeStep()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_NORMAL_ITEM)
        step.process(item, None, collector)
        assert len(collector.items) == 1
        emitted, tag = collector.items[0]
        assert tag == "main"
        assert "_dedup_hash" in emitted

    def test_same_text_same_hash(self) -> None:
        from steps import HashComputeStep

        step = HashComputeStep()
        c1, c2 = _Collector(), _Collector()
        step.process(copy.deepcopy(SAMPLE_DUPLICATE_ITEMS[0]), None, c1)
        step.process(copy.deepcopy(SAMPLE_DUPLICATE_ITEMS[1]), None, c2)
        assert c1.items[0][0]["_dedup_hash"] == c2.items[0][0]["_dedup_hash"]

    def test_normalization_produces_same_hash(self) -> None:
        """Different casing/whitespace → same hash after normalization."""
        from steps import HashComputeStep

        step = HashComputeStep()
        c1, c2 = _Collector(), _Collector()
        step.process({"id": "a", "text": "Hello  World"}, None, c1)
        step.process({"id": "b", "text": "hello world"}, None, c2)
        assert c1.items[0][0]["_dedup_hash"] == c2.items[0][0]["_dedup_hash"]

    def test_different_text_different_hash(self) -> None:
        from steps import HashComputeStep

        step = HashComputeStep()
        c1, c2 = _Collector(), _Collector()
        step.process({"id": "a", "text": "hello"}, None, c1)
        step.process({"id": "b", "text": "world"}, None, c2)
        assert c1.items[0][0]["_dedup_hash"] != c2.items[0][0]["_dedup_hash"]


# ---------------------------------------------------------------------------
# W-11c: HashLookupStep
# ---------------------------------------------------------------------------


class TestHashLookupStep:
    @pytest.mark.timeout(15)
    def test_duplicate_hash_removed(self) -> None:
        from steps import HashLookupStep

        step = HashLookupStep()
        c1, c2 = _Collector(), _Collector()
        item1 = {"id": "a", "text": "hello", "_dedup_hash": "abc123"}
        item2 = {"id": "b", "text": "world", "_dedup_hash": "abc123"}
        r1 = step.process(item1, None, c1)
        r2 = step.process(item2, None, c2)
        assert c1.items[0][1] == "kept"
        assert r1.kept == 1
        assert c2.items[0][1] == "removed"
        assert r2.removed == 1

    @pytest.mark.timeout(15)
    def test_different_hash_all_kept(self) -> None:
        from steps import HashLookupStep

        step = HashLookupStep()
        c1, c2 = _Collector(), _Collector()
        step.process({"id": "a", "text": "x", "_dedup_hash": "hash1"}, None, c1)
        step.process({"id": "b", "text": "y", "_dedup_hash": "hash2"}, None, c2)
        assert c1.items[0][1] == "kept"
        assert c2.items[0][1] == "kept"

    @pytest.mark.timeout(15)
    def test_hash_field_removed_from_kept(self) -> None:
        from steps import HashLookupStep

        step = HashLookupStep()
        collector = _Collector()
        step.process({"id": "a", "text": "x", "_dedup_hash": "h1"}, None, collector)
        assert "_dedup_hash" not in collector.items[0][0]


# ---------------------------------------------------------------------------
# W-11d: MinHashComputeStep
# ---------------------------------------------------------------------------


class TestMinHashComputeStep:
    def test_adds_minhash_field(self) -> None:
        from steps import MinHashComputeStep

        step = MinHashComputeStep()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_NORMAL_ITEM)
        step.process(item, None, collector)
        assert len(collector.items) == 1
        assert "_minhash" in collector.items[0][0]
        assert collector.items[0][1] == "main"

    def test_same_text_same_signature(self) -> None:
        from steps import MinHashComputeStep

        step = MinHashComputeStep()
        c1, c2 = _Collector(), _Collector()
        step.process(copy.deepcopy(SAMPLE_DUPLICATE_ITEMS[0]), None, c1)
        step.process(copy.deepcopy(SAMPLE_DUPLICATE_ITEMS[1]), None, c2)
        mh1 = c1.items[0][0]["_minhash"]
        mh2 = c2.items[0][0]["_minhash"]
        assert mh1.jaccard(mh2) == 1.0

    def test_similar_text_high_jaccard(self) -> None:
        from steps import MinHashComputeStep

        step = MinHashComputeStep()
        c1, c2 = _Collector(), _Collector()
        text1 = " ".join([f"word{i}" for i in range(100)])
        text2 = " ".join([f"word{i}" for i in range(100)] + ["extra"])
        step.process({"id": "a", "text": text1}, None, c1)
        step.process({"id": "b", "text": text2}, None, c2)
        mh1 = c1.items[0][0]["_minhash"]
        mh2 = c2.items[0][0]["_minhash"]
        assert mh1.jaccard(mh2) > 0.5

    def test_short_text_handled(self) -> None:
        """Text with fewer words than ngram_size should still work."""
        from steps import MinHashComputeStep

        step = MinHashComputeStep(ngram_size=5)
        collector = _Collector()
        step.process({"id": "a", "text": "짧은 글"}, None, collector)
        assert len(collector.items) == 1
        assert "_minhash" in collector.items[0][0]


# ---------------------------------------------------------------------------
# W-11e: MinHashLookupStep
# ---------------------------------------------------------------------------


class TestMinHashLookupStep:
    @pytest.mark.timeout(15)
    def test_duplicate_minhash_removed(self) -> None:
        from datasketch import MinHash
        from steps import MinHashLookupStep

        step = MinHashLookupStep()
        mh = MinHash(num_perm=128)
        mh.update(b"hello world")

        c1, c2 = _Collector(), _Collector()
        item1 = {"id": "a", "text": "x", "_minhash": mh}
        item2 = {"id": "b", "text": "y", "_minhash": mh}
        r1 = step.process(item1, None, c1)
        r2 = step.process(item2, None, c2)
        assert c1.items[0][1] == "kept"
        assert r1.kept == 1
        assert c2.items[0][1] == "removed"
        assert r2.removed == 1

    @pytest.mark.timeout(15)
    def test_different_minhash_all_kept(self) -> None:
        from datasketch import MinHash
        from steps import MinHashLookupStep

        step = MinHashLookupStep()
        mh1 = MinHash(num_perm=128)
        mh1.update(b"completely different text one")
        mh2 = MinHash(num_perm=128)
        mh2.update(b"another totally separate text")

        c1, c2 = _Collector(), _Collector()
        step.process({"id": "a", "text": "x", "_minhash": mh1}, None, c1)
        step.process({"id": "b", "text": "y", "_minhash": mh2}, None, c2)
        assert c1.items[0][1] == "kept"
        assert c2.items[0][1] == "kept"

    @pytest.mark.timeout(15)
    def test_minhash_field_removed_from_kept(self) -> None:
        from datasketch import MinHash
        from steps import MinHashLookupStep

        step = MinHashLookupStep()
        mh = MinHash(num_perm=128)
        mh.update(b"test")
        collector = _Collector()
        step.process({"id": "a", "text": "x", "_minhash": mh}, None, collector)
        assert "_minhash" not in collector.items[0][0]


# ---------------------------------------------------------------------------
# W-12: WriterStep
# ---------------------------------------------------------------------------


class TestWriterStep:
    def test_kept_item_written(self, tmp_path: Path) -> None:
        from steps import WriterStep

        step = WriterStep(output_dir=str(tmp_path))
        collector = _Collector()
        item = {"id": "a", "text": "hello"}
        step.process(item, None, collector)
        step.close()

        kept_path = tmp_path / "kept.jsonl"
        assert kept_path.exists()
        lines = kept_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        assert json.loads(lines[0])["id"] == "a"

    def test_removed_item_written(self, tmp_path: Path) -> None:
        from steps import WriterStep

        step = WriterStep(output_dir=str(tmp_path))
        collector = _Collector()
        item = {"id": "b", "text": "world", "_removed_reason": "length/empty"}
        step.process(item, None, collector)
        step.close()

        removed_path = tmp_path / "removed.jsonl"
        assert removed_path.exists()
        data = json.loads(removed_path.read_text(encoding="utf-8").strip().split("\n")[0])
        assert data["_removed_reason"] == "length/empty"

    def test_close_cleans_handles(self, tmp_path: Path) -> None:
        from steps import WriterStep

        step = WriterStep(output_dir=str(tmp_path))
        step.process({"id": "a", "text": "x"}, None, _Collector())
        step.close()
        # Calling close again should not error
        step.close()
