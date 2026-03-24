"""Tests for pipeline step classes."""

from __future__ import annotations

import copy

import pytest
from dummy_data import (
    SAMPLE_DIRTY_ITEM,
    SAMPLE_DUPLICATE_ITEMS,
    SAMPLE_NAVER_ITEM,
    SAMPLE_SAME_ID_DIFF_TEXT,
    SAMPLE_SHORT_ITEM,
)
from steps import ConvertStep, DeduplicateStep, PreprocessStep


class _Collector:
    """Captures items emitted by step.process()."""

    def __init__(self) -> None:
        self.items: list[tuple[dict, str]] = []

    def __call__(self, item: dict, tag: str) -> None:
        self.items.append((item, tag))


# ---------------------------------------------------------------------------
# PreprocessStep
# ---------------------------------------------------------------------------


class TestPreprocessStep:
    """PreprocessStep — text preprocessing and validation."""

    @pytest.mark.timeout(15)
    def test_normal_article_emitted(self) -> None:
        worker = PreprocessStep().create_worker()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_NAVER_ITEM)
        worker.process(item, None, collector)
        assert len(collector.items) == 1
        assert collector.items[0][1] == "kept"

    @pytest.mark.timeout(15)
    def test_short_article_skipped(self) -> None:
        worker = PreprocessStep().create_worker()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_SHORT_ITEM)
        worker.process(item, None, collector)
        assert len(collector.items) == 1
        assert collector.items[0][1] == "removed"

    @pytest.mark.timeout(15)
    def test_dirty_article_lines_filtered(self) -> None:
        worker = PreprocessStep().create_worker()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_DIRTY_ITEM)
        worker.process(item, None, collector)
        assert len(collector.items) == 1
        emitted_item, emitted_tag = collector.items[0]
        assert emitted_tag == "kept"
        emitted_text: str = emitted_item["text"]
        # Copyright, reporter email, and related-article lines removed
        assert "ⓒ뉴스1" not in emitted_text
        assert "reporter@news1.com" not in emitted_text
        assert "▶ 관련기사" not in emitted_text

    @pytest.mark.timeout(15)
    def test_result_counts(self) -> None:
        worker = PreprocessStep().create_worker()
        c1, c2 = _Collector(), _Collector()
        worker.process(copy.deepcopy(SAMPLE_NAVER_ITEM), None, c1)
        worker.process(copy.deepcopy(SAMPLE_SHORT_ITEM), None, c2)
        assert c1.items[0][1] == "kept"
        assert c2.items[0][1] == "removed"


# ---------------------------------------------------------------------------
# ConvertStep
# ---------------------------------------------------------------------------


class TestConvertStep:
    """ConvertStep — taxonomy schema conversion."""

    @pytest.mark.timeout(15)
    def test_normal_conversion(self) -> None:
        worker = ConvertStep().create_worker()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_NAVER_ITEM)
        worker.process(item, None, collector)
        assert len(collector.items) == 1
        taxonomy, tag = collector.items[0]
        assert tag == "kept"
        assert taxonomy["dataset_name"] == "naver_econ_news"
        assert taxonomy["language"] == "korean"
        assert taxonomy["type"] == "public"
        assert taxonomy["method"] == "document_parsing"
        assert taxonomy["category"] == "hass"
        assert taxonomy["industrial_field"] == "finance"
        assert taxonomy["template"] == "article"

    @pytest.mark.timeout(15)
    def test_id_format(self) -> None:
        worker = ConvertStep().create_worker()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_NAVER_ITEM)
        worker.process(item, None, collector)
        taxonomy, tag = collector.items[0]
        assert tag == "kept"
        assert taxonomy["id"] == "HanaTI-NaverNews-20240115-1"

    @pytest.mark.timeout(15)
    def test_title_omitted_when_similar(self) -> None:
        worker = ConvertStep().create_worker()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_NAVER_ITEM)
        # Make content start with title
        item["text"] = item["title"] + " 관련 상세 내용이 이어집니다."
        worker.process(item, None, collector)
        taxonomy, tag = collector.items[0]
        assert tag == "kept"
        # Title should be omitted since content starts similarly
        assert not taxonomy["text"].startswith(item["title"] + "\n\n")

    @pytest.mark.timeout(15)
    def test_metadata_fields(self) -> None:
        worker = ConvertStep().create_worker()
        collector = _Collector()
        item = copy.deepcopy(SAMPLE_NAVER_ITEM)
        worker.process(item, None, collector)
        taxonomy, tag = collector.items[0]
        assert tag == "kept"
        assert taxonomy["metadata"]["source"] == "HanaTI/NaverNewsEconomy"
        assert taxonomy["metadata"]["published_date"] == "2024-01-15"

    @pytest.mark.timeout(15)
    def test_missing_index_errors(self) -> None:
        worker = ConvertStep().create_worker()
        collector = _Collector()
        item = {"text": "some text", "_date_prefix": "20240101"}
        worker.process(item, None, collector)
        # Error is logged but item is silently dropped (no emit)
        assert len(collector.items) == 0


# ---------------------------------------------------------------------------
# DeduplicateStep
# ---------------------------------------------------------------------------


class TestDeduplicateStep:
    """DeduplicateStep — deduplication by id + text hash."""

    @pytest.mark.timeout(15)
    def test_unique_items_all_emitted(self) -> None:
        step = DeduplicateStep()
        collector = _Collector()
        items = [{"id": f"id-{i}", "text": f"unique text {i}"} for i in range(5)]
        for item in items:
            step.process(item, None, collector)
        assert len(collector.items) == 5

    @pytest.mark.timeout(15)
    def test_exact_duplicate_skipped(self) -> None:
        step = DeduplicateStep()
        collector = _Collector()
        for item in SAMPLE_DUPLICATE_ITEMS:
            step.process(copy.deepcopy(item), None, collector)
        assert len(collector.items) == 1

    @pytest.mark.timeout(15)
    def test_same_id_diff_text_suffixed(self) -> None:
        step = DeduplicateStep()
        collector = _Collector()
        for item in SAMPLE_SAME_ID_DIFF_TEXT:
            step.process(copy.deepcopy(item), None, collector)
        assert len(collector.items) == 2
        assert collector.items[0][0]["id"] == "HanaTI-NaverNews-20240115-1"
        assert collector.items[0][1] == "kept"
        assert collector.items[1][0]["id"] == "HanaTI-NaverNews-20240115-1-1"
        assert collector.items[1][1] == "kept"

    @pytest.mark.timeout(15)
    @pytest.mark.parametrize("n", [0, 1, 5])
    def test_duplicate_counts(self, n: int) -> None:
        step = DeduplicateStep()
        collector = _Collector()
        base_item = {"id": "test-id", "text": "same content"}
        for i in range(1 + n):
            step.process(copy.deepcopy(base_item), None, collector)
        # Only first unique item emitted; n duplicates skipped
        assert len(collector.items) == 1
