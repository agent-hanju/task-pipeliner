"""Tests for schema module — W-01."""

from __future__ import annotations

import pytest
from schema import SimpleTaxonomyDict, TaxonomyDict, detect_schema

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TAXONOMY_RECORD: TaxonomyDict = {
    "dataset_name": "test",
    "id": "1",
    "text": "hello",
    "language": "ko",
    "type": "news",
    "method": "crawl",
    "category": "경제",
    "industrial_field": "금융",
    "template": "",
    "metadata": {
        "source": "naver",
        "published_date": "2024-01-01",
        "collected_date": "2024-01-02",
        "token_len": 100,
        "quality_level": "high",
        "author": "tester",
    },
}

SIMPLE_RECORD: SimpleTaxonomyDict = {
    "id": "2",
    "text": "world",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "record, expected",
    [
        (TAXONOMY_RECORD, "taxonomy"),
        (SIMPLE_RECORD, "simple"),
        ({"id": "3", "text": "no metadata"}, "simple"),
        ({"id": "4", "text": "has meta", "metadata": {"source": "x"}}, "taxonomy"),
    ],
    ids=["taxonomy", "simple", "no_metadata", "has_metadata_key"],
)
def test_detect_schema(record: dict, expected: str) -> None:
    assert detect_schema(record) == expected


def test_taxonomy_dict_has_expected_keys() -> None:
    """TaxonomyDict should accept all specified keys."""
    record: TaxonomyDict = TAXONOMY_RECORD
    assert "metadata" in record
    assert "dataset_name" in record


def test_simple_dict_has_expected_keys() -> None:
    """SimpleTaxonomyDict needs only id and text."""
    record: SimpleTaxonomyDict = SIMPLE_RECORD
    assert "id" in record
    assert "text" in record
