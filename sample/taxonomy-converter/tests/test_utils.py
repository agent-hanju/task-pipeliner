"""Tests for utils module."""

from __future__ import annotations

import pytest
from utils import make_metadata, merge_title, parse_date


class TestMergeTitle:
    """merge_title — title/content merging with similarity check."""

    @pytest.mark.parametrize(
        ("title", "content", "expected"),
        [
            (
                "제목입니다",
                "제목입니다로 시작하는 본문입니다.",
                "제목입니다로 시작하는 본문입니다.",
            ),
            ("제목", "다른 시작의 본문", "제목\n\n다른 시작의 본문"),
            ("", "본문만 있음", "본문만 있음"),
            ("제목만 있음", "", "제목만 있음"),
            ("", "", ""),
        ],
        ids=["similar-start", "different-start", "empty-title", "empty-content", "both-empty"],
    )
    def test_merge_title(self, title: str, content: str, expected: str) -> None:
        assert merge_title(title, content) == expected

    def test_custom_threshold(self) -> None:
        # With very low threshold, even somewhat different texts merge
        result = merge_title("ABC", "ABD something", threshold=0.3)
        assert result == "ABD something"

    def test_exact_match_omits_title(self) -> None:
        result = merge_title("동일한 제목", "동일한 제목 그리고 본문 계속.")
        assert result == "동일한 제목 그리고 본문 계속."


class TestParseDate:
    """parse_date — date string parsing to ISO format."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("20240115", "2024-01-15"),
            ("20231231", "2023-12-31"),
            ("202401", "2024-01-01"),
            ("2024011", "2024-01-10"),
            (None, None),
            ("abc", None),
            ("12", None),
            ("file_20240301_data", "2024-03-01"),
        ],
        ids=["8digits", "year-end", "6digits", "7digits", "none", "alpha", "short", "embedded"],
    )
    def test_parse_date(self, raw: str | None, expected: str | None) -> None:
        assert parse_date(raw) == expected

    def test_invalid_date_returns_none(self) -> None:
        assert parse_date("20241399") is None


class TestMakeMetadata:
    """make_metadata — default values and kwargs override."""

    def test_defaults(self) -> None:
        meta = make_metadata("TestSource", "2024-01-15")
        assert meta["source"] == "TestSource"
        assert meta["published_date"] == "2024-01-15"
        assert meta["collected_date"]  # should have a default date
        assert meta["token_len"] is None
        assert meta["quality_level"] is None
        assert meta["author"] is None

    def test_kwargs_override(self) -> None:
        meta = make_metadata(
            "Src",
            None,
            collected_date="2024-06-01",
            token_len=500,
            quality_level="gold",
            author="홍길동",
        )
        assert meta["collected_date"] == "2024-06-01"
        assert meta["token_len"] == 500
        assert meta["quality_level"] == "gold"
        assert meta["author"] == "홍길동"
        assert meta["published_date"] is None
