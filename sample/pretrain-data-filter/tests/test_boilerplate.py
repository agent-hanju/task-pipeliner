"""Tests for boilerplate utility — W-14."""

from __future__ import annotations

from utils.boilerplate import BoilerplateDetector


def test_frequent_lines_removed() -> None:
    """Lines appearing >= min_line_freq times should be removed."""
    detector = BoilerplateDetector(min_line_freq=3, min_line_length=5)
    docs = ["반복되는 보일러플레이트 줄\n고유한 내용"] * 5
    detector.build_freq(docs)

    result = detector.clean("반복되는 보일러플레이트 줄\n이것은 고유한 줄입니다")
    assert result is not None
    assert "반복되는 보일러플레이트 줄" not in result
    assert "이것은 고유한 줄입니다" in result


def test_infrequent_lines_kept() -> None:
    """Lines below freq threshold should be kept."""
    detector = BoilerplateDetector(min_line_freq=100, min_line_length=5)
    docs = ["한 번만 나오는 줄"] * 3
    detector.build_freq(docs)

    result = detector.clean("한 번만 나오는 줄\n다른 내용")
    assert result is not None
    assert "한 번만 나오는 줄" in result


def test_empty_after_cleaning_returns_none() -> None:
    """If all content is boilerplate, return None."""
    detector = BoilerplateDetector(min_line_freq=3, min_line_length=5)
    docs = ["보일러플레이트"] * 10
    detector.build_freq(docs)

    result = detector.clean("보일러플레이트")
    assert result is None


def test_consecutive_blank_lines_limited() -> None:
    """Consecutive blank lines should be limited to max_blank_lines."""
    detector = BoilerplateDetector(min_line_freq=100, min_line_length=5, max_blank_lines=1)
    detector.build_freq([])

    text = "줄1\n\n\n\n줄2"
    result = detector.clean(text)
    assert result is not None
    # Should have at most 1 consecutive blank line
    assert "\n\n\n" not in result
