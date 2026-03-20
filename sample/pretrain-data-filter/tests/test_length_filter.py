"""Tests for length filter — W-03."""

from __future__ import annotations

import pytest
from filters.length import filter_length


@pytest.mark.parametrize(
    "text, expected_pass, expected_reason",
    [
        ("", False, "empty"),
        ("   ", False, "empty"),
        # short chars (< 200)
        ("짧은 텍스트", False, "too_few_chars"),
        # few words (< 50) but enough chars
        ("가" * 250, False, "too_few_words"),
        # many words (> 100_000)
        (" ".join(["단어"] * 100_001), False, "too_many_words"),
        # mean word length too short
        (" ".join(["a"] * 300), False, "mean_word_too_short"),
        # pass case — 200+ chars, 50+ words, reasonable mean word length
        (" ".join(["안녕하세요"] * 60), True, ""),
    ],
    ids=[
        "empty",
        "whitespace_only",
        "too_few_chars",
        "too_few_words",
        "too_many_words",
        "mean_word_too_short",
        "pass",
    ],
)
def test_filter_length(text: str, expected_pass: bool, expected_reason: str) -> None:
    passed, reason = filter_length(text)
    assert passed == expected_pass
    assert reason == expected_reason


def test_boundary_min_chars_exact() -> None:
    """Exactly 200 chars should pass the min_chars check."""
    # 200 chars exactly, with enough words
    text = " ".join(["abcde"] * 50)  # 50 words * 5 chars + 49 spaces = 299 chars
    passed, reason = filter_length(text, min_chars=200, min_words=50)
    assert passed is True


def test_boundary_min_chars_just_below() -> None:
    """199 chars should fail."""
    text = "a" * 199
    passed, reason = filter_length(text, min_chars=200)
    assert passed is False
    assert reason == "too_few_chars"
