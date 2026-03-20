"""Tests for repetition filter — W-04."""

from __future__ import annotations

from filters.repetition import filter_repetition


def test_duplicate_lines() -> None:
    """High duplicate line fraction → dup_line_frac."""
    text = "\n".join(["동일한 줄입니다"] * 20)
    passed, reason = filter_repetition(text, max_dup_line_frac=0.3)
    assert passed is False
    assert reason == "dup_line_frac"


def test_duplicate_paragraphs() -> None:
    """High duplicate paragraph fraction → dup_para_frac.

    dup_line_frac checks are bypassed by setting max_dup_line_frac=1.0.
    """
    para = "이것은 문단입니다. 여러 줄로 구성됩니다."
    text = "\n\n".join([para] * 20)
    passed, reason = filter_repetition(text, max_dup_line_frac=1.0, max_dup_para_frac=0.3)
    assert passed is False
    assert reason == "dup_para_frac"


def test_high_top_2gram() -> None:
    """High frequency 2-gram → top_2gram_frac."""
    # Repeat same 2 words many times
    text = " ".join(["반복 단어"] * 200)
    passed, reason = filter_repetition(text, max_top_2gram_frac=0.20)
    assert passed is False
    assert reason == "top_2gram_frac"


def test_normal_text_passes() -> None:
    """Diverse text should pass all checks."""
    words = [f"단어{i}" for i in range(200)]
    text = " ".join(words)
    passed, reason = filter_repetition(text)
    assert passed is True
    assert reason == ""


def test_empty_text() -> None:
    """Empty text has no repetition issues."""
    passed, reason = filter_repetition("")
    assert passed is True
    assert reason == ""
