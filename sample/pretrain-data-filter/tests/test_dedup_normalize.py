"""Tests for dedup normalize — W-11a."""

from __future__ import annotations

import pytest
from dedup.normalize import normalize_for_dedup


@pytest.mark.parametrize(
    "input_text, expected",
    [
        # NFKC normalization (fullwidth → halfwidth)
        ("\uff21\uff22\uff23", "abc"),
        # Lowercase
        ("Hello World", "hello world"),
        # Strip
        ("  hello  ", "hello"),
        # Collapse whitespace
        ("hello   world\t\ttab", "hello world tab"),
        # Combined
        ("  \uff21\uff22  Hello   ", "ab hello"),
        # Empty
        ("", ""),
        # Whitespace only
        ("   \t  ", ""),
    ],
    ids=["nfkc", "lowercase", "strip", "collapse_ws", "combined", "empty", "whitespace_only"],
)
def test_normalize_for_dedup(input_text: str, expected: str) -> None:
    assert normalize_for_dedup(input_text) == expected
