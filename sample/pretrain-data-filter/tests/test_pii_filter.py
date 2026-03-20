"""Tests for PII filter — W-09."""

from __future__ import annotations

import pytest
from filters.pii import filter_pii


@pytest.mark.parametrize(
    "text, expected_pass, expected_reason",
    [
        # Phone numbers
        ("연락처: 010-1234-5678 으로 연락주세요", False, "pii_detected_phone"),
        ("전화: 02-123-4567", False, "pii_detected_phone"),
        # RRN (주민등록번호)
        ("주민번호 900101-1234567 입니다", False, "pii_detected_rrn"),
        # BRN (사업자등록번호)
        ("사업자번호 123-45-67890", False, "pii_detected_brn"),
        # Email
        ("메일 주소: test@example.com 입니다", False, "pii_detected_email"),
        # Clean text
        ("이것은 깨끗한 텍스트입니다. PII가 없습니다.", True, ""),
    ],
    ids=["phone_mobile", "phone_landline", "rrn", "brn", "email", "clean"],
)
def test_filter_pii(text: str, expected_pass: bool, expected_reason: str) -> None:
    passed, reason = filter_pii(text)
    assert passed == expected_pass
    assert reason == expected_reason


def test_date_not_detected_as_rrn() -> None:
    """Date-like patterns should not trigger RRN detection."""
    text = "날짜: 2024-01-15 에 작성됨"
    passed, reason = filter_pii(text)
    assert passed is True


def test_no_pii_in_normal_article() -> None:
    """Normal Korean article text should pass."""
    text = (
        "한국은행은 기준금리를 동결했다. "
        "이번 결정은 경제 상황을 고려한 것이다. "
        "전문가들은 하반기 인하 가능성을 언급했다."
    )
    passed, reason = filter_pii(text)
    assert passed is True
