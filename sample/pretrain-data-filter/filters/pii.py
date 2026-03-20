"""PII (Personally Identifiable Information) filter — W-09.

Detects Korean phone numbers, RRN, BRN, and email addresses.
"""

from __future__ import annotations

import re

# Phone: 0XX-XXXX-XXXX or 0XX-XXX-XXXX
_PHONE_RE = re.compile(r"\b0\d{1,2}-\d{3,4}-\d{4}\b")

# RRN (주민등록번호): 6 digits - 7 digits starting with 1-4
_RRN_RE = re.compile(r"\b\d{6}-[1-4]\d{6}\b")

# BRN (사업자등록번호): 3-2-5 digits
_BRN_RE = re.compile(r"\b\d{3}-\d{2}-\d{5}\b")

# Email
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")

_PII_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (_PHONE_RE, "pii_detected_phone"),
    (_RRN_RE, "pii_detected_rrn"),
    (_BRN_RE, "pii_detected_brn"),
    (_EMAIL_RE, "pii_detected_email"),
]


def filter_pii(text: str) -> tuple[bool, str]:
    """Filter text containing PII.

    1. Phone (0XX-XXXX-XXXX) → (False, 'pii_detected_phone')
    2. RRN (6-7 digits) → (False, 'pii_detected_rrn')
    3. BRN (3-2-5 digits) → (False, 'pii_detected_brn')
    4. Email → (False, 'pii_detected_email')
    5. None matched → (True, '')
    """
    for pattern, reason in _PII_PATTERNS:
        if pattern.search(text):
            return (False, reason)
    return (True, "")
