"""Dedup text normalization — W-11a."""

from __future__ import annotations

import re
import unicodedata

_WS_RE = re.compile(r"\s+")


def normalize_for_dedup(text: str) -> str:
    """Normalize text for dedup comparison.

    1. NFKC unicode normalization
    2. Lowercase
    3. Strip
    4. Collapse consecutive whitespace → single space
    """
    text = unicodedata.normalize("NFKC", text)
    text = text.lower()
    text = text.strip()
    text = _WS_RE.sub(" ", text)
    return text
