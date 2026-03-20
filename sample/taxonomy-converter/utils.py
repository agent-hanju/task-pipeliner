"""Common utility functions: merge_title, parse_date, make_metadata."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from difflib import SequenceMatcher

from taxonomy import TaxonomyMetadata

logger = logging.getLogger(__name__)

_DIGITS_RE = re.compile(r"\d{6,8}")


def merge_title(title: str, content: str, threshold: float = 0.8) -> str:
    """Merge title and content. Omit title if content starts similarly."""
    logger.debug("title_len=%d content_len=%d threshold=%.2f", len(title), len(content), threshold)
    if not title:
        return content
    if not content:
        return title
    head = content[: len(title)]
    ratio = SequenceMatcher(None, title, head).ratio()
    logger.debug("similarity ratio=%.4f", ratio)
    if ratio >= threshold:
        return content
    return f"{title}\n\n{content}"


def parse_date(raw: str | None) -> str | None:
    """Convert YYYYMMDD (6-8 digits) to YYYY-MM-DD ISO format. None on failure."""
    logger.debug("raw=%s", raw)
    if raw is None:
        return None
    match = _DIGITS_RE.search(raw)
    if not match:
        return None
    digits = match.group()
    # Pad to 8 digits if 6 or 7
    if len(digits) == 6:
        digits = digits + "01"
    elif len(digits) == 7:
        digits = digits + "0"
    try:
        parsed = datetime.strptime(digits[:8], "%Y%m%d")
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        return None


def make_metadata(
    source: str,
    published_date: str | None,
    collected_date: str | None = None,
    token_len: int | None = None,
    quality_level: str | None = None,
    author: str | None = None,
) -> TaxonomyMetadata:
    """Create TaxonomyMetadata with defaults filled in."""
    logger.debug("source=%s published_date=%s", source, published_date)
    return {
        "source": source,
        "published_date": published_date,
        "collected_date": collected_date or date.today().isoformat(),
        "token_len": token_len,
        "quality_level": quality_level,
        "author": author,
    }
