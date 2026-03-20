"""Boilerplate line detector — W-14.

2-pass algorithm: build_freq() → clean().
Runs outside the pipeline as a pre-processing step.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable

logger = logging.getLogger(__name__)


class BoilerplateDetector:
    """2-pass boilerplate line remover.

    Pass 1 (build_freq): collect line frequencies across the corpus.
    Pass 2 (clean): remove high-frequency boilerplate lines.
    """

    def __init__(
        self,
        min_line_freq: int = 100,
        min_line_length: int = 10,
        max_blank_lines: int = 1,
    ) -> None:
        self.min_line_freq = min_line_freq
        self.min_line_length = min_line_length
        self.max_blank_lines = max_blank_lines
        self.line_counts: Counter[str] = Counter()

    def build_freq(self, docs: Iterable[str]) -> None:
        """1st pass: collect line frequencies from documents."""
        logger.debug(
            "min_line_freq=%d min_line_length=%d",
            self.min_line_freq,
            self.min_line_length,
        )
        for text in docs:
            for line in str(text).splitlines():
                stripped = line.strip()
                if len(stripped) >= self.min_line_length:
                    self.line_counts[stripped] += 1
        logger.info("freq built unique_lines=%d", len(self.line_counts))

    def clean(self, text: str) -> str | None:
        """2nd pass: remove high-frequency boilerplate lines.

        Returns cleaned text, or None if result is empty.
        """
        cleaned_lines: list[str] = []
        consecutive_blanks = 0

        for line in text.splitlines():
            stripped = line.strip()

            # Remove high-frequency lines
            if (
                len(stripped) >= self.min_line_length
                and self.line_counts.get(stripped, 0) >= self.min_line_freq
            ):
                continue

            # Limit consecutive blank lines
            if not stripped:
                consecutive_blanks += 1
                if consecutive_blanks > self.max_blank_lines:
                    continue
            else:
                consecutive_blanks = 0

            cleaned_lines.append(line)

        result = "\n".join(cleaned_lines).strip()
        logger.debug("cleaned len=%d", len(result))
        return result if result else None
