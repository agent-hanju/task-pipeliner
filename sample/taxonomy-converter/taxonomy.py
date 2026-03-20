"""Taxonomy schema definitions and result class."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Self, TypedDict

import orjson

from task_pipeliner import BaseResult

logger = logging.getLogger(__name__)


class TaxonomyMetadata(TypedDict):
    """Metadata fields for a taxonomy record."""

    source: str
    published_date: str | None
    collected_date: str
    token_len: int | None
    quality_level: str | None
    author: str | None


class TaxonomyDict(TypedDict):
    """Full taxonomy record schema."""

    dataset_name: str
    id: str
    text: str
    language: str
    type: str
    method: str
    category: str
    industrial_field: str
    template: str
    metadata: TaxonomyMetadata


class SimpleTaxonomyDict(TypedDict):
    """Minimal taxonomy record with id and text only."""

    id: str
    text: str


@dataclass
class TaxonomyResult(BaseResult):
    """Tracks conversion results: success/skipped/error counts."""

    success: int = 0
    skipped: int = 0
    errored: int = 0

    def merge(self, other: Self) -> Self:
        """Combine two results by summing all counters."""
        logger.debug(
            "self=(%d,%d,%d) other=(%d,%d,%d)",
            self.success,
            self.skipped,
            self.errored,
            other.success,
            other.skipped,
            other.errored,
        )
        return type(self)(
            success=self.success + other.success,
            skipped=self.skipped + other.skipped,
            errored=self.errored + other.errored,
        )

    def write(self, output_dir: Path) -> None:
        """Write count_summary.json to output_dir."""
        logger.debug("output_dir=%s", output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "count_summary.json"
        data = {
            "success": self.success,
            "skipped": self.skipped,
            "errored": self.errored,
            "total": self.success + self.skipped + self.errored,
        }
        path.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2))
        logger.info("count_summary.json written path=%s", path)
