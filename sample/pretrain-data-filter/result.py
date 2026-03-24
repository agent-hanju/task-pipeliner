"""FilterResult — pipeline result aggregation — W-02."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Self

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    """Pipeline result aggregation.

    Fields: kept, removed, removed_reasons (dict[str, int]).
    """

    kept: int = 0
    removed: int = 0
    removed_reasons: dict[str, int] = field(default_factory=dict)

    def merge(self, other: Self) -> Self:
        """Merge two results: sum counters, merge reason dicts."""
        merged_reasons = dict(self.removed_reasons)
        for reason, count in other.removed_reasons.items():
            merged_reasons[reason] = merged_reasons.get(reason, 0) + count
        return type(self)(
            kept=self.kept + other.kept,
            removed=self.removed + other.removed,
            removed_reasons=merged_reasons,
        )

    def write(self, output_dir: Path, step_name: str = "") -> None:
        """Write stats.json to output_dir."""
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "stats.json"
        data = {
            "kept": self.kept,
            "removed": self.removed,
            "total": self.kept + self.removed,
            "removed_reasons": self.removed_reasons,
        }
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("stats.json written path=%s", path)
