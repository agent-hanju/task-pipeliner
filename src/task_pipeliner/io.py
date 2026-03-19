"""JSONL reader and writer for task-pipeliner."""

from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

import orjson

logger = logging.getLogger(__name__)


class JsonlReader:
    """Reads JSONL files from paths (files or directories)."""

    def __init__(self, paths: list[Path]) -> None:
        logger.debug("paths=%s", paths)
        self._raw_paths = paths
        self._resolved: list[Path] = self._resolve_paths()
        logger.info("Reader opened (%d file(s))", len(self._resolved))

    def _resolve_paths(self) -> list[Path]:
        """Expand directories to *.jsonl files; keep plain files as-is."""
        resolved: list[Path] = []
        for p in self._raw_paths:
            if p.is_dir():
                resolved.extend(sorted(p.glob("*.jsonl")))
            else:
                resolved.append(p)
        return resolved

    def read(self) -> Generator[dict, None, None]:  # type: ignore[type-arg]
        """Yield parsed dicts from all resolved JSONL files."""
        for path in self._resolved:
            logger.debug("reading %s", path)
            with open(path, "rb") as fh:
                for line in fh:
                    stripped = line.strip()
                    if stripped:
                        yield orjson.loads(stripped)


class JsonlWriter:
    """Writes kept / removed items to JSONL files in an output directory."""

    def __init__(self, output_dir: Path) -> None:
        logger.debug("output_dir=%s", output_dir)
        self._output_dir = output_dir
        self._kept_fh: BinaryIO | None = None
        self._removed_fh: BinaryIO | None = None

    def open(self) -> None:
        """Create output directory and open kept.jsonl / removed.jsonl for writing."""
        logger.debug("opening writer at %s", self._output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._kept_fh = open(self._output_dir / "kept.jsonl", "wb")  # noqa: SIM115
        self._removed_fh = open(self._output_dir / "removed.jsonl", "wb")  # noqa: SIM115
        logger.info("Writer opened (output=%s)", self._output_dir)

    def write_kept(self, item: dict) -> None:  # type: ignore[type-arg]
        """Write a kept item as a JSONL line."""
        assert self._kept_fh is not None
        self._kept_fh.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE))

    def write_removed(self, item: dict, reason: str) -> None:  # type: ignore[type-arg]
        """Write a removed item with reason as a JSONL line."""
        assert self._removed_fh is not None
        record = {"item": item, "reason": reason}
        self._removed_fh.write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))

    def close(self) -> None:
        """Close open file handles."""
        logger.debug("closing writer at %s", self._output_dir)
        if self._kept_fh is not None:
            self._kept_fh.close()
            self._kept_fh = None
        if self._removed_fh is not None:
            self._removed_fh.close()
            self._removed_fh = None
        logger.info("Writer closed (output=%s)", self._output_dir)

    def __enter__(self) -> JsonlWriter:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
