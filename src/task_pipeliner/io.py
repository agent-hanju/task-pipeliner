"""JSONL reader, writer, and SOURCE step for task-pipeliner."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, Self

import orjson

from task_pipeliner.base import BaseResult, BaseStep, StepType

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
    """Writes items to a single JSONL file."""

    def __init__(self, path: Path) -> None:
        logger.debug("path=%s", path)
        self._path = path
        self._fh: BinaryIO | None = None

    def open(self) -> None:
        """Create parent directory and open the file for writing."""
        logger.debug("opening writer at %s", self._path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._path, "wb")  # noqa: SIM115
        logger.info("Writer opened (output=%s)", self._path)

    def write(self, item: dict) -> None:  # type: ignore[type-arg]
        """Write a single item as a JSONL line."""
        assert self._fh is not None
        self._fh.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE))

    def close(self) -> None:
        """Close the file handle."""
        logger.debug("closing writer at %s", self._path)
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        logger.info("Writer closed (output=%s)", self._path)

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


# ---------------------------------------------------------------------------
# JSONL SOURCE step
# ---------------------------------------------------------------------------


class _NullResult(BaseResult):
    """Minimal no-op result for JsonlSourceStep."""

    def merge(self, other: Self) -> Self:
        return self

    def write(self, output_dir: Path) -> None:
        pass


class JsonlSourceStep(BaseStep[_NullResult]):
    """SOURCE step that reads JSONL files via JsonlReader."""

    def __init__(self, paths: list[str] | None = None) -> None:
        self._paths = [Path(p) for p in paths] if paths else []

    @property
    def step_type(self) -> StepType:
        return StepType.SOURCE

    def items(self) -> Generator[Any, None, None]:
        reader = JsonlReader(self._paths)
        yield from reader.read()

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> _NullResult:
        raise NotImplementedError("SOURCE step does not process items")

    @property
    def name(self) -> str:
        return "JsonlSourceStep"
