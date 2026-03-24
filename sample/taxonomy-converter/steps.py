"""Pipeline Step classes: Loader, Preprocess, Convert, Deduplicate, Writer."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any, BinaryIO

import orjson
from loader import load_items
from naver_rules import apply_naver_line_inline, apply_naver_transforms, should_filter_line
from taxonomy import TaxonomyDict
from text_rules import normalize_text
from utils import make_metadata, merge_title, parse_date

from task_pipeliner import ParallelStep, SequentialStep, SourceStep, Worker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SOURCE
# ---------------------------------------------------------------------------


class LoaderStep(SourceStep):
    """SOURCE step that loads items from JSON/JSONL files."""

    outputs = ("main",)

    def __init__(self, paths: list[str] | None = None, **_kwargs: Any) -> None:
        self._paths = [Path(p) for p in paths] if paths else []

    def items(self) -> Generator[Any, None, None]:
        logger.info("loading from %d paths", len(self._paths))
        yield from load_items(self._paths)


# ---------------------------------------------------------------------------
# PARALLEL — Preprocess
# ---------------------------------------------------------------------------


class PreprocessWorker(Worker):
    """Normalizes text, applies Naver-specific transforms, validates result."""

    def __init__(self, min_lines: int, min_chars: int) -> None:
        self._min_lines = min_lines
        self._min_chars = min_chars

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        logger.debug("index=%s", item.get("index"))
        raw_text: str = item.get("text", "")

        # 1. Normalize
        text = normalize_text(raw_text)

        # 2. Document-level transforms
        text = apply_naver_transforms(text)

        # 3. Line filter
        lines = text.split("\n")
        lines = [line for line in lines if not should_filter_line(line)]

        # 4. Line inline transforms
        lines = [apply_naver_line_inline(line) for line in lines]

        # 5. Final normalization
        text = normalize_text("\n".join(lines)).strip()

        # 6. Validation
        non_empty = [line for line in text.split("\n") if line.strip()]
        if len(non_empty) < self._min_lines or len(text) < self._min_chars:
            logger.debug("skipped: lines=%d chars=%d", len(non_empty), len(text))
            item["_removed_reason"] = (
                f"too short: lines={len(non_empty)} (min {self._min_lines}), "
                f"chars={len(text)} (min {self._min_chars})"
            )
            emit(item, "removed")
            return

        # 7. Emit with cleaned text
        item["text"] = text
        emit(item, "kept")


class PreprocessStep(ParallelStep):
    """Text preprocessing step (PARALLEL)."""

    outputs = ("kept", "removed")

    def __init__(self, min_lines: int = 2, min_chars: int = 100, **_kwargs: Any) -> None:
        self._min_lines = min_lines
        self._min_chars = min_chars

    def create_worker(self) -> PreprocessWorker:
        return PreprocessWorker(self._min_lines, self._min_chars)


# ---------------------------------------------------------------------------
# PARALLEL — Convert
# ---------------------------------------------------------------------------


class ConvertWorker(Worker):
    """Builds TaxonomyDict from preprocessed item."""

    def __init__(
        self,
        dataset_name: str,
        source: str,
        language: str,
        category: str,
        industrial_field: str,
        template: str,
    ) -> None:
        self._dataset_name = dataset_name
        self._source = source
        self._language = language
        self._category = category
        self._industrial_field = industrial_field
        self._template = template

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        logger.debug("index=%s", item.get("index"))
        try:
            index = str(item["index"])
            date_prefix = item.get("_date_prefix", "00000000")
            title = (item.get("title") or "").strip()
            cleaned = item["text"]

            text = merge_title(title, cleaned)
            published_date = parse_date(item.get("date"))
            metadata = make_metadata(self._source, published_date)

            taxonomy_dict: TaxonomyDict = {
                "dataset_name": self._dataset_name,
                "id": f"HanaTI-NaverNews-{date_prefix}-{index}",
                "text": text,
                "language": self._language,
                "type": "public",
                "method": "document_parsing",
                "category": self._category,
                "industrial_field": self._industrial_field,
                "template": self._template,
                "metadata": metadata,
            }

            emit(taxonomy_dict, "kept")
        except Exception:
            logger.warning(
                "convert failed item=%s",
                repr(item)[:200],
                exc_info=True,
            )


class ConvertStep(ParallelStep):
    """Taxonomy schema conversion step (PARALLEL)."""

    outputs = ("kept",)

    def __init__(
        self,
        dataset_name: str = "naver_econ_news",
        source: str = "HanaTI/NaverNewsEconomy",
        language: str = "korean",
        category: str = "hass",
        industrial_field: str = "finance",
        template: str = "article",
        **_kwargs: Any,
    ) -> None:
        self._dataset_name = dataset_name
        self._source = source
        self._language = language
        self._category = category
        self._industrial_field = industrial_field
        self._template = template

    def create_worker(self) -> ConvertWorker:
        return ConvertWorker(
            self._dataset_name,
            self._source,
            self._language,
            self._category,
            self._industrial_field,
            self._template,
        )


# ---------------------------------------------------------------------------
# SEQUENTIAL — Deduplicate
# ---------------------------------------------------------------------------


class DeduplicateStep(SequentialStep):
    """Deduplication step (SEQUENTIAL).

    Deduplicates by id + text_hash. Same id + same text → skip.
    Same id + different text → emit with suffix.
    """

    outputs = ("kept",)

    def __init__(self) -> None:
        self._seen_ids: dict[str, int] = {}
        self._id_counters: dict[str, int] = {}

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        doc_id: str = item["id"]
        text_hash = hash(item["text"])
        logger.debug("doc_id=%s text_hash=%d", doc_id, text_hash)

        if doc_id not in self._seen_ids:
            # First occurrence
            self._seen_ids[doc_id] = text_hash
            emit(item, "kept")
            return

        if self._seen_ids[doc_id] == text_hash:
            # Exact duplicate — skip
            logger.debug("duplicate skipped doc_id=%s", doc_id)
            return

        # Same id, different text — assign suffix
        self._id_counters[doc_id] = self._id_counters.get(doc_id, 0) + 1
        suffix = self._id_counters[doc_id]
        item["id"] = f"{doc_id}-{suffix}"
        logger.debug("renamed doc_id=%s suffix=%d", doc_id, suffix)
        emit(item, "kept")


# ---------------------------------------------------------------------------
# SEQUENTIAL — Writer (terminal)
# ---------------------------------------------------------------------------


class WriterStep(SequentialStep):
    """JSONL file writer step (SEQUENTIAL, terminal).

    Receives items from both kept and removed paths (fan-in).
    Writes kept items to kept.jsonl and removed items to removed.jsonl.
    Distinguishes by presence of 'dataset_name' key (taxonomy schema).
    Does not emit — items terminate here.
    """

    def __init__(self, dir: str = "output", **_kwargs: Any) -> None:
        self._output_dir = Path(dir)
        self._kept_fh: BinaryIO | None = None
        self._removed_fh: BinaryIO | None = None

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        logger.debug("writing item id=%s", item.get("id"))
        self._output_dir.mkdir(parents=True, exist_ok=True)

        if "dataset_name" in item:
            # Kept item (taxonomy dict from deduplicate)
            if self._kept_fh is None:
                self._kept_fh = open(self._output_dir / "kept.jsonl", "wb")  # noqa: SIM115
                logger.info("kept writer opened output=%s", self._output_dir)
            self._kept_fh.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE))
        else:
            # Removed item (raw item from preprocess)
            if self._removed_fh is None:
                self._removed_fh = open(self._output_dir / "removed.jsonl", "wb")  # noqa: SIM115
                logger.info("removed writer opened output=%s", self._output_dir)
            self._removed_fh.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE))

    def close(self) -> None:
        """Close the output file handles."""
        if self._kept_fh is not None:
            self._kept_fh.close()
            self._kept_fh = None
        if self._removed_fh is not None:
            self._removed_fh.close()
            self._removed_fh = None
        logger.info("writer closed output=%s", self._output_dir)
