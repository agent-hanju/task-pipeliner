"""Pipeline steps — W-10, W-11b, W-11c, W-11d, W-11e, W-12.

All step classes are defined at module level (spawn-mode pickle compatible).
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any, BinaryIO, ClassVar

import orjson
from datasketch import MinHash, MinHashLSH
from dedup.normalize import normalize_for_dedup
from filters.length import filter_length
from filters.pii import filter_pii
from filters.repetition import filter_repetition

from task_pipeliner import ParallelStep, SequentialStep, SourceStep, Worker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SOURCE step — JSONL loader
# ---------------------------------------------------------------------------


class LoaderStep(SourceStep):
    """SOURCE step that reads JSONL files."""

    outputs: ClassVar[tuple[str, ...]] = ("main",)

    def __init__(self, paths: list[str] | None = None, **_kwargs: Any) -> None:
        self._paths = [Path(p) for p in paths] if paths else []

    def items(self) -> Generator[Any, None, None]:
        logger.info("loading from %d paths", len(self._paths))
        for p in self._paths:
            files = sorted(p.glob("*.jsonl")) if p.is_dir() else [p]
            for f in files:
                logger.debug("reading %s", f)
                with open(f, "rb") as fh:
                    for line in fh:
                        stripped = line.strip()
                        if stripped:
                            yield orjson.loads(stripped)


# ---------------------------------------------------------------------------
# Filter registry — maps config key → filter function
# ---------------------------------------------------------------------------

_FILTER_REGISTRY: dict[str, Callable[..., tuple[bool, str]]] = {
    "length": filter_length,
    "repetition": filter_repetition,
    "pii": filter_pii,
}


# ---------------------------------------------------------------------------
# W-10: QualityFilterStep (PARALLEL)
# ---------------------------------------------------------------------------


class QualityFilterWorker(Worker):
    """Applies quality filters to each item."""

    def __init__(
        self,
        text_key: str,
        filters: dict[str, dict[str, Any]],
    ) -> None:
        self._text_key = text_key
        self._filters = filters

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        logger.debug("text_key=%s filters=%s", self._text_key, list(self._filters))
        text = item.get(self._text_key, "")

        for filter_name, params in self._filters.items():
            filter_fn = _FILTER_REGISTRY.get(filter_name)
            if filter_fn is None:
                logger.warning("unknown filter %s, skipping", filter_name)
                continue

            passed, reason = filter_fn(text, **params) if params else filter_fn(text)
            if not passed:
                full_reason = f"{filter_name}/{reason}"
                item["_removed_reason"] = full_reason
                emit(item, "removed")
                logger.debug("removed reason=%s id=%s", full_reason, item.get("id"))
                return

        emit(item, "kept")
        logger.debug("kept id=%s", item.get("id"))


class QualityFilterStep(ParallelStep):
    """PARALLEL step — applies quality filters to each item."""

    outputs: ClassVar[tuple[str, ...]] = ("kept", "removed")

    def __init__(
        self,
        text_key: str = "text",
        filters: dict[str, dict[str, Any]] | None = None,
        **_kwargs: Any,
    ) -> None:
        self._text_key = text_key
        self._filters = (
            filters
            if filters is not None
            else {
                "length": {},
                "repetition": {},
                "pii": {},
            }
        )

    def create_worker(self) -> QualityFilterWorker:
        return QualityFilterWorker(self._text_key, self._filters)


# ---------------------------------------------------------------------------
# W-11b: HashComputeStep (PARALLEL)
# ---------------------------------------------------------------------------


class HashComputeWorker(Worker):
    """Pre-computes SHA hash for exact dedup."""

    def __init__(self, text_key: str, hash_algo: str) -> None:
        self._text_key = text_key
        self._hash_algo = hash_algo

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        logger.debug("hash_algo=%s", self._hash_algo)
        text = item.get(self._text_key, "")
        normalized = normalize_for_dedup(text)
        hash_value = hashlib.new(self._hash_algo, normalized.encode("utf-8")).hexdigest()
        item["_dedup_hash"] = hash_value
        emit(item, "main")


class HashComputeStep(ParallelStep):
    """PARALLEL step — pre-computes SHA hash for exact dedup."""

    outputs: ClassVar[tuple[str, ...]] = ("main",)

    def __init__(
        self,
        text_key: str = "text",
        hash_algo: str = "sha256",
        **_kwargs: Any,
    ) -> None:
        self._text_key = text_key
        self._hash_algo = hash_algo

    def create_worker(self) -> HashComputeWorker:
        return HashComputeWorker(self._text_key, self._hash_algo)


# ---------------------------------------------------------------------------
# W-11c: HashLookupStep (SEQUENTIAL)
# ---------------------------------------------------------------------------


class HashLookupStep(SequentialStep):
    """SEQUENTIAL step — hash set lookup/insert (stateful)."""

    outputs: ClassVar[tuple[str, ...]] = ("kept", "removed")

    @property
    def initial_state(self) -> set[str]:
        return set()

    def process(
        self, item: Any, state: set[str], emit: Callable[[Any, str], None]
    ) -> None:
        hash_value: str = item["_dedup_hash"]
        logger.debug("hash=%s", hash_value[:16])

        if hash_value in state:
            item["_removed_reason"] = "dedup/exact"
            emit(item, "removed")
            return

        state.add(hash_value)
        del item["_dedup_hash"]
        emit(item, "kept")


# ---------------------------------------------------------------------------
# W-11d: MinHashComputeStep (PARALLEL)
# ---------------------------------------------------------------------------


class MinHashComputeWorker(Worker):
    """Pre-computes MinHash signatures."""

    def __init__(self, text_key: str, num_perm: int, ngram_size: int) -> None:
        self._text_key = text_key
        self._num_perm = num_perm
        self._ngram_size = ngram_size

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        logger.debug("num_perm=%d ngram_size=%d", self._num_perm, self._ngram_size)
        text = item.get(self._text_key, "")
        normalized = normalize_for_dedup(text)
        words = normalized.split()

        mh = MinHash(num_perm=self._num_perm)
        if len(words) >= self._ngram_size:
            ngrams = [
                " ".join(words[i : i + self._ngram_size])
                for i in range(len(words) - self._ngram_size + 1)
            ]
        elif words:
            ngrams = [" ".join(words)]
        else:
            ngrams = []

        for ng in ngrams:
            mh.update(ng.encode("utf-8"))

        item["_minhash"] = mh
        emit(item, "main")


class MinHashComputeStep(ParallelStep):
    """PARALLEL step — pre-computes MinHash signatures."""

    outputs: ClassVar[tuple[str, ...]] = ("main",)

    def __init__(
        self,
        text_key: str = "text",
        num_perm: int = 128,
        ngram_size: int = 5,
        **_kwargs: Any,
    ) -> None:
        self._text_key = text_key
        self._num_perm = num_perm
        self._ngram_size = ngram_size

    def create_worker(self) -> MinHashComputeWorker:
        return MinHashComputeWorker(self._text_key, self._num_perm, self._ngram_size)


# ---------------------------------------------------------------------------
# W-11e: MinHashLookupStep (SEQUENTIAL)
# ---------------------------------------------------------------------------


class MinHashLookupStep(SequentialStep):
    """SEQUENTIAL step — LSH index lookup/insert (stateful)."""

    outputs: ClassVar[tuple[str, ...]] = ("kept", "removed")

    def __init__(
        self,
        threshold: float = 0.8,
        num_perm: int = 128,
        **_kwargs: Any,
    ) -> None:
        self._threshold = threshold
        self._num_perm = num_perm
        self._lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self._counter = 0

    def process(
        self,
        item: Any,
        state: Any,
        emit: Callable[[Any, str], None],
    ) -> None:
        mh: MinHash = item["_minhash"]
        logger.debug("counter=%d", self._counter)

        matches = self._lsh.query(mh)
        if matches:
            item["_removed_reason"] = "dedup/minhash"
            del item["_minhash"]
            emit(item, "removed")
            return

        self._counter += 1
        self._lsh.insert(str(self._counter), mh)
        del item["_minhash"]
        emit(item, "kept")


# ---------------------------------------------------------------------------
# W-12: WriterStep (SEQUENTIAL, terminal)
# ---------------------------------------------------------------------------


class WriterStep(SequentialStep):
    """SEQUENTIAL terminal step — writes kept/removed JSONL files."""

    outputs: ClassVar[tuple[str, ...]] = ()

    def __init__(self, output_dir: str = "output", **_kwargs: Any) -> None:
        self._output_dir = Path(output_dir)
        self._kept_fh: BinaryIO | None = None
        self._removed_fh: BinaryIO | None = None

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        self._output_dir.mkdir(parents=True, exist_ok=True)

        if "_removed_reason" in item:
            if self._removed_fh is None:
                self._removed_fh = open(self._output_dir / "removed.jsonl", "wb")
                logger.info("removed writer opened output=%s", self._output_dir)
            line = json.dumps(item, ensure_ascii=False) + "\n"
            self._removed_fh.write(line.encode("utf-8"))
        else:
            if self._kept_fh is None:
                self._kept_fh = open(self._output_dir / "kept.jsonl", "wb")
                logger.info("kept writer opened output=%s", self._output_dir)
            line = json.dumps(item, ensure_ascii=False) + "\n"
            self._kept_fh.write(line.encode("utf-8"))

    def close(self) -> None:
        """Release file handles."""
        if self._kept_fh is not None:
            self._kept_fh.close()
            self._kept_fh = None
        if self._removed_fh is not None:
            self._removed_fh.close()
            self._removed_fh = None
        logger.info("writer closed output=%s", self._output_dir)
