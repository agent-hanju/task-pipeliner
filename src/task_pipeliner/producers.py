"""Producer classes — execute steps, manage queues, collect results."""

from __future__ import annotations

import atexit
import logging
import multiprocessing
import pickle
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from multiprocessing.synchronize import Event
from typing import Any

from task_pipeliner.base import ParallelStep, SequentialStep, SourceStep, StepBase
from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sentinels
# ---------------------------------------------------------------------------


class Sentinel:
    """Marker placed into a queue to signal 'no more items'."""


class ErrorSentinel(Sentinel):
    """Sentinel that also carries exception info."""

    def __init__(self, *, exc: BaseException, step_name: str) -> None:
        self.exc = exc
        self.step_name = step_name


def is_sentinel(obj: object) -> bool:
    """Return True if *obj* is any kind of sentinel."""
    return isinstance(obj, Sentinel)


# ---------------------------------------------------------------------------
# InputProducer
# ---------------------------------------------------------------------------


class InputProducer:
    """Feeds items from a SOURCE step into output queues.

    Calls ``step.items()`` to produce items, increments stats,
    and calls ``step.close()`` when finished.
    """

    def __init__(
        self,
        *,
        step: SourceStep,
        step_name: str,
        output_queues: dict[str, list[multiprocessing.Queue[Any]]],
        stats: StatsCollector | None = None,
    ) -> None:
        logger.debug("step=%s output_queues=%d tags", step_name, len(output_queues))
        self._step = step
        self._step_name = step_name
        self._output_queues = output_queues
        self._stats = stats

    def run(self) -> None:
        """Iterate step.items() into output queues, then send sentinel."""
        logger.info("input producer started step=%s", self._step_name)
        first_item_recorded = False
        try:
            self._step.open()
            if self._stats is not None:
                self._stats.set_state(self._step_name, "processing")
            for item in self._step.items():
                if not first_item_recorded and self._stats is not None:
                    self._stats.record_first_item(self._step_name)
                    first_item_recorded = True
                for tag, tag_queues in self._output_queues.items():
                    for q in tag_queues:
                        q.put(item)
                    if self._stats is not None:
                        self._stats.increment_emitted(self._step_name, tag)
                if self._stats is not None:
                    self._stats.increment(self._step_name, "processed")
        finally:
            self._step.close()
            sentinel = Sentinel()
            seen: set[int] = set()
            for tag_queues in self._output_queues.values():
                for q in tag_queues:
                    qid = id(q)
                    if qid not in seen:
                        q.put(sentinel)
                        seen.add(qid)
            if self._stats is not None:
                self._stats.set_state(self._step_name, "done")
                self._stats.finish(self._step_name)
            logger.info("input producer finished step=%s", self._step_name)


# ---------------------------------------------------------------------------
# BaseProducer
# ---------------------------------------------------------------------------


class BaseProducer(ABC):
    """Abstract base for queue-consuming step executors.

    Runs as a thread in the main process (not a subprocess).
    Provides shared infrastructure: emit callback, sentinel propagation,
    result publishing, and ready-event synchronisation.
    """

    def __init__(
        self,
        *,
        step: StepBase,
        step_name: str,
        input_queue: multiprocessing.Queue[Any],
        output_queues: dict[str, list[multiprocessing.Queue[Any]]],
        stats: StatsCollector,
        state: Any = None,
        sentinel_count: int = 1,
        ready_events: list[Event] | None = None,
        state_changed_event: threading.Event | None = None,
        state_dispatch: Callable[[str, Any], None] | None = None,
    ) -> None:
        logger.debug(
            "step=%s output_queues=%d tags sentinel_count=%d",
            step_name,
            len(output_queues),
            sentinel_count,
        )
        self.step = step
        self.step_name = step_name
        self.input_queue = input_queue
        self.output_queues = output_queues
        self.stats = stats
        self.state = state
        self.sentinel_count = sentinel_count
        self.ready_events = ready_events
        self.state_changed_event = state_changed_event or threading.Event()
        self.state_dispatch = state_dispatch

    # -- helpers -------------------------------------------------------------

    def _make_emit(self) -> Callable[[Any, str], None]:
        """Return a callback that routes an item to the queues for the given tag.

        - ``outputs = ()`` → emit raises ``RuntimeError``
        - unconnected tag → silent drop (DEBUG log)
        - connected tag → put into each queue for that tag
        """
        queues_by_tag = self.output_queues
        step_name = self.step_name
        step_outputs = self.step.outputs
        stats = self.stats

        def emit(item: Any, tag: str) -> None:
            if not step_outputs:
                raise RuntimeError(
                    f"Step '{step_name}' has no declared outputs — emit() not allowed"
                )
            tag_queues = queues_by_tag.get(tag)
            if tag_queues is None:
                logger.debug("unconnected tag=%s step=%s — dropped", tag, step_name)
                return
            for q in tag_queues:
                q.put(item)
            stats.increment_emitted(step_name, tag)

        return emit

    def _wait_until_is_ready(self) -> None:
        """Block until ready_events are set AND step.is_ready(state) is True."""
        if self.ready_events is not None:
            logger.debug("waiting on %d ready events", len(self.ready_events))
            for evt in self.ready_events:
                evt.wait()
            logger.debug("all ready events set")
        while not self.step.is_ready(self.state):
            self.stats.set_state(self.step_name, "waiting_for_state")
            logger.debug("is_ready=False step=%s, waiting", self.step_name)
            self.state_changed_event.wait(timeout=5)
            self.state_changed_event.clear()
        logger.debug("is_ready=True step=%s", self.step_name)

    def _send_sentinel(self) -> None:
        """Put a Sentinel into each unique output queue across all tags."""
        sentinel = Sentinel()
        seen: set[int] = set()
        for tag_queues in self.output_queues.values():
            for q in tag_queues:
                qid = id(q)
                if qid not in seen:
                    q.put(sentinel)
                    seen.add(qid)
        logger.info(
            "sent sentinel to %d unique queue(s) step=%s",
            len(seen),
            self.step_name,
        )

    def _dispatch_output_state(self) -> None:
        """Call step.get_output_state() and dispatch results via state_dispatch."""
        output_state = self.step.get_output_state()
        if output_state is None or self.state_dispatch is None:
            return
        for target_name, state_value in output_state.items():
            self.state_dispatch(target_name, state_value)

    @abstractmethod
    def run(self) -> None: ...


# ---------------------------------------------------------------------------
# SequentialProducer
# ---------------------------------------------------------------------------


class SequentialProducer(BaseProducer):
    """Consumes items one-by-one in a single process."""

    step: SequentialStep

    def run(self) -> None:
        logger.debug("run started step=%s", self.step_name)
        first_item_recorded = False
        sentinel_remaining = self.sentinel_count
        try:
            self._wait_until_is_ready()
            self.step.open()
            emit = self._make_emit()
            self.stats.set_state(self.step_name, "idle")
            logger.info("producer started step=%s", self.step_name)
            while True:
                if self.input_queue.empty():
                    idle_start = time.monotonic_ns()
                    item = self.input_queue.get()
                    idle_ns = time.monotonic_ns() - idle_start
                    self.stats.add_idle_ns(self.step_name, idle_ns)
                else:
                    item = self.input_queue.get()

                if is_sentinel(item):
                    sentinel_remaining -= 1
                    if sentinel_remaining == 0:
                        logger.info("all sentinels received step=%s", self.step_name)
                        break
                    logger.debug(
                        "sentinel received (%d remaining) step=%s",
                        sentinel_remaining,
                        self.step_name,
                    )
                    continue

                if not first_item_recorded:
                    self.stats.record_first_item(self.step_name)
                    first_item_recorded = True

                self.stats.set_state(self.step_name, "processing")
                try:
                    proc_start = time.monotonic_ns()
                    self.step.process(item, self.state, emit)
                    proc_ns = time.monotonic_ns() - proc_start
                    self.stats.add_processing_ns(self.step_name, proc_ns)
                    self.stats.increment(self.step_name, "processed")
                except Exception:
                    self.stats.increment(self.step_name, "errored")
                    logger.warning(
                        "process() raised for step=%s item=%s",
                        self.step_name,
                        repr(item)[:200],
                        exc_info=True,
                    )
                self.stats.set_state(self.step_name, "idle")
        finally:
            self._send_sentinel()
            self.step.close()
            self._dispatch_output_state()
            self.stats.set_state(self.step_name, "done")
            self.stats.finish(self.step_name)
            logger.info("producer finished step=%s", self.step_name)


# ---------------------------------------------------------------------------
# Module-level worker functions (must be picklable)
# ---------------------------------------------------------------------------


@dataclass
class ChunkResult:
    """Data returned by a worker process after processing a chunk of items.

    This is a framework-internal transport object — not visible to step authors.
    """

    processed: int = 0
    errored: int = 0
    emitted: dict[str, list[Any]] = field(default_factory=dict[str, list[Any]])
    processing_ns: int = 0


# Process-global worker state (set by _worker_init in each worker process)
_worker_instance: Any = None
_worker_state: Any = None


def _worker_init(worker_bytes: bytes, state: Any) -> None:
    """Initializer for worker processes: deserialize Worker and call open()."""
    global _worker_instance, _worker_state  # noqa: PLW0603
    _worker_instance = pickle.loads(worker_bytes)  # noqa: S301
    _worker_state = state
    _worker_instance.open()
    atexit.register(_worker_finalize)


def _worker_finalize() -> None:
    """Cleanup for worker processes: call Worker.close()."""
    global _worker_instance  # noqa: PLW0603
    if _worker_instance is not None:
        try:
            _worker_instance.close()
        except Exception:
            logger.warning("worker close() raised", exc_info=True)
        _worker_instance = None


def _parallel_worker(chunk: list[Any], step_name: str) -> ChunkResult:
    """Process a chunk of items in a worker process.

    Uses the process-global ``_worker_instance`` and ``_worker_state``
    set up by ``_worker_init``.  Items are collected in memory and
    returned to the producer, which puts them into output queues.
    """
    worker = _worker_instance
    state = _worker_state
    result = ChunkResult()
    emitted: dict[str, list[Any]] = {}

    def _collect_emit(item: Any, tag: str) -> None:
        emitted.setdefault(tag, []).append(item)

    for item in chunk:
        try:
            t0 = time.monotonic_ns()
            worker.process(item, state, _collect_emit)
            result.processing_ns += time.monotonic_ns() - t0
            result.processed += 1
        except Exception:
            result.errored += 1
            logger.warning(
                "process() raised in worker step=%s item=%s",
                step_name,
                repr(item)[:200],
                exc_info=True,
            )

    result.emitted = emitted
    return result


# ---------------------------------------------------------------------------
# ParallelProducer
# ---------------------------------------------------------------------------


class ParallelProducer(BaseProducer):
    """Distributes items across worker processes via ProcessPoolExecutor."""

    step: ParallelStep

    def __init__(
        self,
        *,
        workers: int = 4,
        chunk_size: int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.workers = workers
        self.chunk_size = chunk_size

    def _drain_chunk_result(self, chunk_result: ChunkResult) -> None:
        """Process a single chunk result: update stats, put items into output queues."""
        self.stats.increment(self.step_name, "processed", chunk_result.processed)
        self.stats.add_processing_ns(self.step_name, chunk_result.processing_ns)
        if chunk_result.errored:
            self.stats.increment(self.step_name, "errored", chunk_result.errored)
        for tag, items in chunk_result.emitted.items():
            tag_queues = self.output_queues.get(tag)
            if tag_queues is None:
                logger.debug("unconnected tag=%s step=%s — dropped", tag, self.step_name)
                continue
            for item in items:
                for q in tag_queues:
                    q.put(item)
            self.stats.increment_emitted(self.step_name, tag, len(items))

    def _collect_completed(self, futures: list[Any]) -> list[Any]:
        """Collect results from completed futures, putting items into output queues."""
        remaining: list[Any] = []
        for f in futures:
            if f.done():
                self._drain_chunk_result(f.result())
            else:
                remaining.append(f)
        return remaining

    def run(self) -> None:
        logger.debug("run started step=%s workers=%d", self.step_name, self.workers)
        first_item_recorded = False
        sentinel_remaining = self.sentinel_count
        ctx = multiprocessing.get_context("spawn")
        try:
            self._wait_until_is_ready()

            # Create and pickle worker before step.open() so the worker
            # doesn't capture any unpicklable main-process resources.
            worker = self.step.create_worker()
            worker_bytes = pickle.dumps(worker)

            self.step.open()
            self.stats.set_state(self.step_name, "idle")
            logger.info("producer started step=%s", self.step_name)

            executor = ProcessPoolExecutor(
                max_workers=self.workers,
                mp_context=ctx,
                initializer=_worker_init,
                initargs=(worker_bytes, self.state),
            )
            try:
                chunk: list[Any] = []
                futures: list[Any] = []

                while True:
                    if self.input_queue.empty():
                        idle_start = time.monotonic_ns()
                        item = self.input_queue.get()
                        idle_ns = time.monotonic_ns() - idle_start
                        self.stats.add_idle_ns(self.step_name, idle_ns)
                    else:
                        item = self.input_queue.get()

                    if is_sentinel(item):
                        sentinel_remaining -= 1
                        if sentinel_remaining == 0:
                            logger.info(
                                "all sentinels received step=%s", self.step_name
                            )
                            # Flush remaining chunk
                            if chunk:
                                logger.debug(
                                    "submitting final chunk size=%d", len(chunk)
                                )
                                futures.append(
                                    executor.submit(
                                        _parallel_worker,
                                        chunk,
                                        self.step_name,
                                    )
                                )
                            break
                        logger.debug(
                            "sentinel received (%d remaining) step=%s",
                            sentinel_remaining,
                            self.step_name,
                        )
                        continue

                    if not first_item_recorded:
                        self.stats.record_first_item(self.step_name)
                        first_item_recorded = True

                    self.stats.set_state(self.step_name, "processing")
                    chunk.append(item)
                    if len(chunk) >= self.chunk_size:
                        logger.debug("submitting chunk size=%d", len(chunk))
                        futures.append(
                            executor.submit(
                                _parallel_worker,
                                chunk,
                                self.step_name,
                            )
                        )
                        chunk = []
                        # Interleave: collect completed futures between chunk submissions
                        futures = self._collect_completed(futures)

                # Collect remaining results
                for future in as_completed(futures):
                    self._drain_chunk_result(future.result())
                # 모든 아이템 처리 완료 → sentinel 먼저 전파
                self._send_sentinel()
                # 처리 완료 시점 기록 (executor cleanup 대기 시간 제외)
                self.stats.finish(self.step_name)
            finally:
                executor.shutdown(wait=True)
        finally:
            self.step.close()
            self._dispatch_output_state()
            self.stats.set_state(self.step_name, "done")
            logger.info("producer finished step=%s", self.step_name)
