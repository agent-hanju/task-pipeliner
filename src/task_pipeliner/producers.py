"""Producer classes — execute steps, manage queues, collect results."""

from __future__ import annotations

import logging
import multiprocessing
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing.synchronize import Event
from typing import Any

from task_pipeliner.base import BaseResult, BaseStep
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
        step: BaseStep[Any],
        output_queues: dict[str, list[multiprocessing.Queue[Any]]],
        stats: StatsCollector | None = None,
    ) -> None:
        logger.debug("step=%s output_queues=%d tags", step.name, len(output_queues))
        self._step = step
        self._output_queues = output_queues
        self._stats = stats

    def run(self) -> None:
        """Iterate step.items() into output queues, then send sentinel."""
        logger.info("input producer started step=%s", self._step.name)
        first_item_recorded = False
        try:
            self._step.open()
            if self._stats is not None:
                self._stats.set_state(self._step.name, "processing")
            for item in self._step.items():
                if not first_item_recorded and self._stats is not None:
                    self._stats.record_first_item(self._step.name)
                    first_item_recorded = True
                for tag, tag_queues in self._output_queues.items():
                    for q in tag_queues:
                        q.put(item)
                    if self._stats is not None:
                        self._stats.increment_emitted(self._step.name, tag)
                if self._stats is not None:
                    self._stats.increment(self._step.name, "processed")
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
                self._stats.set_state(self._step.name, "done")
                self._stats.finish(self._step.name)
            logger.info("input producer finished step=%s", self._step.name)


# ---------------------------------------------------------------------------
# BaseProducer
# ---------------------------------------------------------------------------


class BaseProducer(ABC, multiprocessing.Process):
    """Abstract base for queue-consuming step executors.

    Provides shared infrastructure: emit callback, sentinel propagation,
    result publishing, and ready-event synchronisation.
    """

    def __init__(
        self,
        *,
        step: BaseStep[Any],
        input_queue: multiprocessing.Queue[Any],
        output_queues: dict[str, list[multiprocessing.Queue[Any]]],
        stats: StatsCollector,
        result_queue: multiprocessing.Queue[Any],
        state: Any = None,
        ready_events: list[Event] | None = None,
        next_state_setter: Callable[[Any], None] | None = None,
        state_changed_event: threading.Event | None = None,
    ) -> None:
        logger.debug(
            "step=%s output_queues=%d tags",
            step.name,
            len(output_queues),
        )
        super().__init__()
        self.step = step
        self.input_queue = input_queue
        self.output_queues = output_queues
        self.stats = stats
        self.result_queue = result_queue
        self.state = state
        self.ready_events = ready_events
        self.next_state_setter = next_state_setter
        self.state_changed_event = state_changed_event or threading.Event()

    # -- helpers -------------------------------------------------------------

    def _make_emit(self) -> Callable[[Any, str], None]:
        """Return a callback that routes an item to the queues for the given tag.

        - ``outputs = ()`` → emit raises ``RuntimeError``
        - unconnected tag → silent drop (DEBUG log)
        - connected tag → put into each queue for that tag
        """
        queues_by_tag = self.output_queues
        step_name = self.step.name
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

    def _wait_until_ready(self) -> None:
        """Block until all *ready_events* are set (if any)."""
        if self.ready_events is None:
            return
        logger.debug("waiting on %d ready events", len(self.ready_events))
        for evt in self.ready_events:
            evt.wait()
        logger.debug("all ready events set")

    def _wait_until_is_ready(self) -> None:
        """Block until ready_events are set AND step.is_ready(state) is True."""
        self._wait_until_ready()
        while not self.step.is_ready(self.state):
            self.stats.set_state(self.step.name, "waiting_for_state")
            logger.debug("is_ready=False step=%s, waiting", self.step.name)
            self.state_changed_event.wait(timeout=5)
            self.state_changed_event.clear()
        logger.debug("is_ready=True step=%s", self.step.name)

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
            self.step.name,
        )

    def _publish_result(self, result: BaseResult) -> None:
        """Send accumulated result to the result queue."""
        logger.debug("publishing result for step=%s", self.step.name)
        self.result_queue.put(result)

    @abstractmethod
    def run(self) -> None: ...


# ---------------------------------------------------------------------------
# SequentialProducer
# ---------------------------------------------------------------------------


class SequentialProducer(BaseProducer):
    """Consumes items one-by-one in a single process."""

    def run(self) -> None:
        logger.debug("run started step=%s", self.step.name)
        accumulated: BaseResult | None = None
        first_item_recorded = False
        try:
            self._wait_until_is_ready()
            self.step.open()
            emit = self._make_emit()
            self.stats.set_state(self.step.name, "idle")
            logger.info("producer started step=%s", self.step.name)
            while True:
                if self.input_queue.empty():
                    idle_start = time.monotonic_ns()
                    item = self.input_queue.get()
                    idle_ns = time.monotonic_ns() - idle_start
                    self.stats.add_idle_ns(self.step.name, idle_ns)
                else:
                    item = self.input_queue.get()

                if is_sentinel(item):
                    logger.info("sentinel received step=%s", self.step.name)
                    break

                if not first_item_recorded:
                    self.stats.record_first_item(self.step.name)
                    first_item_recorded = True

                self.stats.set_state(self.step.name, "processing")
                try:
                    proc_start = time.monotonic_ns()
                    result = self.step.process(item, self.state, emit)
                    proc_ns = time.monotonic_ns() - proc_start
                    self.stats.add_processing_ns(self.step.name, proc_ns)
                    accumulated = result if accumulated is None else accumulated.merge(result)
                    self.stats.increment(self.step.name, "processed")
                except Exception:
                    self.stats.increment(self.step.name, "errored")
                    logger.warning(
                        "process() raised for step=%s item=%s",
                        self.step.name,
                        repr(item)[:200],
                        exc_info=True,
                    )
                self.stats.set_state(self.step.name, "idle")
        finally:
            self._send_sentinel()
            self.step.close()
            if accumulated is not None:
                self._publish_result(accumulated)
            self.stats.set_state(self.step.name, "done")
            self.stats.finish(self.step.name)
            if self.next_state_setter is not None:
                self.next_state_setter(self.state)
            logger.info("producer finished step=%s", self.step.name)


# ---------------------------------------------------------------------------
# Module-level worker function (must be picklable)
# ---------------------------------------------------------------------------

_worker_output_queues: dict[str, list[multiprocessing.Queue[Any]]] = {}


def _init_worker(output_queues: dict[str, list[multiprocessing.Queue[Any]]]) -> None:
    """Initializer for ProcessPoolExecutor workers — stores queues in a global."""
    global _worker_output_queues  # noqa: PLW0603
    _worker_output_queues = output_queues
    # 워커 프로세스 종료 시 Queue feeder thread의 pipe flush 대기를 비활성화.
    # 부모 프로세스(다운스트림)가 큐를 소비하므로 데이터 손실 없음.
    for tag_queues in output_queues.values():
        for q in tag_queues:
            q.cancel_join_thread()


def _parallel_worker(
    chunk: list[Any],
    step: BaseStep[Any],
    state: Any,
) -> tuple[BaseResult | None, int, int, dict[str, int], int]:
    """Process a chunk of items in a worker process.

    Items are emitted directly to output queues during processing
    (streaming put) — avoids buffering large numbers of items in memory
    and spreads pipe I/O over the processing duration.

    Returns (accumulated_result, processed_count, errored_count, emitted_by_tag, processing_ns).
    """
    accumulated: BaseResult | None = None
    processed = 0
    errored = 0
    total_processing_ns = 0
    step_outputs = step.outputs
    emitted_by_tag: dict[str, int] = {}

    def _direct_emit(item: Any, tag: str) -> None:
        if not step_outputs:
            raise RuntimeError(f"Step '{step.name}' has no declared outputs — emit() not allowed")
        tag_queues = _worker_output_queues.get(tag)
        if tag_queues is None:
            return
        for q in tag_queues:
            q.put(item)
        emitted_by_tag[tag] = emitted_by_tag.get(tag, 0) + 1

    for item in chunk:
        try:
            t0 = time.monotonic_ns()
            result = step.process(item, state, _direct_emit)
            total_processing_ns += time.monotonic_ns() - t0
            accumulated = result if accumulated is None else accumulated.merge(result)
            processed += 1
        except Exception:
            errored += 1
            logger.warning(
                "process() raised in worker step=%s item=%s",
                step.name,
                repr(item)[:200],
                exc_info=True,
            )

    return accumulated, processed, errored, emitted_by_tag, total_processing_ns


# ---------------------------------------------------------------------------
# ParallelProducer
# ---------------------------------------------------------------------------


class ParallelProducer(BaseProducer):
    """Distributes items across worker processes via ProcessPoolExecutor."""

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

    def _collect_completed(
        self,
        futures: list[Any],
        accumulated: BaseResult | None,
    ) -> tuple[list[Any], BaseResult | None]:
        """Collect results from completed futures, updating stats incrementally."""
        remaining = []
        for f in futures:
            if f.done():
                chunk_result, processed, errored, emitted_by_tag, proc_ns = f.result()
                self.stats.increment(self.step.name, "processed", processed)
                self.stats.add_processing_ns(self.step.name, proc_ns)
                if errored:
                    self.stats.increment(self.step.name, "errored", errored)
                for tag, count in emitted_by_tag.items():
                    self.stats.increment_emitted(self.step.name, tag, count)
                if chunk_result is not None:
                    accumulated = (
                        chunk_result if accumulated is None else accumulated.merge(chunk_result)
                    )
            else:
                remaining.append(f)
        return remaining, accumulated

    def run(self) -> None:
        logger.debug("run started step=%s workers=%d", self.step.name, self.workers)
        accumulated: BaseResult | None = None
        first_item_recorded = False
        ctx = multiprocessing.get_context("spawn")
        try:
            self._wait_until_is_ready()
            self.step.open()
            self.stats.set_state(self.step.name, "idle")
            logger.info("producer started step=%s", self.step.name)

            executor = ProcessPoolExecutor(
                max_workers=self.workers,
                mp_context=ctx,
                initializer=_init_worker,
                initargs=(self.output_queues,),
            )
            try:
                chunk: list[Any] = []
                futures: list[Any] = []

                while True:
                    if self.input_queue.empty():
                        idle_start = time.monotonic_ns()
                        item = self.input_queue.get()
                        idle_ns = time.monotonic_ns() - idle_start
                        self.stats.add_idle_ns(self.step.name, idle_ns)
                    else:
                        item = self.input_queue.get()

                    if is_sentinel(item):
                        logger.info("sentinel received step=%s", self.step.name)
                        # Flush remaining chunk
                        if chunk:
                            logger.debug("submitting final chunk size=%d", len(chunk))
                            futures.append(
                                executor.submit(
                                    _parallel_worker,
                                    chunk,
                                    self.step,
                                    self.state,
                                )
                            )
                        break

                    if not first_item_recorded:
                        self.stats.record_first_item(self.step.name)
                        first_item_recorded = True

                    self.stats.set_state(self.step.name, "processing")
                    chunk.append(item)
                    if len(chunk) >= self.chunk_size:
                        logger.debug("submitting chunk size=%d", len(chunk))
                        futures.append(
                            executor.submit(
                                _parallel_worker,
                                chunk,
                                self.step,
                                self.state,
                            )
                        )
                        chunk = []
                        # Interleave: collect completed futures between chunk submissions
                        futures, accumulated = self._collect_completed(futures, accumulated)

                # Collect remaining results
                for future in as_completed(futures):
                    chunk_result, processed, errored, emitted_by_tag, proc_ns = future.result()
                    self.stats.increment(self.step.name, "processed", processed)
                    self.stats.add_processing_ns(self.step.name, proc_ns)
                    if errored:
                        self.stats.increment(self.step.name, "errored", errored)
                    for tag, count in emitted_by_tag.items():
                        self.stats.increment_emitted(self.step.name, tag, count)
                    if chunk_result is not None:
                        accumulated = (
                            chunk_result
                            if accumulated is None
                            else accumulated.merge(chunk_result)
                        )
                # 모든 아이템 처리 완료 → sentinel 먼저 전파
                self._send_sentinel()
                # 처리 완료 시점 기록 (executor cleanup 대기 시간 제외)
                self.stats.finish(self.step.name)
            finally:
                executor.shutdown(wait=True)
        finally:
            self.step.close()
            if accumulated is not None:
                self._publish_result(accumulated)
            self.stats.set_state(self.step.name, "done")
            if self.next_state_setter is not None:
                self.next_state_setter(self.state)
            logger.info("producer finished step=%s", self.step.name)
