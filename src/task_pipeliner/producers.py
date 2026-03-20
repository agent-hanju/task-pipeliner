"""Producer classes — execute steps, manage queues, collect results."""

from __future__ import annotations

import logging
import multiprocessing
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
        output_queues: list[multiprocessing.Queue[Any]],
        stats: StatsCollector | None = None,
    ) -> None:
        logger.debug("step=%s output_queues=%d", step.name, len(output_queues))
        self._step = step
        self._output_queues = output_queues
        self._stats = stats

    def run(self) -> None:
        """Iterate step.items() into output queues, then send sentinel."""
        logger.info("input producer started step=%s", self._step.name)
        try:
            for item in self._step.items():
                for q in self._output_queues:
                    q.put(item)
                if self._stats is not None:
                    self._stats.increment(self._step.name, "passed")
        finally:
            self._step.close()
            sentinel = Sentinel()
            for q in self._output_queues:
                q.put(sentinel)
            if self._stats is not None:
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
        output_queues: list[multiprocessing.Queue[Any]],
        stats: StatsCollector,
        result_queue: multiprocessing.Queue[Any],
        state: Any = None,
        ready_events: list[Event] | None = None,
        next_state_setter: Callable[[Any], None] | None = None,
    ) -> None:
        logger.debug(
            "step=%s output_queues=%d",
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

    # -- helpers -------------------------------------------------------------

    def _make_emit(self) -> Callable[[Any], None]:
        """Return a callback that forwards an item to every output queue
        and increments the *passed* stat counter."""
        queues = self.output_queues
        step_name = self.step.name
        stats = self.stats

        def emit(item: Any) -> None:
            for q in queues:
                q.put(item)
            stats.increment(step_name, "passed")

        return emit

    def _wait_until_ready(self) -> None:
        """Block until all *ready_events* are set (if any)."""
        if self.ready_events is None:
            return
        logger.debug("waiting on %d ready events", len(self.ready_events))
        for evt in self.ready_events:
            evt.wait()
        logger.debug("all ready events set")

    def _send_sentinel(self) -> None:
        """Put a Sentinel into each output queue."""
        logger.info(
            "sending sentinel to %d output queue(s) step=%s",
            len(self.output_queues),
            self.step.name,
        )
        sentinel = Sentinel()
        for q in self.output_queues:
            q.put(sentinel)

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
        try:
            self._wait_until_ready()
            emit = self._make_emit()
            logger.info("producer started step=%s", self.step.name)
            while True:
                item = self.input_queue.get()
                if is_sentinel(item):
                    logger.info("sentinel received step=%s", self.step.name)
                    break
                try:
                    result = self.step.process(item, self.state, emit)
                    accumulated = result if accumulated is None else accumulated.merge(result)
                except Exception:
                    self.stats.increment(self.step.name, "errored")
                    logger.warning(
                        "process() raised for step=%s item=%s",
                        self.step.name,
                        repr(item)[:200],
                        exc_info=True,
                    )
        finally:
            self.step.close()
            if accumulated is not None:
                self._publish_result(accumulated)
            self.stats.finish(self.step.name)
            if self.next_state_setter is not None:
                self.next_state_setter(self.state)
            self._send_sentinel()
            logger.info("producer finished step=%s", self.step.name)


# ---------------------------------------------------------------------------
# Module-level worker function (must be picklable)
# ---------------------------------------------------------------------------

_worker_output_queues: list[multiprocessing.Queue[Any]] = []


def _init_worker(output_queues: list[multiprocessing.Queue[Any]]) -> None:
    """Initializer for ProcessPoolExecutor workers — stores queues in a global."""
    global _worker_output_queues  # noqa: PLW0603
    _worker_output_queues = output_queues


def _parallel_worker(
    chunk: list[Any],
    step: BaseStep[Any],
    state: Any,
) -> tuple[BaseResult | None, int, int]:
    """Process a chunk of items in a worker process.

    Items are collected locally during processing, then put into
    output queues in bulk after the chunk is done — reduces lock
    contention compared to per-item put.

    Returns (accumulated_result, passed_count, errored_count).
    """
    accumulated: BaseResult | None = None
    emitted: list[Any] = []
    errored = 0

    for item in chunk:
        try:
            result = step.process(item, state, emitted.append)
            accumulated = result if accumulated is None else accumulated.merge(result)
        except Exception:
            errored += 1
            logger.warning(
                "process() raised in worker step=%s item=%s",
                step.name,
                repr(item)[:200],
                exc_info=True,
            )

    # Bulk put — concentrated lock contention instead of interleaved
    for out_item in emitted:
        for q in _worker_output_queues:
            q.put(out_item)

    return accumulated, len(emitted), errored


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

    def run(self) -> None:
        logger.debug("run started step=%s workers=%d", self.step.name, self.workers)
        accumulated: BaseResult | None = None
        ctx = multiprocessing.get_context("spawn")
        try:
            self._wait_until_ready()
            logger.info("producer started step=%s", self.step.name)

            executor = ProcessPoolExecutor(
                max_workers=self.workers,
                mp_context=ctx,
                initializer=_init_worker,
                initargs=(self.output_queues,),
            )
            try:
                chunk: list[Any] = []
                futures = []

                while True:
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

                # Collect results
                for future in as_completed(futures):
                    chunk_result, passed, errored = future.result()
                    self.stats.increment(self.step.name, "passed", passed)
                    if errored:
                        self.stats.increment(self.step.name, "errored", errored)
                    if chunk_result is not None:
                        accumulated = (
                            chunk_result if accumulated is None else accumulated.merge(chunk_result)
                        )
            finally:
                executor.shutdown(wait=True)
        finally:
            self.step.close()
            if accumulated is not None:
                self._publish_result(accumulated)
            self.stats.finish(self.step.name)
            if self.next_state_setter is not None:
                self.next_state_setter(self.state)
            self._send_sentinel()
            logger.info("producer finished step=%s", self.step.name)
