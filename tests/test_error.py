"""Tests: Error handling / error propagation — W-12."""

from __future__ import annotations

import multiprocessing
from typing import Any

import pytest
from dummy_steps import ErrorOnItemStep, SequentialErrorOnItemStep

from task_pipeliner.stats import StatsCollector
from task_pipeliner.step_runners import (
    ParallelStepRunner,
    Sentinel,
    SequentialStepRunner,
    is_sentinel,
)


class TestErrorHandlingSequential:
    _STEP_NAME = "SequentialErrorOnItemStep"

    def _run(
        self, items: list[Any], error_value: Any = -1
    ) -> tuple[list[Any], StatsCollector]:
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register(self._STEP_NAME)

        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = SequentialStepRunner(
            step=SequentialErrorOnItemStep(error_value=error_value),
            step_name=self._STEP_NAME,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
        )
        producer.run()

        collected = []
        while True:
            obj = out_q.get(timeout=2)
            if is_sentinel(obj):
                break
            collected.append(obj)

        return collected, stats

    @pytest.mark.timeout(15)
    @pytest.mark.parametrize(
        "items,expected_ok,expected_err",
        [
            ([1, 2, 3], [1, 2, 3], 0),
            ([1, -1, 2], [1, 2], 1),
            ([1, -1, -1, 2, -1], [1, 2], 3),
        ],
    )
    def test_error_skip_and_count(
        self,
        items: list[int],
        expected_ok: list[int],
        expected_err: int,
    ) -> None:
        collected, stats = self._run(items)
        assert collected == expected_ok
        assert stats._stats["SequentialErrorOnItemStep"].errored == expected_err

    @pytest.mark.timeout(15)
    def test_100_percent_error_rate(self) -> None:
        """All items error → 0 output + sentinel still propagated."""
        collected, stats = self._run([-1, -1, -1, -1, -1])
        assert collected == []
        assert stats._stats["SequentialErrorOnItemStep"].errored == 5

    @pytest.mark.timeout(15)
    def test_stats_errored_accurate(self) -> None:
        items = list(range(10)) + [-1, -1]
        collected, stats = self._run(items)
        assert stats._stats["SequentialErrorOnItemStep"].errored == 2
        assert stats._stats["SequentialErrorOnItemStep"].processed == 10

    @pytest.mark.timeout(15)
    def test_sentinel_always_sent_even_after_errors(self) -> None:
        """Sentinel must arrive regardless of errors."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register(self._STEP_NAME)

        in_q.put(1)
        in_q.put(-1)
        in_q.put(Sentinel())

        producer = SequentialStepRunner(
            step=SequentialErrorOnItemStep(error_value=-1),
            step_name=self._STEP_NAME,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
        )
        producer.run()

        assert out_q.get(timeout=2) == 1
        assert is_sentinel(out_q.get(timeout=2))

    @pytest.mark.timeout(15)
    def test_errors_do_not_prevent_processing(self) -> None:
        """Errors don't prevent remaining items from being processed."""
        items = [1, -1, 2, -1, 3]
        collected, stats = self._run(items)
        assert len(collected) == 3
        assert stats._stats["SequentialErrorOnItemStep"].errored == 2


class TestErrorHandlingParallel:
    _STEP_NAME = "ErrorOnItemStep"

    @pytest.mark.timeout(30)
    def test_worker_exception_reflected_in_stats(self) -> None:
        """ParallelStepRunner worker exception → stats.errored reflects it."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register(self._STEP_NAME)

        items = [1, 2, -1, 3, -1, 4]
        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = ParallelStepRunner(
            step=ErrorOnItemStep(error_value=-1),
            step_name=self._STEP_NAME,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            workers=2,
            chunk_size=3,
        )
        producer.run()

        collected = []
        while True:
            obj = out_q.get(timeout=5)
            if is_sentinel(obj):
                break
            collected.append(obj)

        assert sorted(collected) == [1, 2, 3, 4]
        assert stats._stats[self._STEP_NAME].errored == 2
        assert stats._stats[self._STEP_NAME].processed == 4

    @pytest.mark.timeout(30)
    def test_errors_do_not_prevent_parallel_processing(self) -> None:
        """Even with errors, remaining items are processed and emitted."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register(self._STEP_NAME)

        items = [1, -1, 2, -1, 3]
        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = ParallelStepRunner(
            step=ErrorOnItemStep(error_value=-1),
            step_name=self._STEP_NAME,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            workers=2,
            chunk_size=2,
        )
        producer.run()

        collected = []
        while True:
            obj = out_q.get(timeout=5)
            if is_sentinel(obj):
                break
            collected.append(obj)

        assert sorted(collected) == [1, 2, 3]
        assert stats._stats[self._STEP_NAME].errored == 2
