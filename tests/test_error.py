"""Tests: Error handling / error propagation — W-12."""

from __future__ import annotations

import multiprocessing
from typing import Any

import pytest
from dummy_steps import ErrorOnItemStep

from task_pipeliner.producers import (
    ParallelProducer,
    Sentinel,
    SequentialProducer,
    is_sentinel,
)
from task_pipeliner.stats import StatsCollector


class TestErrorHandlingSequential:
    def _run(
        self, items: list[Any], error_value: Any = -1
    ) -> tuple[list[Any], StatsCollector]:
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = ErrorOnItemStep(error_value=error_value)
        stats.register(step.name)

        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=step,
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
        assert stats._stats["ErrorOnItemStep"].errored == expected_err

    @pytest.mark.timeout(15)
    def test_100_percent_error_rate(self) -> None:
        """All items error → 0 output + sentinel still propagated."""
        collected, stats = self._run([-1, -1, -1, -1, -1])
        assert collected == []
        assert stats._stats["ErrorOnItemStep"].errored == 5

    @pytest.mark.timeout(15)
    def test_stats_errored_accurate(self) -> None:
        items = list(range(10)) + [-1, -1]
        collected, stats = self._run(items)
        assert stats._stats["ErrorOnItemStep"].errored == 2
        assert stats._stats["ErrorOnItemStep"].processed == 10

    @pytest.mark.timeout(15)
    def test_sentinel_always_sent_even_after_errors(self) -> None:
        """Sentinel must arrive regardless of errors."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = ErrorOnItemStep(error_value=-1)
        stats.register(step.name)

        # Error as the very last item before sentinel
        in_q.put(1)
        in_q.put(-1)
        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=step,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
        )
        producer.run()

        # Must get item 1, then sentinel
        assert out_q.get(timeout=2) == 1
        assert is_sentinel(out_q.get(timeout=2))

    @pytest.mark.timeout(15)
    def test_errors_do_not_prevent_processing(self) -> None:
        """Errors don't prevent remaining items from being processed."""
        items = [1, -1, 2, -1, 3]
        collected, stats = self._run(items)
        assert len(collected) == 3
        assert stats._stats["ErrorOnItemStep"].errored == 2


class TestErrorHandlingParallel:
    @pytest.mark.timeout(30)
    def test_worker_exception_reflected_in_stats(self) -> None:
        """ParallelProducer worker exception → stats.errored reflects it."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = ErrorOnItemStep(error_value=-1)
        stats.register(step.name)

        items = [1, 2, -1, 3, -1, 4]
        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = ParallelProducer(
            step=step,
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
        assert stats._stats[step.name].errored == 2
        assert stats._stats[step.name].processed == 4

    @pytest.mark.timeout(30)
    def test_errors_do_not_prevent_parallel_processing(self) -> None:
        """Even with errors, remaining items are processed and emitted."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = ErrorOnItemStep(error_value=-1)
        stats.register(step.name)

        items = [1, -1, 2, -1, 3]
        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = ParallelProducer(
            step=step,
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
        assert stats._stats[step.name].errored == 2
