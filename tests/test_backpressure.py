"""Tests: Backpressure (bounded queue blocking) — W-11."""

from __future__ import annotations

import multiprocessing
import threading
from typing import Any

import pytest
from dummy_steps import PassthroughStep, SlowStep

from task_pipeliner.producers import (
    ParallelProducer,
    Sentinel,
    SequentialProducer,
    is_sentinel,
)
from task_pipeliner.stats import StatsCollector


class TestBackpressure:
    @pytest.mark.timeout(20)
    def test_bounded_queue_blocks_producer(self) -> None:
        """Queue(maxsize=3) + PassthroughStep → producer blocks when full."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue(maxsize=3)
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("PassthroughStep")

        # Put more items than maxsize
        for i in range(10):
            in_q.put(i)
        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=PassthroughStep(),
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
        )

        # Run producer in thread — it will block when out_q fills up
        t = threading.Thread(target=producer.run)
        t.start()

        # Drain output queue — producer should resume and finish
        collected = []
        while True:
            obj = out_q.get(timeout=5)
            if is_sentinel(obj):
                break
            collected.append(obj)

        t.join(timeout=10)
        assert not t.is_alive()
        assert collected == list(range(10))
        # Queue never exceeded maxsize (implicit — if it did, put would block,
        # and we successfully drained everything)

    @pytest.mark.timeout(20)
    def test_consumer_drains_and_all_items_arrive(self) -> None:
        """Consumer drains bounded queue → producer resumes, total count matches."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue(maxsize=2)
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("PassthroughStep")

        n = 50
        for i in range(n):
            in_q.put(i)
        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=PassthroughStep(),
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
        )

        t = threading.Thread(target=producer.run)
        t.start()

        collected = []
        while True:
            obj = out_q.get(timeout=5)
            if is_sentinel(obj):
                break
            collected.append(obj)

        t.join(timeout=10)
        assert not t.is_alive()
        assert len(collected) == n

    @pytest.mark.timeout(30)
    def test_parallel_producer_slow_step_bounded_queue(self) -> None:
        """ParallelProducer + SlowStep + bounded queue → all items processed."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue(maxsize=5)
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("SlowStep")

        n = 15
        for i in range(n):
            in_q.put(i)
        in_q.put(Sentinel())

        producer = ParallelProducer(
            step=SlowStep(sleep_seconds=0.01),
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
            workers=2,
            chunk_size=5,
        )

        t = threading.Thread(target=producer.run)
        t.start()

        collected = []
        while True:
            obj = out_q.get(timeout=10)
            if is_sentinel(obj):
                break
            collected.append(obj)

        t.join(timeout=15)
        assert not t.is_alive()
        assert sorted(collected) == list(range(n))
