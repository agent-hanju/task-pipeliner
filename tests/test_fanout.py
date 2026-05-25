"""Tests: Fan-out — W-10."""

from __future__ import annotations

from typing import Any

import pytest
from dummy_steps import SequentialPassthroughStep

from task_pipeliner.stats import StatsCollector
from task_pipeliner.step_runners import Sentinel, SequentialStepRunner, is_sentinel


class TestFanOut:
    """One producer broadcasting to multiple output queues."""

    @pytest.mark.timeout(20)
    def test_fanout_both_queues_receive_all_items(self) -> None:
        """1 producer → 2 output queues, both receive N items."""
        import multiprocessing

        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_a: multiprocessing.Queue[Any] = ctx.Queue()
        out_b: multiprocessing.Queue[Any] = ctx.Queue()

        stats = StatsCollector()
        stats.register("SequentialPassthroughStep")

        items = list(range(20))
        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = SequentialStepRunner(
            step=SequentialPassthroughStep(),
            step_name="SequentialPassthroughStep",
            input_queue=in_q,
            output_queues={"main": [out_a, out_b]},
            stats=stats,
        )
        producer.run()

        collected_a = []
        while True:
            obj = out_a.get(timeout=2)
            if is_sentinel(obj):
                break
            collected_a.append(obj)

        collected_b = []
        while True:
            obj = out_b.get(timeout=2)
            if is_sentinel(obj):
                break
            collected_b.append(obj)

        assert collected_a == items
        assert collected_b == items
