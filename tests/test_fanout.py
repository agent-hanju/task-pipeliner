"""Tests: Fan-out, State/Event synchronisation — W-10."""

from __future__ import annotations

import multiprocessing
import threading
from typing import Any

import pytest
from dummy_steps import SequentialPassthroughStep, StateAwareStep

from task_pipeliner.stats import StatsCollector
from task_pipeliner.step_runners import Sentinel, SequentialStepRunner, is_sentinel


class TestFanOut:
    """One producer broadcasting to multiple output queues."""

    @pytest.mark.timeout(20)
    def test_fanout_both_queues_receive_all_items(self) -> None:
        """1 producer → 2 output queues, both receive N items."""
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


class TestStateEvent:
    """Event-based synchronisation: producer waits until state is ready."""

    @pytest.mark.timeout(20)
    def test_producer_blocks_until_event_set(self) -> None:
        """ready_events not set → producer blocks; set from another thread → resumes."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()

        stats = StatsCollector()
        stats.register("SequentialPassthroughStep")

        in_q.put(1)
        in_q.put(Sentinel())

        evt = ctx.Event()
        # Event NOT set — producer should block

        producer = SequentialStepRunner(
            step=SequentialPassthroughStep(),
            step_name="SequentialPassthroughStep",
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            ready_events=[evt],
        )

        # Run producer in a thread; it will block on _wait_until_ready
        t = threading.Thread(target=producer.run)
        t.start()

        # Queue should be empty while event is unset
        assert out_q.empty()

        # Set event → producer resumes
        evt.set()
        t.join(timeout=10)
        assert not t.is_alive()

        obj = out_q.get(timeout=2)
        assert obj == 1

    @pytest.mark.timeout(20)
    def test_two_events_both_must_be_set(self) -> None:
        """Two ready_events — producer waits until BOTH are set."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()

        stats = StatsCollector()
        stats.register("SequentialPassthroughStep")

        in_q.put(42)
        in_q.put(Sentinel())

        evt_a = ctx.Event()
        evt_b = ctx.Event()

        producer = SequentialStepRunner(
            step=SequentialPassthroughStep(),
            step_name="SequentialPassthroughStep",
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            ready_events=[evt_a, evt_b],
        )

        t = threading.Thread(target=producer.run)
        t.start()

        # Set only one — should still block
        evt_a.set()
        t.join(timeout=0.3)
        assert t.is_alive(), "Producer should still be blocked (evt_b not set)"

        # Set second — producer resumes
        evt_b.set()
        t.join(timeout=10)
        assert not t.is_alive()

        obj = out_q.get(timeout=2)
        assert obj == 42


class TestFanOutEndToEnd:
    """Fan-out aggregation: Queue B completes → state injected → Queue A resumes."""

    @pytest.mark.timeout(20)
    def test_aggregation_sets_state_for_downstream(self) -> None:
        """
        items → [PassthroughStep] ─┬→ Queue A (StateAwareStep, blocked until evt)
                                   └→ Queue B (PassthroughStep, completes immediately)

        Queue B completion sets event → Queue A resumes with state.
        """
        ctx = multiprocessing.get_context("spawn")

        source_q: multiprocessing.Queue[Any] = ctx.Queue()
        queue_a: multiprocessing.Queue[Any] = ctx.Queue()
        queue_b: multiprocessing.Queue[Any] = ctx.Queue()
        out_a: multiprocessing.Queue[Any] = ctx.Queue()
        out_b: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("SequentialPassthroughStep")
        stats.register("StateAwareStep")

        evt = ctx.Event()
        state_a: dict[str, Any] = {"multiplier": 10}

        items = [1, 2, 3]
        for item in items:
            source_q.put(item)
        source_q.put(Sentinel())

        # Source: fan-out to queue_a and queue_b
        source_producer = SequentialStepRunner(
            step=SequentialPassthroughStep(),
            step_name="SequentialPassthroughStep",
            input_queue=source_q,
            output_queues={"main": [queue_a, queue_b]},
            stats=stats,
        )

        # Queue A: StateAwareStep, blocked until evt
        producer_a = SequentialStepRunner(
            step=StateAwareStep(),
            step_name="StateAwareStep",
            input_queue=queue_a,
            output_queues={"main": [out_a]},
            stats=stats,
            state=state_a,
            ready_events=[evt],
        )

        # Queue B: completes immediately, then sets event to unblock A
        producer_b = SequentialStepRunner(
            step=SequentialPassthroughStep(),
            step_name="SequentialPassthroughStep",
            input_queue=queue_b,
            output_queues={"main": [out_b]},
            stats=stats,
        )

        def _run_b_then_signal() -> None:
            producer_b.run()
            evt.set()

        threads = [
            threading.Thread(target=source_producer.run),
            threading.Thread(target=producer_a.run),
            threading.Thread(target=_run_b_then_signal),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        for t in threads:
            assert not t.is_alive()

        # Queue A: items * 10
        collected_a = []
        while True:
            obj = out_a.get(timeout=2)
            if is_sentinel(obj):
                break
            collected_a.append(obj)

        assert sorted(collected_a) == [10, 20, 30]
