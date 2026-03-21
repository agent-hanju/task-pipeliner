"""Tests: is_ready gating + set_step_state dispatch + engine wiring."""

from __future__ import annotations

import multiprocessing
import threading
from pathlib import Path
from typing import Any

import pytest
from dummy_steps import (
    CollectorStep,
    DummySourceStep,
    PassthroughStep,
    StateGatedStep,
    TerminalStep,
)

from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.engine import PipelineEngine, StepRegistry
from task_pipeliner.producers import Sentinel, SequentialProducer
from task_pipeliner.stats import StatsCollector

# ---------------------------------------------------------------------------
# BaseStep.is_ready
# ---------------------------------------------------------------------------


class TestIsReady:
    def test_default_returns_true(self) -> None:
        step = PassthroughStep()
        assert step.is_ready(None) is True
        assert step.is_ready({"some": "data"}) is True

    def test_override_returns_false_when_state_none(self) -> None:
        step = StateGatedStep()
        assert step.is_ready(None) is False

    def test_override_returns_true_when_state_set(self) -> None:
        step = StateGatedStep()
        assert step.is_ready([1, 2, 3]) is True


# ---------------------------------------------------------------------------
# BaseStep.set_step_state
# ---------------------------------------------------------------------------


class TestSetStepState:
    def test_raises_outside_pipeline(self) -> None:
        step = PassthroughStep()
        with pytest.raises(RuntimeError, match="not available outside pipeline"):
            step.set_step_state("SomeStep", {"data": 1})

    def test_dispatches_via_callback(self) -> None:
        step = PassthroughStep()
        calls: list[tuple[str, Any]] = []
        step._state_dispatch = lambda target, state: calls.append((target, state))

        step.set_step_state("TargetStep", {"freq": {1: 3}})

        assert len(calls) == 1
        assert calls[0] == ("TargetStep", {"freq": {1: 3}})

    def test_collector_calls_set_step_state_on_close(self) -> None:
        step = CollectorStep(target_step="MyFilter")
        calls: list[tuple[str, Any]] = []
        step._state_dispatch = lambda target, state: calls.append((target, state))

        # Simulate processing
        state = step.initial_state
        step.process(1, state, lambda item, tag: None)
        step.process(2, state, lambda item, tag: None)
        step.close()

        assert len(calls) == 1
        assert calls[0][0] == "MyFilter"
        assert calls[0][1] == [1, 2]


# ---------------------------------------------------------------------------
# Producer: _wait_until_is_ready blocks on is_ready=False
# ---------------------------------------------------------------------------


class TestProducerStateGating:
    @pytest.mark.timeout(20)
    def test_producer_blocks_when_not_ready_resumes_on_state_change(self) -> None:
        """StateGatedStep producer blocks; state set from another thread; resumes."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("StateGatedStep")

        in_q.put(42)
        in_q.put(Sentinel())

        state_changed = threading.Event()

        producer = SequentialProducer(
            step=StateGatedStep(),
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            result_queue=result_q,
            state=None,  # is_ready returns False
            state_changed_event=state_changed,
        )

        t = threading.Thread(target=producer.run)
        t.start()

        # Give time for producer to start and block
        t.join(timeout=0.5)
        assert t.is_alive(), "Producer should still be blocked (is_ready=False)"
        assert out_q.empty()

        # Inject state and fire event
        producer.state = [1, 2, 3]
        state_changed.set()

        t.join(timeout=10)
        assert not t.is_alive()

        obj = out_q.get(timeout=2)
        assert obj["item"] == 42
        assert obj["state_len"] == 3

    @pytest.mark.timeout(20)
    def test_producer_starts_immediately_when_is_ready_true(self) -> None:
        """Default is_ready=True — no blocking."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("PassthroughStep")

        in_q.put(99)
        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=PassthroughStep(),
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            result_queue=result_q,
        )

        t = threading.Thread(target=producer.run)
        t.start()
        t.join(timeout=10)
        assert not t.is_alive()

        obj = out_q.get(timeout=2)
        assert obj == 99


# ---------------------------------------------------------------------------
# Engine: end-to-end collector → gated filter
# ---------------------------------------------------------------------------


class TestEngineStateDispatch:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(workers=workers, chunk_size=chunk_size),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_collector_sets_state_for_gated_step(self, tmp_path: Path) -> None:
        """
        Source → fan-out [CollectorStep (terminal), StateGatedStep]
        CollectorStep close() calls set_step_state → StateGatedStep unblocks.
        """
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source",
                    items=[1, 2, 3],
                    outputs={"main": ["collector", "gated"]},
                ),
                StepConfig(type="collector", target_step="StateGatedStep"),
                StepConfig(type="gated", outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {
                "source": DummySourceStep,
                "collector": CollectorStep,
                "gated": StateGatedStep,
                "terminal": TerminalStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")

        assert stats._stats["CollectorStep"].processed == 3
        assert stats._stats["StateGatedStep"].processed == 3
        assert stats._stats["TerminalStep"].processed == 3
