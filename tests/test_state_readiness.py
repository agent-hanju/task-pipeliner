"""Tests: is_ready gating + get_output_state dispatch + engine wiring."""

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
    SequentialPassthroughStep,
    StateGatedStep,
    TerminalStep,
)

from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.engine import PipelineEngine
from task_pipeliner.pipeline import StepRegistry
from task_pipeliner.producers import Sentinel, SequentialProducer
from task_pipeliner.stats import StatsCollector

# ---------------------------------------------------------------------------
# StepBase.is_ready
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
# StepBase.get_output_state
# ---------------------------------------------------------------------------


class TestGetOutputState:
    def test_default_returns_none(self) -> None:
        step = PassthroughStep()
        assert step.get_output_state() is None

    def test_collector_returns_output_state_after_processing(self) -> None:
        step = CollectorStep(target_step="MyFilter")

        # Simulate processing
        state = step.initial_state
        step.process(1, state, lambda item, tag: None)
        step.process(2, state, lambda item, tag: None)

        result = step.get_output_state()
        assert result == {"MyFilter": [1, 2]}


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

        stats = StatsCollector()
        stats.register("StateGatedStep")

        in_q.put(42)
        in_q.put(Sentinel())

        state_changed = threading.Event()

        producer = SequentialProducer(
            step=StateGatedStep(),
            step_name="StateGatedStep",
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
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

        stats = StatsCollector()
        stats.register("SequentialPassthroughStep")

        in_q.put(99)
        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=SequentialPassthroughStep(),
            step_name="SequentialPassthroughStep",
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
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
        CollectorStep get_output_state() → Producer dispatches → StateGatedStep unblocks.
        """
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source",
                    items=[1, 2, 3],
                    outputs={"main": ["collector", "gated"]},
                ),
                StepConfig(type="collector", target_step="gated"),
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

        assert stats._stats["collector"].processed == 3
        assert stats._stats["gated"].processed == 3
        assert stats._stats["terminal"].processed == 3
