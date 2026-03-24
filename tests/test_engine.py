"""Tests: engine.py — StepRegistry + PipelineEngine — W-13, W-T03."""

from __future__ import annotations

import logging
from pathlib import Path

import orjson
import pytest
from dummy_steps import (
    BranchEvenOddStep,
    DummySourceStep,
    FilterEvenStep,
    InitialStateStep,
    LifecycleTrackingStep,
    PassthroughStep,
    SlowStep,
    TerminalStep,
)

from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.engine import PipelineEngine
from task_pipeliner.pipeline import StepRegistry
from task_pipeliner.exceptions import ConfigValidationError, StepRegistrationError
from task_pipeliner.stats import StatsCollector

# ---------------------------------------------------------------------------
# StepRegistry
# ---------------------------------------------------------------------------


class TestStepRegistry:
    def test_register_and_get(self) -> None:
        reg = StepRegistry()
        reg.register("passthrough", PassthroughStep)
        assert reg.get("passthrough") is PassthroughStep

    def test_duplicate_raises(self) -> None:
        reg = StepRegistry()
        reg.register("passthrough", PassthroughStep)
        with pytest.raises(StepRegistrationError, match="passthrough"):
            reg.register("passthrough", PassthroughStep)

    def test_get_unregistered_raises(self) -> None:
        reg = StepRegistry()
        with pytest.raises(StepRegistrationError):
            reg.get("nonexistent")

    def test_get_error_lists_available(self) -> None:
        reg = StepRegistry()
        reg.register("alpha", PassthroughStep)
        reg.register("beta", FilterEvenStep)
        with pytest.raises(StepRegistrationError) as exc_info:
            reg.get("gamma")
        msg = str(exc_info.value)
        assert "alpha" in msg
        assert "beta" in msg

    def test_unpicklable_class_raises(self) -> None:
        """Class defined inside function is not picklable — StepRegistrationError."""

        class _Local:
            pass

        reg = StepRegistry()
        with pytest.raises(StepRegistrationError):
            reg.register("local", _Local)


# ---------------------------------------------------------------------------
# PipelineEngine — linear chain (with explicit outputs)
# ---------------------------------------------------------------------------


class TestPipelineEngine:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        queue_size: int = 100,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_single_passthrough(self, tmp_path: Path) -> None:
        """Single PassthroughStep — all items flow through."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 10

    @pytest.mark.timeout(30)
    def test_filter_even_stats(self, tmp_path: Path) -> None:
        """FilterEvenStep — 10 processed, 5 emitted."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10)), outputs={"main": "filter_even"}),
                StepConfig(type="filter_even"),
            ],
            {"source": DummySourceStep, "filter_even": FilterEvenStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["filter_even"].processed == 10

    @pytest.mark.timeout(30)
    def test_multi_step_chain(self, tmp_path: Path) -> None:
        """source → passthrough → filter_even: items flow through all steps."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough", outputs={"main": "filter_even"}),
                StepConfig(type="filter_even"),
            ],
            {
                "source": DummySourceStep,
                "passthrough": PassthroughStep,
                "filter_even": FilterEvenStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 10
        assert stats._stats["filter_even"].processed == 10

    @pytest.mark.timeout(30)
    def test_disabled_step_skipped(self, tmp_path: Path) -> None:
        """enabled=False step is skipped; source routes directly to filter_even."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10)), outputs={"main": "filter_even"}),
                StepConfig(type="passthrough", enabled=False),
                StepConfig(type="filter_even"),
            ],
            {
                "source": DummySourceStep,
                "passthrough": PassthroughStep,
                "filter_even": FilterEvenStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")
        assert "passthrough" not in stats._stats
        assert stats._stats["filter_even"].processed == 10

    @pytest.mark.timeout(30)
    def test_stats_json_written(self, tmp_path: Path) -> None:
        """stats.json created with correct content after run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        step_names = [d["step_name"] for d in data]
        assert "source" in step_names
        assert "passthrough" in step_names
        pt = next(d for d in data if d["step_name"] == "passthrough")
        assert pt["processed"] == 5

    @pytest.mark.timeout(30)
    def test_empty_input(self, tmp_path: Path) -> None:
        """Empty input — 0 processed, no errors."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=[], outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 0

    def test_unregistered_step_in_config(self, tmp_path: Path) -> None:
        """Config references unregistered step → StepRegistrationError."""
        config = PipelineConfig(
            pipeline=[StepConfig(type="source", items=[1]), StepConfig(type="unknown")]
        )
        registry = StepRegistry()
        registry.register("source", DummySourceStep)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        with pytest.raises(StepRegistrationError):
            engine.run(output_dir=tmp_path / "out")

    def test_source_not_first_raises(self, tmp_path: Path) -> None:
        """SOURCE step not at position 0 → ConfigValidationError."""
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="passthrough"),
                StepConfig(type="source", items=[1]),
            ]
        )
        registry = StepRegistry()
        registry.register("passthrough", PassthroughStep)
        registry.register("source", DummySourceStep)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        with pytest.raises(ConfigValidationError):
            engine.run(output_dir=tmp_path / "out")

    def test_no_source_step_raises(self, tmp_path: Path) -> None:
        """No SOURCE step in pipeline → ConfigValidationError."""
        config = PipelineConfig(pipeline=[StepConfig(type="passthrough")])
        registry = StepRegistry()
        registry.register("passthrough", PassthroughStep)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        with pytest.raises(ConfigValidationError):
            engine.run(output_dir=tmp_path / "out")


# ---------------------------------------------------------------------------
# PipelineEngine — DAG topology (W-T03)
# ---------------------------------------------------------------------------


class TestPipelineEngineDAG:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        queue_size: int = 100,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_branching_even_odd(self, tmp_path: Path) -> None:
        """source → branch → (even → passthrough, odd → terminal).

        Items 0-9: evens (0,2,4,6,8) → passthrough, odds (1,3,5,7,9) → terminal.
        """
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10)), outputs={"main": "branch"}),
                StepConfig(type="branch", outputs={"even": "passthrough", "odd": "terminal"}),
                StepConfig(type="passthrough"),
                StepConfig(type="terminal"),
            ],
            {
                "source": DummySourceStep,
                "branch": BranchEvenOddStep,
                "passthrough": PassthroughStep,
                "terminal": TerminalStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["branch"].processed == 10
        assert stats._stats["passthrough"].processed == 5
        assert stats._stats["terminal"].processed == 5

    @pytest.mark.timeout(30)
    def test_fan_out(self, tmp_path: Path) -> None:
        """source → passthrough (main → [filter_even, terminal]): fan-out.

        Both filter_even and terminal receive all 5 items.
        """
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough", outputs={"main": ["filter_even", "terminal"]}),
                StepConfig(type="filter_even"),
                StepConfig(type="terminal"),
            ],
            {
                "source": DummySourceStep,
                "passthrough": PassthroughStep,
                "filter_even": FilterEvenStep,
                "terminal": TerminalStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 5
        assert stats._stats["filter_even"].processed == 5
        assert stats._stats["terminal"].processed == 5

    @pytest.mark.timeout(30)
    def test_fan_in(self, tmp_path: Path) -> None:
        """source → branch → (even → terminal, odd → terminal): fan-in.

        Terminal receives all 10 items via two input queues.
        """
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10)), outputs={"main": "branch"}),
                StepConfig(type="branch", outputs={"even": "terminal", "odd": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {
                "source": DummySourceStep,
                "branch": BranchEvenOddStep,
                "terminal": TerminalStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["branch"].processed == 10
        assert stats._stats["terminal"].processed == 10

    @pytest.mark.timeout(30)
    def test_terminal_no_output_queues(self, tmp_path: Path) -> None:
        """Terminal step (outputs=()) gets empty output_queues."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(3)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {"source": DummySourceStep, "terminal": TerminalStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["terminal"].processed == 3

    @pytest.mark.timeout(30)
    def test_unconnected_tag_dropped(self, tmp_path: Path) -> None:
        """Step has outputs=('main',) but config doesn't connect 'main' → silent drop."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),  # no outputs in config → emits are dropped
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 5
        assert stats._stats["passthrough"].emitted == {}


# ---------------------------------------------------------------------------
# M-03: initial_state integration
# ---------------------------------------------------------------------------


class TestInitialStateIntegration:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        queue_size: int = 0,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_initial_state_passed_to_sequential_producer(self, tmp_path: Path) -> None:
        """InitialStateStep provides initial_state={'count': 0}, mutates during process()."""
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source", items=list(range(5)), outputs={"main": "stateful"}
                ),
                StepConfig(type="stateful"),
            ],
            {"source": DummySourceStep, "stateful": InitialStateStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["stateful"].processed == 5


# ---------------------------------------------------------------------------
# M-06: SequentialProducer timing instrumentation
# ---------------------------------------------------------------------------


class TestSequentialProducerTiming:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        queue_size: int = 0,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_sequential_processing_ns(self, tmp_path: Path) -> None:
        """SEQUENTIAL step has processing_ns > 0 after pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source", items=list(range(3)),
                    outputs={"main": "slow"},
                ),
                StepConfig(type="slow", sleep_seconds=0.01),
            ],
            {"source": DummySourceStep, "slow": SlowStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["slow"].processing_ns > 0

    @pytest.mark.timeout(30)
    def test_sequential_first_item_at(self, tmp_path: Path) -> None:
        """SEQUENTIAL step has first_item_at set after pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {"source": DummySourceStep, "terminal": TerminalStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["terminal"].first_item_at is not None

    @pytest.mark.timeout(30)
    def test_sequential_idle_ns(self, tmp_path: Path) -> None:
        """SEQUENTIAL step has idle_ns > 0 when upstream is slow."""
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source", items=list(range(3)),
                    outputs={"main": "slow"},
                ),
                StepConfig(
                    type="slow", sleep_seconds=0.01,
                    outputs={"main": "terminal"},
                ),
                StepConfig(type="terminal"),
            ],
            {"source": DummySourceStep, "slow": SlowStep, "terminal": TerminalStep},
        )
        engine.run(output_dir=tmp_path / "out")
        # terminal waits for slow upstream → idle_ns > 0
        assert stats._stats["terminal"].idle_ns > 0

    @pytest.mark.timeout(30)
    def test_sequential_current_state_done(self, tmp_path: Path) -> None:
        """SEQUENTIAL step has current_state == 'done' after pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {"source": DummySourceStep, "terminal": TerminalStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["terminal"].current_state == "done"


# ---------------------------------------------------------------------------
# M-07: ParallelProducer timing instrumentation
# ---------------------------------------------------------------------------


class TestParallelProducerTiming:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 2,
        queue_size: int = 0,
        chunk_size: int = 3,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_parallel_processing_ns(self, tmp_path: Path) -> None:
        """PARALLEL step has processing_ns > 0 after pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source", items=list(range(3)),
                    outputs={"main": "slow"},
                ),
                StepConfig(type="slow", sleep_seconds=0.01),
            ],
            {"source": DummySourceStep, "slow": SlowStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["slow"].processing_ns > 0

    @pytest.mark.timeout(30)
    def test_parallel_first_item_at(self, tmp_path: Path) -> None:
        """PARALLEL step has first_item_at set after pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].first_item_at is not None

    @pytest.mark.timeout(30)
    def test_parallel_current_state_done(self, tmp_path: Path) -> None:
        """PARALLEL step has current_state == 'done' after pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].current_state == "done"

    @pytest.mark.timeout(30)
    def test_parallel_regression_all_items(self, tmp_path: Path) -> None:
        """All items still processed correctly with interleaved collection."""
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source", items=list(range(20)), outputs={"main": "filter_even"}
                ),
                StepConfig(type="filter_even"),
            ],
            {"source": DummySourceStep, "filter_even": FilterEvenStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["filter_even"].processed == 20


# ---------------------------------------------------------------------------
# M-08: InputProducer instrumentation
# ---------------------------------------------------------------------------


class TestInputProducerTiming:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        queue_size: int = 0,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_source_current_state_done(self, tmp_path: Path) -> None:
        """SOURCE step has current_state == 'done' after pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {"source": DummySourceStep, "terminal": TerminalStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["source"].current_state == "done"

    @pytest.mark.timeout(30)
    def test_source_first_item_at(self, tmp_path: Path) -> None:
        """SOURCE step has first_item_at set after pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {"source": DummySourceStep, "terminal": TerminalStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["source"].first_item_at is not None


# ---------------------------------------------------------------------------
# M-11: Engine integration + log separation
# ---------------------------------------------------------------------------


class TestEngineProgressIntegration:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        queue_size: int = 0,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_progress_log_created(self, tmp_path: Path) -> None:
        """progress.log file is created in output_dir during pipeline run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {"source": DummySourceStep, "terminal": TerminalStep},
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        assert (output_dir / "progress.log").exists()

    @pytest.mark.timeout(30)
    def test_propagate_restored_after_run(self, tmp_path: Path) -> None:
        """task_pipeliner logger propagate is restored after pipeline run."""
        parent_logger = logging.getLogger("task_pipeliner")
        original_propagate = parent_logger.propagate
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(3)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            {"source": DummySourceStep, "terminal": TerminalStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert parent_logger.propagate == original_propagate


# ---------------------------------------------------------------------------
# Step open/close lifecycle
# ---------------------------------------------------------------------------


class TestStepOpenLifecycle:
    """open() is called before processing begins, close() after."""

    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        queue_size: int = 0,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_sequential_step_open_called(self, tmp_path: Path) -> None:
        """SEQUENTIAL step's open() is called before process() begins."""
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source", items=list(range(3)), outputs={"main": "lifecycle"}
                ),
                StepConfig(type="lifecycle"),
            ],
            {"source": DummySourceStep, "lifecycle": LifecycleTrackingStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["lifecycle"].processed == 3


# ---------------------------------------------------------------------------
# Multi-instance same-type steps (name/type separation)
# ---------------------------------------------------------------------------


class TestMultiInstanceSameType:
    def _make_engine(
        self,
        steps: list[StepConfig],
        registry_map: dict[str, type],
        *,
        workers: int = 1,
        queue_size: int = 0,
        chunk_size: int = 50,
    ) -> tuple[PipelineEngine, StatsCollector]:
        config = PipelineConfig(
            pipeline=steps,
            execution=ExecutionConfig(
                workers=workers, queue_size=queue_size, chunk_size=chunk_size
            ),
        )
        registry = StepRegistry()
        for name, cls in registry_map.items():
            registry.register(name, cls)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        return engine, stats

    @pytest.mark.timeout(30)
    def test_two_terminals_same_type_different_names(
        self, tmp_path: Path
    ) -> None:
        """Same type registered once, used twice with different names."""
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source",
                    items=list(range(10)),
                    outputs={"main": "branch"},
                ),
                StepConfig(
                    type="branch",
                    outputs={"even": "term_even", "odd": "term_odd"},
                ),
                StepConfig(type="terminal", name="term_even"),
                StepConfig(type="terminal", name="term_odd"),
            ],
            {
                "source": DummySourceStep,
                "branch": BranchEvenOddStep,
                "terminal": TerminalStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["term_even"].processed == 5
        assert stats._stats["term_odd"].processed == 5

    @pytest.mark.timeout(30)
    def test_two_passthroughs_same_type_chain(
        self, tmp_path: Path
    ) -> None:
        """Chain: source → pass1 → pass2 → terminal."""
        engine, stats = self._make_engine(
            [
                StepConfig(
                    type="source",
                    items=list(range(5)),
                    outputs={"main": "pass1"},
                ),
                StepConfig(
                    type="passthrough",
                    name="pass1",
                    outputs={"main": "pass2"},
                ),
                StepConfig(
                    type="passthrough",
                    name="pass2",
                    outputs={"main": "terminal"},
                ),
                StepConfig(type="terminal"),
            ],
            {
                "source": DummySourceStep,
                "passthrough": PassthroughStep,
                "terminal": TerminalStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["pass1"].processed == 5
        assert stats._stats["pass2"].processed == 5
        assert stats._stats["terminal"].processed == 5

    def test_duplicate_name_without_explicit_name_raises(self) -> None:
        """Two steps with same type and no name → duplicate name error."""
        with pytest.raises(Exception, match="duplicate step name"):
            PipelineConfig(
                pipeline=[
                    StepConfig(type="terminal"),
                    StepConfig(type="terminal"),
                ],
            )
