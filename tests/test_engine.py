"""Tests: engine.py — StepRegistry + PipelineEngine — W-13, W-T03."""

from __future__ import annotations

from pathlib import Path

import orjson
import pytest
from dummy_steps import (
    BranchEvenOddStep,
    DummySourceStep,
    FilterEvenStep,
    PassthroughStep,
    TerminalStep,
)

from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.engine import PipelineEngine, StepRegistry
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
        assert stats._stats["PassthroughStep"].processed == 10

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
        assert stats._stats["FilterEvenStep"].processed == 10

    @pytest.mark.timeout(30)
    def test_filter_even_result_file(self, tmp_path: Path) -> None:
        """FilterEvenStep — count_result.json written by result.write()."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10)), outputs={"main": "filter_even"}),
                StepConfig(type="filter_even"),
            ],
            {"source": DummySourceStep, "filter_even": FilterEvenStep},
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        result_file = output_dir / "count_result.json"
        assert result_file.exists()
        data = orjson.loads(result_file.read_bytes())
        assert data["passed"] == 5
        assert data["filtered"] == 5

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
        assert stats._stats["PassthroughStep"].processed == 10
        assert stats._stats["FilterEvenStep"].processed == 10

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
        assert "PassthroughStep" not in stats._stats
        assert stats._stats["FilterEvenStep"].processed == 10

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
        assert "DummySourceStep" in step_names
        assert "PassthroughStep" in step_names
        pt = next(d for d in data if d["step_name"] == "PassthroughStep")
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
        assert stats._stats["PassthroughStep"].processed == 0

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
        assert stats._stats["BranchEvenOddStep"].processed == 10
        assert stats._stats["PassthroughStep"].processed == 5
        assert stats._stats["TerminalStep"].processed == 5

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
        assert stats._stats["PassthroughStep"].processed == 5
        assert stats._stats["FilterEvenStep"].processed == 5
        assert stats._stats["TerminalStep"].processed == 5

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
        assert stats._stats["BranchEvenOddStep"].processed == 10
        assert stats._stats["TerminalStep"].processed == 10

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
        assert stats._stats["TerminalStep"].processed == 3

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
        assert stats._stats["PassthroughStep"].processed == 5
        assert stats._stats["PassthroughStep"].emitted == {}
