"""Tests: engine.py — StepRegistry + PipelineEngine — W-13."""

from __future__ import annotations

from pathlib import Path

import orjson
import pytest
from dummy_steps import DummySourceStep, FilterEvenStep, PassthroughStep

from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.engine import PipelineEngine, StepRegistry
from task_pipeliner.exceptions import StepRegistrationError
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
# PipelineEngine
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
                StepConfig(type="source", items=list(range(10))),
                StepConfig(type="passthrough"),
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        assert stats._stats["PassthroughStep"].passed == 10

    @pytest.mark.timeout(30)
    def test_filter_even_stats(self, tmp_path: Path) -> None:
        """FilterEvenStep — 5 even pass, 5 odd filtered."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10))),
                StepConfig(type="filter_even"),
            ],
            {"source": DummySourceStep, "filter_even": FilterEvenStep},
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        assert stats._stats["FilterEvenStep"].passed == 5

    @pytest.mark.timeout(30)
    def test_filter_even_result_file(self, tmp_path: Path) -> None:
        """FilterEvenStep — count_result.json written by result.write()."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10))),
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
                StepConfig(type="source", items=list(range(10))),
                StepConfig(type="passthrough"),
                StepConfig(type="filter_even"),
            ],
            {
                "source": DummySourceStep,
                "passthrough": PassthroughStep,
                "filter_even": FilterEvenStep,
            },
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        assert stats._stats["PassthroughStep"].passed == 10
        assert stats._stats["FilterEvenStep"].passed == 5

    @pytest.mark.timeout(30)
    def test_disabled_step_skipped(self, tmp_path: Path) -> None:
        """enabled=False step is not registered in stats."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(10))),
                StepConfig(type="passthrough", enabled=False),
                StepConfig(type="filter_even"),
            ],
            {
                "source": DummySourceStep,
                "passthrough": PassthroughStep,
                "filter_even": FilterEvenStep,
            },
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        assert "PassthroughStep" not in stats._stats
        assert stats._stats["FilterEvenStep"].passed == 5

    @pytest.mark.timeout(30)
    def test_stats_json_written(self, tmp_path: Path) -> None:
        """stats.json created with correct content after run."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=list(range(5))),
                StepConfig(type="passthrough"),
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        # SOURCE step + PassthroughStep
        step_names = [d["step_name"] for d in data]
        assert "DummySourceStep" in step_names
        assert "PassthroughStep" in step_names
        pt = next(d for d in data if d["step_name"] == "PassthroughStep")
        assert pt["passed"] == 5

    @pytest.mark.timeout(30)
    def test_empty_input(self, tmp_path: Path) -> None:
        """Empty input — 0 passed, no errors."""
        engine, stats = self._make_engine(
            [
                StepConfig(type="source", items=[]),
                StepConfig(type="passthrough"),
            ],
            {"source": DummySourceStep, "passthrough": PassthroughStep},
        )
        output_dir = tmp_path / "out"
        engine.run(output_dir=output_dir)
        assert stats._stats["PassthroughStep"].passed == 0

    def test_unregistered_step_in_config(self, tmp_path: Path) -> None:
        """Config references unregistered step → StepRegistrationError."""
        config = PipelineConfig(
            pipeline=[StepConfig(type="source", items=[1]), StepConfig(type="unknown")]
        )
        registry = StepRegistry()
        registry.register("source", DummySourceStep)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        output_dir = tmp_path / "out"
        with pytest.raises(StepRegistrationError):
            engine.run(output_dir=output_dir)

    def test_source_not_first_raises(self, tmp_path: Path) -> None:
        """SOURCE step not at position 0 → ConfigValidationError."""
        from task_pipeliner.exceptions import ConfigValidationError

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
        output_dir = tmp_path / "out"
        with pytest.raises(ConfigValidationError):
            engine.run(output_dir=output_dir)

    def test_no_source_step_raises(self, tmp_path: Path) -> None:
        """No SOURCE step in pipeline → ConfigValidationError."""
        from task_pipeliner.exceptions import ConfigValidationError

        config = PipelineConfig(pipeline=[StepConfig(type="passthrough")])
        registry = StepRegistry()
        registry.register("passthrough", PassthroughStep)
        stats = StatsCollector()
        engine = PipelineEngine(config=config, registry=registry, stats=stats)
        output_dir = tmp_path / "out"
        with pytest.raises(ConfigValidationError):
            engine.run(output_dir=output_dir)
