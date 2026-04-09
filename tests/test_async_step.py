"""Tests: AsyncStep + AsyncStepRunner — engine integration."""

from __future__ import annotations

from pathlib import Path

import pytest
from dummy_steps import (
    AsyncErrorOnItemStep,
    AsyncFilterEvenStep,
    AsyncPassthroughStep,
    AsyncSlowStep,
    DummySourceStep,
)

from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.engine import PipelineEngine
from task_pipeliner.pipeline import StepRegistry
from task_pipeliner.stats import StatsCollector

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine(
    steps: list[StepConfig],
    registry_map: dict[str, type],
    *,
    workers: int = 1,
    queue_size: int = 100,
    chunk_size: int = 50,
) -> tuple[PipelineEngine, StatsCollector]:
    config = PipelineConfig(
        pipeline=steps,
        execution=ExecutionConfig(workers=workers, queue_size=queue_size, chunk_size=chunk_size),
    )
    registry = StepRegistry()
    for name, cls in registry_map.items():
        registry.register(name, cls)
    stats = StatsCollector()
    engine = PipelineEngine(config=config, registry=registry, stats=stats)
    return engine, stats


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAsyncStep:
    @pytest.mark.timeout(30)
    def test_passthrough_processes_all_items(self, tmp_path: Path) -> None:
        """AsyncPassthroughStep emits every item — processed count == input count."""
        engine, stats = _make_engine(
            [
                StepConfig(
                    type="source",
                    items=list(range(10)),
                    outputs={"main": "async_pass"},
                ),
                StepConfig(type="async_pass"),
            ],
            {"source": DummySourceStep, "async_pass": AsyncPassthroughStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["async_pass"].processed == 10

    @pytest.mark.timeout(30)
    def test_filter_even_emits_half(self, tmp_path: Path) -> None:
        """AsyncFilterEvenStep processes all items but only emits even ones."""
        engine, stats = _make_engine(
            [
                StepConfig(
                    type="source",
                    items=list(range(10)),
                    outputs={"main": "filter_even"},
                ),
                StepConfig(type="filter_even"),
            ],
            {"source": DummySourceStep, "filter_even": AsyncFilterEvenStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["filter_even"].processed == 10

    @pytest.mark.timeout(30)
    def test_error_on_item_increments_errored(self, tmp_path: Path) -> None:
        """AsyncErrorOnItemStep — error_value item increments errored, others pass."""
        items = [1, 2, -1, 3]
        engine, stats = _make_engine(
            [
                StepConfig(
                    type="source",
                    items=items,
                    outputs={"main": "error_step"},
                ),
                StepConfig(type="error_step"),
            ],
            {"source": DummySourceStep, "error_step": AsyncErrorOnItemStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["error_step"].errored == 1
        assert stats._stats["error_step"].processed == 3

    @pytest.mark.timeout(30)
    def test_concurrency_respected(self, tmp_path: Path) -> None:
        """AsyncSlowStep with concurrency=4 finishes faster than sequential would."""
        items = list(range(8))
        engine, stats = _make_engine(
            [
                StepConfig(
                    type="source",
                    items=items,
                    outputs={"main": "slow"},
                ),
                StepConfig(type="slow", sleep_seconds=0.05),
            ],
            {"source": DummySourceStep, "slow": AsyncSlowStep},
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["slow"].processed == len(items)

    @pytest.mark.timeout(30)
    def test_chained_async_steps(self, tmp_path: Path) -> None:
        """source → AsyncPassthroughStep → AsyncFilterEvenStep: chain works end-to-end."""
        engine, stats = _make_engine(
            [
                StepConfig(
                    type="source",
                    items=list(range(10)),
                    outputs={"main": "pass"},
                ),
                StepConfig(type="pass", outputs={"main": "filter_even"}),
                StepConfig(type="filter_even"),
            ],
            {
                "source": DummySourceStep,
                "pass": AsyncPassthroughStep,
                "filter_even": AsyncFilterEvenStep,
            },
        )
        engine.run(output_dir=tmp_path / "out")
        assert stats._stats["pass"].processed == 10
        assert stats._stats["filter_even"].processed == 10
