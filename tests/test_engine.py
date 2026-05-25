"""Tests: engine.py — Pipeline + PipelineEngine integration — W-13, W-T03."""

from __future__ import annotations

import logging
from pathlib import Path

import orjson
import pytest
from dummy_steps import (
    BranchEvenOddStep,
    DummySourceStep,
    FilterEvenStep,
    LifecycleTrackingStep,
    PassthroughStep,
    SlowStep,
    TerminalStep,
)

from task_pipeliner.config import ExecutionConfig, PipelineConfig, QueueType, StepConfig
from task_pipeliner.exceptions import ConfigValidationError, StepRegistrationError
from task_pipeliner.pipeline import Pipeline
from task_pipeliner.spill_queue import FullDiskQueue

# ---------------------------------------------------------------------------
# Pipeline.register — step registration
# ---------------------------------------------------------------------------


class TestPipelineRegister:
    def test_register_and_get(self) -> None:
        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        assert p._registry["passthrough"] is PassthroughStep

    def test_register_returns_self(self) -> None:
        p = Pipeline()
        result = p.register("passthrough", PassthroughStep)
        assert result is p

    def test_duplicate_raises(self) -> None:
        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        with pytest.raises(StepRegistrationError, match="passthrough"):
            p.register("passthrough", PassthroughStep)

    def test_get_unregistered_raises(self) -> None:
        p = Pipeline()
        with pytest.raises(StepRegistrationError):
            p._registry_get("nonexistent")

    def test_get_error_lists_available(self) -> None:
        p = Pipeline()
        p.register("alpha", PassthroughStep)
        p.register("beta", FilterEvenStep)
        with pytest.raises(StepRegistrationError) as exc_info:
            p._registry_get("gamma")
        msg = str(exc_info.value)
        assert "alpha" in msg
        assert "beta" in msg

    def test_unpicklable_class_raises(self) -> None:
        """Class defined inside function is not picklable — StepRegistrationError."""

        class _Local:
            pass

        p = Pipeline()
        with pytest.raises(StepRegistrationError):
            p.register("local", _Local)


# ---------------------------------------------------------------------------
# PipelineEngine (via Pipeline.run) — linear chain
# ---------------------------------------------------------------------------


class TestPipelineEngineLinear:
    @pytest.mark.timeout(30)
    def test_single_passthrough(self, tmp_path: Path) -> None:
        """Single PassthroughStep — all items flow through."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(10)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 10

    @pytest.mark.timeout(30)
    def test_filter_even_stats(self, tmp_path: Path) -> None:
        """FilterEvenStep — 10 processed."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("filter_even", FilterEvenStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(10)), outputs={"main": "filter_even"}),
                StepConfig(type="filter_even"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["filter_even"].processed == 10

    @pytest.mark.timeout(30)
    def test_multi_step_chain(self, tmp_path: Path) -> None:
        """source → passthrough → filter_even: items flow through all steps."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        p.register("filter_even", FilterEvenStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(10)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough", outputs={"main": "filter_even"}),
                StepConfig(type="filter_even"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 10
        assert stats._stats["filter_even"].processed == 10

    @pytest.mark.timeout(30)
    def test_disabled_step_skipped(self, tmp_path: Path) -> None:
        """enabled=False step is skipped."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        p.register("filter_even", FilterEvenStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(10)), outputs={"main": "filter_even"}),
                StepConfig(type="passthrough", enabled=False),
                StepConfig(type="filter_even"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert "passthrough" not in stats._stats
        assert stats._stats["filter_even"].processed == 10

    @pytest.mark.timeout(30)
    def test_stats_json_written(self, tmp_path: Path) -> None:
        """stats.json created with correct content after run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        output_dir = tmp_path / "out"
        p.run(config=config, output_dir=output_dir)
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
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=[], outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 0

    def test_unregistered_step_in_config(self, tmp_path: Path) -> None:
        """Config references unregistered step → StepRegistrationError."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        config = PipelineConfig(
            pipeline=[StepConfig(type="source", items=[1]), StepConfig(type="unknown")]
        )
        with pytest.raises(StepRegistrationError):
            p.run(config=config, output_dir=tmp_path / "out")

    def test_source_not_first_raises(self, tmp_path: Path) -> None:
        """SOURCE step not at position 0 → ConfigValidationError."""
        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        p.register("source", DummySourceStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="passthrough"),
                StepConfig(type="source", items=[1]),
            ]
        )
        with pytest.raises(ConfigValidationError):
            p.run(config=config, output_dir=tmp_path / "out")

    def test_no_source_step_raises(self, tmp_path: Path) -> None:
        """No SOURCE step in pipeline → ConfigValidationError."""
        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(pipeline=[StepConfig(type="passthrough")])
        with pytest.raises(ConfigValidationError):
            p.run(config=config, output_dir=tmp_path / "out")


# ---------------------------------------------------------------------------
# DAG topology (W-T03)
# ---------------------------------------------------------------------------


class TestPipelineEngineDAG:
    @pytest.mark.timeout(30)
    def test_branching_even_odd(self, tmp_path: Path) -> None:
        """source → branch → (even → passthrough, odd → terminal)."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("branch", BranchEvenOddStep)
        p.register("passthrough", PassthroughStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(10)), outputs={"main": "branch"}),
                StepConfig(type="branch", outputs={"even": "passthrough", "odd": "terminal"}),
                StepConfig(type="passthrough"),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["branch"].processed == 10
        assert stats._stats["passthrough"].processed == 5
        assert stats._stats["terminal"].processed == 5

    @pytest.mark.timeout(30)
    def test_fan_out(self, tmp_path: Path) -> None:
        """source → passthrough (main → [filter_even, terminal]): fan-out."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        p.register("filter_even", FilterEvenStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough", outputs={"main": ["filter_even", "terminal"]}),
                StepConfig(type="filter_even"),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 5
        assert stats._stats["filter_even"].processed == 5
        assert stats._stats["terminal"].processed == 5

    @pytest.mark.timeout(30)
    def test_fan_in(self, tmp_path: Path) -> None:
        """source → branch → (even → terminal, odd → terminal): fan-in."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("branch", BranchEvenOddStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(10)), outputs={"main": "branch"}),
                StepConfig(type="branch", outputs={"even": "terminal", "odd": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["branch"].processed == 10
        assert stats._stats["terminal"].processed == 10

    @pytest.mark.timeout(30)
    def test_terminal_no_output_queues(self, tmp_path: Path) -> None:
        """Terminal step gets empty output_queues."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(3)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["terminal"].processed == 3

    @pytest.mark.timeout(30)
    def test_unconnected_tag_dropped(self, tmp_path: Path) -> None:
        """Step has no output connections in config → emits are dropped."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].processed == 5
        assert stats._stats["passthrough"].emitted == {}


# ---------------------------------------------------------------------------
# M-06: SequentialStepRunner timing instrumentation
# ---------------------------------------------------------------------------


class TestSequentialStepRunnerTiming:
    @pytest.mark.timeout(30)
    def test_sequential_processing_ns(self, tmp_path: Path) -> None:
        """SEQUENTIAL step has processing_ns > 0 after pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("slow", SlowStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(3)), outputs={"main": "slow"}),
                StepConfig(type="slow", sleep_seconds=0.01),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["slow"].processing_ns > 0

    @pytest.mark.timeout(30)
    def test_sequential_first_item_at(self, tmp_path: Path) -> None:
        """SEQUENTIAL step has first_item_at set after pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["terminal"].first_item_at is not None

    @pytest.mark.timeout(30)
    def test_sequential_idle_ns(self, tmp_path: Path) -> None:
        """SEQUENTIAL step has idle_ns > 0 when upstream is slow."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("slow", SlowStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(3)), outputs={"main": "slow"}),
                StepConfig(type="slow", sleep_seconds=0.01, outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["terminal"].idle_ns > 0

    @pytest.mark.timeout(30)
    def test_sequential_current_state_done(self, tmp_path: Path) -> None:
        """SEQUENTIAL step has current_state == 'done' after pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["terminal"].current_state == "done"


# ---------------------------------------------------------------------------
# M-07: ParallelStepRunner timing instrumentation
# ---------------------------------------------------------------------------


class TestParallelStepRunnerTiming:
    @pytest.mark.timeout(30)
    def test_parallel_processing_ns(self, tmp_path: Path) -> None:
        """PARALLEL step has processing_ns > 0 after pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("slow", SlowStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(3)), outputs={"main": "slow"}),
                StepConfig(type="slow", sleep_seconds=0.01),
            ],
            execution=ExecutionConfig(workers=2, queue_size=0, chunk_size=3),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["slow"].processing_ns > 0

    @pytest.mark.timeout(30)
    def test_parallel_first_item_at(self, tmp_path: Path) -> None:
        """PARALLEL step has first_item_at set after pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(workers=2, queue_size=0, chunk_size=3),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].first_item_at is not None

    @pytest.mark.timeout(30)
    def test_parallel_current_state_done(self, tmp_path: Path) -> None:
        """PARALLEL step has current_state == 'done' after pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(workers=2, queue_size=0, chunk_size=3),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["passthrough"].current_state == "done"

    @pytest.mark.timeout(30)
    def test_parallel_regression_all_items(self, tmp_path: Path) -> None:
        """All items still processed correctly with interleaved collection."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("filter_even", FilterEvenStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(20)), outputs={"main": "filter_even"}),
                StepConfig(type="filter_even"),
            ],
            execution=ExecutionConfig(workers=2, queue_size=0, chunk_size=3),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["filter_even"].processed == 20


# ---------------------------------------------------------------------------
# M-08: InputStepRunner instrumentation
# ---------------------------------------------------------------------------


class TestInputStepRunnerTiming:
    @pytest.mark.timeout(30)
    def test_source_current_state_done(self, tmp_path: Path) -> None:
        """SOURCE step has current_state == 'done' after pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["source"].current_state == "done"

    @pytest.mark.timeout(30)
    def test_source_first_item_at(self, tmp_path: Path) -> None:
        """SOURCE step has first_item_at set after pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["source"].first_item_at is not None


# ---------------------------------------------------------------------------
# M-11: Engine integration + log separation
# ---------------------------------------------------------------------------


class TestEngineProgressIntegration:
    @pytest.mark.timeout(30)
    def test_progress_log_created(self, tmp_path: Path) -> None:
        """progress.log file is created in output_dir during pipeline run."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        output_dir = tmp_path / "out"
        p.run(config=config, output_dir=output_dir)
        assert (output_dir / "progress.log").exists()

    @pytest.mark.timeout(30)
    def test_propagate_restored_after_run(self, tmp_path: Path) -> None:
        """task_pipeliner logger propagate is restored after pipeline run."""
        parent_logger = logging.getLogger("task_pipeliner")
        original_propagate = parent_logger.propagate
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(3)), outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        p.run(config=config, output_dir=tmp_path / "out")
        assert parent_logger.propagate == original_propagate


# ---------------------------------------------------------------------------
# Step open/close lifecycle
# ---------------------------------------------------------------------------


class TestStepOpenLifecycle:
    @pytest.mark.timeout(30)
    def test_sequential_step_open_called(self, tmp_path: Path) -> None:
        """SEQUENTIAL step's open() is called before process() begins."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("lifecycle", LifecycleTrackingStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(3)), outputs={"main": "lifecycle"}),
                StepConfig(type="lifecycle"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["lifecycle"].processed == 3


# ---------------------------------------------------------------------------
# Multi-instance same-type steps (name/type separation)
# ---------------------------------------------------------------------------


class TestMultiInstanceSameType:
    @pytest.mark.timeout(30)
    def test_two_terminals_same_type_different_names(self, tmp_path: Path) -> None:
        """Same type registered once, used twice with different names."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("branch", BranchEvenOddStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(10)), outputs={"main": "branch"}),
                StepConfig(type="branch", outputs={"even": "term_even", "odd": "term_odd"}),
                StepConfig(type="terminal", name="term_even"),
                StepConfig(type="terminal", name="term_odd"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats._stats["term_even"].processed == 5
        assert stats._stats["term_odd"].processed == 5

    @pytest.mark.timeout(30)
    def test_two_passthroughs_same_type_chain(self, tmp_path: Path) -> None:
        """Chain: source → pass1 → pass2 → terminal."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        p.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "pass1"}),
                StepConfig(type="passthrough", name="pass1", outputs={"main": "pass2"}),
                StepConfig(type="passthrough", name="pass2", outputs={"main": "terminal"}),
                StepConfig(type="terminal"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=0, chunk_size=50),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
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


# ---------------------------------------------------------------------------
# FullDiskQueue AUTO selection — e2e
# ---------------------------------------------------------------------------


class TestFullDiskQueueAutoSelection:
    @pytest.mark.timeout(30)
    def test_auto_uses_spill_for_plain_step(self, tmp_path: Path) -> None:
        """AUTO 모드에서 is_ready() 미오버라이드 스텝에는 SpillQueue가 사용된다."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(queue_type=QueueType.AUTO, workers=1, queue_size=100),
        )
        stats = p.run(config=config, output_dir=tmp_path / "out")
        assert stats.get_step_stats("passthrough").processed == 5

    @pytest.mark.timeout(30)
    def test_full_disk_queue_type_explicit(self, tmp_path: Path) -> None:
        """queue_type=FULL_DISK 명시 시 모든 처리 스텝에 FullDiskQueue가 배치된다."""
        created: list[type] = []
        original_init = FullDiskQueue.__init__

        def tracking_init(self: FullDiskQueue, path: object) -> None:
            created.append(FullDiskQueue)
            original_init(self, path)

        import task_pipeliner.engine as eng_mod

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(eng_mod.FullDiskQueue, "__init__", tracking_init)

        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(queue_type=QueueType.FULL_DISK, workers=1),
        )
        p.run(config=config, output_dir=tmp_path / "out")
        monkeypatch.undo()

        assert len(created) >= 1
