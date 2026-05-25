"""Tests: Programmable API — code-path pipeline assembly via Pipeline.step() + pipe().

Phase 12 (R2-09): YAML 없이 Python 코드만으로 파이프라인을 조립·실행하는 경로 검증.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from dummy_steps import (
    BranchEvenOddStep,
    DummySourceStep,
    FilterEvenStep,
    PassthroughStep,
    TerminalStep,
)

from task_pipeliner.exceptions import StepRegistrationError
from task_pipeliner.pipeline import Pipeline

# ---------------------------------------------------------------------------
# Pipeline.step() — step 생성 및 인스턴스 반환
# ---------------------------------------------------------------------------


class TestPipelineStep:
    def test_step_returns_instance(self) -> None:
        """pipeline.step() returns the created step instance."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        step = p.step("src", "source", items=[1, 2, 3])
        assert step is not None
        assert isinstance(step, DummySourceStep)

    def test_step_stored_in_steps(self) -> None:
        """Created step is stored in pipeline._steps."""
        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        step = p.step("my_pass", "passthrough")
        assert p._steps["my_pass"] is step

    def test_step_duplicate_name_raises(self) -> None:
        """Creating two steps with the same name raises StepRegistrationError."""
        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        p.step("pass1", "passthrough")
        with pytest.raises(StepRegistrationError):
            p.step("pass1", "passthrough")

    def test_step_unregistered_type_raises(self) -> None:
        """Creating step with unregistered type raises StepRegistrationError."""
        p = Pipeline()
        with pytest.raises(StepRegistrationError):
            p.step("src", "unknown_type")

    def test_step_kwargs_passed_to_constructor(self) -> None:
        """Extra kwargs are forwarded to the step class constructor."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        step = p.step("src", "source", items=[10, 20])
        assert isinstance(step, DummySourceStep)
        assert step._items == [10, 20]


# ---------------------------------------------------------------------------
# StepBase.pipe() — 연결 구성
# ---------------------------------------------------------------------------


class TestPipeMethod:
    def test_pipe_returns_target(self) -> None:
        """pipe() returns the target step for chaining."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        src = p.step("src", "source")
        pass_ = p.step("pass", "passthrough")
        result = src.pipe(pass_)
        assert result is pass_

    def test_pipe_chain(self) -> None:
        """src.pipe(a).pipe(b) sets up src→a and a→b connections."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        p.register("terminal", TerminalStep)
        src = p.step("src", "source")
        pass_ = p.step("pass", "passthrough")
        term = p.step("term", "terminal")
        src.pipe(pass_).pipe(term)

        graph = p._build_graph()
        assert graph.connections.get("src", {}).get("main") == ["pass"]
        assert graph.connections.get("pass", {}).get("main") == ["term"]

    def test_pipe_with_tag(self) -> None:
        """pipe(step, tag='kept') creates connection on 'kept' tag."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        src = p.step("src", "source")
        pass_ = p.step("pass", "passthrough")
        src.pipe(pass_, tag="kept")

        graph = p._build_graph()
        assert "kept" in graph.connections.get("src", {})
        assert graph.connections["src"]["kept"] == ["pass"]

    def test_pipe_fan_out(self) -> None:
        """Two pipe() calls from same step = fan-out."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        p.register("terminal", TerminalStep)
        src = p.step("src", "source")
        pass_ = p.step("pass", "passthrough")
        term = p.step("term", "terminal")
        src.pipe(pass_, tag="default")
        src.pipe(term, tag="default")

        graph = p._build_graph()
        targets = graph.connections["src"]["default"]
        assert "pass" in targets
        assert "term" in targets

    def test_pipe_unregistered_step_skipped(self) -> None:
        """Piping to a step not in pipeline._steps logs warning but doesn't crash."""
        p = Pipeline()
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        src = p.step("src", "source")
        # Create a step instance without registering in pipeline
        orphan = PassthroughStep()
        src.pipe(orphan)
        # Build graph — orphan target is not in _steps, so it's skipped
        graph = p._build_graph()
        assert "src" not in graph.connections or not graph.connections.get("src", {})


# ---------------------------------------------------------------------------
# Code-path pipeline execution — end-to-end
# ---------------------------------------------------------------------------


class TestCodePathExecution:
    @pytest.mark.timeout(30)
    def test_linear_chain_runs(self, tmp_path: Path) -> None:
        """source → passthrough → terminal: code-path end-to-end."""
        p = Pipeline(workers=1, queue_size=100, chunk_size=50)
        p.register("source", DummySourceStep)
        p.register("passthrough", PassthroughStep)
        p.register("terminal", TerminalStep)

        src = p.step("src", "source", items=list(range(5)))
        pass_ = p.step("pass", "passthrough")
        term = p.step("term", "terminal")
        src.pipe(pass_).pipe(term)

        stats = p.run(output_dir=tmp_path / "out")

        assert stats._stats["src"].processed == 5
        assert stats._stats["pass"].processed == 5
        assert stats._stats["term"].processed == 5

    @pytest.mark.timeout(30)
    def test_filter_reduces_items(self, tmp_path: Path) -> None:
        """source → FilterEvenStep: only even items reach terminal."""
        p = Pipeline(workers=1, queue_size=100, chunk_size=50)
        p.register("source", DummySourceStep)
        p.register("filter", FilterEvenStep)
        p.register("terminal", TerminalStep)

        src = p.step("src", "source", items=list(range(10)))
        filt = p.step("filt", "filter")
        term = p.step("term", "terminal")
        src.pipe(filt).pipe(term)

        stats = p.run(output_dir=tmp_path / "out")

        assert stats._stats["filt"].processed == 10
        assert stats._stats["term"].processed == 5

    @pytest.mark.timeout(30)
    def test_branching_fan_out(self, tmp_path: Path) -> None:
        """source → branch → (even → terminal1, odd → terminal2): fan-out."""
        p = Pipeline(workers=1, queue_size=100, chunk_size=50)
        p.register("source", DummySourceStep)
        p.register("branch", BranchEvenOddStep)
        p.register("terminal", TerminalStep)

        src = p.step("src", "source", items=list(range(10)))
        branch = p.step("branch", "branch")
        t_even = p.step("t_even", "terminal")
        t_odd = p.step("t_odd", "terminal")

        src.pipe(branch)
        branch.pipe(t_even, tag="even")
        branch.pipe(t_odd, tag="odd")

        stats = p.run(output_dir=tmp_path / "out")

        assert stats._stats["branch"].processed == 10
        assert stats._stats["t_even"].processed == 5
        assert stats._stats["t_odd"].processed == 5

    @pytest.mark.timeout(30)
    def test_empty_items(self, tmp_path: Path) -> None:
        """Empty source: downstream step gets 0 items."""
        p = Pipeline(workers=1, queue_size=100, chunk_size=50)
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)

        src = p.step("src", "source", items=[])
        term = p.step("term", "terminal")
        src.pipe(term)

        stats = p.run(output_dir=tmp_path / "out")
        assert stats._stats["term"].processed == 0

    @pytest.mark.timeout(30)
    def test_run_returns_stats_collector(self, tmp_path: Path) -> None:
        """pipeline.run() returns a StatsCollector with per-step metrics."""
        from task_pipeliner.stats import StatsCollector

        p = Pipeline(workers=1)
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        p.step("src", "source", items=[1, 2]).pipe(p.step("term", "terminal"))

        result = p.run(output_dir=tmp_path / "out")
        assert isinstance(result, StatsCollector)
        assert result.get_step_stats("src") is not None
        assert result.get_step_stats("term") is not None

    @pytest.mark.timeout(30)
    def test_stats_json_written(self, tmp_path: Path) -> None:
        """Code-path run writes stats.json to output_dir."""
        import json

        p = Pipeline(workers=1)
        p.register("source", DummySourceStep)
        p.register("terminal", TerminalStep)
        p.step("src", "source", items=list(range(3))).pipe(p.step("term", "terminal"))

        output_dir = tmp_path / "out"
        p.run(output_dir=output_dir)

        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = json.loads(stats_file.read_text())
        names = {d["step_name"] for d in data}
        assert "src" in names
        assert "term" in names


# ---------------------------------------------------------------------------
# Code path + YAML path produce equivalent results
# ---------------------------------------------------------------------------


class TestCodeVsYamlEquivalence:
    @pytest.mark.timeout(30)
    def test_code_path_matches_yaml_path(self, tmp_path: Path) -> None:
        """Code path and YAML path produce the same processed count."""
        from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig

        items = list(range(8))

        # Code path
        p_code = Pipeline(workers=1, queue_size=100, chunk_size=50)
        p_code.register("source", DummySourceStep)
        p_code.register("passthrough", PassthroughStep)
        p_code.register("terminal", TerminalStep)
        src = p_code.step("src", "source", items=items)
        pass_ = p_code.step("pass", "passthrough")
        term = p_code.step("term", "terminal")
        src.pipe(pass_).pipe(term)
        stats_code = p_code.run(output_dir=tmp_path / "code")

        # YAML path
        p_yaml = Pipeline(workers=1, queue_size=100, chunk_size=50)
        p_yaml.register("source", DummySourceStep)
        p_yaml.register("passthrough", PassthroughStep)
        p_yaml.register("terminal", TerminalStep)
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=items, outputs={"main": "pass"}),
                StepConfig(type="passthrough", name="pass", outputs={"main": "term"}),
                StepConfig(type="terminal", name="term"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        stats_yaml = p_yaml.run(config=config, output_dir=tmp_path / "yaml")

        assert stats_code._stats["term"].processed == stats_yaml._stats["term"].processed
        assert stats_code._stats["pass"].processed == stats_yaml._stats["pass"].processed
