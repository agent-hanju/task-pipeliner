"""Tests for exceptions, base classes, and config (W-01, W-02, W-03)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import pytest

from task_pipeliner.base import BaseResult, BaseStep, StepType
from task_pipeliner.config import (
    ExecutionConfig,
    PipelineConfig,
    StepConfig,
    load_config,
)
from task_pipeliner.exceptions import (
    ConfigValidationError,
    PipelineError,
    PipelineShutdownError,
    StepRegistrationError,
)

# ── W-01: exceptions ──────────────────────────────────────────────


class TestExceptionHierarchy:
    """All custom exceptions must be subclasses of PipelineError."""

    @pytest.mark.parametrize(
        "exc_cls",
        [StepRegistrationError, ConfigValidationError, PipelineShutdownError],
    )
    def test_is_subclass_of_pipeline_error(self, exc_cls: type) -> None:
        assert issubclass(exc_cls, PipelineError)

    @pytest.mark.parametrize(
        "exc_cls",
        [StepRegistrationError, ConfigValidationError, PipelineShutdownError],
    )
    def test_catchable_as_pipeline_error(self, exc_cls: type) -> None:
        with pytest.raises(PipelineError):
            raise exc_cls("test")


class TestStepRegistrationError:
    def test_has_step_name(self) -> None:
        err = StepRegistrationError("bad step", step_name="my_step")
        assert err.step_name == "my_step"
        assert "bad step" in str(err)


class TestConfigValidationError:
    def test_has_field_and_cause(self) -> None:
        cause = ValueError("negative workers")
        err = ConfigValidationError("invalid", field="workers", cause=cause)
        assert err.field == "workers"
        assert err.cause is cause

    def test_field_defaults_to_none(self) -> None:
        err = ConfigValidationError("generic error")
        assert err.field is None
        assert err.cause is None


# ── W-02: base ────────────────────────────────────────────────────


# --- Dummy BaseResult implementations for testing ---


@dataclass
class _DummyResult(BaseResult):
    """Concrete BaseResult for testing merge/write."""

    count: int = 0

    def merge(self, other: Self) -> Self:
        return type(self)(count=self.count + other.count)  # type: ignore[return-value]

    def write(self, output_dir: Path, step_name: str = "") -> None:
        (output_dir / "dummy_result.txt").write_text(f"count={self.count}")


@dataclass
class _NoOpResult(BaseResult):
    """Minimal no-op BaseResult."""

    def merge(self, other: Self) -> Self:
        return self

    def write(self, output_dir: Path, step_name: str = "") -> None:
        pass


class TestStepType:
    def test_enum_values(self) -> None:
        assert StepType.PARALLEL.value == "parallel"
        assert StepType.SEQUENTIAL.value == "sequential"
        assert StepType.SOURCE.value == "source"


class TestBaseResult:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            BaseResult()  # type: ignore[abstract]

    def test_merge_combines_results(self) -> None:
        a = _DummyResult(count=3)
        b = _DummyResult(count=5)
        merged = a.merge(b)
        assert merged.count == 8

    def test_merge_is_associative(self) -> None:
        a = _DummyResult(count=1)
        b = _DummyResult(count=2)
        c = _DummyResult(count=3)
        assert a.merge(b).merge(c).count == a.merge(b.merge(c)).count

    def test_write_creates_file(self, tmp_path: Path) -> None:
        result = _DummyResult(count=42)
        result.write(tmp_path)
        out_file = tmp_path / "dummy_result.txt"
        assert out_file.exists()
        assert out_file.read_text() == "count=42"

    def test_noop_result_merge_returns_self(self) -> None:
        a = _NoOpResult()
        b = _NoOpResult()
        assert a.merge(b) is a

    def test_noop_result_write_is_noop(self, tmp_path: Path) -> None:
        _NoOpResult().write(tmp_path)
        assert list(tmp_path.iterdir()) == []


class TestBaseStep:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            BaseStep()  # type: ignore[abstract]

    def test_name_defaults_to_class_name(self) -> None:
        class MyStep(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        assert MyStep().name == "MyStep"

    def test_name_override(self) -> None:
        class Custom(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL

            @property
            def name(self) -> str:
                return "custom_name"

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        assert Custom().name == "custom_name"

    def test_step_type_required(self) -> None:
        """Subclass without step_type cannot be instantiated."""
        with pytest.raises(TypeError):

            class Bad(BaseStep[_NoOpResult]):
                def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                    return _NoOpResult()

            Bad()  # type: ignore[abstract]

    def test_outputs_defaults_to_empty_tuple(self) -> None:
        """BaseStep.outputs defaults to () — terminal step."""

        class TerminalStep(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        assert TerminalStep.outputs == ()
        assert TerminalStep().outputs == ()

    def test_outputs_can_be_declared(self) -> None:
        """Subclass can declare named outputs."""

        class BranchStep(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL
            outputs = ("kept", "removed")

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                emit(item, "kept")
                return _NoOpResult()

        assert BranchStep.outputs == ("kept", "removed")

    def test_process_emit_with_tag(self) -> None:
        """emit(item, tag) — tag is required second argument."""
        collected: list[tuple[Any, str]] = []

        class TaggedStep(BaseStep[_DummyResult]):
            step_type = StepType.PARALLEL
            outputs = ("out",)

            def process(self, item: Any, state: Any, emit: Any) -> _DummyResult:
                emit(item, "out")
                return _DummyResult(count=1)

        step = TaggedStep()
        result = step.process("hello", None, lambda i, t: collected.append((i, t)))
        assert collected == [("hello", "out")]
        assert result.count == 1

    def test_items_raises_not_implemented_by_default(self) -> None:
        """Non-SOURCE steps should raise NotImplementedError on items()."""

        class NormalStep(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        step = NormalStep()
        with pytest.raises(NotImplementedError):
            list(step.items())

    def test_initial_state_defaults_to_none(self) -> None:
        """BaseStep.initial_state defaults to None."""

        class SimpleStep(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        assert SimpleStep().initial_state is None

    def test_initial_state_override(self) -> None:
        """Subclass can override initial_state to return custom state."""

        class StatefulStep(BaseStep[_NoOpResult]):
            step_type = StepType.SEQUENTIAL

            @property
            def initial_state(self) -> dict[str, int]:
                return {"count": 0}

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        step = StatefulStep()
        assert step.initial_state == {"count": 0}
        # Each access should return a fresh dict (not cached)
        assert step.initial_state is not step.initial_state or step.initial_state == {"count": 0}

    def test_open_is_noop_by_default(self) -> None:
        """open() should be callable without error (no-op)."""

        class NormalStep(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        step = NormalStep()
        step.open()  # should not raise

    def test_close_is_noop_by_default(self) -> None:
        """close() should be callable without error (no-op)."""

        class NormalStep(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        step = NormalStep()
        step.close()  # should not raise

    def test_open_close_symmetry(self) -> None:
        """Subclass can override both open() and close() for resource management."""

        class ResourceStep(BaseStep[_NoOpResult]):
            step_type = StepType.SEQUENTIAL

            def __init__(self) -> None:
                self.opened = False
                self.closed = False

            def open(self) -> None:
                self.opened = True

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

            def close(self) -> None:
                self.closed = True

        step = ResourceStep()
        assert not step.opened and not step.closed
        step.open()
        assert step.opened and not step.closed
        step.close()
        assert step.opened and step.closed


# ── W-03: config ──────────────────────────────────────────────────


class TestStepConfig:
    def test_basic_parsing(self) -> None:
        cfg = StepConfig(type="filter")
        assert cfg.type == "filter"
        assert cfg.enabled is True

    def test_enabled_false(self) -> None:
        cfg = StepConfig(type="filter", enabled=False)
        assert cfg.enabled is False

    def test_extra_kwargs_preserved(self) -> None:
        cfg = StepConfig(type="filter", threshold=0.5, mode="strict")
        assert cfg.model_extra["threshold"] == 0.5
        assert cfg.model_extra["mode"] == "strict"

    # ── W-T04: outputs field ──────────────────────────────────────

    def test_outputs_defaults_to_none(self) -> None:
        """outputs field defaults to None (terminal step or unspecified)."""
        cfg = StepConfig(type="writer")
        assert cfg.outputs is None

    def test_outputs_single_downstream(self) -> None:
        """String value = single downstream step."""
        cfg = StepConfig(type="loader", outputs={"main": "preprocess"})
        assert cfg.outputs == {"main": "preprocess"}

    def test_outputs_fan_out(self) -> None:
        """list[str] value = fan-out to multiple downstream steps."""
        cfg = StepConfig(
            type="preprocess", outputs={"kept": ["convert", "audit"], "removed": "writer"}
        )
        assert cfg.outputs is not None
        assert cfg.outputs["kept"] == ["convert", "audit"]
        assert cfg.outputs["removed"] == "writer"

    def test_outputs_not_in_model_extra(self) -> None:
        """outputs is an explicit field, not captured by extra='allow'."""
        cfg = StepConfig(type="loader", outputs={"main": "next"}, threshold=0.5)
        assert cfg.outputs == {"main": "next"}
        assert "outputs" not in cfg.model_extra
        assert cfg.model_extra["threshold"] == 0.5

    def test_outputs_empty_dict_allowed(self) -> None:
        """Empty dict is valid (step declares outputs but none connected)."""
        cfg = StepConfig(type="loader", outputs={})
        assert cfg.outputs == {}

    def test_outputs_invalid_type_rejected(self) -> None:
        """outputs must be dict or None, not a string or list."""
        with pytest.raises(ConfigValidationError):
            StepConfig(type="loader", outputs="main")

    def test_outputs_invalid_value_type_rejected(self) -> None:
        """Dict values must be str or list[str], not int."""
        with pytest.raises(ConfigValidationError):
            StepConfig(type="loader", outputs={"main": 123})


class TestExecutionConfig:
    def test_defaults(self) -> None:
        cfg = ExecutionConfig()
        assert cfg.workers == 4
        assert cfg.queue_size == 0
        assert cfg.chunk_size == 100

    @pytest.mark.parametrize("field", ["workers", "chunk_size"])
    def test_zero_value_rejected(self, field: str) -> None:
        with pytest.raises(ConfigValidationError):
            ExecutionConfig(**{field: 0})

    def test_queue_size_zero_allowed(self) -> None:
        cfg = ExecutionConfig(queue_size=0)
        assert cfg.queue_size == 0

    def test_queue_size_negative_rejected(self) -> None:
        with pytest.raises(ConfigValidationError):
            ExecutionConfig(queue_size=-1)

    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(ConfigValidationError):
            ExecutionConfig(unknown_field=42)


class TestPipelineConfig:
    def test_valid_config(self) -> None:
        cfg = PipelineConfig(pipeline=[StepConfig(type="filter")])
        assert len(cfg.pipeline) == 1
        assert cfg.execution.workers == 4

    def test_empty_pipeline_rejected(self) -> None:
        with pytest.raises(ConfigValidationError):
            PipelineConfig(pipeline=[])

    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(ConfigValidationError):
            PipelineConfig(pipeline=[StepConfig(type="a")], unknown=1)

    # ── W-T04: outputs validation ─────────────────────────────────

    def test_valid_outputs_topology(self) -> None:
        """All referenced step types exist in the pipeline."""
        cfg = PipelineConfig(
            pipeline=[
                StepConfig(type="loader", outputs={"main": "filter"}),
                StepConfig(type="filter", outputs={"kept": "writer"}),
                StepConfig(type="writer"),
            ]
        )
        assert cfg.pipeline[0].outputs == {"main": "filter"}
        assert cfg.pipeline[2].outputs is None

    def test_outputs_reference_nonexistent_step_rejected(self) -> None:
        """Referencing a step type not in the pipeline raises error."""
        with pytest.raises(ConfigValidationError):
            PipelineConfig(
                pipeline=[
                    StepConfig(type="loader", outputs={"main": "nonexistent"}),
                ]
            )

    def test_outputs_fanout_reference_nonexistent_step_rejected(self) -> None:
        """Fan-out list referencing a nonexistent step raises error."""
        with pytest.raises(ConfigValidationError):
            PipelineConfig(
                pipeline=[
                    StepConfig(type="loader", outputs={"main": ["filter", "ghost"]}),
                    StepConfig(type="filter"),
                ]
            )

    def test_outputs_fanout_all_valid(self) -> None:
        """Fan-out with all valid references passes validation."""
        cfg = PipelineConfig(
            pipeline=[
                StepConfig(type="loader", outputs={"main": ["filter", "writer"]}),
                StepConfig(type="filter"),
                StepConfig(type="writer"),
            ]
        )
        assert cfg.pipeline[0].outputs == {"main": ["filter", "writer"]}


class TestLoadConfig:
    def test_valid_yaml(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n  - type: filter\n    threshold: 0.5\nexecution:\n  workers: 2\n"
        )
        cfg = load_config(yaml_file)
        assert cfg.pipeline[0].type == "filter"
        assert cfg.pipeline[0].model_extra["threshold"] == 0.5
        assert cfg.execution.workers == 2

    def test_missing_type_raises(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "bad.yaml"
        yaml_file.write_text("pipeline:\n  - enabled: true\n")
        with pytest.raises(ConfigValidationError):
            load_config(yaml_file)

    def test_empty_pipeline_raises(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("pipeline: []\n")
        with pytest.raises(ConfigValidationError):
            load_config(yaml_file)

    def test_execution_defaults_applied(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "minimal.yaml"
        yaml_file.write_text("pipeline:\n  - type: pass\n")
        cfg = load_config(yaml_file)
        assert cfg.execution.workers == 4

    def test_yaml_with_outputs(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "topo.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: loader\n"
            "    outputs:\n"
            "      main: preprocess\n"
            "  - type: preprocess\n"
            "    outputs:\n"
            "      kept: writer\n"
            "      removed: writer\n"
            "  - type: writer\n"
        )
        cfg = load_config(yaml_file)
        assert cfg.pipeline[0].outputs == {"main": "preprocess"}
        assert cfg.pipeline[1].outputs == {"kept": "writer", "removed": "writer"}
        assert cfg.pipeline[2].outputs is None

    def test_name_defaults_to_type(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "cfg.yaml"
        yaml_file.write_text("pipeline:\n  - type: filter\n")
        cfg = load_config(yaml_file)
        assert cfg.pipeline[0].name == "filter"

    def test_name_explicit(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "cfg.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: filter\n"
            "    name: strict_filter\n"
        )
        cfg = load_config(yaml_file)
        assert cfg.pipeline[0].name == "strict_filter"
        assert cfg.pipeline[0].type == "filter"

    def test_same_type_different_names(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "cfg.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: filter\n"
            "    name: strict\n"
            "    outputs:\n"
            "      main: loose\n"
            "  - type: filter\n"
            "    name: loose\n"
        )
        cfg = load_config(yaml_file)
        assert cfg.pipeline[0].name == "strict"
        assert cfg.pipeline[1].name == "loose"
        assert cfg.pipeline[0].type == cfg.pipeline[1].type == "filter"

    def test_duplicate_name_raises(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "dup.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: filter\n"
            "  - type: filter\n"
        )
        with pytest.raises(ConfigValidationError, match="duplicate step name"):
            load_config(yaml_file)

    def test_outputs_reference_by_name(self, tmp_path: Path) -> None:
        """outputs must reference step names, not types."""
        yaml_file = tmp_path / "cfg.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: source\n"
            "    name: my_source\n"
            "    outputs:\n"
            "      main: my_sink\n"
            "  - type: sink\n"
            "    name: my_sink\n"
        )
        cfg = load_config(yaml_file)
        assert cfg.pipeline[0].outputs == {"main": "my_sink"}

    def test_outputs_reference_type_when_name_differs_raises(
        self, tmp_path: Path
    ) -> None:
        """If name differs from type, outputs must use the name."""
        yaml_file = tmp_path / "bad.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: source\n"
            "    name: my_source\n"
            "    outputs:\n"
            "      main: sink\n"
            "  - type: sink\n"
            "    name: my_sink\n"
        )
        with pytest.raises(ConfigValidationError):
            load_config(yaml_file)

    def test_yaml_outputs_reference_invalid_step_raises(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "bad_topo.yaml"
        yaml_file.write_text("pipeline:\n  - type: loader\n    outputs:\n      main: ghost\n")
        with pytest.raises(ConfigValidationError):
            load_config(yaml_file)
