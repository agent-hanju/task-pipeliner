"""Tests for exceptions, base classes, and config (W-01, W-02, W-03)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import pytest

from task_pipeliner.base import BaseAggStep, BaseResult, BaseStep, StepType
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

    def write(self, output_dir: Path) -> None:
        (output_dir / "dummy_result.txt").write_text(f"count={self.count}")


@dataclass
class _NoOpResult(BaseResult):
    """Minimal no-op BaseResult."""

    def merge(self, other: Self) -> Self:
        return self

    def write(self, output_dir: Path) -> None:
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

    def test_process_with_emit_callback(self) -> None:
        """process() receives emit callback; emit sends items, return is result."""
        collected: list[Any] = []

        class EmitStep(BaseStep[_DummyResult]):
            step_type = StepType.PARALLEL

            def process(self, item: Any, state: Any, emit: Any) -> _DummyResult:
                emit(item)
                emit(item)
                return _DummyResult(count=1)

        step = EmitStep()
        result = step.process("hello", None, collected.append)
        assert collected == ["hello", "hello"]
        assert isinstance(result, _DummyResult)
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

    def test_close_is_noop_by_default(self) -> None:
        """close() should be callable without error (no-op)."""

        class NormalStep(BaseStep[_NoOpResult]):
            step_type = StepType.PARALLEL

            def process(self, item: Any, state: Any, emit: Any) -> _NoOpResult:
                return _NoOpResult()

        step = NormalStep()
        step.close()  # should not raise


class TestBaseAggStep:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            BaseAggStep()  # type: ignore[abstract]

    def test_name_defaults_to_class_name(self) -> None:
        class MyAgg(BaseAggStep):
            def process_batch(self, items: list[Any]) -> Any:
                return items

        assert MyAgg().name == "MyAgg"


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


class TestExecutionConfig:
    def test_defaults(self) -> None:
        cfg = ExecutionConfig()
        assert cfg.workers == 4
        assert cfg.queue_size == 200
        assert cfg.chunk_size == 100

    @pytest.mark.parametrize("field", ["workers", "queue_size", "chunk_size"])
    def test_zero_value_rejected(self, field: str) -> None:
        with pytest.raises(ConfigValidationError):
            ExecutionConfig(**{field: 0})

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
