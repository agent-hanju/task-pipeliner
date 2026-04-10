"""Tests for exceptions, base classes, and config (W-01, W-02, W-03)."""

from __future__ import annotations

from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest

from task_pipeliner.base import (
    ParallelStep,
    SequentialStep,
    SourceStep,
    Worker,
)
from task_pipeliner.config import (
    ExecutionConfig,
    PipelineConfig,
    QueueType,
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


class TestStepBase:
    """Tests for shared StepBase functionality."""

    def test_outputs_defaults_to_empty_tuple(self) -> None:
        class MyStep(SequentialStep):
            def process(self, item: Any, state: Any, emit: Any) -> None:
                pass

        assert MyStep.outputs == ()
        assert MyStep().outputs == ()

    def test_outputs_can_be_declared(self) -> None:
        class MyStep(SequentialStep):
            outputs = ("kept", "removed")

            def process(self, item: Any, state: Any, emit: Any) -> None:
                pass

        assert MyStep.outputs == ("kept", "removed")

    def test_initial_state_defaults_to_none(self) -> None:
        class MyStep(SequentialStep):
            def process(self, item: Any, state: Any, emit: Any) -> None:
                pass

        assert MyStep().initial_state is None

    def test_initial_state_override(self) -> None:
        class MyStep(SequentialStep):
            @property
            def initial_state(self) -> dict[str, int]:
                return {"count": 0}

            def process(self, item: Any, state: Any, emit: Any) -> None:
                pass

        step = MyStep()
        assert step.initial_state == {"count": 0}

    def test_open_is_noop_by_default(self) -> None:
        class MyStep(SequentialStep):
            def process(self, item: Any, state: Any, emit: Any) -> None:
                pass

        MyStep().open()  # should not raise

    def test_close_is_noop_by_default(self) -> None:
        class MyStep(SequentialStep):
            def process(self, item: Any, state: Any, emit: Any) -> None:
                pass

        MyStep().close()  # should not raise

    def test_open_close_symmetry(self) -> None:
        class ResourceStep(SequentialStep):
            def __init__(self) -> None:
                self.opened = False
                self.closed = False

            def open(self) -> None:
                self.opened = True

            def process(self, item: Any, state: Any, emit: Any) -> None:
                pass

            def close(self) -> None:
                self.closed = True

        step = ResourceStep()
        assert not step.opened and not step.closed
        step.open()
        assert step.opened and not step.closed
        step.close()
        assert step.opened and step.closed


class TestSourceStep:
    def test_cannot_instantiate_without_items(self) -> None:
        with pytest.raises(TypeError):
            SourceStep()  # type: ignore[abstract]

    def test_items_abstract(self) -> None:
        class MySource(SourceStep):
            outputs = ("main",)

            def items(self) -> Generator[Any, None, None]:
                yield 1
                yield 2

        assert list(MySource().items()) == [1, 2]


class TestSequentialStep:
    def test_cannot_instantiate_without_process(self) -> None:
        with pytest.raises(TypeError):
            SequentialStep()  # type: ignore[abstract]

    def test_process_emit_with_tag(self) -> None:
        collected: list[tuple[Any, str]] = []

        class MyStep(SequentialStep):
            outputs = ("out",)

            def process(self, item: Any, state: Any, emit: Any) -> None:
                emit(item, "out")

        step = MyStep()
        step.process("hello", None, lambda i, t: collected.append((i, t)))
        assert collected == [("hello", "out")]


class TestWorker:
    def test_cannot_instantiate_without_process(self) -> None:
        with pytest.raises(TypeError):
            Worker()  # type: ignore[abstract]

    def test_open_close_noop_by_default(self) -> None:
        class MyWorker(Worker):
            def process(
                self, item: Any, state: Any, emit: Callable[[Any, str], None]
            ) -> None:
                pass

        w = MyWorker()
        w.open()   # should not raise
        w.close()  # should not raise


class TestParallelStep:
    def test_cannot_instantiate_without_create_worker(self) -> None:
        with pytest.raises(TypeError):
            ParallelStep()  # type: ignore[abstract]

    def test_create_worker_returns_worker(self) -> None:
        class MyWorker(Worker):
            def process(
                self, item: Any, state: Any, emit: Callable[[Any, str], None]
            ) -> None:
                emit(item, "main")

        class MyStep(ParallelStep):
            outputs = ("main",)

            def create_worker(self) -> MyWorker:
                return MyWorker()

        step = MyStep()
        worker = step.create_worker()
        assert isinstance(worker, Worker)


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
            type="preprocess",
            outputs={"kept": ["convert", "audit"], "removed": "writer"},
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
        assert cfg.queue_type == QueueType.AUTO

    def test_queue_type_values(self) -> None:
        assert ExecutionConfig(queue_type="spill").queue_type == QueueType.SPILL
        assert ExecutionConfig(queue_type="full_disk").queue_type == QueueType.FULL_DISK
        assert ExecutionConfig(queue_type="auto").queue_type == QueueType.AUTO

    def test_queue_type_invalid_rejected(self) -> None:
        with pytest.raises(ConfigValidationError):
            ExecutionConfig(queue_type="unknown")

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
        assert cfg.checkpoint_dir is None
        assert cfg.resume_run_id is None

    def test_checkpoint_fields(self, tmp_path: Path) -> None:
        cfg = PipelineConfig(
            pipeline=[StepConfig(type="filter")],
            checkpoint_dir=tmp_path / "ckpt",
            resume_run_id="abc123",
        )
        assert cfg.checkpoint_dir == tmp_path / "ckpt"
        assert cfg.resume_run_id == "abc123"

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
                    StepConfig(
                        type="loader",
                        outputs={"main": ["filter", "ghost"]},
                    ),
                    StepConfig(type="filter"),
                ]
            )

    def test_outputs_fanout_all_valid(self) -> None:
        """Fan-out with all valid references passes validation."""
        cfg = PipelineConfig(
            pipeline=[
                StepConfig(
                    type="loader",
                    outputs={"main": ["filter", "writer"]},
                ),
                StepConfig(type="filter"),
                StepConfig(type="writer"),
            ]
        )
        assert cfg.pipeline[0].outputs == {"main": ["filter", "writer"]}


class TestLoadConfig:
    def test_valid_yaml(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: filter\n"
            "    threshold: 0.5\n"
            "execution:\n"
            "  workers: 2\n"
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
        assert cfg.pipeline[1].outputs == {
            "kept": "writer",
            "removed": "writer",
        }
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

    def test_yaml_outputs_reference_invalid_step_raises(
        self, tmp_path: Path
    ) -> None:
        yaml_file = tmp_path / "bad_topo.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: loader\n"
            "    outputs:\n"
            "      main: ghost\n"
        )
        with pytest.raises(ConfigValidationError):
            load_config(yaml_file)
