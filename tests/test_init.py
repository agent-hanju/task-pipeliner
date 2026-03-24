"""Tests: __init__.py public API — W-16."""

from __future__ import annotations

import task_pipeliner


class TestPublicAPI:
    def test_all_symbols_importable(self) -> None:
        """Every name in __all__ is importable."""
        for name in task_pipeliner.__all__:
            assert hasattr(task_pipeliner, name), f"{name} not found in task_pipeliner"

    def test_pipeline_importable(self) -> None:
        from task_pipeliner import Pipeline

        assert Pipeline is not None

    def test_step_classes_importable(self) -> None:
        from task_pipeliner import ParallelStep, SequentialStep, SourceStep, Worker

        assert SourceStep is not None
        assert SequentialStep is not None
        assert ParallelStep is not None
        assert Worker is not None

    def test_producers_importable(self) -> None:
        from task_pipeliner import InputProducer, ParallelProducer, SequentialProducer

        assert InputProducer is not None
        assert SequentialProducer is not None
        assert ParallelProducer is not None

    def test_exceptions_importable(self) -> None:
        from task_pipeliner import (
            ConfigValidationError,
            PipelineError,
            StepRegistrationError,
        )

        assert PipelineError is not None
        assert StepRegistrationError is not None
        assert ConfigValidationError is not None

    def test_all_completeness(self) -> None:
        """__all__ contains all expected public symbols."""
        expected = {
            "Pipeline",
            "SourceStep",
            "SequentialStep",
            "ParallelStep",
            "Worker",
            "InputProducer",
            "ParallelProducer",
            "SequentialProducer",
            "PipelineError",
            "StepRegistrationError",
            "ConfigValidationError",
        }
        assert set(task_pipeliner.__all__) == expected
