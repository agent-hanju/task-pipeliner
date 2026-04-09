"""task-pipeliner: configurable data processing pipeline framework."""

from task_pipeliner.base import AsyncStep, ParallelStep, SequentialStep, SourceStep, Worker
from task_pipeliner.exceptions import (
    ConfigValidationError,
    PipelineError,
    StepRegistrationError,
)
from task_pipeliner.pipeline import Pipeline
from task_pipeliner.step_runners import (
    AsyncStepRunner,
    InputStepRunner,
    ParallelStepRunner,
    SequentialStepRunner,
)

# Backward-compatible aliases
InputProducer = InputStepRunner
ParallelProducer = ParallelStepRunner
SequentialProducer = SequentialStepRunner

__all__ = [
    "Pipeline",
    "SourceStep",
    "SequentialStep",
    "AsyncStep",
    "ParallelStep",
    "Worker",
    # New names
    "InputStepRunner",
    "SequentialStepRunner",
    "AsyncStepRunner",
    "ParallelStepRunner",
    # Legacy aliases
    "InputProducer",
    "ParallelProducer",
    "SequentialProducer",
    "PipelineError",
    "StepRegistrationError",
    "ConfigValidationError",
]
