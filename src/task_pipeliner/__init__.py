"""task-pipeliner: configurable data processing pipeline framework."""

from task_pipeliner.base import ParallelStep, SequentialStep, SourceStep, Worker
from task_pipeliner.exceptions import (
    ConfigValidationError,
    PipelineError,
    StepRegistrationError,
)
from task_pipeliner.pipeline import Pipeline
from task_pipeliner.step_runners import (
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
    "ParallelStep",
    "Worker",
    # New names
    "InputStepRunner",
    "ParallelStepRunner",
    "SequentialStepRunner",
    # Legacy aliases
    "InputProducer",
    "ParallelProducer",
    "SequentialProducer",
    "PipelineError",
    "StepRegistrationError",
    "ConfigValidationError",
]
