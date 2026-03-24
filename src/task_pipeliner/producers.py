"""Backward-compatibility shim — re-exports from step_runners.

All classes have been renamed from ``*Producer`` to ``*StepRunner``
and moved to :mod:`task_pipeliner.step_runners`.  This module keeps
the old import paths working.
"""

from task_pipeliner.step_runners import (  # noqa: F401
    BaseProducer,
    BaseStepRunner,
    ChunkResult,
    ErrorSentinel,
    InputProducer,
    InputStepRunner,
    ParallelProducer,
    ParallelStepRunner,
    Sentinel,
    SequentialProducer,
    SequentialStepRunner,
    is_sentinel,
)
from task_pipeliner.step_runners import (  # noqa: F401
    _parallel_worker as _parallel_worker,
)

__all__ = [
    # New names
    "InputStepRunner",
    "BaseStepRunner",
    "SequentialStepRunner",
    "ParallelStepRunner",
    # Legacy aliases
    "InputProducer",
    "BaseProducer",
    "SequentialProducer",
    "ParallelProducer",
    # Sentinels & utilities
    "Sentinel",
    "ErrorSentinel",
    "is_sentinel",
    "ChunkResult",
]
