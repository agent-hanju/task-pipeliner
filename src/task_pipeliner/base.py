"""Abstract base classes for pipeline steps and results."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any, Self


class StepType(Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"


class BaseResult(ABC):
    """Interface for step result data objects.

    Each result type carries its own merge rules and file output rules,
    keeping the framework generic — the Producer merges and writes
    without domain knowledge.
    """

    @abstractmethod
    def merge(self, other: Self) -> Self:
        """Combine two results into one (must be associative)."""
        ...

    @abstractmethod
    def write(self, output_dir: Path) -> None:
        """Write this result to files under *output_dir*."""
        ...


class BaseStep[R: BaseResult](ABC):
    """Abstract base for individual-item processing steps."""

    @property
    def name(self) -> str:
        return type(self).__name__

    @property
    @abstractmethod
    def step_type(self) -> StepType: ...

    @abstractmethod
    def process(self, item: Any, state: Any, emit: Callable[[Any], None]) -> R:
        """Process a single item.

        *emit* — callback provided by the Producer to forward items to
        the next queue.  Call it zero or more times at any point.

        Returns a result data object ``R`` that the Producer will
        atomically merge with previous results.
        """
        ...


class BaseAggStep(ABC):
    """Abstract base for batch/aggregation steps."""

    @property
    def name(self) -> str:
        return type(self).__name__

    @abstractmethod
    def process_batch(self, items: list[Any]) -> Any:
        """Process a batch of items. Return value becomes next stage state."""
        ...
