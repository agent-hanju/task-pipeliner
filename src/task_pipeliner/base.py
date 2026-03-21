"""Abstract base classes for pipeline steps and results."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Self


class StepType(Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    SOURCE = "source"


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

    outputs: ClassVar[tuple[str, ...]] = ()
    """Declared output tags. Empty tuple ``()`` means terminal step (emit not allowed)."""

    @property
    def name(self) -> str:
        return type(self).__name__

    @property
    @abstractmethod
    def step_type(self) -> StepType: ...

    @abstractmethod
    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> R:
        """Process a single item.

        *emit(item, tag)* — callback provided by the Producer to forward
        items to the named output.  Call it zero or more times.

        Example::

            emit(item, "kept")     # forward to "kept" output
            emit(item, "removed")  # forward to "removed" output

        ``outputs = ()`` steps (terminal) must NOT call emit —
        doing so raises ``RuntimeError``.

        Returns a result data object ``R`` that the Producer will
        atomically merge with previous results.
        """
        ...

    @property
    def initial_state(self) -> Any:
        """Return the initial state object for this step.

        Override in subclasses that need mutable state passed to process().
        The returned object is passed as the ``state`` argument to every
        ``process()`` call. For SEQUENTIAL steps, it is the same object
        across all calls (accumulated in-place).
        """
        return None

    def items(self) -> Generator[Any, None, None]:
        """SOURCE 스텝 전용. 아이템을 yield한다."""
        raise NotImplementedError("items() must be implemented by SOURCE steps")

    def close(self) -> None:
        """Release resources held by this step.

        Called by the Producer after the processing loop finishes.
        Override in subclasses that open files, connections, etc.
        """


class BaseAggStep(ABC):
    """Abstract base for batch/aggregation steps."""

    @property
    def name(self) -> str:
        return type(self).__name__

    @abstractmethod
    def process_batch(self, items: list[Any]) -> Any:
        """Process a batch of items. Return value becomes next stage state."""
        ...
