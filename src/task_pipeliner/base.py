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

    _state_dispatch: Callable[[str, Any], None] | None = None

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_state_dispatch", None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._state_dispatch = None

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

    def is_ready(self, state: Any) -> bool:
        """Return True if this step is ready to process items.

        Override in subclasses to gate processing on state availability.
        When False, the producer will not call process() even if items
        are queued — it stays idle until a state change triggers
        re-evaluation.  Default returns True (always ready).
        """
        return True

    def set_step_state(self, target: str, state: Any) -> None:
        """Set another step's state and fire a state-change event.

        Analogous to emit for items — this method lets a step push
        state to another step (identified by class name).  The engine
        injects the dispatch callback before the pipeline runs.
        """
        if self._state_dispatch is None:
            raise RuntimeError(
                "set_step_state is not available outside pipeline execution"
            )
        self._state_dispatch(target, state)

    def open(self) -> None:
        """Acquire resources right before processing begins.

        Called once per execution, after ``is_ready()`` passes and before
        the first ``process()`` call.  Paired with ``close()``.

        Typical use: opening file handles, database connections, or
        temporary directories that should not be created at ``__init__``
        time.

        .. note::

            For PARALLEL steps this runs on the **main-process**
            instance, not on the worker-process copies.  Worker
            processes receive a pickle-restored copy of the step
            and do not call ``open()``.  Use lazy initialisation
            inside ``process()`` if workers need per-process
            resources.
        """

    def close(self) -> None:
        """Release resources held by this step.

        Called by the Producer after the processing loop finishes.
        Override in subclasses that open files, connections, etc.
        """
