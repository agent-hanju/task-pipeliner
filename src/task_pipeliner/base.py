"""Abstract base classes for pipeline steps."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from typing import Any, ClassVar

# ---------------------------------------------------------------------------
# StepBase — shared infrastructure (private)
# ---------------------------------------------------------------------------


class StepBase:
    """Common infrastructure for all step types.

    Not intended for direct subclassing — use ``SourceStep``,
    ``SequentialStep``, or ``ParallelStep`` instead.
    """

    outputs: ClassVar[tuple[str, ...]] = ()
    """Declared output tags. Empty tuple ``()`` means terminal step (emit not allowed)."""

    @property
    def initial_state(self) -> Any:
        """Return the initial state object for this step.

        Override in subclasses that need mutable state passed to process().
        The returned object is passed as the ``state`` argument to every
        ``process()`` call.  For SEQUENTIAL steps, it is the same object
        across all calls (accumulated in-place).
        """
        return None

    def is_ready(self, state: Any) -> bool:
        """Return True if this step is ready to process items.

        Override in subclasses to gate processing on state availability.
        When False, the producer will not call process() even if items
        are queued — it stays idle until a state change triggers
        re-evaluation.  Default returns True (always ready).
        """
        return True

    def get_output_state(self) -> dict[str, Any] | None:
        """Return state to dispatch to other steps after processing completes.

        Override in subclasses that need to push state to other steps.
        Called by the Producer after ``close()``.  Return a mapping of
        ``{target_step_name: state_value}`` or ``None`` if no state
        dispatch is needed.
        """
        return None

    def open(self) -> None:
        """Acquire resources on the main process before processing begins.

        Called once per execution, after ``is_ready()`` passes and before
        the first ``process()`` call.  Paired with ``close()``.

        Typical use: opening file handles, database connections, or
        temporary directories that should not be created at ``__init__``
        time.
        """

    def close(self) -> None:
        """Release main-process resources after processing ends.

        Called by the Producer after the processing loop finishes.
        Override in subclasses that open files, connections, etc.
        """


# ---------------------------------------------------------------------------
# SourceStep
# ---------------------------------------------------------------------------


class SourceStep(StepBase, ABC):
    """Base class for SOURCE steps that generate items.

    Must implement ``items()`` which yields input items for the pipeline.
    """

    @abstractmethod
    def items(self) -> Generator[Any, None, None]:
        """Yield items to feed into the pipeline."""
        ...


# ---------------------------------------------------------------------------
# SequentialStep
# ---------------------------------------------------------------------------


class SequentialStep(StepBase, ABC):
    """Base class for steps that process items sequentially in the main thread.

    Must implement ``process()`` for per-item logic.
    """

    @abstractmethod
    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        """Process a single item.

        *emit(item, tag)* — callback provided by the Producer to forward
        items to the named output.  Call it zero or more times.

        ``outputs = ()`` steps (terminal) must NOT call emit —
        doing so raises ``RuntimeError``.
        """
        ...


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class Worker(ABC):
    """Processing unit sent to worker processes.

    Instances must be picklable (module-level class, no lambdas/closures).
    ``open()`` and ``close()`` run once per worker process, bracketing
    all ``process()`` calls within that process.
    """

    @abstractmethod
    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        """Process a single item in a worker process."""
        ...

    def open(self) -> None:
        """Called once per worker process before the first ``process()`` call.

        Use for per-process resource acquisition: DB connections,
        model loading, GPU context, etc.
        """

    def close(self) -> None:
        """Called once per worker process after the last ``process()`` call.

        Use for per-process resource cleanup.
        """


# ---------------------------------------------------------------------------
# ParallelStep
# ---------------------------------------------------------------------------


class ParallelStep(StepBase, ABC):
    """Base class for steps that distribute work across worker processes.

    Must implement ``create_worker()`` which returns a picklable ``Worker``
    instance.  The Worker's ``open()``/``close()`` provide per-worker-process
    lifecycle hooks, while the step's own ``open()``/``close()`` run on the
    main process only.
    """

    @abstractmethod
    def create_worker(self) -> Worker:
        """Create a picklable Worker instance for worker processes."""
        ...
