"""Abstract base classes for pipeline steps."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from typing import Any

# ---------------------------------------------------------------------------
# StepBase — shared infrastructure (private)
# ---------------------------------------------------------------------------


class StepBase:
    """Common infrastructure for all step types.

    Not intended for direct subclassing — use ``SourceStep``,
    ``SequentialStep``, or ``ParallelStep`` instead.
    """

    def pipe(self, step: StepBase, tag: str = "main") -> StepBase:
        """Connect this step's output *tag* to *step*.

        Returns *step* to allow chaining::

            source.pipe(filter_step).pipe(writer_step)

        Multiple calls with the same tag fan-out to multiple targets.
        """
        if "_connections" not in self.__dict__:
            self.__dict__["_connections"] = {}
        self.__dict__["_connections"].setdefault(tag, []).append(step)
        return step

    def open(self) -> None:
        """Acquire resources on the main process before processing begins.

        Called once per execution before the first ``process()`` call.
        Paired with ``close()``.

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

    def item_key(self, item: Any) -> str | None:  # noqa: ARG002
        """Return a unique string key for *item*, used for checkpoint deduplication.

        Return ``None`` (default) to opt out of checkpointing for this item.
        Override to enable resume-from-checkpoint support.
        """
        return None


# ---------------------------------------------------------------------------
# SequentialStep
# ---------------------------------------------------------------------------


class SequentialStep(StepBase, ABC):
    """Base class for steps that process items sequentially in the main thread.

    Must implement ``process()`` for per-item logic.
    """

    @abstractmethod
    def process(self, item: Any, emit: Callable[[Any, str], None]) -> None:
        """Process a single item.

        *emit(item, tag)* — callback provided by the Producer to forward
        items to the named output.  Call it zero or more times.

        ``outputs = ()`` steps (terminal) must NOT call emit —
        doing so raises ``RuntimeError``.
        """
        ...


# ---------------------------------------------------------------------------
# AsyncStep
# ---------------------------------------------------------------------------


class AsyncStep(StepBase, ABC):
    """Base class for I/O-bound async steps (e.g. LLM API calls, HTTP requests).

    Unlike ``ParallelStep`` which uses ``ProcessPoolExecutor``, ``AsyncStep``
    runs an ``asyncio`` event loop in a single thread and limits concurrency
    with a ``Semaphore``.  This is the right choice for I/O-bound tasks that
    use ``asyncio``-native libraries (``aiohttp``, ``httpx``, etc.).

    Must implement ``process_async()`` — an async coroutine that processes
    one item and calls ``emit`` to forward results downstream.
    """

    @property
    def concurrency(self) -> int:
        """Maximum number of concurrent ``process_async()`` coroutines.

        Override in subclasses to tune per-step parallelism.
        Default: 8.
        """
        return 8

    @abstractmethod
    async def process_async(
        self, item: Any, emit: Callable[[Any, str], None]
    ) -> None:
        """Process a single item asynchronously.

        *emit(item, tag)* — sync callback to forward items downstream.
        Safe to call from async context (puts to ``multiprocessing.Queue``).

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
    def process(self, item: Any, emit: Callable[[Any, str], None]) -> None:
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
