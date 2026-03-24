"""Dummy steps for framework testing (module-level definitions required for spawn pickling)."""

from __future__ import annotations

import time
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import orjson

from task_pipeliner.base import ParallelStep, SequentialStep, SourceStep, Worker

# ---------------------------------------------------------------------------
# SOURCE steps
# ---------------------------------------------------------------------------


class DummySourceStep(SourceStep):
    """SOURCE step that yields a fixed list of items."""

    outputs = ("main",)

    def __init__(self, items: list[Any] | None = None, **_kwargs: Any) -> None:
        self._items = items or []
        self.closed = False

    def items(self) -> Generator[Any, None, None]:
        yield from self._items

    def close(self) -> None:
        self.closed = True


class DummyJsonlSourceStep(SourceStep):
    """SOURCE step that reads JSONL files (for testing)."""

    outputs = ("main",)

    def __init__(self, items: list[str] | None = None, **_kwargs: Any) -> None:
        self._paths = [Path(p) for p in items] if items else []

    def items(self) -> Generator[Any, None, None]:
        for p in self._paths:
            with open(p, "rb") as fh:
                for line in fh:
                    stripped = line.strip()
                    if stripped:
                        yield orjson.loads(stripped)


# ---------------------------------------------------------------------------
# PARALLEL steps (Worker + Step pairs)
# ---------------------------------------------------------------------------


class PassthroughWorker(Worker):
    """Emits item unchanged."""

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        emit(item, "main")


class PassthroughStep(ParallelStep):
    """Emits item unchanged."""

    outputs = ("main",)

    def create_worker(self) -> PassthroughWorker:
        return PassthroughWorker()


class FilterEvenWorker(Worker):
    """Emits even integers, filters out odd ones."""

    def process(self, item: int, state: Any, emit: Callable[[Any, str], None]) -> None:
        if item % 2 == 0:
            emit(item, "main")


class FilterEvenStep(ParallelStep):
    """Emits even integers, filters out odd ones."""

    outputs = ("main",)

    def create_worker(self) -> FilterEvenWorker:
        return FilterEvenWorker()


class ErrorOnItemWorker(Worker):
    """Raises RuntimeError when item matches error_value."""

    def __init__(self, error_value: Any = -1) -> None:
        self.error_value = error_value

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        if item == self.error_value:
            raise RuntimeError(f"Error triggered on item {item!r}")
        emit(item, "main")


class ErrorOnItemStep(ParallelStep):
    """Raises RuntimeError when item matches error_value."""

    outputs = ("main",)

    def __init__(self, error_value: Any = -1) -> None:
        self.error_value = error_value

    def create_worker(self) -> ErrorOnItemWorker:
        return ErrorOnItemWorker(self.error_value)


class SlowWorker(Worker):
    """Sleeps before emitting item."""

    def __init__(self, sleep_seconds: float = 0.1) -> None:
        self.sleep_seconds = sleep_seconds

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        time.sleep(self.sleep_seconds)
        emit(item, "main")


class SlowStep(ParallelStep):
    """Sleeps before emitting item."""

    outputs = ("main",)

    def __init__(self, sleep_seconds: float = 0.1) -> None:
        self.sleep_seconds = sleep_seconds

    def create_worker(self) -> SlowWorker:
        return SlowWorker(self.sleep_seconds)


# ---------------------------------------------------------------------------
# SEQUENTIAL steps
# ---------------------------------------------------------------------------


class SequentialPassthroughStep(SequentialStep):
    """Sequential step that emits item unchanged (for testing SequentialProducer)."""

    outputs = ("main",)

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        emit(item, "main")


class SequentialFilterEvenStep(SequentialStep):
    """Sequential step that emits even integers, filters out odd ones."""

    outputs = ("main",)

    def process(self, item: int, state: Any, emit: Callable[[Any, str], None]) -> None:
        if item % 2 == 0:
            emit(item, "main")


class SequentialErrorOnItemStep(SequentialStep):
    """Sequential step that raises RuntimeError when item matches error_value."""

    outputs = ("main",)

    def __init__(self, error_value: Any = -1) -> None:
        self.error_value = error_value

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        if item == self.error_value:
            raise RuntimeError(f"Error triggered on item {item!r}")
        emit(item, "main")


class TerminalStep(SequentialStep):
    """Terminal step — outputs = (), emit not allowed."""

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        pass  # Terminal step should NOT call emit


class BranchEvenOddStep(SequentialStep):
    """Routes even items to 'even' tag, odd items to 'odd' tag."""

    outputs = ("even", "odd")

    def process(self, item: int, state: Any, emit: Callable[[Any, str], None]) -> None:
        if item % 2 == 0:
            emit(item, "even")
        else:
            emit(item, "odd")


class StateAwareStep(SequentialStep):
    """Multiplies item by state['multiplier'] and emits."""

    outputs = ("main",)

    def process(
        self, item: int, state: dict[str, Any], emit: Callable[[Any, str], None]
    ) -> None:
        emit(state["multiplier"] * item, "main")


class CollectorStep(SequentialStep):
    """SEQUENTIAL terminal — collects items, sets another step's state on close."""

    def __init__(self, target_step: str = "StateGatedStep") -> None:
        self._collected: list[Any] = []
        self._target_step = target_step

    @property
    def initial_state(self) -> list[Any]:
        return self._collected

    def process(
        self, item: Any, state: list[Any], emit: Callable[[Any, str], None]
    ) -> None:
        state.append(item)

    def get_output_state(self) -> dict[str, Any] | None:
        return {self._target_step: list(self._collected)}


class StateGatedStep(SequentialStep):
    """SEQUENTIAL step gated by is_ready — waits until state is not None."""

    outputs = ("main",)

    def is_ready(self, state: Any) -> bool:
        return state is not None

    def process(
        self, item: Any, state: list[Any], emit: Callable[[Any, str], None]
    ) -> None:
        emit({"item": item, "state_len": len(state)}, "main")


class LifecycleTrackingStep(SequentialStep):
    """SEQUENTIAL terminal — tracks open/close calls for lifecycle testing."""

    def __init__(self, **_kwargs: Any) -> None:
        self.opened = False
        self.closed = False
        self.process_count = 0

    def open(self) -> None:
        self.opened = True

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        self.process_count += 1

    def close(self) -> None:
        self.closed = True


class InitialStateStep(SequentialStep):
    """SEQUENTIAL step that provides initial_state and mutates it during process()."""

    outputs = ("main",)

    @property
    def initial_state(self) -> dict[str, int]:
        return {"count": 0}

    def process(
        self, item: Any, state: dict[str, int], emit: Callable[[Any, str], None]
    ) -> None:
        state["count"] += 1
        emit({"item": item, "count": state["count"]}, "main")
