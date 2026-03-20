"""Dummy steps for framework testing (module-level definitions required for spawn pickling)."""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import orjson

from task_pipeliner.base import BaseAggStep, BaseResult, BaseStep, StepType

# ---------------------------------------------------------------------------
# Dummy result types
# ---------------------------------------------------------------------------


@dataclass
class NullResult(BaseResult):
    """No-op result for steps that don't produce result data."""

    def merge(self, other: Self) -> Self:
        return self

    def write(self, output_dir: Path) -> None:
        pass


@dataclass
class CountResult(BaseResult):
    """Tracks passed/filtered item counts."""

    passed: int = 0
    filtered: int = 0

    def merge(self, other: Self) -> Self:
        return type(self)(  # type: ignore[return-value]
            passed=self.passed + other.passed,
            filtered=self.filtered + other.filtered,
        )

    def write(self, output_dir: Path) -> None:
        (output_dir / "count_result.json").write_bytes(
            orjson.dumps({"passed": self.passed, "filtered": self.filtered})
        )


# ---------------------------------------------------------------------------
# Dummy steps
# ---------------------------------------------------------------------------


class DummySourceStep(BaseStep[NullResult]):
    """SOURCE step that yields a fixed list of items."""

    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = items or []
        self.closed = False

    @property
    def step_type(self) -> StepType:
        return StepType.SOURCE

    def items(self) -> Generator[Any, None, None]:
        yield from self._items

    def process(self, item: Any, state: Any, emit: Callable[[Any], None]) -> NullResult:
        raise NotImplementedError("SOURCE step does not process")

    def close(self) -> None:
        self.closed = True


class PassthroughStep(BaseStep[NullResult]):
    """Emits item unchanged."""

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def process(self, item: Any, state: Any, emit: Callable[[Any], None]) -> NullResult:
        emit(item)
        return NullResult()


class FilterEvenStep(BaseStep[CountResult]):
    """Emits even integers, filters out odd ones."""

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def process(self, item: int, state: Any, emit: Callable[[Any], None]) -> CountResult:
        if item % 2 == 0:
            emit(item)
            return CountResult(passed=1)
        return CountResult(filtered=1)


class ErrorOnItemStep(BaseStep[NullResult]):
    """Raises RuntimeError when item matches error_value."""

    def __init__(self, error_value: Any = -1) -> None:
        self.error_value = error_value

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def process(self, item: Any, state: Any, emit: Callable[[Any], None]) -> NullResult:
        if item == self.error_value:
            raise RuntimeError(f"Error triggered on item {item!r}")
        emit(item)
        return NullResult()


class SlowStep(BaseStep[NullResult]):
    """Sleeps before emitting item."""

    def __init__(self, sleep_seconds: float = 0.1) -> None:
        self.sleep_seconds = sleep_seconds

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def process(self, item: Any, state: Any, emit: Callable[[Any], None]) -> NullResult:
        time.sleep(self.sleep_seconds)
        emit(item)
        return NullResult()


class CountingAggStep(BaseAggStep):
    """Returns a Counter of items."""

    def process_batch(self, items: list[Any]) -> Counter[Any]:
        return Counter(items)


class StateAwareStep(BaseStep[NullResult]):
    """Multiplies item by state['multiplier'] and emits."""

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    def process(self, item: int, state: dict[str, Any], emit: Callable[[Any], None]) -> NullResult:
        emit(state["multiplier"] * item)
        return NullResult()
