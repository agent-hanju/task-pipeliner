"""Dummy steps for framework testing (module-level definitions required for spawn pickling)."""

from __future__ import annotations

import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import orjson

from task_pipeliner.base import BaseResult, BaseStep, StepType

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

    outputs = ("main",)

    def __init__(self, items: list[Any] | None = None, **_kwargs: Any) -> None:
        self._items = items or []
        self.closed = False

    @property
    def step_type(self) -> StepType:
        return StepType.SOURCE

    def items(self) -> Generator[Any, None, None]:
        yield from self._items

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> NullResult:
        raise NotImplementedError("SOURCE step does not process")

    def close(self) -> None:
        self.closed = True


class DummyJsonlSourceStep(BaseStep[NullResult]):
    """SOURCE step that reads JSONL files (for testing)."""

    outputs = ("main",)

    def __init__(self, items: list[str] | None = None, **_kwargs: Any) -> None:
        self._paths = [Path(p) for p in items] if items else []

    @property
    def step_type(self) -> StepType:
        return StepType.SOURCE

    def items(self) -> Generator[Any, None, None]:
        for p in self._paths:
            with open(p, "rb") as fh:
                for line in fh:
                    stripped = line.strip()
                    if stripped:
                        yield orjson.loads(stripped)

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> NullResult:
        raise NotImplementedError("SOURCE step does not process")


class PassthroughStep(BaseStep[NullResult]):
    """Emits item unchanged."""

    outputs = ("main",)

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> NullResult:
        emit(item, "main")
        return NullResult()


class FilterEvenStep(BaseStep[CountResult]):
    """Emits even integers, filters out odd ones."""

    outputs = ("main",)

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def process(self, item: int, state: Any, emit: Callable[[Any, str], None]) -> CountResult:
        if item % 2 == 0:
            emit(item, "main")
            return CountResult(passed=1)
        return CountResult(filtered=1)


class ErrorOnItemStep(BaseStep[NullResult]):
    """Raises RuntimeError when item matches error_value."""

    outputs = ("main",)

    def __init__(self, error_value: Any = -1) -> None:
        self.error_value = error_value

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> NullResult:
        if item == self.error_value:
            raise RuntimeError(f"Error triggered on item {item!r}")
        emit(item, "main")
        return NullResult()


class SlowStep(BaseStep[NullResult]):
    """Sleeps before emitting item."""

    outputs = ("main",)

    def __init__(self, sleep_seconds: float = 0.1) -> None:
        self.sleep_seconds = sleep_seconds

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> NullResult:
        time.sleep(self.sleep_seconds)
        emit(item, "main")
        return NullResult()


class TerminalStep(BaseStep[NullResult]):
    """Terminal step — outputs = (), emit not allowed."""

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> NullResult:
        # Terminal step should NOT call emit
        return NullResult()


class BranchEvenOddStep(BaseStep[NullResult]):
    """Routes even items to 'even' tag, odd items to 'odd' tag."""

    outputs = ("even", "odd")

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    def process(self, item: int, state: Any, emit: Callable[[Any, str], None]) -> NullResult:
        if item % 2 == 0:
            emit(item, "even")
        else:
            emit(item, "odd")
        return NullResult()


class StateAwareStep(BaseStep[NullResult]):
    """Multiplies item by state['multiplier'] and emits."""

    outputs = ("main",)

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    def process(
        self, item: int, state: dict[str, Any], emit: Callable[[Any, str], None]
    ) -> NullResult:
        emit(state["multiplier"] * item, "main")
        return NullResult()


class CollectorStep(BaseStep[NullResult]):
    """SEQUENTIAL terminal — collects items, sets another step's state on close."""

    def __init__(self, target_step: str = "StateGatedStep") -> None:
        self._collected: list[Any] = []
        self._target_step = target_step

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    @property
    def initial_state(self) -> list[Any]:
        return self._collected

    def process(
        self, item: Any, state: list[Any], emit: Callable[[Any, str], None]
    ) -> NullResult:
        state.append(item)
        return NullResult()

    def close(self) -> None:
        self.set_step_state(self._target_step, list(self._collected))


class StateGatedStep(BaseStep[NullResult]):
    """SEQUENTIAL step gated by is_ready — waits until state is not None."""

    outputs = ("main",)

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    def is_ready(self, state: Any) -> bool:
        return state is not None

    def process(
        self, item: Any, state: list[Any], emit: Callable[[Any, str], None]
    ) -> NullResult:
        emit({"item": item, "state_len": len(state)}, "main")
        return NullResult()


class LifecycleTrackingStep(BaseStep[NullResult]):
    """SEQUENTIAL terminal — tracks open/close calls for lifecycle testing."""

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    def __init__(self, **_kwargs: Any) -> None:
        self.opened = False
        self.closed = False
        self.process_count = 0

    def open(self) -> None:
        self.opened = True

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> NullResult:
        self.process_count += 1
        return NullResult()

    def close(self) -> None:
        self.closed = True


class InitialStateStep(BaseStep[NullResult]):
    """SEQUENTIAL step that provides initial_state and mutates it during process()."""

    outputs = ("main",)

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    @property
    def initial_state(self) -> dict[str, int]:
        return {"count": 0}

    def process(
        self, item: Any, state: dict[str, int], emit: Callable[[Any, str], None]
    ) -> NullResult:
        state["count"] += 1
        emit({"item": item, "count": state["count"]}, "main")
        return NullResult()
