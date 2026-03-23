# task-pipeliner

A configurable Python data processing pipeline framework with mixed parallel/sequential steps, queue-based multiprocessing, and YAML-driven DAG definition.

## Installation

```bash
# From source
git clone https://github.com/your-org/task-pipeliner.git
cd task-pipeliner
python -m venv .venv
.venv/Scripts/pip install ".[dev]"   # Windows
# .venv/bin/pip install ".[dev]"     # Linux/macOS
```

## Quick Start

### 1. Define your steps

```python
# steps.py
from collections.abc import Callable, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from task_pipeliner import BaseResult, BaseStep, StepType


@dataclass
class CountResult(BaseResult):
    kept: int = 0
    removed: int = 0

    def merge(self, other: "CountResult") -> "CountResult":
        return CountResult(kept=self.kept + other.kept, removed=self.removed + other.removed)

    def write(self, output_dir: Path) -> None:
        import json
        (output_dir / "summary.json").write_text(json.dumps({"kept": self.kept, "removed": self.removed}))


class LoaderStep(BaseStep[CountResult]):
    """SOURCE step: reads lines from text files."""
    outputs = ("main",)

    @property
    def step_type(self) -> StepType:
        return StepType.SOURCE

    def __init__(self, paths: list[str] | None = None, **_: Any) -> None:
        self._paths = [Path(p) for p in (paths or [])]

    def items(self) -> Generator[dict[str, Any], None, None]:
        for path in self._paths:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    yield {"text": line.strip()}

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> CountResult:
        raise NotImplementedError


class FilterStep(BaseStep[CountResult]):
    """PARALLEL step: filters items by text length."""
    outputs = ("kept", "removed")

    @property
    def step_type(self) -> StepType:
        return StepType.PARALLEL

    def __init__(self, min_length: int = 10, **_: Any) -> None:
        self._min_length = min_length

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> CountResult:
        if len(item.get("text", "")) >= self._min_length:
            emit(item, "kept")
            return CountResult(kept=1)
        emit(item, "removed")
        return CountResult(removed=1)


class WriterStep(BaseStep[CountResult]):
    """SEQUENTIAL terminal step: writes items to a JSONL file."""
    outputs = ()  # terminal — no emit allowed

    @property
    def step_type(self) -> StepType:
        return StepType.SEQUENTIAL

    def __init__(self, output_path: str = "output.jsonl", **_: Any) -> None:
        self._path = Path(output_path)

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._path, "w", encoding="utf-8")

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> CountResult:
        import json
        self._fh.write(json.dumps(item) + "\n")
        return CountResult(kept=1)

    def close(self) -> None:
        self._fh.close()
```

### 2. Write the pipeline config

```yaml
# pipeline_config.yaml
pipeline:
  - type: loader
    paths: ["./data/input.txt"]
    outputs:
      main: filter

  - type: filter
    min_length: 10
    outputs:
      kept: writer
      removed: writer

  - type: writer

execution:
  workers: 4
  chunk_size: 100
```

### 3. Run the pipeline

```python
# run.py
from pathlib import Path
from task_pipeliner import Pipeline
from steps import LoaderStep, FilterStep, WriterStep

pipeline = Pipeline()
pipeline.register_all({
    "loader": LoaderStep,
    "filter": FilterStep,
    "writer": WriterStep,
})
pipeline.run(config=Path("pipeline_config.yaml"), output_dir=Path("./output"))
```

Or via CLI:

```bash
task-pipeliner run --config pipeline_config.yaml --output ./output
```

## Core Concepts

### BaseStep

The abstract base class for all pipeline steps.

| Method/Property | Required | Description |
|----------------|----------|-------------|
| `step_type` | Yes | `StepType.SOURCE`, `PARALLEL`, or `SEQUENTIAL` |
| `process(item, state, emit)` | Yes | Process a single item. Call `emit(item, tag)` to route outputs. |
| `outputs` | ClassVar | Tuple of declared output tags. Empty `()` = terminal step. |
| `items()` | SOURCE only | Generator that yields input items. |
| `initial_state` | Optional | Returns initial state object for stateful processing. |
| `is_ready(state)` | Optional | Gate processing until state is available (default: `True`). |
| `set_step_state(target, state)` | Optional | Push state to another step (triggers `is_ready` re-evaluation). |
| `open()` | Optional | Acquire resources before processing begins (called once, after `is_ready`). |
| `close()` | Optional | Release resources after processing completes. Paired with `open()`. |

**Step lifecycle:**

```
__init__(config) → is_ready() gating → open() → process() × N → close()
```

> `open()` and `close()` run on the **main process** only. For PARALLEL steps, worker processes receive pickle-restored copies and do not call `open()`.

### BaseResult

Result data objects that accumulate across items and workers:

- `merge(other)` — Combine two results (must be associative).
- `write(output_dir)` — Write final results to files.

### Pipeline

High-level facade:

```python
pipeline = Pipeline()
pipeline.register("step_name", StepClass)     # register one
pipeline.register_all({"name": Class, ...})    # register many
pipeline.run(config=path_or_config, output_dir=path)
```

### StepType

| Type | Execution | Use case |
|------|-----------|----------|
| `SOURCE` | Main thread | First step only. Yields items via `items()`. |
| `PARALLEL` | ProcessPoolExecutor (spawn mode) | Stateless per-item processing. CPU-bound work. |
| `SEQUENTIAL` | Single thread | Stateful processing: dedup, aggregation, file writes. |

## Configuration

Pipeline topology is defined in YAML:

```yaml
pipeline:
  - type: step_name          # Matches registered step class name
    name: instance_name       # Optional instance name (defaults to type)
    enabled: true             # Optional (default: true). Set false to skip.
    param1: value1            # Extra fields → Step.__init__(**kwargs)
    outputs:                  # DAG edges: tag → downstream step name(s)
      kept: next_step         # Single target
      removed:                # Multiple targets (fan-out)
        - step_a
        - step_b

execution:
  workers: 4                  # ProcessPoolExecutor worker count
  queue_size: 0               # Reserved for future disk-spill (0 = unbounded)
  chunk_size: 100             # Items per batch for parallel workers
```

### Config Rules

- First step must be `SOURCE`. No `SOURCE` steps after the first.
- Each step `name` must be unique. When omitted, defaults to `type`.
- `outputs` tags must reference step names (not types).
- Steps with `outputs = ()` are terminal (calling `emit()` raises `RuntimeError`).
- Extra config fields are passed as `**kwargs` to `Step.__init__()`.

## Advanced Features

### Fan-out / Fan-in

**Fan-out** — one step emits to different downstream steps by tag:

```yaml
- type: classifier
  outputs:
    category_a: processor_a
    category_b: processor_b
```

**Fan-in** — multiple steps feed into one. The framework spawns merger threads and waits for all upstreams to complete before forwarding the sentinel:

```yaml
- type: processor_a
  outputs: { done: writer }
- type: processor_b
  outputs: { done: writer }   # Both feed into writer
```

### Multiple Instances of the Same Step Type

Use `name` to create multiple instances of the same registered step type with different configs:

```yaml
pipeline:
  - type: source
    items: ["./data/input.jsonl"]
    outputs:
      main: strict_filter

  - type: quality_filter
    name: strict_filter        # Unique instance name
    min_score: 0.9
    outputs:
      kept: good_writer

  - type: quality_filter
    name: loose_filter         # Same type, different config
    min_score: 0.3
    outputs:
      kept: bad_writer

  - type: writer
    name: good_writer
    output_path: ./kept.jsonl

  - type: writer
    name: bad_writer
    output_path: ./removed.jsonl
```

The `type` field selects which registered class to instantiate. The `name` field (defaulting to `type` when omitted) is the unique identifier used for `outputs` routing, stats tracking, and `set_step_state()` targeting. Existing configs without `name` work unchanged.

### State Gating

For two-pass algorithms (build histogram → clean using histogram):

```python
class CollectorStep(BaseStep[R]):
    """Pass 1: Accumulate statistics while forwarding items."""
    step_type = StepType.SEQUENTIAL
    outputs = ("main",)

    def process(self, item, state, emit):
        self._histogram.update(item)
        emit(item, "main")             # Forward immediately
        return result

    def close(self):
        # After all items processed, dispatch state to gated step (by config name)
        self.set_step_state("cleaner", self._histogram)


class CleanerStep(BaseStep[R]):
    """Pass 2: Process items using collected statistics."""
    step_type = StepType.SEQUENTIAL
    outputs = ("kept",)

    def is_ready(self, state):
        return state is not None        # Block until state arrives

    def process(self, item, state, emit):
        cleaned = apply_histogram(item, state)
        emit(cleaned, "kept")
        return result
```

Items queue up in `CleanerStep` while `is_ready()` returns `False`. Once `CollectorStep.close()` dispatches state via `set_step_state()`, the gated step unblocks and processes all queued items.

### Graceful Shutdown

`SIGINT` (Ctrl+C) and `SIGBREAK` (Windows) inject sentinels into all queues, allowing threads to exit cleanly. Statistics are preserved.

### Progress Reporting

Real-time progress is printed to stderr and written to `progress.log`:

```
--- Pipeline Progress (12.3s) -------------------------------------------
  loader                 372 produced                          [done 1.2s]
  filter                 372 in → 329 kept, 43 removed  1.2ms/item  [done 4.5s]
  writer                  43 in                          0.0ms/item  [idle]
-------------------------------------------------------------------------
```

### Statistics

After completion, `stats.json` in the output directory contains per-step metrics:

```json
[
  {
    "step_name": "filter",
    "processed": 1000,
    "errored": 2,
    "emitted": {"kept": 850, "removed": 148},
    "elapsed_seconds": 5.2,
    "processing_seconds": 4.8,
    "processing_avg_ms": 4.8,
    "idle_seconds": 0.4,
    "current_state": "done"
  }
]
```

## CLI Usage

```bash
# Run a single pipeline
task-pipeliner run --config pipeline.yaml --output ./output [--workers 8]

# Run multiple jobs from a JSON array file
task-pipeliner batch jobs.json
```

**Jobs file format**:

```json
[
  {"config": "config1.yaml", "output_dir": "./out1"},
  {"config": "config2.yaml", "output_dir": "./out2"}
]
```

> Note: The CLI creates a bare `Pipeline()` with no steps registered. For custom steps, use the programmatic API in your project's `run.py`.

## Creating Your Own Pipeline

### Step-by-step

1. **Define a Result class** extending `BaseResult` with `merge()` and `write()`.

2. **Define Step classes** at module level (required for spawn-mode pickle):
   - `SOURCE` step: implement `items()` to yield input data.
   - `PARALLEL` steps: stateless `process()` for CPU-bound work.
   - `SEQUENTIAL` steps: stateful `process()` for dedup, aggregation, I/O.
   - Terminal steps: set `outputs = ()` and don't call `emit()`.

3. **Write `pipeline_config.yaml`**: define the step DAG with `type`, `outputs`, and step-specific params.

4. **Write `run.py`**: register steps via `Pipeline.register_all()` and call `pipeline.run()`.

5. **Run and inspect**: check `output/stats.json`, `output/pipeline.log`, `output/progress.log`.

### Constraints

- **Pickle rule**: All step classes must be defined at module level. No lambdas, closures, or nested classes. `functools.partial` wrapping module-level functions is allowed.
- **Spawn mode**: Workers use `multiprocessing.get_context("spawn")` for Windows/Linux compatibility. The engine validates picklability at registration time.
- **Error handling**: Exceptions in `process()` are caught, logged as WARNING, and the item is skipped. The `errored` counter increments in stats.

## API Reference

| Class | Module | Description |
|-------|--------|-------------|
| `Pipeline` | `task_pipeliner` | Register steps, run pipeline |
| `BaseStep[R]` | `task_pipeliner` | Abstract base for all steps |
| `BaseResult` | `task_pipeliner` | Abstract base for result data |
| `StepType` | `task_pipeliner` | Enum: `SOURCE`, `PARALLEL`, `SEQUENTIAL` |
| `PipelineError` | `task_pipeliner` | Base exception |
| `StepRegistrationError` | `task_pipeliner` | Duplicate/unpicklable registration |
| `ConfigValidationError` | `task_pipeliner` | Invalid YAML config |
| `PipelineConfig` | `task_pipeliner.config` | Pydantic model for pipeline config |
| `StepConfig` | `task_pipeliner.config` | Pydantic model for step config |
| `ExecutionConfig` | `task_pipeliner.config` | Pydantic model for execution settings |
| `load_config(path)` | `task_pipeliner.config` | Load and validate YAML config |

## License

MIT
