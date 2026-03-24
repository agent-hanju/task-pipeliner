# task-pipeliner

A configurable Python data processing pipeline framework with mixed parallel/sequential steps, queue-based multiprocessing, and YAML-driven DAG definition.

## Installation

```bash
# As a dependency (in your pyproject.toml)
pip install "task-pipeliner @ git+https://github.com/agent-hanju/task-pipeliner.git@v0.1.0"
```

```toml
# pyproject.toml
dependencies = [
    "task-pipeliner @ git+https://github.com/agent-hanju/task-pipeliner.git@v0.1.0",
]
```

```bash
# From source (for development)
git clone https://github.com/agent-hanju/task-pipeliner.git
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
from pathlib import Path
from typing import Any

from task_pipeliner import ParallelStep, SequentialStep, SourceStep, Worker


class LoaderStep(SourceStep):
    """SOURCE step: reads lines from text files."""
    outputs = ("main",)

    def __init__(self, paths: list[str] | None = None, **_: Any) -> None:
        self._paths = [Path(p) for p in (paths or [])]

    def items(self) -> Generator[dict[str, Any], None, None]:
        for path in self._paths:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    yield {"text": line.strip()}


class FilterWorker(Worker):
    """Worker for FilterStep: filters items by text length."""

    def __init__(self, min_length: int) -> None:
        self._min_length = min_length

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        if len(item.get("text", "")) >= self._min_length:
            emit(item, "kept")
        else:
            emit(item, "removed")


class FilterStep(ParallelStep):
    """PARALLEL step: filters items by text length."""
    outputs = ("kept", "removed")

    def __init__(self, min_length: int = 10, **_: Any) -> None:
        self._min_length = min_length

    def create_worker(self) -> FilterWorker:
        return FilterWorker(self._min_length)


class WriterStep(SequentialStep):
    """SEQUENTIAL terminal step: writes items to a JSONL file."""
    outputs = ()  # terminal — no emit allowed

    def __init__(self, output_path: str = "output.jsonl", **_: Any) -> None:
        self._path = Path(output_path)

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._path, "w", encoding="utf-8")

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        import json
        self._fh.write(json.dumps(item) + "\n")

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

## Core Concepts

### Step Hierarchy

Three abstract base classes for different step types:

| Class | Description | Key Method |
|-------|-------------|------------|
| `SourceStep` | First step only. Yields items. | `items()` |
| `ParallelStep` | CPU-bound parallel processing. | `create_worker() -> Worker` |
| `SequentialStep` | Stateful single-thread processing. | `process(item, state, emit)` |

**Common interface** (inherited from `StepBase`):

| Method/Property | Required | Description |
|----------------|----------|-------------|
| `outputs` | ClassVar | Tuple of declared output tags. Empty `()` = terminal step. |
| `initial_state` | Optional | Returns initial state object for stateful processing. |
| `is_ready(state)` | Optional | Gate processing until state is available (default: `True`). |
| `get_output_state()` | Optional | Return `{target: state}` dict to dispatch state to other steps after `close()`. |
| `open()` | Optional | Acquire resources before processing begins (called once, after `is_ready`). |
| `close()` | Optional | Release resources after processing completes. Paired with `open()`. |

### Worker

A separate picklable class for `ParallelStep`. Workers run in spawned processes.

```python
class MyWorker(Worker):
    def __init__(self, config_param: int) -> None:
        self.config_param = config_param
        self._model = None  # lazy init not needed — use open()

    def open(self) -> None:
        """Called once per worker process."""
        self._model = load_model()

    def process(self, item: Any, state: Any, emit: Callable) -> None:
        result = self._model.predict(item)
        emit(result, "main")

    def close(self) -> None:
        """Called once per worker process on shutdown."""
        del self._model

class MyStep(ParallelStep):
    outputs = ("main",)

    def __init__(self, config_param: int = 42) -> None:
        self.config_param = config_param

    def create_worker(self) -> MyWorker:
        return MyWorker(self.config_param)
```

**Lifecycle:**

```
Main process:                      Worker process N:
─────────────                      ─────────────────
step = ParallelStep(**config)
worker = step.create_worker()
worker_bytes = pickle.dumps(worker)
step.open()
                                   worker = pickle.loads(worker_bytes)
                                   worker.open()
                                   worker.process(item, state, emit) × N
                                   worker.close()
step.close()
```

### Pipeline

High-level facade:

```python
pipeline = Pipeline()
pipeline.register("step_name", StepClass)     # register one
pipeline.register_all({"name": Class, ...})    # register many
pipeline.run(config=path_or_config, output_dir=path, variables={"key": "val"})
```

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

### Variable Substitution

Config values can use `${var}` placeholders, which are substituted at load time via `variables` parameter:

```yaml
# pipeline_config.yaml
pipeline:
  - type: loader
    paths:
      - ${input_dir}
    outputs:
      main: filter

  - type: filter
    min_length: 10
    outputs:
      kept: writer

  - type: writer
    output_path: ${output_dir}/result.jsonl

execution:
  workers: 4
```

```python
# run.py
import sys
from pathlib import Path
from task_pipeliner import Pipeline
from steps import LoaderStep, FilterStep, WriterStep

input_dir = sys.argv[1]
output_dir = sys.argv[2]

pipeline = Pipeline()
pipeline.register_all({
    "loader": LoaderStep,
    "filter": FilterStep,
    "writer": WriterStep,
})
pipeline.run(
    config=Path("pipeline_config.yaml"),
    output_dir=Path(output_dir),
    variables={"input_dir": input_dir, "output_dir": output_dir},
)
```

```bash
python run.py ./data ./output
```

**Type-preserving substitution** — YAML parse 후 dict tree를 순회하며 `${var}`를 치환합니다:

| YAML 값 | 변수 값 | 치환 결과 | 결과 타입 |
|----------|---------|-----------|-----------|
| `${input_dir}` | `"/data"` | `"/data"` | `str` |
| `${paths}` | `["/a.jsonl", "/b.jsonl"]` | `["/a.jsonl", "/b.jsonl"]` | `list` |
| `${threshold}` | `42` | `42` | `int` |
| `${output_dir}/result.jsonl` | `"/out"` | `"/out/result.jsonl"` | `str` |
| `${mode:-fast}` | *(미제공)* | `"fast"` | `str` |
| `$${NOT_A_VAR}` | — | `"${NOT_A_VAR}"` | `str` |

- 값 전체가 `${var}`인 경우 → 변수 값을 그대로 대입 (list, dict, int 등 모든 타입)
- 문자열 내부에 `${var}`가 포함된 경우 → `str()` 변환 후 문자열 치환
- `${var:-default}` — 변수 미제공 시 `default` 값 사용
- `$${var}` — 이스케이프, 리터럴 `${var}` 문자열로 출력
- `variables` 제공 시 미해결 `${var}` (기본값 없음) → `ConfigValidationError` 발생
- `variables` 미제공 시 `${...}`는 일반 문자열로 처리 (하위 호환)

### Config Rules

- First step must be `SourceStep`. No `SourceStep` after the first.
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

**Fan-in** — multiple steps feed into one. The downstream uses a shared input queue and waits for all upstream sentinels before terminating:

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

The `type` field selects which registered class to instantiate. The `name` field (defaulting to `type` when omitted) is the unique identifier used for `outputs` routing, stats tracking, and `get_output_state()` targeting. Existing configs without `name` work unchanged.

### State Gating

For two-pass algorithms (build histogram → clean using histogram):

```python
class CollectorStep(SequentialStep):
    """Pass 1: Accumulate statistics while forwarding items."""
    outputs = ("main",)

    def process(self, item, state, emit):
        self._histogram.update(item)
        emit(item, "main")             # Forward immediately

    def get_output_state(self):
        # After all items processed, dispatch state to gated step (by config name)
        return {"cleaner": self._histogram}


class CleanerStep(SequentialStep):
    """Pass 2: Process items using collected statistics."""
    outputs = ("kept",)

    def is_ready(self, state):
        return state is not None        # Block until state arrives

    def process(self, item, state, emit):
        cleaned = apply_histogram(item, state)
        emit(cleaned, "kept")
```

Items queue up in `CleanerStep` while `is_ready()` returns `False`. Once `CollectorStep.close()` completes and the StepRunner calls `get_output_state()`, the returned state is dispatched to the gated step, which unblocks and processes all queued items.

### Graceful Shutdown

`SIGINT` (Ctrl+C) and `SIGBREAK` (Windows) inject sentinels into all queues, allowing threads to exit cleanly. Statistics are preserved.

### Progress Reporting

Real-time progress is printed to stderr and saved to `progress.log` (overwritten each cycle — only the latest snapshot is kept):

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

## Creating Your Own Pipeline

### Step-by-step

1. **Define Step classes** at module level (required for spawn-mode pickle):
   - `SourceStep`: implement `items()` to yield input data.
   - `ParallelStep`: implement `create_worker()` returning a `Worker` with `process()`.
   - `SequentialStep`: implement `process()` for stateful/sequential work.
   - Terminal steps: set `outputs = ()` and don't call `emit()`.

2. **Write `pipeline_config.yaml`**: define the step DAG with `type`, `outputs`, and step-specific params.

3. **Write `run.py`**: register steps via `Pipeline.register_all()` and call `pipeline.run()`.

4. **Run and inspect**: check `output/stats.json`, `output/pipeline.log` (execution log), `output/progress.log` (final progress snapshot).

### Constraints

- **Pickle rule**: All step and worker classes must be defined at module level. No lambdas, closures, or nested classes. `functools.partial` wrapping module-level functions is allowed.
- **Spawn mode**: Workers use `multiprocessing.get_context("spawn")` for Windows/Linux compatibility. The engine validates picklability at registration time.
- **Error handling**: Exceptions in `process()` are caught, logged as WARNING, and the item is skipped. The `errored` counter increments in stats.

## API Reference

| Class | Module | Description |
|-------|--------|-------------|
| `Pipeline` | `task_pipeliner.pipeline` | Register steps, run pipeline |
| `StepRegistry` | `task_pipeliner.pipeline` | Step name → class mapping with pickle validation |
| `StepBase` | `task_pipeliner.base` | Common interface for all step types (outputs, state, lifecycle) |
| `SourceStep` | `task_pipeliner.base` | ABC for SOURCE steps (yields items) |
| `SequentialStep` | `task_pipeliner.base` | ABC for SEQUENTIAL steps (process in main thread) |
| `ParallelStep` | `task_pipeliner.base` | ABC for PARALLEL steps (create workers for subprocesses) |
| `Worker` | `task_pipeliner.base` | ABC for worker objects sent to subprocesses |
| `PipelineError` | `task_pipeliner.exceptions` | Base exception |
| `StepRegistrationError` | `task_pipeliner.exceptions` | Duplicate/unpicklable registration |
| `ConfigValidationError` | `task_pipeliner.exceptions` | Invalid YAML config |
| `PipelineConfig` | `task_pipeliner.config` | Pydantic model for pipeline config |
| `StepConfig` | `task_pipeliner.config` | Pydantic model for step config |
| `ExecutionConfig` | `task_pipeliner.config` | Pydantic model for execution settings |
| `load_config(path, variables)` | `task_pipeliner.config` | Load YAML config with optional `${var}` substitution |

## Changelog

### v0.2.1

내부 클래스명을 `*Producer` → `*StepRunner`로 변경하고, 구현을 `producers.py` → `step_runners.py`로 이동. 기존 `producers.py`는 re-export shim으로 유지되므로 외부 API 변경 없음.

## License

MIT
