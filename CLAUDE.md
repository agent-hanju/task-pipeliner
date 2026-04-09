# CLAUDE.md

> Korean version: `docs/CLAUDE-ko.md`

## 구현 계획 준수

- `docs/IMPLEMENTATION_PLAN.md` 를 확인하고 준수한다.
- 작업 착수 전 의존 관계와 완료 상태를 반드시 확인한다.
- 작업 완료 시 해당 항목을 ✅로 갱신한다.
- 명시된 TDD 절차(레퍼런스 탐색 → 테스트 작성 → 구현 → 테스트 통과 → 린트/타입 → 계획 갱신)를 건너뛰지 않는다.

## Environment

- Shell: Git Bash (Windows) — use Unix syntax, `/dev/null` not `NUL`, forward slashes in paths
- Python: `.venv/` at project root — always use `.venv/Scripts/python` or `.venv/Scripts/pip` directly
- Install packages: `.venv/Scripts/pip install ".[dev]"`

## Common Commands

```bash
# Run tests
.venv/Scripts/pytest --timeout=30 -v

# Run specific file
.venv/Scripts/pytest tests/test_queue.py

# Coverage
.venv/Scripts/pytest --cov=task_pipeliner

# Lint
.venv/Scripts/ruff check src tests

# Format
.venv/Scripts/ruff format src tests

# Type check
.venv/Scripts/mypy src
```

## Project Structure

- `src/task_pipeliner/` — package source (`src/` layout)
- `tests/` — framework tests (dummy steps only, no business logic)
- `docs/` — PRD, tech stack, WBS documents
- `sample/` — sample projects using task-pipeliner
- `pyproject.toml` — package metadata, dependencies, build config

## Sample Projects

When working under the `sample/` directory, follow each sample project's own `CLAUDE.md` in addition to this file.

- `sample/taxonomy-converter/` — Naver news taxonomy converter (`sample/taxonomy-converter/CLAUDE.md`)
- `sample/pretrain-data-filter/` — Korean pretrain data quality filter + dedup pipeline (`sample/pretrain-data-filter/CLAUDE.md`)

## Important Notes

- Uses spawn-mode multiprocessing — all functions must be defined at module level (no lambdas, nested functions, or nested classes)
- Step classes in `tests/dummy_steps.py` must also be defined at module level
- Windows + Linux cross-platform compatibility required

## Test Isolation

- Framework tests use only dummy steps from `tests/dummy_steps.py`. No business logic (filter criteria, dedup algorithms, etc.).
- Follow AAA (Arrange-Act-Assert) pattern for all tests.
- Use `@pytest.mark.parametrize` for multiple input scenarios.
- Apply `@pytest.mark.timeout` to all Queue/sentinel tests to detect deadlocks.

## Step Hierarchy

Three ABC base classes replace the old monolithic `BaseStep`:

- `SourceStep` — implements `items()` to yield input data
- `SequentialStep` — implements `process(item, state, emit)` for single-thread stateful work
- `ParallelStep` — implements `create_worker() -> Worker` for multi-process CPU-bound work

`Worker` is a separate ABC whose instances are pickled and sent to worker processes.

## Step Lifecycle

**SequentialStep:**
```
__init__(config) → [is_ready() gating] → open() → process() × N → close()
```

**ParallelStep:**
```
Main process:                      Worker process N:
step.__init__(config)
worker = step.create_worker()
pickle.dumps(worker)
[is_ready() gating]
step.open()
                                   worker = pickle.loads(...)
                                   worker.open()
                                   worker.process() × N
                                   worker.close()
step.close()
```

- `__init__`: Config injection only. Do NOT acquire runtime resources (file handles, connections) here.
- `open()` / `close()`: Main-process lifecycle. Acquire/release resources (files, connections, temp dirs).
- `Worker.open()` / `Worker.close()`: Per-worker-process lifecycle. Use for per-process resources (DB connections, model loading, GPU context).
- `create_worker()`: Called before `step.open()` so the Worker doesn't capture unpicklable main-process resources.

## Pickle Rule

All objects passed across processes must be picklable (spawn mode requirement).
- Define all Step, Worker, and utility classes/functions at module level (no lambdas, closures, or inner classes)
- `functools.partial` is allowed when wrapping module-level functions
- The engine validates picklability eagerly when registering a new Step class
- For `ParallelStep`: the `Worker` returned by `create_worker()` must be picklable. The `ParallelStep` itself is never pickled.

## Logging Strategy

Logging must be added **during implementation of each module**, not retroactively.

**Logger setup — per-module, hierarchical:**

Each module creates its own logger with `__name__`. This produces a hierarchy under `task_pipeliner`:

```python
# In every module (e.g., config.py):
import logging
logger = logging.getLogger(__name__)  # → "task_pipeliner.config"
```

- Parent logger `task_pipeliner` owns the handler (set up by `StatsCollector.setup_log_handler`).
- Child loggers (`task_pipeliner.config`, `task_pipeliner.step_runners`, …) propagate to it automatically.
- The formatter includes module, function, and line — no need to manually write function names in messages.

**Log format:**

```python
"%(asctime)s %(levelname)-5s %(name)s:%(funcName)s:%(lineno)d %(message)s"
# Output example:
# 2026-03-19 10:00:01 DEBUG task_pipeliner.config:load_config:42 path=/data/config.yaml
# 2026-03-19 10:00:01 INFO  task_pipeliner.io:open:58 Writer opened output=/out
```

**Level guidelines:**

| Level | When to use |
|-------|-------------|
| `DEBUG` | Function entry (key parameters), internal branching, result summary before return |
| `INFO` | Key domain events within the main logic (lifecycle transitions, I/O completion, important outputs) |
| `WARNING` | Recoverable issues caught in `try/except` — processing continues |
| `ERROR` | Severe failures — pipeline stops or should stop |

**Standard function structure (follow this pattern for every non-trivial function):**

```python
def do_something(self, path: Path, count: int) -> Result:
    logger.debug("path=%s count=%d", path, count)             # entry — funcName auto-filled

    # 1. Parameter validation (early return / raise on invalid)
    if count <= 0:
        raise ConfigValidationError("count must be positive", field="count")

    # 2. Main logic — INFO for key domain events, DEBUG for branching
    logger.info("Processing started for %s", path)
    items = self._load(path)
    logger.debug("loaded %d items", len(items))

    # 3. Error handling — WARNING if recovered, re-raise or wrap if fatal
    try:
        result = self._transform(items)
    except SomeRecoverableError:
        logger.warning("Transform failed for %s, skipping", path)
        return Result.empty()

    # 4. Summarize result
    logger.debug("returning %d results", len(result))
    return result
```

Key points:
- First line: `DEBUG` with key parameters (function name is auto-filled by formatter)
- Validation block: no logging — just raise with a clear message
- Main logic: `INFO` for what the operator needs to see, `DEBUG` for decisions/branching
- `try/except`: `WARNING` if swallowed, propagate (raise/wrap) if fatal
- Last line before return: `DEBUG` with result summary

**Per-module expectations:**

- `config.py` — INFO: config loaded (path, step count). WARNING: step disabled.
- `io.py` — INFO: reader opened (file count), writer opened/closed (output path).
- `stats.py` — INFO: stats JSON written. WARNING: write failure suppressed.
- `step_runners.py` — INFO: step runner started/finished (step name), sentinel sent/received. WARNING: `process()` exception (item skipped, with truncated repr).
- `engine.py` — INFO: pipeline started (step count, worker count), pipeline completed. WARNING: process join timeout. ERROR: signal-triggered shutdown, worker process crash.
- `pipeline.py` — INFO: pipeline run started/completed (config path, input count).
- `cli.py` — Logging setup only (configures root handler). No direct log calls needed.

**Rules:**
- Never log sensitive data or full item contents — use truncated repr (`repr(item)[:200]`).
- Child processes inherit the logger name but add their own handlers via `StatsCollector.setup_log_handler`.
- `exceptions.py`, `base.py` — No logging (pure data definitions).
