# CLAUDE.md

> Korean version: `docs/CLAUDE-ko.md`
> WBS-based development methodology: `docs/WBS-CLAUDE.md`

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
- `pyproject.toml` — package metadata, dependencies, build config

## Important Notes

- Uses spawn-mode multiprocessing — all functions must be defined at module level (no lambdas, nested functions, or nested classes)
- Step classes in `tests/dummy_steps.py` must also be defined at module level
- Windows + Linux cross-platform compatibility required

## Test Isolation

- Framework tests use only dummy steps from `tests/dummy_steps.py`. No business logic (filter criteria, dedup algorithms, etc.).
- Follow AAA (Arrange-Act-Assert) pattern for all tests.
- Use `@pytest.mark.parametrize` for multiple input scenarios.
- Apply `@pytest.mark.timeout` to all Queue/sentinel tests to detect deadlocks.

## Pickle Rule

All objects passed across processes must be picklable (spawn mode requirement).
- Define all classes and functions at module level (no lambdas, closures, or inner classes)
- `functools.partial` is allowed when wrapping module-level functions
- The engine validates picklability eagerly when registering a new Step class

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
- Child loggers (`task_pipeliner.config`, `task_pipeliner.producers`, …) propagate to it automatically.
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
- `producers.py` — INFO: producer started/finished (step name), sentinel sent/received. WARNING: `process()` exception (item skipped, with truncated repr).
- `engine.py` — INFO: pipeline started (step count, worker count), pipeline completed. WARNING: process join timeout. ERROR: signal-triggered shutdown, worker process crash.
- `pipeline.py` — INFO: pipeline run started/completed (config path, input count).
- `cli.py` — Logging setup only (configures root handler). No direct log calls needed.

**Rules:**
- Never log sensitive data or full item contents — use truncated repr (`repr(item)[:200]`).
- Child processes inherit the logger name but add their own handlers via `StatsCollector.setup_log_handler`.
- `exceptions.py`, `base.py` — No logging (pure data definitions).
