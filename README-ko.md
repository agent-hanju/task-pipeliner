<p align="center">
  <strong>task-pipeliner</strong><br/>
  <em>Step을 정의하고, YAML로 연결하면 파이프라인이 완성됩니다.</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.12+-3776ab?logo=python&logoColor=white" alt="Python 3.12+"/>
  <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT License"/>
  <img src="https://img.shields.io/badge/multiprocessing-spawn_mode-orange" alt="Spawn Mode"/>
</p>

<p align="center">
  <a href="./README.md">English</a>
</p>

---

## Why task-pipeliner?

데이터 파이프라인을 만들 때마다 반복되는 코드가 있습니다 — 큐 연결, 프로세스 관리, 종료 시그널, 에러 핸들링, 통계 수집. task-pipeliner는 이 **boilerplate를 모두 제거**하고, **Step 클래스 하나만 정의하면 나머지는 프레임워크가 처리**합니다.

### 핵심 강점

**Stateful + Stateless를 자유롭게 혼합** — DB 커넥션을 유지하는 순차 Step과 CPU를 풀로 활용하는 병렬 Step을 하나의 파이프라인에서 자연스럽게 연결합니다.

**아이템 스트리밍** — 배치가 아니라 아이템이 하나씩 큐를 타고 흐릅니다. 메모리 효율이 높고, 각 Step이 동시에 동작합니다.

**YAML 한 장으로 토폴로지 정의** — Step 간 연결, 파라미터, Fan-out/Fan-in을 코드 수정 없이 설정만으로 변경합니다.

**`pip install` 하나면 끝** — Redis, broker, 데몬 없이 순수 Python만으로 동작합니다.

---

## 파이프라인 구조

```
SourceStep ──→ Queue ──→ ParallelStep ──→ Queue ──→ SequentialStep
 (데이터 생성)     (자동)   (N workers,       (자동)    (stateful,
                           CPU-bound)                  single-thread)
```

> Step을 정의하면 Queue 연결, 프로세스 생성, Sentinel 종료, 통계 수집은 모두 자동으로 처리됩니다.

---

## 설치

```bash
pip install "task-pipeliner @ git+https://github.com/agent-hanju/task-pipeliner.git@v0.1.0"
```

<details>
<summary>pyproject.toml에 의존성으로 추가</summary>

```toml
dependencies = [
    "task-pipeliner @ git+https://github.com/agent-hanju/task-pipeliner.git@v0.1.0",
]
```
</details>

<details>
<summary>소스에서 개발 설치</summary>

```bash
git clone https://github.com/agent-hanju/task-pipeliner.git
cd task-pipeliner
python -m venv .venv
.venv/Scripts/pip install ".[dev]"   # Windows
# .venv/bin/pip install ".[dev]"     # Linux/macOS
```
</details>

---

## Quick Start

### 1. Step 정의

```python
# steps.py
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

from task_pipeliner import ParallelStep, SequentialStep, SourceStep, Worker


class LoaderStep(SourceStep):
    """텍스트 파일에서 라인을 읽어 아이템으로 생성"""
    outputs = ("main",)

    def __init__(self, paths: list[str] | None = None, **_: Any) -> None:
        self._paths = [Path(p) for p in (paths or [])]

    def items(self) -> Generator[dict[str, Any], None, None]:
        for path in self._paths:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    yield {"text": line.strip()}


class FilterWorker(Worker):
    """텍스트 길이 기반 필터링 워커"""

    def __init__(self, min_length: int) -> None:
        self._min_length = min_length

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        if len(item.get("text", "")) >= self._min_length:
            emit(item, "kept")
        else:
            emit(item, "removed")


class FilterStep(ParallelStep):
    """N개 워커로 병렬 필터링"""
    outputs = ("kept", "removed")

    def __init__(self, min_length: int = 10, **_: Any) -> None:
        self._min_length = min_length

    def create_worker(self) -> FilterWorker:
        return FilterWorker(self._min_length)


class WriterStep(SequentialStep):
    """JSONL 파일 작성 (순차, stateful)"""
    outputs = ()  # terminal step

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

### 2. YAML로 파이프라인 연결

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

### 3. 실행

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

---

## Step 타입

세 가지 ABC를 상속받아 Step을 만듭니다.

### SourceStep — 데이터 생성

파이프라인의 시작점. `items()`에서 아이템을 yield합니다.

```python
class MySource(SourceStep):
    outputs = ("main",)

    def items(self):
        for record in load_data():
            yield record
```

### ParallelStep — 병렬 처리

CPU-bound 작업을 N개 워커 프로세스로 분산합니다. `Worker` 클래스를 별도 정의합니다.

```python
class MyWorker(Worker):
    def open(self):                         # 워커 프로세스 시작 시 1회
        self._model = load_model()

    def process(self, item, state, emit):   # 아이템마다 호출
        result = self._model.predict(item)
        emit(result, "main")

    def close(self):                        # 워커 프로세스 종료 시 1회
        del self._model

class MyParallelStep(ParallelStep):
    outputs = ("main",)

    def create_worker(self) -> MyWorker:
        return MyWorker()
```

**Worker 라이프사이클:**

```
Main process                        Worker process (x N)
────────────                        ────────────────────
step.__init__(config)
worker = step.create_worker()
pickle.dumps(worker) ─────────→     worker = pickle.loads(...)
step.open()                         worker.open()
                                    worker.process() x N
                                    worker.close()
step.close()
```

### SequentialStep — 순차 처리

파일 핸들, DB 커넥션 등 **상태를 유지하며** 순서대로 처리합니다.

```python
class MyWriter(SequentialStep):
    outputs = ()  # terminal step

    def open(self):
        self._conn = db.connect()

    def process(self, item, state, emit):
        self._conn.insert(item)

    def close(self):
        self._conn.close()
```

### 공통 인터페이스 (StepBase)

| Method / Property | 필수 | 설명 |
|---|---|---|
| `outputs` | ClassVar | 선언된 출력 태그 튜플. `()` = terminal step |
| `open()` / `close()` | Optional | 리소스 획득/해제 (메인 프로세스) |
| `initial_state` | Optional | 초기 state 객체 반환 |
| `is_ready(state)` | Optional | state가 준비될 때까지 처리 차단 (기본: `True`) |
| `get_output_state()` | Optional | `close()` 후 다른 Step에 state 전달 |

---

## YAML 설정

### 기본 구조

```yaml
pipeline:
  - type: step_name          # 등록된 Step 클래스 이름
    name: instance_name       # 인스턴스 이름 (기본값: type)
    enabled: true             # false로 비활성화 가능
    param1: value1            # Step.__init__(**kwargs)로 전달
    outputs:
      kept: next_step         # 단일 대상
      removed:                # 복수 대상 (fan-out)
        - step_a
        - step_b

execution:
  workers: 4                  # 병렬 워커 수
  queue_size: 0               # 큐 크기 (0 = 무제한)
  chunk_size: 100             # 워커당 배치 크기
```

### 변수 치환

`${var}` 플레이스홀더를 런타임에 치환합니다.

```yaml
pipeline:
  - type: loader
    paths: ["${input_dir}"]
    outputs: { main: filter }

  - type: writer
    output_path: ${output_dir}/result.jsonl
```

```python
pipeline.run(
    config=Path("pipeline_config.yaml"),
    output_dir=Path("./output"),
    variables={"input_dir": "./data", "output_dir": "./output"},
)
```

| YAML 값 | 변수 값 | 결과 | 타입 |
|---|---|---|---|
| `${input_dir}` | `"/data"` | `"/data"` | `str` |
| `${paths}` | `["/a.jsonl", "/b.jsonl"]` | `["/a.jsonl", "/b.jsonl"]` | `list` |
| `${threshold}` | `42` | `42` | `int` |
| `${output_dir}/result.jsonl` | `"/out"` | `"/out/result.jsonl"` | `str` |
| `${mode:-fast}` | *(미제공)* | `"fast"` | `str` (기본값) |
| `$${NOT_A_VAR}` | — | `"${NOT_A_VAR}"` | `str` (이스케이프) |

### 설정 규칙

- 첫 번째 Step은 반드시 `SourceStep`. 이후에는 `SourceStep` 불가.
- 각 Step의 `name`은 유일해야 함. 생략 시 `type`이 기본값.
- `outputs` 태그는 Step의 `name`을 참조.
- `outputs = ()` Step은 terminal — `emit()` 호출 시 `RuntimeError`.

---

## 고급 기능

### Fan-out / Fan-in

**Fan-out** — 태그에 따라 서로 다른 Step으로 분기:

```yaml
- type: classifier
  outputs:
    category_a: processor_a
    category_b: processor_b
```

**Fan-in** — 여러 Step이 하나의 Step에 합류:

```yaml
- type: processor_a
  outputs: { done: writer }
- type: processor_b
  outputs: { done: writer }   # 둘 다 writer로 합류
```

### 같은 타입의 Step을 여러 인스턴스로

`name`으로 구분하여 동일 타입을 다른 설정으로 복수 생성합니다.

```yaml
pipeline:
  - type: source
    items: ["./data/input.jsonl"]
    outputs: { main: strict_filter }

  - type: quality_filter
    name: strict_filter
    min_score: 0.9
    outputs: { kept: good_writer }

  - type: quality_filter
    name: loose_filter
    min_score: 0.3
    outputs: { kept: bad_writer }

  - type: writer
    name: good_writer
    output_path: ./kept.jsonl

  - type: writer
    name: bad_writer
    output_path: ./removed.jsonl
```

### State Gating — 2-Pass 알고리즘

1단계에서 통계를 수집하고, 2단계에서 그 통계를 기반으로 처리하는 패턴입니다.

```python
class CollectorStep(SequentialStep):
    """Pass 1: 통계 수집 + 아이템 포워딩"""
    outputs = ("main",)

    def process(self, item, state, emit):
        self._histogram.update(item)
        emit(item, "main")

    def get_output_state(self):
        return {"cleaner": self._histogram}


class CleanerStep(SequentialStep):
    """Pass 2: 수집된 통계로 처리 (state 도착까지 대기)"""
    outputs = ("kept",)

    def is_ready(self, state):
        return state is not None

    def process(self, item, state, emit):
        cleaned = apply_histogram(item, state)
        emit(cleaned, "kept")
```

> `CleanerStep`은 `is_ready()`가 `True`를 반환할 때까지 아이템을 큐에 쌓아두고, `CollectorStep.close()` 후 state가 전달되면 일괄 처리를 시작합니다.

### Graceful Shutdown

`SIGINT` (Ctrl+C) / `SIGBREAK` (Windows) 수신 시 모든 큐에 sentinel을 주입하여 깔끔하게 종료합니다. 통계는 보존됩니다.

### 실시간 진행 상황

실행 중 stderr와 `progress.log`에 실시간 현황이 표시됩니다.

```
--- Pipeline Progress (12.3s) -------------------------------------------
  loader                 372 produced                          [done 1.2s]
  filter                 372 in → 329 kept, 43 removed  1.2ms/item  [done 4.5s]
  writer                  43 in                          0.0ms/item  [idle]
-------------------------------------------------------------------------
```

### 실행 통계

완료 후 `output/stats.json`에 Step별 상세 메트릭이 기록됩니다.

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

---

## 사용 시 주의사항

| 규칙 | 이유 |
|---|---|
| Step, Worker 클래스는 **모듈 최상위**에 정의 | spawn 모드 pickle 호환 |
| `__init__`에서 리소스(파일, 커넥션) 획득 금지 | `open()`에서 획득, `close()`에서 해제 |
| lambda, 클로저, 내부 클래스 사용 금지 | pickle 불가 (`functools.partial`은 허용) |
| 등록 시 picklability 자동 검증 | 런타임 에러 방지 |

---

## API Reference

| Class | Module | 설명 |
|---|---|---|
| `Pipeline` | `task_pipeliner.pipeline` | Step 등록 및 파이프라인 실행 |
| `SourceStep` | `task_pipeliner.base` | 데이터 생성 Step (ABC) |
| `SequentialStep` | `task_pipeliner.base` | 순차 처리 Step (ABC) |
| `ParallelStep` | `task_pipeliner.base` | 병렬 처리 Step (ABC) |
| `Worker` | `task_pipeliner.base` | 워커 프로세스용 객체 (ABC) |
| `load_config(path, variables)` | `task_pipeliner.config` | YAML 설정 로드 + 변수 치환 |
| `PipelineConfig` | `task_pipeliner.config` | 파이프라인 설정 모델 |
| `PipelineError` | `task_pipeliner.exceptions` | 기본 예외 |
| `StepRegistrationError` | `task_pipeliner.exceptions` | 등록 오류 |
| `ConfigValidationError` | `task_pipeliner.exceptions` | 설정 검증 오류 |

---

## License

MIT
