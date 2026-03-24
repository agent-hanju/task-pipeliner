# task-pipeliner

병렬/순차 혼합 스텝, 큐 기반 멀티프로세싱, YAML DAG 정의를 지원하는 Python 데이터 처리 파이프라인 프레임워크.

## 설치

```bash
# 의존성으로 설치 (다른 프로젝트의 pyproject.toml에 추가)
pip install "task-pipeliner @ git+https://github.com/agent-hanju/task-pipeliner.git@v0.1.0"
```

```toml
# pyproject.toml
dependencies = [
    "task-pipeliner @ git+https://github.com/agent-hanju/task-pipeliner.git@v0.1.0",
]
```

```bash
# 소스에서 설치 (개발용)
git clone https://github.com/agent-hanju/task-pipeliner.git
cd task-pipeliner
python -m venv .venv
.venv/Scripts/pip install ".[dev]"   # Windows
# .venv/bin/pip install ".[dev]"     # Linux/macOS
```

## 빠른 시작

### 1. 스텝 정의

```python
# steps.py
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

from task_pipeliner import ParallelStep, SequentialStep, SourceStep, Worker


class LoaderStep(SourceStep):
    """SOURCE 스텝: 텍스트 파일에서 줄 단위로 읽기."""
    outputs = ("main",)

    def __init__(self, paths: list[str] | None = None, **_: Any) -> None:
        self._paths = [Path(p) for p in (paths or [])]

    def items(self) -> Generator[dict[str, Any], None, None]:
        for path in self._paths:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    yield {"text": line.strip()}


class FilterWorker(Worker):
    """FilterStep의 워커: 텍스트 길이로 필터링."""

    def __init__(self, min_length: int) -> None:
        self._min_length = min_length

    def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> None:
        if len(item.get("text", "")) >= self._min_length:
            emit(item, "kept")
        else:
            emit(item, "removed")


class FilterStep(ParallelStep):
    """PARALLEL 스텝: 텍스트 길이로 필터링."""
    outputs = ("kept", "removed")

    def __init__(self, min_length: int = 10, **_: Any) -> None:
        self._min_length = min_length

    def create_worker(self) -> FilterWorker:
        return FilterWorker(self._min_length)


class WriterStep(SequentialStep):
    """SEQUENTIAL 터미널 스텝: 아이템을 JSONL 파일에 기록."""
    outputs = ()  # 터미널 — emit 호출 불가

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

### 2. 파이프라인 설정 작성

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

### 3. 파이프라인 실행

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

또는 CLI로:

```bash
task-pipeliner run --config pipeline_config.yaml --output ./output
```

## 핵심 개념

### Step 계층 구조

세 가지 추상 기본 클래스:

| 클래스 | 설명 | 핵심 메서드 |
|--------|------|------------|
| `SourceStep` | 첫 번째 스텝 전용. 아이템 생산. | `items()` |
| `ParallelStep` | CPU-bound 병렬 처리. | `create_worker() -> Worker` |
| `SequentialStep` | 상태 기반 단일 스레드 처리. | `process(item, state, emit)` |

**공통 인터페이스** (`StepBase`에서 상속):

| 메서드/속성 | 필수 | 설명 |
|------------|------|------|
| `outputs` | ClassVar | 선언된 출력 태그 튜플. 빈 `()` = 터미널 스텝. |
| `initial_state` | 선택 | 상태 기반 처리를 위한 초기 state 객체 반환. |
| `is_ready(state)` | 선택 | state가 준비될 때까지 처리를 게이팅 (기본: `True`). |
| `get_output_state()` | 선택 | `close()` 후 `{target: state}` 딕셔너리를 반환하여 다른 스텝에 state 전달. |
| `open()` | 선택 | 처리 시작 전 리소스 획득 (`is_ready` 이후 1회 호출). |
| `close()` | 선택 | 처리 완료 후 리소스 해제. `open()`과 대칭. |

### Worker

`ParallelStep`용 별도의 picklable 클래스. Worker는 스폰된 프로세스에서 실행됩니다.

```python
class MyWorker(Worker):
    def __init__(self, config_param: int) -> None:
        self.config_param = config_param
        self._model = None  # lazy init 불필요 — open() 사용

    def open(self) -> None:
        """워커 프로세스당 1회 호출."""
        self._model = load_model()

    def process(self, item: Any, state: Any, emit: Callable) -> None:
        result = self._model.predict(item)
        emit(result, "main")

    def close(self) -> None:
        """워커 프로세스 종료 시 1회 호출."""
        del self._model

class MyStep(ParallelStep):
    outputs = ("main",)

    def __init__(self, config_param: int = 42) -> None:
        self.config_param = config_param

    def create_worker(self) -> MyWorker:
        return MyWorker(self.config_param)
```

**생명주기:**

```
메인 프로세스:                     워커 프로세스 N:
──────────────                     ────────────────
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

고수준 파사드:

```python
pipeline = Pipeline()
pipeline.register("step_name", StepClass)     # 하나 등록
pipeline.register_all({"name": Class, ...})    # 여러 개 등록
pipeline.run(config=path_or_config, output_dir=path)
```

## 설정

파이프라인 토폴로지를 YAML로 정의:

```yaml
pipeline:
  - type: step_name          # 등록된 스텝 클래스 이름과 매칭
    name: instance_name       # 선택. 인스턴스 이름 (기본: type과 동일)
    enabled: true             # 선택 (기본: true). false면 스킵.
    param1: value1            # 추가 필드 → Step.__init__(**kwargs)
    outputs:                  # DAG 엣지: tag → 다운스트림 스텝 name
      kept: next_step         # 단일 타겟
      removed:                # 복수 타겟 (fan-out)
        - step_a
        - step_b

execution:
  workers: 4                  # ProcessPoolExecutor 워커 수
  queue_size: 0               # 향후 디스크 스필용 예약 (0 = 무제한)
  chunk_size: 100             # 병렬 워커당 배치 크기
```

### 설정 규칙

- 첫 번째 스텝은 반드시 `SourceStep`. 이후에는 `SourceStep` 불가.
- 각 스텝의 `name`은 고유해야 함. 생략 시 `type` 값이 `name`으로 사용됨.
- `outputs` 태그는 스텝 name을 참조해야 함 (type이 아님).
- `outputs = ()` 스텝은 터미널 (`emit()` 호출 시 `RuntimeError`).
- 추가 설정 필드는 `Step.__init__(**kwargs)`로 전달됨.

## 고급 기능

### Fan-out / Fan-in

**Fan-out** — 하나의 스텝이 태그별로 다른 다운스트림 스텝에 출력:

```yaml
- type: classifier
  outputs:
    category_a: processor_a
    category_b: processor_b
```

**Fan-in** — 여러 스텝이 하나의 다운스트림 스텝에 입력. 다운스트림은 공유 입력 큐를 사용하며, 모든 업스트림의 센티넬이 도착해야 종료:

```yaml
- type: processor_a
  outputs: { done: writer }
- type: processor_b
  outputs: { done: writer }   # 둘 다 writer로 입력
```

### 같은 타입의 스텝 다중 인스턴스

`name`을 사용하면 같은 등록 타입의 스텝을 서로 다른 설정으로 여러 번 사용할 수 있습니다:

```yaml
pipeline:
  - type: source
    items: ["./data/input.jsonl"]
    outputs:
      main: strict_filter

  - type: quality_filter
    name: strict_filter        # 고유 인스턴스 이름
    min_score: 0.9
    outputs:
      kept: good_writer

  - type: quality_filter
    name: loose_filter         # 같은 타입, 다른 설정
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

`type`은 등록된 클래스를 선택하고, `name`(생략 시 `type`과 동일)은 `outputs` 라우팅, stats 추적, `get_output_state()` 대상 지정에 사용되는 고유 식별자입니다. `name` 없는 기존 설정은 그대로 동작합니다.

### State 게이팅

2-pass 알고리즘용 (히스토그램 구축 → 히스토그램으로 정제):

```python
class CollectorStep(SequentialStep):
    """Pass 1: 통계를 누적하면서 아이템을 전달."""
    outputs = ("main",)

    def process(self, item, state, emit):
        self._histogram.update(item)
        emit(item, "main")             # 즉시 전달

    def get_output_state(self):
        # 모든 아이템 처리 후, 게이트된 스텝에 state 전달 (설정의 name 기준)
        return {"cleaner": self._histogram}


class CleanerStep(SequentialStep):
    """Pass 2: 수집된 통계를 사용하여 아이템 처리."""
    outputs = ("kept",)

    def is_ready(self, state):
        return state is not None        # state 도착까지 블록

    def process(self, item, state, emit):
        cleaned = apply_histogram(item, state)
        emit(cleaned, "kept")
```

`is_ready()`가 `False`인 동안 아이템은 `CleanerStep` 큐에 대기. `CollectorStep.close()` 완료 후 Producer가 `get_output_state()`를 호출하여 반환된 state를 게이트된 스텝에 전달하면, 해당 스텝이 언블록되어 대기 중인 모든 아이템을 처리.

### 안전한 종료

`SIGINT` (Ctrl+C)와 `SIGBREAK` (Windows)는 모든 큐에 센티넬을 주입하여 스레드가 깨끗하게 종료되도록 함. 통계는 보존됨.

### 진행률 표시

실행 중 stderr에 실시간 진행률 출력, `progress.log`에는 최신 스냅샷만 저장 (매 갱신 시 덮어쓰기):

```
--- Pipeline Progress (12.3s) -------------------------------------------
  loader                 372 produced                          [done 1.2s]
  filter                 372 in → 329 kept, 43 removed  1.2ms/item  [done 4.5s]
  writer                  43 in                          0.0ms/item  [idle]
-------------------------------------------------------------------------
```

### 통계

완료 후 출력 디렉토리의 `stats.json`에 스텝별 메트릭:

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

## CLI 사용법

```bash
# 단일 파이프라인 실행
task-pipeliner run --config pipeline.yaml --output ./output [--workers 8]

# JSON 배열 파일에서 여러 작업 실행
task-pipeliner batch jobs.json
```

**작업 파일 형식**:

```json
[
  {"config": "config1.yaml", "output_dir": "./out1"},
  {"config": "config2.yaml", "output_dir": "./out2"}
]
```

> 참고: CLI는 스텝이 등록되지 않은 빈 `Pipeline()`을 생성합니다. 커스텀 스텝은 프로젝트의 `run.py`에서 프로그래밍 API를 사용하세요.

## 나만의 파이프라인 만들기

### 단계별 가이드

1. **Step 클래스를 모듈 레벨에 정의** (spawn 모드 pickle 필수):
   - `SourceStep`: `items()`로 입력 데이터 생산.
   - `ParallelStep`: `create_worker()`로 `Worker` 반환. Worker에 `process()` 구현.
   - `SequentialStep`: 상태 기반/순차 작업을 위한 `process()` 구현.
   - 터미널 스텝: `outputs = ()`로 설정, `emit()` 호출 금지.

2. **`pipeline_config.yaml` 작성**: `type`, `outputs`, 스텝별 파라미터로 DAG 정의.

3. **`run.py` 작성**: `Pipeline.register_all()`로 스텝 등록 후 `pipeline.run()` 호출.

4. **실행 및 확인**: `output/stats.json`, `output/pipeline.log` (실행 로그), `output/progress.log` (최종 진행률 스냅샷) 확인.

### 제약 사항

- **Pickle 규칙**: 모든 스텝 및 Worker 클래스는 모듈 레벨에 정의해야 함. 람다, 클로저, 중첩 클래스 불가. `functools.partial`로 모듈 레벨 함수 래핑은 허용.
- **Spawn 모드**: 워커는 `multiprocessing.get_context("spawn")`을 사용 (Windows/Linux 호환). 엔진이 등록 시점에 pickle 가능 여부를 검증.
- **에러 처리**: `process()` 내 예외는 캐치되어 WARNING으로 로깅되고, 해당 아이템은 스킵됨. stats의 `errored` 카운터가 증가.

## API 레퍼런스

| 클래스 | 모듈 | 설명 |
|--------|------|------|
| `Pipeline` | `task_pipeliner.pipeline` | 스텝 등록, 파이프라인 실행 |
| `StepRegistry` | `task_pipeliner.pipeline` | 스텝 이름 → 클래스 매핑 (pickle 검증 포함) |
| `StepBase` | `task_pipeliner.base` | 모든 스텝 타입의 공통 인터페이스 (outputs, state, lifecycle) |
| `SourceStep` | `task_pipeliner.base` | SOURCE 스텝 ABC (아이템 생산) |
| `SequentialStep` | `task_pipeliner.base` | SEQUENTIAL 스텝 ABC (메인 스레드에서 처리) |
| `ParallelStep` | `task_pipeliner.base` | PARALLEL 스텝 ABC (서브프로세스에 Worker 전달) |
| `Worker` | `task_pipeliner.base` | 서브프로세스로 전달되는 워커 ABC |
| `PipelineError` | `task_pipeliner.exceptions` | 기본 예외 |
| `StepRegistrationError` | `task_pipeliner.exceptions` | 중복/pickle 불가 등록 |
| `ConfigValidationError` | `task_pipeliner.exceptions` | 잘못된 YAML 설정 |
| `PipelineConfig` | `task_pipeliner.config` | 파이프라인 설정 Pydantic 모델 |
| `StepConfig` | `task_pipeliner.config` | 스텝 설정 Pydantic 모델 |
| `ExecutionConfig` | `task_pipeliner.config` | 실행 설정 Pydantic 모델 |
| `load_config(path)` | `task_pipeliner.config` | YAML 설정 파일 로드 및 검증 |

## 라이선스

MIT
