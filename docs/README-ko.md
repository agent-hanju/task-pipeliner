# task-pipeliner

병렬/순차 혼합 스텝, 큐 기반 멀티프로세싱, YAML DAG 정의를 지원하는 Python 데이터 처리 파이프라인 프레임워크.

## 설치

```bash
# 의존성으로 설치 (다른 프로젝트의 pyproject.toml에 추가)
pip install "task-pipeliner @ git+https://github.com/agent-hanju/task-pipeliner.git@v0.2.2"
```

```toml
# pyproject.toml
dependencies = [
    "task-pipeliner @ git+https://github.com/agent-hanju/task-pipeliner.git@v0.2.2",
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

    def process(self, item: Any, emit: Callable[[Any, str], None]) -> None:
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

    def process(self, item: Any, emit: Callable[[Any, str], None]) -> None:
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

## 핵심 개념

### Step 계층 구조

네 가지 추상 기본 클래스:

| 클래스 | 설명 | 핵심 메서드 |
|--------|------|------------|
| `SourceStep` | 첫 번째 스텝 전용. 아이템 생산. | `items()` |
| `SequentialStep` | 단일 스레드 순차 처리. | `process(item, emit)` |
| `AsyncStep` | I/O-bound 비동기 처리 (asyncio). | `process_async(item, emit)` |
| `ParallelStep` | CPU-bound 병렬 처리. | `create_worker() -> Worker` |

**공통 인터페이스** (`StepBase`에서 상속):

| 메서드/속성 | 필수 | 설명 |
|------------|------|------|
| `outputs` | ClassVar | 선언된 출력 태그 튜플. 빈 `()` = 터미널 스텝. |
| `open()` | 선택 | 처리 시작 전 리소스 획득 (1회 호출). |
| `close()` | 선택 | 처리 완료 후 리소스 해제. `open()`과 대칭. |
| `pipe(step, tag)` | 선택 | 출력 태그를 다른 스텝에 연결 (코드 경로). |

**`AsyncStep` 전용:**

| 속성 | 기본값 | 설명 |
|------|--------|------|
| `concurrency` | `8` | 동시 실행 `process_async()` 코루틴 최대 수. |

**`SourceStep` 전용:**

| 메서드 | 기본값 | 설명 |
|--------|--------|------|
| `item_key(item)` | `None` | 체크포인트 중복 제거용 고유 키 반환. `None`이면 스킵. |

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
                                   worker.process(item, emit) × N
                                   worker.close()
step.close()
```

### Pipeline

고수준 파사드:

```python
# YAML 경로
pipeline = Pipeline(workers=4)
pipeline.register("step_name", StepClass)     # 하나 등록
pipeline.register_all({"name": Class, ...})    # 여러 개 등록
stats = pipeline.run(config=Path("pipeline.yaml"), output_dir=path, variables={"key": "val"})

# 코드 경로 (YAML 없이)
pipeline = Pipeline(workers=4)
pipeline.register("source", SourceClass)
pipeline.register("writer", WriterClass)
source = pipeline.step("src", "source", paths=["./data"])
writer = pipeline.step("out", "writer", output_path="./output.jsonl")
source.pipe(writer)
stats = pipeline.run(output_dir=path)
```

`Pipeline.__init__` 파라미터: `workers`, `chunk_size`, `queue_type` (`QueueType.AUTO/SPILL/FULL_DISK`), `queue_size`, `checkpoint_dir`.

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
  queue_size: 0               # SpillQueue 버퍼 크기 (0 = 무제한 인메모리)
  chunk_size: 100             # 병렬 워커당 배치 크기
  queue_type: auto            # auto | spill | full_disk
```

### 변수 치환

설정 값에 `${var}` 플레이스홀더를 사용할 수 있으며, `variables` 파라미터로 로드 시점에 치환됩니다:

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

**타입 보존 치환** — YAML 파싱 후 dict tree를 순회하며 `${var}`를 치환합니다:

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

`type`은 등록된 클래스를 선택하고, `name`(생략 시 `type`과 동일)은 `outputs` 라우팅과 stats 추적에 사용되는 고유 식별자입니다. `name` 없는 기존 설정은 그대로 동작합니다.

### Async 스텝

I/O-bound 작업 (LLM API 호출, HTTP 요청)에는 `ParallelStep` 대신 `AsyncStep`을 사용하세요:

```python
class LLMStep(AsyncStep):
    """Async 스텝: 제한된 동시성으로 LLM API 호출."""
    outputs = ("main",)
    concurrency = 16  # 최대 동시 코루틴 수

    async def process_async(self, item: Any, emit: Callable) -> None:
        result = await call_llm_api(item["text"])
        emit({**item, "result": result}, "main")
```

`AsyncStep`은 단일 스레드에서 asyncio 이벤트 루프를 실행합니다. `asyncio` 네이티브 라이브러리(`aiohttp`, `httpx` 등)에 적합. CPU-bound 작업은 `ParallelStep`을 사용하세요.

### 체크포인트 / 재개

긴 파이프라인에 체크포인트 기반 재개 활성화:

```python
pipeline = Pipeline(checkpoint_dir=Path(".checkpoints"))
stats = pipeline.run(config=Path("pipeline.yaml"), output_dir=Path("./output"))
# 같은 checkpoint_dir로 다음 실행 시, 이미 처리된 아이템 건너뜀
```

또는 YAML:

```yaml
checkpoint_dir: .checkpoints
resume_run_id: <이전_실행의_run_id>   # 선택: 이미 처리된 아이템 스킵

pipeline:
  - type: source
    ...
```

`SourceStep.item_key(item)` 구현 시 아이템별 중복 제거 활성화. `pip install "task-pipeliner[checkpoint]"` 필요.

### 큐 타입

스텝 간 큐 동작 방식 제어:

| `queue_type` | 동작 |
|-------------|------|
| `auto` (기본) | 인메모리 큐; `queue_size > 0`이면 SpillQueue로 전환 |
| `spill` | 메모리 우선, 버퍼 가득 찰 때 디스크로 스필 |
| `full_disk` | 디스크 우선, 모든 아이템 즉시 기록 |

디스크 큐는 `pip install "task-pipeliner[disk-queue]"` 필요.

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
| `Pipeline` | `task_pipeliner.pipeline` | 스텝 등록, 파이프라인 실행 (YAML 또는 코드 경로) |
| `StepBase` | `task_pipeliner.base` | 모든 스텝 타입의 공통 인터페이스 (outputs, open/close, pipe) |
| `SourceStep` | `task_pipeliner.base` | SOURCE 스텝 ABC (`items()`, `item_key()`) |
| `SequentialStep` | `task_pipeliner.base` | 순차 스텝 ABC (`process(item, emit)`) |
| `AsyncStep` | `task_pipeliner.base` | 비동기 I/O-bound 스텝 ABC (`process_async(item, emit)`, `concurrency`) |
| `ParallelStep` | `task_pipeliner.base` | 병렬 스텝 ABC (`create_worker() -> Worker`) |
| `Worker` | `task_pipeliner.base` | 서브프로세스로 전달되는 워커 ABC |
| `PipelineError` | `task_pipeliner.exceptions` | 기본 예외 |
| `StepRegistrationError` | `task_pipeliner.exceptions` | 중복/직렬화 불가 등록 |
| `ConfigValidationError` | `task_pipeliner.exceptions` | 잘못된 YAML 설정 |
| `PipelineConfig` | `task_pipeliner.config` | 파이프라인 설정 Pydantic 모델 (`checkpoint_dir`, `resume_run_id` 포함) |
| `StepConfig` | `task_pipeliner.config` | 스텝 설정 Pydantic 모델 |
| `ExecutionConfig` | `task_pipeliner.config` | 실행 설정 Pydantic 모델 (`queue_type` 포함) |
| `QueueType` | `task_pipeliner.config` | Enum: `AUTO`, `SPILL`, `FULL_DISK` |
| `load_config(path, variables)` | `task_pipeliner.config` | YAML 설정 파일 로드 및 `${var}` 치환 |
| `SpillQueue` | `task_pipeliner.spill_queue` | 메모리 우선, 디스크 스필 큐 |
| `FullDiskQueue` | `task_pipeliner.spill_queue` | 디스크 우선 큐 |
| `CheckpointStore` | `task_pipeliner.checkpoint` | 체크포인트 백엔드 프로토콜 |
| `NullCheckpointStore` | `task_pipeliner.checkpoint` | No-op 체크포인트 스토어 (기본) |
| `DiskCacheCheckpointStore` | `task_pipeliner.checkpoint` | diskcache 기반 체크포인트 스토어 |
| `make_checkpoint_store` | `task_pipeliner.checkpoint` | 팩토리: `DiskCacheCheckpointStore` 또는 `NullCheckpointStore` 반환 |

## Changelog

### v0.2.2

`AsyncStep` 추가 (I/O-bound 비동기 처리), 체크포인트/재개 지원 (`checkpoint_dir`, `resume_run_id`, `item_key()`), 큐 타입 선택 (`QueueType.AUTO/SPILL/FULL_DISK`), `Pipeline.__init__`에 `queue_type`/`checkpoint_dir` 파라미터 추가. `process()` 시그니처에서 `state` 파라미터 제거 (`process(item, emit)`).

### v0.2.1

내부 클래스명을 `*Producer` → `*StepRunner`로 변경하고, 구현을 `producers.py` → `step_runners.py`로 이동. 기존 `producers.py`는 re-export shim으로 유지되므로 외부 API 변경 없음.

## 라이선스

MIT
