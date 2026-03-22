# task-pipeliner

병렬/순차 혼합 스텝, 큐 기반 멀티프로세싱, YAML DAG 정의를 지원하는 Python 데이터 처리 파이프라인 프레임워크.

## 설치

```bash
# 소스에서 설치
git clone https://github.com/your-org/task-pipeliner.git
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
    """SOURCE 스텝: 텍스트 파일에서 줄 단위로 읽기."""
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
    """PARALLEL 스텝: 텍스트 길이로 필터링."""
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
    """SEQUENTIAL 터미널 스텝: 아이템을 JSONL 파일에 기록."""
    outputs = ()  # 터미널 — emit 호출 불가

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

### BaseStep

모든 파이프라인 스텝의 추상 기본 클래스.

| 메서드/속성 | 필수 | 설명 |
|------------|------|------|
| `step_type` | 예 | `StepType.SOURCE`, `PARALLEL`, 또는 `SEQUENTIAL` |
| `process(item, state, emit)` | 예 | 단일 아이템 처리. `emit(item, tag)`으로 출력 라우팅. |
| `outputs` | ClassVar | 선언된 출력 태그 튜플. 빈 `()` = 터미널 스텝. |
| `items()` | SOURCE만 | 입력 아이템을 yield하는 제너레이터. |
| `initial_state` | 선택 | 상태 기반 처리를 위한 초기 state 객체 반환. |
| `is_ready(state)` | 선택 | state가 준비될 때까지 처리를 게이팅 (기본: `True`). |
| `set_step_state(target, state)` | 선택 | 다른 스텝에 state 전달 (`is_ready` 재평가 트리거). |
| `open()` | 선택 | 처리 시작 전 리소스 획득 (`is_ready` 이후 1회 호출). |
| `close()` | 선택 | 처리 완료 후 리소스 해제. `open()`과 대칭. |

**Step 생명주기:**

```
__init__(config) → is_ready() 게이팅 → open() → process() × N → close()
```

> `open()`과 `close()`는 **메인 프로세스**에서만 실행됨. PARALLEL 스텝의 워커 프로세스는 pickle로 복원된 복사본을 받으며 `open()`을 호출하지 않음.

### BaseResult

아이템과 워커에 걸쳐 누적되는 결과 데이터 객체:

- `merge(other)` — 두 결과를 합침 (결합법칙 필수).
- `write(output_dir)` — 최종 결과를 파일로 기록.

### Pipeline

고수준 파사드:

```python
pipeline = Pipeline()
pipeline.register("step_name", StepClass)     # 하나 등록
pipeline.register_all({"name": Class, ...})    # 여러 개 등록
pipeline.run(config=path_or_config, output_dir=path)
```

### StepType

| 타입 | 실행 방식 | 용도 |
|------|----------|------|
| `SOURCE` | 메인 스레드 | 첫 번째 스텝 전용. `items()`로 아이템 생산. |
| `PARALLEL` | ProcessPoolExecutor (spawn 모드) | 무상태 아이템별 처리. CPU 바운드 작업. |
| `SEQUENTIAL` | 단일 스레드 | 상태 기반 처리: 중복 제거, 집계, 파일 쓰기. |

## 설정

파이프라인 토폴로지를 YAML로 정의:

```yaml
pipeline:
  - type: step_name          # 등록된 스텝 이름과 매칭
    enabled: true             # 선택 (기본: true). false면 스킵.
    param1: value1            # 추가 필드 → Step.__init__(**kwargs)
    outputs:                  # DAG 엣지: tag → 다운스트림 스텝
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

- 첫 번째 스텝은 반드시 `SOURCE`. 이후에는 `SOURCE` 불가.
- `outputs` 태그는 알려진 스텝 타입을 참조해야 함.
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

**Fan-in** — 여러 스텝이 하나의 다운스트림 스텝에 입력. 프레임워크가 머저 스레드를 생성하여 큐를 합치고, 모든 업스트림이 완료되어야 센티넬을 전달:

```yaml
- type: processor_a
  outputs: { done: writer }
- type: processor_b
  outputs: { done: writer }   # 둘 다 writer로 입력
```

### State 게이팅

2-pass 알고리즘용 (히스토그램 구축 → 히스토그램으로 정제):

```python
class CollectorStep(BaseStep[R]):
    """Pass 1: 통계를 누적하면서 아이템을 전달."""
    step_type = StepType.SEQUENTIAL
    outputs = ("main",)

    def process(self, item, state, emit):
        self._histogram.update(item)
        emit(item, "main")             # 즉시 전달
        return result

    def close(self):
        # 모든 아이템 처리 후, 게이트된 스텝에 state 전달
        self.set_step_state("CleanerStep", self._histogram)


class CleanerStep(BaseStep[R]):
    """Pass 2: 수집된 통계를 사용하여 아이템 처리."""
    step_type = StepType.SEQUENTIAL
    outputs = ("kept",)

    def is_ready(self, state):
        return state is not None        # state 도착까지 블록

    def process(self, item, state, emit):
        cleaned = apply_histogram(item, state)
        emit(cleaned, "kept")
        return result
```

`is_ready()`가 `False`인 동안 아이템은 `CleanerStep` 큐에 대기. `CollectorStep.close()`가 `set_step_state()`로 state를 전달하면, 게이트된 스텝이 언블록되어 대기 중인 모든 아이템을 처리.

### 안전한 종료

`SIGINT` (Ctrl+C)와 `SIGBREAK` (Windows)는 모든 큐에 센티넬을 주입하여 스레드가 깨끗하게 종료되도록 함. 통계는 보존됨.

### 진행률 표시

실행 중 stderr와 `progress.log`에 실시간 진행률 출력:

```
--- Pipeline Progress (12.3s) -------------------------------------------
  LoaderStep             372 produced                          [done 1.2s]
  FilterStep             372 in → 329 kept, 43 removed  1.2ms/item  [done 4.5s]
  WriterStep              43 in                          0.0ms/item  [idle]
-------------------------------------------------------------------------
```

### 통계

완료 후 출력 디렉토리의 `stats.json`에 스텝별 메트릭:

```json
[
  {
    "step_name": "FilterStep",
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

1. **Result 클래스 정의**: `BaseResult`를 확장하여 `merge()`와 `write()` 구현.

2. **Step 클래스를 모듈 레벨에 정의** (spawn 모드 pickle 필수):
   - `SOURCE` 스텝: `items()`로 입력 데이터 생산.
   - `PARALLEL` 스텝: 무상태 `process()` — CPU 바운드 작업.
   - `SEQUENTIAL` 스텝: 상태 기반 `process()` — 중복 제거, 집계, I/O.
   - 터미널 스텝: `outputs = ()`로 설정, `emit()` 호출 금지.

3. **`pipeline_config.yaml` 작성**: `type`, `outputs`, 스텝별 파라미터로 DAG 정의.

4. **`run.py` 작성**: `Pipeline.register_all()`로 스텝 등록 후 `pipeline.run()` 호출.

5. **실행 및 확인**: `output/stats.json`, `output/pipeline.log`, `output/progress.log` 확인.

### 제약 사항

- **Pickle 규칙**: 모든 스텝 클래스는 모듈 레벨에 정의해야 함. 람다, 클로저, 중첩 클래스 불가. `functools.partial`로 모듈 레벨 함수 래핑은 허용.
- **Spawn 모드**: 워커는 `multiprocessing.get_context("spawn")`을 사용 (Windows/Linux 호환). 엔진이 등록 시점에 pickle 가능 여부를 검증.
- **에러 처리**: `process()` 내 예외는 캐치되어 WARNING으로 로깅되고, 해당 아이템은 스킵됨. stats의 `errored` 카운터가 증가.

## API 레퍼런스

| 클래스 | 모듈 | 설명 |
|--------|------|------|
| `Pipeline` | `task_pipeliner` | 스텝 등록, 파이프라인 실행 |
| `BaseStep[R]` | `task_pipeliner` | 모든 스텝의 추상 기본 클래스 |
| `BaseResult` | `task_pipeliner` | 결과 데이터 추상 기본 클래스 |
| `StepType` | `task_pipeliner` | Enum: `SOURCE`, `PARALLEL`, `SEQUENTIAL` |
| `PipelineError` | `task_pipeliner` | 기본 예외 |
| `StepRegistrationError` | `task_pipeliner` | 중복/pickle 불가 등록 |
| `ConfigValidationError` | `task_pipeliner` | 잘못된 YAML 설정 |
| `PipelineConfig` | `task_pipeliner.config` | 파이프라인 설정 Pydantic 모델 |
| `StepConfig` | `task_pipeliner.config` | 스텝 설정 Pydantic 모델 |
| `ExecutionConfig` | `task_pipeliner.config` | 실행 설정 Pydantic 모델 |
| `load_config(path)` | `task_pipeliner.config` | YAML 설정 파일 로드 및 검증 |

## 라이선스

MIT
