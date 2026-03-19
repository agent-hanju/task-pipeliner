# WBS: Task Pipeliner 개발 계획

> 의존 관계 없는 작업 단위는 병렬 작업 가능. 각 단위는 구현 파일 + 테스트 파일로 구성.
> 각 작업 단위 완료 시 해당 체크박스를 체크한다. 체크 기준: **구현 + 테스트 전부 통과**.

---

## 의존 관계 맵

```
[exceptions] ──┬──▶ [base] ──┬──▶ [dummy_steps]
               │             └──▶ [producers] ──▶ [engine] ──▶ [pipeline] ──▶ [cli]
               └──▶ [config] ──┘        ▲               ▲
                                        │               │
[stats] ────────────────────────────────┘           [io] ──────┘
```

- `exceptions`, `stats`, `io` 는 서로 독립 → Phase 1/2에서 병렬 작업 가능
- `producers` 테스트 파일(`test_queue`, `test_fanout`, `test_backpressure`, `test_error`)은 `producers.py` 구현 단위가 완료되면 독립 병렬 작업 가능
- `engine` 이후는 순차

---

## Phase 1 — 기반 스키마 (순차)

> 다른 모든 작업의 전제. 완료 후 Phase 2, 3 병렬 착수 가능.

### - [x] W-01 `exceptions.py` + 테스트
**파일**: `src/task_pipeliner/exceptions.py`, `tests/test_schema.py`

- [x] 레퍼런스 탐색 (Python 예외 계층 패턴)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
# 클래스 목록
class PipelineError(Exception)
class StepRegistrationError(PipelineError)   # step_name: str 포함
class ConfigValidationError(PipelineError)   # field: str | None, cause 포함
class PipelineShutdownError(PipelineError)
```

테스트:
- 각 예외가 `PipelineError`의 하위 클래스인지 `issubclass` 확인
- `PipelineError`로 catch 가능한지 `@pytest.mark.parametrize` 확인

---

### - [x] W-02 `base.py` + 테스트
**파일**: `src/task_pipeliner/base.py`, `tests/test_schema.py`
**의존**: W-01

- [x] 레퍼런스 탐색 (ABC, Enum, abstractmethod, Generic, TypeVar, Self)
- [x] 테스트 작성 (red) — BaseResult, BaseStep[R], process(item, state, emit) -> R
- [x] 구현 — BaseResult 인터페이스 + BaseStep[R] 제너릭 + emit 콜백
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class StepType(Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"

class BaseResult(ABC):
    """step의 process() 반환 타입이 구현해야 하는 인터페이스.
    병합 규칙과 파일 출력 규칙을 결과 객체 자체가 보유한다."""
    @abstractmethod
    def merge(self, other: Self) -> Self              # 두 결과를 병합하는 규칙
    @abstractmethod
    def write(self, output_dir: Path) -> None         # 파일로 출력하는 규칙

R = TypeVar("R", bound=BaseResult)

class BaseStep(ABC, Generic[R]):
    @property
    def name(self) -> str                                                  # 기본값: 클래스명
    @property @abstractmethod
    def step_type(self) -> StepType
    @abstractmethod
    def process(self, item: Any, state: Any, emit: Callable[[Any], None]) -> R
    # emit: Producer가 제공하는 콜백. 다음 큐로 아이템을 보낼 때 호출.
    # 리턴값 R: 이 아이템 처리의 결과 데이터. Producer가 원자적으로 merge.

class BaseAggStep(ABC):
    @property
    def name(self) -> str                                    # 기본값: 클래스명
    @abstractmethod
    def process_batch(self, items: list[Any]) -> Any        # 반환값 = 다음 stage state
```

테스트:
- `BaseStep()` 직접 인스턴스화 → `TypeError`
- `BaseResult()` 직접 인스턴스화 → `TypeError`
- `name` 기본값 = 클래스명, 오버라이드 가능
- `step_type` 미구현 시 인스턴스화 불가
- `BaseResult` 구현체의 `merge()` 동작 확인 (더미 구현)
- `BaseResult` 구현체의 `write()` 파일 생성 확인 (더미 구현)
- `process(item, state, emit)` 시그니처 — emit 호출 시 아이템 수집 확인

---

### - [x] W-03 `config.py` + 테스트
**파일**: `src/task_pipeliner/config.py`, `tests/test_schema.py`
**의존**: W-01

- [x] 레퍼런스 탐색 (Pydantic v2: BaseModel, ConfigDict, field_validator, model_validator)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 로깅 추가 (INFO: config loaded with path+step count; WARNING: step disabled)
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class StepConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    type: str
    enabled: bool = True
    # 나머지 kwargs → model_extra

class ExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workers: int = 4
    queue_size: int = 200
    chunk_size: int = 100
    # @field_validator: workers/queue_size/chunk_size 양수 검증

class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pipeline: list[StepConfig]
    execution: ExecutionConfig = ExecutionConfig()
    # @model_validator: pipeline 비어있으면 ValidationError

def load_config(path: Path) -> PipelineConfig
# yaml.safe_load → PipelineConfig(**data)
# 실패 시 ConfigValidationError(field, cause=원본예외) 로 변환
```

테스트 (`@pytest.mark.parametrize` 로 다수 케이스):
- 정상 YAML → `PipelineConfig` 파싱, 필드 값 일치
- `type` 없는 step / 빈 pipeline / `extra` 필드 금지 → `ConfigValidationError`
- `enabled: false` 보존, `execution` 기본값 적용
- `StepConfig` 추가 kwargs → `model_extra`에 보존
- `workers=0` → `ConfigValidationError`

---

## Phase 2 — 지원 레이어 (W-01 완료 후 병렬 작업 가능)

> `stats`, `io`, `dummy_steps` 세 작업 단위는 서로 독립. 동시 진행 가능.

### - [x] W-04 `stats.py` + 테스트
**파일**: `src/task_pipeliner/stats.py`, `tests/test_stats.py`
**의존**: 없음 (stdlib + orjson만 사용)

- [x] 레퍼런스 탐색 (orjson.dumps 옵션, logging FileHandler, 원자적 파일 쓰기)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 로깅 추가 (INFO: stats JSON written; WARNING: write failure suppressed)
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
@dataclass
class StepStats:
    step_name: str
    passed: int = 0
    filtered: int = 0
    errored: int = 0
    _start_time: float = field(default_factory=time.monotonic)
    _end_time: float | None = None

    def finish(self) -> None
    @property
    def elapsed_seconds(self) -> float
    def to_dict(self) -> dict

class StatsCollector:
    def register(self, step_name: str) -> StepStats
    def increment(self, step_name: str, field: str, n: int = 1) -> None  # Lock 보호
    def finish(self, step_name: str) -> None
    def setup_log_handler(self, path: Path) -> None  # FileHandler 전용
    def write_json(self, path: Path) -> None          # 원자적 쓰기 (tmp → rename)
    def flush(self) -> None
```

테스트:
- `increment` 후 카운터 정확성
- `write_json` → 유효 JSON, 출력 디렉토리 자동 생성
- 쓰기 실패 시 예외 미전파 (파이프라인 계속)
- `elapsed_seconds` — `finish()` 전후 값 변화

---

### - [x] W-05 `io.py` + 테스트
**파일**: `src/task_pipeliner/io.py`, `tests/test_io.py`
**의존**: 없음 (pathlib + orjson만 사용)

- [x] 레퍼런스 탐색 (orjson.dumps for JSONL, pathlib glob, context manager 프로토콜)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 로깅 추가 (INFO: reader opened with file count, writer opened/closed with path)
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class JsonlReader:
    def __init__(self, paths: list[Path]) -> None
    def _resolve_paths(self) -> list[Path]  # 디렉토리 → *.jsonl glob 확장
    def read(self) -> Generator[dict, None, None]

class JsonlWriter:
    def __init__(self, output_dir: Path) -> None
    def open(self) -> None                                  # 디렉토리 생성 + 파일 오픈 (덮어쓰기)
    def write_kept(self, item: dict) -> None
    def write_removed(self, item: dict, reason: str) -> None  # {"item":…, "reason": step_name}
    def close(self) -> None
    def __enter__(self) / __exit__(self, …)
```

테스트 (`@pytest.mark.parametrize("n", [0, 1, 100])`):
- 단일 파일 / 디렉토리 / 빈 파일 읽기 건수
- `write_kept` 건수, `write_removed` reason 필드
- 출력 디렉토리 자동 생성, 기존 파일 덮어쓰기

---

### - [x] W-06 `dummy_steps.py`
**파일**: `tests/dummy_steps.py`
**의존**: W-02 (`base.py`)

- [x] 레퍼런스 탐색 (pickle 호환성, module-level 클래스 패턴)
- [x] 구현 — `process(item, state, emit)` 시그니처 + NullResult/CountResult 구현
- [x] ruff / mypy 통과

> 모든 프레임워크 테스트의 기반. 비즈니스 로직 없음. 모두 **모듈 레벨** 정의.

```python
# --- 더미 결과 타입 ---

@dataclass
class NullResult(BaseResult):
    """결과 데이터 없는 step용. merge/write 모두 no-op."""
    def merge(self, other: Self) -> Self: return self
    def write(self, output_dir: Path) -> None: pass

@dataclass
class CountResult(BaseResult):
    """아이템 통과/필터 건수를 추적하는 결과."""
    passed: int = 0
    filtered: int = 0
    def merge(self, other: Self) -> Self:
        return CountResult(passed=self.passed + other.passed,
                           filtered=self.filtered + other.filtered)
    def write(self, output_dir: Path) -> None:
        (output_dir / "count_result.json").write_bytes(
            orjson.dumps({"passed": self.passed, "filtered": self.filtered}))

# --- 더미 step ---

class PassthroughStep(BaseStep[NullResult]):
    step_type = StepType.PARALLEL
    def process(self, item, state, emit) -> NullResult:
        emit(item)
        return NullResult()

class FilterEvenStep(BaseStep[CountResult]):
    step_type = StepType.PARALLEL
    def process(self, item: int, state, emit) -> CountResult:
        if item % 2 == 0:
            emit(item)
            return CountResult(passed=1)
        return CountResult(filtered=1)

class ErrorOnItemStep(BaseStep[NullResult]):
    step_type = StepType.PARALLEL
    def __init__(self, error_value: Any = -1)
    def process(self, item, state, emit) -> NullResult:
        if item == self.error_value:
            raise RuntimeError(f"error on {item!r}")
        emit(item)
        return NullResult()

class SlowStep(BaseStep[NullResult]):
    step_type = StepType.PARALLEL
    def __init__(self, sleep_seconds: float = 0.1)
    def process(self, item, state, emit) -> NullResult:
        time.sleep(self.sleep_seconds)
        emit(item)
        return NullResult()

class CountingAggStep(BaseAggStep):
    def process_batch(self, items: list) -> Counter  # Counter(items) 반환

class StateAwareStep(BaseStep[NullResult]):
    step_type = StepType.SEQUENTIAL
    def process(self, item: int, state: dict, emit) -> NullResult:
        emit(state["multiplier"] * item)
        return NullResult()
```

---

## Phase 3 — 실행 코어 (W-02, W-04, W-06 완료 후)

### - [x] W-07 `producers.py` — Sentinel + BaseProducer
**파일**: `src/task_pipeliner/producers.py` (일부)
**의존**: W-02, W-04

- [x] 레퍼런스 탐색 (multiprocessing.Process spawn 모드, Queue, Event, Flink Collector 패턴)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 로깅 추가 (INFO: _send_sentinel with output queue count; DEBUG: 함수 진입/종료)
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class Sentinel
class ErrorSentinel(Sentinel):
    exc: BaseException
    step_name: str

def is_sentinel(obj: object) -> bool

class BaseProducer(ABC, multiprocessing.Process):
    def __init__(self,
        step: BaseStep[R],
        input_queue: Queue,
        output_queues: list[Queue],
        stats: StatsCollector,
        result_queue: Queue,                           # NEW: 결과 데이터 수신용 큐
        state: Any = None,
        ready_events: list[Event] | None = None,
        next_state_setter: Callable[[Any], None] | None = None,
    )
    def _make_emit(self) -> Callable[[Any], None]      # NEW: output_queues에 put + stats.passed 증가
    def _wait_until_ready(self) -> None                # 모든 ready_events.wait()
    def _send_sentinel(self) -> None                   # output_queues 각각에 Sentinel
    def _publish_result(self, result: BaseResult) -> None  # NEW: result_queue에 누적 결과 전송
    @abstractmethod
    def run(self) -> None
```

---

### - [x] W-08 `SequentialProducer` + 기본 Queue 테스트
**파일**: `src/task_pipeliner/producers.py` (SequentialProducer), `tests/test_queue.py` (Sequential 섹션)
**의존**: W-07, W-06

- [x] 레퍼런스 탐색 (Queue.get sentinel 패턴, Process.run)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 로깅 추가 (DEBUG: run 진입/종료; INFO: producer started/finished, sentinel received; WARNING: process() exception)
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class SequentialProducer(BaseProducer):
    def run(self) -> None
    #   accumulated: R | None = None
    #   try:
    #     _wait_until_ready()
    #     emit = self._make_emit()
    #     while True: item = input_queue.get()
    #       is_sentinel → break
    #       try:
    #         result = step.process(item, state, emit) → R
    #         accumulated = result if accumulated is None
    #                       else accumulated.merge(result)
    #       except: stats.increment(errored) + logging.warning (파이프라인 계속)
    #   finally:                                         # 에러/시그널에도 반드시 실행
    #     if accumulated: _publish_result(accumulated)
    #     stats.finish() → next_state_setter?(있으면) → _send_sentinel()
```

테스트 (`@pytest.mark.timeout(15)`):
- `@pytest.mark.parametrize("n", [0, 1, 10, 100])` — N개 입력 → N개 출력 (PassthroughStep)
- `FilterEvenStep` — 10개 입력 → 5개 emit, 모두 짝수
- 빈 input_queue → Sentinel 도달 확인
- result_queue에서 수신한 결과 객체의 `passed + filtered == n` 확인 (CountResult)
- `stats.passed` == emit 호출 횟수 확인

---

### - [x] W-09 `ParallelProducer` + 기본 Queue 테스트
**파일**: `src/task_pipeliner/producers.py` (ParallelProducer), `tests/test_queue.py` (Parallel 섹션)
**의존**: W-07, W-06

- [x] 레퍼런스 탐색 (ProcessPoolExecutor, concurrent.futures.as_completed, chunk 패턴)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 로깅 추가 (DEBUG: run 진입/종료; INFO: producer started/finished, chunk submitted, sentinel received; WARNING: worker exception)
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
# 모듈 레벨 worker 함수 (pickle 필수)
def _parallel_worker(chunk: list, step: BaseStep[R], state: Any,
                     output_queue: Queue) -> R | None
# 각 아이템에 대해:
#   result = step.process(item, state, output_queue.put)
#   chunk_accumulated = chunk_accumulated.merge(result)
# 반환: chunk_accumulated (chunk 전체의 병합된 결과)

class ParallelProducer(BaseProducer):
    def __init__(self, …, workers: int = 4, chunk_size: int = 100)
    def run(self) -> None
    #   accumulated: R | None = None
    #   try:
    #     _wait_until_ready()
    #     ProcessPoolExecutor(max_workers=workers)
    #     chunk_size만큼 모아 executor.submit(_parallel_worker, chunk, step, state, output_queue)
    #     as_completed → chunk_result = future.result()
    #       accumulated = chunk_result if accumulated is None
    #                     else accumulated.merge(chunk_result)
    #     sentinel 수신 → 잔여 chunk flush
    #   finally:
    #     if accumulated: _publish_result(accumulated)
    #     stats.finish() → _send_sentinel()
```

테스트 (`@pytest.mark.timeout(30)`):
- `@pytest.mark.parametrize("workers,n", [(1,50),(2,50),(4,100)])` — 건수 일치
- `workers=1 vs workers=4` — 동일 건수 (FilterEvenStep 기준)
- Sentinel 도달 확인
- result_queue에서 수신한 결과의 `passed + filtered == n` 확인

---

### - [x] W-10 Fan-out 및 State/Event 테스트
**파일**: `tests/test_fanout.py`
**의존**: W-08, W-09 (producers.py 완료 후)

- [x] 레퍼런스 탐색 (multiprocessing.Event, fan-out 패턴)
- [x] 테스트 작성 (red)
- [x] 테스트 통과 (green) — 테스트 전용 단위, 추가 구현 없음
- [x] ruff 통과

> W-11, W-12와 독립적으로 병렬 작업 가능.

테스트 (`@pytest.mark.timeout(20)`):
- 1개 producer → 2개 output_queue, 양쪽 모두 N개 수신
- `state=None` + `ready_event` → set 전 block / set 후 처리 재개
- 두 event 모두 set 후 처리 시작 (하나만 set → block 유지)
- Fan-out 집계 end-to-end: Queue B(CountingAggStep) 완료 → state 주입 → Queue A(StateAwareStep) 재개
- 집계 실패 → state 영원히 미set → timeout으로 deadlock 감지

---

### - [x] W-11 백프레셔 테스트
**파일**: `tests/test_backpressure.py`
**의존**: W-08, W-09 (producers.py 완료 후)

- [x] 레퍼런스 탐색 (Queue maxsize, blocking put 동작)
- [x] 테스트 작성 (red)
- [x] 테스트 통과 (green) — 테스트 전용 단위
- [x] ruff 통과

> W-10, W-12와 독립적으로 병렬 작업 가능.

테스트 (`@pytest.mark.timeout(20)`):
- `Queue(maxsize=3)` + `PassthroughStep` → producer block 확인 (qsize ≤ maxsize)
- consumer가 소비 → producer 재개, 전체 건수 일치
- `ParallelProducer` + `SlowStep` + bounded queue → 전체 처리 완료

---

### - [x] W-12 에러 처리 테스트
**파일**: `tests/test_error.py`
**의존**: W-08, W-09 (producers.py 완료 후)

- [x] 레퍼런스 탐색 (에러 전파 패턴, ErrorSentinel 활용)
- [x] 테스트 작성 (red)
- [x] 테스트 통과 (green) — 테스트 전용 단위
- [x] ruff 통과

> W-10, W-11과 독립적으로 병렬 작업 가능.

테스트 (`@pytest.mark.timeout(15)`):
- `@pytest.mark.parametrize` — 에러 item skip, 나머지 정상 통과 건수
- 에러율 100% → 0개 출력 + Sentinel 정상 전파
- `stats.errored` 정확히 집계
- Sentinel 전송 직전 에러 → Sentinel 반드시 도달
- `ParallelProducer` worker 내 예외 → 메인 프로세스 `stats.errored` 반영
- **에러 발생 시에도 그 시점까지의 accumulated result가 result_queue에 전송됨 (graceful)**

---

## Phase 4 — 통합 (순차)

### - [x] W-13 `engine.py` + StepRegistry
**파일**: `src/task_pipeliner/engine.py`
**의존**: W-07~09 (producers), W-03 (config), W-04 (stats), W-05 (io)

- [x] 레퍼런스 탐색 (pickle.dumps 검증, mp.set_start_method, signal 핸들러)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 로깅 추가 (DEBUG: _build_producers 진입/종료; INFO: pipeline started/completed; WARNING: join timeout; ERROR: signal shutdown, worker crash)
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class StepRegistry:
    def register(self, name: str, cls: type) -> None
    # 중복 등록 → StepRegistrationError
    # pickle.dumps(cls) 실패 → StepRegistrationError (조기 감지)
    def get(self, name: str) -> type
    # 미등록 → StepRegistrationError (사용 가능 목록 포함)

class PipelineEngine:
    def __init__(self, config: PipelineConfig, registry: StepRegistry, stats: StatsCollector)
    def _build_producers(self, input_items: Generator, writer: JsonlWriter) -> list[BaseProducer]
    # config 순서대로 Queue 연결, enabled=False skip, Fan-out 분기
    # 각 producer에 result_queue 제공
    def run(self, input_items: Generator, writer: JsonlWriter, output_dir: Path) -> None
    # mp.set_start_method("spawn", force=True)
    # SIGINT/SIGTERM handler 등록
    # try: 모든 process start/join
    # finally:
    #   result_queue에서 각 step의 accumulated result 수거
    #   각 result.write(output_dir) 호출          # 결과 객체 스스로 파일 출력
    #   stats.write_json() + stats.flush()
    # join timeout → terminate → join → kill 순서
```

---

### - [x] W-14 Graceful Shutdown 테스트
**파일**: `tests/test_shutdown.py`
**의존**: W-13 (engine.py)

- [x] 레퍼런스 탐색 (subprocess로 SIGINT 전송, Windows vs Linux 차이)
- [x] 테스트 작성 (red)
- [x] 테스트 통과 (green) — 테스트 전용 단위
- [x] ruff 통과

테스트 (`@pytest.mark.timeout(20)`, subprocess 방식):
- 실행 중 SIGINT → `stats.json` 존재 + 유효 JSON
- 실행 중 SIGINT → `pipeline.log` 존재 + 내용 있음
- state unset 상태에서 SIGINT → timeout 내 종료 (deadlock 없음)
- 정상 완료 후 `stats.json` 모든 step 포함 확인
- **정상 완료 시 각 step의 `BaseResult.write()` 결과 파일 존재 확인**
- **SIGINT 시에도 그 시점까지의 결과 파일 존재 확인 (graceful result preservation)**

---

### - [x] W-15 `pipeline.py` + CLI + 통합 테스트
**파일**: `src/task_pipeliner/pipeline.py`, `src/task_pipeliner/cli.py`, `tests/test_cli.py`
**의존**: W-13, W-05

- [x] 레퍼런스 탐색 (click group/command/option, CliRunner 테스트 패턴)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 로깅 추가 (pipeline.py — DEBUG: register/run 진입/종료; INFO: run started/completed. cli.py — 로깅 설정만)
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class Pipeline:
    def __init__(self) -> None                                           # StepRegistry + StatsCollector 초기화
    def register(self, name: str, cls: type[BaseStep]) -> "Pipeline"    # chaining
    def register_all(self, mapping: dict[str, type]) -> "Pipeline"
    def run(self, config: Path | PipelineConfig, inputs: list[Path], output_dir: Path) -> None

# cli.py
@click.group()
def main() -> None

@main.command("filter")
@click.option("--config", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--input", "inputs", multiple=True, required=True, type=click.Path(path_type=Path))
@click.option("--output", required=True, type=click.Path(path_type=Path))
@click.option("--workers", default=None, type=int)
def filter_cmd(config, inputs, output, workers) -> None

@main.command("batch")
@click.option("--config", required=True, type=click.Path(exists=True, path_type=Path))
@click.argument("jobs_file", type=click.Path(exists=True, path_type=Path))
@click.option("--parallel", is_flag=True, default=False)
def batch_cmd(config, jobs_file, parallel) -> None
```

CLI 테스트 (`CliRunner`):
- `filter` end-to-end → `exit_code == 0`, `kept.jsonl` 존재
- `@pytest.mark.parametrize` — 잘못된 YAML / 미등록 step → non-zero exit + 메시지
- `--input` 없는 경로 → `click.Path(exists=True)` 에러
- `batch` jobs.json 순차 실행, 각 output 존재 확인

---

### - [x] W-16 `__init__.py` 공개 API
**파일**: `src/task_pipeliner/__init__.py`
**의존**: W-15

- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
from .pipeline import Pipeline
from .base import BaseStep, BaseAggStep, BaseResult, StepType
from .producers import ParallelProducer, SequentialProducer
from .exceptions import PipelineError, StepRegistrationError, ConfigValidationError

__all__ = [
    "Pipeline",
    "BaseStep", "BaseAggStep", "BaseResult", "StepType",
    "ParallelProducer", "SequentialProducer",
    "PipelineError", "StepRegistrationError", "ConfigValidationError",
]
```

테스트:
- `from task_pipeliner import Pipeline, BaseStep, BaseResult, StepType` 동작
- `__all__` 완전성 — 모든 심볼 import 가능

---

## 병렬 작업 가능 구간 요약

| Phase | 작업 단위 | 병렬 가능 여부 |
|---|---|---|
| 1 | W-01 → W-02, W-03 | W-02, W-03은 W-01 후 병렬 가능 |
| 2 | W-04, W-05, W-06 | **3개 동시 병렬** (W-01/W-02 완료 후) |
| 3 | W-07 → W-08, W-09 | W-08, W-09 병렬 가능 |
| 3 | W-10, W-11, W-12 | **3개 동시 병렬** (W-08/W-09 완료 후) |
| 4 | W-13 → W-14 → W-15 → W-16 | 순차 |
