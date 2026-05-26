# task-pipeliner Interface Reference

> 재구축 참고용 — 현재 구현 기준 전체 인터페이스 정리

---

## 전체 구조

```
사용자 코드
    │
    ▼
Pipeline                          ← 등록·조립·실행 진입점
    │  _build_graph() or
    │  _build_graph_from_config()
    ▼
_StepGraph                        ← 파이프라인 토폴로지 데이터
    │
    ▼
PipelineEngine.run()              ← DAG 큐 구성 + 스레드 조율
    │
    ├── InputStepRunner           ← SourceStep 전용 (feeder 스레드)
    ├── SequentialStepRunner      ← SequentialStep 처리
    ├── AsyncStepRunner           ← AsyncStep 처리 (asyncio)
    └── ParallelStepRunner        ← ParallelStep 처리 (ProcessPool)
```

---

## StepBase (공통 베이스)

모든 Step이 상속. 직접 서브클래싱 X — 아래 4가지 ABC 중 하나를 씀.

```python
class StepBase:
    def pipe(self, step: StepBase, tag: str = "main") -> StepBase:
        """다음 step 연결. 반환값이 step이라 체이닝 가능.
        같은 tag로 여러 번 호출하면 fan-out."""
        # 내부적으로 step.__dict__["_connections"][tag].append(step) 저장

    def open(self) -> None:
        """실행 전 리소스 획득. main process에서 1회."""

    def close(self) -> None:
        """실행 후 리소스 해제. main process에서 1회."""
```

---

## Step ABC 4종

### SourceStep — 데이터 생성 (파이프라인 첫 번째 step 전용)

```python
class SourceStep(StepBase, ABC):
    @abstractmethod
    def items(self) -> Generator[Any, None, None]:
        """파이프라인에 흘려보낼 아이템을 yield."""

    def item_key(self, item: Any) -> str | None:
        """체크포인트 키. None 반환 시 체크포인트 비활성 (기본값)."""
        return None

    def open(self) -> None: ...
    def close(self) -> None: ...
```

### SequentialStep — 단일 스레드 순차 처리

```python
class SequentialStep(StepBase, ABC):
    @abstractmethod
    def process(self, item: Any, emit: Callable[[Any, str], None]) -> None:
        """emit(item, tag) 으로 다음 step에 전달.
        emit 안 하면 드롭. terminal step은 emit 호출 금지 (RuntimeError)."""

    def open(self) -> None: ...
    def close(self) -> None: ...
```

### AsyncStep — asyncio I/O 바운드 처리

```python
class AsyncStep(StepBase, ABC):
    @property
    def concurrency(self) -> int:
        """동시 실행 코루틴 수. 기본값 8. 서브클래스에서 오버라이드."""
        return 8

    @abstractmethod
    async def process_async(self, item: Any, emit: Callable[[Any, str], None]) -> None:
        """asyncio 코루틴. emit은 동기 콜백 (multiprocessing.Queue.put 호출)."""

    def open(self) -> None: ...
    def close(self) -> None: ...
```

### ParallelStep + Worker — 멀티프로세스 CPU 바운드

```python
class Worker(ABC):
    """worker process로 직렬화해서 전송. 반드시 picklable (모듈 최상위 정의)."""

    @abstractmethod
    def process(self, item: Any, emit: Callable[[Any, str], None]) -> None: ...

    def open(self) -> None:
        """worker process당 1회 — 모델 로딩, DB 연결 등."""

    def close(self) -> None:
        """worker process 종료 시 정리."""


class ParallelStep(StepBase, ABC):
    @abstractmethod
    def create_worker(self) -> Worker:
        """picklable Worker 반환.
        open() 전에 호출되므로 unpicklable 리소스 캡처 금지."""

    def open(self) -> None: ...   # main process에서만
    def close(self) -> None: ...  # main process에서만
```

---

## Pipeline

```python
class Pipeline:
    def __init__(
        self,
        *,
        workers: int = 4,
        chunk_size: int = 100,
        queue_type: QueueType = QueueType.AUTO,
        queue_size: int = 0,           # 0 = unbounded
        checkpoint_dir: Path | None = None,
    ) -> None: ...

    # 등록
    def register(self, name: str, cls: type[StepBase]) -> Pipeline:
        """step 클래스를 이름으로 등록. 중복·unpicklable 시 StepRegistrationError.
        등록 시 pickle.dumps(cls) 로 picklability 즉시 검증."""

    def register_all(self, mapping: dict[str, type[StepBase]]) -> Pipeline:
        """여러 step 클래스 일괄 등록."""

    # 코드 방식 step 생성
    def step(self, name: str, type_name: str, **kwargs: Any) -> StepBase:
        """등록된 type_name으로 step 인스턴스 생성. kwargs → step 생성자."""

    # 실행
    def run(
        self,
        *,
        output_dir: Path,
        config: Path | PipelineConfig | None = None,
        variables: dict[str, Any] | None = None,
    ) -> StatsCollector:
        """그래프 조립 후 파이프라인 실행.
        config=None          → step()/pipe() 코드 방식
        config=Path          → YAML 파일 로드
        config=PipelineConfig → 파싱된 객체 직접 사용
        output_dir에 stats.json, pipeline.log, progress.log 생성."""

    # 내부 그래프 조립
    def _build_graph(self) -> _StepGraph:
        """코드 방식: step()/pipe() 결과를 _StepGraph으로 변환."""

    def _build_graph_from_config(self, path, variables) -> _StepGraph: ...

    def _build_graph_from_config_object(self, cfg: PipelineConfig) -> _StepGraph:
        """enabled step만 필터 → 인스턴스 생성 → connections 맵 → retry_configs 조립."""
```

---

## _StepGraph (내부 — Pipeline → PipelineEngine 전달용)

```python
@dataclass
class _StepGraph:
    nodes: list[tuple[str, StepBase]]
    """순서 있는 (이름, step) 쌍. nodes[0]이 반드시 SourceStep."""

    connections: dict[str, dict[str, list[str]]]
    """step_name → tag → [target_step_names]
    예: {"src": {"main": ["filter"], "error": ["logger"]}}"""

    execution: ExecutionConfig

    checkpoint_dir: Path | None = None
    resume_run_id: str | None = None

    retry_configs: dict[str, tuple[int, float, float]] = field(default_factory=dict)
    """step_name → (retry_count, retry_delay, retry_backoff)"""
```

---

## PipelineEngine (내부 — 실행 조율)

```python
class PipelineEngine:
    def __init__(self, *, graph: _StepGraph, stats: StatsCollector) -> None: ...

    def run(self, *, output_dir: Path) -> None:
        """
        1. nodes[0]이 SourceStep인지 검증
        2. DAG 큐 토폴로지 구성
           - processing step마다 입력 큐 1개 생성 (QueueType에 따라 종류 선택)
           - connections 순회 → output_queues_map[step][tag] = [큐들]
           - sentinel_count_for[step] = 이 step으로 들어오는 upstream 수
           - upstream 없는 step → 즉시 Sentinel 주입 (sentinel_count=1)
        3. StepRunner 생성 (isinstance 분기: ParallelStep / AsyncStep / 그 외)
        4. feeder 스레드 + runner 스레드 시작
        5. join 대기. SIGINT/SIGBREAK 시 graceful shutdown (5초 타임아웃 + 강제 Sentinel 주입)
        6. 큐 close, tmpdir 삭제, stats.json 저장, signal 복원
        """
```

큐 선택 로직:

```
queue_type == FULL_DISK  → FullDiskQueue (모든 아이템 즉시 디스크)
queue_size > 0           → SpillQueue (메모리 초과 시 디스크 spill)
그 외                    → multiprocessing.Queue (순수 메모리)
```

---

## StepRunner 4종 (내부 — 각각 별도 스레드에서 run() 실행)

```python
class Sentinel:
    """큐 종료 신호. sentinel_count개를 받으면 해당 runner 종료."""

class InputStepRunner:
    """SourceStep.items() 순회 → checkpoint 필터 → output_queues에 put.
    완료 시 각 output queue에 Sentinel 전파."""
    def __init__(self, step, step_name, output_queues, stats, checkpoint): ...
    def run(self) -> None: ...

class SequentialStepRunner:
    """input_queue.get() → step.process() → output_queues에 put.
    sentinel_count개 Sentinel 수신 시 종료 + downstream Sentinel 전파.
    retry: 지수 백오프, 실패 시 WARNING 로그 후 아이템 스킵."""
    def __init__(self, step, step_name, input_queue, output_queues, stats,
                 sentinel_count, retry_count, retry_delay, retry_backoff): ...
    def run(self) -> None: ...

class AsyncStepRunner:
    """asyncio 이벤트 루프를 단일 스레드에서 실행.
    run_in_executor로 blocking queue.get()을 비동기화.
    Semaphore로 concurrency 제한.
    retry 로직은 SequentialStepRunner와 동일 구조 (현재 중복 구현)."""
    def __init__(self, step, step_name, input_queue, output_queues, stats,
                 sentinel_count, retry_count, retry_delay, retry_backoff): ...
    def run(self) -> None: ...

class ParallelStepRunner:
    """ProcessPoolExecutor로 Worker.process() 병렬 실행.
    chunk 단위로 배치 제출 → as_completed로 결과 수집.
    retry 없음 (현재 미구현)."""
    def __init__(self, step, step_name, input_queue, output_queues, stats,
                 sentinel_count, workers, chunk_size): ...
    def run(self) -> None: ...
```

---

## Config Pydantic 모델

```python
class QueueType(StrEnum):
    AUTO = "auto"
    SPILL = "spill"
    FULL_DISK = "full_disk"

class StepConfig(BaseModel):
    type: str
    name: str = ""           # 기본값: type과 동일 (model_validator로 설정)
    enabled: bool = True
    outputs: dict[str, str | list[str]] | None = None
    retry_count: int = 0
    retry_delay: float = 0.0
    retry_backoff: float = 2.0
    # extra="allow" → 커스텀 필드 허용 → step 생성자 kwargs로 전달

class ExecutionConfig(BaseModel):
    workers: int = 4
    queue_size: int = 0
    chunk_size: int = 100
    queue_type: QueueType = QueueType.AUTO

class PipelineConfig(BaseModel):
    pipeline: list[StepConfig]   # 최소 1개 필수
    execution: ExecutionConfig = ExecutionConfig()
    checkpoint_dir: Path | None = None
    resume_run_id: str | None = None
    # 검증: step name 중복 금지, outputs의 target이 실존 step인지 확인

def load_config(path: Path, variables: dict | None = None) -> PipelineConfig:
    """YAML 로드 → ${var:-default} 치환 → PipelineConfig 반환.
    전체 값이 ${var}이면 타입 그대로 치환 (list, dict 등).
    문자열 내 삽입이면 str 변환."""
```

### YAML 스키마

```yaml
execution:
  workers: 4
  chunk_size: 100
  queue_size: 0          # 0 = unbounded
  queue_type: auto       # auto | spill | full_disk

checkpoint_dir: .checkpoint   # 선택. DiskCacheCheckpointStore 활성화
resume_run_id: abc123         # 선택. 이전 run_id로 재개

pipeline:
  - type: MySource
    name: src            # 생략 시 type과 동일
    enabled: true        # false면 그래프에서 제외
    outputs:
      main: [filter]     # tag → [step names]. 단일값 문자열도 허용
      error: logger
    retry_count: 3
    retry_delay: 1.0
    retry_backoff: 2.0
    input_path: data/    # 커스텀 필드 → step 생성자 kwargs

  - type: MyWriter
    name: writer
    output_path: ${output_dir:-out/}   # 변수 치환
```

---

## Exceptions

```python
PipelineError                    # 베이스
├── StepRegistrationError        # 등록 실패 (중복, unpicklable)
│     .step_name: str
├── ConfigValidationError        # 설정 검증 실패
│     .field: str | None
│     .cause: Exception | None
└── PipelineShutdownError        # 셧다운 중 오류
```

---

## StatsCollector

```python
class StatsCollector:
    def register(self, step_name: str) -> None: ...
    def write_json(self, path: Path) -> None: ...
    def get_step_stats(self, step_name: str) -> StepStats: ...

@dataclass
class StepStats:
    processed_count: int
    idle_time_ns: int      # 큐 대기 시간 (나노초)
    process_time_ns: int   # 실제 처리 시간 (나노초)
```

---

## 재구축 시 보존 / 재작성 대상

| 모듈 | 방향 | 비고 |
|------|------|------|
| `base.py` | 보존 | Step ABC 구조 안정적 |
| `config.py` | 보존 | Pydantic 스키마 + ${var} 치환 완성도 높음 |
| `stats.py` | 보존 | |
| `exceptions.py` | 보존 | |
| `checkpoint.py` | 보존 | |
| `progress.py` | 보존 | |
| `spill_queue.py` | 보존 | |
| `engine.py` | 재작성 | 큐 토폴로지 구성 + 셧다운 로직이 한 메서드에 집중 |
| `step_runners.py` | 재작성 | retry/sentinel 중복, runner 간 구조 불일치 (694줄) |
| `pipeline.py` | 부분 수정 | engine 재작성 시 연동 조정 필요 |
