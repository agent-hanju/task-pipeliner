# Design Decisions: task-pipeliner

> 이 문서는 task-pipeliner의 핵심 설계 결정을 기록한다.
> 초기 아키텍처 결정부터 기능별 구현 결정까지 순서대로 정리한다.

---

## 프로젝트 배경

**문제:** 데이터 처리 파이프라인에는 Stateless step(병렬화 가능)과 Stateful step(순차 필요)이 혼재한다. 기존 도구들의 한계:

| 도구 | 한계 |
|------|------|
| HuggingFace datasets | Arrow/파일 캐시 의존, stateful 병렬화 불가, 메모리 전체 로딩 |
| Apache Beam | 추상화 비용 과대, global stateful dedup 표현 어려움 |
| datatrove | 파일 기반 중간 통신(단일 머신 오버헤드), Slurm/클러스터 종속 |

**설계 목표:**
1. 균일한 step 인터페이스 — 필터/변환/dedup/집계가 같은 인터페이스로 표현
2. 선언적 파이프라인 — YAML config로 구성, step 교체 용이
3. 병렬성 극대화 — stateless는 자동 병렬, stateful은 자동 순차 (엔진이 판단)
4. 메모리 효율 — generator chain 기반 streaming, 전체 로딩 없음
5. 크로스플랫폼 — Windows(spawn) + Linux 모두 정상 동작
6. 백프레셔 및 에러 전파 내장 — 구현자가 별도 처리 불필요
7. Graceful shutdown — 중단 시에도 그 시점까지의 결과 보존

**datatrove에서 가져온 것 / 버린 것:**

| 가져온 것 | 버린 것 |
|-----------|---------|
| generator chain 파이프라인 구조 | 파일 기반 중간 통신 |
| stateful step을 별도 실행 단위로 분리 | Slurm/Ray executor 종속 |
| exclusion_writer 패턴 아이디어 | S3/분산 스토리지 의존 |

---

## 아키텍처 결정 (ADR)

### ADR-1: src/ 레이아웃
설치하지 않은 상태에서 `import task_pipeliner`가 되는 것을 방지. pip install한 패키지를 참조하도록 강제.

### ADR-2: CLI 없음
`filter`/`run` 커맨드명이 특정 도메인 워크플로우를 가정하게 된다. 사용자가 실행 스크립트를 직접 작성하거나 `Pipeline` 파사드를 호출하는 것이 더 범용적.

### ADR-3: pydantic v2 config 검증
YAML 파싱 후 pydantic 모델로 검증 — step 필드 누락, 타입 오류 등을 실행 전에 조기 감지. `StepConfig extra → cls(**extra)` 패턴으로 생성자 주입을 자연스럽게 지원.

### ADR-4: ProcessPoolExecutor spawn 방식 고정
Windows는 spawn이 기본이나 Linux는 fork가 기본. fork는 CUDA/파일핸들 상속 문제 발생 가능. spawn으로 통일하면 크로스플랫폼 동작이 보장되고 pickle 가능 여부가 조기에 드러남.

### ADR-5: Queue maxsize=0 (무제한)
bounded queue는 fan-out/fan-in 그래프에서 순환 대기 데드락을 유발한다. `maxsize=0`으로 정확성을 먼저 확보하고, OOM 방어는 SpillQueue로 분리.

### ADR-6: 공개 API를 `__init__.py`에서 명시적 노출
구현 프로젝트가 내부 모듈 경로를 알 필요 없음. 내부 리팩터링 시에도 공개 API 호환성 유지 가능.

### ADR-7: orjson을 프레임워크 본체에서 제거
프레임워크가 특정 직렬화 라이브러리에 종속되면 사용자의 의존성 관리 부담 증가. 프레임워크 자체는 JSON 직렬화 성능이 병목이 아님. stdlib `json` 사용.

### ADR-8: asyncio 포함
LLM API 호출, HTTP 요청 등 I/O 바운드 작업은 multiprocessing보다 asyncio가 효율적. `asyncio.run()`을 스레드 안에서 실행하여 다른 StepRunner와 스레드 모델 통일.

---

## AsyncStep (Phase 10, 2026-04-09)

**목표:** I/O 바운드 작업 (LLM API 호출, HTTP 요청 등)을 위한 asyncio 기반 스텝 타입.

- `asyncio.run()`을 스레드 안에서 실행 — 다른 StepRunner와 스레드 모델 통일
- `loop.run_in_executor(None, queue.get)`으로 blocking Queue → asyncio 브리지
- `asyncio.Semaphore(concurrency)`로 동시 실행 수 제한
- `state`는 코루틴 간 공유 — `process_async()` 내부에서 뮤테이션 금지 (docstring에 경고)

---

## SpillQueue / FullDiskQueue (Phase 11, 2026-04-10)

**목표:** 대용량 처리 시 OOM 방지. 큐 항목이 `maxmem` 초과 시 임시 파일에 직렬화, 메모리 여유 생기면 자동 복원.

- SpillQueue는 스레드 전용 (프로세스 간 전달 불필요 — 큐는 메인 프로세스에만 존재)
- `_refill_cond` (threading.Condition)이 모든 공유 상태와 파일 I/O 직렬화
- unbuffered I/O (`buffering=0`) — `write_fd.flush()` 후 즉시 `read_fd`로 읽기 가능
- `maxmem=0` 시 밸리데이션 오류 (무한 디스크 스필 방지)
- FullDiskQueue: `QueueType.FULL_DISK` 명시 설정 시 선택 — 디스크 우선 큐가 필요한 경우 명시적 옵션으로 유지

---

## State Dispatch 제거 (Phase 13, 2026-05-25)

**목표:** `is_ready` / `initial_state` / `get_output_state` / `state_dispatch` 메커니즘 전면 제거. `process()` 시그니처 단순화.

**제거된 것:**
- `StepBase.initial_state`, `StepBase.is_ready(state)`, `StepBase.get_output_state()` 프로퍼티/메서드
- `BaseStepRunner.state`, `state_changed_event`, `state_dispatch`, `ready_events` 필드
- `_wait_until_is_ready()`, `_dispatch_output_state()` 내부 메서드
- `engine.py`의 `_state_dispatch` 콜백, `state_events` dict, `runner_by_name` dict

**변경된 것:**
- `SequentialStep.process(item, state, emit)` → `process(item, emit)`
- `AsyncStep.process_async(item, state, emit)` → `process_async(item, emit)`
- `Worker.process(item, state, emit)` → `process(item, emit)`
- `QueueType.AUTO`: `is_ready` override 감지 분기 제거 — `queue_size > 0`이면 SpillQueue, 아니면 plain Queue

**유지된 것:**
- `FullDiskQueue`는 `QueueType.FULL_DISK` 명시 설정으로 계속 사용 가능 — 큐 지연/정체 대비 디스크 우선 옵션

**결정 이유:**
- State dispatch 메커니즘은 한 step의 출력 state가 다른 step의 `is_ready()`를 해제하는 구조였으나, 실제 사용 사례에서 과도한 복잡성 대비 이점이 없었음
- `process()` 시그니처에 `state` 파라미터가 항상 포함되어 있어 불필요한 인지 부하 유발
- 삭제 후 306개 테스트 통과 — 기능적 영향 없음

---

## Programmable API (Phase 12, 2026-05-25)

**목표:** YAML 의존 없이 Python 코드만으로 파이프라인 조립·실행. YAML은 여러 조립 방법 중 하나로 위치 변경.

```python
pipeline = Pipeline(workers=4)
pipeline.register("reader", FileReader)
pipeline.register("writer", FileWriter)

source = pipeline.step("src", "reader", path="data/")
writer = pipeline.step("out", "writer", path="out/")
source.pipe(writer)

pipeline.run(output_dir=Path("out/"))
```

- `outputs: ClassVar` 제거 — `.pipe()` 연결이 유효 태그를 대체
- `pipeline.step(name, type_str, **kwargs)` — 코드 경로에서도 문자열 타입명 사용
- `register()`는 클래스 레퍼런스가 등장하는 유일한 지점
- 내부 `_StepGraph`에서 코드/YAML 두 경로 수렴 → 엔진은 단일 입력 타입만 처리
- 실행 설정(`workers`, `chunk_size`, `queue_type`, `queue_size`)은 `Pipeline` 생성자 필드
- `output_dir`는 실행마다 달라질 수 있으므로 `run()` 인자로 유지
- `pipe()` 기본 태그 `"main"` — `emit(item, "main")`과 일치
- terminal step은 `outputs = ()`로 명시 선언, 미선언 step의 unconnected 태그는 silent drop
