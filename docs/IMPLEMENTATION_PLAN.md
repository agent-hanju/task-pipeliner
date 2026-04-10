# Implementation Plan: task-pipeliner

> 이 문서는 task-pipeliner 프레임워크의 구현 계획 및 진행 상태를 기록한다.
> 작성일: 2026-03-01 | 최종 갱신: 2026-04-10

---

## Overview

범용 데이터 처리 파이프라인 프레임워크. YAML config 기반으로 Step을 선언하고, 멀티프로세스/비동기 실행 엔진이 데이터를 흘려보내는 구조. taxonomy-converter, pretrain-data-filter 등 샘플 프로젝트의 실제 사용을 통해 안티패턴을 발견·제거하면서 프레임워크 순수성을 높였다.

**상태:** 기반 구현 완료 (M-01 ~ M-24 + AsyncStep + SpillQueue). 다음 목표: P2 Retry 지원.

---

## Requirements

- Python 3.11+, spawn 모드 멀티프로세싱 (Windows 호환 필수)
- config YAML 한 파일로 파이프라인 전체 선언 가능
- SOURCE → SEQUENTIAL / PARALLEL / ASYNC 스텝 조합 지원
- 실시간 진행률 표시 (stderr), 완료 후 stats.json 출력
- Step 간 state 주입 + is_ready 게이팅
- 80%+ 테스트 커버리지, ruff + mypy 통과

---

## Architecture

```
Pipeline (facade)
  └─ PipelineEngine
       ├─ StepRegistry          # type → class 매핑
       ├─ StatsCollector        # 스텝별 메트릭 누적
       ├─ ProgressReporter      # stderr 실시간 출력
       └─ StepRunner × N        # 스텝당 1개 스레드
            ├─ InputStepRunner  # SourceStep 실행
            ├─ SequentialStepRunner  # 단일 스레드 처리
            ├─ AsyncStepRunner  # asyncio 이벤트 루프
            └─ ParallelStepRunner    # ProcessPoolExecutor
```

**Step 계층 (base.py):**
```
StepBase
  ├─ SourceStep       → items() 구현 → InputStepRunner
  ├─ SequentialStep   → process() 구현 → SequentialStepRunner
  ├─ AsyncStep        → process_async() 구현 → AsyncStepRunner
  └─ ParallelStep     → create_worker() → Worker → ParallelStepRunner
```

---

## Implementation Steps

### Phase 1: 기반 스키마 ✅

| ID | 작업 | 상태 |
|----|------|------|
| W-01 | `exceptions.py` — PipelineError, StepRegistrationError, ConfigValidationError | ✅ |
| W-02 | `base.py` — BaseStep (SourceStep/SequentialStep/ParallelStep/Worker) | ✅ |
| W-03 | `config.py` — StepConfig, ExecutionConfig, PipelineConfig (Pydantic v2) | ✅ |

### Phase 2: 지원 모듈 ✅

| ID | 작업 | 상태 |
|----|------|------|
| W-04 | `stats.py` — StepStats, StatsCollector | ✅ |
| W-05 | `io.py` — JsonlReader, JsonlWriter (유틸리티) | ✅ |

### Phase 3: 실행 코어 ✅

| ID | 작업 | 상태 |
|----|------|------|
| W-06 | `step_runners.py` — InputStepRunner (SourceStep 실행) | ✅ |
| W-07 | `step_runners.py` — SequentialStepRunner (단일 스레드) | ✅ |
| W-08 | `step_runners.py` — ParallelStepRunner (ProcessPoolExecutor) | ✅ |
| W-09 | Queue / fanout / 백프레셔 / 에러 처리 테스트 | ✅ |

### Phase 4: 통합 ✅

| ID | 작업 | 상태 |
|----|------|------|
| W-10 | `engine.py` — PipelineEngine | ✅ |
| W-11 | `pipeline.py` — Pipeline 파사드 + StepRegistry | ✅ |
| W-12 | `__init__.py` — 공개 API 확정 | ✅ |
| W-13 | `cli.py` — run 커맨드 | ✅ |

### Phase 5: 안티패턴 제거 (refactoring) ✅

> 발견 경위: taxonomy-converter 샘플 구축 과정에서 A-1~A-5 안티패턴 발견.
> 상세: `docs/RETROSPECTIVE.md`

| ID | 작업 | 상태 |
|----|------|------|
| R-01 | SourceStep 도입 — engine.run(input_items=) 제거 | ✅ |
| R-02 | JsonlWriter를 engine에서 분리 → 유틸리티로 전환 | ✅ |
| R-03 | cli.py `filter` → `run` 커맨드 명칭 변경 | ✅ |
| R-04 | StepConfig extra → `cls(**extra)` 생성자 주입 패턴 확립 | ✅ |

### Phase 6: Step Metrics + Queue 안정화 ✅

| ID | 작업 | 상태 |
|----|------|------|
| M-01 | maxsize=0 전환 — 데드락/백프레셔 우회 근본 해결 | ✅ |
| M-02 | `StepBase.initial_state` 프로퍼티 추가 | ✅ |
| M-03 | Engine에서 `state=step.initial_state` 전달 | ✅ |
| M-04 | StepStats 타이밍 필드 추가 | ✅ |
| M-05 | StatsCollector API 확장 | ✅ |
| M-06 | SequentialStepRunner 타이밍 계측 | ✅ |
| M-07 | ParallelStepRunner 구조 변경 + 타이밍 계측 | ✅ |
| M-08 | InputStepRunner 계측 + 총 입력 수 산출 | ✅ |
| M-09 | `format_progress()` 포맷팅 함수 | ✅ |
| M-10 | `ProgressReporter` 스레드 클래스 | ✅ |
| M-11 | Engine 통합 + 콘솔 로그 분리 | ✅ |
| M-12 | HashLookupStep `state` 사용 (pretrain-data-filter) | ✅ |
| M-14 | 통합 테스트 & 검증 | ✅ |
| M-15 | export 및 문서 갱신 | ✅ |

### Phase 7: Step 간 state 주입 + is_ready 게이팅 ✅

| ID | 작업 | 상태 |
|----|------|------|
| M-16 | `StepBase.is_ready()` + `get_output_state()` 추가 | ✅ |
| M-17 | StepRunner `is_ready` 게이팅 | ✅ |
| M-18 | Engine state dispatch 배선 | ✅ |
| M-19 | BaseAggStep 제거 (사용처 0) | ✅ |

### Phase 8: Step Lifecycle open()/close() ✅

| ID | 작업 | 상태 |
|----|------|------|
| M-20 | `StepBase.open()` 추가 | ✅ |
| M-21 | StepRunner에서 `open()` 호출 | ✅ |
| M-22 | CLAUDE.md Step Lifecycle 섹션 추가 | ✅ |

### Phase 9: 핵심 의존성 정리 ✅

| ID | 작업 | 상태 |
|----|------|------|
| M-23 | orjson → json stdlib 전환 (프레임워크 본체) | ✅ |
| M-24 | README Step Lifecycle 반영 | ✅ |

### Phase 10: AsyncStep P1 ✅ (2026-04-09 완료)

**목표:** I/O 바운드 작업 (LLM API 호출, HTTP 요청 등)을 위한 asyncio 기반 스텝 타입 추가.

| ID | 작업 | 상태 |
|----|------|------|
| A-01 | `base.py` — `AsyncStep` 클래스 추가 (`concurrency` 프로퍼티, `process_async()` 추상 메서드) | ✅ |
| A-02 | `step_runners.py` — `AsyncStepRunner` 구현 (asyncio + Semaphore) | ✅ |
| A-03 | `engine.py` — AsyncStep 디스패치 분기 추가 | ✅ |
| A-04 | `__init__.py` — `AsyncStep`, `AsyncStepRunner` export 추가 | ✅ |
| A-05 | `tests/dummy_steps.py` — async dummy 스텝 4종 추가 | ✅ |
| A-06 | `tests/test_async_step.py` — 통합 테스트 5개 (passthrough, filter, error, concurrency, chain) | ✅ |
| A-07 | `tests/test_init.py` — `__all__` 완성도 테스트 갱신 | ✅ |

**설계 결정:**
- `asyncio.run()` 을 스레드 안에서 실행 (다른 StepRunner와 스레드 모델 통일)
- `loop.run_in_executor(None, queue.get)` 으로 blocking Queue → asyncio 브리지
- `asyncio.Semaphore(concurrency)` 로 동시 실행 수 제한
- `state` 는 코루틴 간 공유 — `process_async()` 내부에서 뮤테이션 금지 (docstring에 경고)

---

## Testing Strategy

```bash
# 전체 테스트
.venv/Scripts/pytest --timeout=30 -v

# AsyncStep만
.venv/Scripts/pytest tests/test_async_step.py -v

# 커버리지
.venv/Scripts/pytest --cov=task_pipeliner --cov-report=term-missing
```

- 모든 테스트에 `@pytest.mark.timeout(30)` — 데드락 감지
- dummy_steps.py는 모두 모듈 레벨 정의 (spawn 모드 pickle 요건)
- AAA (Arrange-Act-Assert) 패턴 준수

---

## Upcoming: P2 Retry 지원

**목표:** SequentialStepRunner / AsyncStepRunner에서 `process()` 실패 시 재시도.

**StepConfig 확장 (안):**
```yaml
- type: llm_call
  retry_count: 3         # 최대 재시도 횟수 (기본 0)
  retry_delay: 1.0       # 초기 대기 시간 (초)
  retry_backoff: 2.0     # 지수 백오프 계수
```

**구현 범위:**
- `config.py` — `StepConfig`에 `retry_count`, `retry_delay`, `retry_backoff` 필드 추가
- `step_runners.py` — `SequentialStepRunner`, `AsyncStepRunner` 재시도 루프 추가
- `stats.py` — `StepStats`에 `retried` 카운터 추가
- 테스트 — 재시도 성공/실패/최대 초과 시나리오

**의존:** A-02 (AsyncStepRunner) 완료 → P2 착수 가능

---

### Phase 11: SpillQueue — 메모리 한계 디스크 스필 큐 ✅ (2026-04-10 완료)

**목표:** 대용량 처리 시 OOM 방지. 큐 항목이 `maxmem` 초과 시 임시 파일에 직렬화, 메모리 여유 생기면 자동 복원.

| ID | 작업 | 상태 |
|----|------|------|
| S-01 | `spill_queue.py` — SpillQueue 구현 (8-byte 길이 prefix + pickle, `_refill_loop` 데몬 스레드) | ✅ |
| S-02 | `step_runners.py` — `QueueLike` Protocol 추가 (SpillQueue ↔ multiprocessing.Queue 호환) | ✅ |
| S-03 | `engine.py` — `execution.queue_size > 0` 시 SpillQueue 활성화, finally에서 `close()` 호출 | ✅ |
| S-04 | `config.py` — `ExecutionConfig.queue_size` 필드 추가 (기본 0 = unbounded) | ✅ |
| S-05 | `__init__.py` — `SpillQueue` export 추가 | ✅ |
| S-06 | `tests/test_spill_queue.py` — 19개 테스트 (Protocol 준수, FIFO, spill 트리거, 병렬 스레드, close 정리 등) | ✅ |

**설계 결정:**
- SpillQueue는 스레드 전용 (프로세스 간 전달 불필요 — 큐는 메인 프로세스에만 존재)
- `_refill_cond` (threading.Condition) 이 모든 공유 상태와 파일 I/O 직렬화
- unbuffered I/O (`buffering=0`) — `write_fd.flush()` 후 즉시 `read_fd`로 읽기 가능
- `maxmem=0` 시 밸리데이션 오류 (무한 디스크 스필 방지)

---

## Risks & Mitigations

| 위험 | 대응 |
|------|------|
| spawn 모드 pickle 오류 | 모든 Step/Worker 클래스를 모듈 레벨에 정의. Engine에서 조기 검증. |
| asyncio + multiprocessing 큐 데드락 | `run_in_executor` 브리지 + sentinel 수신 시 `asyncio.gather` 드레인 |
| state 동시 뮤테이션 (AsyncStep) | docstring 경고 + 뮤테이션 필요 시 SequentialStep 사용 권장 |
| 대용량 처리 OOM (unbounded 큐) | SpillQueue 도입으로 해결 — `execution.queue_size > 0` 설정 시 활성화 |

---

## Success Criteria

- [x] 전체 테스트 통과 (`pytest --timeout=30`)
- [x] `ruff check src tests` — 오류 없음
- [x] `mypy src` — 오류 없음
- [x] `__all__` 에 공개 심볼 전부 포함
- [x] SpillQueue — `maxmem` 초과 시 디스크 스필, 19개 테스트 통과
- [ ] P2 Retry — 재시도 시나리오 테스트 통과
