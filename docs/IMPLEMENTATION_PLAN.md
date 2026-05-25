# Implementation Plan: task-pipeliner

> 이 문서는 task-pipeliner 프레임워크의 구현 계획 및 진행 상태를 기록한다.
> 작성일: 2026-03-01 | 최종 갱신: 2026-05-25

---

## Overview

범용 데이터 처리 파이프라인 프레임워크. YAML config 기반으로 Step을 선언하고, 멀티프로세스/비동기 실행 엔진이 데이터를 흘려보내는 구조.

**상태:** Phase 1~12 완료 (AsyncStep, SpillQueue, Programmable API 포함). 다음 목표: Retry 지원.

---

## Requirements

- Python 3.11+, spawn 모드 멀티프로세싱 (Windows 호환 필수)
- config YAML 한 파일로 파이프라인 전체 선언 가능
- SOURCE → SEQUENTIAL / PARALLEL / ASYNC 스텝 조합 지원
- 실시간 진행률 표시 (stderr), 완료 후 stats.json 출력
- 80%+ 테스트 커버리지, ruff + mypy 통과

---

## Architecture

```
Pipeline (facade)
  └─ PipelineEngine
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

### Phase 13: P2 Retry 지원

**목표:** SequentialStepRunner / AsyncStepRunner에서 `process()` 실패 시 재시도.

**StepConfig 확장 (안):**
```yaml
- type: llm_call
  retry_count: 3         # 최대 재시도 횟수 (기본 0)
  retry_delay: 1.0       # 초기 대기 시간 (초)
  retry_backoff: 2.0     # 지수 백오프 계수
```

| ID | 작업 | 상태 |
|----|------|------|
| R-01 | `config.py` — `StepConfig`에 `retry_count`, `retry_delay`, `retry_backoff` 필드 추가 | ⬜ |
| R-02 | `step_runners.py` — `SequentialStepRunner` 재시도 루프 추가 | ⬜ |
| R-03 | `step_runners.py` — `AsyncStepRunner` 재시도 루프 추가 | ⬜ |
| R-04 | `stats.py` — `StepStats`에 `retried` 카운터 추가 | ⬜ |
| R-05 | 테스트 — 재시도 성공/실패/최대 초과 시나리오 | ⬜ |

---

## Risks & Mitigations

| 위험 | 대응 |
|------|------|
| spawn 모드 pickle 오류 | 모든 Step/Worker 클래스를 모듈 레벨에 정의. Engine에서 조기 검증. |
| asyncio + multiprocessing 큐 데드락 | `run_in_executor` 브리지 + sentinel 수신 시 `asyncio.gather` 드레인 |
| 대용량 처리 OOM (unbounded 큐) | SpillQueue 도입으로 해결 — `execution.queue_size > 0` 설정 시 활성화 |

---

## Success Criteria

- [x] 전체 테스트 통과 (`pytest --timeout=30`)
- [x] `ruff check src tests` — 오류 없음
- [x] `mypy src` — 오류 없음
- [x] `__all__` 에 공개 심볼 전부 포함
- [x] SpillQueue — `maxmem` 초과 시 디스크 스필, 19개 테스트 통과
- [ ] Retry — 재시도 시나리오 테스트 통과
