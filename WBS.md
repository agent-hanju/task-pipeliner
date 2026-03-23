# WBS: Step Metrics, Queue 안정화, State 활용

> 기반 문서: `docs/refactoring-step-metrics.md`
> 작성일: 2026-03-20 | 갱신일: 2026-03-20

## 개발 방법론

CLAUDE.md TDD 절차 준수:
1. 레퍼런스 탐색 → 2. 테스트 작성 → 3. 구현 → 4. 테스트 통과 → 5. 린트/타입 → 6. WBS 갱신

---

## Phase 1: 큐 안정화

### M-01: `maxsize=0` 전환
> 모든 큐를 무제한으로 전환하여 데드락/백프레셔 우회 문제를 근본적으로 해결.

- [x] 레퍼런스 탐색
- [x] engine.py 큐 생성부 수정 (3곳: line 212, 228, 236) — `ctx.Queue(maxsize=queue_size)` → `ctx.Queue()`
- [x] config.py `ExecutionConfig.queue_size` 기본값 0으로 변경, docstring에 "향후 디스크 스필 임계치 전용" 명시
- [x] 기존 테스트 통과 확인
- [x] 린트/타입 통과
- 의존: 없음

---

## Phase 2: State 메커니즘 활용

### M-02: `BaseStep.initial_state` 프로퍼티 추가
> Step이 필요로 하는 초기 state를 선언할 수 있도록 프로퍼티를 추가.

```python
# BaseStep에 추가
@property
def initial_state(self) -> Any:
    """Return the initial state object for this step.

    Override in subclasses that need mutable state passed to process().
    The returned object is passed as the ``state`` argument to every
    ``process()`` call. For SEQUENTIAL steps, it is the same object
    across all calls (accumulated in-place).
    """
    return None
```

- [x] 레퍼런스 탐색
- [x] 테스트 작성 (dummy step with initial_state, 기본값 None 검증)
- [x] base.py에 `initial_state` 프로퍼티 추가
- [x] 테스트 통과
- [x] 린트/타입 통과
- 의존: 없음

### M-03: Engine에서 `state=step.initial_state` 전달
> Producer 생성 시 step의 `initial_state`를 `state` 인자로 전달.

- [x] engine.py Producer 생성부 수정 (SequentialProducer, ParallelProducer 모두)
- [x] state가 process()에 전달되는지 통합 테스트 추가 (dummy step이 state를 mutate하고 검증)
- [x] 테스트 통과
- [x] 린트/타입 통과
- 의존: M-02

---

## Phase 3: StepStats 확장 (데이터 모델)

### M-04: StepStats 타이밍 필드 추가
- [x] 테스트 작성 (`tests/test_stats.py`)
  - `first_item_at` 초기값 None, 설정 후 값 존재
  - `processing_ns`, `idle_ns`, `idle_count` 초기값 0
  - `current_state` 초기값 `"waiting"`
  - `to_dict()` 에 신규 키 포함 확인
  - 기존 `test_to_dict_keys` 갱신
- [x] 구현: `StepStats` dataclass에 필드 추가
- [x] 구현: `to_dict()` 확장 — `initial_wait_seconds`, `processing_seconds`, `processing_avg_ms`, `idle_seconds`, `idle_avg_ms` 산출
- [x] 린트/타입 통과
- 의존: 없음

### M-05: StatsCollector API 확장
- [x] 테스트 작성 (`tests/test_stats.py`)
  - `set_total_items(n)` → `total_items` 속성 반환
  - `record_first_item(step_name)` → `first_item_at` 설정, 중복 호출 시 무시
  - `add_processing_ns(step_name, ns)` → `processing_ns` 누적
  - `add_idle_ns(step_name, ns)` → `idle_ns` 누적, `idle_count` +1
  - `set_state(step_name, state)` → `current_state` 갱신
  - `write_json()` 결과에 신규 필드 포함 확인
- [x] 구현: 각 메서드 추가 (기존 `_lock` 활용)
- [x] 린트/타입 통과
- 의존: M-04

---

## Phase 4: Producer 계측

### M-06: SequentialProducer 타이밍 계측
- [x] 테스트 작성 (`tests/test_engine.py`)
  - 파이프라인 실행 후 SEQUENTIAL step의 `processing_ns > 0` 확인
  - `first_item_at is not None` 확인
  - `current_state == "done"` 확인
- [x] 구현: `SequentialProducer.run()` 내 계측 코드 삽입
  - `input_queue.get()` 전후 시간 측정 → `add_idle_ns`
  - `step.process()` 전후 시간 측정 → `add_processing_ns`
  - 첫 아이템 시 `record_first_item`, 이후 대기시간만 `add_idle_ns`
  - `set_state` 호출 (idle → processing → done)
- [x] 린트/타입 통과
- 의존: M-05

### M-07: ParallelProducer 구조 변경 + 타이밍 계측
- [x] 테스트 작성 (`tests/test_engine.py`)
  - PARALLEL step 실행 중 `processed`가 점진적으로 증가함 확인 (sentinel 후 일괄 점프 아님)
  - 파이프라인 실행 후 PARALLEL step의 `processing_ns > 0` 확인
  - `first_item_at is not None` 확인
  - 기존 통합 테스트 회귀 없음 확인
- [x] 구현: result collection 인터리빙 — `ParallelProducer.run()`
  - 현재: sentinel 수신 후 `as_completed(futures)` 일괄 수거
  - 수정: 청크 submit 사이에 `f.done()` 체크하여 완료된 future 즉시 수거
  - `_collect_chunk_result()` 헬퍼로 통계 갱신 로직 추출
- [x] 구현: `_parallel_worker` 반환값에 `processing_ns` 추가
- [x] 구현: `ParallelProducer.run()` 메인 스레드 계측
  - `input_queue.get()` 대기시간 측정 → `add_idle_ns`
  - 워커 결과에서 `processing_ns` 집계 → `add_processing_ns`
  - `set_state` 호출
- [x] 린트/타입 통과
- 의존: M-05

### M-08: InputProducer 계측 + 총 입력 수 산출
- [x] 테스트 작성
  - SOURCE step의 `current_state == "done"` 확인
  - `first_item_at` 설정 확인
  - `stats.total_items > 0` 확인
- [x] 구현: `InputProducer.run()` 계측 (record_first_item, set_state)
- [x] 구현: `io.py`에 `count_jsonl_lines(paths) -> int` 추가 (바이너리 모드)
- [x] 구현: `pipeline.py`의 `run()` 시작부에 라인 카운트 → `stats.set_total_items()`
- [x] 린트/타입 통과
- 의존: M-05

---

## Phase 5: ProgressReporter

### M-09: format_progress 포맷팅 함수
- [x] 테스트 작성 (`tests/test_progress.py` — 신규)
  - StatsCollector 스냅샷으로 포맷 문자열 생성 검증
  - step 순서 유지 확인
  - 각 step 상태별 표시 형식 검증 (waiting, idle, processing, done)
  - 건당 평균 시간 포맷 확인 (ms 단위)
  - processed=0 일 때 avg 표시 안 함
- [x] 구현: `src/task_pipeliner/progress.py` 신규 모듈
  - `format_progress(stats, step_names, elapsed) -> str` — 순수 함수
- [x] 린트/타입 통과
- 의존: M-04

**표시 형식**:
```
--- Pipeline Progress (12.3s) -------------------------------------------
  JsonlSourceStep        372 produced                          [done 1.2s]
  QualityFilterStep      372 in → 329 kept, 43 removed  1.2ms/item  [done 4.5s]
  HashComputeStep        329 in → 329 out               0.8ms/item  [processing]
  HashLookupStep          50 in → 48 kept, 2 removed    0.1ms/item  [idle 0.3s]
  MinHashComputeStep       0 in                                      [waiting]
  WriterStep              43 in                          0.0ms/item  [processing]
-------------------------------------------------------------------------
```

### M-10: ProgressReporter 스레드 클래스
- [x] 테스트 작성 (`tests/test_progress.py`)
  - start/stop 라이프사이클 정상 동작
  - stop 후 스레드 종료 확인
  - interval 간격으로 출력 확인 (짧은 interval로 테스트)
  - stderr 출력 확인 (stdout 아님)
  - progress.log 파일 생성 및 내용 기록 확인
  - 매 write 시 flush (tail -f 호환)
- [x] 구현: `ProgressReporter(Thread)`
  - `__init__(stats, step_names, interval=5.0, output_dir=None)`
  - `run()`: `stop_event.wait(interval)` 루프 → stderr + progress.log 동시 출력
  - `stop()`: event set, 최종 출력 1회, 파일 핸들 close
  - daemon=True
- [x] 린트/타입 통과
- 의존: M-09

### M-11: Engine 통합 + 콘솔 로그 분리
- [x] 테스트 작성 (`tests/test_engine.py`)
  - 파이프라인 실행 시 ProgressReporter 동작 확인 (stderr 캡처)
  - 파이프라인 완료 후 reporter 스레드 종료 확인
  - progress.log 파일이 output_dir에 생성됨 확인
  - 파이프라인 실행 중 task_pipeliner INFO 로그가 콘솔에 출력되지 않음 확인
  - 파이프라인 완료 후 propagate 원복 확인
- [x] 구현: `PipelineEngine.run()` 에서 ProgressReporter 생성/start/stop
- [x] 구현: 콘솔 로그 분리 (`propagate` 방식)
  - `logging.getLogger("task_pipeliner").propagate = False` (실행 중)
  - finally에서 `propagate = True` 원복
- [x] 린트/타입 통과
- 의존: M-06, M-07, M-08, M-10

---

## Phase 6: 샘플 프로젝트 수정 (pretrain-data-filter)

### M-12: HashLookupStep state 사용
> `self._seen` 인스턴스 변수를 제거하고 `state` 파라미터를 사용하도록 수정.

- [x] 테스트 수정/추가 (state로 seen set 전달 검증)
- [x] steps.py: `initial_state` 프로퍼티 추가 (`set()` 반환), `process()`에서 `state` 사용, `self._seen` 제거
- [x] 테스트 통과 (sample 테스트)
- [x] 린트/타입 통과
- 의존: M-03

### M-13: ~~MinHashLookupStep state 사용~~ (불필요 — M-12로 패턴 검증 충분)
> HashLookupStep(M-12)에서 `initial_state` 패턴이 검증되었으므로 MinHashLookupStep은 인스턴스 변수 방식 유지.
- 의존: M-03

---

## Phase 7: 마무리

### M-14: 통합 테스트 & 검증
- [x] sample/pretrain-data-filter 파이프라인 실행하여 실제 progress 출력 확인
- [x] progress.log에 실시간 스냅샷 기록 확인
- [x] stats.json에 신규 메트릭 포함 확인
- [x] 기존 테스트 전체 통과 확인
- [x] 린트/타입 전체 통과 확인
- 의존: M-11, M-12, M-13

### M-15: export 및 문서 갱신
- [x] `ProgressReporter`를 `__init__.py`에 export 여부 결정 및 반영 (내부 사용만 — export하지 않음)
- [x] 리팩토링 보고서에 완료 상태 반영
- 의존: M-14

---

## 의존 관계 요약

```
M-01 (큐 안정화) ──────────────────────────────────────────┐
                                                            │
M-02 (initial_state) → M-03 (engine 연동) → M-12 (Hash)   │
                                           → M-13 (MinHash)│
                                                            │
M-04 (StepStats 필드) → M-05 (StatsCollector API)          │
                         ├→ M-06 (Sequential 계측)          │
                         ├→ M-07 (Parallel 계측)            │
                         └→ M-08 (Input 계측 + 라인카운트)  │
                                                            │
M-04 → M-09 (format_progress) → M-10 (Reporter 스레드)     │
                                                            │
M-06 + M-07 + M-08 + M-10 → M-11 (Engine 통합 + 로그분리) │
                                                            │
M-11 + M-12 + M-13 → M-14 (통합 검증) → M-15 (문서)       │
```

**병렬 가능 그룹:**
- M-01, M-02, M-04 — 모두 독립, 동시 착수 가능
- M-06, M-07, M-08 — M-05 이후 병렬 가능
- M-12, M-13 — M-03 이후 병렬 가능
- M-09 — M-04 이후 M-05와 독립적으로 착수 가능

## 출력 채널 정리

```
파이프라인 실행 시:
  콘솔(stderr)  ← ProgressReporter (카운터 + 상태)  +  WARNING/ERROR 로그만
  pipeline.log  ← 기존 DEBUG 이상 상세 로그 (변경 없음)
  progress.log  ← progress 스냅샷 (tail -f 감시용, 매 write flush)
  stats.json    ← 완료 후 최종 메트릭 (기존 + 신규 타이밍 필드)
```

---

## Phase 8: Step 간 동적 state 주입 + is_ready 게이팅

### M-16: BaseStep.is_ready + set_step_state
> Step이 다른 Step의 state를 동적으로 설정하고, 수신 Step이 is_ready로 처리 시작을 제어.

- [x] base.py: `is_ready(state) -> bool` 메서드 추가 (기본 True)
- [x] base.py: `set_step_state(target, state)` 메서드 + `_state_dispatch` 콜백 속성 추가
- [x] base.py: `__getstate__`/`__setstate__`로 pickle 시 `_state_dispatch` 제외
- [x] dummy_steps.py: `CollectorStep`, `StateGatedStep` 추가
- [x] 테스트: is_ready 기본값, 오버라이드, set_step_state 콜백 동작
- 의존: M-02

### M-17: Producer is_ready 게이팅
> Producer가 is_ready=False일 때 처리를 유보하고, state 변경 이벤트에 의해 재평가.

- [x] producers.py: `state_changed_event: threading.Event` 파라미터 추가
- [x] producers.py: `_wait_until_is_ready()` 메서드 추가 (ready_events + is_ready 루프)
- [x] SequentialProducer/ParallelProducer: `_wait_until_ready()` → `_wait_until_is_ready()` 교체
- [x] 테스트: producer 블로킹/해제 동작 검증
- 의존: M-16

### M-18: Engine state dispatch 배선
> Engine이 step.set_step_state()용 콜백을 생성하여 각 Step에 주입.

- [x] engine.py: producer_by_name, state_events 맵 구축
- [x] engine.py: `_make_state_dispatch` 콜백 생성 → 각 step._state_dispatch에 주입
- [x] engine.py: state_changed_event를 각 producer에 전달
- [x] 테스트: 엔진 통합 (source → collector + gated filter) 검증
- [x] 린트/타입 통과
- 의존: M-17

### M-19: BaseAggStep 제거
> 실사용 0건인 BaseAggStep 클래스 및 참조 제거.

- [x] base.py에서 BaseAggStep 클래스 삭제 (사용자 수동)
- [x] __init__.py, dummy_steps.py, test_schema.py, test_init.py에서 참조 제거
- 의존: 없음

---

## Phase 9: Step Lifecycle — open() hook

### M-20: BaseStep.open() 메서드 추가
> process() 루프 시작 직전에 호출되는 리소스 초기화 훅. close()와 대칭.

- [x] 레퍼런스 탐색 (현재 생명주기 확인)
- [x] 테스트 작성 (test_schema.py: open() no-op 기본값, open/close 대칭 검증)
- [x] base.py: `open()` 메서드 추가 (no-op 기본, PARALLEL pickle 주의사항 docstring)
- [x] 테스트 통과
- [x] 린트/타입 통과
- 의존: 없음

### M-21: Producer에서 open() 호출
> 각 Producer의 run()에서 처리 루프 직전에 step.open() 호출.

- [x] 테스트 작성 (test_engine.py: LifecycleTrackingStep으로 open 호출 검증)
- [x] dummy_steps.py: LifecycleTrackingStep 추가
- [x] producers.py: InputProducer, SequentialProducer, ParallelProducer에 step.open() 삽입
- [x] 테스트 통과
- [x] 린트/타입 통과
- 의존: M-20

### M-22: CLAUDE.md Step Lifecycle 섹션 추가
- [x] CLAUDE.md에 Step Lifecycle 컨벤션 문서화 (open/close 대칭, PARALLEL pickle 주의사항)
- 의존: M-20

## Phase 10: orjson 핵심 의존성 제거

### M-23: 프레임워크 본체에서 orjson → json stdlib 전환
> 프레임워크 핵심 의존성을 최소화. orjson은 파이프라인 완료 시 1회 / batch 명령 1회만 사용하므로 stdlib json으로 대체.

- [x] stats.py: `orjson.dumps()` → `json.dumps()` (indent, sort_keys, ensure_ascii 옵션)
- [x] cli.py: `orjson.loads()` → `json.loads()`
- [x] import 정렬 (ruff I001)
- [x] pyproject.toml: `orjson>=3.9`을 dependencies에서 제거, dev optional-dependencies로 이동
- [x] 패키지 재설치 후 전체 테스트 통과 (230/230)
- [x] 린트/타입 통과
- 의존: 없음

---

## Phase 11: README 문서화

### M-24: README에 Step Lifecycle 반영
> open()/close() 생명주기를 README의 BaseStep 테이블, Quick Start 예제, lifecycle 다이어그램에 반영.

- [x] BaseStep 테이블에 `open()` 행 추가, `close()` 설명 보강
- [x] lifecycle 다이어그램 추가 (`__init__ → is_ready → open → process × N → close`)
- [x] Quick Start WriterStep 예제를 open/close 패턴으로 교체
- 의존: M-20

---

## Phase 12: ParallelProducer 프로듀서 주도 큐 구조 전환

### M-25: _parallel_worker 프로듀서 주도 반환 구조
> 워커가 output_queue에 직접 put하는 streaming put 구조를 제거하고,
> 아이템을 반환값으로 돌려서 프로듀서가 output_queue에 단독 put하는 구조로 전환.
> Sentinel race condition, 워커 사망 시 큐 파손, cross-process write 경합 해결.

- [x] 레퍼런스 탐색 (pipeline-stall-analysis.md)
- [x] 테스트 작성 (TestParallelWorker — _parallel_worker 단위 테스트 5개)
- [x] `_worker_output_queues` 글로벌 + `_init_worker` 함수 제거
- [x] `_parallel_worker` 변경: `_collect_emit`으로 로컬 리스트 누적, 반환 타입 `dict[str, list[Any]]`
- [x] `_drain_future_result` 헬퍼 추가 (아이템 → output_queue put + stats 갱신)
- [x] `_collect_completed` 리팩토링 (`_drain_future_result` 사용)
- [x] `ProcessPoolExecutor` 생성에서 `initializer`/`initargs` 제거
- [x] `as_completed` 루프 → `_drain_future_result` 사용
- [x] 기존 TestParallelProducer + TestTaggedEmitRouting 통과 확인
- [x] 린트/타입 통과 (ruff check + mypy)
- 의존: 없음

---

## TODO: 디스크 스필 큐 (이번 범위 밖)

> `maxsize=0` 전환으로 데드락은 해결되지만 대용량 처리 시 OOM 위험이 남는다.
> 이후 별도 WBS로 `DiskSpillQueue` (multiprocessing.Queue 래퍼 + 디스크 오버플로우)를 구현한다.
> 외부 의존성 없이 표준 라이브러리만 사용. 상세: `docs/refactoring-step-metrics.md` §9 참조.
