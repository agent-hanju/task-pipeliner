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

- [ ] 레퍼런스 탐색
- [ ] engine.py 큐 생성부 수정 (3곳: line 212, 228, 236) — `ctx.Queue(maxsize=queue_size)` → `ctx.Queue()`
- [ ] config.py `ExecutionConfig.queue_size` 기본값 0으로 변경, docstring에 "향후 디스크 스필 임계치 전용" 명시
- [ ] 기존 테스트 통과 확인
- [ ] 린트/타입 통과
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

- [ ] 레퍼런스 탐색
- [ ] 테스트 작성 (dummy step with initial_state, 기본값 None 검증)
- [ ] base.py에 `initial_state` 프로퍼티 추가
- [ ] 테스트 통과
- [ ] 린트/타입 통과
- 의존: 없음

### M-03: Engine에서 `state=step.initial_state` 전달
> Producer 생성 시 step의 `initial_state`를 `state` 인자로 전달.

- [ ] engine.py Producer 생성부 수정 (SequentialProducer, ParallelProducer 모두)
- [ ] state가 process()에 전달되는지 통합 테스트 추가 (dummy step이 state를 mutate하고 검증)
- [ ] 테스트 통과
- [ ] 린트/타입 통과
- 의존: M-02

---

## Phase 3: StepStats 확장 (데이터 모델)

### M-04: StepStats 타이밍 필드 추가
- [ ] 테스트 작성 (`tests/test_stats.py`)
  - `first_item_at` 초기값 None, 설정 후 값 존재
  - `processing_ns`, `idle_ns`, `idle_count` 초기값 0
  - `current_state` 초기값 `"waiting"`
  - `to_dict()` 에 신규 키 포함 확인
  - 기존 `test_to_dict_keys` 갱신
- [ ] 구현: `StepStats` dataclass에 필드 추가
- [ ] 구현: `to_dict()` 확장 — `initial_wait_seconds`, `processing_seconds`, `processing_avg_ms`, `idle_seconds`, `idle_avg_ms` 산출
- [ ] 린트/타입 통과
- 의존: 없음

### M-05: StatsCollector API 확장
- [ ] 테스트 작성 (`tests/test_stats.py`)
  - `set_total_items(n)` → `total_items` 속성 반환
  - `record_first_item(step_name)` → `first_item_at` 설정, 중복 호출 시 무시
  - `add_processing_ns(step_name, ns)` → `processing_ns` 누적
  - `add_idle_ns(step_name, ns)` → `idle_ns` 누적, `idle_count` +1
  - `set_state(step_name, state)` → `current_state` 갱신
  - `write_json()` 결과에 신규 필드 포함 확인
- [ ] 구현: 각 메서드 추가 (기존 `_lock` 활용)
- [ ] 린트/타입 통과
- 의존: M-04

---

## Phase 4: Producer 계측

### M-06: SequentialProducer 타이밍 계측
- [ ] 테스트 작성 (`tests/test_engine.py`)
  - 파이프라인 실행 후 SEQUENTIAL step의 `processing_ns > 0` 확인
  - `first_item_at is not None` 확인
  - `current_state == "done"` 확인
- [ ] 구현: `SequentialProducer.run()` 내 계측 코드 삽입
  - `input_queue.get()` 전후 시간 측정 → `add_idle_ns`
  - `step.process()` 전후 시간 측정 → `add_processing_ns`
  - 첫 아이템 시 `record_first_item`, 이후 대기시간만 `add_idle_ns`
  - `set_state` 호출 (idle → processing → done)
- [ ] 린트/타입 통과
- 의존: M-05

### M-07: ParallelProducer 구조 변경 + 타이밍 계측
- [ ] 테스트 작성 (`tests/test_engine.py`)
  - PARALLEL step 실행 중 `processed`가 점진적으로 증가함 확인 (sentinel 후 일괄 점프 아님)
  - 파이프라인 실행 후 PARALLEL step의 `processing_ns > 0` 확인
  - `first_item_at is not None` 확인
  - 기존 통합 테스트 회귀 없음 확인
- [ ] 구현: result collection 인터리빙 — `ParallelProducer.run()`
  - 현재: sentinel 수신 후 `as_completed(futures)` 일괄 수거
  - 수정: 청크 submit 사이에 `f.done()` 체크하여 완료된 future 즉시 수거
  - `_collect_chunk_result()` 헬퍼로 통계 갱신 로직 추출
- [ ] 구현: `_parallel_worker` 반환값에 `processing_ns` 추가
- [ ] 구현: `ParallelProducer.run()` 메인 스레드 계측
  - `input_queue.get()` 대기시간 측정 → `add_idle_ns`
  - 워커 결과에서 `processing_ns` 집계 → `add_processing_ns`
  - `set_state` 호출
- [ ] 린트/타입 통과
- 의존: M-05

### M-08: InputProducer 계측 + 총 입력 수 산출
- [ ] 테스트 작성
  - SOURCE step의 `current_state == "done"` 확인
  - `first_item_at` 설정 확인
  - `stats.total_items > 0` 확인
- [ ] 구현: `InputProducer.run()` 계측 (record_first_item, set_state)
- [ ] 구현: `io.py`에 `count_jsonl_lines(paths) -> int` 추가 (바이너리 모드)
- [ ] 구현: `pipeline.py`의 `run()` 시작부에 라인 카운트 → `stats.set_total_items()`
- [ ] 린트/타입 통과
- 의존: M-05

---

## Phase 5: ProgressReporter

### M-09: format_progress 포맷팅 함수
- [ ] 테스트 작성 (`tests/test_progress.py` — 신규)
  - StatsCollector 스냅샷으로 포맷 문자열 생성 검증
  - step 순서 유지 확인
  - 각 step 상태별 표시 형식 검증 (waiting, idle, processing, done)
  - 건당 평균 시간 포맷 확인 (ms 단위)
  - processed=0 일 때 avg 표시 안 함
- [ ] 구현: `src/task_pipeliner/progress.py` 신규 모듈
  - `format_progress(stats, step_names, elapsed) -> str` — 순수 함수
- [ ] 린트/타입 통과
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
- [ ] 테스트 작성 (`tests/test_progress.py`)
  - start/stop 라이프사이클 정상 동작
  - stop 후 스레드 종료 확인
  - interval 간격으로 출력 확인 (짧은 interval로 테스트)
  - stderr 출력 확인 (stdout 아님)
  - progress.log 파일 생성 및 내용 기록 확인
  - 매 write 시 flush (tail -f 호환)
- [ ] 구현: `ProgressReporter(Thread)`
  - `__init__(stats, step_names, interval=5.0, output_dir=None)`
  - `run()`: `stop_event.wait(interval)` 루프 → stderr + progress.log 동시 출력
  - `stop()`: event set, 최종 출력 1회, 파일 핸들 close
  - daemon=True
- [ ] 린트/타입 통과
- 의존: M-09

### M-11: Engine 통합 + 콘솔 로그 분리
- [ ] 테스트 작성 (`tests/test_engine.py`)
  - 파이프라인 실행 시 ProgressReporter 동작 확인 (stderr 캡처)
  - 파이프라인 완료 후 reporter 스레드 종료 확인
  - progress.log 파일이 output_dir에 생성됨 확인
  - 파이프라인 실행 중 task_pipeliner INFO 로그가 콘솔에 출력되지 않음 확인
  - 파이프라인 완료 후 propagate 원복 확인
- [ ] 구현: `PipelineEngine.run()` 에서 ProgressReporter 생성/start/stop
- [ ] 구현: 콘솔 로그 분리 (`propagate` 방식)
  - `logging.getLogger("task_pipeliner").propagate = False` (실행 중)
  - finally에서 `propagate = True` 원복
- [ ] 린트/타입 통과
- 의존: M-06, M-07, M-08, M-10

---

## Phase 6: 샘플 프로젝트 수정 (pretrain-data-filter)

### M-12: HashLookupStep state 사용
> `self._seen` 인스턴스 변수를 제거하고 `state` 파라미터를 사용하도록 수정.

- [ ] 테스트 수정/추가 (state로 seen set 전달 검증)
- [ ] steps.py: `initial_state` 프로퍼티 추가 (`set()` 반환), `process()`에서 `state` 사용, `self._seen` 제거
- [ ] 테스트 통과 (sample 테스트)
- [ ] 린트/타입 통과
- 의존: M-03

### M-13: MinHashLookupStep state 사용
> `self._lsh`, `self._counter` 인스턴스 변수를 제거하고 `state` 파라미터를 사용하도록 수정.

- [ ] 테스트 수정/추가 (state로 LSH + counter 전달 검증)
- [ ] steps.py: `initial_state` 프로퍼티 추가 (`(MinHashLSH(...), [0])` 반환), `process()`에서 `state` 사용, `self._lsh`/`self._counter` 제거
- [ ] 테스트 통과 (sample 테스트)
- [ ] 린트/타입 통과
- 의존: M-03

---

## Phase 7: 마무리

### M-14: 통합 테스트 & 검증
- [ ] sample/pretrain-data-filter 파이프라인 실행하여 실제 progress 출력 확인
- [ ] progress.log에 실시간 스냅샷 기록 확인
- [ ] stats.json에 신규 메트릭 포함 확인
- [ ] 기존 테스트 전체 통과 확인
- [ ] 린트/타입 전체 통과 확인
- 의존: M-11, M-12, M-13

### M-15: export 및 문서 갱신
- [ ] `ProgressReporter`를 `__init__.py`에 export 여부 결정 및 반영
- [ ] 리팩토링 보고서에 완료 상태 반영
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

## TODO: 디스크 스필 큐 (이번 범위 밖)

> `maxsize=0` 전환으로 데드락은 해결되지만 대용량 처리 시 OOM 위험이 남는다.
> 이후 별도 WBS로 `DiskSpillQueue` (multiprocessing.Queue 래퍼 + 디스크 오버플로우)를 구현한다.
> 외부 의존성 없이 표준 라이브러리만 사용. 상세: `docs/refactoring-step-metrics.md` §9 참조.
