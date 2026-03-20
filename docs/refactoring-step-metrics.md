# 리팩토링 보고서: Step Metrics, Queue Architecture, State 활용

> 작성일: 2026-03-20 | 갱신일: 2026-03-20

## 1. 배경 및 동기

sample/pretrain-data-filter 파이프라인을 실행하면 각 step이 어디까지 진행됐는지, 병목이 어디인지 파악할 수 없다.
레퍼런스 프로젝트(hanati-pretrain-data-filter)는 `tqdm` 기반 진행률 표시 + 최종 요약 리포트를 제공했으나,
task-pipeliner는 DAG 기반 멀티프로세스 파이프라인이라 단순한 tqdm 적용이 불가능하다.

또한 기존 큐 아키텍처 검토 과정에서 두 가지 추가 이슈가 발견됨:

1. **큐 `maxsize` + 백프레셔 설계 결함**: `maxsize` 큐가 데드락과 ParallelProducer 백프레셔 우회를 유발
2. **`state` 메커니즘 미사용**: 프레임워크에 구현된 `state`/`ready_events`/`next_state_setter`를 샘플 프로젝트의 dedup step들이 사용하지 않고 인스턴스 변수로 우회

**핵심 과제**:
- 파이프라인 실행 중 각 step의 실시간 상태를 직관적으로 파악할 수 있는 메트릭 수집 + CLI 표시 체계 구축
- 큐 아키텍처 안정화 (`maxsize=0` 전환)
- dedup step들이 프레임워크의 `state` 메커니즘을 정상적으로 사용하도록 수정

---

## 2. 현황 분석

### 2.1 현재 StepStats 구조 (stats.py)

| 필드 | 타입 | 용도 |
|------|------|------|
| `step_name` | `str` | step 식별자 |
| `processed` | `int` | 처리 완료 아이템 수 |
| `errored` | `int` | 예외 발생 아이템 수 |
| `emitted` | `dict[str, int]` | 태그별 출력 아이템 수 |
| `_start_time` | `float` | 등록 시점 (monotonic) |
| `_end_time` | `float \| None` | 완료 시점 |

**한계점:**

- **타이밍 정보 부재**: `process()` 건당 소요시간, 큐 대기시간, 유휴시간 등 미수집
- **상태 표현 없음**: step이 현재 처리 중인지, 큐 대기 중인지, 아직 시작 전인지 알 수 없음
- **실시간 표시 없음**: 모든 정보가 완료 후 stats.json에만 기록됨
- **총 입력 수 미추적**: 파이프라인 전체의 입력 아이템 수를 모름

### 2.2 현재 수집 지점 (producers.py)

| Producer | 수집 시점 | 수집 내용 |
|----------|-----------|-----------|
| `InputProducer` | 아이템 yield 시 | processed +1, emitted[tag] +1 |
| `SequentialProducer` | process() 후 | processed +1, errored +1 (예외 시) |
| `ParallelProducer` | 워커 청크 완료 시 | processed +n, errored +n, emitted[tag] +n |

**한계점:**
- `process()` 호출 전후 시간 측정 없음
- `input_queue.get()` 대기 시간 측정 없음
- 첫 아이템 도착 시점 기록 없음
- 현재 상태(idle/processing) 추적 없음
- **ParallelProducer 통계 지연**: result collection이 sentinel 수신 후에만 발생 — 워커에서 실제 처리가 진행 중이어도 `processed=0`으로 보이다가 완료 시 전체 수로 점프. 실시간 progress 표시 시 치명적 UX 결함

### 2.3 레퍼런스 프로젝트 (hanati-pretrain-data-filter)

- **진행률**: `tqdm` 프로그레스 바 (단일 프로세스 기반이라 가능)
- **타이밍**: `time.perf_counter()`로 phase별 wall-clock 시간 수집
- **최종 리포트**: 총 문서수/유지/제거/비율/사유별통계/단계별시간/용량 정보
- **한계**: 단일 프로세스 + HuggingFace Dataset 기반이라 task-pipeliner에 직접 적용 불가

---

## 3. 요구사항

사용자가 요청한 수집/표시 항목:

### 3.1 수집 메트릭 (per step)

| 메트릭 | 설명 | 산출 방식 |
|--------|------|-----------|
| **총 실행 시간** | step 시작~종료 wall-clock | `finished_at - started_at` |
| **process() 총 시간** | 모든 process() 호출 시간 합 | `Σ (process_end - process_start)` |
| **process() 건당 평균** | 위를 processed 수로 나눔 | `processing_ns / processed` |
| **큐 대기 시간 총합** | 첫 아이템 이후 queue.get() 대기 합 | `Σ (get_end - get_start)` (첫 아이템 이후) |
| **큐 대기 평균** | 위를 대기 횟수로 나눔 | `idle_ns / idle_count` |
| **초기 대기 시간** | step 시작 ~ 첫 아이템 도착 | `first_item_at - started_at` |
| **입력 수** | step에 들어온 아이템 수 | `processed + errored` |
| **출력 수** | emit된 아이템 수 (태그별) | 기존 `emitted` dict |
| **현재 상태** | waiting / idle / processing / done | 실시간 갱신 |

### 3.2 실시간 CLI 표시

- 기존 파일 로깅(pipeline.log)과 **별도** 체계
- 주기적(예: 5초)으로 각 step의 카운터/상태를 콘솔에 출력
- 퍼센티지 없이 카운터 기반 (앞 step 완료 전까지 denominator 미확정)
- 건당 수행시간, 유휴 여부 등 시각적으로 판단 가능하게 표현

---

## 4. 설계

### 4.1 StepStats 확장

```
현재:                          추가:
step_name                      first_item_at: float | None
processed                      processing_ns: int        (process() 총 나노초)
errored                        idle_ns: int               (큐 대기 총 나노초)
emitted                        idle_count: int            (큐 대기 횟수)
_start_time                    state: str                 (waiting|idle|processing|done)
_end_time
```

**주요 결정:**

- 나노초(`time.perf_counter_ns()`) 사용 → ms 미만 단위까지 정밀 측정 (호출당 ~50ns, C 레벨)
- `state` 필드: progress 표시 전용이므로 lock 없이 단순 대입 — CPython GIL 하에서 str 대입은 atomic, 근사값이면 충분
- `idle_count` 분리: 평균 산출 시 `idle_ns / idle_count` (processed 기반이 아닌 실제 대기 횟수 기반)
- lock 호출 증가 (아이템당 1~2회 → 4~5회): threading.Lock 획득 ~100ns, `process()` 대비 무시 가능

### 4.2 StatsCollector API 확장

```python
# 기존 (유지)
register(step_name) → StepStats
increment(step_name, field, n=1)
increment_emitted(step_name, tag, n=1)
finish(step_name)
setup_log_handler(path)
write_json(path)
flush()

# 신규
set_total_items(n: int)        # 파이프라인 총 입력 아이템 수 설정
record_first_item(step_name)   # 첫 아이템 도착 시각 기록
add_processing_ns(step_name, ns: int)  # process() 소요시간 누적
add_idle_ns(step_name, ns: int)        # 큐 대기시간 누적
set_state(step_name, state: str)       # 현재 상태 갱신
```

### 4.3 to_dict() 확장

```python
def to_dict(self) -> dict[str, object]:
    return {
        "step_name": self.step_name,
        "processed": self.processed,
        "errored": self.errored,
        "emitted": dict(self.emitted),
        "elapsed_seconds": round(self.elapsed_seconds, 4),
        # 신규
        "initial_wait_seconds": ...,       # 첫 아이템 도착까지 대기
        "processing_seconds": ...,         # process() 총 시간
        "processing_avg_ms": ...,          # 건당 평균 (ms)
        "idle_seconds": ...,               # 큐 대기 총 시간
        "idle_avg_ms": ...,                # 큐 대기 평균 (ms)
    }
```

### 4.4 Producer 계측 패턴

**SequentialProducer (단일 스레드)**:
```python
while True:
    stats.set_state(step.name, "idle")
    t0 = time.perf_counter_ns()
    item = input_queue.get()
    wait_ns = time.perf_counter_ns() - t0

    if is_sentinel(item): break

    if not first_item_seen:
        stats.record_first_item(step.name)
        first_item_seen = True
    else:
        stats.add_idle_ns(step.name, wait_ns)

    stats.set_state(step.name, "processing")
    t0 = time.perf_counter_ns()
    result = step.process(item, state, emit)
    stats.add_processing_ns(step.name, time.perf_counter_ns() - t0)
    stats.increment(step.name, "processed")
```

**ParallelProducer (구조 변경 필요)**:

현재 구조의 문제: result collection이 sentinel 수신 후에만 발생하여
워커에서 실제 처리 중이어도 `processed=0`으로 보임:

```python
# 현재 (문제)
while True:                          # 1. 아이템 소비 루프
    item = input_queue.get()
    futures.append(submit(chunk))
for future in as_completed(futures): # 2. sentinel 이후에야 통계 수집 ← 문제
    stats.increment("processed", n)
```

수정: 청크 submit 사이에 완료된 future를 즉시 수거 (인터리빙):

```python
# 수정 후
while True:
    item = input_queue.get()
    futures.append(submit(chunk))
    # 완료된 future 즉시 수거 → 실시간 통계 갱신
    done = [f for f in futures if f.done()]
    for f in done:
        futures.remove(f)
        _collect_chunk_result(f)     # stats.increment 즉시 호출
# 잔여 future 수거
for future in as_completed(futures):
    _collect_chunk_result(future)
```

계측 포인트:
- 메인 스레드: `input_queue.get()` 대기시간 측정 (idle_ns)
- 워커 프로세스: `_parallel_worker` 반환값에 `processing_ns: int` 추가
- 메인 스레드에서 워커 결과 집계 시 `add_processing_ns` 호출

**InputProducer**:
- `items()` 반복하며 총 아이템 수 카운트 → `set_total_items()` 호출
- 또는 `pipeline.py`에서 JSONL 라인 수 사전 카운트 (2-pass 부담 vs 정확한 total)

### 4.5 콘솔 로그 전략

**현재 문제**: CLI 실행 시 `logging.basicConfig(level=logging.INFO)`로 인해
`task_pipeliner.*` 로거의 INFO 메시지("producer started", "sentinel received" 등)가
콘솔(stderr)에 출력됨. progress 표시와 섞이면 지저분해짐.

**결정: B안 — 콘솔은 progress 전용, 상세 로그는 파일로**

```
콘솔(stderr)  ← ProgressReporter만 출력 + WARNING/ERROR 로그
pipeline.log  ← 기존 DEBUG 이상 상세 로그 (변경 없음)
progress.log  ← progress 스냅샷 파일 (신규, tail -f 감시용)
```

**구현 — `propagate` 제어 방식 (글로벌 부작용 없음):**

~~레벨 변경 방식~~ (기각): root 핸들러 레벨을 WARNING으로 올리면
`task_pipeliner.*`뿐 아니라 **사용자 코드의 모든 INFO 로그**도 콘솔에서 사라짐.

`propagate` 방식 (채택):
```python
# 파이프라인 실행 중
tp_logger = logging.getLogger("task_pipeliner")
tp_logger.propagate = False   # root로 전파 차단 → 콘솔 출력 안 됨
# file handler는 task_pipeliner 로거에 직접 붙어있으므로 파일 기록 유지

# 파이프라인 완료 후
tp_logger.propagate = True    # 원복
```

- task_pipeliner 로그만 콘솔에서 사라지고, 사용자 코드 로깅은 영향 없음
- file handler는 `task_pipeliner` 로거에 직접 부착되어 있으므로 propagate 무관하게 기록
- WARNING/ERROR는 파일에만 기록됨 — 심각한 이슈는 progress.log에도 별도 표시 검토

### 4.6 ProgressReporter

**위치**: `src/task_pipeliner/progress.py` (신규 모듈)

**출력 대상 2곳:**

| 대상 | 용도 | flush 전략 |
|------|------|------------|
| **stderr** | CLI 실행 시 콘솔에 실시간 표시 | 매 출력마다 `flush()` |
| **progress.log** | `tail -f` 원격 모니터링용 | 매 출력마다 `flush()` → OS 버퍼 즉시 반영 → `tail -f`에서 실시간 감지 |

> `tail -f`가 실시간으로 보이는 원리: 파일에 `flush()` 하면 OS 레벨
> `inotify`(Linux) / `ReadDirectoryChangesW`(Windows)가 변경을 감지하여
> tail이 즉시 새 내용을 읽음. 핵심은 **매 write마다 flush** 하는 것.

**표시 형식** (카운터 기반):
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

**구성 요소:**
- `ProgressReporter` 클래스: 데몬 스레드 기반, `start()` / `stop()` 인터페이스
- `format_progress(stats, step_names, elapsed)` → 포맷팅 순수 함수 (테스트 가능)
- interval: 기본 5초 (ExecutionConfig에 `progress_interval` 추가 가능)
- `progress.log`: `output_dir / "progress.log"` 에 append, 매 write 시 `flush()`

### 4.6 총 입력 수 산출

**선택지 비교:**

| 방식 | 장점 | 단점 |
|------|------|------|
| A. JSONL 사전 카운트 | 정확한 total, 바로 사용 가능 | 파일 2-pass (대용량 시 I/O 추가) |
| B. InputProducer 완료 후 확정 | 추가 I/O 없음 | 파이프라인 시작 직후엔 total 미확정 |

**결정: A안 채택** — JSONL 라인 카운트는 파싱 없이 줄 수만 세면 되므로 I/O 부담 미미.
`pipeline.py`에서 `run()` 시작 시 입력 파일 라인 수 카운트 → `stats.set_total_items(n)`.

---

## 5. 영향 범위

### 5.1 수정 대상 파일

| 파일 | 변경 내용 | 영향도 |
|------|-----------|--------|
| `src/task_pipeliner/stats.py` | StepStats 필드 추가, StatsCollector API 확장 | **높음** — 핵심 데이터 모델 |
| `src/task_pipeliner/producers.py` | 계측 코드 삽입 (3개 Producer 모두) | **높음** — 실행 핵심 경로 |
| `src/task_pipeliner/progress.py` | **신규 모듈** — ProgressReporter | **중간** — 독립 모듈 |
| `src/task_pipeliner/engine.py` | ProgressReporter 시작/종료 통합 | **낮음** — 수 줄 추가 |
| `src/task_pipeliner/pipeline.py` | JSONL 라인 카운트, total_items 설정 | **낮음** — 수 줄 추가 |
| `src/task_pipeliner/io.py` | `count_lines()` 유틸 함수 추가 | **낮음** |
| `src/task_pipeliner/cli.py` | 콘솔 로그 레벨 WARNING 상향 | **낮음** |
| `src/task_pipeliner/__init__.py` | ProgressReporter export (선택) | **낮음** |

### 5.2 수정 대상 테스트

| 파일 | 변경 내용 |
|------|-----------|
| `tests/test_stats.py` | 신규 필드/메서드 테스트, to_dict 키 검증 갱신 |
| `tests/test_engine.py` | 통합 테스트에서 타이밍 메트릭 존재 검증 |
| `tests/test_progress.py` | **신규** — ProgressReporter, format_progress 테스트 |

### 5.3 하위 호환성

- `StepStats.to_dict()` 반환 키 추가 → stats.json에 새 필드 추가됨 (기존 소비자에 영향 없음, additive)
- `StatsCollector` 공개 API 추가만 (기존 메서드 시그니처 불변)
- `_parallel_worker` 반환 타입 변경 → 프레임워크 내부 함수이므로 외부 영향 없음
- 기존 테스트의 `to_dict_keys` 검증 → 키 목록 갱신 필요

### 5.4 성능 영향

- `time.perf_counter_ns()` 호출: 건당 ~50ns (Python 수준에서 무시 가능)
- ProgressReporter 스레드: 5초 간격 wake-up → CPU 영향 없음
- JSONL 라인 카운트: 바이너리 모드 줄 수 카운팅 → 파싱 대비 10배+ 빠름

---

## 6. 외부 레퍼런스 조사

멀티프로세스 파이프라인의 실시간 진행률 표시에 대해 조사한 주요 패턴과 라이브러리:

### 6.1 진행률 표시 라이브러리

| 라이브러리 | 특징 | 적합성 |
|------------|------|--------|
| [tqdm](https://github.com/tqdm/tqdm) | 가장 널리 쓰이는 프로그레스 바. stderr 출력. 멀티프로세스 지원 | 단일 step에는 적합하나, DAG 다중 step 동시 표시에는 제한적 |
| [Rich Progress](https://rich.readthedocs.io/en/stable/progress.html) | 멀티라인 프로그레스 디스플레이. stdout/stderr 리디렉션 지원 | 시각적으로 우수하나 의존성 추가 필요 |
| [alive-progress](https://github.com/rsalmei/alive-progress) | 실시간 throughput, ETA 표시. stderr 지원 | 단일 태스크 중심 설계 |
| [tqdm-multiprocess](https://pypi.org/project/tqdm-multiprocess/) | 멀티프로세스 환경에서 여러 tqdm 바 동시 표시 | 워커 프로세스별 바 — 우리 구조와 불일치 |

### 6.2 멀티프로세스 진행률 패턴

- **Shared Value/Array**: `mp.Value`로 프로세스 간 카운터 공유 → 우리는 스레드 기반 Producer이므로 `threading.Lock` 기반 StatsCollector로 충분
- **Callback 기반**: `ProcessPoolExecutor.submit()` + `as_completed()` 에서 future 완료 시 갱신 → ParallelProducer가 이미 이 패턴 사용 중
- **Queue 기반**: 워커가 진행 메시지를 별도 큐에 전송 → 오버헤드 대비 이점 적음

### 6.3 설계 결정

**plain text + stderr 채택 이유:**
- 외부 의존성 없이 프레임워크 단독 동작
- 파이프라인 출력(stdout)과 분리 가능
- 향후 Rich/tqdm 통합은 플러그인 형태로 확장 가능
- 우리 Producer는 스레드 기반 → StatsCollector의 threading.Lock만으로 안전한 읽기 가능

Sources:
- [tqdm - GitHub](https://github.com/tqdm/tqdm)
- [alive-progress - GitHub](https://github.com/rsalmei/alive-progress)
- [Rich Progress Display](https://rich.readthedocs.io/en/stable/progress.html)
- [Python multiprocessing progress patterns](https://superfastpython.com/multiprocessing-pool-show-progress/)
- [tqdm-multiprocess - PyPI](https://pypi.org/project/tqdm-multiprocess/)
- [Multiprocessing for Real-Time Applications](https://e2eml.school/multiproc_pipeline)

---

## 7. 큐 아키텍처 이슈 및 수정

### 7.1 현재 문제: `maxsize` 큐의 구조적 결함

**문제 1 — ParallelProducer 백프레셔 우회:**

`engine.py`에서 `ctx.Queue(maxsize=queue_size)` (기본값 200)로 큐를 생성하지만,
ParallelProducer는 `executor.submit()`으로 청크를 워커에 보내고, 워커가 `q.put()`을 호출한다.
`executor.submit()` 자체는 논블로킹이므로 워커들이 내부 큐에 무한정 쌓일 수 있다.
즉, `maxsize`가 PARALLEL→SEQUENTIAL 전환 구간에서 실질적으로 무효화됨.

**문제 2 — `ready_events` + `maxsize` 큐 데드락 가능성:**

`_wait_until_ready()`는 처리 루프 시작 전에 블로킹된다 (producers.py:206, 326).
Step K가 완료되어야 Step L의 `ready_event`가 set되는 구조에서,
Step L로 향하는 큐가 `maxsize`에 도달하면 Step N의 `put()`이 블로킹 →
Step K도 자신의 처리가 끝나지 않아 `ready_event`를 set하지 못함 → 순환 대기 (데드락).

### 7.2 수정: `maxsize=0` (무제한) 전환

**결정:** 모든 큐를 `maxsize=0` (무제한)으로 전환.

- `maxsize`가 원래 의도한 메모리 보호 기능이 ParallelProducer에서 작동하지 않으므로, 일관성 있게 제거
- 데드락 가능성 원천 차단
- OOM 우려에 대해서는 향후 디스크 스필 큐 래퍼로 대응 (§9 참조)

**변경 위치:**

| 파일 | 위치 | 변경 |
|------|------|------|
| `engine.py:212` | output 큐 생성 | `ctx.Queue(maxsize=queue_size)` → `ctx.Queue()` |
| `engine.py:228` | sentinel-only 큐 | `ctx.Queue(maxsize=queue_size)` → `ctx.Queue()` |
| `engine.py:236` | fan-in 병합 큐 | `ctx.Queue(maxsize=queue_size)` → `ctx.Queue()` |
| `config.py` | `ExecutionConfig.queue_size` | 필드 유지하되 기본값 0, 향후 디스크 스필 임계치로 전용 |

---

## 8. Dedup Step의 `state` 메커니즘 활용

### 8.1 현재 문제: `state` 미사용

프레임워크는 step 간 상태 전달을 위한 메커니즘을 제공한다:
- `BaseStep.process(item, state, emit)` — `state` 파라미터로 외부 상태 수신
- `BaseProducer.state` — Producer가 보유하는 상태 객체
- `BaseProducer.ready_events` — 선행 step 완료 대기
- `BaseProducer.next_state_setter` — 후행 step에 상태 전달

그러나 sample/pretrain-data-filter의 dedup step들은 이 메커니즘을 **전혀 사용하지 않는다:**

| Step | 설계 의도 | 현재 구현 (문제) |
|------|-----------|-----------------|
| `HashLookupStep` | `state`로 seen set 수신/관리 | `self._seen: set[str]` 인스턴스 변수 |
| `MinHashLookupStep` | `state`로 LSH 인덱스 수신/관리 | `self._lsh`, `self._counter` 인스턴스 변수 |

### 8.2 수정 방향

dedup step들이 `state` 파라미터를 사용하도록 수정하여 프레임워크의 state 메커니즘을 검증한다.

**HashLookupStep:**
```python
# 수정 후: state로 seen set 관리
def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> FilterResult:
    seen: set[str] = state  # Producer가 전달하는 state 객체
    hash_value: str = item["_dedup_hash"]
    if hash_value in seen:
        emit(item, "removed")
        return FilterResult(removed=1, removed_reasons={"dedup/exact": 1})
    seen.add(hash_value)
    emit(item, "kept")
    return FilterResult(kept=1)
```

**MinHashLookupStep:**
```python
# 수정 후: state로 (lsh, counter) 관리
def process(self, item: Any, state: Any, emit: Callable[[Any, str], None]) -> FilterResult:
    lsh, counter = state  # Producer가 전달하는 state 튜플
    mh: MinHash = item["_minhash"]
    if lsh.query(mh):
        emit(item, "removed")
        return FilterResult(removed=1, removed_reasons={"dedup/minhash": 1})
    counter[0] += 1  # mutable container로 카운터 갱신
    lsh.insert(str(counter[0]), mh)
    emit(item, "kept")
    return FilterResult(kept=1)
```

**Engine 측 변경:**

Producer 생성 시 `state` 초기값을 전달해야 한다. Step이 필요로 하는 초기 state를
선언할 수 있도록 `BaseStep`에 `initial_state()` 메서드를 추가하거나,
step 설정(yaml)에서 state 초기값을 지정하는 방식을 검토한다.

가장 단순한 접근: step의 `__init__`에서 초기 state를 생성하고 `initial_state` 프로퍼티로 노출.
Engine이 Producer 생성 시 `state=step.initial_state`를 전달.

```python
# BaseStep에 추가 (기본값: None)
@property
def initial_state(self) -> Any:
    return None

# HashLookupStep
@property
def initial_state(self) -> set[str]:
    return set()
```

**영향 범위:**
- `src/task_pipeliner/base.py` — `initial_state` 프로퍼티 추가
- `src/task_pipeliner/engine.py` — Producer 생성 시 `state=step.initial_state` 전달
- `sample/pretrain-data-filter/steps.py` — HashLookupStep, MinHashLookupStep 수정
- `sample/pretrain-data-filter/tests/` — state 관련 테스트 추가/수정

---

## 9. 디스크 스필 큐 (향후 TODO)

### 9.1 배경

`maxsize=0`으로 전환하면 데드락은 해결되지만, 대용량 데이터 처리 시
큐에 아이템이 무한히 쌓여 OOM이 발생할 수 있다.

### 9.2 조사 결과: 외부 라이브러리

| 라이브러리 | 백엔드 | 크로스 프로세스 | 비고 |
|------------|--------|----------------|------|
| [persist-queue](https://github.com/peter-wangxu/persist-queue) | SQLite/파일 | **미지원** (thread-safe only, [issue #132](https://github.com/peter-wangxu/persist-queue/issues/132)) | 메인테이너가 multiprocessing 미검증 명시 |
| [diskcache](https://github.com/grantjenks/python-diskcache) | SQLite+파일 | **지원** (프로세스 안전) | 범용 캐시 라이브러리, Deque 클래스로 큐 사용 가능 |
| FileQueue | flat file | 자체 파일 락 | 동기화 신뢰도 불확실 |

### 9.3 채택 방향: `multiprocessing.Queue` 래퍼

외부 라이브러리 대신 `multiprocessing.Queue`를 래핑하는 디스크 스필 큐를 자체 구현:

```
DiskSpillQueue
├── _queue: multiprocessing.Queue(maxsize=N)  ← 메모리 버퍼 (IPC, 크로스 프로세스 안전)
├── _overflow: 디스크 파일 (임계치 초과 시 pickle 직렬화)
└── put() / get() ← 기존 API 동일
```

- **put()**: 메모리 큐에 여유 있으면 `_queue.put()`, 가득 차면 디스크에 직렬화
- **get()**: 메모리 큐에서 꺼내되, 디스크에 밀린 아이템이 있으면 복원하여 보충
- 크로스 프로세스 안전성: `multiprocessing.Queue`가 보장
- 외부 의존성: 없음 (표준 라이브러리만 사용)

**구현 시기**: 이번 리팩토링 범위 밖. 메트릭/큐 안정화 완료 후 별도 작업으로 진행.

Sources:
- [persist-queue - PyPI](https://pypi.org/project/persist-queue/)
- [persist-queue multiprocess issue #132](https://github.com/peter-wangxu/persist-queue/issues/132)
- [diskcache - GitHub](https://github.com/grantjenks/python-diskcache)
- [DiskCache Tutorial](https://grantjenks.com/docs/diskcache/tutorial.html)

---

## 10. 미포함 사항 (향후 확장)

- ANSI 컬러/프로그레스 바 (rich/tqdm 통합) — 현 단계에서는 plain text
- 웹 기반 대시보드
- 메트릭 스트리밍 (Prometheus/StatsD)
- 최종 요약 리포트 (레퍼런스의 `═══` 형태) — 별도 작업으로 분리
- 디스크 스필 큐 (`DiskSpillQueue`) — §9 참조
