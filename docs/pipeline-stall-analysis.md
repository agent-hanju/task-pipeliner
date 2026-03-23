# Pipeline Stall & Data Loss 분석 보고서

> 작성일: 2026-03-23
> 대상 프로젝트: task-pipeliner (ParallelProducer)
> 테스트 환경: Docker (python:3.14-slim), naver_news 도메인 (~5M 레코드, 1072 파일)

---

## 1. 문제 요약

| 구분 | 증상 | 원인 |
|------|------|------|
| **Bulk Put Stall** | ~4.1M 레코드 처리 후 전체 파이프라인 무한 정지 | `cancel_join_thread` 미적용 + `join_timeout=30s` |
| **Streaming Put 유실** | 4,406,514건 emit → 다운스트림 2,007건만 수신 | Sentinel race condition (워커 feeder thread vs 메인 프로세스) |

---

## 2. Bulk Put Stall (AS-WAS 아키텍처)

### 2.1 아키텍처

```
ParallelProducer.run()  ─── input_queue.get() ──→  chunk 수집
                        ─── executor.submit()  ──→  _parallel_worker (워커 프로세스)
                                                      └── step.process(item, state, emit)
                                                      └── emit → items 리스트에 누적
                                                      └── return (result, items)  ← pickle 직렬화
                        ─── future.result()    ──→  items를 output_queue에 bulk put
                        ─── _send_sentinel()   ──→  다운스트림에 종료 신호
```

워커 프로세스에서 처리한 아이템을 **반환값(pickle)**으로 받아 **메인 프로세스에서 큐에 넣는** 구조.

### 2.2 증상

- 3회 연속 재현: ~4.1M 레코드에서 일관되게 정지
- `jsonl_file_loader`가 `[processing]` 상태에서 멈춤 (future 완료 대기)
- `naver_news_convert`도 `[processing]`에서 갱신 중단
- 다운스트림 스텝들은 정상 카운트 증가 중 → 이후 전부 정지
- OOM 아님 (`docker inspect`: OOMKilled=false, `dmesg` 클린)

### 2.3 원인 분석

#### (a) `cancel_join_thread` 미적용

```python
# multiprocessing.Queue 내부 구조:
#   put() → 내부 deque에 추가 (non-blocking)
#         → feeder thread가 deque → OS pipe로 drain (blocking when pipe full)
#
# 워커 프로세스 종료 시:
#   feeder thread의 pipe flush를 기다림 (join)
#   → pipe가 가득 차면 무한 대기 → 프로세스 종료 불가 → future 완료 불가
```

`ProcessPoolExecutor`가 워커 프로세스에 아이템을 보내는 `call_queue`도 `multiprocessing.Queue`이며,
워커 프로세스 내부의 `result_queue` feeder thread가 pipe 용량 초과 시 블로킹되면
워커 프로세스 자체가 종료되지 못한다.

**수정**:
```python
# producers.py - _init_worker()
def _init_worker(output_queues):
    global _worker_output_queues
    _worker_output_queues = output_queues
    for tag_queues in output_queues.values():
        for q in tag_queues:
            q.cancel_join_thread()  # ← 워커 프로세스에서 호출해야 함

# engine.py - 큐 생성 시
q = ctx.Queue()
q.cancel_join_thread()  # ← 부모 프로세스에서도 호출
```

#### (b) `join_timeout=30s` 조기 엔진 종료

```python
# engine.py 수정 전:
feeder.join(timeout=30)      # 30초 후 포기
for t in producer_threads:
    t.join(timeout=30)       # 각 스레드 30초씩

# 대규모 데이터 처리 시:
#   30s × N threads 후 엔진이 종료 → executor.shutdown() 호출
#   → "cannot schedule new futures after shutdown" RuntimeError
```

**수정**:
```python
join_timeout = 5 if shutdown_event.is_set() else None  # 정상 종료 시 무한 대기
```

### 2.4 재현 조건

- 대규모 fan-out (1 파일 → 수천 레코드)이 있는 ParallelProducer
- 충분히 많은 데이터 (pipe buffer 포화 필요, ~4M 레코드 수준)
- 복수의 output queue가 동시에 사용될 때 (kept + removed 등)

---

## 3. Streaming Put 유실 (시도한 대안)

### 3.1 아키텍처

```
_parallel_worker (워커 프로세스)
    └── step.process(item, state, _direct_emit)
        └── _direct_emit:
              tag_queues = _worker_output_queues[tag]
              for q in tag_queues:
                  q.put(item)            ← 워커에서 직접 큐에 put
    └── return (result, counts)          ← items는 반환하지 않음 (이미 큐에 들어감)

ParallelProducer.run()
    └── future.result()                  ← counts만 수집
    └── _send_sentinel()                 ← 다운스트림에 종료 신호
```

items를 반환값으로 받지 않고 **워커 프로세스에서 직접 output_queue에 put**하는 구조.
메모리 사용량 감소 + pickle 직렬화 비용 제거가 목적.

### 3.2 증상

```
--- Pipeline Progress (775.0s) ---
  jsonl_file_loader      4,406,514 in → 4,406,514 main    [done]
  naver_news_convert          2,007 in → 1,815 kept          [done]
  preprocess                  1,815 in → 1,815 kept          [done]
  ...
```

- `jsonl_file_loader`가 4,406,514건 emit
- **다음 스텝이 2,007건만 수신** (99.95% 유실)
- 파이프라인은 정상 완료 (에러 없음)

#### Gated step에서의 증폭 효과

```
--- Pipeline Progress (595.0s) ---
  quality_filter     1788 in → 1652 kept, 136 removed  [done]
  bp_collect                        1652 produced       [done]
  bp_clean                                 0 in         [processing]  ← 완전 유실
```

`quality_filter`(PARALLEL)가 `bp_collect`와 `bp_clean` 두 큐에 동시 emit하는 구조에서:
- `bp_collect`(SEQUENTIAL): 즉시 소비 → 1652건 정상 수신
- `bp_clean`(gated): `bp_collect` 완료까지 큐 소비 안 함 → 큐에 아이템 적체

**Gated step이 소비를 미루는 동안 Sentinel이 데이터를 완전히 추월:**
```
bp_clean 큐 (소비 없이 쌓이는 중):
  time 1: [item, item, ...]          ← 워커 feeder thread가 flush
  time 2: [item, SENTINEL, item...]  ← 메인이 sentinel 전송, 데이터 사이에 삽입
  time 3: [SENTINEL, item, item...]  ← 최악: sentinel이 맨 앞에 도달

bp_collect 완료 → bp_clean 소비 시작 → 첫 아이템이 Sentinel → 0 in
```

즉시 소비하는 스텝에서는 "일부 유실"이지만, gated step에서는 **전량 유실**이 발생한다.

### 3.3 원인: Sentinel Race Condition

```
시간 →

워커 프로세스 (N개)                   메인 프로세스 (ParallelProducer)
─────────────────                   ────────────────────────────
process(item, emit)
  emit → q.put(item)
  emit → q.put(item)               future = executor.submit(chunk)
  ...                               ...
  return (result, counts)           future.result() ← 여기서 return 받음

  [feeder thread 아직 flush 중]     _send_sentinel()
  q._buffer → pipe (진행 중)           q.put(Sentinel)  ← pipe에 직접 들어감

  pipe: [...data...][SENTINEL][...data...]

  다운스트림: SENTINEL 수신 → 소비 중단
             뒤에 남은 data는 영원히 읽히지 않음
```

**핵심 문제**: `multiprocessing.Queue.put()`은 내부 deque에 추가 후 feeder thread가
비동기로 pipe에 drain한다. `future.result()`는 워커 함수의 return만 기다리지,
feeder thread의 pipe flush 완료를 기다리지 않는다.

따라서:
1. 워커가 `q.put(item)` 호출 → deque에 추가 (즉시 반환)
2. 워커 함수 return → future 완료
3. 메인 프로세스가 `_send_sentinel()` → **Sentinel이 pipe에 먼저 도달**
4. feeder thread가 뒤늦게 남은 데이터를 pipe에 flush
5. 다운스트림은 Sentinel 이후 데이터를 읽지 않음 → **유실**

### 3.4 왜 bulk put에서는 이 문제가 없는가

Bulk put 구조에서는 아이템이 **워커의 반환값(pickle)**으로 메인 프로세스에 전달된 후,
**메인 프로세스가 직접** output_queue에 put한다. Sentinel도 메인 프로세스가 보내므로
동일 프로세스 내에서 순서가 보장된다.

```
Bulk: 메인이 put(data) → 메인이 put(sentinel)  ← 같은 프로세스, 순서 보장
Stream: 워커가 put(data) → 메인이 put(sentinel)  ← 다른 프로세스, 순서 미보장
```

---

## 4. 해결 방안

### 4.1 적용된 수정 (0.1.0 태그)

| 커밋 | 수정 내용 | 상태 |
|------|----------|------|
| ad59bbc | `cancel_join_thread` (워커 + 부모) | **유지** |
| e756e0a | `join_timeout = None` (정상 종료) | **유지** |
| a05c52b | streaming put 변경 | **되돌려야 함** |

### 4.2 필요한 조치

1. **streaming put 되돌리기**: `_parallel_worker`를 bulk put 구조로 복원
2. `cancel_join_thread` + `join_timeout` 수정은 유지
3. 0.1.0 태그 재발행 (streaming put 제거 후)
4. Docker 이미지 재빌드 후 대규모 테스트 수행

### 4.3 Streaming Put을 안전하게 구현하려면

만약 향후 streaming put의 메모리 이점이 필요하다면:

```python
# 방법 1: 워커에서 sentinel을 직접 보내기
def _parallel_worker(chunk, step, state, is_last_chunk):
    for item in chunk:
        result = step.process(item, state, _direct_emit)
    # 마지막 청크의 마지막 워커에서만 sentinel 전송
    # → 문제: "마지막"을 판단하기 어려움

# 방법 2: Queue.join() 또는 feeder thread flush 대기
# → multiprocessing.Queue에는 feeder flush 대기 API가 없음

# 방법 3: SimpleQueue 사용 (put이 직접 pipe write, blocking)
# → pipe full 시 워커가 블로킹되어 처리량 저하

# 방법 4: Barrier + Event 동기화
# → 복잡도 증가, 실익 불분명
```

현재로서는 **bulk put이 안전하고 충분한 성능**을 제공하므로 되돌리는 것이 최선.

---

## 5. 교훈

1. **`multiprocessing.Queue.put()`은 비동기**: put() 반환 ≠ 데이터가 pipe에 도달
2. **cross-process 순서 보장 없음**: 서로 다른 프로세스의 put()은 pipe에서 순서가 섞일 수 있음
3. **`cancel_join_thread()`는 반드시 워커 프로세스에서 호출**: 부모에서만 호출하면 spawn 모드에서 효과 없음
4. **`ProcessPoolExecutor` 워커 종료는 feeder thread에 의존**: pipe가 가득 차면 워커가 종료 불가 → future 무한 대기
5. **대규모 데이터에서만 재현되는 버그**: pipe buffer 포화 조건이 필요하므로 소규모 테스트에서 발견 어려움

---

## 6. 관련 파일

- `task_pipeliner/producers.py`: ParallelProducer, _parallel_worker, _init_worker
- `task_pipeliner/engine.py`: 큐 생성, join_timeout, 파이프라인 라이프사이클
- `taxanomy-converter/pipelines/naver_news_prod.yaml`: 테스트 파이프라인 설정
- `taxanomy-converter/run.py`: faulthandler 추가 (디버깅용)
