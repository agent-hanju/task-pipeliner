# PRD: Task Pipeliner - Python Pipeline Execution Framework

## 목적

병렬 작업과 Map-Reduce 형태의 정의가 필요한 step을 지연 없이 구동시킬 수 있고,
동작을 파이프라인 형태로 관리 가능한 Python 구조 설계.

## 프로젝트 범위

**이 프로젝트는 실행 프레임워크만 책임진다.**

- 포함: Producer 클래스, 실행 엔진, Queue/Event 통신, CLI, config 로딩, stats/로깅 출력
- 제외: 필터 기준, dedup 알고리즘, 텍스트 정규화 등 도메인 비즈니스 로직

비즈니스 로직은 이 프레임워크를 패키지로 import하는 **별도 구현 프로젝트**의 책임이다.
구현 프로젝트는 step 클래스를 정의하고 등록하는 것 외에 실행 인프라를 직접 다루지 않는다.

---

## 배경 및 문제 정의

데이터 처리 파이프라인은 아래 두 종류의 step이 혼재한다:

- **Stateless step**: 문서 단위로 독립 처리 가능 (길이 필터, 품질 필터 등) → 병렬화 가능
- **Stateful step**: 전체 또는 이전 데이터에 의존 (중복 제거, 보일러플레이트 빈도 집계 등) → 순차 필요

기존 접근법의 문제:
- HuggingFace `datasets`: Arrow/파일 캐시 의존, stateful step 병렬화 불가, 메모리 전체 로딩
- Apache Beam: 추상화 비용 과대, 글로벌 stateful dedup 표현 어려움
- datatrove: 파일 기반 중간 통신(단일 머신에서 순수 오버헤드), Slurm/클러스터 환경 종속

---

## 설계 목표

1. **균일한 step 인터페이스**: 필터, 변환, dedup, 집계가 모두 동일한 인터페이스로 표현됨
2. **선언적 파이프라인**: YAML config로 파이프라인 구성, step을 넣고 빼기 쉬울 것
3. **병렬성 극대화**: stateless step은 자동 병렬, stateful step은 자동 순차 — 실행 엔진이 판단
4. **메모리 효율**: 전체 데이터를 메모리에 올리지 않는 streaming 구조 (generator chain)
5. **크로스플랫폼**: Windows(spawn) + Linux 모두 정상 동작
6. **백프레셔 및 에러 전파 내장**: 구현자가 별도로 처리할 필요 없음
7. **Graceful shutdown**: 중단 시에도 그 시점까지의 결과가 보존되도록 설계

---

## 핵심 설계

### Worker 타입

두 타입 모두 state + Event 조합을 동일하게 사용한다.
차이는 처리 방식(병렬/순차)뿐이다.

```
ParallelProducer:
  - state 확인 (nullable, read-only)
    → null이면 Event.wait() (CPU 소모 없이 block)
    → Event.set() 수신 시 state 읽어서 처리 시작
  - chunk_size만큼 읽어 executor.submit(worker_fn, chunk)
  - 워커는 처리 후 결과를 output_queue에 넣고 자연 종료
  - input_queue 소진 시 모든 future 완료 대기 후 output_queue에 sentinel 전송

SequentialProducer:
  - state 확인 (nullable, read-only) — ParallelProducer와 동일
    → null이면 Event.wait()
    → Event.set() 수신 시 처리 시작
  - 워커 없이 혼자 순차 처리
  - 완료 시 output_queue에 sentinel 전송
    → 필요 시 다음 단계의 state를 채우고 Event.set()
```

각 step 동작 예시:

| Step | Worker 타입 | state |
|---|---|---|
| LengthFilter | ParallelProducer | null (즉시 시작) |
| QualityFilter | ParallelProducer | null (즉시 시작) |
| BoilerplateFilter (2nd pass) | ParallelProducer | line_counts (집계 완료 전까지 block) |
| ExactDedupFilter | SequentialProducer | - |
| MinHashDedupFilter (서명 계산) | ParallelProducer | null |
| MinHashDedupFilter (LSH 조회) | SequentialProducer | - |

### Boilerplate Fan-out 구조

문서 단위 필터링의 마지막 step이 통과 문서를 두 개의 queue로 동시에 전송:

```
[마지막 필터] ─┬→ Queue A (BoilerplateFilter 입력, line_counts null이면 block)
               └→ Queue B (라인 빈도 집계용 병렬 작업 입력)

Queue B → [집계 ParallelProducer] → [집계 SequentialProducer]
                                          │ 완료 시
                                          └→ BoilerplateFilter.state = line_counts

line_counts 확정 → Queue A 소비 시작 → [BoilerplateFilter ParallelProducer]
```

Queue A는 집계가 완료될 때까지 문서를 버퍼링한다.
Queue A가 가득 찰 경우 spill-to-disk로 메모리 부하를 제한한다 (선택적 구현).

### 실행 엔진

```
[Reader] → Queue → [ParallelProducer] → Queue → [SequentialProducer] → Queue → ... → [Writer]
```

- step 간 통신: 메모리 `multiprocessing.Queue` (파일 없음)
- 병렬 워커: `ProcessPoolExecutor` (spawn 방식, Windows/Linux 공통)
- `Queue(maxsize=N)`: 백프레셔 자동 처리
- sentinel 객체: 스트림 종료 및 에러 전파에 사용
- 모든 전달 객체는 pickle 가능해야 함 (lambda 사용 금지, `functools.partial` 또는 모듈 레벨 함수)

### 선언적 파이프라인 (YAML)

```yaml
pipeline:
  - type: jsonl_reader
    path: ./input/

  - type: length_filter
    min_length: 100

  - type: quality_filter

  - type: boilerplate_filter
    min_line_freq: 100

  - type: exact_dedup

  - type: minhash_dedup
    threshold: 0.8
    num_perm: 128

  - type: jsonl_writer
    path: ./output/

execution:
  workers: 8
  queue_size: 200
```

### 에러 처리 정책

ParallelProducer(worker_fn 내부)와 SequentialProducer(while 루프 내부) 모두 동일한 패턴:

- item 단위 try/except — 실패한 item은 skip, 파이프라인은 계속 실행
- 실패 발생 즉시 로그에 실시간 기록
- producer가 실패 건수를 집계하여 최종 stats에 명시

### Graceful Shutdown

중단(SIGINT, SIGTERM, 예외)이 발생하더라도 아래 항목은 반드시 실행되어야 한다:

- **로깅**: 실시간으로 파일에 flush. 프로세스 종료 후에도 그 시점까지의 로그 보존
- **stats/결과 파일**: 중단 시점까지의 집계를 저장 (signal handler 또는 finally 블록)
- **중단/재실행 복원은 지원하지 않음** (현재 프로젝트와 동등 수준): 중단 시 처음부터 재실행

---

## 테스트 전략

프레임워크 구조(Producer 클래스, 실행 엔진)를 먼저 구현하고 충분히 검증한 뒤,
효율성이 확인되면 실제 비즈니스 로직(필터, dedup 등)을 이식한다.

### 원칙

- **인프라와 로직 분리**: 프레임워크 테스트는 dummy step으로, 로직 테스트는 workers=1로
- **dummy step**: 아이템을 그대로 통과시키는 최소 구현체. 실제 처리 없이 구조만 검증
- **workers=1 모드**: 단일 프로세스로 실행, 결정적이고 디버깅 용이. 로직 테스트의 기준값 생성에 활용

### 프레임워크 테스트의 독립성

프레임워크 자체의 테스트는 **구현 프로젝트(필터, dedup 등 비즈니스 로직)의 정상 동작 여부와 완전히 독립적**이다.

- 프레임워크 테스트는 `tests/dummy_steps.py`에 정의된 최소 구현체만 사용한다:
  - `PassthroughStep`: 아이템을 그대로 통과
  - `ErrorOnItemStep`: 특정 조건에서 강제 예외 발생
  - `SlowStep`: `time.sleep()`으로 백프레셔/타이밍 시나리오 재현
  - `CountingAggStep`: partial Counter를 반환하는 집계 step (Fan-out 검증용)
- dummy step은 반드시 모듈 레벨에 정의한다 (spawn 방식 pickle 요건 충족)
- 프레임워크 테스트가 검증하는 것: **"데이터가 stage 사이를 올바르게 흐르는가"**
- 프레임워크 테스트가 검증하지 않는 것: "필터 기준이 올바른가", "dedup 해시가 정확한가" 등 도메인 로직

구현 프로젝트는 프레임워크를 패키지로 import한 뒤, 자체 `tests/`에서 비즈니스 로직만 독립적으로 검증한다.
두 테스트 suite는 서로를 전혀 알 필요가 없다.

### 프레임워크 구조 테스트 (dummy step 사용)

| 항목 | 검증 내용 |
|---|---|
| Queue 통신 | 아이템이 stage 간 정상 전달되는지 |
| Sentinel 전파 | 모든 stage가 sentinel을 수신하고 정상 종료하는지 (타임아웃으로 deadlock 감지) |
| Fan-out | 하나의 producer가 여러 queue에 동시 전송 시 모두 정상 수신하는지 |
| Event/state 알림 | state가 null인 producer가 Event.set() 후 정상 재개하는지 |
| 다중 state 대기 | event_A.wait() + event_B.wait() 순서 무관하게 정상 동작하는지 |
| 백프레셔 | bounded queue가 꽉 찼을 때 producer가 정상 block되는지 |
| 에러 처리 | 특정 아이템에서 예외 발생 시 skip + 집계되고 파이프라인이 계속 실행되는지 |
| Graceful shutdown | SIGINT 수신 시 stats/로그가 정상 저장되는지 |
| workers=N 결과 동등성 | workers=1과 workers=N의 처리 결과 건수가 동일한지 |

### 엣지 케이스 및 구현 프로젝트 사용 시나리오

프레임워크는 비즈니스 로직을 갖지 않지만, 구현 프로젝트가 사용할 수 있는 모든 패턴과 그 엣지 케이스를 프레임워크 수준에서 보장해야 한다.

**데이터 흐름**
- 빈 입력 (아이템 0개)
- 아이템 1개
- 정확히 chunk_size개
- 모든 아이템이 필터링되어 0개 통과
- 모든 아이템이 에러인 경우

**step 등록**
- 등록되지 않은 step 이름을 config에 명시 → 명확한 오류 메시지
- 같은 이름으로 중복 등록 시도
- pickle 불가능한 step 클래스 등록 시도 (spawn 실패 조기 감지)

**Fan-out / state**
- Queue A가 집계 완료 전에 꽉 차는 경우 (백프레셔 + spill 경계)
- 집계 step 자체가 실패하는 경우 → state가 영원히 set되지 않는 경우 (데드락 방지)
- 집계 결과가 비어있는 경우 (state가 빈 값으로 set됨)
- state가 매우 늦게 set되는 경우 (집계가 오래 걸리는 시나리오)
- 다중 state 중 하나만 set되고 나머지가 지연되는 경우

**에러 전파**
- sentinel 전송 직전/직후에 에러 발생
- 특정 아이템에서 반복적으로 에러 (에러율 100%)

**Graceful shutdown**
- 파이프라인 실행 중 SIGINT → stats/로그 저장 후 종료
- 집계 미완료 상태(state unset)에서 SIGINT

**출력 파일**
- 출력 디렉토리가 없는 경우 → 자동 생성
- 출력 파일이 이미 존재하는 경우 → 덮어쓰기 또는 오류 명시
- stats.json/pipeline.log 쓰기 실패 시 파이프라인 계속 여부

**CLI/config**
- 잘못된 YAML 형식
- 필수 필드 누락 (type 없는 step 등)
- `--input`/`--output` 수 불일치 (filter 모드)
- jobs.json 형식 오류 (batch 모드)

---

## 참고: datatrove에서 가져올 것 / 버릴 것

| 가져올 것 | 버릴 것 |
|---|---|
| generator chain 파이프라인 구조 | 파일 기반 중간 통신 |
| `run(data, rank, world_size)` 인터페이스 아이디어 | Slurm/Ray executor 종속 |
| stateful step을 별도 실행 단위로 분리하는 전략 | S3/분산 스토리지 의존 |
| exclusion_writer 패턴 (제거 문서 별도 저장) | completion marker 파일 |

---

## 실행 인터페이스 및 출력 파일 관리

### CLI

subcommand 방식으로 동작 유형을 명확히 구분:

```bash
# filter: n개 input → 1개 output
python run.py filter --config pipeline.yaml \
  --input news_2023/ news_2024/ --output output/news/ --workers 8

# batch: jobs 파일로 여러 job 일괄 예약 실행 (순차)
python run.py batch --config pipeline.yaml jobs.json

# batch 병렬 실행 (opt-in, 기본은 순차)
python run.py batch --config pipeline.yaml jobs.json --parallel
```

`jobs.json` 예시:
```json
[
  {"inputs": ["news_2023/", "news_2024/"], "output": "output/news/"},
  {"inputs": ["wiki/"], "output": "output/wiki/"}
]
```

`pipeline.yaml` — 파이프라인 구조만 정의:
```yaml
pipeline:
  - type: length_filter
    min_length: 100
  - type: exact_dedup
  - type: minhash_dedup
    threshold: 0.8

execution:
  workers: 8
  queue_size: 200
```

- config는 파이프라인 구조만, 입출력은 CLI 또는 jobs 파일로 분리
- step 단위 활성화/비활성화는 config에서 `enabled: false`로 처리
- batch는 기본 순차 실행 (각 job이 workers를 전부 사용), `--parallel`은 추후 opt-in

### 출력 파일 구조

```
output/
  kept.jsonl        # 최종 통과 문서
  removed.jsonl     # 제거된 문서 (제거 사유 포함)
  stats.json        # step별 통과/탈락/에러 건수, 소요 시간
  pipeline.log      # 실시간 로그 (실행 중 tail -f로 확인 가능)
  samples/
    kept.jsonl      # 통과 문서 샘플
    removed.jsonl   # 제거 문서 샘플 (사유별)
```

- `stats.json`: 병목 지점 파악 가능하도록 step별 소요 시간 포함
- `pipeline.log`: 실시간 flush, 에러 즉시 기록
- graceful shutdown 시에도 위 파일들은 그 시점까지의 내용으로 저장

---

## 비고 (이 프로젝트 적용 시 고려사항)

- 보일러플레이트 라인 정규화 방식 검토 (소문자화, 공백/구두점 제거)
- 검출된 보일러플레이트 라인 샘플링 출력 (빈도 상위 N개)
- 로깅: 각 step의 실행 상태, 문서 탈락 수, 병목 지점을 실시간으로 표시
