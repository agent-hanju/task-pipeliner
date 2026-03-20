# WBS: Tagged Output — 파이프라인 분기 지원

> 이전 WBS: `docs/WBS-v2.md`

---

## WBS 기반 개발 방법론

### 진행 원칙 (TDD)

각 작업 단위는 아래 순서를 **반드시** 지킨다. 어떤 단계도 건너뛰지 않는다.

1. **레퍼런스 탐색** — 코드 작성 전, 해당 단위에서 사용하는 라이브러리(예: Pydantic v2, orjson, click, multiprocessing spawn)의 웹 문서를 검색해 API 사용법이 설치된 버전과 일치하는지 확인한다. 또한 비슷한 기능, 패턴을 동일한 기술 스택 기반으로 검색하여 검증된 프로그래밍 패턴을 파악하고 참고한다. 비자명한 사항은 기록해 둔다.
2. **인터페이스 확인** — 본 WBS의 인터페이스 스펙을 확인한다.
3. **테스트 작성** — 테스트를 먼저 작성한다. 실행하여 실패(red)를 확인한다.
4. **구현** — 테스트를 통과시키기 위한 최소한의 코드를 작성한다.
5. **테스트 통과 확인** — pytest를 실행한다. 모든 테스트가 green이어야 한다.
6. **린트 & 타입 검사** — ruff와 mypy를 실행한다. 모든 오류를 해결한 뒤 다음으로 넘어간다.
7. **WBS 업데이트** — 본 문서에서 완료된 세부 단계를 체크한다.

각 작업 단위는 이전 단계의 테스트가 전부 통과한 뒤 진행한다.
테스트 실행 시 항상 `--timeout=30` 플래그를 사용해 데드락을 감지한다.

```bash
# 5단계: 테스트
.venv/Scripts/pytest --timeout=30 -v

# 6단계: 린트 & 타입
.venv/Scripts/ruff check src tests
.venv/Scripts/ruff format --check src tests
.venv/Scripts/mypy src
```

### 개발 단계 (Phase)

```
Phase 1 — 코어 인터페이스: base.py (outputs 선언, emit 시그니처 변경)
Phase 2 — 큐 라우팅: producers.py, engine.py (태그별 큐 생성·라우팅)
Phase 3 — 설정 스키마: config.py (outputs 매핑 지원)
Phase 4 — 파사드·CLI: pipeline.py, cli.py (변경 전파)
Phase 5 — 테스트 수정
Phase 6 — 샘플 프로젝트(taxonomy-converter) 적용
```

### WBS 체크박스 관리

진행 상황은 체크박스로 추적한다. 각 작업 단위에는 **세부 체크리스트**가 있다.

- **세부 단계 체크**: 해당 단계가 완료되면 체크 (예: `[x] 테스트 통과`)
- **최상위 박스 체크**: **모든** 세부 단계가 체크된 경우에만
- **작업 시작 전**: 전제 작업 단위의 최상위 박스가 모두 체크됐는지 확인 후 착수
- 구현만 완료된 상태에서 체크 금지 — 테스트 + 린트 + 타입 모두 통과해야 함

체크 전 실행할 검증 명령어:

```bash
.venv/Scripts/pytest --timeout=30 -v        # 전체 green 확인
.venv/Scripts/ruff check src tests           # 린트 오류 없음
.venv/Scripts/ruff format --check src tests  # 포맷 OK
.venv/Scripts/mypy src                       # 타입 오류 없음
```

---

## 리팩토링 배경

### 현재 상태

현재 파이프라인은 **선형 큐 체인**만 지원한다:

```
SOURCE → Q0 → Step1 → Q1 → Step2 → ... → StepN (output_queues=[])
```

- `emit(item)` 호출 시 다음 스텝의 큐에만 전달
- 스텝이 아이템을 건너뛰면(emit 미호출) 해당 아이템은 소실
- 특정 스텝의 큐로 직접 보내는 방법 없음
- 출력이 없는 terminal 스텝(WriterStep 등)도 `emit` 콜백을 받지만 사용하지 않음

### 목표

**Tagged Output** 패턴을 도입하여:

1. 스텝이 여러 이름의 출력으로 아이템을 분기할 수 있게 한다
2. 출력이 없는 terminal 스텝을 자연스럽게 표현한다

```
PreprocessStep ──kept──→ ConvertStep
               └─removed──→ WriterStep (terminal, outputs 없음)
```

### 설계 원칙

1. **`outputs`를 선언한 스텝만 emit 가능** — `outputs = ()`이면 emit 불가 (terminal step)
2. **emit 시 태그 필수** — `emit(item, "kept")`. 기본값 없음
3. **Config가 토폴로지를 결정** — 어떤 output tag가 어떤 스텝의 입력 큐로 가는지 선언
4. **하나의 출력에 여러 downstream 가능** — fan-out (`kept: [convert, audit_logger]`)
5. **미연결 출력은 버린다** — config에 연결 안 된 output tag로 emit하면 무시

### 레퍼런스

| 프레임워크 | 패턴 | 핵심 API |
|---|---|---|
| Apache Beam | Tagged Side Output | `yield TaggedOutput(tag, item)` |
| Apache Flink | OutputTag | `ctx.output(tag, item)` |
| Dagster | Named Output | `yield Output(item, output_name=tag)` |
| Kafka Streams | Predicate Split | `split().branch(predicate, name)` |

→ **Tagged Emit** (Beam/Flink/Dagster 방식) 채택. 스텝 내부에서 태그 지정, 파이프라인 config에서 연결.

---

## 의존 관계 맵

```
[W-T01 base.py] ──▶ [W-T02 producers.py] ──▶ [W-T03 engine.py]
                                                     │
                            ┌────────────────────────┤
                            ▼                        ▼
                   [W-T04 config.py]        [W-T05 pipeline.py + cli.py]
                            │                        │
                            └────────┬───────────────┘
                                     ▼
                            [W-T06 프레임워크 테스트]
                                     │
                                     ▼
                            [W-T07 taxonomy-converter 적용]
```

---

## Phase 1 — 코어 인터페이스

### - [x] W-T01 `base.py` — outputs 선언 + emit 시그니처 변경

**파일**: `src/task_pipeliner/base.py`
**의존**: 없음

- [x] `BaseStep.outputs` 클래스 변수 추가 (기본값: `()` — 빈 튜플, emit 불가)
- [x] `emit` 콜백 타입을 `Callable[[Any, str], None]`으로 변경 (tag 필수, 기본값 없음)
- [x] `process()` 시그니처에서 `emit`은 유지하되, `outputs = ()`인 스텝에서 호출 시 에러
- [x] `process()` 독스트링에 `emit(item, "tag")` 사용법 명시
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

```python
class BaseStep(ABC, Generic[R]):
    outputs: ClassVar[tuple[str, ...]] = ()  # 빈 튜플 = terminal step

    @abstractmethod
    def process(
        self,
        item: Any,
        state: Any,
        emit: Callable[[Any, str], None],
    ) -> R:
        """Process single item.

        emit(item, "kept") → "kept" 출력으로 전달
        emit(item, "removed") → "removed" 출력으로 전달

        outputs = ()인 스텝에서 emit을 호출하면 RuntimeError.
        선언하지 않은 tag로 emit하면 config에 연결이 없으면 무시.
        """
        ...
```

> **기존 코드와의 차이**: 기존 `emit(item)`은 인자 1개, 새 `emit(item, tag)`는 인자 2개.
> 기존 `emit(item)` 호출은 모두 `emit(item, "tag_name")`으로 변경해야 한다.
> `outputs = ()`인 스텝(WriterStep 등)은 `emit`을 호출하지 않으므로 변경 불필요.

---

## Phase 2 — 큐 라우팅

### - [x] W-T02 `producers.py` — 태그별 큐 라우팅

**파일**: `src/task_pipeliner/producers.py`
**의존**: W-T01

- [x] `output_queues` 타입을 `list[Queue]` → `dict[str, list[Queue]]`로 변경 (tag → queues)
- [x] `_make_emit()` 수정:
  - `outputs = ()`인 스텝: emit 호출 시 `RuntimeError` raise
  - 미연결 tag (config에 없음): 무시 (로그 DEBUG)
  - 연결된 tag: 해당 큐들에 put
- [x] `InputProducer`의 `output_queues`도 `dict[str, list[Queue]]`로 변경
- [x] `_send_sentinel()` — 모든 tag의 모든 큐에 sentinel 전송
- [x] `_parallel_worker`의 emit 버퍼도 태그 지원 (`list[tuple[Any, str]]`)
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

```python
class BaseProducer(ABC, multiprocessing.Process):
    def __init__(
        self,
        *,
        step: BaseStep[Any],
        input_queue: multiprocessing.Queue[Any],
        output_queues: dict[str, list[multiprocessing.Queue[Any]]],  # tag → [queues]
        ...
    ) -> None: ...

    def _make_emit(self) -> Callable[[Any, str], None]:
        queues_by_tag = self.output_queues
        step_name = self.step.name
        step_outputs = self.step.outputs
        stats = self.stats

        def emit(item: Any, tag: str) -> None:
            if not step_outputs:
                raise RuntimeError(
                    f"Step '{step_name}' has no declared outputs — emit() not allowed"
                )
            tag_queues = queues_by_tag.get(tag)
            if tag_queues is None:
                logger.debug("unconnected tag=%s step=%s — dropped", tag, step_name)
                return
            for q in tag_queues:
                q.put(item)
            stats.increment(step_name, "passed")

        return emit

    def _send_sentinel(self) -> None:
        sentinel = Sentinel()
        seen: set[int] = set()  # 중복 큐 방지 (fan-in으로 같은 큐가 여러 tag에 등장할 수 있음)
        for tag_queues in self.output_queues.values():
            for q in tag_queues:
                qid = id(q)
                if qid not in seen:
                    q.put(sentinel)
                    seen.add(qid)
```

> **주의**: `_parallel_worker`는 module-level 함수이므로 `output_queues` dict를 global로 전달하는 방식도 변경 필요.
> worker 내부에서는 emit 버퍼가 `list[tuple[Any, str]]`이 되고, chunk 완료 후 tag별로 큐에 bulk put.

---

### - [x] W-T03 `engine.py` — DAG 큐 빌더

**파일**: `src/task_pipeliner/engine.py`
**의존**: W-T02

- [x] 선형 큐 체인 제거 → config의 `outputs` 매핑 기반 큐 생성
- [x] 큐 생성 로직: config의 outputs 매핑을 순회하며 `(source_step, tag) → target_step` 연결마다 큐 생성
- [x] `outputs` 미선언 스텝 (`outputs = ()`) → 출력 큐 없음 (terminal)
- [x] `outputs`를 선언했지만 config에 매핑이 없는 tag → 큐 미생성 (emit 시 silent drop)
- [x] 복수 입력 큐: 하나의 스텝이 여러 upstream에서 아이템을 받을 수 있음
- [x] 복수 입력 큐 소비 전략: 모든 입력 큐에서 sentinel이 도착해야 스텝 종료
- [x] SOURCE 스텝 검증 로직 유지
- [x] sentinel 주입 (graceful shutdown) — 모든 큐에 sentinel
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

큐 구조 (예시):

```
LoaderStep(SOURCE)
  outputs: ("main",)
  config outputs: { main: preprocess }
  → Q_preprocess 생성

PreprocessStep
  outputs: ("kept", "removed")
  config outputs: { kept: convert, removed: writer }
  input: Q_preprocess
  → Q_convert, Q_writer_removed 생성

ConvertStep
  outputs: ("kept",)
  config outputs: { kept: deduplicate }
  input: Q_convert
  → Q_dedup 생성

DeduplicateStep
  outputs: ("kept",)
  config outputs: { kept: writer }
  input: Q_dedup
  → Q_writer_kept 생성

WriterStep
  outputs: ()  ← terminal
  inputs: [Q_writer_kept, Q_writer_removed]  ← 복수 입력 큐
```

> **복수 입력 큐 소비**: 하나의 스텝이 여러 upstream에서 아이템을 받을 수 있다.
> 각 입력 큐를 별도 스레드로 소비하거나, 단일 스레드에서 select 방식으로 소비.
> 모든 입력 큐에서 sentinel이 도착해야 해당 스텝이 종료된다.

---

## Phase 3 — 설정 스키마

### - [x] W-T04 `config.py` — outputs 매핑 스키마

**파일**: `src/task_pipeliner/config.py`
**의존**: W-T01

- [x] `StepConfig`에 `outputs` 필드 추가: `dict[str, str | list[str]] | None`
- [x] `outputs`가 `None`이고 스텝의 `outputs`가 비어있으면 정상 (terminal step)
- [x] `outputs`가 `None`이고 스텝의 `outputs`가 비어있지 않으면 경고 (연결 없는 출력)
- [x] `str` 값이면 단일 downstream, `list[str]` 값이면 fan-out
- [x] config validation: outputs에 참조된 스텝 type이 pipeline에 존재하는지 검증
- [x] `outputs` 필드는 `model_extra`가 아닌 명시적 필드로 선언 (extra에 빠지지 않도록)
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

```python
class StepConfig(_WrappingModel):
    model_config = ConfigDict(extra="allow")

    type: str
    enabled: bool = True
    outputs: dict[str, str | list[str]] | None = None
    # 나머지 extra 필드 → step.__init__ kwargs로 전달
```

config 예시:

```yaml
pipeline:
  - type: loader
    paths: [./data]
    outputs:
      main: preprocess
  - type: preprocess
    min_lines: 2
    outputs:
      kept: convert
      removed: writer
  - type: convert
    outputs:
      kept: deduplicate
  - type: deduplicate
    outputs:
      kept: writer
  - type: writer
    dir: ./output
    # outputs 없음 = terminal step
```

---

## Phase 4 — 파사드·CLI

### - [x] W-T05 `pipeline.py` + `cli.py` — 변경 전파

**파일**: `src/task_pipeliner/pipeline.py`, `src/task_pipeliner/cli.py`
**의존**: W-T03, W-T04

- [x] `Pipeline.run()`에서 `JsonlSourceStep` 주입 시 `outputs` 설정 반영
- [x] CLI 커맨드에 영향 없음 확인 (config가 모든 라우팅을 담당)
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

---

## Phase 5 — 테스트 수정

### - [x] W-T06 프레임워크 테스트 전체 수정

**파일**: `tests/` 전체
**의존**: W-T01 ~ W-T05

#### 신규 테스트

- [x] `test_schema.py` — `BaseStep.outputs` 기본값 `()` 검증
- [x] `test_queue.py` — 태그별 emit 라우팅 테스트
  - `emit(item, "kept")` → kept 큐에 전달
  - `emit(item, "removed")` → removed 큐에 전달
  - `emit(item, "unknown")` → config에 없으면 무시 (큐에 아무것도 안 감)
  - `outputs = ()`인 스텝에서 `emit()` 호출 → `RuntimeError`
  - fan-out: 하나의 tag에 복수 큐 연결 → 양쪽 모두 수신
- [x] `test_engine.py` — DAG 큐 빌드 테스트
  - outputs 매핑이 있는 config → 올바른 큐 연결 검증
  - terminal 스텝 (outputs 없음) → 출력 큐 없이 정상 동작
  - 미연결 출력 tag → 아이템 소실 확인 (에러 아님)
  - 복수 입력 큐 → 모든 sentinel 도착 후 종료 확인
- [x] `test_config.py` — outputs 스키마 검증
  - `outputs: null` + terminal step → 정상
  - `outputs: {kept: convert}` → 정상 파싱
  - `outputs: {kept: [convert, logger]}` → fan-out 파싱
  - 존재하지 않는 스텝 참조 → validation error

#### 기존 테스트 수정

- [x] `tests/dummy_steps.py` 수정:
  - 기존 DummyStep들에 `outputs` 선언 추가 (emit을 호출하는 스텝)
  - `outputs = ()`인 terminal DummyStep 추가
  - `emit(item)` → `emit(item, "tag")` 호출로 변경
- [x] 기존 테스트에서 `emit(item)` → `emit(item, "tag")` 호출 변경 반영
- [x] config에 `outputs` 매핑 추가
- [x] 전체 pytest 통과
- [x] ruff / mypy 통과

---

## Phase 6 — 샘플 프로젝트 적용

### - [x] W-T07 taxonomy-converter 적용

**파일**: `sample/taxonomy-converter/` 전체
**의존**: W-T06

- [x] `steps.py` — 각 스텝에 `outputs` 선언:
  - `LoaderStep.outputs = ("main",)` (SOURCE)
  - `PreprocessStep.outputs = ("kept", "removed")`
  - `ConvertStep.outputs = ("kept",)`
  - `DeduplicateStep.outputs = ("kept",)`
  - `WriterStep` — outputs 없음 (terminal)
- [x] `steps.py` — emit 호출 변경:
  - `PreprocessStep`: `emit(item)` → `emit(item, "kept")`, skip 시 `emit(item, "removed")`
  - `ConvertStep`: `emit(taxonomy_dict)` → `emit(taxonomy_dict, "kept")`
  - `DeduplicateStep`: `emit(item)` → `emit(item, "kept")`
  - `WriterStep`: emit 호출 제거 (이미 없음)
- [x] `steps.py` — `WriterStep`이 복수 입력(kept + removed)을 받아 파일 분리
- [x] `pipeline_config.yaml` — outputs 매핑 추가

```yaml
pipeline:
  - type: loader
    paths: [./data]
    outputs:
      main: preprocess
  - type: preprocess
    min_lines: 2
    min_chars: 100
    outputs:
      kept: convert
      removed: writer
  - type: convert
    dataset_name: naver_econ_news
    source: HanaTI/NaverNewsEconomy
    language: korean
    category: hass
    industrial_field: finance
    template: article
    outputs:
      kept: deduplicate
  - type: deduplicate
    outputs:
      kept: writer
  - type: writer
    dir: ./output
    format: jsonl

execution:
  workers: 4
  queue_size: 200
  chunk_size: 100
```

- [x] `run.py` — CLI override에서 outputs 보존 확인
- [x] 샘플 테스트에서 `kept.jsonl` + `removed.jsonl` 모두 생성 검증
- [x] ruff 통과

---

## 수정 대상 요약

| 우선순위 | 항목 | 변경 내용 |
|---------|------|----------|
| **P0** | `base.py` | `outputs: ClassVar[tuple[str, ...]] = ()`, `emit(item, tag)` 시그니처 (기본값 없음) |
| **P0** | `producers.py` | `output_queues: dict[str, list[Queue]]`, 태그별 라우팅, terminal 스텝 emit 차단 |
| **P0** | `engine.py` | DAG 큐 빌더, 복수 입력 큐 소비, config outputs 기반 연결 |
| **P1** | `config.py` | `StepConfig.outputs` 명시적 필드, 참조 검증 |
| **P1** | `pipeline.py` | SOURCE 주입 시 outputs 반영 |
| **P2** | 테스트 | 전면 수정: emit 시그니처, outputs 선언, config outputs 매핑 |
| **P3** | taxonomy-converter | PreprocessStep removed 출력, WriterStep terminal, config outputs |

### 수정하지 않는 항목

| 항목 | 이유 |
|------|------|
| `stats.py` | 기존 `passed`/`errored`/`filtered` 카운터 구조 유지. 태그별 카운트는 향후 별도 작업 |
| `io.py` | `JsonlReader`/`JsonlWriter`는 유틸리티 — 라우팅과 무관 |
| `BaseResult` / `BaseAggStep` | 결과 집계 구조는 변경 없음 |
| `exceptions.py` | 기존 예외 클래스 충분 |

### 주요 설계 결정

| 결정 | 선택 | 근거 |
|------|------|------|
| `outputs` 기본값 | `()` (빈 튜플) | terminal step이 자연스러움. WriterStep, DB 저장 등은 emit 불필요 |
| emit 태그 | 필수 (기본값 없음) | 사용자가 없으므로 하위호환 불필요. 명시적이 더 명확 |
| `outputs = ()` + emit 호출 | `RuntimeError` | 선언과 동작의 불일치를 빠르게 감지 |
| 미연결 태그 처리 | 무시 (silent drop) | 에러보다 유연. config에서 필요한 것만 연결 |
| 복수 입력 큐 | 스텝당 하나의 consumer가 여러 큐를 소비 | fan-in 자연스럽게 지원 |
| outputs 선언 위치 | 클래스 변수 (`ClassVar`) | 인스턴스 아닌 타입 레벨 계약 |
