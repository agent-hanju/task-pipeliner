# WBS: task-pipeliner 프레임워크 리팩토링

> 이전 WBS: `docs/WBS-v1.md`
> 다음 WBS: `WBS.md`

---

## WBS 기반 개발 방법론

### 진행 원칙 (TDD)

각 작업 단위는 아래 순서를 **반드시** 지킨다. 어떤 단계도 건너뛰지 않는다.

1. **레퍼런스 탐색** — 코드 작성 전, 해당 단위에서 사용하는 라이브러리(예: Pydantic v2, orjson, click, multiprocessing spawn)의 웹 문서를 검색해 API 사용법이 설치된 버전과 일치하는지 확인한다. 비자명한 사항은 기록해 둔다.
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
Phase 1 — 프레임워크 코어 수정: base.py, producers.py, engine.py
Phase 2 — 프레임워크 부수 수정: pipeline.py, cli.py, io.py
Phase 3 — 프레임워크 테스트 수정
Phase 4 — 샘플 프로젝트(taxonomy-converter) 적용
```

각 Phase 내에서 독립적인 모듈은 병렬 작업이 가능하다.

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

> **참고: "사용자 프로젝트의 실행 스크립트"란?**
> 본 문서에서 "사용자 프로젝트의 실행 스크립트"는 task-pipeliner 프레임워크 내부의 코드가 아니라, task-pipeliner를 사용해 구축한 프로젝트(taxonomy-converter 등)가 자기 자신을 실행하기 위해 작성하는 Python 스크립트(예: `sample/taxonomy-converter/run.py`)를 의미한다. task-pipeliner 프레임워크 자체의 코드와 혼동하지 않는다.

---

## 리팩토링 배경

> `docs/refactoring-background.md` 참조.

---

## 수정 계획

### 의존 관계 맵

```
[W-R01 base.py] ──▶ [W-R02 producers.py] ──▶ [W-R03 engine.py]
                                                      │
                              ┌────────────────────────┤
                              ▼                        ▼
                    [W-R04 pipeline.py]       [W-R05 cli.py]
                              │                        │
                              └────────┬───────────────┘
                                       ▼
                              [W-R06 io.py]
                                       │
                                       ▼
                              [W-R07 프레임워크 테스트]
                                       │
                                       ▼
                              [W-R08 taxonomy-converter 적용]
```

---

## Phase 1 — 프레임워크 코어 수정

### - [x] W-R01 `base.py` — StepType.SOURCE + items()

**파일**: `src/task_pipeliner/base.py`
**의존**: 없음

- [x] `StepType.SOURCE = "source"` 추가
- [x] `BaseStep.items()` 메서드 추가 (기본 구현: `raise NotImplementedError`)
- [x] `BaseStep.close()` 메서드 추가 (기본 구현: no-op) — 리소스 정리용
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

```python
class StepType(Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    SOURCE = "source"

class BaseStep(ABC, Generic[R]):
    # 기존 process() 유지

    def items(self) -> Generator[Any, None, None]:
        """SOURCE 스텝 전용. 아이템을 yield한다."""
        raise NotImplementedError("items() must be implemented by SOURCE steps")

    def close(self) -> None:
        """리소스 정리. Producer가 종료 시 호출한다."""
        pass
```

---

### - [x] W-R02 `producers.py` — InputProducer가 step.items() 사용

**파일**: `src/task_pipeliner/producers.py`
**의존**: W-R01

- [x] `InputProducer.__init__`에서 `input_items` 대신 `step: BaseStep` 수용
- [x] `InputProducer.run()`에서 `step.items()`를 호출하여 아이템 생성
- [x] `step.close()` 호출 추가 (finally 블록)
- [x] SequentialProducer, ParallelProducer에서도 `step.close()` 호출 추가
- [x] stats 연동: `step.name`으로 passed 카운트
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

```python
class InputProducer:
    def __init__(self, *, step, output_queues, stats):
        self._step = step
        self._output_queues = output_queues
        self._stats = stats

    def run(self):
        try:
            for item in self._step.items():
                for q in self._output_queues:
                    q.put(item)
                self._stats.increment(self._step.name, "passed")
        finally:
            self._step.close()
            sentinel = Sentinel()
            for q in self._output_queues:
                q.put(sentinel)
            self._stats.finish(self._step.name)
```

---

### - [x] W-R03 `engine.py` — input_items 제거, SOURCE 스텝 감지

**파일**: `src/task_pipeliner/engine.py`
**의존**: W-R02

- [x] `run()` 시그니처에서 `input_items` 제거
- [x] drain loop 제거 (writer.write_kept 호출 삭제)
- [x] `JsonlWriter` import 제거
- [x] config의 첫 번째 스텝이 SOURCE인지 감지 → `InputProducer` 생성
- [x] 마지막 스텝은 `output_queues=[]` (출력은 스텝 내부에서 처리)
- [x] 큐 개수: N-1개 (N steps, 첫 번째가 SOURCE)
- [x] SOURCE가 첫 번째가 아닌 경우 에러 발생
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

```python
def run(self, *, output_dir: Path) -> None:
    # config에서 스텝 인스턴스화
    # 첫 번째 스텝이 SOURCE인지 확인
    # N-1개 큐 생성
    # SOURCE → InputProducer, PARALLEL/SEQUENTIAL → 기존 Producer
    # 마지막 스텝: output_queues=[]
    # result 수거 + stats 기록은 유지
```

큐 체인 구조:

```
Step0(SOURCE) → Q0 → Step1 → Q1 → ... → StepN-1 (output_queues=[])
큐 N-1개
```

---

## Phase 2 — 프레임워크 부수 수정

### - [x] W-R04 `pipeline.py` — Pipeline 파사드 수정

**파일**: `src/task_pipeliner/pipeline.py`
**의존**: W-R03

- [x] `Pipeline.run()`에서 `inputs` 파라미터를 유지하되, config에 JSONL SOURCE 스텝 주입 후 engine 호출
- [x] `JsonlReader` 직접 호출 제거
- [x] engine.run()에 `input_items` 전달하지 않음
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

---

### - [x] W-R05 `cli.py` — 커맨드 정리

**파일**: `src/task_pipeliner/cli.py`
**의존**: W-R04

- [x] `filter` → `run`으로 커맨드명 변경
- [x] `--input`, `--output`을 config SOURCE/출력 스텝 파라미터에 주입
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

---

### - [x] W-R06 `io.py` — JsonlWriter 정리

**파일**: `src/task_pipeliner/io.py`
**의존**: W-R03 (engine에서 import 제거 확인)

- [x] `write_kept()` / `write_removed()` → 범용화 또는 사용자 구현으로 이동
- [x] `kept.jsonl` / `removed.jsonl` 하드코딩 제거
- [x] JSONL SOURCE 스텝 클래스 추가 (Pipeline 파사드용)
- [x] 기존 테스트 통과 확인
- [x] ruff / mypy 통과

---

## Phase 3 — 테스트 수정

### - [x] W-R07 프레임워크 테스트 전체 수정

**파일**: `tests/` 전체
**의존**: W-R01 ~ W-R06

- [x] `tests/dummy_steps.py`에 DummySourceStep 추가 (SOURCE 스텝 테스트용)
- [x] `test_engine.py` — engine.run()에서 input_items 제거에 맞게 수정
- [x] `test_cli.py` — `filter` → `run` 커맨드명 변경 반영
- [x] `test_io.py` — JsonlWriter 인터페이스 변경 반영
- [x] `_run_pipeline.py` — writer 제거 반영
- [x] `test_init.py` — 공개 API 변경 반영
- [x] 전체 pytest 통과
- [x] ruff / mypy 통과

---

## Phase 4 — 샘플 프로젝트 적용

### - [x] W-R08 taxonomy-converter 적용

**파일**: `sample/taxonomy-converter/` 전체
**의존**: W-R07

- [x] `pipeline_config.yaml` — 전체 파이프라인 선언 (loader → preprocess → convert → deduplicate → writer)

```yaml
pipeline:
  - type: loader
    paths:
      - ./data
  - type: preprocess
    min_lines: 2
    min_chars: 100
  - type: convert
    dataset_name: naver_econ_news
    source: HanaTI/NaverNewsEconomy
    language: korean
    category: hass
    industrial_field: finance
    template: article
  - type: deduplicate
  - type: writer
    dir: ./output
    format: jsonl

execution:
  workers: 4
  queue_size: 200
  chunk_size: 100
```

- [x] `steps.py` — LoaderStep 추가 (SOURCE 스텝, load_items 래핑)

```python
class LoaderStep(BaseStep[TaxonomyResult]):
    step_type = StepType.SOURCE

    def __init__(self, paths: list[str] | None = None):
        self._paths = [Path(p) for p in paths] if paths else []

    def items(self):
        yield from load_items(self._paths)

    def process(self, item, state, emit):
        raise NotImplementedError("SOURCE step")
```

- [x] `steps.py` — PreprocessStep, ConvertStep에 `__init__` 파라미터 추가 (config 주입)

```python
class PreprocessStep(BaseStep[TaxonomyResult]):
    def __init__(self, min_lines: int = 2, min_chars: int = 100):
        self._min_lines = min_lines
        self._min_chars = min_chars

class ConvertStep(BaseStep[TaxonomyResult]):
    def __init__(self, dataset_name: str = "naver_econ_news",
                 source: str = "HanaTI/NaverNewsEconomy", ...):
        self._dataset_name = dataset_name
        self._source = source
        ...
```

- [x] `steps.py` — WriterStep이 config에서 dir 파라미터 수용

- [x] 사용자 프로젝트의 실행 스크립트 (`run.py`) 정리: register + load_config + CLI override만 수행. pipeline.append 제거.

```python
def main(input_paths, output_dir, config_path=None):
    cfg = load_config(config_path or _DEFAULT_CONFIG)

    # CLI override (필요한 경우만)
    for i, step_cfg in enumerate(cfg.pipeline):
        if step_cfg.type == "loader" and input_paths:
            cfg.pipeline[i] = StepConfig(type="loader", paths=[str(p) for p in input_paths])
        elif step_cfg.type == "writer" and output_dir:
            cfg.pipeline[i] = StepConfig(type="writer", dir=str(output_dir))

    registry = StepRegistry()
    registry.register("loader", LoaderStep)
    registry.register("preprocess", PreprocessStep)
    registry.register("convert", ConvertStep)
    registry.register("deduplicate", DeduplicateStep)
    registry.register("writer", WriterStep)

    stats = StatsCollector()
    stats.setup_log_handler(output_dir / "pipeline.log")
    engine = PipelineEngine(config=cfg, registry=registry, stats=stats)
    engine.run(output_dir=output_dir)
```

- [x] 샘플 테스트 전체 통과
- [x] ruff 통과

---

## 수정 대상 요약

| 우선순위 | 항목 | 변경 내용 |
|---------|------|----------|
| **P0** | `base.py` | `StepType.SOURCE` 추가, `items()`, `close()` 메서드 추가 |
| **P0** | `producers.py` | `InputProducer`가 `step.items()` 사용, `step.close()` 호출 |
| **P0** | `engine.py` | `input_items` 제거, drain loop 제거, SOURCE 스텝 감지 |
| **P1** | `pipeline.py` | config에 SOURCE 스텝 주입 후 engine 호출 |
| **P1** | `cli.py` | `filter` → `run`, config SOURCE 스텝에 경로 주입 |
| **P2** | `io.py` | kept/removed 제거, JSONL SOURCE 스텝 추가 |
| **P2** | 테스트 | DummySourceStep 추가, engine/cli/io 테스트 수정 |
| **P3** | taxonomy-converter | config 완전 선언, 스텝 설정 주입, run.py 정리 |

### 수정하지 않는 항목

| 항목 | 이유 |
|------|------|
| `engine.run(output_dir=...)` | result.write()와 stats.write_json()은 실행 기록이지 파이프라인 출력이 아님 |
| `StatsCollector.write_json()` | 실행 통계 기록은 엔진의 정당한 책임 |
| `BaseResult.write()` | 각 스텝의 자체 실행 결과 요약. 파이프라인 데이터 출력과 무관 |
| `JsonlReader` 존재 자체 | 유틸리티로 제공되는 것은 문제없음. engine이 강제하지만 않으면 됨 |
