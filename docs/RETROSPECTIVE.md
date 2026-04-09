# 회고: 처음부터 어떻게 접근했으면 좋았을지

> task-pipeliner 개발 과정에서 발견된 설계 실수, 안티패턴, 그리고 그로부터 얻은 교훈을 정리한다.
> 작성일: 2026-04-09

---

## 1. 핵심 교훈 요약

**처음부터 옳았으면 좋았을 네 가지:**

1. **SourceStep을 처음부터 설계한다** — 입력은 파이프라인 밖이 아니라 안에 있어야 한다.
2. **config가 파이프라인 전체를 선언한다** — 실행 스크립트는 경로와 파라미터만 전달한다.
3. **Step 계층을 명확히 분리한다** — SourceStep / SequentialStep / ParallelStep / Worker는 처음부터 별도 ABC여야 한다.
4. **Queue는 maxsize=0으로 시작한다** — 백프레셔를 먼저 넣으면 팬아웃 그래프에서 데드락이 생긴다.

---

## 2. 설계 진화: WBS-v1 → 현재

### v1 설계 (초기 오류)

```
[exceptions] → [base] → [producers] → [engine] → [pipeline] → [cli]
```

초기 `base.py`에는 단일 `BaseStep`만 있었고, `items()`를 가진 Source 개념이 없었다. 이 결정이 아래 안티패턴을 전부 낳았다.

초기 `engine.run()` 시그니처:
```python
def run(self, input_items: Iterable[Any], output_dir: Path) -> None:
    # 엔진이 직접 InputProducer에 데이터를 주입
```

### v2 설계 (SourceStep 도입)

taxonomy-converter 샘플 구축 중 안티패턴(A-1~A-3)이 발견되면서 SourceStep이 추가됐다.

```python
class SourceStep(StepBase, ABC):
    @abstractmethod
    def items(self) -> Generator[Any, None, None]: ...
```

`engine.run(input_items=...)` 파라미터 제거. 파이프라인의 첫 번째 스텝이 직접 데이터를 생성한다.

### 현재 설계 (Step 계층 완성)

```
StepBase
  ├─ SourceStep       # items() → InputStepRunner
  ├─ SequentialStep   # process() → SequentialStepRunner
  ├─ AsyncStep        # process_async() → AsyncStepRunner (2026-04-09 추가)
  └─ ParallelStep     # create_worker() → Worker → ParallelStepRunner
```

CLI는 프레임워크 본체에서 제거됐다. 실행 스크립트는 사용자가 직접 작성하거나 `Pipeline` 파사드를 호출한다.

---

## 3. 안티패턴과 원인 분석

### A-1. engine.run()에 input_items를 직접 전달

**코드:**
```python
# 실행 스크립트에서
input_items = load_items(input_paths)
engine.run(input_items=input_items, output_dir=output_dir)
```

**원인:** SourceStep 개념 부재. 첫 번째 스텝이 큐에서 데이터를 받는 구조만 가능하여, 데이터 생성 책임이 엔진 외부로 흘러나왔다.

**올바른 설계:**
```yaml
pipeline:
  - type: jsonl_source
    items: ["data/input.jsonl"]
  - type: process
```

파이프라인 config가 입력 소스까지 선언한다. 실행 스크립트는 config 경로만 전달한다.

---

### A-2. WriterStep을 코드로 pipeline에 append

**코드:**
```python
# 실행 스크립트에서
cfg.pipeline.append(StepConfig(type="write", output_dir=str(output_dir)))
```

**원인:** A-1과 같은 맥락. 파이프라인의 시작과 끝이 config가 아니라 코드로 조작됐다.

**올바른 설계:** WriterStep을 config에 선언한다. 출력 경로는 `output_dir` 파라미터로 주입한다.

---

### A-3. config가 파이프라인의 일부만 선언

**코드:**
```yaml
# 나쁜 예 — 입출력 없음
pipeline:
  - type: preprocess
  - type: convert
```

**원인:** A-1, A-2의 결과. SourceStep과 WriterStep이 없으니 config에 선언할 수 없었다.

**올바른 설계:**
```yaml
pipeline:
  - type: jsonl_source      # 입력
    items: ["${INPUT_PATH}"]
    outputs: {main: preprocess}
  - type: preprocess
    outputs: {main: convert}
  - type: convert
    outputs: {main: writer}
  - type: jsonl_writer      # 출력
    output_dir: "${OUTPUT_DIR}"
```

---

### A-4. 스텝 설정값이 코드에 하드코딩

**코드:**
```python
class ConvertStep(SequentialStep):
    # 도메인 상수가 코드에 박혀 있음
    metadata = make_metadata("HanaTI/NaverNewsEconomy", published_date)
    "dataset_name": "naver_econ_news"
```

**원인:** `StepConfig extra → cls(**extra)` 패턴이 확립되지 않았다. Config에서 생성자로 파라미터를 주입하는 경로가 없으니 코드에 박을 수밖에 없었다.

**올바른 설계:**
```yaml
- type: convert
  dataset_name: naver_econ_news
  source_repo: HanaTI/NaverNewsEconomy
```
```python
class ConvertStep(SequentialStep):
    def __init__(self, dataset_name: str, source_repo: str, **_kwargs: Any) -> None:
        self.dataset_name = dataset_name
        self.source_repo = source_repo
```

---

### A-5. Queue에 maxsize=N을 먼저 설정

**코드:**
```python
queue = Queue(maxsize=100)  # 메모리 바운드 방지 목적
```

**원인:** 단일 큐 구간에서는 백프레셔가 직관적으로 동작하지만, 팬아웃/팬인이 있는 멀티 스텝 그래프에서는 데드락이 발생한다. 업스트림이 꽉 찬 큐에 `put()`을 대기하는 동안, 다운스트림도 자신의 결과를 어딘가에 `put()`하려고 블로킹되는 순환 대기가 생긴다.

**올바른 설계:** `maxsize=0`(무제한)으로 먼저 파이프라인 정확성을 확보한다. OOM 방어는 `DiskSpillQueue` 같은 별도 메커니즘으로 분리한다.

```python
queue = Queue(maxsize=0)  # 무제한 — 데드락 없음. OOM은 DiskSpillQueue로 별도 처리.
```

---

### C-1. kept/removed 도메인 개념이 프레임워크에 내재

**코드:**
```python
# io.py — 프레임워크 내부
class JsonlWriter:
    def write_kept(self, item): ...
    def write_removed(self, item): ...
```

**원인:** pretrain-data-filter 샘플의 도메인 로직이 프레임워크로 유출됐다. "필터링 파이프라인"이라는 특정 유스케이스를 프레임워크가 가정했다.

**올바른 설계:** 프레임워크는 범용 스트림 처리기. kept/removed 분류는 사용자 도메인 코드(Step 내부)에서 처리. `io.py` 자체도 제거됐다.

---

### C-2. engine.py가 I/O를 직접 수행

초기 engine은 `result.write(output_dir)` (각 스텝의 결과 파일 기록)과 `input_items` 주입을 직접 담당했다.

**올바른 설계:** 엔진의 역할 = config 읽기 → 스텝 인스턴스화 → 큐 연결 → StepRunner 실행 → 완료 대기. 파일 I/O는 스텝이 담당하거나 파사드(Pipeline)가 정리한다.

---

### C-3. CLI 커맨드명이 도메인 워크플로우를 가정

**코드:**
```bash
task-pipeliner filter --input data/ --output out/   # 'filter'는 필터링 파이프라인 전용
task-pipeliner batch jobs.json                       # 'batch'는 특정 실행 패턴 가정
```

**원인:** 초기 CLI는 pretrain-data-filter 유스케이스 중심으로 설계됐다. `filter`라는 커맨드명이 다른 유스케이스(변환, 집계, 생성 등)에서 어색해진다.

**올바른 설계:** 범용 프레임워크는 CLI를 본체에 포함하지 않는다. 사용자가 실행 스크립트를 직접 작성하거나 `Pipeline` 파사드를 호출한다. 커맨드명이 "무슨 일을 하는지"를 암시하는 순간 프레임워크 범용성이 희생된다.

---

## 4. 처음부터 이렇게 했으면 좋았을 설계

### 체크리스트

```
□ SourceStep ABC 먼저 설계 (items() 추상 메서드)
□ StepConfig extra → cls(**extra) 패턴 문서화
□ config YAML에 입력 + 처리 + 출력 스텝 전부 선언 예시 작성
□ engine.run() 시그니처를 run() 로 고정 (input_items 없음)
□ Worker를 별도 ABC로 분리 (ParallelStep ≠ Worker)
□ Queue는 maxsize=0으로 시작 (OOM 방어는 DiskSpillQueue로 분리)
□ 프레임워크 순수성 체크리스트:
    - engine이 도메인 개념(kept/removed)을 알면 안 됨
    - engine이 특정 입력 형식(JSONL)을 강제하면 안 됨
    - 프레임워크 본체에 CLI를 포함하지 않음
    - PRD에서 기술을 "제외"할 때 "지금 당장 불필요"와 "영원히 불필요"를 구분할 것
```

### 초기부터 올바른 의존 관계 다이어그램

```
[exceptions] ──┬──▶ [base: SourceStep, SequentialStep, AsyncStep, ParallelStep, Worker]
               └──▶ [config: StepConfig(extra 주입 패턴)]
                         │
                    [step_runners: InputStepRunner, SequentialStepRunner,
                                   AsyncStepRunner, ParallelStepRunner]
                         │
                    [engine: config 읽기 + 큐 배선 + StepRunner 실행]
                         │
                    [pipeline: 파사드 (Pipeline.run(config_path))]
```

---

## 5. 개발 방법론 교훈

### 샘플 프로젝트 없이 설계하면 안 된다

v1 설계는 실제 사용 사례 없이 만들어진 추상적인 구조였다. taxonomy-converter를 구축하면서 비로소 A-1~A-4 안티패턴이 가시화됐다.

**교훈:** 프레임워크 설계 시 최소 두 개의 서로 다른 실제 사용 사례를 미리 스케치한다. 두 케이스에서 자연스럽게 사용되는 인터페이스가 올바른 인터페이스다.

### Step 타입 추가는 상향식으로 검증한다

AsyncStep은 처음부터 계획에 없었다. PRD에서 "asyncio 제외"라고 명시했지만, LLM API 호출 유스케이스가 생기면서 비로소 필요성이 확인됐다. 초기 제약인 "CPU 바운드 파이프라인"이 실제 워크로드(데이터 가공 + 외부 API 호출 혼재)를 너무 좁게 가정한 것이었다.

**교훈:** YAGNI — 실제 사용 사례 없는 타입을 미리 추가하지 않는다. 단, 명확한 유스케이스가 생기면 빠르게 추가할 수 있도록 `StepBase → StepRunner` 연결 패턴을 일관되게 유지한다. PRD에서 기술을 "제외"할 때는 "지금 당장 불필요함"이지 "영원히 불필요함"이 아님을 명시해야 한다.

### TDD가 안티패턴을 조기에 잡는다

A-1~A-3은 "engine.run()을 어떻게 테스트할까?" 를 먼저 물었다면 발견됐을 것이다. `input_items=generator`를 테스트에서 주입하는 게 어색하다는 느낌이 설계 문제의 신호였다.

**교훈:** 테스트가 어색하면 설계를 의심한다. 테스트를 쉽게 쓸 수 없는 인터페이스는 사용자도 쓰기 어렵다.

---

## 6. 아카이브된 문서

| 파일 | 내용 | 상태 |
|------|------|------|
| `docs/WBS-v1.md` | 초기 구현 계획 (W-01~W-16, BaseStep 단일 계층) | 아카이브 |
| `docs/WBS-v2.md` | SourceStep 도입 이후 계획 | 아카이브 |
| `docs/WBS-v3.md` | Step 계층 리팩토링 계획 | 아카이브 |
| `docs/WBS-v4.md` | Step Metrics + Phase 6~11 계획 | 아카이브 |
| `docs/refactoring-background.md` | A-1~A-5 안티패턴 + C-1~C-5 순수성 점검 원문 | 아카이브 |
| `docs/refactoring-step-metrics.md` | Step Metrics 구현 배경 및 분석 원문 | 아카이브 |
| `docs/pipeline-stall-analysis.md` | 파이프라인 stall 분석 | 아카이브 |

현재 활성 계획: `docs/IMPLEMENTATION_PLAN.md`
