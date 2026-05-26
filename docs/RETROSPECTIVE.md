# 회고: 처음부터 어떻게 접근했으면 좋았을지

> task-pipeliner 개발 과정에서 발견된 설계 실수, 안티패턴, 그리고 그로부터 얻은 교훈을 정리한다.
> 작성일: 2026-04-09 / 마지막 갱신: 2026-05-26

---

## 1. 핵심 교훈 요약

1. **입력을 위한 구조와 방식, 필요 클래스는 처음부터 설계한다**: 이 프로젝트에서 입력은 파이프라인 밖이 아니라 안에 있어야 했으며 SourceStep이란 특수한 입력용 Step을 사용하여 자연스럽게 입력 형태를 정의하고 pipeline.run 시점에 해당 파이프라인을 구성하는 SourceStep이 지정하는 형태를 주입할 수 있는 가변적인 입력 구조의 parameter를 설계하고 전달해야 한다고 구체적으로 지정해야 했으나 해당 결정이 느려져 문제 파악, 적합한 패턴 설계 및 수정에 많은 시간이 걸렸다.
2. **편의적인 config 구조와 config 독립적인 programmable 객체를 모두 만족시키도록 설계한다**: config는 실제 작성해야 하는 입장에서 다양한 환경(docker 컨테이너 등)에서 어떻게 전달될지를 포함해 간편한 구조를 유지해야 하며, 복잡한 객체 및 임의 자료형에 대한 표현을 항상 염두에 두어 사용자의 혼란을 줄여야 한다. 한편으로, config체계가 일종의 프로그래밍 역할을 한다고 해서, config 없이는 프로그램 동작이 불가능하도록 의존관계를 만들면 안된다. 이는 테스트를 곤란하게 만드는 원인이며 프로그램의 독립적인 구성을 막는 안티패턴이므로, config의 역할은 '어떤 프로그래머블한 코드의 대체 방법 중 하나'임을 보장하라
3. **ABC의 체계를 먼저 확정한다**: 어떻게 분류되어야 하는가와, 어떻게 관리되어야 하는가에 착안해 추상화 체계를 모순없이 구체화해놓아야 구조적 변경이 덜하다. 같은 Step으로 분류되어야 하면서도 서로의 실행과 역할이 완전히 다른 SourceStep, SequentialStep, AsyncStep, PallelelStep이 있듯이 계층 체계를 필요한 만큼 구체화해놓아야 추후 구현에 있어 혼란이 덜하다.
4. **멀티프로세싱에 쓰일 워커 클래스는 별도의 ABC로 분리한다**: 서브프로세스에 복사되어 실행될 기능과 병렬화를 호출하고 책임지는 기능이 하나의 클래스로 합쳐져 있으면 두 관심사가 뒤섞일 뿐더러 직렬화 문제때문에 지속적으로 오류의 가능성을 내포한다. 서브프로세스로 복사할 객체는 반드시 별도로 정의하고 항상 직렬화 가능한 상태를 유지하라.
5. **최악의 상황과 병목을 설계 초기에 고려한다** — 어떤 시나리오든 메모리 경계를 넘기 전에 디스크로 spill할 전략이 처음부터 설계 안에 있어야 한다. 이 프로젝트에서도 큐를 무제한으로 두면서 데드락을 해결한 적이 있지만, 이는 OOM 위험을 뒤로 미룰 뿐이었다. 

---

## 2. 설계 진화

### v1 — 단일 BaseStep + 외부 입력 주입

```
[exceptions] → [base: BaseStep] → [engine] → [pipeline] → [cli]
```

`base.py`에는 단일 `BaseStep`만 있었고, 입력은 엔진 외부에서 주입됐다.

```python
# 실행 스크립트에서
engine.run(input_items=load_items(paths), output_dir=out_dir)
```

파이프라인 자체가 입력을 "모른다". 데이터 생성 책임이 실행 스크립트에 있어 config로 선언할 수 없었다. (→ A-1~A-3)

---

### v2 — SourceStep 도입, config가 입력을 선언

taxonomy-converter 샘플 구축 중 A-1~A-3 안티패턴이 가시화되면서 SourceStep이 추가됐다.

```python
class SourceStep(StepBase, ABC):
    @abstractmethod
    def items(self) -> Generator[Any, None, None]: ...
```

`engine.run(input_items=...)` 파라미터 제거. config가 입력 소스까지 선언하게 됐다.

---

### v3 — Step 계층 분리 + Worker 분리 + outputs DAG 라우팅

단일 `BaseStep`에서 역할별 ABC로 분리됐다.

```
StepBase
  ├─ SourceStep       # items() — 입력 생성
  ├─ SequentialStep   # process() — 단일 스레드 순차 처리
  ├─ AsyncStep        # process_async() — asyncio I/O 바운드 처리
  └─ ParallelStep     # create_worker() → Worker — 멀티프로세스 CPU 바운드
```

`Worker`가 `ParallelStep`에서 분리됐다. 메인 프로세스 조율(ParallelStep)과 서브프로세스 실행(Worker)의 라이프사이클과 직렬화 제약이 다르기 때문이다.

같은 시기에 `outputs: {tag: next_step}` 기반 DAG 라우팅이 추가되어 팬아웃/팬인 구조를 config에서 선언할 수 있게 됐다.

---

### v4 — config 고도화 (변수 치환, 재시도, 큐 전략, 체크포인트)

파이프라인 실행의 제어권을 코드가 아닌 config에서 갖도록 기능이 추가됐다.

| 추가 기능 | 내용 |
|-----------|------|
| 변수 치환 | `${var}`, `${var:-default}`, `$${var}` — 환경별 값 주입 |
| 재시도 정책 | `retry_count`, `retry_delay`, `retry_backoff` — Step별 선언 |
| 큐 전략 | `QueueType.AUTO/SPILL/FULL_DISK` — 메모리/디스크 경계 제어 |
| 체크포인트 | `checkpoint_dir`, `resume_run_id` — 중단 재시작 지원 |
| 다중 인스턴스 | `name` 필드 — 같은 `type`을 여러 번 선언 가능 |

---

### v5 — Programmable API 도입, config 의존성 제거

config 없이 코드로 파이프라인을 구성하는 경로가 추가됐다.

```python
# 이전: config 파일이 없으면 실행 불가
Pipeline.run("pipeline.yaml")

# 현재: programmable이 일급 인터페이스
pipeline = Pipeline()
pipeline.register(MySourceStep)
pipeline.register(MyProcessStep)
pipeline.run()

# config는 위 코드의 직렬화 표현
Pipeline.from_config("pipeline.yaml").run()
```

config가 필수 의존성이었던 구조에서, config는 "programmable 코드의 대체 방법 중 하나"로 역할이 재정의됐다. (→ A-6)

### 최종 의존 관계

```
[exceptions] ──┬──▶ [base: SourceStep, SequentialStep, AsyncStep, ParallelStep, Worker]
               └──▶ [config: StepConfig, ExecutionConfig, PipelineConfig]
                         │
                    [step_runners: InputStepRunner, SequentialStepRunner,
                                   AsyncStepRunner, ParallelStepRunner]
                         │
                    [engine: 큐 배선 + StepRunner 실행]
                         │
                    [pipeline: Pipeline.register() + run()  ← programmable 일급
                               Pipeline.from_config(path)  ← config 편의 수단]
```

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

### A-5. 최악의 상황을 고려하지 않은 큐 설계

**코드:**
```python
queue = Queue(maxsize=100)  # 메모리 바운드 방지 목적
```

**원인:** 단일 큐 구간에서는 백프레셔가 직관적으로 동작하지만, 팬아웃/팬인이 있는 멀티 스텝 그래프에서는 데드락이 발생한다. 업스트림이 꽉 찬 큐에 `put()`을 대기하는 동안, 다운스트림도 자신의 결과를 어딘가에 `put()`하려고 블로킹되는 순환 대기가 생긴다. 이 문제를 `maxsize=0`으로 해결했지만, 그것이 최종 설계라고 착각한 것이 더 큰 문제였다.

**수정 1 — 데드락 제거:**
```python
queue = Queue(maxsize=0)  # 무제한 — 데드락 없음. 단, OOM 위험은 여전히 존재.
```

**놓친 점 — 최악의 상황 설계 부재:** 무제한 큐는 업스트림 처리 속도가 다운스트림보다 훨씬 빠를 때 메모리를 무한정 소모한다. "일단 무제한으로 두고 나중에 OOM이 나면 고친다"는 접근은 프로덕션에서 불안정성을 가중시킬 뿐이다. 처음 설계할 때부터 "업스트림이 다운스트림보다 10배 빠르면 어떻게 되는가"를 질문하고 병목 위치를 시뮬레이션했어야 했다.

**수정 2 — 메모리 경계 설계 포함:**
`FullDiskQueue` / `SpillQueue` 같은 메모리 경계 메커니즘을 처음부터 설계 안에 포함해야 했다. OOM은 "나중에 생각할 것"이 아니라 "파이프라인 설계의 일부"다.

---

### A-6. config를 필수 의존성으로 만든 설계

**상황:**
```python
# 초기: config 없이는 파이프라인을 실행할 방법이 없었음
Pipeline.run("pipeline.yaml")  # 유일한 진입점

# 원했던 것: 코드로 직접 구성 가능해야 함
pipeline = Pipeline()
pipeline.register(MySourceStep)
pipeline.register(MyProcessStep)
pipeline.run(source_config={"threshold": 0.9})
```

**원인:** config 파일이 파이프라인을 "선언"한다는 원칙에 집중한 나머지, config 자체가 동작의 필수 조건이 되어버렸다. 프레임워크가 내부적으로 Step 인스턴스를 조립하는 과정을 config 파싱에만 의존하도록 만들었기 때문에, programmable 경로를 나중에 추가할 때 engine과 Pipeline 파사드의 인터페이스가 동시에 변경됐다. 기존 config 경로가 여전히 동작하는지 검증하는 비용도 함께 증가했다.

**올바른 원칙:** config의 역할은 "programmable한 코드의 대체 방법 중 하나"다. programmable API가 일급(first-class) 인터페이스이고, config는 그 코드를 YAML로 표현하는 편의 수단이어야 한다.

```python
# programmable이 일급 인터페이스
pipeline = Pipeline()
pipeline.register(MySourceStep)
pipeline.register(MyProcessStep)
pipeline.run()

# config는 위 코드를 YAML로 표현한 것
# Pipeline.from_config("pipeline.yaml").run()
# → 내부에서 위와 동일한 인스턴스 조립 경로를 통과함
```

이 원칙을 지키면 테스트에서 config 파일 없이 Step 인스턴스를 직접 등록하고 실행할 수 있고, config 파싱 로직과 파이프라인 실행 로직을 독립적으로 검증할 수 있다.

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

## 4. config 시스템이 갖춰야 할 기능 수준

config는 "YAML에서 Step을 고르는 것" 이상이어야 한다. 실제 데이터 파이프라인 운영에서 config가 커버해야 하는 기능 범위를 정리한다. 이 기준은 현재 구현을 통해 도달한 수준이며, 초기에 이 범위를 설계 목표로 잡았다면 중간 리팩토링 비용이 크게 줄었을 것이다.

### 5-1. 스텝 식별과 다중 인스턴스

```yaml
- type: jsonl_writer
  name: writer_kept      # 같은 type을 여러 개 쓸 때 name으로 구분
  output_dir: kept/

- type: jsonl_writer
  name: writer_removed
  output_dir: removed/
```

`type`은 클래스를 가리키고, `name`은 파이프라인 내 인스턴스를 구분한다. `name`을 생략하면 `type`으로 대체되므로 단순한 경우 기존 YAML 그대로 동작한다. `name`이 없으면 같은 Step을 두 번 선언할 방법이 없다.

### 5-2. 라우팅: outputs 태그 기반 DAG

```yaml
- type: filter_step
  outputs:
    kept: writer_kept
    removed: writer_removed
```

Step이 `emit(item, tag="kept")`로 출력하면 config의 `outputs.kept`에 지정된 다음 Step으로 라우팅된다. 팬아웃(하나 → 여럿), 팬인(여럿 → 하나) 모두 지원한다. 선형 파이프라인에서는 `outputs`를 생략하면 다음 Step으로 자동 연결된다.

config에서 `outputs`가 존재하지 않는 Step 이름을 참조하면 로드 시점에 즉시 오류가 발생한다. 실행 전에 DAG 유효성을 검증하는 것이 중요하다.

### 5-3. 스텝 파라미터 주입

```yaml
- type: convert_step
  dataset_name: naver_econ_news   # StepConfig.extra → cls(**extra)로 전달됨
  threshold: 0.9
```

`StepConfig`는 `extra="allow"`로 선언되어, 알려지지 않은 필드를 모두 통과시킨다. 이 값들이 `cls(**extra)` 형태로 Step 생성자에 그대로 전달된다. Step은 생성자에서 필요한 파라미터를 명시적으로 선언하면 된다.

### 5-4. 변수 치환

```yaml
- type: jsonl_source
  items: ["${INPUT_DIR}/data.jsonl"]   # 전체 값이 ${var}이면 타입 보존
  batch_size: ${BATCH_SIZE:-100}       # 기본값 지원
```

`load_config(path, variables={"INPUT_DIR": "/data", "BATCH_SIZE": 200})`로 런타임 값을 주입한다. 세 가지 규칙:
- `"${var}"` — 전체 매칭: 변수 값의 타입(list, int 등)을 그대로 보존
- `"prefix_${var}_suffix"` — 부분 매칭: 문자열로 치환
- `${var:-default}` — 변수가 없을 때 기본값 사용
- `$${var}` — 이스케이프: 리터럴 `${var}`로 출력

동일한 config를 환경(dev/prod/docker)마다 다르게 실행할 수 있게 해준다. 이 기능이 없으면 실행 스크립트가 config를 런타임에 직접 수정하는 안티패턴으로 돌아간다.

### 5-5. 실행 전략 설정

```yaml
execution:
  workers: 8           # ParallelStep 워커 수
  queue_size: 1000     # 메모리 임계치 (초과 시 디스크 spill)
  chunk_size: 100      # 배치 단위
  queue_type: auto     # auto / spill / full_disk
```

큐 전략(`queue_type`)을 config에서 제어할 수 있어야 한다. 코드 변경 없이 메모리 우선(spill) 또는 디스크 우선(full_disk) 모드로 전환할 수 있다. `queue_size=0`은 무제한이며, 값이 있으면 메모리 임계치로 동작한다.

### 5-6. 재시도 정책

```yaml
- type: llm_call_step
  retry_count: 3
  retry_delay: 1.0     # 첫 재시도 전 대기 (초)
  retry_backoff: 2.0   # 지수 백오프 배율
```

외부 API 호출처럼 일시적 실패가 예상되는 Step에 재시도를 config에서 선언한다. 코드에 재시도 로직을 직접 넣으면 Step마다 구현이 달라지고, 재시도 정책을 바꾸려면 코드를 수정해야 한다.

### 5-7. 체크포인트와 재시작

```yaml
checkpoint_dir: /tmp/pipeline_checkpoints
resume_run_id: "a3f2..."   # 이전 실패 run의 ID
```

`checkpoint_dir`을 설정하면 SourceStep이 처리한 각 아이템을 `item_key()`로 기록한다. `resume_run_id`를 지정하면 이미 처리된 아이템을 건너뛰고 실패 지점부터 재개한다. 긴 파이프라인에서 중간 실패 후 처음부터 재실행하는 비용을 없앤다.

### 5-8. 입력 검증과 에러 래핑

config 로드 시점에 다음을 모두 검사한다:
- `pipeline`이 비어 있지 않음
- `name`이 파이프라인 내에서 중복되지 않음
- `outputs`가 존재하는 Step 이름만 참조함
- 미해결 변수(`${var}`) 참조가 없음

모든 Pydantic `ValidationError`는 `ConfigValidationError`로 래핑되어 `field` 속성과 함께 전달된다. 사용자는 "어떤 필드가 잘못됐는가"를 즉시 알 수 있다.

---

## 5. 이력 참고

이 문서에서 언급된 WBS 및 리팩토링 배경 문서들(WBS-v1~v4, refactoring-background.md, refactoring-step-metrics.md, pipeline-stall-analysis.md)은 `chore: cleanup WBS backup documents and consolidate planning` 커밋에서 모두 삭제됐다. git 이력에서 확인할 수 있다.

현재 활성 계획: `PLAN.md`
