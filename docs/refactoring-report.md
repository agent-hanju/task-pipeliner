# task-pipeliner 프레임워크 리팩토링 보고서

## 1. taxonomy-converter에서 발생한 안티패턴과 원인

### 1-1. engine.run()에 input_items를 직접 전달

**현상:** `run.py`가 `load_items()` 제너레이터를 만들어서 `engine.run(input_items=...)` 으로 전달한다.

```python
# 사용자 프로젝트의 실행 스크립트 (예: sample/taxonomy-converter/run.py)
input_items = load_items(input_paths)
engine.run(input_items=input_items, output_dir=output_dir)
```

**원인:** `engine.run()` 시그니처가 `input_items`를 파라미터로 요구한다. 입력 소스가 config에 선언된 파이프라인 스텝이 아니라 engine의 외부 파라미터다. 엔진이 "어디서 데이터를 가져올지"를 직접 결정하는 구조.

### 1-2. WriterStep을 코드로 pipeline에 append

**현상:** `run.py`가 config를 로드한 후 `cfg.pipeline.append(StepConfig(type="write", ...))` 로 writer 스텝을 코드에서 끼워넣는다.

```python
# 사용자 프로젝트의 실행 스크립트 (예: sample/taxonomy-converter/run.py)
cfg.pipeline.append(StepConfig(type="write", output_dir=str(output_dir)))
```

**원인:** pipeline_config.yaml에 writer가 선언되어 있지 않다. 출력이 config의 파이프라인 정의가 아니라 코드의 조작에 의해 결정된다.

### 1-3. pipeline_config.yaml이 파이프라인의 일부만 선언

**현상:** config에는 preprocess → convert → deduplicate만 있고, 입력(loader)과 출력(writer)은 없다.

```yaml
pipeline:
  - type: preprocess
  - type: convert
  - type: deduplicate
```

**원인:** 프레임워크가 SOURCE 타입의 스텝 개념을 지원하지 않는다. 첫 번째 스텝은 반드시 Queue에서 아이템을 받는 구조만 가능하고, 아이템을 생성하는 스텝은 존재하지 않는다. 따라서 입력은 engine 외부에서 주입할 수밖에 없고, config에 선언할 수 없다.

### 1-4. 스텝의 설정값이 코드에 하드코딩

**현상:** ConvertStep의 도메인 값과 PreprocessStep의 검증 임계값이 코드에 직접 박혀 있다.

```python
# ConvertStep — 도메인 상수가 코드에 하드코딩
metadata = make_metadata("HanaTI/NaverNewsEconomy", published_date)
"dataset_name": "naver_econ_news",
"id": f"HanaTI-NaverNews-{date_prefix}-{index}",
"language": "korean",
"category": "hass",
"industrial_field": "finance",

# PreprocessStep — 검증 임계값이 모듈 상수
_MIN_LINES = 2
_MIN_CHARS = 100
```

**원인:** 스텝이 `__init__`에서 설정 파라미터를 받지 않는다. StepConfig extra → `cls(**extra)` 로 생성자에 주입하는 패턴이 확립되지 않았고, "config에 스텝별 설정을 선언한다"는 설계 의도가 샘플 구현에 반영되지 않았다. config에서 주입 가능한 값이 코드에 고정되어, config 변경 없이는 다른 데이터셋이나 기준에 적용할 수 없다.

### 1-5. 전처리 스텝의 분할에 대하여

**현상:** PreprocessStep이 normalize → document transform → line filter → line inline → validate를 모두 하나의 process()에서 처리한다.

**프레임워크 제약 여부:** 없다. 각 단계를 독립 PARALLEL 스텝으로 분리하여 config에 선언하는 것이 가능하다.

```yaml
# 분리한다면:
pipeline:
  - type: normalize
  - type: doc_transform
  - type: line_filter
  - type: validate
```

하나로 합친 것은 프레임워크 제약이 아니라 granularity 판단이다. 이 수준의 텍스트 처리는 각 단계가 가볍고 아이템 단위로 독립적이므로, 분리 시 스텝 경계마다 큐 직렬화/역직렬화 오버헤드가 발생하는 것 대비 이점이 크지 않다. 다만 일부 단계가 무거워지거나 단계별로 다른 병렬도가 필요해지면 분리하는 것이 적절하다.

> **참고:** 본 보고서에서 "사용자 프로젝트의 실행 스크립트"는 task-pipeliner 프레임워크 내부의 코드가 아니라, task-pipeliner를 사용해 구축한 프로젝트(taxonomy-converter 등)가 자기 자신을 실행하기 위해 작성하는 Python 스크립트(예: `sample/taxonomy-converter/run.py`)를 의미한다.

---

## 2. 프레임워크 인터페이스 수정 방안

### 2-1. StepType.SOURCE 추가

```
현재: PARALLEL | SEQUENTIAL
수정: PARALLEL | SEQUENTIAL | SOURCE
```

SOURCE 스텝은 Queue 대신 자체적으로 아이템을 생성한다. `process(item, state, emit)` 대신 `items()` 제너레이터를 구현한다.

**BaseStep 변경:**
```python
class BaseStep[R: BaseResult](ABC):
    def items(self) -> Generator[Any, None, None]:
        """SOURCE 스텝 전용. 아이템을 yield한다."""
        raise NotImplementedError
```

### 2-2. InputProducer가 SOURCE 스텝을 래핑

현재 `InputProducer`는 engine이 직접 생성하며 raw generator를 받는다. 수정 후에는 config에 선언된 SOURCE 스텝을 래핑한다.

```
현재: engine.run(input_items=generator) → engine이 InputProducer(input_items=generator) 생성
수정: config에 SOURCE 스텝 선언 → engine이 SOURCE 스텝을 감지 → InputProducer(step=source_step) 생성
```

InputProducer는 `step.items()`를 호출하여 아이템을 output queue에 넣는다. engine.run()은 더 이상 `input_items`를 받지 않는다.

### 2-3. engine.run() 시그니처 변경

```python
# 현재
def run(self, *, input_items, output_dir): ...

# 수정
def run(self, *, output_dir): ...
```

`input_items` 제거. 입력은 config의 SOURCE 스텝이 담당한다.

### 2-4. StepConfig extra로 스텝별 설정 선언

pipeline_config.yaml이 각 스텝의 설정값을 직접 선언한다. StepConfig는 `extra="allow"`이므로 type/enabled 외의 필드는 모두 `model_extra`로 들어가고, engine이 `cls(**extra)`로 스텝을 인스턴스화할 때 생성자 파라미터가 된다.

```yaml
pipeline:
  - type: loader
    paths:
      - ./data/input
  - type: preprocess
  - type: convert
  - type: deduplicate
  - type: writer
    dir: ./output

execution:
  workers: 4
  queue_size: 200
  chunk_size: 100
```

사용자 프로젝트(taxonomy-converter 등)의 run.py는 자체 CLI 인자로 일부 설정만 override한다. config가 **전체 기본 설정**을 가지고, CLI는 **일부 override**만 하는 구조. 스텝이 늘어나고 설정이 복잡해져도 config에 선언되어 있으므로 CLI가 모든 파라미터를 알 필요 없다.

```python
# taxonomy-converter/run.py 예시
cfg = load_config(config_path)

# CLI로 받은 값만 override
if args.output:
    for i, step_cfg in enumerate(cfg.pipeline):
        if step_cfg.type == "writer":
            cfg.pipeline[i] = StepConfig(type="writer", dir=str(args.output))
```

### 2-5. 큐 체인 구조 변경

```
현재 (N steps + InputProducer):
  InputProducer → Q0 → Step0 → Q1 → ... → StepN-1 (output_queues=[])
  큐 N개

수정 (N steps, 첫 번째가 SOURCE):
  Step0(SOURCE) → Q0 → Step1 → Q1 → ... → StepN-1 (output_queues=[])
  큐 N-1개
```

SOURCE 스텝은 input queue 없이 output queue만 가진다. 마지막 스텝은 output queue 없이 자체적으로 출력을 처리한다 (내부 구현).

---

## 3. 프레임워크 순수성을 해치는 기존 코드 점검

### 3-1. io.py — JsonlWriter: 도메인 특화 I/O가 프레임워크에 내재

| 항목 | 문제 |
|------|------|
| `kept.jsonl`, `removed.jsonl` 파일명 하드코딩 | 프레임워크가 "필터링 파이프라인"을 가정. 범용 프레임워크가 kept/removed라는 도메인 개념을 가질 이유 없음 |
| `write_kept()`, `write_removed()` 메서드 | kept/removed 분류는 사용자 도메인 로직. 프레임워크 레벨에서 강제할 인터페이스 아님 |
| `JsonlWriter`를 engine이 import | engine → io 의존. 엔진이 특정 출력 형식을 알고 있음 |

**판정:** JsonlWriter는 프레임워크가 제공하는 유틸리티로 존재할 수 있으나, engine이 import하거나 사용해서는 안 된다. 사용자가 선택적으로 사용하는 유틸리티여야 한다. kept/removed 분류는 제거하거나 사용자 구현으로 이동해야 한다.

### 3-2. engine.py — 엔진이 I/O를 수행

| 항목 | 위치 | 문제 |
|------|------|------|
| `input_items` 파라미터 | `run()` 시그니처 | 엔진이 입력 소스를 외부에서 받아 직접 InputProducer에 주입. config-driven이 아님 |
| `InputProducer` 직접 생성 | L141-145 | 엔진이 특정 Producer 타입을 하드코딩하여 생성 |
| `output_dir` 파라미터 | `run()` 시그니처 | 엔진이 파일시스템 출력 위치를 알아야 함 |
| `result.write(output_dir)` | L241 | 엔진이 각 스텝의 결과를 직접 파일에 기록 |
| `stats.write_json()` | L247 | 엔진이 통계를 직접 파일에 기록 |
| `output_dir.mkdir()` | L237 | 엔진이 디렉토리를 생성 |

**판정:** 엔진의 역할은 "config 읽기 → 스텝 인스턴스화 → 큐 연결 → Producer 실행 → 완료 대기"까지다. result 수집과 stats 기록은 엔진의 역할이지만, 파일 기록은 호출자가 결정해야 한다.

다만 `result.write(output_dir)`와 `stats.write_json()`은 **실행 기록**의 영역이므로, 엔진이 output_dir를 받아서 기록하는 것은 수용 가능하다. BaseResult.write()는 스텝 자신의 실행 결과 요약(count_summary.json 등)이지 파이프라인 출력(kept.jsonl)이 아니기 때문이다. 이 부분은 현재 설계를 유지해도 된다.

**핵심 문제는 `input_items` 파라미터와 `InputProducer` 직접 생성이다.**

### 3-3. pipeline.py — Pipeline 파사드가 JsonlReader를 강제

| 항목 | 위치 | 문제 |
|------|------|------|
| `JsonlReader(inputs)` 직접 생성 | L54 | 입력 형식을 JSONL로 강제. JSON array, CSV, DB 등 다른 형식 사용 불가 |
| `inputs: list[Path]` 파라미터 | `run()` 시그니처 | Pipeline 파사드가 파일 경로 기반 입력을 전제 |

**판정:** Pipeline은 편의 파사드이므로 특정 입력 형식(JSONL)을 기본 제공하는 것 자체는 문제없다. 그러나 그 구현이 engine.run(input_items=...)에 의존하고 있어서, 엔진의 input_items가 제거되면 파사드도 수정이 필요하다.

수정 방향: Pipeline.run()이 inputs를 받으면 내부적으로 config에 JSONL SOURCE 스텝을 주입하여 engine에 전달. 엔진 자체는 config만 본다.

### 3-4. cli.py — filter/batch 커맨드

| 항목 | 문제 |
|------|------|
| `filter` 커맨드명 | "필터링"이라는 특정 워크플로우를 가정 |
| `--input`, `--output` 플래그 | 입출력을 CLI가 처리하여 Pipeline에 전달 |

**판정:** CLI는 사용자 인터페이스이므로 `--input`, `--output`을 받는 것 자체는 정상이다. 다만 `filter`라는 이름은 범용 파이프라인 프레임워크에 맞지 않다. `run` 정도가 적절하다. CLI가 받은 경로를 config의 SOURCE/출력 스텝 파라미터로 주입하는 구조가 되어야 한다.

### 3-5. io.py — JsonlReader

| 항목 | 문제 |
|------|------|
| JSONL 전용 | 범용 프레임워크가 특정 형식의 Reader를 내장 |

**판정:** 유틸리티로 존재하는 것은 문제없다. Pipeline 파사드가 이것을 기본 제공하는 것도 편의 기능으로 수용 가능. 문제는 engine이 이것에 의존하는 것인데, 현재 engine은 JsonlReader를 직접 사용하지 않으므로 이 부분은 정상.

---

## 요약: 수정이 필요한 항목

| 우선순위 | 항목 | 변경 내용 |
|---------|------|----------|
| **P0** | `base.py` | `StepType.SOURCE` 추가, `BaseStep.items()` 메서드 추가 |
| **P0** | `producers.py` | `InputProducer`가 `step.items()` 사용하도록 변경 |
| **P0** | `engine.py` | `input_items` 파라미터 제거, SOURCE 스텝을 config에서 감지하여 InputProducer 생성 |
| **P1** | `pipeline.py` | `inputs` 파라미터 유지하되, config에 SOURCE 스텝 주입 후 engine 호출 |
| **P1** | `cli.py` | `filter` → `run` 이름 변경 고려. 입력 경로를 config SOURCE 스텝에 주입 |
| **P2** | `io.py` | `JsonlWriter`의 kept/removed 제거 또는 범용화. engine에서 import 제거 (이미 완료) |
| **P2** | `io.py` | JSONL SOURCE 스텝 클래스 추가 (Pipeline 파사드용) |

### 수정하지 않아도 되는 항목

| 항목 | 이유 |
|------|------|
| `engine.run(output_dir=...)` | result.write()와 stats.write_json()은 실행 기록이지 파이프라인 출력이 아님 |
| `StatsCollector.write_json()` | 실행 통계 기록은 엔진의 정당한 책임 |
| `BaseResult.write()` | 각 스텝의 자체 실행 결과 요약. 파이프라인 데이터 출력과 무관 |
| `JsonlReader` 존재 자체 | 유틸리티로 제공되는 것은 문제없음. engine이 강제하지만 않으면 됨 |
