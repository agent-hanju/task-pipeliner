# 기술 스택 명세서: Task Pipeliner

## 개요

Task Pipeliner 프레임워크의 기술 스택 선택 근거와 버전 명세.
의존성 최소화 원칙: 런타임 의존성은 YAML 파싱과 config 검증 두 가지로만 제한한다.

---

## Python 버전

| 항목 | 결정 |
|---|---|
| 최소 지원 버전 | Python **3.12** |
| 이유 | `type X = ...` 타입 별칭, `@override` 데코레이터를 `typing_extensions` 없이 사용 가능. `typing_extensions` 의존성 제거. |

---

## 런타임 의존성

### 표준 라이브러리 (추가 설치 불필요)

| 모듈 | 용도 |
|---|---|
| `multiprocessing` | Queue, Event, Process — step 간 통신 및 동기화 |
| `concurrent.futures` | ProcessPoolExecutor (spawn 방식) — 병렬 워커 실행 |
| `asyncio` | AsyncStep 기반 I/O 바운드 작업 (LLM API, HTTP 요청 등) |
| `logging` | 계층적 로거, 실시간 로그 flush |
| `signal` | SIGINT / SIGTERM graceful shutdown |
| `pathlib` | 파일 경로 처리 |
| `abc` | 추상 기반 클래스 |
| `dataclasses` | 경량 데이터 구조 |
| `functools` | `functools.partial` — lambda 대신 pickle 가능한 함수 래핑 |
| `typing` | 타입 힌트 |

### 서드파티 런타임 의존성

| 패키지 | 버전 | 용도 | 선택 근거 |
|---|---|---|---|
| `PyYAML` | `>=6.0.2` | YAML 파이프라인 config 파싱 | 가장 널리 사용, 안정적. `ruamel.yaml`은 기능 과잉. |
| `pydantic` | `>=2.0` | config 스키마 검증 및 파싱 | v2는 Rust 기반으로 빠름. YAML → dataclass 변환 + 필드 검증 + extra 주입 패턴에 활용. |

> **CLI 없음**: 프레임워크 본체에 CLI가 없다. 실행 스크립트는 사용자가 직접 작성하거나 `Pipeline` 파사드를 사용한다.

---

## 개발 의존성

| 패키지 | 버전 | 용도 |
|---|---|---|
| `pytest` | `>=8.0` | 테스트 프레임워크 |
| `pytest-timeout` | `>=2.3` | 데드락 감지 (Queue sentinel 전파 테스트에 필수) |
| `pytest-cov` | `>=5.0` | 커버리지 측정 |
| `mypy` | `>=1.8` | 정적 타입 검사 |
| `types-PyYAML` | `>=6.0` | PyYAML mypy 타입 스텁 |
| `ruff` | `>=0.4` | 린팅 + 포맷팅 (black + flake8 + isort 통합 대체) |
| `orjson` | `>=3.9` | sample 프로젝트에서 사용 (framework 본체는 stdlib json 사용) |

---

## 빌드 및 패키징

| 항목 | 결정 |
|---|---|
| 빌드 백엔드 | **hatchling** (`>=1.21`) |
| 패키지 레이아웃 | `src/` 레이아웃 (설치 전 실수로 import되는 것 방지) |
| 패키지 명세 파일 | `pyproject.toml` (PEP 517/518) |
| 배포 패키지명 | `task-pipeliner` |
| import명 | `task_pipeliner` |

### 사용 예

```python
from task_pipeliner import Pipeline, SourceStep, SequentialStep, ParallelStep, AsyncStep, Worker
```

---

## 프로젝트 디렉토리 구조

```
task-pipeliner/
├── pyproject.toml
├── README.md
│
├── src/
│   └── task_pipeliner/
│       ├── __init__.py         # 공개 API (Pipeline, SourceStep, SequentialStep, AsyncStep, ParallelStep, Worker 등)
│       ├── base.py             # SourceStep / SequentialStep / AsyncStep / ParallelStep / Worker ABC
│       ├── step_runners.py     # InputStepRunner / SequentialStepRunner / AsyncStepRunner / ParallelStepRunner
│       ├── producers.py        # 하위호환 shim (step_runners → legacy *Producer 이름)
│       ├── engine.py           # PipelineEngine (Queue 배선, StepRunner 실행, 통계 수집)
│       ├── pipeline.py         # Pipeline 파사드 + StepRegistry
│       ├── config.py           # PipelineConfig, StepConfig (pydantic v2, extra 주입 지원)
│       ├── stats.py            # StatsCollector, StepStats
│       ├── progress.py         # ProgressReporter (stderr 실시간 진행률)
│       └── exceptions.py       # PipelineError, StepRegistrationError, ConfigValidationError
│
├── tests/
│   ├── dummy_steps.py          # 프레임워크 테스트용 dummy step (모듈 레벨 정의 필수)
│   ├── test_queue.py
│   ├── test_fanout.py
│   ├── test_backpressure.py
│   ├── test_error.py
│   ├── test_shutdown.py
│   ├── test_engine.py
│   ├── test_schema.py
│   ├── test_stats.py
│   ├── test_progress.py
│   ├── test_state_readiness.py
│   ├── test_variables.py
│   ├── test_async_step.py
│   └── test_init.py
│
└── sample/
    ├── pretrain-data-filter/   # 한국어 pretrain 데이터 품질 필터 + dedup
    └── taxonomy-converter/     # Naver 뉴스 taxonomy 변환기
```

---

## 아키텍처 결정 기록 (ADR)

### ADR-1: `src/` 레이아웃 채택
- **결정**: `src/task_pipeliner/` 구조 사용
- **이유**: 설치하지 않은 상태에서 `import task_pipeliner`가 되는 것을 방지. 올바르게 pip install한 패키지를 참조하도록 강제.

### ADR-2: CLI 없음
- **결정**: 프레임워크 본체에 CLI 미포함
- **이유**: `filter`/`batch`/`run` 커맨드명이 특정 도메인 워크플로우를 가정하게 된다 (C 계열 안티패턴). 사용자가 실행 스크립트를 직접 작성하거나 `Pipeline` 파사드를 호출하는 것이 더 범용적이고 명확하다.

### ADR-3: pydantic v2 config 검증
- **결정**: YAML 파싱 후 pydantic 모델로 검증
- **이유**: step 필드 누락, 타입 오류, 알 수 없는 step name 등을 실행 전에 조기 감지. `StepConfig extra → cls(**extra)` 패턴으로 생성자 주입을 자연스럽게 지원.

### ADR-4: ProcessPoolExecutor spawn 방식 고정
- **결정**: `multiprocessing.set_start_method("spawn")`을 엔진 진입점에서 명시적으로 설정
- **이유**: Windows는 spawn이 기본이나 Linux는 fork가 기본. fork는 CUDA/파일핸들 상속 문제 발생 가능. spawn으로 통일하면 크로스플랫폼 동작이 보장되고 pickle 가능 여부가 조기에 드러남.

### ADR-5: 공개 API를 `__init__.py`에서 명시적 노출
- **결정**: `from task_pipeliner import Pipeline, SourceStep, ...` 형태로 사용 가능
- **이유**: 구현 프로젝트가 내부 모듈 경로를 알 필요 없음. 내부 리팩터링 시에도 공개 API 호환성 유지 가능.

### ADR-6: asyncio 포함 (AsyncStep P1)
- **결정**: `asyncio` + `asyncio.Semaphore` 기반 `AsyncStepRunner` 추가
- **이유**: LLM API 호출, HTTP 요청 등 I/O 바운드 작업은 multiprocessing보다 asyncio가 효율적. `asyncio.run()`을 스레드 안에서 실행하여 다른 StepRunner와 스레드 모델 통일.

### ADR-7: orjson을 프레임워크 본체에서 제거 (M-23)
- **결정**: 프레임워크 본체는 stdlib `json` 사용. orjson은 sample 프로젝트에서 선택적 사용.
- **이유**: 프레임워크가 특정 직렬화 라이브러리에 종속되면 사용자의 의존성 관리 부담 증가. 프레임워크 자체는 JSON 직렬화 성능이 병목이 아님.

---

## 제외 결정

| 패키지 | 제외 이유 |
|---|---|
| `click` / `typer` | CLI는 프레임워크 본체에서 제거. 도메인 워크플로우를 가정하는 CLI명이 프레임워크 순수성을 해침. |
| `celery` / `ray` | 분산 인프라 종속성 발생. PRD 명시적 제외 대상. |
| `apache-beam` | 추상화 비용 과대. PRD 명시적 제외 대상. |
| `rich` | 출력이 파일 기반(`stats.json`)이라 필요 없음. stderr 진행률은 직접 구현. |
| `structlog` / `loguru` | stdlib `logging`으로 충분. 의존성 최소화 원칙. |
