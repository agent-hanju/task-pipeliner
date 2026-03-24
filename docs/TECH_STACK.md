# 기술 스택 명세서: Task Pipeliner

## 개요

이 문서는 Task Pipeliner 프레임워크의 기술 스택 선택 근거와 버전 명세를 기술한다.
PRD 요구사항(크로스플랫폼, 배포 가능 패키지, 병렬 실행, YAML 파이프라인)을 기준으로 선택하였다.

---

## Python 버전

| 항목 | 결정 |
|---|---|
| 최소 지원 버전 | Python **3.12** |
| 개발 기준 버전 | Python **3.14** |
| 이유 | 3.12부터 `type X = ...` 타입 별칭 문법, `@override` 데코레이터를 `typing_extensions` 없이 사용 가능. `typing_extensions` 의존성 제거. 3.12 미만 환경 지원 포기로 타입 힌트 제약 없음. |

---

## 런타임 의존성

### 표준 라이브러리 (추가 설치 불필요)

| 모듈 | 용도 |
|---|---|
| `multiprocessing` | Queue, Event, Process — step 간 통신 및 동기화 |
| `concurrent.futures` | ProcessPoolExecutor (spawn 방식) — 병렬 워커 실행 |
| `logging` | 실시간 로그 파일 flush |
| `signal` | SIGINT / SIGTERM graceful shutdown |
| `pathlib` | 파일 경로 처리 |
| `abc` | 추상 기반 클래스 (BaseStep, BaseStepRunner 인터페이스) |
| `dataclasses` | 경량 데이터 구조 |
| `functools` | `functools.partial` — lambda 대신 pickle 가능한 함수 래핑 |
| `typing` | 타입 힌트 |

### 서드파티 의존성 (최소화 원칙)

| 패키지 | 버전 | 용도 | 선택 근거 |
|---|---|---|---|
| `PyYAML` | `>=6.0.2` | YAML 파이프라인 config 파싱 | 가장 널리 사용, 안정적. `ruamel.yaml`은 기능 과잉. |
| `pydantic` | `>=2.0` | config 스키마 검증 및 파싱 | v2는 Rust 기반으로 빠름. YAML → dataclass 변환 + 필드 검증에 활용. |
| `click` | `>=8.1` | CLI subcommand 구조 | `argparse` 대비 subcommand 정의가 간결. `filter` / `batch` 분기에 적합. |
| `orjson` | `>=3.9` | JSONL 읽기/쓰기 | stdlib `json` 대비 5~10배 빠름 (Rust 기반). JSONL이 핵심 I/O 포맷이라 직접적인 효과. |

> **의존성 결정 원칙**: stdlib로 가능한 것은 stdlib를 사용한다. 서드파티는 CLI와 config 파싱 두 영역에만 허용한다.

---

## 개발 의존성

| 패키지 | 버전 | 용도 |
|---|---|---|
| `pytest` | `>=8.0` | 테스트 프레임워크 |
| `pytest-timeout` | `>=2.3` | 데드락 감지 (Queue sentinel 전파 테스트에 필수) |
| `pytest-cov` | `>=5.0` | 커버리지 측정 |
| `mypy` | `>=1.8` | 정적 타입 검사 |
| `types-PyYAML` | `>=6.0` | PyYAML mypy 타입 스텁 (`PyYAML`은 내장 타입 힌트 없음) |
| `ruff` | `>=0.4` | 린팅 + 포맷팅 (black + flake8 + isort 통합 대체) |

---

## 빌드 및 패키징

| 항목 | 결정 |
|---|---|
| 빌드 백엔드 | **hatchling** (`>=1.21`) |
| 패키지 레이아웃 | `src/` 레이아웃 (설치 전 실수로 import되는 것 방지) |
| 패키지 명세 파일 | `pyproject.toml` (PEP 517/518) |
| 배포 패키지명 | `task-pipeliner` (PyPI 배포 시 사용) |
| import명 | `task_pipeliner` |
| CLI 진입점 | `task-pipeliner` 명령어 → `task_pipeliner.cli:main` |

### 설치 방법 (설계 기준)

```bash
# 소스에서 개발용 설치
pip install -e ".[dev]"

# 배포 패키지로 설치 (향후)
pip install task-pipeliner

# 구현 프로젝트에서 import
from task_pipeliner import Pipeline, BaseStep, ParallelStepRunner, SequentialStepRunner
```

---

## 프로젝트 디렉토리 구조

```
task-pipeliner/
├── pyproject.toml              # 패키지 메타데이터, 의존성, 빌드 설정
├── README.md
│
├── src/
│   └── task_pipeliner/         # import task_pipeliner
│       ├── __init__.py         # 공개 API 노출 (Pipeline, BaseStep 등)
│       ├── base.py             # BaseStep, StepType(PARALLEL/SEQUENTIAL) 추상 인터페이스
│       ├── step_runners.py     # ParallelStepRunner, SequentialStepRunner 구현
│       ├── producers.py        # 하위호환 re-export shim (step_runners → producers)
│       ├── engine.py           # 실행 엔진 (Queue 연결, sentinel 관리, 통계 수집)
│       ├── pipeline.py         # Pipeline 클래스 (step 등록, YAML 로딩)
│       ├── config.py           # pydantic 기반 config 스키마 (PipelineConfig, StepConfig 등)
│       ├── cli.py              # click 기반 CLI (filter / batch subcommand)
│       ├── io.py               # JsonlReader, JsonlWriter
│       ├── stats.py            # StatsCollector, stats.json / pipeline.log 출력
│       └── exceptions.py       # PipelineError, StepRegistrationError 등 커스텀 예외
│
└── tests/
    ├── __init__.py
    ├── dummy_steps.py          # PassthroughStep, ErrorOnItemStep, SlowStep, CountingAggStep
    ├── test_queue.py           # Queue 통신 / sentinel 전파
    ├── test_fanout.py          # Fan-out / state / Event 알림
    ├── test_backpressure.py    # 백프레셔
    ├── test_error.py           # 에러 처리 / 에러 전파
    ├── test_shutdown.py        # Graceful shutdown (SIGINT)
    └── test_cli.py             # CLI / config 파싱
```

---

## 아키텍처 결정 기록 (ADR)

### ADR-1: `src/` 레이아웃 채택
- **결정**: `src/task_pipeliner/` 구조 사용
- **이유**: 설치하지 않은 상태에서 `import task_pipeliner`가 되는 것을 방지. 구현 프로젝트가 올바르게 pip install한 패키지를 참조하도록 강제.

### ADR-2: `argparse` 대신 `click` 선택
- **결정**: `click` 사용
- **이유**: `filter` / `batch` subcommand 정의가 데코레이터 기반으로 간결. `--input` 다중값(nargs) 처리가 직관적. 의존성 추가 비용 대비 CLI 유지보수성 향상.

### ADR-3: `pydantic` v2 config 검증
- **결정**: YAML 파싱 후 pydantic 모델로 검증
- **이유**: step 필드 누락, 타입 오류, 알 수 없는 step name 등을 실행 전에 조기 감지. 오류 메시지가 명확함. v2는 v1 대비 10x 빠른 검증.

### ADR-4: `ProcessPoolExecutor` spawn 방식 고정
- **결정**: `multiprocessing.set_start_method("spawn")`을 엔진 진입점에서 명시적으로 설정
- **이유**: Windows는 spawn이 기본이나 Linux는 fork가 기본. fork는 CUDA/파일핸들 상속 문제 발생 가능. spawn으로 통일하면 크로스플랫폼 동작이 보장되고 pickle 가능 여부가 조기에 드러남.

### ADR-5: 공개 API를 `__init__.py`에서 명시적 노출
- **결정**: `from task_pipeliner import Pipeline, BaseStep` 형태로 사용 가능하도록 `__init__.py`에서 re-export
- **이유**: 구현 프로젝트가 내부 모듈 경로(`task_pipeliner.base.BaseStep`)를 알 필요 없음. 내부 리팩터링 시에도 공개 API 호환성 유지 가능.

---

## 제외 결정

| 패키지 | 제외 이유 |
|---|---|
| `asyncio` / `aiofiles` | 병렬성은 CPU 바운드 → multiprocessing이 적합. async는 I/O 바운드에 유리. |
| `celery` / `ray` | 분산 인프라 종속성 발생. PRD 명시적 제외 대상. |
| `apache-beam` | 추상화 비용 과대. PRD 명시적 제외 대상. |
| `rich` | 출력이 파일 기반(`stats.json`, `pipeline.log`)이라 필요 없음. `tail -f`로 보는 로그에 ANSI 코드가 오히려 노이즈. |
| `structlog` | stdlib `logging`으로 충분. 의존성 최소화 원칙. |
| `loguru` | 동상. |
| `typer` | click 기반이지만 pydantic 종속. click 직접 사용이 더 명확. |
