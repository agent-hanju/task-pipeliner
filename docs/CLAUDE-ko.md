# CLAUDE-ko.md (한국어 버전)

> **동기화 안내**: 이 파일은 `CLAUDE.md`의 한국어 버전입니다.
> `CLAUDE.md`가 변경될 때마다 이 파일도 동일한 내용으로 업데이트해야 합니다.
> WBS 기반 개발 방법론은 `docs/WBS-CLAUDE.md`에 별도 정리되어 있습니다.

## 환경

- Shell: Git Bash (Windows) — Unix 문법 사용, `NUL` 대신 `/dev/null`, 경로는 `/` 사용
- Python: `.venv/` (프로젝트 루트) — 항상 `.venv/Scripts/python` 또는 `.venv/Scripts/pip` 직접 사용
- 패키지 설치: `.venv/Scripts/pip install ".[dev]"`

## 자주 쓰는 명령어

```bash
# 테스트
.venv/Scripts/pytest --timeout=30 -v

# 특정 파일만
.venv/Scripts/pytest tests/test_queue.py

# 커버리지
.venv/Scripts/pytest --cov=task_pipeliner

# 린트
.venv/Scripts/ruff check src tests

# 포맷
.venv/Scripts/ruff format src tests

# 타입 검사
.venv/Scripts/mypy src
```

## 프로젝트 구조

- `src/task_pipeliner/` — 패키지 본체 (`src/` 레이아웃)
- `tests/` — 프레임워크 테스트 (dummy step만 사용, 비즈니스 로직 없음)
- `docs/` — PRD, 기술 스택, WBS 문서
- `sample/` — task-pipeliner 활용 샘플 프로젝트
- `pyproject.toml` — 패키지 메타데이터, 의존성, 빌드 설정

## 샘플 프로젝트

`sample/` 디렉토리 하위 개발 시, 이 파일과 함께 각 샘플 프로젝트의 `CLAUDE.md`를 따른다.

- `sample/taxonomy-converter/` — 네이버 뉴스 taxonomy 변환기 (`sample/taxonomy-converter/CLAUDE.md`)

## 주의사항

- spawn 방식 multiprocessing 사용 — 모든 함수는 모듈 레벨에 정의 (lambda, 중첩 함수, 중첩 클래스 금지)
- `tests/dummy_steps.py`의 step 클래스도 반드시 모듈 레벨에 정의
- Windows + Linux 크로스플랫폼 보장 필요

## 테스트 격리 원칙

- 프레임워크 테스트는 `tests/dummy_steps.py`의 dummy step만 사용한다. 비즈니스 로직(필터 기준, dedup 알고리즘 등)은 포함하지 않는다.
- 테스트 구조는 AAA(Arrange-Act-Assert) 패턴을 따른다.
- 다중 입력을 검증할 때는 `@pytest.mark.parametrize`를 사용한다.
- Queue/sentinel 관련 모든 테스트에는 `@pytest.mark.timeout`을 붙여 deadlock을 감지한다.

## Pickle 원칙

spawn 방식 multiprocessing에서 모든 전달 객체는 pickle 가능해야 한다.
- 모든 클래스/함수는 모듈 레벨에 정의 (lambda, 중첩 함수, 중첩 클래스 금지)
- `functools.partial`은 허용 (모듈 레벨 함수 기반일 때)
- 새 Step 클래스 등록 전 pickle 가능 여부를 엔진이 조기 검증한다

## 로깅 전략

로깅은 **각 모듈 구현 시점에** 함께 추가한다. 나중에 몰아서 넣지 않는다.

**로거 설정 — 모듈별, 계층형:**

각 모듈은 `__name__`으로 자체 로거를 생성한다. `task_pipeliner` 하위에 계층이 만들어진다:

```python
# 모든 모듈에서 (예: config.py):
import logging
logger = logging.getLogger(__name__)  # → "task_pipeliner.config"
```

- 부모 로거 `task_pipeliner`가 핸들러를 소유한다 (`StatsCollector.setup_log_handler`에서 설정).
- 자식 로거(`task_pipeliner.config`, `task_pipeliner.producers`, …)는 자동으로 부모에 전파한다.
- Formatter에 모듈, 함수, 라인이 포함되므로 메시지에 함수명을 수동으로 적을 필요 없다.

**로그 포맷:**

```python
"%(asctime)s %(levelname)-5s %(name)s:%(funcName)s:%(lineno)d %(message)s"
# 출력 예시:
# 2026-03-19 10:00:01 DEBUG task_pipeliner.config:load_config:42 path=/data/config.yaml
# 2026-03-19 10:00:01 INFO  task_pipeliner.io:open:58 Writer opened output=/out
```

**레벨 기준:**

| 레벨 | 사용 시점 |
|------|----------|
| `DEBUG` | 함수 진입 (주요 파라미터), 내부 분기, 리턴 전 결과 요약 |
| `INFO` | 메인 로직 내 핵심 도메인 이벤트 (생명주기 전환, I/O 완료, 주요 출력) |
| `WARNING` | `try/except`에서 처리한 복구 가능 이슈 — 처리 계속 |
| `ERROR` | 심각한 실패 — 파이프라인 중단 또는 중단 필요 |

**표준 함수 구조 (모든 non-trivial 함수에 이 패턴을 적용한다):**

```python
def do_something(self, path: Path, count: int) -> Result:
    logger.debug("path=%s count=%d", path, count)             # 진입 — funcName 자동

    # 1. 파라미터 검증 (invalid → 즉시 raise)
    if count <= 0:
        raise ConfigValidationError("count must be positive", field="count")

    # 2. 메인 로직 — INFO: 핵심 도메인 이벤트, DEBUG: 분기/중간 결과
    logger.info("Processing started for %s", path)
    items = self._load(path)
    logger.debug("loaded %d items", len(items))

    # 3. 에러 처리 — WARNING: 복구 시, raise/wrap: 치명적일 때
    try:
        result = self._transform(items)
    except SomeRecoverableError:
        logger.warning("Transform failed for %s, skipping", path)
        return Result.empty()

    # 4. 결과 정리
    logger.debug("returning %d results", len(result))
    return result
```

핵심:
- 첫 줄: `DEBUG` — 주요 파라미터만 (함수명은 Formatter가 자동 출력)
- 검증 블록: 로깅 불필요 — 명확한 메시지로 raise만
- 메인 로직: `INFO` — 운영자가 봐야 할 것, `DEBUG` — 판단/분기
- `try/except`: `WARNING` — 삼키는 경우, raise/wrap — 치명적인 경우
- 마지막 줄: `DEBUG` — 결과 요약

**모듈별 기대 로그:**

- `config.py` — INFO: config 로드 (경로, step 수). WARNING: step 비활성.
- `io.py` — INFO: reader 열림 (파일 수), writer open/close (출력 경로).
- `stats.py` — INFO: stats JSON 작성. WARNING: 쓰기 실패 억제.
- `producers.py` — INFO: producer 시작/종료 (step 이름), sentinel 송수신. WARNING: `process()` 예외 (아이템 skip, 잘라낸 repr 포함).
- `engine.py` — INFO: 파이프라인 시작 (step 수, worker 수), 파이프라인 완료. WARNING: process join 타임아웃. ERROR: 시그널에 의한 셧다운, worker 프로세스 crash.
- `pipeline.py` — INFO: 파이프라인 실행 시작/완료 (config 경로, input 수).
- `cli.py` — 로깅 설정만 (루트 핸들러 구성). 직접 로그 호출 불필요.

**원칙:**
- 민감 데이터나 전체 아이템 내용을 로깅하지 않는다 — 잘라낸 repr 사용 (`repr(item)[:200]`).
- 자식 프로세스는 로거 이름을 상속하되, `StatsCollector.setup_log_handler`로 자체 핸들러를 추가한다.
- `exceptions.py`, `base.py` — 로깅 없음 (순수 데이터 정의).
