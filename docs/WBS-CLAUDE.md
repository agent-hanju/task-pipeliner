# WBS 기반 개발 방법론

> 이 문서는 `docs/WBS.md`를 활용한 개발 진행 방법을 정리한 것이다.
> CLAUDE.md의 공통 규칙과 함께 적용한다.

## 진행 원칙 (TDD)

각 작업 단위는 아래 순서를 **반드시** 지킨다. 어떤 단계도 건너뛰지 않는다.

1. **레퍼런스 탐색** — 코드 작성 전, 해당 단위에서 사용하는 라이브러리(예: Pydantic v2, orjson, click, multiprocessing spawn)의 웹 문서를 검색해 API 사용법이 설치된 버전과 일치하는지 확인한다. 비자명한 사항은 기록해 둔다.
2. **인터페이스 확인** — `docs/WBS.md`의 인터페이스 스펙을 확인한다.
3. **테스트 작성** — 테스트를 먼저 작성한다. 실행하여 실패(red)를 확인한다.
4. **구현** — 테스트를 통과시키기 위한 최소한의 코드를 작성한다.
5. **테스트 통과 확인** — pytest를 실행한다. 모든 테스트가 green이어야 한다.
6. **린트 & 타입 검사** — ruff와 mypy를 실행한다. 모든 오류를 해결한 뒤 다음으로 넘어간다.
7. **WBS 업데이트** — `docs/WBS.md`에서 완료된 세부 단계를 체크한다.

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

## 개발 단계 (Phase)

```
Phase 1 — 기반: exceptions → base, config (+ 각 테스트)
Phase 2 — 지원: stats, io (서로 독립, Phase 1 완료 후 병렬 작업 가능)
           dummy_steps (base 완료 후)
Phase 3 — 실행 코어: producers → Queue/Fanout/백프레셔/에러 테스트
Phase 4 — 통합: engine → shutdown → pipeline → cli → __init__
```

각 Phase 내에서 독립적인 모듈은 병렬 작업이 가능하다. 의존 관계는 `docs/WBS.md` 참조.

## WBS 체크박스 관리

진행 상황은 `docs/WBS.md`의 체크박스로 추적한다. 각 작업 단위에는 **세부 체크리스트**가 있다.

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
