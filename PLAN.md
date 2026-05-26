# Plan: task-pipeliner

> 이 문서는 task-pipeliner 프레임워크의 현재 상태와 다음 작업을 기록한다.
> 최종 갱신: 2026-05-26

---

## 현재 상태

**Phase 1~13 완료.** Programmable API, AsyncStep, SpillQueue/FullDiskQueue, Checkpoint, Retry 포함.

**Requirements:**
- Python 3.11+, spawn 모드 멀티프로세싱 (Windows/Linux 호환)
- Programmable API (`Pipeline.register()` + `run()`) 및 YAML config (`Pipeline.from_config()`) 모두 지원
- SOURCE → SEQUENTIAL / PARALLEL / ASYNC 스텝 조합
- 실시간 진행률 표시 (stderr), 완료 후 stats.json 출력
- 80%+ 테스트 커버리지, ruff + mypy 통과

**Risks & Mitigations:**

| 위험 | 대응 |
|------|------|
| spawn 모드 직렬화 오류 | 모든 Step/Worker 클래스를 모듈 레벨에 정의. Engine에서 조기 검증. |
| asyncio + multiprocessing 큐 데드락 | `run_in_executor` 브리지 + sentinel 수신 시 `asyncio.gather` 드레인 |
| 대용량 처리 OOM (unbounded 큐) | SpillQueue — `execution.queue_size > 0` 설정 시 활성화 |

---

## 다음 작업

_(다음 Phase 작업이 결정되면 여기에 추가)_
