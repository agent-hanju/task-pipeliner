# TODO

## AsyncStep 추가 (P1 — 필수)

LLM API 호출은 I/O 바운드 + asyncio 기반이므로 `ProcessPoolExecutor` 기반 `ParallelStep`은 구조적으로 맞지 않음.
`AsyncStep` + `AsyncStepRunner`를 추가하면 나머지 파이프라인 구조(DAG, 큐, 통계, 진행률)는 그대로 재사용 가능.

- `AsyncStep(StepBase)` 추가 — `concurrency` 프로퍼티 + `async def process_async(item, emit)` 추상 메서드
- `AsyncStepRunner(BaseStepRunner)` 추가 — 스레드에서 `asyncio.run()` 실행, `asyncio.Semaphore(step.concurrency)` 동시 제한
- `multiprocessing.Queue` → asyncio 브릿지: `loop.run_in_executor(None, self.input_queue.get)` 패턴
- `engine.py`에 `isinstance(step, AsyncStep)` 브랜치 추가

## Retry 지원 (P2 — 권장)

현재 예외 처리가 `logger.warning` + `errored` 증가로 끝남 → LLM 429/503 에러 시 아이템 유실.

- `SequentialStepRunner` / `AsyncStepRunner`에 `retry_count` / `retry_delay` 파라미터 추가
- 지수 백오프 내장 구현 (외부 라이브러리 없이)
- config.yaml에서 step별 설정 가능하게
