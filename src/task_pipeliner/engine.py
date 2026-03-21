"""Execution engine: StepRegistry, DAG queue wiring, producer lifecycle."""

from __future__ import annotations

import logging
import multiprocessing
import pickle
import signal
import threading
from pathlib import Path
from typing import Any

from task_pipeliner.base import BaseResult, BaseStep, StepType
from task_pipeliner.config import PipelineConfig
from task_pipeliner.exceptions import ConfigValidationError, StepRegistrationError
from task_pipeliner.producers import (
    InputProducer,
    ParallelProducer,
    Sentinel,
    SequentialProducer,
)
from task_pipeliner.progress import ProgressReporter
from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# StepRegistry — Step 클래스 등록소
# ---------------------------------------------------------------------------


class StepRegistry:
    """Step 이름 → Step 클래스 매핑을 관리한다.

    등록 시 pickle 가능 여부를 검증하여,
    spawn 모드 멀티프로세싱에서 문제가 될 클래스를 조기에 차단한다.
    """

    def __init__(self) -> None:
        logger.debug("initialising StepRegistry")
        self._registry: dict[str, type] = {}

    def register(self, name: str, cls: type) -> None:
        """Step 클래스를 name으로 등록한다.

        - 이미 같은 이름이 등록되어 있으면 StepRegistrationError
        - pickle 불가능한 클래스면 StepRegistrationError
        """
        logger.debug("name=%s cls=%s", name, cls.__name__)
        # 중복 등록 방지
        if name in self._registry:
            raise StepRegistrationError(
                f"Step '{name}' is already registered",
                step_name=name,
            )
        # spawn 모드에서 워커 프로세스로 전달하려면 pickle 가능해야 함
        try:
            pickle.dumps(cls)
        except Exception as exc:
            raise StepRegistrationError(
                f"Step '{name}' class {cls.__name__} is not picklable: {exc}",
                step_name=name,
            ) from exc
        self._registry[name] = cls
        logger.debug("registered step '%s'", name)

    def get(self, name: str) -> type:
        """name에 해당하는 Step 클래스를 반환한다.

        미등록 시 StepRegistrationError (등록된 이름 목록 포함).
        """
        if name not in self._registry:
            available = sorted(self._registry.keys())
            raise StepRegistrationError(
                f"Step '{name}' is not registered. "
                f"Available: {', '.join(available) if available else '(none)'}",
                step_name=name,
            )
        return self._registry[name]


# ---------------------------------------------------------------------------
# Fan-in 큐 머저 — 여러 입력 큐를 하나의 큐로 합친다
# ---------------------------------------------------------------------------


def _start_queue_merger(
    input_queues: list[multiprocessing.Queue[Any]],
    merged_queue: multiprocessing.Queue[Any],
) -> list[threading.Thread]:
    """input_queues 각각에 feeder 스레드를 붙여서 merged_queue로 합친다.

    모든 입력 큐에서 Sentinel이 도착해야 merged_queue에도 Sentinel을 넣는다.
    (하나라도 아직 보내고 있으면 merged_queue는 열려 있음)
    """
    n = len(input_queues)
    lock = threading.Lock()
    remaining = [n]  # 아직 Sentinel을 안 보낸 큐 수 (리스트로 감싸서 클로저에서 수정 가능하게)

    def _feed(q: multiprocessing.Queue[Any]) -> None:
        while True:
            item = q.get()
            if isinstance(item, Sentinel):
                # 이 큐는 끝남 → 남은 카운트 감소
                with lock:
                    remaining[0] -= 1
                    # 마지막 큐까지 다 끝나면 merged_queue에도 Sentinel
                    if remaining[0] == 0:
                        merged_queue.put(Sentinel())
                return
            # 일반 아이템은 그대로 전달
            merged_queue.put(item)

    threads: list[threading.Thread] = []
    for q in input_queues:
        t = threading.Thread(target=_feed, args=(q,), daemon=True)
        t.start()
        threads.append(t)
    logger.debug("started %d merger threads for fan-in", n)
    return threads


# ---------------------------------------------------------------------------
# PipelineEngine — 파이프라인 전체를 조율하는 엔진
# ---------------------------------------------------------------------------


class PipelineEngine:
    """config에 따라 DAG 큐 토폴로지를 구성하고, Producer들을 생성·실행·정리한다."""

    def __init__(
        self,
        *,
        config: PipelineConfig,
        registry: StepRegistry,
        stats: StatsCollector,
    ) -> None:
        logger.debug(
            "config steps=%d registry=%d",
            len(config.pipeline),
            len(registry._registry),
        )
        self.config = config
        self.registry = registry
        self.stats = stats

    def run(
        self,
        *,
        output_dir: Path,
    ) -> None:
        """파이프라인을 구성하고 실행한다.

        큰 흐름:
        1) Step 인스턴스 생성 + SOURCE 검증
        2) DAG 큐 토폴로지 구성 (outputs 설정 기반)
        3) fan-in 머지 (여러 큐 → 하나)
        4) Producer 생성 (Sequential / Parallel)
        5) state dispatch 콜백 주입
        6) 스레드 시작 → join 대기
        7) 결과 수집 + stats 저장
        """
        logger.info(
            "pipeline started steps=%d workers=%d",
            len(self.config.pipeline),
            self.config.execution.workers,
        )
        # spawn 모드 컨텍스트 — Windows 호환을 위해 fork 대신 spawn 사용
        ctx = multiprocessing.get_context("spawn")

        # ======================================================================
        # 1단계: Step 인스턴스 생성 + SOURCE 배치 검증
        # ======================================================================

        # enabled된 step config만 필터링
        enabled_cfgs = [s for s in self.config.pipeline if s.enabled]
        if not enabled_cfgs:
            logger.info("no enabled steps — nothing to do")
            return

        # config의 type 이름으로 registry에서 클래스를 찾아 인스턴스화
        # StepConfig의 extra 필드들이 Step.__init__의 kwargs로 전달됨
        instance_by_type: dict[str, BaseStep[Any]] = {}
        for step_cfg in enabled_cfgs:
            cls = self.registry.get(step_cfg.type)
            extra = step_cfg.model_extra or {}
            instance_by_type[step_cfg.type] = cls(**extra)

        # 첫 번째 step은 반드시 SOURCE여야 함
        source_type = enabled_cfgs[0].type
        source_step = instance_by_type[source_type]

        if source_step.step_type != StepType.SOURCE:
            raise ConfigValidationError(
                "Pipeline must have a SOURCE step as the first step",
                field="pipeline",
            )
        # 두 번째 이후에 SOURCE가 있으면 에러
        for step_cfg in enabled_cfgs[1:]:
            step = instance_by_type[step_cfg.type]
            if step.step_type == StepType.SOURCE:
                raise ConfigValidationError(
                    "SOURCE step must be the first step in the pipeline",
                    field="pipeline",
                )

        # 통계 수집기에 모든 step 등록
        for step in instance_by_type.values():
            self.stats.register(step.name)

        # ======================================================================
        # 2단계: DAG 큐 토폴로지 구성
        # ======================================================================
        #
        # YAML config의 outputs 설정을 기반으로 step 간 큐를 생성한다.
        #
        # 예: StepA.outputs = {"kept": "StepB", "removed": "StepC"}
        #   → StepA의 "kept" 태그 큐 → StepB의 입력 큐
        #   → StepA의 "removed" 태그 큐 → StepC의 입력 큐
        #
        # output_queues_map[step_type][tag] = [큐들]  (한 태그가 여러 step으로 fan-out 가능)
        # input_queues_map[step_type] = [이 step으로 들어오는 큐들]  (여러 step에서 fan-in 가능)

        output_queues_map: dict[str, dict[str, list[multiprocessing.Queue[Any]]]] = {
            t: {} for t in instance_by_type
        }
        input_queues_map: dict[str, list[multiprocessing.Queue[Any]]] = {
            t: [] for t in instance_by_type if t != source_type
        }
        # 비상 종료 시 sentinel 주입을 위해 모든 큐를 추적
        all_queues: list[multiprocessing.Queue[Any]] = []

        for step_cfg in enabled_cfgs:
            if step_cfg.outputs is None:
                continue
            for tag, targets in step_cfg.outputs.items():
                # targets는 "StepB" (str) 또는 ["StepB", "StepC"] (list) 형태
                target_list = [targets] if isinstance(targets, str) else targets
                for target_type in target_list:
                    if target_type not in instance_by_type:
                        # disabled된 step을 가리키면 무시
                        logger.debug(
                            "output target '%s' not in enabled steps — skipped",
                            target_type,
                        )
                        continue
                    # 각 연결(edge)마다 독립적인 큐를 생성
                    q: multiprocessing.Queue[Any] = ctx.Queue()
                    output_queues_map[step_cfg.type].setdefault(tag, []).append(q)
                    input_queues_map[target_type].append(q)
                    all_queues.append(q)

        # ======================================================================
        # 3단계: Fan-in 머지 — 여러 입력 큐를 하나로 합치기
        # ======================================================================
        #
        # 한 step에 여러 upstream이 있으면 (fan-in), 머저 스레드가
        # 여러 큐를 하나의 merged_queue로 합쳐준다.
        #
        # 입력 큐가 0개: upstream이 없음 → 즉시 sentinel을 넣어서 바로 종료되게 함
        # 입력 큐가 1개: 그대로 사용
        # 입력 큐가 2+개: 머저 스레드로 합침

        merged_input: dict[str, multiprocessing.Queue[Any]] = {}
        merger_threads: list[threading.Thread] = []
        # SOURCE를 제외한 나머지 step들 (처리 대상)
        processing_types = [cfg.type for cfg in enabled_cfgs if cfg.type != source_type]

        for step_type in processing_types:
            in_queues = input_queues_map.get(step_type, [])
            if len(in_queues) == 0:
                # upstream 없음 → 빈 큐에 sentinel을 넣어서 즉시 종료
                q = ctx.Queue()
                q.put(Sentinel())
                merged_input[step_type] = q
                all_queues.append(q)
            elif len(in_queues) == 1:
                # 1:1 연결 → 큐를 그대로 사용
                merged_input[step_type] = in_queues[0]
            else:
                # fan-in → 머저 스레드로 합침
                merged_q: multiprocessing.Queue[Any] = ctx.Queue()
                threads = _start_queue_merger(in_queues, merged_q)
                merger_threads.extend(threads)
                merged_input[step_type] = merged_q
                all_queues.append(merged_q)

        # ======================================================================
        # 4단계: Producer 생성
        # ======================================================================

        # SOURCE step 전용 InputProducer — items()를 호출해서 출력 큐에 넣는다
        input_producer = InputProducer(
            step=source_step,
            output_queues=output_queues_map[source_type],
            stats=self.stats,
        )

        # 나머지 step들의 Producer 생성
        result_queues: dict[str, multiprocessing.Queue[Any]] = {}
        producers: list[SequentialProducer | ParallelProducer] = []
        producer_by_name: dict[str, SequentialProducer | ParallelProducer] = {}
        state_events: dict[str, threading.Event] = {}  # state 변경 시 깨우기 위한 이벤트

        for step_type in processing_types:
            step = instance_by_type[step_type]
            in_q = merged_input[step_type]
            out_qs = output_queues_map[step_type]
            rq: multiprocessing.Queue[Any] = ctx.Queue()  # 결과 수집용 큐
            result_queues[step_type] = rq
            evt = threading.Event()
            state_events[step.name] = evt

            # PARALLEL step → ProcessPoolExecutor 기반 ParallelProducer
            # SEQUENTIAL step → 단일 스레드 SequentialProducer
            p: SequentialProducer | ParallelProducer
            if step.step_type == StepType.PARALLEL:
                p = ParallelProducer(
                    step=step,
                    input_queue=in_q,
                    output_queues=out_qs,
                    stats=self.stats,
                    result_queue=rq,
                    state=step.initial_state,
                    workers=self.config.execution.workers,
                    chunk_size=self.config.execution.chunk_size,
                    state_changed_event=evt,
                )
            else:
                p = SequentialProducer(
                    step=step,
                    input_queue=in_q,
                    output_queues=out_qs,
                    stats=self.stats,
                    result_queue=rq,
                    state=step.initial_state,
                    state_changed_event=evt,
                )
            producers.append(p)
            producer_by_name[step.name] = p

        # ======================================================================
        # 5단계: State dispatch 콜백 주입
        # ======================================================================
        #
        # step.set_step_state("TargetStep", value) 호출 시:
        #   1) producer_by_name에서 대상 Producer를 찾아서 state를 교체
        #   2) state_events를 set()해서 is_ready() 재평가를 트리거
        #
        # 이 콜백은 메인 프로세스에서만 동작한다 (pickle 불가 → 워커에선 None)

        def _make_state_dispatch(
            pmap: dict[str, SequentialProducer | ParallelProducer],
            emap: dict[str, threading.Event],
        ) -> Any:
            def dispatch(target_name: str, state: Any) -> None:
                target = pmap.get(target_name)
                if target is None:
                    raise ValueError(
                        f"set_step_state target '{target_name}' not found. "
                        f"Available: {sorted(pmap.keys())}"
                    )
                # Producer의 state를 교체
                target.state = state
                # 대상 step의 is_ready() 재평가를 위해 이벤트 시그널
                target_evt = emap.get(target_name)
                if target_evt is not None:
                    target_evt.set()
                logger.info(
                    "state dispatched to %s", target_name,
                )
            return dispatch

        state_dispatch = _make_state_dispatch(producer_by_name, state_events)
        # 모든 step에 콜백 주입 (워커로 pickle될 때는 __getstate__에서 제외됨)
        for step in instance_by_type.values():
            step._state_dispatch = state_dispatch

        logger.debug("built %d producers", len(producers))

        # ======================================================================
        # 6단계: 스레드 시작 + 실행 대기
        # ======================================================================

        # 진행률 표시에 사용할 step 이름 목록
        step_names = [instance_by_type[cfg.type].name for cfg in enabled_cfgs]
        # InputProducer는 별도 스레드에서 실행
        feeder = threading.Thread(target=input_producer.run, daemon=True)
        # 각 Producer도 별도 스레드에서 실행
        producer_threads = [threading.Thread(target=p.run, daemon=True) for p in producers]

        # Ctrl+C / SIGBREAK 시 graceful shutdown을 위한 이벤트
        shutdown_event = threading.Event()
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigbreak = (
            signal.getsignal(signal.SIGBREAK) if hasattr(signal, "SIGBREAK") else None
        )

        def _signal_handler(signum: int, frame: Any) -> None:
            logger.error("signal %d received — initiating shutdown", signum)
            shutdown_event.set()

        # 실행 중에는 콘솔 로그 전파를 끔 (ProgressReporter가 대신 표시)
        parent_logger = logging.getLogger("task_pipeliner")
        original_propagate = parent_logger.propagate

        reporter = ProgressReporter(
            stats=self.stats,
            step_names=step_names,
            output_dir=output_dir,
        )

        try:
            # 콘솔 로그 전파 끄기 (진행률 표시와 겹치지 않도록)
            parent_logger.propagate = False

            # 시그널 핸들러 등록
            signal.signal(signal.SIGINT, _signal_handler)
            if hasattr(signal, "SIGBREAK"):
                signal.signal(signal.SIGBREAK, _signal_handler)

            output_dir.mkdir(parents=True, exist_ok=True)
            # 진행률 리포터 시작 (별도 스레드에서 주기적으로 콘솔 출력)
            reporter.start()

            # 모든 스레드 시작
            feeder.start()
            for t in producer_threads:
                t.start()

            # 스레드 종료 대기
            join_timeout = 5 if shutdown_event.is_set() else 30
            feeder.join(timeout=join_timeout)
            for t in merger_threads:
                t.join(timeout=join_timeout)
            for t in producer_threads:
                t.join(timeout=join_timeout)

            # 시그널로 종료된 경우 — 모든 큐에 sentinel을 강제 주입하여 스레드 탈출 유도
            if shutdown_event.is_set():
                for q in all_queues:
                    try:
                        q.put_nowait(Sentinel())
                    except Exception:
                        pass
                for t in producer_threads:
                    t.join(timeout=5)

            # join 후에도 살아있는 스레드가 있으면 경고
            for t in producer_threads:
                if t.is_alive():
                    logger.warning("producer thread still alive after join timeout")

        # ======================================================================
        # 7단계: 정리 — 결과 수집, stats 저장, 시그널 복원
        # ======================================================================
        finally:
            # 진행률 리포터 중지
            reporter.stop()
            # 콘솔 로그 전파 복원
            parent_logger.propagate = original_propagate

            # 시그널 핸들러를 원래대로 복원
            signal.signal(signal.SIGINT, original_sigint)
            if hasattr(signal, "SIGBREAK") and original_sigbreak is not None:
                signal.signal(signal.SIGBREAK, original_sigbreak)

            import queue as _queue

            # 각 step의 결과(BaseResult)를 result_queue에서 꺼내서 파일로 기록
            output_dir.mkdir(parents=True, exist_ok=True)
            for step_type in processing_types:
                rq = result_queues[step_type]
                step = instance_by_type[step_type]
                try:
                    result: BaseResult = rq.get(timeout=2)
                    result.write(output_dir)
                    logger.debug("result written for step %s", step.name)
                except _queue.Empty:
                    logger.debug("no result for step %s", step.name)

            # 통계 JSON 저장
            self.stats.write_json(output_dir / "stats.json")
            logger.info("pipeline completed")
