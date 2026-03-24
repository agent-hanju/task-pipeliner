"""Execution engine: DAG queue wiring, step runner lifecycle."""

from __future__ import annotations

import logging
import multiprocessing
import signal
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from task_pipeliner.base import ParallelStep, SourceStep, StepBase
from task_pipeliner.config import PipelineConfig
from task_pipeliner.exceptions import ConfigValidationError
from task_pipeliner.progress import ProgressReporter
from task_pipeliner.stats import StatsCollector
from task_pipeliner.step_runners import (
    InputStepRunner,
    ParallelStepRunner,
    Sentinel,
    SequentialStepRunner,
)

if TYPE_CHECKING:
    from task_pipeliner.pipeline import StepRegistry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PipelineEngine — 파이프라인 전체를 조율하는 엔진
# ---------------------------------------------------------------------------


class PipelineEngine:
    """config에 따라 DAG 큐 토폴로지를 구성하고, StepRunner들을 생성·실행·정리한다."""

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
            len(registry),
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
        1) Step 인스턴스 생성 + SourceStep 검증
        2) DAG 큐 토폴로지 구성 (공유 입력 큐 + sentinel_count)
        3) StepRunner 생성 (Sequential / Parallel) + state dispatch
        4) 스레드 시작 → join 대기
        5) 결과 수집 + stats 저장
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
        # step_cfg.name이 인스턴스 고유 키 (생략 시 type과 동일)
        instance_by_name: dict[str, StepBase] = {}
        for step_cfg in enabled_cfgs:
            cls = self.registry.get(step_cfg.type)
            extra = step_cfg.model_extra or {}
            step = cls(**extra)
            instance_by_name[step_cfg.name] = step

        # 첫 번째 step은 반드시 SOURCE여야 함
        source_name = enabled_cfgs[0].name
        source_step = instance_by_name[source_name]

        if not isinstance(source_step, SourceStep):
            raise ConfigValidationError(
                "Pipeline must have a SourceStep as the first step",
                field="pipeline",
            )
        # 두 번째 이후에 SourceStep이 있으면 에러
        for step_cfg in enabled_cfgs[1:]:
            step = instance_by_name[step_cfg.name]
            if isinstance(step, SourceStep):
                raise ConfigValidationError(
                    "SourceStep must be the first step in the pipeline",
                    field="pipeline",
                )

        # 통계 수집기에 모든 step 등록
        for name in instance_by_name:
            self.stats.register(name)

        # ======================================================================
        # 2단계: DAG 큐 토폴로지 구성
        # ======================================================================
        #
        # YAML config의 outputs 설정을 기반으로 step 간 큐를 생성한다.
        #
        # 예: StepA.outputs = {"kept": "StepB", "removed": "StepC"}
        #   → StepA의 "kept" 태그 출력 → StepB의 입력 큐
        #   → StepA의 "removed" 태그 출력 → StepC의 입력 큐
        #
        # Fan-in 처리: 여러 upstream이 같은 target을 가리키면 하나의 공유 큐에 put.
        # downstream Producer는 sentinel_count만큼 Sentinel을 받아야 종료한다.
        #
        # output_queues_map[step_name][tag] = [큐들]  (한 태그가 여러 step으로 fan-out 가능)

        # SOURCE를 제외한 나머지 step들 (처리 대상)
        processing_names = [cfg.name for cfg in enabled_cfgs if cfg.name != source_name]

        output_queues_map: dict[str, dict[str, list[multiprocessing.Queue[Any]]]] = {
            n: {} for n in instance_by_name
        }

        # 각 처리 step에 하나의 공유 입력 큐를 생성
        input_queue_for: dict[str, multiprocessing.Queue[Any]] = {}
        all_queues: list[multiprocessing.Queue[Any]] = []
        for step_name in processing_names:
            q: multiprocessing.Queue[Any] = ctx.Queue()
            # 프로듀서 스레드 종료 시 Queue 내부 feeder thread의
            # pipe flush를 기다리지 않도록 설정.
            q.cancel_join_thread()
            input_queue_for[step_name] = q
            all_queues.append(q)

        # 각 target에 도착할 sentinel 개수 (= 고유 upstream step 수)
        sentinel_count_for: dict[str, int] = {n: 0 for n in processing_names}

        for step_cfg in enabled_cfgs:
            if step_cfg.outputs is None:
                continue
            # 이 step에서 연결되는 target을 추적 (sentinel 카운팅용)
            targets_from_this_step: set[str] = set()
            for tag, targets in step_cfg.outputs.items():
                # targets는 "StepB" (str) 또는 ["StepB", "StepC"] (list) 형태
                target_list = [targets] if isinstance(targets, str) else targets
                for target_name in target_list:
                    if target_name not in instance_by_name:
                        # disabled된 step을 가리키면 무시
                        logger.debug(
                            "output target '%s' not in enabled steps — skipped",
                            target_name,
                        )
                        continue
                    q = input_queue_for[target_name]
                    output_queues_map[step_cfg.name].setdefault(tag, []).append(q)
                    targets_from_this_step.add(target_name)
            for target_name in targets_from_this_step:
                sentinel_count_for[target_name] += 1

        # upstream이 없는 step → sentinel을 넣어서 즉시 종료되게 함
        for step_name in processing_names:
            if sentinel_count_for[step_name] == 0:
                input_queue_for[step_name].put(Sentinel())
                sentinel_count_for[step_name] = 1

        # ======================================================================
        # 4단계: StepRunner 생성
        # ======================================================================

        # SOURCE step 전용 InputStepRunner — items()를 호출해서 출력 큐에 넣는다
        input_runner = InputStepRunner(
            step=source_step,
            step_name=source_name,
            output_queues=output_queues_map[source_name],
            stats=self.stats,
        )

        # 나머지 step들의 StepRunner 생성
        #
        # state_dispatch 콜백: StepRunner가 step.close() 후 get_output_state()를
        # 호출하고, 반환된 {target_name: state} 매핑을 이 콜백으로 dispatch한다.
        # runner_by_name / state_events를 클로저로 캡처하므로,
        # StepRunner 생성 후에도 새로 추가된 StepRunner를 참조할 수 있다.
        runners: list[SequentialStepRunner | ParallelStepRunner] = []
        runner_by_name: dict[str, SequentialStepRunner | ParallelStepRunner] = {}
        state_events: dict[str, threading.Event] = {}

        def _state_dispatch(target_name: str, state: Any) -> None:
            target = runner_by_name.get(target_name)
            if target is None:
                raise ValueError(
                    f"state dispatch target '{target_name}' not found. "
                    f"Available: {sorted(runner_by_name.keys())}"
                )
            target.state = state
            target_evt = state_events.get(target_name)
            if target_evt is not None:
                target_evt.set()
            logger.info("state dispatched to %s", target_name)

        for step_name in processing_names:
            step = instance_by_name[step_name]
            in_q = input_queue_for[step_name]
            out_qs = output_queues_map[step_name]
            evt = threading.Event()
            state_events[step_name] = evt

            # ParallelStep → ProcessPoolExecutor 기반 ParallelStepRunner
            # SequentialStep → 단일 스레드 SequentialStepRunner
            p: SequentialStepRunner | ParallelStepRunner
            sc = sentinel_count_for[step_name]
            if isinstance(step, ParallelStep):
                p = ParallelStepRunner(
                    step=step,
                    step_name=step_name,
                    input_queue=in_q,
                    output_queues=out_qs,
                    stats=self.stats,
                    state=step.initial_state,
                    sentinel_count=sc,
                    workers=self.config.execution.workers,
                    chunk_size=self.config.execution.chunk_size,
                    state_changed_event=evt,
                    state_dispatch=_state_dispatch,
                )
            else:
                p = SequentialStepRunner(
                    step=step,
                    step_name=step_name,
                    input_queue=in_q,
                    output_queues=out_qs,
                    stats=self.stats,
                    state=step.initial_state,
                    sentinel_count=sc,
                    state_changed_event=evt,
                    state_dispatch=_state_dispatch,
                )
            runners.append(p)
            runner_by_name[step_name] = p

        logger.debug("built %d step runners", len(runners))

        # ======================================================================
        # 5단계: 스레드 시작 + 실행 대기
        # ======================================================================

        # 진행률 표시에 사용할 step 이름 목록
        step_names = [cfg.name for cfg in enabled_cfgs]
        # InputStepRunner는 별도 스레드에서 실행
        feeder = threading.Thread(target=input_runner.run, daemon=True)
        # 각 StepRunner도 별도 스레드에서 실행
        runner_threads = [threading.Thread(target=p.run, daemon=True) for p in runners]

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
            for t in runner_threads:
                t.start()

            # 스레드 종료 대기
            # 정상 실행: 모든 스레드가 끝날 때까지 무제한 대기
            # 시그널 종료: 5초 타임아웃 후 강제 sentinel 주입
            join_timeout = 5 if shutdown_event.is_set() else None
            feeder.join(timeout=join_timeout)
            for t in runner_threads:
                t.join(timeout=join_timeout)

            # 시그널로 종료된 경우 — 모든 큐에 sentinel을 강제 주입하여 스레드 탈출 유도
            if shutdown_event.is_set():
                for q in all_queues:
                    try:
                        q.put_nowait(Sentinel())
                    except Exception:
                        pass
                for t in runner_threads:
                    t.join(timeout=5)

            # join 후에도 살아있는 스레드가 있으면 경고
            for t in runner_threads:
                if t.is_alive():
                    logger.warning("runner thread still alive after join timeout")

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

            # 통계 JSON 저장
            self.stats.write_json(output_dir / "stats.json")
            logger.info("pipeline completed")
