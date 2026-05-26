"""Execution engine: DAG queue wiring, step runner lifecycle."""

from __future__ import annotations

import logging
import multiprocessing
import shutil
import signal
import tempfile
import threading
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from task_pipeliner.base import AsyncStep, ParallelStep, SourceStep, StepBase
from task_pipeliner.checkpoint import make_checkpoint_store
from task_pipeliner.config import QueueType
from task_pipeliner.exceptions import ConfigValidationError
from task_pipeliner.progress import ProgressReporter
from task_pipeliner.spill_queue import FullDiskQueue, SpillQueue
from task_pipeliner.stats import StatsCollector
from task_pipeliner.step_runners import (
    AsyncStepRunner,
    InputStepRunner,
    ParallelStepRunner,
    QueueLike,
    Sentinel,
    SequentialStepRunner,
)

if TYPE_CHECKING:
    from task_pipeliner.pipeline import _StepGraph

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PipelineEngine — 파이프라인 전체를 조율하는 엔진
# ---------------------------------------------------------------------------


class PipelineEngine:
    """graph에 따라 DAG 큐 토폴로지를 구성하고, StepRunner들을 생성·실행·정리한다."""

    def __init__(
        self,
        *,
        graph: _StepGraph,
        stats: StatsCollector,
    ) -> None:
        logger.debug(
            "graph nodes=%d",
            len(graph.nodes),
        )
        self.graph = graph
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
            "pipeline started nodes=%d workers=%d",
            len(self.graph.nodes),
            self.graph.execution.workers,
        )
        # checkpoint 초기화 — checkpoint_dir 미설정 시 NullCheckpointStore (no-op)
        run_id = self.graph.resume_run_id or str(uuid.uuid4())
        checkpoint = make_checkpoint_store(self.graph.checkpoint_dir, run_id)
        logger.info("run_id=%s checkpoint=%s", run_id, type(checkpoint).__name__)

        # spawn 모드 컨텍스트 — Windows 호환을 위해 fork 대신 spawn 사용
        ctx = multiprocessing.get_context("spawn")

        # ======================================================================
        # 1단계: graph.nodes 검증 + instance 맵 구성
        # ======================================================================

        nodes = self.graph.nodes
        if not nodes:
            logger.info("no nodes in graph — nothing to do")
            return

        instance_by_name: dict[str, StepBase] = {name: step for name, step in nodes}

        # 첫 번째 step은 반드시 SOURCE여야 함
        source_name, source_step = nodes[0]

        if not isinstance(source_step, SourceStep):
            raise ConfigValidationError(
                "Pipeline must have a SourceStep as the first step",
                field="pipeline",
            )
        # 두 번째 이후에 SourceStep이 있으면 에러
        for _, step in nodes[1:]:
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
        processing_names = [name for name, _ in nodes if name != source_name]

        output_queues_map: dict[str, dict[str, list[QueueLike]]] = {
            n: {} for n in instance_by_name
        }

        # 각 처리 step에 하나의 공유 입력 큐를 생성
        # queue_type에 따라 FullDiskQueue / SpillQueue / multiprocessing.Queue 선택.
        queue_size = self.graph.execution.queue_size
        queue_type = self.graph.execution.queue_type
        # FullDiskQueue 인스턴스가 하나라도 생기면 tmpdir이 필요함 (파이프라인 종료 시 삭제)
        _disk_tmpdir: str | None = None
        input_queue_for: dict[str, QueueLike] = {}
        all_queues: list[QueueLike] = []
        for step_name in processing_names:
            q: QueueLike
            if queue_type == QueueType.FULL_DISK:
                if _disk_tmpdir is None:
                    _disk_tmpdir = tempfile.mkdtemp(prefix="task_pipeliner_queue_")
                q = FullDiskQueue(Path(_disk_tmpdir) / step_name)
                logger.info("step '%s' using FullDiskQueue", step_name)
            elif queue_size > 0:
                q = SpillQueue(maxmem=queue_size)
            else:
                q = ctx.Queue()
                # 프로듀서 스레드 종료 시 Queue 내부 feeder thread의
                # pipe flush를 기다리지 않도록 설정.
                q.cancel_join_thread()
            input_queue_for[step_name] = q
            all_queues.append(q)

        # 각 target에 도착할 sentinel 개수 (= 고유 upstream step 수)
        sentinel_count_for: dict[str, int] = {n: 0 for n in processing_names}

        for step_name, _ in nodes:
            step_conns = self.graph.connections.get(step_name, {})
            targets_from_this_step: set[str] = set()
            for tag, target_names in step_conns.items():
                for target_name in target_names:
                    if target_name not in instance_by_name:
                        logger.debug(
                            "output target '%s' not in graph nodes — skipped",
                            target_name,
                        )
                        continue
                    target_q = input_queue_for.get(target_name)
                    if target_q is not None:
                        output_queues_map[step_name].setdefault(tag, []).append(target_q)
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
            checkpoint=checkpoint,
        )

        # 나머지 step들의 StepRunner 생성
        runners: list[SequentialStepRunner | ParallelStepRunner | AsyncStepRunner] = []

        for step_name in processing_names:
            step = instance_by_name[step_name]
            in_q = input_queue_for[step_name]
            out_qs = output_queues_map[step_name]

            # ParallelStep → ProcessPoolExecutor 기반 ParallelStepRunner
            # AsyncStep   → asyncio 이벤트 루프 기반 AsyncStepRunner
            # SequentialStep → 단일 스레드 SequentialStepRunner
            p: SequentialStepRunner | ParallelStepRunner | AsyncStepRunner
            sc = sentinel_count_for[step_name]
            retry_cfg = self.graph.retry_configs.get(step_name, (0, 0.0, 2.0))
            retry_count, retry_delay, retry_backoff = retry_cfg
            if isinstance(step, ParallelStep):
                p = ParallelStepRunner(
                    step=step,
                    step_name=step_name,
                    input_queue=in_q,
                    output_queues=out_qs,
                    stats=self.stats,
                    sentinel_count=sc,
                    workers=self.graph.execution.workers,
                    chunk_size=self.graph.execution.chunk_size,
                )
            elif isinstance(step, AsyncStep):
                p = AsyncStepRunner(
                    step=step,
                    step_name=step_name,
                    input_queue=in_q,
                    output_queues=out_qs,
                    stats=self.stats,
                    sentinel_count=sc,
                    retry_count=retry_count,
                    retry_delay=retry_delay,
                    retry_backoff=retry_backoff,
                )
            else:
                p = SequentialStepRunner(
                    step=step,
                    step_name=step_name,
                    input_queue=in_q,
                    output_queues=out_qs,
                    stats=self.stats,
                    sentinel_count=sc,
                    retry_count=retry_count,
                    retry_delay=retry_delay,
                    retry_backoff=retry_backoff,
                )
            runners.append(p)

        logger.debug("built %d step runners", len(runners))

        # ======================================================================
        # 5단계: 스레드 시작 + 실행 대기
        # ======================================================================

        # 진행률 표시에 사용할 step 이름 목록
        step_names = [name for name, _ in nodes]
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

        # 30초마다 stats를 디스크에 flush하는 daemon 스레드
        _stats_stop = threading.Event()

        def _stats_flush_loop() -> None:
            while not _stats_stop.wait(30):
                self.stats.write_json(output_dir / "stats.json")
                logger.debug("stats flushed periodically")

        stats_flush_thread = threading.Thread(
            target=_stats_flush_loop, daemon=True, name="stats-flusher"
        )

        def _signal_handler(signum: int, frame: Any) -> None:
            logger.error("signal %d received — initiating shutdown", signum)
            self.stats.write_json(output_dir / "stats.json")
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
            stats_flush_thread.start()

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
            # stats flush 스레드 중지
            _stats_stop.set()
            # 진행률 리포터 중지
            reporter.stop()
            # SpillQueue / FullDiskQueue 인스턴스 정리
            for q in all_queues:
                if isinstance(q, SpillQueue):
                    q.close()
                elif isinstance(q, FullDiskQueue):
                    q.close()
            # FullDiskQueue 임시 디렉토리 삭제
            if _disk_tmpdir is not None:
                try:
                    shutil.rmtree(_disk_tmpdir, ignore_errors=True)
                    logger.debug("removed disk queue tmpdir=%s", _disk_tmpdir)
                except OSError:
                    pass
            # 콘솔 로그 전파 복원
            parent_logger.propagate = original_propagate

            # 시그널 핸들러를 원래대로 복원
            signal.signal(signal.SIGINT, original_sigint)
            if hasattr(signal, "SIGBREAK") and original_sigbreak is not None:
                signal.signal(signal.SIGBREAK, original_sigbreak)

            # checkpoint store 정리
            checkpoint.close()
            # 통계 JSON 저장
            self.stats.write_json(output_dir / "stats.json")
            logger.info("pipeline completed")
