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
from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# StepRegistry
# ---------------------------------------------------------------------------


class StepRegistry:
    """Maintains a mapping of step names to step classes with pickle validation."""

    def __init__(self) -> None:
        logger.debug("initialising StepRegistry")
        self._registry: dict[str, type] = {}

    def register(self, name: str, cls: type) -> None:
        """Register a step class under *name*.

        Raises StepRegistrationError on duplicate names or unpicklable classes.
        """
        logger.debug("name=%s cls=%s", name, cls.__name__)
        if name in self._registry:
            raise StepRegistrationError(
                f"Step '{name}' is already registered",
                step_name=name,
            )
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
        """Return the step class for *name*.

        Raises StepRegistrationError if not found (includes available names).
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
# Queue merger for fan-in (multiple input queues → single merged queue)
# ---------------------------------------------------------------------------


def _start_queue_merger(
    input_queues: list[multiprocessing.Queue[Any]],
    merged_queue: multiprocessing.Queue[Any],
) -> list[threading.Thread]:
    """Start feeder threads that merge *input_queues* into *merged_queue*.

    Each feeder thread reads from one input queue. When ALL input queues
    have delivered a Sentinel, a single Sentinel is placed into *merged_queue*.
    """
    n = len(input_queues)
    lock = threading.Lock()
    remaining = [n]

    def _feed(q: multiprocessing.Queue[Any]) -> None:
        while True:
            item = q.get()
            if isinstance(item, Sentinel):
                with lock:
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        merged_queue.put(Sentinel())
                return
            merged_queue.put(item)

    threads: list[threading.Thread] = []
    for q in input_queues:
        t = threading.Thread(target=_feed, args=(q,), daemon=True)
        t.start()
        threads.append(t)
    logger.debug("started %d merger threads for fan-in", n)
    return threads


# ---------------------------------------------------------------------------
# PipelineEngine
# ---------------------------------------------------------------------------


class PipelineEngine:
    """Orchestrates a pipeline: wires queues as a DAG, runs producers, collects results."""

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
        """Build and execute the full pipeline."""
        logger.info(
            "pipeline started steps=%d workers=%d",
            len(self.config.pipeline),
            self.config.execution.workers,
        )
        ctx = multiprocessing.get_context("spawn")

        # Filter enabled steps
        enabled_cfgs = [s for s in self.config.pipeline if s.enabled]
        if not enabled_cfgs:
            logger.info("no enabled steps — nothing to do")
            return

        # Resolve step classes and instantiate, keyed by config type
        instance_by_type: dict[str, BaseStep[Any]] = {}
        for step_cfg in enabled_cfgs:
            cls = self.registry.get(step_cfg.type)
            extra = step_cfg.model_extra or {}
            instance_by_type[step_cfg.type] = cls(**extra)

        # Validate SOURCE step placement
        source_type = enabled_cfgs[0].type
        source_step = instance_by_type[source_type]

        if source_step.step_type != StepType.SOURCE:
            raise ConfigValidationError(
                "Pipeline must have a SOURCE step as the first step",
                field="pipeline",
            )
        for step_cfg in enabled_cfgs[1:]:
            step = instance_by_type[step_cfg.type]
            if step.step_type == StepType.SOURCE:
                raise ConfigValidationError(
                    "SOURCE step must be the first step in the pipeline",
                    field="pipeline",
                )

        # Register stats for all steps
        for step in instance_by_type.values():
            self.stats.register(step.name)

        # ------------------------------------------------------------------
        # Build DAG queue topology from config outputs
        # ------------------------------------------------------------------
        queue_size = self.config.execution.queue_size

        # output_queues_map: step_type → {tag → [queues]}
        output_queues_map: dict[str, dict[str, list[multiprocessing.Queue[Any]]]] = {
            t: {} for t in instance_by_type
        }
        # input_queues_map: step_type → [queues feeding into this step]
        input_queues_map: dict[str, list[multiprocessing.Queue[Any]]] = {
            t: [] for t in instance_by_type if t != source_type
        }
        # Track all queues for shutdown sentinel injection
        all_queues: list[multiprocessing.Queue[Any]] = []

        for step_cfg in enabled_cfgs:
            if step_cfg.outputs is None:
                continue
            for tag, targets in step_cfg.outputs.items():
                target_list = [targets] if isinstance(targets, str) else targets
                for target_type in target_list:
                    if target_type not in instance_by_type:
                        logger.debug(
                            "output target '%s' not in enabled steps — skipped",
                            target_type,
                        )
                        continue
                    q: multiprocessing.Queue[Any] = ctx.Queue(maxsize=queue_size)
                    output_queues_map[step_cfg.type].setdefault(tag, []).append(q)
                    input_queues_map[target_type].append(q)
                    all_queues.append(q)

        # ------------------------------------------------------------------
        # Merge multiple input queues (fan-in) into single queue per step
        # ------------------------------------------------------------------
        merged_input: dict[str, multiprocessing.Queue[Any]] = {}
        merger_threads: list[threading.Thread] = []
        processing_types = [cfg.type for cfg in enabled_cfgs if cfg.type != source_type]

        for step_type in processing_types:
            in_queues = input_queues_map.get(step_type, [])
            if len(in_queues) == 0:
                # No upstream — create queue with immediate sentinel
                q = ctx.Queue(maxsize=queue_size)
                q.put(Sentinel())
                merged_input[step_type] = q
                all_queues.append(q)
            elif len(in_queues) == 1:
                merged_input[step_type] = in_queues[0]
            else:
                # Fan-in: merge multiple input queues
                merged_q: multiprocessing.Queue[Any] = ctx.Queue(maxsize=queue_size)
                threads = _start_queue_merger(in_queues, merged_q)
                merger_threads.extend(threads)
                merged_input[step_type] = merged_q
                all_queues.append(merged_q)

        # ------------------------------------------------------------------
        # Create producers
        # ------------------------------------------------------------------

        # InputProducer for SOURCE step
        input_producer = InputProducer(
            step=source_step,
            output_queues=output_queues_map[source_type],
            stats=self.stats,
        )

        # Result queues and step producers
        result_queues: dict[str, multiprocessing.Queue[Any]] = {}
        producers: list[SequentialProducer | ParallelProducer] = []

        for step_type in processing_types:
            step = instance_by_type[step_type]
            in_q = merged_input[step_type]
            out_qs = output_queues_map[step_type]
            rq: multiprocessing.Queue[Any] = ctx.Queue()
            result_queues[step_type] = rq

            if step.step_type == StepType.PARALLEL:
                producers.append(
                    ParallelProducer(
                        step=step,
                        input_queue=in_q,
                        output_queues=out_qs,
                        stats=self.stats,
                        result_queue=rq,
                        workers=self.config.execution.workers,
                        chunk_size=self.config.execution.chunk_size,
                    )
                )
            else:
                producers.append(
                    SequentialProducer(
                        step=step,
                        input_queue=in_q,
                        output_queues=out_qs,
                        stats=self.stats,
                        result_queue=rq,
                    )
                )

        logger.debug("built %d producers", len(producers))

        # ------------------------------------------------------------------
        # Run pipeline
        # ------------------------------------------------------------------
        feeder = threading.Thread(target=input_producer.run, daemon=True)
        producer_threads = [threading.Thread(target=p.run, daemon=True) for p in producers]

        shutdown_event = threading.Event()
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigbreak = (
            signal.getsignal(signal.SIGBREAK) if hasattr(signal, "SIGBREAK") else None
        )

        def _signal_handler(signum: int, frame: Any) -> None:
            logger.error("signal %d received — initiating shutdown", signum)
            shutdown_event.set()

        try:
            signal.signal(signal.SIGINT, _signal_handler)
            if hasattr(signal, "SIGBREAK"):
                signal.signal(signal.SIGBREAK, _signal_handler)

            feeder.start()
            for t in producer_threads:
                t.start()

            join_timeout = 5 if shutdown_event.is_set() else 30
            feeder.join(timeout=join_timeout)
            for t in merger_threads:
                t.join(timeout=join_timeout)
            for t in producer_threads:
                t.join(timeout=join_timeout)

            if shutdown_event.is_set():
                for q in all_queues:
                    try:
                        q.put_nowait(Sentinel())
                    except Exception:
                        pass
                for t in producer_threads:
                    t.join(timeout=5)

            for t in producer_threads:
                if t.is_alive():
                    logger.warning("producer thread still alive after join timeout")

        finally:
            signal.signal(signal.SIGINT, original_sigint)
            if hasattr(signal, "SIGBREAK") and original_sigbreak is not None:
                signal.signal(signal.SIGBREAK, original_sigbreak)

            import queue as _queue

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

            self.stats.write_json(output_dir / "stats.json")
            logger.info("pipeline completed")
