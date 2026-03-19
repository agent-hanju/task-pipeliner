"""Execution engine: StepRegistry, queue wiring, producer lifecycle."""

from __future__ import annotations

import logging
import multiprocessing
import pickle
import signal
import threading
from collections.abc import Generator
from pathlib import Path
from typing import Any

from task_pipeliner.base import BaseResult, BaseStep, StepType
from task_pipeliner.config import PipelineConfig
from task_pipeliner.exceptions import StepRegistrationError
from task_pipeliner.io import JsonlWriter
from task_pipeliner.producers import ParallelProducer, Sentinel, SequentialProducer, is_sentinel
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
# PipelineEngine
# ---------------------------------------------------------------------------


class PipelineEngine:
    """Orchestrates a pipeline: wires queues, runs producers, collects results."""

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
        input_items: Generator[Any, None, None] | Any,
        writer: JsonlWriter,
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
        enabled_steps = [s for s in self.config.pipeline if s.enabled]
        if not enabled_steps:
            logger.info("no enabled steps — nothing to do")
            return

        # Resolve step classes and instantiate
        step_instances: list[BaseStep[Any]] = []
        for step_cfg in enabled_steps:
            cls = self.registry.get(step_cfg.type)
            extra = step_cfg.model_extra or {}
            step_instances.append(cls(**extra))

        # Build queue chain
        queues: list[multiprocessing.Queue[Any]] = []
        for _ in range(len(step_instances) + 1):
            queues.append(ctx.Queue(maxsize=self.config.execution.queue_size))
        # queues[0] = input, queues[i+1] = output of step i
        # queues[-1] = final output

        # Result queues — one per step
        result_queues: list[multiprocessing.Queue[Any]] = [ctx.Queue() for _ in step_instances]

        # Register stats for each step
        for step in step_instances:
            self.stats.register(step.name)

        # Build producers
        producers: list[SequentialProducer | ParallelProducer] = []
        for i, step in enumerate(step_instances):
            in_q = queues[i]
            out_qs = [queues[i + 1]]
            rq = result_queues[i]
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

        # Feeder thread — pushes input items into queues[0]
        def _feed() -> None:
            try:
                for item in input_items:
                    queues[0].put(item)
            finally:
                queues[0].put(Sentinel())

        feeder = threading.Thread(target=_feed, daemon=True)

        # Producer threads — each runs producer.run() in a thread
        producer_threads = [threading.Thread(target=p.run, daemon=True) for p in producers]

        # Shutdown flag
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

            # Start feeder + all producers
            feeder.start()
            for t in producer_threads:
                t.start()

            # Drain final output queue → writer
            final_q = queues[-1]
            while True:
                if shutdown_event.is_set():
                    logger.warning("shutdown requested — stopping drain")
                    break
                try:
                    obj = final_q.get(timeout=1)
                except Exception:
                    if shutdown_event.is_set():
                        break
                    continue
                if is_sentinel(obj):
                    break
                writer.write_kept(obj)

            # On shutdown, inject sentinels to unblock blocked producers
            if shutdown_event.is_set():
                for q in queues:
                    try:
                        q.put_nowait(Sentinel())
                    except Exception:
                        pass

            # Wait for all threads to finish
            join_timeout = 5 if shutdown_event.is_set() else 30
            feeder.join(timeout=join_timeout)
            for t in producer_threads:
                t.join(timeout=join_timeout)
            for t in producer_threads:
                if t.is_alive():
                    logger.warning("producer thread still alive after join timeout")

        finally:
            # Restore original signal handlers
            signal.signal(signal.SIGINT, original_sigint)
            if hasattr(signal, "SIGBREAK") and original_sigbreak is not None:
                signal.signal(signal.SIGBREAK, original_sigbreak)

            # Collect and write results
            import queue as _queue

            for i, rq in enumerate(result_queues):
                try:
                    result: BaseResult = rq.get(timeout=2)
                    result.write(output_dir)
                    logger.debug("result written for step %s", step_instances[i].name)
                except _queue.Empty:
                    logger.debug("no result for step %s", step_instances[i].name)

            # Write stats
            self.stats.write_json(output_dir / "stats.json")
            logger.info("pipeline completed")
