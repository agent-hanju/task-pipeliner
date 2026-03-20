"""Helper script for shutdown tests — runs a pipeline in a subprocess."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from dummy_steps import FilterEvenStep, PassthroughStep, SlowStep

from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.engine import PipelineEngine, StepRegistry
from task_pipeliner.stats import StatsCollector


def main() -> None:
    output_dir = Path(sys.argv[1])
    mode = sys.argv[2] if len(sys.argv) > 2 else "slow"

    # Set up file logging
    log_path = output_dir / "pipeline.log"
    output_dir.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(str(log_path), encoding="utf-8")
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-5s %(name)s:%(funcName)s:%(lineno)d %(message)s"
        )
    )
    root = logging.getLogger("task_pipeliner")
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)

    registry = StepRegistry()
    registry.register("slow", SlowStep)
    registry.register("passthrough", PassthroughStep)
    registry.register("filter_even", FilterEvenStep)

    if mode == "slow":
        config = PipelineConfig(
            pipeline=[StepConfig(type="slow", sleep_seconds=0.5)],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=10),
        )
        items = iter(range(100))
    elif mode == "filter":
        config = PipelineConfig(
            pipeline=[StepConfig(type="filter_even")],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        items = iter(range(20))
    else:
        config = PipelineConfig(
            pipeline=[StepConfig(type="passthrough")],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
        items = iter(range(10))

    stats = StatsCollector()
    engine = PipelineEngine(config=config, registry=registry, stats=stats)

    engine.run(input_items=items, output_dir=output_dir)


if __name__ == "__main__":
    main()
