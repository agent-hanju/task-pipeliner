"""Helper script for shutdown tests — runs a pipeline in a subprocess."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from dummy_steps import DummySourceStep, FilterEvenStep, PassthroughStep, SlowStep

from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.pipeline import Pipeline


def main() -> None:
    output_dir = Path(sys.argv[1])
    mode = sys.argv[2] if len(sys.argv) > 2 else "slow"

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

    p = Pipeline()
    p.register("source", DummySourceStep)
    p.register("slow", SlowStep)
    p.register("passthrough", PassthroughStep)
    p.register("filter_even", FilterEvenStep)

    if mode == "slow":
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(100)), outputs={"main": "slow"}),
                StepConfig(type="slow", sleep_seconds=0.5),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=10),
        )
    elif mode == "filter":
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(20)), outputs={"main": "filter_even"}),
                StepConfig(type="filter_even"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )
    else:
        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=list(range(10)), outputs={"main": "passthrough"}),
                StepConfig(type="passthrough"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=100, chunk_size=50),
        )

    p.run(config=config, output_dir=output_dir)


if __name__ == "__main__":
    main()
