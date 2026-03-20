"""Entry point for the Naver news taxonomy converter pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

from steps import ConvertStep, DeduplicateStep, LoaderStep, PreprocessStep, WriterStep

from task_pipeliner.config import StepConfig, load_config
from task_pipeliner.engine import PipelineEngine, StepRegistry
from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG = Path(__file__).parent / "pipeline_config.yaml"


def main(
    input_paths: list[Path],
    output_dir: Path,
    config_path: Path | None = None,
) -> None:
    """Run the Naver news taxonomy conversion pipeline.

    1. Load config (loader → preprocess → convert → deduplicate → writer)
    2. Override loader paths and writer dir from CLI args
    3. Register all steps
    4. Execute pipeline
    """
    logger.debug(
        "input_paths=%d output_dir=%s config_path=%s",
        len(input_paths),
        output_dir,
        config_path,
    )

    cfg = load_config(config_path or _DEFAULT_CONFIG)

    # CLI override: inject runtime paths/output_dir into config
    for i, step_cfg in enumerate(cfg.pipeline):
        if step_cfg.type == "loader" and input_paths:
            cfg.pipeline[i] = StepConfig(  # type: ignore[call-arg]
                type="loader",
                paths=[str(p) for p in input_paths],
                outputs=step_cfg.outputs,
            )
        elif step_cfg.type == "writer":
            extra = step_cfg.model_extra or {}
            cfg.pipeline[i] = StepConfig(  # type: ignore[call-arg]
                type="writer",
                dir=str(output_dir),
                outputs=step_cfg.outputs,
                **{k: v for k, v in extra.items() if k != "dir"},
            )

    registry = StepRegistry()
    registry.register("loader", LoaderStep)
    registry.register("preprocess", PreprocessStep)
    registry.register("convert", ConvertStep)
    registry.register("deduplicate", DeduplicateStep)
    registry.register("writer", WriterStep)

    stats = StatsCollector()
    stats.setup_log_handler(output_dir / "pipeline.log")

    try:
        engine = PipelineEngine(config=cfg, registry=registry, stats=stats)
        engine.run(output_dir=output_dir)
        logger.info("pipeline run completed config=%s", config_path)
    finally:
        stats.flush()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <output_dir> <input_path> [input_path ...]")
        sys.exit(1)

    out = Path(sys.argv[1])
    inputs = [Path(p) for p in sys.argv[2:]]
    main(inputs, out)
