"""Pipeline facade: step registration, config loading, execution."""

from __future__ import annotations

import logging
from pathlib import Path

from task_pipeliner.config import PipelineConfig, StepConfig, load_config
from task_pipeliner.engine import PipelineEngine, StepRegistry
from task_pipeliner.io import JsonlSourceStep
from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)

_JSONL_SOURCE_TYPE = "_jsonl_source"


class Pipeline:
    """High-level API for building and running a pipeline."""

    def __init__(self) -> None:
        logger.debug("initialising Pipeline")
        self._registry = StepRegistry()

    def register(self, name: str, cls: type) -> Pipeline:
        """Register a step class. Returns self for chaining."""
        self._registry.register(name, cls)
        return self

    def register_all(self, mapping: dict[str, type]) -> Pipeline:
        """Register multiple step classes at once."""
        for name, cls in mapping.items():
            self._registry.register(name, cls)
        return self

    def run(
        self,
        *,
        config: Path | PipelineConfig,
        inputs: list[Path],
        output_dir: Path,
    ) -> None:
        """Load config (if path), read inputs, and run the full pipeline."""
        logger.debug("config=%s inputs=%d output_dir=%s", config, len(inputs), output_dir)

        if isinstance(config, Path):
            cfg = load_config(config)
        else:
            cfg = config

        # Inject JSONL SOURCE step at the beginning of the pipeline
        source_cfg = StepConfig(type=_JSONL_SOURCE_TYPE, paths=[str(p) for p in inputs])  # type: ignore[call-arg]
        cfg = PipelineConfig(
            pipeline=[source_cfg, *cfg.pipeline],
            execution=cfg.execution,
        )

        stats = StatsCollector()
        stats.setup_log_handler(output_dir / "pipeline.log")

        try:
            # Register the internal JSONL source step
            self._registry.register(_JSONL_SOURCE_TYPE, JsonlSourceStep)
            engine = PipelineEngine(config=cfg, registry=self._registry, stats=stats)

            engine.run(output_dir=output_dir)

            logger.info("pipeline run completed config=%s", config)
        finally:
            # Remove internal registration to avoid duplicate on re-use
            self._registry._registry.pop(_JSONL_SOURCE_TYPE, None)
            stats.flush()
