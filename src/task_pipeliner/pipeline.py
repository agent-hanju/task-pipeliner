"""Pipeline facade: step registration, config loading, execution."""

from __future__ import annotations

import logging
from pathlib import Path

from task_pipeliner.config import PipelineConfig, load_config
from task_pipeliner.engine import PipelineEngine, StepRegistry
from task_pipeliner.io import JsonlReader, JsonlWriter
from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)


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

        stats = StatsCollector()
        stats.setup_log_handler(output_dir / "pipeline.log")

        try:
            engine = PipelineEngine(config=cfg, registry=self._registry, stats=stats)
            reader = JsonlReader(inputs)
            input_items = reader.read()

            with JsonlWriter(output_dir) as writer:
                engine.run(input_items=input_items, writer=writer, output_dir=output_dir)

            logger.info("pipeline run completed config=%s", config)
        finally:
            stats.flush()
