"""Pipeline facade: step registration, graph assembly, execution."""

from __future__ import annotations

import logging
import pickle
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from task_pipeliner.base import StepBase
from task_pipeliner.config import ExecutionConfig, PipelineConfig, QueueType, load_config
from task_pipeliner.exceptions import StepRegistrationError
from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s %(levelname)-5s %(name)s:%(funcName)s:%(lineno)d %(message)s"


# ---------------------------------------------------------------------------
# _StepGraph — resolved pipeline topology
# ---------------------------------------------------------------------------


@dataclass
class _StepGraph:
    """Resolved pipeline topology passed to PipelineEngine.

    Both the code path (register + step + pipe) and the YAML path
    (config file or PipelineConfig object) converge here before execution.
    """

    nodes: list[tuple[str, StepBase]]
    """Ordered list of (name, step) pairs.  First entry must be a SourceStep."""
    connections: dict[str, dict[str, list[str]]]
    """step_name → tag → [target_step_names]"""
    execution: ExecutionConfig
    checkpoint_dir: Path | None = None
    resume_run_id: str | None = None
    retry_configs: dict[str, tuple[int, float, float]] = field(
        default_factory=dict[str, tuple[int, float, float]]
    )
    """step_name → (retry_count, retry_delay, retry_backoff)"""


# ---------------------------------------------------------------------------
# Pipeline — registration, graph assembly, execution
# ---------------------------------------------------------------------------


class Pipeline:
    """High-level API for building and running a pipeline.

    Code path::

        pipeline = Pipeline(workers=4)
        pipeline.register("source", FileReader)
        pipeline.register("writer", FileWriter)

        source = pipeline.step("src", "source")
        writer = pipeline.step("out", "writer")
        source.pipe(writer)

        stats = pipeline.run(output_dir=Path("out/"))

    YAML path::

        pipeline = Pipeline(workers=4)
        pipeline.register("source", FileReader)
        pipeline.register("writer", FileWriter)

        stats = pipeline.run(config=Path("pipeline.yaml"), output_dir=Path("out/"))
    """

    def __init__(
        self,
        *,
        workers: int = 4,
        chunk_size: int = 100,
        queue_type: QueueType = QueueType.AUTO,
        queue_size: int = 0,
        checkpoint_dir: Path | None = None,
    ) -> None:
        logger.debug(
            "workers=%d chunk_size=%d queue_type=%s queue_size=%d checkpoint_dir=%s",
            workers,
            chunk_size,
            queue_type,
            queue_size,
            checkpoint_dir,
        )
        self._registry: dict[str, type[StepBase]] = {}
        self._steps: dict[str, StepBase] = {}  # insertion-ordered (Python 3.7+)
        self._execution = ExecutionConfig(
            workers=workers,
            chunk_size=chunk_size,
            queue_type=queue_type,
            queue_size=queue_size,
        )
        self._checkpoint_dir = checkpoint_dir

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, name: str, cls: type[StepBase]) -> Pipeline:
        """Register a step class under *name*. Returns self for chaining.

        Raises ``StepRegistrationError`` if *name* is already registered or
        *cls* is not picklable (spawn mode requirement).
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
        logger.debug("registered '%s'", name)
        return self

    def register_all(self, mapping: dict[str, type[StepBase]]) -> Pipeline:
        """Register multiple step classes at once. Returns self for chaining."""
        for name, cls in mapping.items():
            self.register(name, cls)
        return self

    # ------------------------------------------------------------------
    # Step creation (code path)
    # ------------------------------------------------------------------

    def step(self, name: str, type_name: str, **kwargs: Any) -> StepBase:
        """Create and register a step instance for the code path.

        Parameters
        ----------
        name:
            Unique name for this step instance.
        type_name:
            Registered type name (from ``register()``).
        **kwargs:
            Passed to the step class constructor.

        Returns the created step instance so callers can chain ``pipe()``.
        """
        logger.debug("name=%s type=%s kwargs=%s", name, type_name, kwargs)
        if name in self._steps:
            raise StepRegistrationError(
                f"Step name '{name}' is already used",
                step_name=name,
            )
        cls = self._registry_get(type_name)
        instance = cls(**kwargs)
        self._steps[name] = instance
        logger.debug("created step '%s' of type '%s'", name, type_name)
        return instance

    # ------------------------------------------------------------------
    # Internal graph assembly
    # ------------------------------------------------------------------

    def _registry_get(self, name: str) -> type[StepBase]:
        """Look up a registered class. Raises StepRegistrationError if not found."""
        if name not in self._registry:
            available = sorted(self._registry.keys())
            raise StepRegistrationError(
                f"Step '{name}' is not registered. "
                f"Available: {', '.join(available) if available else '(none)'}",
                step_name=name,
            )
        return self._registry[name]

    def _find_step_name(self, target: StepBase) -> str | None:
        """Return the registered name for *target* instance, or None."""
        for name, step in self._steps.items():
            if step is target:
                return name
        return None

    def _build_graph(self) -> _StepGraph:
        """Build a ``_StepGraph`` from steps added via ``step()`` + ``pipe()``."""
        logger.debug("building code-path graph from %d steps", len(self._steps))
        nodes = list(self._steps.items())
        connections: dict[str, dict[str, list[str]]] = {}
        for name, step in nodes:
            raw_conns: dict[str, list[StepBase]] = step.__dict__.get("_connections", {})
            if not raw_conns:
                continue
            conns: dict[str, list[str]] = {}
            for tag, targets in raw_conns.items():
                target_names: list[str] = []
                for target_step in targets:
                    target_name = self._find_step_name(target_step)
                    if target_name is not None:
                        target_names.append(target_name)
                    else:
                        logger.warning(
                            "step '%s' tag '%s' connects to unregistered step — skipped",
                            name,
                            tag,
                        )
                if target_names:
                    conns[tag] = target_names
            if conns:
                connections[name] = conns
        return _StepGraph(
            nodes=nodes,
            connections=connections,
            execution=self._execution,
            checkpoint_dir=self._checkpoint_dir,
        )

    def _build_graph_from_config(
        self,
        path: Path,
        variables: dict[str, Any] | None = None,
    ) -> _StepGraph:
        """Load a YAML config file and build a ``_StepGraph``."""
        logger.debug("path=%s variables=%s", path, variables)
        cfg = load_config(path, variables=variables)
        return self._build_graph_from_config_object(cfg)

    def _build_graph_from_config_object(self, cfg: PipelineConfig) -> _StepGraph:
        """Build a ``_StepGraph`` from a ``PipelineConfig`` object."""
        logger.debug("building config-path graph steps=%d", len(cfg.pipeline))
        enabled_cfgs = [s for s in cfg.pipeline if s.enabled]
        if not enabled_cfgs:
            return _StepGraph(
                nodes=[],
                connections={},
                execution=cfg.execution,
                checkpoint_dir=cfg.checkpoint_dir,
                resume_run_id=cfg.resume_run_id,
            )

        nodes: list[tuple[str, StepBase]] = []
        for step_cfg in enabled_cfgs:
            cls = self._registry_get(step_cfg.type)
            extra = step_cfg.model_extra or {}
            step = cls(**extra)
            nodes.append((step_cfg.name, step))

        enabled_names = {name for name, _ in nodes}
        connections: dict[str, dict[str, list[str]]] = {}
        for step_cfg in enabled_cfgs:
            if step_cfg.outputs is None:
                continue
            conns: dict[str, list[str]] = {}
            for tag, targets in step_cfg.outputs.items():
                target_list = [targets] if isinstance(targets, str) else targets
                valid_targets = []
                for t in target_list:
                    if t in enabled_names:
                        valid_targets.append(t)
                    else:
                        logger.debug(
                            "output target '%s' not in enabled steps — skipped", t
                        )
                if valid_targets:
                    conns[tag] = valid_targets
            if conns:
                connections[step_cfg.name] = conns

        retry_configs: dict[str, tuple[int, float, float]] = {}
        for step_cfg in enabled_cfgs:
            if step_cfg.retry_count > 0:
                retry_configs[step_cfg.name] = (
                    step_cfg.retry_count,
                    step_cfg.retry_delay,
                    step_cfg.retry_backoff,
                )

        return _StepGraph(
            nodes=nodes,
            connections=connections,
            execution=cfg.execution,
            checkpoint_dir=cfg.checkpoint_dir,
            resume_run_id=cfg.resume_run_id,
            retry_configs=retry_configs,
        )

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        output_dir: Path,
        config: Path | PipelineConfig | None = None,
        variables: dict[str, Any] | None = None,
    ) -> StatsCollector:
        """Assemble the graph and run the pipeline.

        Parameters
        ----------
        output_dir:
            Directory for output files (stats.json, progress.log, pipeline.log).
        config:
            ``Path`` → load YAML config.  ``PipelineConfig`` → use directly.
            ``None`` → use the code-path steps added via ``step()``.
        variables:
            Variable overrides for YAML ``${var}`` interpolation.
            Ignored when *config* is not a ``Path``.

        Returns the ``StatsCollector`` with per-step metrics.
        """
        logger.debug(
            "output_dir=%s config=%s variables=%s", output_dir, config, variables
        )

        if config is not None:
            if isinstance(config, Path):
                graph = self._build_graph_from_config(config, variables=variables)
            else:
                graph = self._build_graph_from_config_object(config)
        else:
            graph = self._build_graph()

        # Imported here to break the pipeline ↔ engine circular import
        from task_pipeliner.engine import PipelineEngine  # noqa: PLC0415

        stats = StatsCollector()
        log_handler = self._setup_log_handler(output_dir / "pipeline.log")

        t0 = time.monotonic()
        try:
            engine = PipelineEngine(graph=graph, stats=stats)
            engine.run(output_dir=output_dir)
            elapsed = time.monotonic() - t0
            logger.info("pipeline completed elapsed=%.3fs", elapsed)
        except Exception:
            elapsed = time.monotonic() - t0
            logger.error("pipeline failed elapsed=%.3fs", elapsed, exc_info=True)
            raise
        finally:
            self._teardown_log_handler(log_handler)

        return stats

    # ------------------------------------------------------------------
    # Log handler helpers (unchanged from previous version)
    # ------------------------------------------------------------------

    @staticmethod
    def _setup_log_handler(path: Path) -> logging.FileHandler:
        """Attach a file handler to the task_pipeliner logger."""
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(str(path), encoding="utf-8")
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        pkg_logger = logging.getLogger("task_pipeliner")
        pkg_logger.addHandler(handler)
        pkg_logger.setLevel(logging.DEBUG)
        logger.debug("log handler attached path=%s", path)
        return handler

    @staticmethod
    def _teardown_log_handler(handler: logging.FileHandler) -> None:
        """Flush and detach the file handler."""
        handler.flush()
        pkg_logger = logging.getLogger("task_pipeliner")
        pkg_logger.removeHandler(handler)
        handler.close()
        logger.debug("log handler detached")
