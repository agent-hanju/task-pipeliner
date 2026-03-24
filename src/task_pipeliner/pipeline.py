"""Pipeline facade: step registration, config loading, execution."""

from __future__ import annotations

import logging
import pickle
import time
from pathlib import Path

from task_pipeliner.config import PipelineConfig, load_config
from task_pipeliner.engine import PipelineEngine
from task_pipeliner.exceptions import StepRegistrationError
from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s %(levelname)-5s %(name)s:%(funcName)s:%(lineno)d %(message)s"


# ---------------------------------------------------------------------------
# StepRegistry — Step 클래스 등록소
# ---------------------------------------------------------------------------


class StepRegistry:
    """Step 이름 → Step 클래스 매핑을 관리한다.

    등록 시 pickle 가능 여부를 검증하여,
    spawn 모드 멀티프로세싱에서 문제가 될 클래스를 조기에 차단한다.
    """

    def __init__(self) -> None:
        logger.debug("initialising StepRegistry")
        self._registry: dict[str, type] = {}

    def register(self, name: str, cls: type) -> None:
        """Step 클래스를 name으로 등록한다.

        - 이미 같은 이름이 등록되어 있으면 StepRegistrationError
        - pickle 불가능한 클래스면 StepRegistrationError
        """
        logger.debug("name=%s cls=%s", name, cls.__name__)
        # 중복 등록 방지
        if name in self._registry:
            raise StepRegistrationError(
                f"Step '{name}' is already registered",
                step_name=name,
            )
        # spawn 모드에서 워커 프로세스로 전달하려면 pickle 가능해야 함
        try:
            pickle.dumps(cls)
        except Exception as exc:
            raise StepRegistrationError(
                f"Step '{name}' class {cls.__name__} is not picklable: {exc}",
                step_name=name,
            ) from exc
        self._registry[name] = cls
        logger.debug("registered step '%s'", name)

    def __len__(self) -> int:
        return len(self._registry)

    def get(self, name: str) -> type:
        """name에 해당하는 Step 클래스를 반환한다.

        미등록 시 StepRegistrationError (등록된 이름 목록 포함).
        """
        if name not in self._registry:
            available = sorted(self._registry.keys())
            raise StepRegistrationError(
                f"Step '{name}' is not registered. "
                f"Available: {', '.join(available) if available else '(none)'}",
                step_name=name,
            )
        return self._registry[name]


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
        output_dir: Path,
    ) -> None:
        """Load config (if path) and run the pipeline."""
        logger.debug("config=%s output_dir=%s", config, output_dir)

        if isinstance(config, Path):
            cfg = load_config(config)
        else:
            cfg = config

        stats = StatsCollector()
        log_handler = self._setup_log_handler(output_dir / "pipeline.log")

        t0 = time.monotonic()
        try:
            engine = PipelineEngine(config=cfg, registry=self._registry, stats=stats)
            engine.run(output_dir=output_dir)
            elapsed = time.monotonic() - t0
            logger.info(
                "pipeline completed config=%s elapsed=%.3fs", config, elapsed
            )
        except Exception:
            elapsed = time.monotonic() - t0
            logger.error(
                "pipeline failed config=%s elapsed=%.3fs", config, elapsed,
                exc_info=True,
            )
            raise
        finally:
            self._teardown_log_handler(log_handler)

    @staticmethod
    def _setup_log_handler(path: Path) -> logging.FileHandler:
        """task_pipeliner 로거에 파일 핸들러를 부착한다."""
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
        """파일 핸들러를 플러시하고 제거한다."""
        handler.flush()
        pkg_logger = logging.getLogger("task_pipeliner")
        pkg_logger.removeHandler(handler)
        handler.close()
        logger.debug("log handler detached")
