"""Pipeline execution statistics collection."""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import orjson

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s %(levelname)-5s %(name)s:%(funcName)s:%(lineno)d %(message)s"


@dataclass
class StepStats:
    step_name: str
    processed: int = 0
    errored: int = 0
    emitted: dict[str, int] = field(default_factory=dict)
    _start_time: float = field(default_factory=time.monotonic)
    _end_time: float | None = None
    first_item_at: float | None = None
    processing_ns: int = 0
    idle_ns: int = 0
    idle_count: int = 0
    current_state: str = "waiting"

    def finish(self) -> None:
        self._end_time = time.monotonic()

    @property
    def elapsed_seconds(self) -> float:
        end = self._end_time if self._end_time is not None else time.monotonic()
        return end - self._start_time

    def to_dict(self) -> dict[str, object]:
        initial_wait: float | None = None
        if self.first_item_at is not None:
            initial_wait = round(self.first_item_at - self._start_time, 4)

        proc_sec = round(self.processing_ns / 1_000_000_000, 4)
        proc_avg: float | None = None
        if self.processed > 0:
            proc_avg = round(self.processing_ns / self.processed / 1_000_000, 4)

        idle_sec = round(self.idle_ns / 1_000_000_000, 4)
        idle_avg: float | None = None
        if self.idle_count > 0:
            idle_avg = round(self.idle_ns / self.idle_count / 1_000_000, 4)

        return {
            "step_name": self.step_name,
            "processed": self.processed,
            "errored": self.errored,
            "emitted": dict(self.emitted),
            "elapsed_seconds": round(self.elapsed_seconds, 4),
            "initial_wait_seconds": initial_wait,
            "processing_seconds": proc_sec,
            "processing_avg_ms": proc_avg,
            "idle_seconds": idle_sec,
            "idle_avg_ms": idle_avg,
            "current_state": self.current_state,
        }


class StatsCollector:
    def __init__(self) -> None:
        self._stats: dict[str, StepStats] = {}
        self._lock = threading.Lock()
        self._handler: logging.FileHandler | None = None
        self.total_items: int = 0

    def register(self, step_name: str) -> StepStats:
        logger.debug("step_name=%s", step_name)
        s = StepStats(step_name=step_name)
        self._stats[step_name] = s
        logger.debug("registered step %s", step_name)
        return s

    def increment(self, step_name: str, field: str, n: int = 1) -> None:
        with self._lock:
            stats = self._stats[step_name]
            setattr(stats, field, getattr(stats, field) + n)

    def increment_emitted(self, step_name: str, tag: str, n: int = 1) -> None:
        with self._lock:
            emitted = self._stats[step_name].emitted
            emitted[tag] = emitted.get(tag, 0) + n

    def set_total_items(self, n: int) -> None:
        with self._lock:
            self.total_items = n

    def record_first_item(self, step_name: str) -> None:
        with self._lock:
            stats = self._stats[step_name]
            if stats.first_item_at is None:
                stats.first_item_at = time.monotonic()

    def add_processing_ns(self, step_name: str, ns: int) -> None:
        with self._lock:
            self._stats[step_name].processing_ns += ns

    def add_idle_ns(self, step_name: str, ns: int) -> None:
        with self._lock:
            self._stats[step_name].idle_ns += ns
            self._stats[step_name].idle_count += 1

    def set_state(self, step_name: str, state: str) -> None:
        with self._lock:
            self._stats[step_name].current_state = state

    def finish(self, step_name: str) -> None:
        self._stats[step_name].finish()

    def setup_log_handler(self, path: Path) -> None:
        logger.debug("path=%s", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(str(path), encoding="utf-8")
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        parent_logger = logging.getLogger("task_pipeliner")
        parent_logger.addHandler(handler)
        parent_logger.setLevel(logging.DEBUG)
        self._handler = handler
        logger.debug("log handler attached to task_pipeliner logger")

    def write_json(self, path: Path) -> None:
        logger.debug("path=%s", path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            data = [s.to_dict() for s in self._stats.values()]
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))
            os.replace(str(tmp), str(path))
            logger.info("stats JSON written to %s", path)
        except Exception:
            logger.warning("failed to write stats JSON to %s", path, exc_info=True)

    def flush(self) -> None:
        if self._handler is not None:
            self._handler.flush()
            parent_logger = logging.getLogger("task_pipeliner")
            parent_logger.removeHandler(self._handler)
            self._handler.close()
            self._handler = None
