"""Progress display: format_progress function and ProgressReporter thread."""

from __future__ import annotations

import logging
import sys
import threading
import time
from pathlib import Path
from typing import IO

from task_pipeliner.stats import StatsCollector

logger = logging.getLogger(__name__)


def format_progress(
    stats: StatsCollector,
    step_names: list[str],
    elapsed: float,
) -> str:
    """Format a progress snapshot as a human-readable string.

    Pure function — no I/O side effects.
    """
    header = f"--- Pipeline Progress ({elapsed:.1f}s) "
    header += "-" * max(0, 73 - len(header))
    lines = [header]

    for name in step_names:
        step_stats = stats.get_step_stats(name)
        if step_stats is None:
            continue

        processed: int = step_stats.processed
        emitted = step_stats.emitted
        state: str = step_stats.current_state

        # Build main info
        parts: list[str] = []

        # Emitted summary
        if emitted and processed:
            kept_parts: list[str] = []
            for tag, count in emitted.items():
                kept_parts.append(f"{count} {tag}")
            parts.append(f"{processed} in → {', '.join(kept_parts)}")
        elif processed:
            parts.append(f"{processed} produced")
        else:
            parts.append("0 in")

        # Average processing time
        if processed and step_stats.processing_ns > 0:
            avg_ms = step_stats.processing_ns / processed / 1_000_000
            parts.append(f"{avg_ms:.1f}ms/item")

        # State label
        state_label = f"[{state}]"

        info = "  ".join(parts)
        line = f"  {name:<24s} {info:>40s}  {state_label}"
        lines.append(line)

    lines.append("-" * 73)
    return "\n".join(lines)


class ProgressReporter(threading.Thread):
    """Daemon thread that periodically prints pipeline progress to stderr and a log file."""

    def __init__(
        self,
        *,
        stats: StatsCollector,
        step_names: list[str],
        interval: float = 5.0,
        output_dir: Path | None = None,
    ) -> None:
        super().__init__(daemon=True)
        self._stats = stats
        self._step_names = step_names
        self._interval = interval
        self._output_dir = output_dir
        self._stop_event = threading.Event()
        self._start_time = time.monotonic()
        self._log_fh: IO[str] | None = None

    def run(self) -> None:
        logger.debug("ProgressReporter started interval=%.1f", self._interval)
        self._start_time = time.monotonic()
        if self._output_dir is not None:
            self._output_dir.mkdir(parents=True, exist_ok=True)
            self._log_fh = open(  # noqa: SIM115
                self._output_dir / "progress.log", "w", encoding="utf-8"
            )

        try:
            while not self._stop_event.wait(self._interval):
                self._emit()
        finally:
            if self._log_fh is not None:
                self._log_fh.close()
                self._log_fh = None

    def _emit(self) -> None:
        elapsed = time.monotonic() - self._start_time
        text = format_progress(self._stats, self._step_names, elapsed)
        print(text, file=sys.stderr, flush=True)
        if self._log_fh is not None:
            self._log_fh.seek(0)
            self._log_fh.write(text + "\n")
            self._log_fh.truncate()
            self._log_fh.flush()

    def stop(self) -> None:
        """Signal the reporter to stop, emit final snapshot, and join."""
        self._stop_event.set()
        self.join(timeout=10)
        # Final output after thread ends
        elapsed = time.monotonic() - self._start_time
        text = format_progress(self._stats, self._step_names, elapsed)
        print(text, file=sys.stderr, flush=True)
        if self._output_dir is not None:
            log_path = self._output_dir / "progress.log"
            with open(log_path, "w", encoding="utf-8") as fh:
                fh.write(text + "\n")
        logger.debug("ProgressReporter stopped")
