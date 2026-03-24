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


def _compute_expected_total(
    stats: StatsCollector,
    upstream: list[tuple[str, str]],
) -> int | None:
    """Return expected input count if ALL upstream steps are done, else None."""
    if not upstream:
        return None
    total = 0
    for up_name, tag in upstream:
        up_stats = stats.get_step_stats(up_name)
        if up_stats is None or up_stats.current_state != "done":
            return None
        total += up_stats.emitted.get(tag, 0)
    return total


def format_progress(
    stats: StatsCollector,
    step_names: list[str],
    elapsed: float,
    upstream_for: dict[str, list[tuple[str, str]]] | None = None,
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

        # Check if total is known (all upstream steps done)
        expected: int | None = None
        if upstream_for is not None and name in upstream_for:
            expected = _compute_expected_total(stats, upstream_for[name])

        # Build main info
        parts: list[str] = []

        # Input / progress summary
        if expected is not None and expected > 0:
            pct = processed / expected * 100
            parts.append(f"{processed}/{expected} ({pct:.1f}%)")
        elif emitted and processed:
            parts.append(f"{processed} in")
        elif processed:
            parts.append(f"{processed} produced")
        else:
            parts.append("0 in")

        # Emitted summary (output tags)
        if emitted and processed:
            kept_parts: list[str] = []
            for tag, count in emitted.items():
                kept_parts.append(f"{count} {tag}")
            parts.append("→ " + ", ".join(kept_parts))

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
        upstream_for: dict[str, list[tuple[str, str]]] | None = None,
    ) -> None:
        super().__init__(daemon=True)
        self._stats = stats
        self._step_names = step_names
        self._interval = interval
        self._output_dir = output_dir
        self._upstream_for = upstream_for
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
        text = format_progress(self._stats, self._step_names, elapsed, self._upstream_for)
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
        text = format_progress(self._stats, self._step_names, elapsed, self._upstream_for)
        print(text, file=sys.stderr, flush=True)
        if self._output_dir is not None:
            log_path = self._output_dir / "progress.log"
            with open(log_path, "w", encoding="utf-8") as fh:
                fh.write(text + "\n")
        logger.debug("ProgressReporter stopped")
