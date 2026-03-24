"""Tests for progress module (M-09, M-10)."""

from __future__ import annotations

import time
from pathlib import Path

from task_pipeliner.stats import StatsCollector


class TestFormatProgress:
    def _make_stats(
        self, step_configs: list[dict[str, object]]
    ) -> tuple[StatsCollector, list[str]]:
        """Create StatsCollector with registered steps and return (stats, step_names)."""
        stats = StatsCollector()
        step_names = []
        for cfg in step_configs:
            name = str(cfg["name"])
            step_names.append(name)
            stats.register(name)
            for field in ("processed", "errored"):
                if field in cfg:
                    stats.increment(name, field, int(cfg[field]))
            for tag, count in cfg.get("emitted", {}).items():  # type: ignore[union-attr]
                stats.increment_emitted(name, str(tag), int(count))
            if "processing_ns" in cfg:
                stats.add_processing_ns(name, int(cfg["processing_ns"]))
            if "state" in cfg:
                stats.set_state(name, str(cfg["state"]))
            if "first_item" in cfg and cfg["first_item"]:
                stats.record_first_item(name)
        return stats, step_names

    def test_basic_format(self) -> None:
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "Source", "processed": 100, "state": "done"},
            {"name": "Filter", "processed": 100, "emitted": {"kept": 80, "removed": 20},
             "processing_ns": 500_000_000, "state": "done"},
        ])
        result = format_progress(stats, step_names, 5.0)
        assert "Source" in result
        assert "Filter" in result
        assert "5.0s" in result

    def test_step_order_preserved(self) -> None:
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "A", "state": "done"},
            {"name": "B", "state": "processing"},
            {"name": "C", "state": "waiting"},
        ])
        result = format_progress(stats, step_names, 1.0)
        pos_a = result.index("A")
        pos_b = result.index("B")
        pos_c = result.index("C")
        assert pos_a < pos_b < pos_c

    def test_state_labels(self) -> None:
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "S1", "state": "waiting"},
            {"name": "S2", "state": "idle"},
            {"name": "S3", "state": "processing"},
            {"name": "S4", "state": "done"},
        ])
        result = format_progress(stats, step_names, 1.0)
        assert "waiting" in result
        assert "idle" in result
        assert "processing" in result
        assert "done" in result

    def test_avg_ms_displayed(self) -> None:
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "Step", "processed": 100, "processing_ns": 1_000_000_000,
             "state": "done"},
        ])
        result = format_progress(stats, step_names, 2.0)
        # 1_000_000_000 ns / 100 items = 10.0 ms/item
        assert "ms/item" in result

    def test_no_avg_when_zero_processed(self) -> None:
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "Step", "processed": 0, "state": "waiting"},
        ])
        result = format_progress(stats, step_names, 1.0)
        assert "ms/item" not in result

    def test_percentage_shown_when_upstream_done(self) -> None:
        """When all upstream steps are done, show processed/total (pct%)."""
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "Source", "processed": 200, "emitted": {"": 200}, "state": "done"},
            {"name": "Filter", "processed": 80, "emitted": {"kept": 60, "removed": 20},
             "processing_ns": 400_000_000, "state": "processing"},
        ])
        upstream_for = {"Source": [], "Filter": [("Source", "")]}
        result = format_progress(stats, step_names, 10.0, upstream_for)
        assert "80/200" in result
        assert "40.0%" in result
        assert "→ 60 kept, 20 removed" in result

    def test_no_percentage_when_upstream_still_running(self) -> None:
        """Before upstream finishes, show count-based format without percentage."""
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "Source", "processed": 100, "emitted": {"": 100}, "state": "processing"},
            {"name": "Filter", "processed": 50, "emitted": {"kept": 40, "removed": 10},
             "state": "processing"},
        ])
        upstream_for = {"Source": [], "Filter": [("Source", "")]}
        result = format_progress(stats, step_names, 5.0, upstream_for)
        assert "%" not in result
        assert "50 in" in result

    def test_percentage_with_fan_in(self) -> None:
        """Fan-in: multiple upstream steps feed into one step."""
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "Source", "processed": 100, "emitted": {"": 100}, "state": "done"},
            {"name": "FilterA", "processed": 100, "emitted": {"kept": 60}, "state": "done"},
            {"name": "FilterB", "processed": 100, "emitted": {"kept": 40}, "state": "done"},
            {"name": "Merge", "processed": 70, "emitted": {"": 70}, "state": "processing"},
        ])
        upstream_for = {
            "Source": [],
            "FilterA": [("Source", "")],
            "FilterB": [("Source", "")],
            "Merge": [("FilterA", "kept"), ("FilterB", "kept")],
        }
        result = format_progress(stats, step_names, 10.0, upstream_for)
        # Merge expects 60 + 40 = 100 total
        assert "70/100" in result
        assert "70.0%" in result

    def test_no_percentage_without_upstream_for(self) -> None:
        """Backward compat: no upstream_for → no percentage."""
        from task_pipeliner.progress import format_progress

        stats, step_names = self._make_stats([
            {"name": "Source", "processed": 100, "emitted": {"": 100}, "state": "done"},
            {"name": "Filter", "processed": 50, "emitted": {"kept": 40, "removed": 10},
             "state": "processing"},
        ])
        result = format_progress(stats, step_names, 5.0)
        assert "%" not in result


class TestProgressReporter:
    def _make_reporter(
        self, output_dir: Path | None = None, interval: float = 0.1
    ) -> tuple[object, StatsCollector, list[str]]:
        from task_pipeliner.progress import ProgressReporter

        stats = StatsCollector()
        step_names = ["A", "B"]
        stats.register("A")
        stats.register("B")
        reporter = ProgressReporter(
            stats=stats, step_names=step_names, interval=interval, output_dir=output_dir
        )
        return reporter, stats, step_names

    def test_start_stop_lifecycle(self) -> None:
        reporter, _, _ = self._make_reporter()
        reporter.start()
        assert reporter.is_alive()
        reporter.stop()
        assert not reporter.is_alive()

    def test_stop_terminates_thread(self) -> None:
        reporter, _, _ = self._make_reporter()
        reporter.start()
        reporter.stop()
        reporter.join(timeout=5)
        assert not reporter.is_alive()

    def test_interval_output(self, capsys: object) -> None:
        """Reporter outputs at interval (using short interval for test)."""
        reporter, stats, _ = self._make_reporter(interval=0.05)
        stats.increment("A", "processed", 5)
        reporter.start()
        time.sleep(0.2)
        reporter.stop()
        # Just verify no crash — output goes to stderr which capsys doesn't capture from threads

    def test_progress_log_created(self, tmp_path: Path) -> None:
        reporter, stats, _ = self._make_reporter(output_dir=tmp_path, interval=0.05)
        stats.increment("A", "processed", 3)
        reporter.start()
        time.sleep(0.2)
        reporter.stop()
        log_file = tmp_path / "progress.log"
        assert log_file.exists()
        content = log_file.read_text(encoding="utf-8")
        assert "Pipeline Progress" in content

    def test_progress_log_flushed(self, tmp_path: Path) -> None:
        """Each write is flushed (tail -f compatible)."""
        reporter, stats, _ = self._make_reporter(output_dir=tmp_path, interval=0.05)
        stats.increment("A", "processed", 1)
        reporter.start()
        time.sleep(0.15)
        # File should exist and have content before stop
        log_file = tmp_path / "progress.log"
        assert log_file.exists()
        assert len(log_file.read_text(encoding="utf-8")) > 0
        reporter.stop()
