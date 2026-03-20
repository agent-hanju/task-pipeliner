"""Tests for stats module (W-04)."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from task_pipeliner.stats import StatsCollector, StepStats


class TestStepStats:
    def test_initial_counters(self) -> None:
        s = StepStats(step_name="test")
        assert s.processed == 0
        assert s.errored == 0
        assert s.emitted == {}

    def test_elapsed_before_finish(self) -> None:
        s = StepStats(step_name="test")
        time.sleep(0.05)
        assert s.elapsed_seconds >= 0.04

    def test_elapsed_after_finish(self) -> None:
        s = StepStats(step_name="test")
        time.sleep(0.05)
        s.finish()
        elapsed = s.elapsed_seconds
        time.sleep(0.05)
        assert s.elapsed_seconds == elapsed  # frozen after finish

    def test_elapsed_changes_before_finish(self) -> None:
        """elapsed_seconds grows before finish(), freezes after."""
        s = StepStats(step_name="test")
        e1 = s.elapsed_seconds
        time.sleep(0.05)
        e2 = s.elapsed_seconds
        assert e2 > e1, "elapsed should grow before finish()"
        s.finish()
        e3 = s.elapsed_seconds
        time.sleep(0.05)
        e4 = s.elapsed_seconds
        assert e3 == e4, "elapsed should freeze after finish()"

    def test_to_dict(self) -> None:
        s = StepStats(step_name="my_step")
        s.processed = 10
        s.errored = 1
        s.emitted = {"kept": 7, "removed": 2}
        s.finish()
        d = s.to_dict()
        assert d["step_name"] == "my_step"
        assert d["processed"] == 10
        assert d["errored"] == 1
        assert d["emitted"] == {"kept": 7, "removed": 2}
        assert isinstance(d["elapsed_seconds"], float)

    def test_to_dict_keys(self) -> None:
        s = StepStats(step_name="x")
        s.finish()
        d = s.to_dict()
        assert set(d.keys()) == {"step_name", "processed", "errored", "emitted", "elapsed_seconds"}


class TestStatsCollector:
    @pytest.mark.parametrize(
        ("field_name", "n", "expected"),
        [
            ("processed", 5, 5),
            ("errored", 1, 1),
        ],
    )
    def test_increment_fields(self, field_name: str, n: int, expected: int) -> None:
        c = StatsCollector()
        c.register("step_a")
        c.increment("step_a", field_name, n)
        assert getattr(c._stats["step_a"], field_name) == expected

    def test_increment_multiple_calls(self) -> None:
        c = StatsCollector()
        c.register("step_a")
        c.increment("step_a", "processed", 5)
        c.increment("step_a", "processed", 3)
        c.increment("step_a", "errored")
        c.finish("step_a")
        stats = c._stats["step_a"]
        assert stats.processed == 8
        assert stats.errored == 1

    def test_increment_emitted(self) -> None:
        c = StatsCollector()
        c.register("step_a")
        c.increment_emitted("step_a", "kept", 3)
        c.increment_emitted("step_a", "removed", 1)
        c.increment_emitted("step_a", "kept", 2)
        stats = c._stats["step_a"]
        assert stats.emitted == {"kept": 5, "removed": 1}

    def test_register_returns_step_stats(self) -> None:
        c = StatsCollector()
        s = c.register("my_step")
        assert isinstance(s, StepStats)
        assert s.step_name == "my_step"

    def test_write_json_valid_json(self, tmp_path: Path) -> None:
        c = StatsCollector()
        c.register("s1")
        c.increment("s1", "processed", 3)
        c.finish("s1")
        out = tmp_path / "stats.json"
        c.write_json(out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["step_name"] == "s1"
        assert data[0]["processed"] == 3

    def test_write_json_multiple_steps(self, tmp_path: Path) -> None:
        c = StatsCollector()
        c.register("s1")
        c.register("s2")
        c.increment("s1", "processed", 2)
        c.increment_emitted("s2", "main", 4)
        c.finish("s1")
        c.finish("s2")
        out = tmp_path / "stats.json"
        c.write_json(out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert len(data) == 2
        names = {d["step_name"] for d in data}
        assert names == {"s1", "s2"}

    def test_write_json_creates_directory(self, tmp_path: Path) -> None:
        c = StatsCollector()
        c.register("s1")
        c.finish("s1")
        out = tmp_path / "sub" / "deep" / "stats.json"
        c.write_json(out)
        assert out.exists()

    def test_write_json_failure_does_not_propagate(self, tmp_path: Path) -> None:
        c = StatsCollector()
        c.register("s1")
        out = tmp_path / "stats.json"
        # Force mkdir to fail -- write_json should not propagate the exception
        with patch.object(Path, "mkdir", side_effect=OSError("disk full")):
            c.write_json(out)  # no exception

    def test_write_json_failure_logs_warning(self, tmp_path: Path) -> None:
        c = StatsCollector()
        c.register("s1")
        out = tmp_path / "stats.json"
        with (
            patch.object(Path, "mkdir", side_effect=OSError("disk full")),
            patch("task_pipeliner.stats.logger") as mock_logger,
        ):
            c.write_json(out)
            mock_logger.warning.assert_called_once()

    def test_write_json_success_logs_info(self, tmp_path: Path) -> None:
        c = StatsCollector()
        c.register("s1")
        c.finish("s1")
        out = tmp_path / "stats.json"
        with patch("task_pipeliner.stats.logger") as mock_logger:
            c.write_json(out)
            mock_logger.info.assert_called()

    def test_setup_log_handler_and_flush(self, tmp_path: Path) -> None:
        c = StatsCollector()
        log_path = tmp_path / "pipeline.log"
        c.setup_log_handler(log_path)
        try:
            parent_logger = logging.getLogger("task_pipeliner")
            parent_logger.warning("test message")
            c.flush()
            assert log_path.exists()
            assert "test message" in log_path.read_text(encoding="utf-8")
        finally:
            # Ensure cleanup even if test fails
            c.flush()

    def test_flush_removes_handler(self, tmp_path: Path) -> None:
        c = StatsCollector()
        log_path = tmp_path / "pipeline.log"
        c.setup_log_handler(log_path)
        parent_logger = logging.getLogger("task_pipeliner")
        handler_count_before = len(parent_logger.handlers)
        c.flush()
        handler_count_after = len(parent_logger.handlers)
        assert handler_count_after == handler_count_before - 1
        assert c._handler is None

    def test_flush_without_handler_is_noop(self) -> None:
        c = StatsCollector()
        c.flush()  # should not raise

    def test_setup_log_handler_creates_directory(self, tmp_path: Path) -> None:
        c = StatsCollector()
        log_path = tmp_path / "sub" / "pipeline.log"
        c.setup_log_handler(log_path)
        try:
            assert log_path.parent.exists()
        finally:
            c.flush()

    def test_setup_log_handler_format(self, tmp_path: Path) -> None:
        """Log output follows the expected format with module:func:lineno."""
        c = StatsCollector()
        log_path = tmp_path / "pipeline.log"
        c.setup_log_handler(log_path)
        try:
            test_logger = logging.getLogger("task_pipeliner.test_module")
            test_logger.warning("format check")
            c.flush()
            content = log_path.read_text(encoding="utf-8")
            # Should contain the logger name with colon-separated func and lineno
            assert "task_pipeliner.test_module:" in content
            assert "format check" in content
        finally:
            c.flush()
