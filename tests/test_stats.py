"""Tests for stats module (W-04)."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from task_pipeliner.pipeline import Pipeline
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

    def test_first_item_at_initial_none(self) -> None:
        s = StepStats(step_name="test")
        assert s.first_item_at is None

    def test_first_item_at_set(self) -> None:
        s = StepStats(step_name="test")
        s.first_item_at = 123.456
        assert s.first_item_at == 123.456

    def test_processing_ns_initial_zero(self) -> None:
        s = StepStats(step_name="test")
        assert s.processing_ns == 0

    def test_idle_ns_initial_zero(self) -> None:
        s = StepStats(step_name="test")
        assert s.idle_ns == 0

    def test_idle_count_initial_zero(self) -> None:
        s = StepStats(step_name="test")
        assert s.idle_count == 0

    def test_current_state_initial_waiting(self) -> None:
        s = StepStats(step_name="test")
        assert s.current_state == "waiting"

    def test_to_dict_includes_timing_fields(self) -> None:
        s = StepStats(step_name="test")
        s.first_item_at = s._start_time + 1.5
        s.processing_ns = 2_000_000_000  # 2s
        s.idle_ns = 500_000_000  # 0.5s
        s.idle_count = 10
        s.processed = 20
        s.finish()
        d = s.to_dict()
        assert "initial_wait_seconds" in d
        assert abs(d["initial_wait_seconds"] - 1.5) < 0.01
        assert "processing_seconds" in d
        assert abs(d["processing_seconds"] - 2.0) < 0.01
        assert "processing_avg_ms" in d
        assert abs(d["processing_avg_ms"] - 100.0) < 0.01  # 2000ms / 20 items
        assert "idle_seconds" in d
        assert abs(d["idle_seconds"] - 0.5) < 0.01
        assert "idle_avg_ms" in d
        assert abs(d["idle_avg_ms"] - 50.0) < 0.01  # 500ms / 10 idles

    def test_to_dict_no_avg_when_zero_processed(self) -> None:
        s = StepStats(step_name="test")
        s.finish()
        d = s.to_dict()
        assert d["processing_avg_ms"] is None
        assert d["idle_avg_ms"] is None

    def test_to_dict_keys(self) -> None:
        s = StepStats(step_name="x")
        s.finish()
        d = s.to_dict()
        assert set(d.keys()) == {
            "step_name", "processed", "errored", "emitted", "elapsed_seconds",
            "initial_wait_seconds", "processing_seconds", "processing_avg_ms",
            "idle_seconds", "idle_avg_ms", "current_state",
        }


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

    def test_set_total_items(self) -> None:
        c = StatsCollector()
        c.set_total_items(100)
        assert c.total_items == 100

    def test_record_first_item(self) -> None:
        c = StatsCollector()
        c.register("step_a")
        c.record_first_item("step_a")
        assert c._stats["step_a"].first_item_at is not None

    def test_record_first_item_idempotent(self) -> None:
        c = StatsCollector()
        c.register("step_a")
        c.record_first_item("step_a")
        first_val = c._stats["step_a"].first_item_at
        c.record_first_item("step_a")
        assert c._stats["step_a"].first_item_at == first_val

    def test_add_processing_ns(self) -> None:
        c = StatsCollector()
        c.register("step_a")
        c.add_processing_ns("step_a", 1000)
        c.add_processing_ns("step_a", 2000)
        assert c._stats["step_a"].processing_ns == 3000

    def test_add_idle_ns(self) -> None:
        c = StatsCollector()
        c.register("step_a")
        c.add_idle_ns("step_a", 500)
        c.add_idle_ns("step_a", 300)
        assert c._stats["step_a"].idle_ns == 800
        assert c._stats["step_a"].idle_count == 2

    def test_set_state(self) -> None:
        c = StatsCollector()
        c.register("step_a")
        assert c._stats["step_a"].current_state == "waiting"
        c.set_state("step_a", "processing")
        assert c._stats["step_a"].current_state == "processing"
        c.set_state("step_a", "done")
        assert c._stats["step_a"].current_state == "done"

    def test_write_json_includes_new_fields(self, tmp_path: Path) -> None:
        c = StatsCollector()
        c.register("s1")
        c.set_total_items(50)
        c.record_first_item("s1")
        c.add_processing_ns("s1", 1_000_000)
        c.add_idle_ns("s1", 500_000)
        c.set_state("s1", "done")
        c.increment("s1", "processed", 10)
        c.finish("s1")
        out = tmp_path / "stats.json"
        c.write_json(out)
        data = json.loads(out.read_text(encoding="utf-8"))
        entry = data[0]
        assert "processing_seconds" in entry
        assert "idle_seconds" in entry
        assert "current_state" in entry
        assert entry["current_state"] == "done"

    def test_flush_is_noop(self) -> None:
        c = StatsCollector()
        c.flush()  # should not raise


class TestPipelineLogHandler:
    """Tests for Pipeline._setup_log_handler / _teardown_log_handler."""

    def test_setup_and_teardown(self, tmp_path: Path) -> None:
        log_path = tmp_path / "pipeline.log"
        handler = Pipeline._setup_log_handler(log_path)
        try:
            pkg_logger = logging.getLogger("task_pipeliner")
            pkg_logger.warning("test message")
            handler.flush()
            assert log_path.exists()
            assert "test message" in log_path.read_text(encoding="utf-8")
        finally:
            Pipeline._teardown_log_handler(handler)

    def test_teardown_removes_handler(self, tmp_path: Path) -> None:
        log_path = tmp_path / "pipeline.log"
        handler = Pipeline._setup_log_handler(log_path)
        pkg_logger = logging.getLogger("task_pipeliner")
        count_before = len(pkg_logger.handlers)
        Pipeline._teardown_log_handler(handler)
        count_after = len(pkg_logger.handlers)
        assert count_after == count_before - 1

    def test_setup_creates_directory(self, tmp_path: Path) -> None:
        log_path = tmp_path / "sub" / "pipeline.log"
        handler = Pipeline._setup_log_handler(log_path)
        try:
            assert log_path.parent.exists()
        finally:
            Pipeline._teardown_log_handler(handler)

    def test_log_format(self, tmp_path: Path) -> None:
        """Log output follows the expected format with module:func:lineno."""
        log_path = tmp_path / "pipeline.log"
        handler = Pipeline._setup_log_handler(log_path)
        try:
            test_logger = logging.getLogger("task_pipeliner.test_module")
            test_logger.warning("format check")
            handler.flush()
            content = log_path.read_text(encoding="utf-8")
            assert "task_pipeliner.test_module:" in content
            assert "format check" in content
        finally:
            Pipeline._teardown_log_handler(handler)
