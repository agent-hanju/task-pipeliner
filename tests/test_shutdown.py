"""Tests: Graceful shutdown — W-14."""

from __future__ import annotations

import os
import platform
import signal
import subprocess
import sys
import time
from pathlib import Path

import orjson
import pytest

_HELPER = str(Path(__file__).parent / "_run_pipeline.py")
_IS_WINDOWS = platform.system() == "Windows"


class TestNormalCompletion:
    @pytest.mark.timeout(20)
    def test_stats_json_all_steps(self, tmp_path: Path) -> None:
        """Normal completion → stats.json contains all step entries."""
        output_dir = tmp_path / "out"
        proc = subprocess.run(
            [sys.executable, _HELPER, str(output_dir), "fast"],
            timeout=15,
            capture_output=True,
        )
        assert proc.returncode == 0, proc.stderr.decode()
        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["step_name"] == "PassthroughStep"
        assert data[0]["passed"] == 10

    @pytest.mark.timeout(20)
    def test_result_write_called(self, tmp_path: Path) -> None:
        """Normal completion → BaseResult.write() output file exists."""
        output_dir = tmp_path / "out"
        proc = subprocess.run(
            [sys.executable, _HELPER, str(output_dir), "filter"],
            timeout=15,
            capture_output=True,
        )
        assert proc.returncode == 0, proc.stderr.decode()
        result_file = output_dir / "count_result.json"
        assert result_file.exists()
        data = orjson.loads(result_file.read_bytes())
        assert data["passed"] == 10
        assert data["filtered"] == 10

    @pytest.mark.timeout(20)
    def test_pipeline_log_exists(self, tmp_path: Path) -> None:
        """Normal completion → pipeline.log exists with content."""
        output_dir = tmp_path / "out"
        proc = subprocess.run(
            [sys.executable, _HELPER, str(output_dir), "fast"],
            timeout=15,
            capture_output=True,
        )
        assert proc.returncode == 0, proc.stderr.decode()
        log_file = output_dir / "pipeline.log"
        assert log_file.exists()
        content = log_file.read_text(encoding="utf-8")
        assert len(content) > 0


class TestSignalShutdown:
    @pytest.mark.timeout(20)
    def test_sigint_stats_json_written(self, tmp_path: Path) -> None:
        """SIGINT during processing → stats.json still written."""
        output_dir = tmp_path / "out"

        if _IS_WINDOWS:
            proc = subprocess.Popen(
                [sys.executable, _HELPER, str(output_dir), "slow"],
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            proc = subprocess.Popen(
                [sys.executable, _HELPER, str(output_dir), "slow"],
            )

        time.sleep(3)

        if _IS_WINDOWS:
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)

        proc.wait(timeout=15)

        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        assert isinstance(data, list)
        assert len(data) >= 1

    @pytest.mark.timeout(20)
    def test_sigint_pipeline_log_exists(self, tmp_path: Path) -> None:
        """SIGINT during processing → pipeline.log exists with content."""
        output_dir = tmp_path / "out"

        if _IS_WINDOWS:
            proc = subprocess.Popen(
                [sys.executable, _HELPER, str(output_dir), "slow"],
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            proc = subprocess.Popen(
                [sys.executable, _HELPER, str(output_dir), "slow"],
            )

        time.sleep(3)

        if _IS_WINDOWS:
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)

        proc.wait(timeout=15)

        log_file = output_dir / "pipeline.log"
        assert log_file.exists()
        content = log_file.read_text(encoding="utf-8")
        assert len(content) > 0

    @pytest.mark.timeout(20)
    def test_sigint_no_deadlock(self, tmp_path: Path) -> None:
        """SIGINT → process exits within timeout (no deadlock)."""
        output_dir = tmp_path / "out"

        if _IS_WINDOWS:
            proc = subprocess.Popen(
                [sys.executable, _HELPER, str(output_dir), "slow"],
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            proc = subprocess.Popen(
                [sys.executable, _HELPER, str(output_dir), "slow"],
            )

        time.sleep(2)

        if _IS_WINDOWS:
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)

        # Must exit within 10 seconds — no deadlock
        proc.wait(timeout=10)

    @pytest.mark.timeout(20)
    def test_sigint_partial_result_preserved(self, tmp_path: Path) -> None:
        """SIGINT → stats reflect partial progress."""
        output_dir = tmp_path / "out"

        if _IS_WINDOWS:
            proc = subprocess.Popen(
                [sys.executable, _HELPER, str(output_dir), "slow"],
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            proc = subprocess.Popen(
                [sys.executable, _HELPER, str(output_dir), "slow"],
            )

        time.sleep(3)

        if _IS_WINDOWS:
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)

        proc.wait(timeout=15)

        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        assert data[0]["step_name"] == "SlowStep"
        assert data[0]["passed"] >= 0
