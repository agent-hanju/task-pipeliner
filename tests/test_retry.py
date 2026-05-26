"""Tests: Retry support — Phase 13 (R-05)."""

from __future__ import annotations

import multiprocessing
from typing import Any

import pytest
from dummy_steps import FlakyAsyncStep, FlakySequentialStep

from task_pipeliner.stats import StatsCollector
from task_pipeliner.step_runners import (
    AsyncStepRunner,
    Sentinel,
    SequentialStepRunner,
    is_sentinel,
)


def _run_sequential(
    items: list[Any],
    step: Any,
    retry_count: int = 0,
    retry_delay: float = 0.0,
    retry_backoff: float = 2.0,
    step_name: str = "test_step",
) -> tuple[list[Any], StatsCollector]:
    ctx = multiprocessing.get_context("spawn")
    in_q: multiprocessing.Queue[Any] = ctx.Queue()
    out_q: multiprocessing.Queue[Any] = ctx.Queue()
    stats = StatsCollector()
    stats.register(step_name)

    for item in items:
        in_q.put(item)
    in_q.put(Sentinel())

    runner = SequentialStepRunner(
        step=step,
        step_name=step_name,
        input_queue=in_q,
        output_queues={"main": [out_q]},
        stats=stats,
        retry_count=retry_count,
        retry_delay=retry_delay,
        retry_backoff=retry_backoff,
    )
    runner.run()

    collected = []
    while True:
        obj = out_q.get(timeout=2)
        if is_sentinel(obj):
            break
        collected.append(obj)

    return collected, stats


def _run_async(
    items: list[Any],
    step: Any,
    retry_count: int = 0,
    retry_delay: float = 0.0,
    retry_backoff: float = 2.0,
    step_name: str = "test_step",
) -> tuple[list[Any], StatsCollector]:
    ctx = multiprocessing.get_context("spawn")
    in_q: multiprocessing.Queue[Any] = ctx.Queue()
    out_q: multiprocessing.Queue[Any] = ctx.Queue()
    stats = StatsCollector()
    stats.register(step_name)

    for item in items:
        in_q.put(item)
    in_q.put(Sentinel())

    runner = AsyncStepRunner(
        step=step,
        step_name=step_name,
        input_queue=in_q,
        output_queues={"main": [out_q]},
        stats=stats,
        retry_count=retry_count,
        retry_delay=retry_delay,
        retry_backoff=retry_backoff,
    )
    runner.run()

    collected = []
    while True:
        obj = out_q.get(timeout=2)
        if is_sentinel(obj):
            break
        collected.append(obj)

    return collected, stats


class TestSequentialRetry:
    @pytest.mark.timeout(15)
    def test_retry_success_on_second_attempt(self) -> None:
        """Item fails once, succeeds on retry → processed=1, retried=1, errored=0."""
        step = FlakySequentialStep(fail_times=1)
        collected, stats = _run_sequential([42], step, retry_count=2)
        assert collected == [42]
        s = stats._stats["test_step"]
        assert s.processed == 1
        assert s.retried == 1
        assert s.errored == 0

    @pytest.mark.timeout(15)
    def test_retry_exhausted(self) -> None:
        """Item always fails, retries exhausted → errored=1, retried=retry_count."""
        step = FlakySequentialStep(fail_times=99)
        collected, stats = _run_sequential([42], step, retry_count=2)
        assert collected == []
        s = stats._stats["test_step"]
        assert s.processed == 0
        assert s.retried == 2
        assert s.errored == 1

    @pytest.mark.timeout(15)
    def test_no_retry_by_default(self) -> None:
        """retry_count=0 → failure counted immediately as error, no retry."""
        step = FlakySequentialStep(fail_times=1)
        collected, stats = _run_sequential([42], step, retry_count=0)
        assert collected == []
        s = stats._stats["test_step"]
        assert s.processed == 0
        assert s.retried == 0
        assert s.errored == 1

    @pytest.mark.timeout(15)
    @pytest.mark.parametrize(
        "fail_times,retry_count,exp_processed,exp_retried,exp_errored",
        [
            (0, 3, 1, 0, 0),   # no failure at all
            (1, 1, 1, 1, 0),   # fails once, exactly 1 retry available
            (2, 2, 1, 2, 0),   # fails twice, exactly 2 retries available
            (3, 2, 0, 2, 1),   # fails 3 times, only 2 retries → exhausted
        ],
    )
    def test_retry_boundary_conditions(
        self,
        fail_times: int,
        retry_count: int,
        exp_processed: int,
        exp_retried: int,
        exp_errored: int,
    ) -> None:
        step = FlakySequentialStep(fail_times=fail_times)
        collected, stats = _run_sequential([1], step, retry_count=retry_count)
        s = stats._stats["test_step"]
        assert s.processed == exp_processed
        assert s.retried == exp_retried
        assert s.errored == exp_errored

    @pytest.mark.timeout(15)
    def test_retry_accumulates_across_items(self) -> None:
        """retried counter accumulates across all items that needed retries."""
        step = FlakySequentialStep(fail_times=1)
        # Each of 3 items fails once → 3 retries total
        collected, stats = _run_sequential([1, 2, 3], step, retry_count=2)
        assert sorted(collected) == [1, 2, 3]
        s = stats._stats["test_step"]
        assert s.processed == 3
        assert s.retried == 3
        assert s.errored == 0

    @pytest.mark.timeout(15)
    def test_sentinel_propagated_after_retry_exhaustion(self) -> None:
        """Sentinel must be sent even when all retries are exhausted."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("test_step")

        in_q.put(1)
        in_q.put(Sentinel())

        runner = SequentialStepRunner(
            step=FlakySequentialStep(fail_times=99),
            step_name="test_step",
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            retry_count=1,
        )
        runner.run()

        assert is_sentinel(out_q.get(timeout=2))

    @pytest.mark.timeout(15)
    def test_retried_in_stats_json(self) -> None:
        """retried counter appears in stats.to_dict() output."""
        step = FlakySequentialStep(fail_times=1)
        _, stats = _run_sequential([1], step, retry_count=2)
        d = stats._stats["test_step"].to_dict()
        assert "retried" in d
        assert d["retried"] == 1


class TestAsyncRetry:
    @pytest.mark.timeout(15)
    def test_retry_success_on_second_attempt(self) -> None:
        """Async item fails once, succeeds on retry → processed=1, retried=1, errored=0."""
        step = FlakyAsyncStep(fail_times=1)
        collected, stats = _run_async([42], step, retry_count=2)
        assert collected == [42]
        s = stats._stats["test_step"]
        assert s.processed == 1
        assert s.retried == 1
        assert s.errored == 0

    @pytest.mark.timeout(15)
    def test_retry_exhausted(self) -> None:
        """Async item always fails, retries exhausted → errored=1, retried=retry_count."""
        step = FlakyAsyncStep(fail_times=99)
        collected, stats = _run_async([42], step, retry_count=2)
        assert collected == []
        s = stats._stats["test_step"]
        assert s.processed == 0
        assert s.retried == 2
        assert s.errored == 1

    @pytest.mark.timeout(15)
    def test_no_retry_by_default(self) -> None:
        """retry_count=0 → first async failure counted as error."""
        step = FlakyAsyncStep(fail_times=1)
        collected, stats = _run_async([42], step, retry_count=0)
        assert collected == []
        s = stats._stats["test_step"]
        assert s.processed == 0
        assert s.retried == 0
        assert s.errored == 1

    @pytest.mark.timeout(15)
    def test_retry_accumulates_across_items(self) -> None:
        """retried counter accumulates across all async items that needed retries."""
        step = FlakyAsyncStep(fail_times=1)
        collected, stats = _run_async([1, 2, 3], step, retry_count=2)
        assert sorted(collected) == [1, 2, 3]
        s = stats._stats["test_step"]
        assert s.processed == 3
        assert s.retried == 3
        assert s.errored == 0


class TestRetryYamlConfig:
    @pytest.mark.timeout(30)
    def test_yaml_retry_config_fields(self) -> None:
        """StepConfig parses retry_count, retry_delay, retry_backoff from YAML dict."""
        from task_pipeliner.config import StepConfig

        cfg = StepConfig(type="my_step", retry_count=3, retry_delay=0.5, retry_backoff=3.0)
        assert cfg.retry_count == 3
        assert cfg.retry_delay == 0.5
        assert cfg.retry_backoff == 3.0

    @pytest.mark.timeout(30)
    def test_yaml_retry_defaults(self) -> None:
        """StepConfig retry fields default to 0, 0.0, 2.0."""
        from task_pipeliner.config import StepConfig

        cfg = StepConfig(type="my_step")
        assert cfg.retry_count == 0
        assert cfg.retry_delay == 0.0
        assert cfg.retry_backoff == 2.0

    @pytest.mark.timeout(30)
    def test_retry_not_passed_to_step_constructor(self) -> None:
        """retry_* fields in StepConfig must NOT appear in model_extra (not forwarded to step)."""
        from task_pipeliner.config import StepConfig

        cfg = StepConfig(type="my_step", retry_count=2, retry_delay=1.0)
        extra = cfg.model_extra or {}
        assert "retry_count" not in extra
        assert "retry_delay" not in extra
        assert "retry_backoff" not in extra
