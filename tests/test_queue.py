"""Tests: Queue communication / sentinel propagation — W-07/W-08/W-09."""

from __future__ import annotations

import multiprocessing
import pickle
from typing import Any

import pytest
from dummy_steps import (
    CountResult,
    DummySourceStep,
    ErrorOnItemStep,
    FilterEvenStep,
    NullResult,
    PassthroughStep,
)

from task_pipeliner.producers import (
    BaseProducer,
    ErrorSentinel,
    InputProducer,
    ParallelProducer,
    Sentinel,
    SequentialProducer,
    is_sentinel,
)
from task_pipeliner.stats import StatsCollector

# ---------------------------------------------------------------------------
# Concrete subclass for testing abstract BaseProducer
# ---------------------------------------------------------------------------


class _DummyProducer(BaseProducer):
    """Minimal concrete subclass — run() is a no-op."""

    def run(self) -> None:
        pass


# ---------------------------------------------------------------------------
# W-07: Sentinel tests
# ---------------------------------------------------------------------------


class TestSentinel:
    def test_sentinel_is_picklable(self) -> None:
        s = Sentinel()
        restored = pickle.loads(pickle.dumps(s))
        assert isinstance(restored, Sentinel)

    def test_error_sentinel_stores_exc_and_step_name(self) -> None:
        exc = ValueError("boom")
        es = ErrorSentinel(exc=exc, step_name="MyStep")
        assert es.exc is exc
        assert es.step_name == "MyStep"

    def test_error_sentinel_is_picklable(self) -> None:
        exc = ValueError("boom")
        es = ErrorSentinel(exc=exc, step_name="MyStep")
        restored = pickle.loads(pickle.dumps(es))
        assert isinstance(restored, ErrorSentinel)
        assert isinstance(restored, Sentinel)
        assert str(restored.exc) == "boom"
        assert restored.step_name == "MyStep"

    def test_is_sentinel_true_for_sentinel(self) -> None:
        assert is_sentinel(Sentinel()) is True

    def test_is_sentinel_true_for_error_sentinel(self) -> None:
        assert is_sentinel(ErrorSentinel(exc=RuntimeError(), step_name="X")) is True

    def test_is_sentinel_false_for_non_sentinel(self) -> None:
        assert is_sentinel("not a sentinel") is False
        assert is_sentinel(42) is False
        assert is_sentinel(None) is False


# ---------------------------------------------------------------------------
# InputProducer tests (W-R02)
# ---------------------------------------------------------------------------


class TestInputProducer:
    """InputProducer: feeds items from a SOURCE step into output queues."""

    @pytest.mark.timeout(10)
    def test_feeds_items_from_step(self) -> None:
        """InputProducer should iterate step.items() and put into output queues."""
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[1, 2, 3])
        stats.register(step.name)

        producer = InputProducer(step=step, output_queues=[out_q], stats=stats)
        producer.run()

        collected = []
        while True:
            obj = out_q.get(timeout=2)
            if is_sentinel(obj):
                break
            collected.append(obj)

        assert collected == [1, 2, 3]

    @pytest.mark.timeout(10)
    def test_increments_passed_stat(self) -> None:
        """InputProducer should increment 'passed' for each item."""
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[10, 20, 30])
        stats.register(step.name)

        producer = InputProducer(step=step, output_queues=[out_q], stats=stats)
        producer.run()

        assert stats._stats[step.name].passed == 3

    @pytest.mark.timeout(10)
    def test_calls_step_close(self) -> None:
        """InputProducer should call step.close() after iteration."""
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[1])
        stats.register(step.name)

        producer = InputProducer(step=step, output_queues=[out_q], stats=stats)
        producer.run()

        assert step.closed is True

    @pytest.mark.timeout(10)
    def test_sends_sentinel_after_items(self) -> None:
        """InputProducer should send Sentinel to all output queues after items."""
        ctx = multiprocessing.get_context("spawn")
        out_q1: multiprocessing.Queue[Any] = ctx.Queue()
        out_q2: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[])
        stats.register(step.name)

        producer = InputProducer(step=step, output_queues=[out_q1, out_q2], stats=stats)
        producer.run()

        assert is_sentinel(out_q1.get(timeout=2))
        assert is_sentinel(out_q2.get(timeout=2))

    @pytest.mark.timeout(10)
    def test_finishes_stats(self) -> None:
        """InputProducer should call stats.finish() for the step."""
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[1, 2])
        stats.register(step.name)

        producer = InputProducer(step=step, output_queues=[out_q], stats=stats)
        producer.run()

        assert stats._stats[step.name].elapsed_seconds is not None


# ---------------------------------------------------------------------------
# W-07: BaseProducer tests
# ---------------------------------------------------------------------------


class TestBaseProducer:
    def _make_producer(self, **overrides: Any) -> _DummyProducer:
        """Helper to create a _DummyProducer with sensible defaults."""
        ctx = multiprocessing.get_context("spawn")
        step = overrides.pop("step", PassthroughStep())
        defaults: dict[str, Any] = {
            "step": step,
            "input_queue": ctx.Queue(),
            "output_queues": [],
            "stats": StatsCollector(),
            "result_queue": ctx.Queue(),
        }
        defaults.update(overrides)
        stats: StatsCollector = defaults["stats"]
        if defaults["step"].name not in stats._stats:
            stats.register(defaults["step"].name)
        return _DummyProducer(**defaults)

    @pytest.mark.timeout(10)
    def test_send_sentinel_puts_sentinel_into_each_output_queue(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        out_q1: multiprocessing.Queue[object] = ctx.Queue()
        out_q2: multiprocessing.Queue[object] = ctx.Queue()
        out_q3: multiprocessing.Queue[object] = ctx.Queue()

        producer = self._make_producer(output_queues=[out_q1, out_q2, out_q3])
        producer._send_sentinel()

        assert isinstance(out_q1.get(timeout=2), Sentinel)
        assert isinstance(out_q2.get(timeout=2), Sentinel)
        assert isinstance(out_q3.get(timeout=2), Sentinel)

    @pytest.mark.timeout(10)
    def test_wait_until_ready_no_events_is_noop(self) -> None:
        producer = self._make_producer(ready_events=None)
        producer._wait_until_ready()

    @pytest.mark.timeout(10)
    def test_wait_until_ready_waits_on_events(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        evt1 = ctx.Event()
        evt2 = ctx.Event()
        evt1.set()
        evt2.set()

        producer = self._make_producer(ready_events=[evt1, evt2])
        # Both events already set — should return immediately
        producer._wait_until_ready()

    @pytest.mark.timeout(10)
    def test_base_producer_stores_attributes(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        step = PassthroughStep()
        in_q: multiprocessing.Queue[object] = ctx.Queue()
        out_q: multiprocessing.Queue[object] = ctx.Queue()
        result_q: multiprocessing.Queue[object] = ctx.Queue()
        stats = StatsCollector()
        stats.register("PassthroughStep")
        state = {"key": "value"}

        def setter(v: object) -> None:
            pass

        producer = _DummyProducer(
            step=step,
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
            state=state,
            next_state_setter=setter,
        )
        assert producer.step is step
        assert producer.input_queue is in_q
        assert producer.output_queues == [out_q]
        assert producer.stats is stats
        assert producer.result_queue is result_q
        assert producer.state is state
        assert producer.next_state_setter is setter

    @pytest.mark.timeout(10)
    def test_make_emit_puts_item_into_output_queues(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        out_q1: multiprocessing.Queue[object] = ctx.Queue()
        out_q2: multiprocessing.Queue[object] = ctx.Queue()

        producer = self._make_producer(output_queues=[out_q1, out_q2])
        emit = producer._make_emit()
        emit({"data": 1})

        assert out_q1.get(timeout=2) == {"data": 1}
        assert out_q2.get(timeout=2) == {"data": 1}

    @pytest.mark.timeout(10)
    def test_make_emit_increments_passed_stat(self) -> None:
        producer = self._make_producer()
        emit = producer._make_emit()
        emit("item1")
        emit("item2")
        emit("item3")

        step_stats = producer.stats._stats[producer.step.name]
        assert step_stats.passed == 3

    @pytest.mark.timeout(10)
    def test_publish_result_puts_result_into_result_queue(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        result_q: multiprocessing.Queue[object] = ctx.Queue()

        producer = self._make_producer(result_queue=result_q)
        result = NullResult()
        producer._publish_result(result)

        received = result_q.get(timeout=2)
        assert isinstance(received, NullResult)


# ---------------------------------------------------------------------------
# W-08: SequentialProducer tests
# ---------------------------------------------------------------------------


class TestSequentialProducer:
    """SequentialProducer: single-process run(), direct call (no spawn)."""

    def _run_sequential(
        self,
        step: Any,
        items: list[Any],
        *,
        state: Any = None,
    ) -> tuple[list[Any], Any, StatsCollector]:
        """Helper: feed *items* into a SequentialProducer.run() and return
        (output_items, result_from_queue, stats).
        """
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register(step.name)

        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=step,
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
            state=state,
        )
        # Direct call — no subprocess
        producer.run()

        # Drain output queue
        collected: list[Any] = []
        while True:
            obj = out_q.get(timeout=2)
            if is_sentinel(obj):
                break
            collected.append(obj)

        # Get result (may be absent if 0 items processed)
        result = result_q.get(timeout=2) if not result_q.empty() else None

        return collected, result, stats

    @pytest.mark.timeout(15)
    @pytest.mark.parametrize("n", [0, 1, 10, 100])
    def test_passthrough_n_items(self, n: int) -> None:
        items = list(range(n))
        collected, result, stats = self._run_sequential(PassthroughStep(), items)
        assert collected == items
        step_stats = stats._stats["PassthroughStep"]
        assert step_stats.passed == n

    @pytest.mark.timeout(15)
    def test_filter_even_step(self) -> None:
        items = list(range(10))
        collected, result, stats = self._run_sequential(FilterEvenStep(), items)
        assert collected == [0, 2, 4, 6, 8]
        assert all(x % 2 == 0 for x in collected)

    @pytest.mark.timeout(15)
    def test_empty_input_sends_sentinel(self) -> None:
        """Empty input → sentinel still propagated to output queue."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("PassthroughStep")

        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=PassthroughStep(),
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
        )
        producer.run()

        obj = out_q.get(timeout=2)
        assert is_sentinel(obj)

    @pytest.mark.timeout(15)
    def test_result_queue_receives_count_result(self) -> None:
        items = list(range(10))
        collected, result, stats = self._run_sequential(FilterEvenStep(), items)
        assert isinstance(result, CountResult)
        assert result.passed + result.filtered == 10
        assert result.passed == 5
        assert result.filtered == 5

    @pytest.mark.timeout(15)
    def test_stats_passed_equals_emit_count(self) -> None:
        items = list(range(20))
        collected, result, stats = self._run_sequential(FilterEvenStep(), items)
        step_stats = stats._stats["FilterEvenStep"]
        assert step_stats.passed == len(collected)

    @pytest.mark.timeout(15)
    def test_error_items_skipped_and_counted(self) -> None:
        """Items that raise in process() are skipped; errored stat incremented."""
        # ErrorOnItemStep raises on error_value=-1, emits everything else
        items = [1, 2, -1, 3, -1, 4]
        collected, result, stats = self._run_sequential(ErrorOnItemStep(error_value=-1), items)
        # -1 항목 2개는 emit되지 않아야 함
        assert collected == [1, 2, 3, 4]
        step_stats = stats._stats["ErrorOnItemStep"]
        assert step_stats.errored == 2
        assert step_stats.passed == 4

    @pytest.mark.timeout(15)
    def test_error_does_not_prevent_result_publish(self) -> None:
        """Even when some items error, accumulated result is still published."""
        items = [1, -1, 2, -1, 3]
        collected, result, stats = self._run_sequential(ErrorOnItemStep(error_value=-1), items)
        # 성공한 3개의 NullResult가 merge되어 publish됨
        assert isinstance(result, NullResult)
        assert len(collected) == 3

    @pytest.mark.timeout(15)
    def test_all_items_error_still_sends_sentinel(self) -> None:
        """If every item errors, sentinel is still sent and no result published."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = ErrorOnItemStep(error_value=-1)
        stats.register(step.name)

        # 모든 아이템이 에러
        for _ in range(5):
            in_q.put(-1)
        in_q.put(Sentinel())

        producer = SequentialProducer(
            step=step,
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
        )
        producer.run()

        # sentinel은 반드시 전파됨
        obj = out_q.get(timeout=2)
        assert is_sentinel(obj)
        # 성공 항목 없으므로 result는 publish 안 됨
        assert result_q.empty()
        assert stats._stats[step.name].errored == 5

    @pytest.mark.timeout(15)
    def test_mixed_filter_and_error_result_merge(self) -> None:
        """FilterEvenStep + error items: result merge reflects only successful processing."""
        # FilterEvenStep은 짝수만 emit, 홀수는 filtered로 카운트
        # 하지만 FilterEvenStep은 에러를 안 내니까, ErrorOnItemStep과 다르게 쓴다.
        # 여기서는 10개 중 5개 짝수, 5개 홀수 → merge 결과 확인
        items = list(range(10))
        collected, result, stats = self._run_sequential(FilterEvenStep(), items)
        assert isinstance(result, CountResult)
        # merge가 10번의 process() 결과를 정확히 누적했는지
        assert result.passed == 5
        assert result.filtered == 5
        assert result.passed + result.filtered == len(items)


# ---------------------------------------------------------------------------
# W-09: ParallelProducer tests
# ---------------------------------------------------------------------------


class TestParallelProducer:
    """ParallelProducer: multi-process via ProcessPoolExecutor, direct run() call."""

    def _run_parallel(
        self,
        step: Any,
        items: list[Any],
        *,
        workers: int = 2,
        chunk_size: int = 10,
        state: Any = None,
    ) -> tuple[list[Any], Any, StatsCollector]:
        """Helper: feed *items* into a ParallelProducer.run() and return
        (output_items, result_from_queue, stats).
        """
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register(step.name)

        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = ParallelProducer(
            step=step,
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
            state=state,
            workers=workers,
            chunk_size=chunk_size,
        )
        # Direct call — no subprocess
        producer.run()

        # Drain output queue
        collected: list[Any] = []
        while True:
            obj = out_q.get(timeout=5)
            if is_sentinel(obj):
                break
            collected.append(obj)

        # Get result (may be absent if 0 items processed)
        result = result_q.get(timeout=5) if not result_q.empty() else None

        return collected, result, stats

    @pytest.mark.timeout(30)
    @pytest.mark.parametrize("workers,n", [(1, 50), (2, 50), (4, 100)])
    def test_passthrough_item_count(self, workers: int, n: int) -> None:
        items = list(range(n))
        collected, result, stats = self._run_parallel(PassthroughStep(), items, workers=workers)
        assert sorted(collected) == items

    @pytest.mark.timeout(30)
    def test_filter_even_same_result_regardless_of_workers(self) -> None:
        items = list(range(20))
        collected_1, _, _ = self._run_parallel(FilterEvenStep(), items, workers=1)
        collected_4, _, _ = self._run_parallel(FilterEvenStep(), items, workers=4)
        assert sorted(collected_1) == sorted(collected_4)
        assert all(x % 2 == 0 for x in collected_1)

    @pytest.mark.timeout(30)
    def test_sentinel_propagated(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        result_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        stats.register("PassthroughStep")

        in_q.put(Sentinel())

        producer = ParallelProducer(
            step=PassthroughStep(),
            input_queue=in_q,
            output_queues=[out_q],
            stats=stats,
            result_queue=result_q,
            workers=2,
        )
        producer.run()

        obj = out_q.get(timeout=5)
        assert is_sentinel(obj)

    @pytest.mark.timeout(30)
    def test_result_queue_count_result(self) -> None:
        items = list(range(20))
        collected, result, stats = self._run_parallel(FilterEvenStep(), items, workers=2)
        assert isinstance(result, CountResult)
        assert result.passed + result.filtered == 20
        assert result.passed == 10
        assert result.filtered == 10

    @pytest.mark.timeout(30)
    def test_error_items_skipped_result_still_published(self) -> None:
        items = [1, 2, -1, 3, -1, 4, 5, 6]
        collected, result, stats = self._run_parallel(
            ErrorOnItemStep(error_value=-1), items, workers=2
        )
        assert sorted(collected) == [1, 2, 3, 4, 5, 6]
        assert isinstance(result, NullResult)
