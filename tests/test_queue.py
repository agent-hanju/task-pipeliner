"""Tests: Queue communication / sentinel propagation — W-07/W-08/W-09."""

from __future__ import annotations

import multiprocessing
import pickle
from typing import Any

import pytest
from dummy_steps import (
    DummySourceStep,
    ErrorOnItemStep,
    ErrorOnItemWorker,
    FilterEvenStep,
    FilterEvenWorker,
    PassthroughStep,
    PassthroughWorker,
    SequentialErrorOnItemStep,
    SequentialFilterEvenStep,
    SequentialPassthroughStep,
    TerminalStep,
)

from task_pipeliner.stats import StatsCollector
from task_pipeliner.step_runners import (
    BaseStepRunner,
    ChunkResult,
    ErrorSentinel,
    InputStepRunner,
    ParallelStepRunner,
    Sentinel,
    SequentialStepRunner,
    _parallel_worker,
    is_sentinel,
)

# ---------------------------------------------------------------------------
# Concrete subclass for testing abstract BaseStepRunner
# ---------------------------------------------------------------------------


class _DummyProducer(BaseStepRunner):
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
# InputStepRunner tests (W-R02)
# ---------------------------------------------------------------------------


class TestInputStepRunner:
    """InputStepRunner: feeds items from a SOURCE step into output queues."""

    @pytest.mark.timeout(10)
    def test_feeds_items_from_step(self) -> None:
        """InputStepRunner should iterate step.items() and put into output queues."""
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[1, 2, 3])
        step_name = "source"
        stats.register(step_name)

        producer = InputStepRunner(
            step=step, step_name=step_name, output_queues={"main": [out_q]}, stats=stats
        )
        producer.run()

        collected = []
        while True:
            obj = out_q.get(timeout=2)
            if is_sentinel(obj):
                break
            collected.append(obj)

        assert collected == [1, 2, 3]

    @pytest.mark.timeout(10)
    def test_increments_processed_stat(self) -> None:
        """InputStepRunner should increment 'processed' for each item."""
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[10, 20, 30])
        step_name = "source"
        stats.register(step_name)

        producer = InputStepRunner(
            step=step, step_name=step_name, output_queues={"main": [out_q]}, stats=stats
        )
        producer.run()

        assert stats._stats[step_name].processed == 3
        assert stats._stats[step_name].emitted == {"main": 3}

    @pytest.mark.timeout(10)
    def test_calls_step_close(self) -> None:
        """InputStepRunner should call step.close() after iteration."""
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[1])
        step_name = "source"
        stats.register(step_name)

        producer = InputStepRunner(
            step=step, step_name=step_name, output_queues={"main": [out_q]}, stats=stats
        )
        producer.run()

        assert step.closed is True

    @pytest.mark.timeout(10)
    def test_sends_sentinel_after_items(self) -> None:
        """InputStepRunner should send Sentinel to all output queues after items."""
        ctx = multiprocessing.get_context("spawn")
        out_q1: multiprocessing.Queue[Any] = ctx.Queue()
        out_q2: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[])
        step_name = "source"
        stats.register(step_name)

        producer = InputStepRunner(
            step=step, step_name=step_name,
            output_queues={"a": [out_q1], "b": [out_q2]}, stats=stats,
        )
        producer.run()

        assert is_sentinel(out_q1.get(timeout=2))
        assert is_sentinel(out_q2.get(timeout=2))

    @pytest.mark.timeout(10)
    def test_finishes_stats(self) -> None:
        """InputStepRunner should call stats.finish() for the step."""
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step = DummySourceStep(items=[1, 2])
        step_name = "source"
        stats.register(step_name)

        producer = InputStepRunner(
            step=step, step_name=step_name, output_queues={"main": [out_q]}, stats=stats
        )
        producer.run()

        assert stats._stats[step_name].elapsed_seconds is not None


# ---------------------------------------------------------------------------
# W-07: BaseStepRunner tests
# ---------------------------------------------------------------------------


class TestBaseStepRunner:
    def _make_producer(self, **overrides: Any) -> _DummyProducer:
        """Helper to create a _DummyProducer with sensible defaults."""
        ctx = multiprocessing.get_context("spawn")
        step = overrides.pop("step", PassthroughStep())
        step_name = overrides.pop("step_name", type(step).__name__)
        defaults: dict[str, Any] = {
            "step": step,
            "step_name": step_name,
            "input_queue": ctx.Queue(),
            "output_queues": {},
            "stats": StatsCollector(),
        }
        defaults.update(overrides)
        stats: StatsCollector = defaults["stats"]
        if defaults["step_name"] not in stats._stats:
            stats.register(defaults["step_name"])
        return _DummyProducer(**defaults)

    @pytest.mark.timeout(10)
    def test_send_sentinel_puts_sentinel_into_each_output_queue(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        out_q1: multiprocessing.Queue[object] = ctx.Queue()
        out_q2: multiprocessing.Queue[object] = ctx.Queue()
        out_q3: multiprocessing.Queue[object] = ctx.Queue()

        producer = self._make_producer(output_queues={"a": [out_q1], "b": [out_q2], "c": [out_q3]})
        producer._send_sentinel()

        assert isinstance(out_q1.get(timeout=2), Sentinel)
        assert isinstance(out_q2.get(timeout=2), Sentinel)
        assert isinstance(out_q3.get(timeout=2), Sentinel)

    @pytest.mark.timeout(10)
    def test_wait_until_is_ready_no_events_is_noop(self) -> None:
        producer = self._make_producer(ready_events=None)
        producer._wait_until_is_ready()

    @pytest.mark.timeout(10)
    def test_wait_until_is_ready_waits_on_events(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        evt1 = ctx.Event()
        evt2 = ctx.Event()
        evt1.set()
        evt2.set()

        producer = self._make_producer(ready_events=[evt1, evt2])
        # Both events already set — should return immediately
        producer._wait_until_is_ready()

    @pytest.mark.timeout(10)
    def test_base_producer_stores_attributes(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        step = PassthroughStep()
        in_q: multiprocessing.Queue[object] = ctx.Queue()
        out_q: multiprocessing.Queue[object] = ctx.Queue()
        stats = StatsCollector()
        stats.register("PassthroughStep")
        state = {"key": "value"}

        producer = _DummyProducer(
            step=step,
            step_name="PassthroughStep",
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            state=state,
        )
        assert producer.step is step
        assert producer.step_name == "PassthroughStep"
        assert producer.input_queue is in_q
        assert producer.output_queues == {"main": [out_q]}
        assert producer.stats is stats
        assert producer.state is state

    @pytest.mark.timeout(10)
    def test_make_emit_puts_item_into_output_queues(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        out_q1: multiprocessing.Queue[object] = ctx.Queue()
        out_q2: multiprocessing.Queue[object] = ctx.Queue()

        producer = self._make_producer(output_queues={"main": [out_q1, out_q2]})
        emit = producer._make_emit()
        emit({"data": 1}, "main")

        assert out_q1.get(timeout=2) == {"data": 1}
        assert out_q2.get(timeout=2) == {"data": 1}

    @pytest.mark.timeout(10)
    def test_make_emit_increments_emitted_stat(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        out_q: multiprocessing.Queue[object] = ctx.Queue()
        producer = self._make_producer(output_queues={"main": [out_q]})
        emit = producer._make_emit()
        emit("item1", "main")
        emit("item2", "main")
        emit("item3", "main")

        step_stats = producer.stats._stats[producer.step_name]
        assert step_stats.emitted == {"main": 3}


# ---------------------------------------------------------------------------
# _parallel_worker unit tests
# ---------------------------------------------------------------------------


class TestParallelWorker:
    """Unit tests for _parallel_worker — items returned, not put into queues."""

    def _setup_worker(self, worker: Any, state: Any = None) -> None:
        """Set process-global worker instance/state for _parallel_worker."""
        import task_pipeliner.step_runners as _mod

        _mod._worker_instance = worker
        _mod._worker_state = state

    def test_returns_emitted_items_by_tag(self) -> None:
        """PassthroughWorker → emitted items included in return value."""
        self._setup_worker(PassthroughWorker())
        chunk_result = _parallel_worker([1, 2, 3], "PassthroughStep")
        assert isinstance(chunk_result, ChunkResult)
        assert chunk_result.processed == 3
        assert chunk_result.errored == 0
        assert "main" in chunk_result.emitted
        assert sorted(chunk_result.emitted["main"]) == [1, 2, 3]

    def test_returns_filtered_items_only(self) -> None:
        """FilterEvenWorker → only even items in return value."""
        self._setup_worker(FilterEvenWorker())
        chunk_result = _parallel_worker([1, 2, 3, 4, 5], "FilterEvenStep")
        assert chunk_result.processed == 5
        assert sorted(chunk_result.emitted.get("main", [])) == [2, 4]

    def test_returns_empty_on_all_errors(self) -> None:
        """All items error → emitted is empty dict."""
        self._setup_worker(ErrorOnItemWorker(error_value=-1))
        chunk_result = _parallel_worker([-1, -1, -1], "ErrorOnItemStep")
        assert chunk_result.processed == 0
        assert chunk_result.errored == 3
        assert chunk_result.emitted == {}

    def test_terminal_step_no_items(self) -> None:
        """TerminalStep (outputs=()) → no items emitted."""

        # TerminalStep is sequential, but we can use a worker that does nothing
        class _NoopWorker:
            def process(self, item: Any, state: Any, emit: Any) -> None:
                pass  # Terminal — no emit

        self._setup_worker(_NoopWorker())
        chunk_result = _parallel_worker([1], "TerminalStep")
        assert chunk_result.processed == 1
        assert chunk_result.emitted == {}

    def test_processing_ns_tracked(self) -> None:
        """Worker tracks processing time."""
        self._setup_worker(PassthroughWorker())
        chunk_result = _parallel_worker(list(range(10)), "PassthroughStep")
        assert chunk_result.processing_ns > 0


# ---------------------------------------------------------------------------
# W-08: SequentialStepRunner tests
# ---------------------------------------------------------------------------


class TestSequentialStepRunner:
    """SequentialStepRunner: single-process run(), direct call (no spawn)."""

    def _run_sequential(
        self,
        step: Any,
        items: list[Any],
        *,
        state: Any = None,
        step_name: str | None = None,
    ) -> tuple[list[Any], StatsCollector]:
        """Helper: feed *items* into a SequentialStepRunner.run() and return
        (output_items, stats).
        """
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        name = step_name or type(step).__name__
        stats.register(name)

        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = SequentialStepRunner(
            step=step,
            step_name=name,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
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

        return collected, stats

    @pytest.mark.timeout(15)
    @pytest.mark.parametrize("n", [0, 1, 10, 100])
    def test_passthrough_n_items(self, n: int) -> None:
        items = list(range(n))
        collected, stats = self._run_sequential(SequentialPassthroughStep(), items)
        assert collected == items
        step_stats = stats._stats["SequentialPassthroughStep"]
        assert step_stats.processed == n
        if n > 0:
            assert step_stats.emitted == {"main": n}
        else:
            assert step_stats.emitted == {}

    @pytest.mark.timeout(15)
    def test_filter_even_step(self) -> None:
        items = list(range(10))
        collected, stats = self._run_sequential(SequentialFilterEvenStep(), items)
        assert collected == [0, 2, 4, 6, 8]
        assert all(x % 2 == 0 for x in collected)

    @pytest.mark.timeout(15)
    def test_empty_input_sends_sentinel(self) -> None:
        """Empty input → sentinel still propagated to output queue."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step_name = "SequentialPassthroughStep"
        stats.register(step_name)

        in_q.put(Sentinel())

        producer = SequentialStepRunner(
            step=SequentialPassthroughStep(),
            step_name=step_name,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
        )
        producer.run()

        obj = out_q.get(timeout=2)
        assert is_sentinel(obj)

    @pytest.mark.timeout(15)
    def test_stats_processed_and_emitted(self) -> None:
        items = list(range(20))
        collected, stats = self._run_sequential(SequentialFilterEvenStep(), items)
        step_stats = stats._stats["SequentialFilterEvenStep"]
        assert step_stats.processed == 20
        assert step_stats.emitted == {"main": len(collected)}

    @pytest.mark.timeout(15)
    def test_error_items_skipped_and_counted(self) -> None:
        """Items that raise in process() are skipped; errored stat incremented."""
        items = [1, 2, -1, 3, -1, 4]
        collected, stats = self._run_sequential(SequentialErrorOnItemStep(error_value=-1), items)
        assert collected == [1, 2, 3, 4]
        step_stats = stats._stats["SequentialErrorOnItemStep"]
        assert step_stats.errored == 2
        assert step_stats.processed == 4
        assert step_stats.emitted == {"main": 4}

    @pytest.mark.timeout(15)
    def test_all_items_error_still_sends_sentinel(self) -> None:
        """If every item errors, sentinel is still sent."""
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step_name = "SequentialErrorOnItemStep"
        stats.register(step_name)

        for _ in range(5):
            in_q.put(-1)
        in_q.put(Sentinel())

        producer = SequentialStepRunner(
            step=SequentialErrorOnItemStep(error_value=-1),
            step_name=step_name,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
        )
        producer.run()

        obj = out_q.get(timeout=2)
        assert is_sentinel(obj)
        assert stats._stats[step_name].errored == 5

    @pytest.mark.timeout(15)
    def test_filter_even_stats(self) -> None:
        """SequentialFilterEvenStep with 10 items: 5 emitted, 10 processed."""
        items = list(range(10))
        collected, stats = self._run_sequential(SequentialFilterEvenStep(), items)
        step_stats = stats._stats["SequentialFilterEvenStep"]
        assert step_stats.processed == 10
        assert step_stats.emitted == {"main": 5}


# ---------------------------------------------------------------------------
# W-09: ParallelStepRunner tests
# ---------------------------------------------------------------------------


class TestParallelStepRunner:
    """ParallelStepRunner: multi-process via ProcessPoolExecutor, direct run() call."""

    def _run_parallel(
        self,
        step: Any,
        items: list[Any],
        *,
        workers: int = 2,
        chunk_size: int = 10,
        state: Any = None,
        step_name: str | None = None,
    ) -> tuple[list[Any], StatsCollector]:
        """Helper: feed *items* into a ParallelStepRunner.run() and return
        (output_items, stats).
        """
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        name = step_name or type(step).__name__
        stats.register(name)

        for item in items:
            in_q.put(item)
        in_q.put(Sentinel())

        producer = ParallelStepRunner(
            step=step,
            step_name=name,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
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

        return collected, stats

    @pytest.mark.timeout(30)
    @pytest.mark.parametrize("workers,n", [(1, 50), (2, 50), (4, 100)])
    def test_passthrough_item_count(self, workers: int, n: int) -> None:
        items = list(range(n))
        collected, stats = self._run_parallel(PassthroughStep(), items, workers=workers)
        assert sorted(collected) == items

    @pytest.mark.timeout(30)
    def test_filter_even_same_result_regardless_of_workers(self) -> None:
        items = list(range(20))
        collected_1, _ = self._run_parallel(FilterEvenStep(), items, workers=1)
        collected_4, _ = self._run_parallel(FilterEvenStep(), items, workers=4)
        assert sorted(collected_1) == sorted(collected_4)
        assert all(x % 2 == 0 for x in collected_1)

    @pytest.mark.timeout(30)
    def test_sentinel_propagated(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        in_q: multiprocessing.Queue[Any] = ctx.Queue()
        out_q: multiprocessing.Queue[Any] = ctx.Queue()
        stats = StatsCollector()
        step_name = "PassthroughStep"
        stats.register(step_name)

        in_q.put(Sentinel())

        producer = ParallelStepRunner(
            step=PassthroughStep(),
            step_name=step_name,
            input_queue=in_q,
            output_queues={"main": [out_q]},
            stats=stats,
            workers=2,
        )
        producer.run()

        obj = out_q.get(timeout=5)
        assert is_sentinel(obj)

    @pytest.mark.timeout(30)
    def test_filter_even_stats(self) -> None:
        items = list(range(20))
        collected, stats = self._run_parallel(FilterEvenStep(), items, workers=2)
        step_stats = stats._stats["FilterEvenStep"]
        assert step_stats.processed == 20
        assert step_stats.emitted == {"main": 10}

    @pytest.mark.timeout(30)
    def test_error_items_skipped(self) -> None:
        items = [1, 2, -1, 3, -1, 4, 5, 6]
        collected, stats = self._run_parallel(
            ErrorOnItemStep(error_value=-1), items, workers=2
        )
        assert sorted(collected) == [1, 2, 3, 4, 5, 6]


# ---------------------------------------------------------------------------
# W-T02: Tagged emit routing tests
# ---------------------------------------------------------------------------


class TestTaggedEmitRouting:
    """Tests for dict-based output_queues with tag routing."""

    @pytest.mark.timeout(10)
    def test_emit_routes_to_correct_tag_queue(self) -> None:
        """emit(item, 'main') puts item only into the 'main' tag's queues."""
        ctx = multiprocessing.get_context("spawn")
        q_main: multiprocessing.Queue[Any] = ctx.Queue()
        q_other: multiprocessing.Queue[Any] = ctx.Queue()

        stats = StatsCollector()
        step_name = "PassthroughStep"
        stats.register(step_name)

        producer = _DummyProducer(
            step=PassthroughStep(),
            step_name=step_name,
            input_queue=ctx.Queue(),
            output_queues={"main": [q_main], "other": [q_other]},
            stats=stats,
        )
        emit = producer._make_emit()
        emit("hello", "main")

        assert q_main.get(timeout=2) == "hello"
        assert q_other.empty()

    @pytest.mark.timeout(10)
    def test_emit_unconnected_tag_is_silent_drop(self) -> None:
        """emit(item, 'unknown') where tag has no queue mapping → silent drop."""
        ctx = multiprocessing.get_context("spawn")
        q_main: multiprocessing.Queue[Any] = ctx.Queue()

        stats = StatsCollector()
        step_name = "PassthroughStep"
        stats.register(step_name)

        producer = _DummyProducer(
            step=PassthroughStep(),
            step_name=step_name,
            input_queue=ctx.Queue(),
            output_queues={"main": [q_main]},
            stats=stats,
        )
        emit = producer._make_emit()
        emit("hello", "nonexistent")

        assert q_main.empty()
        # emitted stat should NOT be incremented for dropped items
        assert stats._stats[step_name].emitted == {}

    @pytest.mark.timeout(10)
    def test_emit_on_terminal_step_raises_runtime_error(self) -> None:
        """outputs = () step calling emit → RuntimeError."""
        ctx = multiprocessing.get_context("spawn")

        stats = StatsCollector()
        step_name = "TerminalStep"
        stats.register(step_name)

        producer = _DummyProducer(
            step=TerminalStep(),
            step_name=step_name,
            input_queue=ctx.Queue(),
            output_queues={},
            stats=stats,
        )
        emit = producer._make_emit()

        with pytest.raises(RuntimeError, match="no declared outputs"):
            emit("item", "any_tag")

    @pytest.mark.timeout(10)
    def test_fanout_single_tag_multiple_queues(self) -> None:
        """One tag connected to multiple queues → item goes to all."""
        ctx = multiprocessing.get_context("spawn")
        q1: multiprocessing.Queue[Any] = ctx.Queue()
        q2: multiprocessing.Queue[Any] = ctx.Queue()

        stats = StatsCollector()
        step_name = "PassthroughStep"
        stats.register(step_name)

        producer = _DummyProducer(
            step=PassthroughStep(),
            step_name=step_name,
            input_queue=ctx.Queue(),
            output_queues={"main": [q1, q2]},
            stats=stats,
        )
        emit = producer._make_emit()
        emit("data", "main")

        assert q1.get(timeout=2) == "data"
        assert q2.get(timeout=2) == "data"

    @pytest.mark.timeout(10)
    def test_send_sentinel_to_all_tag_queues(self) -> None:
        """_send_sentinel() puts sentinel into every unique queue across all tags."""
        ctx = multiprocessing.get_context("spawn")
        q1: multiprocessing.Queue[Any] = ctx.Queue()
        q2: multiprocessing.Queue[Any] = ctx.Queue()

        stats = StatsCollector()
        step_name = "PassthroughStep"
        stats.register(step_name)

        producer = _DummyProducer(
            step=PassthroughStep(),
            step_name=step_name,
            input_queue=ctx.Queue(),
            output_queues={"kept": [q1], "removed": [q2]},
            stats=stats,
        )
        producer._send_sentinel()

        assert isinstance(q1.get(timeout=2), Sentinel)
        assert isinstance(q2.get(timeout=2), Sentinel)

    @pytest.mark.timeout(10)
    def test_send_sentinel_deduplicates_shared_queue(self) -> None:
        """If the same queue appears under multiple tags, sentinel sent only once."""
        ctx = multiprocessing.get_context("spawn")
        shared_q: multiprocessing.Queue[Any] = ctx.Queue()

        stats = StatsCollector()
        step_name = "PassthroughStep"
        stats.register(step_name)

        producer = _DummyProducer(
            step=PassthroughStep(),
            step_name=step_name,
            input_queue=ctx.Queue(),
            output_queues={"tag_a": [shared_q], "tag_b": [shared_q]},
            stats=stats,
        )
        producer._send_sentinel()

        assert isinstance(shared_q.get(timeout=2), Sentinel)
        # Only one sentinel should have been sent
        assert shared_q.empty()
