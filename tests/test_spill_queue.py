"""Tests for SpillQueue — hybrid memory/disk queue."""

from __future__ import annotations

import threading
import time

import pytest

from task_pipeliner.spill_queue import SpillQueue
from task_pipeliner.step_runners import QueueLike

# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_spill_queue_is_queue_like() -> None:
    q = SpillQueue(maxmem=10)
    assert isinstance(q, QueueLike)


# ---------------------------------------------------------------------------
# Basic put / get
# ---------------------------------------------------------------------------


def test_put_get_single_item() -> None:
    q = SpillQueue(maxmem=10)
    q.put("hello")
    assert q.get() == "hello"


def test_put_get_multiple_items_fifo() -> None:
    q = SpillQueue(maxmem=10)
    items = list(range(5))
    for i in items:
        q.put(i)
    result = [q.get() for _ in items]
    assert result == items


def test_empty_returns_true_when_empty() -> None:
    q = SpillQueue(maxmem=10)
    assert q.empty() is True


def test_empty_returns_false_after_put() -> None:
    q = SpillQueue(maxmem=10)
    q.put(1)
    assert q.empty() is False


def test_empty_returns_true_after_get() -> None:
    q = SpillQueue(maxmem=10)
    q.put(1)
    q.get()
    assert q.empty() is True


def test_put_nowait_accepted() -> None:
    q = SpillQueue(maxmem=10)
    q.put_nowait(42)
    assert q.get() == 42


def test_cancel_join_thread_is_noop() -> None:
    q = SpillQueue(maxmem=10)
    q.cancel_join_thread()  # should not raise


# ---------------------------------------------------------------------------
# Spill path: overflow to disk
# ---------------------------------------------------------------------------


@pytest.mark.timeout(10)
def test_spill_preserves_fifo_ordering() -> None:
    """Items exceeding maxmem spill to disk; FIFO order must be preserved."""
    maxmem = 5
    total = 20
    q = SpillQueue(maxmem=maxmem)

    for i in range(total):
        q.put(i)

    result = [q.get() for _ in range(total)]
    assert result == list(range(total))


@pytest.mark.timeout(10)
def test_spill_file_created_when_overflow() -> None:
    """Spill file is lazily created only when memory limit is exceeded."""
    q = SpillQueue(maxmem=3)
    assert q._spill_path is None  # no spill yet

    for i in range(3):
        q.put(i)
    assert q._spill_path is None  # still within limit

    q.put(3)  # triggers spill
    assert q._spill_path is not None


@pytest.mark.timeout(10)
def test_spill_disk_count_increases_on_overflow() -> None:
    q = SpillQueue(maxmem=2)
    q.put("a")
    q.put("b")
    assert q._disk_count == 0
    q.put("c")  # spills
    assert q._disk_count == 1


@pytest.mark.timeout(10)
def test_refill_moves_items_from_disk_to_memory() -> None:
    """After consuming memory items, the refiller must move spilled items back."""
    maxmem = 4
    total = 10
    q = SpillQueue(maxmem=maxmem)
    for i in range(total):
        q.put(i)

    # Give refiller time to run
    time.sleep(0.2)

    result = [q.get() for _ in range(total)]
    assert result == list(range(total))
    assert q.empty()


# ---------------------------------------------------------------------------
# Sentinel handling (used by the engine)
# ---------------------------------------------------------------------------


@pytest.mark.timeout(10)
def test_sentinel_object_passes_through() -> None:
    from task_pipeliner.step_runners import Sentinel

    q = SpillQueue(maxmem=10)
    s = Sentinel()
    q.put(s)
    result = q.get()
    assert isinstance(result, Sentinel)


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


@pytest.mark.timeout(15)
def test_concurrent_put_get_ordering() -> None:
    """Multiple producer + consumer threads must not lose or duplicate items."""
    n = 200
    q = SpillQueue(maxmem=20)
    produced: list[int] = []
    consumed: list[int] = []
    lock = threading.Lock()

    def producer() -> None:
        for i in range(n):
            q.put(i)
            with lock:
                produced.append(i)

    def consumer() -> None:
        for _ in range(n):
            item = q.get()
            with lock:
                consumed.append(item)

    t_prod = threading.Thread(target=producer)
    t_cons = threading.Thread(target=consumer)
    t_prod.start()
    t_cons.start()
    t_prod.join()
    t_cons.join()

    assert sorted(consumed) == sorted(produced)
    assert q.empty()


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


def test_close_cleans_up_spill_file() -> None:
    q = SpillQueue(maxmem=2)
    q.put(1)
    q.put(2)
    q.put(3)  # triggers spill
    spill_path = q._spill_path
    assert spill_path is not None and spill_path.exists()

    q.close()
    assert not spill_path.exists()


def test_close_without_spill_is_safe() -> None:
    q = SpillQueue(maxmem=10)
    q.put(1)
    q.close()  # should not raise


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_maxmem_zero_raises() -> None:
    with pytest.raises(ValueError):
        SpillQueue(maxmem=0)


def test_maxmem_negative_raises() -> None:
    with pytest.raises(ValueError):
        SpillQueue(maxmem=-1)


def test_various_types_survive_spill() -> None:
    """Arbitrary picklable types must round-trip through disk."""
    q = SpillQueue(maxmem=1)
    items = [{"key": "value"}, [1, 2, 3], (True, None), b"bytes"]
    for item in items:
        q.put(item)

    result = [q.get() for _ in items]
    assert result == items
