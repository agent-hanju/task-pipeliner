"""SpillQueue — thread-safe queue with automatic disk spill to bound memory usage.

Drop-in replacement for *multiprocessing.Queue* in contexts where the queue is
only accessed by threads within a single process (never pickled across process
boundaries).

Usage
-----
SpillQueue is activated via the ``execution.queue_size`` config key:

.. code-block:: yaml

    execution:
      queue_size: 10000   # keep ≤ 10 000 items in memory; remainder spills to disk
      workers: 4

When ``queue_size`` is 0 (the default) the engine uses a plain
*multiprocessing.Queue* with no size limit (original behaviour).
"""

from __future__ import annotations

import logging
import os
import pickle  # noqa: S403
import queue
import struct
import tempfile
import threading
from pathlib import Path
from typing import IO, Any

logger = logging.getLogger(__name__)

_HEADER = struct.Struct(">Q")  # 8-byte big-endian length prefix


class SpillQueue:
    """Thread-safe FIFO queue that spills items to disk when the memory buffer is full.

    Parameters
    ----------
    maxmem:
        Maximum number of items to keep in the in-memory buffer.  When the
        buffer is full *and* no spill exists yet, further ``put()`` calls write
        items to a temporary file instead of blocking.  A background *refiller*
        thread continuously drains the file back into memory whenever space
        becomes available.

        Must be a positive integer.
    temp_dir:
        Directory for the spill file.  Uses the system temp directory when
        *None*.

    Notes
    -----
    * FIFO ordering is preserved across memory and disk.
    * ``put()`` never blocks — if memory is full the item is serialised to disk
      immediately.
    * ``get()`` blocks until an item is available (same as
      ``multiprocessing.Queue.get()``).
    * Not picklable — do not pass to worker processes.
    """

    def __init__(self, maxmem: int = 10_000, temp_dir: Path | None = None) -> None:
        if maxmem <= 0:
            raise ValueError(f"maxmem must be positive, got {maxmem}")
        self._maxmem = maxmem
        self._temp_dir = temp_dir

        # In-memory buffer — unbounded; we enforce the limit ourselves so we
        # never have to worry about queue.Full being raised inside a lock.
        self._mem: queue.Queue[Any] = queue.Queue()

        # _lock guards _mem_count, _disk_count, and the file handles.
        # It is also the underlying lock for _refill_cond.
        self._lock = threading.Lock()
        self._refill_cond = threading.Condition(self._lock)

        # Item counters (protected by _lock)
        self._mem_count: int = 0
        self._disk_count: int = 0

        # Spill file — lazily created on first spill
        self._spill_path: Path | None = None
        self._write_fd: IO[bytes] | None = None  # binary file opened for appending
        self._read_fd: IO[bytes] | None = None   # binary file opened for reading

        # Background refiller thread — started when spill file is created
        self._refiller: threading.Thread | None = None
        self._closed = False

    # ------------------------------------------------------------------
    # Public interface — matches multiprocessing.Queue
    # ------------------------------------------------------------------

    def put(self, obj: Any, block: bool = True, timeout: float | None = None) -> None:  # noqa: ARG002
        """Add *obj* to the queue.

        Never blocks: if memory is full the item is written to disk.
        The *block* and *timeout* parameters are accepted for API compatibility
        but are ignored.
        """
        with self._refill_cond:
            if self._disk_count == 0 and self._mem_count < self._maxmem:
                self._mem.put_nowait(obj)
                self._mem_count += 1
            else:
                self._spill(obj)

    def put_nowait(self, obj: Any) -> None:
        """Alias for ``put(obj)`` — never raises :exc:`queue.Full`."""
        self.put(obj)

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        """Remove and return an item, blocking until one is available."""
        item = self._mem.get(block=block, timeout=timeout)
        with self._refill_cond:
            self._mem_count -= 1
            # Wake the refiller when memory has drained enough
            if self._disk_count > 0 and self._mem_count < self._maxmem // 2:
                self._refill_cond.notify()
        return item

    def empty(self) -> bool:
        """Return *True* if there are no items in memory or on disk."""
        with self._lock:
            return self._mem_count == 0 and self._disk_count == 0

    def cancel_join_thread(self) -> None:
        """No-op — compatibility shim for *multiprocessing.Queue*."""

    def close(self) -> None:
        """Release spill file resources and stop the refiller thread."""
        with self._refill_cond:
            self._closed = True
            self._refill_cond.notify_all()
        if self._refiller is not None:
            self._refiller.join(timeout=2)
        if self._write_fd is not None:
            try:
                self._write_fd.close()
            except OSError:
                pass
        if self._read_fd is not None:
            try:
                self._read_fd.close()
            except OSError:
                pass
        if self._spill_path is not None:
            try:
                self._spill_path.unlink(missing_ok=True)
            except OSError:
                pass

    # ------------------------------------------------------------------
    # Internal helpers (all called with _lock / _refill_cond held)
    # ------------------------------------------------------------------

    def _spill(self, item: Any) -> None:
        """Serialise *item* and append to the spill file (lock held)."""
        if self._write_fd is None:
            self._init_spill_file()
        write_fd = self._write_fd
        assert write_fd is not None  # guaranteed by _init_spill_file()
        data = pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
        header = _HEADER.pack(len(data))
        write_fd.write(header + data)
        write_fd.flush()
        self._disk_count += 1
        logger.debug("spilled item to disk disk_count=%d", self._disk_count)
        self._refill_cond.notify()

    def _init_spill_file(self) -> None:
        """Create the spill file and start the refiller thread (lock held)."""
        fd, path = tempfile.mkstemp(suffix=".spill.pkl", dir=self._temp_dir)
        os.close(fd)
        spill_path = Path(path)
        write_fd: IO[bytes] | None = None
        read_fd: IO[bytes] | None = None
        try:
            # Unbuffered I/O — writes are immediate; no fsync() so not crash-durable,
            # which is acceptable (data still in memory on the producer side).
            write_fd = open(spill_path, "ab", buffering=0)  # noqa: SIM115
            read_fd = open(spill_path, "rb", buffering=0)   # noqa: SIM115
            refiller = threading.Thread(
                target=self._refill_loop, daemon=True, name="spill-refiller"
            )
            refiller.start()
        except Exception:
            logger.error(
                "failed to initialize spill file temp_dir=%s", self._temp_dir, exc_info=True
            )
            if write_fd is not None:
                try:
                    write_fd.close()
                except OSError:
                    pass
            if read_fd is not None:
                try:
                    read_fd.close()
                except OSError:
                    pass
            try:
                spill_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        # Commit state only after all resources are successfully acquired
        self._spill_path = spill_path
        self._write_fd = write_fd
        self._read_fd = read_fd
        self._refiller = refiller
        logger.info(
            "spill file created path=%s maxmem=%d", self._spill_path, self._maxmem
        )

    def _read_one(self) -> tuple[Any | None, bool]:
        """Read one item from the spill file (lock held).

        Returns ``(item, True)`` on success, ``(None, False)`` if not enough
        data is available yet (writer is still flushing).
        """
        if self._read_fd is None:
            raise RuntimeError("SpillQueue read fd not initialized")
        header_bytes = self._read_fd.read(_HEADER.size)
        if len(header_bytes) < _HEADER.size:
            # Incomplete header — seek back
            if header_bytes:
                self._read_fd.seek(-len(header_bytes), 1)
            return None, False
        (size,) = _HEADER.unpack(header_bytes)
        data = self._read_fd.read(size)
        if len(data) < size:
            # Incomplete payload — seek back to start of this record
            self._read_fd.seek(-(_HEADER.size + len(data)), 1)
            return None, False
        return pickle.loads(data), True  # noqa: S301

    def _refill_loop(self) -> None:
        """Background thread: move items from disk into memory when space is free."""
        target = max(1, self._maxmem // 2)
        while True:
            with self._refill_cond:
                # Wait until there is disk data AND memory has room
                while not self._closed and (
                    self._disk_count == 0 or self._mem_count >= target
                ):
                    self._refill_cond.wait(timeout=0.1)
                if self._closed:
                    return
                # Move items until memory is half-full or disk is empty
                moved = 0
                while self._disk_count > 0 and self._mem_count < target:
                    item, ok = self._read_one()
                    if not ok:
                        break
                    self._mem.put_nowait(item)
                    self._disk_count -= 1
                    self._mem_count += 1
                    moved += 1
                if moved:
                    logger.debug(
                        "refilled %d items from disk disk_remaining=%d",
                        moved,
                        self._disk_count,
                    )


class FullDiskQueue:
    """Disk-primary FIFO queue for is_ready()-gated pipeline steps.

    All items are written to disk immediately.  Use this queue when the
    upstream stage must fully complete before the downstream stage starts
    (i.e. the downstream step overrides ``is_ready()``).  Unlike
    :class:`SpillQueue`, there is no in-memory buffer — every ``put()``
    goes straight to disk and every ``get()`` reads from disk.

    Backend: ``persistqueue.Queue`` (file-based, no SQLite dependency).

    .. note::
        Requires the ``disk-queue`` optional dependency::

            pip install "task-pipeliner[disk-queue]"

    Parameters
    ----------
    path:
        Directory path for the queue's backing files.  Created automatically
        if it does not exist.
    """

    def __init__(self, path: str | Path) -> None:
        try:
            import persistqueue
        except ImportError as exc:
            raise ImportError(
                "FullDiskQueue requires the 'persist-queue' package. "
                "Install it with: pip install \"task-pipeliner[disk-queue]\""
            ) from exc
        logger.debug("path=%s", path)
        self._q = persistqueue.Queue(str(path), autosave=True)
        logger.info("FullDiskQueue opened path=%s", path)

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        """Write *item* to disk (block and timeout are unused — disk writes are synchronous)."""
        self._q.put(item)

    def put_nowait(self, item: Any) -> None:
        """Non-blocking put (alias for ``put()``)."""
        self._q.put_nowait(item)

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        """Block until an item is available and return it."""
        if timeout is not None:
            return self._q.get(block=True, timeout=timeout)
        return self._q.get(block=True)

    def empty(self) -> bool:
        """Return ``True`` if the queue contains no items."""
        return self._q.empty()

    def cancel_join_thread(self) -> None:
        """No-op — FullDiskQueue has no background thread to cancel."""

    def task_done(self) -> None:
        """Signal that the previously retrieved item has been processed."""
        self._q.task_done()

    def close(self) -> None:
        """Release persistqueue resources."""
        if hasattr(self._q, "close"):
            self._q.close()
