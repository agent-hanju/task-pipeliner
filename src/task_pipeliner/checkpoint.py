"""Checkpoint store: track processed items across pipeline runs.

When a long-running pipeline is interrupted, restarting from scratch wastes
work.  A ``CheckpointStore`` records which items have already been fully
processed, so a subsequent run can skip them.

Usage
-----
Enable checkpointing by setting ``checkpoint_dir`` in the pipeline config:

.. code-block:: yaml

    checkpoint_dir: /tmp/my_pipeline_checkpoints
    resume_run_id: "2026-04-10-run1"  # omit to start a new run

When ``checkpoint_dir`` is not set, a :class:`NullCheckpointStore` is used
automatically and adds zero overhead.

.. note::
    Requires the ``checkpoint`` optional dependency::

        pip install "task-pipeliner[checkpoint]"
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class CheckpointStore(Protocol):
    """Protocol for checkpoint stores.

    Implementations must be thread-safe — ``is_done`` and ``mark_done`` may be
    called concurrently from multiple threads.
    """

    def is_done(self, item_key: str) -> bool:
        """Return ``True`` if *item_key* has already been processed."""
        ...

    def mark_done(self, item_key: str) -> None:
        """Record *item_key* as fully processed."""
        ...

    def close(self) -> None:
        """Release resources (file handles, connections, etc.)."""
        ...


class NullCheckpointStore:
    """No-op checkpoint store used when ``checkpoint_dir`` is not configured.

    All operations are no-ops and add zero overhead.
    """

    def is_done(self, item_key: str) -> bool:  # noqa: ARG002
        return False

    def mark_done(self, item_key: str) -> None:  # noqa: ARG002
        pass

    def close(self) -> None:
        pass


class DiskCacheCheckpointStore:
    """Checkpoint store backed by ``diskcache`` (SQLite).

    Parameters
    ----------
    checkpoint_dir:
        Directory where checkpoint data is stored. One subdirectory per
        *run_id* is created automatically.
    run_id:
        Unique identifier for the current (or resumed) run.

    .. note::
        Requires the ``checkpoint`` optional dependency::

            pip install "task-pipeliner[checkpoint]"
    """

    def __init__(self, checkpoint_dir: Path, run_id: str) -> None:
        try:
            import diskcache
        except ImportError as exc:
            raise ImportError(
                "DiskCacheCheckpointStore requires the 'diskcache' package. "
                "Install it with: pip install \"task-pipeliner[checkpoint]\""
            ) from exc
        store_path = checkpoint_dir / run_id
        store_path.mkdir(parents=True, exist_ok=True)
        self._cache = diskcache.Cache(str(store_path))
        self._run_id = run_id
        logger.info(
            "DiskCacheCheckpointStore opened run_id=%s path=%s", run_id, store_path
        )

    def is_done(self, item_key: str) -> bool:
        return item_key in self._cache

    def mark_done(self, item_key: str) -> None:
        self._cache[item_key] = True

    def close(self) -> None:
        self._cache.close()
        logger.info("DiskCacheCheckpointStore closed run_id=%s", self._run_id)


def make_checkpoint_store(
    checkpoint_dir: Path | None,
    run_id: str,
) -> CheckpointStore:
    """Factory: return a :class:`DiskCacheCheckpointStore` when *checkpoint_dir*
    is set, otherwise a :class:`NullCheckpointStore`.
    """
    if checkpoint_dir is None:
        return NullCheckpointStore()
    return DiskCacheCheckpointStore(checkpoint_dir, run_id)
