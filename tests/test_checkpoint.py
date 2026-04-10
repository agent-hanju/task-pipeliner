"""CheckpointStore 테스트."""

from __future__ import annotations

import builtins
from pathlib import Path

import pytest

from task_pipeliner.checkpoint import (
    CheckpointStore,
    DiskCacheCheckpointStore,
    NullCheckpointStore,
    make_checkpoint_store,
)

# ---------------------------------------------------------------------------
# NullCheckpointStore
# ---------------------------------------------------------------------------


def test_null_store_is_done_always_false() -> None:
    store = NullCheckpointStore()
    assert store.is_done("any_key") is False


def test_null_store_mark_done_no_error() -> None:
    store = NullCheckpointStore()
    store.mark_done("any_key")  # should not raise


def test_null_store_close_no_error() -> None:
    store = NullCheckpointStore()
    store.close()  # should not raise


def test_null_store_implements_protocol() -> None:
    assert isinstance(NullCheckpointStore(), CheckpointStore)


# ---------------------------------------------------------------------------
# DiskCacheCheckpointStore
# ---------------------------------------------------------------------------


def test_diskcache_store_mark_and_is_done(tmp_path: Path) -> None:
    store = DiskCacheCheckpointStore(tmp_path, "run1")
    try:
        assert store.is_done("item-1") is False
        store.mark_done("item-1")
        assert store.is_done("item-1") is True
    finally:
        store.close()


def test_diskcache_store_different_keys_independent(tmp_path: Path) -> None:
    store = DiskCacheCheckpointStore(tmp_path, "run1")
    try:
        store.mark_done("a")
        assert store.is_done("a") is True
        assert store.is_done("b") is False
    finally:
        store.close()


def test_diskcache_store_persists_across_instances(tmp_path: Path) -> None:
    """재생성 후에도 이전에 mark_done된 키가 is_done() == True여야 한다."""
    store1 = DiskCacheCheckpointStore(tmp_path, "run1")
    store1.mark_done("item-x")
    store1.close()

    store2 = DiskCacheCheckpointStore(tmp_path, "run1")
    try:
        assert store2.is_done("item-x") is True
    finally:
        store2.close()


def test_diskcache_store_run_id_isolation(tmp_path: Path) -> None:
    """다른 run_id는 독립된 네임스페이스를 가진다."""
    store_a = DiskCacheCheckpointStore(tmp_path, "run-a")
    store_b = DiskCacheCheckpointStore(tmp_path, "run-b")
    try:
        store_a.mark_done("shared-key")
        assert store_a.is_done("shared-key") is True
        assert store_b.is_done("shared-key") is False
    finally:
        store_a.close()
        store_b.close()


def test_diskcache_store_implements_protocol(tmp_path: Path) -> None:
    store = DiskCacheCheckpointStore(tmp_path, "run1")
    try:
        assert isinstance(store, CheckpointStore)
    finally:
        store.close()


def test_diskcache_import_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """diskcache 미설치 시 명확한 ImportError를 반환해야 한다."""
    real_import = builtins.__import__

    def mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "diskcache":
            raise ImportError("No module named 'diskcache'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)

    # DiskCacheCheckpointStore.__init__에서 import diskcache가 실패하는지 확인
    # monkeypatch가 builtins.__import__를 대체한 상태에서 직접 호출
    with pytest.raises(ImportError, match="diskcache"):
        DiskCacheCheckpointStore(tmp_path, "run1")


# ---------------------------------------------------------------------------
# make_checkpoint_store factory
# ---------------------------------------------------------------------------


def test_factory_returns_null_when_no_dir() -> None:
    store = make_checkpoint_store(None, "run1")
    assert isinstance(store, NullCheckpointStore)


def test_factory_returns_diskcache_when_dir_given(tmp_path: Path) -> None:
    store = make_checkpoint_store(tmp_path, "run1")
    assert isinstance(store, DiskCacheCheckpointStore)
    store.close()
