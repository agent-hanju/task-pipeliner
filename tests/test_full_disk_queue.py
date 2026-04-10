"""FullDiskQueue 테스트.

persistqueue.Queue 래퍼의 기본 동작, thread-safety, 엣지케이스,
Windows 경로 안전성을 검증한다.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from task_pipeliner.spill_queue import FullDiskQueue

# ---------------------------------------------------------------------------
# 기본 동작
# ---------------------------------------------------------------------------


def test_put_get_fifo(tmp_path: Path) -> None:
    """put() → get() FIFO 순서 보장."""
    q = FullDiskQueue(tmp_path / "q")
    q.put("a")
    q.put("b")
    q.put("c")
    assert q.get() == "a"
    assert q.get() == "b"
    assert q.get() == "c"


def test_empty_true_when_no_items(tmp_path: Path) -> None:
    q = FullDiskQueue(tmp_path / "q")
    assert q.empty() is True


def test_empty_false_after_put(tmp_path: Path) -> None:
    q = FullDiskQueue(tmp_path / "q")
    q.put(1)
    assert q.empty() is False


def test_put_nowait(tmp_path: Path) -> None:
    q = FullDiskQueue(tmp_path / "q")
    q.put_nowait(42)
    assert q.get() == 42


def test_task_done_no_error(tmp_path: Path) -> None:
    """task_done() 호출이 예외 없이 완료되어야 한다."""
    q = FullDiskQueue(tmp_path / "q")
    q.put("x")
    item = q.get()
    q.task_done()  # should not raise
    assert item == "x"


def test_cancel_join_thread_no_error(tmp_path: Path) -> None:
    q = FullDiskQueue(tmp_path / "q")
    q.cancel_join_thread()  # should not raise


# ---------------------------------------------------------------------------
# 다양한 데이터 타입
# ---------------------------------------------------------------------------


def test_various_types(tmp_path: Path) -> None:
    """문자열, 정수, dict, list, None 직렬화/역직렬화."""
    q = FullDiskQueue(tmp_path / "q")
    items = ["hello", 42, {"key": "value"}, [1, 2, 3], None]
    for item in items:
        q.put(item)
    for expected in items:
        assert q.get() == expected


def test_large_item(tmp_path: Path) -> None:
    """1MB 이상 대형 아이템 처리."""
    q = FullDiskQueue(tmp_path / "q")
    large = b"x" * (1024 * 1024)  # 1MB
    q.put(large)
    assert q.get() == large


# ---------------------------------------------------------------------------
# Thread-safety
# ---------------------------------------------------------------------------


def test_concurrent_put_get(tmp_path: Path) -> None:
    """1 writer + 1 reader 스레드로 200건 순서/손실 없이 처리.

    파일 기반 큐라 항목당 I/O 오버헤드가 있으므로 소규모로 검증.
    """
    q = FullDiskQueue(tmp_path / "q")
    count = 200
    results: list[int] = []
    errors: list[Exception] = []

    def writer() -> None:
        try:
            for i in range(count):
                q.put(i)
            q.put(None)  # sentinel
        except Exception as e:
            errors.append(e)

    def reader() -> None:
        try:
            while True:
                item = q.get()
                if item is None:
                    break
                results.append(item)
        except Exception as e:
            errors.append(e)

    t_write = threading.Thread(target=writer)
    t_read = threading.Thread(target=reader)
    t_write.start()
    t_read.start()
    t_write.join()
    t_read.join()

    assert not errors, f"스레드 오류: {errors}"
    assert len(results) == count
    assert results == list(range(count))


def test_multi_writer_single_reader(tmp_path: Path) -> None:
    """여러 writer 스레드가 동시에 put()해도 손실 없음."""
    q = FullDiskQueue(tmp_path / "q")
    n_writers = 4
    per_writer = 50
    total = n_writers * per_writer
    received: list[int] = []
    errors: list[Exception] = []

    def writer(start: int) -> None:
        try:
            for i in range(start, start + per_writer):
                q.put(i)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(i * per_writer,)) for i in range(n_writers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # sentinel
    for _ in range(n_writers):
        q.put(None)

    sentinel_count = 0
    while sentinel_count < n_writers:
        item = q.get()
        if item is None:
            sentinel_count += 1
        else:
            received.append(item)

    assert not errors
    assert len(received) == total
    assert sorted(received) == list(range(total))


# ---------------------------------------------------------------------------
# blocking get()
# ---------------------------------------------------------------------------


def test_blocking_get_waits_for_item(tmp_path: Path) -> None:
    """get()은 아이템이 올 때까지 블로킹해야 한다."""
    q = FullDiskQueue(tmp_path / "q")
    result: list[str] = []

    def reader() -> None:
        result.append(q.get())

    t = threading.Thread(target=reader)
    t.start()
    time.sleep(0.1)
    assert result == [], "아직 아이템이 없으므로 블로킹 중이어야 함"
    q.put("arrived")
    t.join(timeout=5)
    assert result == ["arrived"]


# ---------------------------------------------------------------------------
# Windows 경로 안전성
# ---------------------------------------------------------------------------


def test_path_with_spaces(tmp_path: Path) -> None:
    """경로에 공백이 포함되어도 정상 동작해야 한다."""
    path = tmp_path / "my queue dir"
    q = FullDiskQueue(path)
    q.put("space test")
    assert q.get() == "space test"


def test_path_with_korean(tmp_path: Path) -> None:
    """경로에 한글이 포함되어도 정상 동작해야 한다."""
    path = tmp_path / "큐디렉토리"
    q = FullDiskQueue(path)
    q.put("한글 데이터")
    assert q.get() == "한글 데이터"


# ---------------------------------------------------------------------------
# ImportError 안내
# ---------------------------------------------------------------------------


def test_import_error_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """persistqueue 미설치 시 명확한 ImportError 메시지를 반환해야 한다."""
    import builtins
    real_import = builtins.__import__

    def mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "persistqueue":
            raise ImportError("No module named 'persistqueue'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)

    # FullDiskQueue 클래스를 직접 재생성해서 ImportError가 __init__에서 발생하는지 확인
    import importlib

    import task_pipeliner.spill_queue as sq_module
    importlib.reload(sq_module)

    with pytest.raises(ImportError, match="disk-queue"):
        sq_module.FullDiskQueue(tmp_path / "q")
