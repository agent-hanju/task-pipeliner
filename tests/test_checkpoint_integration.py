"""Checkpoint 재시작 통합 테스트.

시나리오:
1. 첫 번째 실행 — N개 아이템 전체 처리 → checkpoint에 N개 기록
2. 같은 run_id로 두 번째 실행 → 모두 스킵 (processed=0)
3. 절반만 미리 mark_done 후 실행 → 나머지 절반만 처리
"""

from __future__ import annotations

from pathlib import Path

import pytest

from task_pipeliner.checkpoint import DiskCacheCheckpointStore
from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig
from task_pipeliner.pipeline import Pipeline
from task_pipeliner.stats import StatsCollector

from .dummy_steps import KeyedSourceStep, SequentialPassthroughStep


def _run_with_checkpoint(
    items: list[int],
    checkpoint_dir: Path,
    run_id: str,
    tmp_path: Path,
    out_suffix: str = "out",
) -> StatsCollector:
    config = PipelineConfig(
        pipeline=[
            StepConfig(type="source", items=items, outputs={"main": "passthrough"}),
            StepConfig(type="passthrough"),
        ],
        execution=ExecutionConfig(workers=1, queue_size=100),
        checkpoint_dir=checkpoint_dir,
        resume_run_id=run_id,
    )
    p = Pipeline()
    p.register("source", KeyedSourceStep)
    p.register("passthrough", SequentialPassthroughStep)
    return p.run(config=config, output_dir=tmp_path / out_suffix)


@pytest.mark.timeout(30)
def test_second_run_skips_all_items(tmp_path: Path) -> None:
    """첫 번째 실행 완료 후 같은 run_id로 재실행 시 모두 스킵된다."""
    items = list(range(10))
    checkpoint_dir = tmp_path / "checkpoints"
    run_id = "test-run-1"

    stats1 = _run_with_checkpoint(items, checkpoint_dir, run_id, tmp_path, "out1")
    assert stats1.get_step_stats("source").processed == 10

    stats2 = _run_with_checkpoint(items, checkpoint_dir, run_id, tmp_path, "out2")
    assert stats2.get_step_stats("source").processed == 0


@pytest.mark.timeout(30)
def test_partial_checkpoint_resumes_remaining(tmp_path: Path) -> None:
    """이미 처리된 절반만 mark_done 후 실행 시 나머지 절반만 처리된다."""
    items = list(range(10))
    checkpoint_dir = tmp_path / "checkpoints"
    run_id = "test-run-2"

    # 처음 5개를 미리 mark_done
    pre_store = DiskCacheCheckpointStore(checkpoint_dir, run_id)
    for i in items[:5]:
        pre_store.mark_done(str(i))
    pre_store.close()

    stats = _run_with_checkpoint(items, checkpoint_dir, run_id, tmp_path)
    assert stats.get_step_stats("source").processed == 5


@pytest.mark.timeout(30)
def test_different_run_id_processes_all(tmp_path: Path) -> None:
    """다른 run_id는 이전 실행과 무관하게 전체를 처리한다."""
    items = list(range(5))
    checkpoint_dir = tmp_path / "checkpoints"

    _run_with_checkpoint(items, checkpoint_dir, "run-a", tmp_path, "out1")

    stats2 = _run_with_checkpoint(items, checkpoint_dir, "run-b", tmp_path, "out2")
    assert stats2.get_step_stats("source").processed == 5
