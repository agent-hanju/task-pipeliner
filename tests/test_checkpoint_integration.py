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
from task_pipeliner.engine import PipelineEngine
from task_pipeliner.pipeline import StepRegistry
from task_pipeliner.stats import StatsCollector

from .dummy_steps import KeyedSourceStep, SequentialPassthroughStep


def _make_engine(
    items: list[int],
    checkpoint_dir: Path,
    run_id: str,
    *,
    tmp_path: Path,
) -> tuple[PipelineEngine, StatsCollector]:
    config = PipelineConfig(
        pipeline=[
            StepConfig(type="source", items=items, outputs={"main": "passthrough"}),
            StepConfig(type="passthrough"),
        ],
        execution=ExecutionConfig(workers=1, queue_size=100),
        checkpoint_dir=checkpoint_dir,
        resume_run_id=run_id,
    )
    registry = StepRegistry()
    registry.register("source", KeyedSourceStep)
    registry.register("passthrough", SequentialPassthroughStep)
    stats = StatsCollector()
    engine = PipelineEngine(config=config, registry=registry, stats=stats)
    return engine, stats


@pytest.mark.timeout(30)
def test_second_run_skips_all_items(tmp_path: Path) -> None:
    """첫 번째 실행 완료 후 같은 run_id로 재실행 시 모두 스킵된다."""
    items = list(range(10))
    checkpoint_dir = tmp_path / "checkpoints"
    run_id = "test-run-1"

    # 첫 번째 실행
    engine1, stats1 = _make_engine(items, checkpoint_dir, run_id, tmp_path=tmp_path)
    engine1.run(output_dir=tmp_path / "out1")
    source_stats1 = stats1.get_step_stats("source")
    assert source_stats1 is not None
    assert source_stats1.processed == 10

    # 두 번째 실행 — 모두 스킵되어야 함
    engine2, stats2 = _make_engine(items, checkpoint_dir, run_id, tmp_path=tmp_path)
    engine2.run(output_dir=tmp_path / "out2")
    source_stats2 = stats2.get_step_stats("source")
    assert source_stats2 is not None
    assert source_stats2.processed == 0


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

    engine, stats = _make_engine(items, checkpoint_dir, run_id, tmp_path=tmp_path)
    engine.run(output_dir=tmp_path / "out")
    source_stats = stats.get_step_stats("source")
    assert source_stats is not None
    assert source_stats.processed == 5  # 나머지 5개만 처리


@pytest.mark.timeout(30)
def test_different_run_id_processes_all(tmp_path: Path) -> None:
    """다른 run_id는 이전 실행과 무관하게 전체를 처리한다."""
    items = list(range(5))
    checkpoint_dir = tmp_path / "checkpoints"

    # run-a 전체 완료
    engine1, _ = _make_engine(items, checkpoint_dir, "run-a", tmp_path=tmp_path)
    engine1.run(output_dir=tmp_path / "out1")

    # run-b는 독립된 네임스페이스 → 전부 처리
    engine2, stats2 = _make_engine(items, checkpoint_dir, "run-b", tmp_path=tmp_path)
    engine2.run(output_dir=tmp_path / "out2")
    source_stats2 = stats2.get_step_stats("source")
    assert source_stats2 is not None
    assert source_stats2.processed == 5
