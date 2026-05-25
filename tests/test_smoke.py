"""Smoke test — 다단계 파이프라인의 end-to-end 실행 검증.

실제 업무 로직 없이 dummy steps만 사용해 아래를 확인한다:
- SourceStep → 다수의 SequentialStep/ParallelStep → 출력까지 전체 흐름 동작
- stats.json 파일 생성
- SpillQueue / FullDiskQueue 각각의 경로에서 데이터 손실 없음
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from task_pipeliner.config import ExecutionConfig, PipelineConfig, QueueType, StepConfig
from task_pipeliner.pipeline import Pipeline

from .dummy_steps import (
    DummySourceStep,
    FilterEvenStep,
    PassthroughStep,
    SequentialPassthroughStep,
)

# ---------------------------------------------------------------------------
# SpillQueue 경로
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_multi_step_spill_queue(tmp_path: Path) -> None:
    """SpillQueue 모드에서 source → filter → passthrough → terminal 전체 흐름."""
    items = list(range(20))
    config = PipelineConfig(
        pipeline=[
            StepConfig(type="source", items=items, outputs={"main": "filter"}),
            StepConfig(type="filter", outputs={"main": "passthrough"}),
            StepConfig(type="passthrough"),
        ],
        execution=ExecutionConfig(workers=2, queue_size=50),
    )
    p = Pipeline()
    p.register("source", DummySourceStep)
    p.register("filter", FilterEvenStep)
    p.register("passthrough", PassthroughStep)
    stats = p.run(config=config, output_dir=tmp_path / "out")

    assert stats.get_step_stats("passthrough").processed == 10


@pytest.mark.timeout(60)
def test_stats_json_written(tmp_path: Path) -> None:
    """파이프라인 완료 후 stats.json 파일이 생성된다."""
    out = tmp_path / "out"
    config = PipelineConfig(
        pipeline=[
            StepConfig(type="source", items=list(range(5)), outputs={"main": "passthrough"}),
            StepConfig(type="passthrough"),
        ],
        execution=ExecutionConfig(workers=1),
    )
    p = Pipeline()
    p.register("source", DummySourceStep)
    p.register("passthrough", SequentialPassthroughStep)
    p.run(config=config, output_dir=out)

    stats_file = out / "stats.json"
    assert stats_file.exists(), "stats.json 파일이 생성되어야 한다"
    data = json.loads(stats_file.read_text(encoding="utf-8"))
    step_names = {s["step_name"] for s in data}
    assert "source" in step_names
    assert "passthrough" in step_names


# ---------------------------------------------------------------------------
# FullDiskQueue 경로
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_full_disk_queue_pipeline(tmp_path: Path) -> None:
    """FullDiskQueue(FULL_DISK) 모드에서 데이터 손실 없이 완료된다."""
    items = list(range(30))
    config = PipelineConfig(
        pipeline=[
            StepConfig(type="source", items=items, outputs={"main": "passthrough"}),
            StepConfig(type="passthrough"),
        ],
        execution=ExecutionConfig(queue_type=QueueType.FULL_DISK, workers=1),
    )
    p = Pipeline()
    p.register("source", DummySourceStep)
    p.register("passthrough", SequentialPassthroughStep)
    stats = p.run(config=config, output_dir=tmp_path / "out")

    assert stats.get_step_stats("passthrough").processed == 30


