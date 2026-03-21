"""Tests for pipeline config — W-13."""

from __future__ import annotations

from pathlib import Path

from task_pipeliner.config import load_config

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "pipeline_config.yaml"


def test_config_loads_successfully() -> None:
    cfg = load_config(_CONFIG_PATH)
    assert len(cfg.pipeline) == 7


def test_config_step_types() -> None:
    cfg = load_config(_CONFIG_PATH)
    types = [s.type for s in cfg.pipeline]
    assert types == [
        "loader",
        "quality_filter",
        "hash_compute",
        "hash_lookup",
        "minhash_compute",
        "minhash_lookup",
        "writer",
    ]


def test_config_outputs_reference_integrity() -> None:
    """All output references should point to existing step types."""
    cfg = load_config(_CONFIG_PATH)
    step_types = {s.type for s in cfg.pipeline}
    for step_cfg in cfg.pipeline:
        if step_cfg.outputs:
            for target in step_cfg.outputs.values():
                assert target in step_types, (
                    f"Step '{step_cfg.type}' references unknown target '{target}'"
                )


def test_config_execution() -> None:
    cfg = load_config(_CONFIG_PATH)
    assert cfg.execution.workers == 4
    assert cfg.execution.queue_size == 200
