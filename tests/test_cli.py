"""Tests: pipeline.py — W-15."""

from __future__ import annotations

from pathlib import Path

import orjson
import pytest
import yaml
from dummy_steps import DummyJsonlSourceStep, FilterEvenStep, PassthroughStep

from task_pipeliner.exceptions import StepRegistrationError
from task_pipeliner.pipeline import Pipeline

# ---------------------------------------------------------------------------
# Pipeline facade
# ---------------------------------------------------------------------------


class TestPipeline:
    def test_register_chaining(self) -> None:
        """register() returns self for chaining."""
        p = Pipeline()
        result = p.register("passthrough", PassthroughStep)
        assert result is p

    def test_register_all(self) -> None:
        """register_all() registers multiple steps."""
        p = Pipeline()
        p.register_all({"passthrough": PassthroughStep, "filter_even": FilterEvenStep})
        # Verify via internal registry
        assert p._registry.get("passthrough") is PassthroughStep
        assert p._registry.get("filter_even") is FilterEvenStep

    @pytest.mark.timeout(30)
    def test_run_with_config_path(self, tmp_path: Path) -> None:
        """run() with YAML path loads config and executes pipeline."""
        input_file = tmp_path / "input.jsonl"
        input_file.write_bytes(b"\n".join(orjson.dumps({"id": i}) for i in range(5)) + b"\n")
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            yaml.dump(
                {
                    "pipeline": [
                        {
                            "type": "source",
                            "items": [str(input_file)],
                            "outputs": {"main": "passthrough"},
                        },
                        {"type": "passthrough"},
                    ],
                    "execution": {"workers": 1, "queue_size": 50, "chunk_size": 20},
                }
            ),
            encoding="utf-8",
        )
        output_dir = tmp_path / "output"

        p = Pipeline()
        p.register("source", DummyJsonlSourceStep)
        p.register("passthrough", PassthroughStep)
        p.run(config=config_path, output_dir=output_dir)

        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        pt = next(d for d in data if d["step_name"] == "passthrough")
        assert pt["processed"] == 5

    @pytest.mark.timeout(30)
    def test_run_with_config_object(self, tmp_path: Path) -> None:
        """run() with PipelineConfig object directly."""
        from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig

        input_file = tmp_path / "input.jsonl"
        input_file.write_bytes(b"\n".join(orjson.dumps(i) for i in range(10)) + b"\n")

        config = PipelineConfig(
            pipeline=[
                StepConfig(type="source", items=[str(input_file)], outputs={"main": "filter_even"}),  # type: ignore[call-arg]
                StepConfig(type="filter_even"),
            ],
            execution=ExecutionConfig(workers=1, queue_size=50, chunk_size=20),
        )
        output_dir = tmp_path / "output"

        p = Pipeline()
        p.register("source", DummyJsonlSourceStep)
        p.register("filter_even", FilterEvenStep)
        p.run(config=config, output_dir=output_dir)

        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        fe = next(d for d in data if d["step_name"] == "filter_even")
        assert fe["processed"] == 10

    def test_run_unregistered_step_raises(self, tmp_path: Path) -> None:
        """run() with unregistered step → StepRegistrationError."""
        from task_pipeliner.config import PipelineConfig, StepConfig

        config = PipelineConfig(pipeline=[StepConfig(type="unknown")])
        output_dir = tmp_path / "output"

        p = Pipeline()
        with pytest.raises(StepRegistrationError):
            p.run(config=config, output_dir=output_dir)
