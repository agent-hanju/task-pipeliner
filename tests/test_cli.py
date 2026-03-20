"""Tests: pipeline.py + cli.py — W-15."""

from __future__ import annotations

from pathlib import Path

import orjson
import pytest
import yaml
from click.testing import CliRunner
from dummy_steps import FilterEvenStep, PassthroughStep

from task_pipeliner.cli import main
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
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            yaml.dump(
                {
                    "pipeline": [{"type": "passthrough"}],
                    "execution": {"workers": 1, "queue_size": 50, "chunk_size": 20},
                }
            ),
            encoding="utf-8",
        )
        input_file = tmp_path / "input.jsonl"
        input_file.write_bytes(b"\n".join(orjson.dumps({"id": i}) for i in range(5)) + b"\n")
        output_dir = tmp_path / "output"

        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        p.run(config=config_path, inputs=[input_file], output_dir=output_dir)

        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        assert data[0]["passed"] == 5

    @pytest.mark.timeout(30)
    def test_run_with_config_object(self, tmp_path: Path) -> None:
        """run() with PipelineConfig object directly."""
        from task_pipeliner.config import ExecutionConfig, PipelineConfig, StepConfig

        config = PipelineConfig(
            pipeline=[StepConfig(type="filter_even")],
            execution=ExecutionConfig(workers=1, queue_size=50, chunk_size=20),
        )
        input_file = tmp_path / "input.jsonl"
        input_file.write_bytes(b"\n".join(orjson.dumps(i) for i in range(10)) + b"\n")
        output_dir = tmp_path / "output"

        p = Pipeline()
        p.register("filter_even", FilterEvenStep)
        p.run(config=config, inputs=[input_file], output_dir=output_dir)

        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data = orjson.loads(stats_file.read_bytes())
        assert data[0]["passed"] == 5

    def test_run_unregistered_step_raises(self, tmp_path: Path) -> None:
        """run() with unregistered step → StepRegistrationError."""
        from task_pipeliner.config import PipelineConfig, StepConfig

        config = PipelineConfig(pipeline=[StepConfig(type="unknown")])
        input_file = tmp_path / "input.jsonl"
        input_file.write_bytes(b'{"id": 1}\n')
        output_dir = tmp_path / "output"

        p = Pipeline()
        with pytest.raises(StepRegistrationError):
            p.run(config=config, inputs=[input_file], output_dir=output_dir)


# ---------------------------------------------------------------------------
# CLI (CliRunner)
# ---------------------------------------------------------------------------


class TestFilterCommand:
    @staticmethod
    def _pipeline_obj() -> dict[str, Pipeline]:
        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        p.register("filter_even", FilterEvenStep)
        return {"pipeline": p}

    @pytest.mark.timeout(30)
    def test_filter_end_to_end(self, tmp_path: Path) -> None:
        """filter command → exit_code 0, kept.jsonl exists."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            yaml.dump(
                {
                    "pipeline": [{"type": "passthrough"}],
                    "execution": {"workers": 1, "queue_size": 50, "chunk_size": 20},
                }
            ),
            encoding="utf-8",
        )
        input_file = tmp_path / "input.jsonl"
        input_file.write_bytes(b"\n".join(orjson.dumps({"id": i}) for i in range(5)) + b"\n")
        output_dir = tmp_path / "output"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "filter",
                "--config",
                str(config_path),
                "--input",
                str(input_file),
                "--output",
                str(output_dir),
            ],
            obj=self._pipeline_obj(),
        )
        assert result.exit_code == 0, result.output
        assert (output_dir / "stats.json").exists()

    @pytest.mark.parametrize(
        "args,match",
        [
            (["filter", "--config", "nonexistent.yaml"], "does not exist"),
            (["filter", "--output", "/tmp/out"], "Missing option"),
        ],
        ids=["bad_yaml", "missing_config"],
    )
    def test_filter_bad_args(self, args: list[str], match: str) -> None:
        """Invalid args → non-zero exit code + error message."""
        runner = CliRunner()
        result = runner.invoke(main, args)
        assert result.exit_code != 0
        assert match.lower() in (result.output + (result.stderr or "")).lower()

    @pytest.mark.timeout(30)
    def test_filter_unregistered_step(self, tmp_path: Path) -> None:
        """Config references unregistered step → non-zero exit + message."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            yaml.dump({"pipeline": [{"type": "nonexistent_step"}]}),
            encoding="utf-8",
        )
        input_file = tmp_path / "input.jsonl"
        input_file.write_bytes(b'{"id": 1}\n')
        output_dir = tmp_path / "output"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "filter",
                "--config",
                str(config_path),
                "--input",
                str(input_file),
                "--output",
                str(output_dir),
            ],
        )
        assert result.exit_code != 0


class TestBatchCommand:
    @staticmethod
    def _pipeline_obj() -> dict[str, Pipeline]:
        p = Pipeline()
        p.register("passthrough", PassthroughStep)
        return {"pipeline": p}

    @pytest.mark.timeout(30)
    def test_batch_sequential(self, tmp_path: Path) -> None:
        """batch command → runs jobs sequentially, each output dir exists."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            yaml.dump(
                {
                    "pipeline": [{"type": "passthrough"}],
                    "execution": {"workers": 1, "queue_size": 50, "chunk_size": 20},
                }
            ),
            encoding="utf-8",
        )

        # Create two input files
        for i in range(2):
            (tmp_path / f"input_{i}.jsonl").write_bytes(
                b"\n".join(orjson.dumps({"id": j}) for j in range(3)) + b"\n"
            )

        # Create jobs file
        jobs = [
            {
                "inputs": [str(tmp_path / f"input_{i}.jsonl")],
                "output_dir": str(tmp_path / f"out_{i}"),
            }
            for i in range(2)
        ]
        jobs_file = tmp_path / "jobs.json"
        jobs_file.write_bytes(orjson.dumps(jobs))

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "batch",
                "--config",
                str(config_path),
                str(jobs_file),
            ],
            obj=self._pipeline_obj(),
        )
        assert result.exit_code == 0, result.output
        for i in range(2):
            assert (tmp_path / f"out_{i}" / "stats.json").exists()
