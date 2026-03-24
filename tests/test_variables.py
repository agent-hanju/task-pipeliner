"""Tests for variable substitution in config loading."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import orjson
import pytest
import yaml
from dummy_steps import DummyJsonlSourceStep, PassthroughStep

from task_pipeliner.config import load_config
from task_pipeliner.exceptions import ConfigValidationError
from task_pipeliner.pipeline import Pipeline

# ---------------------------------------------------------------------------
# load_config with variables
# ---------------------------------------------------------------------------


class TestLoadConfigVariables:
    """Variable substitution via load_config(path, variables=...)."""

    def test_simple_substitution(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: loader\n"
            "    path: ${input_dir}/data.jsonl\n"
        )
        cfg = load_config(yaml_file, variables={"input_dir": "/mnt/data"})
        assert cfg.pipeline[0].model_extra["path"] == "/mnt/data/data.jsonl"

    def test_multiple_variables(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: loader\n"
            "    src: ${src_dir}\n"
            "    outputs:\n"
            "      main: writer\n"
            "  - type: writer\n"
            "    dst: ${dst_dir}\n"
        )
        cfg = load_config(
            yaml_file, variables={"src_dir": "/in", "dst_dir": "/out"}
        )
        assert cfg.pipeline[0].model_extra["src"] == "/in"
        assert cfg.pipeline[1].model_extra["dst"] == "/out"

    def test_unresolved_variable_raises(self, tmp_path: Path) -> None:
        """Unresolved ${var} with variables dict raises ConfigValidationError."""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: loader\n"
            "    path: ${undefined_var}/data\n"
        )
        # When variables are provided but missing the referenced key, substitute raises
        with pytest.raises(ConfigValidationError):
            load_config(yaml_file, variables={"other_var": "x"})

    def test_list_variable(self, tmp_path: Path) -> None:
        """${var} as entire value → replaced with list."""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: loader\n"
            "    paths: ${input_paths}\n"
        )
        cfg = load_config(
            yaml_file,
            variables={"input_paths": ["/a.jsonl", "/b.jsonl"]},
        )
        assert cfg.pipeline[0].model_extra["paths"] == ["/a.jsonl", "/b.jsonl"]

    def test_int_variable(self, tmp_path: Path) -> None:
        """${var} as entire value → replaced with int."""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: filter\n"
            "    threshold: ${threshold}\n"
        )
        cfg = load_config(yaml_file, variables={"threshold": 42})
        assert cfg.pipeline[0].model_extra["threshold"] == 42

    def test_partial_match_stays_string(self, tmp_path: Path) -> None:
        """${var} inside a larger string → string substitution."""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: loader\n"
            "    path: ${base}/data.jsonl\n"
        )
        cfg = load_config(yaml_file, variables={"base": "/mnt"})
        assert cfg.pipeline[0].model_extra["path"] == "/mnt/data.jsonl"

    def test_no_variables_passthrough(self, tmp_path: Path) -> None:
        """Without variables, existing configs work unchanged."""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(
            "pipeline:\n"
            "  - type: filter\n"
            "    threshold: 0.5\n"
            "execution:\n"
            "  workers: 2\n"
        )
        cfg = load_config(yaml_file)
        assert cfg.pipeline[0].model_extra["threshold"] == 0.5
        assert cfg.execution.workers == 2


# ---------------------------------------------------------------------------
# Pipeline.run() with variables
# ---------------------------------------------------------------------------


class TestPipelineRunVariables:
    @pytest.mark.timeout(30)
    def test_run_with_variables(self, tmp_path: Path) -> None:
        """Pipeline.run() passes variables to load_config."""
        input_dir = tmp_path / "inputs"
        input_dir.mkdir()
        input_file = input_dir / "data.jsonl"
        input_file.write_bytes(
            b"\n".join(orjson.dumps({"id": i}) for i in range(3)) + b"\n"
        )

        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            yaml.dump(
                {
                    "pipeline": [
                        {
                            "type": "source",
                            "items": ["${input_path}"],
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
        p.run(
            config=config_path,
            output_dir=output_dir,
            variables={"input_path": str(input_file)},
        )

        stats_file = output_dir / "stats.json"
        assert stats_file.exists()
        data: Any = orjson.loads(stats_file.read_bytes())
        pt = next(d for d in data if d["step_name"] == "passthrough")
        assert pt["processed"] == 3
