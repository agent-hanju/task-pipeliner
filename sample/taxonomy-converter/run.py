"""Entry point for the Naver news taxonomy converter pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

from steps import ConvertStep, DeduplicateStep, LoaderStep, PreprocessStep, WriterStep

from task_pipeliner import Pipeline

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG = Path(__file__).parent / "pipeline_config.yaml"


def main(
    input_dir: Path,
    output_dir: Path,
    config_path: Path | None = None,
) -> None:
    """Run the Naver news taxonomy conversion pipeline."""
    logger.debug("input_dir=%s output_dir=%s config_path=%s", input_dir, output_dir, config_path)

    pipeline = Pipeline()
    pipeline.register_all({
        "loader": LoaderStep,
        "preprocess": PreprocessStep,
        "convert": ConvertStep,
        "deduplicate": DeduplicateStep,
        "writer": WriterStep,
    })
    pipeline.run(
        config=config_path or _DEFAULT_CONFIG,
        output_dir=output_dir,
        variables={"input_dir": str(input_dir), "output_dir": str(output_dir)},
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <input_dir> <output_dir>")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO)
    main(Path(sys.argv[1]), Path(sys.argv[2]))
