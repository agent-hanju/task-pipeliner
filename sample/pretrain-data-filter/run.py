"""CLI entry point for pretrain-data-filter pipeline — W-15."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from steps import (
    HashComputeStep,
    HashLookupStep,
    LoaderStep,
    MinHashComputeStep,
    MinHashLookupStep,
    QualityFilterStep,
    WriterStep,
)

from task_pipeliner import Pipeline

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG = Path(__file__).parent / "pipeline_config.yaml"


def main(
    input_dir: Path,
    output_dir: Path,
    config_path: Path | None = None,
    no_minhash: bool = False,
) -> None:
    """Run the pretrain-data-filter pipeline."""
    logger.debug(
        "input_dir=%s output_dir=%s config_path=%s no_minhash=%s",
        input_dir,
        output_dir,
        config_path,
        no_minhash,
    )

    cfg_path = config_path or _DEFAULT_CONFIG

    # If --no-minhash, load config and disable MinHash steps programmatically
    if no_minhash:
        from task_pipeliner.config import StepConfig, load_config

        cfg = load_config(cfg_path, variables={"input_dir": str(input_dir), "output_dir": str(output_dir)})
        for i, step_cfg in enumerate(cfg.pipeline):
            if step_cfg.type in ("minhash_compute", "minhash_lookup"):
                cfg.pipeline[i] = StepConfig(  # type: ignore[call-arg,unused-ignore]
                    type=step_cfg.type,
                    enabled=False,
                    outputs=step_cfg.outputs,
                    **(step_cfg.model_extra or {}),
                )
                if step_cfg.type == "minhash_compute":
                    for j, s in enumerate(cfg.pipeline):
                        if s.type == "hash_lookup" and s.outputs:
                            new_outputs = dict(s.outputs)
                            new_outputs["kept"] = "writer"
                            cfg.pipeline[j] = StepConfig(  # type: ignore[call-arg,unused-ignore]
                                type=s.type,
                                outputs=new_outputs,
                                **(s.model_extra or {}),
                            )
        config_arg: Path | object = cfg
    else:
        config_arg = cfg_path

    pipeline = Pipeline()
    pipeline.register_all({
        "loader": LoaderStep,
        "quality_filter": QualityFilterStep,
        "hash_compute": HashComputeStep,
        "hash_lookup": HashLookupStep,
        "minhash_compute": MinHashComputeStep,
        "minhash_lookup": MinHashLookupStep,
        "writer": WriterStep,
    })
    pipeline.run(
        config=config_arg,  # type: ignore[arg-type]
        output_dir=output_dir,
        variables={"input_dir": str(input_dir), "output_dir": str(output_dir)},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pretrain data quality filter pipeline")
    parser.add_argument("--config", type=Path, default=None, help="Pipeline config YAML")
    parser.add_argument("--input", type=Path, required=True, help="Input directory")
    parser.add_argument("--output", type=Path, required=True, help="Output directory")
    parser.add_argument("--no-minhash", action="store_true", help="Disable MinHash dedup")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    main(
        input_dir=args.input,
        output_dir=args.output,
        config_path=args.config,
        no_minhash=args.no_minhash,
    )
