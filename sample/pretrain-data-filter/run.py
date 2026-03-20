"""CLI entry point for pretrain-data-filter pipeline — W-15."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from steps import (
    HashComputeStep,
    HashLookupStep,
    MinHashComputeStep,
    MinHashLookupStep,
    QualityFilterStep,
    WriterStep,
)

from task_pipeliner.config import StepConfig, load_config
from task_pipeliner.pipeline import Pipeline

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG = Path(__file__).parent / "pipeline_config.yaml"


def main(
    input_paths: list[Path],
    output_dir: Path,
    config_path: Path | None = None,
    no_minhash: bool = False,
) -> None:
    """Run the pretrain-data-filter pipeline.

    1. Load config
    2. Optionally disable MinHash steps
    3. Override writer output_dir
    4. Register all steps
    5. Execute pipeline
    """
    logger.debug(
        "input_paths=%d output_dir=%s config_path=%s no_minhash=%s",
        len(input_paths),
        output_dir,
        config_path,
        no_minhash,
    )

    cfg = load_config(config_path or _DEFAULT_CONFIG)

    # CLI overrides
    for i, step_cfg in enumerate(cfg.pipeline):
        # Disable MinHash steps if requested
        if no_minhash and step_cfg.type in ("minhash_compute", "minhash_lookup"):
            cfg.pipeline[i] = StepConfig(  # type: ignore[call-arg,unused-ignore]
                type=step_cfg.type,
                enabled=False,
                outputs=step_cfg.outputs,
                **(step_cfg.model_extra or {}),
            )
            # Rewire hash_lookup kept → writer instead of minhash_compute
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

        # Override writer output_dir
        if step_cfg.type == "writer":
            extra = step_cfg.model_extra or {}
            cfg.pipeline[i] = StepConfig(  # type: ignore[call-arg,unused-ignore]
                type="writer",
                output_dir=str(output_dir),
                outputs=step_cfg.outputs,
                **{k: v for k, v in extra.items() if k != "output_dir"},
            )

    pipeline = Pipeline()
    pipeline.register_all(
        {
            "quality_filter": QualityFilterStep,
            "hash_compute": HashComputeStep,
            "hash_lookup": HashLookupStep,
            "minhash_compute": MinHashComputeStep,
            "minhash_lookup": MinHashLookupStep,
            "writer": WriterStep,
        }
    )

    pipeline.run(config=cfg, inputs=input_paths, output_dir=output_dir)
    logger.info("pipeline run completed config=%s", config_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pretrain data quality filter pipeline")
    parser.add_argument("--config", type=Path, default=None, help="Pipeline config YAML")
    parser.add_argument("--input", type=Path, nargs="+", required=True, help="Input JSONL file(s)")
    parser.add_argument("--output", type=Path, required=True, help="Output directory")
    parser.add_argument("--workers", type=int, default=None, help="Number of workers")
    parser.add_argument("--no-minhash", action="store_true", help="Disable MinHash dedup")

    args = parser.parse_args()

    main(
        input_paths=args.input,
        output_dir=args.output,
        config_path=args.config,
        no_minhash=args.no_minhash,
    )
