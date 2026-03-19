"""Click-based CLI: filter / batch subcommands."""

from __future__ import annotations

import logging
from pathlib import Path

import click
import orjson

from task_pipeliner.pipeline import Pipeline


@click.group()
@click.pass_context
def main(ctx: click.Context) -> None:
    """task-pipeliner: configurable data processing pipeline."""
    ctx.ensure_object(dict)
    if "pipeline" not in ctx.obj:
        ctx.obj["pipeline"] = Pipeline()


@main.command("filter")
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to YAML config file.",
)
@click.option(
    "--input",
    "inputs",
    multiple=True,
    required=True,
    type=click.Path(path_type=Path),
    help="Input file or directory (repeatable).",
)
@click.option(
    "--output",
    "output_dir",
    required=True,
    type=click.Path(path_type=Path),
    help="Output directory.",
)
@click.option(
    "--workers",
    default=None,
    type=int,
    help="Override worker count from config.",
)
@click.pass_context
def filter_cmd(
    ctx: click.Context,
    config_path: Path,
    inputs: tuple[Path, ...],
    output_dir: Path,
    workers: int | None,
) -> None:
    """Run a filter pipeline on the given inputs."""
    logging.basicConfig(level=logging.INFO)
    pipeline: Pipeline = ctx.obj["pipeline"]

    try:
        from task_pipeliner.config import load_config

        cfg = load_config(config_path)
        if workers is not None:
            cfg.execution.workers = workers
        pipeline.run(config=cfg, inputs=list(inputs), output_dir=output_dir)
    except click.ClickException:
        raise
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


@main.command("batch")
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to YAML config file.",
)
@click.argument(
    "jobs_file",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--parallel",
    is_flag=True,
    default=False,
    help="Run jobs in parallel (not yet implemented).",
)
@click.pass_context
def batch_cmd(
    ctx: click.Context,
    config_path: Path,
    jobs_file: Path,
    parallel: bool,
) -> None:
    """Run multiple pipeline jobs from a jobs file."""
    logging.basicConfig(level=logging.INFO)
    pipeline: Pipeline = ctx.obj["pipeline"]

    jobs = orjson.loads(jobs_file.read_bytes())
    if not isinstance(jobs, list):
        raise click.ClickException("jobs file must contain a JSON array")

    try:
        from task_pipeliner.config import load_config

        cfg = load_config(config_path)

        for i, job in enumerate(jobs):
            job_inputs = [Path(p) for p in job["inputs"]]
            job_output = Path(job["output_dir"])
            click.echo(f"Job {i + 1}/{len(jobs)}: {job_output}")
            pipeline.run(config=cfg, inputs=job_inputs, output_dir=job_output)

    except click.ClickException:
        raise
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc
