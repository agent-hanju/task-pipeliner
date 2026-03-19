"""Pydantic-based configuration schema for pipelines."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

from task_pipeliner.exceptions import ConfigValidationError

logger = logging.getLogger(__name__)


class _WrappingModel(BaseModel):
    """Base that converts pydantic ValidationError → ConfigValidationError."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)

    @classmethod
    def _wrap(cls, e: ValidationError) -> ConfigValidationError:
        field: str | None = None
        errors = e.errors()
        if errors:
            loc = errors[0].get("loc", ())
            field = str(loc[0]) if loc else None
        return ConfigValidationError(str(e), field=field, cause=e)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as e:
            raise self.__class__._wrap(e) from e


class StepConfig(_WrappingModel):
    model_config = ConfigDict(extra="allow")

    type: str
    enabled: bool = True


class ExecutionConfig(_WrappingModel):
    model_config = ConfigDict(extra="forbid")

    workers: int = 4
    queue_size: int = 200
    chunk_size: int = 100

    @field_validator("workers", "queue_size", "chunk_size")
    @classmethod
    def _must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be positive")
        return v


class PipelineConfig(_WrappingModel):
    model_config = ConfigDict(extra="forbid")

    pipeline: list[StepConfig]
    execution: ExecutionConfig = ExecutionConfig()

    @model_validator(mode="after")
    def _pipeline_not_empty(self) -> PipelineConfig:
        if not self.pipeline:
            raise ValueError("pipeline must not be empty")
        return self


def _wrap_validation_error(e: Exception) -> ConfigValidationError:
    """Convert pydantic/yaml errors into ConfigValidationError."""
    field: str | None = None
    if hasattr(e, "errors"):
        errors = e.errors()
        if errors:
            loc = errors[0].get("loc", ())
            field = str(loc[0]) if loc else None
    return ConfigValidationError(str(e), field=field, cause=e)


def load_config(path: Path) -> PipelineConfig:
    """Load a YAML config file and return a validated PipelineConfig."""
    logger.debug("path=%s", path)
    try:
        with open(path, encoding="utf-8") as f:
            data: Any = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ConfigValidationError("config must be a YAML mapping")
        cfg = PipelineConfig(**data)
    except ConfigValidationError:
        raise
    except Exception as e:
        raise _wrap_validation_error(e) from e

    disabled = [s.type for s in cfg.pipeline if not s.enabled]
    for step_type in disabled:
        logger.warning("step disabled: %s", step_type)

    logger.info("config loaded path=%s steps=%d", path, len(cfg.pipeline))
    return cfg
