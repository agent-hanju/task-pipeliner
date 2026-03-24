"""Pydantic-based configuration schema for pipelines."""

from __future__ import annotations

import logging
import re
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
    name: str = ""
    """Instance name. Defaults to ``type`` when omitted (backward compatible)."""
    enabled: bool = True
    outputs: dict[str, str | list[str]] | None = None

    @model_validator(mode="after")
    def _default_name(self) -> StepConfig:
        if not self.name:
            self.name = self.type
        return self


class ExecutionConfig(_WrappingModel):
    model_config = ConfigDict(extra="forbid")

    workers: int = 4
    queue_size: int = 0
    """queue_size is reserved for future disk-spill threshold. 0 means unbounded."""
    chunk_size: int = 100

    @field_validator("workers", "chunk_size")
    @classmethod
    def _must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be positive")
        return v

    @field_validator("queue_size")
    @classmethod
    def _must_be_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("must be non-negative")
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

    @model_validator(mode="after")
    def _names_unique(self) -> PipelineConfig:
        seen: dict[str, int] = {}
        for i, step in enumerate(self.pipeline):
            assert step.name is not None  # guaranteed by StepConfig._default_name
            if step.name in seen:
                raise ValueError(
                    f"duplicate step name '{step.name}' at index {i} "
                    f"(first at index {seen[step.name]}). "
                    f"Use the 'name' field to distinguish instances of the same type"
                )
            seen[step.name] = i
        return self

    @model_validator(mode="after")
    def _outputs_reference_valid_steps(self) -> PipelineConfig:
        known_names = {step.name for step in self.pipeline}
        for step in self.pipeline:
            if step.outputs is None:
                continue
            for tag, targets in step.outputs.items():
                if isinstance(targets, str):
                    targets = [targets]
                for target in targets:
                    if target not in known_names:
                        raise ValueError(
                            f"step '{step.name}' outputs tag '{tag}' references "
                            f"unknown step '{target}'"
                        )
        return self


_VAR_PATTERN = re.compile(r"(?<!\$)\$\{(\w+)(?::-(.*?))?\}")
_ESCAPE_PATTERN = re.compile(r"\$\$\{")


def _resolve_variables(obj: Any, variables: dict[str, Any]) -> Any:
    """Recursively resolve ``${var}`` placeholders in a parsed YAML tree.

    - If a string value is exactly ``${var}``, replace with the variable as-is
      (supports lists, dicts, numbers, etc.).
    - ``${var:-default}`` uses ``default`` when ``var`` is not in variables.
    - ``$${var}`` is an escape — produces literal ``${var}``.
    - If ``${var}`` appears within a larger string, do string substitution
      (variable value must be stringable).
    - Raise ``ConfigValidationError`` for unresolved references (no default).
    """
    if isinstance(obj, str):
        # Exact match: entire value is ${var} or ${var:-default} → replace with any type
        m = _VAR_PATTERN.fullmatch(obj)
        if m:
            name = m.group(1)
            default = m.group(2)
            if name in variables:
                return variables[name]
            if default is not None:
                return default
            raise ConfigValidationError(
                f"unresolved variable: ${{{name}}}", field=name
            )

        # Partial match: ${var} embedded in a string → string substitution
        def _replacer(match: re.Match[str]) -> str:
            name = match.group(1)
            default = match.group(2)
            if name in variables:
                return str(variables[name])
            if default is not None:
                return default
            raise ConfigValidationError(
                f"unresolved variable: ${{{name}}}", field=name
            )

        result = _VAR_PATTERN.sub(_replacer, obj)
        # Unescape: $${...} → ${...}
        result = _ESCAPE_PATTERN.sub("${", result)
        return result if result != obj else obj

    if isinstance(obj, dict):
        return {k: _resolve_variables(v, variables) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_resolve_variables(item, variables) for item in obj]

    return obj


def _wrap_validation_error(e: Exception) -> ConfigValidationError:
    """Convert pydantic/yaml errors into ConfigValidationError."""
    field: str | None = None
    if isinstance(e, ValidationError):
        errors = e.errors()
        if errors:
            loc = errors[0].get("loc", ())
            field = str(loc[0]) if loc else None
    return ConfigValidationError(str(e), field=field, cause=e)


def load_config(
    path: Path,
    variables: dict[str, Any] | None = None,
) -> PipelineConfig:
    """Load a YAML config file and return a validated PipelineConfig.

    Parameters
    ----------
    path:
        Path to the YAML config file.
    variables:
        Optional variable overrides for ``${var}`` substitution in the config.
        If the entire YAML value is ``${var}``, the variable replaces it as-is
        (supports lists, dicts, numbers). If ``${var}`` appears within a string,
        it is substituted as a string. ``${var:-default}`` provides a fallback
        when the variable is not in the dict. ``$${var}`` escapes to literal
        ``${var}``.
    """
    logger.debug("path=%s variables=%s", path, variables)
    try:
        with open(path, encoding="utf-8") as f:
            data: Any = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ConfigValidationError("config must be a YAML mapping")

        if variables is not None:
            data = _resolve_variables(data, variables)

        cfg = PipelineConfig(**data)    # pyright: ignore[reportUnknownArgumentType]
    except ConfigValidationError:
        raise
    except Exception as e:
        raise _wrap_validation_error(e) from e

    disabled = [s.name for s in cfg.pipeline if not s.enabled]
    for step_name in disabled:
        logger.warning("step disabled: %s", step_name)

    logger.info("config loaded path=%s steps=%d", path, len(cfg.pipeline))
    return cfg
