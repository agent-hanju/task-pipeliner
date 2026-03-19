"""Custom exceptions for task-pipeliner."""

from __future__ import annotations


class PipelineError(Exception):
    """Base exception for all pipeline errors."""


class StepRegistrationError(PipelineError):
    """Raised when step registration fails (duplicate, unpicklable, etc.)."""

    def __init__(self, message: str, *, step_name: str = "") -> None:
        super().__init__(message)
        self.step_name = step_name


class ConfigValidationError(PipelineError):
    """Raised when configuration validation fails."""

    def __init__(
        self,
        message: str,
        *,
        field: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.field = field
        self.cause = cause


class PipelineShutdownError(PipelineError):
    """Raised when pipeline shutdown encounters an error."""
