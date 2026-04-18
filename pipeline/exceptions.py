"""Domain exceptions for the ETL pipeline."""


class PipelineError(Exception):
    """Base class for pipeline failures."""


class CheckpointError(PipelineError):
    """Checkpoint read/write failed."""


class ExtractionError(PipelineError):
    """Incremental extraction failed."""


class ValidationError(PipelineError):
    """Schema validation failed (non-fatal rows go to DLQ)."""


class LoadError(PipelineError):
    """Upsert / load step failed."""
