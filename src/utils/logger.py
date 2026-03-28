"""
Structured Logger
=================
JSON-formatted structured logging for observability and debugging.

All pipeline modules use this logger to produce machine-parseable log
lines that can be ingested by log aggregation systems (e.g., Datadog,
Grafana Loki) or simply read in GitHub Actions output.

Design Decisions:
    - Uses `structlog` for structured key-value logging.
    - Outputs JSON in production/CI, human-readable in development.
    - Automatically attaches context: timestamp, module, log level.
    - Thread-safe for concurrent pipeline execution.

Usage:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("extraction_started", source="ibge", records=150)
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def _configure_structlog(log_level: str = "INFO") -> None:
    """
    Configure structlog with appropriate processors.

    In development, uses a human-readable console renderer.
    In CI/production, outputs JSON for machine parsing.

    Args:
        log_level: The minimum log level to capture (DEBUG, INFO, WARNING, ERROR).
    """
    # Determine environment from env var (defaults to dev-friendly output)
    import os
    environment = os.getenv("PIPELINE_ENV", "development")

    # Shared processors for all environments
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if environment == "development":
        # Human-readable colored output for local development
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        # JSON output for CI/production (GitHub Actions, log aggregators)
        renderer = structlog.processors.JSONRenderer()  # type: ignore[assignment]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure stdlib logging handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=renderer,
            foreign_pre_chain=shared_processors,
        )
    )

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))


# Initialize on module import
_configure_structlog()


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a named structured logger instance.

    Args:
        name: The logger name, typically ``__name__`` of the calling module.

    Returns:
        BoundLogger: A structured logger bound to the given name.

    Example:
        >>> logger = get_logger("src.extractors.ibge_sidra")
        >>> logger.info("extraction_complete", records_inserted=42)
    """
    return structlog.get_logger(name)
