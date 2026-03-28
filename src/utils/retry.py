"""
Retry Decorator with Exponential Backoff
=========================================
Resilient retry logic for network-bound operations (API calls, FTP transfers,
database writes). Prevents transient failures from crashing entire pipeline runs.

Design Decisions:
    - Exponential backoff: wait = base_delay * (2 ^ attempt) + jitter
    - Jitter prevents thundering herd when multiple pipelines retry simultaneously.
    - Configurable exception whitelist (only retry on expected transient errors).
    - Logs every retry attempt with structured context for debugging.

Usage:
    from src.utils.retry import with_retry

    @with_retry(max_attempts=3, base_delay=2.0)
    def fetch_ibge_data() -> dict:
        ...
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from src.utils.logger import get_logger

logger = get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

# Default transient exceptions to retry on
DEFAULT_RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
)


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable_exceptions: tuple[type[Exception], ...] = DEFAULT_RETRYABLE_EXCEPTIONS,
) -> Callable[[F], F]:
    """
    Decorator that retries a function on transient failures.

    Args:
        max_attempts: Maximum number of attempts (including the first call).
        base_delay: Initial delay in seconds before the first retry.
        max_delay: Maximum delay cap in seconds (prevents absurdly long waits).
        jitter: If True, adds random jitter to prevent synchronized retries.
        retryable_exceptions: Tuple of exception types that trigger a retry.
            Non-matching exceptions propagate immediately.

    Returns:
        The decorated function with retry behavior.

    Raises:
        The last exception if all retry attempts are exhausted.

    Example:
        >>> @with_retry(max_attempts=3, base_delay=2.0)
        ... def unstable_api_call() -> dict:
        ...     response = httpx.get("https://api.example.com/data")
        ...     response.raise_for_status()
        ...     return response.json()
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except retryable_exceptions as exc:
                    last_exception = exc

                    if attempt >= max_attempts:
                        logger.error(
                            "retry_exhausted",
                            function=func.__name__,
                            attempt=attempt,
                            max_attempts=max_attempts,
                            error=str(exc),
                            error_type=type(exc).__name__,
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)

                    # Add jitter (0-50% of calculated delay)
                    if jitter:
                        delay += random.uniform(0, delay * 0.5)  # noqa: S311

                    logger.warning(
                        "retry_attempt",
                        function=func.__name__,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        delay_seconds=round(delay, 2),
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )

                    time.sleep(delay)

            # This should never be reached, but satisfies the type checker
            if last_exception is not None:
                raise last_exception
            msg = "Retry logic error: no exception captured."
            raise RuntimeError(msg)

        return wrapper  # type: ignore[return-value]

    return decorator
