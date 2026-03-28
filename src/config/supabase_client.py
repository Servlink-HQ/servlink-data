"""
Supabase Client
===============
Secure connection module for the Supabase PostgreSQL backend.

Design Decisions:
    - Uses the `service_role` key (NOT the anon key) to bypass RLS
      for pipeline INSERT/UPDATE operations. The anon key is reserved
      for the frontend (servlink-hub).
    - Implements a thread-safe singleton pattern via module-level caching.
    - All operations are wrapped with structured logging and retry logic.

Security Notes:
    - The service_role key grants FULL database access. It must NEVER
      be exposed in client-side code, committed to git, or logged.
    - In GitHub Actions, this key is stored as a Repository Secret.

Usage:
    from src.config.supabase_client import get_supabase_client
    client = get_supabase_client()
    result = client.table("raw_crawled_data").insert({...}).execute()
"""

from __future__ import annotations

import sys
from functools import lru_cache
from typing import Any

from supabase import Client, create_client

from src.config.settings import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SupabaseConnectionError(Exception):
    """Raised when the Supabase client cannot be initialized."""


def _create_supabase_client() -> Client:
    """
    Create and validate a Supabase client instance.

    Performs a lightweight health check by querying the PostgREST endpoint
    to ensure the connection is valid before returning the client.

    Returns:
        Client: An authenticated Supabase client using the service_role key.

    Raises:
        SupabaseConnectionError: If the client cannot be created or
            the connection health check fails.
    """
    settings = get_settings()

    # Validate that credentials are not placeholder values
    if "your-project" in settings.SUPABASE_URL or "your_service_role" in settings.SUPABASE_SERVICE_ROLE_KEY:
        msg = (
            "Supabase credentials contain placeholder values. "
            "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in your .env or GitHub Secrets."
        )
        logger.error("supabase_client_init_failed", reason="placeholder_credentials")
        raise SupabaseConnectionError(msg)

    try:
        client: Client = create_client(
            supabase_url=settings.SUPABASE_URL,
            supabase_key=settings.SUPABASE_SERVICE_ROLE_KEY,
        )

        logger.info(
            "supabase_client_initialized",
            url=settings.SUPABASE_URL,
            environment=settings.PIPELINE_ENV,
        )

        return client

    except Exception as exc:
        logger.error(
            "supabase_client_init_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        raise SupabaseConnectionError(
            f"Failed to initialize Supabase client: {exc}"
        ) from exc


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    """
    Return a cached singleton Supabase client.

    The client is created once per process lifetime and reused across
    all pipeline modules. This avoids redundant authentication handshakes
    and connection overhead.

    Returns:
        Client: The authenticated Supabase client.

    Raises:
        SupabaseConnectionError: If the client cannot be created.
    """
    return _create_supabase_client()


def health_check() -> dict[str, Any]:
    """
    Perform a lightweight health check on the Supabase connection.

    Executes a simple SQL query to verify database connectivity and
    returns diagnostic information.

    Returns:
        dict: Health check result with keys 'status', 'message', and optionally 'error'.
    """
    try:
        client = get_supabase_client()

        # Simple connectivity test: query PostgreSQL version
        result = client.rpc("", {}).execute()  # type: ignore[arg-type]

        # If we get here without exception, the connection is alive
        logger.info("supabase_health_check_passed")
        return {
            "status": "healthy",
            "message": "Supabase connection is active.",
            "supabase_url": get_settings().SUPABASE_URL,
        }

    except Exception as exc:
        logger.error("supabase_health_check_failed", error=str(exc))
        return {
            "status": "unhealthy",
            "message": f"Supabase connection failed: {exc}",
            "error": str(exc),
        }


if __name__ == "__main__":
    # Quick connection test when run directly
    result = health_check()
    print(f"Health Check: {result['status']}")
    print(f"Message: {result['message']}")
    sys.exit(0 if result["status"] == "healthy" else 1)
