"""
Application Settings
====================
Centralized configuration management using Pydantic Settings.
All secrets are loaded from environment variables (GitHub Secrets in CI,
.env file in local development). No secrets are ever hardcoded.

Usage:
    from src.config.settings import get_settings
    settings = get_settings()
    print(settings.SUPABASE_URL)
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Immutable application configuration.

    Values are loaded from environment variables with fallback to a .env file.
    Required fields will raise a ValidationError at startup if missing,
    preventing silent failures in production.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # --- Supabase (Required) ---
    SUPABASE_URL: str
    SUPABASE_SERVICE_ROLE_KEY: str

    # --- Overpass API ---
    OVERPASS_API_URL: str = "https://overpass-api.de/api/interpreter"

    # --- SearXNG ---
    SEARXNG_URL: str = "https://localhost:8080"

    # --- Outscraper (Future - Paid tier) ---
    OUTSCRAPER_API_KEY: Optional[str] = None

    # --- LLM Classification ---
    OLLAMA_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3.1:8b"

    # --- IBGE Constants ---
    IBGE_MUNICIPIO_COD: str = "4205407"  # Florianopolis
    IBGE_CNAE_ALOJAMENTO: str = "55"  # CNAE Division 55 - Accommodation
    IBGE_CNAE_ALIMENTACAO: str = "56"  # CNAE Division 56 - Food Services

    # --- Logging ---
    LOG_LEVEL: str = "INFO"

    # --- Pipeline Metadata ---
    PIPELINE_ENV: str = "development"  # "development" | "production" | "ci"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return a cached singleton of the application settings.

    Uses lru_cache to ensure the settings object is only instantiated once
    per process lifetime, avoiding repeated file I/O and env parsing.

    Returns:
        Settings: The validated application configuration.

    Raises:
        pydantic.ValidationError: If required environment variables are missing.
    """
    return Settings()
