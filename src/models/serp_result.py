"""
SERP Keyword Ranking Models
============================
Pydantic schemas for search engine position tracking data.
Maps to the fact_serp_keywords table.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class SerpKeywordResult(BaseModel):
    """
    Schema for a single SERP keyword ranking data point.

    Each record captures the position of a specific domain for a
    tracked keyword at a point in time.
    """

    keyword: str = Field(
        ...,
        description="The search keyword being tracked (e.g., 'hoteis florianopolis')",
    )
    domain: Optional[str] = Field(
        default=None,
        description="The domain that appears in results (e.g., 'servlink.com.br')",
    )
    position: Optional[int] = Field(
        default=None,
        ge=1,
        le=100,
        description="Ranking position in SERP (1 = top result)",
    )
    search_engine: str = Field(
        default="google",
        description="Search engine: 'google', 'bing', 'duckduckgo'",
    )
    location: str = Field(
        default="Florianopolis, SC, Brazil",
        description="Geographic location for the search query",
    )
    checked_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp of when this ranking was checked",
    )
    metadata: Optional[dict[str, object]] = Field(
        default=None,
        description="Additional data: snippet text, result URL, featured snippet flag",
    )


class TrackedKeyword(BaseModel):
    """
    Configuration model for keywords to be monitored.

    This is used as input configuration, not persisted to the database.
    """

    keyword: str
    category: str = Field(
        ...,
        description="Category: 'accommodation', 'dining', 'tourism', 'competitor'",
    )
    priority: int = Field(
        default=1,
        ge=1,
        le=5,
        description="Priority level (1=highest, 5=lowest) for scheduling",
    )
    target_domains: list[str] = Field(
        default_factory=list,
        description="Domains to specifically track for this keyword",
    )
