"""
Establishment Domain Models
============================
Pydantic schemas that define the ontology of hospitality establishments
in Florianopolis. These models serve as the single source of truth for
data validation across the entire pipeline:

    - Raw extraction -> validated via RawEstablishment
    - LLM classification -> structured via ClassifiedEstablishment
    - Database persistence -> serialized via DimEstablishment

All field names, types, and constraints are documented here as part
of the Data Governance strategy.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class EstablishmentType(str, Enum):
    """Primary classification of hospitality establishments."""

    HOTEL = "hotel"
    HOSTEL = "hostel"
    POUSADA = "pousada"
    GUEST_HOUSE = "guest_house"
    RESTAURANT = "restaurant"
    CAFE = "cafe"
    BAR = "bar"
    PUB = "pub"
    BAKERY = "bakery"
    OTHER = "other"


class RawEstablishment(BaseModel):
    """
    Schema for raw establishment data as received from extraction sources.

    This is the most permissive model -- it accepts incomplete data from
    Overpass API, Outscraper, or SANTUR and validates only the minimum
    required fields. Missing fields are None by default.
    """

    source: str = Field(
        ...,
        description="Data source identifier: 'overpass', 'outscraper', 'santur', 'serp'",
    )
    source_id: Optional[str] = Field(
        default=None,
        description="External unique ID from the source (OSM node_id, Google place_id, etc.)",
    )
    name: Optional[str] = Field(
        default=None,
        description="Establishment name as extracted (may contain noise)",
    )
    latitude: Optional[float] = Field(
        default=None,
        ge=-90.0,
        le=90.0,
        description="Latitude in decimal degrees (WGS84)",
    )
    longitude: Optional[float] = Field(
        default=None,
        ge=-180.0,
        le=180.0,
        description="Longitude in decimal degrees (WGS84)",
    )
    raw_type: Optional[str] = Field(
        default=None,
        description="Type as declared by the source (amenity=restaurant, tourism=hotel, etc.)",
    )
    phone: Optional[str] = Field(default=None, description="Raw phone number")
    website: Optional[str] = Field(default=None, description="Website URL")
    opening_hours: Optional[str] = Field(default=None, description="Opening hours string")
    cuisine: Optional[str] = Field(
        default=None,
        description="Cuisine type(s), semicolon-separated in OSM format",
    )
    address: Optional[str] = Field(default=None, description="Street address")
    rating: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=5.0,
        description="Rating score (0.0 to 5.0)",
    )
    review_count: Optional[int] = Field(
        default=None,
        ge=0,
        description="Total number of reviews",
    )
    extra_data: Optional[dict[str, object]] = Field(
        default=None,
        description="Catch-all for source-specific fields not in the standard schema",
    )


class ClassifiedEstablishment(BaseModel):
    """
    Schema for LLM-classified establishment data.

    Produced by the LLM classifier (Ollama) with structured outputs.
    This model is used as the `format` parameter in the Ollama API call,
    forcing deterministic JSON output from the language model.
    """

    standardized_name: str = Field(
        ...,
        description="Cleaned, standardized establishment name",
    )
    type: EstablishmentType = Field(
        ...,
        description="Primary classification",
    )
    subtype: Optional[str] = Field(
        default=None,
        description="Detailed subtype (e.g., 'boutique_hotel', 'fine_dining', 'fast_food')",
    )
    suggested_cnae: Optional[str] = Field(
        default=None,
        description="Suggested CNAE subclass code (e.g., '5510-8/01')",
    )
    estimated_neighborhood: Optional[str] = Field(
        default=None,
        description="Estimated neighborhood in Florianopolis",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Descriptive tags: ['pet_friendly', 'wifi', 'ocean_view', 'organic']",
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="LLM confidence score for this classification (0.0 to 1.0)",
    )


class DimEstablishment(BaseModel):
    """
    Schema for the final dim_estabelecimentos table.

    This is the fully validated, production-ready representation of an
    establishment. All fields that reach this model have been cleaned,
    classified, and cross-referenced.
    """

    name: str
    original_name: Optional[str] = None
    type: EstablishmentType
    subtype: Optional[str] = None
    cnae_code: Optional[str] = None
    address: Optional[str] = None
    neighborhood: Optional[str] = None
    latitude: float
    longitude: float
    phone: Optional[str] = None
    website: Optional[str] = None
    opening_hours: Optional[str] = None
    google_rating: Optional[float] = Field(default=None, ge=0.0, le=5.0)
    total_reviews: Optional[int] = Field(default=None, ge=0)
    cuisine: Optional[list[str]] = None
    llm_tags: Optional[dict[str, object]] = None
    source_refs: Optional[dict[str, str]] = Field(
        default=None,
        description="Cross-references: {'osm_id': '...', 'place_id': '...', 'cadastur': '...'}",
    )
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator("cnae_code")
    @classmethod
    def validate_cnae_format(cls, v: Optional[str]) -> Optional[str]:
        """Validate CNAE code format if provided."""
        if v is None:
            return None
        import re

        if not re.match(r"^\d{4}-\d/\d{2}$", v):
            msg = f"Invalid CNAE format: {v}. Expected pattern: XXXX-X/XX"
            raise ValueError(msg)
        return v
