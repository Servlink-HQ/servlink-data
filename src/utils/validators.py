"""
Validators
==========
Domain-specific validation functions for Brazilian hospitality data.
Validates CNAE codes, geographic coordinates, and data integrity rules
before pipeline operations proceed.
"""

from __future__ import annotations

import re
from typing import Optional


# Bounding box for Florianopolis municipality
FLORIANOPOLIS_BBOX = {
    "min_lat": -27.85,
    "max_lat": -27.38,
    "min_lon": -48.65,
    "max_lon": -48.33,
}

# Valid CNAE patterns for hospitality sector
VALID_CNAE_DIVISIONS = {"55", "56"}
CNAE_PATTERN = re.compile(r"^\d{4}-\d/\d{2}$")  # e.g., 5510-8/01


def is_valid_cnae(code: str) -> bool:
    """
    Validate a CNAE subclass code format.

    Args:
        code: The CNAE code string (e.g., "5510-8/01").

    Returns:
        True if the code matches the expected CNAE subclass format.
    """
    return bool(CNAE_PATTERN.match(code))


def is_cnae_hospitality(code: str) -> bool:
    """
    Check if a CNAE code belongs to the hospitality sector.

    Hospitality is defined as CNAE Division 55 (Accommodation) or
    Division 56 (Food Services).

    Args:
        code: The CNAE code string.

    Returns:
        True if the code belongs to Division 55 or 56.
    """
    if not code or len(code) < 2:
        return False
    return code[:2] in VALID_CNAE_DIVISIONS


def is_within_florianopolis(lat: float, lon: float) -> bool:
    """
    Check if coordinates fall within the Florianopolis bounding box.

    Uses a conservative bounding box that covers the entire municipality
    including the island and mainland portions.

    Args:
        lat: Latitude in decimal degrees (WGS84).
        lon: Longitude in decimal degrees (WGS84).

    Returns:
        True if the point is within the Florianopolis bounding box.
    """
    return (
        FLORIANOPOLIS_BBOX["min_lat"] <= lat <= FLORIANOPOLIS_BBOX["max_lat"]
        and FLORIANOPOLIS_BBOX["min_lon"] <= lon <= FLORIANOPOLIS_BBOX["max_lon"]
    )


def normalize_phone_br(phone: Optional[str]) -> Optional[str]:
    """
    Normalize a Brazilian phone number to the standard format.

    Strips non-digit characters and ensures the number has a valid
    Brazilian structure (DDD + number).

    Args:
        phone: Raw phone string from crawled data.

    Returns:
        Normalized phone string or None if invalid.
    """
    if not phone:
        return None

    digits = re.sub(r"\D", "", phone)

    # Remove country code prefix if present
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]

    # Valid Brazilian numbers: 10 digits (landline) or 11 digits (mobile)
    if len(digits) not in (10, 11):
        return None

    # Format: (XX) XXXXX-XXXX or (XX) XXXX-XXXX
    ddd = digits[:2]
    if len(digits) == 11:
        return f"({ddd}) {digits[2:7]}-{digits[7:]}"
    return f"({ddd}) {digits[2:6]}-{digits[6:]}"


def sanitize_establishment_name(name: Optional[str]) -> Optional[str]:
    """
    Clean and standardize an establishment name.

    Removes excessive whitespace, trims special characters, and applies
    title case normalization.

    Args:
        name: Raw establishment name from crawled data.

    Returns:
        Cleaned name string or None if empty.
    """
    if not name:
        return None

    # Remove excessive whitespace
    cleaned = re.sub(r"\s+", " ", name.strip())

    # Remove leading/trailing punctuation (but preserve internal)
    cleaned = cleaned.strip(".-*#")

    return cleaned if cleaned else None
