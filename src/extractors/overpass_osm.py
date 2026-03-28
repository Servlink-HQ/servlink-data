"""
Overpass OSM Extractor
======================
Extracts Points of Interest (POIs) for hospitality establishments in
Florianópolis from OpenStreetMap via the Overpass API.

Categories extracted:
    - tourism: hotel, hostel, guest_house
    - amenity: restaurant, cafe, bar, pub

Bounding box: (-27.85, -48.65, -27.38, -48.33)  # (south, west, north, east)

Usage:
    python -m src.extractors.overpass_osm
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

from src.config.settings import get_settings
from src.config.supabase_client import get_supabase_client
from src.models.estabelecimento import RawEstablishment
from src.utils.logger import get_logger
from src.utils.retry import with_retry

logger = get_logger(__name__)

# Florianópolis bounding box: (south, west, north, east)
FLORIANOPOLIS_BBOX: tuple[float, float, float, float] = (-27.85, -48.65, -27.38, -48.33)

# Queries to execute: (key, value) pairs
OVERPASS_QUERIES: list[dict[str, str]] = [
    {"key": "tourism", "value": "hotel"},
    {"key": "tourism", "value": "hostel"},
    {"key": "tourism", "value": "guest_house"},
    {"key": "amenity", "value": "restaurant"},
    {"key": "amenity", "value": "cafe"},
    {"key": "amenity", "value": "bar"},
    {"key": "amenity", "value": "pub"},
]

# Rate limiting: max 2 concurrent requests, 10s cooldown between sequential queries
_RATE_LIMIT_SEMAPHORE = threading.Semaphore(2)
_RATE_LIMIT_COOLDOWN = 10.0  # seconds between queries

_HTTP_TIMEOUT = 60.0  # seconds


def _build_overpass_query(
    key: str,
    value: str,
    bbox: tuple[float, float, float, float],
) -> str:
    """
    Build an Overpass QL query for nodes and ways matching the given tag.

    Args:
        key: OSM tag key (e.g. 'tourism', 'amenity').
        value: OSM tag value (e.g. 'hotel', 'restaurant').
        bbox: Bounding box as (south, west, north, east).

    Returns:
        Overpass QL query string.
    """
    south, west, north, east = bbox
    bbox_str = f"{south},{west},{north},{east}"
    return (
        f"[out:json][timeout:30];\n"
        f"(\n"
        f"  node[{key}={value}]({bbox_str});\n"
        f"  way[{key}={value}]({bbox_str});\n"
        f");\n"
        f"out center tags;"
    )


@with_retry(
    max_attempts=3,
    base_delay=2.0,
    retryable_exceptions=(ConnectionError, TimeoutError, OSError),
)
def _fetch_overpass(query: str, api_url: str) -> dict[str, Any]:
    """
    Execute an Overpass QL query via HTTP POST.

    Handles 429 rate-limit responses by raising a ConnectionError so the
    with_retry decorator backs off and retries automatically.

    Args:
        query: Overpass QL query string.
        api_url: Overpass API endpoint URL.

    Returns:
        Parsed JSON response from the Overpass API.

    Raises:
        ConnectionError: On network errors or 429 / unexpected status responses.
    """
    with _RATE_LIMIT_SEMAPHORE:
        try:
            response = httpx.post(
                api_url,
                data={"data": query},
                timeout=_HTTP_TIMEOUT,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ConnectionError(f"Overpass API request failed: {exc}") from exc

    if response.status_code == 429:
        raise ConnectionError("Overpass API rate limit exceeded (429). Backing off.")

    if response.status_code != 200:
        raise ConnectionError(
            f"Overpass API returned status {response.status_code}: {response.text[:200]}"
        )

    try:
        return response.json()  # type: ignore[no-any-return]
    except Exception as exc:
        raise ConnectionError(
            f"Failed to parse Overpass API response as JSON: {exc}"
        ) from exc


def _parse_element(
    element: dict[str, Any],
    key: str,
    value: str,
) -> RawEstablishment | None:
    """
    Convert a single Overpass API element into a RawEstablishment.

    Handles both node elements (lat/lon directly on the element) and
    way elements (lat/lon inside a 'center' sub-object).

    Args:
        element: A single OSM element dict from the Overpass API response.
        key: OSM tag key used in the query (e.g. 'tourism').
        value: OSM tag value used in the query (e.g. 'hotel').

    Returns:
        A validated RawEstablishment instance, or None if the element is
        missing coordinates or fails validation.
    """
    tags: dict[str, str] = element.get("tags", {})
    element_type: str = element.get("type", "node")
    element_id: int = element.get("id", 0)

    # Ways use a 'center' sub-object for their representative coordinate
    if element_type == "node":
        lat = element.get("lat")
        lon = element.get("lon")
    else:
        center = element.get("center", {})
        lat = center.get("lat")
        lon = center.get("lon")

    if lat is None or lon is None:
        logger.warning(
            "overpass_element_missing_coords",
            element_id=element_id,
            element_type=element_type,
        )
        return None

    # Build a composite address from addr:* tags
    addr_parts = [
        tags.get("addr:street", ""),
        tags.get("addr:housenumber", ""),
        tags.get("addr:city", ""),
    ]
    address = ", ".join(p for p in addr_parts if p) or None

    # Anything not in the standard schema goes to extra_data
    standard_keys = {
        "name", "phone", "contact:phone", "website", "contact:website",
        "opening_hours", "cuisine", "addr:street", "addr:housenumber",
        "addr:city", key,
    }
    extra: dict[str, Any] = {k: v for k, v in tags.items() if k not in standard_keys}

    try:
        return RawEstablishment(
            source="overpass",
            source_id=f"{element_type}/{element_id}",
            name=tags.get("name"),
            latitude=float(lat),
            longitude=float(lon),
            raw_type=f"{key}={value}",
            phone=tags.get("phone") or tags.get("contact:phone"),
            website=tags.get("website") or tags.get("contact:website"),
            opening_hours=tags.get("opening_hours"),
            cuisine=tags.get("cuisine"),
            address=address,
            extra_data=extra or None,
        )
    except Exception as exc:
        logger.warning(
            "overpass_element_validation_failed",
            element_id=element_id,
            error=str(exc),
        )
        return None


def _insert_batch(records: list[RawEstablishment], batch_id: str) -> int:
    """
    Upsert validated records into raw_crawled_data.

    Uses ON CONFLICT DO NOTHING on (source, source_id) so re-running the
    extractor never creates duplicate rows.

    Args:
        records: Validated RawEstablishment instances to persist.
        batch_id: UUID string grouping all records from this pipeline run.

    Returns:
        Number of rows actually inserted.
    """
    if not records:
        return 0

    client = get_supabase_client()

    rows = [
        {
            "source": r.source,
            "source_id": r.source_id,
            "payload": r.model_dump(mode="json"),
            "batch_id": batch_id,
        }
        for r in records
    ]

    result = (
        client.table("raw_crawled_data")
        .upsert(rows, on_conflict="source,source_id", ignore_duplicates=True)
        .execute()
    )

    inserted = len(result.data) if result.data else 0
    logger.info("overpass_batch_inserted", count=inserted, batch_id=batch_id)
    return inserted


def _log_pipeline(
    batch_id: str,
    status: str,
    records_processed: int = 0,
    error_message: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    """
    Write a pipeline execution record to data_pipeline_logs.

    Failures are swallowed so that audit log errors never crash the extractor.
    """
    try:
        client = get_supabase_client()
        row: dict[str, Any] = {
            "pipeline_name": "overpass_osm_extractor",
            "batch_id": batch_id,
            "status": status,
            "records_processed": records_processed,
            "github_run_id": os.getenv("GITHUB_RUN_ID"),
            "github_workflow": os.getenv("GITHUB_WORKFLOW"),
        }
        if error_message is not None:
            row["error_message"] = error_message
        if started_at is not None:
            row["started_at"] = started_at.isoformat()
        if finished_at is not None:
            row["finished_at"] = finished_at.isoformat()
            if started_at is not None:
                row["duration_ms"] = int((finished_at - started_at).total_seconds() * 1000)

        client.table("data_pipeline_logs").insert(row).execute()
    except Exception as exc:
        logger.error("pipeline_log_write_failed", error=str(exc))


def run_extraction() -> int:
    """
    Run the full Overpass OSM extraction pipeline.

    For each configured query:
        1. Build the Overpass QL query for the bounding box.
        2. Fetch POIs from the Overpass API (with retry on transient errors).
        3. Parse and validate each OSM element with RawEstablishment.
        4. Upsert valid records into raw_crawled_data under a shared batch_id.

    A 10-second cooldown is applied between consecutive API requests.
    Pipeline start/finish/error events are recorded in data_pipeline_logs.

    Returns:
        Total number of records inserted across all queries.
    """
    settings = get_settings()
    api_url = settings.OVERPASS_API_URL
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(tz=timezone.utc)

    _log_pipeline(batch_id=batch_id, status="running", started_at=started_at)
    logger.info("extraction_started", pipeline="overpass_osm", batch_id=batch_id)

    total_inserted = 0
    total_parsed = 0

    try:
        for i, query_config in enumerate(OVERPASS_QUERIES):
            if i > 0:
                time.sleep(_RATE_LIMIT_COOLDOWN)

            key = query_config["key"]
            value = query_config["value"]
            raw_type = f"{key}={value}"

            logger.info("fetching_overpass_query", raw_type=raw_type)

            query = _build_overpass_query(key, value, FLORIANOPOLIS_BBOX)
            data = _fetch_overpass(query, api_url)

            elements: list[dict[str, Any]] = data.get("elements", [])
            logger.info(
                "overpass_elements_received",
                raw_type=raw_type,
                element_count=len(elements),
            )

            records: list[RawEstablishment] = []
            for element in elements:
                record = _parse_element(element, key, value)
                if record is not None:
                    records.append(record)

            inserted = _insert_batch(records, batch_id)
            total_inserted += inserted
            total_parsed += len(records)

            logger.info(
                "overpass_query_complete",
                raw_type=raw_type,
                records_parsed=len(records),
                records_inserted=inserted,
            )

    except Exception as exc:
        finished_at = datetime.now(tz=timezone.utc)
        logger.error("extraction_failed", error=str(exc), error_type=type(exc).__name__)
        _log_pipeline(
            batch_id=batch_id,
            status="error",
            records_processed=total_inserted,
            error_message=str(exc),
            started_at=started_at,
            finished_at=finished_at,
        )
        raise

    finished_at = datetime.now(tz=timezone.utc)
    _log_pipeline(
        batch_id=batch_id,
        status="success",
        records_processed=total_inserted,
        started_at=started_at,
        finished_at=finished_at,
    )
    logger.info(
        "extraction_complete",
        pipeline="overpass_osm",
        total_parsed=total_parsed,
        total_inserted=total_inserted,
        duration_ms=int((finished_at - started_at).total_seconds() * 1000),
    )

    return total_inserted


if __name__ == "__main__":
    total = run_extraction()
    print(f"Extracted {total} records from Overpass OSM.")
