"""
LLM Classifier Transformer
===========================
Reads unprocessed records from ``raw_crawled_data``, classifies each
establishment via Ollama (structured JSON output), and writes clean
records to ``dim_estabelecimentos``.

Data flow::

    raw_crawled_data (processed=false)
        → RawEstablishment (Pydantic validation)
        → Ollama structured output (ClassifiedEstablishment schema)
        → Fuzzy dedup check (pg_trgm via check_establishment_duplicate RPC)
        → dim_estabelecimentos INSERT
        → raw_crawled_data.processed = true

Deduplication strategy:
    Before every INSERT the pipeline calls the ``check_establishment_duplicate``
    PostgreSQL function (migration 008).  If a record with trigram similarity
    > 0.8 AND PostGIS distance < 50 m already exists, the raw record is marked
    as processed and skipped — no duplicate dimension row is created.

Usage::

    python -m src.transformers.llm_classifier
    python -m src.transformers.llm_classifier --batch-size 50
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

import ollama

from src.config.settings import get_settings
from src.config.supabase_client import get_supabase_client
from src.models.estabelecimento import (
    ClassifiedEstablishment,
    RawEstablishment,
)
from src.utils.logger import get_logger
from src.utils.retry import with_retry

logger = get_logger(__name__)

_BATCH_SIZE = 20          # default records per classification run
_LLM_TEMPERATURE = 0.1   # low temperature for deterministic output

SYSTEM_PROMPT = (
    "You are a hospitality industry data analyst specializing in Florianópolis, Brazil. "
    "Classify establishments from raw OSM or web-scraped data into structured records.\n\n"
    "Guidelines:\n"
    "- Standardize names: fix capitalization and accents, remove noise "
    '(e.g., "HOTEL BEIRA MAR LTDA" → "Hotel Beira Mar").\n'
    "- Classify type using only the allowed enum values.\n"
    "- Suggest CNAE codes: 5510-8/01 (hotels), 5590-6/01 (hostels/pousadas), "
    "5611-2/01 (restaurants), 5611-2/03 (lanchonetes/fast-food), "
    "5611-2/05 (bares/botequins), 5620-1/04 (cafés).\n"
    "- Estimate the Florianópolis neighborhood from the coordinates and address.\n"
    "- Generate concise, useful tags such as pet_friendly, wifi, ocean_view, "
    "pool, parking, outdoor_seating, delivery.\n"
    "- Set confidence ≥ 0.9 only when name, type, and location are all clearly "
    "identifiable. Use confidence < 0.6 when the establishment type is ambiguous."
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_prompt(raw: RawEstablishment) -> str:
    """
    Build a user-turn prompt from a validated RawEstablishment.

    Includes all non-null fields from the raw record so the LLM has the
    maximum available context for classification.

    Args:
        raw: Validated RawEstablishment instance.

    Returns:
        Formatted prompt string ready to send to the LLM.
    """
    parts = [
        f"Name: {raw.name or '(unknown)'}",
        f"OSM type: {raw.raw_type or '(unknown)'}",
        f"Coordinates: lat={raw.latitude}, lon={raw.longitude}",
    ]
    if raw.address:
        parts.append(f"Address: {raw.address}")
    if raw.cuisine:
        parts.append(f"Cuisine: {raw.cuisine}")
    if raw.phone:
        parts.append(f"Phone: {raw.phone}")
    if raw.website:
        parts.append(f"Website: {raw.website}")
    if raw.opening_hours:
        parts.append(f"Opening hours: {raw.opening_hours}")
    if raw.extra_data:
        useful_keys = {
            "stars", "rooms", "wheelchair", "internet_access",
            "outdoor_seating", "takeaway", "delivery", "smoking",
            "air_conditioning", "payment:cash", "payment:cards",
        }
        useful = {k: v for k, v in raw.extra_data.items() if k in useful_keys}
        if useful:
            parts.append(f"Extra attributes: {useful}")

    return (
        "Classify this hospitality establishment from Florianópolis, SC, Brazil:\n\n"
        + "\n".join(parts)
        + "\n\nReturn a JSON object matching the required schema."
    )


@with_retry(
    max_attempts=3,
    base_delay=2.0,
    retryable_exceptions=(ConnectionError, TimeoutError, OSError),
)
def _classify_with_llm(
    raw: RawEstablishment,
    ollama_url: str,
    ollama_model: str,
) -> ClassifiedEstablishment:
    """
    Classify a raw establishment using Ollama with structured JSON output.

    Passes ``ClassifiedEstablishment.model_json_schema()`` as the ``format``
    parameter, which forces the Ollama model to emit a JSON object that
    conforms to the schema on every call.

    Args:
        raw: Validated RawEstablishment to classify.
        ollama_url: Ollama server base URL (e.g. ``http://localhost:11434``).
        ollama_model: Model identifier (e.g. ``llama3.1:8b``).

    Returns:
        Validated ClassifiedEstablishment instance.

    Raises:
        ConnectionError: On Ollama network failures (triggers with_retry backoff).
        ValueError: On malformed LLM JSON (does not trigger retry).
    """
    client = ollama.Client(host=ollama_url)
    prompt = _build_prompt(raw)

    try:
        response = client.chat(
            model=ollama_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            format=ClassifiedEstablishment.model_json_schema(),
            options={"temperature": _LLM_TEMPERATURE},
        )
    except Exception as exc:
        msg = str(exc).lower()
        if any(kw in msg for kw in ("connection", "refused", "timeout", "network")):
            raise ConnectionError(f"Ollama connection failed: {exc}") from exc
        raise ConnectionError(f"Ollama API error: {exc}") from exc

    return ClassifiedEstablishment.model_validate_json(response.message.content)


def _is_duplicate(
    client: Any,
    name: str,
    lat: float,
    lon: float,
) -> str | None:
    """
    Check for a fuzzy-duplicate establishment in ``dim_estabelecimentos``.

    Delegates to the ``check_establishment_duplicate`` PostgreSQL function
    (migration 008) which combines pg_trgm similarity > 0.8 with a 50 m
    PostGIS proximity filter.

    If the RPC call fails (e.g., the function is not yet deployed), the error
    is logged as a warning and ``None`` is returned — the pipeline continues
    without dedup protection rather than crashing.

    Args:
        client: Supabase client instance.
        name: Standardized establishment name to check.
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        UUID string of an existing duplicate record, or ``None``.
    """
    try:
        result = client.rpc(
            "check_establishment_duplicate",
            {"p_nome": name, "p_lat": lat, "p_lon": lon},
        ).execute()
        return result.data if result.data else None
    except Exception as exc:
        logger.warning("dedup_check_failed", name=name, error=str(exc))
        return None


def _build_dim_row(
    raw: RawEstablishment,
    classified: ClassifiedEstablishment,
) -> dict[str, Any]:
    """
    Merge raw extraction data and LLM classification into a ``dim_estabelecimentos`` row.

    The PostGIS ``location`` column accepts WKT strings in the format
    ``SRID=4326;POINT(lon lat)``; Postgres casts this to
    ``geography(Point, 4326)`` on insert.

    Cuisine is converted from the OSM semicolon-separated string
    (e.g. ``"italian;seafood"``) to a PostgreSQL text array.

    Args:
        raw: Validated RawEstablishment from the extraction phase.
        classified: Validated ClassifiedEstablishment from the LLM phase.

    Returns:
        Dictionary ready for ``client.table("dim_estabelecimentos").insert()``.
    """
    source_refs: dict[str, str] = {}
    if raw.source_id:
        source_refs["osm_id" if raw.source == "overpass" else raw.source] = raw.source_id

    cuisine_list: list[str] | None = None
    if raw.cuisine:
        cuisine_list = [c.strip() for c in raw.cuisine.split(";") if c.strip()]

    return {
        "nome": classified.standardized_name,
        "nome_original": raw.name,
        "tipo": classified.type.value,
        "subtipo": classified.subtype,
        "cnae_codigo": classified.suggested_cnae,
        "endereco": raw.address,
        "bairro": classified.estimated_neighborhood,
        "location": f"SRID=4326;POINT({raw.longitude} {raw.latitude})",
        "telefone": raw.phone,
        "website": raw.website,
        "horario_funcionamento": raw.opening_hours,
        "rating_google": raw.rating,
        "total_reviews": raw.review_count,
        "cuisine": cuisine_list,
        "tags_llm": {
            "tags": classified.tags,
            "confidence": classified.confidence,
            "subtype": classified.subtype,
        },
        "source_refs": source_refs,
        "ativo": True,
    }


def _insert_dim_record(client: Any, row: dict[str, Any]) -> int:
    """
    Insert one record into ``dim_estabelecimentos``.

    Args:
        client: Supabase client instance.
        row: Dict produced by ``_build_dim_row``.

    Returns:
        1 if the row was inserted, 0 otherwise.
    """
    result = client.table("dim_estabelecimentos").insert(row).execute()
    return len(result.data) if result.data else 0


def _mark_processed(client: Any, record_ids: list[str]) -> None:
    """
    Set ``processed = true`` on raw_crawled_data records.

    Called after classification (whether the record was inserted, skipped as
    a duplicate, or skipped due to missing coordinates). Idempotent.

    Args:
        client: Supabase client instance.
        record_ids: List of ``raw_crawled_data`` UUID strings to mark.
    """
    if not record_ids:
        return
    (
        client.table("raw_crawled_data")
        .update({"processed": True})
        .in_("id", record_ids)
        .execute()
    )


def _log_pipeline(
    batch_id: str,
    status: str,
    records_processed: int = 0,
    error_message: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    """Write a pipeline execution record to data_pipeline_logs. Failures are swallowed."""
    try:
        client = get_supabase_client()
        row: dict[str, Any] = {
            "pipeline_name": "llm_classifier",
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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_classification(batch_size: int = _BATCH_SIZE) -> int:
    """
    Run one batch of the LLM classification pipeline.

    Processing loop for each raw record:

    1. Validate the JSONB payload into a ``RawEstablishment``.
    2. Skip records with no coordinates (mark as processed).
    3. Call ``check_establishment_duplicate`` RPC — skip if found.
    4. Classify via Ollama (``@with_retry`` for transient errors).
    5. Insert classified record into ``dim_estabelecimentos``.
    6. Collect the record ID for bulk ``processed = true`` update.

    LLM errors are logged and the record is **not** marked as processed,
    so it will be retried in the next batch run.

    Args:
        batch_size: Maximum number of raw records to process per run.

    Returns:
        Number of records successfully classified and inserted.
    """
    settings = get_settings()
    client = get_supabase_client()
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(tz=timezone.utc)

    _log_pipeline(batch_id=batch_id, status="running", started_at=started_at)
    logger.info(
        "classification_started",
        pipeline="llm_classifier",
        batch_size=batch_size,
        model=settings.OLLAMA_MODEL,
    )

    # --- Fetch unprocessed records ---
    raw_rows: list[dict[str, Any]] = (
        client.table("raw_crawled_data")
        .select("*")
        .eq("processed", False)
        .limit(batch_size)
        .execute()
    ).data or []

    if not raw_rows:
        finished_at = datetime.now(tz=timezone.utc)
        logger.info("no_unprocessed_records", pipeline="llm_classifier")
        _log_pipeline(
            batch_id=batch_id,
            status="skipped",
            records_processed=0,
            started_at=started_at,
            finished_at=finished_at,
        )
        return 0

    logger.info("raw_records_fetched", count=len(raw_rows))

    classified_count = 0
    processed_ids: list[str] = []

    try:
        for row in raw_rows:
            record_id: str = row["id"]

            # Step 1 — Validate payload
            try:
                raw = RawEstablishment.model_validate(row["payload"])
            except Exception as exc:
                logger.warning("raw_validation_failed", record_id=record_id, error=str(exc))
                processed_ids.append(record_id)
                continue

            if raw.latitude is None or raw.longitude is None:
                logger.warning("skipping_no_coordinates", record_id=record_id)
                processed_ids.append(record_id)
                continue

            # Step 2 — Fuzzy dedup
            duplicate_id = _is_duplicate(
                client, raw.name or "", raw.latitude, raw.longitude
            )
            if duplicate_id:
                logger.info(
                    "duplicate_skipped",
                    record_id=record_id,
                    duplicate_dim_id=duplicate_id,
                )
                processed_ids.append(record_id)
                continue

            # Step 3 — LLM classification
            try:
                classified = _classify_with_llm(
                    raw, settings.OLLAMA_URL, settings.OLLAMA_MODEL
                )
            except Exception as exc:
                logger.error(
                    "classification_failed",
                    record_id=record_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                # Do NOT mark as processed — will retry in next batch
                continue

            logger.info(
                "record_classified",
                record_id=record_id,
                name=classified.standardized_name,
                type=classified.type.value,
                confidence=classified.confidence,
            )

            # Step 4 — Insert into dim_estabelecimentos
            try:
                dim_row = _build_dim_row(raw, classified)
                _insert_dim_record(client, dim_row)
                classified_count += 1
            except Exception as exc:
                logger.error("dim_insert_failed", record_id=record_id, error=str(exc))
                continue

            processed_ids.append(record_id)

        # Step 5 — Bulk mark as processed
        _mark_processed(client, processed_ids)
        logger.info("records_marked_processed", count=len(processed_ids))

    except Exception as exc:
        finished_at = datetime.now(tz=timezone.utc)
        logger.error("pipeline_failed", error=str(exc), error_type=type(exc).__name__)
        _log_pipeline(
            batch_id=batch_id,
            status="error",
            records_processed=classified_count,
            error_message=str(exc),
            started_at=started_at,
            finished_at=finished_at,
        )
        raise

    finished_at = datetime.now(tz=timezone.utc)
    _log_pipeline(
        batch_id=batch_id,
        status="success",
        records_processed=classified_count,
        started_at=started_at,
        finished_at=finished_at,
    )
    logger.info(
        "pipeline_complete",
        pipeline="llm_classifier",
        total_classified=classified_count,
        total_processed=len(processed_ids),
        duration_ms=int((finished_at - started_at).total_seconds() * 1000),
    )
    return classified_count


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run LLM classification pipeline")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_BATCH_SIZE,
        help=f"Records per run (default: {_BATCH_SIZE})",
    )
    args = parser.parse_args()
    total = run_classification(batch_size=args.batch_size)
    print(f"Classified {total} establishments.")
