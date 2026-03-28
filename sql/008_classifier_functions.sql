-- ============================================================
-- Migration 008: LLM Classifier Support Functions
-- ============================================================
-- PostgreSQL helper functions used by the LLM classification
-- pipeline (src/transformers/llm_classifier.py).
--
-- Requires: pg_trgm (migration 001), PostGIS (migration 001),
--           dim_estabelecimentos (migration 003).
-- ============================================================

-- ----------------------------------------------------------------
-- check_establishment_duplicate
-- ----------------------------------------------------------------
-- Returns the UUID of an existing active establishment that matches
-- the given name (pg_trgm similarity > threshold) AND is within
-- the given distance in meters (PostGIS ST_DWithin).
--
-- Called by the classifier before every INSERT to avoid creating
-- duplicate dimension records from different extraction sources.
--
-- Args:
--   p_nome               Standardized establishment name to check
--   p_lat                Latitude in decimal degrees (WGS84)
--   p_lon                Longitude in decimal degrees (WGS84)
--   p_similarity_threshold  Minimum trigram similarity (default 0.8)
--   p_distance_meters    Maximum spatial distance in metres (default 50)
--
-- Returns:
--   UUID of the most similar active duplicate, or NULL if none found.
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION check_establishment_duplicate(
    p_nome                  text,
    p_lat                   double precision,
    p_lon                   double precision,
    p_similarity_threshold  float DEFAULT 0.8,
    p_distance_meters       float DEFAULT 50.0
)
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
    SELECT id
    FROM public.dim_estabelecimentos
    WHERE ativo = true
      AND extensions.similarity(nome, p_nome) > p_similarity_threshold
      AND ST_DWithin(
            location,
            ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::extensions.geography,
            p_distance_meters
          )
    ORDER BY extensions.similarity(nome, p_nome) DESC
    LIMIT 1;
$$;

COMMENT ON FUNCTION check_establishment_duplicate IS
    'Returns the UUID of an existing dim_estabelecimentos record that fuzzy-matches the given
     name (pg_trgm similarity > threshold) and is within p_distance_meters of the coordinates.
     Returns NULL when no duplicate is found. Used by the LLM classifier to prevent duplicate rows.';
