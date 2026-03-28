-- ============================================================
-- Migration 001: Enable Required Extensions
-- ============================================================
-- Activates PostGIS for geospatial queries, pg_jsonschema for
-- JSONB validation, and pg_trgm for fuzzy text matching.
--
-- Run order: FIRST (before any table creation)
-- ============================================================

-- PostGIS: Spatial types (geography, geometry) and functions
-- Required for dim_estabelecimentos.location column
CREATE EXTENSION IF NOT EXISTS postgis
    SCHEMA extensions;

-- pg_jsonschema: JSON Schema validation inside CHECK constraints
-- Required for validating raw_crawled_data.payload structure
CREATE EXTENSION IF NOT EXISTS pg_jsonschema
    SCHEMA extensions;

-- pg_trgm: Trigram-based text similarity for fuzzy name matching
-- Required for deduplication of establishment names
CREATE EXTENSION IF NOT EXISTS pg_trgm
    SCHEMA extensions;
