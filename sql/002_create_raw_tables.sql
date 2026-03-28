-- ============================================================
-- Migration 002: Create Raw Landing Zone Table
-- ============================================================
-- raw_crawled_data is the "landing zone" where all extraction
-- scripts dump their payloads in flexible JSONB format.
--
-- Design: Schema-on-read. No rigid column structure imposed on
-- incoming data. Validation happens at the Pydantic model layer
-- and via pg_jsonschema CHECK constraints for minimum fields.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.raw_crawled_data (
    id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    
    -- Source identification
    source          text NOT NULL,           -- 'overpass', 'outscraper', 'caged', 'santur', 'serp'
    source_id       text,                    -- External unique ID (OSM node_id, place_id, etc.)
    
    -- The raw payload (schema-on-read)
    payload         jsonb NOT NULL DEFAULT '{}'::jsonb,
    
    -- Pipeline metadata
    crawled_at      timestamptz NOT NULL DEFAULT now(),
    processed       boolean NOT NULL DEFAULT false,
    batch_id        uuid,                    -- Groups records from the same pipeline run
    
    -- Constraints
    CONSTRAINT valid_source CHECK (
        source IN ('overpass', 'outscraper', 'caged', 'santur', 'serp', 'rais', 'ibge', 'dados_sc')
    )
);

-- Add descriptive comment
COMMENT ON TABLE public.raw_crawled_data IS
    'Landing zone for all extracted data. JSONB payloads are validated at the application layer.';
COMMENT ON COLUMN public.raw_crawled_data.payload IS
    'Flexible JSONB payload -- structure varies by source. Validated via Pydantic before insert.';
COMMENT ON COLUMN public.raw_crawled_data.processed IS
    'Set to true after the transform pipeline has cleaned and loaded this record into dim/fact tables.';
