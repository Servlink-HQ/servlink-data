-- ============================================================
-- Migration 007: Performance Indexes
-- ============================================================
-- Optimized indexes for the query patterns used by the pipeline
-- (INSERT/SELECT on processed flag) and the dashboard (spatial
-- queries, time-series lookups).
-- ============================================================

-- --------------------------------
-- raw_crawled_data
-- --------------------------------
-- Pipeline query: SELECT * FROM raw_crawled_data WHERE processed = false AND source = ?
CREATE INDEX IF NOT EXISTS idx_raw_source_processed
    ON public.raw_crawled_data (source, processed)
    WHERE processed = false;

-- GIN index for JSONB containment queries on payload
CREATE INDEX IF NOT EXISTS idx_raw_payload_gin
    ON public.raw_crawled_data
    USING gin (payload jsonb_path_ops);

-- Batch grouping
CREATE INDEX IF NOT EXISTS idx_raw_batch_id
    ON public.raw_crawled_data (batch_id)
    WHERE batch_id IS NOT NULL;

-- --------------------------------
-- dim_estabelecimentos
-- --------------------------------
-- Spatial index for PostGIS proximity queries (ST_DWithin, ST_Distance)
CREATE INDEX IF NOT EXISTS idx_dim_location_gist
    ON public.dim_estabelecimentos
    USING gist (location);

-- Type filtering (dashboard filter by hotel/restaurant/etc.)
CREATE INDEX IF NOT EXISTS idx_dim_tipo
    ON public.dim_estabelecimentos (tipo)
    WHERE ativo = true;

-- GIN index for LLM tags (JSONB containment queries)
CREATE INDEX IF NOT EXISTS idx_dim_tags_gin
    ON public.dim_estabelecimentos
    USING gin (tags_llm jsonb_path_ops);

-- Fuzzy name search using trigram similarity
CREATE INDEX IF NOT EXISTS idx_dim_nome_trgm
    ON public.dim_estabelecimentos
    USING gist (nome extensions.gist_trgm_ops);

-- --------------------------------
-- fact_indicadores_macro
-- --------------------------------
-- Time-series queries: WHERE indicador = ? AND periodo BETWEEN ? AND ?
CREATE INDEX IF NOT EXISTS idx_fact_indicador_periodo
    ON public.fact_indicadores_macro (indicador, periodo);

-- --------------------------------
-- fact_serp_keywords
-- --------------------------------
-- Historical tracking: WHERE keyword = ? ORDER BY checked_at DESC
CREATE INDEX IF NOT EXISTS idx_serp_keyword_time
    ON public.fact_serp_keywords (keyword, checked_at DESC);

-- --------------------------------
-- data_pipeline_logs
-- --------------------------------
-- Recent execution lookup
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_recent
    ON public.data_pipeline_logs (pipeline_name, started_at DESC);
