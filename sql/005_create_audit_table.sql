-- ============================================================
-- Migration 005: Create Pipeline Audit Table
-- ============================================================
-- Tracks every pipeline execution for observability and debugging.
-- Enables monitoring of success rates, execution times, and error patterns.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.data_pipeline_logs (
    id                      uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    
    -- Pipeline identification
    pipeline_name           text NOT NULL,       -- 'ingest_ibge', 'transform_clean', etc.
    batch_id                uuid,                -- Links to raw_crawled_data.batch_id
    
    -- Execution status
    status                  text NOT NULL DEFAULT 'running',
    records_processed       integer DEFAULT 0,
    error_message           text,
    
    -- Performance
    duration_ms             integer,
    
    -- CI/CD reference
    github_run_id           text,                -- GitHub Actions run ID for traceability
    github_workflow         text,                -- Workflow filename
    
    -- Timestamps
    started_at              timestamptz NOT NULL DEFAULT now(),
    finished_at             timestamptz,
    
    -- Constraints
    CONSTRAINT valid_status CHECK (status IN ('running', 'success', 'error', 'skipped'))
);

COMMENT ON TABLE public.data_pipeline_logs IS
    'Audit trail for all pipeline executions. Each row = one pipeline run with status and metrics.';
