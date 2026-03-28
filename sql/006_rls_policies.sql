-- ============================================================
-- Migration 006: Row Level Security Policies
-- ============================================================
-- Security model:
--   - Pipeline (service_role): Full CRUD access, bypasses RLS
--   - Frontend (anon key): Read-only on dim/fact tables only
--   - No public access to raw data or audit logs
--
-- The service_role key is used by GitHub Actions and the Python
-- pipeline. It automatically bypasses RLS, so INSERT/UPDATE
-- operations work without explicit policies.
-- ============================================================

-- --------------------------------
-- Enable RLS on all new tables
-- --------------------------------
ALTER TABLE public.raw_crawled_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.dim_estabelecimentos ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.fact_indicadores_macro ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.fact_serp_keywords ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.data_pipeline_logs ENABLE ROW LEVEL SECURITY;

-- --------------------------------
-- raw_crawled_data: NO public access (pipeline only)
-- --------------------------------
-- No SELECT policy for anon role = completely locked down
-- service_role bypasses RLS automatically

-- --------------------------------
-- dim_estabelecimentos: Public READ for dashboard
-- --------------------------------
CREATE POLICY "anon_select_dim_estabelecimentos"
    ON public.dim_estabelecimentos
    FOR SELECT
    TO anon
    USING (true);

-- --------------------------------
-- fact_indicadores_macro: Public READ for dashboard
-- --------------------------------
CREATE POLICY "anon_select_fact_indicadores"
    ON public.fact_indicadores_macro
    FOR SELECT
    TO anon
    USING (true);

-- --------------------------------
-- fact_serp_keywords: NO public access (competitive intelligence)
-- --------------------------------
-- No SELECT policy for anon role

-- --------------------------------
-- data_pipeline_logs: NO public access (internal audit only)
-- --------------------------------
-- No SELECT policy for anon role
