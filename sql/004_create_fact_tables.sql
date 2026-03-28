-- ============================================================
-- Migration 004: Create Fact Tables (Indicators + SERP)
-- ============================================================
-- Star schema fact tables for time-series analytics.
--
-- fact_indicadores_macro: Socioeconomic indicators from IBGE, CAGED, RAIS
-- fact_serp_keywords: Search engine ranking tracking
-- ============================================================

-- --------------------------------
-- Fact: Macroeconomic Indicators
-- --------------------------------
CREATE TABLE IF NOT EXISTS public.fact_indicadores_macro (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    
    -- Indicator identification
    indicador           text NOT NULL,           -- 'pib_municipal', 'emprego_formal', 'populacao', etc.
    fonte               text NOT NULL,           -- 'sidra', 'caged', 'rais', 'santur'
    
    -- Geographic scope
    municipio_cod       text NOT NULL DEFAULT '4205407',  -- Florianopolis IBGE code
    cnae_divisao        text,                    -- CNAE division filter ('55', '56')
    
    -- Time series
    periodo             text NOT NULL,           -- Flexible: '2024-Q1', '2025-01', '2025'
    
    -- Value
    valor               numeric NOT NULL,
    unidade             text NOT NULL,           -- 'BRL', 'persons', 'percentage', 'count'
    
    -- Metadata
    metadata            jsonb DEFAULT '{}'::jsonb,
    collected_at        timestamptz NOT NULL DEFAULT now(),
    
    -- Prevent duplicate data points
    CONSTRAINT uq_indicator_period UNIQUE (indicador, fonte, municipio_cod, cnae_divisao, periodo)
);

COMMENT ON TABLE public.fact_indicadores_macro IS
    'Time-series fact table for socioeconomic indicators. Each row = one data point for one period.';

-- --------------------------------
-- Fact: SERP Keyword Rankings
-- --------------------------------
CREATE TABLE IF NOT EXISTS public.fact_serp_keywords (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    
    -- Keyword tracking
    keyword             text NOT NULL,
    domain              text,                    -- Domain that ranks for this keyword
    position            integer,                 -- Ranking position (1 = top)
    
    -- Search context
    search_engine       text NOT NULL DEFAULT 'google',
    location            text NOT NULL DEFAULT 'Florianopolis, SC, Brazil',
    
    -- Time
    checked_at          timestamptz NOT NULL DEFAULT now(),
    
    -- Rich metadata (snippet, URL, featured snippet flag, etc.)
    metadata            jsonb DEFAULT '{}'::jsonb,
    
    -- Constraints
    CONSTRAINT valid_position CHECK (position IS NULL OR (position >= 1 AND position <= 100))
);

COMMENT ON TABLE public.fact_serp_keywords IS
    'SERP ranking history. Tracks keyword positions over time for competitive intelligence.';
