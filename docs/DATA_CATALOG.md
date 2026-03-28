# Data Catalog - ServLink Data Engine

> **Data Governance Document** - Defines every data entity, its source, schema, refresh cadence,
> and quality rules. This is the single source of truth for what data enters the system.

---

## 1. Data Sources Registry

| ID | Source Name | Type | Protocol | Refresh Cadence | Cost | Status |
|---|---|---|---|---|---|---|
| `SRC-01` | IBGE/SIDRA | Government API | HTTP REST | Monthly (day 15) | Free | Active |
| `SRC-02` | OpenStreetMap (Overpass API) | Open Data | HTTP REST | Weekly (Sunday) | Free | Active |
| `SRC-03` | SearXNG | Self-hosted metasearch | HTTP REST | Weekly (Tuesday) | Free (VPS cost) | Planned |
| `SRC-04` | Novo CAGED/RAIS | Government FTP | FTP/SFTP | Monthly (day 20) | Free | Planned |
| `SRC-05` | SANTUR/Dados SC | Government Portal | HTTP REST (CKAN) | Biweekly | Free | Planned |
| `SRC-06` | Outscraper (Google Places) | Commercial API | HTTP REST | Monthly (day 1) | Paid (future) | Future |
| `SRC-07` | SerpBear | Self-hosted SEO tracker | HTTP REST | Weekly | Free (VPS cost) | Future |

---

## 2. Data Entities

### 2.1 `raw_crawled_data` - Landing Zone

| Attribute | Description |
|---|---|
| **Purpose** | Schema-on-read landing zone for all extraction pipelines |
| **Owner** | Data Engine (service_role) |
| **Access** | Pipeline only (no public/anon access) |
| **Retention** | Indefinite (processed records archived after 90 days) |
| **PII Level** | None (no personal data stored - business data only) |

**Schema:**

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `id` | uuid | No | `gen_random_uuid()` | Primary key |
| `source` | text | No | - | Source identifier: `overpass`, `outscraper`, `caged`, `santur`, `serp`, `rais`, `ibge`, `dados_sc` |
| `source_id` | text | Yes | - | External ID from the source system |
| `payload` | jsonb | No | `'{}'` | Raw data payload (structure varies by source) |
| `crawled_at` | timestamptz | No | `now()` | Timestamp of extraction |
| `processed` | boolean | No | `false` | Flag for transform pipeline consumption |
| `batch_id` | uuid | Yes | - | Groups records from the same pipeline execution |

**Quality Rules:**
- `source` must be one of the predefined enum values
- `payload` must not be empty (`payload != '{}'`)
- `crawled_at` must not be in the future
- Duplicates detected via `source + source_id` combination

---

### 2.2 `dim_estabelecimentos` - Establishment Dimension

| Attribute | Description |
|---|---|
| **Purpose** | Clean, validated registry of hospitality establishments in Florianopolis |
| **Owner** | Transform pipeline (LLM classifier) |
| **Access** | Public READ (dashboard), Pipeline WRITE |
| **Retention** | Permanent (soft-delete via `ativo = false`) |
| **PII Level** | Low (business contact info only, no personal data) |
| **Spatial** | PostGIS `geography(Point, 4326)` with GiST index |

**Schema:**

| Column | Type | Nullable | Description |
|---|---|---|---|
| `id` | uuid | No | Primary key |
| `nome` | text | No | Standardized establishment name |
| `nome_original` | text | Yes | Original name before cleaning |
| `tipo` | establishment_type | No | Enum: hotel, hostel, pousada, guest_house, restaurant, cafe, bar, pub, bakery, other |
| `subtipo` | text | Yes | LLM-classified subtype (e.g., `fine_dining`, `boutique_hotel`) |
| `cnae_codigo` | text | Yes | CNAE subclass code (format: `XXXX-X/XX`) |
| `endereco` | text | Yes | Formatted street address |
| `bairro` | text | Yes | Neighborhood in Florianopolis |
| `location` | geography(Point, 4326) | Yes | PostGIS point - WGS84 coordinates |
| `telefone` | text | Yes | Normalized Brazilian phone `(XX) XXXXX-XXXX` |
| `website` | text | Yes | Official website URL |
| `horario_funcionamento` | text | Yes | Opening hours (OSM format normalized) |
| `rating_google` | numeric(2,1) | Yes | Google rating 0.0-5.0 |
| `total_reviews` | integer | Yes | Total review count |
| `cuisine` | text[] | Yes | Array of cuisine types |
| `tags_llm` | jsonb | Yes | LLM-generated tags and attributes |
| `source_refs` | jsonb | Yes | Cross-references: `{osm_id, place_id, cadastur}` |
| `ativo` | boolean | No | Active status (soft-delete) |
| `created_at` | timestamptz | No | First insertion timestamp |
| `updated_at` | timestamptz | No | Last update (auto-trigger) |

**Quality Rules:**
- `nome` must not be empty or whitespace-only
- `tipo` must be a valid establishment_type enum value
- `location` coordinates must fall within Florianopolis bounding box (lat: -27.85 to -27.38, lon: -48.65 to -48.33)
- `rating_google` must be between 0.0 and 5.0
- `cnae_codigo` must match pattern `^\d{4}-\d/\d{2}$`
- Deduplication via fuzzy name matching (pg_trgm similarity > 0.8) + location proximity (< 50m)

---

### 2.3 `fact_indicadores_macro` - Macroeconomic Indicators

| Attribute | Description |
|---|---|
| **Purpose** | Time-series socioeconomic data for Florianopolis hospitality |
| **Owner** | IBGE/CAGED/SANTUR extractors |
| **Access** | Public READ (dashboard), Pipeline WRITE |
| **Retention** | Permanent (historical data is never deleted) |
| **Grain** | One row = one indicator + one period + one source |

**Schema:**

| Column | Type | Nullable | Description |
|---|---|---|---|
| `id` | uuid | No | Primary key |
| `indicador` | text | No | Indicator name (standardized via IndicatorName enum) |
| `fonte` | text | No | Data source identifier |
| `municipio_cod` | text | No | IBGE municipal code (default: `4205407`) |
| `cnae_divisao` | text | Yes | CNAE division filter (`55`, `56`) |
| `periodo` | text | No | Time period: `2024-Q1`, `2025-01`, `2025` |
| `valor` | numeric | No | Indicator value |
| `unidade` | text | No | Unit: `BRL`, `persons`, `percentage`, `count` |
| `metadata` | jsonb | Yes | Source-specific metadata |
| `collected_at` | timestamptz | No | Pipeline collection timestamp |

**Tracked Indicators:**

| Indicator | Source | Unit | Cadence |
|---|---|---|---|
| `pib_municipal` | SIDRA | BRL | Annual |
| `emprego_formal` | SIDRA/CAGED | persons | Monthly |
| `populacao` | SIDRA | persons | Decennial (est. yearly) |
| `admissoes` | CAGED | persons | Monthly |
| `desligamentos` | CAGED | persons | Monthly |
| `salario_medio` | CAGED/RAIS | BRL | Monthly |
| `receita_turismo` | SANTUR | BRL | Quarterly |
| `taxa_ocupacao_hoteleira` | SANTUR | percentage | Monthly |
| `estabelecimentos_cadastrados` | SANTUR | count | Quarterly |
| `chegadas_turistas` | SANTUR | count | Monthly |

**Quality Rules:**
- UNIQUE constraint on `(indicador, fonte, municipio_cod, cnae_divisao, periodo)` prevents duplicates
- `valor` must be numeric (no null values)
- `periodo` format validated at application layer via Pydantic

---

### 2.4 `fact_serp_keywords` - SERP Rankings

| Attribute | Description |
|---|---|
| **Purpose** | Search engine keyword position tracking for competitive intelligence |
| **Owner** | SERP extractor (SearXNG / SerpBear) |
| **Access** | Pipeline only (competitive data - no public access) |
| **Retention** | Permanent |

**Quality Rules:**
- `position` must be between 1 and 100
- `keyword` must not be empty
- Time-series consistency: at least one data point per keyword per check cycle

---

### 2.5 `data_pipeline_logs` - Audit Trail

| Attribute | Description |
|---|---|
| **Purpose** | Full observability of every pipeline execution |
| **Owner** | All pipeline modules |
| **Access** | Pipeline only (internal audit) |
| **Retention** | 180 days rolling |

**Quality Rules:**
- Every pipeline run MUST create a log entry (start + finish)
- `status` must be one of: `running`, `success`, `error`, `skipped`
- `github_run_id` links to the exact GitHub Actions execution

---

## 3. Data Flow Diagram

```
Source APIs --> Extractors --> raw_crawled_data (JSONB)
                                     |
                               Transform Pipeline
                                     |
                        +------------+------------+
                        v            v            v
              dim_estabelecimentos  fact_*     pipeline_logs
              (PostGIS + Relational) (Time Series) (Audit)
                        |            |
                        v            v
                   servlink-hub Dashboard (SELECT only)
```

## 4. IBGE Reference Codes

| Code | Description | Usage |
|---|---|---|
| `4205407` | Municipality of Florianopolis | Geographic filter for all SIDRA queries |
| `55` | CNAE Division - Accommodation | Hotels, hostels, pousadas |
| `56` | CNAE Division - Food Services | Restaurants, cafes, bars, catering |
| `5510-8/01` | CNAE Subclass - Hotels | Primary hotel classification |
| `5590-6/01` | CNAE Subclass - Other lodging | Hostels, pousadas |
| `5620-1/04` | CNAE Subclass - Prepared food delivery | Food delivery services |

## 5. Geospatial Constants

| Parameter | Value | Description |
|---|---|---|
| SRID | `4326` | WGS84 coordinate reference system |
| Bounding Box (NE) | `-27.38, -48.33` | Northeast corner of Florianopolis |
| Bounding Box (SW) | `-27.85, -48.65` | Southwest corner of Florianopolis |
| Island Centroid | `-27.5969, -48.5495` | Approximate center of the island |
