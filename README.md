# ServLink Data Engine

> The invisible motor that powers the ServLink BI Hub with continuous data extraction, transformation, and loading pipelines for the hospitality sector in Florianopolis, SC - Brazil.

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org)
[![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-green)](https://supabase.com)
[![License](https://img.shields.io/badge/License-Private-red)]()

---

## Architecture

```
GitHub Actions (Cron) --> Python Extractors --> Supabase PostgreSQL
                              |                       |
                         Transform (LLM)         servlink-hub
                              |                  (Dashboard)
                         dim/fact tables ---------------+
```

**Repositories:**
- **servlink-data** (this repo): Data engineering - extraction, transformation, loading
- **[servlink-hub](https://github.com/Servlink-HQ/servlink-hub)**: Frontend - React dashboard that consumes the data

## Data Sources

| Source | Type | Protocol | Cadence |
|---|---|---|---|
| IBGE/SIDRA | Government API | REST | Monthly |
| OpenStreetMap (Overpass API) | Open Data | REST | Weekly |
| SearXNG | Self-hosted metasearch | REST | Weekly |
| CAGED/RAIS | Government FTP | FTP | Monthly |
| SANTUR/Dados SC | Government Portal | REST/CKAN | Biweekly |

## Project Structure

```
src/
├── config/          # Settings, Supabase connection
├── extractors/      # One module per data source
├── transformers/    # Cleaning, LLM classification
├── loaders/         # Database persistence
├── models/          # Pydantic domain schemas
└── utils/           # Logging, retry, validators
sql/                 # PostgreSQL migrations
tests/               # Unit and integration tests
docs/                # Data Catalog, architecture docs
```

## Quick Start

```bash
# 1. Clone
git clone https://github.com/Servlink-HQ/servlink-data.git
cd servlink-data

# 2. Virtual environment
python -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your Supabase credentials

# 5. Test connection
python -m src.config.supabase_client
```

## Data Governance

See [docs/DATA_CATALOG.md](docs/DATA_CATALOG.md) for the complete data catalog including:
- Data source registry
- Entity schemas and quality rules
- IBGE reference codes
- Geospatial constants

## Tech Stack

- **Language**: Python 3.12
- **Data Validation**: Pydantic v2
- **Database**: Supabase (PostgreSQL + PostGIS)
- **Orchestration**: GitHub Actions (cron)
- **Logging**: structlog (JSON in CI, colored in dev)
- **Testing**: pytest
- **Quality**: ruff (lint) + mypy (types)
