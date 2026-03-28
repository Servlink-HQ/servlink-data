"""
Macroeconomic Indicator Models
==============================
Pydantic schemas for socioeconomic indicators extracted from IBGE/SIDRA,
CAGED/RAIS, and SANTUR. These represent the fact_indicadores_macro table.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class IndicatorSource(str, Enum):
    """Data source for macroeconomic indicators."""

    SIDRA = "sidra"
    CAGED = "caged"
    RAIS = "rais"
    SANTUR = "santur"
    DADOS_SC = "dados_sc"


class IndicatorName(str, Enum):
    """Standardized names for tracked indicators."""

    GDP_MUNICIPAL = "pib_municipal"
    FORMAL_EMPLOYMENT = "emprego_formal"
    POPULATION = "populacao"
    ADMISSIONS = "admissoes"
    DISMISSALS = "desligamentos"
    AVERAGE_SALARY = "salario_medio"
    TOURISM_REVENUE = "receita_turismo"
    HOTEL_OCCUPANCY = "taxa_ocupacao_hoteleira"
    REGISTERED_BUSINESSES = "estabelecimentos_cadastrados"
    TOURIST_ARRIVALS = "chegadas_turistas"


class FactIndicatorMacro(BaseModel):
    """
    Schema for the fact_indicadores_macro table.

    Each record represents a single data point in a time series:
    one indicator, for one period, from one source.
    """

    indicator: IndicatorName = Field(
        ...,
        description="Standardized indicator name",
    )
    source: IndicatorSource = Field(
        ...,
        description="Data source that produced this value",
    )
    municipality_code: str = Field(
        default="4205407",
        description="IBGE municipality code (Florianopolis = 4205407)",
    )
    cnae_division: Optional[str] = Field(
        default=None,
        description="CNAE division filter (55=Accommodation, 56=Food Services)",
    )
    period: str = Field(
        ...,
        description="Time period in flexible format: '2024-Q1', '2025-01', '2025'",
    )
    value: float = Field(
        ...,
        description="Numeric value of the indicator",
    )
    unit: str = Field(
        ...,
        description="Unit of measurement: 'BRL', 'persons', 'percentage', 'count'",
    )
    metadata: Optional[dict[str, object]] = Field(
        default=None,
        description="Additional context from the source (table number, variable ID, etc.)",
    )
    collected_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp of when this data point was collected by our pipeline",
    )
