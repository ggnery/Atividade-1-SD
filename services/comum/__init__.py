"""Codigo de dominio compartilhado pelos servicos clinicos do Hospital Inteligente (G3).

Este pacote reune as regras de negocio puras -- sem dependencia de framework, banco ou rede --
que ``admission-service``, ``vitals-service``, ``triage-service`` e ``api-gateway`` importam.
Hoje contem a regra clinica NEWS2 (design 7.5/7.6); a fronteira publica esta reexportada abaixo
para que os importadores facam ``from services.comum import calcular_news2, SinaisVitais``.
"""

from __future__ import annotations

from services.comum.news2 import (
    FAIXAS_FISIOLOGICAS,
    NIVEIS_CONSCIENCIA,
    FaixaFisiologica,
    NivelConsciencia,
    ScoreNEWS2,
    Severidade,
    SinaisVitais,
    calcular_news2,
    classificar,
    classificar_severidade,
    dentro_das_faixas_fisiologicas,
    detectar_componente_critico,
    validar_faixas_fisiologicas,
)

__all__ = [
    "FAIXAS_FISIOLOGICAS",
    "NIVEIS_CONSCIENCIA",
    "FaixaFisiologica",
    "NivelConsciencia",
    "ScoreNEWS2",
    "Severidade",
    "SinaisVitais",
    "calcular_news2",
    "classificar",
    "classificar_severidade",
    "dentro_das_faixas_fisiologicas",
    "detectar_componente_critico",
    "validar_faixas_fisiologicas",
]
