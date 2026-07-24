"""Testes de dominio testaveis sem PostgreSQL (R6.6, design 7.7).

Cobre a validacao das faixas fisiologicas de aceitacao -- a "ultima linha de defesa" antes do
calculo NEWS2 (design 7.7.1) -- e a totalidade da funcao ``calcular_news2`` fora do dominio da
tabela. Verifica ainda que os modelos SQLAlchemy espelham ``db/schema.sql`` no que da para conferir
sem banco (nome de schema, tabela e colunas), deixando os ``CHECK``/trigger para o e2e.
"""

from __future__ import annotations

import importlib
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from services.comum.news2 import (
    SinaisVitais,
    calcular_news2,
    dentro_das_faixas_fisiologicas,
    validar_faixas_fisiologicas,
)

_RAIZ = Path(__file__).resolve().parents[2]
_SERVICOS = _RAIZ / "services"
_TOP = ("modelos", "handler", "main", "notificacao", "repositorio", "outbox", "operacoes")


def _carregar(servico: str, modulo: str) -> object:
    """Importa um modulo de topo de um servico de diretorio hifenizado (design 12.1).

    Os servicos usam hifen no nome do diretorio e importam os irmaos pelo nome de topo
    (``from modelos import ...``); por isso o diretorio entra em ``sys.path`` e os nomes de topo
    colididos sao limpos antes de cada importacao.
    """
    for nome in list(sys.modules):
        if nome in _TOP:
            del sys.modules[nome]
    caminho = str(_SERVICOS / servico)
    if caminho in sys.path:
        sys.path.remove(caminho)
    sys.path.insert(0, caminho)
    return importlib.import_module(modulo)


def _saudavel(**override: object) -> SinaisVitais:
    """Leitura dentro de todas as faixas fisiologicas de aceitacao (design 7.7.1)."""
    campos: dict[str, object] = {
        "frequencia_respiratoria": 16,
        "saturacao_o2": 97,
        "oxigenio_suplementar": False,
        "temperatura": Decimal("36.5"),
        "pressao_sistolica": 120,
        "frequencia_cardiaca": 70,
        "nivel_consciencia": "A",
    }
    campos.update(override)
    return SinaisVitais(**campos)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# R6.6: sinal fora da faixa fisiologica e rejeitado antes de qualquer NEWS2     #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("campo", "valor"),
    [
        ("saturacao_o2", 20),  # < 50
        ("saturacao_o2", 150),  # > 100
        ("frequencia_respiratoria", 200),  # > 80
        ("pressao_sistolica", 10),  # < 40
        ("frequencia_cardiaca", 500),  # > 250
        ("temperatura", Decimal("10.0")),  # < 25.0
    ],
)
def test_valor_fora_da_faixa_fisiologica_e_rejeitado(campo: str, valor: object) -> None:
    """R6.6: ``validar_faixas_fisiologicas`` levanta ``ValueError`` citando o campo violado."""
    sinais = _saudavel(**{campo: valor})
    assert dentro_das_faixas_fisiologicas(sinais) is False
    with pytest.raises(ValueError, match=campo):
        validar_faixas_fisiologicas(sinais)


def test_nivel_consciencia_invalido_e_rejeitado() -> None:
    """Nivel de consciencia fora do dominio AVPU e rejeitado (design 7.7.1)."""
    sinais = _saudavel(nivel_consciencia="Z")
    assert dentro_das_faixas_fisiologicas(sinais) is False
    with pytest.raises(ValueError, match="nivel_consciencia"):
        validar_faixas_fisiologicas(sinais)


def test_leitura_saudavel_passa_pelas_faixas() -> None:
    """Uma leitura dentro de todas as faixas nao levanta e pontua NEWS2 normalmente."""
    sinais = _saudavel()
    assert dentro_das_faixas_fisiologicas(sinais) is True
    validar_faixas_fisiologicas(sinais)  # nao levanta
    assert calcular_news2(sinais).total == 0


def test_calcular_news2_e_funcao_total_levanta_fora_da_tabela() -> None:
    """``calcular_news2`` nao valida faixa fisiologica, mas e total: fora da tabela levanta.

    ``saturacao_o2 = 150`` nao pertence a nenhuma faixa NEWS2 -- falha ruidosa, nunca escore
    silenciosamente errado (design 7.6, caso F10). A rejeicao de faixa (R6.6) e um passo anterior e
    separado, coberto acima.
    """
    with pytest.raises(ValueError, match="fora de qualquer faixa"):
        calcular_news2(_saudavel(saturacao_o2=150))


# --------------------------------------------------------------------------- #
# Modelos SQLAlchemy espelham db/schema.sql (nomes de schema, tabela, colunas)  #
# --------------------------------------------------------------------------- #


def test_modelo_sinais_vitais_espelha_schema() -> None:
    """O modelo do vitals-service mapeia ``vitais.sinais_vitais`` conforme db/schema.sql."""
    modelos = _carregar("vitals-service", "modelos")
    tabela = modelos.SinaisVitais.__table__  # type: ignore[attr-defined]
    assert tabela.schema == "vitais"
    assert tabela.name == "sinais_vitais"
    esperadas = {
        "id",
        "internacao_id",
        "leito_codigo",
        "coletado_em",
        "registrado_em",
        "frequencia_respiratoria",
        "saturacao_o2",
        "oxigenio_suplementar",
        "temperatura",
        "pressao_sistolica",
        "frequencia_cardiaca",
        "nivel_consciencia",
        "origem_message_id",
        "correlation_id",
    }
    assert set(tabela.columns.keys()) == esperadas


def test_modelo_alerta_espelha_schema() -> None:
    """O modelo do alert-service mapeia ``alertas.alertas`` com as colunas de db/schema.sql."""
    modelos = _carregar("alert-service", "modelos")
    tabela = modelos.AlertaClinico.__table__  # type: ignore[attr-defined]
    assert tabela.schema == "alertas"
    assert tabela.name == "alertas"
    esperadas = {
        "id",
        "internacao_id",
        "leito_codigo",
        "sinais_vitais_id",
        "score_total",
        "severidade",
        "componente_critico",
        "componentes",
        "gerado_em",
        "notificado_em",
        "canal",
        "tentativas_notificacao",
        "ultimo_erro",
        "origem_message_id",
        "correlation_id",
    }
    assert set(tabela.columns.keys()) == esperadas


def test_modelo_evento_auditoria_espelha_schema() -> None:
    """O modelo do audit-service mapeia ``auditoria.eventos_auditoria`` (design 7.4.5)."""
    modelos = _carregar("audit-service", "modelos")
    tabela = modelos.EventoAuditoria.__table__  # type: ignore[attr-defined]
    assert tabela.schema == "auditoria"
    assert tabela.name == "eventos_auditoria"
    esperadas = {
        "id",
        "message_id",
        "correlation_id",
        "causation_id",
        "tipo",
        "versao",
        "routing_key",
        "ocorrido_em",
        "registrado_em",
        "produtor",
        "identidade_sub",
        "identidade_role",
        "identidade_tipo",
        "paciente_id",
        "payload",
    }
    assert set(tabela.columns.keys()) == esperadas
