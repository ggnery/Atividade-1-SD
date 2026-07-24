"""Testes de unidade do gerador deterministico e do catalogo de cenarios (design 10.7.3, 12.5.5).

Sem broker, sem banco, sem rede: exercitam apenas as funcoes puras do cliente. Para verificar o
cruzamento do NEWS2, reusam a funcao pura oficial ``services/comum/news2.py`` -- a mesma que o
``vitals-service`` e o ``triage-service`` usam --, em vez de duplicar a tabela normativa aqui.

Estes testes moram no pacote do cliente (``clients/bedside_monitor/``) porque a tarefa do agente se
restringe a ``clients/bedside_monitor/*``. Rode-os com:
    .venv/bin/python -m pytest clients/bedside_monitor/test_gerador.py
"""

from __future__ import annotations

import pytest

from clients.bedside_monitor.cenarios import ALIASES, CENARIOS, Cenario, resolver_cenario
from clients.bedside_monitor.perfis import GeradorSinais, Perfil
from services.comum.news2 import SinaisVitais as SinaisNews2
from services.comum.news2 import calcular_news2, dentro_das_faixas_fisiologicas


def _news2(gerador_saida: object) -> int:
    """Calcula o NEWS2 agregado de uma leitura do gerador, via a funcao pura oficial."""
    reading = SinaisNews2(**_como_dict(gerador_saida))
    return calcular_news2(reading).total


def _como_dict(leitura: object) -> dict[str, object]:
    return leitura.como_dict()  # type: ignore[attr-defined]


@pytest.mark.unit
@pytest.mark.requisito("R10.6")
def test_gerador_e_reprodutivel_com_a_mesma_semente() -> None:
    """Mesma semente e mesmo perfil produzem exatamente a mesma sequencia (T-36 do design)."""
    a = GeradorSinais(semente=42, perfil=Perfil.DETERIORACAO).sequencia(20)
    b = GeradorSinais(semente=42, perfil=Perfil.DETERIORACAO).sequencia(20)
    assert a == b
    assert GeradorSinais(semente=7, perfil=Perfil.DETERIORACAO).sequencia(20) != a


@pytest.mark.unit
@pytest.mark.requisito("R10.6")
def test_deterioracao_cruza_news2_5_no_setimo_passo() -> None:
    """O agregado passa de 5 no 7o passo (indice 6) e fica abaixo antes, para qualquer semente."""
    for semente in (7, 42, 20260727):
        seq = GeradorSinais(semente=semente, perfil=Perfil.DETERIORACAO).sequencia(10)
        totais = [_news2(s) for s in seq]
        assert all(t < 5 for t in totais[:6]), (semente, totais)
        assert totais[6] >= 5, (semente, totais)


@pytest.mark.unit
@pytest.mark.requisito("R11.1")
def test_estavel_mantem_news2_entre_0_e_1() -> None:
    """O perfil estavel nunca ultrapassa NEWS2 1 -- o card verde do contraste visual."""
    seq = GeradorSinais(semente=20260727, perfil=Perfil.ESTAVEL).sequencia(30)
    assert all(_news2(s) <= 1 for s in seq)


@pytest.mark.unit
@pytest.mark.requisito("R6.6")
def test_fora_de_faixa_injeta_valor_impossivel_no_terceiro_passo() -> None:
    """No 3o passo (indice 2) a saturacao sai da faixa fisiologica; os demais passos sao normais."""
    seq = GeradorSinais(semente=42, perfil=Perfil.FORA_DE_FAIXA).sequencia(5)
    reading = SinaisNews2(**_como_dict(seq[2]))
    assert seq[2].saturacao_o2 == 20
    assert not dentro_das_faixas_fisiologicas(reading)
    for i, s in enumerate(seq):
        if i == 2:
            continue
        assert dentro_das_faixas_fisiologicas(SinaisNews2(**_como_dict(s))), i


@pytest.mark.unit
@pytest.mark.requisito("R6.3")
def test_componente_isolado_e_critico_com_agregado_baixo() -> None:
    """FR >= 25 pontua 3 sozinho (componente critico) com agregado abaixo do limiar de 5."""
    seq = GeradorSinais(semente=42, perfil=Perfil.COMPONENTE_ISOLADO).sequencia(5)
    for leitura in seq:
        score = calcular_news2(SinaisNews2(**_como_dict(leitura)))
        assert score.componente_critico is True
        assert score.total < 5
        assert score.exige_alerta() is True


@pytest.mark.unit
@pytest.mark.requisito("R10.6")
def test_catalogo_cobre_os_quatro_criterios_do_enunciado() -> None:
    """O catalogo cobre exatamente os quatro criterios de R10.6 -- nem faltando, nem inventado."""
    exigidos = {
        "paciente estavel",
        "paciente em deterioracao",
        "falha de consumidor com retentativa",
        "mensagem enviada a DLQ",
    }
    cobertos = {d.criterio_r10_6 for d in CENARIOS.values() if d.criterio_r10_6}
    assert exigidos == cobertos


@pytest.mark.unit
@pytest.mark.requisito("R10.6")
def test_falha_consumidor_aponta_para_leito_sabotavel() -> None:
    """O leito padrao do cenario falha-consumidor e UTI-03, o alvo de ALERT_FAILURE_LEITOS."""
    assert CENARIOS[Cenario.FALHA_CONSUMIDOR].leito_padrao == "UTI-03"
    assert CENARIOS[Cenario.FALHA_CONSUMIDOR].api_key_padrao == "dev-monitor-uti03"


@pytest.mark.unit
def test_alias_dlq_resolve_para_fora_de_faixa() -> None:
    """O apelido 'dlq' aponta para o cenario fora-de-faixa (design 12.5.5)."""
    assert ALIASES["dlq"] is Cenario.FORA_DE_FAIXA
    assert resolver_cenario("dlq") is Cenario.FORA_DE_FAIXA
    assert resolver_cenario("DETERIORACAO") is Cenario.DETERIORACAO
    with pytest.raises(ValueError, match="cenario desconhecido"):
        resolver_cenario("inexistente")
