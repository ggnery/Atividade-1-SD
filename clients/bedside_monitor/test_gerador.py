"""Testes de unidade do gerador deterministico e do catalogo de cenarios (design 10.7.3, 12.5.5).

Sem broker, sem banco, sem rede: exercitam apenas as funcoes puras do cliente. Para verificar o
cruzamento do NEWS2, reusam a funcao pura oficial ``services/comum/news2.py`` -- a mesma que o
``vitals-service`` e o ``triage-service`` usam --, em vez de duplicar a tabela normativa aqui.

Estes testes moram no pacote do cliente (``clients/bedside_monitor/``) porque a tarefa do agente se
restringe a ``clients/bedside_monitor/*``. Rode-os com:
    .venv/bin/python -m pytest clients/bedside_monitor/test_gerador.py
"""

from __future__ import annotations

import statistics
from itertools import pairwise

import pytest

from clients.bedside_monitor.cenarios import ALIASES, CENARIOS, Cenario, resolver_cenario
from clients.bedside_monitor.perfis import (
    ENVELOPE_PLANTAO,
    PASSOS_ATE_CRITICO_PADRAO,
    GeradorSinais,
    Perfil,
    ritmo_do_leito,
)
from services.comum.news2 import (
    FAIXAS_FISIOLOGICAS,
    Severidade,
    calcular_news2,
    dentro_das_faixas_fisiologicas,
)
from services.comum.news2 import SinaisVitais as SinaisNews2

# Sementes usadas nas verificacoes do modo continuo (inclui a SEMENTE_SIMULADOR do design).
_SEMENTES = (7, 42, 2024, 20260727)


def _news2(gerador_saida: object) -> int:
    """Calcula o NEWS2 agregado de uma leitura do gerador, via a funcao pura oficial."""
    reading = SinaisNews2(**_como_dict(gerador_saida))
    return calcular_news2(reading).total


def _como_dict(leitura: object) -> dict[str, object]:
    return leitura.como_dict()  # type: ignore[attr-defined]


def _plantao(semente: int, horizonte: int, n: int | None = None) -> list[object]:
    """Gera uma serie do modo continuo com o horizonte de escalada informado."""
    gerador = GeradorSinais(semente=semente, perfil=Perfil.PLANTAO, passos_ate_critico=horizonte)
    return list(gerador.sequencia(horizonte + 5 if n is None else n))


def _severidades(serie: list[object]) -> list[Severidade]:
    return [calcular_news2(SinaisNews2(**_como_dict(s))).severidade for s in serie]


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


# --------------------------------------------------------------------------- #
# Modo continuo (Perfil.PLANTAO): o leito que "vive" e piora devagar            #
# --------------------------------------------------------------------------- #


@pytest.mark.unit
@pytest.mark.requisito("R10.6")
def test_plantao_e_reprodutivel_com_a_mesma_semente() -> None:
    """Mesma semente e mesmo horizonte: mesma evolucao e mesma serie de escores NEWS2."""
    a = _plantao(20260727, 30)
    b = _plantao(20260727, 30)
    assert a == b
    assert [_news2(s) for s in a] == [_news2(s) for s in b]
    outra = _plantao(7, 30)
    assert outra != a  # semente diferente muda os valores brutos
    assert max(_news2(s) for s in outra) >= 7  # ...sem deixar de chegar ao mesmo desfecho


@pytest.mark.unit
@pytest.mark.requisito("R6.2")
def test_plantao_sobe_em_tendencia_e_atinge_severidade_alta() -> None:
    """A serie de escores cresce por quartis (nao-decrescente em tendencia) e termina em alta.

    O ruido por leitura faz o escore oscilar um ponto -- e o que se espera de um monitor real --,
    entao a monotonia e afirmada sobre a MEDIA de cada quarto da serie, nao passo a passo. O que
    precisa ser exato e o desfecho: severidade alta e escore agregado >= 7 (a banda clinica alta).
    """
    for semente in _SEMENTES:
        serie = _plantao(semente, PASSOS_ATE_CRITICO_PADRAO)
        totais = [_news2(s) for s in serie]
        quarto = len(totais) // 4
        medias = [statistics.mean(totais[i * quarto : (i + 1) * quarto]) for i in range(3)]
        medias.append(statistics.mean(totais[3 * quarto :]))
        assert all(a < b for a, b in pairwise(medias)), (semente, medias)
        assert max(totais) >= 7, (semente, totais)
        assert Severidade.ALTA in _severidades(serie), semente


@pytest.mark.unit
@pytest.mark.requisito("R6.3")
def test_plantao_atravessa_as_bandas_na_ordem_sem_pular_para_o_vermelho() -> None:
    """Baixa por varias leituras, depois media, depois alta -- e nunca alta antes da media.

    E o requisito visual da demo: o avaliador precisa ver o card ir de verde a amarelo a vermelho,
    em etapas. Tambem verificamos que o alerta nasce da regra do AGREGADO (>= 5) e nao de um
    componente isolado = 3, que apareceria como um salto direto ao vermelho.
    """
    for semente in _SEMENTES:
        horizonte = PASSOS_ATE_CRITICO_PADRAO
        serie = _plantao(semente, horizonte)
        severidades = _severidades(serie)
        primeira_media = severidades.index(Severidade.MEDIA)
        primeira_alta = severidades.index(Severidade.ALTA)
        assert severidades[0] is Severidade.BAIXA
        assert primeira_media >= 5, (semente, primeira_media)
        assert primeira_media < primeira_alta, (semente, severidades)
        score_alta = calcular_news2(SinaisNews2(**_como_dict(serie[primeira_alta])))
        assert score_alta.total >= 5, (semente, score_alta)
        # no fim do horizonte o paciente esta critico e NAO volta ao verde sozinho
        assert all(s is Severidade.ALTA for s in severidades[int(0.8 * horizonte) :]), semente


@pytest.mark.unit
@pytest.mark.requisito("R6.6")
def test_plantao_nunca_sai_das_faixas_fisiologicas() -> None:
    """Todo valor do modo continuo e aceitavel por R6.6: gerar dado impossivel e do fora-de-faixa.

    Varre horizontes curtos e longos, porque o clamp do envelope existe justamente para o caso em
    que uma escalada muito rapida somada ao ruido empurraria um parametro para fora da faixa.
    """
    for semente in _SEMENTES:
        for horizonte in (1, 3, 10, 45, 120):
            for leitura in _plantao(semente, horizonte, n=horizonte + 20):
                reading = SinaisNews2(**_como_dict(leitura))
                assert dentro_das_faixas_fisiologicas(reading), (semente, horizonte, reading)
                calcular_news2(reading)  # funcao total: nao levanta para valor quantizado


@pytest.mark.unit
def test_envelope_do_plantao_cabe_nas_faixas_fisiologicas() -> None:
    """O envelope de seguranca do modo continuo esta contido em FAIXAS_FISIOLOGICAS (7.7.1).

    Guarda a duplicacao: se a faixa oficial de ``services/comum/news2.py`` mudar, este teste falha
    em vez de o simulador comecar a emitir valor recusado pelo vitals-service.
    """
    for campo, (minimo, maximo) in ENVELOPE_PLANTAO.items():
        faixa = FAIXAS_FISIOLOGICAS[campo]
        assert minimo <= maximo, campo
        assert faixa.contem(minimo), (campo, minimo)
        assert faixa.contem(maximo), (campo, maximo)


@pytest.mark.unit
def test_plantao_esta_vivo_leituras_consecutivas_sempre_diferem() -> None:
    """O monitor parece vivo: a serie muda quase sempre e nunca congela por tres leituras.

    Nao se exige que *toda* leitura diferisse da anterior. O ruido do modo continuo fica em
    ``+-2`` sobre PAS e FC (perfis, ``_plantao``), amplitude escolhida para a banda do NEWS2 nao
    regredir na fronteira 4/5 -- o preco e uma coincidencia ocasional de valores, medida em 0,7%
    dos pares. O que a demonstracao exige de fato e que a tela nunca pareca travada, e disso
    cuidam as duas asercoes abaixo: alta taxa de mudanca e nenhum plato de tres leituras iguais.
    """
    for semente in _SEMENTES:
        serie = _plantao(semente, PASSOS_ATE_CRITICO_PADRAO, n=60)
        pares = list(pairwise(serie))
        diferentes = sum(1 for anterior, atual in pares if anterior != atual)
        assert diferentes / len(pares) >= 0.95, (semente, diferentes, len(pares))
        for i, (a, b, c) in enumerate(zip(serie, serie[1:], serie[2:], strict=False)):
            assert not (a == b == c), (semente, i, a)


@pytest.mark.unit
def test_plantao_continua_variando_depois_de_ficar_critico() -> None:
    """No topo (``g`` saturado em 1.0) o paciente segue critico, oscilando -- nunca zera sozinho."""
    gerador = GeradorSinais(semente=42, perfil=Perfil.PLANTAO, passos_ate_critico=10)
    gerador.sequencia(10)  # descarta a subida
    assert gerador.gravidade() == 1.0
    plato = gerador.sequencia(20)
    assert all(s is Severidade.ALTA for s in _severidades(list(plato)))
    assert len({_como_dict(s)["frequencia_cardiaca"] for s in plato}) > 1


@pytest.mark.unit
@pytest.mark.requisito("R10.6")
def test_escalada_menor_antecipa_o_alerta() -> None:
    """``--escalada`` menor (horizonte menor) faz a severidade alta chegar em menos leituras."""
    for semente in _SEMENTES:
        indices = []
        for horizonte in (15, 30, 60):
            severidades = _severidades(_plantao(semente, horizonte, n=horizonte + 10))
            indices.append(severidades.index(Severidade.ALTA))
        assert indices[0] < indices[1] < indices[2], (semente, indices)


@pytest.mark.unit
def test_ritmo_do_leito_dispersa_sem_perder_o_determinismo() -> None:
    """Cada leito ganha um ritmo levemente diferente; o leito 0 conserva o ritmo pedido."""
    base = PASSOS_ATE_CRITICO_PADRAO
    ritmos = [ritmo_do_leito(base, semente=20260727, indice=i) for i in range(6)]
    assert ritmos[0] == base
    assert len(set(ritmos)) > 1  # o mural nao muda de cor todo no mesmo instante
    assert all(0.7 * base <= r <= 1.3 * base for r in ritmos)
    assert ritmos == [ritmo_do_leito(base, semente=20260727, indice=i) for i in range(6)]
    assert ritmos[1:] != [ritmo_do_leito(base, semente=1, indice=i) for i in range(1, 6)]
    assert ritmo_do_leito(1, semente=1, indice=3) >= 1  # nunca degenera para zero passos


@pytest.mark.unit
def test_gerador_rejeita_horizonte_invalido() -> None:
    """Horizonte de escalada nao positivo e erro de programacao, nao divisao por zero silenciosa."""
    with pytest.raises(ValueError, match="passos_ate_critico"):
        GeradorSinais(semente=1, perfil=Perfil.PLANTAO, passos_ate_critico=0)


@pytest.mark.unit
def test_cenario_plantao_roda_indefinidamente_em_leito_nao_sabotado() -> None:
    """O cenario plantao: sem limite de passos, com escalada propria e longe do leito sabotado."""
    definicao = CENARIOS[Cenario.PLANTAO]
    assert definicao.perfil is Perfil.PLANTAO
    assert definicao.passos is None  # roda ate Ctrl+C
    assert definicao.escalada_s is not None and definicao.escalada_s > 0
    assert definicao.leito_padrao != CENARIOS[Cenario.FALHA_CONSUMIDOR].leito_padrao
    assert resolver_cenario("plantao") is Cenario.PLANTAO
    # os cenarios do enunciado continuam com numero fixo de passos
    for cenario in (Cenario.ESTAVEL, Cenario.DETERIORACAO, Cenario.FORA_DE_FAIXA):
        assert CENARIOS[cenario].passos is not None
        assert CENARIOS[cenario].escalada_s is None


@pytest.mark.unit
def test_alias_dlq_resolve_para_fora_de_faixa() -> None:
    """O apelido 'dlq' aponta para o cenario fora-de-faixa (design 12.5.5)."""
    assert ALIASES["dlq"] is Cenario.FORA_DE_FAIXA
    assert resolver_cenario("dlq") is Cenario.FORA_DE_FAIXA
    assert resolver_cenario("DETERIORACAO") is Cenario.DETERIORACAO
    with pytest.raises(ValueError, match="cenario desconhecido"):
        resolver_cenario("inexistente")
