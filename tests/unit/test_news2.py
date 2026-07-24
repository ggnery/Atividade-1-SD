"""Testes da regra clinica NEWS2 (design 7.5, 7.6; R6.2, R6.3).

Exercita a funcao pura ``calcular_news2`` e a classificacao de severidade **sem banco e sem broker**
(R10.4): a tabela normativa parametro a parametro com os limites de cada faixa (R6.2), a regra do
componente isolado igual a 3 (R6.3), o mapeamento escore -> severidade e os dois exemplos numericos
do design 7.5.3 / 7.5.4.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.comum.news2 import (
    Severidade,
    SinaisVitais,
    calcular_news2,
    classificar_severidade,
)


def _base(**override: object) -> SinaisVitais:
    """Leitura de referencia com **todos** os componentes pontuando 0 (total 0).

    Cada teste sobrescreve um unico campo para isolar a contribuicao daquele parametro.
    """
    campos: dict[str, object] = {
        "frequencia_respiratoria": 12,  # 12-20 -> 0
        "saturacao_o2": 96,  # >= 96 -> 0
        "oxigenio_suplementar": False,  # ar ambiente -> 0
        "temperatura": Decimal("37.0"),  # 36.1-38.0 -> 0
        "pressao_sistolica": 118,  # 111-219 -> 0
        "frequencia_cardiaca": 60,  # 51-90 -> 0
        "nivel_consciencia": "A",  # alerta -> 0
    }
    campos.update(override)
    return SinaisVitais(**campos)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Tabela normativa parametro a parametro, com os limites de cada faixa (R6.2)   #
# design 7.5.1 / 7.5.5 (F5, F6, F7)                                             #
# --------------------------------------------------------------------------- #

_FR = [
    (0, 3),
    (8, 3),
    (9, 1),
    (11, 1),
    (12, 0),
    (20, 0),
    (21, 2),
    (24, 2),
    (25, 3),
    (80, 3),
]


@pytest.mark.parametrize(("valor", "pontos"), _FR)
def test_frequencia_respiratoria_tabela(valor: int, pontos: int) -> None:
    """Cada limite de faixa de FR pontua exatamente conforme a tabela normativa (F6)."""
    score = calcular_news2(_base(frequencia_respiratoria=valor))
    assert score.componentes["frequencia_respiratoria"] == pontos
    assert score.total == pontos


_SPO2 = [(50, 3), (91, 3), (92, 2), (93, 2), (94, 1), (95, 1), (96, 0), (100, 0)]


@pytest.mark.parametrize(("valor", "pontos"), _SPO2)
def test_saturacao_tabela(valor: int, pontos: int) -> None:
    """Cada limite de faixa de SpO2 pontua conforme a tabela normativa."""
    score = calcular_news2(_base(saturacao_o2=valor))
    assert score.componentes["saturacao_o2"] == pontos
    assert score.total == pontos


_PAS = [
    (40, 3),
    (90, 3),
    (91, 2),
    (100, 2),
    (101, 1),
    (110, 1),
    (111, 0),
    (219, 0),
    (220, 3),
    (300, 3),
]


@pytest.mark.parametrize(("valor", "pontos"), _PAS)
def test_pressao_sistolica_tabela(valor: int, pontos: int) -> None:
    """PAS e faixa nao monotonica: 219 -> 0 e 220 -> 3 (F7). Limites conferidos um a um."""
    score = calcular_news2(_base(pressao_sistolica=valor))
    assert score.componentes["pressao_sistolica"] == pontos
    assert score.total == pontos


_FC = [
    (20, 3),
    (40, 3),
    (41, 1),
    (50, 1),
    (51, 0),
    (90, 0),
    (91, 1),
    (110, 1),
    (111, 2),
    (130, 2),
    (131, 3),
    (250, 3),
]


@pytest.mark.parametrize(("valor", "pontos"), _FC)
def test_frequencia_cardiaca_tabela(valor: int, pontos: int) -> None:
    """Cada limite de faixa de FC pontua conforme a tabela normativa."""
    score = calcular_news2(_base(frequencia_cardiaca=valor))
    assert score.componentes["frequencia_cardiaca"] == pontos
    assert score.total == pontos


_TEMP = [
    ("25.0", 3),
    ("35.0", 3),
    ("35.1", 1),
    ("36.0", 1),
    ("36.1", 0),
    ("38.0", 0),
    ("38.1", 1),
    ("39.0", 1),
    ("39.1", 2),
    ("45.0", 2),
]


@pytest.mark.parametrize(("valor", "pontos"), _TEMP)
def test_temperatura_tabela(valor: str, pontos: int) -> None:
    """Fronteira decimal de temperatura (F5): 36.0 -> 1 e 36.1 -> 0, na resolucao de uma casa."""
    score = calcular_news2(_base(temperatura=Decimal(valor)))
    assert score.componentes["temperatura"] == pontos
    assert score.total == pontos


@pytest.mark.parametrize(("suplementar", "pontos"), [(True, 2), (False, 0)])
def test_oxigenio_suplementar(suplementar: bool, pontos: int) -> None:
    """O parametro booleano pontua 2 (com oxigenio) ou 0 (ar ambiente); nunca chega a 3 (F9)."""
    score = calcular_news2(_base(oxigenio_suplementar=suplementar))
    assert score.componentes["oxigenio_suplementar"] == pontos
    assert score.total == pontos


@pytest.mark.parametrize(("avpu", "pontos"), [("A", 0), ("V", 3), ("P", 3), ("U", 3)])
def test_nivel_consciencia(avpu: str, pontos: int) -> None:
    """AVPU: ``A`` pontua 0; ``V``/``P``/``U`` pontuam 3 (deterioracao concentrada)."""
    score = calcular_news2(_base(nivel_consciencia=avpu))
    assert score.componentes["nivel_consciencia"] == pontos


# --------------------------------------------------------------------------- #
# Regra do componente isolado == 3 (R6.3, design 7.5.2, caso F2)                #
# --------------------------------------------------------------------------- #


def test_componente_isolado_tres_dispara_alta_com_total_abaixo_de_cinco() -> None:
    """F2/R6.3: ``nivel_consciencia = P`` soma so 3, mas um componente == 3 forca alta."""
    score = calcular_news2(_base(nivel_consciencia="P"))
    assert score.total == 3
    assert score.componente_critico is True
    assert score.severidade is Severidade.ALTA
    assert score.exige_alerta() is True


def test_sem_componente_tres_nao_e_critico() -> None:
    """Sem nenhum componente pontuando 3, ``componente_critico`` e falso (base do mapeamento)."""
    score = calcular_news2(_base(saturacao_o2=94))  # 1 ponto isolado
    assert score.componente_critico is False


# --------------------------------------------------------------------------- #
# Mapeamento escore -> severidade (baixa / media / alta), design 7.5.2 / 7.5.5  #
# --------------------------------------------------------------------------- #


def test_severidade_baixa_total_zero() -> None:
    """F1: elemento neutro -- total 0, sem componente critico, severidade baixa."""
    score = calcular_news2(_base())
    assert score.total == 0
    assert score.severidade is Severidade.BAIXA


def test_severidade_media_total_quatro_sem_critico() -> None:
    """F4: total 4 sem componente 3 -> media (nao dispara alerta)."""
    # quatro componentes pontuando 1 cada, nenhum pontuando 3.
    score = calcular_news2(
        _base(
            saturacao_o2=94,  # 1
            temperatura=Decimal("35.5"),  # 1
            pressao_sistolica=101,  # 1
            frequencia_cardiaca=41,  # 1
        )
    )
    assert score.total == 4
    assert score.componente_critico is False
    assert score.severidade is Severidade.MEDIA
    assert score.exige_alerta() is False


def test_severidade_alta_no_limiar_exato_cinco() -> None:
    """F3: cinco componentes pontuando 1 -> total 5 -> alta (fronteira ``>= 5``, nao ``> 5``)."""
    score = calcular_news2(
        _base(
            frequencia_respiratoria=9,  # 1
            saturacao_o2=94,  # 1
            temperatura=Decimal("35.5"),  # 1
            pressao_sistolica=101,  # 1
            frequencia_cardiaca=41,  # 1
        )
    )
    assert score.total == 5
    assert score.componente_critico is False
    assert score.severidade is Severidade.ALTA


def test_escore_maximo_vinte() -> None:
    """F8: pior valor em todos os parametros -> total 20 (limite superior do dominio)."""
    score = calcular_news2(
        SinaisVitais(
            frequencia_respiratoria=25,  # 3
            saturacao_o2=91,  # 3
            oxigenio_suplementar=True,  # 2
            temperatura=Decimal("35.0"),  # 3
            pressao_sistolica=90,  # 3
            frequencia_cardiaca=40,  # 3
            nivel_consciencia="U",  # 3
        )
    )
    assert score.total == 20
    assert score.componente_critico is True
    assert score.severidade is Severidade.ALTA


def test_oxigenio_suplementar_isolado_e_baixa() -> None:
    """F9: apenas ``oxigenio_suplementar = True`` -> total 2, nao critico, severidade baixa."""
    score = calcular_news2(_base(oxigenio_suplementar=True))
    assert score.total == 2
    assert score.componente_critico is False
    assert score.severidade is Severidade.BAIXA


@pytest.mark.parametrize(
    ("total", "critico", "esperado"),
    [
        (0, False, Severidade.BAIXA),
        (2, False, Severidade.BAIXA),
        (3, False, Severidade.MEDIA),
        (4, False, Severidade.MEDIA),
        (5, False, Severidade.ALTA),
        (7, False, Severidade.ALTA),
        (0, True, Severidade.ALTA),  # componente critico domina o total baixo (R6.3)
        (3, True, Severidade.ALTA),
    ],
)
def test_classificar_severidade_mapeamento(total: int, critico: bool, esperado: Severidade) -> None:
    """A funcao de classificacao segue o CHECK ``ck_alertas_severidade_coerente`` (INV-5)."""
    assert classificar_severidade(total, critico) is esperado


# --------------------------------------------------------------------------- #
# Exemplos numericos do design 7.5.3 (estavel) e 7.5.4 (deterioracao)          #
# --------------------------------------------------------------------------- #


def test_exemplo_a_paciente_estavel_total_um_baixa() -> None:
    """Design 7.5.3: Maria Souza, total esperado 1, severidade baixa, sem alerta."""
    score = calcular_news2(
        SinaisVitais(
            frequencia_respiratoria=18,  # 0
            saturacao_o2=95,  # 1
            oxigenio_suplementar=False,  # 0
            temperatura=Decimal("37.2"),  # 0
            pressao_sistolica=118,  # 0
            frequencia_cardiaca=88,  # 0
            nivel_consciencia="A",  # 0
        )
    )
    assert score.total == 1
    assert score.componente_critico is False
    assert score.severidade is Severidade.BAIXA
    assert score.exige_alerta() is False


def test_exemplo_b_paciente_em_deterioracao_total_quinze_alta() -> None:
    """Design 7.5.4: Joao Pereira, total esperado 15, componente critico, severidade alta."""
    score = calcular_news2(
        SinaisVitais(
            frequencia_respiratoria=26,  # 3
            saturacao_o2=92,  # 2
            oxigenio_suplementar=True,  # 2
            temperatura=Decimal("38.6"),  # 1
            pressao_sistolica=96,  # 2
            frequencia_cardiaca=118,  # 2
            nivel_consciencia="V",  # 3
        )
    )
    assert score.total == 15
    assert score.componente_critico is True
    assert score.severidade is Severidade.ALTA
    assert score.exige_alerta() is True
