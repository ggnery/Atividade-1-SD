"""Regra clinica NEWS2 como funcao pura (design secao 7.5 e 7.6).

Este modulo e a unica origem de ``Severidade`` no sistema (INV-5). Ele nao importa
``hospitalmq``, nao importa SQLAlchemy, nao importa FastAPI, nao abre socket, nao le
variavel de ambiente e nao consulta relogio: e determinismo puro, ``O(1)`` e sem estado,
o que o torna testavel sem RabbitMQ nem PostgreSQL (R10.4) e reusavel por
``triage-service`` e ``api-gateway`` (R11.1).

A tabela normativa de pontuacao vive aqui como **dado** -- uma lista de faixas por
parametro (design 7.5.1) --, nunca como cadeia de ``if``, para que a suite de testes possa
parametrizar a tabela inteira (R10.3). ``calcular_news2`` e uma **funcao total**: toda
entrada dentro da faixa fisiologica de aceitacao (7.7) tem escore; um valor que escape de
todas as faixas faz ``_pontuar`` levantar ``ValueError`` -- falha ruidosa, nunca escore
silenciosamente errado.

A validacao das faixas fisiologicas de aceitacao (R6.6, design 7.7) e um passo separado do
calculo: ``FAIXAS_FISIOLOGICAS`` e ``validar_faixas_fisiologicas`` existem para a borda HTTP
e para o consumidor rejeitarem dado impossivel *antes* de calcular NEWS2 -- ``calcular_news2``
nunca valida faixa por si so, coerente com a separacao de responsabilidades do design.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Literal

NivelConsciencia = Literal["A", "V", "P", "U"]
"""Escala AVPU adotada pelo projeto (design 7.5.6): ``A`` alerta; ``V``/``P``/``U`` graves."""


class Severidade(StrEnum):
    """Banda de risco derivada de R6.3 (design 7.5.2). Valores iguais aos gravados no banco."""

    BAIXA = "baixa"
    MEDIA = "media"
    ALTA = "alta"


@dataclass(frozen=True, slots=True)
class SinaisVitais:
    """Entrada do calculo. Congelada: o mesmo objeto nunca muda de escore.

    Os tipos espelham as colunas de ``vitais.sinais_vitais`` (design 7.4): ``temperatura`` e
    ``Decimal`` porque a coluna e ``NUMERIC(4,1)`` e a contiguidade das faixas de temperatura
    so vale na resolucao de uma casa decimal (ver nota em ``_pontuar``).
    """

    frequencia_respiratoria: int
    saturacao_o2: int
    oxigenio_suplementar: bool
    temperatura: Decimal
    pressao_sistolica: int
    frequencia_cardiaca: int
    nivel_consciencia: NivelConsciencia


@dataclass(frozen=True, slots=True)
class ScoreNEWS2:
    """Resultado deterministico do calculo: os sete componentes, o total, o indicador do
    componente isolado igual a 3 (R6.3) e a severidade classificada.

    Value Object sem identidade nem tabela propria: e recomputavel a qualquer momento a partir
    da leitura imutavel que o originou (design 5.9, glossario ScoreNEWS2).
    """

    componentes: Mapping[str, int]
    total: int
    componente_critico: bool
    severidade: Severidade

    def exige_alerta(self) -> bool:
        """Indica se esta leitura deve disparar ``alerta.gerado`` (R6.3): so severidade alta."""
        return self.severidade is Severidade.ALTA


# --------------------------------------------------------------------------- #
# Tabela normativa de pontuacao (design 7.5.1) -- DADO, nao codigo.
# Cada linha e (limite_inferior, limite_superior, pontos), fechada nos dois extremos e
# mutuamente exclusiva. Os limites superiores "abertos" usam sentinelas altos (10_000) para
# manter a funcao total sem ramo especial.
# --------------------------------------------------------------------------- #
_Faixa = tuple[int | Decimal, int | Decimal, int]

_FR: Final[tuple[_Faixa, ...]] = ((0, 8, 3), (9, 11, 1), (12, 20, 0), (21, 24, 2), (25, 10_000, 3))
_SPO2: Final[tuple[_Faixa, ...]] = ((0, 91, 3), (92, 93, 2), (94, 95, 1), (96, 100, 0))
_PAS: Final[tuple[_Faixa, ...]] = (
    (0, 90, 3),
    (91, 100, 2),
    (101, 110, 1),
    (111, 219, 0),
    (220, 10_000, 3),
)
_FC: Final[tuple[_Faixa, ...]] = (
    (0, 40, 3),
    (41, 50, 1),
    (51, 90, 0),
    (91, 110, 1),
    (111, 130, 2),
    (131, 10_000, 3),
)
_TEMP: Final[tuple[_Faixa, ...]] = (
    (Decimal("0"), Decimal("35.0"), 3),
    (Decimal("35.1"), Decimal("36.0"), 1),
    (Decimal("36.1"), Decimal("38.0"), 0),
    (Decimal("38.1"), Decimal("39.0"), 1),
    (Decimal("39.1"), Decimal("100"), 2),
)


def _pontuar(valor: int | Decimal, faixas: tuple[_Faixa, ...]) -> int:
    """Pontua ``valor`` percorrendo a tabela normativa; levanta ``ValueError`` se cair fora.

    Nota de precisao: as faixas de temperatura sao contiguas apenas na resolucao de uma casa
    decimal. ``36.05`` nao pertence a nenhuma faixa e faria esta funcao levantar ``ValueError``.
    Isso e intencional e seguro porque a coluna e ``NUMERIC(4,1)`` e o modelo Pydantic quantiza
    a entrada com ``Decimal.quantize(Decimal("0.1"))`` antes do calculo: nenhum valor com duas
    casas chega a esta funcao.
    """
    for minimo, maximo, pontos in faixas:
        if minimo <= valor <= maximo:
            return pontos
    raise ValueError(f"valor fora de qualquer faixa da tabela NEWS2: {valor!r}")


def detectar_componente_critico(componentes: Mapping[str, int]) -> bool:
    """Regra R6.3 (design 7.5.2): verdadeiro se algum componente isolado pontuar exatamente 3.

    A soma esconde deterioracao concentrada -- um paciente ``nivel_consciencia = P`` soma so 3
    pontos, abaixo do limiar 5, mas esta em falencia de um sistema. Esta deteccao equipara esse
    caso a severidade alta independentemente do total.
    """
    return any(pontos == 3 for pontos in componentes.values())


def classificar_severidade(total: int, componente_critico: bool) -> Severidade:
    """Classifica a severidade a partir do par ``(total, componente_critico)`` (INV-5, R6.3).

    Coerente ao pe da letra com o ``CHECK ck_alertas_severidade_coerente`` de ``alertas.alertas``
    (design 7.4): ``alta`` se ``total >= 5`` ou componente critico; ``media`` se ``3 <= total <= 4``
    sem componente critico; ``baixa`` caso contrario (``total <= 2`` sem componente critico).
    """
    if total >= 5 or componente_critico:
        return Severidade.ALTA
    if total >= 3:
        return Severidade.MEDIA
    return Severidade.BAIXA


# Alias de fidelidade ao pseudocodigo executavel do design 7.5.2/7.6.1, que chama ``classificar``.
classificar = classificar_severidade


def calcular_news2(sinais: SinaisVitais) -> ScoreNEWS2:
    """Calcula o ``ScoreNEWS2`` de uma leitura de sinais vitais (design 7.6.1).

    Funcao total, deterministica e sem I/O. Nao valida faixa fisiologica de aceitacao (R6.6): isso
    e responsabilidade de ``validar_faixas_fisiologicas`` na borda e no consumidor, executada antes.
    A ordem das chaves em ``componentes`` segue os sete parametros exigidos pelo
    ``CHECK ck_alertas_componentes``.
    """
    componentes: dict[str, int] = {
        "frequencia_respiratoria": _pontuar(sinais.frequencia_respiratoria, _FR),
        "saturacao_o2": _pontuar(sinais.saturacao_o2, _SPO2),
        "oxigenio_suplementar": 2 if sinais.oxigenio_suplementar else 0,
        "temperatura": _pontuar(sinais.temperatura, _TEMP),
        "pressao_sistolica": _pontuar(sinais.pressao_sistolica, _PAS),
        "frequencia_cardiaca": _pontuar(sinais.frequencia_cardiaca, _FC),
        "nivel_consciencia": 0 if sinais.nivel_consciencia == "A" else 3,
    }
    total = sum(componentes.values())
    critico = detectar_componente_critico(componentes)
    return ScoreNEWS2(
        componentes=MappingProxyType(componentes),
        total=total,
        componente_critico=critico,
        severidade=classificar_severidade(total, critico),
    )


# --------------------------------------------------------------------------- #
# Faixas fisiologicas de aceitacao (R6.6, design 7.7.1) -- reusaveis pela borda e consumidor.
# Deliberadamente mais largas que a tabela NEWS2: separam "clinicamente ruim" (pontua) de
# "fisicamente impossivel" (rejeita). Sao a fonte unica de verdade dos limites ``Field(ge=, le=)``
# do modelo Pydantic do api-gateway e do vitals-service.
# --------------------------------------------------------------------------- #
NIVEIS_CONSCIENCIA: Final[tuple[NivelConsciencia, ...]] = ("A", "V", "P", "U")
"""Dominio fechado do nivel de consciencia (AVPU)."""


@dataclass(frozen=True, slots=True)
class FaixaFisiologica:
    """Faixa de aceitacao fechada de um parametro numerico (design 7.7.1)."""

    minimo: int | Decimal
    maximo: int | Decimal
    unidade: str

    def contem(self, valor: int | Decimal) -> bool:
        """Indica se ``valor`` esta dentro da faixa de aceitacao (inclusiva nos dois extremos)."""
        return self.minimo <= valor <= self.maximo


FAIXAS_FISIOLOGICAS: Final[Mapping[str, FaixaFisiologica]] = MappingProxyType(
    {
        "frequencia_respiratoria": FaixaFisiologica(0, 80, "rpm"),
        "saturacao_o2": FaixaFisiologica(50, 100, "%"),
        "temperatura": FaixaFisiologica(Decimal("25.0"), Decimal("45.0"), "C"),
        "pressao_sistolica": FaixaFisiologica(40, 300, "mmHg"),
        "frequencia_cardiaca": FaixaFisiologica(20, 250, "bpm"),
    }
)
"""Faixas de aceitacao dos cinco parametros numericos, na ordem do design 7.7.1."""


def dentro_das_faixas_fisiologicas(sinais: SinaisVitais) -> bool:
    """Predicado puro: verdadeiro se todos os campos de ``sinais`` estao na faixa de aceitacao."""
    return (
        FAIXAS_FISIOLOGICAS["frequencia_respiratoria"].contem(sinais.frequencia_respiratoria)
        and FAIXAS_FISIOLOGICAS["saturacao_o2"].contem(sinais.saturacao_o2)
        and FAIXAS_FISIOLOGICAS["temperatura"].contem(sinais.temperatura)
        and FAIXAS_FISIOLOGICAS["pressao_sistolica"].contem(sinais.pressao_sistolica)
        and FAIXAS_FISIOLOGICAS["frequencia_cardiaca"].contem(sinais.frequencia_cardiaca)
        and sinais.nivel_consciencia in NIVEIS_CONSCIENCIA
    )


def validar_faixas_fisiologicas(sinais: SinaisVitais) -> None:
    """Valida ``sinais`` contra as faixas de aceitacao (R6.6); levanta ``ValueError`` na primeira
    violacao, com o campo, o valor recebido e a faixa aceita -- os dados que compoem o payload de
    ``sinais.rejeitados`` (design 7.7.3).

    Defesa em profundidade separada de ``calcular_news2``: rejeita dado impossivel *sem tentar
    calcular NEWS2*, exatamente como R6.6 exige.
    """
    for campo, faixa in FAIXAS_FISIOLOGICAS.items():
        valor = getattr(sinais, campo)
        if not faixa.contem(valor):
            raise ValueError(
                f"{campo}={valor!r} fora da faixa fisiologica aceita "
                f"[{faixa.minimo}, {faixa.maximo}] {faixa.unidade}"
            )
    if sinais.nivel_consciencia not in NIVEIS_CONSCIENCIA:
        raise ValueError(
            f"nivel_consciencia={sinais.nivel_consciencia!r} fora do dominio {NIVEIS_CONSCIENCIA}"
        )
