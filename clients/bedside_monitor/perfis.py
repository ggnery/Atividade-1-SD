"""Perfis fisiologicos e gerador deterministico de sinais vitais do Cliente_Leito.

Este modulo e o coracao reprodutivel da demonstracao (R10.6, C6): dado o mesmo ``--semente``,
ele produz **exatamente** a mesma sequencia de leituras, o que permite cronometrar os 15 minutos
de C5 sem a cena de "esperem, agora ele nao deteriorou". A reproducao usa ``random.Random(semente)``
em instancia propria -- **nunca** o ``random`` global --, exatamente como pede o design 10.7.3, para
que a ordem de execucao (de testes ou de leitos concorrentes) nao interfira no resultado.

Onde o gerador vive. O design tem duas referencias ao gerador: o comentario de 10.7.3 cita
``clients/bedside_monitor/gerador.py``, mas a arvore de empacotamento de 12.8 e a tarefa deste
agente listam apenas ``perfis.py`` para as "trajetorias fisiologicas por cenario, deterministicas".
Seguimos a arvore de 12.8: :class:`Perfil`, :class:`SinaisVitais` e :class:`GeradorSinais` moram
todos aqui, e ``cenarios.py`` importa ``Perfil`` daqui como o design 12.5.5 mostra
(``from .perfis import Perfil``).

Duas faixas, um proposito (design 8.2.3). A borda do gateway aceita uma faixa larga de sanidade;
o ``vitals-service`` aplica a faixa **fisiologica** mais estreita de 7.7.1. E dessa folga deliberada
que nasce o cenario de DLQ: uma ``saturacao_o2 = 20`` passa na borda (0--100) e e recusada pelo
consumidor (aceita so 50--100), caindo em ``q.vitals.sinais-coletados.dlq``. O perfil
:attr:`Perfil.FORA_DE_FAIXA` injeta esse valor no 3o passo.

Determinismo do cruzamento do NEWS2. O perfil :attr:`Perfil.DETERIORACAO` foi calibrado para que o
escore NEWS2 agregado cruze 5 **exatamente no 7o passo** (indice 6), sem que nenhum componente
isolado chegue a 3 antes disso -- de modo que o alerta dispare pela regra do agregado, no instante
ensaiado. O ruido pseudoaleatorio por semente e mantido **dentro** da mesma faixa de pontuacao de
cada parametro (7.5.1), entao a trajetoria de escore e identica para qualquer semente, ao mesmo
tempo que os valores brutos variam -- as duas propriedades que o teste T-36 do design verifica.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Final, Literal

__all__ = ["GeradorSinais", "NivelConsciencia", "Perfil", "SinaisVitais"]

NivelConsciencia = Literal["A", "V", "P", "U"]
"""Escala AVPU do NEWS2 (design 7.5.1): ``A`` alerta; ``V``/``P``/``U`` graves."""

# Passo (indice zero) em que o perfil FORA_DE_FAIXA injeta o valor impossivel (design 12.5.5:
# "valor fisiologicamente impossivel no 3o passo"). O 3o passo e o indice 2.
_PASSO_FORA_DE_FAIXA: Final[int] = 2

# Saturacao aceita pela borda (0--100) mas recusada pela faixa fisiologica (50--100) do
# vitals-service (design 8.2.3): dispara PermanentError -> DLQ sem consumir retentativa (R6.6).
_SATURACAO_IMPOSSIVEL: Final[int] = 20


class Perfil(StrEnum):
    """Trajetoria fisiologica de um cenario (design 10.7.3).

    Cada valor existe para exercitar um ramo distinto da regra clinica NEWS2 e o cenario de falha:
    :attr:`ESTAVEL` (contraste visual, R11.1), :attr:`DETERIORACAO` (agregado cruza 5, R6.2/R6.3),
    :attr:`COMPONENTE_ISOLADO` (um componente = 3 com agregado baixo, R6.3) e :attr:`FORA_DE_FAIXA`
    (valor impossivel -> DLQ, R6.6).
    """

    ESTAVEL = "estavel"
    DETERIORACAO = "deterioracao"
    COMPONENTE_ISOLADO = "componente-isolado"
    FORA_DE_FAIXA = "fora-de-faixa"


@dataclass(frozen=True, slots=True)
class SinaisVitais:
    """Uma leitura de sinais vitais gerada, congelada e reprodutivel.

    Os sete campos espelham, com os mesmos nomes, o modelo Pydantic da borda (design 8.2.3) e a
    funcao pura NEWS2 (7.6.1). ``temperatura`` e :class:`~decimal.Decimal` porque a cadeia inteira
    e decimal (D5 de 7.3.2): comparar fronteiras como 38.0/38.1 em ponto flutuante binario poderia
    inverter o ponto NEWS2 atribuido.
    """

    frequencia_respiratoria: int
    saturacao_o2: int
    oxigenio_suplementar: bool
    temperatura: Decimal
    pressao_sistolica: int
    frequencia_cardiaca: int
    nivel_consciencia: NivelConsciencia

    def como_dict(self) -> dict[str, object]:
        """Projeta os sete parametros em ``dict``, para o calculo NEWS2 e para os testes.

        Returns:
            Mapa campo -> valor, com ``temperatura`` mantida como :class:`~decimal.Decimal`.
        """
        return {
            "frequencia_respiratoria": self.frequencia_respiratoria,
            "saturacao_o2": self.saturacao_o2,
            "oxigenio_suplementar": self.oxigenio_suplementar,
            "temperatura": self.temperatura,
            "pressao_sistolica": self.pressao_sistolica,
            "frequencia_cardiaca": self.frequencia_cardiaca,
            "nivel_consciencia": self.nivel_consciencia,
        }

    def para_payload(self, *, leito_id: str, coletado_em: str) -> dict[str, object]:
        """Monta o corpo JSON do ``POST /sinais`` a partir desta leitura (design 8.2.3).

        ``internacao_id`` e deliberadamente omitido: o gateway o resolve a partir do ``leito_id``
        pela projecao em memoria (design 8.2.5). ``temperatura`` viaja como **string** (``"38.4"``)
        -- forma aceita pelo modelo da borda para clientes cujo serializador de JSON nao conheca
        ``Decimal``, e que preserva a exatidao decimal de uma casa.

        Args:
            leito_id: Codigo do leito, no formato ``^[A-Z]{2,4}-\\d{2}$`` (ex.: ``"UTI-03"``).
            coletado_em: Instante da coleta em ISO-8601 com fuso (ex.: ``"...T14:02:07+00:00"``).

        Returns:
            Dicionario com exatamente os campos que ``SinaisVitaisRequest`` aceita -- o modelo usa
            ``extra="forbid"``, entao nenhum campo desconhecido pode ser incluido.
        """
        return {
            "leito_id": leito_id,
            "coletado_em": coletado_em,
            "frequencia_respiratoria": self.frequencia_respiratoria,
            "saturacao_o2": self.saturacao_o2,
            "oxigenio_suplementar": self.oxigenio_suplementar,
            "temperatura": str(self.temperatura),
            "pressao_sistolica": self.pressao_sistolica,
            "frequencia_cardiaca": self.frequencia_cardiaca,
            "nivel_consciencia": self.nivel_consciencia,
        }


def _temp(valor: str) -> Decimal:
    """Constroi uma temperatura decimal de uma casa a partir do texto (ex.: ``"38.4"``)."""
    return Decimal(valor)


# --------------------------------------------------------------------------- #
# Trajetoria normativa da deterioracao (design 10.7.3 e 12.5.5)                #
#                                                                             #
# Uma linha por passo (indice 0..9). Os valores foram calibrados contra a     #
# tabela NEWS2 de 7.5.1 para que o agregado seja 0,1,2,3,3,4,6,10,13,15 --     #
# isto e, cruze 5 no 7o passo (indice 6) e que nenhum componente isolado       #
# chegue a 3 antes desse passo. A frequencia cardiaca guarda folga de faixa    #
# de proposito, para receber ruido por semente sem mudar de ponto NEWS2.       #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _PassoDeterioracao:
    """Valores base de um passo da deterioracao, antes do ruido por semente."""

    frequencia_respiratoria: int
    saturacao_o2: int
    oxigenio_suplementar: bool
    temperatura: Decimal
    pressao_sistolica: int
    frequencia_cardiaca: int


_TRAJETORIA_DETERIORACAO: Final[tuple[_PassoDeterioracao, ...]] = (
    _PassoDeterioracao(15, 97, False, _temp("36.8"), 122, 70),  # p0  NEWS2 0
    _PassoDeterioracao(16, 95, False, _temp("37.1"), 120, 75),  # p1  NEWS2 1
    _PassoDeterioracao(16, 94, False, _temp("37.4"), 118, 100),  # p2  NEWS2 2
    _PassoDeterioracao(17, 93, False, _temp("37.7"), 116, 100),  # p3  NEWS2 3
    _PassoDeterioracao(18, 93, False, _temp("38.0"), 114, 100),  # p4  NEWS2 3
    _PassoDeterioracao(19, 92, False, _temp("38.3"), 112, 105),  # p5  NEWS2 4
    _PassoDeterioracao(19, 92, True, _temp("38.6"), 111, 105),  # p6  NEWS2 6  <- alerta
    _PassoDeterioracao(22, 91, True, _temp("38.9"), 111, 120),  # p7  NEWS2 10
    _PassoDeterioracao(26, 90, True, _temp("39.2"), 108, 122),  # p8  NEWS2 13
    _PassoDeterioracao(28, 89, True, _temp("39.5"), 100, 140),  # p9  NEWS2 15
)


class GeradorSinais:
    """Gera leituras plausiveis e reprodutiveis. Mesma semente, mesma sequencia (design 10.7.3).

    O gerador usa uma instancia propria de :class:`random.Random`, semeada no construtor, e mantem
    o contador de passos internamente. Cada perfil consome o gerador em um padrao fixo de sorteios
    por passo, o que garante que a sequencia dependa **apenas** da semente e do perfil -- e nao da
    ordem em que outros geradores (de outros leitos, na tempestade) foram chamados.
    """

    __slots__ = ("_passo", "_perfil", "_rng")

    def __init__(self, *, semente: int = 42, perfil: Perfil = Perfil.ESTAVEL) -> None:
        """Constroi o gerador.

        Args:
            semente: Semente do gerador pseudoaleatorio (``SEMENTE_SIMULADOR``; padrao 42 aqui).
            perfil: Trajetoria fisiologica a seguir.
        """
        self._rng = random.Random(semente)
        self._perfil = perfil
        self._passo = 0

    @property
    def perfil(self) -> Perfil:
        """Perfil fisiologico seguido por este gerador."""
        return self._perfil

    @property
    def passo(self) -> int:
        """Quantidade de leituras ja geradas -- o proximo indice a ser produzido."""
        return self._passo

    def proxima(self) -> SinaisVitais:
        """Produz a proxima leitura da trajetoria e avanca o contador de passos.

        Returns:
            A :class:`SinaisVitais` do passo corrente, deterministica dada a semente e o perfil.
        """
        passo = self._passo
        self._passo += 1
        if self._perfil is Perfil.DETERIORACAO:
            return self._deterioracao(passo)
        if self._perfil is Perfil.COMPONENTE_ISOLADO:
            return self._componente_isolado()
        if self._perfil is Perfil.FORA_DE_FAIXA:
            return self._fora_de_faixa(passo)
        return self._estavel()

    def sequencia(self, n: int) -> list[SinaisVitais]:
        """Gera ``n`` leituras consecutivas a partir do estado atual.

        Args:
            n: Quantidade de leituras a produzir.

        Returns:
            Lista com as ``n`` leituras, na ordem de coleta.
        """
        return [self.proxima() for _ in range(n)]

    # -- perfis -------------------------------------------------------------- #

    def _estavel(self) -> SinaisVitais:
        """Oscilacao pequena em torno do normal; NEWS2 sempre 0 (design 10.7.3, R11.1).

        Todos os parametros sao sorteados **dentro** da faixa de pontuacao 0 da tabela NEWS2 de
        7.5.1, entao o agregado e invariavelmente 0 -- dentro do "entre 0 e 1" pedido para o card
        verde -- enquanto os valores brutos variam a cada leitura e a cada semente.
        """
        return SinaisVitais(
            frequencia_respiratoria=self._rng.randint(14, 19),  # 12-20 -> 0
            saturacao_o2=self._rng.randint(96, 99),  # >=96 -> 0
            oxigenio_suplementar=False,  # -> 0
            temperatura=Decimal("36.4") + Decimal(self._rng.randint(0, 4)) / 10,  # 36.4-36.8 -> 0
            pressao_sistolica=self._rng.randint(112, 128),  # 111-219 -> 0
            frequencia_cardiaca=self._rng.randint(60, 85),  # 51-90 -> 0
            nivel_consciencia="A",  # -> 0
        )

    def _deterioracao(self, passo: int) -> SinaisVitais:
        """Queda de SpO2 e alta de FR/FC/temperatura; agregado cruza 5 no 7o passo (R6.2, R6.3).

        Le a linha base de :data:`_TRAJETORIA_DETERIORACAO` (repetindo a ultima quando o passo
        excede a tabela, de modo que uma coleta longa permaneca critica) e aplica um ruido pequeno
        **dentro da mesma faixa de pontuacao** em ``frequencia_respiratoria`` e
        ``frequencia_cardiaca`` -- os dois parametros com folga suficiente para variar sem trocar
        de ponto NEWS2. Assim a trajetoria de escore e identica para qualquer semente (o alerta
        dispara sempre no 7o passo) e os valores brutos ainda diferem entre sementes.
        """
        base = _TRAJETORIA_DETERIORACAO[min(passo, len(_TRAJETORIA_DETERIORACAO) - 1)]
        return SinaisVitais(
            frequencia_respiratoria=base.frequencia_respiratoria + self._rng.randint(-1, 1),
            saturacao_o2=base.saturacao_o2,
            oxigenio_suplementar=base.oxigenio_suplementar,
            temperatura=base.temperatura,
            pressao_sistolica=base.pressao_sistolica,
            frequencia_cardiaca=base.frequencia_cardiaca + self._rng.randint(-4, 4),
            nivel_consciencia="A",
        )

    def _componente_isolado(self) -> SinaisVitais:
        """Agregado baixo com um componente isolado = 3: FR >= 25 (design 10.7.3, R6.3).

        A ``frequencia_respiratoria`` fixa em 26 pontua 3 sozinha (faixa >= 25 de 7.5.1), o que
        classifica a leitura como severidade alta pela regra do componente critico mesmo com o
        agregado em 3 -- o caso que a soma esconderia. Os demais parametros ficam na faixa 0.
        """
        return SinaisVitais(
            frequencia_respiratoria=26,  # >= 25 -> 3 (componente critico)
            saturacao_o2=self._rng.randint(96, 99),  # >=96 -> 0
            oxigenio_suplementar=False,  # -> 0
            temperatura=Decimal("36.6"),  # 36.1-38.0 -> 0
            pressao_sistolica=self._rng.randint(112, 128),  # 111-219 -> 0
            frequencia_cardiaca=self._rng.randint(60, 85),  # 51-90 -> 0
            nivel_consciencia="A",  # -> 0
        )

    def _fora_de_faixa(self, passo: int) -> SinaisVitais:
        """Leitura normal, salvo no 3o passo, onde injeta uma saturacao impossivel (R6.6).

        A leitura base e a mesma oscilacao estavel; no passo :data:`_PASSO_FORA_DE_FAIXA` (indice 2,
        o 3o passo) a ``saturacao_o2`` recebe :data:`_SATURACAO_IMPOSSIVEL` -- valor aceito pela
        borda (0--100) mas recusado pela faixa fisiologica do ``vitals-service`` (50--100). O
        resultado e ``PermanentError`` -> ``q.vitals.sinais-coletados.dlq``, sem consumir
        retentativa (design 8.2.3 e 12.5.5).
        """
        leitura = self._estavel()
        if passo != _PASSO_FORA_DE_FAIXA:
            return leitura
        return SinaisVitais(
            frequencia_respiratoria=leitura.frequencia_respiratoria,
            saturacao_o2=_SATURACAO_IMPOSSIVEL,
            oxigenio_suplementar=leitura.oxigenio_suplementar,
            temperatura=leitura.temperatura,
            pressao_sistolica=leitura.pressao_sistolica,
            frequencia_cardiaca=leitura.frequencia_cardiaca,
            nivel_consciencia=leitura.nivel_consciencia,
        )
