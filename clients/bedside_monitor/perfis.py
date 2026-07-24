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

Modo continuo (:attr:`Perfil.PLANTAO`). Os quatro perfis acima sao *trajetorias por passo*: cada
passo tem uma faixa de pontuacao pre-definida. O perfil ``plantao`` e diferente -- ele modela um
**estado interno continuo de gravidade** ``g`` (um ``float`` de 0.0, paciente compensado, a 1.0,
paciente critico) e deriva os sete parametros de ``g`` por interpolacao entre um extremo saudavel e
um extremo grave, com ruido pequeno por cima. Duas consequencias praticas: as leituras consecutivas
nunca sao iguais (parece um monitor de verdade, nao um script) e a gravidade sobe **devagar**,
atravessando as bandas do NEWS2 em ordem -- baixa, media e so entao alta -- em vez de pular direto
para o vermelho. O horizonte da escalada e parametrizavel
(:data:`PASSOS_ATE_CRITICO_PADRAO`, ``--escalada`` no CLI) e o determinismo por semente e o mesmo
dos outros perfis: mesma semente, mesma evolucao.
"""

from __future__ import annotations

import random
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Literal

__all__ = [
    "DISPERSAO_RITMO_PADRAO",
    "ENVELOPE_PLANTAO",
    "PASSOS_ATE_CRITICO_PADRAO",
    "GeradorSinais",
    "NivelConsciencia",
    "Perfil",
    "SinaisVitais",
    "ritmo_do_leito",
]

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
    (valor impossivel -> DLQ, R6.6). :attr:`PLANTAO` e o modo continuo: em vez de uma trajetoria
    por passo, um estado de gravidade que sobe devagar e faz o escore atravessar as bandas em ordem.
    """

    ESTAVEL = "estavel"
    DETERIORACAO = "deterioracao"
    COMPONENTE_ISOLADO = "componente-isolado"
    FORA_DE_FAIXA = "fora-de-faixa"
    PLANTAO = "plantao"


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


# --------------------------------------------------------------------------- #
# Modo continuo -- Perfil.PLANTAO                                              #
#                                                                             #
# Uma rampa por parametro, interpolando o extremo saudavel no extremo grave a  #
# medida que a gravidade g caminha de 0.0 a 1.0. As janelas (inicio, fim) e a  #
# curva foram CALIBRADAS contra a tabela NEWS2 de 7.5.1 para que o escore suba  #
# ponto a ponto, na ordem abaixo, sem pular bandas (g -> total agregado, sem   #
# ruido; medido de verdade, nao estimado):                                     #
#                                                                             #
#   g 0.26  FC 91      -> 1     g 0.65  FC 111     -> 5   (severidade ALTA)    #
#   g 0.38  T 38.1     -> 2     g 0.70  T 39.1     -> 6                        #
#   g 0.43  SpO2 95    -> 3     g 0.71  SpO2 93    -> 7   (banda clinica alta)  #
#          (severidade MEDIA)   g 0.80  O2 suplem. -> 9                        #
#   g 0.57  PAS 110    -> 4     g 0.83  PAS 100    -> 10                       #
#                               g 0.84  FR 21      -> 12                       #
#                               g 0.94  AVPU V     -> 15  (comp. critico)      #
#                               g 0.95  FR 25      -> 16                       #
#                                                                             #
# Nenhum componente isolado chega a 3 antes de g 0.94, entao o primeiro alerta  #
# nasce da regra do AGREGADO (>= 5), como no cenario deterioracao.             #
# --------------------------------------------------------------------------- #
PASSOS_ATE_CRITICO_PADRAO: Final[int] = 45
"""Leituras para ``g`` ir de 0.0 a 1.0 no modo continuo.

Calibrado junto com o ``intervalo_s`` de 4 s do cenario ``plantao``: 45 leituras x 4 s = 180 s de
escalada, com severidade **media** por volta de 1min20, **alta** (agregado >= 5) por volta de 2 min
e a banda clinica alta (>= 7) logo em seguida -- a janela de 2 a 3 minutos que cabe na apresentacao
de 15 minutos sem deixar o avaliador esperando, e longa o bastante para ele ver o card passar pelo
amarelo antes do vermelho. O CLI sobrepoe este padrao com ``--escalada SEG``.
"""

DISPERSAO_RITMO_PADRAO: Final[float] = 0.22
"""Dispersao relativa do ritmo entre leitos (+-22%), para os cards nao virarem todos juntos."""

ENVELOPE_PLANTAO: Final[Mapping[str, tuple[int | Decimal, int | Decimal]]] = MappingProxyType(
    {
        "frequencia_respiratoria": (8, 40),
        "saturacao_o2": (85, 100),
        "temperatura": (Decimal("35.5"), Decimal("41.0")),
        "pressao_sistolica": (80, 140),
        "frequencia_cardiaca": (50, 150),
    }
)
"""Envelope de seguranca do modo continuo: teto e piso aplicados **depois** do ruido.

Deliberadamente mais estreito que ``FAIXAS_FISIOLOGICAS`` (design 7.7.1): o modo continuo simula um
paciente que piora, nao um sensor quebrado, entao nenhuma leitura sua pode ser recusada por R6.6 --
gerar dado impossivel e trabalho exclusivo do perfil :attr:`Perfil.FORA_DE_FAIXA`. O teste
``test_envelope_do_plantao_cabe_nas_faixas_fisiologicas`` prova a continencia contra a fonte de
verdade em ``services/comum/news2.py``, de modo que uma mudanca de la nao passe silenciosa aqui.
"""

_LIMIAR_O2_SUPLEMENTAR: Final[float] = 0.80
"""Gravidade a partir da qual a equipe instala oxigenio suplementar (+2 no NEWS2)."""

_LIMIAR_CONSCIENCIA_V: Final[float] = 0.94
"""Gravidade a partir da qual o paciente deixa de estar alerta (AVPU ``A`` -> ``V``, +3)."""

_UM_DECIMO: Final[Decimal] = Decimal("0.1")


@dataclass(frozen=True, slots=True)
class _Rampa:
    """Interpolacao de um parametro entre o extremo saudavel e o extremo grave.

    Attributes:
        saudavel: Valor em ``g <= inicio`` (paciente compensado).
        grave: Valor em ``g >= fim`` (paciente critico).
        inicio: Gravidade em que o parametro comeca a se mover.
        fim: Gravidade em que o parametro atinge o extremo grave.
        curva: Expoente da interpolacao. ``1.0`` e linear; ``> 1`` retarda o inicio e acelera o
            final (a taquipneia da ``frequencia_respiratoria``, que aparece tarde e rapido).
    """

    saudavel: float
    grave: float
    inicio: float
    fim: float
    curva: float = 1.0

    def em(self, gravidade: float) -> float:
        """Valor interpolado do parametro para a ``gravidade`` informada.

        Args:
            gravidade: Estado interno ``g`` do paciente, em ``[0.0, 1.0]``.

        Returns:
            O valor do parametro, ainda em ponto flutuante e sem ruido nem arredondamento.
        """
        if gravidade <= self.inicio:
            fracao = 0.0
        elif gravidade >= self.fim:
            fracao = 1.0
        else:
            fracao = ((gravidade - self.inicio) / (self.fim - self.inicio)) ** self.curva
        return self.saudavel + (self.grave - self.saudavel) * fracao


_FC_PLANTAO: Final[_Rampa] = _Rampa(78.0, 126.0, 0.02, 0.95)
_TEMPERATURA_PLANTAO: Final[_Rampa] = _Rampa(37.0, 39.8, 0.05, 0.94)
_SATURACAO_PLANTAO: Final[_Rampa] = _Rampa(98.0, 92.0, 0.08, 0.92)
_PAS_PLANTAO: Final[_Rampa] = _Rampa(126.0, 94.0, 0.16, 1.0)
_FR_PLANTAO: Final[_Rampa] = _Rampa(15.0, 26.0, 0.20, 0.98, curva=3.5)


def _limitar(valor: Decimal, campo: str) -> Decimal:
    """Aplica o :data:`ENVELOPE_PLANTAO` de ``campo`` a ``valor`` (clamp nos dois extremos)."""
    minimo, maximo = ENVELOPE_PLANTAO[campo]
    return max(Decimal(minimo), min(Decimal(maximo), valor))


def ritmo_do_leito(
    passos_base: int,
    *,
    semente: int,
    indice: int,
    dispersao: float = DISPERSAO_RITMO_PADRAO,
) -> int:
    """Varia levemente o horizonte de escalada de cada leito, de forma reprodutivel.

    Num mural com varios leitos, uma escalada identica faria todos os cards trocarem de cor no
    mesmo instante -- artificial e, pior, ambiguo para quem assiste. Esta funcao dispersa o ritmo em
    ``+-dispersao`` a partir da semente base e do indice do leito, mantendo o determinismo. O leito
    de indice ``0`` conserva **exatamente** o ritmo pedido, para que uma execucao de um unico leito
    honre ``--escalada`` ao pe da letra.

    Args:
        passos_base: Horizonte pedido, em leituras, para ``g`` ir de 0.0 a 1.0.
        semente: Semente base do simulador.
        indice: Posicao do leito na lista de leitos.
        dispersao: Dispersao relativa maxima aplicada ao horizonte.

    Returns:
        O horizonte deste leito, em leituras, sempre ``>= 1``.
    """
    if indice <= 0:
        return max(1, passos_base)
    rng = random.Random(semente + 9973 * indice)
    return max(1, round(passos_base * (1.0 + rng.uniform(-dispersao, dispersao))))


class GeradorSinais:
    """Gera leituras plausiveis e reprodutiveis. Mesma semente, mesma sequencia (design 10.7.3).

    O gerador usa uma instancia propria de :class:`random.Random`, semeada no construtor, e mantem
    o contador de passos internamente. Cada perfil consome o gerador em um padrao fixo de sorteios
    por passo, o que garante que a sequencia dependa **apenas** da semente e do perfil -- e nao da
    ordem em que outros geradores (de outros leitos, na tempestade) foram chamados.
    """

    __slots__ = ("_passo", "_passos_ate_critico", "_perfil", "_rng")

    def __init__(
        self,
        *,
        semente: int = 42,
        perfil: Perfil = Perfil.ESTAVEL,
        passos_ate_critico: int = PASSOS_ATE_CRITICO_PADRAO,
    ) -> None:
        """Constroi o gerador.

        Args:
            semente: Semente do gerador pseudoaleatorio (``SEMENTE_SIMULADOR``; padrao 42 aqui).
            perfil: Trajetoria fisiologica a seguir.
            passos_ate_critico: Horizonte da escalada do modo continuo -- quantas leituras a
                gravidade leva para ir de 0.0 a 1.0. Ignorado pelos demais perfis.

        Raises:
            ValueError: Se ``passos_ate_critico`` nao for positivo (divisao por zero na gravidade).
        """
        if passos_ate_critico < 1:
            raise ValueError(f"passos_ate_critico deve ser >= 1; recebido {passos_ate_critico}")
        self._rng = random.Random(semente)
        self._perfil = perfil
        self._passo = 0
        self._passos_ate_critico = passos_ate_critico

    @property
    def perfil(self) -> Perfil:
        """Perfil fisiologico seguido por este gerador."""
        return self._perfil

    @property
    def passo(self) -> int:
        """Quantidade de leituras ja geradas -- o proximo indice a ser produzido."""
        return self._passo

    @property
    def passos_ate_critico(self) -> int:
        """Horizonte da escalada do modo continuo, em leituras."""
        return self._passos_ate_critico

    def gravidade(self, passo: int | None = None) -> float:
        """Estado interno de gravidade ``g`` do modo continuo, em ``[0.0, 1.0]``.

        Sobe linearmente com o passo e **satura** em 1.0: uma vez critico, o paciente permanece
        critico (com oscilacao vinda do ruido), nunca voltando sozinho ao verde.

        Args:
            passo: Indice da leitura; ``None`` usa o passo corrente do gerador.

        Returns:
            A gravidade correspondente, saturada em 1.0.
        """
        indice = self._passo if passo is None else passo
        return min(1.0, max(0, indice) / self._passos_ate_critico)

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
        if self._perfil is Perfil.PLANTAO:
            return self._plantao(passo)
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

    def _plantao(self, passo: int) -> SinaisVitais:
        """Modo continuo: deriva a leitura da gravidade ``g`` do passo e aplica ruido pequeno.

        Tres etapas, na ordem: (1) ``g`` avanca um passo do horizonte de escalada; (2) cada
        parametro e interpolado pela sua :class:`_Rampa` entre o extremo saudavel e o extremo grave,
        com o oxigenio suplementar entrando em :data:`_LIMIAR_O2_SUPLEMENTAR` e a consciencia
        degradando de ``A`` para ``V`` em :data:`_LIMIAR_CONSCIENCIA_V`; (3) um ruido
        pseudoaleatorio pequeno em **pressao sistolica e frequencia cardiaca**, para que duas
        leituras consecutivas nunca sejam iguais. O clamp final no :data:`ENVELOPE_PLANTAO` garante
        que nem o ruido nem uma escalada muito curta produzam valor fora da faixa de R6.6.

        Por que o ruido nao entra em FR, SpO2 e temperatura: as faixas do NEWS2 (7.5.1) para esses
        tres sao estreitas (2 rpm, 2 %, 1,0 C) e sao justamente as que empurram o escore para a
        banda seguinte -- um ruido de +-1 ali atravessa a fronteira e faz o escore RECUAR, o que na
        tela vira um card piscando amarelo/verde no meio da escalada. PAS (111-219 -> 0) e FC
        (91-110 -> 1) tem folga larga, entao o numero quase sempre se mexe sem trocar de ponto.
        A amplitude +-2 foi calibrada medindo 60 sementes: com +-4 a banda regredia em 28 pontos
        da serie e com +-2 cai para 11 (49 das 60 sementes ficam com a trilha perfeita), ao custo
        de 0,7% de leituras iguais a anterior. E a mesma decisao ja tomada no modo
        continuo do console (``TRAJETORIA_CONTINUA`` em ``painel.js``) e o mesmo principio que
        :meth:`_deterioracao` aplica: ruido visivel, banda estavel.

        Args:
            passo: Indice da leitura na serie deste leito.

        Returns:
            A leitura do passo, plausivel, reprodutivel e dentro do envelope de seguranca.
        """
        g = self.gravidade(passo)
        fr = int(_limitar(Decimal(round(_FR_PLANTAO.em(g))), "frequencia_respiratoria"))
        saturacao = int(_limitar(Decimal(round(_SATURACAO_PLANTAO.em(g))), "saturacao_o2"))
        temperatura = _limitar(
            Decimal(f"{_TEMPERATURA_PLANTAO.em(g):.1f}"), "temperatura"
        ).quantize(_UM_DECIMO)
        pas = self._inteiro_com_ruido(_PAS_PLANTAO.em(g), 2, "pressao_sistolica")
        fc = self._inteiro_com_ruido(_FC_PLANTAO.em(g), 2, "frequencia_cardiaca")
        return SinaisVitais(
            frequencia_respiratoria=fr,
            saturacao_o2=saturacao,
            oxigenio_suplementar=g >= _LIMIAR_O2_SUPLEMENTAR,
            temperatura=temperatura,
            pressao_sistolica=pas,
            frequencia_cardiaca=fc,
            nivel_consciencia="V" if g >= _LIMIAR_CONSCIENCIA_V else "A",
        )

    # -- ruido do modo continuo ---------------------------------------------- #

    def _inteiro_com_ruido(self, valor: float, amplitude: int, campo: str) -> int:
        """Arredonda ``valor``, soma ruido em ``+-amplitude`` e aplica o envelope de ``campo``."""
        bruto = Decimal(round(valor) + self._rng.randint(-amplitude, amplitude))
        return int(_limitar(bruto, campo))

    def _temperatura_com_ruido(self, valor: float) -> Decimal:
        """Quantiza ``valor`` em uma casa decimal, soma ruido de +-0,1 C e aplica o envelope.

        A quantizacao e obrigatoria: as faixas de temperatura do NEWS2 (7.5.1) sao contiguas apenas
        na resolucao de uma casa, e ``calcular_news2`` levanta ``ValueError`` para 36.05.
        """
        bruto = Decimal(f"{valor:.1f}") + Decimal(self._rng.randint(-1, 1)) * _UM_DECIMO
        return _limitar(bruto, "temperatura").quantize(_UM_DECIMO)
