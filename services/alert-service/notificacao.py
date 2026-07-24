"""Canal de notificacao da equipe do leito, com a falha sabotavel do design 12.5 (R6.5, R10.6).

O canal de notificacao e o unico ponto do sistema que **nao existe de verdade**: nao ha SMS,
*pager* nem sistema hospitalar do outro lado. Para que R6.5 -- "falha de canal vira retentativa" --
seja **demonstravel ao vivo** (design 12.5.1), a simulacao de falha vive aqui, atras de um
``Protocol``, e o handler recebe um :class:`CanalNotificacao` por injecao: ele **nao sabe** qual
implementacao esta do outro lado, nem que existe uma probabilidade de falha configurada (12.5.2).

A sabotagem tem dois escopos, ambos lidos de ambiente (nada e fixado em codigo, design 12.5.3):

* ``ALERT_FAILURE_RATE`` -- probabilidade **global**, todos os leitos (padrao ``0.0``).
* ``ALERT_FAILURE_LEITOS`` + ``ALERT_FAILURE_RATE_LEITOS`` -- probabilidade **por leito** (padrao
  ``UTI-03`` a ``1.0``), o que permite ao ``Cliente_Leito`` disparar o cenario ``falha-consumidor``
  de R10.6 so escolhendo o leito, sem recriar conter.

A sequencia de falhas usa uma instancia **propria** de :class:`random.Random`, semeada por
``SEMENTE_SIMULADOR`` -- nunca o ``random`` global --, para ser **reprodutivel**: mesma semente e
mesma taxa, mesmo alerta falha nas mesmas tentativas (design 12.5.3). Com
``ALERT_FAILURE_RATE_LEITOS=1.0`` a falha independe do sorteio (``random() < 1.0`` e sempre
verdadeiro), entao o cenario e deterministico mesmo com varias replicas do ``alert-service``.

Divergencia registrada (nao silenciosa): o pseudocodigo de 12.5.2 le a taxa de ``Settings`` (secao
12.4) e usa ``alerta.leito_id``/``alerta.equipe_responsavel``. O middleware ``hospitalmq``
-- congelado -- **nao** expoe essas variaveis em ``hospitalmq.config.Settings`` (elas sao
configuracao da aplicacao, nao do middleware), entao sao lidas de ``os.environ`` por
:func:`criar_canal_de_ambiente`; e o schema normativo ``alertas.alertas`` (design 7.4.4) nomeia a
coluna ``leito_codigo`` (nao ``leito_id``) e **nao** tem coluna de equipe, entao o canal usa
``alerta.leito_codigo`` e deriva o destinatario com :func:`equipe_do_leito`.
"""

from __future__ import annotations

import os
import random
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from hospitalmq.logging import get_logger

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from modelos import AlertaClinico

__all__ = [
    "CanalIndisponivelError",
    "CanalNotificacao",
    "CanalNotificacaoSimulado",
    "criar_canal_de_ambiente",
    "equipe_do_leito",
]

_log = get_logger(__name__)

SEMENTE_PADRAO: Final[int] = 20260727
"""Semente padrao do simulador (design 12.4, ``SEMENTE_SIMULADOR``): torna a falha reprodutivel."""

LEITOS_SABOTADOS_PADRAO: Final[str] = "UTI-03"
"""Padrao de ``ALERT_FAILURE_LEITOS`` (design 12.5.4): o leito de P-005, alvo do cenario de DLQ."""

TAXA_LEITOS_PADRAO: Final[float] = 1.0
"""Padrao de ``ALERT_FAILURE_RATE_LEITOS``: falha certa e reprodutivel nos leitos sabotados."""

TAXA_GLOBAL_PADRAO: Final[float] = 0.0
"""Padrao de ``ALERT_FAILURE_RATE``: nada falha fora dos leitos sabotados (estado normal)."""

NOME_CANAL_SIMULADO: Final[str] = "simulado"
"""Identificador gravado na coluna ``canal`` e no evento ``alerta.notificado`` (design 6.4)."""


class CanalIndisponivelError(Exception):
    """O canal de notificacao esta indisponivel: a entrega falhou e deveria ser retentada (R6.5).

    E uma excecao **de dominio do canal**, deliberadamente ignorante do middleware: o adapter nao
    conhece ``hospitalmq.errors.TransientError``. Quem traduz esta falha em ``TransientError``
    -- para acionar a retentativa 1s/2s/4s e a DLQ -- e o handler (design 12.5.2), mantendo o canal
    substituivel sem tocar em regra de mensageria.
    """


def equipe_do_leito(leito_codigo: str) -> str:
    """Deriva o destinatario da notificacao a partir do codigo do leito (design 6.4, R6.4).

    R6.4 nao pede "notificar", pede notificar **a equipe responsavel pelo leito**. Como o schema
    ``alertas.alertas`` (design 7.4.4) nao guarda a equipe, ela e derivada de forma estavel do
    proprio ``leito_codigo`` -- ``UTI-02`` -> ``equipe-UTI-02`` --, o mesmo endereco que o card do
    painel e a linha de log usam para o leito.

    Args:
        leito_codigo: Codigo do ``Leito``, ex. ``"UTI-02"``.

    Returns:
        O identificador da equipe responsavel, ex. ``"equipe-UTI-02"``.
    """
    return f"equipe-{leito_codigo}"


@runtime_checkable
class CanalNotificacao(Protocol):
    """Canal por onde um ``Alerta_Clinico`` chega a equipe responsavel pelo leito (design 12.5.2).

    O handler depende apenas deste ``Protocol``; a troca de canal (simulado, *webhook* real, canal
    falso de teste) e troca de construtor, sem tocar em ``handler.py`` -- a mesma inversao de
    dependencia que a interface ``Transport`` aplica ao broker (design 4.3).
    """

    @property
    def nome(self) -> str:
        """Identificador do canal, gravado na coluna ``canal`` e nos eventos de despacho."""
        ...

    async def notificar(self, alerta: AlertaClinico) -> str:
        """Entrega o ``alerta`` a equipe do leito e devolve o destinatario notificado.

        Raises:
            CanalIndisponivelError: Se o canal estiver indisponivel -- o handler traduz em
                ``TransientError`` para acionar a retentativa (R6.5).
        """
        ...


class CanalNotificacaoSimulado:
    """Canal de demonstracao: falha com probabilidade configurada e semente deterministica (12.5.2).

    A probabilidade tem dois escopos -- uma taxa global e uma taxa aplicada apenas aos leitos de
    ``ALERT_FAILURE_LEITOS`` --, e **nenhum** dos tres valores e fixado em codigo: os tres chegam
    pelo construtor, vindos do ambiente por :func:`criar_canal_de_ambiente`. Usa uma instancia
    propria de :class:`random.Random`, nunca o ``random`` global, para que a sequencia de falhas
    seja reprodutivel.
    """

    __slots__ = ("_nome", "_sabotados", "_sorteio", "_taxa_padrao", "_taxa_sabotada")

    def __init__(
        self,
        *,
        taxa_padrao: float,
        leitos_sabotados: frozenset[str],
        taxa_sabotada: float,
        semente: int,
        nome: str = NOME_CANAL_SIMULADO,
    ) -> None:
        """Constroi o canal simulado.

        Args:
            taxa_padrao: Probabilidade global de falha (``ALERT_FAILURE_RATE``), em ``[0.0, 1.0]``.
            leitos_sabotados: Codigos de leito cuja equipe e atendida por canal sabotado
                (``ALERT_FAILURE_LEITOS``).
            taxa_sabotada: Probabilidade de falha aplicada aos ``leitos_sabotados``
                (``ALERT_FAILURE_RATE_LEITOS``), em ``[0.0, 1.0]``.
            semente: Semente do gerador pseudoaleatorio (``SEMENTE_SIMULADOR``).
            nome: Identificador do canal, gravado na coluna ``canal`` e nos eventos de despacho.
        """
        self._taxa_padrao = taxa_padrao
        self._sabotados = leitos_sabotados
        self._taxa_sabotada = taxa_sabotada
        self._sorteio = random.Random(semente)  # instancia propria, nunca o random global
        self._nome = nome

    @property
    def nome(self) -> str:
        """Identificador do canal, gravado na coluna ``canal`` e nos eventos de despacho."""
        return self._nome

    def _taxa_para(self, leito_codigo: str) -> float:
        """Taxa de falha do leito: a sabotada se ele consta da lista, senao a global."""
        return self._taxa_sabotada if leito_codigo in self._sabotados else self._taxa_padrao

    async def notificar(self, alerta: AlertaClinico) -> str:
        """Entrega o alerta a equipe do leito ou falha conforme a taxa sorteada (design 12.5.2).

        Args:
            alerta: O ``Alerta_Clinico`` a despachar; so ``alerta.leito_codigo`` e lido.

        Returns:
            O destinatario notificado, ``equipe-<leito_codigo>``.

        Raises:
            CanalIndisponivelError: Quando o sorteio cai abaixo da taxa de falha do leito.
        """
        leito = alerta.leito_codigo
        taxa = self._taxa_para(leito)
        if self._sorteio.random() < taxa:
            _log.warning(
                "canal.notificacao.falhou",
                leito_codigo=leito,
                simulado=True,  # marca explicita: nao e defeito, e cenario (design 12.5.3)
                taxa_falha=taxa,
                escopo="leito" if leito in self._sabotados else "global",
            )
            raise CanalIndisponivelError("canal de notificacao indisponivel")
        destinatario = equipe_do_leito(leito)
        _log.info("canal.notificacao.entregue", leito_codigo=leito, destinatario=destinatario)
        return destinatario


def _ler_taxa(variavel: str, padrao: float) -> float:
    """Le uma probabilidade de ambiente, limitada a ``[0.0, 1.0]``; volta ao padrao se invalida.

    Args:
        variavel: Nome da variavel de ambiente.
        padrao: Valor usado quando a variavel esta ausente ou nao e um numero.

    Returns:
        A taxa lida e limitada ao intervalo ``[0.0, 1.0]``.
    """
    bruto = os.environ.get(variavel)
    if bruto is None or bruto.strip() == "":
        return padrao
    try:
        valor = float(bruto)
    except ValueError:
        _log.warning("alert.config_invalida", variavel=variavel, valor=bruto, usando=padrao)
        return padrao
    return min(1.0, max(0.0, valor))


def _ler_leitos(variavel: str, padrao: str) -> frozenset[str]:
    """Le uma lista de codigos de leito separada por virgula; sem entradas vazias.

    Args:
        variavel: Nome da variavel de ambiente.
        padrao: Lista padrao usada quando a variavel esta ausente.

    Returns:
        Conjunto imutavel dos codigos de leito sabotados.
    """
    bruto = os.environ.get(variavel)
    fonte = bruto if bruto is not None else padrao
    return frozenset(item.strip() for item in fonte.split(",") if item.strip())


def _ler_int(variavel: str, padrao: int) -> int:
    """Le um inteiro de ambiente; volta ao padrao se ausente ou invalido.

    Args:
        variavel: Nome da variavel de ambiente.
        padrao: Valor usado quando a variavel esta ausente ou nao e um inteiro.

    Returns:
        O inteiro lido, ou ``padrao``.
    """
    bruto = os.environ.get(variavel)
    if bruto is None or bruto.strip() == "":
        return padrao
    try:
        return int(bruto)
    except ValueError:
        _log.warning("alert.config_invalida", variavel=variavel, valor=bruto, usando=padrao)
        return padrao


def criar_canal_de_ambiente() -> CanalNotificacaoSimulado:
    """Constroi o :class:`CanalNotificacaoSimulado` lendo a sabotagem do ambiente (design 12.5).

    Le ``ALERT_FAILURE_RATE`` (global), ``ALERT_FAILURE_LEITOS`` + ``ALERT_FAILURE_RATE_LEITOS``
    (por leito) e ``SEMENTE_SIMULADOR``, com os padroes do ``.env.example`` (design 12.5.4): nada
    falha fora dos leitos sabotados, e todo alerta de ``UTI-03`` falha com certeza. As variaveis vem
    de ``os.environ`` porque nao pertencem a ``hospitalmq.config.Settings`` -- ver a nota de
    divergencia no modulo.

    Returns:
        O canal simulado pronto para ser injetado no handler.
    """
    return CanalNotificacaoSimulado(
        taxa_padrao=_ler_taxa("ALERT_FAILURE_RATE", TAXA_GLOBAL_PADRAO),
        leitos_sabotados=_ler_leitos("ALERT_FAILURE_LEITOS", LEITOS_SABOTADOS_PADRAO),
        taxa_sabotada=_ler_taxa("ALERT_FAILURE_RATE_LEITOS", TAXA_LEITOS_PADRAO),
        semente=_ler_int("SEMENTE_SIMULADOR", SEMENTE_PADRAO),
    )
