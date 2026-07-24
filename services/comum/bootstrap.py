"""*Bootstrap* comum dos ``Servico_Consumidor``: observabilidade, transporte e Consumer (9.2.2).

O objetivo deste modulo e reduzir o ``main.py`` de cada servico a poucas linhas: configurar log e
metricas uma unica vez, construir o :class:`~hospitalmq.transport.base.Transport` pela *factory* e
devolver um :class:`~hospitalmq.consumer.Consumer` ja ligado ao ``session_factory``, **pronto para
registrar os *handlers***. A semantica de mensageria mora no middleware; o servico so acrescenta os
*handlers* clinicos e as operacoes RPC.

Fluxo tipico de um ``main.py`` (~30 linhas), com o transporte compartilhado entre consumo e RPC::

    from services.comum.app import criar_app
    from services.comum.bootstrap import montar_consumidor, montar_transporte
    from services.comum.db import obter_engine, obter_sessionmaker

    Sessao = obter_sessionmaker()
    transporte = montar_transporte("vitals-service")
    consumidor = montar_consumidor(
        "vitals-service", ["q.vitals.sinais-coletados"],
        transporte=transporte, session_factory=Sessao, schema="vitais",
    )

    @consumidor.on("sinais.coletados", queue="q.vitals.sinais-coletados", modelo=SinaisColetados)
    async def registrar(ctx): ...

    app = criar_app(servico="vitals-service", transporte=transporte,
                    consumidor=consumidor, engine=obter_engine())

Nenhum simbolo especifico de um servico e importado aqui: o *bootstrap* e generico por construcao.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from hospitalmq.config import TOPOLOGIA_PADRAO, criar_transporte, settings
from hospitalmq.consumer import Consumer
from hospitalmq.logging import configure_logging
from hospitalmq.metrics import configure_metrics

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from hospitalmq.config import Settings, TopologySpec
    from hospitalmq.transport.base import Transport

__all__ = [
    "FORMATO_LOG_PADRAO",
    "configurar_observabilidade",
    "montar_consumidor",
    "montar_transporte",
]

FORMATO_LOG_PADRAO: Final[str] = "json"
"""Formato de log padrao: JSON, como no Compose e no ``jq`` unico da demonstracao (R5.3)."""

_observabilidade_configurada: bool = False


def configurar_observabilidade(servico: str, *, formato: str = FORMATO_LOG_PADRAO) -> None:
    """Configura o log estruturado e as metricas do processo, **uma unica vez** (secao 9.2.2).

    Chamada no *boot*, antes de qualquer log. O log estruturado
    (:func:`~hospitalmq.logging.configure_logging`) e idempotente por assinatura e pode ser
    reaplicado sem custo; as metricas (:func:`~hospitalmq.metrics.configure_metrics`), porem,
    **zeram os contadores** a cada chamada, entao sao instaladas so na primeira vez -- reinstala-las
    no meio da execucao apagaria a contagem acumulada que R5.5 exige.

    Args:
        servico: Nome do processo, normalmente ``settings.service_name``. Alimenta o campo
            ``servico`` de toda linha de log e a segmentacao das metricas.
        formato: ``"json"`` no Compose; ``"console"`` apenas em execucao local fora dele.
    """
    global _observabilidade_configurada
    configure_logging(servico, nivel=settings.log_level, formato=formato)
    if not _observabilidade_configurada:
        configure_metrics(servico)
        _observabilidade_configurada = True


def montar_transporte(
    servico: str,
    *,
    cfg: Settings = settings,
    formato: str = FORMATO_LOG_PADRAO,
) -> Transport:
    """Configura a observabilidade e constroi o transporte pela *factory* (R9.2, secao 4.3.4).

    O transporte volta **nao conectado**: quem chama ``connect`` e ``declare_topology`` e o
    *lifespan* da fabrica de aplicacao (:func:`services.comum.app.criar_app`). Nenhum servico
    menciona um transporte concreto -- todos recebem o
    :class:`~hospitalmq.transport.base.Transport` construido a partir de ``TRANSPORTE`` (``amqp`` |
    ``memory`` | ``sqs``), o que faz a suite funcional rodar sem broker com o mesmo codigo de
    producao.

    Args:
        servico: Nome do processo.
        cfg: Configuracao lida do ambiente. Padrao a global :data:`~hospitalmq.config.settings`.
        formato: Formato do log, repassado a :func:`configurar_observabilidade`.

    Returns:
        O transporte construido, pronto para o *lifespan* conectar.
    """
    configurar_observabilidade(servico, formato=formato)
    return criar_transporte(cfg)


def montar_consumidor(
    servico: str,
    filas: Sequence[str] = (),
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    transporte: Transport | None = None,
    prefetch: int | None = None,
    schema: str | None = None,
    exige_identidade: bool = False,
    max_tentativas: int | None = None,
    topologia: TopologySpec = TOPOLOGIA_PADRAO,
    cfg: Settings = settings,
    formato: str = FORMATO_LOG_PADRAO,
) -> Consumer:
    """Instancia o :class:`~hospitalmq.consumer.Consumer` de um servico, pronto para os *handlers*.

    Configura a observabilidade uma vez, constroi o transporte (se nao vier pronto) e devolve o
    ``Consumer`` ligado ao ``session_factory``. Os *handlers* sao registrados **depois**, com
    ``@consumidor.on(...)``; a assinatura das filas so acontece no *startup*, dentro do *lifespan*.

    O argumento ``filas`` e validado contra a ``topologia`` no ato da montagem: uma fila nao
    declarada e defeito de configuracao, e falhar aqui -- e nao na primeira mensagem -- torna o erro
    imediato e obvio no *boot* (secao 6.2). Passe as filas que este servico consome; a lista tambem
    documenta, no proprio ``main.py``, o contrato de consumo do servico.

    Args:
        servico: Nome do processo, ex. ``"vitals-service"``. Compoe a chave de idempotencia e o
            ``producer`` dos eventos derivados.
        filas: Filas de negocio que este servico consome, para validacao antecipada contra a
            topologia. Opcional: as filas efetivas vem dos ``@consumidor.on(...)``.
        session_factory: ``async_sessionmaker`` de :class:`AsyncSession`, tipicamente
            :func:`services.comum.db.obter_sessionmaker`. ``None`` no perfil sem banco.
        transporte: Transporte ja construido por :func:`montar_transporte`, compartilhado com os
            :class:`~hospitalmq.rpc.RpcServer` do mesmo processo. ``None`` constroi um dedicado.
        prefetch: Maximo de mensagens nao confirmadas em voo por replica (R2.6). ``None`` usa
            ``settings.prefetch``.
        schema: *Schema* do banco onde vivem a marca de idempotencia e o outbox, ex. ``"vitais"``.
        exige_identidade: Quando ``True``, mensagem sem identidade vai a DLQ com ``AuthError``
            (R4.4).
        max_tentativas: Teto de retentativas antes da DLQ. ``None`` usa ``settings.max_tentativas``.
        topologia: Topologia contra a qual as ``filas`` sao validadas e de onde saem as
            ``QueueSpec`` da decisao de retentativa. Padrao
            :data:`~hospitalmq.config.TOPOLOGIA_PADRAO`.
        cfg: Configuracao lida do ambiente, usada ao construir o transporte quando ``transporte`` e
            ``None``.
        formato: Formato do log, repassado a :func:`configurar_observabilidade`.

    Returns:
        O :class:`~hospitalmq.consumer.Consumer` pronto para receber os ``@consumidor.on(...)``.

    Raises:
        ConfigError: Se alguma fila de ``filas`` nao constar da ``topologia``.
    """
    configurar_observabilidade(servico, formato=formato)
    _validar_filas(filas, topologia)
    transporte_efetivo = transporte if transporte is not None else criar_transporte(cfg)
    return Consumer(
        service=servico,
        transport=transporte_efetivo,
        session_factory=session_factory,
        prefetch=prefetch,
        schema=schema,
        exige_identidade=exige_identidade,
        max_tentativas=max_tentativas,
        topologia=topologia,
    )


def _validar_filas(filas: Sequence[str], topologia: TopologySpec) -> None:
    """Valida que cada fila consumida esta declarada na topologia (secao 6.2).

    Args:
        filas: Nomes das filas de negocio que o servico pretende consumir.
        topologia: Topologia declarativa de referencia.

    Raises:
        ConfigError: Se alguma fila nao constar da topologia -- ``TopologySpec.fila`` levanta.
    """
    for nome in filas:
        topologia.fila(nome)
