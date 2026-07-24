"""*Transactional Outbox* do ``admission-service`` (design 4.9, 5.6.5) -- R1.3, R2.4, R7.1.

O ``admission-service`` e o **unico** serviço com outbox (D6/ADR-005): perder um evento de admissao
ou alta produziria inconsistencia permanente -- o leito ficaria ocupado no banco e livre em toda
projecao, para sempre (design 4.9.3). Por isso os eventos de dominio sao gravados em
``clinico.outbox_mensagens`` **na mesma transacao** da escrita, e um *relay* assincrono os publica
depois.

Este modulo tem tres responsabilidades:

* :func:`gravar` -- escreve um Envelope-filho em ``clinico.outbox_mensagens`` dentro da transacao
  corrente. E o equivalente, no caminho RPC, do ``ctx.emitir_no_outbox`` do ``Consumer`` (que nao
  existe em :class:`~hospitalmq.rpc.ContextoRpc`); reusa a mesma ``Table`` Core declarada pelo
  middleware em :func:`hospitalmq.publisher.tabela_outbox_mensagens`, com o ``message_id`` gerado na
  gravacao e reusado pelo *relay* (design 4.9.2).
* :class:`FabricaSessaoRpc` -- a ponte que faltava. O :class:`~hospitalmq.rpc.RpcServer` abre a
  transacao de escrita (``async with sessao.begin()``), grava a marca de idempotencia e chama o
  handler passando apenas ``(payload, ctx)`` -- **sem** a ``AsyncSession`` (o ``ContextoRpc``
  congelado nao a expoe). Esta fabrica, passada como ``sessao=`` ao ``RpcServer``, publica a sessao
  da transacao num :class:`~contextvars.ContextVar` que o handler lê por :func:`sessao_rpc_atual`.
  Assim escrita de dominio, outbox e marca de idempotencia compartilham **um** ``COMMIT`` (5.6.5).
* :func:`criar_relay` -- instancia o *relay* do middleware
  (:class:`hospitalmq.publisher.OutboxRelay`) ligado ao *schema* ``clinico``. A logica de *polling*
  ``SELECT ... FOR UPDATE SKIP LOCKED`` e de
  publicacao sob *confirms* mora no middleware (design 4.9.2); aqui so a configuramos.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from modelos import SCHEMA
from sqlalchemy import insert

from hospitalmq.clock import Clock, RealClock
from hospitalmq.envelope import Envelope, Identity
from hospitalmq.publisher import OutboxRelay, Publisher, tabela_outbox_mensagens

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from collections.abc import AsyncIterator, Mapping
    from contextvars import Token
    from typing import Any

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from contextvars import ContextVar

__all__ = [
    "FabricaSessaoRpc",
    "criar_relay",
    "gravar",
    "sessao_rpc_atual",
]

PRODUTOR: str = "admission-service"
"""Nome gravado em ``Envelope.producer`` dos eventos emitidos pelo serviço (design 6.4)."""

_sessao_rpc: ContextVar[AsyncSession | None] = ContextVar("admission_sessao_rpc", default=None)
"""Sessao da transacao de escrita RPC em curso, publicada por :class:`FabricaSessaoRpc`.

Uma variavel de contexto e segura sob concorrencia: cada mensagem RPC e processada em sua propria
*task* asyncio (o ``prefetch`` do ``RpcServer`` permite ate 10 em voo), e o valor definido numa
*task* nao vaza para as demais. O par ``set``/``reset`` de :class:`FabricaSessaoRpc` garante que a
variavel so esta preenchida durante a transacao.
"""


def sessao_rpc_atual() -> AsyncSession:
    """Devolve a :class:`AsyncSession` da transacao de escrita RPC em curso.

    Chamada pelos handlers de escrita para gravar dominio e outbox na mesma transacao da marca de
    idempotencia (design 5.6.5).

    Returns:
        A sessao publicada por :class:`FabricaSessaoRpc`.

    Raises:
        RuntimeError: Se chamada fora de uma operacao de escrita (nenhuma sessao ativa) -- um
            defeito de programacao, nao um erro de dado.
    """
    sessao = _sessao_rpc.get()
    if sessao is None:
        raise RuntimeError(
            "nenhuma sessao RPC ativa: sessao_rpc_atual() so vale dentro de operacao escrita=True"
        )
    return sessao


class FabricaSessaoRpc:
    """Adaptador de ``sessao=`` do :class:`~hospitalmq.rpc.RpcServer` que expoe a sessao ao handler.

    O ``RpcServer`` congelado abre a transacao de escrita com ``async with self._sessao.begin() as
    session`` e chama o handler com ``(payload, ctx)`` apenas -- o ``ContextoRpc`` nao carrega a
    sessao. Esta fabrica substitui o ``async_sessionmaker`` nesse ponto: seu :meth:`begin` cria a
    sessao, abre a transacao unica, publica-a em :data:`_sessao_rpc` para o handler alcanca-la
    por
    :func:`sessao_rpc_atual`, e a remove ao sair -- fazendo dominio + outbox + marca de idempotencia
    caberem no mesmo ``COMMIT`` (design 5.6.5).

    Args:
        sessionmaker: O ``async_sessionmaker`` real do processo (``services.comum.db``).
    """

    __slots__ = ("_sessionmaker",)

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        """Guarda o ``async_sessionmaker`` que cria as sessoes reais."""
        self._sessionmaker = sessionmaker

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[AsyncSession]:
        """Abre a transacao de escrita e publica a sessao para o handler (design 5.6.5).

        Espelha o contrato de ``async_sessionmaker.begin()``: cria a sessao, inicia a transacao,
        entrega a sessao e confirma no fim (``ROLLBACK`` se o bloco levantar). Enquanto o bloco esta
        aberto, :func:`sessao_rpc_atual` devolve esta mesma sessao.

        Yields:
            A :class:`AsyncSession` da transacao, tambem usada pelo ``RpcServer`` para a marca de
            idempotencia.
        """
        async with self._sessionmaker() as session, session.begin():
            token: Token[AsyncSession | None] = _sessao_rpc.set(session)
            try:
                yield session
            finally:
                _sessao_rpc.reset(token)


async def gravar(
    session: AsyncSession,
    *,
    tipo: str,
    payload: Mapping[str, Any],
    correlation_id: str,
    causation_id: str | None,
    identity: Identity | None,
    version: int = 1,
    clock: Clock | None = None,
) -> Envelope:
    """Grava um evento de dominio em ``clinico.outbox_mensagens`` dentro da transacao corrente.

    Constroi o Envelope-filho preservando a cadeia de correlacao e causalidade (R5.3) -- novo
    ``message_id``, mesmo ``correlation_id`` do fluxo, ``causation_id`` igual ao ``message_id`` da
    requisicao RPC, ``identity`` herdada de quem originou a acao (R4.3/R7.1) -- e o insere na tabela
    do outbox. O ``message_id`` gravado e reusado pelo *relay* na publicacao, o que faz uma
    republicacao apos queda ser suprimida pela idempotencia dos consumidores (design 4.9.2).

    Args:
        session: A transacao corrente (a mesma da escrita de dominio e da marca de idempotencia).
        tipo: Tipo/routing key do evento, ex. ``"paciente.admitido"``.
        payload: Corpo do evento, ja em tipos nativos de JSON (str/num/bool/None/listas/dicts).
        correlation_id: Correlacao de ponta a ponta, herdada da requisicao RPC.
        causation_id: ``message_id`` da requisicao RPC que causou este evento.
        identity: Quem originou a acao, propagada para a Trilha_de_Auditoria.
        version: Versao do *schema* do payload (design 4.2.4).
        clock: Fonte de tempo para o ``timestamp`` do Envelope. Padrao :class:`RealClock`.

    Returns:
        O Envelope-filho gravado (com o ``message_id`` que o *relay* publicara).
    """
    relogio = clock if clock is not None else RealClock()
    filho = Envelope(
        message_id=str(uuid.uuid4()),
        correlation_id=correlation_id,
        causation_id=causation_id,
        type=tipo,
        version=version,
        timestamp=relogio.now(),
        producer=PRODUTOR,
        identity=identity,
        attempt=1,
        payload=dict(payload),
    )
    tabela = tabela_outbox_mensagens(schema=SCHEMA)
    await session.execute(
        insert(tabela).values(
            message_id=uuid.UUID(filho.message_id),
            tipo=filho.type,
            routing_key=filho.type,
            envelope=filho.to_dict(),
        )
    )
    return filho


def criar_relay(
    *,
    publisher: Publisher,
    session_factory: async_sessionmaker[AsyncSession],
    clock: Clock | None = None,
) -> OutboxRelay:
    """Instancia o *relay* do outbox ligado ao *schema* ``clinico`` (design 4.9.2).

    A mecanica -- ``SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100`` a cada 200 ms,
    ``publish_envelope`` sob *publisher confirms* e ``UPDATE publicado_em`` -- mora inteira no
    middleware; aqui so a
    configuramos. O ``session_factory`` deve ser o ``async_sessionmaker`` **real** (nao a
    :class:`FabricaSessaoRpc`): o *relay* abre sua propria transacao por ciclo.

    Args:
        publisher: O :class:`Publisher` do serviço, que republica cada Envelope pendente.
        session_factory: O ``async_sessionmaker`` real do processo.
        clock: Fonte de tempo injetada; alimenta ``publicado_em`` e a espera entre ciclos.

    Returns:
        O :class:`~hospitalmq.publisher.OutboxRelay` pronto para o *lifespan* rodar.
    """
    return OutboxRelay(
        publisher=publisher,
        session_factory=session_factory,
        schema=SCHEMA,
        clock=clock,
    )
