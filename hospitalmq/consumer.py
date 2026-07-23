"""O *pipeline* por mensagem: ``Consumer`` e ``MessageContext`` (secao 4.5, R2.1/R2.5/R2.6/R4.6).

**Este e o arquivo que demonstra que a semantica de mensageria e autoral, e nao do RabbitMQ.** O
broker entrega bytes e para por ai (4.10); tudo o que caracteriza *middleware* -- desserializar
o Envelope, vincular a correlacao ao log, resolver a identidade, suprimir a duplicata na **mesma
transacao** do efeito, validar o payload, executar o handler, publicar os eventos derivados e so
entao confirmar, com a retentativa exponencial e o encaminhamento enriquecido a DLQ nas bordas -- e
codigo deste modulo.

O *pipeline* por mensagem, na ordem normativa da secao 4.5.4::

    from_bytes -> contexto de log -> log recebida -> resolver identidade -> resolver handler
        -> BEGIN -> marcar idempotencia -> validar payload -> handler -> COMMIT
        -> publicar derivados -> ACK

com dois desvios nas bordas: erro **permanente** (``PermanentError``, ``ValidationError``,
``AuthError``) vai direto a DLQ sem gastar retentativa; erro **transitorio** ou nao classificado
consome a escada de espera 1s/2s/4s e so entao vai a DLQ (secoes 4.6 e 4.7). O ``ACK`` fica **fora**
da transacao e **so acontece apos o sucesso** (R2.1): se o processo morrer entre o ``COMMIT`` e o
``ACK``, a reentrega encontra a marca de idempotencia ja commitada, detecta a duplicata e confirma
sem reexecutar -- a janela existe e e benigna por construcao (secao 4.5.4, observacao 1).

O :class:`MessageContext` e o mecanismo que torna dificil escrever um handler errado: ``ctx.emitir``
e ``ctx.emitir_no_outbox`` ja propagam correlacao e causalidade (R5.3), o ``ctx.log`` ja carrega os
identificadores (R5.1), a ``ctx.session`` ja e a transacao certa (secao 4.8.3) e a ``ctx.identity``
ja veio do Envelope (R4.6). O :class:`~hospitalmq.publisher.Publisher` **nao** e exposto ao handler:
um ``ctx.publisher.publish`` no meio da transacao seria a escrita dupla na forma mais perigosa
(secao 4.9.1), e remover o objeto torna o erro inexprimivel.
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from hospitalmq.auth import Identity, identidade_do_envelope
from hospitalmq.clock import Clock, RealClock
from hospitalmq.config import (
    TOPOLOGIA_PADRAO,
    QueueSpec,
    settings,
)
from hospitalmq.envelope import Envelope
from hospitalmq.errors import (
    AuthError,
    ConfigError,
    InvalidEnvelopeError,
    PermanentError,
    TransportError,
)
from hospitalmq.idempotency import (
    IdempotencyStore,
    MemoryIdempotencyStore,
    SessaoTransacional,
    consumidor_de,
)
from hospitalmq.logging import LogEvent, Logger, contexto_de_log, get_logger
from hospitalmq.metrics import Metrics, get_metrics
from hospitalmq.publisher import Publisher
from hospitalmq.retry import (
    MOTIVO_ENVELOPE_INVALIDO,
    MensagemDlq,
    PoliticaRetentativa,
    tentativas_restantes,
)
from hospitalmq.transport.base import InboundMessage

if TYPE_CHECKING:  # pragma: no cover - evita importacoes desnecessarias em tempo de execucao
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence

    from hospitalmq.config import TopologySpec
    from hospitalmq.transport.base import Subscription, Transport

    FabricaDeSessao = Callable[[], Any]
    """Fabrica de :class:`AsyncSession` (``async_sessionmaker``), ou ``None`` no perfil "borda"."""

    Handler = Callable[["MessageContext[Any]"], Awaitable[None]]

__all__ = ["Consumer", "MessageContext"]


# --------------------------------------------------------------------------- #
# Sessao sentinela do modo sem banco                                            #
# --------------------------------------------------------------------------- #


class _SessaoEmMemoria:
    """Sentinela de transacao para o modo sem banco (``api-gateway`` e testes de unidade).

    Serve de identidade estavel para o :class:`~hospitalmq.idempotency.MemoryIdempotencyStore`, que
    enquadra a transacao por ``id(session)``. Uma instancia nova por mensagem garante que as marcas
    provisorias de mensagens distintas nunca colidam. Tentar executar SQL nela falha alto -- nao ha
    banco a que recorrer.
    """

    __slots__ = ()

    async def execute(self, *_args: object, **_kwargs: object) -> Any:  # noqa: ANN401
        """Recusa qualquer SQL: este ``Consumer`` roda sem banco.

        Raises:
            ConfigError: Sempre. Forneca ``session_factory`` para dispor de ``ctx.session``.
        """
        raise ConfigError(
            "este Consumer roda sem banco (session_factory ausente): nao ha sessao para SQL",
            causa="sessao_em_memoria",
        )


# --------------------------------------------------------------------------- #
# Contexto entregue ao handler (secao 4.5.2)                                    #
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class MessageContext[P]:
    """Tudo o que o handler recebe -- e o unico objeto com que ele interage (secao 4.5.2).

    Attributes:
        envelope: O Envelope recebido, imutavel.
        payload: O ``payload`` ja validado pelo ``modelo`` declarado em ``@consumer.on`` -- objeto
            Pydantic tipado quando ha modelo, o ``dict`` cru quando nao ha.
        identity: Quem originou a acao, lido do Envelope sem nova consulta ao auth (R4.6).
        session: A **mesma** transacao da marca de idempotencia (secao 4.8.3). ``AsyncSession`` no
            perfil "dominio"; sentinela sem banco no perfil "borda".
        log: Logger estruturado ja vinculado a ``correlation_id`` e ``message_id`` (R5.1).
        producer: Nome do servico, usado no campo ``producer`` dos eventos derivados.
        attempt: Numero da tentativa corrente, ``>= 1``.
        tentativas_restantes: Quantas retentativas ainda cabem antes da DLQ (R2.2).
    """

    envelope: Envelope
    payload: P
    identity: Identity | None
    session: SessaoTransacional
    log: Logger
    producer: str
    attempt: int
    tentativas_restantes: int
    _schema: str | None = None
    _derivados: list[Envelope] = field(default_factory=list, repr=False)

    def emitir(self, tipo: str, payload: Mapping[str, Any], *, version: int = 1) -> Envelope:
        """Enfileira um evento derivado para publicacao **apos o ``COMMIT``** (secao 4.5.2).

        Nao faz E/S: apenas deriva o Envelope-filho -- novo ``message_id``, **mesmo**
        ``correlation_id``, ``causation_id`` igual ao ``message_id`` desta mensagem, ``identity``
        herdada (R5.3) -- e o acumula. Quem publica e o ``Consumer``, depois do ``COMMIT``, antes do
        ``ack``. Usado pelos servicos de telemetria, cujo fluxo e auto-corretivo (secao 4.9.3).

        Args:
            tipo: Tipo do evento-filho, ex. ``"sinais.registrados"``. Vira a *routing key*.
            payload: Corpo do evento-filho, serializavel em JSON.
            version: Versao do *schema* do ``payload`` do filho (secao 4.2.4).

        Returns:
            O Envelope-filho derivado e enfileirado.
        """
        filho = self.envelope.derivar(
            tipo=tipo, payload=payload, producer=self.producer, version=version
        )
        self._derivados.append(filho)
        return filho

    async def emitir_no_outbox(
        self, tipo: str, payload: Mapping[str, Any], *, version: int = 1
    ) -> Envelope:
        """Grava o evento derivado em ``outbox_mensagens`` **dentro** da transacao (secao 4.5.2).

        Usado pelo ``admission-service``, onde perder o evento e irreversivel (secao 4.9.3): como o
        Envelope e gravado na mesma transacao do efeito de dominio, "persistiu => publicara". O
        ``message_id`` e gerado aqui e reusado pelo *relay*, o que garante que uma republicacao apos
        queda seja suprimida pela idempotencia dos consumidores (secao 4.9.2).

        Args:
            tipo: Tipo do evento-filho, ex. ``"paciente.admitido"``.
            payload: Corpo do evento-filho, serializavel em JSON.
            version: Versao do *schema* do ``payload`` do filho.

        Returns:
            O Envelope-filho gravado no outbox.

        Raises:
            ConfigError: Se o extra ``dominio`` (SQLAlchemy) nao estiver instalado, ou se este
                ``Consumer`` rodar sem ``session_factory``.
        """
        from hospitalmq.publisher import _sqlalchemy, tabela_outbox_mensagens

        filho = self.envelope.derivar(
            tipo=tipo, payload=payload, producer=self.producer, version=version
        )
        sa = _sqlalchemy()
        tabela = tabela_outbox_mensagens(schema=self._schema)
        await self.session.execute(
            sa.insert(tabela).values(
                message_id=uuid.UUID(filho.message_id),
                tipo=filho.type,
                routing_key=filho.type,
                envelope=filho.to_dict(),
            )
        )
        self._derivados.append(filho)
        return filho

    @property
    def derivados(self) -> Sequence[Envelope]:
        """Os eventos derivados acumulados por :meth:`emitir`, na ordem de emissao."""
        return tuple(self._derivados)


# --------------------------------------------------------------------------- #
# Registro de handler                                                           #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class _Registro:
    """Um handler registrado por ``@consumer.on``, com sua configuracao de despacho."""

    handler: Handler
    queue: str
    tipo: str
    version: int
    modelo: type[BaseModel] | None
    idempotente: bool
    max_tentativas: int | None


# --------------------------------------------------------------------------- #
# Consumer                                                                       #
# --------------------------------------------------------------------------- #


class Consumer:
    """Consome filas e roda o *pipeline* por mensagem da secao 4.5.4 (R2.1, R2.5, R2.6).

    Os handlers sao registrados de forma declarativa por :meth:`on`, o que constroi o mapa
    ``(type, version) -> handler`` em tempo de importacao e mantem cada handler com assinatura de
    funcao comum, testavel isoladamente sem broker (secao 4.5.1). :meth:`run` assina cada fila
    distinta, mantendo no maximo ``prefetch`` mensagens nao confirmadas em voo (R2.6); bloqueia ate
    o encerramento gracioso.
    """

    __slots__ = (
        "_assinaturas",
        "_clock",
        "_exige_identidade",
        "_filas_por_fila",
        "_handlers",
        "_log",
        "_max_tentativas",
        "_metrics",
        "_parar",
        "_politica",
        "_politicas_por_max",
        "_prefetch",
        "_publisher",
        "_schema",
        "_service",
        "_session_factory",
        "_store",
        "_topologia",
        "_transport",
    )

    def __init__(
        self,
        *,
        service: str,
        transport: Transport,
        session_factory: FabricaDeSessao | None = None,
        prefetch: int | None = None,
        store: IdempotencyStore | None = None,
        publisher: Publisher | None = None,
        topologia: TopologySpec = TOPOLOGIA_PADRAO,
        schema: str | None = None,
        max_tentativas: int | None = None,
        exige_identidade: bool = False,
        clock: Clock | None = None,
        logger: Logger | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        """Constroi o consumidor de um servico.

        Args:
            service: Nome do processo, ex. ``"vitals-service"``. Compoe a chave de idempotencia
                ``"<servico>:<fila>"`` e o campo ``producer`` dos eventos derivados.
            transport: O :class:`~hospitalmq.transport.base.Transport` ja conectado.
            session_factory: ``async_sessionmaker`` de ``AsyncSession``. ``None`` no perfil
                "borda": a idempotencia usa o :class:`MemoryIdempotencyStore`
                e ``ctx.session`` e a sentinela sem banco.
            prefetch: Limite de mensagens nao confirmadas em voo por replica (R2.6). ``None`` usa
                ``settings.prefetch``.
            store: Registro de idempotencia. ``None`` escolhe :class:`SqlIdempotencyStore` quando ha
                ``session_factory`` e :class:`MemoryIdempotencyStore` caso contrario.
            publisher: Publicador dos eventos derivados. ``None`` constroi um sobre ``transport``.
            topologia: Topologia de onde saem as ``QueueSpec`` para a decisao de retentativa. Padrao
                :data:`~hospitalmq.config.TOPOLOGIA_PADRAO`.
            schema: *Schema* do banco (idempotencia e outbox). ``None`` usa o padrao da conexao.
            max_tentativas: Teto global de retentativas. ``None`` usa ``settings.max_tentativas``.
            exige_identidade: Quando ``True``, uma mensagem sem ``identity`` vai a DLQ com
                :class:`~hospitalmq.errors.AuthError` (R4.4).
            clock: Relogio injetavel (secao 10.3.1).
            logger: Logger estruturado; padrao :func:`~hospitalmq.logging.get_logger`.
            metrics: Registro de metricas; padrao o singleton de :func:`~hospitalmq.metrics.
                get_metrics`.
        """
        self._service: str = service
        self._transport: Transport = transport
        self._session_factory: FabricaDeSessao | None = session_factory
        self._prefetch: int = prefetch if prefetch is not None else settings.prefetch
        self._topologia: TopologySpec = topologia
        self._schema: str | None = schema
        self._max_tentativas: int = (
            max_tentativas if max_tentativas is not None else settings.max_tentativas
        )
        self._exige_identidade: bool = exige_identidade
        self._clock: Clock = clock if clock is not None else RealClock()
        self._log: Logger = logger if logger is not None else get_logger(__name__)
        self._metrics: Metrics = metrics if metrics is not None else get_metrics()

        self._store: IdempotencyStore = (
            store if store is not None else self._store_padrao(schema, self._clock)
        )
        self._publisher: Publisher = (
            publisher
            if publisher is not None
            else Publisher(
                transport=transport,
                producer=service,
                clock=self._clock,
                logger=self._log,
                metrics=self._metrics,
            )
        )
        self._politica: PoliticaRetentativa = PoliticaRetentativa(
            servico=service, max_tentativas=self._max_tentativas, clock=self._clock
        )
        self._politicas_por_max: dict[int, PoliticaRetentativa] = {
            self._max_tentativas: self._politica
        }

        self._handlers: dict[tuple[str, int], _Registro] = {}
        self._filas_por_fila: dict[str, set[tuple[str, int]]] = {}
        self._assinaturas: list[Subscription] = []
        self._parar: Any = None  # asyncio.Event criado sob demanda em run()

    def _store_padrao(self, schema: str | None, clock: Clock) -> IdempotencyStore:
        """Escolhe o registro de idempotencia conforme haja ou nao banco (secao 4.8)."""
        if self._session_factory is not None:
            from hospitalmq.idempotency import SqlIdempotencyStore

            return SqlIdempotencyStore(schema=schema, clock=clock)
        return MemoryIdempotencyStore(clock=clock)

    # -- registro declarativo (secao 4.5.1) -------------------------------- #

    def on(
        self,
        tipo: str,
        *,
        queue: str,
        modelo: type[BaseModel] | None = None,
        version: int = 1,
        idempotente: bool = True,
        max_tentativas: int | None = None,
    ) -> Callable[[Handler], Handler]:
        """*Decorator* que registra o handler de um ``(tipo, version)`` numa fila (secao 4.5.1).

        O handler devolvido e o **mesmo** recebido, sem embrulho -- fica com assinatura de funcao
        comum, testavel isoladamente sem broker. O registro em tempo de importacao permite ao
        ``Consumer`` validar na inicializacao que toda fila assinada tem handler.

        Args:
            tipo: Tipo do evento tratado, ex. ``"sinais.coletados"``.
            queue: Fila de onde as mensagens desse tipo chegam, ex. ``"q.vitals.sinais-coletados"``.
            modelo: Modelo Pydantic que valida o ``payload`` antes do handler. ``ValidationError`` e
                traduzido em erro permanente e vai direto a DLQ (R6.6, 4.5.1). ``None`` entrega
                o ``dict`` cru.
            version: Versao do *schema* do ``payload`` tratada por este handler (secao 4.2.4).
            idempotente: Quando ``True`` (padrao), a mensagem passa pela marca ``(consumidor,
                message_id)`` antes do handler (R2.4).
            max_tentativas: Teto de retentativas especifico deste handler. ``None`` usa o global.

        Returns:
            O *decorator* que registra e devolve o handler inalterado.
        """

        def registrar(handler: Handler) -> Handler:
            chave = (tipo, version)
            if chave in self._handlers:
                raise ConfigError(
                    f"handler duplicado para (tipo={tipo!r}, version={version})",
                    tipo=tipo,
                    version=version,
                )
            self._handlers[chave] = _Registro(
                handler=handler,
                queue=queue,
                tipo=tipo,
                version=version,
                modelo=modelo,
                idempotente=idempotente,
                max_tentativas=max_tentativas,
            )
            self._filas_por_fila.setdefault(queue, set()).add(chave)
            return handler

        return registrar

    # -- ciclo de vida ----------------------------------------------------- #

    async def iniciar(self) -> None:
        """Assina todas as filas registradas e comeca a receber (secao 4.5.3).

        Cada fila distinta e assinada uma unica vez; o despacho por ``(type, version)`` acontece
        dentro de :meth:`_processar`. Idempotente: chamar duas vezes nao duplica assinaturas.

        Raises:
            ConfigError: Se nenhum handler foi registrado.
            TransportError: Se uma fila nao estiver declarada no broker.
        """
        if not self._handlers:
            raise ConfigError("Consumer sem handler registrado: use @consumer.on(...)")
        if self._assinaturas:
            return
        for fila in sorted(self._filas_por_fila):
            assinatura = await self._transport.consume(
                queue=fila, handler=self._processar, prefetch=self._prefetch
            )
            self._assinaturas.append(assinatura)

    async def run(self) -> None:
        """Assina as filas e bloqueia ate :meth:`parar` (secao 4.5.3).

        E o ponto de entrada do processo consumidor. O ``main`` do servico o aguarda; um manipulador
        de ``SIGTERM`` chama :meth:`parar` no encerramento gracioso, que interrompe este ``await``.
        """
        self._parar = asyncio.Event()
        await self.iniciar()
        await self._parar.wait()
        await self.parar()

    async def parar(self) -> None:
        """Cancela as assinaturas e sinaliza o fim do laco de :meth:`run` (secao 4.5.3).

        Cancelar interrompe a entrega de **novas** mensagens sem descartar as que ja estao em voo --
        estas ainda serao confirmadas ou devolvidas a fila por ``Transport.close`` no encerramento.
        Idempotente.
        """
        for assinatura in self._assinaturas:
            await assinatura.cancel()
        self._assinaturas.clear()
        if self._parar is not None:
            self._parar.set()

    # -- o pipeline por mensagem (secao 4.5.4) ----------------------------- #

    async def _processar(self, raw: InboundMessage) -> None:
        """Executa o *pipeline* completo de uma mensagem entregue (secao 4.5.4).

        **Nunca levanta**: o contrato de ``Transport.consume`` diz que quem trata erro e este
        *pipeline*. Toda saida termina em ``ack`` (sucesso, duplicata ou DLQ) ou em ``ack`` do
        original apos republicar na fila de espera (retentativa) -- exceto quando a **propria
        publicacao na DLX/fila de espera** falha por ``TransportError``, caso em que a mensagem
        permanece nao confirmada e sera reentregue, que e a rede de seguranca de R1.5/R2.3.
        """
        inicio = self._clock.monotonic()

        try:
            envelope = Envelope.from_bytes(raw.body)
        except (InvalidEnvelopeError, ValueError) as exc:
            # Mensagem-veneno: nem ha message_id para deduplicar nem proveito em retentar (4.7.1).
            await self._para_dlq(
                raw, None, motivo=MOTIVO_ENVELOPE_INVALIDO, exc=exc, corpo=raw.body, inicio=inicio
            )
            return

        consumidor = consumidor_de(self._service, raw.queue)
        with contexto_de_log(
            correlation_id=envelope.correlation_id,
            causation_id=envelope.causation_id,
            message_id=envelope.message_id,
            servico=self._service,
            tipo=envelope.type,
            tentativa=envelope.attempt,
            fila=raw.queue,
        ):
            self._log.debug(
                LogEvent.MENSAGEM_RECEBIDA,
                fila=raw.queue,
                tipo=envelope.type,
                tentativa=envelope.attempt,
            )
            self._metrics.inc("mensagens_consumidas", tipo=envelope.type)

            registro: _Registro | None = None
            try:
                identity = self._resolver_identidade(envelope)
                registro = self._resolver_handler(envelope)

                duplicada = False
                ctx: MessageContext[Any] | None = None
                async with self._abrir_sessao() as session, self._enquadrar(session):
                    if registro.idempotente:
                        novo = await self._store.marcar(session, envelope, consumidor)
                    else:
                        novo = True
                    if not novo:
                        duplicada = True
                    else:
                        ctx = self._montar_contexto(envelope, registro, identity, session)
                        await registro.handler(ctx)
                # COMMIT aqui: efeito de dominio + marca de idempotencia, atomicos.

                if duplicada:
                    self._log.info(
                        LogEvent.MENSAGEM_DUPLICADA, tipo=envelope.type, resultado="duplicada"
                    )
                    self._metrics.inc("mensagens_duplicadas", tipo=envelope.type)
                    await self._transport.ack(raw)  # R2.4: confirma sem reexecutar
                    return

                await self._drenar_derivados(ctx)
                await self._transport.ack(raw)  # R2.1: ACK so apos o sucesso
                duracao_ms = (self._clock.monotonic() - inicio) * 1000.0
                self._metrics.observe_duracao(f"handler.{envelope.type}", duracao_ms)
                self._log.info(
                    LogEvent.MENSAGEM_PROCESSADA,
                    fila=raw.queue,
                    tipo=envelope.type,
                    tentativa=envelope.attempt,
                    duracao_ms=round(duracao_ms, 2),
                    resultado="sucesso",
                )

            except (PermanentError, ValidationError, AuthError) as exc:
                await self._para_dlq(raw, envelope, motivo=None, exc=exc, inicio=inicio)
            except Exception as exc:  # inclui TransientError e o nao classificado (secao 4.5.4)
                await self._retentar_ou_dlq(raw, envelope, exc, registro, inicio)

    def _montar_contexto(
        self,
        envelope: Envelope,
        registro: _Registro,
        identity: Identity | None,
        session: SessaoTransacional,
    ) -> MessageContext[Any]:
        """Valida o payload e monta o :class:`MessageContext` entregue ao handler (secao 4.5.2)."""
        payload: Any
        if registro.modelo is not None:
            payload = registro.modelo.model_validate(envelope.payload)
        else:
            payload = envelope.payload
        max_t = (
            registro.max_tentativas if registro.max_tentativas is not None else self._max_tentativas
        )
        return MessageContext(
            envelope=envelope,
            payload=payload,
            identity=identity,
            session=session,
            log=self._log,
            producer=self._service,
            attempt=envelope.attempt,
            tentativas_restantes=tentativas_restantes(envelope.attempt, max_tentativas=max_t),
            _schema=self._schema,
        )

    def _resolver_identidade(self, envelope: Envelope) -> Identity | None:
        """Le a identidade do Envelope e exige-a quando a fila a exige (R4.6, secao 4.5.4).

        Raises:
            AuthError: ``credencial_ausente`` quando ``exige_identidade`` e a mensagem nao traz
                identidade -- vai direto a DLQ, sem retentativa, com log ``warning`` (R4.4).
        """
        identidade = identidade_do_envelope(envelope)
        if self._exige_identidade and identidade is None:
            raise AuthError(
                "mensagem sem identidade em fila que a exige",
                motivo="credencial_ausente",
                fila=envelope.type,
            )
        return identidade

    def _resolver_handler(self, envelope: Envelope) -> _Registro:
        """Encontra o handler de ``(type, version)`` ou levanta erro permanente (secao 4.2.4).

        Raises:
            PermanentError: Se nao houver handler para o par -- ``version`` sem handler registrado
                falharia identicamente em toda retentativa, entao vai direto a DLQ (secao 4.7.1).
        """
        registro = self._handlers.get((envelope.type, envelope.version))
        if registro is None:
            raise PermanentError(
                f"sem handler para (tipo={envelope.type!r}, version={envelope.version})",
                codigo="OPERACAO_DESCONHECIDA",
                tipo=envelope.type,
                version=envelope.version,
            )
        return registro

    async def _drenar_derivados(self, ctx: MessageContext[Any] | None) -> None:
        """Publica, **apos o ``COMMIT``**, os Envelopes acumulados por ``ctx.emitir`` (secao 4.5.4).

        A falha em publicar um derivado **nao** vira retentativa: o efeito de negocio ja esta
        commitado e a marca de idempotencia ja suprimiria a reexecucao, entao republicar a mensagem
        original seria inutil (secao 4.5.4, observacao 5). Registra em nivel ``error`` e conta a
        ocorrencia -- a janela e observavel, nao invisivel (secao 4.9.3).
        """
        if ctx is None:
            return
        for filho in ctx.derivados:
            try:
                await self._publisher.publish_envelope(filho)
            except TransportError as exc:
                self._log.error(
                    "evento.derivado_nao_publicado",
                    tipo=filho.type,
                    message_id_filho=filho.message_id,
                    erro=type(exc).__name__,
                    detalhe=str(exc),
                )
                self._metrics.inc("derivados_nao_publicados", tipo=filho.type)

    # -- retentativa e DLQ (secoes 4.6 e 4.7) ------------------------------ #

    async def _retentar_ou_dlq(
        self,
        raw: InboundMessage,
        envelope: Envelope,
        exc: BaseException,
        registro: _Registro | None,
        inicio: float,
    ) -> None:
        """Decide entre republicar na fila de espera e encaminhar a DLQ (secoes 4.6.1 e 4.7.1).

        Na retentativa, republica ``envelope.para_retentativa()`` -- com ``attempt`` incrementado e
        ``message_id`` **preservado** (secao 4.6.3) -- na fila de espera do nivel correspondente e
        confirma a entrega original; o atraso e cumprido pelo TTL da fila de espera, nunca por
        ``asyncio.sleep`` (secao 4.6.2). Esgotado o orcamento, publica o envelope enriquecido em
        ``hospital.dlx`` e confirma o original.
        """
        fila = self._spec_da_fila(raw.queue)
        politica = self._politica_para(registro)
        decisao = politica.decidir(fila=fila, attempt=envelope.attempt, exc=exc)
        duracao_ms = round((self._clock.monotonic() - inicio) * 1000.0, 2)

        if decisao.retentar:
            retentativa = envelope.para_retentativa()
            await self._transport.publish(
                destination=decisao.exchange,
                routing_key=decisao.routing_key,
                body=retentativa.to_bytes(),
                headers=retentativa.to_headers(),
                message_id=retentativa.message_id,
                correlation_id=retentativa.correlation_id,
                persistent=True,
            )
            await self._transport.ack(raw)
            self._metrics.inc("mensagens_retentadas", tipo=envelope.type)
            self._log.warning(
                LogEvent.MENSAGEM_RETENTATIVA,
                tentativa=envelope.attempt,
                espera_ms=decisao.espera_ms,
                erro=type(exc).__name__,
                detalhe=str(exc),
                duracao_ms=duracao_ms,
                resultado="retentativa",
            )
            return

        dlq = politica.montar_dlq(
            fila=fila,
            exc=exc,
            envelope=envelope,
            motivo=decisao.motivo,
            tentativas=envelope.attempt,
        )
        await self._publicar_dlq(dlq, envelope)
        await self._transport.ack(raw)
        self._metrics.inc("mensagens_dlq", tipo=envelope.type)
        campos: dict[str, Any] = {
            "fila_dlq": dlq.routing_key,
            "erro": type(exc).__name__,
            "detalhe": str(exc),
            "duracao_ms": duracao_ms,
            "resultado": "dlq",
            "tentativa": envelope.attempt,
        }
        if decisao.tentativas_esgotadas is not None:
            campos["tentativas_esgotadas"] = decisao.tentativas_esgotadas
        self._log.error(LogEvent.MENSAGEM_DLQ, **campos)

    async def _para_dlq(
        self,
        raw: InboundMessage,
        envelope: Envelope | None,
        *,
        motivo: str | None,
        exc: BaseException,
        corpo: bytes | None = None,
        inicio: float | None = None,
    ) -> None:
        """Publica o envelope enriquecido em ``hospital.dlx`` e confirma o original (secao 4.7).

        Caminho **sem gastar retentativa**: erro permanente do handler, payload invalido, identidade
        ausente ou mensagem-veneno (``envelope=None``). O envelope original e preservado byte a byte
        sob a chave ``envelope`` e o diagnostico (motivo, excecao, ``timestamp``, servico)
        vai no objeto irmao ``falha`` (secao 4.7.2).
        """
        fila = self._spec_da_fila(raw.queue)
        dlq = self._politica.montar_dlq(
            fila=fila, exc=exc, envelope=envelope, motivo=motivo, corpo_original=corpo
        )
        await self._publicar_dlq(dlq, envelope)
        await self._transport.ack(raw)

        tipo = envelope.type if envelope is not None else "_sem_tipo"
        self._metrics.inc("mensagens_dlq", tipo=tipo)

        if isinstance(exc, AuthError):
            # R4.4: credencial invalida e operacao normal -- registra tambem em warning (secao 9.4).
            self._log.warning(
                LogEvent.AUTH_NEGADA,
                motivo=exc.motivo,
                identity_sub=(
                    envelope.identity.sub
                    if envelope is not None and envelope.identity is not None
                    else None
                ),
            )
        campos: dict[str, Any] = {
            "fila_dlq": dlq.routing_key,
            "erro": type(exc).__name__,
            "detalhe": str(exc),
            "resultado": "dlq",
            "tentativa": envelope.attempt if envelope is not None else 0,
            "motivo": dlq.motivo,
        }
        if inicio is not None:
            campos["duracao_ms"] = round((self._clock.monotonic() - inicio) * 1000.0, 2)
        self._log.error(LogEvent.MENSAGEM_DLQ, **campos)

    async def _publicar_dlq(self, dlq: MensagemDlq, envelope: Envelope | None) -> None:
        """Publica a :class:`~hospitalmq.retry.MensagemDlq` em ``hospital.dlx`` (secao 4.7.1).

        A *routing key* e ``<fila>.dlq`` e casa com o *binding* declarado; se nao casasse, o
        ``mandatory=True`` do driver a viraria ``TransportError``, e o ``ack`` original nunca
        aconteceria e a mensagem entraria em reentrega perpetua (secao 4.4.3). Por isso esta falha
        **nao** e silenciada: ela propaga e a entrega original fica nao confirmada.
        """
        await self._transport.publish(
            destination=dlq.exchange,
            routing_key=dlq.routing_key,
            body=dlq.to_bytes(),
            headers=dlq.headers(),
            message_id=envelope.message_id if envelope is not None else str(uuid.uuid4()),
            correlation_id=envelope.correlation_id if envelope is not None else str(uuid.uuid4()),
            persistent=True,
        )

    # -- auxiliares -------------------------------------------------------- #

    def _spec_da_fila(self, queue: str) -> QueueSpec | str:
        """Recupera a ``QueueSpec`` da topologia, ou devolve o nome cru se a fila nao consta.

        Passar a :class:`QueueSpec` a politica e o caminho preferido: os nomes das
        filas de espera e da DLQ saem dela e nunca divergem do que foi declarado no broker.
        """
        try:
            return self._topologia.fila(queue)
        except ConfigError:
            return queue

    def _politica_para(self, registro: _Registro | None) -> PoliticaRetentativa:
        """Devolve a politica de retentativa efetiva, honrando o ``max_tentativas`` do handler."""
        if registro is None or registro.max_tentativas is None:
            return self._politica
        max_t = registro.max_tentativas
        politica = self._politicas_por_max.get(max_t)
        if politica is None:
            politica = PoliticaRetentativa(
                servico=self._service, max_tentativas=max_t, clock=self._clock
            )
            self._politicas_por_max[max_t] = politica
        return politica

    @asynccontextmanager
    async def _abrir_sessao(self) -> AsyncIterator[SessaoTransacional]:
        """Abre a unidade de trabalho da mensagem: sessao de banco ou sentinela sem banco."""
        if self._session_factory is not None:
            async with self._session_factory() as session:
                yield session
        else:
            yield _SessaoEmMemoria()

    @asynccontextmanager
    async def _enquadrar(self, session: SessaoTransacional) -> AsyncIterator[None]:
        """Enquadra a transacao que torna atomicos a marca de idempotencia e o efeito (secao 4.8.3).

        No perfil "dominio", o limite e o ``async with session.begin()`` da ``AsyncSession`` -- a
        mesma transacao em que ``marcar`` e o handler gravam. No perfil "borda", o limite
        e o ``transacao`` do :class:`MemoryIdempotencyStore`, que confirma as marcas provisorias na
        saida normal e as **reverte** na excecao, para que uma falha nao deixe marca capaz
        de suprimir a retentativa seguinte (secao 4.6.3).
        """
        begin = getattr(session, "begin", None)
        if self._session_factory is not None and callable(begin):
            async with begin():
                yield
            return
        transacao = getattr(self._store, "transacao", None)
        if callable(transacao):
            async with transacao(session):
                yield
            return
        yield
