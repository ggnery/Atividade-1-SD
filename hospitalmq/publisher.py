"""``Publisher`` e o *relay* do *outbox* transacional (secoes 4.4 e 4.9) -- R1.1, R1.2, R1.5.

O :class:`Publisher` e a porta de saida do middleware: o produtor entrega **apenas ``tipo`` e
``payload``** e o :class:`Publisher` preenche os oito campos restantes do :class:`~hospitalmq.
envelope.Envelope` (secao 4.4.2), serializa, publica com *publisher confirms* e devolve o Envelope
construido. E literalmente o texto de R1.1: quem publica nao conhece fila, endereco de rede, host,
porta nem identidade de consumidor -- quem consome e decidido pelas *bindings* declaradas no broker.

Sao **dois** metodos de publicacao, e a distincao existe para preservar a idempotencia (4.4.1):

* :meth:`Publisher.publish` constroi um Envelope **novo** a partir de ``tipo`` e ``payload`` e o
  publica -- o caminho do produtor de raiz (``bedside_monitor`` pelo ``api-gateway``).
* :meth:`Publisher.publish_envelope` recebe um Envelope **ja construido** e apenas o serializa e
  entrega. Seus dois unicos chamadores sao o *relay* do outbox (:class:`OutboxRelay`), que le o
  Envelope gravado em ``JSONB``, e o ``Consumer`` ao drenar os eventos derivados de ``ctx.emitir``
  depois do ``COMMIT`` (secao 4.5.4). Reconstruir o Envelope nesses casos geraria um ``message_id``
  novo a cada tentativa e destruiria a idempotencia que o outbox existe para preservar.

O *relay* (:class:`OutboxRelay`) fecha a lacuna da escrita dupla (secao 4.9): le as linhas pendentes
de ``outbox_mensagens`` -- gravadas na **mesma transacao** do efeito de dominio por
``ctx.emitir_no_outbox`` --, republica cada uma com ``publish_envelope`` sob *confirms* e so
entao marca ``publicado_em``. A dupla "outbox + idempotencia" garante "persistiu => publicou"
sem duplicar efeito: um lado garante que nada se perde, o outro que nada duplica (R2.4).
"""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any, Final

from hospitalmq.clock import Clock, RealClock
from hospitalmq.config import EXCHANGE_EVENTOS
from hospitalmq.envelope import Envelope, Identity
from hospitalmq.errors import ConfigError, TransportError
from hospitalmq.idempotency import SessaoTransacional
from hospitalmq.logging import (
    LogEvent,
    Logger,
    causation_id_atual,
    correlation_id_atual,
    get_logger,
)
from hospitalmq.metrics import Metrics, get_metrics

if TYPE_CHECKING:  # pragma: no cover - sqlalchemy so entra no perfil "dominio" (secao 12.2)
    from collections.abc import Callable, Mapping
    from types import ModuleType

    from sqlalchemy import MetaData, Table
    from sqlalchemy.ext.asyncio import AsyncSession

    from hospitalmq.transport.base import Transport

    FabricaDeSessao = Callable[[], AsyncSession]
    """Fabrica de :class:`AsyncSession`, o ``async_sessionmaker(engine)`` (secao 4.9.2)."""

__all__ = [
    "NOME_TABELA_OUTBOX",
    "OutboxRelay",
    "Publisher",
    "tabela_outbox_mensagens",
]

NOME_TABELA_OUTBOX: Final[str] = "outbox_mensagens"
"""Nome fisico da tabela do *outbox* (secao 4.9.2).

Existe **apenas** no *schema* ``clinico`` do ``admission-service`` (D6, ADR-005): os servicos de
telemetria publicam direto depois do ``COMMIT`` e nao a possuem (secao 4.9.3).
"""

_INTERVALO_RELAY_S: Final[float] = 0.2
"""Ciclo de *polling* do *relay*: 200 ms, bem dentro do orcamento de 2 s do painel (R11.2)."""

_LOTE_RELAY: Final[int] = 100
"""Tamanho do lote lido por ciclo: ``SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100`` (secao 4.9.2)."""

_LIMITE_ULTIMO_ERRO: Final[int] = 512
"""Corte do texto gravado em ``ultimo_erro``, para nao inflar a linha do outbox."""

# Caches de ``Table`` e do ``MetaData`` interno -- ver idempotency.py, que declara a
# tabela irma ``mensagens_processadas`` com o mesmo padrao. Redeclarar a mesma tabela no mesmo
# ``MetaData`` levanta ``InvalidRequestError``, e o *relay* chama a fabrica a cada ciclo.
_TABELAS_OUTBOX: dict[tuple[int, str | None], Table] = {}
_METADATA_PADRAO: list[Any] = []


# --------------------------------------------------------------------------- #
# Tabela do outbox (SQLAlchemy Core, importado sob demanda)                     #
# --------------------------------------------------------------------------- #


def _sqlalchemy() -> ModuleType:
    """Importa ``sqlalchemy`` sob demanda, com erro de configuracao legivel se faltar.

    Raises:
        ConfigError: Quando o extra ``dominio`` nao foi instalado. Falhar aqui, e nao no ``import``
            do modulo, e o que mantem o ``api-gateway`` -- perfil "borda", sem banco -- capaz de
            importar :mod:`hospitalmq.publisher` para usar so o :class:`Publisher` (secao 12.2).
    """
    try:
        import sqlalchemy
    except ModuleNotFoundError as exc:  # pragma: no cover - depende do perfil instalado
        raise ConfigError(
            "o outbox transacional exige SQLAlchemy: instale o extra 'dominio' "
            "(pip install 'hospitalmq[dominio]')",
            dependencia="sqlalchemy",
        ) from exc
    return sqlalchemy


def tabela_outbox_mensagens(
    *, schema: str | None = None, metadata: MetaData | None = None
) -> Table:
    """Declara -- e memoriza -- a tabela ``outbox_mensagens`` (secoes 4.9.2 e 7).

    A declaracao e em **SQLAlchemy Core**, e nao um modelo ``DeclarativeBase``, pelo mesmo motivo da
    tabela irma ``mensagens_processadas``: o middleware nao impoe ``Base`` ao servico, que ja tem
    a sua com as tabelas de dominio. Os tipos sao portateis de proposito -- ``Uuid`` vira ``UUID``
    nativo no PostgreSQL e ``CHAR(32)`` no SQLite, e ``JSON`` vira ``JSONB`` no PostgreSQL --, o que
    permite a suite de integracao rodar em banco em memoria com o **mesmo** codigo de producao.

    Args:
        schema: *Schema* do servico, sempre ``"clinico"`` em producao (D6). ``None`` usa o *schema*
            padrao da conexao, util na suite com SQLite.
        metadata: ``MetaData`` onde declarar. ``None`` usa um interno do modulo; passe o do servico
            quando quiser que ``create_all`` crie a tabela junto com as de dominio.

    Returns:
        A ``Table``, sempre a mesma instancia para um dado par ``(metadata, schema)``.
    """
    sa = _sqlalchemy()
    if metadata is None:
        if not _METADATA_PADRAO:
            _METADATA_PADRAO.append(sa.MetaData())
        alvo = _METADATA_PADRAO[0]
    else:
        alvo = metadata

    chave = (id(alvo), schema)
    memorizada = _TABELAS_OUTBOX.get(chave)
    if memorizada is not None:
        return memorizada

    from sqlalchemy.dialects.postgresql import JSONB

    tipo_json = sa.JSON().with_variant(JSONB(), "postgresql")
    # BIGSERIAL no PostgreSQL; no SQLite, ``BigInteger`` nao auto-incrementa (so ``INTEGER PRIMARY
    # KEY`` o faz), entao a variante mantem a suite de integracao rodando em banco em memoria.
    tipo_id = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

    tabela: Table = sa.Table(
        NOME_TABELA_OUTBOX,
        alvo,
        sa.Column("id", tipo_id, primary_key=True, autoincrement=True),
        sa.Column("message_id", sa.Uuid(as_uuid=True), nullable=False, unique=True),
        sa.Column("tipo", sa.Text, nullable=False),
        sa.Column("routing_key", sa.Text, nullable=False),
        sa.Column("envelope", tipo_json, nullable=False),
        sa.Column(
            "criado_em",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("publicado_em", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tentativas", sa.SmallInteger, nullable=False, server_default=sa.text("0")),
        sa.Column("ultimo_erro", sa.Text, nullable=True),
        # Indice parcial: so o que falta publicar, exatamente o WHERE do loop do relay (4.9.2).
        sa.Index(
            f"ix_{schema or 'public'}_outbox_pendentes",
            "criado_em",
            postgresql_where=sa.text("publicado_em IS NULL"),
        ),
        schema=schema,
    )
    _TABELAS_OUTBOX[chave] = tabela
    return tabela


def _nome_do_dialeto(session: SessaoTransacional) -> str:
    """Descobre o dialeto ligado a sessao, com ``postgresql`` como padrao seguro.

    ``FOR UPDATE SKIP LOCKED`` e uma extensao do PostgreSQL e nao existe no SQLite; detectar aqui
    evita obrigar o *relay* a declarar em que banco esta rodando.
    """
    obter = getattr(session, "get_bind", None)
    if obter is None:
        return "postgresql"
    try:
        bind = obter()
    except Exception:  # pragma: no cover - sessao sem bind e caso de duble em teste
        return "postgresql"
    dialeto = getattr(bind, "dialect", None)
    nome = getattr(dialeto, "name", None)
    return str(nome) if nome else "postgresql"


# --------------------------------------------------------------------------- #
# Publisher                                                                     #
# --------------------------------------------------------------------------- #


class Publisher:
    """Porta de saida do middleware: constroi o Envelope e publica com durabilidade (R1.1, R1.2).

    O produtor informa **tipo e payload**; os demais campos do Envelope sao preenchidos aqui
    (secao 4.4.2). A publicacao so retorna **apos a confirmacao de durabilidade pelo broker**
    (*publisher confirms*), e falha em no maximo ``confirm_timeout_s`` segundos com
    :class:`~hospitalmq.errors.TransportError`, nunca com descarte silencioso -- e melhor o produtor
    saber que a publicacao nao aconteceu do que acreditar que aconteceu (R1.5, secao 4.4.4).
    """

    __slots__ = (
        "_clock",
        "_confirm_timeout_s",
        "_exchange",
        "_log",
        "_metrics",
        "_producer",
        "_transport",
    )

    def __init__(
        self,
        *,
        transport: Transport,
        producer: str,
        exchange: str = EXCHANGE_EVENTOS,
        confirm_timeout_s: float = 5.0,
        clock: Clock | None = None,
        logger: Logger | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        """Constroi o publicador de um servico.

        Args:
            transport: O :class:`~hospitalmq.transport.base.Transport` ja conectado. O
                :class:`Publisher` nunca importa ``aio_pika`` -- fala apenas com esta interface.
            producer: Nome do servico emissor, gravado no campo ``producer`` do Envelope e usado na
                trilha de auditoria (R7.1). Normalmente ``settings.service_name``.
            exchange: Destino das publicacoes. Padrao ``hospital.events``; o ``RpcClient``
                usa ``hospital.rpc`` com o mesmo mecanismo.
            confirm_timeout_s: Teto, em segundos, para o broker confirmar a durabilidade (R1.5).
            clock: Relogio injetavel para medir a duracao da publicacao (secao 10.3.1).
            logger: Logger estruturado; padrao :func:`~hospitalmq.logging.get_logger`.
            metrics: Registro de metricas; padrao o singleton de :func:`~hospitalmq.metrics.
                get_metrics`.
        """
        self._transport: Transport = transport
        self._producer: str = producer
        self._exchange: str = exchange
        self._confirm_timeout_s: float = confirm_timeout_s
        self._clock: Clock = clock if clock is not None else RealClock()
        self._log: Logger = logger if logger is not None else get_logger(__name__)
        self._metrics: Metrics = metrics if metrics is not None else get_metrics()

    @property
    def producer(self) -> str:
        """Nome do servico emissor gravado em ``Envelope.producer``."""
        return self._producer

    @property
    def exchange(self) -> str:
        """Exchange de destino das publicacoes deste publicador."""
        return self._exchange

    async def publish(
        self,
        tipo: str,
        payload: Mapping[str, Any],
        *,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        identity: Identity | None = None,
        version: int = 1,
        message_id: str | None = None,
    ) -> Envelope:
        """Constroi o Envelope a partir de ``tipo`` e ``payload`` e o publica (R1.1, R1.2).

        Preenche automaticamente os oito campos que o produtor nao informa (secao 4.4.2):
        ``message_id`` (UUIDv4, salvo reuso do *relay*), ``correlation_id`` (argumento ->
        contexto de log corrente -> novo UUIDv4), ``causation_id`` (argumento, ``None`` na raiz),
        ``timestamp`` (relogio no instante da chamada), ``producer``, ``attempt`` (sempre ``1`` na
        publicacao original) e ``version``. A *routing key* e o proprio ``tipo``.

        Args:
            tipo: Tipo do evento, ex. ``"sinais.coletados"``. E tambem a *routing key* AMQP.
            payload: Corpo do evento, serializavel em JSON.
            correlation_id: Correlacao do fluxo. ``None`` herda do contexto de log corrente
                e, na ausencia dele, gera um UUIDv4 novo -- nesta ordem (R5.3).
            causation_id: ``message_id`` da mensagem que causou esta. ``None`` na raiz do fluxo.
            identity: Quem originou a acao, ou ``None`` para processo interno sem credencial (R4.3).
            version: Versao do *schema* do ``payload`` (secao 4.2.4).
            message_id: Identidade da mensagem. Informado **apenas** pelo *relay* do outbox, que
                reusa o ``message_id`` ja gravado; caso contrario um UUIDv4 novo.

        Returns:
            O Envelope efetivamente publicado, ja com todos os campos preenchidos.

        Raises:
            TransportError: Broker fora, publicacao nao confirmada em ``confirm_timeout_s``
                segundos, ou mensagem recusada como nao roteavel (R1.5).
        """
        envelope = Envelope(
            message_id=message_id if message_id is not None else str(uuid.uuid4()),
            correlation_id=self._resolver_correlation_id(correlation_id),
            causation_id=causation_id if causation_id is not None else causation_id_atual(),
            type=tipo,
            version=version,
            timestamp=self._clock.now(),
            producer=self._producer,
            identity=identity,
            attempt=1,
            payload=dict(payload),
        )
        await self.publish_envelope(envelope)
        return envelope

    async def publish_envelope(self, envelope: Envelope) -> None:
        """Serializa e publica um Envelope **ja construido** (secao 4.4.1).

        Nao mexe em nenhum campo do Envelope -- em especial preserva o ``message_id``, do que
        dependem a idempotencia do consumidor (R2.4) e o *relay* do outbox (secao 4.9.2). Retorna
        somente apos o *publisher confirm*; converte a ausencia de confirmacao em ate
        ``confirm_timeout_s`` segundos em :class:`~hospitalmq.errors.TransportError` (R1.5).

        Args:
            envelope: O Envelope a publicar, tipicamente vindo do *relay* do outbox ou dos eventos
                derivados acumulados por ``ctx.emitir`` no ``Consumer``.

        Raises:
            TransportError: Broker indisponivel, publicacao nao confirmada no prazo, ou mensagem nao
                roteavel.
        """
        inicio = self._clock.monotonic()
        body = envelope.to_bytes()
        try:
            await asyncio.wait_for(
                self._transport.publish(
                    destination=self._exchange,
                    routing_key=envelope.type,
                    body=body,
                    headers=envelope.to_headers(),
                    message_id=envelope.message_id,
                    correlation_id=envelope.correlation_id,
                    persistent=True,
                ),
                timeout=self._confirm_timeout_s,
            )
        except TimeoutError as exc:
            raise TransportError(
                f"broker nao confirmou a publicacao em {self._confirm_timeout_s}s",
                tipo=envelope.type,
                message_id=envelope.message_id,
            ) from exc

        duracao_ms = (self._clock.monotonic() - inicio) * 1000.0
        self._metrics.inc("mensagens_publicadas", tipo=envelope.type)
        campos: dict[str, Any] = {
            "tipo": envelope.type,
            "exchange": self._exchange,
            "payload_bytes": len(body),
            "duracao_ms": round(duracao_ms, 2),
            "resultado": "sucesso",
            "message_id": envelope.message_id,
            "correlation_id": envelope.correlation_id,
            "causation_id": envelope.causation_id,
        }
        if envelope.identity is not None:
            campos["identity_sub"] = envelope.identity.sub
        self._log.info(LogEvent.MENSAGEM_PUBLICADA, **campos)

    @staticmethod
    def _resolver_correlation_id(explicito: str | None) -> str:
        """Resolve o ``correlation_id`` na ordem de 4.4.2: argumento, contexto, UUIDv4 novo."""
        if explicito is not None:
            return explicito
        do_contexto = correlation_id_atual()
        if do_contexto is not None:
            return do_contexto
        return str(uuid.uuid4())


# --------------------------------------------------------------------------- #
# Relay do outbox transacional (secao 4.9.2)                                    #
# --------------------------------------------------------------------------- #


class OutboxRelay:
    """Publica, em ciclo, as mensagens acumuladas na tabela ``outbox_mensagens`` (secao 4.9.2).

    E a metade assincrona do *outbox*: ``ctx.emitir_no_outbox`` grava o Envelope na **mesma
    transacao** do efeito de dominio; este *relay* o publica depois. O ciclo le um lote de linhas
    pendentes com ``SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100`` -- o ``SKIP LOCKED`` permite
    **multiplas replicas** do ``admission-service`` no *relay* sem publicar a mesma linha duas
    vezes --, chama :meth:`Publisher.publish_envelope` sob *publisher confirms* e so entao grava
    ``publicado_em``. Se o *relay* publicar e cair antes de marcar, a republicacao carrega o
    **mesmo** ``message_id`` e e suprimida pela idempotencia dos consumidores (4.8): a garantia
    e *at-least-once*, e a dupla "outbox + idempotencia" a converte em efeito unico.
    """

    __slots__ = (
        "_clock",
        "_intervalo_s",
        "_log",
        "_lote",
        "_metadata",
        "_metrics",
        "_parar",
        "_publisher",
        "_schema",
        "_session_factory",
    )

    def __init__(
        self,
        *,
        publisher: Publisher,
        session_factory: FabricaDeSessao,
        schema: str | None = "clinico",
        metadata: MetaData | None = None,
        clock: Clock | None = None,
        intervalo_s: float = _INTERVALO_RELAY_S,
        lote: int = _LOTE_RELAY,
        logger: Logger | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        """Constroi o *relay* do outbox.

        Args:
            publisher: O :class:`Publisher` usado para republicar cada Envelope pendente.
            session_factory: Fabrica de :class:`AsyncSession`, tipicamente
                ``async_sessionmaker(engine)``. Cada ciclo abre uma transacao propria.
            schema: *Schema* onde a tabela ``outbox_mensagens`` vive -- ``"clinico"`` em producao
                (D6). ``None`` na suite com SQLite.
            metadata: ``MetaData`` do servico, quando a tabela e compartilhada com as de dominio.
            clock: Relogio injetavel; alimenta ``publicado_em`` e a espera entre ciclos.
            intervalo_s: Intervalo entre ciclos de *polling*, em segundos. Padrao 200 ms.
            lote: Maximo de linhas lidas por ciclo (``LIMIT``). Padrao 100.
            logger: Logger estruturado; padrao :func:`~hospitalmq.logging.get_logger`.
            metrics: Registro de metricas; padrao o singleton de :func:`~hospitalmq.metrics.
                get_metrics`.
        """
        self._publisher: Publisher = publisher
        self._session_factory: FabricaDeSessao = session_factory
        self._schema: str | None = schema
        self._metadata: MetaData | None = metadata
        self._clock: Clock = clock if clock is not None else RealClock()
        self._intervalo_s: float = intervalo_s
        self._lote: int = lote
        self._log: Logger = logger if logger is not None else get_logger(__name__)
        self._metrics: Metrics = metrics if metrics is not None else get_metrics()
        self._parar: asyncio.Event = asyncio.Event()

    async def publicar_pendentes(self) -> int:
        """Executa **um** ciclo do *relay*: le o lote pendente e publica cada linha (secao 4.9.2).

        Abre uma transacao propria, seleciona as linhas com ``publicado_em IS NULL`` em ordem de
        ``criado_em`` (com ``FOR UPDATE SKIP LOCKED`` no PostgreSQL) e, para cada uma, reidrata o
        Envelope do ``JSONB`` e chama :meth:`Publisher.publish_envelope`. Em caso de sucesso, marca
        ``publicado_em``; em caso de :class:`~hospitalmq.errors.TransportError`, incrementa
        ``tentativas`` e grava ``ultimo_erro``, deixando a linha para o proximo ciclo -- o broker
        indisponivel nao trava o *relay*, apenas adia a publicacao.

        Returns:
            Quantas linhas foram efetivamente publicadas e confirmadas neste ciclo.
        """
        sa = _sqlalchemy()
        tabela = tabela_outbox_mensagens(schema=self._schema, metadata=self._metadata)
        publicados = 0

        async with self._session_factory() as session, session.begin():
            consulta = (
                sa.select(tabela)
                .where(tabela.c.publicado_em.is_(None))
                .order_by(tabela.c.criado_em)
                .limit(self._lote)
            )
            if _nome_do_dialeto(session) == "postgresql":
                consulta = consulta.with_for_update(skip_locked=True)

            linhas = (await session.execute(consulta)).mappings().all()
            for linha in linhas:
                envelope = Envelope.from_dict(linha["envelope"])
                try:
                    await self._publisher.publish_envelope(envelope)
                except TransportError as exc:
                    await session.execute(
                        sa.update(tabela)
                        .where(tabela.c.id == linha["id"])
                        .values(
                            tentativas=(linha["tentativas"] or 0) + 1,
                            ultimo_erro=str(exc)[:_LIMITE_ULTIMO_ERRO],
                        )
                    )
                    self._log.warning(
                        LogEvent.MENSAGEM_PUBLICADA,
                        resultado="pendente",
                        tipo=envelope.type,
                        message_id=envelope.message_id,
                        erro=type(exc).__name__,
                        detalhe=str(exc),
                    )
                    continue

                await session.execute(
                    sa.update(tabela)
                    .where(tabela.c.id == linha["id"])
                    .values(publicado_em=self._clock.now())
                )
                publicados += 1

        return publicados

    async def run(self) -> None:
        """Roda o *relay* em laco continuo ate :meth:`stop` (secao 4.9.2).

        Publica o lote pendente e dorme ``intervalo_s`` quando nao ha nada a fazer, cedendo o
        *event loop* imediatamente enquanto ainda houver fila -- de modo que uma rajada de admissoes
        e escoada sem esperar 200 ms por linha. Uma falha inesperada de um ciclo e registrada e o
        laco continua: o *relay* e um processo de fundo que nao pode morrer por um erro transitorio.
        """
        self._parar.clear()
        while not self._parar.is_set():
            try:
                publicados = await self.publicar_pendentes()
            except Exception as exc:  # pragma: no cover - rede de seguranca do laco de fundo
                self._log.error(
                    LogEvent.MENSAGEM_PUBLICADA,
                    resultado="erro_relay",
                    erro=type(exc).__name__,
                    detalhe=str(exc),
                )
                publicados = 0
            if publicados >= self._lote:
                continue
            try:
                await asyncio.wait_for(self._parar.wait(), timeout=self._intervalo_s)
            except TimeoutError:
                continue

    async def stop(self) -> None:
        """Sinaliza o encerramento gracioso do laco de :meth:`run`. Idempotente."""
        self._parar.set()
