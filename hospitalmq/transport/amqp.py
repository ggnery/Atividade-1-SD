"""Driver AMQP do HospitalMQ sobre ``aio-pika`` 9.x e RabbitMQ (R9.1, R9.2).

Este e o unico modulo do middleware que importa ``aio_pika``. Todo o resto do nucleo fala apenas com
o ``Protocol`` de :mod:`hospitalmq.transport.base`, e e por isso que trocar ``TRANSPORTE=amqp`` por
``TRANSPORTE=memory`` nao muda uma linha de codigo de servico. Aqui vivem, e **so** aqui:

* a traducao da :class:`~hospitalmq.config.TopologySpec` declarativa em ``exchange_declare``,
  ``queue_declare`` e ``queue_bind`` concretos, na ordem normativa da secao 6.11.1;
* os *publisher confirms* -- ``publish`` so retorna depois de o broker confirmar a durabilidade, com
  teto de tempo (R1.5);
* o ``mandatory=True`` que transforma mensagem nao roteavel em
  :class:`~hospitalmq.errors.TransportError` em vez de descarte silencioso (secao 4.7.1);
* o ``x-dead-letter-exchange`` das filas de negocio e o ``x-message-ttl`` das filas de espera da
  secao 4.6.2, ambos derivados da ``QueueSpec`` para que nome e argumento nunca divirjam;
* o ACK/NACK manual, com ``prefetch`` por canal (R2.6);
* a reconexao automatica via :func:`aio_pika.connect_robust`, que ainda re-declara topologia e
  re-registra consumidores apos uma queda do broker.

Os quatro invariantes da secao 4.3.2 sao respeitados a risca:

1. **``delivery_tag`` e opaco.** O driver guarda a ``AbstractIncomingMessage`` nativa **dentro** do
   campo ``delivery_tag`` da :class:`~hospitalmq.transport.base.InboundMessage` e a recupera em
   ``ack``, ``nack`` e ``reply``. O nucleo nunca a inspeciona.
2. **Bytes entram, bytes saem.** O driver nunca conhece Envelope, JSON, retentativa ou idempotencia.
3. **Toda falha de infraestrutura vira :class:`~hospitalmq.errors.TransportError`.** Nenhuma excecao
   de ``aio_pika`` ou ``aiormq`` escapa; a unica excecao e o ``406 PRECONDITION_FAILED`` na
   declaracao, que vira :class:`~hospitalmq.errors.ConfigError` por ser falha fatal de *boot*.
4. **Entrega e *at-least-once*.** A supressao de duplicata e do nucleo (``idempotency.py``).

Modelo de canais (a leitura de "prefetch por canal" da secao 4.3.3):

* um **canal publicador** unico, com ``publisher_confirms=True`` e ``on_return_raises=True``, usado
  por :meth:`~AmqpTransport.publish`, :meth:`~AmqpTransport.reply` e por
  :meth:`~AmqpTransport.declare_topology`;
* um **canal por assinatura** de :meth:`~AmqpTransport.consume`, com o seu proprio
  ``basic.qos(prefetch_count=...)``, de modo que cada consumidor tem a sua janela de nao confirmadas
  isolada das demais.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Final

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from aio_pika.exceptions import (
    CONNECTION_EXCEPTIONS,
    AMQPError,
    ChannelPreconditionFailed,
    DeliveryError,
)

from hospitalmq.clock import Clock, RealClock
from hospitalmq.config import (
    EXCHANGE_PADRAO,
    PREFETCH_PADRAO,
    ExchangeSpec,
    QueueSpec,
    TopologySpec,
)
from hospitalmq.envelope import CONTENT_ENCODING, CONTENT_TYPE
from hospitalmq.errors import ConfigError, TransportError
from hospitalmq.transport.base import InboundMessage, MessageHandler, Subscription

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao
    from aiormq.abc import ConfirmationFrameType

__all__ = ["AmqpTransport"]


# --------------------------------------------------------------------------- #
# Constantes de reconexao e encerramento                                       #
# --------------------------------------------------------------------------- #

#: Tentativas de conexao inicial antes de desistir -- cobre um broker que sobe depois do servico.
_TENTATIVAS_CONEXAO_PADRAO: Final[int] = 30

#: Espera, em segundos, entre tentativas de conexao inicial.
_ESPERA_CONEXAO_PADRAO_S: Final[float] = 2.0

#: Teto, em segundos, de cada tentativa de abrir a conexao com o broker.
_TIMEOUT_CONEXAO_PADRAO_S: Final[float] = 10.0

#: Teto, em segundos, que ``close`` concede aos handlers em voo antes de fechar os canais.
_TIMEOUT_CLOSE_S: Final[float] = 10.0

#: Nome dos argumentos AMQP usados na declaracao (evita strings soltas espalhadas pelo modulo).
_ARG_DLX: Final[str] = "x-dead-letter-exchange"
_ARG_DLK: Final[str] = "x-dead-letter-routing-key"
_ARG_TTL: Final[str] = "x-message-ttl"
_ARG_MAX_LENGTH: Final[str] = "x-max-length"
_ARG_OVERFLOW: Final[str] = "x-overflow"

_TIPOS_EXCHANGE: Final[dict[str, aio_pika.ExchangeType]] = {
    "topic": aio_pika.ExchangeType.TOPIC,
    "direct": aio_pika.ExchangeType.DIRECT,
    "fanout": aio_pika.ExchangeType.FANOUT,
    "headers": aio_pika.ExchangeType.HEADERS,
}


def _tipo_exchange(tipo: str) -> aio_pika.ExchangeType:
    """Traduz o tipo textual da ``ExchangeSpec`` no enum de ``aio_pika``.

    Args:
        tipo: ``"topic"``, ``"direct"``, ``"fanout"`` ou ``"headers"``.

    Returns:
        O :class:`aio_pika.ExchangeType` correspondente.

    Raises:
        ConfigError: Se o tipo for desconhecido -- defeito de configuracao, nao de runtime.
    """
    try:
        return _TIPOS_EXCHANGE[tipo]
    except KeyError as exc:  # pragma: no cover - protegido pelo Literal da ExchangeSpec
        raise ConfigError(f"tipo de exchange desconhecido: {tipo!r}", tipo=tipo) from exc


def _argumentos_de_fila(spec: QueueSpec) -> dict[str, Any]:
    """Monta os argumentos AMQP de uma fila de negocio a partir da ``QueueSpec`` (secao 6.10.2).

    A verdade sobre *dead-lettering* e *back-pressure* vive versionada em ``config.py`` e chega aqui
    como dado; este driver apenas a materializa. E a mesma funcao conceitual do ``queue_arguments``
    documentado na secao 6.10.2 do design.

    Args:
        spec: Declaracao da fila de negocio.

    Returns:
        Dicionario pronto para o parametro ``arguments`` de ``channel.declare_queue``.
    """
    args: dict[str, Any] = {}
    if spec.with_dlq:
        # x-dead-letter-routing-key = <fila>.dlq casa exatamente com o binding da DLQ e com a chave
        # que o Consumer usa ao publicar na DLQ explicitamente (secao 4.7.1).
        args[_ARG_DLX] = spec.dead_letter_exchange
        args[_ARG_DLK] = spec.dlq_name
    if spec.max_length is not None:
        args[_ARG_MAX_LENGTH] = spec.max_length
        args[_ARG_OVERFLOW] = spec.overflow
    return args


def _argumentos_de_espera(origem: str, ttl_ms: int) -> dict[str, Any]:
    """Monta os argumentos de uma fila de espera da secao 4.6.2 (o ``retry_queue_arguments``).

    A fila de espera nao tem consumidor: e um temporizador. O ``x-message-ttl`` mantem a mensagem
    parada pelo atraso desejado e, ao expirar, o ``x-dead-letter-exchange`` vazio (exchange padrao)
    com ``x-dead-letter-routing-key`` igual ao nome da fila de origem devolve a mensagem exatamente
    a fila de onde veio.

    Args:
        origem: Nome da fila de negocio a que a mensagem retorna quando o TTL expira.
        ttl_ms: Atraso da fila de espera, em milissegundos (``1000``, ``2000`` ou ``4000``).

    Returns:
        Dicionario pronto para o parametro ``arguments`` de ``channel.declare_queue``.
    """
    return {
        _ARG_TTL: ttl_ms,
        _ARG_DLX: EXCHANGE_PADRAO,
        _ARG_DLK: origem,
    }


# --------------------------------------------------------------------------- #
# Assinatura de consumo                                                        #
# --------------------------------------------------------------------------- #


class _AssinaturaAmqp:
    """Assinatura ativa de consumo, cancelavel, ligada a um canal dedicado (secao 4.3.1).

    Cancelar apenas interrompe a entrega de **novas** mensagens (``basic.cancel``); as que ja estao
    em voo continuam validas para ``ack``/``nack``. O canal em si so e fechado por
    :meth:`AmqpTransport.close`, para nao perder o direito de confirmar o que ja foi entregue.
    """

    __slots__ = ("_cancelada", "_consumer_tag", "_fila")

    def __init__(self, *, fila: AbstractQueue, consumer_tag: str) -> None:
        """Guarda a fila e o *consumer tag* devolvido por ``queue.consume``.

        Args:
            fila: A fila de onde o consumidor foi registrado.
            consumer_tag: Identificador do consumidor no broker, usado para cancelar.
        """
        self._fila = fila
        self._consumer_tag = consumer_tag
        self._cancelada = False

    async def cancel(self) -> None:
        """Cancela a assinatura, parando a entrega de novas mensagens. Idempotente.

        Nao aguarda os handlers em voo -- quem faz isso e :meth:`AmqpTransport.close` -- e nunca
        levanta por assinatura ja cancelada.
        """
        if self._cancelada:
            return
        self._cancelada = True
        with contextlib.suppress(AMQPError, RuntimeError):
            await self._fila.cancel(self._consumer_tag)


# --------------------------------------------------------------------------- #
# O driver                                                                     #
# --------------------------------------------------------------------------- #


class AmqpTransport:
    """Transporte AMQP sobre ``aio-pika`` e RabbitMQ (R9.1, R9.2).

    Implementa estruturalmente o ``Protocol`` de :mod:`hospitalmq.transport.base`: os mesmos oito
    metodos, com as mesmas assinaturas. Nao herda de ``Transport`` de proposito -- a tipagem
    estrutural elimina o acoplamento de importacao (secao 4.3.4).

    A conexao e **robusta**: :func:`aio_pika.connect_robust` reconecta sozinha apos uma queda e, por
    usarmos ``RobustChannel``, a topologia declarada e os consumidores registrados sao restaurados
    automaticamente no religamento -- de modo que um ``docker restart rabbitmq`` nao exige reinicio
    dos servicos.
    """

    def __init__(
        self,
        *,
        url: str,
        confirm_timeout_s: float = 5.0,
        clock: Clock | None = None,
        timeout_conexao_s: float = _TIMEOUT_CONEXAO_PADRAO_S,
        tentativas_conexao: int = _TENTATIVAS_CONEXAO_PADRAO,
        espera_conexao_s: float = _ESPERA_CONEXAO_PADRAO_S,
    ) -> None:
        """Constroi o driver **sem** abrir conexao -- quem chama :meth:`connect` e o *bootstrap*.

        Args:
            url: URL AMQP completa, incluindo o vhost, ex.
                ``"amqp://hospital:hospital@rabbitmq:5672/hospital"``.
            confirm_timeout_s: Teto, em segundos, para o broker confirmar a durabilidade de uma
                publicacao antes de :meth:`publish` levantar :class:`TransportError` (R1.5).
            clock: Relogio injetavel; usado apenas para a espera entre tentativas de conexao. O
                padrao e :class:`~hospitalmq.clock.RealClock`.
            timeout_conexao_s: Teto de cada tentativa de abrir a conexao com o broker.
            tentativas_conexao: Numero de tentativas de conexao inicial antes de desistir -- cobre o
                caso, previsto no contrato de :meth:`connect`, de o processo subir antes do broker.
            espera_conexao_s: Espera entre tentativas de conexao inicial, em segundos.
        """
        self._url = url
        self._confirm_timeout_s = confirm_timeout_s
        self._clock: Clock = clock if clock is not None else RealClock()
        self._timeout_conexao_s = timeout_conexao_s
        self._tentativas_conexao = tentativas_conexao
        self._espera_conexao_s = espera_conexao_s

        self._connection: AbstractRobustConnection | None = None
        self._canal_pub: AbstractChannel | None = None
        self._exchanges: dict[str, AbstractExchange] = {}
        self._filas: dict[str, AbstractQueue] = {}
        self._assinaturas: list[_AssinaturaAmqp] = []
        self._canais_consumo: list[AbstractChannel] = []
        self._em_voo: set[asyncio.Task[Any]] = set()
        self._fechado = False

    # -- ciclo de vida ----------------------------------------------------- #

    async def connect(self) -> None:
        """Estabelece a conexao com o broker, tentando de novo ate ele aceitar (secao 4.3.2).

        Seguro chamar em processo que sobe **antes** do broker: tenta ``tentativas_conexao`` vezes,
        esperando ``espera_conexao_s`` entre elas. **Nao** declara topologia -- isso e trabalho de
        :meth:`declare_topology`, chamado logo em seguida. Chamar duas vezes com a conexao ja aberta
        e um no-op e nao abre uma segunda conexao.

        Raises:
            TransportError: Se a conexao nao for estabelecida dentro do numero de tentativas.
        """
        if self._connection is not None and not self._connection.is_closed:
            return

        ultimo_erro: BaseException | None = None
        for tentativa in range(1, self._tentativas_conexao + 1):
            try:
                conexao = await aio_pika.connect_robust(
                    self._url,
                    timeout=self._timeout_conexao_s,
                    client_properties={"connection_name": "hospitalmq"},
                )
                canal = await conexao.channel(
                    publisher_confirms=True,
                    on_return_raises=True,
                )
            except (*CONNECTION_EXCEPTIONS, TimeoutError) as exc:
                ultimo_erro = exc
                if tentativa < self._tentativas_conexao:
                    await self._clock.sleep(self._espera_conexao_s)
                continue
            self._connection = conexao
            self._canal_pub = canal
            self._fechado = False
            return

        raise TransportError(
            f"nao foi possivel conectar ao broker em {self._tentativas_conexao} tentativas",
            url=self._url,
        ) from ultimo_erro

    async def declare_topology(self, spec: TopologySpec) -> None:
        """Materializa a topologia inteira, de forma idempotente, na ordem normativa (secao 6.11.1).

        A ordem nao e negociavel: **exchanges** -> **DLQs e filas de espera** -> **filas de
        negocio** -> **bindings**. Os exchanges vem primeiro porque o RabbitMQ so resolve o
        ``x-dead-letter-exchange`` quando a mensagem morre; se ``hospital.dlx`` nao existisse
        naquele instante, a mensagem descartada sumiria sem erro.

        As DLQs e as filas de espera **nao** aparecem em ``spec.queues``: sao derivadas aqui, de
        :attr:`~hospitalmq.config.QueueSpec.dlq_name` e
        :meth:`~hospitalmq.config.QueueSpec.retry_queues`, para que nome e argumento nunca divirjam
        entre este driver e o resto do sistema.

        Redeclarar com os mesmos argumentos e no-op (o broker serializa as declaracoes concorrentes
        dos varios servicos). Redeclarar com argumentos divergentes e falha fatal de *boot*: o
        ``406 PRECONDITION_FAILED`` do AMQP vira :class:`ConfigError`, nunca redeclaracao
        destrutiva.

        Args:
            spec: A topologia declarativa completa.

        Raises:
            TransportError: Falha de comunicacao com o broker.
            ConfigError: Objeto ja existente com argumentos divergentes -- o processo deve encerrar
                com codigo 78 (``EX_CONFIG``).
        """
        canal = self._garantir_ativo("declare_topology")

        try:
            # 1. exchanges, inclusive o de descarte de cada fila, mesmo que a spec nao o liste.
            for exchange in spec.exchanges:
                await self._declarar_exchange(canal, exchange)
            for fila in spec.queues:
                if fila.with_dlq and fila.dead_letter_exchange not in self._exchanges:
                    await self._declarar_exchange(
                        canal,
                        ExchangeSpec(name=fila.dead_letter_exchange, type="topic", durable=True),
                    )

            # 2. DLQs (ligadas ao DLX pela chave <fila>.dlq) e filas de espera com TTL.
            for fila in spec.queues:
                if fila.with_dlq:
                    dlq = await canal.declare_queue(fila.dlq_name, durable=fila.durable)
                    self._filas[fila.dlq_name] = dlq
                    dlx = self._exchanges[fila.dead_letter_exchange]
                    await dlq.bind(dlx, routing_key=fila.dlq_name)
                for nome, atraso_ms in fila.retry_queues():
                    espera = await canal.declare_queue(
                        nome,
                        durable=fila.durable,
                        arguments=_argumentos_de_espera(fila.name, atraso_ms),
                    )
                    self._filas[nome] = espera

            # 3. filas de negocio, com os argumentos derivados da QueueSpec.
            for fila in spec.queues:
                declarada = await canal.declare_queue(
                    fila.name,
                    durable=fila.durable,
                    exclusive=fila.exclusive,
                    auto_delete=fila.auto_delete,
                    arguments=_argumentos_de_fila(fila),
                )
                self._filas[fila.name] = declarada

            # 4. bindings.
            for fila in spec.queues:
                for binding in fila.bindings:
                    ex_destino = self._exchanges.get(binding.exchange)
                    if ex_destino is None:
                        raise ConfigError(
                            f"binding para exchange nao declarado: {binding.exchange!r}",
                            exchange=binding.exchange,
                            fila=fila.name,
                        )
                    await self._filas[fila.name].bind(ex_destino, routing_key=binding.key)
        except ChannelPreconditionFailed as exc:
            raise ConfigError(
                f"topologia divergente do broker (406 PRECONDITION_FAILED): {exc}",
                detalhe=str(exc),
            ) from exc
        except AMQPError as exc:
            raise TransportError(f"falha ao declarar a topologia: {exc}") from exc

    async def _declarar_exchange(
        self, canal: AbstractChannel, spec: ExchangeSpec
    ) -> AbstractExchange:
        """Declara um exchange de forma idempotente e o memoriza para uso em ``publish`` e bindings.

        Args:
            canal: Canal sobre o qual declarar.
            spec: Declaracao do exchange.

        Returns:
            O objeto ``AbstractExchange`` declarado.
        """
        exchange = await canal.declare_exchange(
            spec.name,
            type=_tipo_exchange(spec.type),
            durable=spec.durable,
        )
        self._exchanges[spec.name] = exchange
        return exchange

    async def close(self) -> None:
        """Encerra o transporte graciosamente. Idempotente e nunca levanta se ja fechado.

        Cancela as assinaturas (para a entrega de novas mensagens), **aguarda os handlers em voo
        terminarem** com um teto de tempo e so entao fecha os canais de consumo, o canal publicador
        e a conexao. As mensagens ainda nao confirmadas voltam a fila quando o canal fecha -- o
        RabbitMQ as re-enfileira, nunca as descarta.
        """
        if self._fechado:
            return
        self._fechado = True

        for assinatura in list(self._assinaturas):
            await assinatura.cancel()

        if self._em_voo:
            with contextlib.suppress(Exception):
                await asyncio.wait(set(self._em_voo), timeout=_TIMEOUT_CLOSE_S)
            for tarefa in list(self._em_voo):
                tarefa.cancel()
            if self._em_voo:
                await asyncio.gather(*list(self._em_voo), return_exceptions=True)

        for canal in list(self._canais_consumo):
            with contextlib.suppress(Exception):
                await canal.close()
        self._canais_consumo.clear()
        self._assinaturas.clear()

        if self._canal_pub is not None:
            with contextlib.suppress(Exception):
                await self._canal_pub.close()
        if self._connection is not None:
            with contextlib.suppress(Exception):
                await self._connection.close()

        self._canal_pub = None
        self._connection = None
        self._exchanges.clear()
        self._filas.clear()

    # -- as cinco operacoes exigidas por R9.1 ------------------------------ #

    async def publish(
        self,
        *,
        destination: str,
        routing_key: str,
        body: bytes,
        headers: Mapping[str, Any],
        message_id: str,
        correlation_id: str,
        reply_to: str | None = None,
        persistent: bool = True,
        delay_ms: int | None = None,
    ) -> None:
        """Publica uma mensagem; so retorna apos o broker confirmar a durabilidade (R9.1, R1.5).

        Usa ``mandatory=True`` com *publisher confirms*: uma mensagem que nao case com fila nenhuma
        e devolvida (``basic.return``) e convertida em :class:`TransportError`, em vez de descartada
        em silencio (secao 4.7.1). A publicacao aguarda a confirmacao por ate ``confirm_timeout_s``;
        o estouro desse prazo tambem vira :class:`TransportError`.

        Args:
            destination: Exchange de destino. A string vazia designa o **exchange padrao**, usado no
                retorno das filas de espera (secao 6.2.4) e na resposta do RPC.
            routing_key: Chave de roteamento. Para eventos, e igual a ``Envelope.type``; para a fila
                de espera, e o nome da fila de destino.
            body: Corpo ja serializado -- bytes opacos (invariante 2 da secao 4.3.2).
            headers: Cabecalhos a espelhar, incluindo ``x-hmq-attempt`` e ``x-hmq-version``.
            message_id: Propriedade nativa ``message_id``, para correlacao na UI do broker.
            correlation_id: Propriedade nativa ``correlation_id``; obrigatoria no RPC.
            reply_to: Endereco de retorno (*Return Address*), preenchido apenas pelo ``RpcClient``.
            persistent: ``delivery_mode = 2`` quando ``True`` (R1.4). O ``RpcClient`` publica
                requisicoes com ``False``.
            delay_ms: Atraso minimo antes da entrega. No AMQP, o atraso e propriedade da **fila de
                espera** de destino (secao 4.6.2), cujo ``x-message-ttl`` foi declarado em
                :meth:`declare_topology`; quando informado, e refletido tambem como TTL por mensagem
                (``expiration``), que compoe com o *dead-lettering* da fila de espera. ``None``
                significa entrega imediata -- o caso de 100% das publicacoes deste sistema, ja que a
                retentativa publica na fila de espera pelo nome (``retry.py``).

        Raises:
            TransportError: Broker indisponivel, publicacao nao confirmada no prazo, ou mensagem
                recusada como nao roteavel.
        """
        canal = self._garantir_ativo("publish")
        exchange = await self._resolver_exchange(canal, destination)

        mensagem = aio_pika.Message(
            bytes(body),
            headers=dict(headers),
            content_type=CONTENT_TYPE,
            content_encoding=CONTENT_ENCODING,
            delivery_mode=(
                aio_pika.DeliveryMode.PERSISTENT
                if persistent
                else aio_pika.DeliveryMode.NOT_PERSISTENT
            ),
            message_id=message_id,
            correlation_id=correlation_id,
            reply_to=reply_to,
            expiration=(delay_ms / 1000.0) if (delay_ms is not None and delay_ms > 0) else None,
        )

        try:
            confirmacao: ConfirmationFrameType | None = await exchange.publish(
                mensagem,
                routing_key=routing_key,
                mandatory=True,
                timeout=self._confirm_timeout_s,
            )
        except DeliveryError as exc:
            raise TransportError(
                f"mensagem nao roteavel ou rejeitada pelo broker: {routing_key!r}",
                destination=destination,
                routing_key=routing_key,
                message_id=message_id,
            ) from exc
        except TimeoutError as exc:
            raise TransportError(
                f"broker nao confirmou a publicacao em {self._confirm_timeout_s}s",
                destination=destination,
                routing_key=routing_key,
                message_id=message_id,
            ) from exc
        except AMQPError as exc:
            raise TransportError(
                f"falha ao publicar em {destination!r}: {exc}",
                destination=destination,
                routing_key=routing_key,
                message_id=message_id,
            ) from exc

        # on_return_raises garante DeliveryError acima; esta checagem cobre o basic.nack do broker
        # (por exemplo, x-overflow=reject-publish em fila cheia) quando ele nao vem como excecao.
        nome_confirmacao = getattr(confirmacao, "name", "ack")
        if confirmacao is not None and nome_confirmacao == "nack":  # pragma: no cover
            raise TransportError(
                f"broker rejeitou a publicacao (nack): {routing_key!r}",
                destination=destination,
                routing_key=routing_key,
                message_id=message_id,
            )

    async def consume(
        self,
        *,
        queue: str,
        handler: MessageHandler,
        prefetch: int = PREFETCH_PADRAO,
    ) -> Subscription:
        """Registra um consumidor da fila, com ``prefetch`` proprio por canal (R9.1, R2.6).

        Abre um **canal dedicado** para a assinatura e aplica ``basic.qos(prefetch_count=prefetch)``
        nele, de modo que a janela de nao confirmadas deste consumidor e independente das demais. O
        broker entrega em regime *push*; **nada** e confirmado automaticamente (R2.1). Se a conexao
        cair com mensagens nao confirmadas, elas voltam para a fila. O canal e ``RobustChannel``:
        apos uma reconexao, o consumidor e restaurado sozinho.

        Args:
            queue: Nome da fila, ja declarada por :meth:`declare_topology`.
            handler: Corrotina invocada por mensagem. Nao deve levantar -- quem trata erro e o
                *pipeline* do ``Consumer``.
            prefetch: Limite de mensagens nao confirmadas em voo por assinatura.

        Returns:
            A :class:`~hospitalmq.transport.base.Subscription` cancelavel correspondente.

        Raises:
            TransportError: Fila inexistente ou falha de comunicacao com o broker.
        """
        if self._fechado or self._connection is None or self._connection.is_closed:
            raise TransportError("transporte nao conectado ou ja fechado", operacao="consume")

        try:
            canal = await self._connection.channel(publisher_confirms=False)
            await canal.set_qos(prefetch_count=prefetch)
            # ensure=True: declara passivo e falha se a fila nao existir.
            fila = await canal.get_queue(queue)
            consumer_tag = await fila.consume(self._fabricar_callback(handler, queue))
        except AMQPError as exc:
            raise TransportError(
                f"nao foi possivel consumir a fila {queue!r}: {exc}", fila=queue
            ) from exc

        assinatura = _AssinaturaAmqp(fila=fila, consumer_tag=consumer_tag)
        self._assinaturas.append(assinatura)
        self._canais_consumo.append(canal)
        return assinatura

    async def ack(self, message: InboundMessage) -> None:
        """Confirma definitivamente a mensagem (R9.1 "confirmar").

        Torna a entrega irrecuperavel, e por isso **deve ser posterior ao *commit*** do efeito
        colateral. Confirmar entrega ja confirmada ou cujo canal caiu **falha visivelmente**: o erro
        de ``aio_pika`` vira :class:`TransportError`, nunca silencio.

        Args:
            message: A mensagem recebida, inteira. O driver le dela a ``AbstractIncomingMessage``
                nativa guardada no campo opaco ``delivery_tag``.

        Raises:
            TransportError: Canal fechado, entrega desconhecida ou ja confirmada.
        """
        nativa = self._mensagem_nativa(message, "ack")
        try:
            await nativa.ack()
        except (AMQPError, RuntimeError) as exc:
            raise TransportError(
                "falha ao confirmar (ack) a mensagem",
                fila=message.queue,
                message_id=message.message_id,
            ) from exc

    async def nack(self, message: InboundMessage, *, requeue: bool = False) -> None:
        """Rejeita a mensagem (R9.1 "rejeitar").

        Com ``requeue=False``, entrega a mensagem ao ``x-dead-letter-exchange`` da fila -- a rede
        de seguranca contra descarte silencioso (R1.5). Com ``requeue=True``, devolve-a a fila;
        esse uso e **restrito ao encerramento gracioso** (secao 4.6.4), nunca e retentativa.

        Args:
            message: A mensagem recebida, inteira.
            requeue: ``True`` apenas no encerramento gracioso.

        Raises:
            TransportError: Canal fechado ou entrega desconhecida.
        """
        nativa = self._mensagem_nativa(message, "nack")
        try:
            await nativa.nack(requeue=requeue)
        except (AMQPError, RuntimeError) as exc:
            raise TransportError(
                "falha ao rejeitar (nack) a mensagem",
                fila=message.queue,
                message_id=message.message_id,
                requeue=requeue,
            ) from exc

    async def reply(
        self,
        message: InboundMessage,
        *,
        body: bytes,
        correlation_id: str,
    ) -> None:
        """Responde a uma requisicao RPC (R9.1 "responder").

        Publica no endereco de retorno indicado por ``message.reply_to``, pelo exchange padrao,
        **preservando o ``correlation_id`` recebido sem reinterpreta-lo** -- e por ele que o
        ``RpcClient`` casa a resposta com a *future* pendente. A resposta nao e persistente: uma que
        sobrevivesse a um reinicio chegaria depois de o chamador ja ter desistido por timeout. Se a
        fila de retorno nao existir mais (o chamador desistiu), o ``mandatory=True`` de
        :meth:`publish` converte a nao-roteabilidade em :class:`TransportError`.

        Args:
            message: A requisicao original, de onde sai o ``reply_to``.
            body: Corpo da resposta, ja serializado.
            correlation_id: O mesmo valor recebido na requisicao.

        Raises:
            TransportError: ``reply_to`` ausente (erro de programacao), fila de retorno inexistente
                ou falha de comunicacao.
        """
        if not message.reply_to:
            raise TransportError(
                "reply sem reply_to: a requisicao nao trazia endereco de retorno",
                fila=message.queue,
                correlation_id=correlation_id,
            )
        await self.publish(
            destination=EXCHANGE_PADRAO,
            routing_key=message.reply_to,
            body=body,
            headers={},
            message_id=uuid.uuid4().hex,
            correlation_id=correlation_id,
            persistent=False,
        )

    # -- internos ---------------------------------------------------------- #

    def _garantir_ativo(self, operacao: str) -> AbstractChannel:
        """Confirma que ha conexao e canal publicador abertos, ou levanta :class:`TransportError`.

        Args:
            operacao: Nome da operacao, apenas para diagnostico no erro.

        Returns:
            O canal publicador pronto para uso.

        Raises:
            TransportError: Transporte nao conectado, ja fechado ou com a conexao caida.
        """
        if (
            self._fechado
            or self._connection is None
            or self._canal_pub is None
            or self._connection.is_closed
        ):
            raise TransportError("transporte nao conectado ou ja fechado", operacao=operacao)
        return self._canal_pub

    async def _resolver_exchange(
        self, canal: AbstractChannel, destination: str
    ) -> AbstractExchange:
        """Resolve o nome de destino no objeto ``AbstractExchange`` correspondente.

        A string vazia (:data:`~hospitalmq.config.EXCHANGE_PADRAO`) e o exchange padrao do canal.
        Nomes ja declarados por :meth:`declare_topology` saem do cache; um nome nao cacheado vira um
        *proxy* sem ida ao broker (``ensure=False``) e passa a ser cacheado.

        Args:
            canal: Canal publicador.
            destination: Nome do exchange de destino; ``""`` designa o exchange padrao.

        Returns:
            O ``AbstractExchange`` a usar em ``publish``.
        """
        if not destination:
            return canal.default_exchange
        exchange = self._exchanges.get(destination)
        if exchange is None:
            exchange = await canal.get_exchange(destination, ensure=False)
            self._exchanges[destination] = exchange
        return exchange

    def _fabricar_callback(
        self, handler: MessageHandler, queue: str
    ) -> Callable[[AbstractIncomingMessage], Awaitable[None]]:
        """Constroi o callback que ``aio_pika`` invoca por mensagem entregue.

        O callback normaliza a mensagem nativa em :class:`InboundMessage` -- guardando a propria
        mensagem nativa no campo opaco ``delivery_tag``, para que ``ack``/``nack``/``reply`` possam
        confirma-la depois -- e a repassa ao ``handler`` do nucleo. Rastreia a *task* corrente em
        :attr:`_em_voo` para que :meth:`close` possa aguardar os handlers em voo. **Nao** confirma
        nada: o ACK/NACK e decisao do nucleo, posterior ao *commit* (R2.1). Se o handler levantar
        -- o que o contrato proibe --, a mensagem fica nao confirmada e volta a fila na proxima
        queda de canal, preservando a garantia *at-least-once*.

        Args:
            handler: Corrotina do nucleo a invocar por mensagem.
            queue: Nome da fila, espelhado em ``InboundMessage.queue`` para compor a chave de
                idempotencia ``"<servico>:<fila>"``.

        Returns:
            O callback assincrono aceito por ``queue.consume``.
        """

        async def _callback(mensagem: AbstractIncomingMessage) -> None:
            tarefa = asyncio.current_task()
            if tarefa is not None:
                self._em_voo.add(tarefa)
            try:
                entrada = InboundMessage(
                    body=mensagem.body,
                    headers=dict(mensagem.headers or {}),
                    queue=queue,
                    message_id=mensagem.message_id,
                    correlation_id=mensagem.correlation_id,
                    reply_to=mensagem.reply_to,
                    redelivered=bool(mensagem.redelivered),
                    delivery_tag=mensagem,
                )
                await handler(entrada)
            finally:
                if tarefa is not None:
                    self._em_voo.discard(tarefa)

        return _callback

    def _mensagem_nativa(self, message: InboundMessage, operacao: str) -> AbstractIncomingMessage:
        """Recupera a ``AbstractIncomingMessage`` nativa guardada no ``delivery_tag`` opaco.

        Args:
            message: A :class:`InboundMessage` recebida.
            operacao: Nome da operacao, apenas para diagnostico no erro.

        Returns:
            A mensagem nativa de ``aio_pika``, apta a ``ack``/``nack``.

        Raises:
            TransportError: Se ``delivery_tag`` nao carregar uma mensagem nativa -- entrega forjada
                fora do fluxo normal de :meth:`consume`.
        """
        nativa = message.delivery_tag
        if not isinstance(nativa, AbstractIncomingMessage):
            raise TransportError(
                f"{operacao}: entrega sem mensagem nativa associada",
                fila=message.queue,
                message_id=message.message_id,
            )
        return nativa
