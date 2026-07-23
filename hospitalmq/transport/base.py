"""A interface Transporte -- a prova de portabilidade do HospitalMQ (R9.1, R9.2).

Esta e a peca que transforma "migrar para AWS" em uma troca de variavel de ambiente. O nucleo do
HospitalMQ **nunca** importa ``aio_pika`` nem ``boto3``: fala apenas com o :class:`Transport`
declarado aqui.

Este modulo e a **definicao normativa unica** da interface. Sao **oito** metodos: as cinco
operacoes que R9.1 exige literalmente -- publicar, consumir, confirmar, rejeitar e responder, isto
e :meth:`~Transport.publish`, :meth:`~Transport.consume`, :meth:`~Transport.ack`,
:meth:`~Transport.nack` e :meth:`~Transport.reply` -- mais tres de ciclo de vida
(:meth:`~Transport.connect`, :meth:`~Transport.declare_topology`, :meth:`~Transport.close`), que
nao sao operacoes de mensagem e existem porque um recurso de rede precisa ser aberto, declarado e
fechado.

Correspondencia entre o texto de R9.1 (em portugues, por ser documento de requisitos) e os
identificadores (em ingles, pela convencao de idioma da secao 4.1.3):

============  ==========================  ==================================================
R9.1          Metodo normativo            Onde e usado no nucleo
============  ==========================  ==================================================
publicar      :meth:`Transport.publish`   ``Publisher``, *relay* do outbox, retentativa, DLQ
consumir      :meth:`Transport.consume`   ``Consumer.run``, ``RpcServer``, fila de retorno
confirmar     :meth:`Transport.ack`       fim do *pipeline* por mensagem e caminho de duplicata
rejeitar      :meth:`Transport.nack`      encerramento gracioso e rede de seguranca do broker
responder     :meth:`Transport.reply`     ``RpcServer``
============  ==========================  ==================================================

**Nao existe** ``Transport.request()``. A maquina de estados do RPC -- dicionario de *futures*
pendentes, registro antes da publicacao, ``asyncio.wait_for``, limpeza em ``finally``, cancelamento
em massa na queda de conexao -- e semantica de mensageria, nao de transporte, e vive uma unica vez
em ``hospitalmq/rpc.py``. Declara-la aqui obrigaria cada driver a reimplementar a mesma maquina.

Invariantes transversais que **toda** implementacao deve respeitar:

1. **``delivery_tag`` e opaco.** Seu tipo estatico e ``object`` e o nucleo nunca o inspeciona:
   apenas devolve a :class:`InboundMessage` inteira em ``ack``, ``nack`` e ``reply``. E essa
   opacidade que permite ao ``ReceiptHandle`` do SQS -- uma **string** -- ocupar o mesmo lugar do
   ``DeliveryTag`` **numerico** do AMQP.
2. **Bytes entram, bytes saem.** O transporte nao conhece Envelope, JSON, retentativa nem
   idempotencia. A serializacao acontece uma vez, no ``Publisher``; a desserializacao uma vez, no
   ``Consumer``.
3. **Toda falha de infraestrutura vira :class:`~hospitalmq.errors.TransportError`.** Nenhuma
   excecao de ``aio_pika``, ``aiormq`` ou ``botocore`` escapa para as camadas superiores.
4. **Entrega e *at-least-once*.** Nenhuma implementacao promete entrega unica; a supressao de
   duplicata e responsabilidade do nucleo (``idempotency.py``, R2.4).

**Contrato de construcao dos drivers**, consumido pela *factory*
:func:`hospitalmq.config.criar_transporte`::

    AmqpTransport(*, url: str, confirm_timeout_s: float = 5.0, clock: Clock | None = None)
    MemoryTransport(*, clock: Clock | None = None)
    SqsTransport(*, region: str, clock: Clock | None = None)

Tipagem **estrutural** (``Protocol``) e nao herança (``ABC``): o driver nao precisa importar este
modulo para ser valido, o que elimina acoplamento de importacao e facilita *fakes* nos testes;
``mypy`` verifica a conformidade estaticamente e ``@runtime_checkable`` permite um ``isinstance``
defensivo na *factory*.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:  # pragma: no cover - evita importacao circular com config.py
    from hospitalmq.config import TopologySpec

__all__ = ["InboundMessage", "MessageHandler", "Subscription", "Transport"]


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """Mensagem recebida, ja normalizada, ainda **nao** confirmada.

    E o unico tipo que atravessa a fronteira do transporte na direcao de entrada. Os drivers
    podem guardar o objeto nativo do broker dentro do campo opaco :attr:`delivery_tag`, mas o que
    o nucleo ve e sempre esta estrutura -- nunca ``MensagemRecebida``, ``Delivery`` ou
    ``SqsDelivery``.

    Attributes:
        body: Corpo bruto, o Envelope serializado em JSON UTF-8. Bytes opacos para o transporte.
        headers: Cabecalhos da mensagem, incluindo os do middleware com prefixo ``x-hmq-`` e os
            do proprio broker (``x-death``, ``x-first-death-reason``).
        queue: Nome da fila de onde a mensagem foi entregue. Compoe a chave de idempotencia
            ``"<servico>:<fila>"`` (secao 4.8.1).
        message_id: Propriedade nativa espelhada do Envelope, quando presente.
        correlation_id: Propriedade nativa espelhada do Envelope. No RPC, e por ela que a
            resposta e casada com a chamada.
        reply_to: Endereco de retorno indicado pelo chamador RPC (*Return Address*). ``None`` em
            mensagens de evento.
        redelivered: ``True`` quando o broker ja tentou entregar esta mensagem antes. E dica de
            diagnostico, **nao** contador de tentativas: o contador confiavel e
            ``Envelope.attempt``, que viaja dentro do corpo (secao 4.6.3).
        delivery_tag: Identificador de entrega, **opaco**. ``DeliveryTag`` (inteiro) no AMQP,
            ``ReceiptHandle`` (string) no SQS, indice interno no ``MemoryTransport``. O nucleo
            jamais o inspeciona.
    """

    body: bytes
    headers: Mapping[str, Any]
    queue: str
    message_id: str | None
    correlation_id: str | None
    reply_to: str | None
    redelivered: bool
    delivery_tag: object


class Subscription(Protocol):
    """Assinatura ativa de consumo, cancelavel.

    Devolvida por :meth:`Transport.consume`. Cancelar interrompe a entrega de **novas** mensagens
    sem descartar as que ja estao em voo: estas ainda serao confirmadas ou devolvidas a fila.
    """

    async def cancel(self) -> None:
        """Cancela a assinatura, parando a entrega de novas mensagens.

        Deve ser idempotente: cancelar duas vezes nao e erro. Nao aguarda os handlers em voo --
        quem faz isso e :meth:`Transport.close`.
        """
        ...


MessageHandler = Callable[[InboundMessage], Awaitable[None]]
"""Corrotina que o transporte invoca para cada mensagem entregue.

O transporte **nunca** confirma automaticamente: quem decide entre ``ack`` e ``nack`` e o nucleo,
depois do *commit* do efeito colateral (R2.1).
"""


@runtime_checkable
class Transport(Protocol):
    """Contrato abstrato de transporte de mensagens (R9.1, R9.2).

    Implementado por ``AmqpTransport`` (RabbitMQ, sobre ``aio-pika``), ``SqsTransport`` (AWS,
    projetado -- C4) e ``MemoryTransport`` (filas ``asyncio``, para os testes sem broker de R10.3
    e R10.4). Nenhum arquivo em ``services/`` menciona um transporte concreto: todos recebem o
    :class:`Transport` construido por :func:`hospitalmq.config.criar_transporte`, e e por isso que
    a suite funcional roda com ``TRANSPORTE=memory`` usando exatamente o mesmo codigo de servico
    que roda com AMQP.
    """

    # -- ciclo de vida ----------------------------------------------------- #

    async def connect(self) -> None:
        """Estabelece a conexao com o broker.

        Contrato obrigatorio: deve ser seguro chamar em processo que sobe **antes** do broker --
        a implementacao tenta novamente, com espera, ate o limite configurado. **Nao** declara
        topologia: isso e trabalho de :meth:`declare_topology`, chamado logo em seguida. Chamar
        duas vezes e permitido e nao deve abrir uma segunda conexao.

        Raises:
            TransportError: Se a conexao nao for estabelecida dentro do limite.
        """
        ...

    async def declare_topology(self, spec: TopologySpec) -> None:
        """Materializa a topologia inteira, de forma idempotente, antes de publicar ou consumir.

        Contrato obrigatorio -- a ordem importa e nao e negociavel (secao 6.11.1):

        1. **exchanges** (``hospital.events``, ``hospital.rpc``, ``hospital.dlx``);
        2. **DLQs e filas de espera**;
        3. **filas de negocio**, com os argumentos derivados da ``QueueSpec``;
        4. **bindings**.

        O passo 1 vem primeiro porque o RabbitMQ **nao valida** ``x-dead-letter-exchange`` na
        declaracao: a resolucao ocorre so quando a mensagem morre. Se ``hospital.dlx`` nao
        existisse naquele instante, a mensagem descartada seria perdida sem erro -- exatamente na
        situacao em que a preservacao importa mais.

        Divergencia de argumentos com um objeto ja existente e **falha fatal de boot** (o
        ``406 PRECONDITION_FAILED`` do AMQP), nunca redeclaracao destrutiva: apagar e recriar
        descartaria mensagens em repouso que podem ser sinais vitais ainda nao persistidos.

        Args:
            spec: A topologia declarativa completa -- exchanges, filas, bindings, DLQs e filas de
                espera. A mesma estrutura alimenta os tres drivers.

        Raises:
            TransportError: Falha de comunicacao com o broker.
            ConfigError: Objeto existente com argumentos divergentes. O processo deve encerrar
                com codigo 78 (``EX_CONFIG``).
        """
        ...

    async def close(self) -> None:
        """Encerra o transporte graciosamente.

        Contrato obrigatorio: cancelar as assinaturas, **aguardar os handlers em voo terminarem**
        e so entao fechar canal e conexao. Mensagens nao confirmadas devem retornar a fila,
        jamais serem descartadas. Deve ser idempotente e nunca levantar por transporte ja fechado.
        """
        ...

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
        """Publica uma mensagem (R9.1 "publicar").

        Contrato obrigatorio: retornar **somente apos confirmacao de durabilidade pelo broker**
        (*publisher confirms*). Nunca engolir falha; nunca bufferizar silenciosamente em memoria.
        Falhar em no maximo 5 s, conforme R1.5 -- e melhor o produtor saber que a publicacao nao
        aconteceu do que acreditar que aconteceu.

        Args:
            destination: Exchange no AMQP; fila ou topico no SQS/SNS. A string vazia designa o
                *exchange padrao* do AMQP, usado no retorno das filas de espera e na resposta RPC.
            routing_key: Chave de roteamento. Para eventos e igual a ``Envelope.type``; para a
                fila de espera e o nome da fila de destino.
            body: Corpo ja serializado. O transporte nao conhece Envelope (invariante 2).
            headers: Cabecalhos a espelhar, incluindo ``x-hmq-attempt`` e ``x-hmq-version``.
            message_id: Propriedade nativa ``message_id``, para correlacao na UI do broker.
            correlation_id: Propriedade nativa ``correlation_id``; obrigatoria no RPC.
            reply_to: Endereco de retorno (*Return Address*), preenchido apenas pelo ``RpcClient``.
            persistent: ``delivery_mode = 2`` quando ``True`` (R1.4). O ``RpcClient`` publica
                requisicoes com ``False``: uma requisicao que sobreviveu a um reinicio do broker
                ja estourou o timeout do chamador.
            delay_ms: Atraso minimo antes da entrega. No AMQP e realizado pela fila de espera com
                ``x-message-ttl``; no SQS por ``DelaySeconds``; no ``MemoryTransport`` pelo
                :class:`~hospitalmq.clock.Clock` injetado. ``None`` significa entrega imediata.

        Raises:
            TransportError: Broker indisponivel, publicacao nao confirmada em ate 5 s, ou
                mensagem recusada como nao roteavel (``basic.return``).
        """
        ...

    async def consume(
        self,
        *,
        queue: str,
        handler: MessageHandler,
        prefetch: int = 10,
    ) -> Subscription:
        """Registra um consumidor da fila (R9.1 "consumir").

        Contrato obrigatorio: entregar mensagens ao ``handler`` mantendo **no maximo ``prefetch``
        mensagens nao confirmadas** por assinatura (R2.6). **Nunca** confirmar automaticamente --
        o ACK e manual e posterior ao *commit* do efeito colateral (R2.1). Se a conexao cair com
        mensagens nao confirmadas, elas voltam para a fila. Multiplas assinaturas concorrentes na
        mesma fila operam em regime de *competing consumers*, com a mensagem entregue a exatamente
        um consumidor (R2.5).

        Args:
            queue: Nome da fila, ja declarada por :meth:`declare_topology`.
            handler: Corrotina invocada por mensagem. Nao deve levantar: quem trata erro e o
                *pipeline* do ``Consumer``.
            prefetch: Limite de mensagens nao confirmadas em voo por assinatura.

        Returns:
            A :class:`Subscription` cancelavel correspondente.

        Raises:
            TransportError: Fila inexistente ou falha de comunicacao com o broker.
        """
        ...

    async def ack(self, message: InboundMessage) -> None:
        """Confirma definitivamente a mensagem (R9.1 "confirmar").

        Contrato obrigatorio: torna a mensagem irrecuperavel, e por isso **deve ser posterior ao
        *commit*** do efeito colateral. Confirmar mensagem ja confirmada ou cujo prazo expirou
        deve **falhar visivelmente**, nunca em silencio: silenciar essa falha esconderia perda de
        mensagem por canal fechado.

        Args:
            message: A mensagem recebida, inteira. O driver le dela o campo opaco
                ``delivery_tag`` -- inteiro no AMQP, ``ReceiptHandle`` no SQS.

        Raises:
            TransportError: Canal fechado, entrega desconhecida ou prazo expirado.
        """
        ...

    async def nack(self, message: InboundMessage, *, requeue: bool = False) -> None:
        """Rejeita a mensagem (R9.1 "rejeitar").

        Contrato obrigatorio: com ``requeue=False``, entregar a mensagem ao mecanismo de descarte
        do broker -- no AMQP, o ``x-dead-letter-exchange`` da fila, que e a **rede de seguranca**
        contra descarte silencioso (R1.5). Com ``requeue=True``, devolve-la a fila; esse uso e
        **restrito ao encerramento gracioso** e nunca serve como retentativa, porque devolver a
        fila reentregaria imediatamente, sem espera, e reiniciaria o contador ``attempt``,
        anulando a garantia de "no maximo 3 retentativas" de R2.2.

        Args:
            message: A mensagem recebida, inteira.
            requeue: ``True`` apenas no encerramento gracioso.

        Raises:
            TransportError: Canal fechado ou entrega desconhecida.
        """
        ...

    async def reply(
        self,
        message: InboundMessage,
        *,
        body: bytes,
        correlation_id: str,
    ) -> None:
        """Responde a uma requisicao RPC (R9.1 "responder").

        Contrato obrigatorio: publicar a resposta no endereco indicado por ``message.reply_to``,
        preservando o ``correlation_id`` recebido **sem reinterpreta-lo** -- e ele que o
        ``RpcClient`` usa para casar a resposta com o *future* pendente. Se ``reply_to`` for nulo,
        e erro de programacao: falhar alto, nunca descartar em silencio.

        Args:
            message: A requisicao original, de onde sai o ``reply_to``.
            body: Corpo da resposta, ja serializado.
            correlation_id: O mesmo valor recebido na requisicao.

        Raises:
            TransportError: ``reply_to`` ausente, fila de retorno inexistente (o chamador
                desistiu) ou falha de comunicacao.
        """
        ...
