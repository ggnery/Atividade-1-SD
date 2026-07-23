"""Configuracao, topologia declarativa e *factory* de transporte (R9.2, R2.6, R3.3).

Tres responsabilidades, todas de "o que o middleware sabe antes de abrir a primeira conexao":

1. :class:`Settings` -- leitura das variaveis de ambiente da secao 12.4, com validacao de tipo e
   de faixa na **entrada**. Um ``PREFETCH=dez`` ou um ``MAX_TENTATIVAS=-1`` falha na inicializacao
   do processo, com mensagem apontando a variavel, em vez de virar comportamento errado em tempo
   de execucao.
2. :class:`TopologySpec` e amigos -- a topologia como **dado declarativo**, nao como codigo
   espalhado pelos servicos. E a mesma estrutura que ``AmqpTransport`` materializa em exchanges,
   filas e bindings, que ``SqsTransport`` traduz em fila SQS + topico SNS + *redrive policy*
   (R9.3), e que ``MemoryTransport`` usa como dicionario de casadores de padrao. Uma unica fonte
   de verdade para tres transportes -- e isso que da substancia a R9.2.
3. :func:`criar_transporte` -- a *factory*. **Nenhum arquivo em** ``services/`` **menciona**
   ``AmqpTransport``: todos recebem o :class:`~hospitalmq.transport.base.Transport` construido
   aqui, e e por isso que a suite funcional roda com ``TRANSPORTE=memory``, sem broker (R10.4),
   usando exatamente o mesmo codigo de servico que roda em producao local com AMQP.

Precedencia de configuracao, do mais forte para o mais fraco: variavel exportada no shell ->
``environment:`` do servico no ``docker-compose.yml`` -> ``env_file: .env`` -> valor padrao no
codigo. Cada variavel aceita **duas grafias**: o nome curto do ``.env.example`` e o nome com o
prefixo ``HOSPITALMQ_``, que tem precedencia -- o prefixo existe para o caso, previsto por C1, de
a biblioteca ser embutida em um sistema hospedeiro que ja defina variaveis de mesmo nome curto.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from hospitalmq.errors import ConfigError

if TYPE_CHECKING:  # pragma: no cover - evita importacao circular com transport/base.py
    from hospitalmq.transport.base import Transport

__all__ = [
    "ATRASOS_PADRAO_MS",
    "EXCHANGE_DLX",
    "EXCHANGE_EVENTOS",
    "EXCHANGE_PADRAO",
    "EXCHANGE_RPC",
    "MAX_TENTATIVAS_PADRAO",
    "PREFETCH_PADRAO",
    "RPC_TIMEOUT_PADRAO_S",
    "TOPOLOGIA_PADRAO",
    "Binding",
    "ExchangeSpec",
    "NomeTransporte",
    "QueueSpec",
    "Settings",
    "TopologySpec",
    "build_transport",
    "criar_transporte",
    "fila_de_retorno_rpc",
    "settings",
]

# --------------------------------------------------------------------------- #
# Constantes canonicas da topologia (secoes 6.1.5 e 6.3)                       #
# --------------------------------------------------------------------------- #

EXCHANGE_EVENTOS: Final[str] = "hospital.events"
"""Exchange ``topic`` durable por onde passam **todos** os eventos de dominio (secao 6.1.2)."""

EXCHANGE_RPC: Final[str] = "hospital.rpc"
"""Exchange ``direct`` durable das requisicoes RPC, separado dos eventos (secao 6.1.3)."""

EXCHANGE_DLX: Final[str] = "hospital.dlx"
"""Exchange ``topic`` durable de descarte -- destino de toda mensagem que morre (secao 6.1.4)."""

EXCHANGE_PADRAO: Final[str] = ""
"""Exchange padrao do AMQP. Entrega pela *routing key* diretamente a fila de mesmo nome.

Usado no retorno das filas de espera (secao 6.2.4) e na resposta do RPC, cujo endereco vem em
``reply_to``. E nomeado aqui para que nenhum modulo precise digitar a string vazia solta.
"""

PREFETCH_PADRAO: Final[int] = 10
"""Maximo de mensagens nao confirmadas em voo por replica (R2.6, secao 6.6.3)."""

MAX_TENTATIVAS_PADRAO: Final[int] = 3
"""Numero maximo de retentativas antes da DLQ (R2.2, R2.3)."""

ATRASOS_PADRAO_MS: Final[tuple[int, ...]] = (1000, 2000, 4000)
"""Backoff exponencial 1s / 2s / 4s, sem *jitter* por decisao explicita (secao 2.9.3)."""

RPC_TIMEOUT_PADRAO_S: Final[float] = 5.0
"""Prazo padrao da chamada RPC, em segundos (R3.3)."""

NomeTransporte = Literal["amqp", "sqs", "memory"]
"""Dominio fechado da variavel ``TRANSPORTE`` (R9.2)."""


def _rotulo_atraso(delay_ms: int) -> str:
    """Converte um atraso em milissegundos no sufixo de nome de fila (``1000`` -> ``"1s"``)."""
    if delay_ms % 1000 == 0:
        return f"{delay_ms // 1000}s"
    return f"{delay_ms}ms"


# --------------------------------------------------------------------------- #
# Topologia declarativa                                                        #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class Binding:
    """Ligacao entre um exchange e uma fila (secao 6.1.1).

    Attributes:
        exchange: Nome do exchange de origem.
        key: *Binding key*. Em exchange ``topic`` aceita os curingas ``*`` (um segmento) e ``#``
            (zero ou mais segmentos) -- e o ``#`` de ``q.audit.todos`` que da a R7.1 a cobertura
            de eventos que ainda nao existem.
    """

    exchange: str
    key: str


@dataclass(frozen=True, slots=True)
class ExchangeSpec:
    """Declaracao de um exchange.

    Attributes:
        name: Nome do exchange.
        type: ``"topic"``, ``"direct"``, ``"fanout"`` ou ``"headers"``.
        durable: Se sobrevive ao reinicio do broker. Sempre ``True`` neste projeto (R1.4).
    """

    name: str
    type: Literal["topic", "direct", "fanout", "headers"] = "topic"
    durable: bool = True


@dataclass(frozen=True, slots=True)
class QueueSpec:
    """Declaracao de uma fila e de tudo que dela deriva (secoes 6.3, 6.9.3, 6.10.2).

    Uma unica ``QueueSpec`` gera, no broker, ate cinco objetos: a fila de negocio, sua DLQ e as
    tres filas de espera com TTL. As derivadas **nunca** sao escritas a mao -- seus nomes saem de
    :attr:`dlq_name` e :meth:`retry_queue_name`.

    Attributes:
        name: Nome da fila, na forma ``q.<servico>.<assunto>`` (secao 6.1.5).
        bindings: Ligacoes que alimentam a fila. Vazio quando a fila so recebe publicacao direta
            pelo exchange padrao (filas de espera e de retorno de RPC).
        durable: Se a fila sobrevive ao reinicio do broker (R1.4).
        exclusive: Se pertence a uma unica conexao. Apenas as filas de retorno do RPC.
        auto_delete: Se e removida quando o ultimo consumidor sai. Apenas as filas de retorno.
        prefetch: Limite de mensagens nao confirmadas em voo por replica (R2.6).
        with_dlq: Se possui DLQ e ``x-dead-letter-exchange`` apontando para ``hospital.dlx``.
        retry_delays_ms: Niveis de espera da retentativa, em milissegundos. Vazio desliga a
            retentativa -- caso de ``q.gateway.projecao`` (projecao se autocorrige) e das filas
            de RPC (R3.4 exige que a excecao volte ao chamador, nao que seja retentada).
        max_length: Teto de mensagens acumuladas; ``None`` significa sem limite. Combinado com
            :attr:`overflow`, e o mecanismo de *back-pressure* da secao 6.9.
        overflow: Politica ao atingir :attr:`max_length`. ``"reject-publish"`` devolve *nack* ao
            produtor em vez de descartar a mensagem mais antiga.
        dead_letter_exchange: Exchange de descarte associado quando :attr:`with_dlq`.
    """

    name: str
    bindings: tuple[Binding, ...] = ()
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    prefetch: int = PREFETCH_PADRAO
    with_dlq: bool = True
    retry_delays_ms: tuple[int, ...] = ()
    max_length: int | None = None
    overflow: Literal["reject-publish", "drop-head"] = "reject-publish"
    dead_letter_exchange: str = EXCHANGE_DLX

    @property
    def dlq_name(self) -> str:
        """Nome da DLQ associada -- **sufixo** ``.dlq``, nunca prefixo (secao 6.1.5).

        Returns:
            ``"<nome-da-fila>.dlq"``. E tambem a *routing key* usada em ``hospital.dlx``.
        """
        return f"{self.name}.dlq"

    def retry_queue_name(self, delay_ms: int) -> str:
        """Nome da fila de espera de um nivel de atraso.

        Args:
            delay_ms: Atraso do nivel, em milissegundos.

        Returns:
            ``"<nome-da-fila>.retry.<atraso>"``, ex. ``"q.alert.alerta-gerado.retry.2s"``.
        """
        return f"{self.name}.retry.{_rotulo_atraso(delay_ms)}"

    def retry_queues(self) -> tuple[tuple[str, int], ...]:
        """Enumera as filas de espera derivadas desta fila.

        Uma fila por nivel de atraso, e nao uma fila unica com TTL por mensagem: o RabbitMQ so
        avalia a expiracao da mensagem que esta na **cabeca** da fila, entao uma mensagem de 4 s
        na frente bloquearia uma de 1 s atras dela (*head-of-line blocking*).

        Returns:
            Pares ``(nome_da_fila, atraso_ms)``, na ordem dos niveis.
        """
        return tuple((self.retry_queue_name(ms), ms) for ms in self.retry_delays_ms)

    def fila_de_retentativa(self, attempt: int) -> str | None:
        """Escolhe a fila de espera para a proxima tentativa.

        Args:
            attempt: Numero da tentativa que **acabou de falhar**, comecando em 1.

        Returns:
            O nome da fila de espera do nivel correspondente, ou ``None`` se a fila nao tem
            retentativa configurada ou se os niveis se esgotaram -- caso em que o destino e a DLQ.
        """
        if attempt < 1 or attempt > len(self.retry_delays_ms):
            return None
        return self.retry_queue_name(self.retry_delays_ms[attempt - 1])


@dataclass(frozen=True, slots=True)
class TopologySpec:
    """A topologia completa do sistema, como dado (secao 6.2).

    Consumida integralmente por ``Transport.declare_topology`` no *boot* de **todo** processo que
    instancia o HospitalMQ -- os seis servicos e o cliente de leito. Nao e um exagero: em AMQP,
    mensagem publicada em exchange sem fila ligada e **descartada silenciosamente**; se cada
    consumidor declarasse apenas a sua fila, os eventos publicados antes de o consumidor subir
    simplesmente sumiriam, violando R1.4 e R1.5. Com todos declarando tudo, a fila existe antes da
    primeira publicacao, independentemente da ordem de *boot*. A declaracao AMQP e idempotente,
    entao os seis processos subindo ao mesmo tempo nao competem.

    Attributes:
        exchanges: Exchanges a declarar, na ordem em que devem ser criados.
        queues: Filas de negocio. As DLQs e as filas de espera **nao** aparecem aqui: sao
            derivadas de cada :class:`QueueSpec` pelo driver, para que nome e argumento nunca
            divirjam.
    """

    exchanges: tuple[ExchangeSpec, ...] = ()
    queues: tuple[QueueSpec, ...] = ()

    def fila(self, nome: str) -> QueueSpec:
        """Recupera a declaracao de uma fila pelo nome.

        Args:
            nome: Nome exato da fila de negocio.

        Returns:
            A :class:`QueueSpec` correspondente.

        Raises:
            ConfigError: Se a fila nao constar da topologia -- consumir ou retentar em uma fila
                nao declarada e defeito de configuracao, nao condicao de corrida.
        """
        for spec in self.queues:
            if spec.name == nome:
                return spec
        raise ConfigError(f"fila nao declarada na topologia: {nome!r}", fila=nome)

    def acrescentar(self, *queues: QueueSpec) -> TopologySpec:
        """Devolve uma copia da topologia com filas adicionais.

        Usado pelo ``RpcClient``, que declara a propria fila de retorno exclusiva sem alterar a
        topologia compartilhada (secao 4.3.6).

        Args:
            *queues: Filas a acrescentar.

        Returns:
            Nova :class:`TopologySpec`; a original permanece intacta.
        """
        return replace(self, queues=(*self.queues, *queues))


def fila_de_retorno_rpc(producer: str) -> QueueSpec:
    """Monta a fila de retorno exclusiva de uma instancia de ``RpcClient`` (secao 4.3.6).

    O nome e gerado pelo **cliente**, e nao pelo broker: nomes gerados pelo servidor sao um
    recurso do AMQP que **nao existe no SQS**, onde ``CreateQueue`` exige o nome. Delegar a
    geracao ao cliente mantem o mesmo codigo de ``rpc.py`` valido nos dois transportes, que e o
    teste real de R9.2.

    Args:
        producer: Nome do servico chamador, ex. ``"api-gateway"``.

    Returns:
        Uma :class:`QueueSpec` ``q.rpc.reply.<producer>.<uuid4hex>``, exclusiva, ``auto_delete`` e
        **nao** duravel -- resposta que sobrevivesse ao reinicio do broker chegaria depois de o
        chamador ja ter desistido por timeout.
    """
    return QueueSpec(
        name=f"q.rpc.reply.{producer}.{uuid.uuid4().hex}",
        durable=False,
        exclusive=True,
        auto_delete=True,
        with_dlq=False,
    )


TOPOLOGIA_PADRAO: Final[TopologySpec] = TopologySpec(
    exchanges=(
        ExchangeSpec(name=EXCHANGE_EVENTOS, type="topic", durable=True),
        ExchangeSpec(name=EXCHANGE_RPC, type="direct", durable=True),
        ExchangeSpec(name=EXCHANGE_DLX, type="topic", durable=True),
    ),
    queues=(
        QueueSpec(
            name="q.vitals.sinais-coletados",
            bindings=(Binding(exchange=EXCHANGE_EVENTOS, key="sinais.coletados"),),
            retry_delays_ms=ATRASOS_PADRAO_MS,
            max_length=50_000,
        ),
        QueueSpec(
            name="q.triage.sinais-registrados",
            bindings=(Binding(exchange=EXCHANGE_EVENTOS, key="sinais.registrados"),),
            retry_delays_ms=ATRASOS_PADRAO_MS,
            max_length=50_000,
        ),
        QueueSpec(
            name="q.alert.alerta-gerado",
            bindings=(Binding(exchange=EXCHANGE_EVENTOS, key="alerta.gerado"),),
            retry_delays_ms=ATRASOS_PADRAO_MS,
            max_length=20_000,
        ),
        # Binding '#' captura o futuro: um tipo de evento novo passa a ser auditado no instante
        # em que o primeiro produtor o publica, sem binding novo e sem deploy (R7.1, secao 6.5).
        QueueSpec(
            name="q.audit.todos",
            bindings=(Binding(exchange=EXCHANGE_EVENTOS, key="#"),),
            retry_delays_ms=ATRASOS_PADRAO_MS,
            max_length=200_000,
        ),
        # Sem retentativa: a projecao do painel e estado derivado e efemero (R11.7); quando a
        # retentativa chegasse, o card ja teria sido atualizado por um evento mais recente.
        QueueSpec(
            name="q.gateway.projecao",
            bindings=(
                Binding(exchange=EXCHANGE_EVENTOS, key="paciente.*"),
                Binding(exchange=EXCHANGE_EVENTOS, key="leito.*"),
                Binding(exchange=EXCHANGE_EVENTOS, key="alerta.*"),
                Binding(exchange=EXCHANGE_EVENTOS, key="sinais.registrados"),
                Binding(exchange=EXCHANGE_EVENTOS, key="sinais.rejeitados"),
            ),
            max_length=20_000,
        ),
        # Sem retentativa: R3.4 exige que a excecao do servidor volte ao chamador como resposta de
        # erro. Retentar em silencio manteria o RpcClient esperando ate virar RpcTimeoutError.
        QueueSpec(
            name="q.rpc.admission",
            bindings=(Binding(exchange=EXCHANGE_RPC, key="rpc.admission"),),
        ),
        QueueSpec(
            name="q.rpc.vitals",
            bindings=(Binding(exchange=EXCHANGE_RPC, key="rpc.vitals"),),
        ),
    ),
)
"""Topologia canonica do Hospital Inteligente: 3 exchanges, 7 filas de negocio, 7 DLQs e 12 filas
de espera (4 filas com retentativa x 3 niveis), alem das filas de retorno de RPC efemeras."""


# --------------------------------------------------------------------------- #
# Configuracao por ambiente (secao 12.4)                                       #
# --------------------------------------------------------------------------- #


class Settings(BaseSettings):
    """Configuracao do HospitalMQ lida do ambiente, com valores padrao seguros.

    Cada campo aceita duas grafias: o nome curto do ``.env.example`` e o nome prefixado com
    ``HOSPITALMQ_``, que tem precedencia.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    transporte: NomeTransporte = Field(
        default="amqp",
        validation_alias=AliasChoices("HOSPITALMQ_TRANSPORT", "TRANSPORTE"),
        description="Driver de transporte: amqp (RabbitMQ), sqs (projetado) ou memory (testes).",
    )
    amqp_url: str = Field(
        default="amqp://hospital:hospital@rabbitmq:5672/hospital",
        validation_alias=AliasChoices("HOSPITALMQ_AMQP_URL", "AMQP_URL"),
        description="URL de conexao com o Broker, incluindo o vhost.",
    )
    aws_region: str = Field(
        default="us-east-1",
        validation_alias=AliasChoices("HOSPITALMQ_AWS_REGION", "AWS_REGION"),
        description="Regiao usada pelo SqsTransport (R9.3).",
    )
    service_name: str = Field(
        default="hospitalmq",
        validation_alias=AliasChoices("HOSPITALMQ_SERVICE_NAME", "SERVICE_NAME"),
        description="Nome do processo. Alimenta o campo 'servico' do log e o 'producer' do "
        "Envelope; cada servico define o seu no docker-compose.yml.",
    )
    database_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HOSPITALMQ_DATABASE_URL", "DATABASE_URL"),
        description="DSN do SQLAlchemy async. Ausente no api-gateway por construcao (secao 8.1.1).",
    )
    jwt_secret: str = Field(
        default="troque-esta-chave-em-producao",
        validation_alias=AliasChoices("HOSPITALMQ_JWT_SECRET", "JWT_SECRET"),
        description="Segredo HS256 de assinatura e verificacao do JWT (R4.1, R4.2).",
    )
    jwt_expiracao_min: int = Field(
        default=30,
        ge=1,
        le=1440,
        validation_alias=AliasChoices("HOSPITALMQ_JWT_EXPIRACAO_MIN", "JWT_EXPIRACAO_MIN"),
        description="Validade do token em minutos (R4.2).",
    )
    api_keys: str = Field(
        default=(
            "dev-monitor-l07:monitor-l07,"
            "dev-monitor-l12:monitor-l12,"
            "dev-monitor-uti03:monitor-uti-03"
        ),
        validation_alias=AliasChoices("HOSPITALMQ_API_KEYS", "API_KEYS"),
        description="Lista 'chave:sub' separada por virgula das credenciais de dispositivo (R4.5).",
    )
    log_level: str = Field(
        default="INFO",
        validation_alias=AliasChoices("HOSPITALMQ_LOG_LEVEL", "LOG_LEVEL"),
        description="Nivel minimo emitido pelo log estruturado (R5.1).",
    )
    prefetch: int = Field(
        default=PREFETCH_PADRAO,
        ge=1,
        le=1000,
        validation_alias=AliasChoices("HOSPITALMQ_PREFETCH", "PREFETCH"),
        description="Maximo de mensagens nao confirmadas em voo por replica (R2.6).",
    )
    max_tentativas: int = Field(
        default=MAX_TENTATIVAS_PADRAO,
        ge=0,
        le=10,
        validation_alias=AliasChoices("HOSPITALMQ_MAX_TENTATIVAS", "MAX_TENTATIVAS"),
        description="Retentativas antes da DLQ. Com 0, a primeira falha vai direto (R2.2, R2.3).",
    )
    rpc_timeout_s: float = Field(
        default=RPC_TIMEOUT_PADRAO_S,
        gt=0,
        le=60,
        validation_alias=AliasChoices("HOSPITALMQ_RPC_TIMEOUT_S", "RPC_TIMEOUT_S"),
        description="Timeout padrao da chamada RPC, em segundos (R3.3, R8.4).",
    )
    confirm_timeout_s: float = Field(
        default=5.0,
        gt=0,
        le=60,
        validation_alias=AliasChoices("HOSPITALMQ_CONFIRM_TIMEOUT_S", "CONFIRM_TIMEOUT_S"),
        description="Teto para o broker confirmar a durabilidade da publicacao (R1.5).",
    )
    metrics_protegido: bool = Field(
        default=False,
        validation_alias=AliasChoices("HOSPITALMQ_METRICS_PROTEGIDO", "METRICS_PROTEGIDO"),
        description="Quando true, /metrics exige papel admin ou auditor (R5.5, secao 8.6.2).",
    )

    def api_keys_pares(self) -> dict[str, str]:
        """Interpreta :attr:`api_keys` no mapa ``chave -> sub`` usado por ``auth.py`` (R4.5).

        Returns:
            Dicionario da chave de API para o identificador do dispositivo. Entradas malformadas
            -- sem os dois-pontos separadores -- sao ignoradas silenciosamente, para que uma
            virgula sobrando no ``.env`` nao derrube o processo.
        """
        pares: dict[str, str] = {}
        for item in self.api_keys.split(","):
            bruto = item.strip()
            if not bruto or ":" not in bruto:
                continue
            chave, _, sub = bruto.partition(":")
            if chave.strip() and sub.strip():
                pares[chave.strip()] = sub.strip()
        return pares

    def atrasos_ms(self) -> tuple[int, ...]:
        """Calcula os niveis de espera efetivos a partir de :attr:`max_tentativas`.

        Returns:
            O prefixo de :data:`ATRASOS_PADRAO_MS` com ``max_tentativas`` niveis -- ``()`` quando
            ``MAX_TENTATIVAS=0``, o que faz a primeira falha ir direto a DLQ.
        """
        return ATRASOS_PADRAO_MS[: self.max_tentativas]


settings: Final[Settings] = Settings()
"""Instancia unica lida no *import*. Todos os processos leem daqui o nome do servico e o nivel de
log antes de qualquer outra coisa (secao 9.2.2)."""


# --------------------------------------------------------------------------- #
# Factory de transporte (R9.2, secao 4.3.4)                                    #
# --------------------------------------------------------------------------- #


def criar_transporte(cfg: Settings | None = None) -> Transport:
    """Constroi o driver de transporte indicado pela configuracao (R9.2).

    E a funcao que materializa a promessa "trocar o transporte e mudar variavel de ambiente". Os
    drivers sao importados **sob demanda**, de modo que um processo rodando com
    ``TRANSPORTE=memory`` jamais carrega ``aio_pika`` ou ``boto3``.

    Args:
        cfg: Configuracao a usar. Se omitida, usa a instancia global :data:`settings`.

    Returns:
        O :class:`~hospitalmq.transport.base.Transport` correspondente a ``cfg.transporte``, ainda
        **nao** conectado -- quem chama ``connect`` e ``declare_topology`` e o *bootstrap* do
        processo.

    Raises:
        ConfigError: Se ``TRANSPORTE`` tiver valor desconhecido, ou se o driver escolhido nao
            satisfizer estruturalmente o contrato de :class:`~hospitalmq.transport.base.Transport`.
    """
    from hospitalmq.transport.base import Transport as _Transport

    alvo = cfg if cfg is not None else settings

    transporte: object
    match alvo.transporte:
        case "amqp":
            from hospitalmq.transport.amqp import AmqpTransport

            transporte = AmqpTransport(
                url=alvo.amqp_url,
                confirm_timeout_s=alvo.confirm_timeout_s,
            )
        case "memory":
            from hospitalmq.transport.memory import MemoryTransport

            transporte = MemoryTransport()
        case "sqs":
            from hospitalmq.transport.sqs import SqsTransport

            transporte = SqsTransport(region=alvo.aws_region)
        case outro:  # pragma: no cover - protegido pelo Literal, mantido como rede de seguranca
            raise ConfigError(f"transporte desconhecido: {outro}", transporte=outro)

    if not isinstance(transporte, _Transport):  # checagem defensiva permitida pelo Protocol
        raise ConfigError(
            f"driver {type(transporte).__name__} nao implementa a interface Transport",
            transporte=alvo.transporte,
        )
    return transporte


def build_transport(cfg: Settings | None = None) -> Transport:
    """Apelido em ingles de :func:`criar_transporte`, citado na secao 4.3.4 do design.

    Args:
        cfg: Configuracao a usar; ``None`` usa a instancia global :data:`settings`.

    Returns:
        O transporte construido.
    """
    return criar_transporte(cfg)
