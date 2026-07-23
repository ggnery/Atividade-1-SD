"""Comunicacao sincrona: RPC sobre fila (Request-Reply), lado chamador e lado respondedor.

Este modulo implementa o padrao *Request-Reply* (secao 5 do design) sobre a interface abstrata
:class:`~hospitalmq.transport.base.Transport`, sem jamais tocar em ``aio_pika`` nem ``boto3``. Ele
existe porque tres operacoes do Hospital Inteligente **precisam** da resposta no mesmo ciclo HTTP --
consultar prontuario, admitir paciente, dar alta -- e um evento assincrono nao devolve resultado a
quem chamou (secao 5.1.3).

Duas classes formam a superficie publica:

* :class:`RpcClient` -- o lado chamador. Um por processo, compartilhado por todas as rotas HTTP.
  Publica a requisicao no exchange ``hospital.rpc``, registra uma *future* indexada pelo
  identificador de correlacao **da chamada** e aguarda a resposta com ``asyncio.wait_for``. A
  disciplina critica (secao 5.4.4) e: registrar a *future* **antes** de publicar, remove-la em um
  unico ``finally`` que cobre toda saida, e nunca deixar o *callback* de resposta propagar excecao.
* :class:`RpcServer` -- o lado respondedor. Um por ``Servico_Consumidor`` que exponha operacoes.
  Despacha por ``Envelope.type``, valida o papel com a identidade que veio no Envelope (R4.6),
  executa o handler e **sempre responde** -- inclusive em caso de erro, que vira resposta tipada em
  vez de deixar a chamada expirar por timeout (R3.4, secao 5.6.4).

**O ponto mais sutil do desenho (secao 5.4.3).** A palavra "correlacao" acumula dois papeis, e
confundi-los produz um bug que viola R3.5. O ``_pendentes`` e indexado pela propriedade AMQP
``correlation_id`` -- que carrega o ``message_id`` da requisicao, **unico por chamada** -- e nao
pelo ``correlation_id`` do Envelope, que e o identificador de **rastreio** e pode ser
compartilhado por duas chamadas concorrentes (o ``asyncio.gather`` do *snapshot* de boot). Indexar
por rastreio faria a segunda chamada sobrescrever a *future* da primeira.

Divergencias registradas em relacao ao pseudocodigo normativo da secao 5, todas por fidelidade a
interface real da fundacao (``hospitalmq/transport/base.py`` e ``hospitalmq/config.py``):

* ``Transport.publish`` **nao** expoe ``mandatory`` nem ``expiration_ms``; a protecao do chamador
  fica inteiramente por conta de ``asyncio.wait_for`` (secao 5.5.1).
* ``Transport.reply`` **nao** expoe ``mandatory``; a resposta orfa e simplesmente descartada pelo
  broker quando a fila de retorno ja nao existe.
* A chave de roteamento das filas de RPC e ``rpc.admission`` / ``rpc.vitals`` -- os *binding keys*
  reais de :data:`~hospitalmq.config.TOPOLOGIA_PADRAO` -- e nao ``admission`` / ``vitals`` como
  sugere o texto da secao 5.2; num exchange ``direct`` a chave publicada precisa ser identica ao
  *binding*, senao a requisicao nao e roteavel.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Final

from hospitalmq.clock import Clock, RealClock
from hospitalmq.config import EXCHANGE_RPC, RPC_TIMEOUT_PADRAO_S, TopologySpec, fila_de_retorno_rpc
from hospitalmq.envelope import Envelope, Identity
from hospitalmq.errors import (
    AuthError,
    HospitalMQError,
    InvalidEnvelopeError,
    RpcRemoteError,
    RpcTimeoutError,
    TransportError,
)
from hospitalmq.logging import LogEvent, Logger, correlation_id_atual, get_logger, message_id_atual
from hospitalmq.metrics import Metrics, get_metrics
from hospitalmq.transport.base import InboundMessage, Subscription, Transport

if TYPE_CHECKING:  # pragma: no cover - apenas para tipagem; nao importado em tempo de execucao
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from hospitalmq.idempotency import IdempotencyStore

    HandlerRpc = Callable[[dict[str, Any], "ContextoRpc"], Awaitable[dict[str, Any]]]

__all__ = [
    "OPERACOES_DE_ESCRITA",
    "ORCAMENTO_ROTA_PADRAO",
    "ROTAS_RPC",
    "TIMEOUT_RPC_PADRAO",
    "ContextoRpc",
    "Orcamento",
    "RpcClient",
    "RpcServer",
]


# --------------------------------------------------------------------------- #
# Constantes e catalogo de operacoes (secoes 5.4.1, 5.5.4 e 5.8)              #
# --------------------------------------------------------------------------- #

TIMEOUT_RPC_PADRAO: Final[float] = RPC_TIMEOUT_PADRAO_S
"""Prazo padrao de uma chamada RPC, em segundos (R3.3).

Igual a :data:`~hospitalmq.config.RPC_TIMEOUT_PADRAO_S`.
"""

ORCAMENTO_ROTA_PADRAO: Final[float] = 8.0
"""Teto de uma rota HTTP que compoe chamadas RPC encadeadas (secao 5.5.4), menor que o timeout do
cliente para que o gateway sempre responda antes de o cliente desistir."""

ROTAS_RPC: Final[dict[str, str]] = {
    # Atendidas pelo admission-service, fila q.rpc.admission.
    "paciente.criar": "rpc.admission",
    "paciente.admitir": "rpc.admission",
    "paciente.dar-alta": "rpc.admission",
    "prontuario.consultar": "rpc.admission",
    "leitos.snapshot": "rpc.admission",
    # Atendida pelo vitals-service, fila q.rpc.vitals.
    "sinais.ultimos": "rpc.vitals",
}
"""Traducao operacao -> *routing key* do exchange ``hospital.rpc`` (secao 5.4.1).

Quem chama nomeia **o que** quer (a operacao), nunca **onde** isso mora (a fila). Mover
``sinais.ultimos`` para um futuro servico e uma linha aqui; nenhuma rota do FastAPI muda. Os valores
sao os *binding keys* reais de :data:`~hospitalmq.config.TOPOLOGIA_PADRAO` -- ``rpc.admission`` e
``rpc.vitals`` -- porque o exchange ``hospital.rpc`` e ``direct`` e exige igualdade exata da chave.
"""

OPERACOES_DE_ESCRITA: Final[frozenset[str]] = frozenset(
    {"paciente.criar", "paciente.admitir", "paciente.dar-alta"}
)
"""Operacoes que alteram estado (secao 5.7). Um timeout numa delas tem resultado **indeterminado**
(secao 5.5.5): o texto devolvido ao cliente instrui a reapresentacao idempotente pelo mesmo
``X-Message-ID``, que o :class:`RpcServer` reconhece (R2.4)."""

#: *routing key* de resposta usada quando nem sequer foi possivel desserializar a requisicao.
_TIPO_RESPOSTA_GENERICO: Final[str] = "rpc"

#: Sufixo do ``Envelope.type`` da resposta -- ``<operacao>.resposta`` (secao 5.6.3).
_SUFIXO_RESPOSTA: Final[str] = ".resposta"


# --------------------------------------------------------------------------- #
# Orcamento de tempo ponta a ponta (secao 5.5.4)                              #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class Orcamento:
    """Prazo absoluto de uma rota HTTP, propagado entre chamadas RPC encadeadas (secao 5.5.4).

    Uma rota que compoe duas chamadas em sequencia -- ``GET /pacientes/{id}/prontuario`` chama
    ``prontuario.consultar`` e depois ``sinais.ultimos`` -- nao pode dar 5 s a cada uma, senao o
    pior caso (10 s) alcanca o timeout do cliente e produz uma corrida entre camadas. A solucao e
    propagar um **prazo**, e nao um timeout por chamada: cada :meth:`para_chamada` devolve o quanto
    ainda sobra, ja limitado ao teto de uma chamada.

    Attributes:
        fim: Instante de :func:`time.monotonic` em que o orcamento se esgota.
    """

    fim: float

    @classmethod
    def de(cls, segundos: float = ORCAMENTO_ROTA_PADRAO) -> Orcamento:
        """Abre um orcamento que expira ``segundos`` a partir de agora.

        Args:
            segundos: Duracao total da rota, em segundos. Padrao :data:`ORCAMENTO_ROTA_PADRAO`.

        Returns:
            Um novo orcamento com o instante de fim ja calculado.
        """
        return cls(fim=time.monotonic() + segundos)

    def restante(self) -> float:
        """Devolve quantos segundos ainda restam do orcamento (pode ser negativo)."""
        return self.fim - time.monotonic()

    def para_chamada(self, teto: float = TIMEOUT_RPC_PADRAO) -> float:
        """Calcula o timeout da proxima chamada, limitado pelo que resta do orcamento.

        Args:
            teto: Teto de uma unica chamada, em segundos. Padrao :data:`TIMEOUT_RPC_PADRAO`.

        Returns:
            O menor entre ``teto`` e o tempo restante.

        Raises:
            RpcTimeoutError: Se o que resta e menor que a latencia de um salto (0,05 s) -- a proxima
                chamada nem chega a ser publicada, evitando trabalho orfao previsivel.
        """
        sobra = self.restante()
        if sobra <= 0.05:
            raise RpcTimeoutError(
                "orcamento da rota esgotado antes da proxima chamada",
                operacao="composicao",
                timeout=0.0,
                correlation_id=correlation_id_atual(),
            )
        return min(teto, sobra)


# --------------------------------------------------------------------------- #
# RpcClient -- lado chamador (secoes 5.4 e 5.5)                               #
# --------------------------------------------------------------------------- #


class RpcClient:
    """Lado chamador do Request-Reply. Um por processo, compartilhado por todas as rotas HTTP.

    A fila de retorno e **nomeada pelo cliente** (``q.rpc.reply.<producer>.<uuid4hex>``, secao
    4.3.6), declarada uma unica vez em :meth:`start` e consumida por uma assinatura de background.
    Cada chamada registra uma :class:`asyncio.Future` em :attr:`_pendentes`, indexada pelo
    identificador de correlacao da chamada (o ``message_id`` da requisicao), e a resolve quando a
    resposta correspondente chega.

    Args:
        transport: O :class:`~hospitalmq.transport.base.Transport` ja construido pela *factory*.
        producer: Nome do servico chamador, ex. ``"api-gateway"``. Compoe o nome da fila de retorno
            e o campo ``producer`` do Envelope de requisicao.
        exchange: Exchange ``direct`` das requisicoes RPC. Padrao ``hospital.rpc``.
        rotas: Mapa operacao -> *routing key*. Padrao :data:`ROTAS_RPC`.
        default_timeout: Timeout padrao aplicado quando :meth:`call` e chamada sem ``timeout``
            explicito. Padrao :data:`TIMEOUT_RPC_PADRAO`.
        clock: Fonte de tempo para carimbar a requisicao e medir a duracao. Padrao
            :class:`~hospitalmq.clock.RealClock`.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        producer: str,
        exchange: str = EXCHANGE_RPC,
        rotas: dict[str, str] | None = None,
        default_timeout: float = TIMEOUT_RPC_PADRAO,
        clock: Clock | None = None,
    ) -> None:
        """Constroi o cliente. Nada de rede acontece aqui -- :meth:`start` faz a declaracao."""
        self._transport: Transport = transport
        self._producer: str = producer
        self._exchange: str = exchange
        self._rotas: dict[str, str] = dict(rotas) if rotas is not None else dict(ROTAS_RPC)
        self._default_timeout: float = default_timeout
        self._clock: Clock = clock if clock is not None else RealClock()

        self._log: Logger = get_logger(__name__)
        self._metrics: Metrics = get_metrics()

        self._pendentes: dict[str, asyncio.Future[Envelope]] = {}
        self._fila_retorno: str | None = None
        self._assinatura: Subscription | None = None

    async def start(self) -> None:
        """Declara a fila de retorno exclusiva e sobe o consumidor de background.

        A fila e ``q.rpc.reply.<producer>.<uuid4hex>``, exclusiva, ``auto_delete`` e nao duravel
        (secao 5.4.2). Ela e declarada por
        :meth:`~hospitalmq.transport.base.Transport.declare_topology` junto a uma
        :class:`~hospitalmq.config.TopologySpec` que contem **apenas** ela -- os demais
        objetos ja foram declarados no *boot* do processo, e a declaracao AMQP e idempotente.

        Chamar duas vezes e permitido e nao reabre nada.
        """
        if self._fila_retorno is not None:
            return
        spec = fila_de_retorno_rpc(self._producer)
        await self._transport.declare_topology(TopologySpec(queues=(spec,)))
        self._fila_retorno = spec.name
        self._assinatura = await self._transport.consume(
            queue=spec.name,
            handler=self._on_reply,
            prefetch=0,
        )

    async def call(
        self,
        operacao: str,
        payload: dict[str, Any],
        *,
        # `timeout` e o contrato publico normativo do RPC (R3.3, secao 5.4.1).
        timeout: float | None = None,  # noqa: ASYNC109
        identity: Identity | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
    ) -> dict[str, Any]:
        """Publica a requisicao e aguarda a resposta correspondente (R3.1, R3.2, R3.3).

        ``message_id`` explicito e reservado as operacoes de escrita: reapresentar o mesmo
        identificador e o que permite ao :class:`RpcServer` reconhecer a repeticao e nao duplicar o
        efeito (R2.4, ver secao 5.6.5).

        Args:
            operacao: Nome da operacao, ex. ``"paciente.admitir"``. Precisa constar de
                :attr:`_rotas`.
            payload: Corpo da requisicao, serializavel em JSON.
            timeout: Prazo em segundos. ``None`` usa o ``default_timeout`` do cliente.
            identity: Identidade de quem originou a acao (R4.3). Propagada no Envelope.
            correlation_id: Identificador de rastreio da requisicao HTTP. ``None`` herda do contexto
                de log corrente, ou gera um novo se nao houver.
            message_id: ``message_id`` da requisicao. ``None`` gera um UUIDv4 novo; um valor
                explicito habilita a reapresentacao idempotente das escritas (secao 5.5.5).

        Returns:
            O objeto ``dados`` do payload de resposta, quando ``status == "ok"``.

        Raises:
            RuntimeError: Se :meth:`start` nao tiver sido chamado.
            KeyError: Se ``operacao`` nao constar de :attr:`_rotas`.
            RpcTimeoutError: Nenhuma resposta dentro de ``timeout`` segundos (R3.3).
            RpcRemoteError: O :class:`RpcServer` devolveu envelope de erro (R3.4).
            TransportError: Broker inacessivel ou conexao perdida com a chamada pendente.
        """
        if self._fila_retorno is None:
            raise RuntimeError("RpcClient.start() nao foi chamado antes de call()")

        try:
            routing_key = self._rotas[operacao]
        except KeyError:
            raise KeyError(f"operacao RPC sem rota configurada: {operacao!r}") from None

        prazo = timeout if timeout is not None else self._default_timeout

        requisicao = Envelope(
            message_id=message_id or str(uuid.uuid4()),
            correlation_id=correlation_id or correlation_id_atual() or str(uuid.uuid4()),
            causation_id=message_id_atual(),
            type=operacao,
            version=1,
            timestamp=self._clock.now(),
            producer=self._producer,
            identity=identity,
            attempt=1,
            payload=dict(payload),
        )
        rpc_id = requisicao.message_id  # chave do casamento, unica por chamada (secao 5.4.3)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Envelope] = loop.create_future()
        self._pendentes[rpc_id] = future  # REGISTRA ANTES DE PUBLICAR (secao 5.4.4, invariante 1)
        self._atualizar_gauge()

        inicio = self._clock.monotonic()
        self._log.debug(
            LogEvent.RPC_CHAMADA_INICIADA, operacao=operacao, rpc_id=rpc_id, timeout_s=prazo
        )
        self._metrics.incr("rpc_chamadas_total", operacao=operacao)

        try:
            await self._transport.publish(
                destination=self._exchange,
                routing_key=routing_key,
                body=requisicao.to_bytes(),
                headers=requisicao.to_headers(),
                message_id=rpc_id,
                correlation_id=rpc_id,  # Correlation Identifier: a propriedade AMQP casa a resposta
                reply_to=self._fila_retorno,  # Return Address
                persistent=False,  # requisicao que sobrevive a restart ja estourou o timeout
            )
            resposta = await asyncio.wait_for(future, timeout=prazo)
        except TimeoutError as exc:
            escrita = operacao in OPERACOES_DE_ESCRITA
            self._metrics.incr("rpc_timeouts_total", operacao=operacao)
            if escrita:
                self._metrics.incr("rpc_escritas_indeterminadas_total", operacao=operacao)
            self._log.error(
                LogEvent.RPC_TIMEOUT,
                operacao=operacao,
                rpc_id=rpc_id,
                timeout_s=prazo,
                escrita=escrita,
            )
            raise RpcTimeoutError(
                operacao=operacao,
                timeout=prazo,
                correlation_id=requisicao.correlation_id,
                message_id=rpc_id,  # o cliente pode reapresenta-lo (secao 5.5.5)
                escrita=escrita,
            ) from exc
        finally:
            # Unico ponto de remocao: cobre sucesso, timeout, erro remoto, TransportError na
            # publicacao e cancelamento externo da task HTTP (secao 5.4.4, invariante 2).
            self._pendentes.pop(rpc_id, None)
            self._atualizar_gauge()

        return self._interpretar_resposta(operacao, rpc_id, resposta, inicio)

    def _interpretar_resposta(
        self, operacao: str, rpc_id: str, resposta: Envelope, inicio: float
    ) -> dict[str, Any]:
        """Traduz o payload de resposta em valor de retorno ou excecao (secoes 5.6.3 e 5.7).

        Args:
            operacao: Nome da operacao chamada.
            rpc_id: Identificador de correlacao da chamada.
            resposta: Envelope de resposta ja desserializado.
            inicio: Instante monotonico do inicio da chamada, para medir a duracao.

        Returns:
            O objeto ``dados`` quando ``status == "ok"``.

        Raises:
            RpcRemoteError: Quando ``status == "erro"`` ou quando o payload nao segue o contrato.
        """
        duracao_ms = (self._clock.monotonic() - inicio) * 1000.0
        status = resposta.payload.get("status")
        self._log.info(
            LogEvent.RPC_RESPOSTA_RECEBIDA,
            operacao=operacao,
            rpc_id=rpc_id,
            duracao_ms=round(duracao_ms, 3),
            status=status,
        )
        self._metrics.observe_duracao(operacao, duracao_ms)

        if status == "ok":
            dados = resposta.payload.get("dados")
            return dados if isinstance(dados, dict) else {}

        if status == "erro":
            erro = resposta.payload.get("erro")
            erro = erro if isinstance(erro, dict) else {}
            codigo = str(erro.get("codigo", "ERRO_INTERNO"))
            mensagem = str(erro.get("mensagem", ""))
            retentavel = bool(erro.get("retentavel", False))
            self._log.warning(
                LogEvent.RPC_ERRO_REMOTO, operacao=operacao, codigo=codigo, retentavel=retentavel
            )
            raise RpcRemoteError(mensagem, codigo=codigo, operacao=operacao, retentavel=retentavel)

        raise RpcRemoteError(
            f"resposta com status desconhecido: {status!r}",
            codigo="RESPOSTA_INVALIDA",
            operacao=operacao,
            retentavel=False,
        )

    async def _on_reply(self, mensagem: InboundMessage) -> None:
        """Callback do consumidor de background da fila de retorno. **Nunca** levanta excecao.

        Casa a resposta com a *future* pendente pela propriedade AMQP ``correlation_id`` (secao
        5.4.3). Confirma a mensagem imediatamente -- a fila de retorno opera com semantica de
        auto-ack (secao 5.4.2): se o processo cai, todas as *futures* morrem junto e reentregar nao
        conserta nada.

        Args:
            mensagem: A resposta recebida da fila de retorno.
        """
        try:
            rpc_id = mensagem.correlation_id
            future = self._pendentes.get(rpc_id) if rpc_id is not None else None

            if future is None:
                self._log.warning(
                    LogEvent.RPC_RESPOSTA_ORFA, rpc_id=rpc_id, motivo="sem_future_pendente"
                )
                return  # timeout ja ocorreu, ou duplicata: descarta (secao 5.5.3)
            if future.done():
                self._log.warning(
                    LogEvent.RPC_RESPOSTA_ORFA, rpc_id=rpc_id, motivo="future_ja_resolvida"
                )
                return  # protege contra InvalidStateError numa future ja cancelada pelo wait_for

            try:
                future.set_result(Envelope.from_bytes(mensagem.body))
            except ValueError as exc:  # resposta malformada (InvalidEnvelopeError e ValueError)
                future.set_exception(
                    RpcRemoteError(
                        str(exc),
                        codigo="RESPOSTA_INVALIDA",
                        operacao="desconhecida",
                        retentavel=False,
                    )
                )
        finally:
            with contextlib.suppress(TransportError):
                await self._transport.ack(mensagem)

    async def close(self) -> None:
        """Cancela o consumo de background e falha todas as *futures* ainda pendentes.

        Sem falhar as pendentes, uma queda do broker faria toda requisicao em voo esperar o timeout
        completo antes de falhar, quando a falha ja e conhecida no instante zero (secao 5.4.4). E
        idempotente: chamar duas vezes nao e erro.
        """
        if self._assinatura is not None:
            await self._assinatura.cancel()
            self._assinatura = None
        self._falhar_pendentes(
            TransportError("RpcClient encerrado com chamadas pendentes", motivo="cliente_encerrado")
        )
        self._fila_retorno = None

    def _falhar_pendentes(self, exc: BaseException) -> None:
        """Resolve com excecao toda *future* ainda pendente e limpa o dicionario."""
        for future in list(self._pendentes.values()):
            if not future.done():
                future.set_exception(exc)
        self._pendentes.clear()
        self._atualizar_gauge()

    def _atualizar_gauge(self) -> None:
        """Publica ``len(self._pendentes)`` no medidor ``rpc_pendentes`` (secao 5.4.5).

        Um valor que cresce monotonicamente e a assinatura inequivoca de vazamento de *futures* -- a
        primeira metrica que se olha quando o gateway comeca a consumir memoria.
        """
        self._metrics.definir_medidor("rpc_pendentes", float(len(self._pendentes)))


# --------------------------------------------------------------------------- #
# RpcServer -- lado respondedor (secoes 5.6 e 5.7)                            #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class ContextoRpc:
    """Contexto de uma operacao RPC entregue ao handler (secao 5.6.1).

    Attributes:
        correlation_id: Identificador de rastreio da requisicao HTTP, para os logs do handler.
        rpc_id: ``message_id`` da requisicao -- o identificador de correlacao da chamada.
        identity: Quem originou a acao, validada na borda e propagada no Envelope (R4.6). ``None``
            apenas quando a operacao nao declara papeis e a requisicao nao trouxe credencial.
        recebido_em: Instante monotonico da chegada, para medir a duracao da operacao.
        repetida: ``True`` quando o ``message_id`` ja constava da marca de idempotencia -- o handler
            de escrita rele pela chave natural e devolve o desfecho ja produzido (secoes 5.5.5,
            5.6.5).
    """

    correlation_id: str
    rpc_id: str
    identity: Identity | None
    recebido_em: float
    repetida: bool


@dataclass(frozen=True, slots=True)
class _OperacaoRegistrada:
    """Uma operacao registrada no despacho do :class:`RpcServer`."""

    handler: HandlerRpc
    roles: frozenset[str] | None
    escrita: bool


class RpcServer:
    """Lado respondedor do Request-Reply. Um por ``Servico_Consumidor`` que exponha operacoes.

    O despacho e um ``dict[str, _OperacaoRegistrada]`` consultado por ``Envelope.type``; operacao
    nao registrada devolve ``OPERACAO_DESCONHECIDA`` em vez de estourar. Cada operacao declara os
    papeis autorizados junto do registro, o que implementa defesa em profundidade: o gateway ja
    barra por
    ``role`` no endpoint, e o ``RpcServer`` valida de novo com a identidade do Envelope (R4.6).

    **Todos** os caminhos convergem para responder e dar ACK (secao 5.6.4): nenhuma falha de handler
    resulta em silencio -- e o que R3.4 exige, "devolver uma resposta de erro em vez de deixar a
    chamada expirar por timeout". Nao ha retentativa no caminho RPC; a idempotencia por
    ``message_id`` cobre apenas as operacoes de escrita (R2.4, secao 5.6.5).

    Args:
        transport: O :class:`~hospitalmq.transport.base.Transport` ja construido pela *factory*.
        fila: Nome da fila de RPC a consumir, ex. ``"q.rpc.admission"``. Ja declarada no *boot*.
        producer: Nome do servico respondedor, gravado no ``producer`` do Envelope de resposta.
        prefetch: Maximo de requisicoes nao confirmadas em voo por assinatura (R2.6).
        sessao: *Session maker* assincrono do SQLAlchemy, exigido pelas operacoes de escrita para
            abrir a transacao unica que envolve a marca de idempotencia e o efeito (secao 5.6.5).
        idempotencia: *Store* de idempotencia. Padrao um
            :class:`~hospitalmq.idempotency.SqlIdempotencyStore`, construido apenas quando
            ``sessao`` e fornecida.
        consumidor: Chave de ``consumidor`` da marca de idempotencia, distinta da usada pelos
            consumidores de evento do mesmo servico. Padrao ``"<producer>:<fila>"``.
        clock: Fonte de tempo para carimbar respostas e medir duracoes. Padrao
            :class:`~hospitalmq.clock.RealClock`.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        fila: str,
        producer: str,
        prefetch: int = 10,
        sessao: async_sessionmaker[AsyncSession] | None = None,
        idempotencia: IdempotencyStore | None = None,
        consumidor: str | None = None,
        clock: Clock | None = None,
    ) -> None:
        """Constroi o servidor. Nada de rede acontece aqui -- :meth:`start` sobe o consumo."""
        self._transport: Transport = transport
        self._fila: str = fila
        self._producer: str = producer
        self._prefetch: int = prefetch
        self._sessao: async_sessionmaker[AsyncSession] | None = sessao
        self._clock: Clock = clock if clock is not None else RealClock()

        self._log: Logger = get_logger(__name__)
        self._metrics: Metrics = get_metrics()

        self._operacoes: dict[str, _OperacaoRegistrada] = {}
        self._assinatura: Subscription | None = None

        if consumidor is not None:
            self._consumidor: str = consumidor
        else:
            from hospitalmq.idempotency import consumidor_de

            self._consumidor = consumidor_de(producer, fila)

        self._idempotencia: IdempotencyStore | None
        if idempotencia is not None:
            self._idempotencia = idempotencia
        elif sessao is not None:
            from hospitalmq.idempotency import SqlIdempotencyStore

            self._idempotencia = SqlIdempotencyStore(clock=self._clock)
        else:
            self._idempotencia = None

    # -- registro de operacoes --------------------------------------------- #

    def operacao(
        self,
        nome: str,
        *,
        roles: frozenset[str] | None = None,
        escrita: bool = False,
    ) -> Callable[[HandlerRpc], HandlerRpc]:
        """Decorador de registro de operacao (secao 5.6.1).

        Args:
            nome: Nome da operacao, ex. ``"prontuario.consultar"``.
            roles: Papeis autorizados. ``None`` significa operacao sem verificacao de papel.
            escrita: ``True`` liga a maquina de idempotencia (R2.4). Exige ``sessao`` configurada.

        Returns:
            O decorador que registra o handler e o devolve inalterado.

        Raises:
            ValueError: Se ``nome`` ja estiver registrado.
            RuntimeError: Se ``escrita=True`` sem ``sessao`` configurada.
        """

        def decorador(handler: HandlerRpc) -> HandlerRpc:
            self.registrar(nome, handler, roles=roles, escrita=escrita)
            return handler

        return decorador

    def registrar(
        self,
        nome: str,
        handler: HandlerRpc,
        *,
        roles: frozenset[str] | None = None,
        escrita: bool = False,
    ) -> None:
        """Registra uma operacao no despacho (forma nao decoradora de :meth:`operacao`).

        Args:
            nome: Nome da operacao.
            handler: Corrotina ``(payload, ctx) -> dados``.
            roles: Papeis autorizados, ou ``None``.
            escrita: ``True`` liga a idempotencia; exige ``sessao``.

        Raises:
            ValueError: Se ``nome`` ja estiver registrado.
            RuntimeError: Se ``escrita=True`` sem ``sessao`` configurada.
        """
        if nome in self._operacoes:
            raise ValueError(f"operacao RPC ja registrada: {nome!r}")
        if escrita and self._sessao is None:
            raise RuntimeError(
                f"operacao de escrita {nome!r} exige um session maker (parametro 'sessao')"
            )
        self._operacoes[nome] = _OperacaoRegistrada(handler=handler, roles=roles, escrita=escrita)

    # -- ciclo de vida ----------------------------------------------------- #

    async def start(self) -> None:
        """Sobe o consumo da fila de RPC. Idempotente."""
        if self._assinatura is not None:
            return
        self._assinatura = await self._transport.consume(
            queue=self._fila, handler=self._ao_receber, prefetch=self._prefetch
        )

    async def stop(self) -> None:
        """Cancela o consumo da fila de RPC. Idempotente e nunca levanta."""
        if self._assinatura is not None:
            await self._assinatura.cancel()
            self._assinatura = None

    # -- pipeline por requisicao ------------------------------------------- #

    async def _ao_receber(self, mensagem: InboundMessage) -> None:
        """Processa uma requisicao, responde e confirma. **Nunca** levanta (contrato de consume).

        Convergencia estrutural da secao 5.6.4: monta a resposta (de sucesso ou de erro), tenta
        entrega-la e, em qualquer desfecho, confirma a requisicao no ``finally``. Nao existe caminho
        de codigo que processe uma requisicao e nao a confirme -- o unico efeito de nao confirmar
        seria reentrega em laco ate a expiracao (secao 5.6.4).

        Args:
            mensagem: A requisicao recebida da fila de RPC.
        """
        try:
            resposta = await self._processar(mensagem)
            try:
                await self._transport.reply(
                    mensagem,
                    body=resposta.to_bytes(),
                    correlation_id=mensagem.correlation_id or "",
                )
            except TransportError as exc:
                # Fila de retorno ja apagada: o chamador desistiu ou caiu. Nao e motivo para
                # reprocessar -- o efeito, se havia, ja foi commitado (secao 5.6.2).
                self._log.warning(
                    LogEvent.RPC_RESPOSTA_NAO_ENTREGUE,
                    operacao=resposta.type.removesuffix(_SUFIXO_RESPOSTA),
                    rpc_id=mensagem.correlation_id,
                    destino=mensagem.reply_to,
                    erro=str(exc),
                )
        except Exception:  # rede de seguranca: o contrato diz que o handler nao deve levantar
            self._log.exception(LogEvent.RPC_OPERACAO_CONCLUIDA, fila=mensagem.queue)
        finally:
            with contextlib.suppress(TransportError):
                await self._transport.ack(mensagem)

    async def _processar(self, mensagem: InboundMessage) -> Envelope:
        """Executa o despacho e devolve o Envelope de resposta (secao 5.6.4).

        Trata internamente todos os desfechos de negocio -- envelope invalido, operacao
        desconhecida, papel insuficiente, erro do handler -- transformando cada um em resposta
        tipada. Nunca levanta por erro de negocio.

        Args:
            mensagem: A requisicao recebida.

        Returns:
            O Envelope de resposta pronto para :meth:`~hospitalmq.transport.base.Transport.reply`.
        """
        try:
            requisicao = Envelope.from_bytes(mensagem.body)
        except InvalidEnvelopeError as exc:
            self._log.warning(LogEvent.RPC_OPERACAO_RECEBIDA, fila=mensagem.queue, erro=str(exc))
            return self._resposta(
                None,
                mensagem,
                _TIPO_RESPOSTA_GENERICO,
                self._payload_erro("ENVELOPE_INVALIDO", str(exc), retentavel=False),
            )

        operacao = requisicao.type
        registro = self._operacoes.get(operacao)
        if registro is None:
            return self._resposta(
                requisicao,
                mensagem,
                operacao,
                self._payload_erro(
                    "OPERACAO_DESCONHECIDA",
                    f"operacao nao registrada neste servidor: {operacao!r}",
                    retentavel=False,
                ),
            )

        if registro.roles is not None:
            try:
                self._verificar_papel(requisicao.identity, registro.roles)
            except AuthError as exc:
                self._log.warning(LogEvent.AUTH_NEGADA, operacao=operacao, motivo=exc.motivo)
                return self._resposta(
                    requisicao,
                    mensagem,
                    operacao,
                    self._payload_erro("NAO_AUTORIZADO", exc.mensagem, retentavel=False),
                )

        ctx = ContextoRpc(
            correlation_id=requisicao.correlation_id,
            rpc_id=requisicao.message_id,
            identity=requisicao.identity,
            recebido_em=self._clock.monotonic(),
            repetida=False,
        )
        self._log.debug(
            LogEvent.RPC_OPERACAO_RECEBIDA, operacao=operacao, rpc_id=requisicao.message_id
        )

        try:
            if registro.escrita:
                dados, repetida = await self._executar_escrita(registro, requisicao, ctx)
            else:
                dados = await registro.handler(requisicao.payload, ctx)
                repetida = False
        except AuthError as exc:
            self._log.warning(LogEvent.AUTH_NEGADA, operacao=operacao, motivo=exc.motivo)
            return self._resposta(
                requisicao,
                mensagem,
                operacao,
                self._payload_erro("NAO_AUTORIZADO", exc.mensagem, retentavel=False),
            )
        except HospitalMQError as exc:
            # PermanentError e TransientError carregam codigo e retentavel proprios (secao 5.7).
            return self._resposta(
                requisicao,
                mensagem,
                operacao,
                self._payload_erro(exc.codigo, exc.mensagem, retentavel=exc.retentavel),
            )
        except Exception:
            # A stack trace vai so para o log; detalhe interno nao cruza a fronteira (secao 5.6.4).
            self._log.exception(LogEvent.RPC_OPERACAO_CONCLUIDA, operacao=operacao)
            return self._resposta(
                requisicao,
                mensagem,
                operacao,
                self._payload_erro(
                    "ERRO_INTERNO", "erro interno ao processar a operacao", retentavel=False
                ),
            )

        duracao_ms = (self._clock.monotonic() - ctx.recebido_em) * 1000.0
        self._log.info(
            LogEvent.RPC_OPERACAO_CONCLUIDA,
            operacao=operacao,
            duracao_ms=round(duracao_ms, 3),
            resultado="ok",
            repetida=repetida,
        )
        self._metrics.observe_duracao(operacao, duracao_ms)
        return self._resposta(requisicao, mensagem, operacao, {"status": "ok", "dados": dados})

    async def _executar_escrita(
        self, registro: _OperacaoRegistrada, requisicao: Envelope, ctx: ContextoRpc
    ) -> tuple[dict[str, Any], bool]:
        """Executa uma operacao de escrita dentro de uma transacao unica (secao 5.6.5).

        A marca de idempotencia e o efeito do handler compartilham a **mesma** transacao: um
        ``ROLLBACK`` desfaz os dois, e um ``COMMIT`` os persiste juntos. Quando o ``message_id`` ja
        constava da marca, ``ctx.repetida`` fica ``True`` e o handler devolve o desfecho existente
        relendo pela chave natural, sem reexecutar o efeito.

        Args:
            registro: A operacao registrada.
            requisicao: O Envelope da requisicao.
            ctx: O contexto base, com ``repetida=False``.

        Returns:
            Par ``(dados, repetida)`` -- o resultado do handler e se foi uma reapresentacao.
        """
        assert self._sessao is not None  # garantido no registro (escrita exige sessao)
        assert self._idempotencia is not None
        async with self._sessao.begin() as session:
            inedito = await self._idempotencia.marcar(session, requisicao, self._consumidor)
            ctx_escrita = replace(ctx, repetida=not inedito)
            dados = await registro.handler(requisicao.payload, ctx_escrita)
        return dados, ctx_escrita.repetida

    @staticmethod
    def _verificar_papel(identidade: Identity | None, papeis: frozenset[str]) -> None:
        """Valida o papel da identidade contra os papeis autorizados da operacao (R4.6).

        Raises:
            AuthError: ``credencial_ausente`` se ``identidade`` for ``None``; ``papel_insuficiente``
                se o papel nao constar de ``papeis``.
        """
        from hospitalmq.auth import verificar_papel

        verificar_papel(identidade, *papeis)

    def _resposta(
        self,
        requisicao: Envelope | None,
        mensagem: InboundMessage,
        operacao: str,
        payload: dict[str, Any],
    ) -> Envelope:
        """Monta o Envelope de resposta (secao 5.6.3).

        Quando ha requisicao valida, deriva dela -- o que preserva ``correlation_id`` de rastreio,
        define ``causation_id`` como o ``message_id`` da requisicao e ecoa a ``identity`` (R4.3).
        Quando a requisicao era indecifravel, constroi a resposta a partir das propriedades nativas
        da mensagem, para que o :class:`RpcClient` ainda consiga casa-la e levantar
        :class:`~hospitalmq.errors.RpcRemoteError`.

        Args:
            requisicao: O Envelope da requisicao, ou ``None`` se indecifravel.
            mensagem: A requisicao bruta recebida.
            operacao: Nome da operacao (ou ``"rpc"`` quando desconhecida).
            payload: O payload da resposta, ``{"status": "ok"|"erro", ...}``.

        Returns:
            O Envelope de resposta.
        """
        tipo = f"{operacao}{_SUFIXO_RESPOSTA}"
        if requisicao is not None:
            return requisicao.derivar(
                tipo=tipo,
                payload=payload,
                producer=self._producer,
                timestamp=self._clock.now(),
            )
        return Envelope(
            message_id=str(uuid.uuid4()),
            correlation_id=mensagem.correlation_id or str(uuid.uuid4()),
            causation_id=mensagem.message_id,
            type=tipo,
            version=1,
            timestamp=self._clock.now(),
            producer=self._producer,
            identity=None,
            attempt=1,
            payload=payload,
        )

    @staticmethod
    def _payload_erro(codigo: str, mensagem: str, *, retentavel: bool) -> dict[str, Any]:
        """Monta o payload de resposta de erro do contrato de 5.6.3."""
        return {
            "status": "erro",
            "erro": {"codigo": codigo, "mensagem": mensagem, "retentavel": retentavel},
        }
