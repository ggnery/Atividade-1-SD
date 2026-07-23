"""Transporte em memoria: mesma interface do ``AmqpTransport``, zero rede, zero broker.

Este modulo e o principal habilitador de teste do HospitalMQ (secao 10.2 do design). Ele implementa
**exatamente** o mesmo ``Protocol`` de :mod:`hospitalmq.transport.base` que o ``AmqpTransport``
implementa -- os mesmos oito metodos, com as mesmas assinaturas -- e por isso a suite inteira do
middleware roda sem broker, sem container e sem espera de rede (R10.3, R10.4).

Reproduz com fidelidade as tres semanticas do AMQP das quais o middleware depende:

1. **Casamento de *routing key* do tipo ``topic``.** O algoritmo e o do AMQP de verdade --
   :func:`casa_topico`, com ``*`` casando exatamente um segmento e ``#`` casando zero ou mais.
   Se o casamento fosse simplificado para "``#`` casa com tudo", o teste do binding ``#`` da
   auditoria (R7.1) seria circular e nao provaria nada.
2. **ACK/NACK manual.** Nada e confirmado automaticamente; ``prefetch`` limita as mensagens nao
   confirmadas em voo por assinatura (R2.6) e o credito so volta em :meth:`MemoryTransport.ack` ou
   :meth:`MemoryTransport.nack`.
3. ***Dead-lettering* por ``x-dead-letter-exchange``.** ``nack(requeue=False)`` roteia a mensagem
   pelo exchange de descarte da fila com *routing key* ``<fila>.dlq``, como o broker faria (R2.3).

A fila de espera com TTL da secao 4.6.2 e **simulada, nao substituida por outro mecanismo**: a fila
``q.<nome>.retry.1s`` existe de verdade, o ``Consumer`` republica nela exatamente como em producao e
o "tempo de prateleira" e cumprido pelo :class:`~hospitalmq.clock.Clock` injetado. O que muda em
relacao ao RabbitMQ e apenas **quem** conta o tempo -- e e por isso que ``clock.sleeps == [1.0, 2.0,
4.0]`` e uma asserção legitima sobre a politica de R2.2, e nao sobre um caminho de codigo
alternativo.

O que este dublê **nao** simula, por decisao explicita (secao 10.2.4), fica coberto por
``tests/broker/``: durabilidade e sobrevivencia a reinicio, *publisher confirms*, a semantica exata
do cabecalho ``x-death``, expiracao real de ``x-message-ttl``, distribuicao entre processos
distintos, latencia e perda de conexao.

Alem dos oito metodos do contrato, a classe expõe **extensoes exclusivas de teste** -- ``drenar``,
``eventos``, ``na_dlq``, ``profundidade``, ``entregar``, ``acks``, ``nacks``,
``filas_de_espera_usadas`` -- que **nao fazem parte** do ``Protocol`` e nunca sao chamadas pelo
nucleo. ``drenar()`` em particular substitui o ``sleep(0.5)`` que suites de mensageria costumam usar
para "esperar o consumidor pegar a mensagem": como o transporte roda no mesmo *event loop*, da para
saber com precisao quando nao ha mais nada em voo.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from collections import deque
from collections.abc import Coroutine, Mapping
from dataclasses import dataclass
from typing import Any, Final

from hospitalmq.clock import Clock, ManualClock
from hospitalmq.config import (
    EXCHANGE_DLX,
    EXCHANGE_PADRAO,
    PREFETCH_PADRAO,
    ExchangeSpec,
    QueueSpec,
    TopologySpec,
)
from hospitalmq.envelope import (
    HEADER_DLQ_ERRO_TIPO,
    HEADER_DLQ_MOTIVO,
    HEADER_DLQ_SERVICO,
    Envelope,
)
from hospitalmq.errors import ConfigError, InvalidEnvelopeError, TransportError
from hospitalmq.transport.base import InboundMessage, MessageHandler, Subscription

__all__ = ["MemoryTransport", "MensagemMorta", "casa_topico"]


# --------------------------------------------------------------------------- #
# Casamento de routing key -- o algoritmo `topic` do AMQP, sem simplificacao    #
# --------------------------------------------------------------------------- #

_SEPARADOR: Final[str] = "."
_CURINGA_UM: Final[str] = "*"
_CURINGA_VARIOS: Final[str] = "#"

#: Quantas voltas do *event loop* sem trabalho pendente `drenar` exige antes de declarar quietude.
_CICLOS_ESTAVEIS: Final[int] = 8

#: Voltas de `sleep(0)` antes de `drenar` passar a ceder de verdade, para nao girar a CPU a toa.
_GIROS_ANTES_DE_CEDER: Final[int] = 64

#: Fatia de espera usada por `drenar` depois de esgotar os giros baratos.
_FATIA_DE_ESPERA_S: Final[float] = 0.001

#: Teto de espera, em tempo real, que `close` concede a um handler em voo antes de cancela-lo.
_TIMEOUT_CLOSE_S: Final[float] = 5.0


def casa_topico(padrao: str, routing_key: str) -> bool:
    """Diz se uma *binding key* de exchange ``topic`` casa com uma *routing key*.

    Implementa o algoritmo do AMQP 0-9-1 sem atalhos: a chave e dividida em segmentos separados por
    ponto, ``*`` casa **exatamente um** segmento e ``#`` casa **zero ou mais** segmentos. E uma
    funcao pura, testada isoladamente, e e ela que torna honesto o teste do binding ``#`` de
    ``q.audit.todos`` (R7.1): se ``#`` fosse tratado como "casa com tudo", o teste seria circular.

    Args:
        padrao: A *binding key* declarada, ex. ``"paciente.*"``, ``"sinais.coletados"`` ou ``"#"``.
        routing_key: A chave da mensagem publicada, ex. ``"paciente.admitido"``.

    Returns:
        ``True`` se a mensagem deve ser roteada para a fila ligada por esse padrao.

    Examples:
        >>> casa_topico("#", "qualquer.coisa.nova")
        True
        >>> casa_topico("paciente.*", "paciente.admitido")
        True
        >>> casa_topico("paciente.*", "paciente.leito.trocado")
        False
    """
    if padrao == routing_key:
        return True

    segmentos_padrao = padrao.split(_SEPARADOR)
    segmentos_chave = routing_key.split(_SEPARADOR)
    total = len(segmentos_chave)

    # Programacao dinamica classica: `anterior[j]` diz se os `i` primeiros segmentos do padrao
    # casam com os `j` primeiros segmentos da routing key.
    anterior: list[bool] = [False] * (total + 1)
    anterior[0] = True

    for segmento in segmentos_padrao:
        atual: list[bool] = [False] * (total + 1)
        if segmento == _CURINGA_VARIOS:
            atual[0] = anterior[0]
        for j in range(1, total + 1):
            if segmento == _CURINGA_VARIOS:
                atual[j] = atual[j - 1] or anterior[j]
            elif segmento == _CURINGA_UM:
                atual[j] = anterior[j - 1]
            else:
                atual[j] = anterior[j - 1] and segmentos_chave[j - 1] == segmento
        anterior = atual

    return anterior[total]


# --------------------------------------------------------------------------- #
# Estruturas internas                                                          #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, eq=False)
class _Entrega:
    """Uma mensagem em repouso dentro de uma fila em memoria.

    ``eq=False`` e deliberado: duas mensagens com o mesmo corpo continuam sendo mensagens
    **distintas**, e a remocao da fila de espera precisa acertar exatamente aquela que expirou.
    """

    body: bytes
    headers: dict[str, Any]
    message_id: str | None
    correlation_id: str | None
    reply_to: str | None
    persistent: bool = True
    redelivered: bool = False

    def clonar(self) -> _Entrega:
        """Devolve uma copia independente, usada quando um publish casa com varias filas."""
        return _Entrega(
            body=self.body,
            headers=dict(self.headers),
            message_id=self.message_id,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            persistent=self.persistent,
            redelivered=self.redelivered,
        )


@dataclass(slots=True, eq=False)
class _Publicacao:
    """Registro auditavel de uma publicacao, para as asserções dos testes."""

    destination: str
    routing_key: str
    entrega: _Entrega
    filas: tuple[str, ...]
    _envelope: Envelope | None = None
    _decodificado: bool = False

    @property
    def envelope(self) -> Envelope | None:
        """O Envelope do corpo, decodificado sob demanda; ``None`` se o corpo nao for Envelope."""
        if not self._decodificado:
            self._decodificado = True
            try:
                self._envelope = Envelope.from_bytes(self.entrega.body)
            except InvalidEnvelopeError:
                self._envelope = None
        return self._envelope


class _Fila:
    """Uma fila em memoria, com sua ``QueueSpec`` e, quando for de espera, seu TTL."""

    __slots__ = (
        "_disponivel",
        "assinaturas",
        "destino_ttl",
        "e_dlq",
        "itens",
        "nome",
        "spec",
        "ttl_s",
    )

    def __init__(
        self,
        spec: QueueSpec,
        *,
        ttl_s: float | None = None,
        destino_ttl: str | None = None,
        e_dlq: bool = False,
    ) -> None:
        self.nome: str = spec.name
        self.spec: QueueSpec = spec
        self.itens: deque[_Entrega] = deque()
        self.ttl_s: float | None = ttl_s
        self.destino_ttl: str | None = destino_ttl
        self.e_dlq: bool = e_dlq
        self.assinaturas: list[_Assinatura] = []
        self._disponivel: asyncio.Event = asyncio.Event()

    @property
    def e_de_espera(self) -> bool:
        """``True`` para as filas de espera com TTL da secao 4.6.2."""
        return self.ttl_s is not None

    def acrescentar(self, entrega: _Entrega) -> None:
        """Enfileira no fim, como qualquer publicacao normal."""
        self.itens.append(entrega)
        self._disponivel.set()

    def devolver(self, entrega: _Entrega) -> None:
        """Devolve a mensagem ao **inicio** da fila, como faz ``basic.nack(requeue=True)``."""
        entrega.redelivered = True
        self.itens.appendleft(entrega)
        self._disponivel.set()

    def remover(self, entrega: _Entrega) -> bool:
        """Remove uma mensagem especifica; ``False`` se ela ja tinha saido."""
        try:
            self.itens.remove(entrega)
        except ValueError:
            return False
        if not self.itens:
            self._disponivel.clear()
        return True

    async def obter(self) -> _Entrega:
        """Espera ate haver mensagem e retira a primeira.

        Varias assinaturas podem esperar na mesma fila: quem chega primeiro ao ``popleft`` leva a
        mensagem, e as demais voltam a esperar. E assim que o regime de *competing consumers* de
        R2.5 se realiza sem entrega dupla.
        """
        while True:
            if self.itens:
                entrega = self.itens.popleft()
                if not self.itens:
                    self._disponivel.clear()
                return entrega
            self._disponivel.clear()
            await self._disponivel.wait()


@dataclass(slots=True, eq=False)
class _Pendente:
    """Entrega ja despachada ao handler e ainda **nao** confirmada."""

    entrega: _Entrega
    fila: _Fila
    assinatura: _Assinatura


@dataclass(frozen=True, slots=True)
class MensagemMorta:
    """Uma mensagem encontrada em uma DLQ, ja interpretada para inspeção em teste.

    Cobre os dois caminhos ate a DLQ da secao 6.2.6: a publicacao explicita do ``Consumer`` em
    ``hospital.dlx``, cujo corpo e ``{"envelope": ..., "falha": ...}`` (secao 4.7.2), e a rede de
    seguranca do broker, em que o corpo chega como o Envelope cru, sem enriquecimento.

    Attributes:
        fila_origem: Fila de negocio de onde a mensagem foi descartada.
        envelope: O Envelope original preservado, ou ``None`` se o corpo era indecifravel.
        falha: O objeto de diagnostico irmao do Envelope; vazio no caminho da rede de seguranca.
        headers: Cabecalhos da mensagem, incluindo ``x-hmq-dlq-motivo`` e companhia.
        body: Corpo bruto, para quem precisar auditar byte a byte.
    """

    fila_origem: str
    envelope: Envelope | None
    falha: Mapping[str, Any]
    headers: Mapping[str, Any]
    body: bytes

    @property
    def message_id(self) -> str | None:
        """``message_id`` do Envelope preservado -- retentativa e a **mesma** mensagem (4.6.3)."""
        return self.envelope.message_id if self.envelope is not None else None

    @property
    def correlation_id(self) -> str | None:
        """``correlation_id`` do Envelope preservado, para agrupar os logs do fluxo (R5.3)."""
        return self.envelope.correlation_id if self.envelope is not None else None

    @property
    def type(self) -> str | None:
        """``type`` do Envelope preservado."""
        return self.envelope.type if self.envelope is not None else None

    @property
    def attempt(self) -> int:
        """Numero da tentativa em que a mensagem morreu -- ``4`` no esgotamento de R2.2."""
        return self.envelope.attempt if self.envelope is not None else 0

    @property
    def payload(self) -> Mapping[str, Any]:
        """Payload do Envelope preservado, byte a byte igual ao publicado (R2.3)."""
        return self.envelope.payload if self.envelope is not None else {}

    @property
    def motivo_falha(self) -> str:
        """Motivo da ultima falha, como R2.3 exige que a DLQ preserve.

        Returns:
            A mensagem da excecao (``falha.erro_mensagem``); na falta dela, ``falha.motivo`` e,
            por ultimo, o cabecalho ``x-hmq-dlq-motivo``. String vazia se nenhum estiver presente.
        """
        for chave in ("erro_mensagem", "motivo"):
            valor = self.falha.get(chave)
            if isinstance(valor, str) and valor:
                return valor
        cabecalho = self.headers.get(HEADER_DLQ_MOTIVO)
        return cabecalho if isinstance(cabecalho, str) else ""

    @property
    def erro_tipo(self) -> str:
        """Nome da classe da excecao que matou a mensagem, para triagem da DLQ (6.2.6)."""
        valor = self.falha.get("erro_tipo")
        if isinstance(valor, str) and valor:
            return valor
        cabecalho = self.headers.get(HEADER_DLQ_ERRO_TIPO)
        return cabecalho if isinstance(cabecalho, str) else ""

    @property
    def servico(self) -> str:
        """Servico consumidor que desistiu da mensagem."""
        valor = self.falha.get("servico")
        if isinstance(valor, str) and valor:
            return valor
        cabecalho = self.headers.get(HEADER_DLQ_SERVICO)
        return cabecalho if isinstance(cabecalho, str) else ""


class _Assinatura:
    """Assinatura ativa de consumo -- a :class:`~hospitalmq.transport.base.Subscription` daqui.

    Mantem o credito de ``prefetch`` como um semaforo: cada entrega despachada consome um credito,
    e o credito so volta quando a mensagem e confirmada ou rejeitada. E isso que reproduz a
    limitacao de mensagens nao confirmadas em voo de R2.6 sem confirmar nada automaticamente.
    """

    __slots__ = ("_cancelada", "_creditos", "_fila", "_handler", "_tarefa", "_transporte")

    def __init__(
        self,
        transporte: MemoryTransport,
        fila: _Fila,
        handler: MessageHandler,
        prefetch: int,
    ) -> None:
        self._transporte: MemoryTransport = transporte
        self._fila: _Fila = fila
        self._handler: MessageHandler = handler
        # prefetch <= 0 significa "sem limite", como o `basic.qos(0)` do AMQP.
        limite = prefetch if prefetch > 0 else 2**31
        self._creditos: asyncio.Semaphore = asyncio.Semaphore(limite)
        self._cancelada: bool = False
        self._tarefa: asyncio.Task[None] = asyncio.create_task(
            self._laco(), name=f"hospitalmq-consume:{fila.nome}"
        )

    async def cancel(self) -> None:
        """Cancela a assinatura, parando a entrega de **novas** mensagens.

        Idempotente. Nao aguarda os handlers em voo -- quem faz isso e
        :meth:`MemoryTransport.close` -- e nao descarta as entregas ja despachadas, que continuam
        aguardando ``ack`` ou ``nack``.
        """
        if self._cancelada:
            return
        self._cancelada = True
        self._tarefa.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._tarefa
        if self in self._fila.assinaturas:
            self._fila.assinaturas.remove(self)
        self._transporte._esquecer_assinatura(self)

    def liberar_credito(self) -> None:
        """Devolve um credito de ``prefetch``, chamado em ``ack`` e em ``nack``."""
        self._creditos.release()

    async def _laco(self) -> None:
        """Drena a fila respeitando o credito de ``prefetch``, sem nunca confirmar sozinho."""
        while not self._cancelada:
            await self._creditos.acquire()
            try:
                entrega = await self._fila.obter()
            except asyncio.CancelledError:
                self._creditos.release()
                raise
            self._despachar(entrega)

    def _despachar(self, entrega: _Entrega) -> None:
        """Monta a :class:`InboundMessage` e dispara o handler em uma tarefa propria."""
        tag = self._transporte._proxima_tag()
        mensagem = InboundMessage(
            body=entrega.body,
            headers=dict(entrega.headers),
            queue=self._fila.nome,
            message_id=entrega.message_id,
            correlation_id=entrega.correlation_id,
            reply_to=entrega.reply_to,
            redelivered=entrega.redelivered,
            delivery_tag=tag,
        )
        self._transporte._registrar_pendente(
            tag, _Pendente(entrega=entrega, fila=self._fila, assinatura=self)
        )
        self._transporte._acompanhar_handler(self._executar(mensagem))

    async def _executar(self, mensagem: InboundMessage) -> None:
        """Invoca o handler e absorve a excecao que ele nao deveria ter deixado escapar.

        O contrato diz que o handler **nao deve levantar**: quem trata erro e o *pipeline* do
        ``Consumer``. Se ainda assim escapar, a excecao e registrada em
        :attr:`MemoryTransport.falhas_de_handler` e a entrega permanece **nao confirmada**,
        exatamente como o RabbitMQ a manteria -- o que faz ``drenar()`` estourar com a excecao
        original em maos, em vez de esconder o defeito.
        """
        try:
            await self._handler(mensagem)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # o dublê registra, nunca engole em silencio
            self._transporte.falhas_de_handler.append(exc)


# --------------------------------------------------------------------------- #
# O transporte                                                                 #
# --------------------------------------------------------------------------- #


class MemoryTransport:
    """Transporte em memoria: mesma interface do ``AmqpTransport``, zero rede, zero broker.

    Implementa estruturalmente o ``Protocol`` :class:`~hospitalmq.transport.base.Transport` -- os
    oito metodos, com as assinaturas normativas da secao 4.3.1 -- e mais nada na parte contratual.
    As extensoes de teste (:meth:`drenar`, :meth:`eventos`, :meth:`na_dlq`, :meth:`profundidade`,
    :meth:`entregar`) vivem fora do ``Protocol`` e o nucleo jamais as chama.

    Args:
        clock: Fonte de tempo. O padrao e :class:`~hospitalmq.clock.ManualClock`, para que o TTL
            das filas de espera seja cumprido instantaneamente e fique registrado em
            ``clock.sleeps``. Passe **o mesmo** relogio usado pelo teste quando quiser afirmar
            ``clock.sleeps == [1.0, 2.0, 4.0]`` (R2.2).
        indisponivel: Quando ``True``, toda operacao de broker levanta
            :class:`~hospitalmq.errors.TransportError`, como o ``AmqpTransport`` levantaria com o
            RabbitMQ fora do ar. E o gatilho do teste de R1.5. Tambem pode ser ligado depois da
            montagem da topologia, atribuindo ``transporte.indisponivel = True``.

    Attributes:
        indisponivel: Simula broker fora do ar; leia acima.
        acks: ``delivery_tag`` de cada confirmacao, **na ordem exata** em que ocorreram. E o que
            permite afirmar que o ACK veio depois do retorno do handler (R2.1).
        nacks: Pares ``(delivery_tag, requeue)`` de cada rejeicao.
        filas_de_espera_usadas: Nome de cada fila de espera que recebeu publicacao, em ordem.
            Prova que a espera de R2.2 passou pelas filas de 4.6.2 e nao por um ``sleep`` solto.
        descartadas: Pares ``(fila, corpo)`` de mensagens que o broker teria perdido -- descarte
            sem DLQ configurada ou sem *binding* de descarte. Deve ficar vazio no caminho feliz.
        falhas_de_handler: Excecoes que escaparam de um handler de consumo.
    """

    def __init__(self, *, clock: Clock | None = None, indisponivel: bool = False) -> None:
        """Cria o transporte. Nada e aberto: nao ha conexao a estabelecer."""
        self._clock: Clock = clock if clock is not None else ManualClock()
        self.indisponivel: bool = indisponivel

        self._exchanges: dict[str, ExchangeSpec] = {}
        self._filas: dict[str, _Fila] = {}
        # (exchange, binding key, fila) -- a lista de casadores de padrao da secao 4.3.3.
        self._bindings: list[tuple[str, str, str]] = []

        self._assinaturas: list[_Assinatura] = []
        self._pendentes: dict[object, _Pendente] = {}
        self._em_voo: set[asyncio.Task[None]] = set()
        self._temporizadores: set[asyncio.Task[None]] = set()
        self._registro: list[_Publicacao] = []

        self._proximo_tag: int = 0
        self._conectado: bool = False
        self._fechado: bool = False

        self.acks: list[object] = []
        self.nacks: list[tuple[object, bool]] = []
        self.filas_de_espera_usadas: list[str] = []
        self.descartadas: list[tuple[str, bytes]] = []
        self.falhas_de_handler: list[BaseException] = []

    # -- ciclo de vida ----------------------------------------------------- #

    async def connect(self) -> None:
        """Estabelece a "conexao" -- que aqui nao existe: nao ha nada a abrir.

        Mantido no contrato porque o ``AmqpTransport`` precisa dele. Chamar duas vezes e permitido
        e nao tem efeito. Reabre um transporte previamente fechado.

        Raises:
            TransportError: Se o transporte foi construido com ``indisponivel=True``.
        """
        self._exigir_disponivel("connect")
        self._conectado = True
        self._fechado = False

    async def declare_topology(self, spec: TopologySpec) -> None:
        """Materializa a topologia inteira, de forma idempotente (secao 6.11.1).

        A ordem e a normativa e nao e negociavel: **exchanges**, depois **DLQs e filas de espera**,
        depois as **filas de negocio** e por fim os **bindings**. Os exchanges vem primeiro porque
        o ``x-dead-letter-exchange`` de uma fila so e resolvido quando a mensagem morre -- se
        ``hospital.dlx`` nao existisse naquele instante, a mensagem descartada sumiria sem erro.

        As DLQs e as filas de espera **nao** aparecem em ``spec.queues``: sao derivadas aqui, de
        ``QueueSpec.dlq_name`` e ``QueueSpec.retry_queues()``, para que nome e argumento nunca
        divirjam entre o driver e o resto do sistema. Cada DLQ derivada e ligada a
        ``spec.dead_letter_exchange`` pela chave ``<fila>.dlq``, que e exatamente a chave que o
        ``Consumer`` usa ao publicar na DLQ (secao 4.7.1).

        Chamar duas vezes com a mesma ``TopologySpec`` e um no-op, como no broker real.

        Args:
            spec: A topologia declarativa completa.

        Raises:
            TransportError: Se o transporte estiver marcado como indisponivel.
            ConfigError: Objeto ja existente com argumentos divergentes -- falha fatal de boot,
                nunca redeclaracao destrutiva -- ou binding para exchange nao declarado.
        """
        self._exigir_disponivel("declare_topology")

        # 1. exchanges (inclusive o de descarte, mesmo que a spec nao o liste)
        for exchange in spec.exchanges:
            self._declarar_exchange(exchange)
        for fila in spec.queues:
            if fila.with_dlq and fila.dead_letter_exchange not in self._exchanges:
                self._declarar_exchange(
                    ExchangeSpec(name=fila.dead_letter_exchange, type="topic", durable=True)
                )

        # 2. DLQs e filas de espera, derivadas de cada QueueSpec
        for fila in spec.queues:
            if fila.with_dlq:
                self._declarar_fila(self._spec_de_dlq(fila), e_dlq=True)
                self._ligar(fila.dead_letter_exchange, fila.dlq_name, fila.dlq_name)
            for nome, atraso_ms in fila.retry_queues():
                self._declarar_fila(
                    self._spec_de_espera(fila, nome),
                    ttl_s=atraso_ms / 1000.0,
                    destino_ttl=fila.name,
                )

        # 3. filas de negocio
        for fila in spec.queues:
            self._declarar_fila(fila)

        # 4. bindings
        for fila in spec.queues:
            for binding in fila.bindings:
                if binding.exchange not in self._exchanges:
                    raise ConfigError(
                        f"binding para exchange nao declarado: {binding.exchange!r}",
                        exchange=binding.exchange,
                        fila=fila.name,
                    )
                self._ligar(binding.exchange, binding.key, fila.name)

    async def close(self) -> None:
        """Encerra o transporte graciosamente. Idempotente e nunca levanta.

        Cancela as assinaturas, aguarda os handlers em voo terminarem (com teto de 5 s de tempo
        real, para que um handler que nunca retorna nao trave a suite), cancela os temporizadores
        das filas de espera e **devolve a fila** todas as mensagens ainda nao confirmadas -- nunca
        as descarta.
        """
        if self._fechado:
            return
        self._fechado = True

        for assinatura in list(self._assinaturas):
            await assinatura.cancel()

        if self._em_voo:
            await asyncio.wait(set(self._em_voo), timeout=_TIMEOUT_CLOSE_S)
            for tarefa in list(self._em_voo):
                tarefa.cancel()
            if self._em_voo:
                await asyncio.gather(*list(self._em_voo), return_exceptions=True)

        for tarefa in list(self._temporizadores):
            tarefa.cancel()
        if self._temporizadores:
            await asyncio.gather(*list(self._temporizadores), return_exceptions=True)

        for pendente in list(self._pendentes.values()):
            pendente.fila.devolver(pendente.entrega)
        self._pendentes.clear()
        self._conectado = False

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

        Roteia como o broker rotearia: ``destination`` vazio designa o *exchange padrao* e entrega
        pelo nome exato da fila; um exchange ``topic`` usa :func:`casa_topico`; um ``direct`` exige
        igualdade da chave; um ``fanout`` entrega a todas as filas ligadas. Mensagem que nao casa
        com fila nenhuma vira :class:`~hospitalmq.errors.TransportError`, como o ``mandatory=True``
        do ``AmqpTransport`` faria -- nao ha descarte silencioso.

        Quando a fila de destino e uma **fila de espera** da secao 4.6.2, a mensagem fica visivel
        nela (``profundidade`` a mostra) e o TTL declarado e cumprido por ``clock.sleep``; ao
        expirar, a mensagem e devolvida a fila de origem, exatamente como o
        ``x-dead-letter-routing-key`` faria. Nesse caso ``delay_ms`` e ignorado, porque o atraso ja
        e propriedade da fila e conta-lo duas vezes falsearia ``clock.sleeps``.

        Args:
            destination: Exchange de destino; ``""`` e o exchange padrao.
            routing_key: Chave de roteamento; no exchange padrao, o nome exato da fila.
            body: Corpo ja serializado -- bytes opacos (invariante 2 da secao 4.3.2).
            headers: Cabecalhos a espelhar, incluindo ``x-hmq-attempt`` e ``x-hmq-version``.
            message_id: Propriedade nativa ``message_id``.
            correlation_id: Propriedade nativa ``correlation_id``.
            reply_to: Endereco de retorno, preenchido apenas pelo ``RpcClient``.
            persistent: Registrado, mas sem efeito: durabilidade nao e simulada (secao 10.2.4).
            delay_ms: Atraso minimo antes de a mensagem ficar visivel, cumprido pelo ``Clock``.

        Raises:
            TransportError: Broker marcado como indisponivel, transporte fechado, exchange nao
                declarado, mensagem nao roteavel ou fila cheia com ``overflow=reject-publish``.
        """
        self._exigir_disponivel("publish")
        molde = _Entrega(
            body=bytes(body),
            headers=dict(headers),
            message_id=message_id,
            correlation_id=correlation_id,
            reply_to=reply_to,
            persistent=persistent,
        )
        destinos = self._rotear(destination, routing_key)
        self._registro.append(
            _Publicacao(
                destination=destination,
                routing_key=routing_key,
                entrega=molde,
                filas=tuple(fila.nome for fila in destinos),
            )
        )
        for fila in destinos:
            if fila.e_de_espera:
                self.filas_de_espera_usadas.append(fila.nome)
            self._entregar_em(fila, molde.clonar(), delay_ms=delay_ms)

    async def consume(
        self,
        *,
        queue: str,
        handler: MessageHandler,
        prefetch: int = PREFETCH_PADRAO,
    ) -> Subscription:
        """Registra um consumidor da fila (R9.1 "consumir").

        Entrega ao ``handler`` mantendo no maximo ``prefetch`` mensagens **nao confirmadas** por
        assinatura (R2.6) e **nunca** confirma automaticamente (R2.1). Varias assinaturas na mesma
        fila competem pelas mensagens, e cada mensagem sai da fila uma unica vez (R2.5).

        Args:
            queue: Nome da fila, ja declarada por :meth:`declare_topology` (ou por
                :meth:`declarar_fila`, nos testes que montam a topologia a mao).
            handler: Corrotina invocada por mensagem.
            prefetch: Limite de mensagens nao confirmadas em voo; ``<= 0`` significa sem limite.

        Returns:
            A assinatura cancelavel -- uma :class:`~hospitalmq.transport.base.Subscription`.

        Raises:
            TransportError: Broker indisponivel, transporte fechado ou fila nao declarada.
        """
        self._exigir_disponivel("consume")
        fila = self._filas.get(queue)
        if fila is None:
            raise TransportError(f"nao ha como consumir fila nao declarada: {queue!r}", fila=queue)
        assinatura = _Assinatura(self, fila, handler, prefetch)
        fila.assinaturas.append(assinatura)
        self._assinaturas.append(assinatura)
        return assinatura

    async def ack(self, message: InboundMessage) -> None:
        """Confirma definitivamente a mensagem (R9.1 "confirmar").

        Descarta a entrega e devolve o credito de ``prefetch`` a assinatura que a recebeu.
        Confirmar entrega desconhecida ou ja confirmada **falha visivelmente**: silenciar isso
        esconderia perda de mensagem.

        Args:
            message: A mensagem recebida, inteira.

        Raises:
            TransportError: Entrega desconhecida ou ja confirmada.
        """
        pendente = self._pendentes.pop(message.delivery_tag, None)
        if pendente is None:
            raise TransportError(
                "ack de entrega desconhecida ou ja confirmada",
                fila=message.queue,
                message_id=message.message_id,
            )
        self.acks.append(message.delivery_tag)
        pendente.assinatura.liberar_credito()

    async def nack(self, message: InboundMessage, *, requeue: bool = False) -> None:
        """Rejeita a mensagem (R9.1 "rejeitar").

        Com ``requeue=False``, entrega a mensagem ao mecanismo de descarte da fila: publica-a no
        ``dead_letter_exchange`` da ``QueueSpec`` com *routing key* ``<fila>.dlq``, que e como o
        ``x-dead-letter-exchange`` do RabbitMQ se comporta (R2.3). Com ``requeue=True``, devolve-a
        ao **inicio** da fila marcada como ``redelivered`` -- uso restrito ao encerramento
        gracioso, nunca como retentativa (secao 4.6.4).

        Args:
            message: A mensagem recebida, inteira.
            requeue: ``True`` apenas no encerramento gracioso.

        Raises:
            TransportError: Entrega desconhecida ou ja confirmada.
        """
        pendente = self._pendentes.pop(message.delivery_tag, None)
        if pendente is None:
            raise TransportError(
                "nack de entrega desconhecida ou ja confirmada",
                fila=message.queue,
                message_id=message.message_id,
            )
        self.nacks.append((message.delivery_tag, requeue))
        if requeue:
            pendente.fila.devolver(pendente.entrega)
        else:
            self._descartar(pendente.fila, pendente.entrega)
        pendente.assinatura.liberar_credito()

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
        ``RpcClient`` casa a resposta com a *future* pendente. A resposta nao e persistente:
        resposta que sobrevivesse a um reinicio chegaria depois de o chamador ja ter desistido.

        Args:
            message: A requisicao original, de onde sai o ``reply_to``.
            body: Corpo da resposta, ja serializado.
            correlation_id: O mesmo valor recebido na requisicao.

        Raises:
            TransportError: ``reply_to`` ausente -- erro de programacao, falha alto -- ou fila de
                retorno inexistente, o que significa que o chamador desistiu.
        """
        self._exigir_disponivel("reply")
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

    # -- extensoes exclusivas de teste (fora do Protocol) ------------------ #

    @property
    def clock(self) -> Clock:
        """O relogio injetado -- o mesmo que cumpre o TTL das filas de espera."""
        return self._clock

    @property
    def publicadas(self) -> list[tuple[str, str, Envelope]]:
        """Publicacoes cujo corpo era um Envelope, como ``(destination, routing_key, envelope)``.

        Returns:
            Lista na ordem exata das publicacoes. Corpos que nao sao Envelope -- respostas de RPC
            que nao usem o formato, por exemplo -- ficam de fora; use :attr:`registro_bruto` para
            ver tudo.
        """
        return [
            (pub.destination, pub.routing_key, pub.envelope)
            for pub in self._registro
            if pub.envelope is not None
        ]

    @property
    def registro_bruto(self) -> list[tuple[str, str, bytes, tuple[str, ...]]]:
        """Toda publicacao, decodificavel ou nao: ``(destination, routing_key, body, filas)``."""
        return [
            (pub.destination, pub.routing_key, pub.entrega.body, pub.filas)
            for pub in self._registro
        ]

    def eventos(self, tipo: str | None = None) -> list[Envelope]:
        """Envelopes publicados, opcionalmente filtrados por ``type``.

        Args:
            tipo: Valor de ``Envelope.type`` a filtrar, ex. ``"alerta.gerado"``. ``None`` devolve
                todos.

        Returns:
            Os Envelopes na ordem de publicacao.
        """
        return [
            pub.envelope
            for pub in self._registro
            if pub.envelope is not None and (tipo is None or pub.envelope.type == tipo)
        ]

    def na_dlq(self, fila: str) -> list[MensagemMorta]:
        """Mensagens que sofreram *dead-lettering* a partir de ``fila``.

        Le a fila ``<fila>.dlq`` **sem consumi-la**, e interpreta os dois formatos possiveis: o
        corpo enriquecido ``{"envelope": ..., "falha": ...}`` da publicacao explicita (secao 4.7.2)
        e o Envelope cru da rede de seguranca do broker (secao 6.2.6).

        Args:
            fila: Nome da fila de **negocio**, nao o da DLQ.

        Returns:
            As mensagens mortas, na ordem em que chegaram. Lista vazia se a DLQ nao existe ou esta
            vazia -- o que torna ``assert mem.na_dlq(...) == []`` uma asserção segura.
        """
        alvo = self._filas.get(f"{fila}.dlq")
        if alvo is None:
            return []
        return [self._interpretar_morta(fila, entrega) for entrega in alvo.itens]

    def envelopes_na_dlq(self, fila: str) -> list[Envelope]:
        """Somente os Envelopes preservados na DLQ de ``fila``, sem o diagnostico irmao.

        Args:
            fila: Nome da fila de negocio.

        Returns:
            Os Envelopes originais; corpos indecifraveis sao omitidos.
        """
        return [morta.envelope for morta in self.na_dlq(fila) if morta.envelope is not None]

    def profundidade(self, fila: str) -> int:
        """Quantas mensagens estao paradas na fila.

        Serve tanto para a fila de negocio quanto para as filas de espera de 4.6.2 e para as DLQs.

        Args:
            fila: Nome exato da fila.

        Returns:
            Numero de mensagens em repouso -- entregas ainda nao confirmadas **nao** contam,
            porque ja sairam da fila.

        Raises:
            ConfigError: Se a fila nao tiver sido declarada.
        """
        alvo = self._filas.get(fila)
        if alvo is None:
            raise ConfigError(f"fila nao declarada: {fila!r}", fila=fila)
        return len(alvo.itens)

    def nao_confirmadas(self) -> int:
        """Quantas entregas foram despachadas e ainda aguardam ``ack`` ou ``nack``.

        Returns:
            O tamanho da janela de mensagens em voo -- o teto e o ``prefetch`` somado das
            assinaturas ativas (R2.6).
        """
        return len(self._pendentes)

    def declarar_fila(self, fila: str, *, dlq: str | None = None) -> None:
        """Declara uma fila avulsa, sem montar a topologia inteira.

        Atalho para os testes de unidade que so precisam de uma fila e da sua DLQ.

        Args:
            fila: Nome da fila de negocio.
            dlq: Nome da DLQ associada. ``None`` significa **sem** DLQ -- o
                ``nack(requeue=False)`` passa a descartar a mensagem, registrando-a em
                :attr:`descartadas`.
        """
        spec = QueueSpec(name=fila, with_dlq=dlq is not None, dead_letter_exchange=EXCHANGE_DLX)
        if dlq is not None:
            self._declarar_exchange(ExchangeSpec(name=EXCHANGE_DLX, type="topic", durable=True))
            self._declarar_fila(QueueSpec(name=dlq, with_dlq=False), e_dlq=True)
            self._ligar(EXCHANGE_DLX, dlq, dlq)
            # A rota de descarte usa `<fila>.dlq`; um nome diferente ganha binding proprio.
            if dlq != spec.dlq_name:
                self._ligar(EXCHANGE_DLX, spec.dlq_name, dlq)
        self._declarar_fila(spec)

    def declarar_fila_de_espera(self, fila: str, *, destino: str, ttl_ms: int) -> None:
        """Equivalente em memoria de ``x-message-ttl`` + ``x-dead-letter-routing-key`` (4.6.2).

        O TTL nao e esperado de verdade: e cumprido por ``clock.sleep(ttl_ms / 1000)``, de modo que
        a politica de 4.6.2 fique observavel em ``clock.sleeps``. A fila nao tem consumidor -- ela
        e um temporizador, nao um destino de trabalho.

        Args:
            fila: Nome da fila de espera, ex. ``"q.alert.alerta-gerado.retry.1s"``.
            destino: Fila de origem para onde a mensagem volta quando o TTL expira.
            ttl_ms: Tempo de prateleira, em milissegundos.
        """
        self._declarar_fila(
            QueueSpec(name=fila, durable=True, with_dlq=False),
            ttl_s=ttl_ms / 1000.0,
            destino_ttl=destino,
        )

    def bind(self, exchange: str, padrao: str, fila: str) -> None:
        """Liga uma fila a um exchange por uma *binding key*.

        Args:
            exchange: Nome do exchange; e declarado como ``topic`` se ainda nao existir.
            padrao: *Binding key*, com ``*`` e ``#`` quando o exchange for ``topic``.
            fila: Nome da fila de destino, que precisa ja existir.

        Raises:
            ConfigError: Se a fila nao tiver sido declarada.
        """
        if fila not in self._filas:
            raise ConfigError(f"fila nao declarada: {fila!r}", fila=fila)
        if exchange and exchange not in self._exchanges:
            self._declarar_exchange(ExchangeSpec(name=exchange, type="topic", durable=True))
        self._ligar(exchange, padrao, fila)

    async def entregar(
        self,
        fila: str,
        envelope: Envelope | bytes,
        *,
        headers: Mapping[str, Any] | None = None,
        reply_to: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
    ) -> None:
        """Injeta uma mensagem diretamente na fila, sem passar por exchange.

        Atalho de teste para "chegou uma mensagem nesta fila", usado por exemplo para entregar o
        **mesmo** Envelope duas vezes e verificar a supressao de duplicata de R2.4. A fila e
        declarada com DLQ automaticamente se ainda nao existir.

        Args:
            fila: Fila de destino.
            envelope: O :class:`~hospitalmq.envelope.Envelope` a entregar, ou o corpo ja em bytes.
            headers: Cabecalhos da mensagem. Se omitido e ``envelope`` for um Envelope, usa
                ``envelope.to_headers()``.
            reply_to: Endereco de retorno, para simular uma requisicao RPC.
            correlation_id: Sobrescreve o ``correlation_id`` nativo.
            message_id: Sobrescreve o ``message_id`` nativo.
        """
        if fila not in self._filas:
            self.declarar_fila(fila, dlq=f"{fila}.dlq")
        corpo: bytes
        cabecalhos: dict[str, Any]
        identificador: str | None
        correlacao: str | None
        if isinstance(envelope, Envelope):
            corpo = envelope.to_bytes()
            cabecalhos = dict(headers) if headers is not None else envelope.to_headers()
            identificador = message_id if message_id is not None else envelope.message_id
            correlacao = correlation_id if correlation_id is not None else envelope.correlation_id
        else:
            corpo = bytes(envelope)
            cabecalhos = dict(headers) if headers is not None else {}
            identificador = message_id
            correlacao = correlation_id
        self._entregar_em(
            self._filas[fila],
            _Entrega(
                body=corpo,
                headers=cabecalhos,
                message_id=identificador,
                correlation_id=correlacao,
                reply_to=reply_to,
            ),
            delay_ms=None,
        )
        await asyncio.sleep(0)

    async def drenar(self, *, limite: float = 1.0) -> None:
        """Aguarda todas as filas esvaziarem e todos os handlers terminarem.

        Substitui o ``sleep(0.5)`` que testes de mensageria costumam usar para "esperar o
        consumidor pegar a mensagem" -- a espera aqui e **deterministica**, porque o transporte
        roda no mesmo *event loop* e sabe exatamente o que ainda esta em voo. O sossego e declarado
        quando, por varias voltas seguidas do laco, nao ha:

        * mensagem parada em fila **com assinatura ativa** (filas sem consumidor ficam paradas de
          proposito, como no broker, e por isso nao contam);
        * entrega despachada e ainda nao confirmada;
        * handler em execucao;
        * temporizador de fila de espera ou de ``delay_ms`` pendente.

        Args:
            limite: Teto em segundos de **tempo real** (nunca do relogio injetado) para a espera.
                Com :class:`~hospitalmq.clock.ManualClock` o padrao de 1 s sobra; com
                :class:`~hospitalmq.clock.RealClock` e retentativas de 1s/2s/4s, aumente-o.

        Raises:
            TimeoutError: Se o sistema nao sossegar dentro de ``limite``. A mensagem descreve o que
                ficou pendente e anexa a primeira excecao de handler registrada, quando houver --
                e a diferenca entre um teste que trava sem explicacao e um que aponta o defeito.
        """
        laco = asyncio.get_running_loop()
        prazo = laco.time() + limite
        giros = 0
        while True:
            if not self._ocupado():
                estavel = True
                for _ in range(_CICLOS_ESTAVEIS):
                    await asyncio.sleep(0)
                    if self._ocupado():
                        estavel = False
                        break
                if estavel:
                    return
            if laco.time() > prazo:
                raise TimeoutError(self._diagnostico_de_drenagem(limite))
            giros += 1
            if giros <= _GIROS_ANTES_DE_CEDER:
                await asyncio.sleep(0)
            else:
                await asyncio.sleep(_FATIA_DE_ESPERA_S)

    # -- interno: topologia ------------------------------------------------ #

    def _declarar_exchange(self, spec: ExchangeSpec) -> None:
        """Declara um exchange de forma idempotente; divergencia e falha fatal de boot."""
        existente = self._exchanges.get(spec.name)
        if existente is None:
            self._exchanges[spec.name] = spec
            return
        if existente != spec:
            raise ConfigError(
                f"exchange {spec.name!r} ja declarado com argumentos divergentes: "
                f"{existente!r} != {spec!r}",
                exchange=spec.name,
            )

    def _declarar_fila(
        self,
        spec: QueueSpec,
        *,
        ttl_s: float | None = None,
        destino_ttl: str | None = None,
        e_dlq: bool = False,
    ) -> None:
        """Declara uma fila de forma idempotente; divergencia de argumentos e ``ConfigError``.

        A comparacao considera apenas o que o broker guardaria como argumento da fila -- e nao,
        por exemplo, ``prefetch``, que no AMQP e QoS de canal, nem ``bindings``, que sao objetos
        separados.
        """
        existente = self._filas.get(spec.name)
        if existente is None:
            self._filas[spec.name] = _Fila(spec, ttl_s=ttl_s, destino_ttl=destino_ttl, e_dlq=e_dlq)
            return
        novo = self._argumentos_de_fila(spec, ttl_s, destino_ttl)
        antigo = self._argumentos_de_fila(existente.spec, existente.ttl_s, existente.destino_ttl)
        if novo != antigo:
            raise ConfigError(
                f"fila {spec.name!r} ja declarada com argumentos divergentes: {antigo} != {novo}",
                fila=spec.name,
            )

    @staticmethod
    def _argumentos_de_fila(
        spec: QueueSpec, ttl_s: float | None, destino_ttl: str | None
    ) -> tuple[object, ...]:
        """Projeta a ``QueueSpec`` nos argumentos que o broker realmente compara na redeclaracao."""
        return (
            spec.durable,
            spec.exclusive,
            spec.auto_delete,
            spec.with_dlq,
            spec.retry_delays_ms,
            spec.max_length,
            spec.overflow,
            spec.dead_letter_exchange if spec.with_dlq else None,
            ttl_s,
            destino_ttl,
        )

    @staticmethod
    def _spec_de_dlq(origem: QueueSpec) -> QueueSpec:
        """Deriva a ``QueueSpec`` da DLQ de uma fila de negocio -- nome sempre de ``dlq_name``."""
        return QueueSpec(name=origem.dlq_name, durable=origem.durable, with_dlq=False)

    @staticmethod
    def _spec_de_espera(origem: QueueSpec, nome: str) -> QueueSpec:
        """Deriva a ``QueueSpec`` de uma fila de espera. Sem DLQ propria e sem ``x-expires``."""
        return QueueSpec(name=nome, durable=origem.durable, with_dlq=False)

    def _ligar(self, exchange: str, padrao: str, fila: str) -> None:
        """Registra um casador de padrao, sem duplicar bindings identicos."""
        chave = (exchange, padrao, fila)
        if chave not in self._bindings:
            self._bindings.append(chave)

    # -- interno: roteamento e entrega ------------------------------------- #

    def _rotear(self, exchange: str, routing_key: str) -> list[_Fila]:
        """Resolve as filas de destino de uma publicacao.

        Raises:
            TransportError: Exchange nao declarado ou mensagem nao roteavel. Nao rotear em silencio
                e deliberado: e o equivalente do ``mandatory=True`` do ``AmqpTransport``, sem o
                qual uma chave errada viraria perda de mensagem invisivel (secao 4.7.1).
        """
        if exchange == EXCHANGE_PADRAO:
            fila = self._filas.get(routing_key)
            if fila is None:
                raise TransportError(
                    f"mensagem nao roteavel: fila {routing_key!r} nao declarada",
                    exchange=exchange,
                    routing_key=routing_key,
                )
            return [fila]

        spec = self._exchanges.get(exchange)
        if spec is None:
            raise TransportError(
                f"exchange nao declarado: {exchange!r}",
                exchange=exchange,
                routing_key=routing_key,
            )

        destinos: list[_Fila] = []
        for nome_exchange, padrao, nome_fila in self._bindings:
            if nome_exchange != exchange:
                continue
            if not self._casa(spec.type, padrao, routing_key):
                continue
            fila = self._filas.get(nome_fila)
            if fila is not None and fila not in destinos:
                destinos.append(fila)
        if not destinos:
            raise TransportError(
                f"mensagem nao roteavel: nenhuma fila ligada a {exchange!r} casa com "
                f"{routing_key!r}",
                exchange=exchange,
                routing_key=routing_key,
            )
        return destinos

    @staticmethod
    def _casa(tipo: str, padrao: str, routing_key: str) -> bool:
        """Aplica a regra de casamento do tipo de exchange."""
        if tipo == "topic":
            return casa_topico(padrao, routing_key)
        if tipo == "fanout":
            return True
        # `direct` -- e `headers`, que sem argumentos de cabecalho degenera em igualdade exata.
        return padrao == routing_key

    def _entregar_em(self, fila: _Fila, entrega: _Entrega, *, delay_ms: int | None) -> None:
        """Coloca a mensagem na fila, respeitando ``delay_ms`` e o TTL da fila de espera."""
        if fila.e_de_espera:
            # O atraso ja e propriedade da fila; `delay_ms` seria contagem dupla.
            self._enfileirar(fila, entrega)
            self._agendar(self._expirar_ttl(fila, entrega, fila.ttl_s or 0.0))
            return
        if delay_ms is not None and delay_ms > 0:
            self._agendar(self._entregar_apos(fila, entrega, delay_ms / 1000.0))
            return
        self._enfileirar(fila, entrega)

    def _enfileirar(self, fila: _Fila, entrega: _Entrega) -> None:
        """Enfileira aplicando ``x-max-length`` e a politica de *overflow* da secao 6.9.3.

        Raises:
            TransportError: Fila cheia com ``overflow="reject-publish"`` -- o *nack* ao produtor
                que da o *back-pressure* honesto de R1.5, em vez de descartar em silencio.
        """
        limite = fila.spec.max_length
        if limite is not None and len(fila.itens) >= limite:
            if fila.spec.overflow == "reject-publish":
                raise TransportError(
                    f"fila {fila.nome!r} atingiu x-max-length ({limite}); publicacao recusada",
                    fila=fila.nome,
                    max_length=limite,
                )
            # `drop-head`: a mais antiga sai e sofre dead-lettering, como no RabbitMQ.
            self._descartar(fila, fila.itens.popleft())
        fila.acrescentar(entrega)

    def _descartar(self, fila: _Fila, entrega: _Entrega) -> None:
        """Faz o *dead-lettering* de uma mensagem, como o ``x-dead-letter-exchange`` faria."""
        spec = fila.spec
        if not spec.with_dlq:
            self.descartadas.append((fila.nome, entrega.body))
            return
        try:
            destinos = self._rotear(spec.dead_letter_exchange, spec.dlq_name)
        except TransportError:
            self.descartadas.append((fila.nome, entrega.body))
            return
        for destino in destinos:
            try:
                self._enfileirar(destino, entrega.clonar())
            except TransportError:
                self.descartadas.append((destino.nome, entrega.body))

    async def _expirar_ttl(self, fila: _Fila, entrega: _Entrega, segundos: float) -> None:
        """Cumpre o tempo de prateleira da fila de espera e devolve a mensagem a origem.

        E aqui que o ``x-message-ttl`` + ``x-dead-letter-routing-key`` de 4.6.2 se realiza em
        memoria: o relogio injetado conta o tempo, o que deixa a politica de R2.2 registrada em
        ``clock.sleeps``, e ao expirar a mensagem volta **exclusivamente** a fila de origem, pelo
        exchange padrao -- nunca reentrando nos demais bindings (secao 6.2.4).
        """
        await self._clock.sleep(segundos)
        if not fila.remover(entrega):
            return
        if fila.destino_ttl is None:
            return
        destino = self._filas.get(fila.destino_ttl)
        if destino is None:
            self.descartadas.append((fila.nome, entrega.body))
            return
        try:
            self._enfileirar(destino, entrega)
        except TransportError:
            self.descartadas.append((destino.nome, entrega.body))

    async def _entregar_apos(self, fila: _Fila, entrega: _Entrega, segundos: float) -> None:
        """Realiza o ``delay_ms`` de :meth:`publish` -- o ``DelaySeconds`` do SQS em memoria."""
        await self._clock.sleep(segundos)
        try:
            self._enfileirar(fila, entrega)
        except TransportError:
            self.descartadas.append((fila.nome, entrega.body))

    # -- interno: contabilidade -------------------------------------------- #

    def _proxima_tag(self) -> int:
        """Gera o proximo ``delivery_tag``. Inteiro aqui, ``ReceiptHandle`` no SQS: e opaco."""
        self._proximo_tag += 1
        return self._proximo_tag

    def _registrar_pendente(self, tag: object, pendente: _Pendente) -> None:
        """Anota uma entrega despachada e ainda nao confirmada."""
        self._pendentes[tag] = pendente

    def _acompanhar_handler(self, corrotina: Coroutine[Any, Any, None]) -> None:
        """Dispara o handler em tarefa propria e a mantem rastreada para ``drenar`` e ``close``."""
        tarefa: asyncio.Task[None] = asyncio.create_task(corrotina)
        self._em_voo.add(tarefa)
        tarefa.add_done_callback(self._em_voo.discard)

    def _agendar(self, corrotina: Coroutine[Any, Any, None]) -> None:
        """Dispara um temporizador (TTL ou ``delay_ms``) rastreado por ``drenar`` e ``close``."""
        tarefa: asyncio.Task[None] = asyncio.create_task(corrotina)
        self._temporizadores.add(tarefa)
        tarefa.add_done_callback(self._temporizadores.discard)

    def _esquecer_assinatura(self, assinatura: _Assinatura) -> None:
        """Remove a assinatura cancelada da contabilidade do transporte."""
        if assinatura in self._assinaturas:
            self._assinaturas.remove(assinatura)

    def _exigir_disponivel(self, operacao: str) -> None:
        """Levanta ``TransportError`` quando o "broker" esta fora do ar ou o transporte, fechado."""
        if self.indisponivel:
            raise TransportError(
                f"broker indisponivel: {operacao} recusado pelo MemoryTransport",
                operacao=operacao,
            )
        if self._fechado:
            raise TransportError(f"transporte fechado: {operacao} recusado", operacao=operacao)

    def _ocupado(self) -> bool:
        """Diz se ha trabalho em voo, no criterio documentado em :meth:`drenar`."""
        if self._pendentes or self._em_voo or self._temporizadores:
            return True
        return any(fila.itens and fila.assinaturas for fila in self._filas.values())

    def _diagnostico_de_drenagem(self, limite: float) -> str:
        """Monta a mensagem de erro de :meth:`drenar`, apontando o que ficou pendente."""
        partes: list[str] = [f"drenar() nao sossegou em {limite:g}s de tempo real"]
        paradas = [
            f"{fila.nome}={len(fila.itens)}"
            for fila in self._filas.values()
            if fila.itens and fila.assinaturas
        ]
        if paradas:
            partes.append(f"filas com consumidor ainda cheias: {', '.join(paradas)}")
        if self._pendentes:
            partes.append(f"entregas nao confirmadas: {len(self._pendentes)}")
        if self._em_voo:
            partes.append(f"handlers em execucao: {len(self._em_voo)}")
        if self._temporizadores:
            partes.append(f"temporizadores pendentes: {len(self._temporizadores)}")
        if self.falhas_de_handler:
            partes.append(f"primeira excecao de handler: {self.falhas_de_handler[0]!r}")
        return "; ".join(partes)

    def _interpretar_morta(self, fila_origem: str, entrega: _Entrega) -> MensagemMorta:
        """Interpreta o corpo de uma mensagem de DLQ nos dois formatos de 6.2.6."""
        envelope: Envelope | None = None
        falha: Mapping[str, Any] = {}
        try:
            dados = json.loads(entrega.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            dados = None
        if isinstance(dados, Mapping):
            bruto = dados.get("envelope")
            if isinstance(bruto, Mapping):
                diagnostico = dados.get("falha")
                if isinstance(diagnostico, Mapping):
                    falha = diagnostico
                try:
                    envelope = Envelope.from_dict(bruto)
                except InvalidEnvelopeError:
                    envelope = None
            else:
                try:
                    envelope = Envelope.from_dict(dados)
                except InvalidEnvelopeError:
                    envelope = None
        return MensagemMorta(
            fila_origem=fila_origem,
            envelope=envelope,
            falha=falha,
            headers=dict(entrega.headers),
            body=entrega.body,
        )
