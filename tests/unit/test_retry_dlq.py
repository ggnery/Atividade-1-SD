"""Retentativa exponencial e encaminhamento a DLQ (R2.2, R2.3)."""

from __future__ import annotations

from hospitalmq.clock import ManualClock
from hospitalmq.consumer import Consumer, MessageContext
from hospitalmq.errors import PermanentError, TransientError
from hospitalmq.publisher import Publisher
from hospitalmq.transport.memory import MemoryTransport

FILA = "q.alert.alerta-gerado"
TIPO = "alerta.gerado"


async def test_erro_transitorio_retenta_na_sequencia_1s_2s_4s(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R2.2: erro transitorio gera retentativa 1s/2s/4s, medida pelo Clock manual."""
    chamadas: list[int] = []
    consumer = Consumer(service="alert-service", transport=mem, clock=clock)

    @consumer.on(TIPO, queue=FILA)
    async def _handler(ctx: MessageContext[object]) -> None:
        chamadas.append(ctx.envelope.attempt)
        raise TransientError("canal de notificacao fora do ar")

    await consumer.iniciar()
    pub = Publisher(transport=mem, producer="vitals-service", clock=clock)
    await pub.publish(TIPO, {"leito_id": "UTI-07"})
    await mem.drenar(limite=2.0)

    # Quatro entregas ao handler: a original mais tres retentativas.
    assert chamadas == [1, 2, 3, 4]
    # A espera passou pelas filas de espera de 4.6.2, nao por um sleep solto.
    assert mem.filas_de_espera_usadas == [
        f"{FILA}.retry.1s",
        f"{FILA}.retry.2s",
        f"{FILA}.retry.4s",
    ]
    assert clock.sleeps == [1.0, 2.0, 4.0]


async def test_dlq_exatamente_apos_a_terceira_retentativa(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R2.2, R2.3: a mensagem vai para a DLQ na quarta falha, com attempt=4, nao antes."""
    consumer = Consumer(service="alert-service", transport=mem, clock=clock)

    @consumer.on(TIPO, queue=FILA)
    async def _handler(ctx: MessageContext[object]) -> None:
        raise TransientError("dependencia instavel")

    await consumer.iniciar()
    pub = Publisher(transport=mem, producer="vitals-service", clock=clock)
    await pub.publish(TIPO, {"leito_id": "UTI-07"})
    await mem.drenar(limite=2.0)

    mortas = mem.na_dlq(FILA)
    assert len(mortas) == 1
    assert mortas[0].attempt == 4
    assert mortas[0].type == TIPO


async def test_erro_permanente_vai_direto_para_a_dlq_sem_retentativa(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R2.3: erro permanente vai direto a DLQ, sem consumir nenhuma retentativa."""
    chamadas: list[int] = []
    consumer = Consumer(service="alert-service", transport=mem, clock=clock)

    @consumer.on(TIPO, queue=FILA)
    async def _handler(ctx: MessageContext[object]) -> None:
        chamadas.append(ctx.envelope.attempt)
        raise PermanentError("leitura fora da faixa fisiologica", codigo="PAYLOAD_INVALIDO")

    await consumer.iniciar()
    pub = Publisher(transport=mem, producer="vitals-service", clock=clock)
    await pub.publish(TIPO, {"leito_id": "UTI-07"})
    await mem.drenar()

    assert chamadas == [1]  # nao houve retentativa
    assert clock.sleeps == []
    assert mem.filas_de_espera_usadas == []
    mortas = mem.na_dlq(FILA)
    assert len(mortas) == 1
    assert mortas[0].attempt == 1
