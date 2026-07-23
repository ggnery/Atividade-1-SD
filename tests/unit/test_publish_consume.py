"""Publicacao e consumo assincronos sobre o MemoryTransport (R1.1, R1.2, R2.1, R4.6)."""

from __future__ import annotations

from datetime import UTC

from hospitalmq.clock import ManualClock
from hospitalmq.consumer import Consumer, MessageContext
from hospitalmq.envelope import Identity
from hospitalmq.publisher import Publisher
from hospitalmq.transport.memory import MemoryTransport


async def test_publish_preenche_campos_do_envelope(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R1.2: o Publisher preenche message_id, correlation_id e timestamp."""
    pub = Publisher(transport=mem, producer="vitals-service", clock=clock)

    envelope = await pub.publish("alerta.gerado", {"leito_id": "UTI-07"})

    assert envelope.message_id
    assert envelope.correlation_id
    assert envelope.timestamp is not None
    assert envelope.timestamp.tzinfo is not None
    assert envelope.timestamp.astimezone(UTC) == envelope.timestamp.astimezone(UTC)
    assert envelope.producer == "vitals-service"
    assert envelope.attempt == 1
    assert envelope.type == "alerta.gerado"
    assert envelope.payload == {"leito_id": "UTI-07"}


async def test_consumidor_recebe_sem_conhecer_a_fila(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R1.1: o produtor publica so por tipo; quem consome vem das bindings."""
    recebidos: list[MessageContext[object]] = []
    consumer = Consumer(service="alert-service", transport=mem, clock=clock)

    @consumer.on("alerta.gerado", queue="q.alert.alerta-gerado")
    async def _handler(ctx: MessageContext[object]) -> None:
        recebidos.append(ctx)

    await consumer.iniciar()

    pub = Publisher(transport=mem, producer="vitals-service", clock=clock)
    publicado = await pub.publish("alerta.gerado", {"leito_id": "UTI-07"})
    await mem.drenar()

    assert len(recebidos) == 1
    assert recebidos[0].envelope.message_id == publicado.message_id
    assert recebidos[0].envelope.payload == {"leito_id": "UTI-07"}


async def test_ack_manual_so_depois_do_sucesso_do_handler(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R2.1: o ACK acontece apenas apos o handler retornar com sucesso."""
    acks_durante_handler: list[int] = []
    consumer = Consumer(service="alert-service", transport=mem, clock=clock)

    @consumer.on("alerta.gerado", queue="q.alert.alerta-gerado")
    async def _handler(ctx: MessageContext[object]) -> None:
        # No instante do handler, esta mensagem ainda NAO foi confirmada.
        acks_durante_handler.append(len(mem.acks))

    await consumer.iniciar()
    pub = Publisher(transport=mem, producer="vitals-service", clock=clock)
    await pub.publish("alerta.gerado", {"leito_id": "UTI-07"})
    await mem.drenar()

    assert acks_durante_handler == [0]
    assert len(mem.acks) == 1


async def test_identidade_do_envelope_chega_ao_handler(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R4.6: a identidade viaja no Envelope e chega ao handler sem nova consulta."""
    vistos: list[Identity | None] = []
    consumer = Consumer(service="alert-service", transport=mem, clock=clock)

    @consumer.on("alerta.gerado", queue="q.alert.alerta-gerado")
    async def _handler(ctx: MessageContext[object]) -> None:
        vistos.append(ctx.identity)

    await consumer.iniciar()

    identidade = Identity(sub="monitor-l07", role="dispositivo", tipo="dispositivo")
    pub = Publisher(transport=mem, producer="vitals-service", clock=clock)
    await pub.publish("alerta.gerado", {"leito_id": "UTI-07"}, identity=identidade)
    await mem.drenar()

    assert vistos == [identidade]
