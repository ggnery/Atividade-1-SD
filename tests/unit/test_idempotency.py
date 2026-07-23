"""Idempotencia do consumidor: efeito unico sob entrega at-least-once (R2.4)."""

from __future__ import annotations

from datetime import UTC, datetime

from hospitalmq.clock import ManualClock
from hospitalmq.consumer import Consumer, MessageContext
from hospitalmq.envelope import Envelope
from hospitalmq.transport.memory import MemoryTransport

FILA = "q.alert.alerta-gerado"
TIPO = "alerta.gerado"


def _envelope() -> Envelope:
    return Envelope(
        message_id="11111111-1111-4111-8111-111111111111",
        correlation_id="22222222-2222-4222-8222-222222222222",
        causation_id=None,
        type=TIPO,
        version=1,
        timestamp=datetime(2026, 7, 27, 12, 0, tzinfo=UTC),
        producer="vitals-service",
        identity=None,
        attempt=1,
        payload={"leito_id": "UTI-07"},
    )


async def test_mesma_mensagem_entregue_duas_vezes_executa_o_handler_uma_vez(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R2.4: a mesma mensagem (mesmo message_id) executa o handler uma unica vez."""
    execucoes: list[str] = []
    consumer = Consumer(service="alert-service", transport=mem, clock=clock)

    @consumer.on(TIPO, queue=FILA)
    async def _handler(ctx: MessageContext[object]) -> None:
        execucoes.append(ctx.envelope.message_id)

    await consumer.iniciar()

    envelope = _envelope()
    await mem.entregar(FILA, envelope)
    await mem.drenar()
    await mem.entregar(FILA, envelope)  # reentrega: mesmo message_id
    await mem.drenar()

    assert execucoes == [envelope.message_id]
