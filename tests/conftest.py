"""Fixtures compartilhadas dos testes de unidade do nucleo do HospitalMQ.

Todos os testes correm sobre o :class:`~hospitalmq.transport.memory.MemoryTransport`, sem broker,
com um :class:`~hospitalmq.clock.ManualClock` injetado para que a politica de espera 1s/2s/4s de
R2.2 seja verificavel por asserção exata sobre ``clock.sleeps``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from hospitalmq.clock import ManualClock
from hospitalmq.config import TOPOLOGIA_PADRAO
from hospitalmq.metrics import configure_metrics
from hospitalmq.transport.memory import MemoryTransport


@pytest.fixture(autouse=True)
def _metricas_isoladas() -> None:
    """Zera o registro de metricas do processo antes de cada teste."""
    configure_metrics("hospitalmq-teste")


@pytest.fixture
def clock() -> ManualClock:
    """Relogio manual, o mesmo injetado no transporte e nos componentes do nucleo."""
    return ManualClock()


@pytest_asyncio.fixture
async def mem(clock: ManualClock) -> AsyncIterator[MemoryTransport]:
    """MemoryTransport ja conectado e com a topologia padrao declarada."""
    transporte = MemoryTransport(clock=clock)
    await transporte.connect()
    await transporte.declare_topology(TOPOLOGIA_PADRAO)
    try:
        yield transporte
    finally:
        await transporte.close()
