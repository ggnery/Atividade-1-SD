"""Relogio injetavel do middleware (secao 10.3.1 do design).

Nenhum modulo de ``hospitalmq`` chama ``datetime.now``, ``time.monotonic`` ou ``asyncio.sleep``
diretamente: todos recebem um :class:`Clock` no construtor, com :class:`RealClock` como padrao.
Essa e a peca que permite verificar a politica de espera 1s/2s/4s de R2.2, o timeout de RPC de
R3.3 e a expiracao do JWT de R4.2 **sem esperar tempo real** -- a suite inteira cai de mais de
90 s para menos de 7 s -- e, mais importante, da poder de asserção exata:
``clock.sleeps == [1.0, 2.0, 4.0]`` verifica a politica do requisito, e nao um "demorou mais de
6 segundos".

Sao dois relogios distintos, deliberadamente (secao 9.2.4):

* :meth:`Clock.now` -- relogio de **parede**, usado no ``timestamp`` do Envelope (R1.2, R5.1).
  Precisa ser comparavel entre processos, entao pode sofrer ajuste de NTP.
* :meth:`Clock.monotonic` -- relogio **monotonico**, usado em ``duracao_ms`` (R5.4) e no prazo do
  RPC. Nunca anda para tras; medir duracao com relogio de parede e o erro classico que produz
  duracoes negativas quando o NTP ajusta a hora.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

__all__ = ["Clock", "ManualClock", "RealClock"]


@runtime_checkable
class Clock(Protocol):
    """Fonte de tempo do middleware. Injetada; nunca acessada globalmente."""

    def now(self) -> datetime:
        """Devolve o instante atual como ``datetime`` *aware* em UTC.

        Returns:
            Momento atual em UTC. E o valor gravado em ``Envelope.timestamp`` (R1.2).
        """
        ...

    def monotonic(self) -> float:
        """Devolve um contador monotonico em segundos, imune a ajuste de relogio.

        Returns:
            Segundos decorridos desde um marco arbitrario. Serve apenas para **diferencas**:
            medicao de ``duracao_ms`` (R5.4) e calculo de prazo de RPC (R3.3).
        """
        ...

    async def sleep(self, seconds: float) -> None:
        """Suspende a corrotina pelo intervalo pedido.

        Args:
            seconds: Intervalo em segundos. Valores <= 0 apenas cedem o controle do laco.
        """
        ...


class RealClock(Clock):
    """Relogio de producao: delega ao sistema operacional e ao ``asyncio``."""

    __slots__ = ()

    def now(self) -> datetime:
        """Devolve ``datetime.now(UTC)``.

        Returns:
            Instante atual, *aware*, em UTC.
        """
        return datetime.now(UTC)

    def monotonic(self) -> float:
        """Devolve ``time.monotonic()``.

        Returns:
            Contador monotonico em segundos.
        """
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        """Aguarda de verdade, cedendo o *event loop*.

        Args:
            seconds: Intervalo em segundos.
        """
        await asyncio.sleep(seconds)


class ManualClock(Clock):
    """Relogio de teste: o tempo so avanca quando o teste manda, e ``sleep`` e instantaneo.

    Alem da velocidade, registra em :attr:`sleeps` toda espera solicitada, o que transforma a
    politica de backoff de R2.2 em uma asserção exata.

    Attributes:
        sleeps: Registro auditavel, em ordem, de cada intervalo pedido a :meth:`sleep`.
    """

    __slots__ = ("_agora", "_mono", "sleeps")

    def __init__(self, inicio: datetime | None = None) -> None:
        """Cria o relogio manual.

        Args:
            inicio: Instante inicial. Se omitido, usa ``2026-07-27T12:00:00Z`` -- uma data fixa,
                para que Envelopes construidos em teste sejam byte a byte reproduziveis
                (secao 10.3.3). Um ``datetime`` ingenuo e interpretado como UTC.
        """
        base = inicio or datetime(2026, 7, 27, 12, 0, 0, tzinfo=UTC)
        self._agora: datetime = base if base.tzinfo is not None else base.replace(tzinfo=UTC)
        self._mono: float = 0.0
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        """Devolve o instante corrente simulado.

        Returns:
            O momento definido na construcao, mais tudo o que foi avancado desde entao.
        """
        return self._agora

    def monotonic(self) -> float:
        """Devolve o contador monotonico simulado.

        Returns:
            Segundos acumulados por :meth:`avancar` e :meth:`sleep`.
        """
        return self._mono

    async def sleep(self, seconds: float) -> None:
        """Registra a espera, avanca o tempo simulado e cede o controle do laco.

        Args:
            seconds: Intervalo pedido, gravado em :attr:`sleeps`.
        """
        self.sleeps.append(seconds)
        self.avancar(seconds)
        await asyncio.sleep(0)  # cede o controle do loop, custo ~0

    def avancar(self, seconds: float) -> None:
        """Avanca o tempo simulado sem registrar espera.

        Args:
            seconds: Quantidade de segundos a somar aos dois relogios.
        """
        self._mono += seconds
        self._agora += timedelta(seconds=seconds)
