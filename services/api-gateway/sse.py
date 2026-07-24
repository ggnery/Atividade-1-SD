"""Difusao Server-Sent Events do Painel_de_Leitos (design 8.7.3, R11.2).

Um :class:`DifusorSSE` mantem uma ``asyncio.Queue`` por conexao aberta e faz *fan-out* dos eventos
de
projecao no formato ``text/event-stream``. Tudo roda no mesmo *event loop* do FastAPI e do
:class:`~hospitalmq.consumer.Consumer`, de modo que a difusao e um ``put_nowait`` sincrono, sem
*thread-safety* extra e sem *polling* (a latencia tipica e de milissegundos, com folga sobre o
limite de
2 s do R11.2).

**Contrapressao (design 8.7.3).** Cada assinante tem uma fila de tamanho fixo. Se um navegador
lento a
enche, o difusor **descarta aquele assinante** e injeta um quadro-sentinela que encerra o gerador; o
``EventSource`` reconecta em 3 s e recebe um ``snapshot`` novo. Prefere-se estado correto e
atrasado a
uma fila infinita que consumiria memoria do gateway e degradaria os demais assinantes.

**Por que SSE e nao WebSocket nem polling (design 8.7.5).** O painel so precisa de fluxo
servidor -> cliente; o ``EventSource`` reconecta sozinho (campo ``retry``), usa HTTP/1.1 comum e a
mesma
cadeia de autenticacao do REST -- ~20 linhas de gerador assincrono, sem dependencia externa (R11.6).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - apenas para type-check
    from collections.abc import AsyncIterator, Callable

    from projecao import ProjecaoLeitos

    from hospitalmq.auth import Identity

__all__ = [
    "DifusorSSE",
    "agora_iso",
    "formatar_frame",
    "formatar_ping",
    "gerar_stream",
]

#: Tamanho maximo da fila de um assinante antes do descarte por contrapressao (design 8.7.3).
TAMANHO_FILA_ASSINANTE = 100

#: Silencio (segundos) apos o qual o gerador envia um ``ping`` para manter a conexao viva (8.7.3).
INTERVALO_PING_S = 15.0

#: Valor de ``retry`` (ms) enviado primeiro; governa o backoff do ``EventSource`` (design 8.7.4).
RETRY_MS = 3000

#: Quadro-sentinela que sinaliza ao gerador para encerrar a conexao (contrapressao, design 8.7.3).
_FRAME_FECHAR = "\x00fechar\x00"


def agora_iso() -> str:
    """Instante atual em ISO-8601 UTC com sufixo ``Z`` (formato dos ``timestamp`` do projeto)."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _json_default(valor: object) -> str:
    """Serializa tipos nao nativos do JSON: ``datetime`` -> ISO ``Z`` e ``Decimal`` -> ``str``
    exata."""
    if isinstance(valor, datetime):
        aware = valor if valor.tzinfo is not None else valor.replace(tzinfo=UTC)
        return aware.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(valor, Decimal):
        return str(valor)
    return str(valor)


def formatar_frame(evento_id: int, tipo: str, dados: object) -> str:
    """Monta um quadro SSE nomeado com ``id`` (design 8.7.3).

    Args:
        evento_id: Identificador monotonico do evento (vira ``Last-Event-ID`` no cliente).
        tipo: Nome do evento SSE (``snapshot`` | ``leito`` | ``alerta``).
        dados: Corpo serializavel em JSON de uma unica linha.

    Returns:
        O quadro no formato literal do protocolo, terminado por linha em branco.
    """
    corpo = json.dumps(dados, default=_json_default, ensure_ascii=False, separators=(",", ":"))
    return f"id: {evento_id}\nevent: {tipo}\ndata: {corpo}\n\n"


def formatar_ping() -> str:
    """Monta o quadro ``ping`` (sem ``id``) que mantem a conexao viva e detecta queda (R11.5)."""
    corpo = json.dumps({"t": agora_iso()}, separators=(",", ":"))
    return f"event: ping\ndata: {corpo}\n\n"


class DifusorSSE:
    """Registro de conexoes SSE e *fan-out* dos eventos de projecao (design 8.7.3).

    Nao trava o *event loop*: a difusao e ``put_nowait`` em cada fila; um assinante cuja fila
    esteja cheia
    e descartado (contrapressao). Uma unica instancia serve todas as conexoes do painel.
    """

    def __init__(self, *, tamanho_fila: int = TAMANHO_FILA_ASSINANTE) -> None:
        """Cria o difusor sem assinantes.

        Args:
            tamanho_fila: Capacidade da fila de cada conexao antes do descarte por contrapressao.
        """
        self._assinantes: set[asyncio.Queue[str]] = set()
        self._tamanho_fila: int = tamanho_fila
        self._ultimo_id: int = 0

    @property
    def ultimo_id(self) -> int:
        """Ultimo ``id`` SSE emitido pelo difusor."""
        return self._ultimo_id

    @property
    def total_assinantes(self) -> int:
        """Numero de conexoes SSE ativas."""
        return len(self._assinantes)

    def assinar(self) -> tuple[asyncio.Queue[str], Callable[[], None]]:
        """Registra uma conexao e devolve ``(fila, cancelar)`` (design 8.7.2).

        Returns:
            A fila de quadros SSE ja formatados e a funcao que remove o assinante (chamada no
            ``finally`` do gerador, garantindo limpeza mesmo em desconexao abrupta).
        """
        fila: asyncio.Queue[str] = asyncio.Queue(maxsize=self._tamanho_fila)
        self._assinantes.add(fila)

        def cancelar() -> None:
            self._assinantes.discard(fila)

        return fila, cancelar

    def publicar(self, tipo: str, dados: object) -> int:
        """Difunde um evento a todos os assinantes, descartando os que nao conseguem acompanhar.

        Args:
            tipo: Nome do evento SSE (``leito`` | ``alerta``).
            dados: Corpo serializavel em JSON.

        Returns:
            O ``id`` atribuido ao evento difundido.
        """
        self._ultimo_id += 1
        quadro = formatar_frame(self._ultimo_id, tipo, dados)
        lentos: list[asyncio.Queue[str]] = []
        for fila in self._assinantes:
            try:
                fila.put_nowait(quadro)
            except asyncio.QueueFull:
                lentos.append(fila)
        for fila in lentos:
            self._descartar(fila)
        return self._ultimo_id

    def _descartar(self, fila: asyncio.Queue[str]) -> None:
        """Remove um assinante lento e injeta o sentinela de fechamento (esvazia a fila antes)."""
        self._assinantes.discard(fila)
        while True:
            try:
                fila.get_nowait()
            except asyncio.QueueEmpty:
                break
        with contextlib.suppress(asyncio.QueueFull):  # a fila acabou de ser esvaziada
            fila.put_nowait(_FRAME_FECHAR)


async def gerar_stream(
    projecao: ProjecaoLeitos,
    identidade: Identity | None = None,
    *,
    intervalo_ping: float = INTERVALO_PING_S,
) -> AsyncIterator[str]:
    """Gera o fluxo ``text/event-stream`` de uma conexao ao ``/painel/stream`` (design 8.7.3,
    R11.2).

    Sequencia: ``retry`` -> ``snapshot`` completo (reconciliacao por snapshot, nao *replay*, design
    8.7.4) -> quadros ``leito`` / ``alerta`` conforme chegam -> ``ping`` a cada ``intervalo_ping``
    de
    silencio. O ``finally`` sempre cancela a assinatura, evitando vazamento de filas.

    Args:
        projecao: A projecao de onde sai o ``snapshot`` inicial e a assinatura de eventos.
        identidade: Identidade autenticada da conexao (reservada para log/auditoria; nao altera o
        fluxo).
        intervalo_ping: Segundos de silencio ate emitir um ``ping``.

    Yields:
        Quadros SSE ja formatados, prontos para o corpo da ``StreamingResponse``.
    """
    fila, cancelar = projecao.assinar()
    try:
        yield f"retry: {RETRY_MS}\n\n"
        yield formatar_frame(projecao.ultimo_id, "snapshot", projecao.snapshot())
        while True:
            try:
                quadro = await asyncio.wait_for(fila.get(), timeout=intervalo_ping)
            except TimeoutError:
                yield formatar_ping()
                continue
            if quadro == _FRAME_FECHAR:
                return
            yield quadro
    finally:
        cancelar()
