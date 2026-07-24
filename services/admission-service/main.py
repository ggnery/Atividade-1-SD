"""Ponto de entrada do ``admission-service`` -- RPC (``q.rpc.admission``), outbox e ``/health``.

O ``admission-service`` e o unico serviço com RPC **e** outbox (design 5.8, 4.9). Este ``main``
reduz-se a cablagem: constroi o transporte, o :class:`~hospitalmq.rpc.RpcServer`, o *relay* do
outbox e o FastAPI minimo de observabilidade, delegando o ciclo de vida (conectar, declarar
topologia, iniciar e encerrar) ao *lifespan* de :func:`services.comum.app.criar_app`.

Executar (perfis ``borda`` + ``dominio``, com ``DATABASE_URL`` e ``HOSPITALMQ_*`` no ambiente):

    uvicorn main:app --host 0.0.0.0 --port 8000

Peca central da montagem: a ``sessao`` do ``RpcServer`` e uma :class:`~outbox.FabricaSessaoRpc`, nao
o ``async_sessionmaker`` cru. E ela que expoe a sessao da transacao de escrita ao handler (que so
recebe ``payload`` e ``ctx``), permitindo a dominio, outbox e marca de idempotencia um so ``COMMIT``
(5.6.5). A marca de idempotencia usa ``schema="clinico"`` e ``consumidor="admission-rpc"`` (5.6.5).

Nota de importacao: o diretorio ``admission-service`` tem hifen e **nao** e um pacote Python
importavel; os modulos internos (``modelos``, ``repositorio``, ``outbox``, ``operacoes``) sao
importados pelo nome de topo, com o diretorio e a raiz do repositorio inseridos em ``sys.path``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

# --- bootstrap de sys.path (diretorio hifenizado nao e pacote importavel) ----- #
_RAIZ_REPO = Path(__file__).resolve().parents[2]
_DIR_SERVICO = Path(__file__).resolve().parent
for _caminho in (_RAIZ_REPO, _DIR_SERVICO):
    if str(_caminho) not in sys.path:
        sys.path.insert(0, str(_caminho))

import outbox  # noqa: E402
from modelos import SCHEMA  # noqa: E402
from operacoes import registrar_operacoes  # noqa: E402

from hospitalmq.clock import RealClock  # noqa: E402
from hospitalmq.idempotency import SqlIdempotencyStore  # noqa: E402
from hospitalmq.publisher import Publisher  # noqa: E402
from hospitalmq.rpc import RpcServer  # noqa: E402
from services.comum.app import criar_app  # noqa: E402
from services.comum.bootstrap import montar_transporte  # noqa: E402
from services.comum.db import obter_engine, obter_sessionmaker  # noqa: E402

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from fastapi import FastAPI

SERVICO: str = "admission-service"
"""Nome do processo -- ``Envelope.producer`` dos eventos e campo ``servico`` do log (design 6.4)."""

FILA_RPC: str = "q.rpc.admission"
"""Fila de requisicoes RPC atendida por este serviço (design 5.8.1, topologia congelada)."""

CONSUMIDOR_RPC: str = "admission-rpc"
"""Valor de ``consumidor`` da marca de idempotencia das escritas RPC (design 5.6.5), distinto dos
consumidores de evento -- o caso que a chave ``(consumidor, message_id)`` existe para cobrir."""


def criar_aplicacao() -> FastAPI:
    """Monta o ``admission-service`` completo e devolve o FastAPI pronto para o ``uvicorn``.

    Constroi transporte, engine/sessionmaker, o ``RpcServer`` (com a ponte de sessao e a marca de
    idempotencia no *schema* ``clinico``), registra as cinco operacoes e liga o *relay* do outbox.

    Returns:
        O :class:`fastapi.FastAPI` com ``/health``, ``/health/ready`` e ``/metrics``.
    """
    transporte = montar_transporte(SERVICO)
    engine = obter_engine()
    sessionmaker = obter_sessionmaker()
    relogio = RealClock()

    publisher = Publisher(transport=transporte, producer=SERVICO, clock=relogio)

    servidor = RpcServer(
        transporte,
        fila=FILA_RPC,
        producer=SERVICO,
        # FabricaSessaoRpc substitui o async_sessionmaker: seu .begin() cria a sessao, publica-a
        # no contextvar e a entrega ao RpcServer, para o handler ver a mesma transacao (5.6.5).
        sessao=outbox.FabricaSessaoRpc(sessionmaker),  # type: ignore[arg-type]
        idempotencia=SqlIdempotencyStore(schema=SCHEMA, clock=relogio),
        consumidor=CONSUMIDOR_RPC,
        clock=relogio,
    )
    registrar_operacoes(servidor, sessionmaker=sessionmaker, publisher=publisher, clock=relogio)

    relay = outbox.criar_relay(publisher=publisher, session_factory=sessionmaker, clock=relogio)

    return criar_app(
        servico=SERVICO,
        transporte=transporte,
        servidores_rpc=[servidor],
        relay=relay,
        engine=engine,
    )


app = criar_aplicacao()
"""Aplicacao ASGI servida pelo ``uvicorn`` (``uvicorn main:app``)."""
