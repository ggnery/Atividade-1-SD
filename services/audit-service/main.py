"""Ponto de entrada do ``audit-service`` (design 9.2.2, 9.7.2; R7.1, R8.6).

Monta o processo consumidor da Trilha_de_Auditoria: um
:class:`~hospitalmq.consumer.Consumer` na fila ``q.audit.todos`` (binding ``#``, design
6.5) com o handler unico de :mod:`handler`, e o FastAPI minimo comum (``/health`` e
``/metrics``) da :func:`services.comum.app.criar_app`. Toda a semantica de mensageria --
Envelope, retentativa, idempotencia, DLQ -- pertence ao ``hospitalmq``; aqui so ligamos as
pecas.

O ``audit-service`` **nao** expoe operacoes RPC nem publica eventos: a trilha e
somente-insercao e o seu acesso e leitura administrativa (design 7.9.1). Ele tem banco
(schema ``auditoria``), logo recebe ``session_factory`` e ``engine`` -- este ultimo
alimenta o ``SELECT 1`` do *readiness* (design 8.6.1).

Como o diretorio tem hifen, o processo sobe **por caminho**, nunca por modulo (design
12.1)::

    uvicorn --app-dir services/audit-service main:app --host 0.0.0.0 --port 8005
"""

from __future__ import annotations

from typing import Final

from handler import FILA_AUDITORIA, registrar_auditoria

from services.comum.app import criar_app
from services.comum.bootstrap import montar_consumidor, montar_transporte
from services.comum.db import obter_engine, obter_sessionmaker

SERVICO: Final[str] = "audit-service"
"""Nome do processo: identidade no Compose, nos logs e no campo ``producer`` (design 12.1)."""

SCHEMA: Final[str] = "auditoria"
"""Schema do banco de que o ``audit-service`` e dono (design 7.9.1)."""

# -- montagem do processo (design 9.2.2) ----------------------------------- #

_sessao = obter_sessionmaker()
_transporte = montar_transporte(SERVICO)
_consumidor = montar_consumidor(
    SERVICO,
    [FILA_AUDITORIA],
    transporte=_transporte,
    session_factory=_sessao,
    schema=SCHEMA,
)
registrar_auditoria(_consumidor)

app = criar_app(
    servico=SERVICO,
    transporte=_transporte,
    consumidor=_consumidor,
    engine=obter_engine(),
)
"""Aplicacao FastAPI servida pelo ``uvicorn`` (``main:app``): ``/health`` e ``/metrics``.

O *lifespan* de :func:`services.comum.app.criar_app` conecta o transporte, declara a
topologia, assina ``q.audit.todos`` e inicia o consumo no *startup* (R8.6, R5.5); encerra
tudo graciosamente no *shutdown*.
"""
