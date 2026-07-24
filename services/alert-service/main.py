"""Ponto de entrada do ``alert-service`` (design 9.2.2, 9.7.2; R6.4, R6.5, R8.6).

Monta o processo consumidor do ``Alerta_Clinico``: um :class:`~hospitalmq.consumer.Consumer` na fila
``q.alert.alerta-gerado`` com o handler de :mod:`handler`, o canal de notificacao sabotavel de
:mod:`notificacao` e o FastAPI minimo comum (``/health`` e ``/metrics``) da
:func:`services.comum.app.criar_app`. Toda a semantica de mensageria -- Envelope, retentativa,
idempotencia, DLQ -- pertence ao ``hospitalmq``; aqui so ligamos as pecas.

O ``alert-service`` tem banco (schema ``alertas``), logo recebe ``session_factory`` e ``engine`` --
este ultimo alimenta o ``SELECT 1`` do *readiness* (design 8.6.1). Alem do publicador interno que o
``Consumer`` usa para ``alerta.notificado`` (via ``ctx.emitir``), o processo constroi um
:class:`~hospitalmq.publisher.Publisher` proprio, sobre o **mesmo** transporte, para emitir
``alerta.falhou`` fora da transacao ao esgotar as retentativas (ver docstring de :mod:`handler`).
Como R2.5 permite escalar para tres replicas (design 6.6), a falha por leito com taxa ``1.0`` e
deterministica mesmo entre replicas (design 12.5.3).

Como o diretorio tem hifen, o processo sobe **por caminho**, nunca por modulo (design 12.1)::

    uvicorn --app-dir services/alert-service main:app --host 0.0.0.0 --port 8004
"""

from __future__ import annotations

from typing import Final

from handler import FILA_ALERTA, registrar_handler
from notificacao import criar_canal_de_ambiente

from hospitalmq.publisher import Publisher
from services.comum.app import criar_app
from services.comum.bootstrap import montar_consumidor, montar_transporte
from services.comum.db import obter_engine, obter_sessionmaker

SERVICO: Final[str] = "alert-service"
"""Nome do processo: identidade no Compose, nos logs e no campo ``producer`` (design 12.1)."""

SCHEMA: Final[str] = "alertas"
"""Schema do banco de que o ``alert-service`` e dono (design 7.4.4)."""

# -- montagem do processo (design 9.2.2) ----------------------------------- #

_sessao = obter_sessionmaker()
_transporte = montar_transporte(SERVICO)
_consumidor = montar_consumidor(
    SERVICO,
    [FILA_ALERTA],
    transporte=_transporte,
    session_factory=_sessao,
    schema=SCHEMA,
)
_canal = criar_canal_de_ambiente()
_publisher = Publisher(transport=_transporte, producer=SERVICO)
registrar_handler(_consumidor, _canal, _publisher)

app = criar_app(
    servico=SERVICO,
    transporte=_transporte,
    consumidor=_consumidor,
    engine=obter_engine(),
)
"""Aplicacao FastAPI servida pelo ``uvicorn`` (``main:app``): ``/health`` e ``/metrics`` (R8.6).

O *lifespan* de :func:`services.comum.app.criar_app` conecta o transporte, declara a topologia,
assina ``q.alert.alerta-gerado`` e inicia o consumo no *startup*; encerra tudo no *shutdown*.
"""
