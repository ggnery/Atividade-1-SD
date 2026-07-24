"""Ponto de entrada do ``triage-service``: FastAPI minimo + ``Consumer`` (design 9.7.2, 12.2).

Reduzido a poucas linhas por construcao -- a semantica de mensageria mora no middleware
``hospitalmq`` e a regra clinica em :mod:`handler`. Aqui apenas se monta o transporte,
liga o ``Consumer`` a fila ``q.triage.sinais-registrados`` com a sessao do schema
``triagem`` (so para a marca de idempotencia; nao ha tabela de dominio, decisao D3) e se
expoe ``/health`` e ``/metrics`` pela fabrica comum.

Executado por ``uvicorn --app-dir services/triage-service main:app`` (design 12.2/12.3): o
objeto ``app`` de modulo e o que o servidor ASGI carrega, e o *lifespan* da fabrica conecta
o transporte, declara a topologia e inicia o consumo no *startup*.
"""

from __future__ import annotations

from handler import FILA_SINAIS_REGISTRADOS, registrar_handlers

from services.comum.app import criar_app
from services.comum.bootstrap import montar_consumidor, montar_transporte
from services.comum.db import obter_engine, obter_sessionmaker

SERVICO = "triage-service"
"""Nome do processo: compoe a chave de idempotencia e o ``producer`` do ``alerta.gerado``."""

SCHEMA = "triagem"
"""Schema do banco onde vive ``mensagens_processadas`` (design 7.4.6). Sem tabela de dominio."""


_transporte = montar_transporte(SERVICO)
_sessao = obter_sessionmaker()
_consumidor = montar_consumidor(
    SERVICO,
    [FILA_SINAIS_REGISTRADOS],
    transporte=_transporte,
    session_factory=_sessao,
    schema=SCHEMA,
)
registrar_handlers(_consumidor)

app = criar_app(
    servico=SERVICO,
    transporte=_transporte,
    consumidor=_consumidor,
    engine=obter_engine(),
)
"""Aplicacao ASGI carregada pelo ``uvicorn``; o *lifespan* sobe e encerra a mensageria."""
