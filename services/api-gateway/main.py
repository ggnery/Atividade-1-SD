"""``api-gateway`` -- a borda HTTP sobre o middleware HospitalMQ (design 8, grupo G3).

Este e o *composition root* do gateway: monta o :class:`FastAPI`, o *lifespan* que sobe e encerra a
infraestrutura de mensageria, os middlewares de correlacao/log e os tratadores de erro RFC 7807.

**Fronteira de dados (R7.2, design 8.1.1).** A montagem injeta nas rotas exatamente tres
colaboradores -- :class:`RpcClient`, :class:`Publisher` e a projecao -- e **nenhuma sessao
de banco clinico**. Nao existe, no processo, objeto capaz de emitir SQL clinico: R7.2 vira uma
propriedade estrutural, verificavel em poucas linhas.

Executar (design 12.1, diretorio com hifen nao e modulo Python)::

    uvicorn --app-dir services/api-gateway main:app --host 0.0.0.0 --port 8000

O painel (casca HTML, projecao, SSE) e responsabilidade de outro agente: o ``main`` monta o roteador
do painel e serve os estaticos **quando presentes**, degradando graciosamente quando ainda nao.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import erros
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from rotas import auth as rota_auth
from rotas import internacoes as rota_internacoes
from rotas import pacientes as rota_pacientes
from rotas import saude as rota_saude
from rotas import sinais as rota_sinais
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from hospitalmq.auth import Autenticador, definir_autenticador_padrao
from hospitalmq.config import TOPOLOGIA_PADRAO, criar_transporte, settings
from hospitalmq.errors import TransportError
from hospitalmq.logging import bind_context, configure_logging, get_logger
from hospitalmq.metrics import get_metrics
from hospitalmq.publisher import Publisher
from hospitalmq.rpc import RpcClient

__all__ = ["app", "criar_app"]

_log = get_logger(__name__)

PRODUTOR: Final[str] = "api-gateway"
"""Nome do processo, gravado em ``Envelope.producer`` e no campo ``servico`` dos logs."""

VERSAO: Final[str] = "1.0.0"
"""Versao da aplicacao reportada em ``/health`` e no OpenAPI (design 8.6.1)."""

_INTERVALO_RECONEXAO_S: Final[float] = 3.0
"""Espera entre tentativas de reconexao quando o Broker esta fora no *boot* (R8.5)."""

_DIR_ESTATICOS: Final[Path] = Path(__file__).resolve().parent / "static"
"""Diretorio dos estaticos do painel (``painel.html``, ``painel.css``, ``painel.js``)."""

TAGS: Final[list[dict[str, str]]] = [
    {"name": "autenticacao", "description": "Login e emissao de JWT (R4.1)."},
    {"name": "pacientes", "description": "Cadastro, admissao, alta e prontuario (RPC sincrono)."},
    {"name": "sinais vitais", "description": "Ingestao assincrona de telemetria (202, R6.1)."},
    {"name": "painel", "description": "Painel de Leitos e stream SSE (R11)."},
    {"name": "operacao", "description": "Health checks e metricas (R8.6, R5.5)."},
]

_DESCRICAO = (
    "Borda HTTP sobre o middleware de mensageria HospitalMQ (grupo G3). Converte cada requisicao "
    "REST em uma operacao do middleware: RPC sincrono sobre fila para escrita/leitura clinica, "
    "publicacao assincrona para telemetria e leitura de projecao em memoria para o painel. O "
    "gateway nao acessa o banco clinico (R7.2)."
)


# --------------------------------------------------------------------------- #
# Estado observavel do processo (lido por /health sem E/S ativa)               #
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class EstadoGateway:
    """Estado do processo mantido pelo *lifespan* e lido pelos endpoints de saude (design 8.6.1).

    Attributes:
        servico: Nome do processo reportado em ``/health``.
        versao: Versao da aplicacao.
        transporte: Nome do transporte configurado (``amqp`` | ``memory`` | ``sqs``).
        iniciado_em: Instante monotonico da criacao do app, base do ``uptime_s``.
        broker_conectado: ``True`` apos conectar, declarar a topologia e iniciar o RPC.
        encerrando: ``True`` a partir do *shutdown*, para o supervisor de reconexao parar.
    """

    servico: str
    versao: str
    transporte: str
    iniciado_em: float
    broker_conectado: bool = False
    encerrando: bool = False


# --------------------------------------------------------------------------- #
# Middleware de borda: correlacao + log + metrica (design 8.1.2)               #
# --------------------------------------------------------------------------- #


class MiddlewareBorda:
    """Middleware ASGI que abre o ``correlation_id``, loga e mede cada requisicao (R5.1--R5.4).

    E ASGI puro (nao ``BaseHTTPMiddleware``) de proposito: assim roda na **mesma** *task* da
    requisicao e o ``correlation_id`` que ele grava na ``contextvar`` propaga para o endpoint e para
    os tratadores de erro sem copia de contexto. Funde as responsabilidades 1--2 de 8.1.2 numa unica
    passagem, preservando a ordem: o ``correlation_id`` existe **antes** de qualquer log, e
    o cabecalho ``X-Correlation-ID`` acompanha toda resposta, inclusive as de erro (R5.2).
    """

    def __init__(self, app: ASGIApp) -> None:
        """Guarda o proximo elemento da cadeia ASGI."""
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Processa uma requisicao HTTP, injetando correlacao, log e metrica."""
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        cid = _correlation_id_de(scope) or str(uuid.uuid4())
        bind_context(correlation_id=cid)
        metodo = scope.get("method", "GET")
        caminho = scope.get("path", "")
        inicio = time.monotonic()
        _log.info("http.request.recebida", metodo=metodo, rota=caminho)

        status_visto = {"codigo": 500}

        async def enviar(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_visto["codigo"] = message["status"]
                cabecalhos = MutableHeaders(scope=message)
                cabecalhos["X-Correlation-ID"] = cid
            await send(message)

        try:
            await self._app(scope, receive, enviar)
        finally:
            duracao_ms = round((time.monotonic() - inicio) * 1000, 2)
            rota = _rota_template(scope, caminho)
            get_metrics().incr(
                "http_requisicoes_total",
                rota=rota,
                metodo=metodo,
                status=str(status_visto["codigo"]),
            )
            _log.info(
                "http.request.concluida",
                metodo=metodo,
                rota=rota,
                status=status_visto["codigo"],
                duracao_ms=duracao_ms,
            )


def _correlation_id_de(scope: Scope) -> str | None:
    """Le ``X-Correlation-ID`` dos cabecalhos ASGI brutos (bytes), se presente (R5.2)."""
    for chave, valor in scope.get("headers", ()):
        if chave == b"x-correlation-id":
            texto = valor.decode("latin-1").strip()
            if texto:
                return texto
    return None


def _rota_template(scope: Scope, caminho: str) -> str:
    """Devolve o *template* da rota casada (ex. ``/pacientes/{paciente_id}/prontuario``).

    Usar o template, e nao o caminho concreto, mantem baixa a cardinalidade do rotulo ``rota`` da
    metrica ``http_requisicoes_total``. Cai no caminho concreto quando nenhuma rota casou (404).
    """
    rota = scope.get("route")
    caminho_rota = getattr(rota, "path", None)
    return caminho_rota if isinstance(caminho_rota, str) else caminho


# --------------------------------------------------------------------------- #
# Lifespan: composition root (design 8.1.1)                                    #
# --------------------------------------------------------------------------- #


def _montar_projecao(
    transporte: Any,  # noqa: ANN401 - duck-typing evita import circular do driver
) -> tuple[object | None, Any | None]:
    """Constroi a projecao e o ``Consumer`` de ``q.gateway.projecao``, se ha painel.

    O modulo ``projecao`` e a funcao ``registrar_handlers`` sao entregues pelo agente do painel. O
    nucleo HTTP os importa de forma tolerante: quando ausentes, o gateway sobe e serve a API sem o
    painel, e a resolucao de ``internacao_id`` de 8.2.5 degrada para ``503``/``409``.

    Returns:
        Par ``(projecao, consumer)``; ambos ``None`` quando o painel nao esta montado.
    """
    try:
        import projecao as mod_projecao  # entregue pelo agente do painel
    except Exception as exc:  # pragma: no cover - painel ainda nao entregue
        _log.info("gateway.projecao_indisponivel", detalhe=str(exc))
        return None, None

    try:
        from hospitalmq.consumer import Consumer

        projecao = mod_projecao.ProjecaoLeitos()
        consumer = Consumer(
            service=PRODUTOR,
            transport=transporte,
            session_factory=None,  # gateway nao tem banco (design 8.1.1, 8.7.1)
            prefetch=settings.prefetch,
        )
        mod_projecao.registrar_handlers(consumer, projecao)
        return projecao, consumer
    except Exception as exc:
        _log.warning("gateway.projecao_falhou", detalhe=str(exc))
        return None, None


def _ciclo_de_vida(
    estado: EstadoGateway,
) -> Callable[[FastAPI], contextlib.AbstractAsyncContextManager[None]]:
    """Fabrica o *context manager* de *lifespan* ligado ao ``estado`` observavel do processo."""

    @contextlib.asynccontextmanager
    async def ciclo(app: FastAPI) -> AsyncIterator[None]:
        transporte = criar_transporte(settings)
        autenticador = Autenticador(cfg=settings)
        definir_autenticador_padrao(autenticador)  # a fachada de modulo usa a mesma instancia

        publisher = Publisher(transport=transporte, producer=PRODUTOR)
        rpc = RpcClient(transporte, producer=PRODUTOR, default_timeout=settings.rpc_timeout_s)
        projecao, consumer = _montar_projecao(transporte)

        app.state.publisher = publisher
        app.state.rpc = rpc
        app.state.projecao = projecao
        app.state.autenticador = autenticador
        app.state.saude = estado

        tarefas: list[asyncio.Task[Any]] = []

        async def _subir() -> None:
            await transporte.connect()
            await transporte.declare_topology(TOPOLOGIA_PADRAO)
            await rpc.start()
            estado.broker_conectado = True
            ja_rodando = any(t.get_name() == "gateway-projecao" for t in tarefas)
            if consumer is not None and not ja_rodando:
                tarefas.append(asyncio.create_task(consumer.run(), name="gateway-projecao"))
            if projecao is not None:
                hidratar = getattr(projecao, "hidratar", None)
                if hidratar is not None:
                    with contextlib.suppress(Exception):
                        await hidratar(rpc)
            _log.info("gateway.pronto", transporte=estado.transporte)

        async def _supervisionar() -> None:
            while not estado.encerrando:
                await asyncio.sleep(_INTERVALO_RECONEXAO_S)
                if estado.encerrando:
                    return
                try:
                    await _subir()
                    return
                except TransportError as exc:
                    _log.warning("gateway.broker_indisponivel", detalhe=str(exc))

        try:
            await _subir()
        except TransportError as exc:
            # R8.5: o gateway sobe mesmo com o Broker fora; /health responde e as rotas dao 503.
            _log.warning("gateway.broker_indisponivel", detalhe=str(exc))
            tarefas.append(asyncio.create_task(_supervisionar(), name="gateway-reconexao"))

        try:
            yield
        finally:
            estado.encerrando = True
            estado.broker_conectado = False
            if consumer is not None:
                with contextlib.suppress(Exception):
                    await consumer.parar()
            for tarefa in tarefas:
                tarefa.cancel()
            for tarefa in tarefas:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await tarefa
            with contextlib.suppress(Exception):
                await rpc.close()
            with contextlib.suppress(Exception):
                await transporte.close()
            _log.info("gateway.encerrado")

    return ciclo


# --------------------------------------------------------------------------- #
# Montagem do aplicativo                                                        #
# --------------------------------------------------------------------------- #


def criar_app() -> FastAPI:
    """Monta e devolve o :class:`FastAPI` do ``api-gateway`` (design 8.1.1, 8.5).

    Configura o log estruturado, o OpenAPI rico (tags, metadados, ``persistAuthorization``), os
    tratadores RFC 7807, o middleware de borda, os roteadores de negocio e -- quando presentes -- o
    roteador do painel e os estaticos.

    Returns:
        O aplicativo pronto para ser servido por ``uvicorn``.
    """
    configure_logging(PRODUTOR, nivel=settings.log_level)
    estado = EstadoGateway(
        servico=PRODUTOR,
        versao=VERSAO,
        transporte=settings.transporte,
        iniciado_em=time.monotonic(),
    )

    app = FastAPI(
        title="HospitalMQ - API do Hospital Inteligente",
        version=VERSAO,
        description=_DESCRICAO,
        openapi_tags=TAGS,
        contact={"name": "Grupo G3 - Sistemas Distribuidos"},
        servers=[{"url": "http://localhost:8000", "description": "Compose local"}],
        swagger_ui_parameters={"persistAuthorization": True, "displayRequestDuration": True},
        lifespan=_ciclo_de_vida(estado),
    )

    # Middleware de borda (unico): correlacao + log + metrica.
    app.add_middleware(MiddlewareBorda)

    # Tratadores RFC 7807 (design 8.4.1).
    erros.registrar_tratadores(app)

    # Roteadores de negocio (design 8.2).
    app.include_router(rota_auth.router)
    app.include_router(rota_pacientes.router)
    app.include_router(rota_internacoes.router)
    app.include_router(rota_sinais.router)
    app.include_router(rota_saude.router)

    _montar_painel(app)

    @app.get("/", include_in_schema=False)
    async def raiz() -> RedirectResponse:
        """Redireciona a raiz para o Painel de Leitos (design 8.2.1, endpoint 18)."""
        return RedirectResponse(url="/painel", status_code=307)

    return app


def _montar_painel(app: FastAPI) -> None:
    """Monta o roteador do painel e serve os estaticos, quando entregues pelo agente do painel.

    Deixa o ``main`` "preparado para montar as rotas do painel e servir os estaticos" sem acoplar o
    nucleo HTTP a existencia deles: ambos os passos sao tolerantes a ausencia.
    """
    try:
        from rotas import painel as rota_painel  # entregue pelo agente do painel

        app.include_router(rota_painel.router)
        _log.info("gateway.painel_montado")
    except Exception as exc:  # pragma: no cover - painel ainda nao entregue
        _log.info("gateway.painel_ausente", detalhe=str(exc))

    if _DIR_ESTATICOS.is_dir() and any(_DIR_ESTATICOS.iterdir()):
        app.mount("/static", StaticFiles(directory=str(_DIR_ESTATICOS)), name="static")
        _log.info("gateway.estaticos_montados", diretorio=str(_DIR_ESTATICOS))


app = criar_app()
