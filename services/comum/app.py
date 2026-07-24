"""Fabrica de FastAPI minima para os ``Servico_Consumidor`` (secoes 9.7.2, 8.6.1, R8.6, R5.5).

Todo processo -- inclusive os consumidores, que nao tem API publica de negocio -- sobe um FastAPI
minimo com ``/health`` e ``/metrics``, executado ao lado do laco de consumo (secao 9.7.2). Este
modulo constroi esse aplicativo e o *lifespan* que sobe e encerra a infraestrutura de mensageria:
conecta o transporte, declara a topologia, inicia o :class:`~hospitalmq.consumer.Consumer` e os
:class:`~hospitalmq.rpc.RpcServer`, opcionalmente o *relay* do outbox, e desfaz tudo na ordem
inversa no *shutdown*.

**Este NAO e o ``api-gateway``.** O gateway tem a sua propria borda (rotas de negocio, OpenAPI rico,
projecao do painel, SSE) e nao usa esta fabrica; aqui ha apenas o par ``/health`` + ``/metrics`` que
R8.6 e R5.5 exigem de todos os processos.

Distincao *liveness* x *readiness* (secao 8.6.1):

* ``GET /health`` -- *liveness*: **sempre 200** enquanto o processo vive, mesmo com o Broker fora
  (R8.5). Nao faz E/S ativa; apenas descreve o estado observado no *boot* e na reconexao.
* ``GET /health/ready`` -- *readiness*: **200** quando o processo atende trafego util (Broker
  conectado, consumo assinado e banco respondendo ao ``SELECT 1``), **503** em ``problem+json`` caso
  contrario.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, Final, Literal

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse

from hospitalmq.config import TOPOLOGIA_PADRAO, settings
from hospitalmq.errors import AuthError, TransportError
from hospitalmq.logging import correlation_id_atual, get_logger
from hospitalmq.metrics import get_metrics, render_prometheus

from .db import checar_saude

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from collections.abc import AsyncIterator, Sequence

    from sqlalchemy.ext.asyncio import AsyncEngine

    from hospitalmq.config import TopologySpec
    from hospitalmq.consumer import Consumer
    from hospitalmq.publisher import OutboxRelay
    from hospitalmq.rpc import RpcServer
    from hospitalmq.transport.base import Transport

__all__ = ["VERSAO_PADRAO", "criar_app"]

VERSAO_PADRAO: Final[str] = "1.0.0"
"""Versao da aplicacao reportada em ``/health`` (secao 8.6.1). Distinta de
``hospitalmq.__version__``, que versiona o middleware, nao o servico que o exercita."""

_INTERVALO_RECONEXAO_S: Final[float] = 3.0
"""Espera entre tentativas de reconexao quando o Broker esta fora no *boot* (R8.5)."""

_MOTIVO_PAPEL_INSUFICIENTE: Final[str] = "papel_insuficiente"
"""Valor de ``AuthError.motivo`` que a borda HTTP traduz em ``403`` (secao do ``auth.py``)."""

_PAPEIS_METRICS: Final[tuple[str, ...]] = ("admin", "auditor")
"""Papeis aceitos em ``/metrics`` quando ``HOSPITALMQ_METRICS_PROTEGIDO=true`` (secao 8.6.2)."""


def _agora_iso() -> str:
    """Devolve o instante corrente em ISO-8601 UTC com sufixo ``Z`` (esquema da secao 8.6.1)."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class _EstadoSaude:
    """Estado observavel do processo, lido pelos endpoints de *health* sem E/S ativa (secao 8.6.1).

    Attributes:
        servico: Nome do processo, reportado em ``/health``.
        versao: Versao da aplicacao.
        transporte: Nome do transporte configurado (``amqp`` | ``memory`` | ``sqs``).
        tem_banco: Se ha engine de banco a sondar no *readiness*.
        iniciado_em: Instante monotonico da criacao do app, base do ``uptime_s``.
        broker_conectado: ``True`` apos ``connect``/``declare_topology`` bem-sucedidos.
        pronto: ``True`` quando o consumo esta assinado e o processo atende trafego util.
        encerrando: ``True`` a partir do *shutdown*, para o supervisor de reconexao parar.
        parar_evt: Evento sinalizado no *shutdown*, que acorda o supervisor de reconexao.
    """

    servico: str
    versao: str
    transporte: str
    tem_banco: bool
    iniciado_em: float
    broker_conectado: bool = False
    pronto: bool = False
    encerrando: bool = False
    parar_evt: asyncio.Event | None = field(default=None, repr=False)

    def uptime_s(self) -> float:
        """Segundos decorridos desde a criacao do app, medidos por relogio monotonico."""
        return round(time.monotonic() - self.iniciado_em, 1)


def criar_app(
    *,
    servico: str,
    transporte: Transport,
    consumidor: Consumer | None = None,
    servidores_rpc: Sequence[RpcServer] = (),
    relay: OutboxRelay | None = None,
    engine: AsyncEngine | None = None,
    topologia: TopologySpec = TOPOLOGIA_PADRAO,
    versao: str = VERSAO_PADRAO,
) -> FastAPI:
    """Monta o FastAPI minimo de um ``Servico_Consumidor`` com ``/health`` e ``/metrics``.

    O *lifespan* devolvido conecta o transporte e declara a topologia no *startup*, inicia o consumo
    e as operacoes RPC, e desfaz tudo no *shutdown*. Se o Broker estiver fora no *boot*, o processo
    **sobe assim mesmo** (R8.5): ``/health`` responde ``200 degradado`` e um supervisor tenta a
    reconexao em segundo plano ate conseguir ou o processo encerrar.

    Args:
        servico: Nome do processo, ex. ``"vitals-service"``. Alimenta ``/health`` e os logs de
            ciclo.
        transporte: O :class:`~hospitalmq.transport.base.Transport` ja construido pela *factory*
            (``criar_transporte``), ainda **nao** conectado -- quem conecta e o *lifespan*.
        consumidor: O :class:`~hospitalmq.consumer.Consumer` com os *handlers* ja registrados, ou
            ``None`` para um processo que so exponha operacoes RPC.
        servidores_rpc: Os :class:`~hospitalmq.rpc.RpcServer` a iniciar, ou vazio.
        relay: O *relay* do outbox transacional, presente apenas no ``admission-service`` (D6).
        engine: O :class:`AsyncEngine` do banco, usado no *readiness* para o ``SELECT 1`` e liberado
            no *shutdown*. ``None`` quando o processo nao tem banco.
        topologia: A topologia declarativa a materializar. Padrao :data:`TOPOLOGIA_PADRAO`.
        versao: Versao da aplicacao reportada em ``/health``.

    Returns:
        O :class:`FastAPI` pronto para ser servido por ``uvicorn``.
    """
    log = get_logger(__name__)
    estado = _EstadoSaude(
        servico=servico,
        versao=versao,
        transporte=settings.transporte,
        tem_banco=engine is not None,
        iniciado_em=time.monotonic(),
    )

    async def _subir() -> None:
        """Conecta o transporte, declara a topologia e inicia consumo e RPC (secao 6.11.1)."""
        await transporte.connect()
        await transporte.declare_topology(topologia)
        if consumidor is not None:
            await consumidor.iniciar()
        for servidor in servidores_rpc:
            await servidor.start()
        estado.broker_conectado = True
        estado.pronto = True
        log.info("servico.pronto", servico=servico, transporte=estado.transporte)

    async def _supervisionar_reconexao() -> None:
        """Repete a subida da infraestrutura enquanto o Broker estiver fora (R8.5)."""
        assert estado.parar_evt is not None
        while not estado.encerrando:
            try:
                await asyncio.wait_for(estado.parar_evt.wait(), timeout=_INTERVALO_RECONEXAO_S)
                return  # parar_evt sinalizado: o processo esta encerrando
            except TimeoutError:
                pass
            if estado.encerrando:
                return
            try:
                await _subir()
                return
            except TransportError as exc:
                log.warning("servico.broker_indisponivel", servico=servico, detalhe=str(exc))

    @contextlib.asynccontextmanager
    async def _ciclo_de_vida(_app: FastAPI) -> AsyncIterator[None]:
        """Sobe a infraestrutura no *startup* e a encerra graciosamente no *shutdown*."""
        estado.parar_evt = asyncio.Event()
        tarefas: list[asyncio.Task[None]] = []
        try:
            await _subir()
        except TransportError as exc:
            log.warning("servico.broker_indisponivel", servico=servico, detalhe=str(exc))
            tarefas.append(
                asyncio.create_task(_supervisionar_reconexao(), name=f"{servico}-reconexao")
            )
        if relay is not None:
            tarefas.append(asyncio.create_task(relay.run(), name=f"{servico}-relay"))
        try:
            yield
        finally:
            estado.encerrando = True
            estado.pronto = False
            if estado.parar_evt is not None:
                estado.parar_evt.set()
            if relay is not None:
                with contextlib.suppress(Exception):
                    await relay.stop()
            for servidor in servidores_rpc:
                with contextlib.suppress(Exception):
                    await servidor.stop()
            if consumidor is not None:
                with contextlib.suppress(Exception):
                    await consumidor.parar()
            for tarefa in tarefas:
                tarefa.cancel()
            for tarefa in tarefas:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await tarefa
            with contextlib.suppress(Exception):
                await transporte.close()
            estado.broker_conectado = False
            if engine is not None:
                with contextlib.suppress(Exception):
                    await engine.dispose()
            log.info("servico.encerrado", servico=servico)

    app = FastAPI(
        title=f"HospitalMQ - {servico}",
        version=versao,
        description=f"Health e metricas do {servico} do Hospital Inteligente.",
        lifespan=_ciclo_de_vida,
        docs_url="/docs",
    )
    app.state.saude = estado

    @app.get("/health", tags=["observabilidade"], summary="Liveness: sempre 200 (R8.5, R8.6)")
    async def health() -> dict[str, Any]:
        """Descreve o proprio estado do processo, respondendo ``200`` mesmo com o Broker fora."""
        return _corpo_saude(estado)

    @app.get(
        "/health/ready",
        tags=["observabilidade"],
        summary="Readiness: 200 pronto / 503 nao pronto (R8.6)",
    )
    async def health_ready(request: Request) -> Response:
        """Confirma que o processo atende trafego util: Broker conectado, consumo e banco de pe."""
        banco_ok = True
        if engine is not None:
            banco_ok = await checar_saude(engine)
        pronto = estado.pronto and estado.broker_conectado and banco_ok
        corpo = _corpo_saude(estado)
        corpo["banco"]["estado"] = _rotulo_banco(estado.tem_banco, banco_ok)
        if pronto:
            return JSONResponse(corpo)
        return _problema(
            503,
            "Servico nao pronto",
            "o processo ainda nao atende trafego util (Broker ou banco indisponivel)",
            request.url.path,
        )

    @app.get(
        "/metrics",
        tags=["observabilidade"],
        summary="Contadores acumulados do HospitalMQ (R5.5)",
    )
    async def metrics(
        request: Request,
        formato: Annotated[Literal["json", "prometheus"], Query(alias="format")] = "json",
    ) -> Response:
        """Expoe os contadores do processo em JSON (padrao) ou no formato Prometheus."""
        if settings.metrics_protegido:
            negado = _autorizar_metrics(request)
            if negado is not None:
                return negado
        instantaneo = get_metrics().snapshot()
        if formato == "prometheus":
            return PlainTextResponse(
                render_prometheus(instantaneo), media_type="text/plain; version=0.0.4"
            )
        return JSONResponse(instantaneo)

    return app


def _rotulo_banco(tem_banco: bool, banco_ok: bool) -> str:
    """Traduz o estado do banco no rotulo exposto em ``/health`` (secao 8.6.1)."""
    if not tem_banco:
        return "sem-banco"
    return "conectado" if banco_ok else "desconectado"


def _corpo_saude(estado: _EstadoSaude) -> dict[str, Any]:
    """Monta o corpo comum de ``/health`` e ``/health/ready`` a partir do estado observado.

    Args:
        estado: O estado do processo mantido pelo *lifespan*.

    Returns:
        O dicionario no esquema da secao 8.6.1, com ``status`` ``"ok"`` quando pronto e conectado,
        ou ``"degradado"`` caso contrario. Nao faz E/S: o campo ``banco`` reflete apenas se ha banco
        configurado -- o *readiness* o sobrescreve com o resultado do ``SELECT 1``.
    """
    saudavel = estado.pronto and estado.broker_conectado
    return {
        "status": "ok" if saudavel else "degradado",
        "servico": estado.servico,
        "versao": estado.versao,
        "uptime_s": estado.uptime_s(),
        "timestamp": _agora_iso(),
        "broker": {
            "estado": "conectado" if estado.broker_conectado else "desconectado",
            "transporte": estado.transporte,
        },
        "banco": {"estado": "conectado" if estado.tem_banco else "sem-banco"},
    }


def _autorizar_metrics(request: Request) -> Response | None:
    """Aplica a protecao opcional de ``/metrics`` por papel ``admin``/``auditor`` (secao 8.6.2).

    Args:
        request: A requisicao, de onde sai o cabecalho ``Authorization``.

    Returns:
        ``None`` quando a credencial e valida e o papel e suficiente; caso contrario um ``problem+
        json`` com ``401`` (credencial ausente/invalida) ou ``403`` (papel insuficiente).
    """
    from hospitalmq.auth import extrair_token_bearer, validar_token, verificar_papel

    try:
        token = extrair_token_bearer(request.headers.get("authorization"))
        identidade = validar_token(token)
        verificar_papel(identidade, *_PAPEIS_METRICS)
        return None
    except AuthError as exc:
        status = 403 if exc.motivo == _MOTIVO_PAPEL_INSUFICIENTE else 401
        titulo = "Papel sem permissao" if status == 403 else "Credencial ausente ou invalida"
        return _problema(status, titulo, exc.mensagem, request.url.path)


def _problema(status: int, titulo: str, detalhe: str, instancia: str) -> JSONResponse:
    """Monta uma resposta de erro em ``application/problem+json`` (RFC 7807, secao 8.4).

    Args:
        status: Codigo HTTP.
        titulo: Titulo curto e estavel do erro.
        detalhe: Descricao legivel especifica desta ocorrencia.
        instancia: Caminho da requisicao que produziu o erro.

    Returns:
        A :class:`JSONResponse` com o corpo RFC 7807 e o ``correlation_id`` corrente quando houver.
    """
    corpo: dict[str, Any] = {
        "type": "about:blank",
        "title": titulo,
        "status": status,
        "detail": detalhe,
        "instance": instancia,
    }
    correlacao = correlation_id_atual()
    if correlacao is not None:
        corpo["correlation_id"] = correlacao
    cabecalhos = {"WWW-Authenticate": "Bearer"} if status == 401 else None
    return JSONResponse(
        corpo, status_code=status, media_type="application/problem+json", headers=cabecalhos
    )
