"""Observabilidade do gateway: ``/health``, ``/health/ready`` e ``/metrics`` (design 8.6, R8.6).

Distincao *liveness* x *readiness* (design 8.6.1):

* ``GET /health`` -- *liveness*: **sempre 200** enquanto o processo vive, mesmo com o Broker fora
  (R8.5). Descreve o estado observado sem E/S ativa. Se o *liveness* falhasse junto com o Broker, o
  Docker reiniciaria o gateway em laco, derrubando as conexoes SSE do painel e apagando a evidencia.
* ``GET /health/ready`` -- *readiness*: **200** quando o gateway atende trafego util (Broker
  conectado), **503** em ``problem+json`` caso contrario.

``/metrics`` expoe os contadores por processo em JSON (padrao) ou Prometheus; a protecao por
papel ``admin``/``auditor`` liga com ``HOSPITALMQ_METRICS_PROTEGIDO=true`` (design 8.6.2).
"""

from __future__ import annotations

import contextlib
import time
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from erros import problem_json
from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse

from hospitalmq.auth import extrair_token_bearer, validar_token, verificar_papel
from hospitalmq.config import settings
from hospitalmq.errors import AuthError
from hospitalmq.metrics import get_metrics, render_prometheus

__all__ = ["router"]

router = APIRouter(tags=["operacao"])

_PAPEIS_METRICS = ("admin", "auditor")
"""Papeis aceitos em ``/metrics`` quando a protecao esta ligada (design 8.6.2)."""


def _agora_iso() -> str:
    """Instante corrente em ISO-8601 UTC com sufixo ``Z`` (esquema da secao 8.6.1)."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _corpo_saude(request: Request) -> dict[str, Any]:
    """Monta o corpo comum de ``/health`` a partir do estado observado, sem E/S ativa (8.6.1)."""
    estado = request.app.state.saude
    conectado = bool(getattr(estado, "broker_conectado", False))
    uptime = round(time.monotonic() - getattr(estado, "iniciado_em", time.monotonic()), 1)
    corpo: dict[str, Any] = {
        "status": "ok" if conectado else "degradado",
        "servico": getattr(estado, "servico", "api-gateway"),
        "versao": getattr(estado, "versao", "1.0.0"),
        "uptime_s": uptime,
        "timestamp": _agora_iso(),
        "broker": {
            "estado": "conectado" if conectado else "desconectado",
            "transporte": getattr(estado, "transporte", settings.transporte),
        },
        "projecao": _resumo_projecao(request),
        "assinantes_sse": _assinantes_sse(request),
    }
    return corpo


def _resumo_projecao(request: Request) -> dict[str, Any]:
    """Le, de forma tolerante, o resumo da projecao de leitos (colaborador opcional do painel).

    Toda leitura e defensiva: ``/health`` e *liveness* e nunca pode falhar por causa da projecao.
    """
    projecao = getattr(request.app.state, "projecao", None)
    if projecao is None:
        return {"montada": False, "hidratada": False}
    resumo: dict[str, Any] = {
        "montada": True,
        "hidratada": bool(getattr(projecao, "hidratada", False)),
    }
    with contextlib.suppress(Exception):
        resumo["leitos"] = len(projecao.listar_leitos())
    with contextlib.suppress(Exception):
        resumo["alertas"] = len(projecao.listar_alertas())
    return resumo


def _assinantes_sse(request: Request) -> int:
    """Conta os assinantes do stream SSE a partir da projecao (que delega ao difusor SSE)."""
    projecao = getattr(request.app.state, "projecao", None)
    if projecao is None:
        return 0
    valor = getattr(projecao, "total_assinantes", 0)
    try:
        return int(valor() if callable(valor) else valor)
    except Exception:  # pragma: no cover - difusor volatil
        return 0


@router.get(
    "/health",
    summary="Liveness: sempre 200, mesmo com o Broker fora (R8.5, R8.6)",
    description="Descreve o proprio estado do processo sem E/S ativa. Nunca falha por dependencia.",
)
async def health(request: Request) -> dict[str, Any]:
    """Devolve o estado observado do gateway, respondendo ``200`` mesmo com o Broker fora (R8.5)."""
    return _corpo_saude(request)


@router.get(
    "/health/ready",
    summary="Readiness: 200 pronto / 503 nao pronto (R8.6)",
    description="Confirma que o gateway atende trafego util (Broker conectado).",
)
async def health_ready(request: Request) -> Response:
    """Responde ``200`` quando conectado ao Broker; ``503`` em ``problem+json`` caso contrario."""
    estado = request.app.state.saude
    pronto = bool(getattr(estado, "broker_conectado", False)) and not getattr(
        estado, "encerrando", False
    )
    if pronto:
        return JSONResponse(_corpo_saude(request))
    return problem_json(
        request,
        status=503,
        slug="broker-indisponivel",
        title="Servico nao pronto",
        detail="o gateway ainda nao atende trafego util (Broker desconectado)",
        retry_after=5,
    )


@router.get(
    "/metrics",
    summary="Contadores acumulados do processo (R5.5)",
    description="JSON por padrao; `?format=prometheus` devolve `text/plain; version=0.0.4`.",
)
async def metrics(
    request: Request,
    formato: Annotated[Literal["json", "prometheus"], Query(alias="format")] = "json",
) -> Response:
    """Expoe os contadores do gateway em JSON ou Prometheus, com protecao por papel (8.6.2)."""
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


def _autorizar_metrics(request: Request) -> Response | None:
    """Aplica a protecao opcional de ``/metrics`` por papel ``admin``/``auditor`` (design 8.6.2).

    Returns:
        ``None`` quando a credencial e valida e o papel basta; caso contrario um ``problem+json``
        ``401`` (credencial ausente/invalida) ou ``403`` (papel insuficiente).
    """
    try:
        token = extrair_token_bearer(request.headers.get("authorization"))
        identidade = validar_token(token)
        verificar_papel(identidade, *_PAPEIS_METRICS)
        return None
    except AuthError as exc:
        if exc.motivo == "papel_insuficiente":
            return problem_json(
                request,
                status=403,
                slug="papel-insuficiente",
                title="Permissao insuficiente",
                detail=exc.mensagem,
            )
        return problem_json(
            request,
            status=401,
            slug="credencial-invalida",
            title="Credencial ausente ou invalida",
            detail=exc.mensagem,
        )
