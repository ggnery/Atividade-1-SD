"""Traducao de excecoes internas para RFC 7807 ``application/problem+json`` (design 8.4, R8.2).

Toda resposta de erro sai com ``Content-Type: application/problem+json`` e o corpo canonico de 8.4:
``type`` (URI estavel do tipo), ``title`` (legivel, fixo por tipo), ``status`` (igual ao HTTP),
``detail`` (especifico da ocorrencia) e a extensao obrigatoria ``correlation_id`` (R8.2). Extensoes
adicionais: ``errors[]`` (so em 422) e ``retry_after_s`` (em 503 e 504, espelhando ``Retry-After``).

A traducao e feita em **dois niveis**, e a ordem importa (design 8.4.1):

1. Se a excecao for :class:`~hospitalmq.errors.RpcRemoteError`, o status vem de
   :data:`~hospitalmq.errors.MAPA_HTTP` **por codigo** -- codigo desconhecido cai em ``500``, nunca
   em ``200``. E o codigo, e nao a classe, o contrato estavel entre servicos (design 5.7).
2. Para as demais excecoes, vale o mapa **por tipo** (:class:`AuthError`, :class:`RpcTimeoutError`,
   :class:`TransportError`, :class:`TransientError`, :class:`PermanentError`).

Os tratadores padrao do FastAPI para ``HTTPException`` e ``RequestValidationError`` sao
**substituidos**; caso contrario escapariam respostas ``{"detail": ...}``, quebrando R8.2 justamente
nos erros mais frequentes.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from hospitalmq.auth import (
    MOTIVO_CREDENCIAL_AUSENTE,
    MOTIVO_PAPEL_INSUFICIENTE,
)
from hospitalmq.errors import (
    MAPA_HTTP,
    AuthError,
    HospitalMQError,
    PermanentError,
    RpcRemoteError,
    RpcTimeoutError,
    TransientError,
    TransportError,
)
from hospitalmq.logging import correlation_id_atual, get_logger

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from fastapi import FastAPI

__all__ = [
    "BASE_PROBLEMAS",
    "RESPOSTAS_PADRAO",
    "LeitoNaoOcupado",
    "problem_json",
    "registrar_tratadores",
]

_log = get_logger(__name__)

BASE_PROBLEMAS: Final[str] = "https://hospitalmq.g3/problems/"
"""Unica base de URI de ``type`` do projeto (design 8.4). Um ``type`` proprio, nao ``about:blank``,
para que o cliente possa decidir programaticamente o que fazer sem interpretar ``detail``."""

_MEDIA_TYPE: Final[str] = "application/problem+json"
_RETRY_APOS_S: Final[int] = 5
"""``Retry-After`` das respostas ``503`` e ``504`` (design 8.4.2, R8.5)."""


# --------------------------------------------------------------------------- #
# Erro local da borda (design 8.2.5)                                           #
# --------------------------------------------------------------------------- #


class LeitoNaoOcupado(Exception):
    """``POST /sinais`` para leito livre ou desconhecido na projecao ja hidratada (design 8.2.5).

    Vira ``409`` ``leito-nao-ocupado``: uma leitura orfa nao tem contexto clinico, e rejeitar na
    borda e melhor do que publicar e ver o ``vitals-service`` manda-la a DLQ -- o monitor recebe a
    causa imediatamente (R6.1).
    """

    def __init__(self, leito_id: str) -> None:
        """Guarda o codigo do leito para compor o ``detail`` do problem+json."""
        self.leito_id = leito_id
        super().__init__(f"leito {leito_id} sem internacao ativa")


# --------------------------------------------------------------------------- #
# Vocabulario de traducao (design 8.4.1)                                       #
# --------------------------------------------------------------------------- #

# Codigo remoto -> (slug, title). Consultado apos MAPA_HTTP resolver o status por codigo.
SLUG_POR_CODIGO: Final[dict[str, tuple[str, str]]] = {
    "PACIENTE_NAO_ENCONTRADO": ("paciente-nao-encontrado", "Paciente nao encontrado"),
    "INTERNACAO_NAO_ENCONTRADA": ("internacao-nao-encontrada", "Internacao nao encontrada"),
    "LEITO_NAO_ENCONTRADO": ("leito-nao-encontrado", "Leito nao encontrado"),
    "LEITO_OCUPADO": ("leito-ocupado", "Leito ja ocupado"),
    "PACIENTE_JA_INTERNADO": ("paciente-ja-internado", "Paciente ja internado"),
    "INTERNACAO_JA_ENCERRADA": ("internacao-ja-encerrada", "Internacao ja encerrada"),
    "NAO_AUTORIZADO": ("papel-insuficiente", "Permissao insuficiente"),
    "PAYLOAD_INVALIDO": ("corpo-invalido", "Corpo da requisicao invalido"),
    "OPERACAO_DESCONHECIDA": ("operacao-desconhecida", "Operacao nao implementada"),
    "BANCO_INDISPONIVEL": ("broker-indisponivel", "Servico temporariamente indisponivel"),
    "ENVELOPE_INVALIDO": ("erro-interno", "Erro interno"),
    "ERRO_INTERNO": ("erro-interno", "Erro interno"),
}

# Status HTTP -> (slug, title) para ``HTTPException`` (rota inexistente, metodo nao permitido...).
_SLUG_POR_STATUS: Final[dict[int, tuple[str, str]]] = {
    404: ("recurso-nao-encontrado", "Recurso nao encontrado"),
    405: ("metodo-nao-permitido", "Metodo nao permitido"),
    401: ("credencial-invalida", "Credencial invalida"),
    403: ("papel-insuficiente", "Permissao insuficiente"),
    422: ("corpo-invalido", "Corpo da requisicao invalido"),
    503: ("broker-indisponivel", "Servico temporariamente indisponivel"),
}


# --------------------------------------------------------------------------- #
# Construtor do problem+json                                                    #
# --------------------------------------------------------------------------- #


def problem_json(
    request: Request | None,
    *,
    status: int,
    slug: str,
    title: str,
    detail: str,
    errors: list[dict[str, Any]] | None = None,
    retry_after: int | None = None,
) -> JSONResponse:
    """Monta a resposta ``application/problem+json`` do erro (design 8.4).

    Args:
        request: A requisicao, usada apenas para o log (o corpo nao expoe o caminho).
        status: Codigo HTTP, ecoado em ``status``.
        slug: Sufixo do ``type`` -- vira ``BASE_PROBLEMAS + slug``.
        title: Titulo curto e estavel do tipo de erro.
        detail: Descricao legivel especifica desta ocorrencia.
        errors: Lista ``[{campo, mensagem, valor_recebido}]`` incluida so em ``422`` (design 8.4).
        retry_after: Segundos do cabecalho ``Retry-After`` e da extensao ``retry_after_s``.

    Returns:
        A :class:`JSONResponse` com o corpo RFC 7807, o ``correlation_id`` corrente e os cabecalhos
        ``X-Correlation-ID``, ``Retry-After`` (quando cabe) e ``WWW-Authenticate`` (nos ``401``).
    """
    correlacao = correlation_id_atual()
    corpo: dict[str, Any] = {
        "type": f"{BASE_PROBLEMAS}{slug}",
        "title": title,
        "status": status,
        "detail": detail,
    }
    if correlacao is not None:
        corpo["correlation_id"] = correlacao
    if errors is not None:
        corpo["errors"] = errors
    if retry_after is not None:
        corpo["retry_after_s"] = retry_after

    cabecalhos: dict[str, str] = {}
    if correlacao is not None:
        cabecalhos["X-Correlation-ID"] = correlacao
    if retry_after is not None:
        cabecalhos["Retry-After"] = str(retry_after)
    if status == 401:
        cabecalhos["WWW-Authenticate"] = "Bearer"

    return JSONResponse(
        corpo, status_code=status, media_type=_MEDIA_TYPE, headers=cabecalhos or None
    )


def _json_seguro(valor: object) -> Any:  # noqa: ANN401 - JSON arbitrario por definicao
    """Projeta um valor recusado numa forma serializavel em JSON (``errors[].valor_recebido``)."""
    if isinstance(valor, Decimal):
        return str(valor)
    if isinstance(valor, datetime):
        return valor.isoformat()
    if isinstance(valor, bytes):
        return valor.decode("utf-8", "replace")
    if valor is None or isinstance(valor, bool | int | float | str):
        return valor
    return str(valor)


# --------------------------------------------------------------------------- #
# Tratadores por tipo de excecao                                               #
# --------------------------------------------------------------------------- #


async def _tratar_rpc_remoto(request: Request, exc: RpcRemoteError) -> JSONResponse:
    """Traduz um erro remoto do ``RpcServer`` pelo **codigo** (design 8.4.1, nivel 1)."""
    status = MAPA_HTTP.get(exc.codigo, 500)
    slug, title = SLUG_POR_CODIGO.get(exc.codigo, ("erro-interno", "Erro interno"))
    retry = _RETRY_APOS_S if status in (503, 504) else None
    detalhe = exc.mensagem if status != 500 else "erro interno no servico de destino"
    _log.warning(
        "http.erro_remoto", codigo=exc.codigo, operacao=exc.operacao, status=status
    )
    return problem_json(
        request, status=status, slug=slug, title=title, detail=detalhe, retry_after=retry
    )


async def _tratar_timeout(request: Request, exc: RpcTimeoutError) -> JSONResponse:
    """``RpcTimeoutError`` -> ``504`` ``tempo-esgotado-no-rpc`` (design 8.4.2, R3.3, R8.4)."""
    sufixo = (
        " A requisicao foi descartada; nenhuma alteracao foi feita."
        if not exc.escrita
        else " O resultado e indeterminado; reapresente com o mesmo X-Message-ID."
    )
    detalhe = f"A operacao '{exc.operacao}' nao respondeu em {exc.timeout}s.{sufixo}"
    return problem_json(
        request,
        status=504,
        slug="tempo-esgotado-no-rpc",
        title="Tempo esgotado na chamada ao servico",
        detail=detalhe,
        retry_after=_RETRY_APOS_S,
    )


async def _tratar_transporte(request: Request, exc: TransportError) -> JSONResponse:
    """``TransportError`` -> ``503`` ``broker-indisponivel`` (design 8.4.1, R1.5, R8.5)."""
    return problem_json(
        request,
        status=503,
        slug="broker-indisponivel",
        title="Servico temporariamente indisponivel",
        detail="O Broker de mensageria esta indisponivel. Tente novamente em instantes.",
        retry_after=_RETRY_APOS_S,
    )


async def _tratar_transitorio(request: Request, exc: TransientError) -> JSONResponse:
    """``TransientError`` -> ``503`` ``broker-indisponivel`` (design 8.4.1)."""
    return problem_json(
        request,
        status=503,
        slug="broker-indisponivel",
        title="Servico temporariamente indisponivel",
        detail=exc.mensagem or "falha transitoria de uma dependencia",
        retry_after=_RETRY_APOS_S,
    )


async def _tratar_auth(request: Request, exc: AuthError) -> JSONResponse:
    """``AuthError`` -> ``401``/``403`` conforme o ``motivo`` (design 8.4.1, R4.1, R4.2, R4.4)."""
    if exc.motivo == MOTIVO_PAPEL_INSUFICIENTE:
        return problem_json(
            request,
            status=403,
            slug="papel-insuficiente",
            title="Permissao insuficiente",
            detail=exc.mensagem,
        )
    if exc.motivo == MOTIVO_CREDENCIAL_AUSENTE:
        return problem_json(
            request,
            status=401,
            slug="sem-credencial",
            title="Credencial ausente",
            detail=exc.mensagem,
        )
    return problem_json(
        request,
        status=401,
        slug="credencial-invalida",
        title="Credencial invalida",
        detail=exc.mensagem,
    )


async def _tratar_permanente(request: Request, exc: PermanentError) -> JSONResponse:
    """``PermanentError`` -> ``422`` ``regra-de-dominio-violada`` (design 8.4.1, R2.3)."""
    return problem_json(
        request,
        status=422,
        slug="regra-de-dominio-violada",
        title="Requisicao rejeitada por regra de dominio",
        detail=exc.mensagem or "requisicao viola uma regra de dominio",
    )


async def _tratar_leito_nao_ocupado(request: Request, exc: LeitoNaoOcupado) -> JSONResponse:
    """``LeitoNaoOcupado`` -> ``409`` ``leito-nao-ocupado`` (design 8.2.5, R6.1)."""
    return problem_json(
        request,
        status=409,
        slug="leito-nao-ocupado",
        title="Leito sem internacao ativa",
        detail=(
            f"O leito {exc.leito_id} nao possui internacao ativa na projecao. "
            "Envie internacao_id explicito ou admita um paciente no leito primeiro."
        ),
    )


async def _tratar_hospitalmq(request: Request, exc: HospitalMQError) -> JSONResponse:
    """Rede de seguranca para qualquer ``HospitalMQError`` nao coberta acima -> ``500``."""
    _log.error("http.erro_middleware", tipo=type(exc).__name__, codigo=exc.codigo)
    return problem_json(
        request,
        status=500,
        slug="erro-interno",
        title="Erro interno",
        detail="erro interno ao processar a requisicao",
    )


async def _tratar_validacao(request: Request, exc: RequestValidationError) -> JSONResponse:
    """``RequestValidationError`` -> ``422`` ``corpo-invalido`` com ``errors[]`` (R8.3)."""
    errors = [
        {
            "campo": ".".join(str(parte) for parte in erro.get("loc", ())) or "corpo",
            "mensagem": erro.get("msg", "valor invalido"),
            "valor_recebido": _json_seguro(erro.get("input")),
        }
        for erro in exc.errors()
    ]
    return problem_json(
        request,
        status=422,
        slug="corpo-invalido",
        title="Corpo da requisicao invalido",
        detail="um ou mais campos do corpo violam o schema",
        errors=errors,
    )


async def _tratar_http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """``HTTPException`` (404, 405, ...) reembrulhado em problem+json (design 8.4.1)."""
    slug, title = _SLUG_POR_STATUS.get(exc.status_code, ("erro-interno", "Erro interno"))
    detalhe = str(exc.detail) if exc.detail else title
    retry = _RETRY_APOS_S if exc.status_code in (503, 504) else None
    return problem_json(
        request,
        status=exc.status_code,
        slug=slug,
        title=title,
        detail=detalhe,
        retry_after=retry,
    )


async def _tratar_generico(request: Request, exc: Exception) -> JSONResponse:
    """Qualquer excecao nao prevista -> ``500`` ``erro-interno``, sem *stack trace* (R8.2)."""
    _log.exception("http.erro_nao_tratado", tipo=type(exc).__name__)
    return problem_json(
        request,
        status=500,
        slug="erro-interno",
        title="Erro interno",
        detail="erro interno inesperado; use o correlation_id para localizar o log",
    )


# --------------------------------------------------------------------------- #
# Registro no aplicativo e respostas padrao do OpenAPI                          #
# --------------------------------------------------------------------------- #

# Respostas de erro anexadas a toda rota protegida no OpenAPI (design 8.5).
RESPOSTAS_PADRAO: Final[dict[int | str, dict[str, Any]]] = {
    401: {"description": "Credencial ausente ou invalida"},
    403: {"description": "Papel sem permissao para a operacao"},
    500: {"description": "Erro interno"},
    503: {"description": "Broker indisponivel"},
    504: {"description": "Tempo esgotado na chamada ao servico"},
}


def registrar_tratadores(app: FastAPI) -> None:
    """Registra todos os tratadores de excecao do gateway (design 8.4.1).

    Os tratadores por classe especifica rodam no ``ExceptionMiddleware`` (dentro dos middlewares de
    borda), de modo que o ``correlation_id`` ja esteja no contexto e o ``X-Correlation-ID``
    seja adicionado a resposta. O tratador de ``Exception`` cobre o que escapar a todos os demais.

    Args:
        app: O aplicativo FastAPI onde instalar os tratadores.
    """
    app.add_exception_handler(RpcRemoteError, _tratar_rpc_remoto)  # type: ignore[arg-type]
    app.add_exception_handler(RpcTimeoutError, _tratar_timeout)  # type: ignore[arg-type]
    app.add_exception_handler(TransportError, _tratar_transporte)  # type: ignore[arg-type]
    app.add_exception_handler(TransientError, _tratar_transitorio)  # type: ignore[arg-type]
    app.add_exception_handler(AuthError, _tratar_auth)  # type: ignore[arg-type]
    app.add_exception_handler(LeitoNaoOcupado, _tratar_leito_nao_ocupado)  # type: ignore[arg-type]
    app.add_exception_handler(PermanentError, _tratar_permanente)  # type: ignore[arg-type]
    app.add_exception_handler(HospitalMQError, _tratar_hospitalmq)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, _tratar_validacao)  # type: ignore[arg-type]
    app.add_exception_handler(StarletteHTTPException, _tratar_http)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, _tratar_generico)
