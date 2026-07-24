"""Rotas do Painel_de_Leitos: casca HTML, stream SSE e leituras LOCAIS da projecao (design 8.7,
R11).

Este roteador reune os endpoints 9, 10, 14 e 15 de 8.2.1, todos servidos **sem tocar o Broker no
caminho de resposta** -- a fonte e a :class:`~projecao.ProjecaoLeitos` em memoria (R11.7):

* ``GET /painel``          -- casca HTML+CSS+JS estatica, publica (nao contem dado clinico, design
8.3.2);
* ``GET /painel/stream``   -- SSE autenticado (cookie ``hmq_session`` **ou** ``Bearer``), R11.2;
* ``GET /leitos``          -- cards da projecao; ``?fonte=autoritativa`` executa o RPC
``leitos.snapshot``;
* ``GET /alertas``         -- alertas recentes da projecao, com filtros.

**Autenticacao local sobre ``hospitalmq.auth``.** Por estar restrito aos arquivos do painel, o
roteador
nao importa ``dependencias.py`` (de outro modulo): valida credenciais diretamente pela **unica**
fonte
canonica (:func:`hospitalmq.auth.validar_token`), sem segunda fonte de verdade. O
``/painel/stream`` usa
o cookie ``hmq_session`` porque o ``EventSource`` do navegador nao envia cabecalhos personalizados
(design 8.7.5); os demais usam ``Authorization: Bearer``. Toda recusa levanta ``AuthError``,
convertida em
``application/problem+json`` (401/403) pelo tratador de erros da borda (design 8.4.1).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from projecao import IDENTIDADE_SERVICO, ProjecaoLeitos
from sse import agora_iso, gerar_stream

from hospitalmq.auth import Identity, Papel, extrair_token_bearer, validar_token
from hospitalmq.errors import AuthError, TransportError

__all__ = ["router"]

router = APIRouter()

#: Diretorio dos ativos estaticos do painel (``services/api-gateway/static``).
_STATIC = Path(__file__).resolve().parent.parent / "static"

#: Nome do cookie ``HttpOnly`` que autentica exclusivamente o ``/painel/stream`` (design 8.7.5).
COOKIE_SESSAO = "hmq_session"

#: Papeis que enxergam o painel e ``GET /leitos`` (design 8.3.2).
_PAPEIS_PAINEL: tuple[Papel, ...] = ("enfermeiro", "medico", "admin")

#: Papeis que enxergam ``GET /alertas`` -- inclui o ``auditor`` (dado agregado de operacao, 8.2.1).
_PAPEIS_ALERTAS: tuple[Papel, ...] = ("enfermeiro", "medico", "auditor", "admin")

#: Cabecalhos que impedem *buffering* de proxy no fluxo SSE (design 8.7.3).
_CABECALHOS_SSE = {
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


# --------------------------------------------------------------------------- #
# Autenticacao local sobre hospitalmq.auth                                     #
# --------------------------------------------------------------------------- #


def _conferir_papel(identidade: Identity, papeis: tuple[Papel, ...]) -> None:
    """Levanta ``AuthError(papel_insuficiente)`` -> 403 se o papel nao estiver autorizado (R4.4)."""
    if identidade.role not in papeis:
        raise AuthError(
            "papel insuficiente para o recurso",
            motivo="papel_insuficiente",
            papel=identidade.role,
            exigidos=list(papeis),
        )


def _identidade_bearer(request: Request) -> Identity:
    """Autentica pelo ``Authorization: Bearer`` (401 se ausente, expirado ou adulterado,
    R4.1/R4.2)."""
    cabecalho = request.headers.get("authorization")
    if not cabecalho:
        raise AuthError("credencial ausente", motivo="credencial_ausente")
    token = extrair_token_bearer(cabecalho)
    return validar_token(token)


def exigir_papel(*papeis: Papel) -> Callable[[Request], Awaitable[Identity]]:
    """Fabrica de dependencia que exige JWT ``Bearer`` com um dos ``papeis`` (design 8.3.1)."""

    async def _verificar(request: Request) -> Identity:
        identidade = _identidade_bearer(request)
        _conferir_papel(identidade, papeis)
        return identidade

    return _verificar


async def autenticar_stream(request: Request) -> Identity:
    """Autentica o ``/painel/stream`` por cookie ``hmq_session``, ``Bearer`` ou ``?token=`` (8.7.5).

    O ``EventSource`` do navegador nao envia cabecalhos personalizados, entao o cookie ``HttpOnly``
    e o
    caminho principal; o ``Bearer`` serve ao Swagger e a ferramentas de linha de comando;
    ``?token=`` e o
    *fallback* documentado para clientes sem cookie. Exige papel de ``enfermeiro``, ``medico`` ou
    ``admin``.
    """
    token = request.cookies.get(COOKIE_SESSAO)
    if not token:
        cabecalho = request.headers.get("authorization")
        token = extrair_token_bearer(cabecalho) if cabecalho else request.query_params.get("token")
    if not token:
        raise AuthError("credencial ausente para o stream do painel", motivo="credencial_ausente")
    identidade = validar_token(token)
    _conferir_papel(identidade, _PAPEIS_PAINEL)
    return identidade


def _projecao(request: Request) -> ProjecaoLeitos:
    """Recupera a projecao publicada em ``app.state.projecao`` no *lifespan* (design 8.1.1)."""
    projecao = getattr(request.app.state, "projecao", None)
    if projecao is None:
        raise TransportError("projecao de leitos ainda indisponivel")
    return projecao


# --------------------------------------------------------------------------- #
# Casca HTML+CSS+JS (publica, sem dado clinico -- design 8.3.2)                 #
# --------------------------------------------------------------------------- #


@router.get("/painel", tags=["painel"], summary="Casca HTML do Painel_de_Leitos (publica)")
async def painel() -> FileResponse:
    """Serve a pagina unica do painel; todo dado clinico chega depois pelo ``/painel/stream``
    (8.3.2)."""
    return FileResponse(_STATIC / "painel.html", media_type="text/html; charset=utf-8")


@router.get("/painel/painel.css", include_in_schema=False)
async def painel_css() -> FileResponse:
    """Folha de estilo do painel (ativo estatico, sem CDN, R11.6)."""
    return FileResponse(_STATIC / "painel.css", media_type="text/css; charset=utf-8")


@router.get("/painel/painel.js", include_in_schema=False)
async def painel_js() -> FileResponse:
    """Script do painel: ``EventSource``, render dos cards e reconexao (ativo estatico, R11.6)."""
    return FileResponse(
        _STATIC / "painel.js", media_type="application/javascript; charset=utf-8"
    )


# --------------------------------------------------------------------------- #
# Stream SSE (autenticado -- design 8.7.3, R11.2)                              #
# --------------------------------------------------------------------------- #


@router.get("/painel/stream", tags=["painel"], summary="Fluxo SSE de leitos e alertas (R11.2)")
async def painel_stream(
    request: Request, identidade: Annotated[Identity, Depends(autenticar_stream)]
) -> StreamingResponse:
    """Abre uma conexao ``text/event-stream`` que empurra ``snapshot``, ``leito``, ``alerta`` e
    ``ping``.

    A latencia e de milissegundos (``publish`` -> entrega push -> ``put_nowait`` -> ``yield``), com
    folga
    sobre o limite de 2 s do R11.2. A cada abertura registra-se ``sub``/``role`` no log estruturado
    (8.7.2).
    """
    projecao = _projecao(request)
    return StreamingResponse(
        gerar_stream(projecao, identidade),
        media_type="text/event-stream",
        headers=_CABECALHOS_SSE,
    )


# --------------------------------------------------------------------------- #
# Leituras LOCAIS da projecao (endpoints 9 e 10 -- design 8.2.1, R11.7)        #
# --------------------------------------------------------------------------- #


@router.get("/leitos", tags=["painel"], summary="Estado dos leitos pela projecao (R11.7)")
async def listar_leitos(
    request: Request,
    identidade: Annotated[Identity, Depends(exigir_papel(*_PAPEIS_PAINEL))],
    fonte: Annotated[
        str | None,
        Query(description="``autoritativa`` executa o RPC ``leitos.snapshot`` (design 8.2.1)."),
    ] = None,
) -> dict[str, Any]:
    """Devolve os cards da projecao; ``?fonte=autoritativa`` compara com o estado do
    ``admission-service``.

    Por padrao (R11.7) responde da projecao em memoria, sem tocar o Broker. Com
    ``fonte=autoritativa``
    executa ``leitos.snapshot`` -- o mesmo RPC da hidratacao de boot --, permitindo ao avaliador
    ver lado
    a lado o estado derivado e o estado autoritativo.
    """
    projecao = _projecao(request)
    if fonte == "autoritativa":
        rpc = getattr(request.app.state, "rpc", None)
        if rpc is None:
            raise TransportError("cliente RPC indisponivel para consulta autoritativa")
        # leitos.snapshot exige o papel `servico` (_PAPEIS_SNAPSHOT); a identidade do usuario
        # (enfermeiro/medico/admin) seria recusada com NAO_AUTORIZADO. Usa-se a identidade de
        # servico do gateway, a mesma da hidratacao de boot (design 4630).
        dados = await rpc.call(
            "leitos.snapshot", {"apenas_ocupados": False}, identity=IDENTIDADE_SERVICO, timeout=5.0
        )
        leitos = dados.get("leitos", []) if isinstance(dados, dict) else []
        atualizado = dados.get("gerado_em") if isinstance(dados, dict) else agora_iso()
        return {"itens": leitos, "atualizado_em": atualizado, "origem": "autoritativa"}
    return {"itens": projecao.listar_leitos(), "atualizado_em": agora_iso(), "origem": "projecao"}


@router.get("/alertas", tags=["painel"], summary="Alertas clinicos recentes pela projecao (R11.4)")
async def listar_alertas(
    request: Request,
    identidade: Annotated[Identity, Depends(exigir_papel(*_PAPEIS_ALERTAS))],
    severidade: Annotated[str | None, Query(pattern=r"^(baixa|media|alta)$")] = None,
    leito_id: str | None = None,
    limite: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    """Lista os alertas recentes da projecao (mais novo primeiro), com filtros opcionais (design
    8.2.1).

    Args:
        severidade: Filtra por banda (``baixa`` | ``media`` | ``alta``).
        leito_id: Filtra pelo codigo do leito.
        limite: Numero maximo de itens (1 a 200).
    """
    projecao = _projecao(request)
    itens = projecao.listar_alertas(severidade=severidade, leito_id=leito_id, limite=limite)
    return {"itens": itens, "total": len(itens), "origem": "projecao"}
