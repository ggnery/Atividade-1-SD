"""Dependencias FastAPI de autenticacao, autorizacao e injecao de colaboradores (design 8.3, R4).

Este modulo e a **borda de seguranca** do gateway. Ele nao contem criptografia: delega inteiramente
a ``hospitalmq.auth`` (JWT HS256 e API Key), que roda igual dentro de um teste sem servidor HTTP. O
papel deste arquivo e tres coisas:

1. Extrair a credencial dos cabecalhos (``Authorization: Bearer`` para pessoas, ``X-API-Key`` para
   dispositivos, cookie ``hmq_session`` como alternativa do painel) e produzir a :class:`Identity`.
2. Verificar o papel exigido por cada endpoint e, quando ele falta, **publicar ``acesso.negado`` na
   Trilha_de_Auditoria antes de devolver o ``403``** (design 6.4.1, R4.4).
3. Entregar as rotas o :class:`RpcClient`, o :class:`Publisher` e a projecao injetados na
   montagem -- **nunca** uma sessao de banco (R7.2, design 8.1.1).

Os esquemas de seguranca sao declarados com as utilidades do FastAPI (``HTTPBearer``,
``APIKeyHeader``, ``APIKeyCookie``) para que aparecam no ``securitySchemes`` do OpenAPI e no botao
*Authorize* do Swagger (design 8.5). ``auto_error=False`` em todos: quem formata a recusa e
o tradutor RFC 7807 de :mod:`erros`, nao o FastAPI.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fastapi import Depends, Request
from fastapi.security import APIKeyCookie, APIKeyHeader, HTTPBearer
from fastapi.security.http import HTTPAuthorizationCredentials

from hospitalmq.auth import (
    MOTIVO_CREDENCIAL_AUSENTE,
    MOTIVO_PAPEL_INSUFICIENTE,
    Autenticador,
    Identity,
)
from hospitalmq.errors import AuthError, TransportError
from hospitalmq.logging import correlation_id_atual, get_logger

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from collections.abc import Awaitable, Callable

    from hospitalmq.publisher import Publisher
    from hospitalmq.rpc import RpcClient

__all__ = [
    "COOKIE_SESSAO",
    "autenticar_dispositivo",
    "autenticar_usuario",
    "esquema_api_key",
    "esquema_bearer",
    "esquema_cookie",
    "exigir_papel",
    "obter_autenticador",
    "obter_projecao",
    "obter_publisher",
    "obter_rpc",
]

_log = get_logger(__name__)

COOKIE_SESSAO = "hmq_session"
"""Nome do cookie que transporta o JWT para o ``EventSource`` do painel (design 8.7)."""

# Esquemas de seguranca -- existem para popular o OpenAPI/Swagger (design 8.5). auto_error=False
# porque a recusa e formatada em RFC 7807 por :mod:`erros`, e nao pelo default do FastAPI.
esquema_bearer = HTTPBearer(
    scheme_name="BearerJWT",
    description="JWT HS256 emitido por POST /auth/token. Cole apenas o token.",
    auto_error=False,
)
esquema_api_key = APIKeyHeader(
    name="X-API-Key",
    scheme_name="ApiKeyDispositivo",
    description="Chave longeva de um monitor de beira-leito (R4.5).",
    auto_error=False,
)
esquema_cookie = APIKeyCookie(
    name=COOKIE_SESSAO,
    scheme_name="CookieSessao",
    description="Cookie de sessao usado exclusivamente pelo stream do painel.",
    auto_error=False,
)


# --------------------------------------------------------------------------- #
# Acesso aos colaboradores injetados no composition root (design 8.1.1)         #
# --------------------------------------------------------------------------- #


def obter_autenticador(request: Request) -> Autenticador:
    """Devolve o :class:`Autenticador` do processo, construido no *composition root*."""
    return request.app.state.autenticador


def obter_rpc(request: Request) -> RpcClient:
    """Devolve o :class:`RpcClient` injetado, ou ``503`` honesto se o Broker esta fora (R8.5).

    Quando a conexao com o Broker ainda nao subiu, o ``RpcClient`` nao tem fila de retorno e uma
    chamada falharia com ``RuntimeError`` -- que viraria ``500``. Recusar aqui com
    :class:`~hospitalmq.errors.TransportError` traduz a indisponibilidade no ``503`` que R8.5 pede.

    Raises:
        TransportError: Se o gateway ainda nao esta conectado ao Broker.
    """
    estado = request.app.state.saude
    if not estado.broker_conectado:
        raise TransportError(
            "gateway sem conexao com o Broker; chamada RPC indisponivel",
            motivo="broker_desconectado",
        )
    return request.app.state.rpc


def obter_publisher(request: Request) -> Publisher:
    """Devolve o :class:`Publisher` injetado no *composition root*."""
    return request.app.state.publisher


def obter_projecao(request: Request) -> object | None:
    """Devolve a projecao de leitos em memoria, ou ``None`` se o painel nao foi montado.

    A projecao e um colaborador do agente do painel; o nucleo HTTP a trata como opcional para poder
    subir e servir a API mesmo antes de o painel existir. A resolucao de ``internacao_id`` de 8.2.5
    lida com o caso ``None`` devolvendo ``503``/``409`` conforme a situacao.
    """
    return getattr(request.app.state, "projecao", None)


# --------------------------------------------------------------------------- #
# Autenticacao (design 8.3.1)                                                   #
# --------------------------------------------------------------------------- #


async def autenticar_usuario(
    request: Request,
    credencial: HTTPAuthorizationCredentials | None = Depends(esquema_bearer),
    cookie: str | None = Depends(esquema_cookie),
    auth: Autenticador = Depends(obter_autenticador),
) -> Identity:
    """Autentica uma pessoa por JWT ``Bearer`` (ou pelo cookie de sessao do painel) (R4.1, R4.2).

    Args:
        request: A requisicao corrente.
        credencial: Credencial ``Bearer`` extraida pelo esquema de seguranca, se presente.
        cookie: Valor do cookie ``hmq_session``, alternativa usada pelo stream do painel.
        auth: O autenticador do processo.

    Returns:
        A :class:`Identity` da pessoa, com ``tipo="usuario"``.

    Raises:
        AuthError: ``credencial_ausente`` se nao houver token; ``expirado`` ou
            ``assinatura_invalida`` se o token for invalido -- ``401`` por :mod:`erros`.
    """
    token = credencial.credentials if credencial is not None else cookie
    if token is None:
        raise AuthError("credencial ausente", motivo=MOTIVO_CREDENCIAL_AUSENTE, esquema="Bearer")
    return auth.validar_token(token)


async def autenticar_dispositivo(
    request: Request,
    chave: str | None = Depends(esquema_api_key),
    auth: Autenticador = Depends(obter_autenticador),
) -> Identity:
    """Autentica um dispositivo pela ``X-API-Key`` (R4.5).

    Args:
        request: A requisicao corrente.
        chave: Valor do cabecalho ``X-API-Key``, se presente.
        auth: O autenticador do processo.

    Returns:
        A :class:`Identity` do dispositivo, com ``role="dispositivo"`` e ``tipo="dispositivo"``.

    Raises:
        AuthError: ``credencial_ausente`` ou ``chave_desconhecida`` -- ``401`` por :mod:`erros`.
    """
    return auth.validar_api_key(chave)


# --------------------------------------------------------------------------- #
# Autorizacao com auditoria da negativa (design 6.4.1, R4.4)                    #
# --------------------------------------------------------------------------- #


def exigir_papel(*papeis: str) -> Callable[..., Awaitable[Identity]]:
    """Fabrica a dependencia que exige um dos ``papeis`` do usuario autenticado (R4.4).

    Quando o papel do chamador nao esta entre os aceitos, a dependencia **publica ``acesso.negado``
    na Trilha_de_Auditoria e so entao levanta** o :class:`AuthError` que vira ``403`` -- a ordem
    importa (design 6.4.1, ponto 4): responder primeiro abriria a janela em que o cliente recebe a
    negativa e a trilha nunca a recebe. Se a publicacao falhar, vale a regra da borda -- a
    :class:`~hospitalmq.errors.TransportError` vira ``503``, coerente com "operar sem trilha
    e pior do que nao operar" (design 6.9.3).

    Args:
        *papeis: Papeis aceitos pelo endpoint, ex. ``exigir_papel("enfermeiro", "admin")``.

    Returns:
        A corrotina de dependencia que devolve a :class:`Identity` autorizada.
    """

    async def _verificar(
        request: Request,
        identidade: Identity = Depends(autenticar_usuario),
        publisher: Publisher = Depends(obter_publisher),
    ) -> Identity:
        if identidade.role in papeis:
            return identidade
        await _auditar_negativa(publisher, request, identidade, papeis)
        raise AuthError(
            f"papel {identidade.role!r} nao autorizado para esta operacao",
            motivo=MOTIVO_PAPEL_INSUFICIENTE,
            papel=identidade.role,
            exigidos=sorted(papeis),
            identity_sub=identidade.sub,
        )

    return _verificar


async def _auditar_negativa(
    publisher: Publisher,
    request: Request,
    identidade: Identity,
    papeis: tuple[str, ...],
) -> None:
    """Publica o evento ``acesso.negado`` com a identidade apresentada (design 6.4.1, R4.4).

    O evento carrega rota, metodo, papel_apresentado, papeis_exigidos e negado_em;
    a ``identity`` do Envelope e a do token valido (o papel e que nao basta). A publicacao usa
    *publisher confirms*: uma :class:`~hospitalmq.errors.TransportError` propaga e a borda traduz em
    ``503``.
    """
    _log.warning(
        "auth.negada",
        rota=request.url.path,
        metodo=request.method,
        papel_apresentado=identidade.role,
        papeis_exigidos=sorted(papeis),
    )
    await publisher.publish(
        "acesso.negado",
        {
            "rota": request.url.path,
            "metodo": request.method,
            "papel_apresentado": identidade.role,
            "papeis_exigidos": sorted(papeis),
            "negado_em": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        },
        identity=identidade,
        correlation_id=correlation_id_atual(),
    )
