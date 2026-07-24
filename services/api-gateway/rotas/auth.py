"""Emissao de token JWT: ``POST /auth/token`` (design 8.2.1 endpoint 1, 8.3.1).

Autentica uma pessoa contra a **tabela de usuarios de demonstracao** e devolve um JWT HS256 no corpo
(para a API e o Swagger) **e** grava um cookie ``hmq_session`` usado exclusivamente pelo stream do
painel -- ``EventSource`` nao permite cabecalho ``Authorization`` (design 8.7).

**Fronteira de dados (R7.2, design 8.1.1).** O gateway nao abre sessao no banco clinico; os usuarios
de demonstracao vivem em memoria, neste modulo. As senhas sao ficticias (C6) e servem so a
demonstracao. Comparar com :func:`hmac.compare_digest` evita que o tempo de resposta revele o quanto
do usuario/senha acertou.
"""

from __future__ import annotations

import hmac
from typing import Final

from dependencias import COOKIE_SESSAO, obter_autenticador
from fastapi import APIRouter, Depends, Response
from schemas import CredenciaisRequest, ProblemDetails, TokenResponse

from hospitalmq.auth import MOTIVO_ASSINATURA_INVALIDA, Autenticador
from hospitalmq.errors import AuthError

__all__ = ["USUARIOS_DEMO", "router"]

router = APIRouter(tags=["autenticacao"])

# Tabela de usuarios de demonstracao (design 8.3.1). Ficticios (C6): usuario -> (senha, papel).
# Cobre um portador de cada papel de usuario para o roteiro do Swagger (design 8.5). O papel
# ``dispositivo`` NAO e emissivel por JWT (R4.5) -- monitores usam X-API-Key.
USUARIOS_DEMO: Final[dict[str, tuple[str, str]]] = {
    "enf.ana": ("demo123", "enfermeiro"),
    "enf.bia": ("demo123", "enfermeiro"),
    "med.silva": ("demo123", "medico"),
    "med.joao": ("demo123", "medico"),
    "aud.paula": ("demo123", "auditor"),
    "admin": ("demo123", "admin"),
}


@router.post(
    "/auth/token",
    response_model=TokenResponse,
    status_code=200,
    responses={
        401: {"model": ProblemDetails, "description": "Usuario ou senha invalidos"},
        422: {"model": ProblemDetails, "description": "Corpo invalido"},
    },
    operation_id="emitir_token",
    summary="Autentica uma pessoa e emite um JWT (login de demonstracao)",
    description=(
        "Valida usuario e senha contra a tabela de demonstracao e devolve um JWT HS256 (R4.1). "
        "Grava tambem o cookie `hmq_session` usado pelo stream do painel. Credenciais ficticias, "
        "ex.: `{\"usuario\": \"enf.ana\", \"senha\": \"demo123\"}`."
    ),
)
async def emitir_token(
    credenciais: CredenciaisRequest,
    resposta: Response,
    auth: Autenticador = Depends(obter_autenticador),
) -> TokenResponse:
    """Emite um JWT para o usuario de demonstracao autenticado (R4.1).

    Args:
        credenciais: Usuario e senha do corpo da requisicao.
        resposta: Resposta HTTP onde gravar o cookie de sessao do painel.
        auth: O autenticador do processo, que assina o token.

    Returns:
        O :class:`TokenResponse` com ``access_token``, ``token_type``, ``expires_in`` e ``role``.

    Raises:
        AuthError: ``assinatura_invalida`` quando usuario ou senha nao conferem -- vira ``401``.
    """
    registro = USUARIOS_DEMO.get(credenciais.usuario)
    senha_ok = registro is not None and hmac.compare_digest(registro[0], credenciais.senha)
    if registro is None or not senha_ok:
        # Mesma recusa para usuario inexistente e senha errada: nao revela quais usuarios existem.
        raise AuthError(
            "usuario ou senha invalidos",
            motivo=MOTIVO_ASSINATURA_INVALIDA,
            causa="credenciais_demo_invalidas",
        )

    _, papel = registro
    token = auth.emitir_token(credenciais.usuario, papel)  # type: ignore[arg-type]
    resposta.set_cookie(
        key=COOKIE_SESSAO,
        value=token,
        httponly=True,
        samesite="strict",
        path="/painel",
        max_age=auth.ttl_padrao_s,
    )
    return TokenResponse(access_token=token, expires_in=auth.ttl_padrao_s, role=papel)
