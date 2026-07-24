"""Rotas de internacao: admissao e alta (design 8.2.1 endpoints 3 e 4, 8.2.4, ADR-019).

Admitir e dar alta sao **RPC sobre fila** (ADR-019): a resposta importa na hora -- qual Internacao
foi criada, em qual Leito, e se houve conflito (``409``) -- e o usuario precisa conhecer
isso no mesmo ciclo HTTP. O evento (``paciente.admitido``, ``leito.ocupado``) continua sendo emitido
pelo ``admission-service`` via *outbox*; o RPC e o *ack* sincrono, o evento e o fato (R1.3, design
8.2.4).

Apos a admissao, o gateway aplica a resposta na projecao **antes** de o evento chegar (aplicacao
otimista de 8.2.4), fechando a janela em que um ``POST /sinais`` imediato veria ``409`` por leito
"livre". A projecao e opcional (colaborador do painel): quando ausente, a aplicacao e ignorada.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from dependencias import exigir_papel, obter_projecao, obter_rpc
from erros import RESPOSTAS_PADRAO
from fastapi import APIRouter, Depends, Header, Response
from schemas import AltaRequest, AltaResponse, InternacaoRequest, InternacaoResponse, ProblemDetails

from hospitalmq.auth import Identity
from hospitalmq.logging import get_logger

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from hospitalmq.rpc import RpcClient

__all__ = ["router"]

_log = get_logger(__name__)

router = APIRouter(tags=["pacientes"])


@router.post(
    "/internacoes",
    status_code=201,
    response_model=InternacaoResponse,
    responses={
        **RESPOSTAS_PADRAO,
        404: {"model": ProblemDetails, "description": "Paciente ou leito nao encontrado"},
        409: {"model": ProblemDetails, "description": "Leito ocupado ou paciente ja internado"},
        422: {"model": ProblemDetails, "description": "Corpo invalido"},
    },
    operation_id="admitir_paciente",
    summary="Admite um paciente em um leito (sincrono, RPC sobre fila)",
    description=(
        "Admite via RPC `paciente.admitir`. Conflitos de ocupacao viram `409` -- `LEITO_OCUPADO` "
        "(INV-1) ou `PACIENTE_JA_INTERNADO` (INV-6). Devolve `201` + `Location` e aplica o card "
        "ocupado na projecao imediatamente (design 8.2.4). Idempotente por `X-Message-ID` (R2.4)."
    ),
)
async def admitir_paciente(
    corpo: InternacaoRequest,
    resposta: Response,
    identidade: Identity = Depends(exigir_papel("enfermeiro", "admin")),
    rpc: RpcClient = Depends(obter_rpc),
    projecao: object | None = Depends(obter_projecao),
    x_message_id: str | None = Header(default=None, alias="X-Message-ID"),
) -> InternacaoResponse:
    """Admite um paciente e devolve ``201`` com ``Location`` (R3.1, R3.4).

    Args:
        corpo: Dados da admissao ja validados.
        resposta: Resposta HTTP, onde o ``Location`` e escrito.
        identidade: Identidade autorizada (``enfermeiro`` ou ``admin``), propagada ao Envelope.
        rpc: Cliente RPC injetado.
        projecao: Projecao de leitos em memoria (opcional), atualizada de forma otimista.
        x_message_id: ``message_id`` opcional para reapresentacao idempotente da escrita.

    Returns:
        O :class:`InternacaoResponse` com ``leito_id`` no formato do codigo humano (ex. ``UTI-03``).
    """
    dados = await rpc.call(
        "paciente.admitir",
        corpo.model_dump(mode="json"),
        identity=identidade,
        message_id=x_message_id,
    )
    internacao = _objeto(dados, "internacao")
    saida = InternacaoResponse(
        internacao_id=internacao["internacao_id"],
        paciente_id=internacao["paciente_id"],
        # A projecao indexa o card pelo codigo do leito; preferimos "leito" a "leito_id" (UUID).
        leito_id=internacao.get("leito") or internacao["leito_id"],
        setor=internacao.get("setor") or "",
        admitido_em=internacao["admitido_em"],
    )
    _aplicar_otimista(projecao, "aplicar_admissao", saida)
    resposta.headers["Location"] = f"/internacoes/{saida.internacao_id}"
    return saida


@router.post(
    "/internacoes/{internacao_id}/alta",
    status_code=200,
    response_model=AltaResponse,
    responses={
        **RESPOSTAS_PADRAO,
        404: {"model": ProblemDetails, "description": "Internacao nao encontrada"},
        409: {"model": ProblemDetails, "description": "Internacao ja encerrada"},
        422: {"model": ProblemDetails, "description": "Corpo invalido"},
    },
    operation_id="dar_alta",
    summary="Registra a alta de uma internacao (sincrono, RPC sobre fila)",
    description=(
        "Encerra a internacao via RPC `paciente.dar-alta` e libera o leito. Alta sobre uma ja "
        "encerrada devolve `409 INTERNACAO_JA_ENCERRADA`. Idempotente por `X-Message-ID` (R2.4)."
    ),
)
async def dar_alta(
    internacao_id: UUID,
    corpo: AltaRequest,
    identidade: Identity = Depends(exigir_papel("medico", "admin")),
    rpc: RpcClient = Depends(obter_rpc),
    projecao: object | None = Depends(obter_projecao),
    x_message_id: str | None = Header(default=None, alias="X-Message-ID"),
) -> AltaResponse:
    """Registra a alta de uma internacao e libera o leito (R3.1, R3.4).

    Args:
        internacao_id: Identificador da internacao na rota.
        corpo: Motivo e observacoes da alta.
        identidade: Identidade autorizada (``medico`` ou ``admin``), propagada ao Envelope.
        rpc: Cliente RPC injetado.
        projecao: Projecao de leitos em memoria (opcional), atualizada de forma otimista.
        x_message_id: ``message_id`` opcional para reapresentacao idempotente da escrita.

    Returns:
        O :class:`AltaResponse` com ``leito_liberado=True``.
    """
    payload = {"internacao_id": str(internacao_id), **corpo.model_dump(mode="json")}
    dados = await rpc.call(
        "paciente.dar-alta",
        payload,
        identity=identidade,
        message_id=x_message_id,
    )
    internacao = _objeto(dados, "internacao")
    saida = AltaResponse(
        internacao_id=internacao.get("internacao_id") or str(internacao_id),
        leito_id=internacao.get("leito") or internacao.get("leito_id") or "",
        alta_em=internacao.get("alta_em"),
        leito_liberado=True,
    )
    _aplicar_otimista(projecao, "aplicar_alta", saida)
    return saida


def _aplicar_otimista(projecao: object | None, metodo: str, saida: Any) -> None:  # noqa: ANN401 - resposta de RPC e dict livre
    """Aplica a escrita na projecao de forma otimista, se ela existir e expuser o metodo (8.2.4).

    Nunca levanta: a projecao e um colaborador opcional do painel e sua ausencia -- ou uma falha na
    aplicacao otimista -- nao pode derrubar uma escrita que ja foi confirmada pelo RPC.
    """
    if projecao is None:
        return
    aplicar = getattr(projecao, metodo, None)
    if aplicar is None:
        return
    try:
        aplicar(saida)
    except Exception as exc:  # pragma: no cover - projecao volatil, best-effort
        _log.warning("projecao.aplicacao_otimista_falhou", metodo=metodo, erro=type(exc).__name__)


def _objeto(dados: dict[str, Any], chave: str) -> dict[str, Any]:
    """Extrai um sub-objeto do ``dados`` da resposta RPC, tolerando ausencia."""
    valor = dados.get(chave)
    return valor if isinstance(valor, dict) else {}
