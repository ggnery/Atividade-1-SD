"""Rotas de paciente: criacao e prontuario (design 8.2.1 endpoints 2 e 7, 8.2.4).

* ``POST /pacientes`` -> RPC ``paciente.criar`` (escrita sincrona, ``201`` + ``Location``).
* ``GET /pacientes/{id}/prontuario`` -> RPC ``prontuario.consultar``, enriquecido best-effort com
  ``sinais.ultimos`` do ``vitals-service``.

O gateway e anemico em dominio: apenas traduz REST <-> RPC, propaga a identidade autenticada para o
``Envelope`` (R4.3) e converte o erro remoto em RFC 7807 pelo codigo (design 8.4.1). O
``correlation_id`` viaja implicitamente pela ``contextvar`` preenchida no middleware de borda.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from dependencias import exigir_papel, obter_rpc
from erros import RESPOSTAS_PADRAO
from fastapi import APIRouter, Depends, Header, Response
from schemas import (
    PacienteProntuario,
    PacienteRequest,
    PacienteResponse,
    ProblemDetails,
    ProntuarioInternacao,
    ProntuarioResponse,
)

from hospitalmq.auth import Identity
from hospitalmq.errors import RpcRemoteError, RpcTimeoutError
from hospitalmq.logging import get_logger

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from hospitalmq.rpc import RpcClient

__all__ = ["router"]

_log = get_logger(__name__)

router = APIRouter(tags=["pacientes"])

_LIMITE_SINAIS_PRONTUARIO = 10
"""Quantas leituras mais recentes buscar do ``vitals-service`` para o prontuario."""


@router.post(
    "/pacientes",
    status_code=201,
    response_model=PacienteResponse,
    responses={
        **RESPOSTAS_PADRAO,
        409: {"model": ProblemDetails, "description": "Documento ja cadastrado"},
        422: {"model": ProblemDetails, "description": "Corpo invalido"},
    },
    operation_id="criar_paciente",
    summary="Cria um paciente (sincrono, RPC sobre fila)",
    description=(
        "Cria um paciente via RPC `paciente.criar`. Chave natural: `documento` -- repetido "
        "devolve `409 DOCUMENTO_DUPLICADO`. Reenvio seguro com o cabecalho opcional `X-Message-ID` "
        "(idempotencia, R2.4)."
    ),
)
async def criar_paciente(
    corpo: PacienteRequest,
    resposta: Response,
    identidade: Identity = Depends(exigir_papel("enfermeiro", "admin")),
    rpc: RpcClient = Depends(obter_rpc),
    x_message_id: str | None = Header(default=None, alias="X-Message-ID"),
) -> PacienteResponse:
    """Cria um paciente e devolve ``201`` com ``Location`` para o prontuario (R3.1, R3.4).

    Args:
        corpo: Dados do paciente ja validados por Pydantic.
        resposta: Resposta HTTP, onde o ``Location`` e escrito.
        identidade: Identidade autorizada (``enfermeiro`` ou ``admin``), propagada ao Envelope.
        rpc: Cliente RPC injetado.
        x_message_id: ``message_id`` opcional para reapresentacao idempotente da escrita.

    Returns:
        O :class:`PacienteResponse` com ``paciente_id``, ``nome`` e ``criado_em``.
    """
    dados = await rpc.call(
        "paciente.criar",
        corpo.model_dump(mode="json"),
        identity=identidade,
        message_id=x_message_id,
    )
    paciente = _objeto(dados, "paciente")
    saida = PacienteResponse.model_validate(paciente)
    resposta.headers["Location"] = f"/pacientes/{saida.paciente_id}/prontuario"
    return saida


@router.get(
    "/pacientes/{paciente_id}/prontuario",
    response_model=ProntuarioResponse,
    responses={
        **RESPOSTAS_PADRAO,
        404: {"model": ProblemDetails, "description": "Paciente nao encontrado"},
    },
    operation_id="consultar_prontuario",
    summary="Consulta o prontuario de um paciente (sincrono, RPC sobre fila)",
    description=(
        "Consulta paciente, internacao atual e ultimas leituras via RPC `prontuario.consultar` "
        "(R7.2/R7.3). Nao ha acesso a banco no gateway; tudo passa pelo `admission-service` e pelo "
        "`vitals-service`. O `auditor` **nao** ve o prontuario (minimizacao LGPD, design 8.2.1)."
    ),
)
async def consultar_prontuario(
    paciente_id: UUID,
    identidade: Identity = Depends(exigir_papel("enfermeiro", "medico")),
    rpc: RpcClient = Depends(obter_rpc),
) -> ProntuarioResponse:
    """Consulta o prontuario e o enriquece com as ultimas leituras (R7.3).

    A consulta principal (``prontuario.consultar``) e critica -- seu erro (404, 504...) sobe. O
    enriquecimento com ``sinais.ultimos`` e best-effort: uma falha dele nao invalida o prontuario,
    apenas devolve a serie vazia.

    Args:
        paciente_id: Identificador do paciente na rota.
        identidade: Identidade autorizada (``enfermeiro`` ou ``medico``), propagada ao Envelope.
        rpc: Cliente RPC injetado.

    Returns:
        O :class:`ProntuarioResponse` consolidado.
    """
    dados = await rpc.call(
        "prontuario.consultar",
        {"paciente_id": str(paciente_id)},
        identity=identidade,
    )

    paciente = PacienteProntuario.model_validate(_objeto(dados, "paciente"))
    internacao_bruta = dados.get("internacao")
    internacao = (
        ProntuarioInternacao.model_validate(internacao_bruta)
        if isinstance(internacao_bruta, dict)
        else None
    )
    leito = dados.get("leito") if isinstance(dados.get("leito"), dict) else None

    ultimos = await _ultimos_sinais(rpc, internacao, identidade)
    return ProntuarioResponse(
        paciente=paciente,
        internacao_atual=internacao,
        leito=leito,
        ultimos_sinais=ultimos,
        alertas=[],
    )


async def _ultimos_sinais(
    rpc: RpcClient,
    internacao: ProntuarioInternacao | None,
    identidade: Identity,
) -> list[dict[str, Any]]:
    """Busca as ultimas leituras da internacao ativa no ``vitals-service`` (RPC ``sinais.ultimos``).

    Best-effort: uma internacao ausente/inativa ou uma falha do ``vitals-service`` resultam em serie
    vazia, sem derrubar a consulta do prontuario (design 8.2.1).
    """
    if internacao is None or not internacao.ativa:
        return []
    try:
        resposta = await rpc.call(
            "sinais.ultimos",
            {"internacao_id": str(internacao.internacao_id), "limite": _LIMITE_SINAIS_PRONTUARIO},
            identity=identidade,
        )
    except (RpcRemoteError, RpcTimeoutError) as exc:
        _log.warning(
            "prontuario.sinais_indisponiveis",
            internacao_id=str(internacao.internacao_id),
            erro=type(exc).__name__,
        )
        return []
    series = resposta.get("series") if isinstance(resposta, dict) else None
    if isinstance(series, dict):
        leituras = series.get(str(internacao.internacao_id))
        if isinstance(leituras, list):
            return leituras
    return []


def _objeto(dados: dict[str, Any], chave: str) -> dict[str, Any]:
    """Extrai um sub-objeto do ``dados`` da resposta RPC, tolerando ausencia."""
    valor = dados.get(chave)
    return valor if isinstance(valor, dict) else {}
