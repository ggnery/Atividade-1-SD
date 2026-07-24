"""Ingestao de telemetria: ``POST /sinais`` (design 8.2.1 endpoint 5, 8.2.2, 8.2.5).

E a **unica** rota assincrona: publica ``sinais.coletados`` e devolve ``202`` (design 8.2.2).
Telemetria e produzida por equipamento, nao por pessoa -- so o tipo ``dispositivo`` (``X-API-Key``)
pode postar (R4.5), o que impede que uma credencial humana comprometida injete sinais falsos.

**Resolucao de ``internacao_id`` na borda (design 8.2.5, R6.1).** O monitor conhece o *leito*, nao a
*internacao*. O gateway resolve ``leito_id -> internacao_id`` pela projecao em memoria (que consome
``paciente.admitido``/``leito.ocupado``) e inclui o resultado no payload. Quando o corpo ja
traz ``internacao_id``, ele e usado como esta -- caminho de reprocessamento e de testes determinis-
ticos. "Aceito" significa **confirmado pelo Broker** (*publisher confirm*): sem Broker, o
o :class:`~hospitalmq.publisher.Publisher` levanta ``TransportError`` e vira ``503`` (R1.5).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from dependencias import autenticar_dispositivo, obter_projecao, obter_publisher, obter_rpc
from erros import LeitoNaoOcupado
from fastapi import APIRouter, Depends, Header
from schemas import AceiteResponse, ProblemDetails, SinaisVitaisRequest

from hospitalmq.auth import Identity
from hospitalmq.errors import TransportError
from hospitalmq.logging import get_logger
from hospitalmq.metrics import get_metrics

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from hospitalmq.publisher import Publisher
    from hospitalmq.rpc import RpcClient

__all__ = ["router"]

_log = get_logger(__name__)

router = APIRouter(tags=["sinais vitais"])


@router.post(
    "/sinais",
    status_code=202,
    response_model=AceiteResponse,
    responses={
        401: {"model": ProblemDetails, "description": "Credencial ausente ou invalida"},
        403: {"model": ProblemDetails, "description": "Papel sem permissao"},
        409: {"model": ProblemDetails, "description": "Leito sem internacao ativa"},
        422: {"model": ProblemDetails, "description": "Corpo invalido"},
        503: {"model": ProblemDetails, "description": "Broker indisponivel"},
    },
    operation_id="publicar_sinais",
    summary="Publica uma leitura de sinais vitais (assincrono, 202)",
    description=(
        "Publica o evento `sinais.coletados` e devolve `202` com `correlation_id` e `message_id`. "
        "Autenticacao por `X-API-Key` (dispositivo). O `internacao_id` e resolvido na borda "
        "do leito (design 8.2.5); envie-o no corpo apenas para reprocessar. Reenvio seguro com "
        "`X-Message-ID` (idempotencia, R2.4)."
    ),
)
async def publicar_sinais(
    corpo: SinaisVitaisRequest,
    identidade: Identity = Depends(autenticar_dispositivo),
    publisher: Publisher = Depends(obter_publisher),
    rpc: RpcClient = Depends(obter_rpc),
    projecao: object | None = Depends(obter_projecao),
    x_message_id: str | None = Header(default=None, alias="X-Message-ID"),
) -> AceiteResponse:
    """Resolve a internacao, publica ``sinais.coletados`` e devolve ``202`` (R1.1, R4.3, R6.1).

    Args:
        corpo: Leitura de sinais vitais ja validada estruturalmente.
        identidade: Identidade do dispositivo (``role="dispositivo"``), propagada ao Envelope.
        publisher: Publicador injetado; publica com *publisher confirm*.
        rpc: Cliente RPC, usado apenas na hidratacao pontual da projecao (design 8.2.5).
        projecao: Projecao de leitos em memoria (opcional).
        x_message_id: ``message_id`` opcional para idempotencia do cliente.

    Returns:
        O :class:`AceiteResponse` com ``message_id`` e ``correlation_id`` confirmados pelo Broker.
    """
    internacao_id = await _resolver_internacao(corpo, projecao, rpc)

    payload = corpo.model_dump(mode="json")
    payload["internacao_id"] = str(internacao_id)

    envelope = await publisher.publish(
        "sinais.coletados",
        payload,
        identity=identidade,
        message_id=x_message_id,
    )
    return AceiteResponse(
        message_id=envelope.message_id,
        correlation_id=envelope.correlation_id,
        recebido_em=datetime.now(UTC),
    )


async def _resolver_internacao(
    corpo: SinaisVitaisRequest,
    projecao: object | None,
    rpc: RpcClient,
) -> UUID:
    """Resolve ``leito_id -> internacao_id`` pela projecao em memoria (design 8.2.5, R6.1).

    Precedencia: ``internacao_id`` explicito no corpo (cliente autoritativo) -> card ocupado na
    projecao -> hidratacao pontual do leito por RPC quando a projecao ainda nao reconciliou o boot.

    Raises:
        LeitoNaoOcupado: Leito livre ou desconhecido na projecao ja hidratada (``409``).
        TransportError: Projecao indisponivel ou nao hidratada sem como consultar o autoritativo
            (``503``) -- o gateway nao sabe se o leito esta ocupado, e ``409`` seria mentir.
    """
    metrics = get_metrics()
    if corpo.internacao_id is not None:
        metrics.incr("internacao_resolvida_total", origem="corpo")
        return corpo.internacao_id

    if projecao is None:
        metrics.incr("internacao_resolvida_total", origem="falha")
        raise TransportError(
            "projecao de leitos indisponivel; nao ha como resolver a internacao do leito",
            motivo="projecao_ausente",
        )

    origem = "projecao"
    card = projecao.leito(corpo.leito_id)  # type: ignore[attr-defined]
    if card is None and not getattr(projecao, "hidratada", True):
        card = await projecao.hidratar_leito(rpc, corpo.leito_id)  # type: ignore[attr-defined]
        origem = "rpc"

    if (
        card is None
        or getattr(card, "estado", None) != "ocupado"
        or getattr(card, "internacao_id", None) is None
    ):
        metrics.incr("internacao_resolvida_total", origem="falha")
        raise LeitoNaoOcupado(corpo.leito_id)

    metrics.incr("internacao_resolvida_total", origem=origem)
    return UUID(str(card.internacao_id))
