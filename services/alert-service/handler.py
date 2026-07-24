"""Handler de ``alerta.gerado``: registra o ``Alerta_Clinico`` e o despacha ao canal (R6.4, R6.5).

Fluxo de uma mensagem ``alerta.gerado`` (fila ``q.alert.alerta-gerado``, design 6.4):

1. **Registrar** o ``Alerta_Clinico`` com o ``ScoreNEWS2`` congelado (D3): o handler monta a linha
   na ``ctx.session`` -- a mesma transacao da marca de idempotencia do middleware.
2. **Despachar** ao canal da equipe do leito (:mod:`notificacao`). O handler nao sabe se o canal e
   real ou simulado, nem que ha probabilidade de falha (design 12.5.2).
3. **Sucesso**: marca ``notificado_em``/``canal``, comita a linha e emite ``alerta.notificado``.
4. **Falha do canal** (``CanalIndisponivelError``): o handler a traduz em ``TransientError`` e a
   deixa subir. O middleware aplica a retentativa 1s/2s/4s e, esgotada, a DLQ (R6.5, R2.2, R2.3). So
   quando o orcamento de retentativas se esgota (``ctx.tentativas_restantes == 0``) o handler emite
   ``alerta.falhou`` -- o evento que a Trilha_de_Auditoria e o painel esperam quando o despacho
   fracassa (design 6.3, linha "esgotou as retentativas de despacho").

**Por que ``alerta.falhou`` e publicado por um ``Publisher`` proprio, e nao por ``ctx.emitir``.** Um
evento derivado enfileirado por ``ctx.emitir`` so e publicado **apos o COMMIT** (design 4.5.2); como
a falha do canal termina em ``TransientError``, a transacao da mensagem sofre *rollback* e um
derivado seria descartado. Para que ``alerta.falhou`` chegue a auditoria **e** a mensagem original
va para a DLQ -- os dois *publishes* distintos do diagrama de 3.6 --, o evento e publicado fora da
transacao, por um :class:`~hospitalmq.publisher.Publisher` injetado, imediatamente antes de o
handler levantar o ``TransientError`` que aciona a DLQ. Uma falha desse *publish* (broker fora) e
registrada e engolida: a telemetria e auto-corretiva (design 4.9.3) e o motivo da DLQ deve continuar
sendo a falha do canal.

**Persistencia e atomicidade.** A linha do ``Alerta_Clinico`` e inserida na ``ctx.session``, atomica
com a marca de idempotencia (design 4.8.3). No caminho de falha o ``TransientError`` reverte a
insercao junto com a marca -- e correto: nao ha meia-marca nem linha orfa, e a retentativa reexecuta
do zero. Consequencia registrada em ``divergencias``: um alerta cujo despacho esgota as retentativas
nao deixa linha em ``alertas.alertas`` -- o evento ``alerta.falhou`` e a entrada na DLQ e que o
registram.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final

from modelos import AlertaClinico
from notificacao import CanalIndisponivelError
from pydantic import BaseModel, ConfigDict, Field, field_validator

from hospitalmq.errors import TransientError
from services.comum.news2 import classificar_severidade, detectar_componente_critico

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from collections.abc import Awaitable, Callable

    from notificacao import CanalNotificacao

    from hospitalmq.consumer import Consumer, MessageContext
    from hospitalmq.envelope import Envelope
    from hospitalmq.publisher import Publisher

    Handler = Callable[[MessageContext["AlertaGerado"]], Awaitable[None]]

__all__ = [
    "FILA_ALERTA",
    "AlertaGerado",
    "criar_handler_alerta_gerado",
    "montar_alerta",
    "registrar_handler",
]

FILA_ALERTA: Final[str] = "q.alert.alerta-gerado"
"""Fila de onde chegam os eventos ``alerta.gerado`` (design 6.4, 6.10)."""

CHAVES_COMPONENTES: Final[tuple[str, ...]] = (
    "frequencia_respiratoria",
    "saturacao_o2",
    "oxigenio_suplementar",
    "temperatura",
    "pressao_sistolica",
    "frequencia_cardiaca",
    "nivel_consciencia",
)
"""Os sete parametros pontuados exigidos pelo ``CHECK ck_alertas_componentes`` (design 7.4.4)."""


class AlertaGerado(BaseModel):
    """Payload do evento ``alerta.gerado`` consumido pelo ``alert-service`` (design 6.4).

    Reune o que a linha ``alertas.alertas`` exige (design 7.4.4). Alem do "payload resumido" da
    tabela 6.4 (``alerta_id``, ``internacao_id``, ``leito_id``, ``score_news2``, ``componentes``,
    ``severidade``, ``coletado_em``), aceita ``sinais_vitais_id`` -- que o ``triage-service`` ja
    possui de ``sinais.registrados`` (design 6.4) e que a coluna ``sinais_vitais_id NOT NULL``
    requer -- e ``gerado_em``. Ver ``divergencias`` da entrega para a reconciliacao com o resumo.

    ``componente_critico`` e ``severidade`` sao **derivados** de ``componentes``/``score_news2`` no
    handler (INV-5), nunca confiados cegamente ao produtor; sao aceitos aqui apenas por tolerancia.
    """

    model_config = ConfigDict(extra="ignore")

    alerta_id: uuid.UUID
    internacao_id: uuid.UUID
    leito_id: str = Field(min_length=1)
    sinais_vitais_id: uuid.UUID
    score_news2: int = Field(ge=0, le=20)
    componentes: dict[str, int]
    severidade: str | None = None
    componente_critico: bool | None = None
    gerado_em: datetime | None = None
    coletado_em: datetime | None = None

    @field_validator("componentes")
    @classmethod
    def _exigir_sete_componentes(cls, valor: dict[str, int]) -> dict[str, int]:
        """Exige os sete parametros pontuados; a ausencia e erro permanente (DLQ direto, R6.6)."""
        faltando = [chave for chave in CHAVES_COMPONENTES if chave not in valor]
        if faltando:
            raise ValueError(f"componentes sem os parametros exigidos: {faltando}")
        return valor


def _iso_utc(momento: datetime) -> str:
    """Serializa um ``datetime`` como ISO-8601 UTC com sufixo ``Z`` (formato do Envelope)."""
    return momento.astimezone(UTC).isoformat().replace("+00:00", "Z")


def montar_alerta(payload: AlertaGerado, envelope: Envelope) -> AlertaClinico:
    """Projeta o payload de ``alerta.gerado`` numa linha de ``alertas.alertas`` (7.4.4, INV-5).

    O escore e **congelado** como veio do ``triage-service`` (``score_total = score_news2``,
    ``componentes`` intactos), mas ``componente_critico`` e ``severidade`` sao **derivados** das
    funcoes de ``services.comum.news2`` -- as mesmas de R6.3 e do ``CHECK
    ck_alertas_severidade_coerente`` --, garantindo por construcao a coerencia com a coluna.
    ``gerado_em`` usa ``gerado_em`` do payload, ou ``coletado_em``, ou o ``timestamp`` do Envelope,
    nesta ordem. A origem e a correlacao vem do Envelope (``origem_message_id`` e
    ``correlation_id``), fechando a rastreabilidade ponta a ponta (R5.3).

    Args:
        payload: O payload ja validado do evento ``alerta.gerado``.
        envelope: O Envelope recebido, fonte de ``origem_message_id``, ``correlation_id`` e do
            ``timestamp`` usado como ultimo recurso para ``gerado_em``.

    Returns:
        A instancia de :class:`AlertaClinico` pronta para inserir, ainda **nao** notificada.
    """
    componentes = dict(payload.componentes)
    critico = detectar_componente_critico(componentes)
    severidade = classificar_severidade(payload.score_news2, critico)
    gerado_em = payload.gerado_em or payload.coletado_em or envelope.timestamp
    return AlertaClinico(
        id=payload.alerta_id,
        internacao_id=payload.internacao_id,
        leito_codigo=payload.leito_id,
        sinais_vitais_id=payload.sinais_vitais_id,
        score_total=payload.score_news2,
        severidade=severidade.value,
        componente_critico=critico,
        componentes=componentes,
        gerado_em=gerado_em,
        tentativas_notificacao=0,
        origem_message_id=uuid.UUID(envelope.message_id),
        correlation_id=uuid.UUID(envelope.correlation_id),
    )


async def _publicar_alerta_falhou(
    publisher: Publisher,
    ctx: MessageContext[AlertaGerado],
    canal: CanalNotificacao,
    erro: Exception,
) -> None:
    """Publica ``alerta.falhou`` fora da transacao, ao esgotar as retentativas (design 6.3, R6.5).

    Publica pelo :class:`~hospitalmq.publisher.Publisher` injetado -- e nao por ``ctx.emitir``, que
    so emitiria apos um COMMIT que a falha do canal impede --, preservando ``correlation_id``,
    ``causation_id`` (o ``message_id`` do ``alerta.gerado``) e a identidade. Uma falha do proprio
    *publish* e registrada e engolida: e telemetria auto-corretiva (design 4.9.3) e nao pode roubar
    da DLQ o motivo real, a falha do canal.

    Args:
        publisher: Publicador do ``alert-service``, ligado ao mesmo transporte.
        ctx: Contexto da mensagem, fonte de correlacao, causalidade e identidade.
        canal: O canal cujo ``nome`` vai no evento.
        erro: A falha do canal, cuja mensagem vira o campo ``erro`` do payload.
    """
    payload = ctx.payload
    corpo = {
        "alerta_id": str(payload.alerta_id),
        "canal": canal.nome,
        "erro": str(erro),
        "tentativas": ctx.envelope.attempt,
    }
    try:
        await publisher.publish(
            "alerta.falhou",
            corpo,
            correlation_id=ctx.envelope.correlation_id,
            causation_id=ctx.envelope.message_id,
            identity=ctx.identity,
        )
        ctx.log.warning(
            "alerta.falhou",
            leito_codigo=payload.leito_id,
            alerta_id=str(payload.alerta_id),
            canal=canal.nome,
            tentativas=ctx.envelope.attempt,
        )
    except Exception as pub_exc:  # telemetria auto-corretiva (design 4.9.3)
        ctx.log.error(
            "alerta.falhou_nao_publicado",
            alerta_id=str(payload.alerta_id),
            erro=type(pub_exc).__name__,
            detalhe=str(pub_exc),
        )


def criar_handler_alerta_gerado(canal: CanalNotificacao, publisher: Publisher) -> Handler:
    """Constroi o handler de ``alerta.gerado`` com o canal e o publicador injetados (12.5.2).

    A injecao mantem o handler ignorante da implementacao do canal (real, simulado ou falso de
    teste) e testavel sem broker: basta injetar um canal que falha *n* vezes e depois entrega.

    Args:
        canal: O :class:`~notificacao.CanalNotificacao` para onde despachar o alerta.
        publisher: O :class:`~hospitalmq.publisher.Publisher` usado para ``alerta.falhou``.

    Returns:
        A corrotina handler, no formato que o ``Consumer`` espera (``async def (ctx) -> None``).
    """

    async def alerta_gerado(ctx: MessageContext[AlertaGerado]) -> None:
        """Registra o ``Alerta_Clinico`` e o despacha ao canal da equipe do leito (R6.4, R6.5)."""
        payload = ctx.payload
        alerta = montar_alerta(payload, ctx.envelope)
        try:
            destinatario = await canal.notificar(alerta)
        except CanalIndisponivelError as exc:
            ctx.log.warning(
                "alerta.notificacao_falhou",
                leito_codigo=payload.leito_id,
                alerta_id=str(payload.alerta_id),
                tentativa=ctx.envelope.attempt,
                tentativas_restantes=ctx.tentativas_restantes,
                erro=str(exc),
            )
            if ctx.tentativas_restantes == 0:
                await _publicar_alerta_falhou(publisher, ctx, canal, exc)
            # Traduz a falha do canal em TransientError: o middleware retenta e faz DLQ (R6.5).
            raise TransientError(str(exc)) from exc

        agora = datetime.now(UTC)
        alerta.notificado_em = agora
        alerta.canal = canal.nome
        alerta.tentativas_notificacao = ctx.envelope.attempt
        ctx.session.add(alerta)
        ctx.emitir(
            "alerta.notificado",
            {
                "alerta_id": str(payload.alerta_id),
                "canal": canal.nome,
                "destinatario": destinatario,
                "notificado_em": _iso_utc(agora),
            },
        )
        ctx.log.info(
            "alerta.notificado",
            leito_codigo=payload.leito_id,
            alerta_id=str(payload.alerta_id),
            canal=canal.nome,
            destinatario=destinatario,
            tentativa=ctx.envelope.attempt,
        )

    return alerta_gerado


def registrar_handler(consumidor: Consumer, canal: CanalNotificacao, publisher: Publisher) -> None:
    """Registra o handler de ``alerta.gerado`` na fila ``q.alert.alerta-gerado`` (6.4, 4.5.1).

    Usa o ``modelo=AlertaGerado`` para que o payload seja validado antes do handler: um payload
    malformado vira ``ValidationError`` -> erro permanente -> DLQ direto, sem gastar retentativa
    (R6.6). A idempotencia por ``(consumidor, message_id)`` fica no padrao (``idempotente=True``): a
    marca e o ``INSERT`` do alerta comitam juntos na mesma transacao (design 4.8.3).

    Args:
        consumidor: O :class:`~hospitalmq.consumer.Consumer` do ``alert-service``, ja montado.
        canal: O canal de notificacao a injetar no handler.
        publisher: O publicador usado para ``alerta.falhou``.
    """
    handler = criar_handler_alerta_gerado(canal, publisher)
    consumidor.on("alerta.gerado", queue=FILA_ALERTA, modelo=AlertaGerado)(handler)
