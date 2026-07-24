"""Handler de ``sinais.registrados``: calcula NEWS2 e decide o ``alerta.gerado`` (R6.2, R6.3).

Este e o coracao do ``triage-service``. O fluxo, na ordem do design 3.4 e 7.5.2:

1. o ``Consumer`` entrega uma leitura ja validada (o ``vitals-service`` rejeitou fora de
   faixa em R6.6 antes de promover a ``sinais.registrados``);
2. :func:`tratar_sinais_registrados` reconstroi os sete componentes clinicos e chama
   :func:`services.comum.news2.calcular_news2` -- funcao pura, sem I/O (R6.2);
3. se, e somente se, ``score.exige_alerta()`` (severidade alta: ``total >= 5`` OU algum
   componente isolado igual a 3, R6.3), emite ``alerta.gerado`` com o escore congelado no
   payload. Severidade baixa ou media **nao** gera evento adicional -- o Painel_de_Leitos
   recalcula o escore delas localmente (design 7.5.2) e o ``audit-service`` audita a leitura
   de qualquer forma pelo binding ``#``.

O handler nao persiste nada de dominio (decisao D3, design 7.3.2): o ``ScoreNEWS2`` e
recomputavel, entao a unica escrita e a marca de idempotencia que o middleware grava na
mesma transacao. O evento derivado sai por ``ctx.emitir`` -- publicado pelo ``Consumer``
**apos o COMMIT**, com ``correlation_id`` preservado e ``causation_id`` apontando para a
mensagem de ``sinais.registrados`` (R5.3).
"""

from __future__ import annotations

import uuid
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any, Final

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from hospitalmq import MessageContext, PermanentError
from services.comum.news2 import NivelConsciencia, SinaisVitais, calcular_news2

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from hospitalmq import Consumer

__all__ = [
    "EVENTO_ALERTA_GERADO",
    "EVENTO_SINAIS_REGISTRADOS",
    "FILA_SINAIS_REGISTRADOS",
    "SinaisRegistrados",
    "registrar_handlers",
    "tratar_sinais_registrados",
]

FILA_SINAIS_REGISTRADOS: Final[str] = "q.triage.sinais-registrados"
"""Fila de onde o ``triage-service`` consome (design 6.4, config ``TOPOLOGIA_PADRAO``)."""

EVENTO_SINAIS_REGISTRADOS: Final[str] = "sinais.registrados"
"""Tipo do evento consumido, produzido pelo ``vitals-service`` (design 6.4)."""

EVENTO_ALERTA_GERADO: Final[str] = "alerta.gerado"
"""Tipo do evento emitido quando a severidade e alta (R6.3, design 6.4)."""


class SinaisRegistrados(BaseModel):
    """Contrato de entrada do payload de ``sinais.registrados`` (design 6.4).

    Espelha o que o ``vitals-service`` promove a evento de dominio: os sete componentes
    clinicos mais os identificadores de rastreio. O ``leito`` e aceito tanto como
    ``leito_codigo`` (nome de dominio, coluna ``vitais.sinais_vitais.leito_codigo``) quanto
    como ``leito_id`` (nome de borda usado em ``sinais.coletados``), porque o codigo do leito
    e o mesmo valor textual nos dois -- e o servico nao deve quebrar por causa da grafia da
    chave. Campos extras sao ignorados (``extra="ignore"``): o evento pode crescer sem que a
    triagem precise ser recompilada (R1.3).

    Um payload que nao valide vira ``ValidationError`` no pipeline do ``Consumer``, traduzido
    em erro permanente -- vai direto a DLQ, sem gastar retentativa (design 4.5.1, 4.7.1).
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    internacao_id: str
    leito_codigo: str = Field(validation_alias=AliasChoices("leito_codigo", "leito_id"))
    sinais_vitais_id: str
    coletado_em: str
    frequencia_respiratoria: int
    saturacao_o2: int
    oxigenio_suplementar: bool
    temperatura: Decimal
    pressao_sistolica: int
    frequencia_cardiaca: int
    nivel_consciencia: NivelConsciencia

    @field_validator("temperatura", mode="before")
    @classmethod
    def _quantizar_temperatura(cls, valor: Any) -> Decimal:  # noqa: ANN401
        """Quantiza a temperatura para uma casa decimal antes do calculo (design 7.5.1).

        As faixas de temperatura da tabela NEWS2 sao contiguas apenas na resolucao de
        ``NUMERIC(4,1)``: ``36.05`` nao pertence a nenhuma faixa e faria ``calcular_news2``
        levantar ``ValueError``. Converter via ``str`` evita o ruido binario de ``float`` e o
        ``quantize`` garante exatamente uma casa, coerente com a coluna do banco.
        """
        return Decimal(str(valor)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)

    def para_sinais_vitais(self) -> SinaisVitais:
        """Projeta o payload no ``SinaisVitais`` congelado que ``calcular_news2`` consome."""
        return SinaisVitais(
            frequencia_respiratoria=self.frequencia_respiratoria,
            saturacao_o2=self.saturacao_o2,
            oxigenio_suplementar=self.oxigenio_suplementar,
            temperatura=self.temperatura,
            pressao_sistolica=self.pressao_sistolica,
            frequencia_cardiaca=self.frequencia_cardiaca,
            nivel_consciencia=self.nivel_consciencia,
        )


async def tratar_sinais_registrados(ctx: MessageContext[SinaisRegistrados]) -> None:
    """Calcula o NEWS2 da leitura e emite ``alerta.gerado`` se a severidade for alta (R6.3).

    Nao grava dominio (decisao D3): o escore e funcao pura e recomputavel. A unica saida e o
    evento derivado, e so quando ``score.exige_alerta()`` -- severidade alta. Fora disso o
    handler retorna sem emitir: a leitura ja foi auditada, e o painel recalcula o escore de
    severidade baixa/media localmente (design 7.5.2).

    Args:
        ctx: Contexto da mensagem. ``ctx.payload`` e o :class:`SinaisRegistrados` ja validado;
            ``ctx.emitir`` propaga correlacao e causalidade ao evento derivado (R5.3).

    Raises:
        PermanentError: Se algum sinal cair fora de toda faixa da tabela NEWS2 -- dado que o
            ``vitals-service`` deveria ter rejeitado (R6.6). Retentar nao mudaria o resultado,
            entao a mensagem vai direto a DLQ (design 4.7.1), preservando o motivo.
    """
    leitura = ctx.payload
    try:
        score = calcular_news2(leitura.para_sinais_vitais())
    except ValueError as exc:
        raise PermanentError(
            "sinais fora das faixas da tabela NEWS2",
            codigo="sinais_fora_de_faixa",
            sinais_vitais_id=leitura.sinais_vitais_id,
            detalhe=str(exc),
        ) from exc

    ctx.log.info(
        "triagem.avaliada",
        leito_id=leitura.leito_codigo,
        internacao_id=leitura.internacao_id,
        sinais_vitais_id=leitura.sinais_vitais_id,
        news2=score.total,
        componente_critico=score.componente_critico,
        severidade=score.severidade.value,
    )

    if not score.exige_alerta():
        return

    ctx.emitir(
        EVENTO_ALERTA_GERADO,
        {
            # O ``alerta_id`` e parte normativa do payload de ``alerta.gerado`` (design 6.4,
            # linha 5234): o ``triage-service`` e o produtor e gera a identidade do alerta, que
            # o ``alert-service`` usa como chave primaria da linha ``alertas.alertas``. Emitido
            # uma unica vez por ``ctx.emitir`` (apos o COMMIT da marca de idempotencia), entao a
            # reentrega da mesma ``sinais.registrados`` nao produz um segundo id.
            "alerta_id": str(uuid.uuid4()),
            "internacao_id": leitura.internacao_id,
            "leito_id": leitura.leito_codigo,
            "sinais_vitais_id": leitura.sinais_vitais_id,
            "score_news2": score.total,
            "componentes": dict(score.componentes),
            "componente_critico": score.componente_critico,
            "severidade": score.severidade.value,
            "coletado_em": leitura.coletado_em,
        },
    )


def registrar_handlers(consumidor: Consumer) -> None:
    """Registra o handler de ``sinais.registrados`` no ``Consumer`` (design 4.5.1).

    Isola a ligacao ``@consumer.on`` do ``main`` para que uma suite de unidade possa registrar
    o mesmo handler num ``Consumer`` sobre ``MemoryTransport`` e exercitar a triagem sem broker
    nem banco (R10.4).

    Args:
        consumidor: O :class:`~hospitalmq.consumer.Consumer` a que o handler sera vinculado.
    """
    consumidor.on(
        EVENTO_SINAIS_REGISTRADOS,
        queue=FILA_SINAIS_REGISTRADOS,
        modelo=SinaisRegistrados,
    )(tratar_sinais_registrados)
