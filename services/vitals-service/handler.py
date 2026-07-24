"""Handler de ``sinais.coletados`` do ``vitals-service`` (R6.1, R6.6, design 6.4 / 7.7).

O *pipeline* de mensageria (desserializar o Envelope, marcar idempotencia na mesma transacao, ACK
apos o sucesso, retentativa e DLQ) mora no ``hospitalmq``; aqui esta apenas a regra clinica de
ingestao. O fluxo por leitura, na ordem das tres camadas de validacao de 7.7.2:

1. **Estrutura** -- ``LeituraColetada`` (Pydantic) confere tipos, formatos e presenca. O handler e
   registrado com ``modelo=None`` **de proposito** (design 7.7.3): fosse o modelo declarado no
   ``@consumer.on``, o proprio pipeline traduziria o ``ValidationError`` em ``PermanentError`` antes
   de o handler rodar e o sistema perderia o evento ``sinais.rejeitados``.
2. **Faixa fisiologica** -- ``FAIXAS_FISIOLOGICAS``/``NIVEIS_CONSCIENCIA`` de
   ``services.comum.news2``, a fonte unica das faixas de aceitacao (R6.6), mais a janela temporal
   de ``coletado_em`` (7.7.1).
3. **Banco** -- ``session.flush()`` explicito dispara os ``CHECK`` de ``vitais.sinais_vitais`` ainda
   dentro do handler, para que uma violacao vire ``PermanentError`` (DLQ imediata) e nao um erro nao
   classificado, que o middleware trataria como transitorio, gastando tres retentativas (7.7.2).

Valor recusado nas camadas 2 ou 3 **nunca** pontua NEWS2: o handler publica ``sinais.rejeitados`` e
levanta ``PermanentError``. Valor aceito e persistido e o handler enfileira ``sinais.registrados``
com ``ctx.emitir`` -- publicado pelo ``Consumer`` **apos o COMMIT**, com correlacao e causalidade ja
propagadas (design 4.5.4). O ``vitals-service`` **nao** calcula NEWS2 na ingestao (R6.6): o evento
carrega os sete parametros crus, e a triagem os pontua.

**Divergencia registrada (frozen middleware).** O design 7.7.3 preve ``ctx.registrar_rejeicao`` +
um ramo do ``Consumer`` que publica ``sinais.rejeitados`` em unidade de trabalho propria. O
``hospitalmq`` congelado nao expoe esse gancho (``MessageContext`` so tem
``emitir``/``emitir_no_outbox``, e o ramo de ``PermanentError`` do pipeline nao drena
``ctx.derivados``). Para preservar o requisito -- rejeicao publicada **antes** da DLQ e marca de
idempotencia revertida com o ``ROLLBACK`` --, o handler publica ``sinais.rejeitados`` diretamente
por um ``Publisher`` injetado e so entao levanta ``PermanentError``. A publicacao e uma operacao de
rede, alheia a transacao do banco: o ``ROLLBACK`` que apaga a marca nao a desfaz, e a ordem
"rejeita, depois DLQ" e mantida.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Final

from modelos import SinaisVitais
from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError
from sqlalchemy.exc import IntegrityError

from hospitalmq import MessageContext, PermanentError, Publisher
from services.comum.news2 import FAIXAS_FISIOLOGICAS, NIVEIS_CONSCIENCIA

__all__ = ["LeituraColetada", "criar_handler_sinais_coletados"]

EVENTO_REGISTRADOS: Final[str] = "sinais.registrados"
EVENTO_REJEITADOS: Final[str] = "sinais.rejeitados"

JANELA_PASSADO: Final[timedelta] = timedelta(hours=24)
"""``coletado_em`` mais antigo que isto e reprocessamento de dado velho (design 7.7.1)."""

TOLERANCIA_FUTURO: Final[timedelta] = timedelta(seconds=60)
"""Folga de ``coletado_em`` no futuro que absorve *drift* de relogio entre containers (7.7.1)."""

_QUANTUM_TEMPERATURA: Final[Decimal] = Decimal("0.1")
"""Resolucao da coluna ``NUMERIC(4,1)``; a temperatura e quantizada a uma casa antes do INSERT."""

Handler = Callable[["MessageContext[dict[str, Any]]"], Awaitable[None]]
"""Assinatura do handler registrado em ``@consumer.on`` para ``sinais.coletados``."""


class LeituraColetada(BaseModel):
    """Camada 1 (estrutura) da validacao de 7.7.2: tipos, formatos e presenca dos campos.

    Deliberadamente **sem** faixa numerica: a faixa fisiologica e domino de ``services.comum.news2``
    (camada 2), aplicada depois, para que um valor como ``saturacao_o2 = 20`` -- aceito na borda,
    recusado aqui -- produza um ``sinais.rejeitados`` limpo com campo, valor e faixa.
    ``extra="ignore"`` torna o consumidor tolerante a campos futuros do evento sem quebrar
    (compatibilidade para frente).
    """

    model_config = ConfigDict(extra="ignore")

    leito_id: str
    internacao_id: uuid.UUID
    frequencia_respiratoria: int
    saturacao_o2: int
    oxigenio_suplementar: bool
    temperatura: Decimal
    pressao_sistolica: int
    frequencia_cardiaca: int
    nivel_consciencia: str
    coletado_em: AwareDatetime


def _valor_json(valor: object) -> Any:  # noqa: ANN401 - projeta qualquer tipo para JSON
    """Projeta um valor recusado numa forma serializavel em JSON, para o payload da rejeicao."""
    if isinstance(valor, Decimal):
        return str(valor)
    if isinstance(valor, datetime):
        return valor.isoformat()
    if valor is None or isinstance(valor, bool | int | float | str):
        return valor
    return str(valor)


def _primeiro_erro_estrutural(exc: ValidationError) -> tuple[str, Any]:
    """Extrai o campo e o valor da primeira violacao estrutural do Pydantic (design 7.7.3)."""
    erros = exc.errors()
    if not erros:
        return "payload", None
    primeiro = erros[0]
    loc = primeiro.get("loc", ())
    campo = ".".join(str(parte) for parte in loc) if loc else "payload"
    return campo, _valor_json(primeiro.get("input"))


def _violacao_de_dominio(
    leitura: LeituraColetada, agora: datetime
) -> tuple[str, Any, str, str] | None:
    """Aplica a camada 2 (R6.6): faixas fisiologicas, dominio AVPU e janela temporal.

    Args:
        leitura: A leitura ja estruturalmente valida.
        agora: Instante de referencia para a janela de ``coletado_em`` (agora-24h .. agora+60s).

    Returns:
        ``(campo, valor_recebido, faixa_aceita, motivo)`` da primeira violacao, ou ``None`` se a
        leitura esta inteiramente dentro das faixas de aceitacao.
    """
    for campo, faixa in FAIXAS_FISIOLOGICAS.items():
        valor = getattr(leitura, campo)
        if not faixa.contem(valor):
            faixa_aceita = f"[{faixa.minimo}, {faixa.maximo}] {faixa.unidade}"
            return campo, _valor_json(valor), faixa_aceita, "sinais_fora_de_faixa"

    if leitura.nivel_consciencia not in NIVEIS_CONSCIENCIA:
        dominio = ", ".join(NIVEIS_CONSCIENCIA)
        return (
            "nivel_consciencia",
            leitura.nivel_consciencia,
            f"{{{dominio}}}",
            "sinais_fora_de_faixa",
        )

    limite_inferior = agora - JANELA_PASSADO
    limite_superior = agora + TOLERANCIA_FUTURO
    if not (limite_inferior <= leitura.coletado_em <= limite_superior):
        faixa_aceita = f"[{limite_inferior.isoformat()}, {limite_superior.isoformat()}]"
        return (
            "coletado_em",
            leitura.coletado_em.isoformat(),
            faixa_aceita,
            "coletado_em_fora_da_janela",
        )

    return None


def _linha_de(leitura: LeituraColetada, envelope: Any) -> SinaisVitais:  # noqa: ANN401
    """Constroi a linha imutavel a partir da leitura validada e do Envelope de origem."""
    return SinaisVitais(
        internacao_id=leitura.internacao_id,
        leito_codigo=leitura.leito_id,
        coletado_em=leitura.coletado_em,
        frequencia_respiratoria=leitura.frequencia_respiratoria,
        saturacao_o2=leitura.saturacao_o2,
        oxigenio_suplementar=leitura.oxigenio_suplementar,
        temperatura=leitura.temperatura.quantize(_QUANTUM_TEMPERATURA),
        pressao_sistolica=leitura.pressao_sistolica,
        frequencia_cardiaca=leitura.frequencia_cardiaca,
        nivel_consciencia=leitura.nivel_consciencia,
        origem_message_id=uuid.UUID(envelope.message_id),
        correlation_id=uuid.UUID(envelope.correlation_id),
    )


def _payload_registrados(linha: SinaisVitais) -> dict[str, Any]:
    """Monta o payload de ``sinais.registrados``: ``sinais_vitais_id`` + os sete componentes crus.

    Os valores sao capturados agora (com a linha ja com ``id`` do ``flush``), embora a publicacao
    ocorra apos o COMMIT; ``temperatura`` viaja como string para preservar a exatidao decimal na
    serializacao JSON do Envelope (o ``json.dumps`` do middleware nao serializa ``Decimal``).
    """
    return {
        "sinais_vitais_id": str(linha.id),
        "internacao_id": str(linha.internacao_id),
        "leito_id": linha.leito_codigo,
        "coletado_em": linha.coletado_em.isoformat(),
        "frequencia_respiratoria": linha.frequencia_respiratoria,
        "saturacao_o2": linha.saturacao_o2,
        "oxigenio_suplementar": linha.oxigenio_suplementar,
        "temperatura": str(linha.temperatura),
        "pressao_sistolica": linha.pressao_sistolica,
        "frequencia_cardiaca": linha.frequencia_cardiaca,
        "nivel_consciencia": linha.nivel_consciencia,
    }


async def _publicar_rejeicao(
    publisher: Publisher,
    ctx: MessageContext[dict[str, Any]],
    *,
    leito_id: Any,  # noqa: ANN401 - pode faltar num payload estruturalmente invalido
    campo: str,
    valor_recebido: Any,  # noqa: ANN401
    faixa_aceita: str,
    motivo: str,
) -> None:
    """Publica ``sinais.rejeitados`` preservando correlacao e causalidade do evento de origem.

    A publicacao antecede o ``PermanentError`` do handler (design 7.7.3): assim a rejeicao sai
    **antes** da DLQ e o ``ROLLBACK`` que reverte a marca de idempotencia nao a apaga -- publicar e
    rede, nao banco. ``correlation_id`` e herdado; ``causation_id`` aponta para o
    ``sinais.coletados`` recusado, do mesmo modo que ``ctx.emitir`` faria no caminho de sucesso.
    """
    await publisher.publish(
        EVENTO_REJEITADOS,
        {
            "leito_id": leito_id,
            "campo": campo,
            "valor_recebido": valor_recebido,
            "faixa_aceita": faixa_aceita,
            "motivo": motivo,
        },
        correlation_id=ctx.envelope.correlation_id,
        causation_id=ctx.envelope.message_id,
        identity=ctx.envelope.identity,
    )
    ctx.log.warning(
        EVENTO_REJEITADOS,
        campo=campo,
        motivo=motivo,
        valor_recebido=valor_recebido,
        leito_id=leito_id,
    )


def criar_handler_sinais_coletados(publisher: Publisher) -> Handler:
    """Constroi o handler de ``sinais.coletados``, ligado ao ``Publisher`` das rejeicoes (7.7.3).

    O ``Publisher`` e injetado -- e nao lido de ``ctx`` -- porque o ``MessageContext`` do middleware
    congelado nao expoe publicacao ao handler; ver a divergencia no cabecalho do modulo.

    Args:
        publisher: ``Publisher`` sobre o mesmo transporte do servico, usado apenas para os eventos
            ``sinais.rejeitados`` fora da transacao principal.

    Returns:
        A corrotina a registrar em ``@consumer.on("sinais.coletados", modelo=None)``.
    """

    async def tratar_sinais_coletados(ctx: MessageContext[dict[str, Any]]) -> None:
        """Valida, persiste e propaga uma leitura de sinais vitais (R6.1) ou a rejeita (R6.6)."""
        bruto = ctx.envelope.payload
        leito_bruto = bruto.get("leito_id") if isinstance(bruto, dict) else None

        # Camada 1: estrutura.
        try:
            leitura = LeituraColetada.model_validate(bruto)
        except ValidationError as exc:
            campo, valor = _primeiro_erro_estrutural(exc)
            await _publicar_rejeicao(
                publisher,
                ctx,
                leito_id=leito_bruto,
                campo=campo,
                valor_recebido=valor,
                faixa_aceita="payload estruturalmente valido conforme design 8.2.3",
                motivo="payload_invalido",
            )
            raise PermanentError(
                f"payload de sinais.coletados invalido no campo {campo!r}",
                codigo="PAYLOAD_INVALIDO",
                campo=campo,
            ) from exc

        # Camada 2: faixa fisiologica, dominio AVPU e janela temporal (R6.6). Sem NEWS2.
        violacao = _violacao_de_dominio(leitura, datetime.now(UTC))
        if violacao is not None:
            campo, valor, faixa_aceita, motivo = violacao
            await _publicar_rejeicao(
                publisher,
                ctx,
                leito_id=leitura.leito_id,
                campo=campo,
                valor_recebido=valor,
                faixa_aceita=faixa_aceita,
                motivo=motivo,
            )
            raise PermanentError(
                f"sinais fora da faixa de aceitacao no campo {campo!r}",
                codigo=motivo,
                campo=campo,
            )

        # Persiste na mesma transacao da marca de idempotencia (aberta pelo Consumer).
        linha = _linha_de(leitura, ctx.envelope)
        ctx.session.add(linha)
        try:
            # Camada 3: o flush explicito dispara os CHECK do banco ainda dentro do handler.
            await ctx.session.flush()
        except IntegrityError as exc:
            raise PermanentError(
                "violacao de integridade ao persistir sinais vitais",
                codigo="sinais_fora_de_faixa",
            ) from exc

        # Publicado APOS o COMMIT pelo Consumer, com correlacao/causalidade ja propagadas.
        ctx.emitir(EVENTO_REGISTRADOS, _payload_registrados(linha))
        ctx.log.info(
            "vitais.registrado",
            sinais_vitais_id=str(linha.id),
            internacao_id=str(linha.internacao_id),
            leito_codigo=linha.leito_codigo,
        )

    return tratar_sinais_coletados
