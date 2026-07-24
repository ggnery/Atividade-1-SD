"""Handler unico da auditoria universal: grava a Trilha_de_Auditoria (design 6.5, 7.9.1).

A fila ``q.audit.todos`` recebe **copia de todo evento** de ``hospital.events`` pelo
binding curinga ``#`` (design 6.5, R7.1). O handler e deliberadamente cego ao significado
do payload: grava os campos comuns a qualquer Envelope e guarda o ``payload`` como
``JSONB`` opaco. Por isso um tipo de evento novo -- ``acesso.negado`` foi o decimo
segundo, ``medicacao.administrada`` seria o proximo -- nao quebra o handler nem exige
migracao de banco (design 6.5.2).

**Idempotencia natural, sem tabela de marca.** O ``audit-service`` e o unico consumidor
**sem** ``mensagens_processadas`` (design 7.9.1): o efeito *e* a marca. O handler inteiro
e um ``INSERT ... ON CONFLICT (message_id) DO NOTHING`` -- uma reentrega duplicada nao
insere linha nova e tampouco levanta erro, entao a mensagem e confirmada sem inflar a
trilha (design 6.5.4). Por isso os handlers usam ``idempotente=False``: a marca
``(consumidor, message_id)`` do middleware gravaria em ``auditoria.mensagens_processadas``,
tabela que nao existe -- e seria redundante com o ``UNIQUE (message_id)`` da propria linha.

**Registro por tipo (limitacao do despacho por ``(type, version)``).** O ``Consumer`` do
middleware despacha por par ``(envelope.type, envelope.version)`` exato
(``hospitalmq/consumer.py``, ``_resolver_handler``); nao ha registro curinga em
``@consumer.on``. Registramos, entao, o **mesmo** handler para cada tipo do catalogo
canonico (design 6.4). O binding ``#`` garante a *chegada* de qualquer tipo a
``q.audit.todos``; para tipos ainda inexistentes ou versoes novas, ver a nota de limitacao
no ``pendencias`` da entrega e o padrao analogo do gateway em design 8.5.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final

from modelos import EventoAuditoria
from sqlalchemy.dialects.postgresql import insert as pg_insert

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from hospitalmq.consumer import Consumer, MessageContext
    from hospitalmq.envelope import Envelope

__all__ = ["FILA_AUDITORIA", "TIPOS_EVENTO", "auditar", "montar_linha", "registrar_auditoria"]

FILA_AUDITORIA: Final[str] = "q.audit.todos"
"""Fila da auditoria universal, ligada a ``hospital.events`` pelo binding ``#`` (design 6.5)."""

TIPOS_EVENTO: Final[tuple[str, ...]] = (
    "paciente.admitido",
    "paciente.alta",
    "sinais.coletados",
    "sinais.registrados",
    "sinais.rejeitados",
    "alerta.gerado",
    "alerta.notificado",
    "alerta.falhou",
    "leito.ocupado",
    "leito.liberado",
    "prontuario.consultado",
    "acesso.negado",
)
"""Catalogo canonico de tipos de evento (design 6.4), incluindo ``acesso.negado`` (design 6.4.1).

Cada tipo e registrado com o **mesmo** handler :func:`auditar`, porque o ``Consumer`` despacha por
``(type, version)`` exato e nao oferece registro curinga (ver docstring do modulo).
"""


def _extrair_paciente_id(payload: Mapping[str, Any]) -> uuid.UUID | None:
    """Extrai ``paciente_id`` do payload quando presente e valido (design 7.4.5; R7.3).

    A extracao alimenta o indice ``ix_ea_paciente`` do relatorio LGPD (R7.3). E deliberadamente
    tolerante: eventos que nao carregam ``paciente_id`` (a maioria) gravam ``NULL``, e um valor
    ilegivel nunca derruba o ``INSERT`` da auditoria -- a permanencia da trilha (R7.4) tem
    precedencia sobre a completude de um campo derivado.

    Args:
        payload: Corpo do evento, ``dict`` opaco do Envelope.

    Returns:
        O UUID do paciente, ou ``None`` quando ausente ou nao conversivel.
    """
    bruto = payload.get("paciente_id")
    if bruto is None:
        return None
    if isinstance(bruto, uuid.UUID):
        return bruto
    try:
        return uuid.UUID(str(bruto))
    except (ValueError, TypeError):
        return None


def montar_linha(envelope: Envelope) -> dict[str, Any]:
    """Projeta o Envelope na linha de ``auditoria.eventos_auditoria`` (design 6.5.2, 7.4.5).

    Grava somente campos comuns a qualquer evento -- por isso funciona para tipos
    desconhecidos. O ``routing_key`` e igual a ``type`` (o Envelope usa o tipo como routing
    key, design 6.4); a identidade e desmembrada em ``sub``/``role``/``tipo``, todos
    ``None`` quando o evento nao carrega identidade (processo interno sem credencial, design
    4.2.1). ``registrado_em`` fica de fora: e o ``DEFAULT now()`` do banco (o instante da
    gravacao, distinto de ``ocorrido_em`` do Envelope).

    Args:
        envelope: O Envelope recebido, ja desserializado pelo ``Consumer``.

    Returns:
        Dicionario coluna->valor pronto para o ``INSERT``.
    """
    identidade = envelope.identity
    return {
        "message_id": uuid.UUID(envelope.message_id),
        "correlation_id": uuid.UUID(envelope.correlation_id),
        "causation_id": uuid.UUID(envelope.causation_id) if envelope.causation_id else None,
        "tipo": envelope.type,
        "versao": envelope.version,
        "routing_key": envelope.type,
        "ocorrido_em": envelope.timestamp,
        "produtor": envelope.producer,
        "identidade_sub": identidade.sub if identidade is not None else None,
        "identidade_role": identidade.role if identidade is not None else None,
        "identidade_tipo": identidade.tipo if identidade is not None else None,
        "paciente_id": _extrair_paciente_id(envelope.payload),
        "payload": envelope.payload,
    }


async def auditar(ctx: MessageContext[dict[str, Any]]) -> None:
    """Grava o evento na Trilha_de_Auditoria, de forma naturalmente idempotente (design 7.9.1).

    O handler inteiro e um ``INSERT ... ON CONFLICT (message_id) DO NOTHING`` na sessao da
    mensagem (``ctx.session``), a mesma transacao que o ``Consumer`` confirma apos o retorno.
    A duplicata (reentrega, redrive) nao insere linha nova e nao levanta erro: a mensagem e
    confirmada sem inflar a trilha (R7.4). Nao interpreta nem valida o payload -- essa
    cegueira e o que faz o binding ``#`` cobrir tipos futuros sem alteracao de codigo (design
    6.5.2, R7.1).

    Args:
        ctx: Contexto da mensagem entregue pelo ``Consumer`` -- envelope, sessao e ``log``
            ja correlacionado por ``correlation_id``/``message_id``.
    """
    linha = montar_linha(ctx.envelope)
    stmt = (
        pg_insert(EventoAuditoria)
        .values(**linha)
        .on_conflict_do_nothing(index_elements=["message_id"])
    )
    await ctx.session.execute(stmt)
    ctx.log.debug(
        "auditoria.registrada",
        tipo=ctx.envelope.type,
        produtor=ctx.envelope.producer,
        paciente_id=str(linha["paciente_id"]) if linha["paciente_id"] is not None else None,
    )


def registrar_auditoria(consumidor: Consumer) -> None:
    """Registra :func:`auditar` para cada tipo canonico em ``q.audit.todos`` (design 6.5).

    Usa ``idempotente=False`` porque a idempotencia da auditoria e natural (``ON CONFLICT``), nao a
    marca ``mensagens_processadas`` do middleware -- que nem existe no schema ``auditoria`` (design
    7.9.1). Sem ``modelo`` Pydantic: o handler nao interpreta o payload (design 6.5.2).

    Args:
        consumidor: O :class:`~hospitalmq.consumer.Consumer` do ``audit-service``, ja montado.
    """
    for tipo in TIPOS_EVENTO:
        consumidor.on(tipo, queue=FILA_AUDITORIA, idempotente=False)(auditar)
