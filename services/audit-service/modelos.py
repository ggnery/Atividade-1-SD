"""Modelo SQLAlchemy da Trilha_de_Auditoria (design 7.4.5, 7.9.1; R7.1, R7.3, R7.4).

Espelha **exatamente** a tabela ``auditoria.eventos_auditoria`` de ``db/schema.sql`` --
nomes de schema, tabela, coluna, tipo e constraints. O ``db/schema.sql`` continua sendo a
fonte da verdade do modelo fisico (criado pelo servico one-shot ``db-init``); este
mapeamento existe para o ``audit-service`` **ler e inserir**, nunca para criar as tabelas.

A tabela e **somente-insercao** (R7.4): a imutabilidade e garantida na camada de
persistencia (trigger ``trg_ea_somente_insert`` e ``REVOKE UPDATE, DELETE, TRUNCATE`` do
papel ``svc_audit`` em ``db/schema.sql``), nao no ORM. Por isso o modelo nao expoe nenhum
metodo de mutacao: a unica escrita legitima e o
``INSERT ... ON CONFLICT (message_id) DO NOTHING`` do :mod:`handler`, cuja idempotencia e
**natural** -- a propria linha, com ``UNIQUE (message_id)``, e a marca (design 7.9.1).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Index,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

__all__ = ["Base", "EventoAuditoria"]


class Base(DeclarativeBase):
    """Base declarativa dos modelos do ``audit-service`` (estilo SQLAlchemy 2.0)."""


class EventoAuditoria(Base):
    """Uma linha da Trilha_de_Auditoria: um evento que trafegou pelo Broker (design 7.4.5).

    Guarda apenas os campos comuns a **qualquer** Envelope -- ``message_id``,
    ``correlation_id``, ``causation_id``, ``tipo``, ``versao``, ``routing_key``, instante de
    ocorrencia, ``produtor`` e a identidade do produtor -- mais o ``payload`` integro em
    ``JSONB``. Nao replica o conteudo clinico campo a campo (design 9.9): a trilha comprova
    *quem* fez *o que* e *quando*, com apoio do ``paciente_id`` extraido do payload quando
    presente (R7.3), sem virar copia do prontuario.

    A chave primaria e ``BIGSERIAL`` (design 6.6/7.4): tabela de insercao sequencial de alto
    volume, chave monotonica evita fragmentacao de indice e da ordem natural de leitura; a
    unicidade logica fica a cargo de ``UNIQUE (message_id)``, que e tambem a idempotencia
    natural do handler (design 7.9.1).
    """

    __tablename__ = "eventos_auditoria"
    __table_args__ = (
        # Idempotencia natural: a marca de processamento e a propria linha (design 7.9.1).
        UniqueConstraint("message_id", name="uq_ea_message"),
        # Vocabulario fechado de identity.tipo do Envelope (design 4.2.1 e nota de 7.4.5): o
        # middleware so admite 'usuario'/'dispositivo'; processo interno sem credencial grava NULL.
        CheckConstraint(
            "identidade_tipo IS NULL OR identidade_tipo IN ('usuario', 'dispositivo')",
            name="ck_ea_identidade_tipo",
        ),
        {"schema": "auditoria"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    message_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    correlation_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    causation_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    tipo: Mapped[str] = mapped_column(Text, nullable=False)
    versao: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))
    routing_key: Mapped[str] = mapped_column(Text, nullable=False)
    ocorrido_em: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    registrado_em: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    produtor: Mapped[str] = mapped_column(Text, nullable=False)
    identidade_sub: Mapped[str | None] = mapped_column(Text, nullable=True)
    identidade_role: Mapped[str | None] = mapped_column(Text, nullable=True)
    identidade_tipo: Mapped[str | None] = mapped_column(Text, nullable=True)
    paciente_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


# Indices da tabela, espelhando db/schema.sql (design 7.4.5). Definidos apos a classe para
# usar os atributos mapeados e expressar a ordenacao DESC, o indice parcial e o GIN
# jsonb_path_ops fielmente.

# Reconstituir uma requisicao ponta a ponta (R5.3, R7.1).
Index("ix_ea_correlation", EventoAuditoria.correlation_id, EventoAuditoria.registrado_em)

# Relatorio LGPD: tudo o que aconteceu com um paciente (R7.3), so onde ha paciente_id.
Index(
    "ix_ea_paciente",
    EventoAuditoria.paciente_id,
    EventoAuditoria.registrado_em.desc(),
    postgresql_where=EventoAuditoria.paciente_id.isnot(None),
)

Index("ix_ea_tipo_tempo", EventoAuditoria.tipo, EventoAuditoria.registrado_em.desc())

# Consulta analitica sobre o payload opaco, sem varredura sequencial.
Index(
    "ix_ea_payload",
    EventoAuditoria.payload,
    postgresql_using="gin",
    postgresql_ops={"payload": "jsonb_path_ops"},
)
