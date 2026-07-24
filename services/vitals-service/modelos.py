"""Modelo SQLAlchemy do ``vitals-service`` — a leitura imutavel ``vitais.sinais_vitais``.

Espelha ao pe da letra a DDL de ``db/schema.sql`` (design 7.4.3): mesmo *schema* (``vitais``), mesma
tabela, mesmas colunas, tipos e *constraints*. O middleware ``hospitalmq`` nao impoe uma ``Base`` ao
servico; esta e a ``Base`` do dominio de vitais, e a marca de idempotencia
(``vitais.mensagens_processadas``) fica a cargo do proprio middleware.

**Imutabilidade (INV-4).** ``SinaisVitais`` e um registro clinico *append-only*: uma leitura, uma
vez persistida, nunca muda de valor nem e apagada. A garantia e do banco -- o *trigger*
``trg_sv_imutavel`` recusa ``UPDATE``/``DELETE`` e o ``REVOKE`` retira esses privilegios do papel
``svc_vitals`` --, e nao uma promessa do ORM. Por isso o modelo nao expoe *setters* de dominio nem
relacionamentos de escrita: o unico caminho legitimo e ``session.add`` de uma instancia nova.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Numeric,
    SmallInteger,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

__all__ = ["Base", "SinaisVitais"]


class Base(DeclarativeBase):
    """``DeclarativeBase`` do dominio do ``vitals-service`` (apenas ``vitais.sinais_vitais``)."""


class SinaisVitais(Base):
    """Uma leitura de sinais vitais aceita e persistida (design 7.4.3, ``vitais.sinais_vitais``).

    Registro clinico imutavel (INV-4). ``id`` e ``registrado_em`` sao gerados pelo banco
    (``gen_random_uuid()`` e ``now()``); ``coletado_em`` vem do evento ``sinais.coletados``.
    ``leito_codigo`` e desnormalizado a partir do ``leito_id`` do evento para alimentar o painel sem
    uma juncao *cross-service* (D2). ``origem_message_id`` guarda o ``message_id`` do
    ``sinais.coletados`` de origem e sustenta a idempotencia natural (``uq_sv_origem``), redundante
    com a marca do middleware.
    """

    __tablename__ = "sinais_vitais"
    __table_args__ = (
        CheckConstraint("frequencia_respiratoria BETWEEN 0 AND 80", name="ck_sv_fr"),
        CheckConstraint("saturacao_o2 BETWEEN 50 AND 100", name="ck_sv_spo2"),
        CheckConstraint("temperatura BETWEEN 25.0 AND 45.0", name="ck_sv_temp"),
        CheckConstraint("pressao_sistolica BETWEEN 40 AND 300", name="ck_sv_pas"),
        CheckConstraint("frequencia_cardiaca BETWEEN 20 AND 250", name="ck_sv_fc"),
        CheckConstraint("nivel_consciencia IN ('A', 'V', 'P', 'U')", name="ck_sv_avpu"),
        CheckConstraint(
            "coletado_em <= registrado_em + INTERVAL '60 seconds'",
            name="ck_sv_coleta_nao_futura",
        ),
        UniqueConstraint("origem_message_id", name="uq_sv_origem"),
        Index("ix_sv_internacao_coleta", "internacao_id", text("coletado_em DESC")),
        Index("ix_sv_correlation", "correlation_id"),
        {"schema": "vitais"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    internacao_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    leito_codigo: Mapped[str] = mapped_column(Text, nullable=False)
    coletado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    registrado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    frequencia_respiratoria: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    saturacao_o2: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    oxigenio_suplementar: Mapped[bool] = mapped_column(Boolean, nullable=False)
    temperatura: Mapped[Decimal] = mapped_column(Numeric(4, 1), nullable=False)
    pressao_sistolica: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    frequencia_cardiaca: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    nivel_consciencia: Mapped[str] = mapped_column(CHAR(1), nullable=False)
    origem_message_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    correlation_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)

    def __repr__(self) -> str:
        """Representacao curta para depuracao, sem expor a leitura clinica inteira."""
        return (
            f"SinaisVitais(id={self.id!r}, internacao_id={self.internacao_id!r}, "
            f"coletado_em={self.coletado_em!r})"
        )
