"""Modelo SQLAlchemy do ``Alerta_Clinico`` (design secoes 7.4.4, 7.5.2; R6.3, R6.4, R6.5).

Espelha **exatamente** a tabela ``alertas.alertas`` de ``db/schema.sql`` -- nomes de schema,
tabela, coluna, tipo, constraints e indices. O ``db/schema.sql`` continua sendo a fonte da verdade
do modelo fisico (criado pelo servico one-shot ``db-init``); este mapeamento existe para o
``alert-service`` **ler e inserir**, nunca para criar as tabelas.

Um ``Alerta_Clinico`` guarda o ``ScoreNEWS2`` **congelado** que o disparou (copia clinica, decisao
D3 do design 7.3.2): ``score_total``, ``severidade``, ``componente_critico`` e o ``componentes``
JSONB com os sete parametros pontuados. Guarda tambem o estado de despacho (``notificado_em``,
``canal``, ``tentativas_notificacao``, ``ultimo_erro``), a origem (``origem_message_id`` do evento
``alerta.gerado``) e a correlacao ponta a ponta (``correlation_id``, R5.3).

O ``CHECK ck_alertas_severidade_coerente`` amarra a severidade gravada a mesma regra de R6.3 que
``services.comum.news2.classificar_severidade`` implementa (INV-5): por isso o handler deriva a
severidade daquela funcao antes de inserir, e a coerencia com a coluna nunca depende de o produtor
ter acertado o campo do evento.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    CheckConstraint,
    Index,
    SmallInteger,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

__all__ = ["AlertaClinico", "Base"]


class Base(DeclarativeBase):
    """Base declarativa dos modelos do ``alert-service`` (estilo SQLAlchemy 2.0)."""


class AlertaClinico(Base):
    """Uma linha de ``alertas.alertas``: alerta gerado por ``ScoreNEWS2`` alto (design 7.4.4).

    Raiz de agregado do ``alert-service``: nasce de um evento ``alerta.gerado`` (do
    ``triage-service``), carrega o escore congelado como prova clinica (D3) e evolui pelo estado de
    despacho ao canal da equipe do leito. As colunas ``internacao_id`` e ``sinais_vitais_id`` sao
    referencias *cross-service* sem FK, por decisao D2 (cada servico e dono do seu schema).

    Coerencia garantida na persistencia (espelhada aqui para fidelidade a ``db/schema.sql``):

    * ``ck_alertas_severidade_coerente`` -- a ``severidade`` e funcao total de ``(score_total,
      componente_critico)``, a mesma regra de R6.3 (INV-5).
    * ``ck_alertas_notificacao_coerente`` -- ``notificado_em`` e ``canal`` sao ambos nulos
      (pendente/falhou) ou ambos preenchidos (notificado): nunca um so.
    * ``ck_alertas_componentes`` -- o JSONB traz os sete parametros pontuados.
    * ``uq_alertas_origem`` -- um evento ``alerta.gerado`` gera no maximo um ``Alerta_Clinico``.
    """

    __tablename__ = "alertas"
    __table_args__ = (
        # Um evento alerta.gerado gera no maximo um Alerta_Clinico (design 7.4.4).
        UniqueConstraint("origem_message_id", name="uq_alertas_origem"),
        CheckConstraint("score_total BETWEEN 0 AND 20", name="ck_alertas_score"),
        CheckConstraint("tentativas_notificacao BETWEEN 0 AND 4", name="ck_alertas_tentativas"),
        # INV-5: a severidade gravada e exatamente a que a regra de R6.3 produz (design 7.4.4).
        CheckConstraint(
            "severidade = CASE "
            "WHEN score_total >= 5 OR componente_critico THEN 'alta' "
            "WHEN score_total >= 3 THEN 'media' "
            "ELSE 'baixa' END",
            name="ck_alertas_severidade_coerente",
        ),
        # O JSONB de componentes deve trazer os sete parametros pontuados (design 7.4.4).
        CheckConstraint(
            "componentes ?& ARRAY["
            "'frequencia_respiratoria', 'saturacao_o2', 'oxigenio_suplementar', "
            "'temperatura', 'pressao_sistolica', 'frequencia_cardiaca', "
            "'nivel_consciencia']",
            name="ck_alertas_componentes",
        ),
        CheckConstraint(
            "(notificado_em IS NULL) = (canal IS NULL)",
            name="ck_alertas_notificacao_coerente",
        ),
        {"schema": "alertas"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    internacao_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    leito_codigo: Mapped[str] = mapped_column(Text, nullable=False)
    sinais_vitais_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    score_total: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    severidade: Mapped[str] = mapped_column(Text, nullable=False)
    componente_critico: Mapped[bool] = mapped_column(Boolean, nullable=False)
    componentes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    gerado_em: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    notificado_em: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    canal: Mapped[str | None] = mapped_column(Text, nullable=True)
    tentativas_notificacao: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, server_default=text("0")
    )
    ultimo_erro: Mapped[str | None] = mapped_column(Text, nullable=True)
    origem_message_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    correlation_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)


# Indices da tabela, espelhando db/schema.sql (design 7.4.4). Definidos apos a classe para usar os
# atributos mapeados e expressar a ordenacao DESC, o indice parcial e o GIN jsonb_path_ops.

# Lista de alertas do painel, mais recentes primeiro (R11.4).
Index("ix_alertas_leito_tempo", AlertaClinico.leito_codigo, AlertaClinico.gerado_em.desc())

# Fila de trabalho: alertas ainda nao despachados (R6.4/R6.5).
Index(
    "ix_alertas_pendentes",
    AlertaClinico.gerado_em,
    postgresql_where=AlertaClinico.notificado_em.is_(None),
)

# Consulta analitica por componente pontuado, sem varredura sequencial.
Index(
    "ix_alertas_componentes",
    AlertaClinico.componentes,
    postgresql_using="gin",
    postgresql_ops={"componentes": "jsonb_path_ops"},
)
