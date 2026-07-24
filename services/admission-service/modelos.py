"""Modelos SQLAlchemy 2.0 do schema ``clinico`` -- os agregados do ``admission-service``.

Espelham **exatamente** o ``db/schema.sql`` (design 7.4.2): nomes de schema, tabela, coluna, tipo e
as *constraints* que carregam invariante de dominio (INV-1, INV-3, INV-6). O DDL fisico e a fonte
da verdade e e criado pelo serviço one-shot ``db-init``; estes modelos apenas mapeiam essas tabelas
para que o repositorio e as operacoes RPC leiam e escrevam com tipagem estatica.

Tres raizes de agregado vivem aqui (design 7.2.1):

* :class:`Paciente` -- a pessoa internada; chave natural ``documento``.
* :class:`Leito`    -- o recurso fisico; chave natural ``codigo`` (ex. ``UTI-03``).
* :class:`Internacao` -- a ocupacao de um :class:`Leito` por um :class:`Paciente`. Uma internacao
  **ativa** tem ``alta_em IS NULL`` (INV-2); os indices unicos parciais garantem INV-1 (um leito,
  uma internacao ativa) e INV-6 (um paciente, uma internacao ativa) no proprio banco.

As tabelas tecnicas do middleware -- ``clinico.mensagens_processadas`` (idempotencia) e
``clinico.outbox_mensagens`` (outbox transacional) -- **nao** sao declaradas aqui: pertencem ao
``hospitalmq`` (``hospitalmq.idempotency`` e ``hospitalmq.publisher``), que as declara em SQLAlchemy
Core para nao impor uma ``Base`` ao serviço.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import (
    CHAR,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SCHEMA: str = "clinico"
"""*Schema* PostgreSQL dono dos agregados clinicos (design 7.4.2)."""


class Base(DeclarativeBase):
    """Base declarativa dos modelos do ``admission-service``.

    ``eager_defaults=True`` faz o SQLAlchemy buscar os valores gerados pelo servidor -- ``id``
    (``gen_random_uuid()``), ``criado_em`` e ``admitido_em`` (``now()``) -- na mesma instrucao de
    ``INSERT`` via ``RETURNING`` (suportado pelo ``asyncpg``), de modo que o objeto ja volta do
    ``flush`` com esses campos preenchidos, sem um ``SELECT`` extra.
    """

    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012 (chave-valor lida pelo SQLAlchemy)


class Paciente(Base):
    """Paciente internavel -- raiz de agregado (design 7.2.1).

    Attributes:
        id: Identidade UUID, gerada pelo banco.
        nome: Nome completo (2 a 200 caracteres uteis).
        data_nascimento: Data de nascimento; base de :meth:`idade_em`.
        documento: Documento unico; chave natural que barra duplicata (``uq_pacientes_documento``).
        sexo: ``'M'``, ``'F'`` ou ``'O'``.
        criado_em: Instante do cadastro, preenchido pelo banco.
    """

    __tablename__ = "pacientes"
    __table_args__ = (
        UniqueConstraint("documento", name="uq_pacientes_documento"),
        CheckConstraint("sexo IN ('M', 'F', 'O')", name="ck_pacientes_sexo"),
        CheckConstraint("length(btrim(nome)) BETWEEN 2 AND 200", name="ck_pacientes_nome"),
        CheckConstraint(
            "data_nascimento <= CURRENT_DATE AND data_nascimento >= DATE '1900-01-01'",
            name="ck_pacientes_nascimento",
        ),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    data_nascimento: Mapped[date] = mapped_column(Date, nullable=False)
    documento: Mapped[str] = mapped_column(Text, nullable=False)
    sexo: Mapped[str] = mapped_column(CHAR(1), nullable=False)
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def idade_em(self, momento: date) -> int:
        """Idade do paciente, em anos completos, num dado momento.

        Args:
            momento: Data de referencia (tipicamente ``date.today()``).

        Returns:
            A idade em anos completos.
        """
        anos = momento.year - self.data_nascimento.year
        aniversario_passou = (momento.month, momento.day) >= (
            self.data_nascimento.month,
            self.data_nascimento.day,
        )
        return anos if aniversario_passou else anos - 1


class Leito(Base):
    """Leito fisico -- raiz de agregado (design 7.2.1).

    O estado ``Livre``/``Ocupado`` **nunca** e gravado: ``Ocupado`` significa "existe
    :class:`Internacao` com este ``leito_id`` e ``alta_em IS NULL``" (design 7.2.3). Nao ha coluna
    de status, de proposito -- duas fontes da mesma verdade divergiriam.

    Attributes:
        id: Identidade UUID, gerada pelo banco.
        codigo: Codigo humano unico no padrao ``AAA-99`` (ex. ``UTI-03``).
        setor: ``'UTI'``, ``'ENFERMARIA'`` ou ``'EMERGENCIA'``.
        ativo: ``False`` retira o leito de operacao (estado ``Desativado``).
        criado_em: Instante do cadastro, preenchido pelo banco.
    """

    __tablename__ = "leitos"
    __table_args__ = (
        UniqueConstraint("codigo", name="uq_leitos_codigo"),
        CheckConstraint("codigo ~ '^[A-Z]{3}-[0-9]{2}$'", name="ck_leitos_codigo"),
        CheckConstraint("setor IN ('UTI', 'ENFERMARIA', 'EMERGENCIA')", name="ck_leitos_setor"),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    codigo: Mapped[str] = mapped_column(Text, nullable=False)
    setor: Mapped[str] = mapped_column(Text, nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("TRUE"))
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Internacao(Base):
    """Ocupacao de um :class:`Leito` por um :class:`Paciente` -- raiz de agregado (design 7.2.1).

    Os dois indices unicos parciais ``WHERE alta_em IS NULL`` sao a barreira **primaria** de INV-1 e
    INV-6 (design 7.2.2): duas transacoes concorrentes tentando internar no mesmo leito (ou o mesmo
    paciente) fazem a segunda falhar com violacao de unicidade, sem nenhuma linha de codigo de
    bloqueio explicito. Os nomes dos indices (``uq_internacoes_leito_ativo`` /
    ``uq_internacoes_paciente_ativo``) sao os que a operacao RPC lê da ``IntegrityError`` para
    devolver ``LEITO_OCUPADO`` /
    ``PACIENTE_JA_INTERNADO``.

    Attributes:
        id: Identidade UUID, gerada pelo banco.
        paciente_id: FK para :class:`Paciente` (``ON DELETE RESTRICT``).
        leito_id: FK para :class:`Leito` (``ON DELETE RESTRICT``).
        admitido_em: Instante da admissao, preenchido pelo banco.
        alta_em: Instante da alta; ``NULL`` enquanto a internacao esta ativa (INV-2).
        motivo: Motivo da admissao (3 a 500 caracteres uteis).
        equipe: Equipe responsavel pela internacao.
    """

    __tablename__ = "internacoes"
    __table_args__ = (
        CheckConstraint(
            "alta_em IS NULL OR alta_em >= admitido_em", name="ck_internacoes_alta_posterior"
        ),
        CheckConstraint("length(btrim(motivo)) BETWEEN 3 AND 500", name="ck_internacoes_motivo"),
        Index(
            "uq_internacoes_leito_ativo",
            "leito_id",
            unique=True,
            postgresql_where=text("alta_em IS NULL"),
        ),
        Index(
            "uq_internacoes_paciente_ativo",
            "paciente_id",
            unique=True,
            postgresql_where=text("alta_em IS NULL"),
        ),
        Index("ix_internacoes_paciente_tempo", "paciente_id", text("admitido_em DESC")),
        Index(
            "ix_internacoes_ativas",
            "leito_id",
            text("admitido_em DESC"),
            postgresql_where=text("alta_em IS NULL"),
        ),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    paciente_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(f"{SCHEMA}.pacientes.id", ondelete="RESTRICT"),
        nullable=False,
    )
    leito_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(f"{SCHEMA}.leitos.id", ondelete="RESTRICT"),
        nullable=False,
    )
    admitido_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    alta_em: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    motivo: Mapped[str] = mapped_column(Text, nullable=False)
    equipe: Mapped[str] = mapped_column(Text, nullable=False)

    @property
    def esta_ativa(self) -> bool:
        """``True`` enquanto a internacao nao recebeu alta (INV-2)."""
        return self.alta_em is None
