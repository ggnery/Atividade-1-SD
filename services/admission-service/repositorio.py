"""Acesso a dados do schema ``clinico`` -- consultas e escritas dos agregados do serviço.

Toda funcao recebe a :class:`~sqlalchemy.ext.asyncio.AsyncSession` **explicitamente**: o repositorio
nao abre transacao nem faz ``commit``. Quem controla a transacao e a operacao RPC (escrita, dentro
da transacao unica do :class:`~hospitalmq.rpc.RpcServer`, junto da marca de idempotencia e do
outbox) ou o proprio ``main`` (leitura, sessao efemera). Manter o repositorio sem transacao e o que
permite as escritas de dominio compartilharem o **mesmo** ``COMMIT`` da marca (design 4.8.3, 5.6.5).

As escritas usam ``session.add(...)`` + ``flush``: o ``flush`` materializa o ``INSERT`` no instante
previsivel em que o ``UNIQUE``/indice parcial e verificado, e devolve o objeto ja com ``id`` e os
carimbos de tempo do banco. A violacao de unicidade **nao** e tratada aqui -- ela sobe como
:class:`~sqlalchemy.exc.IntegrityError` para a operacao RPC, que a traduz no codigo de erro do
contrato (``DOCUMENTO_DUPLICADO``, ``LEITO_OCUPADO``, ``PACIENTE_JA_INTERNADO``; design 5.7).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, cast

from modelos import Internacao, Leito, Paciente
from sqlalchemy import Row, and_, select, update

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession


# --------------------------------------------------------------------------- #
# Paciente                                                                      #
# --------------------------------------------------------------------------- #


async def criar_paciente(
    session: AsyncSession,
    *,
    nome: str,
    documento: str,
    data_nascimento: date,
    sexo: str,
) -> Paciente:
    """Insere um novo :class:`Paciente` e devolve-o ja com ``id`` e ``criado_em``.

    Args:
        session: Transacao corrente.
        nome: Nome completo.
        documento: Documento (chave natural unica).
        data_nascimento: Data de nascimento.
        sexo: ``'M'``, ``'F'`` ou ``'O'``.

    Returns:
        O paciente persistido.

    Raises:
        sqlalchemy.exc.IntegrityError: Se ``documento`` ja existir (``uq_pacientes_documento``).
    """
    paciente = Paciente(nome=nome, documento=documento, data_nascimento=data_nascimento, sexo=sexo)
    session.add(paciente)
    await session.flush()
    return paciente


async def buscar_paciente_por_id(session: AsyncSession, paciente_id: uuid.UUID) -> Paciente | None:
    """Carrega um paciente por identidade, ou ``None`` se nao existir."""
    return await session.get(Paciente, paciente_id)


async def buscar_paciente_por_documento(session: AsyncSession, documento: str) -> Paciente | None:
    """Carrega um paciente pela chave natural ``documento``, ou ``None`` se nao existir.

    E a releitura da reapresentacao idempotente de ``paciente.criar`` (design 5.6.5).
    """
    stmt = select(Paciente).where(Paciente.documento == documento)
    return (await session.execute(stmt)).scalar_one_or_none()


# --------------------------------------------------------------------------- #
# Leito                                                                         #
# --------------------------------------------------------------------------- #


async def buscar_leito_por_id(session: AsyncSession, leito_id: uuid.UUID) -> Leito | None:
    """Carrega um leito por identidade, ou ``None`` se nao existir."""
    return await session.get(Leito, leito_id)


async def buscar_leito_por_codigo(session: AsyncSession, codigo: str) -> Leito | None:
    """Carrega um leito pelo codigo humano (ex. ``UTI-03``), ou ``None`` se nao existir."""
    stmt = select(Leito).where(Leito.codigo == codigo)
    return (await session.execute(stmt)).scalar_one_or_none()


# --------------------------------------------------------------------------- #
# Internacao                                                                    #
# --------------------------------------------------------------------------- #


async def criar_internacao(
    session: AsyncSession,
    *,
    paciente_id: uuid.UUID,
    leito_id: uuid.UUID,
    motivo: str,
    equipe: str,
) -> Internacao:
    """Abre uma :class:`Internacao` e devolve-a ja com ``id`` e ``admitido_em``.

    Args:
        session: Transacao corrente.
        paciente_id: Paciente a internar.
        leito_id: Leito a ocupar.
        motivo: Motivo da admissao.
        equipe: Equipe responsavel.

    Returns:
        A internacao ativa recem-criada.

    Raises:
        sqlalchemy.exc.IntegrityError: Se o leito ja tiver internacao ativa
            (``uq_internacoes_leito_ativo``, INV-1) ou o paciente ja estiver internado
            (``uq_internacoes_paciente_ativo``, INV-6).
    """
    internacao = Internacao(
        paciente_id=paciente_id, leito_id=leito_id, motivo=motivo, equipe=equipe
    )
    session.add(internacao)
    await session.flush()
    return internacao


async def buscar_internacao(session: AsyncSession, internacao_id: uuid.UUID) -> Internacao | None:
    """Carrega uma internacao por identidade, ou ``None`` se nao existir."""
    return await session.get(Internacao, internacao_id)


async def internacao_ativa_de_paciente(
    session: AsyncSession, paciente_id: uuid.UUID
) -> Internacao | None:
    """Carrega a internacao **ativa** de um paciente, ou ``None`` se ele nao estiver internado.

    E a releitura da reapresentacao idempotente de ``paciente.admitir`` (design 5.6.5). O predicado
    ``alta_em IS NULL`` casa o indice unico parcial de INV-6; ha no maximo uma linha.
    """
    stmt = select(Internacao).where(
        Internacao.paciente_id == paciente_id, Internacao.alta_em.is_(None)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def internacao_mais_recente_de_paciente(
    session: AsyncSession, paciente_id: uuid.UUID
) -> Internacao | None:
    """Carrega a internacao mais recente de um paciente (ativa ou nao), ou ``None`` se nunca houve.

    Sustenta ``prontuario.consultar`` (design 5.8.1): o prontuario mostra o paciente e sua ultima
    internacao. Usa o indice ``ix_internacoes_paciente_tempo`` (``paciente_id, admitido_em DESC``).
    """
    stmt = (
        select(Internacao)
        .where(Internacao.paciente_id == paciente_id)
        .order_by(Internacao.admitido_em.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def encerrar_internacao(
    session: AsyncSession, internacao_id: uuid.UUID, momento: datetime
) -> Internacao | None:
    """Registra a alta de uma internacao **ativa** e devolve o estado atualizado.

    O ``UPDATE ... WHERE id = :id AND alta_em IS NULL`` e a barreira natural da reapresentacao de
    ``paciente.dar-alta`` (design 5.6.5): se a internacao ja recebeu alta, afeta zero linhas e o
    retorno e ``None``, que a operacao traduz em ``INTERNACAO_JA_ENCERRADA``.

    Args:
        session: Transacao corrente.
        internacao_id: Internacao a encerrar.
        momento: Instante da alta (``>= admitido_em`` por INV-3).

    Returns:
        A internacao ja com ``alta_em`` preenchido, ou ``None`` se nao havia internacao ativa com
        esse ``id`` (inexistente ou ja encerrada).
    """
    stmt = (
        update(Internacao)
        .where(Internacao.id == internacao_id, Internacao.alta_em.is_(None))
        .values(alta_em=momento)
        .returning(Internacao.id)
    )
    encerrada = (await session.execute(stmt)).first()
    if encerrada is None:
        return None
    # populate_existing recarrega o objeto do banco caso a checagem de existencia ja o tenha
    # trazido para o identity map com alta_em=None antes deste UPDATE.
    return await session.get(Internacao, internacao_id, populate_existing=True)


# --------------------------------------------------------------------------- #
# Painel de leitos (snapshot RPC, design 5.8.2)                                 #
# --------------------------------------------------------------------------- #


async def listar_leitos(
    session: AsyncSession, *, apenas_ocupados: bool
) -> Sequence[Row[tuple[Leito, Internacao | None, Paciente | None]]]:
    """Lista os leitos ativos e, quando ocupados, a internacao ativa e o paciente.

    Sustenta ``leitos.snapshot`` (design 5.8.2), a hidratacao do Painel_de_Leitos no *boot* do
    gateway. O ``LEFT JOIN`` com ``alta_em IS NULL`` resolve o estado ``ocupado``/``livre`` sem
    coluna de status (design 7.2.2/7.2.3).

    Args:
        session: Sessao de leitura.
        apenas_ocupados: Quando ``True``, devolve so os leitos com internacao ativa.

    Returns:
        Linhas ``(Leito, Internacao | None, Paciente | None)``, ordenadas por ``codigo``.
    """
    stmt = (
        select(Leito, Internacao, Paciente)
        .outerjoin(
            Internacao,
            and_(Internacao.leito_id == Leito.id, Internacao.alta_em.is_(None)),
        )
        .outerjoin(Paciente, Paciente.id == Internacao.paciente_id)
        .where(Leito.ativo.is_(True))
        .order_by(Leito.codigo)
    )
    if apenas_ocupados:
        stmt = stmt.where(Internacao.id.is_not(None))
    linhas = (await session.execute(stmt)).all()
    # O ``outerjoin`` torna Internacao/Paciente anulaveis (leito livre), mas o tipo inferido pelo
    # SQLAlchemy nao capta o ``LEFT``; o ``cast`` reconcilia a assinatura com a consulta real.
    return cast("Sequence[Row[tuple[Leito, Internacao | None, Paciente | None]]]", linhas)
