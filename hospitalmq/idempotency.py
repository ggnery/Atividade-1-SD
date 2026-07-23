"""Idempotencia do consumidor: a marca ``(consumidor, message_id)`` (secao 4.8) -- R2.4.

**Nao existe "exactly-once" na entrega.** O que o HospitalMQ entrega e *at-least-once* no
transporte somado a idempotencia no consumidor, o que produz **efeito efetivamente unico** no banco
de dados. E por isso que a idempotencia continua obrigatoria na proposta AWS: o SQS Standard
tambem entrega ao menos uma vez (R9.5).

O mecanismo e uma unica tabela e uma unica instrucao:

.. code-block:: sql

    CREATE TABLE mensagens_processadas (
        consumidor      TEXT        NOT NULL,
        message_id      UUID        NOT NULL,
        tipo            TEXT        NOT NULL,
        correlation_id  UUID        NOT NULL,
        tentativa       SMALLINT    NOT NULL DEFAULT 1,
        processado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT pk_mensagens_processadas PRIMARY KEY (consumidor, message_id)
    );

**Por que a chave e o par e nao so o** ``message_id``. A mesma mensagem chega a mais de uma
assinatura: ``sinais.coletados`` alimenta ``q.vitals.sinais-coletados`` **e** ``q.audit.todos``
(binding ``#``), com o mesmo ``message_id``. Uma chave simples so funcionaria enquanto cada servico
tivesse o proprio *schema*, e quebraria no instante em que um mesmo servico assinasse duas filas --
mudanca de topologia, nao de codigo: as duas entregas competiriam pela mesma linha e a marca de um
handler suprimiria indevidamente o outro, exatamente a perda silenciosa que R2.4 existe para
evitar. O valor da coluna ``consumidor`` e, portanto, ``f"{servico}:{fila}"`` -- ver
:func:`consumidor_de`.

**Por que a checagem precisa ser transacional com o efeito colateral (secao 4.8.4).** Ha tres
arranjos possiveis e apenas um e correto:

===================================================  ==========================================
Arranjo                                              Resultado de uma queda no ponto de corte
===================================================  ==========================================
(a) marcar, confirmar, depois executar o efeito      A mensagem consta processada e **nada
                                                     aconteceu**; a reentrega e suprimida ->
                                                     perda silenciosa
(b) executar o efeito, confirmar, depois marcar      A reentrega executa o efeito de novo ->
                                                     duplicidade, corrompendo o prontuario
(c) **marca e efeito na mesma transacao** (adotado)  ``ROLLBACK`` de tudo; a mensagem nao foi
                                                     confirmada ao broker e sera reentregue ->
                                                     o estado converge
===================================================  ==========================================

Com o arranjo (c) a unica janela remanescente e entre o ``COMMIT`` e o ``ack``, e ela resolve para
uma reentrega que a propria marca ja commitada suprime. Isso tem uma consequencia direta em
:mod:`hospitalmq.retry`: como o ``message_id`` **nao muda entre retentativas** (secao 4.6.3), a
marca so pode ser gravada em caso de sucesso -- se sobrevivesse ao erro, a primeira retentativa
seria classificada como duplicata e descartada. E o ``ROLLBACK`` do caminho de erro que garante
isso, e e por isso que a marca vive dentro da transacao do handler e nao em um cache externo.

Duas implementacoes atras do mesmo protocolo :class:`IdempotencyStore`:

* :class:`SqlIdempotencyStore` -- producao. ``INSERT ... ON CONFLICT DO NOTHING ... RETURNING`` na
  sessao que o handler recebe, portanto na mesma transacao do efeito de dominio.
* :class:`MemoryIdempotencyStore` -- testes de unidade e handlers sem banco. Emula a transacao com
  marcas provisorias, para que uma falha do handler nao deixe marca capaz de suprimir a
  retentativa seguinte.

Requisitos atendidos: R2.4, R2.5, R9.5.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

from hospitalmq.clock import Clock, RealClock
from hospitalmq.envelope import Envelope
from hospitalmq.errors import ConfigError, InvalidEnvelopeError

if TYPE_CHECKING:  # pragma: no cover - sqlalchemy so entra no perfil "dominio" (secao 12.2)
    from sqlalchemy import Executable, MetaData, Table
    from sqlalchemy.engine import Result

__all__ = [
    "COLUNAS",
    "COLUNA_CONSUMIDOR",
    "COLUNA_MESSAGE_ID",
    "NOME_TABELA",
    "RETENCAO_PADRAO",
    "IdempotencyStore",
    "Marca",
    "MemoryIdempotencyStore",
    "SessaoTransacional",
    "SqlIdempotencyStore",
    "consumidor_de",
    "expurgar",
    "ja_processado",
    "marcar",
    "tabela_mensagens_processadas",
]

NOME_TABELA: Final[str] = "mensagens_processadas"
"""Nome fisico da tabela, replicado nos *schemas* ``clinico``, ``vitais``, ``triagem`` e
``alertas`` (secao 7.4.6). O ``audit-service`` nao a possui: a auditoria e naturalmente idempotente
porque grava a mesma linha com a mesma chave (secao 7.9.1)."""

COLUNA_CONSUMIDOR: Final[str] = "consumidor"
"""Primeira coluna da chave primaria composta."""

COLUNA_MESSAGE_ID: Final[str] = "message_id"
"""Segunda coluna da chave primaria composta."""

COLUNAS: Final[tuple[str, ...]] = (
    COLUNA_CONSUMIDOR,
    COLUNA_MESSAGE_ID,
    "tipo",
    "correlation_id",
    "tentativa",
    "processado_em",
)
"""Colunas da tabela, na ordem do DDL da secao 4.8.1."""

RETENCAO_PADRAO: Final[timedelta] = timedelta(days=7)
"""Janela de retencao da marca: **7 dias** (secao 4.8.2).

Cobre com folga o cenario realista mais longo -- uma mensagem parada na DLQ durante um fim de
semana e reprocessada por ``redrive`` na segunda-feira, quando a supressao de duplicata ainda
precisa estar disponivel. Nunca expurgar nao e opcao: cerca de 345 mil linhas por dia e por
servico fariam o ``INSERT`` do caminho critico desacelerar junto com o indice.
"""

_TABELAS: dict[tuple[int, str | None], Table] = {}
"""Cache de :class:`~sqlalchemy.Table` por ``(id(metadata), schema)``.

Existe porque declarar duas vezes a mesma tabela no mesmo ``MetaData`` levanta
``InvalidRequestError``, e o ``Consumer`` chama :func:`marcar` uma vez por mensagem.
"""

_METADATA_PADRAO: list[Any] = []
"""Caixa preguicosa do ``MetaData`` interno; lista de um elemento para evitar ``global``."""


def consumidor_de(servico: str, fila: str) -> str:
    """Monta o valor canonico da coluna ``consumidor`` (secao 4.8.1).

    Args:
        servico: Nome do processo, ex. ``"vitals-service"``.
        fila: Nome da fila assinada, ex. ``"q.vitals.sinais-coletados"``.

    Returns:
        ``"<servico>:<fila>"``. Usar so o nome do servico e o erro listado no indice de nomes
        canonicos da secao 4.11: um servico que assine duas filas passaria a suprimir o proprio
        segundo handler.
    """
    return f"{servico}:{fila}"


def _para_uuid(valor: str, *, campo: str) -> uuid.UUID:
    """Converte um identificador do Envelope no ``UUID`` exigido pela coluna.

    Raises:
        InvalidEnvelopeError: Se o texto nao for um UUID. E permanente por natureza -- o valor nao
            passa a ser valido em uma retentativa -- e por isso leva a mensagem direto a DLQ.
    """
    try:
        return uuid.UUID(valor)
    except (AttributeError, TypeError, ValueError) as exc:
        raise InvalidEnvelopeError(
            f"{campo} nao e um UUID: {valor!r}", campo=campo, valor=valor
        ) from exc


# --------------------------------------------------------------------------- #
# Contratos                                                                     #
# --------------------------------------------------------------------------- #


@runtime_checkable
class SessaoTransacional(Protocol):
    """Fatia minima de ``AsyncSession`` de que a idempotencia depende.

    Tipar o parametro como a interface estreita -- e nao como ``AsyncSession`` -- e o que mantem
    :mod:`hospitalmq.idempotency` importavel no ``api-gateway``, que roda no perfil "borda" e nao
    tem ``sqlalchemy`` instalado (secoes 8.1.1 e 12.2), e o que permite a suite substituir a sessao
    por um duble sem subir banco.
    """

    async def execute(self, statement: Executable, /) -> Result[Any]:
        """Executa uma instrucao SQL na transacao corrente.

        Args:
            statement: Construcao do SQLAlchemy Core a executar.

        Returns:
            O ``Result`` da execucao.
        """
        ...


@runtime_checkable
class IdempotencyStore(Protocol):
    """Registro de mensagens ja processadas, por assinatura logica (secao 2.5, R2.4).

    Uma unica operacao decide: :meth:`marcar` **reserva e confirma ao mesmo tempo**, porque a
    confirmacao e o ``COMMIT`` da transacao do handler. Um par ``try_begin``/``confirm`` separado
    reintroduziria o arranjo (a) ou (b) da secao 4.8.4 -- os dois incorretos.
    """

    async def marcar(
        self, session: SessaoTransacional, envelope: Envelope, consumidor: str
    ) -> bool:
        """Tenta conquistar o direito de processar a mensagem.

        Args:
            session: Transacao corrente, a mesma em que o efeito de dominio sera gravado.
            envelope: Envelope recebido.
            consumidor: ``"<servico>:<fila>"`` -- ver :func:`consumidor_de`.

        Returns:
            ``True`` se esta transacao conquistou o direito de processar; ``False`` se outra ja
            processou, caso em que o handler e suprimido e a mensagem e confirmada mesmo assim.
        """
        ...

    async def ja_processado(
        self, session: SessaoTransacional, *, message_id: str, consumidor: str
    ) -> bool:
        """Consulta se a marca existe, **sem** reservar nada.

        Args:
            session: Sessao de leitura.
            message_id: ``message_id`` do Envelope.
            consumidor: ``"<servico>:<fila>"``.

        Returns:
            ``True`` se a marca ja esta gravada.
        """
        ...

    async def expurgar(self, session: SessaoTransacional, *, antes_de: datetime) -> int:
        """Remove marcas fora da janela de retencao (secao 4.8.2).

        Args:
            session: Transacao do job de purga.
            antes_de: Corte; marcas com ``processado_em`` anterior sao removidas.

        Returns:
            Quantidade de linhas removidas.
        """
        ...


@dataclass(frozen=True, slots=True)
class Marca:
    """Uma linha de ``mensagens_processadas`` (secao 4.8.1).

    Attributes:
        consumidor: ``"<servico>:<fila>"`` da assinatura que processou.
        message_id: ``message_id`` do Envelope; nao muda entre retentativas nem no ``redrive``.
        tipo: ``type`` do Envelope; diz qual evento esta duplicando.
        correlation_id: Amarra a marca ao fluxo de ponta a ponta (R5.3).
        tentativa: ``attempt`` no momento do sucesso; mostra quantas entregas foram necessarias.
        processado_em: Instante do processamento; coluna da janela de retencao e do indice de
            expurgo.
    """

    consumidor: str
    message_id: str
    tipo: str
    correlation_id: str
    tentativa: int
    processado_em: datetime

    @property
    def chave(self) -> tuple[str, str]:
        """Chave primaria composta ``(consumidor, message_id)``."""
        return (self.consumidor, self.message_id)

    @classmethod
    def de_envelope(cls, envelope: Envelope, consumidor: str, *, agora: datetime) -> Marca:
        """Projeta um Envelope na linha da tabela.

        Args:
            envelope: Envelope processado com sucesso.
            consumidor: ``"<servico>:<fila>"``.
            agora: Instante do processamento, vindo do :class:`~hospitalmq.clock.Clock` injetado.

        Returns:
            A marca correspondente.
        """
        return cls(
            consumidor=consumidor,
            message_id=envelope.message_id,
            tipo=envelope.type,
            correlation_id=envelope.correlation_id,
            tentativa=envelope.attempt,
            processado_em=agora,
        )

    def to_dict(self) -> dict[str, Any]:
        """Projeta a marca no dicionario de valores do ``INSERT``.

        Returns:
            Dicionario com as seis colunas da secao 4.8.1.
        """
        return {
            COLUNA_CONSUMIDOR: self.consumidor,
            COLUNA_MESSAGE_ID: self.message_id,
            "tipo": self.tipo,
            "correlation_id": self.correlation_id,
            "tentativa": self.tentativa,
            "processado_em": self.processado_em,
        }


# --------------------------------------------------------------------------- #
# Tabela (SQLAlchemy Core, importado sob demanda)                               #
# --------------------------------------------------------------------------- #


def _sqlalchemy() -> ModuleType:
    """Importa ``sqlalchemy`` sob demanda, com erro de configuracao legivel se faltar.

    Raises:
        ConfigError: Quando o extra ``dominio`` nao foi instalado. Falhar aqui, e nao no
            ``import`` do modulo, e o que mantem o ``api-gateway`` -- que nao tem banco -- capaz
            de importar :mod:`hospitalmq` inteiro no perfil "borda" (secao 12.2).
    """
    try:
        import sqlalchemy
    except ModuleNotFoundError as exc:  # pragma: no cover - depende do perfil instalado
        raise ConfigError(
            "SqlIdempotencyStore exige SQLAlchemy: instale o extra 'dominio' "
            "(pip install 'hospitalmq[dominio]')",
            dependencia="sqlalchemy",
        ) from exc
    return sqlalchemy


def tabela_mensagens_processadas(
    *, schema: str | None = None, metadata: MetaData | None = None
) -> Table:
    """Declara -- e memoriza -- a tabela ``mensagens_processadas`` (secoes 4.8.1 e 7.4.6).

    A declaracao e em **SQLAlchemy Core**, e nao um modelo ``DeclarativeBase``, por dois motivos:
    o middleware nao pode impor uma ``Base`` aos servicos, que ja tem a sua com as tabelas de
    dominio; e a mesma tabela e replicada em quatro *schemas* (``clinico``, ``vitais``,
    ``triagem``, ``alertas``), o que com modelo declarativo exigiria quatro classes identicas.

    Os tipos usados sao portateis de proposito: ``Uuid`` vira ``UUID`` nativo no PostgreSQL e
    ``CHAR(32)`` no SQLite, o que permite a suite de integracao rodar em banco em memoria, sem
    Docker, com o **mesmo** codigo de producao (secao 10.2).

    Args:
        schema: *Schema* do servico, ex. ``"vitais"``. ``None`` usa o *schema* padrao da conexao.
        metadata: ``MetaData`` onde declarar. ``None`` usa um interno do modulo; passe o do
            servico quando quiser que ``create_all`` crie a tabela junto com as de dominio.

    Returns:
        A ``Table``, sempre a mesma instancia para um dado par ``(metadata, schema)``.
    """
    sa = _sqlalchemy()
    if metadata is None:
        if not _METADATA_PADRAO:
            _METADATA_PADRAO.append(sa.MetaData())
        alvo = _METADATA_PADRAO[0]
    else:
        alvo = metadata

    chave = (id(alvo), schema)
    memorizada = _TABELAS.get(chave)
    if memorizada is not None:
        return memorizada

    tabela: Table = sa.Table(
        NOME_TABELA,
        alvo,
        sa.Column(COLUNA_CONSUMIDOR, sa.Text, primary_key=True, nullable=False),
        sa.Column(COLUNA_MESSAGE_ID, sa.Uuid(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("tipo", sa.Text, nullable=False),
        sa.Column("correlation_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("tentativa", sa.SmallInteger, nullable=False, server_default=sa.text("1")),
        sa.Column(
            "processado_em",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Index(f"ix_{schema or 'public'}_msgproc_purga", "processado_em"),
        schema=schema,
    )
    _TABELAS[chave] = tabela
    return tabela


def _nome_do_dialeto(session: SessaoTransacional) -> str:
    """Descobre o dialeto ligado a sessao, com ``postgresql`` como padrao seguro.

    O ``ON CONFLICT`` e uma extensao de dialeto: existe em ``postgresql`` e em ``sqlite``, e a
    construcao do SQLAlchemy e diferente em cada um. Detectar aqui evita obrigar cada servico a
    declarar em que banco esta rodando.
    """
    obter = getattr(session, "get_bind", None)
    if obter is None:
        return "postgresql"
    try:
        bind = obter()
    except Exception:  # pragma: no cover - sessao sem bind e caso de duble em teste
        return "postgresql"
    dialeto = getattr(bind, "dialect", None)
    nome = getattr(dialeto, "name", None)
    return str(nome) if nome else "postgresql"


# --------------------------------------------------------------------------- #
# Implementacao de producao                                                     #
# --------------------------------------------------------------------------- #


class SqlIdempotencyStore:
    """Marca de idempotencia em SQL, na **mesma transacao** do efeito de dominio (secao 4.8.3).

    A instrucao e uma so::

        INSERT INTO mensagens_processadas (...) VALUES (...)
        ON CONFLICT (consumidor, message_id) DO NOTHING
        RETURNING message_id

    Dois detalhes de implementacao valem uma frase cada:

    * **``index_elements`` lista as duas colunas.** O ``ON CONFLICT`` do PostgreSQL exige um indice
      unico que case **exatamente** com os elementos declarados; com a chave primaria composta,
      declarar so ``message_id`` faria o servidor levantar ``InvalidColumnReference`` em **toda**
      mensagem -- o pipeline inteiro de R2.4 pararia na primeira entrega.
    * **A duplicata e detectada por ``RETURNING`` + ``scalar_one_or_none()``, nao por
      ``rowcount``.** Em ``asyncpg`` via SQLAlchemy assincrono o ``rowcount`` de um ``ON CONFLICT``
      nao e garantido em todos os caminhos; ``RETURNING`` devolve zero ou uma linha de forma
      explicita.

    Corrida entre replicas, que e o cenario real quando a replica A morre depois do ``COMMIT`` e
    antes do ``ack``: no PostgreSQL o ``INSERT ... ON CONFLICT`` faz uma *insercao especulativa* e,
    ao encontrar chave duplicada ainda **nao confirmada**, **espera** o desfecho da transacao
    concorrente. Se A confirmou, B recebe zero linhas, suprime o handler e confirma a mensagem ao
    broker; se A reverteu, a linha especulativa some e o ``INSERT`` de B sucede. Nao ha janela em
    que ambas processem, nem janela em que nenhuma processe -- e tudo sem ``SELECT ... FOR
    UPDATE``, sem tabela de *locks* e sem servico de coordenacao.

    *Alternativa descartada:* ``SELECT`` de verificacao seguido de ``INSERT``. Sob ``READ
    COMMITTED``, dois ``SELECT`` concorrentes retornam "nao existe" e ambos prosseguem -- a corrida
    classica *check-then-act*.
    """

    __slots__ = ("_clock", "_metadata", "_schema")

    def __init__(
        self,
        *,
        schema: str | None = None,
        metadata: MetaData | None = None,
        clock: Clock | None = None,
    ) -> None:
        """Constroi o registro sobre SQL.

        Args:
            schema: *Schema* do servico, ex. ``"vitais"``. ``None`` usa o padrao da conexao.
            metadata: ``MetaData`` onde declarar a tabela; ``None`` usa o interno do modulo.
            clock: Fonte de tempo injetada, usada no calculo do corte de :meth:`expurgar`.
        """
        self._schema: str | None = schema
        self._metadata: MetaData | None = metadata
        self._clock: Clock = clock if clock is not None else RealClock()

    @property
    def tabela(self) -> Table:
        """A ``Table`` de ``mensagens_processadas`` no *schema* configurado."""
        return tabela_mensagens_processadas(schema=self._schema, metadata=self._metadata)

    @property
    def schema(self) -> str | None:
        """*Schema* configurado, ou ``None`` para o padrao da conexao."""
        return self._schema

    async def marcar(
        self, session: SessaoTransacional, envelope: Envelope, consumidor: str
    ) -> bool:
        """Grava a marca dentro da transacao corrente e diz se ela e nova (secao 4.8.3).

        A chamada **nao** faz ``commit``: quem confirma e o ``async with session.begin()`` do
        ``Consumer``, o mesmo que confirma o efeito de dominio. E dessa unica transacao que sai a
        propriedade central de R2.4 -- nunca ha efeito sem marca nem marca sem efeito.

        Args:
            session: A sessao que o handler vai usar para gravar o efeito. **Precisa** ser a mesma;
                usar uma sessao separada reintroduz os arranjos (a) e (b) da secao 4.8.4.
            envelope: Envelope recebido.
            consumidor: ``"<servico>:<fila>"`` -- ver :func:`consumidor_de`.

        Returns:
            ``True`` se **esta** transacao conquistou o direito de processar a mensagem; ``False``
            quando outra transacao ja processou, caso em que o ``Consumer`` registra
            ``mensagem.duplicada``, **confirma a mensagem ao broker** e nao executa o handler.

        Raises:
            InvalidEnvelopeError: Se ``message_id`` ou ``correlation_id`` nao forem UUID.
            ConfigError: Se o extra ``dominio`` (SQLAlchemy) nao estiver instalado.
        """
        tabela = self.tabela
        valores: dict[str, Any] = {
            COLUNA_CONSUMIDOR: consumidor,
            COLUNA_MESSAGE_ID: _para_uuid(envelope.message_id, campo="message_id"),
            "tipo": envelope.type,
            "correlation_id": _para_uuid(envelope.correlation_id, campo="correlation_id"),
            "tentativa": envelope.attempt,
        }

        if _nome_do_dialeto(session) == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as inserir_sqlite

            stmt: Any = inserir_sqlite(tabela)
        else:
            from sqlalchemy.dialects.postgresql import insert as inserir_postgresql

            stmt = inserir_postgresql(tabela)

        stmt = (
            stmt.values(**valores)
            .on_conflict_do_nothing(index_elements=[COLUNA_CONSUMIDOR, COLUNA_MESSAGE_ID])
            .returning(tabela.c[COLUNA_MESSAGE_ID])
        )
        resultado = await session.execute(stmt)
        return resultado.scalar_one_or_none() is not None

    async def ja_processado(
        self, session: SessaoTransacional, *, message_id: str, consumidor: str
    ) -> bool:
        """Consulta a marca sem reservar nada -- uso **diagnostico**, nunca decisorio.

        Serve ao ``scripts/redrive.py``, ao endpoint administrativo de DLQ e a asserção de teste.
        **Nao** deve preceder o handler: um ``SELECT`` seguido de ``INSERT`` e a corrida
        *check-then-act* que a secao 4.8.3 descarta explicitamente -- sob ``READ COMMITTED``, duas
        replicas leem "nao existe" e ambas executam o efeito. Quem decide e :meth:`marcar`.

        Args:
            session: Sessao de leitura.
            message_id: ``message_id`` do Envelope.
            consumidor: ``"<servico>:<fila>"``.

        Returns:
            ``True`` se a marca existe.
        """
        sa = _sqlalchemy()
        tabela = self.tabela
        stmt = sa.select(tabela.c[COLUNA_MESSAGE_ID]).where(
            tabela.c[COLUNA_CONSUMIDOR] == consumidor,
            tabela.c[COLUNA_MESSAGE_ID] == _para_uuid(message_id, campo="message_id"),
        )
        resultado = await session.execute(stmt)
        return resultado.scalar_one_or_none() is not None

    async def expurgar(self, session: SessaoTransacional, *, antes_de: datetime) -> int:
        """Remove marcas anteriores ao corte (secao 4.8.2).

        Equivale a ``DELETE FROM mensagens_processadas WHERE processado_em < :antes_de`` e usa o
        indice ``ix_<schema>_msgproc_purga``. Chamado por job diario; a janela padrao e
        :data:`RETENCAO_PADRAO` (7 dias).

        Args:
            session: Transacao do job de purga -- **nao** e a transacao de nenhuma mensagem.
            antes_de: Corte; marcas com ``processado_em`` anterior sao removidas.

        Returns:
            Quantidade de linhas removidas.
        """
        sa = _sqlalchemy()
        tabela = self.tabela
        stmt = sa.delete(tabela).where(tabela.c["processado_em"] < antes_de)
        resultado = await session.execute(stmt)
        return int(getattr(resultado, "rowcount", 0) or 0)

    def corte_de_retencao(self, retencao: timedelta = RETENCAO_PADRAO) -> datetime:
        """Calcula o instante de corte da purga a partir do relogio injetado.

        Args:
            retencao: Janela de retencao; padrao :data:`RETENCAO_PADRAO`.

        Returns:
            ``clock.now() - retencao``, em UTC.
        """
        return self._clock.now() - retencao


# --------------------------------------------------------------------------- #
# Implementacao em memoria (testes)                                             #
# --------------------------------------------------------------------------- #


class MemoryIdempotencyStore:
    """Registro em memoria, para teste de unidade e handler sem banco (secao 10.2).

    Reproduz as duas propriedades que importam do :class:`SqlIdempotencyStore`:

    1. **Exclusao mutua por chave** -- a segunda chamada com o mesmo ``(consumidor, message_id)``
       devolve ``False``, e o ``Consumer`` confirma a mensagem sem reexecutar o handler.
    2. **Reversibilidade** -- enquanto a "transacao" nao e confirmada, a marca fica **provisoria**.
       Sem isso, um teste de retentativa quebraria de forma sutil e cara de diagnosticar: o handler
       falharia na tentativa 1, a marca permaneceria e a tentativa 2 seria classificada como
       duplicata, escondendo o defeito que o teste existe para pegar (secao 4.6.3).

    A "transacao" e identificada pelo objeto ``session`` recebido em :meth:`marcar`. Use
    :meth:`transacao` para obter o mesmo enquadramento de ``async with session.begin()``::

        store = MemoryIdempotencyStore()
        async with store.transacao(sessao):
            if await store.marcar(sessao, envelope, consumidor):
                await handler(...)

    Passar ``session=None`` confirma a marca imediatamente -- atalho valido para testes que so
    exercitam a supressao de duplicata, e nao a reversao.

    **Nao e implementacao de producao.** Ela nao e transacional com o efeito de dominio e nao
    sobrevive ao reinicio do processo, que sao exatamente as duas garantias exigidas por R2.4;
    ver a comparacao de arranjos no cabecalho deste modulo.
    """

    __slots__ = ("_clock", "_confirmadas", "_provisorias")

    def __init__(self, *, clock: Clock | None = None) -> None:
        """Constroi o registro em memoria.

        Args:
            clock: Fonte de tempo injetada; alimenta ``processado_em`` de cada :class:`Marca`.
        """
        self._clock: Clock = clock if clock is not None else RealClock()
        self._confirmadas: dict[tuple[str, str], Marca] = {}
        self._provisorias: dict[int, dict[tuple[str, str], Marca]] = {}

    # -- protocolo ---------------------------------------------------------- #

    async def marcar(
        self, session: SessaoTransacional | None, envelope: Envelope, consumidor: str
    ) -> bool:
        """Reserva o par ``(consumidor, message_id)``.

        Args:
            session: Objeto que identifica a transacao. ``None`` confirma a marca de imediato.
            envelope: Envelope recebido.
            consumidor: ``"<servico>:<fila>"``.

        Returns:
            ``True`` se a reserva foi conquistada agora; ``False`` se a chave ja estava marcada,
            confirmada ou provisoria -- o equivalente em memoria do bloqueio no indice unico.
        """
        chave = (consumidor, envelope.message_id)
        if self._reservada(chave):
            return False
        marca = Marca.de_envelope(envelope, consumidor, agora=self._clock.now())
        if session is None:
            self._confirmadas[chave] = marca
        else:
            self._provisorias.setdefault(id(session), {})[chave] = marca
        return True

    async def ja_processado(
        self, session: SessaoTransacional | None, *, message_id: str, consumidor: str
    ) -> bool:
        """Consulta se a chave ja esta marcada, sem reservar nada.

        Args:
            session: Ignorado; presente para casar com :class:`IdempotencyStore`.
            message_id: ``message_id`` do Envelope.
            consumidor: ``"<servico>:<fila>"``.

        Returns:
            ``True`` se a marca existe, confirmada ou provisoria.
        """
        return self._reservada((consumidor, message_id))

    async def expurgar(self, session: SessaoTransacional | None, *, antes_de: datetime) -> int:
        """Remove marcas confirmadas anteriores ao corte.

        Args:
            session: Ignorado; presente para casar com :class:`IdempotencyStore`.
            antes_de: Corte; marcas com ``processado_em`` anterior sao removidas.

        Returns:
            Quantidade de marcas removidas.
        """
        corte = antes_de if antes_de.tzinfo is not None else antes_de.replace(tzinfo=UTC)
        vencidas = [c for c, m in self._confirmadas.items() if m.processado_em < corte]
        for chave in vencidas:
            del self._confirmadas[chave]
        return len(vencidas)

    # -- controle de transacao ---------------------------------------------- #

    @asynccontextmanager
    async def transacao(self, session: SessaoTransacional | None = None) -> AsyncIterator[None]:
        """Enquadra um bloco com a mesma semantica de ``async with session.begin()``.

        Confirma as marcas provisorias na saida normal e as reverte quando o bloco levanta --
        e o ``ROLLBACK`` que devolve a mensagem ao ciclo de retentativa com a marca desfeita.

        Args:
            session: Objeto que identifica a transacao; o mesmo passado a :meth:`marcar`.

        Yields:
            Nada; o valor util e o efeito de confirmacao ou reversao na saida.
        """
        try:
            yield
        except BaseException:
            self.reverter(session)
            raise
        self.confirmar(session)

    def confirmar(self, session: SessaoTransacional | None = None) -> int:
        """Promove as marcas provisorias de uma transacao a confirmadas (o ``COMMIT``).

        Args:
            session: Objeto que identifica a transacao.

        Returns:
            Quantidade de marcas confirmadas.
        """
        pendentes = self._provisorias.pop(id(session), {})
        self._confirmadas.update(pendentes)
        return len(pendentes)

    def reverter(self, session: SessaoTransacional | None = None) -> int:
        """Descarta as marcas provisorias de uma transacao (o ``ROLLBACK``).

        Args:
            session: Objeto que identifica a transacao.

        Returns:
            Quantidade de marcas descartadas.
        """
        return len(self._provisorias.pop(id(session), {}))

    # -- introspeccao para asserção de teste -------------------------------- #

    @property
    def marcas(self) -> Mapping[tuple[str, str], Marca]:
        """Marcas ja confirmadas, por chave ``(consumidor, message_id)``."""
        return dict(self._confirmadas)

    def limpar(self) -> None:
        """Esvazia o registro, confirmadas e provisorias."""
        self._confirmadas.clear()
        self._provisorias.clear()

    def __len__(self) -> int:
        """Conta as marcas confirmadas.

        Returns:
            Numero de marcas confirmadas.
        """
        return len(self._confirmadas)

    def __contains__(self, chave: object) -> bool:
        """Testa se uma chave ``(consumidor, message_id)`` esta reservada.

        Args:
            chave: Tupla ``(consumidor, message_id)``.

        Returns:
            ``True`` se a chave esta confirmada ou provisoria.
        """
        if not isinstance(chave, tuple) or len(chave) != 2:
            return False
        consumidor, message_id = chave
        return self._reservada((str(consumidor), str(message_id)))

    def __iter__(self) -> Iterator[tuple[str, str]]:
        """Itera as chaves confirmadas.

        Returns:
            Iterador de tuplas ``(consumidor, message_id)``.
        """
        return iter(dict(self._confirmadas))

    def _reservada(self, chave: tuple[str, str]) -> bool:
        """Diz se a chave esta confirmada ou provisoria em qualquer transacao."""
        if chave in self._confirmadas:
            return True
        return any(chave in pendentes for pendentes in self._provisorias.values())


# --------------------------------------------------------------------------- #
# API de modulo (a forma citada em 4.5.4 e 4.8.3)                               #
# --------------------------------------------------------------------------- #

_PADRAO: dict[str | None, SqlIdempotencyStore] = {}


def _store_padrao(schema: str | None) -> SqlIdempotencyStore:
    """Devolve o registro SQL memorizado para um *schema*."""
    existente = _PADRAO.get(schema)
    if existente is None:
        existente = SqlIdempotencyStore(schema=schema)
        _PADRAO[schema] = existente
    return existente


async def marcar(
    session: SessaoTransacional,
    envelope: Envelope,
    consumidor: str,
    *,
    schema: str | None = None,
) -> bool:
    """Retorna ``True`` se ESTA transacao conquistou o direito de processar a mensagem (R2.4).

    E a forma citada no *pipeline* do ``Consumer`` (secao 4.5.4)::

        novo = await idempotency.marcar(session, envelope, consumidor)
        if not novo:
            log.info("mensagem.duplicada", resultado="duplicada")
            await self.transport.ack(raw)     # confirma sem reexecutar o handler
            return

    Args:
        session: A **mesma** sessao em que o handler grava o efeito de dominio. A atomicidade entre
            marca e efeito e o requisito, nao um detalhe: ver a tabela de arranjos no cabecalho
            deste modulo (secao 4.8.4).
        envelope: Envelope recebido.
        consumidor: Identifica a assinatura, no formato ``"<servico>:<fila>"`` -- ver
            :func:`consumidor_de` e a secao 4.8.1.
        schema: *Schema* do servico, ex. ``"vitais"``. ``None`` usa o padrao da conexao.

    Returns:
        ``True`` na primeira vez; ``False`` quando outra transacao ja processou a mensagem.
    """
    return await _store_padrao(schema).marcar(session, envelope, consumidor)


async def ja_processado(
    session: SessaoTransacional,
    *,
    message_id: str,
    consumidor: str,
    schema: str | None = None,
) -> bool:
    """Consulta se a marca existe, sem reservar nada -- uso **diagnostico**, nunca decisorio.

    Ver :meth:`SqlIdempotencyStore.ja_processado` para o motivo: preceder o handler com esta
    consulta reintroduz a corrida *check-then-act* que a secao 4.8.3 descarta.

    Args:
        session: Sessao de leitura.
        message_id: ``message_id`` do Envelope.
        consumidor: ``"<servico>:<fila>"``.
        schema: *Schema* do servico.

    Returns:
        ``True`` se a marca ja esta gravada.
    """
    return await _store_padrao(schema).ja_processado(
        session, message_id=message_id, consumidor=consumidor
    )


async def expurgar(
    session: SessaoTransacional,
    *,
    antes_de: datetime | None = None,
    retencao: timedelta = RETENCAO_PADRAO,
    clock: Clock | None = None,
    schema: str | None = None,
) -> int:
    """Executa a purga da janela de retencao de 7 dias (secao 4.8.2).

    Args:
        session: Transacao do job de purga.
        antes_de: Corte explicito. ``None`` calcula ``clock.now() - retencao``.
        retencao: Janela de retencao; padrao :data:`RETENCAO_PADRAO` (7 dias).
        clock: Fonte de tempo injetada; ``None`` usa :class:`~hospitalmq.clock.RealClock`.
        schema: *Schema* do servico.

    Returns:
        Quantidade de linhas removidas.
    """
    relogio = clock if clock is not None else RealClock()
    corte = antes_de if antes_de is not None else relogio.now() - retencao
    return await _store_padrao(schema).expurgar(session, antes_de=corte)
