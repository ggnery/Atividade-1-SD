"""Fabrica de ``AsyncEngine`` e ``async_sessionmaker`` compartilhada pelos servicos (secao 7.8.1).

O middleware ``hospitalmq`` recebe um ``session_factory`` **pronto** -- ele nao abre conexao com o
banco por conta propria (ver ``Consumer(session_factory=...)`` e ``RpcServer(sessao=...)``). Este
modulo e o lado da aplicacao que constroi esse *factory*: cria o :class:`AsyncEngine` sobre
``asyncpg`` a partir da variavel de ambiente ``DATABASE_URL`` e o :class:`async_sessionmaker`, com
os parametros justificados na tabela da secao 7.8.1 do design.

Cada processo de dominio (``vitals-service``, ``triage-service``, ``alert-service``,
``admission-service``, ``audit-service``) compartilha **um** engine -- o *pool* e por processo, e o
``schema`` de cada servico e resolvido por tabela (``__table_args__ = {"schema": ...}``). O
``api-gateway`` **nao** usa este modulo: ele nao toca o banco em nenhuma hipotese (secao 3.7).

Uso tipico no ``main.py`` de um servico::

    from services.comum.db import obter_engine, obter_sessionmaker

    engine = obter_engine()
    Sessao = obter_sessionmaker()
    # ... Sessao vira o session_factory do Consumer e a 'sessao' do RpcServer
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Final

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from hospitalmq.config import settings
from hospitalmq.errors import ConfigError

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from hospitalmq.config import Settings

__all__ = [
    "MAX_OVERFLOW_PADRAO",
    "POOL_RECYCLE_PADRAO_S",
    "checar_saude",
    "criar_engine",
    "criar_sessionmaker",
    "encerrar_engine",
    "obter_engine",
    "obter_sessionmaker",
]

MAX_OVERFLOW_PADRAO: Final[int] = 5
"""Folga de conexoes fora do fluxo de mensagens: purga, *relay* do outbox e rejeicao (7.8.1)."""

POOL_RECYCLE_PADRAO_S: Final[int] = 300
"""Idade maxima de uma conexao no *pool* antes do reciclo, em segundos (secao 7.8.1)."""

_TIMEOUT_SAUDE_PADRAO_S: Final[float] = 2.0
"""Teto do ``SELECT 1`` de saude; um banco lento nao pode travar o *health check*."""


def _normalizar_dsn(dsn: str) -> str:
    """Garante que a DSN use o *driver* assincrono ``asyncpg`` (secao 7.8.1).

    ``create_async_engine`` exige um dialeto async explicito: ``postgresql://`` (o *driver* sincrono
    ``psycopg2``) faria a criacao do engine falhar. Esta normalizacao aceita a forma curta comum em
    ``.env`` e a promove a ``postgresql+asyncpg://``, deixando intactas as DSNs que ja nomeiam um
    *driver* (``postgresql+asyncpg://``, ``sqlite+aiosqlite://`` da suite de integracao).

    Args:
        dsn: A DSN bruta lida do ambiente.

    Returns:
        A DSN pronta para :func:`create_async_engine`.
    """
    esquema, separador, resto = dsn.partition("://")
    if not separador:
        return dsn
    if "+" in esquema:
        return dsn
    if esquema in {"postgresql", "postgres"}:
        return f"postgresql+asyncpg://{resto}"
    return dsn


def criar_engine(
    dsn: str | None = None,
    *,
    prefetch: int | None = None,
    echo: bool = False,
) -> AsyncEngine:
    """Constroi o :class:`AsyncEngine` sobre ``asyncpg`` (secao 7.8.1).

    Os parametros do *pool* saem diretamente da tabela de justificativas da secao 7.8.1:
    ``pool_size`` igual ao ``prefetch`` (ate 10 *handlers* em voo por replica, cada um abrindo uma
    sessao), ``max_overflow`` de 5 para o trabalho fora do fluxo de mensagens, ``pool_pre_ping``
    para descartar conexao morta apos reinicio do PostgreSQL e ``pool_recycle`` de 300 s.

    Args:
        dsn: DSN do SQLAlchemy. ``None`` usa ``settings.database_url``; a ausencia dela e erro de
            configuracao, pois um servico de dominio sem banco nao pode marcar idempotencia.
        prefetch: Alvo de ``pool_size``. ``None`` usa ``settings.prefetch`` -- a ligacao direta
            entre a politica R2.6 do middleware e o dimensionamento do *pool* (secao 7.8.1).
        echo: Ecoa o SQL emitido; ``False`` em producao, util apenas na depuracao local.

    Returns:
        O engine assincrono, ainda sem conexao aberta -- o *pool* conecta sob demanda.

    Raises:
        ConfigError: Se nenhuma DSN for informada e ``DATABASE_URL`` estiver ausente do ambiente.
    """
    alvo = dsn if dsn is not None else settings.database_url
    if not alvo:
        raise ConfigError(
            "DATABASE_URL ausente: um servico de dominio precisa de banco para a marca de "
            "idempotencia e o outbox (secao 7.8.1)",
            variavel="DATABASE_URL",
        )
    tamanho_pool = prefetch if prefetch is not None else settings.prefetch
    return create_async_engine(
        _normalizar_dsn(alvo),
        pool_size=tamanho_pool,
        max_overflow=MAX_OVERFLOW_PADRAO,
        pool_pre_ping=True,
        pool_recycle=POOL_RECYCLE_PADRAO_S,
        echo=echo,
    )


def criar_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Constroi o ``async_sessionmaker`` que o middleware recebe como ``session_factory``.

    ``expire_on_commit=False`` e ``autoflush=False`` seguem a secao 7.8.1: apos o ``COMMIT`` o
    *handler* ainda le atributos do objeto para montar o payload do evento derivado, e com o padrao
    ``expire_on_commit=True`` cada leitura dispararia um ``SELECT`` novo -- em sessao async isso
    vira ``MissingGreenlet`` fora do contexto de ``await``. O *flush* e explicito para tornar
    previsivel o instante em que o ``UNIQUE`` de idempotencia e verificado.

    Args:
        engine: O :class:`AsyncEngine` ao qual as sessoes se ligam.

    Returns:
        O ``async_sessionmaker`` de :class:`AsyncSession`, pronto para ser passado a
        ``Consumer(session_factory=...)`` e ``RpcServer(sessao=...)``.
    """
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


async def checar_saude(engine: AsyncEngine, *, timeout_s: float = _TIMEOUT_SAUDE_PADRAO_S) -> bool:
    """Verifica a conexao com o banco executando ``SELECT 1`` (secao 8.6.1, R8.6).

    Usada pelo *readiness probe* ``/health/ready`` da fabrica de aplicacao. Nunca levanta: uma falha
    de conexao -- banco fora, *pool* esgotado, expiracao do prazo -- e traduzida em ``False``, que o
    endpoint transforma em ``503``. O prazo evita que um banco lento trave o *health check*
    justamente quando o sistema esta degradado.

    Args:
        engine: O engine a testar.
        timeout_s: Prazo maximo, em segundos, para a resposta do banco.

    Returns:
        ``True`` se o banco respondeu ``1`` dentro do prazo; ``False`` em qualquer falha.
    """
    try:
        async with engine.connect() as conexao:
            resultado = await asyncio.wait_for(conexao.execute(text("SELECT 1")), timeout=timeout_s)
            return resultado.scalar_one() == 1
    except (SQLAlchemyError, OSError, TimeoutError):
        return False


# --------------------------------------------------------------------------- #
# Singletons preguicosos por processo (secao 7.8.2)                            #
# --------------------------------------------------------------------------- #

_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def obter_engine(cfg: Settings | None = None) -> AsyncEngine:
    """Devolve o :class:`AsyncEngine` do processo, criando-o na primeira chamada.

    Um engine por processo, e nao por chamada: o *pool* de conexoes so faz sentido compartilhado.
    Idempotente -- chamadas seguintes devolvem o mesmo engine.

    Args:
        cfg: Configuracao de onde ler a DSN e o ``prefetch``. ``None`` usa a global ``settings``.

    Returns:
        O engine unico do processo.
    """
    global _engine
    if _engine is None:
        alvo = cfg if cfg is not None else settings
        _engine = criar_engine(alvo.database_url, prefetch=alvo.prefetch)
    return _engine


def obter_sessionmaker(cfg: Settings | None = None) -> async_sessionmaker[AsyncSession]:
    """Devolve o ``async_sessionmaker`` do processo, criando-o na primeira chamada.

    E o valor passado como ``session_factory`` ao :class:`~hospitalmq.consumer.Consumer` e como
    ``sessao`` ao :class:`~hospitalmq.rpc.RpcServer`. Ligado ao engine de :func:`obter_engine`.

    Args:
        cfg: Configuracao repassada a :func:`obter_engine` na primeira criacao.

    Returns:
        O ``async_sessionmaker`` unico do processo.
    """
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = criar_sessionmaker(obter_engine(cfg))
    return _sessionmaker


async def encerrar_engine() -> None:
    """Libera o *pool* de conexoes do engine do processo (encerramento gracioso).

    Chamada pelo *lifespan* da fabrica de aplicacao no *shutdown*. Idempotente: sem engine criado,
    nao faz nada; apos o descarte, uma nova chamada a :func:`obter_engine` recria o engine -- o
    que a suite de testes usa para isolar casos.
    """
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _sessionmaker = None
