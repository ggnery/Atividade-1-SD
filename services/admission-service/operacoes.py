"""Operacoes RPC do ``admission-service`` -- as cinco de ``q.rpc.admission`` (design 5.8).

O ``admission-service`` e o unico serviço com RPC **e** outbox. Ele responde por cinco operacoes do
catalogo canonico de 5.8.1, o mesmo dicionario ``ROTAS_RPC`` do middleware
(:data:`hospitalmq.rpc.ROTAS_RPC`):

* **Escritas** (``escrita=True``, protegidas pela idempotencia de 5.6.5): ``paciente.criar``,
  ``paciente.admitir``, ``paciente.dar-alta``. Cada uma grava o dominio e os eventos no outbox na
  **mesma transacao** da marca de idempotencia, via :func:`outbox.sessao_rpc_atual`.
* **Leituras** (``escrita=False``): ``prontuario.consultar`` (emite ``prontuario.consultado`` para a
  Trilha_de_Auditoria, R7.3) e ``leitos.snapshot`` (hidratacao do Painel_de_Leitos no *boot* do
  gateway, R11.7). Ambas abrem sessao efemera propria.

Eventos emitidos (design 6.4 / 7.2.4): admissao publica ``paciente.admitido`` + ``leito.ocupado``;
alta publica ``paciente.alta`` + ``leito.liberado``; a consulta de prontuario publica
``prontuario.consultado``. Os dois primeiros pares vao pelo **outbox** (perda = inconsistencia
permanente); ``prontuario.consultado`` vai direto pelo :class:`~hospitalmq.publisher.Publisher`,
fora do caminho critico, porque a auditoria de uma leitura nao pode derrubar a leitura (5.8.3).

Nota de contrato (registrada em ``divergencias``): as operacoes seguem os nomes de 5.8 e do
``ROTAS_RPC`` congelado -- ``paciente.admitir``/``paciente.dar-alta``/``leitos.snapshot`` -- e nao
os nomes ``internacao.admitir``/``internacao.dar-alta``/``leito.listar`` de 8.2.4, que 5.8.3
retira do catalogo. As respostas seguem ``{"<agregado>": {...}, "repetida": bool}`` (5.6.1/5.6.5).
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, Literal

import outbox
import repositorio
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy.exc import IntegrityError

from hospitalmq.clock import Clock, RealClock
from hospitalmq.errors import PermanentError, TransportError
from hospitalmq.logging import get_logger

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    from modelos import Internacao, Leito, Paciente
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from hospitalmq.publisher import Publisher
    from hospitalmq.rpc import ContextoRpc, RpcServer

_log = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Papeis autorizados por operacao (design 5.8.1, defesa em profundidade R4.6)   #
# --------------------------------------------------------------------------- #

_PAPEIS_CRIAR: frozenset[str] = frozenset({"enfermeiro", "admin"})
_PAPEIS_ADMITIR: frozenset[str] = frozenset({"enfermeiro", "admin"})
_PAPEIS_DAR_ALTA: frozenset[str] = frozenset({"medico", "admin"})
_PAPEIS_PRONTUARIO: frozenset[str] = frozenset({"enfermeiro", "medico"})
_PAPEIS_SNAPSHOT: frozenset[str] = frozenset({"servico"})

_CODIGO_LEITO = re.compile(r"^[A-Z]{3}-[0-9]{2}$")
"""Padrao do codigo humano do leito (ex. ``UTI-03``), identico ao ``ck_leitos_codigo`` do banco."""

# Nome do indice/constraint violado -> (codigo de erro do contrato, mensagem). Design 5.7.
_ERROS_INTEGRIDADE: dict[str, tuple[str, str]] = {
    "uq_pacientes_documento": ("DOCUMENTO_DUPLICADO", "documento ja cadastrado"),
    "uq_internacoes_leito_ativo": ("LEITO_OCUPADO", "leito ja possui internacao ativa"),
    "uq_internacoes_paciente_ativo": (
        "PACIENTE_JA_INTERNADO",
        "paciente ja possui internacao ativa",
    ),
}


# --------------------------------------------------------------------------- #
# Modelos de entrada (validacao de payload no handler, design 5.7 PAYLOAD_INVALIDO) #
# --------------------------------------------------------------------------- #


class _EntradaCriarPaciente(BaseModel):
    """Payload de ``paciente.criar`` (design 5.8.1)."""

    model_config = ConfigDict(extra="ignore")
    # Limites alinhados aos CHECK do banco (ck_pacientes_nome 2..200) para que dado curto vire
    # PAYLOAD_INVALIDO na borda do handler, e nao IntegrityError -> ERRO_INTERNO no flush.
    nome: str = Field(min_length=2, max_length=200)
    documento: str = Field(min_length=1, max_length=100)
    data_nascimento: date
    sexo: Literal["M", "F", "O"]


class _EntradaAdmitir(BaseModel):
    """Payload de ``paciente.admitir`` (design 5.8.1)."""

    model_config = ConfigDict(extra="ignore")
    paciente_id: str = Field(min_length=1)
    leito_id: str = Field(min_length=1)
    # motivo e persistido em clinico.internacoes (ck_internacoes_motivo 3..500).
    motivo: str = Field(min_length=3, max_length=500)
    equipe_responsavel: str = Field(min_length=1)


class _EntradaDarAlta(BaseModel):
    """Payload de ``paciente.dar-alta`` (design 5.8.1)."""

    model_config = ConfigDict(extra="ignore")
    internacao_id: str = Field(min_length=1)
    motivo: str = Field(min_length=1, max_length=500)
    observacoes: str | None = None


class _EntradaProntuario(BaseModel):
    """Payload de ``prontuario.consultar`` (design 5.8.1)."""

    model_config = ConfigDict(extra="ignore")
    paciente_id: str = Field(min_length=1)


class _EntradaSnapshot(BaseModel):
    """Payload de ``leitos.snapshot`` (design 5.8.1)."""

    model_config = ConfigDict(extra="ignore")
    apenas_ocupados: bool = False


# --------------------------------------------------------------------------- #
# Auxiliares                                                                     #
# --------------------------------------------------------------------------- #


def _iso(momento: datetime) -> str:
    """Serializa um ``datetime`` como ISO-8601 UTC com sufixo ``Z`` (formato do Envelope)."""
    return momento.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _validar[M: BaseModel](modelo: type[M], payload: dict[str, Any]) -> M:
    """Valida o payload no modelo Pydantic, ou levanta ``PAYLOAD_INVALIDO`` (design 5.7).

    Raises:
        PermanentError: Com codigo ``PAYLOAD_INVALIDO`` quando o payload nao valida -- vai a
            resposta RPC de erro que o gateway traduz em ``422``.
    """
    try:
        return modelo.model_validate(payload)
    except ValidationError as exc:
        campos = ", ".join(".".join(str(p) for p in erro["loc"]) for erro in exc.errors())
        raise PermanentError(f"payload invalido: {campos}", codigo="PAYLOAD_INVALIDO") from exc


def _uuid(valor: str, *, campo: str) -> uuid.UUID:
    """Converte um identificador textual em ``UUID``, ou levanta ``PAYLOAD_INVALIDO``.

    Raises:
        PermanentError: Com codigo ``PAYLOAD_INVALIDO`` se ``valor`` nao for um UUID.
    """
    try:
        return uuid.UUID(valor)
    except (ValueError, AttributeError, TypeError) as exc:
        raise PermanentError(
            f"{campo} nao e um identificador valido: {valor!r}", codigo="PAYLOAD_INVALIDO"
        ) from exc


async def _resolver_leito(session: AsyncSession, referencia: str) -> Leito:
    """Resolve o leito por codigo (ex. ``UTI-03``) ou por ``UUID``, exigindo que exista e ativo.

    Aceita as duas formas porque 5.8.1 ilustra ``leito_id`` com o codigo humano (``UTI-03``)
    enquanto o banco usa ``UUID``; qual delas a borda envia e decisao do gateway.

    Raises:
        PermanentError: ``PAYLOAD_INVALIDO`` se a referencia nao e codigo nem UUID;
            ``LEITO_NAO_ENCONTRADO`` se nao existe ou esta inativo.
    """
    if _CODIGO_LEITO.match(referencia):
        leito = await repositorio.buscar_leito_por_codigo(session, referencia)
    else:
        leito = await repositorio.buscar_leito_por_id(session, _uuid(referencia, campo="leito_id"))
    if leito is None or not leito.ativo:
        raise PermanentError(
            f"leito nao encontrado ou inativo: {referencia!r}", codigo="LEITO_NAO_ENCONTRADO"
        )
    return leito


def _integridade_para_erro(exc: IntegrityError) -> PermanentError:
    """Traduz uma violacao de unicidade no codigo de erro do contrato (design 5.7).

    O nome do indice/constraint violado aparece no texto da ``IntegrityError`` (ex. ``duplicate key
    value violates unique constraint "uq_internacoes_leito_ativo"``); casa-se por esse nome.
    """
    texto = f"{getattr(exc, 'orig', '')} {exc}"
    for chave, (codigo, mensagem) in _ERROS_INTEGRIDADE.items():
        if chave in texto:
            return PermanentError(mensagem, codigo=codigo)
    return PermanentError("violacao de integridade nao mapeada", codigo="ERRO_INTERNO")


# -- projecoes de agregado em dicionario JSON-nativo (str/num/bool/None) ----- #


def _dict_paciente(paciente: Paciente) -> dict[str, Any]:
    """Projeta um :class:`Paciente` no objeto de resposta (design 5.8.1)."""
    return {
        "paciente_id": str(paciente.id),
        "nome": paciente.nome,
        "documento": paciente.documento,
        "data_nascimento": paciente.data_nascimento.isoformat(),
        "sexo": paciente.sexo,
        "criado_em": _iso(paciente.criado_em),
    }


def _dict_internacao(internacao: Internacao, leito: Leito | None) -> dict[str, Any]:
    """Projeta uma :class:`Internacao` (e seu leito) no objeto de resposta (design 5.8.1)."""
    return {
        "internacao_id": str(internacao.id),
        "paciente_id": str(internacao.paciente_id),
        "leito_id": str(internacao.leito_id),
        "leito": leito.codigo if leito is not None else None,
        "setor": leito.setor if leito is not None else None,
        "admitido_em": _iso(internacao.admitido_em),
        "alta_em": _iso(internacao.alta_em) if internacao.alta_em is not None else None,
        "motivo": internacao.motivo,
        "equipe": internacao.equipe,
        "ativa": internacao.esta_ativa,
    }


def _dict_alta(internacao: Internacao, leito: Leito | None) -> dict[str, Any]:
    """Projeta o desfecho de ``paciente.dar-alta`` (design 5.8.1)."""
    codigo = leito.codigo if leito is not None else None
    return {
        "internacao_id": str(internacao.id),
        "leito_id": str(internacao.leito_id),
        "leito": codigo,
        "leito_liberado": codigo,
        "alta_em": _iso(internacao.alta_em) if internacao.alta_em is not None else None,
    }


def _dict_leito(leito: Leito) -> dict[str, Any]:
    """Projeta um :class:`Leito` no objeto de resposta do prontuario (design 5.8.1)."""
    return {
        "leito_id": str(leito.id),
        "codigo": leito.codigo,
        "setor": leito.setor,
        "ativo": leito.ativo,
    }


# --------------------------------------------------------------------------- #
# Registro das operacoes                                                        #
# --------------------------------------------------------------------------- #


def registrar_operacoes(
    servidor: RpcServer,
    *,
    sessionmaker: async_sessionmaker[AsyncSession],
    publisher: Publisher,
    clock: Clock | None = None,
) -> None:
    """Registra as cinco operacoes do ``admission-service`` no :class:`~hospitalmq.rpc.RpcServer`.

    Args:
        servidor: O ``RpcServer`` que consome ``q.rpc.admission``. As escritas exigem que ele tenha
            sido construido com ``sessao=`` (a :class:`~outbox.FabricaSessaoRpc`) e
            ``escrita=True``.
        sessionmaker: ``async_sessionmaker`` real do processo, usado pelas leituras (efemeras).
        publisher: :class:`Publisher` para o evento ``prontuario.consultado`` (fora do outbox).
        clock: Fonte de tempo injetada para carimbar eventos e respostas. Padrao :class:`RealClock`.
    """
    relogio = clock if clock is not None else RealClock()

    # -- escrita: paciente.criar ---------------------------------------------- #

    @servidor.operacao("paciente.criar", roles=_PAPEIS_CRIAR, escrita=True)
    async def criar_paciente(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
        """Cria um paciente. Chave natural ``documento`` (design 5.6.5, 5.8.1)."""
        dados = _validar(_EntradaCriarPaciente, payload)
        session = outbox.sessao_rpc_atual()

        if ctx.repetida:
            existente = await repositorio.buscar_paciente_por_documento(session, dados.documento)
            if existente is not None:
                return {"paciente": _dict_paciente(existente), "repetida": True}

        try:
            paciente = await repositorio.criar_paciente(
                session,
                nome=dados.nome,
                documento=dados.documento,
                data_nascimento=dados.data_nascimento,
                sexo=dados.sexo,
            )
        except IntegrityError as exc:
            raise _integridade_para_erro(exc) from exc

        return {"paciente": _dict_paciente(paciente), "repetida": False}

    # -- escrita: paciente.admitir -------------------------------------------- #

    @servidor.operacao("paciente.admitir", roles=_PAPEIS_ADMITIR, escrita=True)
    async def admitir(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
        """Admite um paciente num leito. Emite ``paciente.admitido`` + ``leito.ocupado`` (7.2.4).

        Barreira primaria de INV-1/INV-6 e o indice unico parcial do banco; a ``IntegrityError`` da
        ``LEITO_OCUPADO``/``PACIENTE_JA_INTERNADO`` (design 5.6.5, 5.7).
        """
        dados = _validar(_EntradaAdmitir, payload)
        session = outbox.sessao_rpc_atual()
        paciente_id = _uuid(dados.paciente_id, campo="paciente_id")

        if ctx.repetida:
            ativa = await repositorio.internacao_ativa_de_paciente(session, paciente_id)
            if ativa is not None:
                leito_ativo = await repositorio.buscar_leito_por_id(session, ativa.leito_id)
                return {"internacao": _dict_internacao(ativa, leito_ativo), "repetida": True}

        paciente = await repositorio.buscar_paciente_por_id(session, paciente_id)
        if paciente is None:
            raise PermanentError(
                f"paciente {dados.paciente_id} inexistente", codigo="PACIENTE_NAO_ENCONTRADO"
            )
        leito = await _resolver_leito(session, dados.leito_id)

        try:
            internacao = await repositorio.criar_internacao(
                session,
                paciente_id=paciente.id,
                leito_id=leito.id,
                motivo=dados.motivo,
                equipe=dados.equipe_responsavel,
            )
        except IntegrityError as exc:
            raise _integridade_para_erro(exc) from exc

        admitido_iso = _iso(internacao.admitido_em)
        await outbox.gravar(
            session,
            tipo="paciente.admitido",
            payload={
                "internacao_id": str(internacao.id),
                "paciente_id": str(paciente.id),
                "nome": paciente.nome,
                "leito_id": str(leito.id),
                "admitido_em": admitido_iso,
            },
            correlation_id=ctx.correlation_id,
            causation_id=ctx.rpc_id,
            identity=ctx.identity,
            clock=relogio,
        )
        await outbox.gravar(
            session,
            tipo="leito.ocupado",
            payload={
                "leito_id": str(leito.id),
                "internacao_id": str(internacao.id),
                "setor": leito.setor,
            },
            correlation_id=ctx.correlation_id,
            causation_id=ctx.rpc_id,
            identity=ctx.identity,
            clock=relogio,
        )
        return {"internacao": _dict_internacao(internacao, leito), "repetida": False}

    # -- escrita: paciente.dar-alta ------------------------------------------- #

    @servidor.operacao("paciente.dar-alta", roles=_PAPEIS_DAR_ALTA, escrita=True)
    async def dar_alta(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
        """Registra alta de uma internacao. Emite ``paciente.alta`` + ``leito.liberado`` (7.2.4)."""
        dados = _validar(_EntradaDarAlta, payload)
        session = outbox.sessao_rpc_atual()
        internacao_id = _uuid(dados.internacao_id, campo="internacao_id")

        if ctx.repetida:
            anterior = await repositorio.buscar_internacao(session, internacao_id)
            if anterior is not None and not anterior.esta_ativa:
                leito_ant = await repositorio.buscar_leito_por_id(session, anterior.leito_id)
                return {"internacao": _dict_alta(anterior, leito_ant), "repetida": True}

        existente = await repositorio.buscar_internacao(session, internacao_id)
        if existente is None:
            raise PermanentError(
                f"internacao {dados.internacao_id} inexistente",
                codigo="INTERNACAO_NAO_ENCONTRADA",
            )

        internacao = await repositorio.encerrar_internacao(session, internacao_id, relogio.now())
        if internacao is None:
            raise PermanentError(
                f"internacao {dados.internacao_id} ja encerrada", codigo="INTERNACAO_JA_ENCERRADA"
            )

        leito = await repositorio.buscar_leito_por_id(session, internacao.leito_id)
        alta_iso = _iso(internacao.alta_em) if internacao.alta_em is not None else None
        await outbox.gravar(
            session,
            tipo="paciente.alta",
            payload={
                "internacao_id": str(internacao.id),
                "leito_id": str(internacao.leito_id),
                "motivo": dados.motivo,
                "observacoes": dados.observacoes,
                "alta_em": alta_iso,
            },
            correlation_id=ctx.correlation_id,
            causation_id=ctx.rpc_id,
            identity=ctx.identity,
            clock=relogio,
        )
        await outbox.gravar(
            session,
            tipo="leito.liberado",
            payload={
                "leito_id": str(internacao.leito_id),
                "liberado_em": alta_iso,
            },
            correlation_id=ctx.correlation_id,
            causation_id=ctx.rpc_id,
            identity=ctx.identity,
            clock=relogio,
        )
        return {"internacao": _dict_alta(internacao, leito), "repetida": False}

    # -- leitura: prontuario.consultar ---------------------------------------- #

    @servidor.operacao("prontuario.consultar", roles=_PAPEIS_PRONTUARIO)
    async def consultar_prontuario(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
        """Retorna paciente + ultima internacao + leito, e audita o acesso (design 5.8.1, R7.3)."""
        dados = _validar(_EntradaProntuario, payload)
        paciente_id = _uuid(dados.paciente_id, campo="paciente_id")

        async with sessionmaker() as session:
            paciente = await repositorio.buscar_paciente_por_id(session, paciente_id)
            if paciente is None:
                raise PermanentError(
                    f"paciente {dados.paciente_id} inexistente", codigo="PACIENTE_NAO_ENCONTRADO"
                )
            internacao = await repositorio.internacao_mais_recente_de_paciente(session, paciente_id)
            leito = (
                await repositorio.buscar_leito_por_id(session, internacao.leito_id)
                if internacao is not None
                else None
            )
            resultado = {
                "paciente": _dict_paciente(paciente),
                "internacao": _dict_internacao(internacao, leito)
                if internacao is not None
                else None,
                "leito": _dict_leito(leito) if leito is not None else None,
            }

        await _emitir_prontuario_consultado(publisher, ctx, dados.paciente_id, relogio)
        return resultado

    # -- leitura: leitos.snapshot --------------------------------------------- #

    @servidor.operacao("leitos.snapshot", roles=_PAPEIS_SNAPSHOT)
    async def snapshot(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
        """Retorna o estado dos leitos para hidratar o Painel_de_Leitos no *boot* (design 5.8.2)."""
        dados = _validar(_EntradaSnapshot, payload)
        async with sessionmaker() as session:
            linhas = await repositorio.listar_leitos(session, apenas_ocupados=dados.apenas_ocupados)
            leitos = [
                {
                    "leito_id": str(leito.id),
                    "codigo": leito.codigo,
                    "setor": leito.setor,
                    "estado": "ocupado" if internacao is not None else "livre",
                    "internacao_id": str(internacao.id) if internacao is not None else None,
                    "paciente_id": str(paciente.id) if paciente is not None else None,
                    "paciente_nome": paciente.nome if paciente is not None else None,
                }
                for leito, internacao, paciente in linhas
            ]
        return {"leitos": leitos, "gerado_em": _iso(relogio.now())}


async def _emitir_prontuario_consultado(
    publisher: Publisher, ctx: ContextoRpc, paciente_id: str, clock: Clock
) -> None:
    """Publica ``prontuario.consultado`` para a Trilha_de_Auditoria (R7.3), fora do caminho critico.

    Falha de publicacao **nao** derruba a consulta (design 5.8.3): a auditoria de uma leitura nao
    pode invalidar a leitura em si. A ausencia de contexto de log no ``RpcServer`` exige propagar a
    correlacao e a identidade explicitamente.
    """
    if ctx.identity is None:
        return
    try:
        await publisher.publish(
            "prontuario.consultado",
            {
                "paciente_id": paciente_id,
                "consultado_por": ctx.identity.sub,
                "role": ctx.identity.role,
                "consultado_em": _iso(clock.now()),
            },
            correlation_id=ctx.correlation_id,
            causation_id=ctx.rpc_id,
            identity=ctx.identity,
        )
    except TransportError as exc:
        _log.error(
            "prontuario.consultado_nao_publicado",
            paciente_id=paciente_id,
            erro=type(exc).__name__,
            detalhe=str(exc),
        )
