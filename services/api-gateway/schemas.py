"""Modelos Pydantic v2 de entrada e saida da borda HTTP -- a fonte do OpenAPI (design 8.2.3, 8.5).

Este modulo e **deliberadamente anemico**: descreve apenas a forma dos dados que entram e saem do
``api-gateway``. Nenhuma regra clinica mora aqui -- NEWS2, faixa fisiologica e decisao de internacao
vivem nos ``Servico_Consumidor`` (design 8.1). A validacao da borda e **estrutural** (tipo, formato,
faixa de sanidade larga); a faixa fisiologica estreita e aplicada pelo ``vitals-service`` (camada 2,
7.7.2), e a diferenca entre as duas faixas e o que permite disparar a DLQ pela API publica
(design 8.2.3).

Os nomes e dominios de valor seguem **exatamente** o DDL de 7.4.3 e a funcao pura de 7.6.1: o mesmo
vocabulario e reimportado pelo ``vitals-service`` como camada 2, e qualquer divergencia de nome fara
100 % das leituras irem para a DLQ por ``ValidationError`` (design 8.2.3).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

__all__ = [
    "AceiteResponse",
    "AltaRequest",
    "AltaResponse",
    "CredenciaisRequest",
    "InternacaoRequest",
    "InternacaoResponse",
    "NivelConsciencia",
    "PacienteProntuario",
    "PacienteRequest",
    "PacienteResponse",
    "ProblemDetails",
    "ProntuarioInternacao",
    "ProntuarioResponse",
    "SinaisVitaisRequest",
    "TokenResponse",
]

NivelConsciencia = Literal["A", "V", "P", "U"]
"""Escala AVPU -- identica a 7.6.1 e ao ``CHECK`` de 7.4.3 (design 8.2.3)."""

_PADRAO_LEITO = r"^[A-Z]{2,4}-\d{2}$"
"""Formato do codigo humano do leito, ex. ``UTI-03`` (design 8.2.3)."""


# --------------------------------------------------------------------------- #
# Autenticacao (endpoint POST /auth/token)                                     #
# --------------------------------------------------------------------------- #


class CredenciaisRequest(BaseModel):
    """Credenciais de login de uma pessoa (design 8.2.1, endpoint 1)."""

    model_config = ConfigDict(extra="forbid")

    usuario: Annotated[str, Field(min_length=1, max_length=120, examples=["enf.ana"])]
    senha: Annotated[str, Field(min_length=1, max_length=200, examples=["demo123"])]


class TokenResponse(BaseModel):
    """Token JWT emitido em ``POST /auth/token`` (design 8.2.1, 8.3.1)."""

    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: Annotated[int, Field(description="Validade do token em segundos.")]
    role: str


# --------------------------------------------------------------------------- #
# Sinais vitais (endpoint POST /sinais)                                        #
# --------------------------------------------------------------------------- #


class SinaisVitaisRequest(BaseModel):
    """Leitura de sinais vitais publicada por um monitor de beira-leito (design 8.2.3).

    ``temperatura`` e :class:`~decimal.Decimal` -- e nao ``float`` -- porque a tabela NEWS2 tem
    fronteiras em 36.0/36.1 e 38.0/38.1: em ponto flutuante binario ``36.1`` nao e representavel e a
    comparacao de fronteira pode inverter o ponto atribuido (design 8.2.3). O campo aceita a forma
    string (``"38.4"``) usada nos exemplos do Swagger.
    """

    model_config = ConfigDict(extra="forbid")

    leito_id: Annotated[str, Field(pattern=_PADRAO_LEITO, examples=["UTI-03"])]
    internacao_id: UUID | None = Field(
        default=None,
        description="Ausente: resolvido na borda a partir da projecao de leitos (design 8.2.5).",
    )
    frequencia_respiratoria: Annotated[int, Field(ge=0, le=80, examples=[18])]
    saturacao_o2: Annotated[int, Field(ge=0, le=100, examples=[97])]
    oxigenio_suplementar: bool
    temperatura: Annotated[
        Decimal,
        Field(ge=Decimal("25.0"), le=Decimal("45.0"), decimal_places=1, examples=["38.4"]),
    ]
    pressao_sistolica: Annotated[int, Field(ge=0, le=300, examples=[120])]
    frequencia_cardiaca: Annotated[int, Field(ge=0, le=250, examples=[88])]
    nivel_consciencia: NivelConsciencia
    coletado_em: AwareDatetime = Field(
        description="ISO-8601 com fuso; a janela temporal e verificada pelo vitals-service (7.7.1)."
    )


class AceiteResponse(BaseModel):
    """Aceite de telemetria: ``202 Accepted`` de ``POST /sinais`` (design 8.2.2).

    Devolve ``correlation_id`` (rastreio ponta a ponta, R5.3) **e** ``message_id`` (chave de
    idempotencia, R2.4) para que o avaliador siga o fluxo inteiro nos logs.
    """

    status: Literal["aceito"] = "aceito"
    tipo_evento: str = "sinais.coletados"
    message_id: str
    correlation_id: str
    recebido_em: datetime
    acompanhar_em: str | None = "/painel"


# --------------------------------------------------------------------------- #
# Paciente (endpoints POST /pacientes e GET /pacientes/{id}/prontuario)         #
# --------------------------------------------------------------------------- #


class PacienteRequest(BaseModel):
    """Cadastro de um paciente (design 8.2.4, operacao ``paciente.criar``)."""

    model_config = ConfigDict(extra="forbid")

    nome: Annotated[str, Field(min_length=2, max_length=200, examples=["Ana Souza"])]
    documento: Annotated[str, Field(min_length=1, max_length=100, examples=["123.456.789-00"])]
    data_nascimento: date = Field(examples=["1980-05-12"])
    sexo: Literal["M", "F", "O"]


class PacienteResponse(BaseModel):
    """Paciente criado, devolvido por ``POST /pacientes`` (design 8.2.1, endpoint 2)."""

    paciente_id: UUID
    nome: str
    criado_em: datetime


class PacienteProntuario(BaseModel):
    """Recorte do paciente exibido no prontuario (design 8.2.1, endpoint 7)."""

    paciente_id: UUID
    nome: str
    documento: str | None = None
    data_nascimento: date | None = None
    sexo: str | None = None
    criado_em: datetime | None = None


class ProntuarioInternacao(BaseModel):
    """Internacao atual do paciente no prontuario (design 8.2.1, endpoint 7)."""

    internacao_id: UUID
    paciente_id: UUID
    leito_id: str
    leito: str | None = None
    setor: str | None = None
    admitido_em: datetime
    alta_em: datetime | None = None
    motivo: str | None = None
    equipe: str | None = None
    ativa: bool = True


class ProntuarioResponse(BaseModel):
    """Prontuario consolidado devolvido por ``GET /pacientes/{id}/prontuario`` (design 8.2.1).

    ``paciente`` e ``internacao_atual`` vem do ``admission-service`` (RPC ``prontuario.consultar``);
    ``ultimos_sinais`` e um enriquecimento best-effort do ``vitals-service`` (``sinais.ultimos``),
    e ``alertas`` vem da projecao em memoria quando disponivel.
    """

    paciente: PacienteProntuario
    internacao_atual: ProntuarioInternacao | None = None
    leito: dict[str, Any] | None = None
    ultimos_sinais: list[dict[str, Any]] = Field(default_factory=list)
    alertas: list[dict[str, Any]] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Internacao (endpoints POST /internacoes e POST /internacoes/{id}/alta)        #
# --------------------------------------------------------------------------- #


class InternacaoRequest(BaseModel):
    """Admissao de um paciente em um leito (design 8.2.3, operacao ``paciente.admitir``)."""

    model_config = ConfigDict(extra="forbid")

    paciente_id: UUID
    leito_id: Annotated[str, Field(pattern=_PADRAO_LEITO, examples=["UTI-03"])]
    equipe_responsavel: Annotated[str, Field(min_length=1, max_length=60, examples=["Equipe A"])]
    motivo: Annotated[
        str, Field(min_length=3, max_length=200, examples=["Insuficiencia respiratoria aguda"])
    ]


class InternacaoResponse(BaseModel):
    """Internacao criada, devolvida por ``POST /internacoes`` (design 8.2.1, endpoint 3).

    ``leito_id`` e o **codigo humano** do leito (ex. ``UTI-03``), e nao o UUID -- e por esse codigo
    que a projecao em memoria e a resolucao de ``internacao_id`` de 8.2.5 indexam os cards.
    """

    internacao_id: UUID
    paciente_id: UUID
    leito_id: str
    setor: str
    admitido_em: datetime


class AltaRequest(BaseModel):
    """Registro de alta de uma internacao (design 8.2.3, operacao ``paciente.dar-alta``)."""

    model_config = ConfigDict(extra="forbid")

    motivo: Annotated[str, Field(min_length=1, max_length=200, examples=["Alta medica"])]
    observacoes: Annotated[str, Field(max_length=500)] = ""


class AltaResponse(BaseModel):
    """Desfecho de ``POST /internacoes/{id}/alta`` (design 8.2.1, endpoint 4)."""

    internacao_id: UUID
    leito_id: str
    alta_em: datetime | None = None
    leito_liberado: bool = True


# --------------------------------------------------------------------------- #
# RFC 7807 (design 8.4)                                                         #
# --------------------------------------------------------------------------- #


class ErroCampo(BaseModel):
    """Um campo ofensor de um ``422`` (extensao ``errors[]`` de 8.4)."""

    campo: str
    mensagem: str
    valor_recebido: Any | None = None


class ProblemDetails(BaseModel):
    """Corpo de erro ``application/problem+json`` (RFC 7807, design 8.4).

    E o ``response_model`` de toda resposta de erro anotada no OpenAPI. ``type`` e a **unica** parte
    que o cliente deve usar para decidir programaticamente o que fazer; ``detail`` e texto livre.
    """

    type: str = Field(examples=["https://hospitalmq.g3/problems/tempo-esgotado-no-rpc"])
    title: str = Field(examples=["Tempo esgotado na chamada ao servico"])
    status: int = Field(examples=[504])
    detail: str | None = None
    correlation_id: str | None = None
    errors: list[ErroCampo] | None = None
    retry_after_s: int | None = None
