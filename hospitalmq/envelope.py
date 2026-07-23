"""Envelope: o contrato de dados do HospitalMQ (R1.2).

**Nenhuma mensagem trafega sem ele.** O produtor entrega apenas ``tipo`` e ``payload``; os oito
campos restantes sao preenchidos pelo middleware, o que elimina de vez a classe de defeito "o
desenvolvedor esqueceu de propagar o ``correlation_id``" (R5.3).

O corpo em transito e **JSON codificado em UTF-8** (``content_type: application/json``),
serializado com ``ensure_ascii=False`` para preservar acentuacao e ``separators=(",", ":")`` para
reduzir bytes; ``datetime`` vira ISO-8601 com sufixo ``Z``.

A API publica e fechada e simetrica (secao 4.2.1): :meth:`Envelope.to_bytes` e
:meth:`Envelope.from_bytes` para a rede, :meth:`Envelope.derivar` para a mensagem-filha e
:meth:`Envelope.para_retentativa` para a proxima tentativa. **Nao existem** ``Envelope.novo`` nem
``Envelope.parse``: a criacao de um Envelope raiz e responsabilidade exclusiva do ``Publisher``,
que e quem conhece ``producer``, relogio e contexto de correlacao.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Final, Literal, get_args

from hospitalmq.clock import Clock, RealClock
from hospitalmq.errors import InvalidEnvelopeError

__all__ = [
    "CONTENT_ENCODING",
    "CONTENT_TYPE",
    "HEADER_ATTEMPT",
    "HEADER_DLQ_ERRO_TIPO",
    "HEADER_DLQ_MOTIVO",
    "HEADER_DLQ_SERVICO",
    "HEADER_VERSION",
    "PREFIXO_HEADER",
    "Envelope",
    "Identity",
    "TipoIdentidade",
    "nome_header",
]

TipoIdentidade = Literal["usuario", "dispositivo"]
"""Dominio **fechado** de ``identity.tipo`` (secao 4.2.1).

Exatamente dois valores. JWT de pessoa produz ``"usuario"``; API Key de dispositivo produz
``"dispositivo"`` (R4.5). Processo interno sem credencial -- *relay* do outbox, expurgo,
``redrive.py`` -- usa ``identity = None``, e **nao** um terceiro valor: inventar ``"sistema"``
faria a auditoria afirmar que alguem agiu quando ninguem agiu. O valor e gravado literalmente na
coluna ``identidade_tipo`` da trilha de auditoria, sujeita a um ``CHECK`` com estes dois valores.
"""

_TIPOS_IDENTIDADE: Final[frozenset[str]] = frozenset(get_args(TipoIdentidade))

PREFIXO_HEADER: Final[str] = "x-hmq-"
"""Prefixo unico de todo cabecalho AMQP escrito pelo middleware (secao 6.1.5).

As variantes ``x-hospitalmq-*`` e ``x-attempt`` nao existem no projeto. Um ``grep -r "x-hmq-"``
lista exaustivamente a superficie de metadados do HospitalMQ. Os cabecalhos ``x-death``,
``x-first-death-reason`` e ``x-message-ttl`` sao do **broker**, nao do middleware.
"""

HEADER_ATTEMPT: Final[str] = f"{PREFIXO_HEADER}attempt"
HEADER_VERSION: Final[str] = f"{PREFIXO_HEADER}version"
HEADER_DLQ_MOTIVO: Final[str] = f"{PREFIXO_HEADER}dlq-motivo"
HEADER_DLQ_SERVICO: Final[str] = f"{PREFIXO_HEADER}dlq-servico"
HEADER_DLQ_ERRO_TIPO: Final[str] = f"{PREFIXO_HEADER}dlq-erro-tipo"

CONTENT_TYPE: Final[str] = "application/json"
CONTENT_ENCODING: Final[str] = "utf-8"

_RELOGIO_PADRAO: Final[Clock] = RealClock()


def nome_header(sufixo: str) -> str:
    """Monta o nome canonico de um cabecalho do middleware.

    Args:
        sufixo: Parte final do nome, em minusculas e com hifen, ex. ``"dlq-motivo"``. Se ja vier
            prefixado com ``x-hmq-``, e devolvido inalterado.

    Returns:
        O nome completo do cabecalho, sempre com o prefixo ``x-hmq-`` (secao 6.1.5).
    """
    return sufixo if sufixo.startswith(PREFIXO_HEADER) else f"{PREFIXO_HEADER}{sufixo}"


def _iso_utc(momento: datetime) -> str:
    """Serializa um ``datetime`` como ISO-8601 UTC com sufixo ``Z``."""
    return momento.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _para_utc(momento: datetime) -> datetime:
    """Normaliza um ``datetime`` para UTC; um valor ingenuo e interpretado como UTC."""
    if momento.tzinfo is None:
        return momento.replace(tzinfo=UTC)
    return momento.astimezone(UTC)


def _exigir_str(dados: Mapping[str, Any], campo: str) -> str:
    """Le um campo obrigatorio de texto, ou levanta :class:`InvalidEnvelopeError`."""
    valor = dados.get(campo)
    if not isinstance(valor, str) or not valor:
        raise InvalidEnvelopeError(
            f"campo obrigatorio ausente ou invalido no Envelope: {campo!r}",
            campo=campo,
        )
    return valor


@dataclass(frozen=True, slots=True)
class Identity:
    """Quem originou a acao que produziu esta mensagem (R4.3, R4.6).

    Copiada do JWT ou da API Key ja validados na borda e propagada automaticamente por
    :meth:`Envelope.derivar`, de modo que o handler recebe a identidade **sem nova consulta ao
    servico de autenticacao** -- que e literalmente o texto de R4.6.

    Attributes:
        sub: *Claim* ``sub`` do JWT ou ``device_id`` da API Key.
        role: *Claim* ``role`` do JWT ou papel fixo do dispositivo. Alimenta a autorizacao (R4.4).
        tipo: ``"usuario"`` ou ``"dispositivo"`` -- ver :data:`TipoIdentidade` (R4.5).
    """

    sub: str
    role: str
    tipo: TipoIdentidade

    def to_dict(self) -> dict[str, str]:
        """Projeta a identidade no objeto JSON do Envelope.

        Returns:
            Dicionario com as chaves ``sub``, ``role`` e ``tipo``.
        """
        return {"sub": self.sub, "role": self.role, "tipo": self.tipo}

    @classmethod
    def from_dict(cls, dados: Mapping[str, Any]) -> Identity:
        """Reconstroi a identidade a partir do objeto JSON do Envelope.

        Args:
            dados: Objeto com ``sub``, ``role`` e ``tipo``.

        Returns:
            A identidade correspondente.

        Raises:
            InvalidEnvelopeError: Se faltar campo ou se ``tipo`` estiver fora do dominio fechado.
        """
        tipo = _exigir_str(dados, "tipo")
        if tipo not in _TIPOS_IDENTIDADE:
            raise InvalidEnvelopeError(
                f"identity.tipo fora do dominio fechado: {tipo!r}",
                campo="identity.tipo",
                aceitos=sorted(_TIPOS_IDENTIDADE),
            )
        return cls(
            sub=_exigir_str(dados, "sub"),
            role=_exigir_str(dados, "role"),
            tipo=tipo,  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class Envelope:
    """Os dez campos que toda mensagem do HospitalMQ carrega (R1.2).

    Imutavel por decisao de projeto (``frozen=True``): impede que um handler mal-comportado altere
    o ``correlation_id`` no meio do fluxo e quebre o rastreamento exigido por R5.3. ``slots=True``
    reduz memoria num servico que processa milhares de leituras por minuto. As duas unicas
    transformacoes legitimas sao :meth:`derivar` e :meth:`para_retentativa`, ambas explicitas e
    produzindo um novo objeto.

    Attributes:
        message_id: UUIDv4, identidade unica da mensagem. E a chave da idempotencia (R2.4) e
            **nao muda** entre retentativas nem no *redrive* da DLQ.
        correlation_id: UUIDv4 que amarra o fluxo de ponta a ponta; aparece em toda linha de log
            e em toda mensagem derivada (R5.1, R5.3).
        causation_id: ``message_id`` da mensagem que causou esta. ``None`` na raiz do fluxo.
            Reconstroi a **arvore** de causalidade, nao apenas o agrupamento.
        type: Tipo do evento. E tambem a *routing key* AMQP e a chave de despacho do ``Consumer``.
        version: Versao do *schema* do ``payload``, inteiro monotonico **por tipo de evento**
            (secao 4.2.4).
        timestamp: Instante da publicacao, sempre *aware* em UTC (R5.1).
        producer: Nome do servico que emitiu; alimenta a trilha de auditoria (R7.1).
        identity: Quem originou a acao, ou ``None`` para processo interno sem credencial (R4.3).
        attempt: Contagem de tentativas de entrega, ``>= 1``. Governa backoff e corte para DLQ
            (R2.2, R2.3).
        payload: Corpo do evento, serializavel em JSON e validado por modelo Pydantic no limite
            do handler.
    """

    message_id: str
    correlation_id: str
    causation_id: str | None
    type: str
    version: int
    timestamp: datetime
    producer: str
    identity: Identity | None
    attempt: int
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        """Normaliza o ``timestamp`` para UTC e valida o invariante ``attempt >= 1``.

        Raises:
            InvalidEnvelopeError: Se ``attempt`` for menor que 1 ou ``version`` menor que 1.
        """
        if self.attempt < 1:
            raise InvalidEnvelopeError(
                f"attempt deve ser >= 1, recebido {self.attempt}", campo="attempt"
            )
        if self.version < 1:
            raise InvalidEnvelopeError(
                f"version deve ser >= 1, recebido {self.version}", campo="version"
            )
        object.__setattr__(self, "timestamp", _para_utc(self.timestamp))

    # -- serializacao ------------------------------------------------------ #

    def to_dict(self) -> dict[str, Any]:
        """Projeta o Envelope no objeto JSON que trafega na rede.

        E tambem o formato gravado em coluna ``JSONB`` pelo *outbox* transacional (secao 4.9.2)
        e pela trilha de auditoria (R7.1).

        Returns:
            Dicionario com os dez campos, na ordem canonica do exemplo da secao 4.2.2.
        """
        return {
            "message_id": self.message_id,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "type": self.type,
            "version": self.version,
            "timestamp": _iso_utc(self.timestamp),
            "producer": self.producer,
            "identity": self.identity.to_dict() if self.identity is not None else None,
            "attempt": self.attempt,
            "payload": self.payload,
        }

    def to_bytes(self) -> bytes:
        """Serializa o Envelope como JSON UTF-8 (secao 4.2.3).

        Returns:
            O corpo pronto para o transporte, com ``content_type: application/json`` e
            ``content_encoding: utf-8``. Acentuacao e preservada sem *escaping* e nao ha espaco
            supérfluo entre os separadores.
        """
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":")).encode(
            CONTENT_ENCODING
        )

    @classmethod
    def from_dict(cls, dados: Mapping[str, Any]) -> Envelope:
        """Reconstroi o Envelope a partir do objeto JSON ja desserializado.

        Args:
            dados: Objeto com os dez campos do contrato.

        Returns:
            O Envelope correspondente.

        Raises:
            InvalidEnvelopeError: Se algum campo obrigatorio faltar, tiver tipo errado ou violar
                um invariante. Como a excecao tambem e ``ValueError`` e ``PermanentError``, o
                consumidor a envia direto a DLQ, sem consumir retentativas.
        """
        payload = dados.get("payload")
        if not isinstance(payload, dict):
            raise InvalidEnvelopeError("payload ausente ou nao e objeto JSON", campo="payload")

        bruto_identity = dados.get("identity")
        if bruto_identity is None:
            identity: Identity | None = None
        elif isinstance(bruto_identity, Mapping):
            identity = Identity.from_dict(bruto_identity)
        else:
            raise InvalidEnvelopeError("identity deve ser objeto ou null", campo="identity")

        causation = dados.get("causation_id")
        if causation is not None and not isinstance(causation, str):
            raise InvalidEnvelopeError("causation_id deve ser string ou null", campo="causation_id")

        try:
            version = int(dados["version"])
            attempt = int(dados["attempt"])
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidEnvelopeError(f"version/attempt invalidos: {exc}") from exc

        try:
            timestamp = datetime.fromisoformat(_exigir_str(dados, "timestamp"))
        except ValueError as exc:
            raise InvalidEnvelopeError(
                f"timestamp nao e ISO-8601: {exc}", campo="timestamp"
            ) from exc

        return cls(
            message_id=_exigir_str(dados, "message_id"),
            correlation_id=_exigir_str(dados, "correlation_id"),
            causation_id=causation,
            type=_exigir_str(dados, "type"),
            version=version,
            timestamp=timestamp,
            producer=_exigir_str(dados, "producer"),
            identity=identity,
            attempt=attempt,
            payload=payload,
        )

    @classmethod
    def from_bytes(cls, raw: bytes) -> Envelope:
        """Desserializa o corpo recebido do transporte (secao 4.2.3).

        Par simetrico de :meth:`to_bytes`. Chamado uma unica vez por mensagem, no ``Consumer``,
        no ``RpcClient`` e no ``redrive.py`` -- o transporte trafega ``bytes`` opacos e nunca
        conhece o Envelope (secao 4.3.2, invariante 2).

        Args:
            raw: Corpo bruto da mensagem, JSON em UTF-8.

        Returns:
            O Envelope reconstruido.

        Raises:
            InvalidEnvelopeError: Se o corpo nao for JSON UTF-8 valido ou nao satisfizer o
                contrato dos dez campos. E ``PermanentError`` e ``ValueError`` ao mesmo tempo:
                envelope ilegivel vai direto a DLQ, porque retentar falharia identicamente.
        """
        try:
            dados = json.loads(raw.decode(CONTENT_ENCODING))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidEnvelopeError(f"corpo nao e JSON UTF-8 valido: {exc}") from exc
        if not isinstance(dados, dict):
            raise InvalidEnvelopeError("corpo JSON nao e um objeto")
        return cls.from_dict(dados)

    # -- transformacoes legitimas ------------------------------------------ #

    def derivar(
        self,
        *,
        tipo: str,
        payload: Mapping[str, Any],
        producer: str,
        version: int = 1,
        message_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> Envelope:
        """Cria a mensagem-filha desta, preservando a cadeia de causalidade (R5.3).

        Novo ``message_id``, **mesmo** ``correlation_id``, ``causation_id`` igual ao
        ``message_id`` desta mensagem, ``identity`` herdada e ``attempt`` reiniciado em 1.

        E o **unico** caminho para publicar de dentro de um handler: ``MessageContext`` nao expoe
        o ``Publisher``, expoe ``ctx.emitir`` e ``ctx.emitir_no_outbox``, e ambos passam por aqui.
        A propagacao do ``correlation_id`` deixa de depender de o desenvolvedor lembrar.

        Args:
            tipo: Tipo do evento-filho, ex. ``"sinais.registrados"``. Vira a *routing key*.
            payload: Corpo do evento-filho, serializavel em JSON.
            producer: Nome do servico que esta emitindo o filho.
            version: Versao do *schema* do ``payload`` do filho (secao 4.2.4).
            message_id: Identidade do filho. Informado apenas pelo *outbox*, que precisa reusar o
                ``message_id`` ja gravado na transacao; caso contrario, um UUIDv4 novo.
            timestamp: Instante de criacao. Informado pelo chamador que possui um :class:`Clock`
                injetado; caso contrario, o relogio real.

        Returns:
            O Envelope filho.
        """
        return Envelope(
            message_id=message_id or str(uuid.uuid4()),
            correlation_id=self.correlation_id,
            causation_id=self.message_id,
            type=tipo,
            version=version,
            timestamp=timestamp if timestamp is not None else _RELOGIO_PADRAO.now(),
            producer=producer,
            identity=self.identity,
            attempt=1,
            payload=dict(payload),
        )

    def para_retentativa(self) -> Envelope:
        """Devolve a copia desta mensagem com ``attempt`` incrementado (R2.2).

        Tudo o mais e preservado -- em especial o ``message_id``, que **nao muda entre
        retentativas**, porque e ele a chave da idempotencia (R2.4). E assim que ``attempt``
        sobrevive a viagem pela fila de espera com TTL: o contador viaja dentro do corpo, nao na
        memoria do consumidor (secao 4.6.3).

        Returns:
            Novo Envelope, identico, com ``attempt = self.attempt + 1``.
        """
        return replace(self, attempt=self.attempt + 1)

    # -- cabecalhos AMQP --------------------------------------------------- #

    def to_headers(self, **extras: object) -> dict[str, object]:
        """Monta os cabecalhos AMQP espelhados do Envelope (secao 4.2.2).

        A redundancia e deliberada: permite inspecionar e filtrar mensagens na interface de
        *management* do RabbitMQ **sem desserializar o corpo**. ``message_id``, ``correlation_id``
        e ``type`` viajam nas propriedades **nativas** do AMQP e por isso nao se repetem aqui.

        Args:
            **extras: Cabecalhos adicionais do middleware, por exemplo
                ``dlq_motivo="canal indisponivel"``. Sublinhados viram hifens e o prefixo
                ``x-hmq-`` e aplicado automaticamente quando ausente.

        Returns:
            Dicionario pronto para o argumento ``headers`` de ``Transport.publish``.
        """
        headers: dict[str, object] = {
            HEADER_ATTEMPT: self.attempt,
            HEADER_VERSION: self.version,
        }
        for chave, valor in extras.items():
            headers[nome_header(chave.replace("_", "-"))] = valor
        return headers

    @staticmethod
    def attempt_de(headers: Mapping[str, Any], *, padrao: int = 1) -> int:
        """Le ``x-hmq-attempt`` sem desserializar o corpo da mensagem.

        Args:
            headers: Cabecalhos da :class:`~hospitalmq.transport.base.InboundMessage`.
            padrao: Valor devolvido quando o cabecalho esta ausente ou ilegivel.

        Returns:
            A tentativa corrente, ``>= 1``.
        """
        return _inteiro_de(headers, HEADER_ATTEMPT, padrao)

    @staticmethod
    def version_de(headers: Mapping[str, Any], *, padrao: int = 1) -> int:
        """Le ``x-hmq-version`` sem desserializar o corpo da mensagem.

        Args:
            headers: Cabecalhos da :class:`~hospitalmq.transport.base.InboundMessage`.
            padrao: Valor devolvido quando o cabecalho esta ausente ou ilegivel.

        Returns:
            A versao do *schema* do ``payload``.
        """
        return _inteiro_de(headers, HEADER_VERSION, padrao)


def _inteiro_de(headers: Mapping[str, Any], chave: str, padrao: int) -> int:
    """Le um cabecalho inteiro tolerando ausencia, texto e tipos nativos do broker."""
    bruto = headers.get(chave)
    if bruto is None:
        return padrao
    try:
        return int(bruto)
    except (TypeError, ValueError):
        return padrao
