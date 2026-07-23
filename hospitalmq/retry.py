"""Politica de retentativa, decisao retentativa x DLQ e enriquecimento do descarte.

Secoes 4.6 e 4.7 do design -- R2.2, R2.3.

Backoff exponencial de **1 s, 2 s e 4 s**, com no maximo **3 retentativas**, ou seja ate quatro
entregas no total (secao 4.6.1):

===================  ==========================  ==================================  ==========
``attempt`` entregue  Falha transitoria leva a    Fila de espera usada                Latencia
===================  ==========================  ==================================  ==========
1                     Retentativa 1 (espera 1 s)  ``q.<nome>.retry.1s``               1 s
2                     Retentativa 2 (espera 2 s)  ``q.<nome>.retry.2s``               3 s
3                     Retentativa 3 (espera 4 s)  ``q.<nome>.retry.4s``               7 s
4                     **Esgotado -> DLQ**         --                                  7 s
===================  ==========================  ==================================  ==========

Tres invariantes deste modulo, todos consequencia direta da secao 4.6:

1. **A espera nunca acontece dentro do processo.** Este modulo *calcula* o atraso e *escolhe* a
   fila de espera; quem materializa a espera e o broker, via ``x-message-ttl`` da fila
   ``q.<nome>.retry.Ns`` mais *dead-lettering* de volta a fila de origem (secao 4.6.2). Nao ha
   ``time.sleep`` nem ``asyncio.sleep`` aqui: dormir seguraria um dos dez espacos de ``prefetch``
   (R2.6), perderia o contador em memoria a cada reinicio e colidiria com o ``consumer_timeout``
   do RabbitMQ (secao 4.6.4). O unico ponto que suspende e
   :meth:`PoliticaRetentativa.aguardar`, que existe apenas para transportes sem fila de espera
   nativa (o ``MemoryTransport`` dos testes) e usa o :class:`~hospitalmq.clock.Clock` injetado.
2. **O contador vive na mensagem, nao no consumidor.** ``attempt`` e campo do Envelope; a
   republicacao usa :meth:`~hospitalmq.envelope.Envelope.para_retentativa`, que incrementa
   ``attempt`` e **preserva o** ``message_id`` -- e por isso que a marca de idempotencia so pode
   ser gravada em caso de sucesso (secao 4.6.3 e :mod:`hospitalmq.idempotency`).
3. **A classificacao do erro e do handler; a decisao de retentar e do middleware.** O handler
   escolhe entre :class:`~hospitalmq.errors.TransientError` e
   :class:`~hospitalmq.errors.PermanentError`; :func:`classificar` e
   :meth:`PoliticaRetentativa.decidir` fazem o resto. Nenhum servico conta tentativas.

Requisitos atendidos: R2.2, R2.3, R5.1 (campos ``espera_ms`` e ``tentativas_esgotadas`` do log).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal

from pydantic import ValidationError

from hospitalmq.clock import Clock, RealClock
from hospitalmq.config import (
    ATRASOS_PADRAO_MS,
    EXCHANGE_DLX,
    EXCHANGE_PADRAO,
    QueueSpec,
    Settings,
    settings,
)
from hospitalmq.envelope import (
    HEADER_ATTEMPT,
    HEADER_DLQ_ERRO_TIPO,
    HEADER_DLQ_MOTIVO,
    HEADER_DLQ_SERVICO,
    HEADER_VERSION,
    Envelope,
)
from hospitalmq.errors import AuthError, InvalidEnvelopeError, PermanentError

__all__ = [
    "ESPERAS_MS",
    "MAX_ENTREGAS",
    "MAX_TENTATIVAS",
    "MOTIVO_ENVELOPE_INVALIDO",
    "MOTIVO_ERRO_PERMANENTE",
    "MOTIVO_FALHA_TRANSITORIA",
    "MOTIVO_NAO_AUTORIZADO",
    "MOTIVO_PAYLOAD_INVALIDO",
    "MOTIVO_RETENTATIVAS_ESGOTADAS",
    "Classificacao",
    "Decisao",
    "Destino",
    "MensagemDlq",
    "PoliticaRetentativa",
    "classificar",
    "motivo_de",
    "nome_dlq",
    "proxima_espera_ms",
    "resumo_de_traceback",
    "tentativas_restantes",
]

# --------------------------------------------------------------------------- #
# Constantes normativas da politica (secao 4.6.1)                              #
# --------------------------------------------------------------------------- #

ESPERAS_MS: Final[tuple[int, ...]] = ATRASOS_PADRAO_MS
"""Escada de espera, em milissegundos: ``(1000, 2000, 4000)``.

E o mesmo objeto de :data:`hospitalmq.config.ATRASOS_PADRAO_MS`, e nao uma segunda copia literal:
os nomes das filas de espera saem de :meth:`~hospitalmq.config.QueueSpec.retry_queue_name` a partir
dessa tupla, e duas fontes de verdade produziriam ``q.<nome>.retry.2s`` declarada no broker e
``q.<nome>.retry.3s`` procurada pelo consumidor -- mensagem nao roteavel a cada retentativa.
"""

MAX_TENTATIVAS: Final[int] = len(ESPERAS_MS)
"""Numero maximo de retentativas: **3**, exatamente o texto de R2.2."""

MAX_ENTREGAS: Final[int] = MAX_TENTATIVAS + 1
"""Numero maximo de entregas ao handler: **4** (a original mais as tres retentativas)."""

MOTIVO_ENVELOPE_INVALIDO: Final[str] = "envelope_invalido"
"""Corpo recebido nao desserializa como Envelope -- mensagem-veneno (secao 4.7.1)."""

MOTIVO_PAYLOAD_INVALIDO: Final[str] = "payload_invalido"
"""``ValidationError`` do modelo Pydantic do handler, inclui R6.6 (secao 4.7.1)."""

MOTIVO_NAO_AUTORIZADO: Final[str] = "nao_autorizado"
"""Identidade ausente ou insuficiente em fila que a exige (R4.4, secao 4.7.1)."""

MOTIVO_ERRO_PERMANENTE: Final[str] = "erro_permanente"
"""``PermanentError`` levantado pelo handler -- DLQ sem gastar retentativa (secao 4.7.1)."""

MOTIVO_RETENTATIVAS_ESGOTADAS: Final[str] = "retentativas_esgotadas"
"""Quarta falha transitoria, ou fila declarada sem escada de espera (secao 4.7.1)."""

MOTIVO_FALHA_TRANSITORIA: Final[str] = "falha_transitoria"
"""Motivo de uma **retentativa** -- a falha ainda tem orcamento e nao vai para a DLQ."""

Classificacao = Literal["transitorio", "permanente"]
"""Dominio fechado da classificacao de falha (secao 4.6.1)."""

Destino = Literal["retentativa", "dlq"]
"""Dominio fechado do destino de uma mensagem que falhou (secoes 4.6.1 e 4.7.1)."""

_LIMITE_ERRO_MENSAGEM: Final[int] = 512
"""Corte do texto do erro no corpo da mensagem de DLQ, para nao inflar a fila de inspecao."""


# --------------------------------------------------------------------------- #
# Funcoes puras da politica (secao 4.6.1)                                      #
# --------------------------------------------------------------------------- #


def proxima_espera_ms(attempt: int, *, esperas_ms: Sequence[int] = ESPERAS_MS) -> int | None:
    """Calcula o atraso da proxima entrega, em milissegundos (secao 4.6.1).

    Args:
        attempt: A tentativa que **acabou de falhar**, comecando em 1 -- e o campo ``attempt`` do
            Envelope recebido, nao um contador do consumidor.
        esperas_ms: Escada de espera a aplicar. O padrao e :data:`ESPERAS_MS`; uma fila pode
            declarar a propria escada em ``QueueSpec.retry_delays_ms``.

    Returns:
        ``1000``, ``2000`` ou ``4000`` enquanto houver orcamento; ``None`` quando as retentativas
        se esgotaram -- e ``None`` e o sinal de que o destino passa a ser a DLQ (R2.3).
    """
    if attempt < 1 or attempt > len(esperas_ms):
        return None
    return esperas_ms[attempt - 1]


def tentativas_restantes(attempt: int, *, max_tentativas: int = MAX_TENTATIVAS) -> int:
    """Conta quantas retentativas ainda cabem no orcamento (campo de ``MessageContext``).

    Args:
        attempt: Tentativa corrente do Envelope, comecando em 1.
        max_tentativas: Teto de retentativas da politica em vigor.

    Returns:
        Numero de retentativas ainda disponiveis; ``0`` quando a proxima falha vai para a DLQ.
    """
    return max(0, max_tentativas - attempt + 1)


def classificar(exc: BaseException) -> Classificacao:
    """Decide se a falha merece nova tentativa (secao 4.6.1).

    O criterio e semantico, nao sintatico: **transitorio e o erro cuja repeticao tem chance real
    de sucesso** -- canal de notificacao fora do ar, ``deadlock``, timeout de rede. **Permanente e
    o erro que falharia de novo com a mesma entrada** -- sinal vital fora da faixa fisiologica
    (R6.6) nao passa a ser valido porque foi tentado quatro vezes.

    Excecao nao prevista e tratada como **transitoria** por assimetria de custo (secao 4.5.4,
    observacao 2): com idempotencia funcionando, retentar em excesso custa uma reexecucao
    suprimida; descartar de menos custa uma leitura clinica perdida.

    Args:
        exc: Excecao levantada pelo handler ou pelo *pipeline* do consumidor.

    Returns:
        ``"permanente"`` para :class:`~hospitalmq.errors.PermanentError` (e seus descendentes,
        incluindo :class:`~hospitalmq.errors.InvalidEnvelopeError`), ``ValidationError`` do
        Pydantic e :class:`~hospitalmq.errors.AuthError`; ``"transitorio"`` para todo o resto.
    """
    if isinstance(exc, PermanentError | ValidationError | AuthError):
        return "permanente"
    return "transitorio"


def motivo_de(exc: BaseException, *, esgotado: bool = False) -> str:
    """Traduz a excecao no vocabulario de motivo gravado na DLQ (secao 4.7.1).

    Args:
        exc: Excecao que causou o descarte.
        esgotado: ``True`` quando o descarte vem do fim do orcamento de retentativas, e nao da
            natureza da excecao. Tem precedencia, porque e essa a informacao acionavel na triagem:
            um ``TransientError`` na DLQ so chega ali por esgotamento.

    Returns:
        Um dos valores de :data:`MOTIVO_ENVELOPE_INVALIDO`, :data:`MOTIVO_PAYLOAD_INVALIDO`,
        :data:`MOTIVO_NAO_AUTORIZADO`, :data:`MOTIVO_ERRO_PERMANENTE` ou
        :data:`MOTIVO_RETENTATIVAS_ESGOTADAS`.
    """
    if esgotado:
        return MOTIVO_RETENTATIVAS_ESGOTADAS
    if isinstance(exc, InvalidEnvelopeError):
        return MOTIVO_ENVELOPE_INVALIDO
    if isinstance(exc, AuthError):
        return MOTIVO_NAO_AUTORIZADO
    if isinstance(exc, ValidationError):
        return MOTIVO_PAYLOAD_INVALIDO
    if isinstance(exc, PermanentError):
        return MOTIVO_ERRO_PERMANENTE
    return MOTIVO_RETENTATIVAS_ESGOTADAS


def nome_dlq(fila: QueueSpec | str) -> str:
    """Devolve o nome -- e a *routing key* -- da DLQ de uma fila de negocio (secao 4.7.1).

    A convencao e **sufixo**, nunca prefixo. Publicar com ``dlq.q.vitals.sinais-coletados`` nao
    casaria com o *binding* declarado em ``hospital.dlx``, o ``mandatory=True`` da publicacao
    converteria a mensagem nao roteavel em :class:`~hospitalmq.errors.TransportError`, o ``ack`` da
    entrega original jamais aconteceria e a mensagem entraria em reentrega perpetua.

    Args:
        fila: A :class:`~hospitalmq.config.QueueSpec` de origem ou apenas o nome dela.

    Returns:
        ``"<nome-da-fila-de-origem>.dlq"``.
    """
    if isinstance(fila, QueueSpec):
        return fila.dlq_name
    return f"{fila}.dlq"


def resumo_de_traceback(exc: BaseException) -> str:
    """Resume o ponto de falha em uma linha, para o campo ``traceback_resumo`` da DLQ.

    Guardar o *traceback* inteiro na fila de inspecao infla a DLQ e raramente acrescenta
    informacao alem do ultimo quadro; guardar apenas o tipo da excecao nao diz **onde**. O meio
    termo e o quadro mais profundo, no formato do exemplo da secao 4.7.2.

    Args:
        exc: Excecao cujo ``__traceback__`` sera resumido.

    Returns:
        Algo como ``"alert_service/canal.py:64 in despachar -> TimeoutError"``. Quando a excecao
        nao carrega ``__traceback__`` -- caso de erro construido manualmente em teste -- devolve
        apenas o nome do tipo.
    """
    tipo = type(exc).__name__
    tb = exc.__traceback__
    if tb is None:
        return tipo
    while tb.tb_next is not None:
        tb = tb.tb_next
    quadro = tb.tb_frame
    partes = quadro.f_code.co_filename.replace("\\", "/").split("/")
    arquivo = "/".join(partes[-2:]) if len(partes) > 1 else partes[-1]
    return f"{arquivo}:{tb.tb_lineno} in {quadro.f_code.co_name} -> {tipo}"


def _texto_do_erro(exc: BaseException, *, limite: int) -> str:
    """Serializa a excecao em texto curto, com corte defensivo de tamanho."""
    texto = str(exc) or type(exc).__name__
    if len(texto) <= limite:
        return texto
    return f"{texto[: limite - 3]}..."


# --------------------------------------------------------------------------- #
# Resultado da decisao                                                          #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class Decisao:
    """O que fazer com uma mensagem que falhou (secoes 4.6.1 e 4.7.1).

    Objeto de valor: :meth:`PoliticaRetentativa.decidir` nao publica nada e nao toca no
    transporte. Quem executa a decisao e o ``Consumer``, que republica com ``delay_ms`` e so
    entao confirma a entrega original -- ``ack`` sempre **depois** do *publisher confirm*, nunca
    antes.

    Attributes:
        destino: ``"retentativa"`` ou ``"dlq"``.
        classificacao: ``"transitorio"`` ou ``"permanente"``, vindo de :func:`classificar`.
        motivo: Vocabulario de :func:`motivo_de`. Vai para ``x-hmq-dlq-motivo`` no descarte.
        attempt: A tentativa que acabou de falhar, comecando em 1.
        exchange: Onde republicar. ``""`` (exchange padrao) na retentativa, porque a *routing key*
            e o proprio nome da fila de espera; ``"hospital.dlx"`` no descarte.
        routing_key: Nome da fila de espera (retentativa) ou da DLQ (descarte).
        espera_ms: Atraso da proxima entrega; ``None`` no descarte.
        tentativas_esgotadas: Quantas retentativas foram gastas ate o descarte; ``None`` quando a
            DLQ vem de erro permanente, caso em que nenhuma foi gasta (secao 9.4, campo de log).
        fila_origem: Fila de onde a mensagem veio, usada no log e no corpo da DLQ.
        decidido_em: Instante da decisao, lido do :class:`~hospitalmq.clock.Clock` injetado.
    """

    destino: Destino
    classificacao: Classificacao
    motivo: str
    attempt: int
    exchange: str
    routing_key: str
    espera_ms: int | None
    tentativas_esgotadas: int | None
    fila_origem: str
    decidido_em: datetime

    @property
    def retentar(self) -> bool:
        """Indica se a mensagem deve ser republicada em fila de espera.

        Returns:
            ``True`` quando :attr:`destino` e ``"retentativa"``.
        """
        return self.destino == "retentativa"

    @property
    def descartar(self) -> bool:
        """Indica se a mensagem deve ir para a DLQ.

        Returns:
            ``True`` quando :attr:`destino` e ``"dlq"``.
        """
        return self.destino == "dlq"

    def para_log(self) -> dict[str, object]:
        """Projeta a decisao nos campos do log estruturado (R5.1, secao 9.4).

        Returns:
            Dicionario com ``resultado``, ``motivo``, ``attempt``, ``fila``, ``fila_destino`` e
            ``classificacao``; mais ``espera_ms`` apenas na retentativa e ``tentativas_esgotadas``
            apenas no descarte por esgotamento -- exatamente a condicionalidade da secao 9.4.
        """
        linha: dict[str, object] = {
            "resultado": self.destino,
            "motivo": self.motivo,
            "classificacao": self.classificacao,
            "attempt": self.attempt,
            "fila": self.fila_origem,
            "fila_destino": self.routing_key,
        }
        if self.espera_ms is not None:
            linha["espera_ms"] = self.espera_ms
        if self.tentativas_esgotadas is not None:
            linha["tentativas_esgotadas"] = self.tentativas_esgotadas
        return linha


@dataclass(frozen=True, slots=True)
class MensagemDlq:
    """A mensagem publicada em ``hospital.dlx``, ja enriquecida (secao 4.7.2).

    R2.3 exige preservar o Envelope original **e** acrescentar o motivo da ultima falha. O
    *dead-lettering* nativo do broker copia o corpo intacto e acrescenta apenas ``x-death``, que
    nao carrega a excecao -- dai a publicacao explicita. O Envelope original vai intacto sob a
    chave ``envelope`` e o diagnostico vai em um objeto irmao, ``falha``, sem contaminar o
    original: e isso que mantem o ``redrive`` (secao 4.7.3) uma operacao de recorte trivial.

    Attributes:
        envelope: O Envelope original, ou ``None`` para mensagem-veneno que nem desserializou.
        falha: Bloco de diagnostico com as nove chaves da secao 4.7.2.
        exchange: Sempre ``"hospital.dlx"``, salvo configuracao explicita.
        routing_key: ``"<fila-de-origem>.dlq"`` -- casa com o *binding* declarado na secao 6.
        motivo: Copia de ``falha["motivo"]``, espelhada no cabecalho ``x-hmq-dlq-motivo``.
        servico: Nome do consumidor que descartou, espelhado em ``x-hmq-dlq-servico``.
        erro_tipo: Nome da classe da excecao, espelhado em ``x-hmq-dlq-erro-tipo``.
        attempt: ``attempt`` do Envelope no descarte, espelhado em ``x-hmq-attempt``.
        corpo_original: Bytes crus recebidos, preenchido **apenas** quando :attr:`envelope` e
            ``None`` -- sem isso, uma mensagem-veneno chegaria a DLQ sem nada para inspecionar.
    """

    envelope: Envelope | None
    falha: dict[str, Any]
    exchange: str
    routing_key: str
    motivo: str
    servico: str
    erro_tipo: str
    attempt: int
    corpo_original: bytes | None = None

    def to_dict(self) -> dict[str, Any]:
        """Projeta a mensagem de DLQ no objeto JSON da secao 4.7.2.

        Returns:
            ``{"envelope": {...}, "falha": {...}}``. Quando o Envelope nao pode ser desserializado,
            ``envelope`` vem ``null`` e uma chave ``corpo_bruto`` carrega o texto recebido, para
            que a inspecao humana ainda tenha o que ler.
        """
        corpo: dict[str, Any] = {
            "envelope": self.envelope.to_dict() if self.envelope is not None else None,
            "falha": dict(self.falha),
        }
        if self.envelope is None and self.corpo_original is not None:
            corpo["corpo_bruto"] = self.corpo_original.decode("utf-8", errors="replace")
        return corpo

    def to_bytes(self) -> bytes:
        """Serializa a mensagem de DLQ como JSON UTF-8.

        Returns:
            O corpo pronto para ``Transport.publish``, no mesmo formato compacto do Envelope.
        """
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    def headers(self) -> dict[str, object]:
        """Monta os cabecalhos AMQP de triagem da DLQ (secao 4.7.2).

        Permitem filtrar mensagens na UI de *management* **sem abrir o corpo**: qual servico
        descartou, por que motivo, com que tipo de erro e em que tentativa.

        Returns:
            Dicionario com ``x-hmq-attempt``, ``x-hmq-version``, ``x-hmq-dlq-motivo``,
            ``x-hmq-dlq-servico`` e ``x-hmq-dlq-erro-tipo``.
        """
        if self.envelope is not None:
            return self.envelope.to_headers(
                dlq_motivo=self.motivo,
                dlq_servico=self.servico,
                dlq_erro_tipo=self.erro_tipo,
            )
        return {
            HEADER_ATTEMPT: self.attempt,
            HEADER_VERSION: 1,
            HEADER_DLQ_MOTIVO: self.motivo,
            HEADER_DLQ_SERVICO: self.servico,
            HEADER_DLQ_ERRO_TIPO: self.erro_tipo,
        }


# --------------------------------------------------------------------------- #
# A politica                                                                    #
# --------------------------------------------------------------------------- #


class PoliticaRetentativa:
    """Aplica a escada 1s/2s/4s e decide entre retentativa e DLQ (secoes 4.6 e 4.7).

    E o unico lugar do projeto que sabe contar tentativas. O ``Consumer`` pergunta o que fazer e
    executa; o handler de negocio nao participa da decisao -- ele apenas escolheu a excecao certa.

    A instancia e imutavel na pratica e segura para uso concorrente: :meth:`decidir` e funcao pura
    dos argumentos mais a configuracao construida, sem estado acumulado entre mensagens. Isso e
    proposital -- estado por mensagem viveria no consumidor e se perderia no ``deploy``, que e
    exatamente o defeito que a secao 4.6.3 evita colocando ``attempt`` dentro do Envelope.
    """

    __slots__ = ("_clock", "_esperas_ms", "_exchange_dlx", "_max_tentativas", "_servico")

    def __init__(
        self,
        *,
        servico: str = "hospitalmq",
        esperas_ms: Sequence[int] | None = None,
        max_tentativas: int | None = None,
        clock: Clock | None = None,
        exchange_dlx: str = EXCHANGE_DLX,
    ) -> None:
        """Constroi a politica.

        Args:
            servico: Nome do processo consumidor. Vai para ``falha.servico`` e para o cabecalho
                ``x-hmq-dlq-servico``, e e o que permite saber **quem** descartou quando duas
                assinaturas leem o mesmo evento.
            esperas_ms: Escada de espera. ``None`` usa :data:`ESPERAS_MS` (1 s, 2 s, 4 s).
            max_tentativas: Teto de retentativas. ``None`` usa o comprimento da escada. Um teto
                menor que a escada e legitimo -- corta o orcamento sem redeclarar filas no broker.
            clock: Fonte de tempo injetada; ``None`` usa :class:`~hospitalmq.clock.RealClock`.
            exchange_dlx: Exchange de descarte. Padrao ``"hospital.dlx"``.

        Raises:
            ValueError: Se ``max_tentativas`` for negativo ou se algum atraso nao for positivo.
        """
        escada = tuple(esperas_ms) if esperas_ms is not None else ESPERAS_MS
        if any(ms <= 0 for ms in escada):
            raise ValueError(f"atrasos devem ser positivos, recebido {escada!r}")
        teto = len(escada) if max_tentativas is None else max_tentativas
        if teto < 0:
            raise ValueError(f"max_tentativas deve ser >= 0, recebido {teto}")
        self._esperas_ms: tuple[int, ...] = escada
        self._max_tentativas: int = teto
        self._clock: Clock = clock if clock is not None else RealClock()
        self._servico: str = servico
        self._exchange_dlx: str = exchange_dlx

    @classmethod
    def de_configuracao(
        cls,
        cfg: Settings | None = None,
        *,
        clock: Clock | None = None,
    ) -> PoliticaRetentativa:
        """Constroi a politica a partir de :class:`~hospitalmq.config.Settings`.

        Args:
            cfg: Configuracao a usar. ``None`` le a instancia global
                :data:`hospitalmq.config.settings`.
            clock: Fonte de tempo injetada.

        Returns:
            Politica com ``servico`` vindo de ``SERVICE_NAME`` e o teto de ``MAX_TENTATIVAS`` --
            com ``MAX_TENTATIVAS=0`` a primeira falha ja vai para a DLQ, como documenta a secao
            12.4.
        """
        alvo = cfg if cfg is not None else settings
        return cls(
            servico=alvo.service_name,
            esperas_ms=alvo.atrasos_ms() or ESPERAS_MS,
            max_tentativas=alvo.max_tentativas,
            clock=clock,
        )

    # -- introspeccao ------------------------------------------------------- #

    @property
    def servico(self) -> str:
        """Nome do processo consumidor ao qual esta politica pertence."""
        return self._servico

    @property
    def esperas_ms(self) -> tuple[int, ...]:
        """Escada de espera em vigor, em milissegundos."""
        return self._esperas_ms

    @property
    def max_tentativas(self) -> int:
        """Teto de retentativas antes da DLQ."""
        return self._max_tentativas

    @property
    def clock(self) -> Clock:
        """Fonte de tempo injetada, usada nos carimbos da decisao e do descarte."""
        return self._clock

    def proxima_espera_ms(self, attempt: int, *, fila: QueueSpec | str | None = None) -> int | None:
        """Calcula o atraso da proxima entrega considerando a escada efetiva da fila.

        Args:
            attempt: Tentativa que acabou de falhar, comecando em 1.
            fila: Fila de origem. Quando e uma :class:`~hospitalmq.config.QueueSpec`, a escada dela
                tem precedencia -- e assim que ``q.gateway.projecao`` e as filas de RPC, declaradas
                com ``retry_delays_ms=()``, nunca retentam.

        Returns:
            O atraso em milissegundos, ou ``None`` quando o orcamento acabou.
        """
        return proxima_espera_ms(attempt, esperas_ms=self._escada(fila))

    def tentativas_restantes(self, attempt: int, *, fila: QueueSpec | str | None = None) -> int:
        """Conta as retentativas ainda disponiveis para esta fila.

        Args:
            attempt: Tentativa corrente do Envelope, comecando em 1.
            fila: Fila de origem, para respeitar uma escada especifica.

        Returns:
            Quantidade de retentativas restantes; ``0`` quando a proxima falha vai para a DLQ.
        """
        return tentativas_restantes(attempt, max_tentativas=len(self._escada(fila)))

    # -- decisao ------------------------------------------------------------ #

    def decidir(self, *, fila: QueueSpec | str, attempt: int, exc: BaseException) -> Decisao:
        """Decide o destino de uma mensagem que falhou (secoes 4.6.1 e 4.7.1).

        Regra, em uma frase: **erro permanente vai direto para a DLQ; erro transitorio consome a
        escada de espera e so entao vai para a DLQ.** Nenhum caminho devolve "descartar
        silenciosamente" -- toda mensagem que morre e observavel (R2.3).

        Args:
            fila: Fila de origem. Passar a :class:`~hospitalmq.config.QueueSpec` e o caminho
                preferido, porque os nomes das filas derivadas saem dela e nunca divergem do que
                foi declarado no broker.
            attempt: Valor de ``attempt`` do Envelope recebido, comecando em 1.
            exc: Excecao que causou a falha.

        Returns:
            A :class:`Decisao` com destino, exchange, *routing key* e atraso ja resolvidos.
        """
        spec = self._spec_de(fila)
        escada = self._escada(spec)
        classificacao = classificar(exc)
        agora = self._clock.now()

        espera: int | None = None
        if classificacao == "transitorio":
            espera = proxima_espera_ms(attempt, esperas_ms=escada)

        if espera is not None:
            destino_fila = spec.fila_de_retentativa(attempt) or spec.retry_queue_name(espera)
            return Decisao(
                destino="retentativa",
                classificacao=classificacao,
                motivo=MOTIVO_FALHA_TRANSITORIA,
                attempt=attempt,
                exchange=EXCHANGE_PADRAO,
                routing_key=destino_fila,
                espera_ms=espera,
                tentativas_esgotadas=None,
                fila_origem=spec.name,
                decidido_em=agora,
            )

        esgotado = classificacao == "transitorio"
        return Decisao(
            destino="dlq",
            classificacao=classificacao,
            motivo=motivo_de(exc, esgotado=esgotado),
            attempt=attempt,
            exchange=self._exchange_dlx,
            routing_key=nome_dlq(spec),
            espera_ms=None,
            tentativas_esgotadas=len(escada) if esgotado else None,
            fila_origem=spec.name,
            decidido_em=agora,
        )

    def montar_dlq(
        self,
        *,
        fila: QueueSpec | str,
        exc: BaseException,
        envelope: Envelope | None = None,
        motivo: str | None = None,
        corpo_original: bytes | None = None,
        tentativas: int | None = None,
        primeira_falha_em: datetime | None = None,
    ) -> MensagemDlq:
        """Monta a mensagem enriquecida de DLQ no formato normativo da secao 4.7.2.

        Args:
            fila: Fila de origem; determina a *routing key* ``<fila>.dlq``.
            exc: Excecao da ultima falha -- e dela que saem ``erro_tipo``, ``erro_mensagem`` e
                ``traceback_resumo``.
            envelope: O Envelope original. ``None`` na mensagem-veneno, quando
                ``Envelope.from_bytes`` falhou e nao ha sequer ``message_id`` para deduplicar.
            motivo: Motivo do descarte. ``None`` deixa :func:`motivo_de` inferir pela excecao.
            corpo_original: Bytes crus recebidos; guardados apenas quando ``envelope`` e ``None``.
            tentativas: Quantas entregas foram feitas. ``None`` usa ``envelope.attempt``.
            primeira_falha_em: Instante da primeira falha desta mensagem, quando o chamador o
                conhece. Omitido do corpo quando desconhecido -- inferir um valor produziria
                diagnostico falso justamente no artefato feito para inspecao humana.

        Returns:
            A :class:`MensagemDlq` pronta para ``Transport.publish``.
        """
        spec_nome = fila.name if isinstance(fila, QueueSpec) else fila
        attempt = tentativas if tentativas is not None else (envelope.attempt if envelope else 1)
        motivo_final = motivo if motivo is not None else motivo_de(exc)
        descartado_em = self._clock.now()

        falha: dict[str, Any] = {
            "servico": self._servico,
            "fila_origem": spec_nome,
            "motivo": motivo_final,
            "erro_tipo": type(exc).__name__,
            "erro_mensagem": _texto_do_erro(exc, limite=_LIMITE_ERRO_MENSAGEM),
            "traceback_resumo": resumo_de_traceback(exc),
            "tentativas": attempt,
        }
        if primeira_falha_em is not None:
            falha["primeira_falha_em"] = _iso(primeira_falha_em)
        elif attempt == 1:
            falha["primeira_falha_em"] = _iso(descartado_em)
        falha["descartado_em"] = _iso(descartado_em)

        return MensagemDlq(
            envelope=envelope,
            falha=falha,
            exchange=self._exchange_dlx,
            routing_key=nome_dlq(fila),
            motivo=motivo_final,
            servico=self._servico,
            erro_tipo=type(exc).__name__,
            attempt=attempt,
            corpo_original=corpo_original if envelope is None else None,
        )

    async def aguardar(self, espera_ms: int) -> None:
        """Suspende pelo atraso pedido usando o :class:`~hospitalmq.clock.Clock` injetado.

        **Nao e o caminho de producao.** No RabbitMQ o atraso e do broker: a mensagem viaja para
        ``q.<nome>.retry.Ns``, cujo ``x-message-ttl`` a devolve a fila de origem por
        *dead-lettering* (secao 4.6.2). Dormir dentro do consumidor seguraria um dos dez espacos de
        ``prefetch`` durante toda a espera, perderia o contador de tentativas a cada reinicio e
        colidiria com o ``consumer_timeout`` do broker (secao 4.6.4).

        Este metodo existe para transportes que **nao** tem fila de espera nativa -- o
        ``MemoryTransport`` da suite -- e para tornar a politica verificavel: com
        :class:`~hospitalmq.clock.ManualClock`, ``clock.sleeps == [1.0, 2.0, 4.0]`` e a asserção
        exata de R2.2, sem esperar sete segundos de tempo real.

        Args:
            espera_ms: Atraso em milissegundos, tipicamente o :attr:`Decisao.espera_ms`.
        """
        await self._clock.sleep(espera_ms / 1000.0)

    # -- auxiliares privados ------------------------------------------------ #

    def _spec_de(self, fila: QueueSpec | str) -> QueueSpec:
        """Normaliza o argumento ``fila`` em uma :class:`~hospitalmq.config.QueueSpec`."""
        if isinstance(fila, QueueSpec):
            return fila
        return QueueSpec(name=fila, retry_delays_ms=self._esperas_ms)

    def _escada(self, fila: QueueSpec | str | None) -> tuple[int, ...]:
        """Resolve a escada efetiva: a da fila, truncada pelo teto da politica."""
        base = fila.retry_delays_ms if isinstance(fila, QueueSpec) else self._esperas_ms
        return base[: self._max_tentativas]


def _iso(momento: datetime) -> str:
    """Serializa um instante como ISO-8601 UTC com sufixo ``Z``, como no Envelope."""
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=UTC)
    return momento.astimezone(UTC).isoformat().replace("+00:00", "Z")
