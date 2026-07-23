"""Log estruturado em JSON, correlacao por ``contextvars`` e mascaramento LGPD (secao 9).

Observabilidade e responsabilidade **do middleware**, nao de cada servico (secao 9.1): quem
escreve um ``Servico_Consumidor`` nao escreve linha de log de infraestrutura. O ``Publisher``, o
``Consumer``, o ``RpcClient`` e o ``RpcServer`` ja emitem todo o rastro operacional; o
desenvolvedor do dominio acrescenta apenas o que for clinico (``news2``, ``severidade``).

Tres decisoes estruturam este modulo:

1. **Configuracao unica.** :func:`configure_logging` e chamada uma vez no *startup* de cada
   processo, antes de qualquer log. Formato identico nos seis servicos e pre-requisito para o
   ``jq`` unico da demonstracao (R5.3): bastaria um servico divergir para quebrar o filtro por
   ``correlation_id``.
2. **Correlacao por ``contextvars``.** O ``correlation_id`` nunca atravessa assinatura de funcao:
   e amarrado ao contexto assincrono corrente por :func:`bind_context` e injetado em toda linha
   por ``structlog.contextvars.merge_contextvars``.
3. **Mascaramento como processador.** :func:`mask_pii` roda no *pipeline*, no penultimo passo
   antes de serializar, e por isso vale tambem para os logs escritos a mao pelo desenvolvedor do
   dominio (R5.6, LGPD).

Uso tipico::

    from hospitalmq.config import settings
    from hospitalmq.logging import LogEvent, configure_logging, get_logger

    configure_logging(servico=settings.service_name, nivel=settings.log_level)
    log = get_logger(__name__)
    log.info(LogEvent.MENSAGEM_PUBLICADA, tipo="sinais.registrados", payload_bytes=327)

Requisitos atendidos: R5.1, R5.2, R5.3, R5.4 e R5.6.
"""

from __future__ import annotations

import logging
import unicodedata
from contextlib import AbstractContextManager
from enum import StrEnum
from typing import Any, Final, Protocol

import structlog
from structlog.typing import EventDict, Processor, WrappedLogger

__all__ = [
    "CAMPOS_ORDENADOS",
    "CHAVES_INTOCAVEIS",
    "CHAVES_ISENTAS",
    "CHAVES_TRUNCAVEIS",
    "LIMITE_TEXTO",
    "MARCADORES_SENSIVEIS",
    "MASCARA",
    "MASCARA_CPF",
    "PROFUNDIDADE_MAXIMA",
    "LogEvent",
    "Logger",
    "bind_context",
    "causation_id_atual",
    "clear_context",
    "configure_logging",
    "contexto_atual",
    "contexto_de_log",
    "correlation_id_atual",
    "get_logger",
    "mask_pii",
    "message_id_atual",
]


# --------------------------------------------------------------------------- #
# Vocabulario fechado de eventos (secao 9.4)                                   #
# --------------------------------------------------------------------------- #


class LogEvent(StrEnum):
    """Dominio fechado do campo ``evento`` da linha de log (secao 9.4).

    O campo ``evento`` **nunca** aceita frase livre. Tres ganhos concretos: o ``jq`` agrupa por
    ``.evento`` e produz contagem confiavel; o *linter* pega o erro de digitacao antes da
    apresentacao; e o conjunto de valores vira a **especificacao** do comportamento observavel do
    middleware -- afirmar que o ``Consumer`` emitiu ``retentativa, retentativa, retentativa, dlq``
    e um teste funcional de R2.2 e R2.3 escrito sobre o log.

    Por ser :class:`~enum.StrEnum`, cada membro **e** a string correspondente: pode ser passado
    direto a ``log.info(...)`` e serializado pelo ``JSONRenderer`` sem conversao.
    """

    # --- ciclo de vida da mensagem assincrona: Publisher e Consumer ---
    MENSAGEM_PUBLICADA = "mensagem.publicada"
    MENSAGEM_RECEBIDA = "mensagem.recebida"
    MENSAGEM_PROCESSADA = "mensagem.processada"
    MENSAGEM_DUPLICADA = "mensagem.duplicada"
    MENSAGEM_RETENTATIVA = "mensagem.retentativa"
    MENSAGEM_DLQ = "mensagem.dlq"
    # --- RPC, lado chamador: RpcClient (secao 5.4.5) ---
    RPC_CHAMADA_INICIADA = "rpc.chamada_iniciada"
    RPC_RESPOSTA_RECEBIDA = "rpc.resposta_recebida"
    RPC_TIMEOUT = "rpc.timeout"
    RPC_ERRO_REMOTO = "rpc.erro_remoto"
    RPC_RESPOSTA_ORFA = "rpc.resposta_orfa"
    # --- RPC, lado servidor: RpcServer (secao 5.4.5) ---
    RPC_OPERACAO_RECEBIDA = "rpc.operacao_recebida"
    RPC_OPERACAO_CONCLUIDA = "rpc.operacao_concluida"
    RPC_RESPOSTA_NAO_ENTREGUE = "rpc.resposta_nao_entregue"
    # --- autenticacao e autorizacao: auth.py e dependencias do gateway ---
    AUTH_NEGADA = "auth.negada"


# --------------------------------------------------------------------------- #
# Mascaramento de dado pessoal (secao 9.8, R5.6)                               #
# --------------------------------------------------------------------------- #

MARCADORES_SENSIVEIS: Final[frozenset[str]] = frozenset(
    {
        "nome",
        "cpf",
        "nascimento",
        "rg",
        "cns",
        "telefone",
        "celular",
        "email",
        "endereco",
        "logradouro",
        "cep",
        "mae",
        "responsavel",
    }
)
"""Subcadeias que, presentes no nome da chave, marcam o valor como dado pessoal (secao 9.8.1)."""

CHAVES_ISENTAS: Final[frozenset[str]] = frozenset(
    {
        "nome_fila",
        "nome_exchange",
        "nome_operacao",
        "nome_evento",
        "nome_servico",
        # O casamento por subcadeia da secao 9.8.2 tem falsos positivos previsiveis: "cep" esta
        # dentro de ex-"cep"-tion e "rg" dentro de a-"rg"-s. Sem estas isencoes, o traceback de
        # toda excecao sairia como "***" e a linha de erro perderia justamente o que a torna util.
        "exception",
        "exception_type",
        "exceptions",
        "args",
        "positional_args",
    }
)
"""Falsos positivos previsiveis do proprio middleware -- chaves de infraestrutura com ``nome``.

O compromisso e declarado na secao 9.8.2: prefere-se mascarar demais a mascarar de menos, e o custo
de um falso positivo e um ``"***"`` num campo de infraestrutura. A isencao existe apenas para as
colisoes conhecidas, nunca para campo de dominio.
"""

CHAVES_INTOCAVEIS: Final[frozenset[str]] = frozenset(
    {"exc_info", "stack_info", "_record", "_from_structlog"}
)
"""Chaves de controle do ``structlog``, repassadas intactas.

``exc_info`` carrega a tripla ``(tipo, excecao, traceback)``; converte-la em lista, como faria a
travessia recursiva, quebraria o ``format_exc_info`` mais adiante no *pipeline*.
"""

CHAVES_TRUNCAVEIS: Final[frozenset[str]] = frozenset(
    {"detalhe", "erro_mensagem", "motivo_dlq", "exception"}
)
"""Campos de texto livre sujeitos a truncagem em :data:`LIMITE_TEXTO` (secao 9.8.4, item 2)."""

MASCARA: Final[str] = "***"
"""Substituto padrao de todo valor classificado como dado pessoal."""

MASCARA_CPF: Final[str] = "***.***.***-**"
"""Substituto do CPF: preserva a **forma**, nunca o valor (ajuda a diagnosticar erro de formato)."""

PROFUNDIDADE_MAXIMA: Final[int] = 6
"""Teto de recursao da travessia -- protege contra estrutura patologicamente profunda."""

LIMITE_TEXTO: Final[int] = 500
"""Comprimento maximo do texto de excecao registrado em ``detalhe`` (secao 9.3.1)."""

_SUFIXO_TRUNCAGEM: Final[str] = "..."


def _sem_acento(texto: str) -> str:
    """Remove os diacriticos de um texto para normalizar a comparacao de nomes de chave.

    Args:
        texto: Nome de chave, possivelmente acentuado (``"endereço"``).

    Returns:
        O mesmo texto sem marcas de combinacao (``"endereco"``).
    """
    decomposto = unicodedata.normalize("NFKD", texto)
    return "".join(caractere for caractere in decomposto if not unicodedata.combining(caractere))


def _sensivel(chave: str) -> bool:
    """Classifica um nome de chave como portador de dado pessoal (secao 9.8.2).

    O casamento e por **subcadeia normalizada**, e nao por igualdade: uma regra so cobre ``nome``,
    ``nomePaciente``, ``nome_completo``, ``NomeSocial`` e ``data_nascimento``.

    Args:
        chave: Nome da chave do dicionario de log.

    Returns:
        ``True`` se o valor associado deve ser mascarado.
    """
    normalizada = _sem_acento(chave).lower()
    if normalizada in CHAVES_ISENTAS:
        return False
    return any(marcador in normalizada for marcador in MARCADORES_SENSIVEIS)


def _truncar(texto: str) -> str:
    """Limita um texto a :data:`LIMITE_TEXTO` caracteres, sinalizando o corte.

    Args:
        texto: Texto original.

    Returns:
        O proprio texto, se couber; caso contrario o prefixo seguido de ``"..."``.
    """
    if len(texto) <= LIMITE_TEXTO:
        return texto
    return texto[: LIMITE_TEXTO - len(_SUFIXO_TRUNCAGEM)] + _SUFIXO_TRUNCAGEM


def _valor_mascarado(chave: str, valor: Any, profundidade: int) -> Any:  # noqa: ANN401
    """Aplica a politica de mascaramento ao par chave/valor de um dicionario de log.

    Args:
        chave: Nome da chave.
        valor: Valor associado.
        profundidade: Nivel corrente da recursao.

    Returns:
        O valor mascarado, truncado ou percorrido recursivamente, conforme a classificacao.
    """
    if chave in CHAVES_INTOCAVEIS:
        return valor
    normalizada = _sem_acento(chave).lower()
    if _sensivel(chave):
        return MASCARA_CPF if "cpf" in normalizada else MASCARA
    resultado = _mascarar(valor, profundidade + 1)
    if chave in CHAVES_TRUNCAVEIS and isinstance(resultado, str):
        return _truncar(resultado)
    return resultado


def _mascarar(valor: Any, profundidade: int) -> Any:  # noqa: ANN401
    """Percorre recursivamente a estrutura do log mascarando o que for dado pessoal.

    Args:
        valor: Valor a inspecionar -- dicionario, sequencia ou escalar.
        profundidade: Nivel corrente da recursao, comecando em ``0``.

    Returns:
        Uma copia da estrutura com os valores sensiveis substituidos.
    """
    if profundidade > PROFUNDIDADE_MAXIMA:
        return "<profundidade_excedida>"
    if isinstance(valor, dict):
        return {
            chave: _valor_mascarado(str(chave), interno, profundidade)
            for chave, interno in valor.items()
        }
    if isinstance(valor, list | tuple):
        return [_mascarar(interno, profundidade + 1) for interno in valor]
    return valor


def mask_pii(logger: WrappedLogger, metodo: str, evento: EventDict) -> EventDict:
    """Processador ``structlog`` que remove dado pessoal da linha de log (R5.6, LGPD).

    Roda **antes** do renderizador e depois do ``TimeStamper``: opera sobre a estrutura do evento,
    e nao sobre a string ja serializada -- mascarar JSON com expressao regular e exatamente o
    antipadrao que a secao 9.8.2 descarta.

    Preserva ``paciente_id``, ``leito_id``, ``internacao_id``, sinais vitais, ``news2`` e todo o
    contexto de correlacao; substitui nome, CPF, data de nascimento, telefone, e-mail e endereco.

    Nao toca no Envelope publicado: opera exclusivamente sobre o dicionario do log. O dado precisa
    trafegar no Broker -- o que nao pode e ir para o log.

    Args:
        logger: Logger de origem, exigido pela assinatura de processador do ``structlog``.
        metodo: Nome do metodo chamado (``"info"``, ``"warning"``, ...).
        evento: Dicionario do evento, ainda nao serializado.

    Returns:
        Uma copia do dicionario com os valores sensiveis substituidos.
    """
    mascarado: EventDict = _mascarar(dict(evento), profundidade=0)
    return mascarado


def _noop(logger: WrappedLogger, metodo: str, evento: EventDict) -> EventDict:
    """Processador neutro usado quando o mascaramento e desligado no teste unitario.

    Args:
        logger: Logger de origem.
        metodo: Nome do metodo chamado.
        evento: Dicionario do evento.

    Returns:
        O proprio dicionario, sem alteracao.
    """
    return evento


# --------------------------------------------------------------------------- #
# Processadores proprios do esquema da linha (secao 9.3)                       #
# --------------------------------------------------------------------------- #

CAMPOS_ORDENADOS: Final[tuple[str, ...]] = (
    "timestamp",
    "nivel",
    "evento",
    "servico",
    "message_id",
    "correlation_id",
    "causation_id",
    "tipo",
    "tentativa",
    "operacao",
    "rpc_id",
    "fila",
    "fila_dlq",
    "fila_retorno",
    "exchange",
    "payload_bytes",
    "timeout_s",
    "duracao_ms",
    "resultado",
    "status",
    "espera_ms",
    "tentativas_esgotadas",
    "codigo",
    "retentavel",
    "motivo",
    "erro",
    "detalhe",
    "identity_sub",
)
"""Ordem canonica do nucleo do esquema (secao 9.3.1).

``timestamp`` e a **primeira** chave de toda linha: como ISO-8601 UTC com sufixo ``Z`` ordena
lexicograficamente na mesma ordem cronologica, o log agregado de varios conteineres pode ser
ordenado com um ``sort`` de shell. Os campos de dominio acrescentados pelo handler vem depois,
na ordem em que foram escritos.
"""


def _add_service(servico: str) -> Processor:
    """Monta o processador que injeta o campo ``servico`` em toda linha (R5.1).

    Args:
        servico: Nome do processo, vindo de ``SERVICE_NAME`` -- nunca digitado a mao no ponto de
            log, para nao divergir do nome do conteiner.

    Returns:
        Processador ``structlog`` que grava ``servico`` no dicionario do evento.
    """

    def processador(logger: WrappedLogger, metodo: str, evento: EventDict) -> EventDict:
        evento["servico"] = servico
        return evento

    return processador


def _rename_key(de: str, para: str) -> Processor:
    """Monta o processador que renomeia uma chave preservando o valor.

    Usado para transformar o ``level`` do ``structlog`` no ``nivel`` do esquema de 9.3.1 -- o
    campo e grafado em portugues, sem cedilha, para nao exigir escape em ``jq`` nem em ``grep``.

    Args:
        de: Nome atual da chave.
        para: Nome desejado.

    Returns:
        Processador ``structlog`` que aplica a renomeacao quando a chave existe.
    """

    def processador(logger: WrappedLogger, metodo: str, evento: EventDict) -> EventDict:
        if de in evento:
            evento[para] = evento.pop(de)
        return evento

    return processador


def _ordenar_campos(logger: WrappedLogger, metodo: str, evento: EventDict) -> EventDict:
    """Reordena o evento segundo :data:`CAMPOS_ORDENADOS` antes da serializacao.

    O ``JSONRenderer`` e configurado com ``sort_keys=False`` justamente para que a ordem seja esta,
    e nao a alfabetica: ``timestamp`` primeiro, nucleo do esquema em seguida, extensao de dominio
    por ultimo.

    Args:
        logger: Logger de origem.
        metodo: Nome do metodo chamado.
        evento: Dicionario do evento.

    Returns:
        Novo dicionario com as chaves na ordem canonica; nenhuma chave e perdida.
    """
    ordenado: EventDict = {campo: evento[campo] for campo in CAMPOS_ORDENADOS if campo in evento}
    for chave, valor in evento.items():
        if chave not in ordenado:
            ordenado[chave] = valor
    return ordenado


def _truncar_excecao(logger: WrappedLogger, metodo: str, evento: EventDict) -> EventDict:
    """Trunca o texto de excecao produzido por ``format_exc_info`` (secao 9.8.4, item 2).

    O mascaramento e por nome de chave e nao alcanca o texto de um ``traceback``; a defesa
    documentada para ele e a truncagem em :data:`LIMITE_TEXTO` caracteres.

    Args:
        logger: Logger de origem.
        metodo: Nome do metodo chamado.
        evento: Dicionario do evento, ja com a excecao formatada.

    Returns:
        O dicionario com os campos de texto livre limitados em comprimento.
    """
    for chave in CHAVES_TRUNCAVEIS:
        valor = evento.get(chave)
        if isinstance(valor, str):
            evento[chave] = _truncar(valor)
    return evento


_NIVEIS: Final[dict[str, int]] = {
    "critical": logging.CRITICAL,
    "fatal": logging.CRITICAL,
    "error": logging.ERROR,
    "exception": logging.ERROR,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}


def _nivel_numerico(nivel: str) -> int:
    """Converte o nome do nivel de log no inteiro correspondente.

    Args:
        nivel: Nome do nivel, em qualquer caixa (``"INFO"``, ``"debug"``).

    Returns:
        O valor numerico do ``logging`` padrao; ``logging.INFO`` para nome desconhecido -- um
        ``LOG_LEVEL`` mal digitado degrada para o padrao seguro em vez de derrubar o processo.
    """
    return _NIVEIS.get(nivel.strip().lower(), logging.INFO)


# --------------------------------------------------------------------------- #
# Superficie publica (secao 9.2.2)                                             #
# --------------------------------------------------------------------------- #


class Logger(Protocol):
    """Contrato minimo do logger devolvido por :func:`get_logger`.

    Existe para que os servicos dependam de uma interface, e nao do tipo concreto do ``structlog``.
    O primeiro argumento e sempre um valor de :class:`LogEvent`; os campos adicionais viram chaves
    da linha JSON.
    """

    def debug(self, evento: str, /, **campos: Any) -> None:  # noqa: ANN401
        """Registra evento de diagnostico fino, suprimido em producao (``LOG_LEVEL=INFO``).

        Args:
            evento: Valor de :class:`LogEvent`.
            **campos: Campos adicionais da linha de log.
        """
        ...

    def info(self, evento: str, /, **campos: Any) -> None:  # noqa: ANN401
        """Registra evento de operacao normal com valor de diagnostico.

        Args:
            evento: Valor de :class:`LogEvent`.
            **campos: Campos adicionais da linha de log.
        """
        ...

    def warning(self, evento: str, /, **campos: Any) -> None:  # noqa: ANN401
        """Registra falha recuperavel ou esperada sob falha parcial (retentativa, ``auth.negada``).

        Args:
            evento: Valor de :class:`LogEvent`.
            **campos: Campos adicionais da linha de log.
        """
        ...

    def error(self, evento: str, /, **campos: Any) -> None:  # noqa: ANN401
        """Registra falha com consequencia externa visivel (DLQ, ``504`` ao cliente).

        Args:
            evento: Valor de :class:`LogEvent`.
            **campos: Campos adicionais da linha de log.
        """
        ...

    def critical(self, evento: str, /, **campos: Any) -> None:  # noqa: ANN401
        """Registra falha que impede o processo de continuar operando.

        Args:
            evento: Valor de :class:`LogEvent`.
            **campos: Campos adicionais da linha de log.
        """
        ...

    def exception(self, evento: str, /, **campos: Any) -> None:  # noqa: ANN401
        """Registra em nivel ``error`` anexando a excecao em tratamento.

        Args:
            evento: Valor de :class:`LogEvent`.
            **campos: Campos adicionais da linha de log.
        """
        ...


_configuracao_atual: tuple[str, str, str, bool] | None = None
_RUIDOSOS: Final[tuple[str, ...]] = ("uvicorn.access", "aio_pika", "aiormq", "sqlalchemy.engine")


def _renderizador(formato: str) -> Processor:
    """Escolhe o renderizador final do *pipeline*.

    Args:
        formato: ``"json"`` (padrao do Docker Compose) ou ``"console"`` (apenas fora dele).

    Returns:
        ``JSONRenderer`` para ``"json"``; ``ConsoleRenderer`` colorido para qualquer outro valor.
        O separador compacto reproduz, byte a byte, as linhas de exemplo da secao 9.3.2 e economiza
        os ~40 bytes por linha contabilizados em 9.2.5.
    """
    if formato == "json":
        return structlog.processors.JSONRenderer(
            ensure_ascii=False, sort_keys=False, separators=(",", ":")
        )
    return structlog.dev.ConsoleRenderer(
        event_key="evento", timestamp_key="timestamp", sort_keys=False
    )


def _pipeline(servico: str, *, formato: str, mascarar_pii: bool) -> list[Processor]:
    """Monta a lista de processadores na ordem normativa da secao 9.2.3.

    Args:
        servico: Nome do processo, injetado no campo ``servico``.
        formato: ``"json"`` ou ``"console"``.
        mascarar_pii: Quando ``False``, o passo de mascaramento vira um processador neutro.

    Returns:
        A lista de processadores, do primeiro ao renderizador.
    """
    return [
        structlog.contextvars.merge_contextvars,
        _add_service(servico),
        structlog.stdlib.add_log_level,
        _rename_key("level", "nivel"),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        mask_pii if mascarar_pii else _noop,
        structlog.processors.format_exc_info,
        _truncar_excecao,
        structlog.processors.EventRenamer("evento"),
        _ordenar_campos,
        _renderizador(formato),
    ]


def _configurar_ponte_stdlib(servico: str, *, nivel: str, formato: str, mascarar_pii: bool) -> None:
    """Faz as bibliotecas de terceiros sairem no mesmo formato JSON (secao 9.2.4).

    Sem esta ponte, o *access log* do uvicorn sairia em texto e a primeira linha nao-JSON faria o
    ``jq`` da demonstracao abortar. ``aio_pika`` e ``aiormq`` sao rebaixados a ``WARNING`` para que
    o *heartbeat* do AMQP nao polua o rastro que o avaliador vai ler.

    Args:
        servico: Nome do processo.
        nivel: Nivel minimo do logger raiz.
        formato: ``"json"`` ou ``"console"``.
        mascarar_pii: Se o mascaramento LGPD deve ser aplicado tambem aos logs de terceiros.
    """
    manipulador = logging.StreamHandler()
    manipulador.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=[
                _add_service(servico),
                structlog.stdlib.add_log_level,
                _rename_key("level", "nivel"),
                structlog.processors.TimeStamper(fmt="iso", utc=True),
            ],
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                # Diferente do pipeline proprio (9.2.3), aqui o format_exc_info vem ANTES do
                # mascaramento: o ProcessorFormatter injeta a tripla ``exc_info`` crua do
                # ``LogRecord``, que sem esta conversao seria serializada como lista de ``repr``.
                structlog.processors.format_exc_info,
                mask_pii if mascarar_pii else _noop,
                _truncar_excecao,
                structlog.processors.EventRenamer("evento"),
                _ordenar_campos,
                _renderizador(formato),
            ],
        )
    )
    raiz = logging.getLogger()
    raiz.handlers = [manipulador]
    raiz.setLevel(_nivel_numerico(nivel))
    for ruidoso in _RUIDOSOS:
        logging.getLogger(ruidoso).setLevel(logging.WARNING)


def configure_logging(
    servico: str,
    *,
    nivel: str = "INFO",
    formato: str = "json",
    mascarar_pii: bool = True,
) -> None:
    """Configura o log estruturado do processo (R5.1, R5.6).

    Idempotente: chamada uma vez no *startup* de cada processo, antes de qualquer log. Repetir a
    chamada com os mesmos argumentos nao faz nada; repetir com argumentos diferentes reconfigura o
    *pipeline* -- o que a suite de testes usa para exercitar :func:`mask_pii` ligado e desligado.

    Args:
        servico: Nome do processo, normalmente ``settings.service_name``. Alimenta o campo
            ``servico`` de toda linha, que R5.1 exige.
        nivel: Nivel minimo emitido. O filtro ocorre **antes** de montar o dicionario, entao um
            ``log.debug`` suprimido custa quase nada -- o que importa porque ``mensagem.recebida``
            e DEBUG e seria emitida a cada leitura de cada leito.
        formato: ``"json"`` no Docker Compose; ``"console"`` apenas em execucao local fora dele.
        mascarar_pii: Mascaramento LGPD de R5.6. So e desligado no teste unitario do processador.
    """
    global _configuracao_atual

    assinatura = (servico, nivel, formato, mascarar_pii)
    if _configuracao_atual == assinatura:
        return

    structlog.configure(
        processors=_pipeline(servico, formato=formato, mascarar_pii=mascarar_pii),
        wrapper_class=structlog.make_filtering_bound_logger(_nivel_numerico(nivel)),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configurar_ponte_stdlib(servico, nivel=nivel, formato=formato, mascarar_pii=mascarar_pii)
    _configuracao_atual = assinatura


def get_logger(nome: str | None = None) -> Logger:
    """Devolve o logger do modulo chamador, ja ligado ao *pipeline* configurado.

    Args:
        nome: Nome do logger, tipicamente ``__name__``. Quando omitido, o ``structlog`` deduz o
            modulo de origem.

    Returns:
        Um :class:`Logger` cujo primeiro argumento e sempre um valor de :class:`LogEvent`.
    """
    if nome is None:
        return structlog.get_logger()  # type: ignore[no-any-return]
    return structlog.get_logger(nome)  # type: ignore[no-any-return]


# --------------------------------------------------------------------------- #
# Correlacao por contextvars (secao 9.5)                                       #
# --------------------------------------------------------------------------- #


def bind_context(**campos: Any) -> None:  # noqa: ANN401
    """Amarra campos ao contexto assincrono corrente (R5.2, R5.3).

    Os campos amarrados aqui sao injetados em **toda** linha emitida no mesmo contexto, sem passar
    por parametro de funcao. E o que faz R5.3 ser propriedade do middleware e nao convencao que
    cada servico precisa lembrar de seguir: o ``Consumer`` amarra antes de invocar o handler e o
    ``Publisher`` le ao publicar.

    Args:
        **campos: Pares a amarrar, tipicamente ``correlation_id``, ``causation_id``,
            ``message_id``, ``tipo`` e ``tentativa``.
    """
    structlog.contextvars.bind_contextvars(**campos)


def clear_context() -> None:
    """Limpa o contexto de correlacao do contexto assincrono corrente.

    Chamada no ``finally`` do middleware HTTP do gateway e do despacho do ``Consumer``: um
    ``contextvars`` sobrevive ao termino da requisicao se o *worker* for reutilizado, e um
    ``correlation_id`` vazado para a requisicao seguinte corromperia o rastro da demonstracao.
    """
    structlog.contextvars.clear_contextvars()


def contexto_de_log(**campos: Any) -> AbstractContextManager[None]:  # noqa: ANN401
    """Amarra campos apenas durante o bloco ``with`` (secao 4.5.4).

    Ao sair do bloco, o contexto anterior e restaurado -- diferente de :func:`clear_context`, que
    apaga tudo. E a forma preferida dentro do ``Consumer``, onde o contexto da mensagem nao deve
    sobreviver a entrega.

    Args:
        **campos: Pares a amarrar durante o bloco.

    Returns:
        Gerenciador de contexto que restaura o estado anterior na saida.
    """
    return structlog.contextvars.bound_contextvars(**campos)


def contexto_atual() -> dict[str, Any]:
    """Devolve uma copia dos campos amarrados ao contexto assincrono corrente.

    Returns:
        Dicionario dos pares vinculados por :func:`bind_context`; vazio fora de qualquer
        requisicao ou entrega de mensagem.
    """
    return dict(structlog.contextvars.get_contextvars())


def correlation_id_atual() -> str | None:
    """Recupera o ``correlation_id`` do contexto corrente (R5.2, R5.3).

    Usado pelo ``Publisher`` para herdar a correlacao ao publicar um evento derivado e pelo
    gateway para incluir o identificador no corpo de erro RFC 7807 (R8.2).

    Returns:
        O ``correlation_id`` amarrado, ou ``None`` se nao houver contexto de requisicao ou
        mensagem -- caso dos logs de *startup* e *shutdown*.
    """
    valor = structlog.contextvars.get_contextvars().get("correlation_id")
    return valor if isinstance(valor, str) else None


def causation_id_atual() -> str | None:
    """Recupera o ``causation_id`` do contexto corrente (secao 9.5.3).

    Enquanto o ``correlation_id`` **agrupa** tudo que pertence a mesma requisicao de negocio, o
    ``causation_id`` **ordena**: e a aresta explicita filho para pai da arvore de causa e efeito.

    Returns:
        O ``causation_id`` amarrado, ou ``None`` na primeira mensagem da cadeia.
    """
    valor = structlog.contextvars.get_contextvars().get("causation_id")
    return valor if isinstance(valor, str) else None


def message_id_atual() -> str | None:
    """Recupera o ``message_id`` da mensagem em processamento (secao 9.5.2).

    E o valor que o ``Publisher`` usa como ``causation_id`` do evento derivado quando o handler
    nao informa nenhum: o pai imediato do que esta sendo publicado.

    Returns:
        O ``message_id`` amarrado pelo ``Consumer`` antes de invocar o handler, ou ``None`` fora
        de uma entrega de mensagem.
    """
    valor = structlog.contextvars.get_contextvars().get("message_id")
    return valor if isinstance(valor, str) else None
