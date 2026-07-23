"""HospitalMQ -- middleware orientado a mensagem do Hospital Inteligente (G3).

O pacote e o artefato avaliado do trabalho (C1): Envelope com correlacao e causalidade,
publicacao e consumo assincronos com ACK manual, retentativa exponencial com *dead letter queue*,
idempotencia por ``(consumidor, message_id)``, RPC sobre fila, propagacao de identidade,
observabilidade estruturada e transporte plugavel. O RabbitMQ e apenas o transporte; a semantica
de mensageria e escrita aqui.

Uso tipico em um servico::

    from hospitalmq import Consumer, Publisher, criar_transporte, settings

    transporte = criar_transporte(settings)         # amqp | memory | sqs (R9.2)
    await transporte.connect()
    await transporte.declare_topology(TOPOLOGIA_PADRAO)

    eventos = Publisher(transport=transporte, producer=settings.service_name)
    await eventos.publish("sinais.coletados", {"leito_id": "UTI-07", ...})

Os simbolos sao resolvidos por **importacao preguicosa** (PEP 562): ``import hospitalmq`` nao
carrega ``aio_pika``, ``sqlalchemy`` nem ``fastapi``, e cada submodulo so entra em memoria quando o
nome que ele define e efetivamente usado. Alem de acelerar o *boot*, e isso que permite ao
``bedside_monitor`` -- que fala apenas HTTP -- importar o pacote sem arrastar o driver AMQP.
"""

from __future__ import annotations

import importlib
from typing import Any, Final

__version__: Final[str] = "0.1.0"

_EXPORTACOES: Final[dict[str, str]] = {
    # nucleo do contrato de dados
    "Envelope": "hospitalmq.envelope",
    "Identity": "hospitalmq.envelope",
    "TipoIdentidade": "hospitalmq.envelope",
    # hierarquia de erros
    "HospitalMQError": "hospitalmq.errors",
    "TransportError": "hospitalmq.errors",
    "TransientError": "hospitalmq.errors",
    "PermanentError": "hospitalmq.errors",
    "InvalidEnvelopeError": "hospitalmq.errors",
    "AuthError": "hospitalmq.errors",
    "ConfigError": "hospitalmq.errors",
    "RpcTimeoutError": "hospitalmq.errors",
    "RpcRemoteError": "hospitalmq.errors",
    "MAPA_HTTP": "hospitalmq.errors",
    # relogio injetavel
    "Clock": "hospitalmq.clock",
    "RealClock": "hospitalmq.clock",
    "ManualClock": "hospitalmq.clock",
    # configuracao e topologia
    "Settings": "hospitalmq.config",
    "settings": "hospitalmq.config",
    "Binding": "hospitalmq.config",
    "ExchangeSpec": "hospitalmq.config",
    "QueueSpec": "hospitalmq.config",
    "TopologySpec": "hospitalmq.config",
    "TOPOLOGIA_PADRAO": "hospitalmq.config",
    "fila_de_retorno_rpc": "hospitalmq.config",
    "criar_transporte": "hospitalmq.config",
    "build_transport": "hospitalmq.config",
    # contrato de transporte
    "Transport": "hospitalmq.transport.base",
    "InboundMessage": "hospitalmq.transport.base",
    "MessageHandler": "hospitalmq.transport.base",
    "Subscription": "hospitalmq.transport.base",
    # API de mensageria
    "Publisher": "hospitalmq.publisher",
    "Consumer": "hospitalmq.consumer",
    "MessageContext": "hospitalmq.consumer",
    "RpcClient": "hospitalmq.rpc",
    "RpcServer": "hospitalmq.rpc",
}

__all__ = ["__version__", *sorted(_EXPORTACOES)]


def __getattr__(nome: str) -> Any:  # noqa: ANN401 - assinatura fixada pela PEP 562
    """Resolve um simbolo publico importando seu modulo sob demanda (PEP 562).

    Args:
        nome: Nome do simbolo acessado como atributo de ``hospitalmq``.

    Returns:
        O objeto exportado, ja memorizado em ``globals()`` para que o acesso seguinte nao repita
        a importacao.

    Raises:
        AttributeError: Se o nome nao pertencer a API publica do pacote.
    """
    modulo = _EXPORTACOES.get(nome)
    if modulo is None:
        raise AttributeError(f"modulo 'hospitalmq' nao exporta {nome!r}")
    valor = getattr(importlib.import_module(modulo), nome)
    globals()[nome] = valor
    return valor


def __dir__() -> list[str]:
    """Lista a API publica para autocompletar e introspeccao.

    Returns:
        Os nomes exportados pelo pacote, em ordem alfabetica.
    """
    return sorted(set(__all__) | set(globals()))
