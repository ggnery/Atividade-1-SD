"""Drivers de transporte do HospitalMQ.

O contrato abstrato vive em :mod:`hospitalmq.transport.base` e e a definicao normativa unica da
interface (R9.1). Os drivers concretos -- ``amqp``, ``memory`` e ``sqs`` -- sao importados **sob
demanda** pela *factory* :func:`hospitalmq.config.criar_transporte`, de modo que um processo que
roda com ``TRANSPORTE=memory`` nunca carrega ``aio_pika`` nem ``boto3``.
"""

from hospitalmq.transport.base import (
    InboundMessage,
    MessageHandler,
    Subscription,
    Transport,
)

__all__ = ["InboundMessage", "MessageHandler", "Subscription", "Transport"]
