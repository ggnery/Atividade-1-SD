"""Log estruturado em JSON, correlacao/causalidade e mascaramento LGPD (R5.1, R5.3, R5.6)."""

from __future__ import annotations

import json

import pytest

from hospitalmq.clock import ManualClock
from hospitalmq.envelope import Envelope
from hospitalmq.logging import (
    LogEvent,
    configure_logging,
    contexto_de_log,
    get_logger,
)
from hospitalmq.publisher import Publisher
from hospitalmq.transport.memory import MemoryTransport


def _ultima_linha_json(saida: str) -> dict[str, object]:
    linhas = [linha for linha in saida.strip().splitlines() if linha.strip()]
    return json.loads(linhas[-1])


def test_linha_de_log_sai_em_json_com_timestamp_correlation_e_message_id(
    capfd: pytest.CaptureFixture[str],
) -> None:
    """R5.1: a linha sai em JSON com timestamp ISO-8601 UTC, correlation_id e message_id."""
    configure_logging("api-gateway", nivel="INFO", formato="json")
    log = get_logger("teste_log_json")

    with contexto_de_log(correlation_id="corr-123", message_id="msg-456"):
        log.info(LogEvent.MENSAGEM_PUBLICADA, tipo="alerta.gerado")

    linha = _ultima_linha_json(capfd.readouterr().out)
    assert linha["evento"] == "mensagem.publicada"
    assert linha["correlation_id"] == "corr-123"
    assert linha["message_id"] == "msg-456"
    timestamp = linha["timestamp"]
    assert isinstance(timestamp, str)
    assert timestamp.endswith("Z")  # ISO-8601 UTC


def test_dado_pessoal_sai_mascarado_do_log(capfd: pytest.CaptureFixture[str]) -> None:
    """R5.6: nome e CPF saem mascarados; identificadores tecnicos permanecem."""
    configure_logging("api-gateway", nivel="INFO", formato="json")
    log = get_logger("teste_log_pii")

    log.info(
        LogEvent.MENSAGEM_PROCESSADA,
        nome="Ana Silva",
        cpf="123.456.789-00",
        paciente_id="P-1",
    )

    saida = capfd.readouterr().out
    linha = _ultima_linha_json(saida)
    assert linha["nome"] == "***"
    assert "Ana Silva" not in saida
    assert "123.456.789-00" not in saida
    assert linha["paciente_id"] == "P-1"


async def test_correlation_id_preservado_e_causation_aponta_para_o_pai(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R5.3: no evento derivado o correlation_id se preserva e causation_id aponta para o pai."""
    pai = Envelope(
        message_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        correlation_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        causation_id=None,
        type="sinais.coletados",
        version=1,
        timestamp=clock.now(),
        producer="vitals-service",
        identity=None,
        attempt=1,
        payload={"leito_id": "UTI-07"},
    )

    filho = pai.derivar(tipo="sinais.registrados", payload={"news2": 3}, producer="triage-service")

    assert filho.correlation_id == pai.correlation_id
    assert filho.causation_id == pai.message_id
    assert filho.message_id != pai.message_id


async def test_publisher_herda_correlation_id_do_contexto_corrente(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R5.3: o Publisher herda o correlation_id e o causation_id do contexto corrente."""
    pub = Publisher(transport=mem, producer="triage-service", clock=clock)

    with contexto_de_log(correlation_id="corr-fluxo", causation_id="msg-pai"):
        envelope = await pub.publish("alerta.gerado", {"leito_id": "UTI-07"})

    assert envelope.correlation_id == "corr-fluxo"
    assert envelope.causation_id == "msg-pai"
