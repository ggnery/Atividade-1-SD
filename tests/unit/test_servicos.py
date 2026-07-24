"""Testes dos handlers clinicos sobre ``MemoryTransport``, sem banco nem broker (R10.4).

Exercita a integracao real dos servicos com o middleware ``hospitalmq`` congelado, usando a
idempotencia em memoria (``session_factory=None``) e o registro de publicacoes do
``MemoryTransport`` para observar os eventos derivados:

* ``vitals-service`` rejeita leitura fora da faixa fisiologica e **nao** calcula NEWS2 (R6.6);
* ``triage-service`` emite ``alerta.gerado`` quando a severidade e alta e nao emite quando baixa
  (R6.2, R6.3);
* ``alert-service`` traduz a falha do canal em ``TransientError`` e o middleware retenta 1s/2s/4s
  antes da DLQ (R6.5);
* o canal sabotavel falha sempre com taxa ``1.0`` e nunca com ``0.0`` (design 12.5).

Os handlers que **gravam** dominio (caminho de sucesso do vitals e do alert) exigem PostgreSQL de
verdade e ficam para o e2e do docker compose; aqui cobrem-se os caminhos puros e de rejeicao, que
nao tocam a ``ctx.session``.
"""

from __future__ import annotations

import importlib
import sys
import types
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from hospitalmq.clock import ManualClock
from hospitalmq.consumer import Consumer
from hospitalmq.errors import TransientError
from hospitalmq.publisher import Publisher
from hospitalmq.transport.memory import MemoryTransport

_RAIZ = Path(__file__).resolve().parents[2]
_SERVICOS = _RAIZ / "services"
_TOP = ("modelos", "handler", "main", "notificacao", "repositorio", "outbox", "operacoes")


def _carregar(servico: str, *modulos: str) -> tuple[Any, ...]:
    """Importa modulos de topo de um servico de diretorio hifenizado, de forma consistente.

    Limpa os nomes de topo colididos entre servicos, poe o diretorio do servico a frente de
    ``sys.path`` e importa os modulos pedidos **na mesma passagem** -- garantindo que ``handler``,
    ``modelos`` e ``notificacao`` de um servico compartilhem as mesmas classes (importantes para
    ``isinstance`` e para a identidade das excecoes).
    """
    for nome in list(sys.modules):
        if nome in _TOP:
            del sys.modules[nome]
    caminho = str(_SERVICOS / servico)
    if caminho in sys.path:
        sys.path.remove(caminho)
    sys.path.insert(0, caminho)
    return tuple(importlib.import_module(m) for m in modulos)


# --------------------------------------------------------------------------- #
# vitals-service: rejeita fora de faixa e nao calcula NEWS2 (R6.6)             #
# --------------------------------------------------------------------------- #


def _payload_coletado(**override: object) -> dict[str, Any]:
    """Payload estruturalmente valido de ``sinais.coletados`` (design 6.4, 8.2)."""
    payload: dict[str, Any] = {
        "leito_id": "UTI-07",
        "internacao_id": str(uuid.uuid4()),
        "frequencia_respiratoria": 18,
        "saturacao_o2": 96,
        "oxigenio_suplementar": False,
        "temperatura": "37.0",
        "pressao_sistolica": 120,
        "frequencia_cardiaca": 70,
        "nivel_consciencia": "A",
        "coletado_em": datetime.now(UTC).isoformat(),
    }
    payload.update(override)
    return payload


async def test_vitals_rejeita_fora_de_faixa_e_nao_registra(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R6.6: ``saturacao_o2`` fora da faixa vira ``sinais.rejeitados`` + DLQ, sem NEWS2."""
    (handler_mod,) = _carregar("vitals-service", "handler")

    publisher = Publisher(transport=mem, producer="vitals-service", clock=clock)
    consumidor = Consumer(
        service="vitals-service", transport=mem, session_factory=None, clock=clock
    )
    consumidor.on("sinais.coletados", queue="q.vitals.sinais-coletados", modelo=None)(
        handler_mod.criar_handler_sinais_coletados(publisher)
    )
    await consumidor.iniciar()

    entrada = Publisher(transport=mem, producer="api-gateway", clock=clock)
    await entrada.publish("sinais.coletados", _payload_coletado(saturacao_o2=20))
    await mem.drenar()

    rejeitados = mem.eventos("sinais.rejeitados")
    assert len(rejeitados) == 1
    assert rejeitados[0].payload["campo"] == "saturacao_o2"
    assert rejeitados[0].payload["motivo"] == "sinais_fora_de_faixa"

    # NEWS2 nunca e calculado nem promovido: nao ha sinais.registrados.
    assert mem.eventos("sinais.registrados") == []
    # Erro permanente -> DLQ direto, sem retentativa.
    assert len(mem.envelopes_na_dlq("q.vitals.sinais-coletados")) == 1
    assert clock.sleeps == []


# --------------------------------------------------------------------------- #
# triage-service: emite alerta.gerado se alta, nao emite se baixa (R6.2, R6.3) #
# --------------------------------------------------------------------------- #


def _payload_registrados(**override: object) -> dict[str, Any]:
    """Payload de ``sinais.registrados`` produzido pelo vitals-service (design 6.4)."""
    payload: dict[str, Any] = {
        "sinais_vitais_id": str(uuid.uuid4()),
        "internacao_id": str(uuid.uuid4()),
        "leito_id": "UTI-03",
        "coletado_em": datetime.now(UTC).isoformat(),
        "frequencia_respiratoria": 18,
        "saturacao_o2": 95,
        "oxigenio_suplementar": False,
        "temperatura": "37.2",
        "pressao_sistolica": 118,
        "frequencia_cardiaca": 88,
        "nivel_consciencia": "A",
    }
    payload.update(override)
    return payload


async def _rodar_triage(mem: MemoryTransport, clock: ManualClock, payload: dict[str, Any]) -> None:
    """Registra o handler de triagem num ``Consumer`` sobre ``mem`` e processa uma leitura."""
    (handler_mod,) = _carregar("triage-service", "handler")
    consumidor = Consumer(
        service="triage-service", transport=mem, session_factory=None, clock=clock
    )
    handler_mod.registrar_handlers(consumidor)
    await consumidor.iniciar()
    entrada = Publisher(transport=mem, producer="vitals-service", clock=clock)
    await entrada.publish("sinais.registrados", payload)
    await mem.drenar()


async def test_triage_emite_alerta_quando_news2_alto(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R6.3: leitura de deterioracao (total 15, alta) gera ``alerta.gerado`` com ``alerta_id``."""
    # Exemplo B do design 7.5.4: total 15, severidade alta.
    payload = _payload_registrados(
        frequencia_respiratoria=26,
        saturacao_o2=92,
        oxigenio_suplementar=True,
        temperatura="38.6",
        pressao_sistolica=96,
        frequencia_cardiaca=118,
        nivel_consciencia="V",
    )
    await _rodar_triage(mem, clock, payload)

    gerados = mem.eventos("alerta.gerado")
    assert len(gerados) == 1
    corpo = gerados[0].payload
    # alerta_id e parte normativa do payload (design 6.4): sem ele o alert-service rejeitaria.
    assert uuid.UUID(str(corpo["alerta_id"]))
    assert corpo["severidade"] == "alta"
    assert corpo["score_news2"] == 15
    assert corpo["componente_critico"] is True
    # Correlacao preservada e causalidade encadeada (R5.3).
    assert gerados[0].causation_id is not None


async def test_triage_nao_emite_alerta_quando_news2_baixo(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R6.2/R6.3: leitura estavel (total 1, baixa) nao gera nenhum ``alerta.gerado``."""
    # Exemplo A do design 7.5.3: total 1, severidade baixa.
    payload = _payload_registrados(
        frequencia_respiratoria=18,
        saturacao_o2=95,
        oxigenio_suplementar=False,
        temperatura="37.2",
        pressao_sistolica=118,
        frequencia_cardiaca=88,
        nivel_consciencia="A",
    )
    await _rodar_triage(mem, clock, payload)
    assert mem.eventos("alerta.gerado") == []


# --------------------------------------------------------------------------- #
# alert-service: falha de canal -> TransientError -> retentativa 1s/2s/4s (R6.5)#
# --------------------------------------------------------------------------- #


def _payload_alerta_gerado(**override: object) -> dict[str, Any]:
    """Payload valido de ``alerta.gerado`` (design 6.4), como o triage-service o emite."""
    payload: dict[str, Any] = {
        "alerta_id": str(uuid.uuid4()),
        "internacao_id": str(uuid.uuid4()),
        "leito_id": "ENF-07",
        "sinais_vitais_id": str(uuid.uuid4()),
        "score_news2": 15,
        "componentes": {
            "frequencia_respiratoria": 3,
            "saturacao_o2": 2,
            "oxigenio_suplementar": 2,
            "temperatura": 1,
            "pressao_sistolica": 2,
            "frequencia_cardiaca": 2,
            "nivel_consciencia": 3,
        },
        "severidade": "alta",
        "componente_critico": True,
        "coletado_em": datetime.now(UTC).isoformat(),
    }
    payload.update(override)
    return payload


async def test_alert_falha_de_canal_retenta_e_vai_a_dlq(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R6.5: canal indisponivel -> ``TransientError`` -> 3 retentativas 1s/2s/4s -> DLQ na 4a."""
    handler_mod, notificacao_mod = _carregar("alert-service", "handler", "notificacao")
    canal_indisponivel = notificacao_mod.CanalIndisponivelError

    class CanalSempreFalha:
        """Canal que representa a equipe do leito sempre inalcancavel (design 12.5, taxa 1.0)."""

        nome = "teste-fail"

        def __init__(self) -> None:
            self.chamadas = 0

        async def notificar(self, alerta: Any) -> str:
            self.chamadas += 1
            raise canal_indisponivel("canal de notificacao indisponivel")

    canal = CanalSempreFalha()
    publisher = Publisher(transport=mem, producer="alert-service", clock=clock)
    consumidor = Consumer(service="alert-service", transport=mem, session_factory=None, clock=clock)
    handler_mod.registrar_handler(consumidor, canal, publisher)
    await consumidor.iniciar()

    entrada = Publisher(transport=mem, producer="triage-service", clock=clock)
    await entrada.publish("alerta.gerado", _payload_alerta_gerado())
    await mem.drenar(limite=2.0)

    # Uma entrega original + tres retentativas: o middleware retentou (R2.2/R6.5).
    assert canal.chamadas == 4
    assert clock.sleeps == [1.0, 2.0, 4.0]
    mortas = mem.na_dlq("q.alert.alerta-gerado")
    assert len(mortas) == 1
    assert mortas[0].attempt == 4
    # Ao esgotar as retentativas, o handler publica alerta.falhou para a auditoria (design 6.3).
    falhou = mem.eventos("alerta.falhou")
    assert len(falhou) == 1
    assert falhou[0].payload["alerta_id"] == _alerta_id_da_dlq(mortas[0])


def _alerta_id_da_dlq(morta: Any) -> str:
    """Extrai o ``alerta_id`` do Envelope preservado na DLQ, para casar com ``alerta.falhou``."""
    assert morta.envelope is not None
    return str(morta.envelope.payload["alerta_id"])


async def test_alert_handler_traduz_falha_em_transient_error(
    mem: MemoryTransport, clock: ManualClock
) -> None:
    """R6.5: com retentativas ainda disponiveis, o handler levanta ``TransientError`` diretamente.

    Prova a traducao no nivel do handler (sem depender da escada de retentativa), invocando-o com um
    ``MessageContext`` montado a mao -- o caminho de falha nao toca a ``ctx.session``.
    """
    from hospitalmq.consumer import MessageContext
    from hospitalmq.envelope import Envelope

    handler_mod, notificacao_mod = _carregar("alert-service", "handler", "notificacao")
    canal_indisponivel = notificacao_mod.CanalIndisponivelError

    class CanalFalha:
        nome = "teste"

        async def notificar(self, alerta: Any) -> str:
            raise canal_indisponivel("fora do ar")

    publisher = Publisher(transport=mem, producer="alert-service", clock=clock)
    handler = handler_mod.criar_handler_alerta_gerado(CanalFalha(), publisher)

    payload = handler_mod.AlertaGerado.model_validate(_payload_alerta_gerado())
    envelope = Envelope(
        message_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4()),
        causation_id=None,
        type="alerta.gerado",
        version=1,
        timestamp=datetime.now(UTC),
        producer="triage-service",
        identity=None,
        attempt=1,
        payload=_payload_alerta_gerado(),
    )
    ctx: MessageContext[Any] = MessageContext(
        envelope=envelope,
        payload=payload,
        identity=None,
        session=object(),  # nao usado no caminho de falha
        log=consumidor_log(),
        producer="alert-service",
        attempt=1,
        tentativas_restantes=3,
    )
    with pytest.raises(TransientError):
        await handler(ctx)


def consumidor_log() -> Any:
    """Logger estruturado do middleware, para montar o ``MessageContext`` de teste."""
    from hospitalmq.logging import get_logger

    return get_logger("teste-alert")


# --------------------------------------------------------------------------- #
# Canal sabotavel: falha sempre com taxa 1.0, nunca com taxa 0.0 (design 12.5)  #
# --------------------------------------------------------------------------- #


async def test_canal_simulado_taxa_um_sempre_falha() -> None:
    """Design 12.5: ``ALERT_FAILURE_RATE = 1.0`` -> toda notificacao falha (reprodutivel)."""
    (notificacao_mod,) = _carregar("alert-service", "notificacao")
    canal = notificacao_mod.CanalNotificacaoSimulado(
        taxa_padrao=1.0, leitos_sabotados=frozenset(), taxa_sabotada=1.0, semente=1
    )
    alerta = types.SimpleNamespace(leito_codigo="ENF-07")
    for _ in range(25):
        with pytest.raises(notificacao_mod.CanalIndisponivelError):
            await canal.notificar(alerta)


async def test_canal_simulado_taxa_zero_nunca_falha() -> None:
    """Design 12.5: ``ALERT_FAILURE_RATE = 0.0`` sem leito sabotado -> nenhuma notificacao falha."""
    (notificacao_mod,) = _carregar("alert-service", "notificacao")
    canal = notificacao_mod.CanalNotificacaoSimulado(
        taxa_padrao=0.0, leitos_sabotados=frozenset(), taxa_sabotada=0.0, semente=1
    )
    alerta = types.SimpleNamespace(leito_codigo="ENF-07")
    for _ in range(25):
        destinatario = await canal.notificar(alerta)
        assert destinatario == "equipe-ENF-07"


async def test_canal_de_ambiente_le_taxa_global(monkeypatch: pytest.MonkeyPatch) -> None:
    """``criar_canal_de_ambiente`` le ``ALERT_FAILURE_RATE`` do ambiente (design 12.5.3/12.5.4)."""
    (notificacao_mod,) = _carregar("alert-service", "notificacao")
    monkeypatch.setenv("ALERT_FAILURE_RATE", "1.0")
    monkeypatch.setenv("ALERT_FAILURE_LEITOS", "")
    monkeypatch.setenv("ALERT_FAILURE_RATE_LEITOS", "1.0")
    canal = notificacao_mod.criar_canal_de_ambiente()
    alerta = types.SimpleNamespace(leito_codigo="ENF-99")
    for _ in range(10):
        with pytest.raises(notificacao_mod.CanalIndisponivelError):
            await canal.notificar(alerta)


def test_temperatura_quantizada_no_payload_registrados() -> None:
    """Sanidade de contrato: o quantum de temperatura do vitals-service e uma casa decimal."""
    (handler_mod,) = _carregar("vitals-service", "handler")
    assert Decimal("0.1") == handler_mod._QUANTUM_TEMPERATURA
