"""Teste ponta a ponta do fluxo clinico contra o ``api-gateway`` no ar (design 12.3.2).

Este nivel exige a stack completa do ``docker compose up -d`` (RabbitMQ, PostgreSQL,
gateway e os cinco consumidores). Nao sobe nada: fala HTTP com ``http://localhost:8000``
como um cliente real e verifica o caminho feliz da demonstracao: login, criacao de
paciente, admissao, telemetria estavel, prontuario enriquecido, deterioracao (NEWS2
alto) e projecoes de leitos e alertas.

Quando o gateway nao responde em ``/health`` (compose no chao), o modulo inteiro e **pulado** com
mensagem clara, em vez de falhar -- e o contrato do marcador ``e2e`` (secao 10.8.2). Rodar:

    .venv/bin/pytest tests/e2e -q                     # exige o compose no ar
    BASE_URL=http://localhost:8000 .venv/bin/pytest tests/e2e -q

Os ``correlation_id`` sao UUIDv4 de proposito: a coluna ``correlacao`` da marca de
idempotencia e a trilha de auditoria sao ``uuid``; um valor nao-UUID em rota de escrita
viraria ``ENVELOPE_INVALIDO``
(nucleo, ver ``hospitalmq/idempotency.py``). Clientes reais deixam o gateway gerar o id; aqui os
fixamos so para poder segui-los.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.e2e

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
_TIMEOUT = httpx.Timeout(10.0)


def _health_ok() -> bool:
    """Retorna ``True`` se o gateway responde ``200`` em ``/health`` (readiness da stack)."""
    try:
        resposta = httpx.get(f"{BASE_URL}/health", timeout=2.0)
    except httpx.HTTPError:
        return False
    return resposta.status_code == 200


# Pula o modulo inteiro, com mensagem clara, quando o compose nao esta no ar (contrato do marcador).
if not _health_ok():
    pytest.skip(
        f"compose fora do ar: {BASE_URL}/health nao respondeu 200 -- "
        "suba com `docker compose up -d --build` antes de rodar tests/e2e",
        allow_module_level=True,
    )


def _cid() -> str:
    """Gera um ``correlation_id`` UUIDv4 novo para uma requisicao."""
    return str(uuid.uuid4())


def _agora_iso() -> str:
    """Instante atual em ISO-8601 UTC com sufixo ``Z`` (formato aceito por ``coletado_em``)."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture(scope="module")
def cliente() -> httpx.Client:
    """Cliente HTTP reutilizado por todo o modulo, apontado ao gateway."""
    with httpx.Client(base_url=BASE_URL, timeout=_TIMEOUT) as c:
        yield c


@pytest.fixture(scope="module")
def token_enfermeiro(cliente: httpx.Client) -> str:
    """JWT de ``enf.ana`` (papel ``enfermeiro``) -- passo (c) do roteiro."""
    resposta = cliente.post(
        "/auth/token", json={"usuario": "enf.ana", "senha": "demo123"}
    )
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["role"] == "enfermeiro"
    assert corpo["token_type"] == "bearer"
    assert isinstance(corpo["access_token"], str) and corpo["access_token"]
    return corpo["access_token"]


@pytest.fixture(scope="module")
def contexto() -> dict[str, Any]:
    """Estado compartilhado entre os passos ordenados (ids criados no caminho)."""
    return {}


@pytest.mark.requisito("R4.1")
def test_c_login_emite_jwt(token_enfermeiro: str) -> None:
    """(c) ``POST /auth/token`` com credencial de enfermeiro devolve um JWT nao vazio."""
    assert token_enfermeiro.count(".") == 2  # header.payload.assinatura


@pytest.mark.requisito("R8.2")
def test_d_prontuario_sem_token_401_problem_json(cliente: httpx.Client) -> None:
    """(d) ``GET /pacientes/{id}/prontuario`` sem token -> ``401`` ``application/problem+json``."""
    pid = str(uuid.uuid4())
    resposta = cliente.get(f"/pacientes/{pid}/prontuario")
    assert resposta.status_code == 401, resposta.text
    assert resposta.headers["content-type"].startswith("application/problem+json")
    corpo = resposta.json()
    assert corpo["status"] == 401
    assert corpo["correlation_id"]  # extensao obrigatoria de R8.2


@pytest.mark.requisito("R3.1")
def test_e_cria_paciente_201(
    cliente: httpx.Client,
    token_enfermeiro: str,
    contexto: dict[str, Any],
) -> None:
    """(e) ``POST /pacientes`` com JWT -> ``201`` e ``Location`` para o prontuario."""
    documento = f"e2e-{uuid.uuid4().hex[:10]}"
    resposta = cliente.post(
        "/pacientes",
        headers={"Authorization": f"Bearer {token_enfermeiro}", "X-Correlation-ID": _cid()},
        json={
            "nome": "Ana Souza",
            "documento": documento,
            "data_nascimento": "1980-05-12",
            "sexo": "F",
        },
    )
    assert resposta.status_code == 201, resposta.text
    corpo = resposta.json()
    assert corpo["nome"] == "Ana Souza"
    assert "Location" in resposta.headers
    contexto["paciente_id"] = corpo["paciente_id"]


def _leito_livre(cliente: httpx.Client, token: str) -> str:
    """Escolhe um leito de enfermaria ``livre`` da projecao (torna o teste re-executavel).

    Sem isto o teste fixaria um leito e uma segunda execucao colidiria com ``409 LEITO_OCUPADO``.
    """
    resposta = cliente.get("/leitos", headers={"Authorization": f"Bearer {token}"})
    assert resposta.status_code == 200, resposta.text
    for item in resposta.json().get("itens", []):
        codigo = item.get("leito_id") or item.get("codigo") or ""
        estado = item.get("estado") or item.get("status")
        if codigo.startswith("ENF-") and estado == "livre":
            return codigo
    raise AssertionError("nenhum leito de enfermaria livre na projecao para admitir")


@pytest.mark.requisito("R3.1")
def test_f_admite_paciente_201(
    cliente: httpx.Client,
    token_enfermeiro: str,
    contexto: dict[str, Any],
) -> None:
    """(f) ``POST /internacoes`` com JWT -> ``201`` (paciente admitido num leito livre)."""
    leito = _leito_livre(cliente, token_enfermeiro)
    resposta = cliente.post(
        "/internacoes",
        headers={"Authorization": f"Bearer {token_enfermeiro}", "X-Correlation-ID": _cid()},
        json={
            "paciente_id": contexto["paciente_id"],
            "leito_id": leito,
            "equipe_responsavel": "Equipe A",
            "motivo": "Observacao clinica",
        },
    )
    assert resposta.status_code == 201, resposta.text
    corpo = resposta.json()
    assert corpo["leito_id"] == leito
    assert corpo["setor"] == "ENFERMARIA"
    contexto["internacao_id"] = corpo["internacao_id"]
    contexto["leito_id"] = corpo["leito_id"]
    time.sleep(3)  # deixa a projecao do gateway consumir leito.ocupado antes de /sinais


@pytest.mark.requisito("R6.1")
def test_g_publica_sinais_estaveis_202(
    cliente: httpx.Client, contexto: dict[str, Any]
) -> None:
    """(g) ``POST /sinais`` com ``X-API-Key`` de dispositivo, paciente estavel -> ``202``."""
    resposta = cliente.post(
        "/sinais",
        headers={"X-API-Key": "dev-monitor-l12", "X-Correlation-ID": _cid()},
        json={
            "leito_id": contexto["leito_id"],
            "frequencia_respiratoria": 16,
            "saturacao_o2": 98,
            "oxigenio_suplementar": False,
            "temperatura": "36.5",
            "pressao_sistolica": 120,
            "frequencia_cardiaca": 75,
            "nivel_consciencia": "A",
            "coletado_em": _agora_iso(),
        },
    )
    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    assert corpo["status"] == "aceito"
    assert corpo["tipo_evento"] == "sinais.coletados"
    assert corpo["message_id"] and corpo["correlation_id"]
    time.sleep(4)  # deixa vitals persistir a leitura antes do prontuario


@pytest.mark.requisito("R7.3")
def test_h_prontuario_traz_ultimos_sinais(
    cliente: httpx.Client,
    token_enfermeiro: str,
    contexto: dict[str, Any],
) -> None:
    """(h) ``GET /pacientes/{id}/prontuario`` -> ``200`` com internacao ativa e sinais."""
    resposta = cliente.get(
        f"/pacientes/{contexto['paciente_id']}/prontuario",
        headers={"Authorization": f"Bearer {token_enfermeiro}", "X-Correlation-ID": _cid()},
    )
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["paciente"]["nome"] == "Ana Souza"
    assert corpo["internacao_atual"] is not None
    assert corpo["internacao_atual"]["ativa"] is True
    assert len(corpo["ultimos_sinais"]) >= 1


@pytest.mark.requisito("R6.4")
def test_i_deterioracao_gera_alerta_alta(
    cliente: httpx.Client,
    token_enfermeiro: str,
    contexto: dict[str, Any],
) -> None:
    """(i) ``POST /sinais`` NEWS2 alto -> ``202``; ``GET /alertas`` traz severidade alta."""
    resposta = cliente.post(
        "/sinais",
        headers={"X-API-Key": "dev-monitor-l12", "X-Correlation-ID": _cid()},
        json={
            "leito_id": contexto["leito_id"],
            "frequencia_respiratoria": 30,
            "saturacao_o2": 86,
            "oxigenio_suplementar": True,
            "temperatura": "39.5",
            "pressao_sistolica": 85,
            "frequencia_cardiaca": 135,
            "nivel_consciencia": "V",
            "coletado_em": _agora_iso(),
        },
    )
    assert resposta.status_code == 202, resposta.text

    # A cadeia sinais->triagem->alerta e assincrona; da algumas tentativas curtas.
    alerta_alta = None
    for _ in range(10):
        time.sleep(1)
        alertas = cliente.get(
            "/alertas",
            headers={"Authorization": f"Bearer {token_enfermeiro}"},
            params={"leito_id": contexto["leito_id"]},
        )
        assert alertas.status_code == 200, alertas.text
        itens = alertas.json().get("itens", [])
        alerta_alta = next((a for a in itens if a.get("severidade") == "alta"), None)
        if alerta_alta is not None:
            break
    assert alerta_alta is not None, "nenhum alerta de severidade 'alta' apareceu na projecao"


@pytest.mark.requisito("R11.7")
def test_j_leitos_mostra_leito_ocupado(
    cliente: httpx.Client,
    token_enfermeiro: str,
    contexto: dict[str, Any],
) -> None:
    """(j) ``GET /leitos`` -> ``200`` e o leito da internacao aparece ocupado na projecao."""
    resposta = cliente.get(
        "/leitos", headers={"Authorization": f"Bearer {token_enfermeiro}"}
    )
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    codigos = {
        item.get("leito_id") or item.get("codigo") for item in corpo.get("itens", [])
    }
    assert contexto["leito_id"] in codigos
