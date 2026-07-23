"""RPC sobre fila (Request-Reply): casamento por correlacao, timeout e erro remoto (R3.2--R3.5)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from hospitalmq.errors import PermanentError, RpcRemoteError, RpcTimeoutError
from hospitalmq.rpc import ContextoRpc, RpcClient, RpcServer
from hospitalmq.transport.memory import MemoryTransport


async def test_resposta_casada_por_correlation_id(mem: MemoryTransport) -> None:
    """R3.2: o RpcClient recebe a resposta casada com a chamada."""
    servidor = RpcServer(mem, fila="q.rpc.admission", producer="admission-service")

    @servidor.operacao("prontuario.consultar")
    async def _consultar(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
        return {"eco": payload}

    await servidor.start()
    cliente = RpcClient(mem, producer="api-gateway")
    await cliente.start()

    dados = await cliente.call("prontuario.consultar", {"paciente_id": "P-1"})

    assert dados == {"eco": {"paciente_id": "P-1"}}
    await cliente.close()
    await servidor.stop()


async def test_timeout_remove_future_pendente(mem: MemoryTransport) -> None:
    """R3.3: sem resposta no prazo levanta RpcTimeoutError e limpa o dicionario de pendentes."""
    cliente = RpcClient(mem, producer="api-gateway")
    await cliente.start()  # nenhum servidor sobe: a chamada nunca sera respondida

    with pytest.raises(RpcTimeoutError):
        await cliente.call("prontuario.consultar", {"paciente_id": "P-1"}, timeout=0.05)

    assert cliente._pendentes == {}
    await cliente.close()


async def test_duas_chamadas_concorrentes_recebem_cada_uma_a_sua_resposta(
    mem: MemoryTransport,
) -> None:
    """R3.5: com o mesmo correlation_id de rastreio, cada chamada recebe a SUA resposta."""
    servidor = RpcServer(mem, fila="q.rpc.admission", producer="admission-service")

    @servidor.operacao("prontuario.consultar")
    async def _consultar(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
        return {"eco": payload["n"]}

    await servidor.start()
    cliente = RpcClient(mem, producer="api-gateway")
    await cliente.start()

    r1, r2 = await asyncio.gather(
        cliente.call("prontuario.consultar", {"n": 1}, correlation_id="rastreio-compartilhado"),
        cliente.call("prontuario.consultar", {"n": 2}, correlation_id="rastreio-compartilhado"),
    )

    assert r1 == {"eco": 1}
    assert r2 == {"eco": 2}
    await cliente.close()
    await servidor.stop()


async def test_erro_no_servidor_volta_como_rpc_remote_error(mem: MemoryTransport) -> None:
    """R3.4: erro do handler volta como RpcRemoteError tipado, nao como timeout."""
    servidor = RpcServer(mem, fila="q.rpc.admission", producer="admission-service")

    @servidor.operacao("prontuario.consultar")
    async def _consultar(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
        raise PermanentError("paciente inexistente", codigo="PACIENTE_NAO_ENCONTRADO")

    await servidor.start()
    cliente = RpcClient(mem, producer="api-gateway")
    await cliente.start()

    with pytest.raises(RpcRemoteError) as info:
        await cliente.call("prontuario.consultar", {"paciente_id": "P-404"})

    assert info.value.codigo == "PACIENTE_NAO_ENCONTRADO"
    assert not isinstance(info.value, RpcTimeoutError)
    await cliente.close()
    await servidor.stop()
