"""Roteamento por routing key com os curingas '#' e '*' do exchange topic (R1.1, R7.1)."""

from __future__ import annotations

from hospitalmq.transport.memory import MemoryTransport, casa_topico


def test_casa_topico_curinga_varios() -> None:
    """'#' casa zero ou mais segmentos -- e o binding de auditoria de q.audit.todos (R7.1)."""
    assert casa_topico("#", "sinais.coletados")
    assert casa_topico("#", "qualquer.evento.novo")
    assert casa_topico("#", "unico")


def test_casa_topico_curinga_um() -> None:
    """'*' casa exatamente um segmento, nem mais nem menos."""
    assert casa_topico("paciente.*", "paciente.admitido")
    assert casa_topico("alerta.*", "alerta.gerado")
    assert not casa_topico("paciente.*", "paciente.leito.trocado")
    assert not casa_topico("paciente.*", "paciente")


def test_casa_topico_igualdade_exata() -> None:
    """Sem curinga, a binding key exige igualdade exata da routing key."""
    assert casa_topico("sinais.coletados", "sinais.coletados")
    assert not casa_topico("sinais.coletados", "sinais.registrados")


async def test_roteamento_no_memory_transport(mem: MemoryTransport) -> None:
    """A publicacao chega a toda fila cujo binding casa, e a nenhuma outra."""
    await mem.publish(
        destination="hospital.events",
        routing_key="paciente.admitido",
        body=b"{}",
        headers={},
        message_id="m-1",
        correlation_id="c-1",
    )

    # 'paciente.*' (projecao) e '#' (auditoria) casam; 'alerta.gerado' nao.
    assert mem.profundidade("q.gateway.projecao") == 1
    assert mem.profundidade("q.audit.todos") == 1
    assert mem.profundidade("q.alert.alerta-gerado") == 0
