"""Autenticacao de borda: JWT invalido e expirado distinguiveis (R4.1, R4.2)."""

from __future__ import annotations

import pytest

from hospitalmq.auth import (
    MOTIVO_ASSINATURA_INVALIDA,
    MOTIVO_EXPIRADO,
    Autenticador,
)
from hospitalmq.clock import ManualClock
from hospitalmq.errors import AuthError


def test_jwt_valido_produz_identidade_de_usuario() -> None:
    """R4.1: token bem formado e assinado produz a identidade da pessoa."""
    auth = Autenticador(segredo="segredo-de-teste", expiracao_min=15, clock=ManualClock())
    token = auth.emitir_token("enf.ana", "enfermeiro")

    identidade = auth.validar_token(token)

    assert identidade.sub == "enf.ana"
    assert identidade.role == "enfermeiro"
    assert identidade.tipo == "usuario"


def test_jwt_expirado_levanta_auth_error_com_motivo_expirado() -> None:
    """R4.2: apos o exp, validar levanta AuthError com motivo 'expirado'."""
    clock = ManualClock()
    auth = Autenticador(segredo="segredo-de-teste", expiracao_min=15, clock=clock)
    token = auth.emitir_token("enf.ana", "enfermeiro")

    clock.avancar(15 * 60 + 1)  # ultrapassa a validade

    with pytest.raises(AuthError) as info:
        auth.validar_token(token)
    assert info.value.motivo == MOTIVO_EXPIRADO


def test_jwt_com_assinatura_invalida_levanta_auth_error_distinguivel() -> None:
    """R4.2: token assinado com outro segredo levanta AuthError 'assinatura_invalida'."""
    emissor = Autenticador(segredo="segredo-do-emissor", clock=ManualClock())
    token = emissor.emitir_token("enf.ana", "enfermeiro")

    verificador = Autenticador(segredo="segredo-diferente", clock=ManualClock())
    with pytest.raises(AuthError) as info:
        verificador.validar_token(token)

    assert info.value.motivo == MOTIVO_ASSINATURA_INVALIDA
    # Os dois motivos sao distinguiveis um do outro (R4.1 vs R4.2).
    assert MOTIVO_ASSINATURA_INVALIDA != MOTIVO_EXPIRADO
