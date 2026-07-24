"""Ponto de entrada do ``vitals-service`` (design 9.7.2, 8.6.1; R5.5, R6.1, R8.6).

Compoe o processo com poucas linhas, delegando a semantica de mensageria ao ``hospitalmq`` e a
fabrica de aplicacao a ``services.comum``:

* um :class:`~hospitalmq.consumer.Consumer` na fila ``q.vitals.sinais-coletados``, com o handler de
  ingestao de ``handler.py`` (R6.1/R6.6);
* um :class:`~hospitalmq.rpc.RpcServer` na fila ``q.rpc.vitals`` que atende a operacao de leitura
  ``sinais.ultimos`` (design 5.8.1), usada pelo prontuario e pelo *snapshot* de boot do painel;
* o FastAPI minimo de ``services.comum.app.criar_app`` com ``/health``, ``/health/ready`` e
  ``/metrics`` (R8.6, R5.5).

Executar com ``uvicorn --app-dir services/vitals-service main:app``: o *lifespan* da fabrica conecta
o transporte, declara a topologia, inicia o consumo e o RPC e desfaz tudo no encerramento.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from handler import criar_handler_sinais_coletados
from modelos import SinaisVitais
from sqlalchemy import select

from hospitalmq import Publisher, RpcServer
from hospitalmq.rpc import ContextoRpc
from services.comum.app import criar_app
from services.comum.bootstrap import montar_consumidor, montar_transporte
from services.comum.db import obter_engine, obter_sessionmaker
from services.comum.news2 import SinaisVitais as SinaisNEWS2
from services.comum.news2 import calcular_news2

SERVICO: Final[str] = "vitals-service"
FILA_EVENTOS: Final[str] = "q.vitals.sinais-coletados"
FILA_RPC: Final[str] = "q.rpc.vitals"
SCHEMA: Final[str] = "vitais"

PAPEIS_SINAIS_ULTIMOS: Final[frozenset[str]] = frozenset({"enfermeiro", "medico", "servico"})
"""Papeis autorizados a ler ``sinais.ultimos`` (design 5.8.1): enfermeiro, medico e o ``servico``
do *snapshot* de boot."""

_LIMITE_PADRAO: Final[int] = 20
_LIMITE_MAX: Final[int] = 200
_DESDE_HORAS_PADRAO: Final[int] = 24
_DESDE_HORAS_MAX: Final[int] = 168
_POR_INTERNACAO_PADRAO: Final[int] = 1
_POR_INTERNACAO_MAX: Final[int] = 50


# --------------------------------------------------------------------------- #
# Composicao do processo                                                        #
# --------------------------------------------------------------------------- #

Sessao = obter_sessionmaker()
transporte = montar_transporte(SERVICO)

# Publisher dedicado apenas aos eventos sinais.rejeitados fora da transacao principal (7.7.3).
_publisher_rejeicoes = Publisher(transport=transporte, producer=SERVICO)

consumidor = montar_consumidor(
    SERVICO,
    [FILA_EVENTOS],
    session_factory=Sessao,
    transporte=transporte,
    schema=SCHEMA,
)
consumidor.on("sinais.coletados", queue=FILA_EVENTOS, modelo=None)(
    criar_handler_sinais_coletados(_publisher_rejeicoes)
)

servidor_rpc = RpcServer(transporte, fila=FILA_RPC, producer=SERVICO)


# --------------------------------------------------------------------------- #
# Operacao RPC de leitura: sinais.ultimos (design 5.8.1)                        #
# --------------------------------------------------------------------------- #


def _inteiro(valor: object, *, padrao: int, minimo: int, maximo: int) -> int:
    """Le um inteiro do payload com padrao e limites, tolerando ausencia e tipo errado."""
    try:
        numero = int(valor) if valor is not None else padrao
    except (TypeError, ValueError):
        numero = padrao
    return max(minimo, min(maximo, numero))


def _serializar(linha: SinaisVitais) -> dict[str, Any]:
    """Projeta uma leitura persistida no item de serie de ``sinais.ultimos``.

    ``componentes`` traz os sete parametros crus da leitura; ``score_news2`` e o resumo derivado por
    :func:`~services.comum.news2.calcular_news2` -- calculo NEWS2 no caminho de **leitura**, sobre
    dado ja validado e persistido, para compor o resumo de severidade que o painel e o prontuario
    exibem (design 5.8.1). Nao e o caminho de ingestao, que R6.6 mantem sem NEWS2.
    """
    score = calcular_news2(
        SinaisNEWS2(
            frequencia_respiratoria=linha.frequencia_respiratoria,
            saturacao_o2=linha.saturacao_o2,
            oxigenio_suplementar=linha.oxigenio_suplementar,
            temperatura=linha.temperatura,
            pressao_sistolica=linha.pressao_sistolica,
            frequencia_cardiaca=linha.frequencia_cardiaca,
            nivel_consciencia=linha.nivel_consciencia,  # type: ignore[arg-type]
        )
    )
    return {
        "sinais_vitais_id": str(linha.id),
        "coletado_em": linha.coletado_em.isoformat(),
        "componentes": {
            "frequencia_respiratoria": linha.frequencia_respiratoria,
            "saturacao_o2": linha.saturacao_o2,
            "oxigenio_suplementar": linha.oxigenio_suplementar,
            "temperatura": str(linha.temperatura),
            "pressao_sistolica": linha.pressao_sistolica,
            "frequencia_cardiaca": linha.frequencia_cardiaca,
            "nivel_consciencia": linha.nivel_consciencia,
        },
        "score_news2": {
            "total": score.total,
            "componente_critico": score.componente_critico,
            "severidade": score.severidade.value,
        },
    }


async def _consultar_series(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Resolve as duas variantes de ``sinais.ultimos`` (design 5.8.1) numa serie por internacao.

    * ``{"internacao_id", "limite"}`` -- as ``limite`` leituras mais recentes de uma internacao.
    * ``{"desde_horas", "por_internacao"}`` -- *snapshot* de boot: as ``por_internacao`` leituras
      mais recentes de **cada** internacao com dado na janela, para repovoar o painel.

    Ambas usam o indice ``ix_sv_internacao_coleta (internacao_id, coletado_em DESC)``; o servico le
    apenas o proprio *schema* ``vitais`` (design 5.8.1, propriedade 3).
    """
    internacao_bruta = payload.get("internacao_id")
    async with Sessao() as session:
        if internacao_bruta:
            internacao_id = uuid.UUID(str(internacao_bruta))
            limite = _inteiro(
                payload.get("limite"), padrao=_LIMITE_PADRAO, minimo=1, maximo=_LIMITE_MAX
            )
            consulta = (
                select(SinaisVitais)
                .where(SinaisVitais.internacao_id == internacao_id)
                .order_by(SinaisVitais.coletado_em.desc())
                .limit(limite)
            )
            linhas = (await session.execute(consulta)).scalars().all()
            return {str(internacao_id): [_serializar(linha) for linha in linhas]}

        desde_horas = _inteiro(
            payload.get("desde_horas"),
            padrao=_DESDE_HORAS_PADRAO,
            minimo=1,
            maximo=_DESDE_HORAS_MAX,
        )
        por_internacao = _inteiro(
            payload.get("por_internacao"),
            padrao=_POR_INTERNACAO_PADRAO,
            minimo=1,
            maximo=_POR_INTERNACAO_MAX,
        )
        limiar = datetime.now(UTC) - timedelta(hours=desde_horas)
        consulta = (
            select(SinaisVitais)
            .where(SinaisVitais.coletado_em >= limiar)
            .order_by(SinaisVitais.internacao_id, SinaisVitais.coletado_em.desc())
        )
        series: dict[str, list[dict[str, Any]]] = {}
        for linha in (await session.execute(consulta)).scalars():
            chave = str(linha.internacao_id)
            serie = series.setdefault(chave, [])
            if len(serie) < por_internacao:
                serie.append(_serializar(linha))
        return series


@servidor_rpc.operacao("sinais.ultimos", roles=PAPEIS_SINAIS_ULTIMOS, escrita=False)
async def sinais_ultimos(payload: dict[str, Any], ctx: ContextoRpc) -> dict[str, Any]:
    """Operacao RPC de leitura ``sinais.ultimos`` (design 5.8.1): serie temporal por internacao.

    Args:
        payload: ``{"internacao_id", "limite"}`` ou ``{"desde_horas", "por_internacao"}``.
        ctx: Contexto RPC do middleware; a identidade ja foi verificada contra ``roles``.

    Returns:
        ``{"series": {"<id>": [{sinais_vitais_id, coletado_em, componentes, score_news2}]}}``.
    """
    series = await _consultar_series(payload)
    return {"series": series}


# --------------------------------------------------------------------------- #
# Aplicacao FastAPI (/health, /health/ready, /metrics) + lifespan               #
# --------------------------------------------------------------------------- #

app = criar_app(
    servico=SERVICO,
    transporte=transporte,
    consumidor=consumidor,
    servidores_rpc=[servidor_rpc],
    engine=obter_engine(),
)
