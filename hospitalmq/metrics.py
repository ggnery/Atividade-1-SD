"""Contadores acumulados do middleware e sua exposicao por HTTP (secao 9.7, R5.5).

Os contadores sao incrementados **dentro** do ``Publisher``, do ``Consumer`` e do ``RpcClient``,
exatamente nos mesmos pontos em que os eventos de log da secao 9.4 sao emitidos. Isso garante a
invariante verificavel em teste::

    contador("mensagens_dlq") == numero de linhas com evento == "mensagem.dlq"

Metrica e log sao **duas projecoes do mesmo ponto de codigo**: divergencia entre elas e defeito,
nao interpretacao.

Cada processo publica **as suas proprias** metricas, em memoria, sem dependencia de
infraestrutura. A limitacao e reconhecida e deliberada (secao 9.7.3): reiniciar zera, e nao ha
agregacao entre replicas -- R5.5 pede um contador acumulado consultavel por HTTP, nao uma serie
temporal. Com Prometheus os contadores virariam ``Counter`` com *labels* e ``duracoes_ms`` viraria
``Histogram``; a porta fica aberta por :func:`render_prometheus`, que renderiza o **mesmo**
``snapshot()`` no formato de exposicao.

Sobre concorrencia: o incremento e ``d[t] = d.get(t, 0) + 1``, sem ``await`` entre a leitura e a
escrita. Sob o *event loop* unico do ``asyncio`` isso e atomico por construcao, o que dispensa
``Lock`` e mantem o custo em nanossegundos no caminho critico do sinal vital. Se o middleware
passasse a usar *threads*, seria necessario ``threading.Lock``.

Uso tipico::

    from hospitalmq.metrics import configure_metrics, get_metrics, render_prometheus

    configure_metrics(settings.service_name)          # startup do processo
    get_metrics().inc("mensagens_publicadas", tipo="sinais.registrados")
    corpo = render_prometheus(get_metrics().snapshot())
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Literal

from hospitalmq.clock import Clock, RealClock

if TYPE_CHECKING:  # pragma: no cover - usado apenas na anotacao de tipo
    from collections.abc import Mapping

__all__ = [
    "AJUDA",
    "BUCKETS_MS",
    "CHAVE_TOTAL",
    "CONTADORES",
    "NOME_HISTOGRAMA",
    "TIPO_PADRAO",
    "Contador",
    "Metrics",
    "configure_metrics",
    "get_metrics",
    "render_json",
    "render_prometheus",
]


Contador = Literal[
    "mensagens_publicadas",
    "mensagens_consumidas",
    "mensagens_duplicadas",
    "mensagens_retentadas",
    "mensagens_dlq",
    "rpc_chamadas",
    "rpc_timeouts",
    # Acrescimos citados fora da secao 9.7.1 e mantidos no mesmo vocabulario fechado:
    "derivados_nao_publicados",  # secoes 1.6 e 4.5.4
    "rpc_escritas_indeterminadas",  # secao 5.4.5
    "http_requisicoes",  # secao 8.6.2, middleware do gateway
    "internacao_resolvida",  # secao 8.6.2, resolucao de internacao de 8.2.5
]
"""Vocabulario fechado dos contadores. Fechado de proposito: cardinalidade aberta em metrica e o
caminho mais curto para um painel inutil e um custo de armazenamento imprevisivel."""

CONTADORES: Final[tuple[str, ...]] = (
    "mensagens_publicadas",
    "mensagens_consumidas",
    "mensagens_duplicadas",
    "mensagens_retentadas",
    "mensagens_dlq",
    "rpc_chamadas",
    "rpc_timeouts",
    "derivados_nao_publicados",
    "rpc_escritas_indeterminadas",
    "http_requisicoes",
    "internacao_resolvida",
)
"""Os mesmos valores de :data:`Contador`, em tempo de execucao e na ordem de exibicao."""

AJUDA: Final[dict[str, str]] = {
    "mensagens_publicadas": "Mensagens publicadas e confirmadas pelo broker (R1.5, R5.5).",
    "mensagens_consumidas": "Mensagens entregues ao handler e confirmadas por ACK manual (R2.1).",
    "mensagens_duplicadas": "Entregas repetidas descartadas pela idempotencia (R2.4).",
    "mensagens_retentadas": "Entregas com erro transitorio que foram reagendadas (R2.2).",
    "mensagens_dlq": "Mensagens enviadas a dead letter queue (R2.3).",
    "rpc_chamadas": "Chamadas RPC iniciadas pelo RpcClient (R3.1).",
    "rpc_timeouts": "Chamadas RPC sem resposta dentro do prazo (R3.3).",
    "derivados_nao_publicados": "Eventos derivados aceitos na transacao e recusados pelo broker.",
    "rpc_escritas_indeterminadas": "Timeouts em operacao de escrita, de resultado indeterminado.",
    "http_requisicoes": "Requisicoes HTTP atendidas pelo API Gateway.",
    "internacao_resolvida": "Resolucoes de internacao por origem (corpo, projecao, rpc, falha).",
}
"""Texto do cabecalho ``# HELP`` de cada contador no formato de exposicao Prometheus."""

BUCKETS_MS: Final[tuple[float, ...]] = (5.0, 25.0, 100.0, 500.0, 2000.0)
"""Faixas fixas do histograma simples de duracao, em milissegundos (secao 8.6.2).

Nao ha percentil interpolavel: guardam-se ``n``, soma, maximo e a contagem **cumulativa** por
faixa. A leitura da cauda -- quantas execucoes passaram de 2 s -- e imediata e nao exige amostra
individual, que custaria memoria proporcional ao trafego.
"""

CHAVE_TOTAL: Final[str] = "_total"
"""Chave agregada presente em todo contador, mesmo quando nenhum tipo foi observado."""

TIPO_PADRAO: Final[str] = "_sem_tipo"
"""Rotulo usado quando o ponto de incremento nao tem *routing key* associada."""

NOME_HISTOGRAMA: Final[str] = "handler_duracao_ms"
"""Nome do histograma de duracao no formato de exposicao Prometheus (secao 8.6.2)."""

_NOME_VALIDO: Final[re.Pattern[str]] = re.compile(r"[^a-zA-Z0-9_:]")


def _rotulo_bucket(limite: float) -> str:
    """Formata o limite de uma faixa do histograma como chave de dicionario.

    Args:
        limite: Limite superior da faixa, em milissegundos.

    Returns:
        ``"5"``, ``"25"``, ``"100"``... -- inteiro quando o limite for inteiro.
    """
    return str(int(limite)) if float(limite).is_integer() else str(limite)


@dataclass(slots=True)
class _Duracao:
    """Acumulador de duracoes de uma operacao (secao 9.7.1).

    Attributes:
        n: Numero de observacoes.
        soma: Soma das duracoes, em milissegundos.
        maximo: Maior duracao observada, em milissegundos.
        buckets: Contagem **cumulativa** por faixa de :data:`BUCKETS_MS`.
    """

    n: int = 0
    soma: float = 0.0
    maximo: float = 0.0
    buckets: dict[str, int] = field(
        default_factory=lambda: {_rotulo_bucket(limite): 0 for limite in BUCKETS_MS}
    )

    def observar(self, ms: float) -> None:
        """Incorpora uma nova duracao ao acumulador.

        Args:
            ms: Duracao medida em milissegundos, obtida de relogio monotonico.
        """
        self.n += 1
        self.soma += ms
        if ms > self.maximo:
            self.maximo = ms
        for limite in BUCKETS_MS:
            if ms <= limite:
                self.buckets[_rotulo_bucket(limite)] += 1

    def para_dict(self) -> dict[str, object]:
        """Projeta o acumulador na forma exposta por ``/metrics``.

        Returns:
            Dicionario com ``n``, ``soma``, ``media``, ``maximo`` e ``buckets`` cumulativos,
            incluindo a faixa ``+Inf`` exigida pela semantica de histograma.
        """
        return {
            "n": self.n,
            "soma": round(self.soma, 2),
            "media": round(self.soma / self.n, 2) if self.n else 0.0,
            "maximo": round(self.maximo, 2),
            "buckets": {**self.buckets, "+Inf": self.n},
        }


class Metrics:
    """Contadores acumulados de um processo, segmentados por tipo (R5.5).

    Todo contador e segmentado por **servico** -- implicito, ja que cada processo tem os seus -- e
    por **tipo**, que e a *routing key* do evento. Isso da a granularidade pedida sem cardinalidade
    aberta: os tipos sao os 11 do contrato da secao 5.

    Attributes:
        servico: Nome do processo dono destes contadores.
    """

    __slots__ = ("_clock", "_contadores", "_duracoes", "_inicio", "_medidores", "servico")

    def __init__(self, servico: str, *, clock: Clock | None = None) -> None:
        """Cria o registro de metricas de um processo.

        Args:
            servico: Nome do processo, normalmente ``settings.service_name``.
            clock: Relogio injetavel (secao 10.3.1). ``coletado_em`` usa o relogio de parede e
                ``uptime_s`` o monotonico; o padrao e :class:`~hospitalmq.clock.RealClock`.
        """
        self.servico: str = servico
        self._clock: Clock = clock if clock is not None else RealClock()
        self._contadores: dict[str, dict[str, int]] = {nome: {} for nome in CONTADORES}
        self._duracoes: dict[str, _Duracao] = {}
        self._medidores: dict[str, float] = {}
        self._inicio: float = self._clock.monotonic()

    def inc(self, contador: Contador, *, tipo: str = TIPO_PADRAO, valor: int = 1) -> None:
        """Incrementa um contador, opcionalmente segmentado por tipo de mensagem (R5.5).

        Args:
            contador: Nome do contador, do vocabulario fechado de :data:`Contador`.
            tipo: *Routing key* do evento (``"sinais.registrados"``), nome da fila ou da operacao
                RPC. O padrao ``"_sem_tipo"`` cobre os pontos sem dimensao natural.
            valor: Quanto somar. Sempre ``1`` no caminho normal; o parametro existe para o
                reprocessamento em lote da DLQ.
        """
        segmentos = self._contadores.setdefault(contador, {})
        segmentos[tipo] = segmentos.get(tipo, 0) + valor

    def incr(self, nome: str, **rotulos: str) -> None:
        """Apelido tolerante de :meth:`inc`, aceito nos esbocos das secoes 4.5.4 e 5.4.5.

        Aceita o nome com o sufixo ``_total`` do formato Prometheus e deduz o segmento a partir do
        primeiro rotulo conhecido (``tipo``, ``operacao``, ``fila``, ``resultado``, ``motivo`` ou
        ``origem``). Existe para que um ponto de instrumentacao escrito na forma de outra secao do
        design nao silencie a metrica.

        Args:
            nome: Nome do contador, com ou sem o sufixo ``_total``.
            **rotulos: Rotulos do ponto de incremento; o primeiro reconhecido vira o ``tipo``.
        """
        base = nome[: -len("_total")] if nome.endswith("_total") else nome
        for chave in ("tipo", "operacao", "fila", "resultado", "motivo", "origem"):
            if chave in rotulos:
                self.inc(base, tipo=rotulos[chave])  # type: ignore[arg-type]
                return
        self.inc(base)  # type: ignore[arg-type]

    def observe_duracao(self, operacao: str, ms: float) -> None:
        """Registra a duracao de uma operacao no histograma simples (R5.4).

        Args:
            operacao: Rotulo da medicao, por convencao ``"handler.<tipo>"`` no ``Consumer`` e
                ``"rpc.<operacao>"`` no ``RpcClient``.
            ms: Duracao em milissegundos, medida com relogio **monotonico** -- medir com relogio
                de parede e o erro classico que produz duracao negativa apos ajuste de NTP.
        """
        self._duracoes.setdefault(operacao, _Duracao()).observar(ms)

    def definir_medidor(self, nome: str, valor: float) -> None:
        """Define o valor corrente de um medidor (*gauge*).

        Diferente do contador, um medidor sobe e desce. Os dois previstos no design sao
        ``rpc_pendentes`` (secao 5.4.5) -- cujo crescimento monotonico e a assinatura inequivoca de
        vazamento de *futures* -- e ``sse_assinantes`` (secao 8.6.2).

        Args:
            nome: Nome do medidor.
            valor: Valor corrente.
        """
        self._medidores[nome] = float(valor)

    def total(self, contador: Contador) -> int:
        """Soma todas as segmentacoes de um contador.

        Args:
            contador: Nome do contador.

        Returns:
            O total acumulado, ``0`` se nada foi observado.
        """
        return sum(self._contadores.get(contador, {}).values())

    def valor(self, contador: Contador, *, tipo: str = TIPO_PADRAO) -> int:
        """Le uma segmentacao especifica de um contador.

        Args:
            contador: Nome do contador.
            tipo: Segmento consultado.

        Returns:
            O valor acumulado do segmento, ``0`` se ele nunca foi incrementado.
        """
        return self._contadores.get(contador, {}).get(tipo, 0)

    def medidor(self, nome: str) -> float:
        """Le o valor corrente de um medidor.

        Args:
            nome: Nome do medidor.

        Returns:
            O ultimo valor definido, ``0.0`` se o medidor nunca foi definido.
        """
        return self._medidores.get(nome, 0.0)

    def snapshot(self) -> dict[str, object]:
        """Fotografa o estado dos contadores para exposicao em ``/metrics`` (R5.5).

        Todo contador do vocabulario aparece, mesmo zerado: forma estavel de resposta e o que
        permite ao ``Painel_de_Leitos`` ler o JSON sem tratar chave ausente.

        Returns:
            Dicionario com ``servico``, ``coletado_em``, ``uptime_s``, ``contadores`` (por tipo,
            mais a chave agregada ``_total``), ``duracoes_ms`` e ``medidores``.
        """
        contadores: dict[str, dict[str, int]] = {}
        for nome in CONTADORES:
            segmentos = self._contadores.get(nome, {})
            contadores[nome] = {**segmentos, CHAVE_TOTAL: sum(segmentos.values())}
        for nome, segmentos in self._contadores.items():
            if nome not in contadores:
                contadores[nome] = {**segmentos, CHAVE_TOTAL: sum(segmentos.values())}
        return {
            "servico": self.servico,
            "coletado_em": self._clock.now().isoformat().replace("+00:00", "Z"),
            "uptime_s": round(self._clock.monotonic() - self._inicio, 1),
            "contadores": contadores,
            "duracoes_ms": {
                operacao: acumulado.para_dict() for operacao, acumulado in self._duracoes.items()
            },
            "medidores": dict(self._medidores),
        }

    def reset(self) -> None:
        """Zera contadores, duracoes, medidores e o instante de inicio.

        Exclusivo para a suite de testes: em producao um contador que zera sozinho tornaria
        qualquer serie temporal derivada mentirosa.
        """
        self._contadores = {nome: {} for nome in CONTADORES}
        self._duracoes = {}
        self._medidores = {}
        self._inicio = self._clock.monotonic()


_metrics: Metrics | None = None


def configure_metrics(servico: str, *, clock: Clock | None = None) -> Metrics:
    """Cria e instala o registro de metricas do processo (secao 9.7.1).

    Chamada uma vez no *startup*, ao lado de
    :func:`~hospitalmq.logging.configure_logging`. Chamar de novo substitui o registro e portanto
    **zera** os contadores -- e o que a suite de testes usa para isolar casos.

    Args:
        servico: Nome do processo, normalmente ``settings.service_name``.
        clock: Relogio injetavel; o padrao e :class:`~hospitalmq.clock.RealClock`.

    Returns:
        O registro recem-instalado, o mesmo que :func:`get_metrics` passara a devolver.
    """
    global _metrics

    _metrics = Metrics(servico, clock=clock)
    return _metrics


def get_metrics() -> Metrics:
    """Devolve o registro de metricas do processo, criando-o na primeira chamada.

    O *singleton* e por processo, nunca por servico logico: agregar replicas exigiria um coletor
    externo, limitacao assumida em 9.7.3.

    Returns:
        O :class:`Metrics` instalado por :func:`configure_metrics`; se nenhum foi instalado, um
        criado sob demanda com ``settings.service_name``, para que um ponto de instrumentacao
        jamais derrube o processo por ordem de inicializacao.
    """
    global _metrics

    if _metrics is None:
        from hospitalmq.config import settings

        _metrics = Metrics(settings.service_name)
    return _metrics


# --------------------------------------------------------------------------- #
# Renderizadores (secao 9.7.2)                                                 #
# --------------------------------------------------------------------------- #


def render_json(instantaneo: Mapping[str, object]) -> str:
    """Serializa o ``snapshot()`` no formato padrao da rota ``/metrics``.

    JSON e o padrao -- e nao o formato de exposicao do Prometheus -- porque e o mesmo formato de
    todo o resto da API (RFC 7807, OpenAPI) e e consumivel direto pelo ``Painel_de_Leitos`` em JS
    puro, sem *build* e sem *parser* de texto (R11.6).

    Args:
        instantaneo: Resultado de :meth:`Metrics.snapshot`.

    Returns:
        Uma linha JSON compacta, com acentuacao preservada.
    """
    return json.dumps(instantaneo, ensure_ascii=False, separators=(",", ":"))


def _escapar_rotulo(valor: str) -> str:
    """Escapa um valor de *label* conforme o formato de exposicao do Prometheus.

    Args:
        valor: Valor bruto do rotulo.

    Returns:
        O valor com a barra invertida, as aspas e a quebra de linha escapadas.
    """
    return valor.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _nome_metrica(nome: str) -> str:
    """Normaliza um nome para o conjunto de caracteres aceito pelo Prometheus.

    Args:
        nome: Nome bruto do contador ou medidor.

    Returns:
        O nome com todo caractere fora de ``[a-zA-Z0-9_:]`` substituido por ``_``.
    """
    return _NOME_VALIDO.sub("_", nome)


def _rotulos(pares: Mapping[str, str]) -> str:
    """Monta o bloco de *labels* de uma serie.

    Args:
        pares: Rotulos da serie, na ordem desejada.

    Returns:
        A string ``{chave="valor",...}``, ou vazia se nao houver rotulo.
    """
    if not pares:
        return ""
    interno = ",".join(f'{chave}="{_escapar_rotulo(valor)}"' for chave, valor in pares.items())
    return "{" + interno + "}"


def _numero(valor: object) -> float:
    """Converte um valor do ``snapshot()`` em numero, com zero como padrao seguro.

    Args:
        valor: Valor lido do dicionario do instantaneo.

    Returns:
        O valor como ``float``; ``0.0`` quando nao for numerico.
    """
    return float(valor) if isinstance(valor, int | float) else 0.0


def _formatar(valor: float) -> str:
    """Formata um numero para o corpo da exposicao Prometheus.

    Args:
        valor: Valor da amostra.

    Returns:
        Representacao inteira quando o valor for inteiro, decimal caso contrario.
    """
    return str(int(valor)) if float(valor).is_integer() else repr(valor)


def render_prometheus(instantaneo: Mapping[str, object]) -> str:
    """Renderiza o ``snapshot()`` no formato de exposicao do Prometheus (``?format=prometheus``).

    O sufixo ``_total`` dos contadores e aplicado **aqui**, e nao nas chaves internas: ``_total`` e
    convencao do formato de exposicao para contador monotonico, e a chave que o ``Painel_de_Leitos``
    le nao a carrega (secao 9.7.1). A chave agregada ``_total`` do JSON nao vira serie, para nao
    contar duas vezes o que o proprio Prometheus soma a partir das segmentacoes.

    Args:
        instantaneo: Resultado de :meth:`Metrics.snapshot`.

    Returns:
        Texto ``text/plain; version=0.0.4``, terminado por quebra de linha.
    """
    servico = str(instantaneo.get("servico", "desconhecido"))
    linhas: list[str] = []

    contadores = instantaneo.get("contadores")
    if isinstance(contadores, dict):
        for nome, segmentos in contadores.items():
            metrica = f"{_nome_metrica(str(nome))}_total"
            linhas.append(f"# HELP {metrica} {AJUDA.get(str(nome), 'Contador do HospitalMQ.')}")
            linhas.append(f"# TYPE {metrica} counter")
            if not isinstance(segmentos, dict):
                continue
            tipos = {t: v for t, v in segmentos.items() if t != CHAVE_TOTAL}
            if not tipos:
                linhas.append(f"{metrica}{_rotulos({'servico': servico})} 0")
                continue
            for tipo, valor in tipos.items():
                rotulos = _rotulos({"servico": servico, "tipo": str(tipo)})
                linhas.append(f"{metrica}{rotulos} {_formatar(_numero(valor))}")

    duracoes = instantaneo.get("duracoes_ms")
    if isinstance(duracoes, dict) and duracoes:
        linhas.append(f"# HELP {NOME_HISTOGRAMA} Duracao das operacoes, em milissegundos (R5.4).")
        linhas.append(f"# TYPE {NOME_HISTOGRAMA} histogram")
        for operacao, resumo in duracoes.items():
            if not isinstance(resumo, dict):
                continue
            base = {"servico": servico, "operacao": str(operacao)}
            buckets = resumo.get("buckets")
            if isinstance(buckets, dict):
                for limite, contagem in buckets.items():
                    rotulos = _rotulos({**base, "le": str(limite)})
                    linhas.append(
                        f"{NOME_HISTOGRAMA}_bucket{rotulos} {_formatar(_numero(contagem))}"
                    )
            linhas.append(
                f"{NOME_HISTOGRAMA}_sum{_rotulos(base)} {_formatar(_numero(resumo.get('soma')))}"
            )
            linhas.append(
                f"{NOME_HISTOGRAMA}_count{_rotulos(base)} {_formatar(_numero(resumo.get('n')))}"
            )

    medidores = instantaneo.get("medidores")
    if isinstance(medidores, dict):
        for nome, valor in medidores.items():
            metrica = _nome_metrica(str(nome))
            linhas.append(f"# HELP {metrica} Medidor do HospitalMQ.")
            linhas.append(f"# TYPE {metrica} gauge")
            linhas.append(f"{metrica}{_rotulos({'servico': servico})} {_formatar(_numero(valor))}")

    uptime = instantaneo.get("uptime_s")
    if uptime is not None:
        linhas.append("# HELP processo_uptime_s Tempo desde a criacao do registro de metricas.")
        linhas.append("# TYPE processo_uptime_s gauge")
        linhas.append(f"processo_uptime_s{_rotulos({'servico': servico})} {_numero(uptime)}")

    return "\n".join(linhas) + "\n"
