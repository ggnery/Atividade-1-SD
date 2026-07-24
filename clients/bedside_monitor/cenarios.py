"""Catalogo dos cenarios de demonstracao do Cliente_Leito (design 12.5.5, R10.6).

O catalogo e a **fonte de verdade** do ``--cenario``, do ``--help`` e do README (R10.2): tudo o que
distingue um cenario do outro mora aqui como **dado** (uma :class:`DefinicaoCenario`), nunca como
ramo de codigo espalhado pelo simulador. Sao os quatro cenarios que R10.6 enumera --
``estavel``, ``deterioracao``, ``falha-consumidor`` e ``fora-de-faixa`` -- mais dois extras
fora do enunciado: ``tempestade``, que exercita *competing consumers* e ``prefetch`` (R2.5, R2.6), e
``plantao``, o modo continuo em que os sinais mudam a cada leitura e a gravidade sobe devagar ate
cruzar as bandas do NEWS2 sozinha (ver :attr:`~.perfis.Perfil.PLANTAO`).

Os cenarios 3 e 4 produzem os **dois caminhos distintos** ate a DLQ, e e por isso que sao dois
(design 12.5.5): ``falha-consumidor`` chega por **esgotamento de retentativa** de um
``TransientError`` (o canal do leito ``UTI-03`` esta sabotado por ``ALERT_FAILURE_LEITOS``, e o
alerta espera 1s/2s/4s antes da DLQ, ~7 s); ``fora-de-faixa`` chega **direto**, na primeira
tentativa, porque um valor fisiologicamente impossivel e ``PermanentError`` e retentar nao mudaria
o resultado.

Ligacao com a infraestrutura. O cenario ``falha-consumidor`` so falha se o seu leito padrao
constar de ``ALERT_FAILURE_LEITOS`` (padrao ``UTI-03`` no ``.env.example``). Por isso o leito padrao
e ``UTI-03`` e a API Key padrao e ``dev-monitor-uti03`` -- a terceira entrada de ``API_KEYS``
(design 12.4.2). O cliente **nao** inspeciona a fila nem o ``/metrics``: ele apenas publica pela
API que tem direito de chamar (``POST /sinais`` com ``X-API-Key``) e narra o que o avaliador deve
observar (design 12.5.5).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from .perfis import Perfil

__all__ = [
    "ALIASES",
    "CENARIOS",
    "Cenario",
    "DefinicaoCenario",
    "definicao_de",
    "resolver_cenario",
]


class Cenario(StrEnum):
    """Nome de cada cenario, tal como digitado em ``--cenario`` (design 12.5.5)."""

    ESTAVEL = "estavel"
    DETERIORACAO = "deterioracao"
    PLANTAO = "plantao"
    FALHA_CONSUMIDOR = "falha-consumidor"
    FORA_DE_FAIXA = "fora-de-faixa"
    TEMPESTADE = "tempestade"


@dataclass(frozen=True, slots=True)
class DefinicaoCenario:
    """Tudo o que distingue um cenario do outro, em dado e nao em ramo de codigo (design 12.5.5).

    Attributes:
        perfil: Trajetoria fisiologica seguida pelo gerador (design 10.7.3).
        leito_padrao: Leito usado quando ``--leito`` nao e informado. No ``falha-consumidor`` deve
            constar de ``ALERT_FAILURE_LEITOS`` para que a falha aconteca.
        api_key_padrao: API Key de dispositivo usada quando ``--api-key`` e o ambiente a omitem.
        intervalo_s: Intervalo padrao entre leituras, em segundos, fixado pelo proprio cenario.
        passos: Numero de leituras que o cenario publica quando ``--duracao`` nao e informado, ou
            ``None`` para os cenarios que rodam **indefinidamente** ate ``Ctrl+C`` (``plantao``).
        criterio_r10_6: Frase literal do enunciado (R10.6) coberta por este cenario, ou ``None``
            para os cenarios extra fora do enunciado.
        narrativa: Texto impresso em ``stdout`` antes do primeiro ``POST``, explicando ao avaliador
            o que observar.
        escalada_s: Horizonte padrao da escalada do modo continuo, em segundos -- quanto tempo o
            paciente leva de compensado (``g = 0``) a critico (``g = 1``). ``None`` nos cenarios que
            nao usam :attr:`~.perfis.Perfil.PLANTAO`; sobreposto por ``--escalada``.
    """

    perfil: Perfil
    leito_padrao: str
    api_key_padrao: str
    intervalo_s: float
    passos: int | None
    criterio_r10_6: str | None
    narrativa: str
    escalada_s: float | None = None


CENARIOS: Final[dict[Cenario, DefinicaoCenario]] = {
    Cenario.ESTAVEL: DefinicaoCenario(
        perfil=Perfil.ESTAVEL,
        leito_padrao="UTI-01",
        api_key_padrao="dev-monitor-l07",
        intervalo_s=5.0,
        passos=12,
        criterio_r10_6="paciente estavel",
        narrativa=(
            "Leito UTI-01: paciente estavel do inicio ao fim. NEWS2 entre 0 e 1, card verde no "
            "painel, nenhum alerta gerado. E o contraste visual que faz a deterioracao dos outros "
            "leitos saltar aos olhos."
        ),
    ),
    Cenario.DETERIORACAO: DefinicaoCenario(
        perfil=Perfil.DETERIORACAO,
        leito_padrao="UTI-02",
        api_key_padrao="dev-monitor-l07",
        intervalo_s=5.0,
        passos=10,
        criterio_r10_6="paciente em deterioracao",
        narrativa=(
            "Leito UTI-02: SpO2 caindo e FR/FC subindo a cada leitura. No 7o passo o NEWS2 "
            "agregado "
            "passa de 5, o card fica vermelho e o alerta entra no topo da lista do painel. Com a "
            "mesma semente, o cruzamento acontece sempre no mesmo instante."
        ),
    ),
    Cenario.PLANTAO: DefinicaoCenario(
        perfil=Perfil.PLANTAO,
        leito_padrao="UTI-04",  # existe no seed e NAO esta em ALERT_FAILURE_LEITOS
        api_key_padrao="dev-monitor-l07",
        intervalo_s=4.0,
        passos=None,  # roda indefinidamente ate Ctrl+C
        criterio_r10_6=None,  # extra, fora de R10.6
        narrativa=(
            "Leito UTI-04 (extra, fora de R10.6): plantao continuo. Os sete sinais mudam a CADA "
            "leitura, como num monitor de verdade, e a gravidade do paciente sobe devagar: card "
            "verde no inicio, amarelo (NEWS2 3-4) por volta de 1min20, vermelho (NEWS2 >= 5, com "
            "alerta no topo do painel) por volta de 2min, oxigenio suplementar em ~2min25 e queda "
            "de consciencia (AVPU V) em ~2min50, com escore acima de 15. Dai em diante o paciente "
            "PERMANECE critico, oscilando -- ninguem volta ao verde sozinho. Roda ate Ctrl+C; use "
            "--escalada SEG para acelerar (ex.: --escalada 60 conta a mesma historia em 1 minuto) "
            "e --leitos N para encher o mural com leitos que pioram em ritmos levemente diferentes."
        ),
        escalada_s=180.0,
    ),
    Cenario.FALHA_CONSUMIDOR: DefinicaoCenario(
        perfil=Perfil.DETERIORACAO,
        leito_padrao="UTI-03",  # precisa constar de ALERT_FAILURE_LEITOS (design 12.4.2)
        api_key_padrao="dev-monitor-uti03",
        intervalo_s=1.0,
        passos=10,
        criterio_r10_6="falha de consumidor com retentativa",
        narrativa=(
            "Leito UTI-03: a equipe deste leito e atendida por um canal de notificacao sabotado "
            "por configuracao (ALERT_FAILURE_LEITOS). A partir do 7o passo o NEWS2 passa de 5, o "
            "alerta e gerado e a notificacao falha; espere 1s, 2s, 4s e a mensagem cair em "
            "q.alert.alerta-gerado.dlq em ~7s. Os demais leitos seguem normais na mesma tela."
        ),
    ),
    Cenario.FORA_DE_FAIXA: DefinicaoCenario(
        perfil=Perfil.FORA_DE_FAIXA,
        leito_padrao="ENF-08",
        api_key_padrao="dev-monitor-l07",
        intervalo_s=2.0,
        passos=5,
        criterio_r10_6="mensagem enviada a DLQ",
        narrativa=(
            "Leito ENF-08: no 3o passo o monitor emite uma saturacao de 20% -- aceita pela borda, "
            "mas fisiologicamente impossivel. O vitals-service a recusa como PermanentError e a "
            "mensagem vai DIRETO para q.vitals.sinais-coletados.dlq, sem consumir retentativa."
        ),
    ),
    Cenario.TEMPESTADE: DefinicaoCenario(
        perfil=Perfil.ESTAVEL,
        leito_padrao="ENF-01",
        api_key_padrao="dev-monitor-l07",
        intervalo_s=0.5,
        passos=20,
        criterio_r10_6=None,  # extra, fora de R10.6
        narrativa=(
            "Tempestade (extra, fora de R10.6): varios leitos publicando em paralelo para tornar "
            "visiveis os competing consumers e o prefetch em acao (R2.5, R2.6). Use --leitos N "
            "para escolher quantos leitos disparam ao mesmo tempo."
        ),
    ),
}
"""Catalogo completo dos cenarios, indexado por :class:`Cenario`."""

ALIASES: Final[dict[str, Cenario]] = {"dlq": Cenario.FORA_DE_FAIXA}
"""Apelidos aceitos em ``--cenario`` alem dos nomes canonicos (design 12.5.5: ``dlq``)."""


def resolver_cenario(nome: str) -> Cenario:
    """Resolve o texto de ``--cenario`` no :class:`Cenario` correspondente, aplicando apelidos.

    Args:
        nome: Valor bruto de ``--cenario`` (ex.: ``"deterioracao"``, ``"dlq"``).

    Returns:
        O :class:`Cenario` correspondente.

    Raises:
        ValueError: Se ``nome`` nao for um cenario conhecido nem um apelido -- com a lista de
            valores aceitos, para que a mensagem de erro do CLI seja util.
    """
    bruto = nome.strip().lower()
    if bruto in ALIASES:
        return ALIASES[bruto]
    try:
        return Cenario(bruto)
    except ValueError as exc:
        aceitos = [c.value for c in Cenario] + list(ALIASES)
        raise ValueError(f"cenario desconhecido: {nome!r}; aceitos: {', '.join(aceitos)}") from exc


def definicao_de(cenario: Cenario) -> DefinicaoCenario:
    """Recupera a :class:`DefinicaoCenario` de um cenario.

    Args:
        cenario: O cenario cuja definicao se quer.

    Returns:
        A definicao completa registrada em :data:`CENARIOS`.
    """
    return CENARIOS[cenario]
