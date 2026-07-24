"""Projecao em memoria dos leitos que alimenta o Painel_de_Leitos (design 8.7, R11).

O ``api-gateway`` hospeda, no mesmo *event loop* do FastAPI, um
:class:`~hospitalmq.consumer.Consumer`
ligado a fila ``q.gateway.projecao``. Cada evento consumido muta esta projecao -- a **unica** fonte
do
painel e dos endpoints ``GET /leitos`` e ``GET /alertas`` -- sem nenhum SELECT no banco clinico
(R7.2
preservado por R11.7). A projecao e um estado **derivado e volatil**: reinicia vazia e e reidratada
por
RPC ``leitos.snapshot`` no *boot* (design 8.7.6).

Indexacao por **codigo humano do leito** (ex. ``UTI-03``). Os eventos ``sinais.*`` e ``alerta.*`` ja
trazem o codigo; os eventos administrativos (``paciente.*`` e ``leito.*``) trazem o **UUID** do
leito, e
a hidratacao por ``leitos.snapshot`` -- que devolve os dois -- constroi o mapa ``UUID -> codigo``
usado
para traduzi-los (ver :meth:`ProjecaoLeitos._codigo_do_evento`).

**Politica de erro na projecao (design 8.7.1).** :meth:`ProjecaoLeitos.aplicar` **nunca** propaga
excecao: captura, registra ``warning`` e confirma a mensagem. A projecao e derivada -- reprocessar
um
evento nao conserta uma renderizacao, e mandar um redesenho de card a DLQ poluiria a fila de
inspecao
humana. E a unica excecao documentada a politica de retentativa/DLQ do R2, e e segura porque o
evento
original continua registrado na Trilha_de_Auditoria pelo ``audit-service``.

**Severidade (R11.3, R11.1).** ``sinais.registrados`` nao carrega o escore -- o ``triage-service``
so
emite ``alerta.gerado`` para severidade **alta** (design 7.5.2). Para que os cards de severidade
baixa/media tambem mudem de cor ao vivo *sem* duplicar o limiar clinico no JavaScript (proibido por
8.7.7), a projecao recomputa o NEWS2 no servidor reusando a **funcao pura unica**
:func:`services.comum.news2.calcular_news2` -- a mesma do ``triage-service`` (R11.1). O evento
``alerta.gerado`` confirma o card por *last-write-wins* e alimenta a lista de alertas.
"""

from __future__ import annotations

import contextlib
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from sse import DifusorSSE, agora_iso

from hospitalmq.envelope import Identity
from services.comum.news2 import ScoreNEWS2, SinaisVitais, calcular_news2

# Identidade de servico do gateway para a hidratacao de boot do painel (design 8.7.6, 4630):
# ``leitos.snapshot`` exige o papel ``servico`` (_PAPEIS_SNAPSHOT no admission-service). O design
# (4666) grafa ``tipo="servico"``, mas o dominio fechado de ``identity.tipo`` do Envelope congelado
# e {``usuario``, ``dispositivo``} -- ``tipo="servico"`` viraria ``ENVELOPE_INVALIDO`` no
# ``from_bytes`` do RpcServer. Usamos ``tipo="usuario"`` com ``role="servico"``: o ``role`` nao tem
# dominio fechado no Envelope, satisfaz o ``_verificar_papel`` do servidor e a coluna
# ``identidade_tipo`` da auditoria (CHECK usuario/dispositivo). Divergencia registrada.
IDENTIDADE_SERVICO: Identity = Identity(sub="api-gateway", role="servico", tipo="usuario")

if TYPE_CHECKING:  # pragma: no cover - apenas para type-check
    import asyncio
    from collections.abc import Callable

    from hospitalmq import Consumer, MessageContext, RpcClient

__all__ = [
    "AlertaPainel",
    "CardLeito",
    "ProjecaoLeitos",
    "registrar_handlers",
]

Severidade = Literal["normal", "baixa", "media", "alta"]

#: Campos de sinais vitais exibidos no card, na grafia de dominio (design 8.7.3).
_CAMPOS_SINAIS: tuple[str, ...] = (
    "frequencia_respiratoria",
    "saturacao_o2",
    "oxigenio_suplementar",
    "temperatura",
    "pressao_sistolica",
    "frequencia_cardiaca",
    "nivel_consciencia",
)

#: Os nove tipos de evento que alimentam a projecao, um por *binding key* da fila (design 8.7.1).
TIPOS_PROJECAO: tuple[str, ...] = (
    "paciente.admitido",
    "paciente.alta",
    "leito.ocupado",
    "leito.liberado",
    "sinais.registrados",
    "sinais.rejeitados",
    "alerta.gerado",
    "alerta.notificado",
    "alerta.falhou",
)


@dataclass(slots=True)
class CardLeito:
    """Estado atual de um leito, tal como o card o exibe (design 8.7.2).

    Indexado pelo ``leito_id`` **codigo humano** (ex. ``UTI-03``). ``componente_critico`` guarda o
    indicador booleano recebido pronto do evento (``True`` quando um componente isolado pontuou 3,
    R6.3),
    nunca recalculado no navegador.
    """

    leito_id: str
    estado: Literal["livre", "ocupado"] = "livre"
    setor: str | None = None
    paciente_id: str | None = None
    paciente_nome: str | None = None
    internacao_id: str | None = None
    sinais: dict[str, Any] | None = None
    score_news2: int | None = None
    componente_critico: bool | None = None
    severidade: Severidade = "normal"
    ultima_rejeicao: dict[str, Any] | None = None
    ultimo_alerta_estado: Literal["nenhum", "gerado", "notificado", "falhou"] = "nenhum"
    atualizado_em: datetime | None = None
    correlation_id: str | None = None


@dataclass(slots=True)
class AlertaPainel:
    """Item da coluna de alertas recentes (design 8.7.2, 8.7.7).

    ``estado`` reflete o despacho (``gerado`` -> ``notificado`` | ``falhou``): extensao pratica
    sobre a
    estrutura de 8.7.2, exigida pela coluna de alertas de 8.7.7 que mostra o estado do envio (R6.4,
    R6.5).
    """

    alerta_id: str
    leito_id: str
    paciente_nome: str | None
    score_news2: int
    severidade: Severidade
    gerado_em: datetime
    correlation_id: str
    estado: Literal["gerado", "notificado", "falhou"] = "gerado"


class ProjecaoLeitos:
    """Estado em memoria dos leitos, alimentado por eventos e lido pelo painel e pelo REST (R11.7).

    Metodos consumidos pelo :class:`~hospitalmq.consumer.Consumer` (:meth:`aplicar`), pela
    hidratacao de
    boot (:meth:`hidratar`, :meth:`hidratar_leito`), pela resolucao de ``internacao_id`` da borda
    (:meth:`leito`, design 8.2.5) e pelas rotas ``GET /leitos`` e ``GET /alertas``
    (:meth:`listar_leitos`, :meth:`listar_alertas`, :meth:`snapshot`).
    """

    def __init__(self, *, limite_alertas: int = 50, difusor: DifusorSSE | None = None) -> None:
        """Inicializa a projecao vazia (nasce sem cards; hidratada por RPC no boot, design 8.7.6).

        Args:
            limite_alertas: Tamanho maximo da lista de alertas recentes (``deque`` limitado, sem
                crescimento ilimitado, design 8.7.2).
            difusor: Difusor SSE injetavel; quando ``None``, cria um proprio.
        """
        self._leitos: dict[str, CardLeito] = {}
        self._alertas: deque[AlertaPainel] = deque(maxlen=limite_alertas)
        self._alerta_index: dict[str, AlertaPainel] = {}
        self._uuid_para_codigo: dict[str, str] = {}
        self._internacao_para_codigo: dict[str, str] = {}
        self._difusor: DifusorSSE = difusor if difusor is not None else DifusorSSE()
        self.hidratada: bool = False

    # -- consumo de eventos (Consumer) ------------------------------------- #

    async def aplicar(self, ctx: MessageContext) -> None:
        """Handler do :class:`~hospitalmq.consumer.Consumer`: muta o estado e difunde ao SSE
        (R11.2).

        **Nunca levanta excecao** (design 8.7.1): captura tudo, registra ``warning`` e retorna, o
        que
        confirma a mensagem. A aplicacao e *idempotente por natureza* (estado derivado,
        *last-write-wins*
        por leito), entao reprocessar e inofensivo.

        Args:
            ctx: Contexto da mensagem. ``ctx.envelope`` traz ``type``, ``timestamp`` e
            ``correlation_id``;
                ``ctx.payload`` e o ``dict`` cru do evento (nenhum modelo Pydantic e registrado).
        """
        try:
            envelope = ctx.envelope
            tipo = envelope.type
            manipulador = self._DESPACHO.get(tipo)
            if manipulador is None:
                return
            payload = ctx.payload if isinstance(ctx.payload, dict) else {}
            manipulador(self, payload, envelope.timestamp, envelope.correlation_id)
        except Exception as exc:
            self._logar_falha(ctx, exc)

    def _logar_falha(self, ctx: MessageContext, exc: Exception) -> None:
        """Registra ``warning`` sem nunca levantar (log tolerante a qualquer ``ctx``)."""
        with contextlib.suppress(Exception):
            ctx.log.warning(
                "projecao.evento_ignorado",
                tipo=getattr(ctx.envelope, "type", None),
                erro=type(exc).__name__,
                detalhe=str(exc),
            )

    # -- handlers por tipo de evento --------------------------------------- #

    def _ev_admitido(self, p: dict[str, Any], ts: datetime, corr: str) -> None:
        """``paciente.admitido``: cria/atualiza o card ocupado com nome, paciente e internacao."""
        card = self._card_do_evento(p)
        self._aprender_internacao(card, p)
        if not self._mais_novo(card, ts):
            return
        card.estado = "ocupado"
        card.internacao_id = self._texto(p.get("internacao_id")) or card.internacao_id
        card.paciente_id = self._texto(p.get("paciente_id")) or card.paciente_id
        card.paciente_nome = self._texto(p.get("nome")) or card.paciente_nome
        self._marcar(card, ts, corr)
        self._difundir_leito(card)

    def _ev_leito_ocupado(self, p: dict[str, Any], ts: datetime, corr: str) -> None:
        """``leito.ocupado``: confirma ocupacao e setor do leito."""
        card = self._card_do_evento(p)
        self._aprender_internacao(card, p)
        if not self._mais_novo(card, ts):
            return
        card.estado = "ocupado"
        card.internacao_id = self._texto(p.get("internacao_id")) or card.internacao_id
        setor = self._texto(p.get("setor"))
        if setor:
            card.setor = setor
        self._marcar(card, ts, corr)
        self._difundir_leito(card)

    def _ev_liberado(self, p: dict[str, Any], ts: datetime, corr: str) -> None:
        """``paciente.alta`` / ``leito.liberado``: zera o card e volta ao estado livre."""
        card = self._card_do_evento(p)
        if not self._mais_novo(card, ts):
            return
        card.estado = "livre"
        card.paciente_id = None
        card.paciente_nome = None
        card.internacao_id = None
        card.sinais = None
        card.score_news2 = None
        card.componente_critico = None
        card.severidade = "normal"
        card.ultima_rejeicao = None
        card.ultimo_alerta_estado = "nenhum"
        self._marcar(card, ts, corr)
        self._difundir_leito(card)

    def _ev_sinais_registrados(self, p: dict[str, Any], ts: datetime, corr: str) -> None:
        """``sinais.registrados``: atualiza os sinais, recomputa NEWS2 e limpa a tarja de
        rejeicao."""
        card = self._card_do_evento(p)
        self._aprender_internacao(card, p)
        if not self._mais_novo(card, ts):
            return
        card.estado = "ocupado"
        card.internacao_id = self._texto(p.get("internacao_id")) or card.internacao_id
        card.sinais = {campo: p.get(campo) for campo in _CAMPOS_SINAIS}
        card.ultima_rejeicao = None
        score = self._pontuar(p)
        if score is not None:
            card.score_news2 = score.total
            card.componente_critico = score.componente_critico
            card.severidade = score.severidade.value  # type: ignore[assignment]
        self._marcar(card, ts, corr)
        self._difundir_leito(card)

    def _ev_sinais_rejeitados(self, p: dict[str, Any], ts: datetime, corr: str) -> None:
        """``sinais.rejeitados``: acende a tarja de rejeicao sem alterar os sinais exibidos
        (R6.6)."""
        card = self._card_do_evento(p)
        card.ultima_rejeicao = {
            "campo": self._texto(p.get("campo")),
            "motivo": self._texto(p.get("motivo")),
            "valor_recebido": p.get("valor_recebido"),
            "faixa_aceita": self._texto(p.get("faixa_aceita")),
        }
        card.correlation_id = corr
        self._difundir_leito(card)

    def _ev_alerta_gerado(self, p: dict[str, Any], ts: datetime, corr: str) -> None:
        """``alerta.gerado``: atualiza escore/severidade e insere o alerta no topo da lista
        (R11.4)."""
        card = self._card_do_evento(p)
        self._aprender_internacao(card, p)
        severidade = self._texto(p.get("severidade")) or "alta"
        score = self._inteiro(p.get("score_news2"))
        if self._mais_novo(card, ts):
            card.estado = "ocupado"
            card.internacao_id = self._texto(p.get("internacao_id")) or card.internacao_id
            if score is not None:
                card.score_news2 = score
            card.componente_critico = bool(p.get("componente_critico"))
            card.severidade = severidade  # type: ignore[assignment]
            self._marcar(card, ts, corr)
        card.ultimo_alerta_estado = "gerado"
        card.correlation_id = corr

        alerta_id = self._texto(p.get("alerta_id")) or corr
        alerta = AlertaPainel(
            alerta_id=alerta_id,
            leito_id=card.leito_id,
            paciente_nome=card.paciente_nome,
            score_news2=score if score is not None else 0,
            severidade=severidade,  # type: ignore[arg-type]
            gerado_em=ts,
            correlation_id=corr,
            estado="gerado",
        )
        self._inserir_alerta(alerta)
        self._difundir_leito(card)
        self._difundir_alerta(alerta)

    def _ev_alerta_notificado(self, p: dict[str, Any], ts: datetime, corr: str) -> None:
        """``alerta.notificado``: registra o despacho bem-sucedido (R6.4)."""
        self._atualizar_estado_alerta(p, corr, "notificado")

    def _ev_alerta_falhou(self, p: dict[str, Any], ts: datetime, corr: str) -> None:
        """``alerta.falhou``: registra o esgotamento das retentativas de despacho (R6.5)."""
        self._atualizar_estado_alerta(p, corr, "falhou")

    def _atualizar_estado_alerta(
        self, p: dict[str, Any], corr: str, estado: Literal["notificado", "falhou"]
    ) -> None:
        """Propaga o estado de despacho ao item da lista e ao card correspondente.

        ``alerta.notificado`` e ``alerta.falhou`` carregam apenas ``alerta_id`` -- o leito e
        resolvido
        pelo indice ``alerta_id -> AlertaPainel`` mantido em :meth:`_inserir_alerta`.
        """
        alerta_id = self._texto(p.get("alerta_id"))
        if alerta_id is None:
            return
        alerta = self._alerta_index.get(alerta_id)
        if alerta is not None:
            alerta.estado = estado
            card = self._leitos.get(alerta.leito_id)
            if card is not None:
                card.ultimo_alerta_estado = estado
                card.correlation_id = corr
                self._difundir_leito(card)
            self._difundir_alerta(alerta)

    #: Despacho ``tipo do evento -> metodo``, resolvido em :meth:`aplicar`.
    _DESPACHO: ClassVar[dict[str, Any]] = {
        "paciente.admitido": _ev_admitido,
        "leito.ocupado": _ev_leito_ocupado,
        "paciente.alta": _ev_liberado,
        "leito.liberado": _ev_liberado,
        "sinais.registrados": _ev_sinais_registrados,
        "sinais.rejeitados": _ev_sinais_rejeitados,
        "alerta.gerado": _ev_alerta_gerado,
        "alerta.notificado": _ev_alerta_notificado,
        "alerta.falhou": _ev_alerta_falhou,
    }

    # -- escrita otimista da admissao (design 8.2.4) ----------------------- #

    def aplicar_admissao(self, resposta: object) -> None:
        """Escrita otimista com a resposta do RPC ``paciente.admitir`` (design 8.2.4).

        Fecha a janela de corrida em que ``POST /internacoes`` ja devolveu 201 e um ``POST /sinais``
        imediato veria o leito "livre" (design 8.2.5). Usa ``admitido_em`` como marca temporal; o
        evento
        ``paciente.admitido`` que chega depois, com ``timestamp`` posterior, confirma o card por
        *last-write-wins*.

        Args:
            resposta: ``InternacaoResponse`` (ou objeto/``dict`` equivalente) cujo ``leito_id`` e o
                **codigo** humano do leito (design 8.2.1, endpoint 3).
        """
        leito_id = self._texto(self._campo(resposta, "leito_id"))
        if leito_id is None:
            return
        internacao_id = self._texto(self._campo(resposta, "internacao_id"))
        paciente_id = self._texto(self._campo(resposta, "paciente_id"))
        setor = self._texto(self._campo(resposta, "setor"))
        ts = self._para_datetime(self._campo(resposta, "admitido_em")) or self._agora()

        card = self._garantir_card(leito_id)
        if internacao_id is not None:
            self._internacao_para_codigo[internacao_id] = card.leito_id
        if not self._mais_novo(card, ts):
            return
        card.estado = "ocupado"
        card.internacao_id = internacao_id or card.internacao_id
        card.paciente_id = paciente_id or card.paciente_id
        if setor:
            card.setor = setor
        self._marcar(card, ts, self._campo(resposta, "correlation_id"))
        self._difundir_leito(card)

    # -- hidratacao de boot por RPC (design 8.7.6, R11.7) ------------------ #

    async def hidratar(self, rpc: RpcClient) -> None:
        """Uma chamada ``leitos.snapshot`` no boot; define ``hidratada=True`` em caso de sucesso.

        Reconstroi ``estado``, ``paciente`` e ``internacao_id`` de cada leito e o mapa ``UUID ->
        codigo``.
        Se o ``admission-service`` estiver fora, a flag permanece ``False`` e o gateway sobe assim
        mesmo
        (R8.5); cada resolucao pontual tentara :meth:`hidratar_leito`.

        Args:
            rpc: Cliente RPC ja iniciado (``leitos.snapshot`` roteia para ``rpc.admission``).
        """
        leitos = await self._buscar_snapshot(rpc)
        if leitos is None:
            self.hidratada = False
            return
        for leito in leitos:
            self._absorver_leito_snapshot(leito)
        self.hidratada = True

    async def hidratar_leito(self, rpc: RpcClient, leito_id: str) -> CardLeito | None:
        """Hidratacao pontual quando a projecao ainda nao esta reconciliada (design 8.2.5).

        Args:
            rpc: Cliente RPC ja iniciado.
            leito_id: Codigo humano do leito procurado (ex. ``UTI-03``).

        Returns:
            O card do leito pedido, ou ``None`` se o snapshot falhar ou o leito nao existir.
        """
        leitos = await self._buscar_snapshot(rpc)
        if leitos is None:
            return None
        alvo: CardLeito | None = None
        for leito in leitos:
            card = self._absorver_leito_snapshot(leito)
            if card is not None and card.leito_id == leito_id:
                alvo = card
        return alvo

    async def _buscar_snapshot(self, rpc: RpcClient) -> list[dict[str, Any]] | None:
        """Executa ``leitos.snapshot`` tolerando qualquer indisponibilidade (retorna ``None``)."""
        try:
            dados = await rpc.call(
                "leitos.snapshot",
                {"apenas_ocupados": False},
                identity=IDENTIDADE_SERVICO,
                timeout=5.0,
            )
        except BaseException as exc:
            if _e_cancelamento(exc):
                raise
            return None
        leitos = dados.get("leitos") if isinstance(dados, dict) else None
        return leitos if isinstance(leitos, list) else []

    def _absorver_leito_snapshot(self, leito: dict[str, Any]) -> CardLeito | None:
        """Aplica uma linha de ``leitos.snapshot`` a projecao (estado, paciente, mapa
        UUID->codigo)."""
        if not isinstance(leito, dict):
            return None
        codigo = self._texto(leito.get("codigo")) or self._texto(leito.get("leito_id"))
        if codigo is None:
            return None
        uuid_leito = self._texto(leito.get("leito_id"))
        if uuid_leito is not None and uuid_leito != codigo:
            self._uuid_para_codigo[uuid_leito] = codigo
        card = self._garantir_card(codigo)
        card.estado = "ocupado" if self._texto(leito.get("estado")) == "ocupado" else card.estado
        if self._texto(leito.get("estado")) == "livre" and card.atualizado_em is None:
            card.estado = "livre"
        internacao_id = self._texto(leito.get("internacao_id"))
        card.internacao_id = internacao_id or card.internacao_id
        card.paciente_id = self._texto(leito.get("paciente_id")) or card.paciente_id
        card.paciente_nome = self._texto(leito.get("paciente_nome")) or card.paciente_nome
        setor = self._texto(leito.get("setor"))
        if setor:
            card.setor = setor
        if internacao_id is not None:
            self._internacao_para_codigo[internacao_id] = codigo
        return card

    # -- leitura pela borda e pelas rotas ---------------------------------- #

    def leito(self, leito_id: str) -> CardLeito | None:
        """Consulta ``O(1)`` de um card por codigo, usada na resolucao de ``internacao_id``
        (8.2.5)."""
        return self._leitos.get(self._codigo_do_evento({"leito_id": leito_id}))

    def snapshot(self) -> dict[str, Any]:
        """Estado completo enviado no inicio de toda conexao SSE (evento ``snapshot``, design
        8.7.3)."""
        return {
            "leitos": [_card_para_dict(card) for card in self._leitos.values()],
            "alertas": [_alerta_para_dict(alerta) for alerta in self._alertas],
            "atualizado_em": agora_iso(),
            "hidratada": self.hidratada,
        }

    def listar_leitos(self) -> list[dict[str, Any]]:
        """Cards ordenados por codigo, para ``GET /leitos`` (origem projecao, design 8.2.1)."""
        return [_card_para_dict(self._leitos[codigo]) for codigo in sorted(self._leitos)]

    def listar_alertas(
        self,
        *,
        severidade: str | None = None,
        leito_id: str | None = None,
        limite: int = 50,
    ) -> list[dict[str, Any]]:
        """Alertas recentes filtrados, para ``GET /alertas`` (mais recente primeiro, design 8.2.1).

        Args:
            severidade: Filtra por banda de risco (``baixa`` | ``media`` | ``alta``); ``None`` nao
            filtra.
            leito_id: Filtra por codigo de leito; ``None`` nao filtra.
            limite: Numero maximo de itens devolvidos.
        """
        itens: list[dict[str, Any]] = []
        for alerta in self._alertas:
            if severidade is not None and alerta.severidade != severidade:
                continue
            if leito_id is not None and alerta.leito_id != leito_id:
                continue
            itens.append(_alerta_para_dict(alerta))
            if len(itens) >= limite:
                break
        return itens

    # -- assinatura SSE (delegada ao difusor) ------------------------------ #

    def assinar(self) -> tuple[asyncio.Queue[str], Callable[[], None]]:
        """Registra um assinante SSE e devolve ``(fila, cancelar)`` (design 8.7.2)."""
        return self._difusor.assinar()

    @property
    def ultimo_id(self) -> int:
        """Ultimo ``id`` SSE emitido, usado para rotular o ``snapshot`` inicial de cada conexao."""
        return self._difusor.ultimo_id

    @property
    def total_assinantes(self) -> int:
        """Quantidade de conexoes SSE ativas (para ``/health`` e depuracao)."""
        return self._difusor.total_assinantes

    # -- utilitarios internos ---------------------------------------------- #

    def _garantir_card(self, codigo: str) -> CardLeito:
        """Devolve o card do codigo, criando um leito livre se ainda nao existir."""
        card = self._leitos.get(codigo)
        if card is None:
            card = CardLeito(leito_id=codigo)
            self._leitos[codigo] = card
        return card

    def _card_do_evento(self, p: dict[str, Any]) -> CardLeito:
        """Resolve o card destino de um evento pelo codigo (traduzindo UUID quando preciso)."""
        return self._garantir_card(self._codigo_do_evento(p))

    def _codigo_do_evento(self, p: dict[str, Any]) -> str:
        """Traduz o ``leito_id`` de um evento no codigo humano indexador (design 8.7.1).

        Ordem: (1) se ja for um codigo conhecido, usa direto; (2) traduz ``UUID -> codigo`` pelo
        mapa da
        hidratacao; (3) recorre a ``internacao_id -> codigo`` aprendido em runtime; (4) *fallback*
        honesto:
        usa o valor cru (leito ainda nao reconciliado; o boot normal ja o teria mapeado).
        """
        bruto = self._texto(p.get("leito_id"))
        if bruto is not None:
            if bruto in self._leitos:
                return bruto
            codigo = self._uuid_para_codigo.get(bruto)
            if codigo is not None:
                return codigo
        internacao_id = self._texto(p.get("internacao_id"))
        if internacao_id is not None:
            codigo = self._internacao_para_codigo.get(internacao_id)
            if codigo is not None:
                return codigo
        return bruto if bruto is not None else "?"

    def _aprender_internacao(self, card: CardLeito, p: dict[str, Any]) -> None:
        """Memoriza ``internacao_id -> codigo`` para resolver eventos que so trazem a internacao."""
        internacao_id = self._texto(p.get("internacao_id"))
        if internacao_id is not None:
            self._internacao_para_codigo[internacao_id] = card.leito_id

    def _inserir_alerta(self, alerta: AlertaPainel) -> None:
        """Insere o alerta no topo do ``deque`` (mais recente a esquerda) e mantem o indice
        enxuto."""
        if len(self._alertas) == self._alertas.maxlen:
            evicto = self._alertas[-1]
            self._alerta_index.pop(evicto.alerta_id, None)
        self._alertas.appendleft(alerta)
        self._alerta_index[alerta.alerta_id] = alerta

    def _mais_novo(self, card: CardLeito, ts: datetime | None) -> bool:
        """Regra de *last-write-wins* por leito: aplica so se ``ts`` nao for anterior ao card
        (8.7.1)."""
        if ts is None or card.atualizado_em is None:
            return True
        return ts >= card.atualizado_em

    def _marcar(self, card: CardLeito, ts: datetime | None, corr: object) -> None:
        """Grava o instante e a correlacao do ultimo evento de estado aplicado ao card."""
        card.atualizado_em = ts if ts is not None else self._agora()
        texto = self._texto(corr)
        if texto is not None:
            card.correlation_id = texto

    def _pontuar(self, p: dict[str, Any]) -> ScoreNEWS2 | None:
        """Recomputa o ``ScoreNEWS2`` da leitura pela funcao pura unica (R11.1); ``None`` se
        inviavel."""
        try:
            sinais = SinaisVitais(
                frequencia_respiratoria=int(p["frequencia_respiratoria"]),
                saturacao_o2=int(p["saturacao_o2"]),
                oxigenio_suplementar=bool(p["oxigenio_suplementar"]),
                temperatura=Decimal(str(p["temperatura"])).quantize(
                    Decimal("0.1"), rounding=ROUND_HALF_UP
                ),
                pressao_sistolica=int(p["pressao_sistolica"]),
                frequencia_cardiaca=int(p["frequencia_cardiaca"]),
                nivel_consciencia=p["nivel_consciencia"],
            )
            return calcular_news2(sinais)
        except (KeyError, ValueError, TypeError, InvalidOperation):
            return None

    def _difundir_leito(self, card: CardLeito) -> None:
        """Difunde o card atualizado como evento SSE ``leito`` a todos os assinantes."""
        self._difusor.publicar("leito", _card_para_dict(card))

    def _difundir_alerta(self, alerta: AlertaPainel) -> None:
        """Difunde o alerta como evento SSE ``alerta`` a todos os assinantes."""
        self._difusor.publicar("alerta", _alerta_para_dict(alerta))

    @staticmethod
    def _texto(valor: object) -> str | None:
        """Normaliza um valor em ``str`` nao-vazia, ou ``None``."""
        if valor is None:
            return None
        texto = str(valor).strip()
        return texto or None

    @staticmethod
    def _inteiro(valor: object) -> int | None:
        """Converte um valor em ``int``, ou ``None`` se impossivel."""
        try:
            return int(valor)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _campo(obj: object, nome: str) -> object:
        """Le ``obj.nome`` (objeto/modelo) ou ``obj[nome]`` (``dict``), tolerando ausencia."""
        if isinstance(obj, dict):
            return obj.get(nome)
        return getattr(obj, nome, None)

    @staticmethod
    def _agora() -> datetime:
        """Instante atual *aware* em UTC (marca temporal de fallback)."""
        return datetime.now(UTC)

    @classmethod
    def _para_datetime(cls, valor: object) -> datetime | None:
        """Converte ``datetime`` ou ISO-8601 (inclusive sufixo ``Z``) em ``datetime`` *aware*."""
        if isinstance(valor, datetime):
            return valor if valor.tzinfo is not None else valor.replace(tzinfo=UTC)
        texto = cls._texto(valor)
        if texto is None:
            return None
        try:
            momento = datetime.fromisoformat(texto.replace("Z", "+00:00"))
        except ValueError:
            return None
        return momento if momento.tzinfo is not None else momento.replace(tzinfo=UTC)


def _card_para_dict(card: CardLeito) -> dict[str, Any]:
    """Projeta um :class:`CardLeito` no objeto JSON do evento SSE ``leito`` / de ``GET /leitos``."""
    return {
        "leito_id": card.leito_id,
        "estado": card.estado,
        "setor": card.setor,
        "paciente_id": card.paciente_id,
        "paciente_nome": card.paciente_nome,
        "internacao_id": card.internacao_id,
        "sinais": card.sinais,
        "score_news2": card.score_news2,
        "componente_critico": card.componente_critico,
        "severidade": card.severidade,
        "ultima_rejeicao": card.ultima_rejeicao,
        "ultimo_alerta_estado": card.ultimo_alerta_estado,
        "atualizado_em": _iso(card.atualizado_em),
        "correlation_id": card.correlation_id,
    }


def _alerta_para_dict(alerta: AlertaPainel) -> dict[str, Any]:
    """Projeta um :class:`AlertaPainel` no objeto JSON do evento SSE ``alerta`` / de ``GET
    /alertas``."""
    return {
        "alerta_id": alerta.alerta_id,
        "leito_id": alerta.leito_id,
        "paciente_nome": alerta.paciente_nome,
        "score_news2": alerta.score_news2,
        "severidade": alerta.severidade,
        "estado": alerta.estado,
        "gerado_em": _iso(alerta.gerado_em),
        "correlation_id": alerta.correlation_id,
    }


def _iso(momento: datetime | None) -> str | None:
    """Serializa ``datetime`` como ISO-8601 UTC com sufixo ``Z``; ``None`` passa direto."""
    if momento is None:
        return None
    aware = momento if momento.tzinfo is not None else momento.replace(tzinfo=UTC)
    return aware.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _e_cancelamento(exc: BaseException) -> bool:
    """Indica se a excecao e um cancelamento de task, que deve sempre propagar."""
    import asyncio

    return isinstance(exc, asyncio.CancelledError)


def registrar_handlers(consumer: Consumer, projecao: ProjecaoLeitos) -> None:
    """Registra :meth:`ProjecaoLeitos.aplicar` para os nove tipos na fila ``q.gateway.projecao``
    (8.7.1).

    ``idempotente=False`` porque o gateway nao tem banco (8.1.1) e a projecao e idempotente por
    natureza
    (8.7.1). Todos os tipos compartilham o mesmo handler; o despacho por ``type`` acontece dentro de
    :meth:`ProjecaoLeitos.aplicar`.

    Args:
        consumer: O :class:`~hospitalmq.consumer.Consumer` do gateway (``session_factory=None``).
        projecao: A projecao a alimentar.
    """
    for tipo in TIPOS_PROJECAO:
        consumer.on(tipo, queue="q.gateway.projecao", idempotente=False)(projecao.aplicar)
