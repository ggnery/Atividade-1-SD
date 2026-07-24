"""Laco assincrono de coleta e envio HTTP autenticado do Cliente_Leito (design 8, 12.5.5, R10.6).

O simulador e um monitor de beira-leito: a cada intervalo ele gera uma leitura (determinista pela
semente, ver :mod:`.perfis`) e a envia com ``POST /sinais`` ao gateway, autenticado por
``X-API-Key`` (R4.5). Ele **nao** publica direto na fila -- fala HTTP com o gateway e nada mais
(C3) -- e **nao** inspeciona a fila nem o ``/metrics``: recebe ``202 Accepted`` com o
``X-Correlation-ID`` e mais nada, a assimetria que justifica a mensageria assincrona (12.5.5).

Erro de rede e telemetria auto-recuperavel (design tabela 2490). Se o ``POST`` falhar por rede
(gateway ainda subindo, conexao recusada, timeout), o simulador registra a falha, **recua** com
espera exponencial e tenta de novo a mesma leitura algumas vezes; esgotadas as tentativas de rede,
ele segue para a proxima leitura em vez de morrer -- um monitor real perde uma amostra, nao o
plantao. Um ``503 broker-indisponivel`` com ``Retry-After`` e tratado do mesmo modo: respeita o
cabecalho e reenvia (R8.5).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from .cenarios import Cenario, DefinicaoCenario, definicao_de
from .perfis import GeradorSinais

__all__ = ["ConfiguracaoSimulador", "Simulador"]

# Recuo exponencial das tentativas de rede: 1s, 2s, 4s, 8s, e teto de 8s dai em diante. Espelha a
# politica 1s/2s/4s do middleware (design 4.6) do lado do cliente, sem jitter, para ser previsivel.
_RECUO_REDE_S: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0)
_MAX_TENTATIVAS_REDE: int = 6
_TIMEOUT_HTTP_S: float = 10.0
# Teto de seguranca ao respeitar Retry-After de um 503, para nao dormir tempo arbitrario.
_MAX_RETRY_AFTER_S: float = 30.0


@dataclass(frozen=True, slots=True)
class ConfiguracaoSimulador:
    """Parametros de uma execucao do simulador, ja resolvidos a partir do CLI e do ambiente.

    Attributes:
        cenario: Cenario escolhido, que fornece perfil, intervalo e narrativa (design 12.5.5).
        base_url: Endereco do gateway (``GATEWAY_URL``; ``http://localhost:8000`` fora do Compose).
        api_key: API Key de dispositivo apresentada em ``X-API-Key`` (R4.5).
        leitos: Leitos que publicarao em paralelo -- um so no caso comum, varios na tempestade.
        intervalo_s: Intervalo efetivo entre leituras, em segundos.
        passos: Numero de leituras por leito quando ``duracao_s`` e ``None``.
        duracao_s: Se informado, publica ate esgotar esta duracao, ignorando ``passos``.
        semente: Semente do gerador, para reproduzir a mesma sequencia (``SEMENTE_SIMULADOR``).
    """

    cenario: Cenario
    base_url: str
    api_key: str
    leitos: tuple[str, ...]
    intervalo_s: float
    passos: int
    duracao_s: float | None
    semente: int

    @property
    def definicao(self) -> DefinicaoCenario:
        """Definicao completa do cenario escolhido."""
        return definicao_de(self.cenario)


@dataclass(slots=True)
class _ResultadoEnvio:
    """Resultado de uma tentativa de envio de uma leitura."""

    status: int | None  # codigo HTTP; None quando a rede falhou sem resposta
    correlation_id: str | None
    detalhe: str


@dataclass(slots=True)
class Simulador:
    """Orquestra a coleta e o envio das leituras de um ou mais leitos (design 12.5.5).

    O simulador imprime a narrativa do cenario antes do primeiro ``POST``, dispara um laco de coleta
    por leito (concorrentes quando ha mais de um) e, ao final, imprime os comandos de observacao --
    ``scripts/trace.sh <CID>`` e a contagem das DLQs -- sem jamais consultar a fila (design 12.5.5).
    """

    config: ConfiguracaoSimulador
    _ultimo_correlation_id: str | None = field(default=None, init=False)

    async def executar(self) -> None:
        """Executa a demonstracao completa: narrativa, laco(s) de coleta e dicas de observacao."""
        cfg = self.config
        self._narrar()
        limits = httpx.Limits(max_connections=max(10, len(cfg.leitos)))
        async with httpx.AsyncClient(
            base_url=cfg.base_url,
            timeout=_TIMEOUT_HTTP_S,
            limits=limits,
            headers={"X-API-Key": cfg.api_key},
        ) as cliente:
            tarefas = [
                asyncio.create_task(self._rodar_leito(cliente, leito, indice))
                for indice, leito in enumerate(cfg.leitos)
            ]
            try:
                await asyncio.gather(*tarefas)
            except asyncio.CancelledError:  # pragma: no cover - so em Ctrl+C
                for tarefa in tarefas:
                    tarefa.cancel()
                raise
        self._dicas_de_observacao()

    def _narrar(self) -> None:
        """Imprime o cabecalho e a narrativa do cenario antes do primeiro envio."""
        cfg = self.config
        alvo = ", ".join(cfg.leitos)
        print(f"== Cliente_Leito | cenario '{cfg.cenario.value}' | leito(s): {alvo} ==")
        print(cfg.definicao.narrativa)
        print(
            f"gateway={cfg.base_url}  intervalo={cfg.intervalo_s:g}s  "
            f"semente={cfg.semente}  "
            + (f"duracao={cfg.duracao_s:g}s" if cfg.duracao_s else f"passos={cfg.passos}")
        )
        print("-" * 72)

    async def _rodar_leito(self, cliente: httpx.AsyncClient, leito: str, indice: int) -> None:
        """Laco de coleta e envio de um unico leito.

        Cada leito tem seu proprio :class:`GeradorSinais`, semeado a partir da semente base somada
        ao indice do leito -- assim a sequencia de cada leito e reproduzivel e, ao mesmo tempo,
        distinta das dos vizinhos na tempestade. O laco para quando ``passos`` leituras foram
        enviadas ou quando ``duracao_s`` se esgota.

        Args:
            cliente: O cliente HTTP compartilhado, ja com ``base_url`` e ``X-API-Key``.
            leito: Codigo do leito deste laco.
            indice: Posicao do leito na lista, usada para derivar a semente e ordenar a saida.
        """
        cfg = self.config
        gerador = GeradorSinais(semente=cfg.semente + indice, perfil=cfg.definicao.perfil)
        inicio = asyncio.get_running_loop().time()
        passo = 0
        while True:
            if cfg.duracao_s is None:
                if passo >= cfg.passos:
                    return
            elif (asyncio.get_running_loop().time() - inicio) >= cfg.duracao_s:
                return

            leitura = gerador.proxima()
            coletado_em = datetime.now(UTC).isoformat()
            payload = leitura.para_payload(leito_id=leito, coletado_em=coletado_em)
            resultado = await self._enviar(cliente, payload)
            self._registrar(leito, passo, leitura.saturacao_o2, resultado)
            passo += 1
            await asyncio.sleep(cfg.intervalo_s)

    async def _enviar(
        self, cliente: httpx.AsyncClient, payload: dict[str, object]
    ) -> _ResultadoEnvio:
        """Envia uma leitura, tratando erro de rede com recuo e ``503`` com ``Retry-After``.

        Args:
            cliente: O cliente HTTP compartilhado.
            payload: Corpo JSON da leitura (ver :meth:`SinaisVitais.para_payload`).

        Returns:
            O :class:`_ResultadoEnvio` da ultima tentativa -- com ``status=None`` se a rede nunca
            respondeu dentro do numero maximo de tentativas.
        """
        ultimo_detalhe = "sem resposta"
        for tentativa in range(1, _MAX_TENTATIVAS_REDE + 1):
            try:
                resposta = await cliente.post("/sinais", json=payload)
            except httpx.RequestError as exc:
                ultimo_detalhe = f"rede: {type(exc).__name__}"
                await self._recuar(tentativa, ultimo_detalhe)
                continue

            if resposta.status_code == 503:
                espera = self._retry_after(resposta, tentativa)
                ultimo_detalhe = f"503 broker-indisponivel; aguardando {espera:g}s"
                print(f"   ! {ultimo_detalhe}")
                await asyncio.sleep(espera)
                continue

            correlation_id = resposta.headers.get("X-Correlation-ID")
            if correlation_id is not None:
                self._ultimo_correlation_id = correlation_id
            return _ResultadoEnvio(
                status=resposta.status_code,
                correlation_id=correlation_id,
                detalhe=self._detalhe_da_resposta(resposta),
            )

        return _ResultadoEnvio(status=None, correlation_id=None, detalhe=ultimo_detalhe)

    async def _recuar(self, tentativa: int, motivo: str) -> None:
        """Dorme o tempo de recuo da ``tentativa`` (exponencial, com teto)."""
        espera = _RECUO_REDE_S[min(tentativa - 1, len(_RECUO_REDE_S) - 1)]
        print(f"   ! falha de {motivo}; recuo de {espera:g}s (tentativa {tentativa})")
        await asyncio.sleep(espera)

    @staticmethod
    def _retry_after(resposta: httpx.Response, tentativa: int) -> float:
        """Le o cabecalho ``Retry-After`` de um ``503``, com fallback no recuo exponencial."""
        bruto = resposta.headers.get("Retry-After")
        if bruto is not None:
            try:
                return min(float(bruto), _MAX_RETRY_AFTER_S)
            except ValueError:
                pass
        return _RECUO_REDE_S[min(tentativa - 1, len(_RECUO_REDE_S) - 1)]

    @staticmethod
    def _detalhe_da_resposta(resposta: httpx.Response) -> str:
        """Extrai um detalhe curto e legivel da resposta (o ``title`` do RFC 7807, se houver)."""
        if resposta.status_code == 202:
            return "aceito"
        try:
            corpo = resposta.json()
        except ValueError:
            return resposta.text[:120]
        if isinstance(corpo, dict):
            titulo = corpo.get("title") or corpo.get("detail") or corpo.get("type")
            if titulo:
                return str(titulo)
        return str(corpo)[:120]

    def _registrar(
        self, leito: str, passo: int, saturacao: int, resultado: _ResultadoEnvio
    ) -> None:
        """Imprime uma linha por leitura, no espirito do ``202`` + ``X-Correlation-ID`` (12.5.5)."""
        prefixo = f"[{leito} #{passo:02d}] SpO2={saturacao}%"
        if resultado.status == 202:
            cid = resultado.correlation_id or "?"
            print(f"{prefixo} -> HTTP 202  X-Correlation-ID={cid}")
        elif resultado.status is None:
            print(f"{prefixo} -> sem envio ({resultado.detalhe})")
        else:
            print(f"{prefixo} -> HTTP {resultado.status}  {resultado.detalhe}")

    def _dicas_de_observacao(self) -> None:
        """Imprime, ao final, os comandos de observacao (design 12.5.5). Nao consulta a fila."""
        print("-" * 72)
        cid = self._ultimo_correlation_id
        if cid is not None:
            print(f"Rastreie o fluxo desta demo:   scripts/trace.sh {cid}")
        if self.config.cenario is Cenario.FALHA_CONSUMIDOR:
            print(
                "Confira a DLQ de alertas (~7s apos o 7o passo):   "
                "fila q.alert.alerta-gerado.dlq na UI do RabbitMQ"
            )
        elif self.config.cenario is Cenario.FORA_DE_FAIXA:
            print(
                "Confira a DLQ de sinais (imediata, sem retentativa):   "
                "fila q.vitals.sinais-coletados.dlq na UI do RabbitMQ"
            )
