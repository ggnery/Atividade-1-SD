"""CLI do Cliente_Leito: simulador de monitor de beira-leito (design 12.5.5, R10.6).

Executavel de duas formas equivalentes, e ambas precisam funcionar:

* como modulo do pacote -- ``python -m clients.bedside_monitor --help`` (design 8/12.8);
* como script solto -- ``python clients/bedside_monitor/__main__.py --cenario estavel``, que e a
  forma que o ``docker-compose.yml`` usa no ``command`` do servico ``bedside-monitor`` (design
  12.3, linha do compose).

A diferenca importa para os *imports*: rodado como modulo, ``__package__`` esta definido e os
*imports* relativos funcionam; rodado como script, ``__package__`` e vazio e eles falhariam. O bloco
de importacao abaixo cobre os dois casos -- tenta o *import* relativo e, se falhar, poe a raiz do
repositorio em ``sys.path`` e importa pelo nome totalmente qualificado do pacote.

Modo continuo. O cenario ``plantao`` roda **indefinidamente** quando ``--duracao`` e omitido: os
sinais mudam a cada leitura e a gravidade sobe devagar ate o paciente ficar critico -- e, dali em
diante, ele permanece critico. ``--escalada SEG`` controla quanto tempo essa subida leva (padrao: o
``escalada_s`` do cenario). ``Ctrl+C`` encerra com codigo 130, sem *traceback*.

Precedencia de configuracao (design 12.4): ``--flag`` explicita > variavel de ambiente > padrao do
cenario > padrao global. A API Key de dispositivo e lida de ``--api-key`` ou da variavel de ambiente
``BEDSIDE_API_KEY``; na ausencia das duas, cai na chave padrao do cenario (que, no
``falha-consumidor``, e ``dev-monitor-uti03``, a terceira entrada de ``API_KEYS``).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

try:  # rodado como modulo do pacote: python -m clients.bedside_monitor
    from .cenarios import Cenario, DefinicaoCenario, definicao_de, resolver_cenario
    from .simulador import ConfiguracaoSimulador, Simulador
except ImportError:  # rodado como script solto: python clients/bedside_monitor/__main__.py
    _RAIZ = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if _RAIZ not in sys.path:
        sys.path.insert(0, _RAIZ)
    from clients.bedside_monitor.cenarios import (
        Cenario,
        DefinicaoCenario,
        definicao_de,
        resolver_cenario,
    )
    from clients.bedside_monitor.simulador import ConfiguracaoSimulador, Simulador

# Padroes globais quando nem a flag, nem o ambiente, nem o cenario fixam o valor (design 12.4.2).
_GATEWAY_URL_PADRAO = "http://localhost:8000"  # fora do Compose; no Compose vira api-gateway:8000
_SEMENTE_PADRAO = 20260727  # SEMENTE_SIMULADOR (design 12.4.2)
# A tempestade usa a enfermaria: o seed (db/seed.sql) tem ENF-01..ENF-10, entao
# --leitos ate 10 gera codigos que existem de verdade.
_PREFIXO_LEITO_TEMPESTADE = "ENF"


def _construir_parser() -> argparse.ArgumentParser:
    """Monta o ``ArgumentParser`` do CLI, com ajuda derivada do catalogo de cenarios."""
    aceitos = [c.value for c in Cenario] + ["dlq"]
    parser = argparse.ArgumentParser(
        prog="python -m clients.bedside_monitor",
        description=(
            "Simulador de monitor de beira-leito (Cliente_Leito). Gera sinais vitais "
            "deterministicos por semente e os envia ao gateway com POST /sinais autenticado "
            "por X-API-Key (R4.5, R10.6)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_epilogo_cenarios(),
    )
    parser.add_argument(
        "--cenario",
        default=Cenario.ESTAVEL.value,
        metavar="NOME",
        help=f"cenario da demonstracao; um de: {', '.join(aceitos)} (padrao: estavel)",
    )
    parser.add_argument(
        "--leito",
        default=None,
        metavar="COD",
        help="leito alvo (ex.: UTI-03). Padrao: o leito do cenario. Ignorado com --leitos.",
    )
    parser.add_argument(
        "--leitos",
        type=int,
        default=None,
        metavar="N",
        help=(
            "numero de leitos publicando em paralelo (cenarios tempestade e plantao). "
            "Gera ENF-01..ENF-NN; o seed vai ate ENF-10."
        ),
    )
    parser.add_argument(
        "--intervalo",
        type=float,
        default=None,
        metavar="SEG",
        help="intervalo entre leituras, em segundos. Padrao: o do cenario ou INTERVALO_COLETA_S.",
    )
    parser.add_argument(
        "--duracao",
        type=float,
        default=None,
        metavar="SEG",
        help=(
            "publica ate esgotar esta duracao (segundos), em vez de um numero fixo de passos. "
            "Sem --duracao, o cenario plantao roda indefinidamente ate Ctrl+C."
        ),
    )
    parser.add_argument(
        "--escalada",
        type=float,
        default=None,
        metavar="SEG",
        help=(
            "cenario plantao: em quantos segundos o paciente vai de compensado a critico. "
            "Padrao: 180s (card amarelo em ~1min20, alerta/vermelho em ~2min, escore 7+ logo "
            "depois). Com --leitos N, cada leito ganha um ritmo levemente diferente da semente."
        ),
    )
    parser.add_argument(
        "--semente",
        type=int,
        default=None,
        metavar="N",
        help="semente do gerador (mesma semente, mesma sequencia). Padrao: SEMENTE_SIMULADOR.",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        metavar="CHAVE",
        help="API Key de dispositivo (X-API-Key). Padrao: BEDSIDE_API_KEY ou a chave do cenario.",
    )
    parser.add_argument(
        "--url",
        default=None,
        metavar="URL",
        help="URL base do gateway. Padrao: GATEWAY_URL ou http://localhost:8000.",
    )
    return parser


def _epilogo_cenarios() -> str:
    """Monta o rodape do ``--help`` listando cada cenario com o criterio de R10.6 que cobre."""
    linhas = ["cenarios disponiveis:"]
    for cenario in Cenario:
        definicao = definicao_de(cenario)
        marca = definicao.criterio_r10_6 or "extra (fora de R10.6)"
        if definicao.escalada_s is not None:
            marca += f"; modo continuo, escalada de {definicao.escalada_s:g}s, roda ate Ctrl+C"
        linhas.append(f"  {cenario.value:<16} leito {definicao.leito_padrao:<7} -> {marca}")
    linhas.append("  dlq              alias de fora-de-faixa")
    return "\n".join(linhas)


def _resolver_leitos(args: argparse.Namespace, cenario: Cenario) -> tuple[str, ...]:
    """Resolve a lista de leitos a partir de ``--leitos`` (tempestade) ou ``--leito`` (unico)."""
    definicao = definicao_de(cenario)
    if args.leitos is not None:
        if args.leitos < 1:
            raise ValueError("--leitos deve ser >= 1")
        return tuple(f"{_PREFIXO_LEITO_TEMPESTADE}-{i:02d}" for i in range(1, args.leitos + 1))
    leito = args.leito or definicao.leito_padrao
    return (leito,)


def _resolver_config(args: argparse.Namespace) -> ConfiguracaoSimulador:
    """Traduz os argumentos e o ambiente na :class:`ConfiguracaoSimulador` (precedencia de 12.4)."""
    cenario = resolver_cenario(args.cenario)
    definicao = definicao_de(cenario)

    base_url = args.url or os.environ.get("GATEWAY_URL") or _GATEWAY_URL_PADRAO
    api_key = args.api_key or os.environ.get("BEDSIDE_API_KEY") or definicao.api_key_padrao
    semente = args.semente if args.semente is not None else _semente_do_ambiente()
    intervalo = (
        args.intervalo
        if args.intervalo is not None
        else _intervalo_do_ambiente(definicao.intervalo_s)
    )
    leitos = _resolver_leitos(args, cenario)
    escalada = _resolver_escalada(args, definicao)

    return ConfiguracaoSimulador(
        cenario=cenario,
        base_url=base_url.rstrip("/"),
        api_key=api_key,
        leitos=leitos,
        intervalo_s=intervalo,
        passos=definicao.passos,
        duracao_s=args.duracao,
        semente=semente,
        escalada_s=escalada,
    )


def _resolver_escalada(args: argparse.Namespace, definicao: DefinicaoCenario) -> float | None:
    """Resolve o horizonte da escalada do modo continuo (``--escalada`` > padrao do cenario).

    Args:
        args: Argumentos ja analisados.
        definicao: Definicao do cenario escolhido, que traz o padrao de ``escalada_s``.

    Returns:
        O horizonte em segundos, ou ``None`` nos cenarios que nao usam o modo continuo.

    Raises:
        ValueError: Se ``--escalada`` nao for positiva -- o CLI transforma isso em erro de uso.
    """
    if args.escalada is None:
        return definicao.escalada_s
    escalada = float(args.escalada)
    if escalada <= 0:
        raise ValueError("--escalada deve ser > 0 (segundos ate o paciente ficar critico)")
    return escalada


def _semente_do_ambiente() -> int:
    """Le ``SEMENTE_SIMULADOR`` do ambiente, com fallback no padrao do design (20260727)."""
    bruto = os.environ.get("SEMENTE_SIMULADOR")
    if bruto is None:
        return _SEMENTE_PADRAO
    try:
        return int(bruto)
    except ValueError:
        return _SEMENTE_PADRAO


def _intervalo_do_ambiente(padrao_do_cenario: float) -> float:
    """Resolve o intervalo: ``INTERVALO_COLETA_S`` do ambiente ou o padrao do cenario (12.4.2)."""
    bruto = os.environ.get("INTERVALO_COLETA_S")
    if bruto is None:
        return padrao_do_cenario
    try:
        return float(bruto)
    except ValueError:
        return padrao_do_cenario


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada do CLI.

    Args:
        argv: Argumentos da linha de comando; ``None`` usa ``sys.argv``.

    Returns:
        Codigo de saida do processo: ``0`` em sucesso, ``2`` em erro de uso, ``130`` em Ctrl+C.

    O ``Ctrl+C`` chega ao :class:`Simulador` como cancelamento das tarefas de coleta -- que o
    trata, imprime as dicas de observacao e sinaliza :attr:`Simulador.interrompido`. So um segundo
    ``Ctrl+C`` (ou um sinal durante a montagem) vira ``KeyboardInterrupt`` aqui. Nos dois caminhos
    a saida e 130 e nenhum *traceback* aparece.
    """
    parser = _construir_parser()
    args = parser.parse_args(argv)
    try:
        config = _resolver_config(args)
    except ValueError as exc:
        parser.error(str(exc))

    simulador = Simulador(config)
    try:
        asyncio.run(simulador.executar())
    except KeyboardInterrupt:  # pragma: no cover - encerramento manual
        print("\nencerrado pelo operador (Ctrl+C).")
        return 130
    return 130 if simulador.interrompido else 0


if __name__ == "__main__":
    raise SystemExit(main())
