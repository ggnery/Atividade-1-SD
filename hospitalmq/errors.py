"""Hierarquia de excecoes do HospitalMQ e classificacao transitorio x permanente.

Toda excecao levantada pelo middleware desce de :class:`HospitalMQError`. A hierarquia existe
por um motivo operacional muito concreto (secao 3.6.1 do design): **a decisao de retentar e do
middleware, e a classificacao do erro e do handler**. O handler nao chama `retry`, nao publica na
DLQ e nao conta tentativas -- ele apenas escolhe entre levantar :class:`TransientError` (vale a pena
tentar de novo) e :class:`PermanentError` (tentar de novo produziria exatamente a mesma falha).

Tabela de politica aplicada por ``retry.py`` (secao 3.6.1):

===========================  ===========  ====================================================
Excecao                      Retentativas Destino final
===========================  ===========  ====================================================
:class:`TransientError`      ate 3        espera 1s/2s/4s e DLQ ao esgotar (R2.2, R2.3)
:class:`PermanentError`      nenhuma      DLQ imediata (R2.3, R6.6)
:class:`AuthError`           nenhuma      DLQ imediata, log em nivel ``warning`` (R4.4)
:class:`TransportError`      nenhuma      propagada ao produtor em ate 5 s (R1.5)
:class:`RpcTimeoutError`     nenhuma      future descartado, ``504`` no gateway (R3.3, R8.4)
:class:`RpcRemoteError`      nenhuma      codigo propagado via :data:`MAPA_HTTP` (R3.4)
Excecao nao prevista         nenhuma      tratada como transitoria pelo consumidor (secao 4.6.1)
===========================  ===========  ====================================================

Requisitos atendidos: R1.5, R2.2, R2.3, R3.3, R3.4, R8.2.
"""

from __future__ import annotations

from typing import Final

__all__ = [
    "MAPA_HTTP",
    "AuthError",
    "ConfigError",
    "HospitalMQError",
    "InvalidEnvelopeError",
    "PermanentError",
    "RpcRemoteError",
    "RpcTimeoutError",
    "TransientError",
    "TransportError",
]


class HospitalMQError(Exception):
    """Raiz da hierarquia de erros do middleware.

    Nenhuma excecao de biblioteca de terceiros (``aio_pika``, ``aiormq``, ``botocore``,
    ``asyncpg``) deve atravessar a fronteira do HospitalMQ: o driver traduz para uma subclasse
    desta (secao 4.3.2, invariante 3). E isso que impede o acoplamento de vazar pela porta dos
    fundos das excecoes.

    Attributes:
        mensagem: Texto legivel da falha. E o que vai para o cabecalho ``x-hmq-dlq-motivo``
            quando a mensagem e descartada (secao 6.2.6), ja mascarado por R5.6.
        codigo: Codigo estavel do vocabulario de erro da secao 5.7. E ele -- e nao o nome da
            classe -- que o gateway traduz para status HTTP por :data:`MAPA_HTTP`.
        retentavel: Se o middleware deve tentar de novo. Padrao de classe; pode ser
            sobrescrito por instancia.
        detalhes: Pares chave/valor adicionais, anexados ao log estruturado (R5.1).
    """

    codigo_padrao: str = "ERRO_INTERNO"
    retentavel_padrao: bool = False

    def __init__(
        self,
        mensagem: str = "",
        *,
        codigo: str | None = None,
        retentavel: bool | None = None,
        **detalhes: object,
    ) -> None:
        """Constroi o erro.

        Args:
            mensagem: Descricao legivel da falha.
            codigo: Codigo do vocabulario da secao 5.7. Se ausente, usa o padrao da classe.
            retentavel: Sobrepoe a classificacao padrao da classe. Uso excepcional.
            **detalhes: Contexto adicional (por exemplo ``leito_id``, ``papel``, ``motivo``),
                exposto em :attr:`detalhes` e anexado a linha de log.
        """
        self.mensagem: str = mensagem
        self.codigo: str = codigo if codigo is not None else self.codigo_padrao
        self.retentavel: bool = retentavel if retentavel is not None else self.retentavel_padrao
        self.detalhes: dict[str, object] = dict(detalhes)
        super().__init__(mensagem or self.codigo)

    def __str__(self) -> str:
        """Devolve a representacao curta usada em log e no cabecalho ``x-hmq-dlq-motivo``."""
        base = self.mensagem or self.codigo
        if not self.detalhes:
            return base
        extras = " ".join(f"{chave}={valor!r}" for chave, valor in sorted(self.detalhes.items()))
        return f"{base} [{extras}]"

    def __repr__(self) -> str:
        """Devolve a representacao de depuracao com classe, codigo e mensagem."""
        return f"{type(self).__name__}(codigo={self.codigo!r}, mensagem={self.mensagem!r})"

    def para_log(self) -> dict[str, object]:
        """Projeta o erro nos campos usados pelo log estruturado (R5.1).

        Returns:
            Dicionario com ``erro_tipo``, ``erro_codigo``, ``erro_mensagem``, ``retentavel``
            e os pares de :attr:`detalhes`.
        """
        return {
            "erro_tipo": type(self).__name__,
            "erro_codigo": self.codigo,
            "erro_mensagem": self.mensagem,
            "retentavel": self.retentavel,
            **self.detalhes,
        }


class TransportError(HospitalMQError):
    """Falha de infraestrutura de mensageria.

    Levantada quando o Broker esta indisponivel, quando a publicacao nao e confirmada dentro do
    prazo de durabilidade, quando a mensagem e recusada como nao roteavel (``basic.return``) ou
    quando o canal cai. O contrato de R1.5 e explicito: **em no maximo 5 segundos** o produtor
    recebe este erro, nunca um descarte silencioso.

    Nao e retentavel pelo middleware: quem publicou decide o que fazer (o gateway degrada para
    ``503`` e mantem ``/health`` respondendo, conforme R8.5 e R8.6).
    """

    codigo_padrao = "BROKER_INDISPONIVEL"
    retentavel_padrao = False


class ConfigError(HospitalMQError):
    """Configuracao invalida ou topologia divergente -- falha fatal de inicializacao.

    Casos previstos: valor de ``TRANSPORTE`` desconhecido (secao 4.3.4) e declaracao de fila ou
    exchange com argumentos diferentes dos que ja existem no broker, o ``406 PRECONDITION_FAILED``
    da secao 6.11.2. Em ambos, o processo deve encerrar com codigo 78 (``EX_CONFIG``) em vez de
    seguir operando contra uma topologia errada.
    """

    codigo_padrao = "CONFIGURACAO_INVALIDA"
    retentavel_padrao = False


class TransientError(HospitalMQError):
    """Falha que provavelmente desaparece sozinha -- o middleware deve tentar de novo.

    Criterio: o erro decorre do **estado momentaneo** de uma dependencia, e a mesma mensagem,
    reapresentada mais tarde, tem chance real de sucesso. Exemplos do projeto: canal de
    notificacao da equipe fora do ar (R6.5), banco de dados recusando conexao, deadlock, timeout
    de rede para um servico externo.

    Consequencia: ``retry.py`` republica o Envelope com ``attempt + 1`` na fila de espera do nivel
    correspondente (1 s, 2 s, 4 s) e, esgotadas as tres tentativas, envia a DLQ com o motivo
    registrado no cabecalho ``x-hmq-dlq-motivo`` (R2.2, R2.3).
    """

    codigo_padrao = "FALHA_TRANSITORIA"
    retentavel_padrao = True


class PermanentError(HospitalMQError):
    """Falha determinista -- retentar produziria exatamente o mesmo resultado.

    Criterio: o defeito esta **no dado ou no contrato**, nao no ambiente. Reapresentar a mesma
    mensagem daqui a uma hora falharia de forma identica. Exemplos do projeto: leitura de sinais
    vitais fora da faixa fisiologica (R6.6), ``version`` de evento sem handler registrado
    (secao 4.2.4), payload que nao valida no modelo Pydantic do handler, paciente inexistente.

    Consequencia: DLQ **imediata**, sem consumir nenhuma das tres retentativas de R2.2. Essa
    distincao e o que torna a triagem da DLQ uma decisao de dez segundos: mensagem marcada com
    ``PermanentError`` volta ao autor do dado; mensagem com ``TransientError`` esgotado volta a
    fila por *redrive* depois que a dependencia se recuperar (secao 6.2.6).
    """

    codigo_padrao = "ERRO_PERMANENTE"
    retentavel_padrao = False


class InvalidEnvelopeError(PermanentError, ValueError):
    """Corpo recebido nao e um Envelope valido (secao 4.2).

    Herda de :class:`PermanentError` -- envelope malformado nunca melhora com retentativa -- e
    tambem de :class:`ValueError`, para que ``Envelope.from_bytes`` continue satisfazendo o
    contrato ``except ValueError`` usado pelo consumidor da fila de retorno do RPC (secao 5.4).
    """

    codigo_padrao = "ENVELOPE_INVALIDO"
    retentavel_padrao = False


class AuthError(HospitalMQError):
    """Credencial ausente, invalida ou insuficiente para a operacao (R4.1, R4.2, R4.4, R4.5).

    Nao e retentavel: reapresentar a mesma credencial produz a mesma recusa. Na borda HTTP vira
    ``401`` (ausente, expirada, assinatura invalida, chave desconhecida) ou ``403``
    (``motivo="papel_insuficiente"``); no caminho assincrono vai direto a DLQ com log em nivel
    ``warning`` e gera o evento ``acesso.negado`` da trilha de auditoria (R4.4, R7.1).

    Attributes:
        motivo: Causa canonica da recusa -- ``credencial_ausente``, ``expirado``,
            ``assinatura_invalida``, ``chave_desconhecida`` ou ``papel_insuficiente``.
    """

    codigo_padrao = "NAO_AUTORIZADO"
    retentavel_padrao = False

    def __init__(
        self,
        mensagem: str = "",
        *,
        motivo: str = "nao_autorizado",
        codigo: str | None = None,
        **detalhes: object,
    ) -> None:
        """Constroi o erro de autenticacao ou autorizacao.

        Args:
            mensagem: Descricao legivel. Se omitida, e derivada do ``motivo``.
            motivo: Causa canonica da recusa (ver :attr:`motivo`).
            codigo: Codigo do vocabulario da secao 5.7. Padrao ``NAO_AUTORIZADO``.
            **detalhes: Contexto adicional, por exemplo ``papel`` e ``exigidos``.
        """
        self.motivo: str = motivo
        super().__init__(mensagem or f"acesso negado: {motivo}", codigo=codigo)
        self.detalhes.update({"motivo": motivo, **detalhes})


class RpcTimeoutError(HospitalMQError):
    """A resposta da chamada RPC nao chegou dentro do prazo (R3.3).

    O ``RpcClient`` remove o *future* pendente e descarta qualquer resposta tardia. No gateway
    vira ``504 Gateway Timeout`` (R8.4). Para operacoes de escrita o resultado e **indeterminado**:
    o texto devolvido ao cliente orienta a reapresentacao com o mesmo ``X-Message-ID``, que a
    idempotencia do servidor reconhece (secao 5.5.5).

    Attributes:
        operacao: Nome da operacao RPC que nao respondeu, ex. ``"paciente.admitir"``.
        timeout: Prazo em segundos que foi excedido.
        correlation_id: Correlacao de ponta a ponta da chamada.
        message_id: ``message_id`` da requisicao; e ele que o cliente reapresenta.
        escrita: ``True`` se a operacao altera estado -- muda o texto do ``504``.
    """

    codigo_padrao = "RPC_TIMEOUT"
    retentavel_padrao = False

    def __init__(
        self,
        mensagem: str = "",
        *,
        operacao: str,
        timeout: float,
        correlation_id: str | None = None,
        message_id: str | None = None,
        escrita: bool = False,
        **detalhes: object,
    ) -> None:
        """Constroi o erro de timeout de RPC.

        Args:
            mensagem: Descricao legivel. Se omitida, e montada a partir de operacao e timeout.
            operacao: Nome da operacao RPC chamada.
            timeout: Prazo em segundos excedido.
            correlation_id: Correlacao de ponta a ponta da chamada.
            message_id: ``message_id`` da requisicao, reapresentavel pelo cliente.
            escrita: ``True`` quando a operacao altera estado.
            **detalhes: Contexto adicional para o log.
        """
        self.operacao: str = operacao
        self.timeout: float = timeout
        self.correlation_id: str | None = correlation_id
        self.message_id: str | None = message_id
        self.escrita: bool = escrita
        super().__init__(mensagem or f"operacao {operacao} nao respondeu em {timeout}s")
        self.detalhes.update(
            {"operacao": operacao, "timeout": timeout, "escrita": escrita, **detalhes}
        )


class RpcRemoteError(HospitalMQError):
    """O servidor RPC respondeu, e a resposta e um erro tipado (R3.4).

    E o oposto de :class:`RpcTimeoutError`: houve resposta, e ela chegou em milissegundos. O
    servidor serializa ``{"status": "erro", "erro": {...}}`` e o ``RpcClient`` reconstroi esta
    excecao no chamador, preservando o codigo. O gateway traduz o codigo -- **nunca a classe** --
    para status HTTP por :data:`MAPA_HTTP`.

    Attributes:
        codigo: Codigo remoto do vocabulario da secao 5.7, ex. ``"PACIENTE_NAO_ENCONTRADO"``.
        mensagem: Texto devolvido pelo servidor, ja mascarado (R5.6).
        operacao: Operacao RPC que falhou.
        retentavel: Sinalizado pelo proprio servidor; ``True`` apenas em ``BANCO_INDISPONIVEL``.
    """

    codigo_padrao = "ERRO_INTERNO"
    retentavel_padrao = False

    def __init__(
        self,
        mensagem: str = "",
        *,
        codigo: str = "ERRO_INTERNO",
        operacao: str = "desconhecida",
        retentavel: bool = False,
        **detalhes: object,
    ) -> None:
        """Constroi o erro remoto recebido do ``RpcServer``.

        Args:
            mensagem: Texto do erro devolvido pelo servidor.
            codigo: Codigo remoto do vocabulario da secao 5.7.
            operacao: Nome da operacao RPC chamada.
            retentavel: Classificacao informada pelo proprio servidor.
            **detalhes: Contexto adicional para o log.
        """
        self.operacao: str = operacao
        super().__init__(mensagem or codigo, codigo=codigo, retentavel=retentavel)
        self.detalhes.update({"operacao": operacao, **detalhes})


MAPA_HTTP: Final[dict[str, int]] = {
    "PACIENTE_NAO_ENCONTRADO": 404,
    "INTERNACAO_NAO_ENCONTRADA": 404,
    "LEITO_NAO_ENCONTRADO": 404,
    "PAYLOAD_INVALIDO": 422,
    "DOCUMENTO_DUPLICADO": 409,
    "LEITO_OCUPADO": 409,
    "PACIENTE_JA_INTERNADO": 409,
    "INTERNACAO_JA_ENCERRADA": 409,
    "NAO_AUTORIZADO": 403,
    "OPERACAO_DESCONHECIDA": 501,
    "BANCO_INDISPONIVEL": 503,
    "ENVELOPE_INVALIDO": 500,
    "ERRO_INTERNO": 500,
}
"""Fonte unica do vocabulario de erro remoto -> status HTTP (secao 5.7).

Consultada **por codigo**, nunca por classe de excecao: um codigo desconhecido cai no ``500``
padrao do ``dict.get``, de modo que nenhuma falha remota jamais vaza como ``200``.
"""
