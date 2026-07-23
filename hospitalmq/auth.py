"""Autenticacao de borda e propagacao de identidade ate o consumidor (R4.1--R4.6).

Dois mecanismos, dois tipos de portador (secao 8.3.1 do design):

======================  ==================================  ==================================
Aspecto                 JWT HS256 -- pessoas                API Key -- dispositivos
======================  ==================================  ==================================
Cabecalho HTTP          ``Authorization: Bearer <jwt>``     ``X-API-Key: <chave>``
Papeis                  ``enfermeiro``, ``medico``,         ``dispositivo``
                        ``auditor``, ``admin``
Validade                ``exp`` de 30 min (configuravel)    longeva, sem expiracao automatica
Estado no servidor      nenhum -- token autocontido         mapa ``chave -> sub`` da configuracao
``Identity`` gerada     ``tipo="usuario"``                  ``tipo="dispositivo"``
======================  ==================================  ==================================

**Por que dois e nao um so (R4.5).** O ciclo de vida das credenciais e diferente. Um monitor de
beira-leito, que so sabe fazer um ``POST`` a cada cinco segundos, pararia de publicar no meio do
plantao quando um token de 30 min expirasse -- uma falha de seguranca convertida em falha clinica.
Uma chave longeva, por sua vez, seria pessima para humanos: sem ``exp``, uma credencial vazada vale
para sempre. Separar os mecanismos deixa explicito, no proprio cabecalho HTTP, se a origem do dado
e maquina ou pessoa -- informacao que segue ate a trilha de auditoria no campo ``identity.tipo`` do
Envelope.

**O que este modulo nao faz.** Nao importa FastAPI, Starlette nem qualquer framework web: ele e
parte do middleware e roda igual dentro do ``api-gateway``, de um script de linha de comando ou de
um teste sem servidor HTTP. As dependencias de rota (``autenticar_usuario``, ``exigir_papel``) vivem
em ``services/api_gateway/dependencias.py`` e apenas **chamam** as funcoes daqui, traduzindo
:class:`~hospitalmq.errors.AuthError` em ``401``/``403`` no formato RFC 7807 (secao 8.4.1).

**Fronteira de confianca (secao 8.3.3).** A autenticacao acontece **uma unica vez**, na borda; a
identidade validada e copiada para ``Envelope.identity`` e viaja com a mensagem. O consumidor le
``ctx.identity`` sem nova consulta ao servico de autenticacao, que e literalmente o texto de R4.6.
A limitacao e reconhecida e esta documentada: nao ha integridade criptografica do Envelope, entao a
confianca e transitiva -- o ponto de extensao para assinar o Envelope fica no ``Publisher`` e no
``Consumer``, sem tocar neste modulo.

**Relogio injetado (secao 10.3.1).** ``exp`` e ``iat`` sao conferidos contra o :class:`Clock`
recebido no construtor, e **nao** contra o relogio interno do ``PyJWT``. E isso que permite ao teste
de R4.2 fazer ``clock.avancar(901)`` e observar o ``401`` de token expirado sem esperar tempo real.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from typing import Any, Final, Literal, get_args

import jwt

from hospitalmq.clock import Clock, RealClock
from hospitalmq.config import Settings, settings
from hospitalmq.envelope import Envelope, Identity, TipoIdentidade
from hospitalmq.errors import AuthError, ConfigError

__all__ = [
    "ALGORITMO",
    "ALGORITMOS_ACEITOS",
    "CABECALHO_API_KEY",
    "CABECALHO_AUTORIZACAO",
    "CLAIMS_OBRIGATORIOS",
    "ESQUEMA_BEARER",
    "MOTIVOS",
    "MOTIVO_ASSINATURA_INVALIDA",
    "MOTIVO_CHAVE_DESCONHECIDA",
    "MOTIVO_CREDENCIAL_AUSENTE",
    "MOTIVO_EXPIRADO",
    "MOTIVO_PAPEL_INSUFICIENTE",
    "PAPEIS",
    "PAPEIS_DE_USUARIO",
    "PAPEL_DISPOSITIVO",
    "TOLERANCIA_IAT_S",
    "Autenticador",
    "Identity",
    "Papel",
    "TipoIdentidade",
    "autenticador_padrao",
    "definir_autenticador_padrao",
    "emitir_token",
    "extrair_token_bearer",
    "identidade_de_dict",
    "identidade_do_envelope",
    "projetar_identidade",
    "validar_api_key",
    "validar_token",
    "verificar_papel",
    "verificar_tipo",
]

# --------------------------------------------------------------------------- #
# Vocabulario fechado de papeis e de motivos de recusa                         #
# --------------------------------------------------------------------------- #

Papel = Literal["enfermeiro", "medico", "auditor", "dispositivo", "admin"]
"""Dominio **fechado** de ``Identity.role`` (secao 8.3.1).

Cinco valores, nem um a mais. Em particular **nao existe** o papel ``servico``: a matriz de
autorizacao da secao 8.3.2 nao tem papel onipotente, e um token assinado com papel fora deste
conjunto e tratado como forjado por :meth:`Autenticador.validar_token`.
"""

PAPEIS: Final[frozenset[str]] = frozenset(get_args(Papel))
"""Conjunto de todos os papeis aceitos, para verificacao em tempo de execucao."""

PAPEL_DISPOSITIVO: Final[str] = "dispositivo"
"""Papel fixo atribuido a toda credencial de dispositivo (R4.5)."""

PAPEIS_DE_USUARIO: Final[frozenset[str]] = PAPEIS - {PAPEL_DISPOSITIVO}
"""Papeis que o emissor de JWT pode assinar.

``dispositivo`` esta de fora **por decisao explicita**: R4.5 exige que a credencial de maquina seja
*distinta* da credencial humana, e emitir um JWT com papel de dispositivo destruiria essa distincao
-- a `Identity` resultante teria ``tipo="usuario"`` e ``role="dispositivo"``, um hibrido que nenhum
mecanismo de autenticacao deveria produzir.
"""

MOTIVO_CREDENCIAL_AUSENTE: Final[str] = "credencial_ausente"
"""Nenhuma credencial apresentada em rota protegida -- vira ``401`` / ``sem-credencial`` (R4.1)."""

MOTIVO_EXPIRADO: Final[str] = "expirado"
"""``exp`` no passado -- vira ``401`` / ``credencial-invalida`` (R4.2)."""

MOTIVO_ASSINATURA_INVALIDA: Final[str] = "assinatura_invalida"
"""Token adulterado, malformado, com algoritmo errado ou com *claim* fora do contrato (R4.2)."""

MOTIVO_CHAVE_DESCONHECIDA: Final[str] = "chave_desconhecida"
"""API Key que nao consta da lista configurada -- vira ``401`` (R4.5)."""

MOTIVO_PAPEL_INSUFICIENTE: Final[str] = "papel_insuficiente"
"""Credencial valida, porem sem a permissao exigida -- vira ``403`` e auditoria (R4.4)."""

MOTIVOS: Final[frozenset[str]] = frozenset(
    {
        MOTIVO_CREDENCIAL_AUSENTE,
        MOTIVO_EXPIRADO,
        MOTIVO_ASSINATURA_INVALIDA,
        MOTIVO_CHAVE_DESCONHECIDA,
        MOTIVO_PAPEL_INSUFICIENTE,
    }
)
"""Dominio fechado de ``AuthError.motivo``, consumido pelo mapa de erro da secao 8.4.1."""

# --------------------------------------------------------------------------- #
# Parametros do JWT                                                            #
# --------------------------------------------------------------------------- #

ALGORITMO: Final[str] = "HS256"
"""Algoritmo unico de assinatura (secao 8.3.1).

HS256 e nao RS256 porque existe um unico emissor e um unico verificador -- o proprio gateway. Chave
assimetrica so se paga quando terceiros precisam validar sem poder emitir. A troca e uma constante:
este modulo e o unico lugar do projeto que nomeia o algoritmo.
"""

ALGORITMOS_ACEITOS: Final[tuple[str, ...]] = (ALGORITMO,)
"""Lista branca passada ao ``PyJWT``. E ela que fecha o ataque classico de ``alg: none``."""

CLAIMS_OBRIGATORIOS: Final[tuple[str, ...]] = ("sub", "role", "exp", "iat")
"""*Claims* exigidos em todo token (secao 8.3.1).

``exp`` e obrigatorio por omissao proposital do padrao: sem esta exigencia, um token sem ``exp``
seria eterno e passaria na verificacao sem que ninguem percebesse.
"""

TOLERANCIA_IAT_S: Final[int] = 30
"""Folga aceita para ``iat`` no futuro, em segundos, cobrindo desvio de relogio entre processos."""

CABECALHO_AUTORIZACAO: Final[str] = "Authorization"
"""Cabecalho HTTP que transporta o JWT de pessoa."""

CABECALHO_API_KEY: Final[str] = "X-API-Key"
"""Cabecalho HTTP que transporta a chave de dispositivo (R4.5)."""

ESQUEMA_BEARER: Final[str] = "Bearer"
"""Esquema de autorizacao esperado em :data:`CABECALHO_AUTORIZACAO`."""

_TIPO_USUARIO: Final[TipoIdentidade] = "usuario"
_TIPO_DISPOSITIVO: Final[TipoIdentidade] = "dispositivo"


# --------------------------------------------------------------------------- #
# Auxiliares internos                                                          #
# --------------------------------------------------------------------------- #


def _digest(chave: str) -> str:
    """Devolve o SHA-256 hexadecimal de uma credencial, em ASCII."""
    return hashlib.sha256(chave.encode("utf-8")).hexdigest()


def _texto_nao_vazio(valor: object) -> str | None:
    """Devolve o texto quando ``valor`` e uma string nao vazia; ``None`` caso contrario."""
    if isinstance(valor, str) and valor.strip():
        return valor
    return None


def _claim_numerico(claims: Mapping[str, Any], nome: str) -> int:
    """Le um *claim* de tempo (``exp``, ``iat``) como inteiro de segundos desde a epoca."""
    bruto = claims.get(nome)
    if isinstance(bruto, bool) or not isinstance(bruto, int | float):
        raise AuthError(
            f"claim {nome!r} do token nao e numerico",
            motivo=MOTIVO_ASSINATURA_INVALIDA,
            causa="claim_invalido",
            claim=nome,
        )
    return int(bruto)


# --------------------------------------------------------------------------- #
# Autenticador                                                                 #
# --------------------------------------------------------------------------- #


class Autenticador:
    """Emissor e verificador de credenciais da borda (R4.1, R4.2, R4.5).

    Concentra tudo o que depende de segredo, de relogio e de configuracao, de modo que as funcoes
    de modulo (:func:`emitir_token`, :func:`validar_token`, :func:`validar_api_key`) sejam apenas
    a fachada conveniente sobre uma instancia padrao. Testes e o *startup* do gateway constroem a
    sua propria instancia com :class:`~hospitalmq.clock.ManualClock` ou com segredo de teste, sem
    tocar em variavel de ambiente.

    As chaves de dispositivo sao guardadas **apenas como SHA-256**: o valor em claro existe na
    configuracao, nunca no estado do objeto, e a comparacao usa ``hmac.compare_digest`` sobre os
    digests, sem saida antecipada do laco -- o custo da verificacao nao varia com o quanto o
    prefixo da chave apresentada acerta (secao 8.3.1).
    """

    __slots__ = ("_algoritmo", "_clock", "_digest_para_sub", "_segredo", "_ttl_padrao_s")

    def __init__(
        self,
        *,
        segredo: str | None = None,
        expiracao_min: int | None = None,
        api_keys: Mapping[str, str] | None = None,
        clock: Clock | None = None,
        algoritmo: str = ALGORITMO,
        cfg: Settings | None = None,
    ) -> None:
        """Constroi o autenticador.

        Args:
            segredo: Segredo HS256. Se omitido, usa ``JWT_SECRET`` da configuracao.
            expiracao_min: Validade padrao do token, em minutos. Se omitido, usa
                ``JWT_EXPIRACAO_MIN`` (padrao 30, o que da os 1800 s da secao 8.3.1).
            api_keys: Mapa ``chave -> sub`` das credenciais de dispositivo. Se omitido, usa
                ``API_KEYS`` da configuracao, ja interpretado por ``Settings.api_keys_pares``.
            clock: Fonte de tempo para ``iat``, ``exp`` e para a verificacao de expiracao.
                Padrao :class:`~hospitalmq.clock.RealClock`.
            algoritmo: Algoritmo de assinatura. Padrao e unico valor suportado: ``HS256``.
            cfg: Configuracao de onde tirar os padroes. Padrao: a instancia global de
                ``hospitalmq.config``.

        Raises:
            ConfigError: Se o algoritmo pedido nao for HS256 ou se o segredo for vazio.
        """
        base = cfg if cfg is not None else settings
        if algoritmo not in ALGORITMOS_ACEITOS:
            raise ConfigError(
                f"algoritmo de JWT nao suportado: {algoritmo!r}",
                aceitos=list(ALGORITMOS_ACEITOS),
            )
        segredo_efetivo = segredo if segredo is not None else base.jwt_secret
        if not segredo_efetivo:
            raise ConfigError("JWT_SECRET nao pode ser vazio", variavel="JWT_SECRET")

        minutos = expiracao_min if expiracao_min is not None else base.jwt_expiracao_min
        if minutos <= 0:
            raise ConfigError(
                f"JWT_EXPIRACAO_MIN deve ser positivo, recebido {minutos}",
                variavel="JWT_EXPIRACAO_MIN",
            )

        pares = dict(api_keys) if api_keys is not None else base.api_keys_pares()

        self._segredo: str = segredo_efetivo
        self._algoritmo: str = algoritmo
        self._ttl_padrao_s: int = minutos * 60
        self._clock: Clock = clock if clock is not None else RealClock()
        self._digest_para_sub: dict[str, str] = {
            _digest(chave): sub for chave, sub in pares.items() if chave and sub
        }

    # -- introspecao -------------------------------------------------------- #

    @property
    def ttl_padrao_s(self) -> int:
        """Validade padrao do token emitido, em segundos.

        Returns:
            ``JWT_EXPIRACAO_MIN * 60`` -- 1800 s com a configuracao padrao (secao 8.3.1).
        """
        return self._ttl_padrao_s

    @property
    def total_de_chaves(self) -> int:
        """Quantidade de credenciais de dispositivo carregadas.

        Returns:
            Numero de entradas validas de ``API_KEYS``. Serve ao ``/health`` e ao log de
            inicializacao; o valor em claro das chaves nunca e exposto.
        """
        return len(self._digest_para_sub)

    # -- JWT: emissao ------------------------------------------------------- #

    def emitir_token(self, sub: str, role: Papel, ttl_segundos: int | None = None) -> str:
        """Emite um JWT HS256 para uma pessoa (R4.1).

        O token carrega exatamente os quatro *claims* de :data:`CLAIMS_OBRIGATORIOS`. Nao ha
        ``jti`` nem lista de revogacao nesta entrega: a janela de exposicao de um token vazado e
        limitada pelo ``exp`` curto, decisao registrada na tabela de riscos da secao 8.3.3.

        Args:
            sub: Identificacao do usuario, ex. ``"enf.ana"``. Vira ``Identity.sub``.
            role: Papel do usuario, obrigatoriamente um de :data:`PAPEIS_DE_USUARIO`.
            ttl_segundos: Validade em segundos. Se omitido, usa :attr:`ttl_padrao_s`.

        Returns:
            O token compacto (``header.payload.signature``), pronto para
            ``Authorization: Bearer``.

        Raises:
            ConfigError: Se ``sub`` for vazio, se ``role`` nao for um papel de usuario ou se
                ``ttl_segundos`` nao for positivo. Sao erros de programacao ou de cadastro do
                emissor, nunca do portador da credencial -- por isso nao sao ``AuthError``.
        """
        identificador = _texto_nao_vazio(sub)
        if identificador is None:
            raise ConfigError("sub do token nao pode ser vazio", campo="sub")
        if role not in PAPEIS_DE_USUARIO:
            raise ConfigError(
                f"papel nao emissivel por JWT: {role!r}",
                campo="role",
                aceitos=sorted(PAPEIS_DE_USUARIO),
            )
        ttl = self._ttl_padrao_s if ttl_segundos is None else int(ttl_segundos)
        if ttl <= 0:
            raise ConfigError(f"ttl_segundos deve ser positivo, recebido {ttl}", campo="ttl")

        emitido_em = int(self._clock.now().timestamp())
        claims: dict[str, Any] = {
            "sub": identificador,
            "role": role,
            "iat": emitido_em,
            "exp": emitido_em + ttl,
        }
        return jwt.encode(claims, self._segredo, algorithm=self._algoritmo)

    # -- JWT: validacao ----------------------------------------------------- #

    def validar_token(self, token: str | None) -> Identity:
        """Valida um JWT e projeta a identidade correspondente (R4.2, R4.3).

        A verificacao e deliberadamente mais estrita que o padrao da biblioteca:

        1. **Algoritmo em lista branca.** ``algorithms=["HS256"]`` recusa ``alg: none`` e qualquer
           tentativa de rebaixamento -- e a checagem explicita pedida pela secao 8.3.1.
        2. **``exp`` e ``iat`` obrigatorios.** Um token sem ``exp`` seria eterno por omissao.
        3. **Expiracao conferida contra o :class:`Clock` injetado**, e nao contra o relogio
           interno do ``PyJWT``, para que o teste de R4.2 possa avancar o tempo.
        4. **``iat`` no futuro** alem de :data:`TOLERANCIA_IAT_S` invalida o token.
        5. **``role`` no dominio fechado** de :data:`Papel`. Um papel desconhecido -- ``servico``,
           por exemplo -- so poderia vir de token forjado com o segredo vazado, e e recusado.

        Args:
            token: O token compacto, ja **sem** o prefixo ``Bearer``.

        Returns:
            A identidade da pessoa, sempre com ``tipo="usuario"``.

        Raises:
            AuthError: Com ``motivo=credencial_ausente`` quando nao ha token;
                ``motivo=expirado`` quando ``exp`` ja passou (mensagem exata
                ``"token expirado"``, exigida por R4.2); ``motivo=assinatura_invalida`` nos demais
                casos, com a causa precisa em ``detalhes["causa"]``.
        """
        bruto = _texto_nao_vazio(token)
        if bruto is None:
            raise AuthError(
                "credencial ausente", motivo=MOTIVO_CREDENCIAL_AUSENTE, esquema=ESQUEMA_BEARER
            )

        claims = self._decodificar(bruto.strip())
        agora = self._clock.now().timestamp()

        expira_em = _claim_numerico(claims, "exp")
        if expira_em < agora:
            raise AuthError(
                "token expirado",
                motivo=MOTIVO_EXPIRADO,
                exp=expira_em,
                agora=int(agora),
            )

        emitido_em = _claim_numerico(claims, "iat")
        if emitido_em > agora + TOLERANCIA_IAT_S:
            raise AuthError(
                "token emitido no futuro",
                motivo=MOTIVO_ASSINATURA_INVALIDA,
                causa="iat_no_futuro",
                iat=emitido_em,
                agora=int(agora),
            )

        identificador = _texto_nao_vazio(claims.get("sub"))
        if identificador is None:
            raise AuthError(
                "token sem sub utilizavel",
                motivo=MOTIVO_ASSINATURA_INVALIDA,
                causa="sub_invalido",
            )

        papel = _texto_nao_vazio(claims.get("role"))
        if papel is None or papel not in PAPEIS:
            raise AuthError(
                "token com papel fora do dominio conhecido",
                motivo=MOTIVO_ASSINATURA_INVALIDA,
                causa="papel_desconhecido",
                papel=papel,
                identity_sub=identificador,
            )

        return Identity(sub=identificador, role=papel, tipo=_TIPO_USUARIO)

    def _decodificar(self, token: str) -> dict[str, Any]:
        """Decodifica e verifica a assinatura, traduzindo toda falha do ``PyJWT`` em AuthError."""
        try:
            return jwt.decode(
                token,
                self._segredo,
                algorithms=list(ALGORITMOS_ACEITOS),
                options={
                    "require": list(CLAIMS_OBRIGATORIOS),
                    # a expiracao e conferida aqui, contra o Clock injetado (secao 10.3.1)
                    "verify_exp": False,
                    "verify_iat": False,
                },
            )
        except jwt.ExpiredSignatureError as exc:  # pragma: no cover - verify_exp desligado
            raise AuthError("token expirado", motivo=MOTIVO_EXPIRADO) from exc
        except jwt.MissingRequiredClaimError as exc:
            raise AuthError(
                f"token sem o claim obrigatorio {exc.claim!r}",
                motivo=MOTIVO_ASSINATURA_INVALIDA,
                causa="claim_ausente",
                claim=exc.claim,
            ) from exc
        except jwt.InvalidAlgorithmError as exc:
            raise AuthError(
                f"algoritmo do token nao e {ALGORITMO}",
                motivo=MOTIVO_ASSINATURA_INVALIDA,
                causa="algoritmo_invalido",
            ) from exc
        except jwt.InvalidSignatureError as exc:
            raise AuthError(
                "assinatura do token e invalida",
                motivo=MOTIVO_ASSINATURA_INVALIDA,
                causa="assinatura",
            ) from exc
        except jwt.InvalidTokenError as exc:
            raise AuthError(
                "token malformado ou ilegivel",
                motivo=MOTIVO_ASSINATURA_INVALIDA,
                causa="malformado",
            ) from exc

    # -- API Key de dispositivo --------------------------------------------- #

    def validar_api_key(self, chave: str | None) -> Identity:
        """Valida a chave de um dispositivo e projeta a identidade correspondente (R4.5).

        A chave apresentada e reduzida a SHA-256 e comparada com ``hmac.compare_digest`` contra os
        digests conhecidos. O laco percorre **todas** as entradas mesmo depois de encontrar a
        correspondencia: sair mais cedo faria o tempo de resposta revelar quantos digests foram
        testados antes do acerto.

        Args:
            chave: Valor do cabecalho ``X-API-Key``.

        Returns:
            A identidade do dispositivo, sempre com ``role="dispositivo"`` e
            ``tipo="dispositivo"``.

        Raises:
            AuthError: ``motivo=credencial_ausente`` se nao houver chave;
                ``motivo=chave_desconhecida`` se a chave nao constar de ``API_KEYS``.
        """
        bruta = _texto_nao_vazio(chave)
        if bruta is None:
            raise AuthError(
                "credencial ausente",
                motivo=MOTIVO_CREDENCIAL_AUSENTE,
                cabecalho=CABECALHO_API_KEY,
            )

        apresentado = _digest(bruta.strip())
        encontrado: str | None = None
        for conhecido, sub in self._digest_para_sub.items():
            if hmac.compare_digest(apresentado, conhecido):
                encontrado = sub
        if encontrado is None:
            raise AuthError(
                "chave de dispositivo desconhecida",
                motivo=MOTIVO_CHAVE_DESCONHECIDA,
                cabecalho=CABECALHO_API_KEY,
            )
        return Identity(sub=encontrado, role=PAPEL_DISPOSITIVO, tipo=_TIPO_DISPOSITIVO)

    # -- conveniencia para a borda HTTP ------------------------------------- #

    def identidade_de_cabecalhos(
        self,
        *,
        authorization: str | None = None,
        api_key: str | None = None,
    ) -> Identity:
        """Resolve a identidade a partir dos cabecalhos brutos de uma requisicao.

        Nao depende de FastAPI: recebe os dois valores ja extraidos pelo chamador. O JWT tem
        precedencia sobre a API Key quando ambos vem na mesma requisicao -- a credencial de pessoa
        e a mais especifica e a que a matriz de 8.3.2 usa para autorizar.

        Args:
            authorization: Conteudo de ``Authorization``, com ou sem o prefixo ``Bearer``.
            api_key: Conteudo de ``X-API-Key``.

        Returns:
            A identidade autenticada.

        Raises:
            AuthError: ``motivo=credencial_ausente`` quando nenhum dos dois cabecalhos veio;
                as demais causas propagam de :meth:`validar_token` ou :meth:`validar_api_key`.
        """
        if _texto_nao_vazio(authorization) is not None:
            return self.validar_token(extrair_token_bearer(authorization))
        if _texto_nao_vazio(api_key) is not None:
            return self.validar_api_key(api_key)
        raise AuthError(
            "credencial ausente",
            motivo=MOTIVO_CREDENCIAL_AUSENTE,
            cabecalhos=[CABECALHO_AUTORIZACAO, CABECALHO_API_KEY],
        )


# --------------------------------------------------------------------------- #
# Instancia padrao e fachada de modulo (assinaturas da secao 8.3.1)            #
# --------------------------------------------------------------------------- #

_padrao: Autenticador | None = None


def autenticador_padrao() -> Autenticador:
    """Devolve o autenticador do processo, construindo-o na primeira chamada.

    A construcao e preguicosa de proposito: importar ``hospitalmq.auth`` nao deve exigir que
    ``JWT_SECRET`` ja esteja no ambiente -- um consumidor que so precisa ler ``ctx.identity`` nunca
    toca no segredo.

    Returns:
        A instancia compartilhada, criada a partir de ``hospitalmq.config.settings`` e do
        :class:`~hospitalmq.clock.RealClock`.
    """
    global _padrao  # estado de processo; trocavel por definir_autenticador_padrao
    if _padrao is None:
        _padrao = Autenticador()
    return _padrao


def definir_autenticador_padrao(auth: Autenticador | None) -> None:
    """Substitui o autenticador do processo.

    Usado pelo *startup* do ``api-gateway`` -- que injeta a configuracao ja carregada -- e pelas
    *fixtures* de teste, que injetam :class:`~hospitalmq.clock.ManualClock` para exercitar a
    expiracao de R4.2 sem esperar tempo real.

    Args:
        auth: A nova instancia, ou ``None`` para descartar a atual e voltar a construcao
            preguicosa a partir da configuracao.
    """
    global _padrao  # ponto unico de troca do estado de processo
    _padrao = auth


def emitir_token(sub: str, role: Papel, ttl_segundos: int | None = None) -> str:
    """Emite um JWT HS256 pelo autenticador padrao do processo (R4.1).

    Args:
        sub: Identificacao do usuario.
        role: Papel do usuario, um de :data:`PAPEIS_DE_USUARIO`.
        ttl_segundos: Validade em segundos; o padrao vem de ``JWT_EXPIRACAO_MIN`` (1800 s).

    Returns:
        O token compacto.

    Raises:
        ConfigError: Nas mesmas condicoes de :meth:`Autenticador.emitir_token`.
    """
    return autenticador_padrao().emitir_token(sub, role, ttl_segundos)


def validar_token(token: str | None) -> Identity:
    """Valida um JWT pelo autenticador padrao do processo (R4.2).

    Args:
        token: Token compacto, sem o prefixo ``Bearer``.

    Returns:
        A identidade da pessoa, com ``tipo="usuario"``.

    Raises:
        AuthError: Com ``motivo`` ``credencial_ausente``, ``expirado`` ou ``assinatura_invalida``.
    """
    return autenticador_padrao().validar_token(token)


def validar_api_key(chave: str | None) -> Identity:
    """Valida uma API Key de dispositivo pelo autenticador padrao do processo (R4.5).

    Args:
        chave: Valor do cabecalho ``X-API-Key``.

    Returns:
        A identidade do dispositivo, com ``tipo="dispositivo"``.

    Raises:
        AuthError: Com ``motivo`` ``credencial_ausente`` ou ``chave_desconhecida``.
    """
    return autenticador_padrao().validar_api_key(chave)


def extrair_token_bearer(authorization: str | None) -> str:
    """Extrai o token do cabecalho ``Authorization``.

    Aceita o esquema em qualquer caixa (``Bearer``, ``bearer``, ``BEARER``) e tolera o valor sem
    esquema algum, caso em que o conteudo inteiro e tratado como token -- conveniencia para
    ferramentas de linha de comando, sem perda de seguranca: um token invalido cai na mesma
    recusa de assinatura.

    Args:
        authorization: Conteudo bruto do cabecalho.

    Returns:
        O token compacto, sem espacos nas pontas.

    Raises:
        AuthError: ``motivo=credencial_ausente`` se o cabecalho estiver ausente, vazio, ou se
            trouxer um esquema diferente de ``Bearer``.
    """
    bruto = _texto_nao_vazio(authorization)
    if bruto is None:
        raise AuthError(
            "credencial ausente",
            motivo=MOTIVO_CREDENCIAL_AUSENTE,
            cabecalho=CABECALHO_AUTORIZACAO,
        )
    partes = bruto.strip().split()
    if len(partes) == 1:
        return partes[0]
    if len(partes) == 2 and partes[0].lower() == ESQUEMA_BEARER.lower():
        return partes[1]
    raise AuthError(
        "esquema de autorizacao nao suportado",
        motivo=MOTIVO_CREDENCIAL_AUSENTE,
        cabecalho=CABECALHO_AUTORIZACAO,
        esquema=partes[0],
    )


# --------------------------------------------------------------------------- #
# Ida e volta da identidade no Envelope (R4.3, R4.6)                           #
# --------------------------------------------------------------------------- #


def projetar_identidade(identidade: Identity | None) -> dict[str, str] | None:
    """Projeta a identidade no objeto JSON que vai para ``Envelope.identity`` (R4.3).

    E a **ida**: o gateway autentica uma vez e entrega o resultado ao ``Publisher``, que copia
    ``sub``, ``role`` e ``tipo`` para o Envelope. Um processo interno sem credencial -- *relay* do
    outbox, expurgo, ``redrive.py`` -- passa ``None`` e o campo vai a ``null``, que e a
    representacao honesta de "ninguem agiu" (secao 4.2.1).

    Args:
        identidade: A identidade autenticada, ou ``None``.

    Returns:
        ``{"sub": ..., "role": ..., "tipo": ...}`` ou ``None``.
    """
    return identidade.to_dict() if identidade is not None else None


def identidade_de_dict(dados: Mapping[str, Any] | None) -> Identity | None:
    """Reconstroi a identidade a partir do objeto JSON do Envelope.

    E a **volta**, simetrica de :func:`projetar_identidade`. Delega a validacao do dominio fechado
    de ``tipo`` a ``Identity.from_dict``, de modo que um valor inventado -- ``"sistema"``,
    ``"servico"`` -- e recusado como envelope invalido antes de chegar ao handler.

    Args:
        dados: Objeto com ``sub``, ``role`` e ``tipo``, ou ``None``.

    Returns:
        A identidade, ou ``None`` quando a mensagem nao carrega credencial.

    Raises:
        InvalidEnvelopeError: Se faltar campo ou se ``tipo`` estiver fora do dominio fechado.
    """
    return Identity.from_dict(dados) if dados is not None else None


def identidade_do_envelope(envelope: Envelope) -> Identity | None:
    """Le a identidade ja desserializada de um Envelope (R4.6).

    Existe para dar nome a operacao que R4.6 descreve -- "disponibilizar ao handler a identidade
    contida no Envelope sem exigir nova consulta ao servico de autenticacao". Nao ha chamada de
    rede, nao ha segredo envolvido e nao ha decodificacao: o ``Consumer`` ja desserializou o corpo
    uma unica vez, e ``MessageContext.identity`` devolve exatamente este valor.

    Args:
        envelope: A mensagem recebida.

    Returns:
        A identidade de quem originou a acao, ou ``None`` para mensagem de processo interno.
    """
    return envelope.identity


def verificar_papel(identidade: Identity | None, *papeis: str) -> Identity:
    """Exige que a identidade tenha um dos papeis informados (R4.4).

    E a regra de autorizacao pura, sem HTTP: a dependencia ``exigir_papel`` do gateway a envolve
    para devolver ``403``, e um handler assincrono pode usar a mesma funcao para recusar uma
    mensagem cuja origem nao tem permissao -- caso em que a :class:`AuthError` leva a mensagem
    direto a DLQ, sem consumir retentativas, com log em nivel ``warning``.

    Args:
        identidade: A identidade autenticada, ou ``None``.
        *papeis: Papeis aceitos. Pelo menos um deve ser informado.

    Returns:
        A propria identidade, para permitir encadeamento.

    Raises:
        AuthError: ``motivo=credencial_ausente`` se ``identidade`` for ``None``;
            ``motivo=papel_insuficiente`` se o papel nao estiver entre os aceitos.
        ConfigError: Se nenhum papel for informado -- exigir "um papel qualquer" seria uma
            autorizacao que nao autoriza nada, quase certamente um descuido de quem chamou.
    """
    if not papeis:
        raise ConfigError("verificar_papel exige ao menos um papel", campo="papeis")
    if identidade is None:
        raise AuthError(
            "credencial ausente",
            motivo=MOTIVO_CREDENCIAL_AUSENTE,
            exigidos=sorted(papeis),
        )
    if identidade.role not in papeis:
        raise AuthError(
            f"papel {identidade.role!r} nao autorizado para esta operacao",
            motivo=MOTIVO_PAPEL_INSUFICIENTE,
            papel=identidade.role,
            exigidos=sorted(papeis),
            identity_sub=identidade.sub,
        )
    return identidade


def verificar_tipo(identidade: Identity | None, *tipos: TipoIdentidade) -> Identity:
    """Exige que a credencial de origem seja de um dos tipos informados (R4.5, R4.6).

    O caso concreto do projeto e o ``vitals-service``: sinais vitais so podem vir de dispositivo,
    e essa decisao e tomada dentro do handler, com a identidade que veio no Envelope.

    Args:
        identidade: A identidade contida no Envelope, ou ``None``.
        *tipos: Tipos aceitos, de :data:`~hospitalmq.envelope.TipoIdentidade`. Pelo menos um.

    Returns:
        A propria identidade, para permitir encadeamento.

    Raises:
        AuthError: ``motivo=credencial_ausente`` se ``identidade`` for ``None``;
            ``motivo=papel_insuficiente`` se o tipo nao estiver entre os aceitos.
        ConfigError: Se nenhum tipo for informado.
    """
    if not tipos:
        raise ConfigError("verificar_tipo exige ao menos um tipo", campo="tipos")
    if identidade is None:
        raise AuthError(
            "mensagem sem identidade de origem",
            motivo=MOTIVO_CREDENCIAL_AUSENTE,
            exigidos=sorted(tipos),
        )
    if identidade.tipo not in tipos:
        raise AuthError(
            f"origem do tipo {identidade.tipo!r} nao autorizada para esta operacao",
            motivo=MOTIVO_PAPEL_INSUFICIENTE,
            tipo=identidade.tipo,
            exigidos=sorted(tipos),
            identity_sub=identidade.sub,
        )
    return identidade
