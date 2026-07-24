"""Roteadores REST do ``api-gateway`` (design 8.2).

Cada modulo traduz REST -> RPC (``pacientes``, ``internacoes``), REST -> publish assincrono
(``sinais``) ou responde localmente (``auth``, ``saude``). O painel (``painel``) e servido por um
roteador do agente do painel, montado condicionalmente pelo ``main`` quando presente.
"""
