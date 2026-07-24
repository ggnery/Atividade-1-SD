"""``triage-service`` -- o consumidor de triagem clinica do Hospital Inteligente (G3).

Processo magro por construcao: consome ``sinais.registrados`` em
``q.triage.sinais-registrados``, calcula o ``ScoreNEWS2`` com a funcao pura de
``services.comum.news2`` (R6.2) e, **exclusivamente** quando a severidade e alta
(``total >= 5`` ou algum componente isolado pontuando 3, R6.3), emite
``alerta.gerado``. Toda a semantica de mensageria -- Envelope, idempotencia,
retentativa, correlacao -- mora no middleware ``hospitalmq``; aqui ha apenas a
regra clinica e a decisao de emitir ou nao o alerta.

Nao possui tabela de dominio (decisao D3, design 7.3.2): o ``ScoreNEWS2`` e funcao
pura e deterministica da leitura, recomputavel a qualquer momento, entao o servico
so toca o banco para a marca de idempotencia ``triagem.mensagens_processadas``,
gerida pelo proprio middleware.
"""

from __future__ import annotations
