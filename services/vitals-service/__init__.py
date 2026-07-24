"""``vitals-service`` — ingestao e persistencia de sinais vitais (R6.1, R6.6, design 6.4 / 7.7).

Servico consumidor que le ``sinais.coletados`` da fila ``q.vitals.sinais-coletados``, valida a
leitura contra as faixas fisiologicas de aceitacao (design 7.7.1), persiste a leitura **imutavel**
em ``vitais.sinais_vitais`` e publica ``sinais.registrados`` para a triagem, o painel e a auditoria.
Um valor fora da faixa nunca chega a pontuar NEWS2: vira ``PermanentError`` com um evento
``sinais.rejeitados`` e segue para a DLQ (R6.6). O calculo NEWS2 e responsabilidade do
``triage-service`` no fluxo de ingestao; aqui ele so reaparece, como funcao pura reusada, no caminho
de **leitura** da operacao RPC ``sinais.ultimos`` (design 5.8.1), para compor o resumo de severidade
que o prontuario e o painel exibem.

Este diretorio usa hifen no nome (``vitals-service``) por decisao de projeto (design 12.1): o nome
com hifen e a identidade do servico no Compose, nos logs e no campo ``servico`` do log JSON. Como
hifen nao e identificador Python valido, o pacote **nao** e importavel por ``import`` pontilhado; o
ponto de entrada e sempre por caminho (``uvicorn --app-dir services/vitals-service main:app``), e os
modulos irmaos (``modelos``, ``handler``) sao importados como modulos de topo, nao como subpacotes.
"""
