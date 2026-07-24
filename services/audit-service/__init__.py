"""``audit-service`` — a Trilha_de_Auditoria somente-insercao do Hospital (R7.1, R7.4).

Servico consumidor que grava **todo** evento que trafega em ``hospital.events`` numa
tabela somente-insercao (``auditoria.eventos_auditoria``), a partir da fila
``q.audit.todos``, ligada ao exchange pelo binding curinga ``#`` (design 6.5 e 9.9). Ele
nao interpreta o payload: grava os campos comuns do Envelope (``type``, ``timestamp``,
``correlation_id``, ``producer``, ``identity``) e o ``payload`` como ``JSONB`` opaco, de
modo que um tipo de evento desconhecido nao quebra o handler.

Este diretorio usa hifen no nome (``audit-service``) por decisao de projeto (design 12.1):
o nome com hifen e a identidade do servico no Compose, nos logs e no campo ``servico`` do
log JSON. Como hifen nao e identificador Python valido, o pacote **nao** e importavel por
``import`` pontilhado; o ponto de entrada e sempre por caminho
(``uvicorn --app-dir services/audit-service main:app``), e os modulos irmaos (``modelos``,
``handler``) sao importados como modulos de topo, nao como subpacotes.
"""
