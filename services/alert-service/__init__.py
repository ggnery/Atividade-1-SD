"""``alert-service`` — registra o ``Alerta_Clinico`` e o despacha a equipe do leito (R6.4, R6.5).

Servico consumidor que ouve ``alerta.gerado`` na fila ``q.alert.alerta-gerado`` (design 6.4), grava
o ``Alerta_Clinico`` com o ``ScoreNEWS2`` congelado em ``alertas.alertas`` (schema ``alertas``) e o
despacha ao canal de notificacao da equipe. O canal e um adapter **sabotavel** por configuracao
(``ALERT_FAILURE_RATE``/``ALERT_FAILURE_LEITOS``, design 12.5): quando falha, o handler apenas
levanta ``TransientError`` e o middleware ``hospitalmq`` aplica a retentativa 1s/2s/4s e a DLQ
(R6.5). Emite ``alerta.notificado`` no sucesso e ``alerta.falhou`` ao esgotar as retentativas
(design 6.3, 6.4).

Este diretorio usa hifen no nome (``alert-service``) por decisao de projeto (design 12.1): o nome
com hifen e a identidade do servico no Compose, nos logs e no campo ``servico``/``producer`` do log
JSON. Como hifen nao e identificador Python valido, o pacote **nao** e importavel por ``import``
pontilhado; o ponto de entrada e sempre por caminho
(``uvicorn --app-dir services/alert-service main:app``), e os modulos irmaos (``modelos``,
``notificacao``, ``handler``) sao importados como modulos de topo, nao como subpacotes.
"""
