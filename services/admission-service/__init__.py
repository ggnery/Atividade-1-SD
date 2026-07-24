"""``admission-service`` -- o serviço de admissao do Hospital Inteligente (grupo G3).

Unico serviço com RPC **e** outbox transacional (design 5.8, 4.9). Dono do *schema* ``clinico``
(:class:`~modelos.Paciente`, :class:`~modelos.Leito`, :class:`~modelos.Internacao`), responde por
cinco operacoes RPC em ``q.rpc.admission`` -- ``paciente.criar``, ``paciente.admitir``,
``paciente.dar-alta``, ``prontuario.consultar``, ``leitos.snapshot`` -- e publica os eventos de
admissao, alta e ocupacao de leito via *Transactional Outbox*.

Aviso de empacotamento: o diretorio ``admission-service`` tem hifen e **nao** e um pacote Python
importavel (``import services.admission-service`` e sintaxe invalida). Os modulos sao executados com
o diretorio do serviço em ``sys.path`` (ver ``main.py``); este ``__init__.py`` existe apenas como
marcador e documentacao, nao para habilitar ``import`` por caminho pontilhado.
"""
