# Implementation Plan

Ordem de construção derivada do `design.md`. As seções normativas para código são a **4** (núcleo do
middleware), **6** (topologia), **7** (domínio e DDL), **8** (gateway) e **12** (empacotamento).

Ambiente verificado nesta máquina: Docker 29.4.1 com daemon ativo, Compose v5.1.3, `uv` 0.11.8
(provê o Python 3.12 — o Python do sistema é 3.9.6 e não serve). Remote já existente:
`git@github.com:ggnery/Atividade-1-SD.git`.

---

- [x] 1. Fundação do projeto e contratos compartilhados
  - Criar `pyproject.toml` com Python 3.12, extras `borda` e `dominio`, e configuração de ruff, pytest e coverage
  - Criar o pacote `hospitalmq/` com `py.typed` e `__init__.py` reexportando a API pública
  - Implementar `envelope.py`: dataclass do Envelope com os 10 campos, serialização JSON UTF-8, `derivar()` para eventos filhos e conversão de/para headers AMQP com prefixo `x-hmq-`
  - Implementar `errors.py`: `HospitalMQError` e a hierarquia `TransportError`, `RpcTimeoutError`, `RpcRemoteError`, `TransientError`, `PermanentError`, `AuthError`
  - Implementar `config.py`: `Settings` por variável de ambiente, `TopologySpec`/`QueueSpec` e a factory de transporte por `HOSPITALMQ_TRANSPORT`
  - Implementar `transport/base.py`: `Protocol Transport` com os 8 métodos normativos de §4.3, mais `InboundMessage`, `MessageHandler` e `Subscription`
  - _Requirements: R1.2, R9.1, R9.2_

- [x] 2. Núcleo do middleware — o artefato avaliado
- [x] 2.1 Observabilidade e métricas
  - `logging.py`: configuração única do structlog, processador de mascaramento LGPD, propagação de `correlation_id`/`causation_id` por `contextvars`, vocabulário fechado de eventos como `StrEnum`
  - `metrics.py`: contadores de publicadas, consumidas, duplicadas, retentadas, DLQ e timeouts de RPC, com serialização JSON e Prometheus
  - _Requirements: R5.1, R5.2, R5.3, R5.4, R5.5, R5.6_

- [x] 2.2 Autenticação e identidade
  - `auth.py`: emissão e validação de JWT HS256 com claims `sub`, `role`, `exp`, `iat`; validação de API Key de dispositivo; `Identity` e sua ida e volta para o Envelope
  - _Requirements: R4.1, R4.2, R4.3, R4.5, R4.6_

- [x] 2.3 Transporte em memória
  - `transport/memory.py`: filas `asyncio`, roteamento por padrão de routing key incluindo `#` e `*`, atraso de entrega para `delay_ms`, `reply_to` funcional
  - É o habilitador de toda a suíte de testes sem broker
  - _Requirements: R9.1, R10.4_

- [x] 2.4 Retentativa, DLQ e idempotência
  - `retry.py`: política 1s/2s/4s, máximo 3 tentativas, classificação transitório × permanente, decisão retentativa × DLQ, contagem em `attempt`
  - `idempotency.py`: registro `(consumidor, message_id)` com `INSERT ... ON CONFLICT DO NOTHING` na mesma transação do handler
  - Relógio injetável (`clock.py`) para que os testes não esperem os 7 s reais
  - _Requirements: R2.2, R2.3, R2.4_

- [x] 2.5 Publisher e Consumer
  - `publisher.py`: `publish(tipo, payload, correlation_id=None, identity=None)`, preenchimento automático do Envelope, publisher confirms, `TransportError` em até 5 s, relay do outbox
  - `consumer.py`: registro de handler por decorator, prefetch 10, ACK manual só após sucesso, pipeline log → idempotência → identidade → handler → derivados → ack, enriquecimento da DLQ
  - _Requirements: R1.1, R1.2, R1.4, R1.5, R2.1, R2.5, R2.6, R5.4_

- [x] 2.6 RPC sobre fila
  - `rpc.py`: `RpcClient` com fila de retorno `q.rpc.reply.<producer>.<uuid4hex>` nomeada pelo cliente, dicionário de futures por `correlation_id`, `asyncio.wait_for`, limpeza em `finally`, descarte de resposta órfã
  - `RpcServer` com registro de operações, resposta via `reply_to` e propagação de erro remoto
  - _Requirements: R3.1, R3.2, R3.3, R3.4, R3.5_

- [x] 2.7 Transporte AMQP
  - `transport/amqp.py`: driver sobre aio-pika, declaração idempotente da topologia a partir da `TopologySpec`, exchanges `hospital.events`/`hospital.rpc`/`hospital.dlx`, filas de espera com TTL e dead-lettering, filas exclusivas de retorno
  - _Requirements: R1.3, R1.4, R2.3, R2.5, R9.1_

- [x] 3. Checkpoint — núcleo isolado
  - Rodar a suíte de unidade sobre `MemoryTransport`: retentativa, DLQ, idempotência, RPC e timeout devem passar sem broker
  - Nenhum serviço da aplicação existe ainda; o middleware precisa estar verde sozinho
  - _Requirements: R2.2, R2.3, R2.4, R3.3, R10.3_

- [x] 4. Domínio e persistência
- [x] 4.1 Esquema do banco
  - `db/schema.sql` idempotente: cinco schemas, papéis `svc_*` com GRANT/REVOKE, tabelas `pacientes`, `internacoes`, `leitos`, `sinais_vitais`, `alertas`, `eventos_auditoria`, `mensagens_processadas`, `outbox_mensagens`
  - Invariantes como barreira declarativa: índice único parcial de uma internação ativa por leito, triggers de imutabilidade, CHECK que codifica a regra do componente isolado
  - `db/seed.sql` com leitos, equipes e usuários fictícios
  - _Requirements: R6.1, R7.4, C6_

- [x] 4.2 Regra clínica NEWS2
  - Função pura `calcular_news2` com a tabela normativa como dado, sem dependência de framework
  - Agregação, classificação de severidade e a regra do componente isolado igual a 3
  - Faixas fisiológicas de validação de entrada
  - _Requirements: R6.2, R6.3, R6.6_

- [x] 4.3 Camada comum dos serviços
  - `services/comum/`: sessão async do SQLAlchemy 2.0, bootstrap do app, `/health` e `/metrics` padronizados, transação que engloba efeito colateral e marca de idempotência
  - _Requirements: R2.4, R8.6, R5.5_

- [x] 5. Serviços da aplicação
- [x] 5.1 admission-service
  - Modelos `Paciente`, `Internacao`, `Leito`; repositório; outbox transacional com relay
  - Operações RPC: `paciente.criar`, `paciente.buscar`, `internacao.admitir`, `internacao.dar-alta`, `internacao.detalhar`, `prontuario.consultar`, `leito.listar`
  - Emissão de `paciente.admitido`, `paciente.alta`, `leito.ocupado`, `leito.liberado`, `prontuario.consultado`
  - _Requirements: R7.2, R7.3, R7.5, R3.1_

- [x] 5.2 vitals-service
  - Consome `sinais.coletados`, valida faixa fisiológica, persiste `SinaisVitais`, publica `sinais.registrados`
  - Valor fora da faixa vira `PermanentError` com `sinais.rejeitados` e vai à DLQ
  - _Requirements: R6.1, R6.6_

- [x] 5.3 triage-service
  - Consome `sinais.registrados`, aplica `calcular_news2`, publica `alerta.gerado` com severidade
  - _Requirements: R6.2, R6.3_

- [x] 5.4 alert-service
  - Consome `alerta.gerado`, registra o `AlertaClinico`, despacha ao canal
  - `notificacao.py` como adapter sabotável por `ALERT_FAILURE_RATE`/`ALERT_FAILURE_LEITOS`; o handler não sabe da simulação e apenas levanta `TransientError`
  - Publica `alerta.notificado` ou `alerta.falhou`
  - _Requirements: R6.4, R6.5_

- [x] 5.5 audit-service
  - Handler único com binding `#`, gravação somente-inserção na Trilha_de_Auditoria
  - _Requirements: R7.1, R7.3, R7.4_

- [x] 6. API Gateway
- [x] 6.1 Borda HTTP, segurança e erros
  - Composition root injetando apenas `Publisher`, `RpcClient` e `ProjecaoLeitos` — sem sessão de banco
  - 18 endpoints de §8.2, schemas Pydantic, dependências de JWT e API Key, matriz papel × endpoint
  - Middleware de correlação e tradutor de erro RFC 7807 cobrindo 401, 403, 404, 409, 422, 500, 503 e 504
  - Emissão de `acesso.negado` na recusa por papel
  - _Requirements: R4.1, R4.2, R4.4, R4.5, R7.2, R8.2, R8.3, R8.4, R8.5, R8.6_

- [x] 6.2 OpenAPI e Swagger
  - Anotação de tags, exemplos de request e response, security schemes e respostas de erro declaradas
  - _Requirements: R8.1_

- [x] 6.3 Painel de leitos
  - `projecao.py` alimentada por `q.gateway.projecao`; `sse.py` com fan-out para `/painel/stream`
  - `static/painel.html`, `.css`, `.js`: cards por leito com cor por severidade, lista de alertas, indicador de conexão, reconexão automática
  - _Requirements: R11.1, R11.2, R11.3, R11.4, R11.5, R11.6, R11.7_

- [x] 7. Cliente simulador de monitor de leito
  - CLI com `--cenario`, `--leito`, `--leitos`, `--intervalo`, `--duracao`, `--semente`, `--api-key`
  - Trajetórias fisiológicas determinísticas por semente
  - Os quatro cenários de R10.6: estável, deterioração, falha de consumidor com retentativa, mensagem à DLQ
  - _Requirements: R10.6, R6.1_

- [x] 8. Empacotamento e orquestração
  - `Dockerfile` único com dois perfis e `hospitalmq` instalado como pacote local
  - `docker-compose.yml` com healthchecks (`rabbitmq-diagnostics`, `pg_isready`, `/health`) e `depends_on: service_healthy`
  - Um `DATABASE_URL` por serviço consumidor; o gateway não recebe nenhum
  - `.env.example`, `infra/rabbitmq.conf`, `scripts/token.sh` e `scripts/trace.sh`
  - _Requirements: R10.1, R7.2_

- [x] 9. Suíte de testes
- [x] 9.1 Unidade e contrato
  - Tabela normativa NEWS2 parâmetro a parâmetro com os limites de cada faixa, regra do componente isolado, mapeamento score → severidade
  - Idempotência, sequência de espera 1s/2s/4s com relógio simulado, DLQ exatamente após a terceira retentativa
  - `RpcTimeoutError` com limpeza da future, duas chamadas concorrentes recebendo cada uma a sua resposta
  - Bateria de conformidade parametrizada rodando contra `MemoryTransport` e `AmqpTransport`
  - _Requirements: R2.2, R2.3, R2.4, R3.3, R3.5, R6.2, R6.3, R10.3_

- [x] 9.2 Integração e API
  - 401 sem token, 401 com token expirado, 403 com papel insuficiente
  - Erro em `application/problem+json` com `correlation_id`; 504 no timeout de RPC; `/health` respondendo com o broker fora
  - Sinal vital fora da faixa indo à DLQ; audit-service capturando evento de tipo desconhecido pelo `#`
  - Propagação do `correlation_id` do gateway até o último consumidor
  - Teste de arquitetura falhando se o gateway ganhar sessão de banco
  - _Requirements: R4.1, R4.2, R4.4, R5.3, R6.6, R7.1, R8.2, R8.4, R8.5, R10.3_

- [x] 9.3 Ponta a ponta com Docker Compose
  - Subir o ambiente, esperar prontidão por healthcheck, exercitar o fluxo clínico completo e encerrar
  - _Requirements: R10.1, R10.4_

- [x] 10. Checkpoint — sistema completo
  - `docker compose up` a partir de clone limpo, sem passo manual
  - Suíte inteira verde; cobertura do pacote `hospitalmq/` medida
  - Percorrer o roteiro de demonstração de §12.6 de ponta a ponta
  - _Requirements: R10.1, R10.3, R10.4, R10.6_

- [x] 11. Documentação de entrega
  - `README.md` com pré-requisitos, execução, credenciais de teste, roteiro da demonstração e exemplos de chamadas
  - `docs/arquitetura.md`: versão condensada do design para entrega ao professor, com os diagramas principais e os ADRs em anexo
  - `.github/workflows/ci.yml` com lint, testes e build da imagem
  - _Requirements: R10.2, R10.5_

- [x] 12. Slides da apresentação
  - Estrutura de 15 minutos seguindo o roteiro de §12.6
  - Nomes e matrículas dos cinco integrantes na capa e no encerramento
  - _Requirements: R10.5_

---

## Pendências que bloqueiam a entrega final

| # | Pendência | Bloqueia |
|---|-----------|----------|
| 1 | — | _Resolvido: os cinco integrantes do G3 estão no README, em `docs/arquitetura.md`, no `CONTRIBUTING.md`, no `pyproject.toml` e nos 2 slides_ |
| 2 | — | _Resolvido: remote `git@github.com:ggnery/Atividade-1-SD.git` já existe_ |

Resta apenas a divisão dos papéis da apresentação entre os integrantes (coluna "Papel" marcada como
`a definir`) — não bloqueia a entrega do código nem da documentação.
