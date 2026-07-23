# Requirements Document

## Introduction

O **Hospital Inteligente** precisa integrar monitores de leito, equipes clínicas e sistemas
administrativos que operam em ritmos diferentes: um monitor emite sinais vitais a cada poucos
segundos, um enfermeiro consulta o prontuário sob demanda e a auditoria da LGPD precisa registrar
tudo sem atrasar o atendimento. Este projeto entrega o **HospitalMQ**, um middleware de mensageria
orientado a filas — desenvolvido pelo grupo G3 sobre o protocolo AMQP (RabbitMQ) — que desacopla
produtores de consumidores, oferece comunicação assíncrona (publish/consume) e síncrona (RPC sobre
fila), e garante entrega confiável mesmo quando serviços caem, ficam lentos ou processam a mesma
mensagem duas vezes.

O escopo desta atividade é o middleware e os serviços que o exercitam, executáveis localmente via
Docker Compose, com a arquitetura equivalente em **Amazon SQS** apresentada como proposta (não
implantada).

## Glossary

| Termo | Definição |
|-------|-----------|
| **HospitalMQ** | A biblioteca de middleware de mensageria desenvolvida pelo grupo G3. Expõe as primitivas `Publisher`, `Consumer`, `RpcClient` e `RpcServer` e concentra retentativa, idempotência, autenticação, correlação e log. É o artefato central da entrega. |
| **Broker** | O servidor de filas que transporta as mensagens. Na implementação local é o RabbitMQ 3.13 com plugin de management. |
| **Transporte** | Adaptador plugável que traduz as primitivas do HospitalMQ para um broker concreto. Dois transportes previstos: `AmqpTransport` (implementado) e `SqsTransport` (projetado para AWS). |
| **Envelope** | Estrutura padronizada que o HospitalMQ coloca em toda mensagem: `message_id`, `correlation_id`, `type`, `timestamp`, `producer`, `identidade` do chamador, `tentativa` e `payload`. |
| **Correlation_ID** | Identificador único de uma requisição de ponta a ponta, gerado no API_Gateway e propagado por todos os serviços e logs que participam do fluxo. |
| **DLQ** | *Dead Letter Queue* — fila de descarte que recebe mensagens cujo processamento falhou após esgotar as retentativas, para inspeção humana. |
| **API_Gateway** | Serviço HTTP de borda (FastAPI) que autentica clientes, valida entrada e traduz requisições REST em mensagens do HospitalMQ. Não acessa o banco clínico diretamente. |
| **Servico_Consumidor** | Qualquer processo que consome mensagens do Broker através do HospitalMQ (`admission-service`, `vitals-service`, `triage-service`, `alert-service`, `audit-service`). |
| **NEWS2** | *National Early Warning Score 2* — escore clínico de 0 a 20 calculado a partir dos sinais vitais que classifica o risco de deterioração do paciente. |
| **Alerta_Clinico** | Notificação gerada quando o NEWS2 ultrapassa o limiar de risco, endereçada à equipe responsável pelo leito. |
| **Trilha_de_Auditoria** | Registro imutável, exigido pela LGPD, de todo evento que trafega pelo Broker e de todo acesso a dado de paciente. |
| **Cliente_Leito** | Simulador de monitor de beira-leito que publica sinais vitais periodicamente; é o cliente da demonstração prática. |
| **Painel_de_Leitos** | Página servida pelo API_Gateway que exibe o estado de todos os leitos em tempo real, alimentada pelos eventos que trafegam no Broker. |

## Requirements

### Requirement 1 — Comunicação assíncrona desacoplada

**User Story:** Como desenvolvedor de um serviço do hospital, quero publicar um evento sem conhecer
quem vai consumi-lo, para que novos serviços sejam acrescentados sem alterar o produtor.

#### Acceptance Criteria

1. WHEN um produtor chama `Publisher.publish(tipo, payload)`, THE HospitalMQ SHALL entregar a
   mensagem ao Broker sem que o produtor informe qualquer fila, endereço de rede ou identidade de
   consumidor.
2. WHEN o HospitalMQ publica uma mensagem, THE HospitalMQ SHALL envolvê-la em um Envelope contendo
   `message_id` (UUIDv4), `correlation_id`, `type`, `timestamp` em ISO-8601 UTC e `producer`.
3. WHEN um novo Servico_Consumidor assina um tipo de evento já existente, THE HospitalMQ SHALL
   entregar cópias da mesma mensagem a esse consumidor e aos anteriores, sem alteração de código no
   produtor.
4. WHILE nenhum Servico_Consumidor estiver ativo, THE HospitalMQ SHALL reter as mensagens publicadas
   em fila durável e entregá-las quando um consumidor voltar a se conectar.
5. IF o Broker estiver indisponível no momento da publicação, THEN THE HospitalMQ SHALL levantar
   `TransportError` ao produtor em no máximo 5 segundos, sem descartar a mensagem silenciosamente.

### Requirement 2 — Entrega confiável e tolerância a falhas

**User Story:** Como responsável pela operação, quero que uma mensagem não se perca nem seja
processada em duplicidade quando um serviço falha, para que o prontuário do paciente permaneça
correto.

#### Acceptance Criteria

1. WHEN um Servico_Consumidor conclui o processamento com sucesso, THE HospitalMQ SHALL confirmar a
   mensagem ao Broker por ACK manual, e somente então.
2. IF o handler do consumidor levanta uma exceção transitória, THEN THE HospitalMQ SHALL reenfileirar
   a mensagem com espera exponencial de 1s, 2s e 4s, até o máximo de 3 retentativas.
3. IF as 3 retentativas se esgotarem, THEN THE HospitalMQ SHALL mover a mensagem para a DLQ
   preservando o Envelope original e acrescentando o motivo da última falha.
4. WHEN o HospitalMQ entrega ao handler uma mensagem cujo `message_id` já foi processado com sucesso,
   THE HospitalMQ SHALL suprimir a segunda execução do handler e confirmar a mensagem ao Broker.
5. WHERE o consumidor for executado em múltiplas réplicas, THE HospitalMQ SHALL distribuir as
   mensagens da fila entre as réplicas em regime de *competing consumers*, entregando cada mensagem a
   exatamente uma réplica por vez.
6. WHILE um consumidor estiver processando, THE HospitalMQ SHALL limitar as mensagens não confirmadas
   em voo a 10 por réplica (`prefetch`), para que a carga se distribua entre as réplicas disponíveis.

### Requirement 3 — Chamada síncrona sobre fila (RPC)

**User Story:** Como enfermeiro consultando o prontuário pela API, quero receber a resposta na mesma
requisição HTTP, para que a consulta seja imediata mesmo que o transporte por baixo seja uma fila.

#### Acceptance Criteria

1. WHEN o API_Gateway chama `RpcClient.call(operacao, payload, timeout)`, THE HospitalMQ SHALL
   publicar a requisição em uma fila de operação e aguardar a resposta em uma fila de retorno
   exclusiva do chamador.
2. WHEN o RpcServer responde, THE HospitalMQ SHALL casar a resposta com a requisição pelo
   `correlation_id` e devolvê-la ao chamador que a originou.
3. IF nenhuma resposta chegar dentro do timeout configurado (padrão 5 segundos), THEN THE HospitalMQ
   SHALL levantar `RpcTimeoutError` e liberar os recursos da requisição pendente.
4. IF o RpcServer levantar exceção ao tratar a operação, THEN THE HospitalMQ SHALL devolver ao
   chamador uma resposta de erro com código e mensagem, em vez de deixar a chamada expirar por
   timeout.
5. WHEN duas chamadas RPC concorrentes estiverem pendentes no mesmo processo, THE HospitalMQ SHALL
   entregar a cada uma exclusivamente a sua própria resposta.

### Requirement 4 — Autenticação e propagação de identidade

**User Story:** Como responsável pela segurança, quero que toda requisição seja autenticada na borda
e que a identidade acompanhe a mensagem até o último consumidor, para que a auditoria saiba quem
originou cada ação.

#### Acceptance Criteria

1. WHEN uma requisição HTTP chega a um endpoint protegido do API_Gateway sem cabeçalho
   `Authorization: Bearer <JWT>` válido, THE API_Gateway SHALL responder `401 Unauthorized` e não
   publicar nenhuma mensagem.
2. WHEN o JWT apresentado estiver expirado ou com assinatura inválida, THE API_Gateway SHALL responder
   `401 Unauthorized` informando a causa da recusa.
3. WHEN o API_Gateway autentica a requisição, THE HospitalMQ SHALL copiar o `sub` (identificação do
   usuário) e o `role` do JWT para o Envelope da mensagem publicada.
4. IF o `role` do chamador não contiver a permissão exigida pelo endpoint, THEN THE API_Gateway SHALL
   responder `403 Forbidden` e registrar a tentativa na Trilha_de_Auditoria.
5. WHEN o Cliente_Leito se conecta para publicar sinais vitais, THE API_Gateway SHALL autenticá-lo por
   API Key de dispositivo, distinta das credenciais de usuário humano.
6. WHEN um Servico_Consumidor recebe uma mensagem, THE HospitalMQ SHALL disponibilizar ao handler a
   identidade contida no Envelope sem exigir nova consulta ao serviço de autenticação.

### Requirement 5 — Observabilidade: log estruturado e rastreamento

**User Story:** Como avaliador da demonstração, quero acompanhar uma mensagem do cliente até o último
consumidor pelos logs, para que o caminho percorrido no middleware fique visível.

#### Acceptance Criteria

1. WHEN o HospitalMQ publica, recebe, reenfileira ou descarta uma mensagem, THE HospitalMQ SHALL
   emitir uma linha de log em JSON contendo `timestamp` ISO-8601 UTC, `nivel`, `evento`, `serviço`,
   `message_id` e `correlation_id`.
2. WHEN o API_Gateway recebe uma requisição sem `X-Correlation-ID`, THE API_Gateway SHALL gerar um
   Correlation_ID novo e devolvê-lo no cabeçalho da resposta.
3. WHILE uma mensagem estiver sendo processada em cadeia por vários serviços, THE HospitalMQ SHALL
   preservar o mesmo Correlation_ID em todos os logs e mensagens derivadas.
4. WHEN um handler termina, THE HospitalMQ SHALL registrar a duração do processamento em
   milissegundos e o resultado (`sucesso`, `retentativa` ou `dlq`).
5. THE HospitalMQ SHALL expor um contador acumulado de mensagens publicadas, consumidas, retentadas e
   enviadas à DLQ, consultável por endpoint HTTP de métricas.
6. IF o payload contiver campo marcado como dado pessoal sensível, THEN THE HospitalMQ SHALL mascarar
   o valor na linha de log, registrando apenas o identificador do paciente.

### Requirement 6 — Fluxo clínico: sinais vitais, triagem e alerta

**User Story:** Como enfermeiro de plantão, quero ser alertado quando um paciente monitorado começar a
deteriorar, para que a equipe possa intervir antes do agravamento.

#### Acceptance Criteria

1. WHEN o Cliente_Leito publica uma leitura de sinais vitais, THE HospitalMQ SHALL entregá-la ao
   `vitals-service`, que persiste a leitura e emite o evento `sinais.registrados`.
2. WHEN o `triage-service` consome `sinais.registrados`, THE triage-service SHALL calcular o escore
   NEWS2 a partir de frequência respiratória, saturação, temperatura, pressão sistólica, frequência
   cardíaca, nível de consciência e uso de oxigênio suplementar.
3. IF o NEWS2 calculado for maior ou igual a 5, OR se qualquer componente isolado pontuar 3, THEN THE
   triage-service SHALL emitir o evento `alerta.gerado` classificando a severidade como `alta`.
4. WHEN o `alert-service` consome `alerta.gerado`, THE alert-service SHALL registrar o Alerta_Clinico e
   despachá-lo ao canal de notificação da equipe do leito.
5. IF o canal de notificação falhar, THEN THE alert-service SHALL levantar erro transitório para que o
   HospitalMQ aplique a política de retentativa do Requirement 2, sem perder o alerta.
6. WHEN uma leitura de sinais vitais chega com valor fora da faixa fisiológica aceita, THE
   vitals-service SHALL rejeitá-la e encaminhá-la à DLQ sem tentar calcular NEWS2.

### Requirement 7 — Prontuário e Trilha de Auditoria (LGPD)

**User Story:** Como encarregado de dados (DPO) do hospital, quero uma trilha de tudo o que acontece
com dados de paciente, para que o hospital comprove conformidade com a LGPD.

#### Acceptance Criteria

1. WHEN qualquer mensagem trafega pelo Broker, THE audit-service SHALL registrar na Trilha_de_Auditoria
   o `type`, o `timestamp`, o `correlation_id` e a identidade do produtor, sem exigir alteração de
   código quando novos tipos de evento surgirem.
2. WHEN um usuário consulta o prontuário de um paciente pelo API_Gateway, THE API_Gateway SHALL obter
   os dados via chamada RPC ao `admission-service`, sem acessar o banco clínico diretamente.
3. WHEN uma consulta a prontuário é atendida, THE audit-service SHALL registrar o acesso identificando
   quem consultou, qual paciente e em que instante.
4. THE Trilha_de_Auditoria SHALL ser somente-inserção, sem operação exposta de alteração ou remoção de
   registros já gravados.
5. WHEN o paciente consultado não existir, THE API_Gateway SHALL responder `404 Not Found` no formato
   de erro do Requirement 8.

### Requirement 8 — API documentada e tratamento de erros

**User Story:** Como cliente da API do hospital, quero uma documentação executável e erros
padronizados, para que eu integre meu sistema sem ler o código-fonte.

#### Acceptance Criteria

1. THE API_Gateway SHALL publicar a especificação OpenAPI 3 de todos os seus endpoints em `/openapi.json`
   e uma interface Swagger navegável em `/docs`.
2. WHEN a API_Gateway retorna qualquer erro, THE API_Gateway SHALL responder no formato RFC 7807
   (`application/problem+json`) com os campos `type`, `title`, `status`, `detail` e `correlation_id`.
3. IF a requisição contiver corpo que viola o schema declarado, THEN THE API_Gateway SHALL responder
   `422 Unprocessable Entity` indicando o campo inválido.
4. IF uma chamada RPC exceder o timeout, THEN THE API_Gateway SHALL responder `504 Gateway Timeout` em
   vez de manter a conexão HTTP aberta indefinidamente.
5. IF o Broker estiver inacessível, THEN THE API_Gateway SHALL responder `503 Service Unavailable` e
   permanecer respondendo ao endpoint de *health check*.
6. THE API_Gateway SHALL expor `/health` retornando o estado da própria aplicação e da conexão com o
   Broker.

### Requirement 9 — Portabilidade de transporte e arquitetura AWS

**User Story:** Como arquiteto do projeto da disciplina, quero que o middleware troque de broker sem
reescrever os serviços, para que a evolução para Amazon SQS seja uma mudança de configuração.

#### Acceptance Criteria

1. THE HospitalMQ SHALL definir a interface `Transporte` com as operações de publicar, consumir,
   confirmar, rejeitar e responder, e SHALL implementar `AmqpTransport` sobre RabbitMQ.
2. WHEN a variável de ambiente de transporte for alterada, THE HospitalMQ SHALL selecionar o
   transporte correspondente sem alteração no código dos Servico_Consumidor.
3. THE documentação de arquitetura SHALL apresentar o mapeamento de cada conceito do HospitalMQ para o
   serviço AWS equivalente (fila → SQS Standard/FIFO, fan-out → SNS, DLQ → *redrive policy*,
   credenciais → IAM, logs → CloudWatch).
4. THE documentação de arquitetura SHALL registrar as diferenças de semântica entre RabbitMQ e SQS que
   afetam o projeto: ausência de *push* nativo, `VisibilityTimeout` no lugar de ACK, ordenação apenas
   em filas FIFO e limite de 256 KB por mensagem.
5. WHERE a implantação em AWS for adotada, THE arquitetura SHALL descrever como a idempotência do
   Requirement 2 permanece necessária, dado que o SQS Standard entrega ao menos uma vez.

### Requirement 10 — Execução reprodutível, testes e entrega

**User Story:** Como professor avaliando o trabalho, quero subir o sistema inteiro com um comando e
ver os testes passarem, para que a demonstração não dependa da máquina do grupo.

#### Acceptance Criteria

1. WHEN o avaliador executa `docker compose up`, THE sistema SHALL subir Broker, banco de dados,
   API_Gateway e todos os Servico_Consumidor prontos para uso, sem passo manual adicional.
2. THE repositório SHALL conter README com pré-requisitos, comando de execução, credenciais de teste,
   roteiro da demonstração e exemplos de chamadas à API.
3. THE suíte de testes SHALL cobrir, no mínimo, a tabela normativa do NEWS2, a idempotência do
   consumidor, o encaminhamento à DLQ após retentativas e o timeout do RPC.
4. WHEN o avaliador executa o comando de testes documentado no README, THE suíte SHALL rodar sem
   depender de serviços externos além dos definidos no Docker Compose.
5. THE repositório SHALL conter o documento de arquitetura e os slides da apresentação, versionados
   junto ao código-fonte.
6. WHEN o Cliente_Leito é executado no modo demonstração, THE Cliente_Leito SHALL reproduzir os
   cenários de paciente estável, paciente em deterioração, falha de consumidor com retentativa e
   mensagem enviada à DLQ.

### Requirement 11 — Painel de monitoramento de leitos

**User Story:** Como enfermeiro do posto de enfermagem, quero ver todos os leitos em uma tela única que
se atualiza sozinha, para que eu perceba a deterioração de um paciente sem precisar consultar a API.

#### Acceptance Criteria

1. WHEN o usuário abre `/painel` no API_Gateway, THE Painel_de_Leitos SHALL exibir um card por leito
   ocupado com identificação do leito, nome do paciente, últimos sinais vitais recebidos e o escore
   NEWS2 mais recente.
2. WHEN um evento `sinais.registrados` ou `alerta.gerado` é publicado no Broker, THE Painel_de_Leitos
   SHALL atualizar o card correspondente em no máximo 2 segundos, sem recarregar a página.
3. WHERE o NEWS2 do leito for maior ou igual a 5, OR houver componente isolado pontuando 3, THE
   Painel_de_Leitos SHALL destacar o card com a cor de severidade alta.
4. WHEN um Alerta_Clinico é gerado, THE Painel_de_Leitos SHALL inseri-lo no topo da lista de alertas
   com horário, identificação do leito e escore que o disparou.
5. IF a conexão de atualização em tempo real cair, THEN THE Painel_de_Leitos SHALL sinalizar
   visualmente o estado desconectado e tentar reconectar automaticamente.
6. THE Painel_de_Leitos SHALL ser servido como página estática pelo próprio API_Gateway, sem build de
   front-end e sem dependência de rede externa em tempo de execução.
7. THE API_Gateway SHALL alimentar o Painel_de_Leitos a partir de uma projeção em memória construída
   pelo consumo dos eventos do Broker, mantendo a regra do Requirement 7.2 de não acessar o banco
   clínico diretamente.

## Restrições e Premissas

| # | Restrição | Origem |
|---|-----------|--------|
| C1 | O middleware de mensageria deve ser desenvolvido pelo grupo; RabbitMQ entra como transporte, não como entrega. | Enunciado, seção 8 |
| C2 | Linguagem Python 3.12; API em FastAPI (OpenAPI nativo); RabbitMQ 3.13; PostgreSQL 16; Docker Compose. | Enunciado, seção 6 |
| C3 | Arquitetura mínima obrigatória: Cliente → Middleware → Servidor → Banco de Dados. | Enunciado, seção 7 |
| C4 | A solução em AWS é apresentada como proposta arquitetural, sem implantação. | Enunciado, seção 10 |
| C5 | A apresentação tem 15 minutos por grupo, na semana de 27/07. | Enunciado, seção 9 |
| C6 | Dados de pacientes são fictícios e gerados pelo simulador; nenhum dado real é usado. | Decisão do grupo |

## Rastreabilidade — Requisitos × Critérios de Avaliação

| Critério (peso) | Requisitos que o atendem |
|-----------------|--------------------------|
| Fundamentação teórica (15%) | R1 (desacoplamento), R2 (at-least-once, idempotência), R3 (síncrono × assíncrono), R9 (portabilidade) |
| Arquitetura da solução (20%) | R1, R3, R7.2, R9, C3 |
| Implementação do middleware (30%) | R1, R2, R3, R4.3/4.6, R5, R9.1/9.2 |
| Qualidade do código e documentação (15%) | R5, R8, R10.2, R10.3, R10.5 |
| Demonstração prática (10%) | R6, R10.1, R10.6, R11 |
| Integração com AWS e respostas às perguntas (10%) | R9 |

| Requisito obrigatório do enunciado | Coberto por |
|-----------------------------------|-------------|
| Comunicação entre cliente e servidor | R1, R3, R6, R11 |
| Middleware desenvolvido pelo grupo | R1, R2, R3, R9.1 |
| Autenticação simples (JWT ou API Key) | R4 |
| Registro de logs e timestamp | R5 |
| Tratamento de exceções e timeout | R2.2, R3.3, R8.2–8.5 |
| Documentação da API (Swagger/OpenAPI) | R8.1 |
| Testes funcionais | R10.3, R10.4 |
| Repositório GitHub | R10.5 |
| README com instruções de execução | R10.2 |

## Pontos em aberto

1. **Composição do grupo** — o README e os slides precisam dos nomes dos integrantes do G3.
2. **Repositório GitHub** — confirmar se o repositório remoto já existe ou se deve ser criado.

_Resolvido em 23/07: stack Python 3.12 + FastAPI + RabbitMQ + PostgreSQL (C2) e demonstração com
Painel_de_Leitos ao vivo (R11)._
