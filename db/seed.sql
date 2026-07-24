-- =============================================================================
-- db/seed.sql — dados fictícios de demonstração (C6) do Hospital Inteligente.
--
-- Aplicado pelo db-init logo após schema.sql, como dono do banco (`hospital`),
-- com `psql -v ON_ERROR_STOP=1`. Roda a CADA boot do Compose, portanto é
-- IDEMPOTENTE: todo INSERT usa ON CONFLICT DO NOTHING sobre a chave única, de
-- modo que reexecutar não duplica linha nem aborta o db-init.
--
-- ESCOPO. Só existem no modelo físico (schema.sql) três tabelas de domínio no
-- schema `clinico`: pacientes, leitos e internacoes. Não há tabela de
-- "usuários" nem de "equipes":
--   * "equipe" é uma coluna TEXT livre em clinico.internacoes, preenchida pela
--     admissão (RPC paciente.admitir, payload `equipe_responsavel`).
--   * os "usuários" da demo (enfermeiro, médico, auditor, admin) e os
--     dispositivos são identidades de AUTENTICAÇÃO do api-gateway (JWT HS256 e
--     API Key), não linhas de banco — o api-gateway não possui papel de banco
--     nem tabela (design §7.9). Por isso este seed CATALOGA equipes e usuários
--     como referência em comentário, e materializa como dados apenas o que tem
--     tabela: leitos e pacientes.
--
-- As internações NÃO são semeadas de propósito: uma admissão nasce da operação
-- RPC `internacao.admitir` no admission-service, que grava a internação e o
-- Envelope de `paciente.admitido`/`leito.ocupado` no outbox na MESMA transação.
-- Inserir internação direta aqui pularia esse fluxo de eventos e deixaria a
-- projeção de leitos do api-gateway sem saber da ocupação. A demo cria as
-- internações pela borda HTTP, exercitando o caminho de mensageria de ponta a
-- ponta — que é o objetivo do trabalho.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Leitos — dado de referência estático do hospital. Código no formato
-- ^[A-Z]{3}-[0-9]{2}$ (ck_leitos_codigo) e setor em
-- ('UTI','ENFERMARIA','EMERGENCIA') (ck_leitos_setor).
-- Prefixo do código por setor: UTI-, ENF- (enfermaria), EME- (emergência).
-- -----------------------------------------------------------------------------

INSERT INTO clinico.leitos (codigo, setor, ativo) VALUES
    ('UTI-01', 'UTI',        TRUE),
    ('UTI-02', 'UTI',        TRUE),
    ('UTI-03', 'UTI',        TRUE),
    ('UTI-04', 'UTI',        TRUE),
    ('UTI-05', 'UTI',        TRUE),
    ('UTI-06', 'UTI',        FALSE),   -- leito desativado (Desativado, §7.2.3)
    ('ENF-01', 'ENFERMARIA', TRUE),
    ('ENF-02', 'ENFERMARIA', TRUE),
    ('ENF-03', 'ENFERMARIA', TRUE),
    ('ENF-04', 'ENFERMARIA', TRUE),
    ('ENF-05', 'ENFERMARIA', TRUE),
    ('ENF-06', 'ENFERMARIA', TRUE),
    ('ENF-07', 'ENFERMARIA', TRUE),
    ('ENF-08', 'ENFERMARIA', TRUE),
    ('ENF-09', 'ENFERMARIA', TRUE),
    ('ENF-10', 'ENFERMARIA', TRUE),
    ('EME-01', 'EMERGENCIA', TRUE),
    ('EME-02', 'EMERGENCIA', TRUE),
    ('EME-03', 'EMERGENCIA', TRUE),
    ('EME-04', 'EMERGENCIA', TRUE)
ON CONFLICT (codigo) DO NOTHING;


-- -----------------------------------------------------------------------------
-- Pacientes — pessoas físicas FICTÍCIAS (C6). O documento é dado pessoal e
-- nunca aparece em log (R5.6); aqui é apenas um identificador único fictício.
-- Restrições: sexo IN ('M','F','O'); nome com 2..200 caracteres (btrim);
-- data_nascimento entre 1900-01-01 e a data corrente.
-- Criar paciente não emite evento de domínio (não há routing key
-- `paciente.criado` no contrato canônico), então semeá-los aqui não pula
-- nenhum fluxo de mensageria.
-- -----------------------------------------------------------------------------

INSERT INTO clinico.pacientes (nome, data_nascimento, documento, sexo) VALUES
    ('Maria Souza',           DATE '1958-03-11', '000.000.001-91', 'F'),
    ('João Pereira',          DATE '1971-09-27', '000.000.002-72', 'M'),
    ('Ana Beatriz Lima',      DATE '1985-12-02', '000.000.003-53', 'F'),
    ('Carlos Eduardo Ramos',  DATE '1949-07-19', '000.000.004-34', 'M'),
    ('Fernanda Oliveira',     DATE '1993-05-06', '000.000.005-15', 'F'),
    ('Roberto Carvalho',      DATE '1967-01-30', '000.000.006-04', 'M'),
    ('Luiza Martins',         DATE '2001-11-14', '000.000.007-88', 'F'),
    ('Paulo Henrique Dias',   DATE '1976-08-23', '000.000.008-69', 'M'),
    ('Sofia Almeida',         DATE '1990-02-17', '000.000.009-40', 'O'),
    ('Antônio Ferreira Gomes',DATE '1943-10-05', '000.000.010-83', 'M')
ON CONFLICT (documento) DO NOTHING;


-- =============================================================================
-- Catálogo de referência (NÃO é dado de banco — sem tabela no modelo físico).
-- =============================================================================
--
-- Equipes de plantão (valor da coluna clinico.internacoes.equipe / payload
-- `equipe_responsavel` da RPC `internacao.admitir`):
--     'Equipe UTI A'   — intensivistas, cobre os leitos UTI-*
--     'Equipe UTI B'   — intensivistas, plantão noturno
--     'Clinica Medica' — enfermaria ENF-*
--     'Emergencia'     — pronto-socorro EME-*
--
-- Usuários e dispositivos de demonstração — identidades de autenticação do
-- api-gateway (JWT HS256 para pessoas; API Key para dispositivos). NÃO existem
-- como linha de banco; são credenciais configuradas no gateway (design §4.2.1,
-- hospitalmq/auth.py). Reproduzidos aqui só como referência da demo:
--     sub='enf.marta'  role='enfermeiro'  tipo='usuario'
--     sub='enf.joao'   role='enfermeiro'  tipo='usuario'
--     sub='med.helena' role='medico'      tipo='usuario'
--     sub='aud.lgpd'   role='auditor'     tipo='usuario'
--     sub='adm.raiz'   role='admin'       tipo='usuario'
--     sub='leito-uti-03' role='dispositivo' tipo='dispositivo'  (Cliente_Leito)
--     sub='leito-enf-07' role='dispositivo' tipo='dispositivo'  (Cliente_Leito)
-- =============================================================================
