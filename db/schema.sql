-- =============================================================================
-- db/schema.sql — Hospital Inteligente (grupo G3)
--
-- DDL PostgreSQL 16, fonte da verdade do modelo físico (design §7.4). Todos os
-- modelos SQLAlchemy dos serviços espelham exatamente as tabelas, colunas,
-- tipos e constraints deste arquivo.
--
-- Executado integralmente pelo serviço one-shot `db-init` do Docker Compose
-- (design §7.8.3), como dono do banco (`hospital`), com `psql -v ON_ERROR_STOP=1`.
-- É IDEMPOTENTE: rodar duas vezes não produz erro nem duplica objeto
-- (CREATE ... IF NOT EXISTS, DO $$ ... $$ para papéis, CREATE OR REPLACE para
-- funções e triggers).
--
-- A ORDEM É PARTE DO DESENHO (design §12.3.4): os papéis e o
-- `ALTER DEFAULT PRIVILEGES` de 7.4.1 rodam ANTES de qualquer CREATE TABLE e
-- como dono, o que é o que faz os GRANT/REVOKE valerem para os papéis svc_*.
--
-- PostgreSQL 16: gen_random_uuid() é nativa do core desde a versão 13; não é
-- necessário CREATE EXTENSION pgcrypto.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 7.4.1  Schemas, papéis e privilégios
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS clinico;
CREATE SCHEMA IF NOT EXISTS vitais;
CREATE SCHEMA IF NOT EXISTS triagem;
CREATE SCHEMA IF NOT EXISTS alertas;
CREATE SCHEMA IF NOT EXISTS auditoria;

-- Um papel de login por serviço. As senhas abaixo são de demonstração (C6);
-- na proposta AWS elas dão lugar a IAM database authentication (ver seção de AWS).
DO $papeis$
DECLARE
    par text[];
BEGIN
    FOREACH par SLICE 1 IN ARRAY ARRAY[
        ['svc_admission', 'clinico'],
        ['svc_vitals',    'vitais'],
        ['svc_triage',    'triagem'],
        ['svc_alert',     'alertas'],
        ['svc_audit',     'auditoria']
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = par[1]) THEN
            EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', par[1], 'demo-' || par[1]);
        END IF;
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', par[2], par[1]);
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I
                 GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', par[2], par[1]);
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I
                 GRANT USAGE, SELECT ON SEQUENCES TO %I', par[2], par[1]);
    END LOOP;
END;
$papeis$;

-- O api-gateway não recebe papel de banco: ele não tem tabela (ver §7.9).


-- -----------------------------------------------------------------------------
-- 7.4.2  Schema `clinico` — admission-service
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clinico.pacientes (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    nome             TEXT        NOT NULL,
    data_nascimento  DATE        NOT NULL,
    documento        TEXT        NOT NULL,
    sexo             CHAR(1)     NOT NULL,
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_pacientes_documento   UNIQUE (documento),
    CONSTRAINT ck_pacientes_sexo        CHECK (sexo IN ('M', 'F', 'O')),
    CONSTRAINT ck_pacientes_nome        CHECK (length(btrim(nome)) BETWEEN 2 AND 200),
    CONSTRAINT ck_pacientes_nascimento  CHECK (data_nascimento <= CURRENT_DATE
                                               AND data_nascimento >= DATE '1900-01-01')
);

CREATE INDEX IF NOT EXISTS ix_pacientes_nome
    ON clinico.pacientes (lower(nome));

CREATE TABLE IF NOT EXISTS clinico.leitos (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    codigo     TEXT        NOT NULL,
    setor      TEXT        NOT NULL,
    ativo      BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_leitos_codigo  UNIQUE (codigo),
    CONSTRAINT ck_leitos_codigo  CHECK (codigo ~ '^[A-Z]{3}-[0-9]{2}$'),
    CONSTRAINT ck_leitos_setor   CHECK (setor IN ('UTI', 'ENFERMARIA', 'EMERGENCIA'))
);

CREATE TABLE IF NOT EXISTS clinico.internacoes (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    paciente_id  UUID        NOT NULL REFERENCES clinico.pacientes (id) ON DELETE RESTRICT,
    leito_id     UUID        NOT NULL REFERENCES clinico.leitos (id)    ON DELETE RESTRICT,
    admitido_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    alta_em      TIMESTAMPTZ,
    motivo       TEXT        NOT NULL,
    equipe       TEXT        NOT NULL,
    CONSTRAINT ck_internacoes_alta_posterior
        CHECK (alta_em IS NULL OR alta_em >= admitido_em),            -- INV-3
    CONSTRAINT ck_internacoes_motivo
        CHECK (length(btrim(motivo)) BETWEEN 3 AND 500)
);

-- INV-1: um Leito não pode ter duas Internacoes ativas.
CREATE UNIQUE INDEX IF NOT EXISTS uq_internacoes_leito_ativo
    ON clinico.internacoes (leito_id)
    WHERE alta_em IS NULL;

-- INV-6: um Paciente não pode estar internado em dois Leitos ao mesmo tempo.
CREATE UNIQUE INDEX IF NOT EXISTS uq_internacoes_paciente_ativo
    ON clinico.internacoes (paciente_id)
    WHERE alta_em IS NULL;

-- Prontuário: histórico do paciente, mais recente primeiro (RPC de R7.2).
CREATE INDEX IF NOT EXISTS ix_internacoes_paciente_tempo
    ON clinico.internacoes (paciente_id, admitido_em DESC);

-- Painel: enumerar leitos ocupados sem varrer o histórico inteiro (R11.1).
CREATE INDEX IF NOT EXISTS ix_internacoes_ativas
    ON clinico.internacoes (leito_id, admitido_em DESC)
    WHERE alta_em IS NULL;


-- -----------------------------------------------------------------------------
-- 7.4.3  Schema `vitais` — vitals-service
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS vitais.sinais_vitais (
    id                       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    internacao_id            UUID         NOT NULL,   -- sem FK: cross-service (D2)
    leito_codigo             TEXT         NOT NULL,   -- desnormalizado para o painel
    coletado_em              TIMESTAMPTZ  NOT NULL,
    registrado_em            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    frequencia_respiratoria  SMALLINT     NOT NULL,
    saturacao_o2             SMALLINT     NOT NULL,
    oxigenio_suplementar     BOOLEAN      NOT NULL,
    temperatura              NUMERIC(4,1) NOT NULL,
    pressao_sistolica        SMALLINT     NOT NULL,
    frequencia_cardiaca      SMALLINT     NOT NULL,
    nivel_consciencia        CHAR(1)      NOT NULL,
    origem_message_id        UUID         NOT NULL,
    correlation_id           UUID         NOT NULL,

    -- Faixas fisiológicas aceitas (R6.6). Última linha de defesa: a mesma faixa
    -- é validada por Pydantic na borda e no consumidor (ver §7.7).
    CONSTRAINT ck_sv_fr    CHECK (frequencia_respiratoria BETWEEN 0   AND 80),
    CONSTRAINT ck_sv_spo2  CHECK (saturacao_o2            BETWEEN 50  AND 100),
    CONSTRAINT ck_sv_temp  CHECK (temperatura             BETWEEN 25.0 AND 45.0),
    CONSTRAINT ck_sv_pas   CHECK (pressao_sistolica       BETWEEN 40  AND 300),
    CONSTRAINT ck_sv_fc    CHECK (frequencia_cardiaca     BETWEEN 20  AND 250),
    CONSTRAINT ck_sv_avpu  CHECK (nivel_consciencia IN ('A', 'V', 'P', 'U')),
    CONSTRAINT ck_sv_coleta_nao_futura
        CHECK (coletado_em <= registrado_em + INTERVAL '60 seconds'),
    CONSTRAINT uq_sv_origem UNIQUE (origem_message_id)     -- idempotência natural
);

-- Índice mais importante da tabela: última leitura de uma internação (R11.1)
-- e série temporal para o prontuário. Ordem DESC evita sort no plano.
CREATE INDEX IF NOT EXISTS ix_sv_internacao_coleta
    ON vitais.sinais_vitais (internacao_id, coletado_em DESC);

-- Rastreamento ponta a ponta pelos logs (R5.3).
CREATE INDEX IF NOT EXISTS ix_sv_correlation
    ON vitais.sinais_vitais (correlation_id);

-- INV-4: SinaisVitais é imutável.
CREATE OR REPLACE FUNCTION vitais.fn_bloqueia_mutacao() RETURNS trigger AS $imutavel$
BEGIN
    RAISE EXCEPTION 'registro clinico imutavel: % negado em %.%',
        TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END;
$imutavel$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_sv_imutavel
    BEFORE UPDATE OR DELETE ON vitais.sinais_vitais
    FOR EACH ROW EXECUTE FUNCTION vitais.fn_bloqueia_mutacao();

-- O trigger e o REVOKE são redundantes de propósito: o REVOKE impede a operação
-- no papel usado pela aplicação; o trigger impede a operação até para um
-- superusuário que abrir psql no container. INV-4 é requisito de registro
-- clínico, não conveniência.
REVOKE UPDATE, DELETE, TRUNCATE ON vitais.sinais_vitais FROM svc_vitals;


-- -----------------------------------------------------------------------------
-- 7.4.4  Schema `alertas` — alert-service
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alertas.alertas (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    internacao_id           UUID        NOT NULL,   -- sem FK: cross-service (D2)
    leito_codigo            TEXT        NOT NULL,
    sinais_vitais_id        UUID        NOT NULL,   -- sem FK: cross-service (D2)
    score_total             SMALLINT    NOT NULL,
    severidade              TEXT        NOT NULL,
    componente_critico      BOOLEAN     NOT NULL,
    componentes             JSONB       NOT NULL,
    gerado_em               TIMESTAMPTZ NOT NULL,
    notificado_em           TIMESTAMPTZ,
    canal                   TEXT,
    tentativas_notificacao  SMALLINT    NOT NULL DEFAULT 0,
    ultimo_erro             TEXT,
    origem_message_id       UUID        NOT NULL,
    correlation_id          UUID        NOT NULL,

    CONSTRAINT uq_alertas_origem UNIQUE (origem_message_id),
    CONSTRAINT ck_alertas_score  CHECK (score_total BETWEEN 0 AND 20),
    CONSTRAINT ck_alertas_tentativas
        CHECK (tentativas_notificacao BETWEEN 0 AND 4),

    -- INV-5: a severidade gravada é exatamente a que a regra de R6.3 produz.
    CONSTRAINT ck_alertas_severidade_coerente CHECK (
        severidade = CASE
            WHEN score_total >= 5 OR componente_critico THEN 'alta'
            WHEN score_total >= 3                        THEN 'media'
            ELSE                                              'baixa'
        END
    ),

    -- O JSONB de componentes deve trazer os sete parâmetros pontuados.
    CONSTRAINT ck_alertas_componentes CHECK (
        componentes ?& ARRAY[
            'frequencia_respiratoria', 'saturacao_o2', 'oxigenio_suplementar',
            'temperatura', 'pressao_sistolica', 'frequencia_cardiaca',
            'nivel_consciencia'
        ]
    ),
    CONSTRAINT ck_alertas_notificacao_coerente
        CHECK ((notificado_em IS NULL) = (canal IS NULL))
);

-- Lista de alertas do painel, mais recentes primeiro (R11.4).
CREATE INDEX IF NOT EXISTS ix_alertas_leito_tempo
    ON alertas.alertas (leito_codigo, gerado_em DESC);

-- Fila de trabalho: alertas ainda não despachados (R6.4/R6.5).
CREATE INDEX IF NOT EXISTS ix_alertas_pendentes
    ON alertas.alertas (gerado_em)
    WHERE notificado_em IS NULL;

-- Consulta analítica por componente pontuado, sem varredura sequencial.
CREATE INDEX IF NOT EXISTS ix_alertas_componentes
    ON alertas.alertas USING GIN (componentes jsonb_path_ops);


-- -----------------------------------------------------------------------------
-- 7.4.5  Schema `auditoria` — audit-service
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS auditoria.eventos_auditoria (
    id               BIGSERIAL   PRIMARY KEY,
    message_id       UUID        NOT NULL,
    correlation_id   UUID        NOT NULL,
    causation_id     UUID,
    tipo             TEXT        NOT NULL,
    versao           SMALLINT    NOT NULL DEFAULT 1,
    routing_key      TEXT        NOT NULL,
    ocorrido_em      TIMESTAMPTZ NOT NULL,   -- timestamp do Envelope
    registrado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    produtor         TEXT        NOT NULL,
    identidade_sub   TEXT,
    identidade_role  TEXT,
    identidade_tipo  TEXT,
    paciente_id      UUID,                   -- extraído do payload quando presente
    payload          JSONB       NOT NULL,

    -- Idempotência natural: a marca de processamento é a própria linha (ver §7.9).
    CONSTRAINT uq_ea_message UNIQUE (message_id),

    -- Vocabulário de identity.tipo do Envelope.
    -- DIVERGÊNCIA JUSTIFICADA (não silenciosa) em relação ao texto literal de
    -- §7.4.5, que grafava ('humano','dispositivo','sistema'). O middleware
    -- `hospitalmq` — já implementado e congelado — define
    -- `TipoIdentidade = Literal["usuario","dispositivo"]` (hospitalmq/envelope.py:45)
    -- e `Identity.from_dict` REJEITA qualquer outro valor no limite do Envelope.
    -- Como o audit-service grava `Envelope.identity.tipo` LITERALMENTE nesta
    -- coluna, os únicos valores que podem chegar são 'usuario', 'dispositivo' ou
    -- NULL (processo interno sem credencial usa identity = null, não um terceiro
    -- valor). Manter ('humano', ...) faria TODO evento originado por pessoa
    -- (paciente.admitido, paciente.alta, prontuario.consultado) falhar no INSERT
    -- com IntegrityError → retentativas → DLQ, deixando a Trilha_de_Auditoria
    -- vazia justamente para os acessos humanos que a LGPD exige rastrear (R7.1,
    -- R7.4). A decisão normativa de §4.2.1 ("Domínio fechado de identity.tipo")
    -- e a tabela anti-drift de §3 (linha "Domínio de identity.tipo") já fixam
    -- ('usuario','dispositivo') como o vocabulário canônico e listam
    -- 'humano'/'sistema' como "erro a evitar". Este CHECK segue a decisão
    -- normativa e o código do middleware.
    CONSTRAINT ck_ea_identidade_tipo
        CHECK (identidade_tipo IS NULL
               OR identidade_tipo IN ('usuario', 'dispositivo'))
);

-- Reconstituir uma requisição ponta a ponta (R5.3, R7.1).
CREATE INDEX IF NOT EXISTS ix_ea_correlation
    ON auditoria.eventos_auditoria (correlation_id, registrado_em);

-- Relatório LGPD: tudo o que aconteceu com um paciente (R7.3).
CREATE INDEX IF NOT EXISTS ix_ea_paciente
    ON auditoria.eventos_auditoria (paciente_id, registrado_em DESC)
    WHERE paciente_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_ea_tipo_tempo
    ON auditoria.eventos_auditoria (tipo, registrado_em DESC);

CREATE INDEX IF NOT EXISTS ix_ea_payload
    ON auditoria.eventos_auditoria USING GIN (payload jsonb_path_ops);

-- INV-7: somente inserção (R7.4).
CREATE OR REPLACE FUNCTION auditoria.fn_bloqueia_mutacao() RETURNS trigger AS $somente_insert$
BEGIN
    RAISE EXCEPTION 'trilha de auditoria e somente-insercao: % negado', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$somente_insert$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_ea_somente_insert
    BEFORE UPDATE OR DELETE ON auditoria.eventos_auditoria
    FOR EACH ROW EXECUTE FUNCTION auditoria.fn_bloqueia_mutacao();

REVOKE UPDATE, DELETE, TRUNCATE ON auditoria.eventos_auditoria FROM svc_audit;


-- -----------------------------------------------------------------------------
-- 7.4.6  Tabelas de middleware
--
-- Duas tabelas com ESCOPOS DIFERENTES:
--   * mensagens_processadas  → clinico, vitais, triagem, alertas (todo serviço
--     que consome mensagem suprime reentrega duplicada, R2.4). Fica de fora
--     apenas auditoria, cuja idempotência é natural (§7.9.1).
--   * outbox_mensagens       → SOMENTE clinico (D6 / ADR-005): outbox só onde a
--     perda de um evento produz inconsistência permanente. Telemetria publica
--     direto depois do COMMIT.
-- -----------------------------------------------------------------------------

-- Bloco 1: marca de idempotência nos quatro schemas que consomem mensagem.
DO $middleware$
DECLARE
    esquema text;
BEGIN
    FOREACH esquema IN ARRAY ARRAY['clinico', 'vitais', 'triagem', 'alertas'] LOOP

        EXECUTE format($ddl$
            CREATE TABLE IF NOT EXISTS %1$I.mensagens_processadas (
                consumidor      TEXT        NOT NULL,
                message_id      UUID        NOT NULL,
                tipo            TEXT        NOT NULL,
                correlation_id  UUID        NOT NULL,
                tentativa       SMALLINT    NOT NULL DEFAULT 1,
                processado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT pk_%1$s_mensagens_processadas
                    PRIMARY KEY (consumidor, message_id)
            );
            CREATE INDEX IF NOT EXISTS ix_%1$s_msgproc_purga
                ON %1$I.mensagens_processadas (processado_em);
        $ddl$, esquema);

    END LOOP;
END;
$middleware$;

-- Bloco 2: outbox transacional APENAS no admission-service (D6, ADR-005).
-- Não há equivalente em vitais, triagem nem alertas: esses três publicam
-- diretamente pelo Publisher depois do COMMIT.
CREATE TABLE IF NOT EXISTS clinico.outbox_mensagens (
    id            BIGSERIAL   PRIMARY KEY,
    message_id    UUID        NOT NULL,
    tipo          TEXT        NOT NULL,
    routing_key   TEXT        NOT NULL,
    envelope      JSONB       NOT NULL,     -- Envelope completo, pronto para publicar
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    publicado_em  TIMESTAMPTZ,
    tentativas    SMALLINT    NOT NULL DEFAULT 0,
    ultimo_erro   TEXT,
    CONSTRAINT uq_clinico_outbox_message UNIQUE (message_id)
);

CREATE INDEX IF NOT EXISTS ix_clinico_outbox_pendentes
    ON clinico.outbox_mensagens (criado_em)
    WHERE publicado_em IS NULL;
