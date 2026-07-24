# Como contribuir

Projeto acadêmico de Sistemas Distribuídos — Grupo G3, tema Hospital Inteligente, middleware do
tipo *Message Queue*. Este documento cobre as convenções de código e como rodar as verificações
localmente. O artefato avaliado é `hospitalmq/`; é nele que o rigor é maior.

## Ambiente local

Python 3.12 (piso declarado em `requires-python` e base da imagem Docker) e [uv](https://docs.astral.sh/uv/):

```bash
uv venv --python 3.12
source .venv/bin/activate
uv pip install -e ".[borda,dominio,dev]"
```

Os três extras juntos porque `dominio` traz o SQLAlchemy que o `mypy` precisa para resolver os
imports de `hospitalmq/{publisher,idempotency,rpc}.py`, e `borda` + `dominio` cobrem os módulos de
`services/` que `tests/unit` importa por caminho dinâmico.

## Verificações antes de abrir PR

Os quatro comandos abaixo são exatamente os que a CI executa (`.github/workflows/ci.yml`):

```bash
ruff check hospitalmq services clients tests
ruff format --check hospitalmq services clients tests
mypy                       # sem argumentos: o alvo vem do pyproject
pytest tests/unit          # 117 testes
```

Nível *end-to-end*, que exige a stack completa (RabbitMQ, PostgreSQL e os seis serviços) e por isso
não roda na CI:

```bash
docker compose up -d
docker compose --profile test run --rm testes   # 8 testes de tests/e2e
```

> **Pendência conhecida:** quatro arquivos ainda divergem do formatador —
> `services/api-gateway/erros.py`, `services/api-gateway/rotas/auth.py`,
> `services/api-gateway/rotas/painel.py` e `tests/e2e/test_fluxo_clinico.py`. A diferença é só de
> quebra de linha e `ruff check` passa nos quatro. Enquanto isso, o passo `ruff format --check` da
> CI é marcado com `continue-on-error`. Ao rodar `ruff format` nesses arquivos, remova essa marca.

## Convenções de código

**Formatação e lint.** `ruff` é a única ferramenta de estilo; a configuração está em
`[tool.ruff]` no `pyproject.toml`. Linha de até 100 colunas, alvo `py312`, conjunto de regras
`E, F, W, I, UP, B, SIM, C4, ANN, D, ASYNC, RUF`. Não desative regra em linha (`# noqa`) sem um
comentário ao lado dizendo por quê.

**Anotações de tipo.** Obrigatórias em `hospitalmq/` — parâmetros e retornos, inclusive privados.
`mypy` roda em modo `strict` e o `pyproject` fixa `files = ["hospitalmq"]`: o middleware é o
artefato avaliado e não abre exceção. Fora dele, `ANN` continua ativo em `services/` e `clients/`
(só `tests/` e `scripts/` são dispensados), mas a checagem de tipos não é bloqueante.

**Docstrings.** Convenção Google, em português, obrigatórias nas APIs públicas do middleware —
módulo, classe pública e método público de `hospitalmq/`. A regra `D` é desligada por
`per-file-ignores` em `services/`, `clients/`, `scripts/` e `tests/`: lá o comentário é opcional e
deve explicar *por que*, não *o que*. A docstring de módulo do middleware referencia a seção
correspondente do design (`specs/middleware-mensageria-hospitalar/design.md`); mantenha essa
referência ao mexer no arquivo.

**Idioma dos nomes.** Domínio em português, infraestrutura em inglês — a fronteira é o que ajuda a
ler o código:

| Camada | Idioma | Exemplos reais no repositório |
| --- | --- | --- |
| Domínio clínico | português | `paciente_id`, `leito_codigo`, `alerta_gerado`, `emitir_no_outbox`, `PoliticaRetentativa` |
| Infraestrutura de mensageria | inglês | `Publisher`, `Consumer`, `Transport`, `queue`, `payload`, `correlation_id` |

Identificadores são sempre ASCII, sem acentuação, mesmo quando a palavra é portuguesa
(`retentativa`, `prontuario`, `sessao`). O texto em prosa — docstrings, comentários e documentação
— usa português normal.

## Mensagens de commit

*Conventional Commits* com assunto em português:

```
tipo(escopo): assunto no imperativo, minúscula, sem ponto final
```

Tipos aceitos: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`. Os escopos já usados no histórico
são `hospitalmq`, `servicos`, `borda` e `gateway`; novos escopos devem nomear um diretório de
primeiro nível ou um serviço. O corpo explica a motivação e o que mudou de
comportamento, e termina com como a mudança foi verificada (por exemplo: "117 unit + 8 e2e
verdes"). Exemplo real do histórico:

```
fix(gateway): X-Correlation-ID malformado não derruba escrita clínica
```

## Integração contínua

`.github/workflows/ci.yml` roda em todo `push` e `pull_request`, em quatro jobs paralelos: `lint`,
`tipos`, `testes` (com relatório de cobertura publicado no sumário do job) e `build` (valida
`docker compose config` e constrói as duas imagens, `borda` e `dominio`, sem subir a stack). Um PR
só deve ser mesclado com os quatro verdes.

## Escopo de alterações

`specs/middleware-mensageria-hospitalar/design.md` e `requirements.md` são normativos: o código se
ajusta ao documento, não o contrário. Divergência encontrada durante a implementação vira issue
antes de virar commit.

<!-- PREENCHER: integrantes do G3 -->

| Integrante | Papel |
| --- | --- |
| (preencher) | (preencher) |
