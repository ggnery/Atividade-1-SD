# syntax=docker/dockerfile:1.7
# Imagem unica para os seis servicos e o cliente (design 12.2). Cada processo do
# docker-compose.yml e esta mesma imagem com um `command` diferente; nao existem
# seis Dockerfiles. O ARG PERFIL seleciona o grupo de extras do pyproject.toml:
#   borda   -> api-gateway e bedside-monitor (FastAPI, uvicorn, sse-starlette, httpx); SEM asyncpg
#   dominio -> os cinco consumidores (SQLAlchemy async + asyncpg)
# A fronteira da imagem e a fronteira de acesso a dados: a imagem :borda nao traz
# driver de banco, entao um import acidental de sessao no gateway quebra no boot,
# e nao vira acesso indevido silencioso (design 12.2.3, 8.1.1).
FROM python:3.12-slim

# PYTHONUNBUFFERED e requisito da demonstracao, nao preferencia: sem ele o stdout do
# container e bufferizado em blocos de 4 KB e as linhas de log JSON da secao 9 chegam
# ao "docker compose logs" atrasadas e cortadas ao meio, quebrando o jq do roteiro.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC

WORKDIR /app

# PERFIL seleciona o grupo de extras do pyproject: "borda", "dominio" ou "dominio,dev".
ARG PERFIL=dominio

# --- camada de dependencias: invalidada apenas por mudanca no pyproject.toml ---
# O truque do hospitalmq/__init__.py vazio permite instalar o pacote local em modo
# editavel ANTES de copiar o codigo real, preservando o cache: alterar um modulo do
# middleware invalida so a camada de copia (milissegundos), nao a de instalacao.
COPY pyproject.toml ./
RUN mkdir -p hospitalmq && touch hospitalmq/__init__.py \
 && pip install -e ".[${PERFIL}]" \
 && rm -rf /root/.cache/pip

# --- camadas de codigo: baratas, invalidadas a cada commit ---
COPY hospitalmq/ ./hospitalmq/
COPY services/   ./services/
COPY clients/    ./clients/
COPY db/         ./db/

RUN useradd --create-home --uid 10001 hospital && chown -R hospital:hospital /app
USER hospital

# Sem ENTRYPOINT de servico. A imagem e um runtime compartilhado; a identidade do
# processo vem do `command` no docker-compose.yml. O CMD e um lembrete, nao um padrao util.
CMD ["python", "-c", "print('Defina o command no docker-compose.yml. Ver README secao Execucao.')"]
