#!/usr/bin/env bash
# trace.sh — filtra o log agregado do Compose por correlation_id (design 9.6, 12.7).
#
# Todas as linhas de log sao JSON (secao 9). Este script le os logs de todos os
# servicos, descarta o que nao for JSON valido e mantem so as linhas cujo campo
# `correlation_id` casa com o pedido — a trilha completa de uma requisicao
# atravessando gateway, filas e os consumidores.
#
# Uso:
#   ./scripts/trace.sh <correlation_id>
#   ./scripts/trace.sh "$CID"
#   CID=... ./scripts/trace.sh          # tambem le da variavel de ambiente CID
set -euo pipefail

CID="${1:-${CID:-}}"
if [ -z "${CID}" ]; then
  echo "uso: ./scripts/trace.sh <correlation_id>" >&2
  exit 2
fi

# --no-log-prefix: remove o "servico  |" que o Compose antepoe, que quebraria o jq.
# fromjson? // empty: descarta silenciosamente qualquer linha nao-JSON.
docker compose logs --no-log-prefix \
  | jq -Rc --arg cid "${CID}" 'fromjson? // empty | select(.correlation_id == $cid)'
