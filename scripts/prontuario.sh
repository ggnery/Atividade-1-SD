#!/usr/bin/env bash
# prontuario.sh — consulta o prontuario de um paciente via RPC sobre fila (design 12.7, R3.1, R7.2).
#
# GET /pacientes/{id}/prontuario exige um JWT (papel de pessoa). O gateway NAO le
# o banco clinico: traduz a chamada em um RPC `prontuario.consultar` sobre fila
# ao admission-service (8.1.1). Se o token nao vier em TOKEN nem como 2o argumento,
# o script obtem um automaticamente via scripts/token.sh (enf.ana / demo123).
#
# Uso:
#   ./scripts/prontuario.sh <paciente_id>
#   ./scripts/prontuario.sh <paciente_id> "$TOKEN"
#   TOKEN=$(./scripts/token.sh) ./scripts/prontuario.sh <paciente_id>
#
# Variaveis:
#   GATEWAY_URL  endereco do gateway (padrao http://localhost:8000)
#   TOKEN        JWT a usar; se ausente, e obtido via token.sh
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8000}"
PACIENTE_ID="${1:-}"
if [ -z "${PACIENTE_ID}" ]; then
  echo "uso: ./scripts/prontuario.sh <paciente_id> [token]" >&2
  exit 2
fi

TOKEN="${2:-${TOKEN:-}}"
if [ -z "${TOKEN}" ]; then
  TOKEN=$(GATEWAY_URL="${GATEWAY_URL}" "$(dirname "$0")/token.sh")
fi

curl -sS -H "Authorization: Bearer ${TOKEN}" \
  "${GATEWAY_URL}/pacientes/${PACIENTE_ID}/prontuario" | jq
