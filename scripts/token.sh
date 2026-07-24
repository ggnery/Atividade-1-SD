#!/usr/bin/env bash
# token.sh — obtem um JWT do gateway e imprime na saida (design 12.7, R4.1).
#
# Uso:
#   ./scripts/token.sh                      # usa enf.ana / demo123
#   ./scripts/token.sh med.silva demo123    # outro usuario/senha
#   export TOKEN=$(./scripts/token.sh)      # captura o token numa variavel
#
# Variaveis:
#   GATEWAY_URL  endereco do gateway (padrao http://localhost:8000)
#
# Usuarios de demonstracao (services/api-gateway/rotas/auth.py, C6):
#   enf.ana enf.bia (enfermeiro) | med.silva med.joao (medico)
#   aud.paula (auditor) | admin (admin) — todos com senha demo123.
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8000}"
USUARIO="${1:-enf.ana}"
SENHA="${2:-demo123}"

TOKEN=$(curl -sS -X POST "${GATEWAY_URL}/auth/token" \
  -H "Content-Type: application/json" \
  -d "{\"usuario\":\"${USUARIO}\",\"senha\":\"${SENHA}\"}" | jq -r .access_token)

if [ -z "${TOKEN}" ] || [ "${TOKEN}" = "null" ]; then
  echo "falha ao obter token para '${USUARIO}' em ${GATEWAY_URL}" >&2
  exit 1
fi

echo "${TOKEN}"
