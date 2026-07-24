#!/usr/bin/env bash
# sinais.sh — publica UMA leitura de sinais vitais como dispositivo (design 12.7, R4.5, R6.1).
#
# Autentica por X-API-Key (so o tipo `dispositivo` publica telemetria) e faz
# POST /sinais. O gateway responde 202 (aceite assincrono) e devolve o
# correlation_id no cabecalho X-Correlation-ID, capturado aqui em CID.
#
# ATENCAO ao contrato real do gateway ja implementado (services/api-gateway):
#   * a rota e POST /sinais (o leito vai NO CORPO, campo `leito_id`), e nao
#     /leitos/{cod}/sinais como aparece no exemplo do design 12.7;
#   * o campo de saturacao chama-se `saturacao_o2` (nao `saturacao`).
#
# Uso:
#   ./scripts/sinais.sh                          # UTI-01 com a API Key dev-monitor-l07
#   ./scripts/sinais.sh UTI-03 dev-monitor-uti03 # outro leito/chave
#
# Variaveis:
#   GATEWAY_URL  endereco do gateway (padrao http://localhost:8000)
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8000}"
LEITO="${1:-UTI-01}"
API_KEY="${2:-dev-monitor-l07}"

# coletado_em e obrigatorio no contrato de POST /sinais (schemas.SinaisVitaisRequest).
COLETADO_EM="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

HDRS="$(mktemp)"
trap 'rm -f "${HDRS}"' EXIT

# Valores deliberadamente alterados (sat 91, temp 38.4, FC 122): geram NEWS2 alto
# e, portanto, um alerta observavel no painel e no audit-service.
CORPO=$(cat <<JSON
{
  "leito_id": "${LEITO}",
  "frequencia_respiratoria": 26,
  "saturacao_o2": 91,
  "oxigenio_suplementar": true,
  "temperatura": 38.4,
  "pressao_sistolica": 96,
  "frequencia_cardiaca": 122,
  "nivel_consciencia": "A",
  "coletado_em": "${COLETADO_EM}"
}
JSON
)

echo "POST ${GATEWAY_URL}/sinais  (leito=${LEITO}, X-API-Key=${API_KEY})"
curl -sS -D "${HDRS}" -X POST "${GATEWAY_URL}/sinais" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d "${CORPO}" | jq

CID=$(awk -F': ' 'tolower($1)=="x-correlation-id"{print $2}' "${HDRS}" | tr -d '\r')
echo
echo "correlation_id: ${CID}"
echo "rastreie com:   ./scripts/trace.sh ${CID}"
