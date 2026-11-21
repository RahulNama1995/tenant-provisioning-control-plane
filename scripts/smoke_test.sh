#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://localhost:8000}"
created=$(curl -fsS -X POST "$BASE_URL/tenants" -H 'Content-Type: application/json' -d '{"slug":"smoke-test","name":"Smoke Test"}')
echo "$created"
tenant_id=$(python -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<< "$created")
for _ in $(seq 1 30); do
  tenant=$(curl -fsS "$BASE_URL/tenants/$tenant_id")
  status=$(python -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<< "$tenant")
  [[ "$status" == "active" ]] && { echo "Lifecycle complete: $tenant"; exit 0; }
  sleep 1
done
echo "Tenant did not become active" >&2
exit 1
