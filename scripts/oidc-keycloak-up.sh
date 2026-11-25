#!/usr/bin/env bash
# Start Keycloak + write graphql-oidc-keycloak.env for e2e (local + CI).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="$ROOT/tests/graphql/graphql_oidc_keycloak/docker-compose.yml"
OUT="${GRAPHQL_OIDC_KEYCLOAK_ENV:-$ROOT/graphql-oidc-keycloak.env}"
ISSUER_HOST="${KEYCLOAK_ISSUER:-http://localhost:8081}"
REALM=graphql
ISSUER="${ISSUER_HOST}/realms/${REALM}"

cd "$ROOT"
echo "==> docker compose up Keycloak"
docker compose -f "$COMPOSE" up -d --remove-orphans

echo "==> Wait for OIDC discovery at $ISSUER"
for i in $(seq 1 90); do
  if curl -fsS "${ISSUER}/.well-known/openid-configuration" >/dev/null 2>&1; then
    echo "    discovery ready (${i}s)"
    break
  fi
  if ! docker compose -f "$COMPOSE" ps --status running --services 2>/dev/null | grep -q keycloak; then
    echo "ERROR: keycloak not running"
    docker compose -f "$COMPOSE" logs --tail=80 || true
    exit 1
  fi
  sleep 2
  if [[ $i -eq 90 ]]; then
    echo "ERROR: Keycloak discovery never ready"
    docker compose -f "$COMPOSE" logs --tail=100 || true
    exit 1
  fi
done

# Wait until client_credentials succeeds (realm import may lag discovery)
echo "==> Wait for client_credentials mint"
TOKEN_URL="${ISSUER}/protocol/openid-connect/token"
for i in $(seq 1 60); do
  code=$(curl -sS -o /tmp/kc-token.json -w '%{http_code}' -X POST "$TOKEN_URL" \
    -d "grant_type=client_credentials" \
    -d "client_id=graphql-e2e-customer" \
    -d "client_secret=customer-secret-e2e" || echo 000)
  if [[ "$code" == "200" ]]; then
    echo "    mint ready (${i}s)"
    break
  fi
  sleep 2
  if [[ $i -eq 60 ]]; then
    echo "ERROR: client_credentials still failing (HTTP $code)"
    cat /tmp/kc-token.json 2>/dev/null || true
    exit 1
  fi
done

# Capture audience from access token (Keycloak often uses client_id or "account")
AUD=$(python3 - <<PY
import json,base64,urllib.request,urllib.parse
url="${TOKEN_URL}"
data=urllib.parse.urlencode({
  "grant_type":"client_credentials",
  "client_id":"graphql-e2e-customer",
  "client_secret":"customer-secret-e2e",
}).encode()
req=urllib.request.Request(url,data=data,headers={"Content-Type":"application/x-www-form-urlencoded"})
tok=json.load(urllib.request.urlopen(req))["access_token"]
payload=tok.split(".")[1]
payload += "=" * (-len(payload) % 4)
claims=json.loads(base64.urlsafe_b64decode(payload))
print("claims keys", sorted(claims.keys()), file=__import__("sys").stderr)
print("aud", claims.get("aud"), "azp", claims.get("azp"), file=__import__("sys").stderr)
aud=claims.get("aud")
azp=claims.get("azp")
# Prefer azp (authorized party / client id) when aud is account or list
if isinstance(aud, list):
  if azp:
    print(azp)
  else:
    print(aud[0] if aud else "account")
elif aud and aud != "account":
  print(aud)
elif azp:
  print(azp)
else:
  print(aud or "graphql-e2e-customer")
PY
)

cat > "$OUT" <<EOF
KEYCLOAK_E2E=1
OIDC_ISSUER=$ISSUER
OIDC_AUDIENCE=$AUD
OIDC_CLIENT_ID=graphql-e2e-customer
KEYCLOAK_E2E_CUSTOMER_CLIENT_ID=graphql-e2e-customer
KEYCLOAK_E2E_CUSTOMER_CLIENT_SECRET=customer-secret-e2e
KEYCLOAK_E2E_ADMIN_CLIENT_ID=graphql-e2e-admin
KEYCLOAK_E2E_ADMIN_CLIENT_SECRET=admin-secret-e2e
EOF
echo "==> Wrote $OUT"
cat "$OUT"
echo "==> Done. source $OUT && cargo test --test graphql --features graphql,sqlite,metrics graphql_oidc_keycloak::"
