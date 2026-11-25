#!/usr/bin/env bash
# Bootstrap live Zitadel for GraphQL OIDC e2e (D11 JWT-bearer mint).
# Expects Zitadel from tests/graphql/graphql_oidc_zitadel/docker-compose.yml.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_DIR="$ROOT/tests/graphql/graphql_oidc_zitadel"
MACHINEKEY_DIR="$COMPOSE_DIR/machinekey"
ZITADEL_HOST="${ZITADEL_HOST:-http://localhost:8080}"
OUT="${GRAPHQL_OIDC_ENV:-$ROOT/graphql-oidc.env}"
PROJECT_NAME="${GRAPHQL_OIDC_PROJECT:-distributed-graphql}"
APP_NAME="${GRAPHQL_OIDC_APP:-graphql-api}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: $1 required"; exit 1; }; }
need jq
need curl
need openssl

b64url() { openssl base64 -e -A | tr '+/' '-_' | tr -d '='; }

echo "==> Waiting for Zitadel at $ZITADEL_HOST ..."
for i in $(seq 1 90); do
  if curl -fsS "$ZITADEL_HOST/debug/ready" >/dev/null 2>&1; then
    echo "    ready"
    break
  fi
  sleep 2
  if [[ $i -eq 90 ]]; then
    echo "ERROR: Zitadel never ready"
    exit 1
  fi
done

# Wait for FirstInstance machine key
KEYFILE=""
for i in $(seq 1 60); do
  shopt -s nullglob
  keys=("$MACHINEKEY_DIR"/*.json)
  shopt -u nullglob
  if [[ ${#keys[@]} -gt 0 ]]; then
    KEYFILE="${keys[0]}"
    break
  fi
  sleep 2
done
if [[ -z "$KEYFILE" || ! -s "$KEYFILE" ]]; then
  echo "ERROR: no machine key in $MACHINEKEY_DIR (FirstInstance steps)"
  exit 1
fi
echo "==> Using machine key: $KEYFILE"

USER_ID=$(jq -r .userId "$KEYFILE")
KEY_ID=$(jq -r .keyId "$KEYFILE")
# Zitadel machine keys store PEM under .key
KEY_PEM=$(jq -r .key "$KEYFILE")
if [[ -z "$USER_ID" || "$USER_ID" == "null" || -z "$KEY_PEM" || "$KEY_PEM" == "null" ]]; then
  echo "ERROR: invalid machine key JSON"
  exit 1
fi

# Sign JWT-bearer assertion (same pattern as website setup-local-zitadel.sh)
NOW=$(date +%s)
EXP=$((NOW + 60))
HEADER=$(printf '{"alg":"RS256","typ":"JWT","kid":"%s"}' "$KEY_ID" | b64url)
PAYLOAD=$(printf '{"iss":"%s","sub":"%s","aud":"%s","iat":%s,"exp":%s}' \
  "$USER_ID" "$USER_ID" "$ZITADEL_HOST" "$NOW" "$EXP" | b64url)
SIGNING_INPUT="${HEADER}.${PAYLOAD}"
TMPKEY=$(mktemp)
trap 'rm -f "$TMPKEY"' EXIT
printf '%s\n' "$KEY_PEM" > "$TMPKEY"
SIGNATURE=$(printf '%s' "$SIGNING_INPUT" | openssl dgst -sha256 -sign "$TMPKEY" | b64url)
JWT="${SIGNING_INPUT}.${SIGNATURE}"

echo "==> Exchanging admin JWT for access token..."
ACCESS_TOKEN=""
for i in $(seq 1 60); do
  # Refresh short-lived assertion if needed
  if [[ $i -gt 1 ]]; then
    NOW=$(date +%s)
    EXP=$((NOW + 60))
    PAYLOAD=$(printf '{"iss":"%s","sub":"%s","aud":"%s","iat":%s,"exp":%s}' \
      "$USER_ID" "$USER_ID" "$ZITADEL_HOST" "$NOW" "$EXP" | b64url)
    SIGNING_INPUT="${HEADER}.${PAYLOAD}"
    SIGNATURE=$(printf '%s' "$SIGNING_INPUT" | openssl dgst -sha256 -sign "$TMPKEY" | b64url)
    JWT="${SIGNING_INPUT}.${SIGNATURE}"
  fi
  TOKEN_RESPONSE=$(curl -sS -X POST "$ZITADEL_HOST/oauth/v2/token" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer" \
    --data-urlencode "scope=openid urn:zitadel:iam:org:project:id:zitadel:aud" \
    --data-urlencode "assertion=$JWT" || true)
  ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token // empty' 2>/dev/null || true)
  if [[ -n "$ACCESS_TOKEN" && "$ACCESS_TOKEN" != "null" ]]; then
    echo "    got access token (attempt $i)"
    break
  fi
  sleep 2
  if [[ $i -eq 60 ]]; then
    echo "ERROR: token exchange failed"
    echo "$TOKEN_RESPONSE" | jq . 2>/dev/null || echo "$TOKEN_RESPONSE"
    exit 1
  fi
done

# curl -f exits 22 on 4xx/5xx; management returns 503 while projections catch up
# after /debug/ready. Retry with backoff until we get HTTP 200.
api() {
  local method="$1" path="$2" body="${3:-}"
  local attempt http_code out
  for attempt in $(seq 1 45); do
    if [[ -n "$body" ]]; then
      out=$(curl -sS -w '\n%{http_code}' -X "$method" "$ZITADEL_HOST$path" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H 'Content-Type: application/json' \
        -d "$body" || true)
    else
      out=$(curl -sS -w '\n%{http_code}' -X "$method" "$ZITADEL_HOST$path" \
        -H "Authorization: Bearer $ACCESS_TOKEN" || true)
    fi
    http_code=$(echo "$out" | tail -n1)
    out=$(echo "$out" | sed '$d')
    if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
      printf '%s' "$out"
      return 0
    fi
    # 401/403 are not readiness races
    if [[ "$http_code" == "401" || "$http_code" == "403" ]]; then
      echo "ERROR: management API $method $path → HTTP $http_code" >&2
      echo "$out" >&2
      return 1
    fi
    sleep 2
  done
  echo "ERROR: management API $method $path still not ready (last HTTP ${http_code:-none})" >&2
  echo "$out" >&2
  return 1
}

echo "==> Wait for management API (projects/_search) — /debug/ready is not enough"
api POST /management/v1/projects/_search '{}' >/dev/null
echo "    management API ready"

echo "==> Ensure project $PROJECT_NAME"
PROJECT_SEARCH=$(api POST /management/v1/projects/_search '{}')
PROJECT_ID=$(echo "$PROJECT_SEARCH" | jq -r --arg n "$PROJECT_NAME" \
  '.result[]? | select(.name == $n) | .id' | head -n1)
if [[ -z "$PROJECT_ID" ]]; then
  PROJECT_ID=$(api POST /management/v1/projects "$(jq -n --arg n "$PROJECT_NAME" '{name: $n}')" | jq -r .id)
fi
echo "    project=$PROJECT_ID"

# Project roles admin + customer (required for E1 isolation via urn:zitadel:iam:org:project:roles)
# Zitadel AddProjectRoleRequest uses **roleKey** (not key); search results use `key`.
echo "==> Ensure project roles admin + customer"
for role in admin customer; do
  role_resp=$(api POST "/management/v1/projects/$PROJECT_ID/roles" \
    "$(jq -n --arg k "$role" --arg d "$role" '{roleKey: $k, displayName: $d}')" 2>/dev/null || true)
  roles_list=$(api POST "/management/v1/projects/$PROJECT_ID/roles/_search" '{}' 2>/dev/null || echo '{}')
  if ! echo "$roles_list" | jq -e --arg k "$role" '.result[]? | select(.key == $k)' >/dev/null 2>&1; then
    echo "ERROR: project role '$role' missing after create"
    echo "create: $role_resp"
    echo "list: $roles_list"
    exit 1
  fi
  echo "    role ok: $role"
done

echo "==> Ensure OIDC app $APP_NAME (JWT access tokens + role assertion)"
APP_SEARCH=$(api POST "/management/v1/projects/$PROJECT_ID/apps/_search" '{}')
APP_ID=$(echo "$APP_SEARCH" | jq -r --arg n "$APP_NAME" \
  '.result[]? | select(.name == $n) | .id' | head -n1)
CLIENT_ID=""
if [[ -z "$APP_ID" ]]; then
  APP_RESP=$(api POST "/management/v1/projects/$PROJECT_ID/apps/oidc" "$(jq -n \
    --arg name "$APP_NAME" \
    '{
      name: $name,
      redirectUris: ["http://localhost/callback"],
      responseTypes: ["OIDC_RESPONSE_TYPE_CODE"],
      grantTypes: ["OIDC_GRANT_TYPE_AUTHORIZATION_CODE", "OIDC_GRANT_TYPE_REFRESH_TOKEN"],
      appType: "OIDC_APP_TYPE_WEB",
      authMethodType: "OIDC_AUTH_METHOD_TYPE_BASIC",
      postLogoutRedirectUris: ["http://localhost/"],
      version: "OIDC_VERSION_1_0",
      devMode: true,
      accessTokenType: "OIDC_TOKEN_TYPE_JWT",
      accessTokenRoleAssertion: true,
      idTokenRoleAssertion: true,
      idTokenUserinfoAssertion: true
    }')")
  CLIENT_ID=$(echo "$APP_RESP" | jq -r '.clientId // empty')
  APP_ID=$(echo "$APP_RESP" | jq -r '.appId // .id // empty')
  echo "    create response clientId=$CLIENT_ID appId=$APP_ID"
  if [[ -z "$CLIENT_ID" || "$CLIENT_ID" == "null" ]]; then
    echo "ERROR: OIDC app create failed"
    echo "$APP_RESP" | jq . 2>/dev/null || echo "$APP_RESP"
    exit 1
  fi
else
  # Existing app: client id from search result
  CLIENT_ID=$(echo "$APP_SEARCH" | jq -r --arg n "$APP_NAME" \
    '.result[]? | select(.name == $n) | .oidcConfig.clientId // empty' | head -n1)
fi
if [[ -z "$CLIENT_ID" || "$CLIENT_ID" == "null" ]]; then
  echo "ERROR: could not resolve OIDC client id"
  exit 1
fi
echo "    client_id=$CLIENT_ID"

create_machine_with_key() {
  # Logs to stderr so only the user id is captured on stdout.
  local username="$1" role="$2" key_out="$3"
  local search uid key_resp key_json

  search=$(api POST /management/v1/users/_search "$(jq -n --arg n "$username" \
    '{queries: [{userNameQuery: {userName: $n, method: "TEXT_QUERY_METHOD_EQUALS"}}]}')")
  uid=$(echo "$search" | jq -r '.result[0].id // empty')
  if [[ -z "$uid" ]]; then
    echo "==> Creating machine user $username" >&2
    uid=$(api POST /management/v1/users/machine "$(jq -n --arg u "$username" \
      '{userName: $u, name: $u, description: "GraphQL e2e", accessTokenType: "ACCESS_TOKEN_TYPE_JWT"}')" \
      | jq -r '.userId // empty')
  fi
  if [[ -z "$uid" || "$uid" == "null" ]]; then
    echo "ERROR: machine user $username" >&2
    exit 1
  fi
  echo "    user id: $uid" >&2

  # Grant project role (must succeed for E1 isolation claims)
  echo "    granting project role $role" >&2
  grant_body=$(jq -n --arg pid "$PROJECT_ID" --arg r "$role" \
    '{projectId: $pid, roleKeys: [$r]}')
  grants=$(api POST "/management/v1/users/grants/_search" "$(jq -n --arg uid "$uid" \
    '{queries: [{userIdQuery: {userId: $uid}}]}')" 2>/dev/null || echo '{}')
  existing_grant=$(echo "$grants" | jq -r --arg pid "$PROJECT_ID" \
    '.result[]? | select(.projectId == $pid) | .id // empty' | head -n1)
  if [[ -n "$existing_grant" ]]; then
    # One-shot update (do not use api() retry — 404 is not a readiness race)
    http=$(curl -sS -o /tmp/zitadel-grant.json -w '%{http_code}' -X PUT \
      "$ZITADEL_HOST/management/v1/users/$uid/grants/$existing_grant" \
      -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
      -d "$(jq -n --arg r "$role" '{roleKeys: [$r]}')" || echo 000)
    # 200/201 ok; 400 "has not been changed" means grant already has roleKeys
    if [[ "$http" == "200" || "$http" == "201" ]]; then
      echo "    updated grant $existing_grant → $role" >&2
    elif [[ "$http" == "400" ]] && grep -q 'has not been changed' /tmp/zitadel-grant.json 2>/dev/null; then
      echo "    grant $existing_grant already has role $role" >&2
    else
      echo "ERROR: update grant $existing_grant → $role failed HTTP $http" >&2
      cat /tmp/zitadel-grant.json >&2 || true
      exit 1
    fi
  else
    grant_resp=$(api POST "/management/v1/users/$uid/grants" "$grant_body") || {
      echo "ERROR: grant role $role to $uid failed" >&2
      exit 1
    }
    echo "    grant created for role $role" >&2
  fi

  # Machine key (JSON type 1) for JWT-bearer
  echo "    creating machine key → $key_out" >&2
  key_resp=$(api POST "/management/v1/users/$uid/keys" \
    "$(jq -n '{type: "KEY_TYPE_JSON", expirationDate: "2029-01-01T00:00:00Z"}')")

  if echo "$key_resp" | jq -e '.keyId and .key' >/dev/null 2>&1; then
    echo "$key_resp" | jq -c --arg uid "$uid" \
      '{keyId: .keyId, key: .key, userId: $uid}' > "$key_out"
  elif echo "$key_resp" | jq -e '.keyDetails' >/dev/null 2>&1; then
    # keyDetails is often base64-encoded JSON machine key
    key_json=$(echo "$key_resp" | jq -r '.keyDetails' | base64 -d 2>/dev/null || true)
    if ! echo "$key_json" | jq -e '.keyId and .key' >/dev/null 2>&1; then
      key_json=$(echo "$key_resp" | jq -r '.keyDetails')
    fi
    if echo "$key_json" | jq -e '.keyId and .key' >/dev/null 2>&1; then
      echo "$key_json" | jq -c --arg uid "$uid" \
        '. + {userId: (.userId // $uid)}' > "$key_out"
    else
      echo "ERROR: could not parse keyDetails for $username" >&2
      echo "$key_resp" | jq . >&2 || echo "$key_resp" >&2
      exit 1
    fi
  else
    echo "ERROR: unexpected key response for $username" >&2
    echo "$key_resp" | jq . >&2 || echo "$key_resp" >&2
    exit 1
  fi

  if [[ ! -s "$key_out" ]]; then
    echo "ERROR: empty key file $key_out" >&2
    exit 1
  fi
  # stdout: user id only
  printf '%s\n' "$uid"
}

mkdir -p "$MACHINEKEY_DIR/e2e"
CUSTOMER_KEY="$MACHINEKEY_DIR/e2e/customer.json"
ADMIN_KEY="$MACHINEKEY_DIR/e2e/admin.json"
CUSTOMER_UID=$(create_machine_with_key "graphql-e2e-customer" "customer" "$CUSTOMER_KEY")
ADMIN_UID=$(create_machine_with_key "graphql-e2e-admin" "admin" "$ADMIN_KEY")
# Strip any accidental whitespace
CUSTOMER_UID=$(echo "$CUSTOMER_UID" | tr -d '[:space:]')
ADMIN_UID=$(echo "$ADMIN_UID" | tr -d '[:space:]')

# Prove customer token carries project roles (E1 isolation depends on this)
echo "==> Verify customer JWT-bearer token includes project roles claim"
verify_roles() {
  local keyfile="$1" uid="$2"
  local kid key_pem now exp header payload sig jwt tok claims
  kid=$(jq -r '.keyId // .key_id' "$keyfile")
  key_pem=$(jq -r '.key' "$keyfile")
  now=$(date +%s)
  exp=$((now + 60))
  header=$(printf '{"alg":"RS256","typ":"JWT","kid":"%s"}' "$kid" | b64url)
  payload=$(printf '{"iss":"%s","sub":"%s","aud":["%s/oauth/v2/token","%s"],"iat":%s,"exp":%s}' \
    "$uid" "$uid" "$ZITADEL_HOST" "$ZITADEL_HOST" "$now" "$exp" | b64url)
  TMPV=$(mktemp)
  printf '%s\n' "$key_pem" > "$TMPV"
  sig=$(printf '%s' "${header}.${payload}" | openssl dgst -sha256 -sign "$TMPV" | b64url)
  rm -f "$TMPV"
  jwt="${header}.${payload}.${sig}"
  tok=$(curl -sS -X POST "$ZITADEL_HOST/oauth/v2/token" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer" \
    --data-urlencode "scope=openid profile urn:zitadel:iam:org:project:id:${PROJECT_ID}:aud urn:zitadel:iam:org:project:roles urn:zitadel:iam:org:projects:roles" \
    --data-urlencode "assertion=$jwt" | jq -r '.access_token // empty')
  if [[ -z "$tok" ]]; then
    echo "ERROR: could not mint verify token for $uid"
    return 1
  fi
  claims=$(python3 -c "
import json,base64,sys
t=sys.argv[1].split('.')[1]
t += '=' * (-len(t) % 4)
print(json.dumps(json.loads(base64.urlsafe_b64decode(t))))
" "$tok")
  # Generic or project-scoped role object (either form is valid for claim map)
  if ! echo "$claims" | jq -e '
      (."urn:zitadel:iam:org:project:roles" | type == "object")
      or ([to_entries[] | select(.key | test("^urn:zitadel:iam:org:project:[^:]+:roles$"))] | length > 0)
    ' >/dev/null; then
    echo "ERROR: access token missing Zitadel project roles claim (E1 isolation will fail)"
    echo "$claims" | jq .
    return 1
  fi
  echo "    roles claim present: $(echo "$claims" | jq -c '[to_entries[] | select(.key | contains("roles")) | {key, roles: (.value | keys)}]')"
}
verify_roles "$CUSTOMER_KEY" "$CUSTOMER_UID"
verify_roles "$ADMIN_KEY" "$ADMIN_UID"

# Zitadel JWT-bearer access tokens with project:id:{PROJECT}:aud put the
# **project id** in `aud` (not the OIDC app client id). Validate against that.
umask 077
cat > "$OUT" <<EOF
ZITADEL_E2E=1
OIDC_ISSUER=$ZITADEL_HOST
OIDC_AUDIENCE=$PROJECT_ID
OIDC_CLIENT_ID=$CLIENT_ID
ZITADEL_PROJECT_ID=$PROJECT_ID
GRAPHQL_E2E_CUSTOMER_KEY=$CUSTOMER_KEY
GRAPHQL_E2E_ADMIN_KEY=$ADMIN_KEY
GRAPHQL_E2E_CUSTOMER_USER_ID=$CUSTOMER_UID
GRAPHQL_E2E_ADMIN_USER_ID=$ADMIN_UID
EOF
echo "==> Wrote $OUT"
cat "$OUT"
