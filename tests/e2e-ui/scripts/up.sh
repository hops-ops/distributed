#!/usr/bin/env bash
# Bring up e2e-ui Docker stack (app Postgres + Zitadel) and bootstrap OIDC.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST_ROOT="$(cd "$ROOT/../.." && pwd)"
COMPOSE="$ROOT/docker/docker-compose.yml"
MACHINEKEY_DIR="$ROOT/docker/machinekey"
ZITADEL_HOST="${ZITADEL_HOST:-http://localhost:18080}"
OUT="${E2E_UI_ENV:-$ROOT/e2e-ui.env}"
# Prefer localhost (not 127.0.0.1): SvelteKit only skips Secure cookies for
# hostname === "localhost" over http. 127.0.0.1 forces Secure and breaks OIDC.
UI_ORIGIN="${PUBLIC_ORIGIN:-${E2E_UI_ORIGIN:-http://localhost:8791}}"
API_ORIGIN="${E2E_API_ORIGIN:-http://127.0.0.1:8791}"
PROJECT_NAME="${E2E_OIDC_PROJECT:-e2e-ui}"
APP_NAME="${E2E_OIDC_APP:-e2e-ui-web}"
API_APP_NAME="${E2E_OIDC_API_APP:-e2e-ui-api}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: $1 required"; exit 1; }; }
need docker
need jq
need curl
need openssl

b64url() { openssl base64 -e -A | tr '+/' '-_' | tr -d '='; }

echo "==> machinekey dir"
mkdir -p "$MACHINEKEY_DIR/e2e"
chmod 777 "$MACHINEKEY_DIR" "$MACHINEKEY_DIR/e2e"
# Preserve FirstInstance admin key; clear only e2e keys
find "$MACHINEKEY_DIR" -mindepth 1 -maxdepth 1 ! -name 'e2e' ! -name '*.json' -exec rm -rf {} + 2>/dev/null || true
rm -rf "$MACHINEKEY_DIR/e2e"
mkdir -p "$MACHINEKEY_DIR/e2e"
chmod 777 "$MACHINEKEY_DIR" "$MACHINEKEY_DIR/e2e"

echo "==> docker compose up"
docker compose -f "$COMPOSE" up -d --remove-orphans

# Heartbeat while waiting (stderr so command substitutions stay clean).
wait_tick() {
  local label="$1" n="$2" every="${3:-5}"
  if (( n == 1 || n % every == 0 )); then
    echo "    … $label (try $n)" >&2
  fi
}

echo "==> Wait for app Postgres"
for i in $(seq 1 60); do
  if docker compose -f "$COMPOSE" exec -T app-db pg_isready -U e2e -d e2e_ui >/dev/null 2>&1; then
    echo "    app-db ready (${i}s)"
    break
  fi
  wait_tick "app-db" "$i" 10
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "ERROR: app-db not ready"
    docker compose -f "$COMPOSE" logs --tail=40 app-db || true
    exit 1
  fi
done

echo "==> Wait for Zitadel (API via proxy; first boot can take ~30–90s)"
for i in $(seq 1 90); do
  if curl -fsS --max-time 2 "$ZITADEL_HOST/debug/healthz" >/dev/null 2>&1 \
    || curl -fsS --max-time 2 "$ZITADEL_HOST/debug/ready" >/dev/null 2>&1; then
    echo "    zitadel probe ok (${i} tries)"
    break
  fi
  if ! docker compose -f "$COMPOSE" ps --status running --services 2>/dev/null | grep -q '^zitadel$'; then
    echo "ERROR: zitadel not running"
    docker compose -f "$COMPOSE" logs --tail=80 zitadel || true
    exit 1
  fi
  wait_tick "zitadel health" "$i" 5
  sleep 2
  if [[ $i -eq 90 ]]; then
    echo "ERROR: Zitadel unreachable"
    docker compose -f "$COMPOSE" logs --tail=100 zitadel || true
    exit 1
  fi
done

echo "==> Wait for FirstInstance machine key"
KEYFILE=""
for i in $(seq 1 60); do
  shopt -s nullglob
  keys=("$MACHINEKEY_DIR"/*.json)
  shopt -u nullglob
  if [[ ${#keys[@]} -gt 0 && -s "${keys[0]}" ]]; then
    KEYFILE="${keys[0]}"
    echo "    found $KEYFILE"
    break
  fi
  wait_tick "machine key" "$i" 5
  sleep 2
  if [[ $i -eq 60 ]]; then
    echo "    recreating stack for FirstInstance key"
    docker compose -f "$COMPOSE" down -v || true
    mkdir -p "$MACHINEKEY_DIR/e2e" && chmod 777 "$MACHINEKEY_DIR" "$MACHINEKEY_DIR/e2e"
    docker compose -f "$COMPOSE" up -d --remove-orphans
    for j in $(seq 1 90); do
      shopt -s nullglob
      keys=("$MACHINEKEY_DIR"/*.json)
      shopt -u nullglob
      if [[ ${#keys[@]} -gt 0 && -s "${keys[0]}" ]]; then
        KEYFILE="${keys[0]}"
        break 2
      fi
      wait_tick "machine key (after recreate)" "$j" 5
      sleep 2
    done
  fi
done
if [[ -z "$KEYFILE" || ! -s "$KEYFILE" ]]; then
  echo "ERROR: no admin SA key"
  ls -la "$MACHINEKEY_DIR" || true
  exit 1
fi

USER_ID=$(jq -r .userId "$KEYFILE")
KEY_ID=$(jq -r .keyId "$KEYFILE")
KEY_PEM=$(jq -r .key "$KEYFILE")

mint_admin() {
  local now exp header payload sig jwt
  now=$(date +%s); exp=$((now + 60))
  header=$(printf '{"alg":"RS256","typ":"JWT","kid":"%s"}' "$KEY_ID" | b64url)
  payload=$(printf '{"iss":"%s","sub":"%s","aud":"%s","iat":%s,"exp":%s}' \
    "$USER_ID" "$USER_ID" "$ZITADEL_HOST" "$now" "$exp" | b64url)
  local tmp; tmp=$(mktemp)
  printf '%s\n' "$KEY_PEM" > "$tmp"
  sig=$(printf '%s' "${header}.${payload}" | openssl dgst -sha256 -sign "$tmp" | b64url)
  rm -f "$tmp"
  jwt="${header}.${payload}.${sig}"
  curl -sS --max-time 10 -X POST "$ZITADEL_HOST/oauth/v2/token" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer" \
    --data-urlencode "scope=openid urn:zitadel:iam:org:project:id:zitadel:aud" \
    --data-urlencode "assertion=$jwt" 2>/dev/null | jq -r '.access_token // empty'
}

echo "==> Admin token (JWT-bearer mint)"
ACCESS_TOKEN=""
for i in $(seq 1 45); do
  ACCESS_TOKEN=$(mint_admin)
  if [[ -n "$ACCESS_TOKEN" && "$ACCESS_TOKEN" != "null" ]]; then
    echo "    ok (try $i)"
    break
  fi
  wait_tick "admin token" "$i" 3
  sleep 2
done
if [[ -z "$ACCESS_TOKEN" || "$ACCESS_TOKEN" == "null" ]]; then
  echo "ERROR: admin token mint failed"
  exit 1
fi

# Management API helper.
# - Retries transient 5xx / empty with progress.
# - Treats 409 as success (idempotent re-bootstrap: role/user already exists).
# - Does NOT silently spin for 90s on permanent client errors.
api() {
  local method="$1" path="$2" body="${3:-}" out http_code attempt
  local max_attempts=30
  for attempt in $(seq 1 "$max_attempts"); do
    if [[ -n "$body" ]]; then
      out=$(curl -sS --max-time 15 -w '\n%{http_code}' -X "$method" "$ZITADEL_HOST$path" \
        -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' -d "$body" || true)
    else
      out=$(curl -sS --max-time 15 -w '\n%{http_code}' -X "$method" "$ZITADEL_HOST$path" \
        -H "Authorization: Bearer $ACCESS_TOKEN" || true)
    fi
    http_code=$(echo "$out" | tail -n1)
    out=$(echo "$out" | sed '$d')
    if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
      printf '%s' "$out"; return 0
    fi
    # Already exists / conflict — fine for re-runs of make up
    if [[ "$http_code" == "409" ]]; then
      printf '%s' "$out"; return 0
    fi
    if [[ "$http_code" == "401" || "$http_code" == "403" ]]; then
      echo "ERROR: $method $path → $http_code" >&2; echo "$out" >&2; return 1
    fi
    # Permanent-ish client errors: fail fast (don't burn ~minutes)
    if [[ "$http_code" =~ ^4[0-9][0-9]$ ]]; then
      echo "ERROR: $method $path → HTTP $http_code" >&2
      echo "$out" >&2
      return 1
    fi
    wait_tick "$method $path (HTTP ${http_code:-?})" "$attempt" 3
    sleep 2
  done
  echo "ERROR: $method $path not ready (HTTP $http_code)" >&2; return 1
}

echo "==> Management API ready"
api POST /management/v1/projects/_search '{}' >/dev/null
echo "    ok"

# Login V2 base URI: Fieldnote UI origin (custom /login pages), NOT Zitadel's
# stock Next.js login image. Authorize redirects to:
#   ${LOGIN_V2_BASE_URI}/login?authRequest=V2_…
# Auth.js still owns OIDC (PKCE + code exchange); Session API finalizes on the app.
LOGIN_V2_BASE_URI="${LOGIN_V2_BASE_URI:-$UI_ORIGIN}"
LOGIN_V2_BASE_URI="${LOGIN_V2_BASE_URI%/}"

# Instance-wide Login V2. required=true → all apps use Login V2 + baseUri.
echo "==> Instance features: Login V2 custom UI (baseUri=$LOGIN_V2_BASE_URI)"
FEAT_BODY=$(jq -n --arg base "$LOGIN_V2_BASE_URI" '{
  loginV2: { required: true, baseUri: $base },
  consoleUseV2UserApi: true
}')
FEAT_RESP=$(curl -sS --max-time 15 -w '\n%{http_code}' -X PUT "$ZITADEL_HOST/v2/features/instance" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d "$FEAT_BODY" || true)
FEAT_CODE=$(echo "$FEAT_RESP" | tail -n1)
FEAT_OUT=$(echo "$FEAT_RESP" | sed '$d')
if [[ "$FEAT_CODE" == "200" || "$FEAT_CODE" == "201" ]]; then
  echo "    loginV2 feature set (HTTP $FEAT_CODE)"
else
  echo "    WARN: set instance features HTTP $FEAT_CODE — $FEAT_OUT"
fi

# ---- login-client PAT ----
# IAM_LOGIN_CLIENT PAT for the e2e-ui custom login pages (Session + User v2 +
# OIDC CreateCallback). Also kept for optional stock zitadel-login container.
# Always mint a fresh PAT: after compose recreate the DB, an old pat is invalid.
LOGIN_CLIENT_DIR="$ROOT/docker/login-client"
LOGIN_CLIENT_PAT_FILE="$LOGIN_CLIENT_DIR/pat"
LOGIN_CLIENT_USERNAME="login-client"
mkdir -p "$LOGIN_CLIENT_DIR"
echo "==> login-client machine user + fresh PAT (custom Login V2 pages)"
USER_SEARCH=$(api POST /management/v1/users/_search "$(jq -n --arg n "$LOGIN_CLIENT_USERNAME" \
  '{queries: [{userNameQuery: {userName: $n, method: "TEXT_QUERY_METHOD_EQUALS"}}]}')")
LOGIN_USER_ID=$(echo "$USER_SEARCH" | jq -r '.result[0].id // empty')
if [[ -z "$LOGIN_USER_ID" ]]; then
  LOGIN_USER_RESP=$(api POST /management/v1/users/machine "$(jq -n \
    --arg u "$LOGIN_CLIENT_USERNAME" \
    '{userName: $u, name: "Login Client", description: "Service user for custom Login V2 UI (Session API)", accessTokenType: "ACCESS_TOKEN_TYPE_BEARER"}')")
  LOGIN_USER_ID=$(echo "$LOGIN_USER_RESP" | jq -r .userId)
  [[ -n "$LOGIN_USER_ID" && "$LOGIN_USER_ID" != "null" ]] || {
    echo "ERROR: login-client create failed"; echo "$LOGIN_USER_RESP"; exit 1
  }
  api POST /admin/v1/members "$(jq -n --arg uid "$LOGIN_USER_ID" \
    '{userId: $uid, roles: ["IAM_LOGIN_CLIENT"]}')" >/dev/null
  echo "    created login-client user $LOGIN_USER_ID"
else
  echo "    existing login-client user $LOGIN_USER_ID"
fi
PAT_RESP=$(api POST "/management/v1/users/$LOGIN_USER_ID/pats" \
  "$(jq -n '{expirationDate: "2029-01-01T00:00:00Z"}')")
LOGIN_PAT=$(echo "$PAT_RESP" | jq -r .token)
[[ -n "$LOGIN_PAT" && "$LOGIN_PAT" != "null" ]] || {
  echo "ERROR: login-client PAT create failed"; echo "$PAT_RESP"; exit 1
}
umask 077
printf '%s' "$LOGIN_PAT" > "$LOGIN_CLIENT_PAT_FILE"
chmod 600 "$LOGIN_CLIENT_PAT_FILE" 2>/dev/null || true
echo "    wrote $LOGIN_CLIENT_PAT_FILE (also exported as ZITADEL_SERVICE_USER_TOKEN)"
# Optional stock login container still mounts the PAT (not used when baseUri is the app).
docker compose -f "$COMPOSE" up -d --force-recreate zitadel-login >/dev/null 2>&1 \
  || docker compose -f "$COMPOSE" restart zitadel-login >/dev/null 2>&1 || true

echo "==> Project $PROJECT_NAME"
PROJECT_SEARCH=$(api POST /management/v1/projects/_search '{}')
PROJECT_ID=$(echo "$PROJECT_SEARCH" | jq -r --arg n "$PROJECT_NAME" \
  '.result[]? | select(.name == $n) | .id' | head -n1)
if [[ -z "$PROJECT_ID" ]]; then
  PROJECT_ID=$(api POST /management/v1/projects "$(jq -n --arg n "$PROJECT_NAME" '{name: $n}')" | jq -r .id)
fi
echo "    project=$PROJECT_ID"

echo "==> Roles user + admin (409 = already exists, ok)"
for role in user admin; do
  # api treats 409 as success so re-runs don't spin for minutes
  if api POST "/management/v1/projects/$PROJECT_ID/roles" \
    "$(jq -n --arg k "$role" --arg d "$role" '{roleKey: $k, displayName: $d}')" >/dev/null; then
    echo "    role $role ok"
  else
    echo "    role $role skipped/failed (continuing)"
  fi
done

# OIDC app: Login V2 baseUri = Fieldnote UI (custom /login). Auth.js keeps PKCE.
echo "==> Web OIDC app $APP_NAME (Auth.js → authorize → custom /login)"
APP_SEARCH=$(api POST "/management/v1/projects/$PROJECT_ID/apps/_search" '{}')
APP_ID=$(echo "$APP_SEARCH" | jq -r --arg n "$APP_NAME" '.result[]? | select(.name == $n) | .id' | head -n1)
CLIENT_ID=""
CLIENT_SECRET=""
if [[ -z "$APP_ID" ]]; then
  APP_RESP=$(api POST "/management/v1/projects/$PROJECT_ID/apps/oidc" "$(jq -n \
    --arg name "$APP_NAME" \
    --arg ui "$UI_ORIGIN" \
    --arg base "$LOGIN_V2_BASE_URI" \
    '{
      name: $name,
      redirectUris: [
        ($ui + "/auth/callback/oidc"),
        ($ui + "/auth/callback"),
        "http://127.0.0.1:8791/auth/callback/oidc",
        "http://localhost:8791/auth/callback/oidc"
      ],
      responseTypes: ["OIDC_RESPONSE_TYPE_CODE"],
      grantTypes: [
        "OIDC_GRANT_TYPE_AUTHORIZATION_CODE",
        "OIDC_GRANT_TYPE_REFRESH_TOKEN"
      ],
      appType: "OIDC_APP_TYPE_WEB",
      authMethodType: "OIDC_AUTH_METHOD_TYPE_BASIC",
      postLogoutRedirectUris: [
        ($ui + "/"),
        $ui,
        "http://127.0.0.1:8791/",
        "http://127.0.0.1:8791",
        "http://localhost:8791/",
        "http://localhost:8791"
      ],
      version: "OIDC_VERSION_1_0",
      devMode: true,
      accessTokenType: "OIDC_TOKEN_TYPE_JWT",
      accessTokenRoleAssertion: true,
      idTokenRoleAssertion: true,
      idTokenUserinfoAssertion: true,
      loginVersion: { loginV2: { baseUri: $base } }
    }')")
  CLIENT_ID=$(echo "$APP_RESP" | jq -r '.clientId // empty')
  CLIENT_SECRET=$(echo "$APP_RESP" | jq -r '.clientSecret // empty')
  APP_ID=$(echo "$APP_RESP" | jq -r '.appId // .id // empty')
  echo "    created web app clientId=$CLIENT_ID (Login V2)"
else
  CLIENT_ID=$(echo "$APP_SEARCH" | jq -r --arg n "$APP_NAME" \
    '.result[]? | select(.name == $n) | .oidcConfig.clientId // empty' | head -n1)
  echo "    existing web app clientId=$CLIENT_ID (secret only on create — re-create stack if missing)"
  # Pin Login V2 base URI on re-bootstrap (idempotent).
  if [[ -n "$APP_ID" && "$APP_ID" != "null" ]]; then
    UPD=$(curl -sS --max-time 15 -w '\n%{http_code}' -X PUT \
      "$ZITADEL_HOST/management/v1/projects/$PROJECT_ID/apps/$APP_ID/oidc_config" \
      -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
      -d "$(jq -n --arg base "$LOGIN_V2_BASE_URI" --arg ui "$UI_ORIGIN" '{
        redirectUris: [
          ($ui + "/auth/callback/oidc"),
          ($ui + "/auth/callback"),
          "http://127.0.0.1:8791/auth/callback/oidc",
          "http://localhost:8791/auth/callback/oidc"
        ],
        responseTypes: ["OIDC_RESPONSE_TYPE_CODE"],
        grantTypes: [
          "OIDC_GRANT_TYPE_AUTHORIZATION_CODE",
          "OIDC_GRANT_TYPE_REFRESH_TOKEN"
        ],
        appType: "OIDC_APP_TYPE_WEB",
        authMethodType: "OIDC_AUTH_METHOD_TYPE_BASIC",
        postLogoutRedirectUris: [
          ($ui + "/"),
          $ui,
          "http://127.0.0.1:8791/",
          "http://127.0.0.1:8791",
          "http://localhost:8791/",
          "http://localhost:8791"
        ],
        devMode: true,
        accessTokenType: "OIDC_TOKEN_TYPE_JWT",
        accessTokenRoleAssertion: true,
        idTokenRoleAssertion: true,
        idTokenUserinfoAssertion: true,
        loginVersion: { loginV2: { baseUri: $base } }
      }')" || true)
    UPD_CODE=$(echo "$UPD" | tail -n1)
    if [[ "$UPD_CODE" == "200" ]]; then
      echo "    updated OIDC app → Login V2 baseUri=$LOGIN_V2_BASE_URI"
    else
      echo "    WARN: UpdateOIDCAppConfig HTTP $UPD_CODE — $(echo "$UPD" | sed '$d' | head -c 200)"
    fi
  fi
fi
if [[ -z "$CLIENT_ID" || "$CLIENT_ID" == "null" ]]; then
  echo "ERROR: web client id missing"; exit 1
fi

# Store secret if we got one
SECRET_FILE="$ROOT/docker/web-client.secret"
if [[ -n "$CLIENT_SECRET" && "$CLIENT_SECRET" != "null" ]]; then
  umask 077
  printf '%s' "$CLIENT_SECRET" > "$SECRET_FILE"
  echo "    wrote $SECRET_FILE"
elif [[ -f "$SECRET_FILE" ]]; then
  CLIENT_SECRET=$(cat "$SECRET_FILE")
fi

# Humans MUST be created via human/_import with a flat password string.
# Nested password objects leave users in USER_STATE_INITIAL with no password
# (account picker shows a red dot; login never works).
create_human() {
  local username="$1" password="$2" role="$3" email="$4"
  local search uid state
  # Status logs go to stderr so callers can capture only the user id on stdout.
  search=$(api POST /management/v1/users/_search "$(jq -n --arg n "$username" \
    '{queries: [{userNameQuery: {userName: $n, method: "TEXT_QUERY_METHOD_EQUALS"}}]}')")
  uid=$(echo "$search" | jq -r '.result[0].id // empty')
  state=$(echo "$search" | jq -r '.result[0].state // empty')

  # Drop broken INITIAL users (no password) so we can re-import.
  if [[ -n "$uid" && "$state" == "USER_STATE_INITIAL" ]]; then
    echo "    removing uninitialized $username ($uid)" >&2
    curl -sS -o /dev/null -X DELETE "$ZITADEL_HOST/management/v1/users/$uid" \
      -H "Authorization: Bearer $ACCESS_TOKEN" || true
    uid=""
  fi

  if [[ -z "$uid" ]]; then
    echo "==> Human user $username (import + password)" >&2
    uid=$(api POST /management/v1/users/human/_import "$(jq -n \
      --arg u "$username" --arg e "$email" --arg p "$password" \
      '{
        userName: $u,
        profile: { firstName: $u, lastName: "E2E", displayName: $u },
        email: { email: $e, isEmailVerified: true },
        password: $p,
        passwordChangeRequired: false
      }')" | jq -r '.userId // empty')
  else
    echo "    reusing human $username ($uid, $state)" >&2
  fi
  [[ -n "$uid" && "$uid" != "null" ]] || { echo "ERROR: human $username" >&2; exit 1; }

  # Grant project role(s). Admin also receives `user` so they can use the
  # normal application surface (e2e-ui) and elevated admin paths.
  local role_keys_json
  if [[ "$role" == "admin" ]]; then
    role_keys_json='["admin","user"]'
  else
    role_keys_json=$(jq -n --arg r "$role" '[$r]')
  fi
  grants=$(api POST /management/v1/users/grants/_search "$(jq -n --arg uid "$uid" \
    '{queries: [{userIdQuery: {userId: $uid}}]}')" 2>/dev/null || echo '{}')
  existing=$(echo "$grants" | jq -r --arg pid "$PROJECT_ID" \
    '.result[]? | select(.projectId == $pid) | .id // empty' | head -n1)
  if [[ -n "$existing" ]]; then
    curl -sS -o /dev/null -X PUT \
      "$ZITADEL_HOST/management/v1/users/$uid/grants/$existing" \
      -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
      -d "$(jq -n --argjson keys "$role_keys_json" '{roleKeys: $keys}')" || true
  else
    api POST "/management/v1/users/$uid/grants" \
      "$(jq -n --arg pid "$PROJECT_ID" --argjson keys "$role_keys_json" \
        '{projectId: $pid, roleKeys: $keys}')" >/dev/null
  fi
  printf '%s' "$uid"
}

# Self-registration for "Create account" (hosted login Register button).
echo "==> Login policy: ensure allowRegister"
POL=$(curl -sS "$ZITADEL_HOST/management/v1/policies/login" \
  -H "Authorization: Bearer $ACCESS_TOKEN" || echo '{}')
ALLOW=$(echo "$POL" | jq -r '.policy.allowRegister // empty')
IS_DEFAULT=$(echo "$POL" | jq -r '.isDefault // .policy.isDefault // empty')
echo "    current allowRegister=$ALLOW isDefault=$IS_DEFAULT"
if [[ "$ALLOW" != "true" ]]; then
  # Create org custom policy (required before update when still on IAM default).
  # Field set matches Zitadel management UpdateCustomLoginPolicy / AddCustomLoginPolicy.
  REG_BODY=$(jq -n '{
    allowRegister: true,
    allowUsernamePassword: true,
    allowExternalIdp: true,
    forceMfa: false,
    passwordCheckLifetime: "864000s",
    externalLoginCheckLifetime: "864000s",
    multiFactorCheckLifetime: "43200s",
    secondFactorCheckLifetime: "64800s",
    mfaInitSkipLifetime: "2592000s",
    passwordlessType: "PASSWORDLESS_TYPE_ALLOWED",
    allowDomainDiscovery: true,
    ignoreUnknownUsernames: false,
    defaultRedirectUri: "",
    passwordlessType: "PASSWORDLESS_TYPE_ALLOWED"
  }')
  if [[ "$IS_DEFAULT" == "true" ]]; then
    code=$(curl -sS -o /tmp/e2e-login-pol.out -w '%{http_code}' -X POST \
      "$ZITADEL_HOST/management/v1/policies/login" \
      -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
      -d "$REG_BODY" || true)
  else
    code=$(curl -sS -o /tmp/e2e-login-pol.out -w '%{http_code}' -X PUT \
      "$ZITADEL_HOST/management/v1/policies/login" \
      -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
      -d "$REG_BODY" || true)
  fi
  echo "    policy write HTTP $code ($(head -c 160 /tmp/e2e-login-pol.out 2>/dev/null || true))"
else
  echo "    allowRegister already true — ok for Create account"
fi

echo "==> Human users (browser login: alice, bob, admin)"
ALICE_UID=$(create_human "alice" "Password1!" "user" "alice@e2e.local")
echo "    alice → $ALICE_UID"
BOB_UID=$(create_human "bob" "Password1!" "user" "bob@e2e.local")
echo "    bob → $BOB_UID"
ADMIN_HUMAN_UID=$(create_human "admin" "Password1!" "admin" "admin@e2e.local")
echo "    admin → $ADMIN_HUMAN_UID (roles: admin + user)"

create_machine() {
  local username="$1" role="$2" key_out="$3"
  local search uid key_resp
  search=$(api POST /management/v1/users/_search "$(jq -n --arg n "$username" \
    '{queries: [{userNameQuery: {userName: $n, method: "TEXT_QUERY_METHOD_EQUALS"}}]}')")
  uid=$(echo "$search" | jq -r '.result[0].id // empty')
  if [[ -z "$uid" ]]; then
    uid=$(api POST /management/v1/users/machine "$(jq -n --arg u "$username" \
      '{userName: $u, name: $u, description: "e2e-ui suite", accessTokenType: "ACCESS_TOKEN_TYPE_JWT"}')" \
      | jq -r '.userId // empty')
  fi
  # Machine users: admin also gets the normal `user` project role.
  local role_keys_json
  if [[ "$role" == "admin" ]]; then
    role_keys_json='["admin","user"]'
  else
    role_keys_json=$(jq -n --arg r "$role" '[$r]')
  fi
  grants=$(api POST /management/v1/users/grants/_search "$(jq -n --arg uid "$uid" \
    '{queries: [{userIdQuery: {userId: $uid}}]}')" 2>/dev/null || echo '{}')
  existing=$(echo "$grants" | jq -r --arg pid "$PROJECT_ID" \
    '.result[]? | select(.projectId == $pid) | .id // empty' | head -n1)
  if [[ -n "$existing" ]]; then
    curl -sS -o /dev/null -X PUT \
      "$ZITADEL_HOST/management/v1/users/$uid/grants/$existing" \
      -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
      -d "$(jq -n --argjson keys "$role_keys_json" '{roleKeys: $keys}')" || true
  else
    api POST "/management/v1/users/$uid/grants" \
      "$(jq -n --arg pid "$PROJECT_ID" --argjson keys "$role_keys_json" \
        '{projectId: $pid, roleKeys: $keys}')" >/dev/null
  fi
  key_resp=$(api POST "/management/v1/users/$uid/keys" \
    "$(jq -n '{type: "KEY_TYPE_JSON", expirationDate: "2029-01-01T00:00:00Z"}')")
  if echo "$key_resp" | jq -e '.keyId and .key' >/dev/null 2>&1; then
    echo "$key_resp" | jq -c --arg uid "$uid" '{keyId: .keyId, key: .key, userId: $uid}' > "$key_out"
  elif echo "$key_resp" | jq -e '.keyDetails' >/dev/null 2>&1; then
    key_json=$(echo "$key_resp" | jq -r '.keyDetails' | base64 -d 2>/dev/null || true)
    echo "$key_json" | jq -c --arg uid "$uid" '. + {userId: (.userId // $uid)}' > "$key_out"
  else
    echo "ERROR: machine key for $username"; echo "$key_resp"; exit 1
  fi
  printf '%s' "$uid"
}

echo "==> Machine users (suite JWT-bearer)"
USER_M_KEY="$MACHINEKEY_DIR/e2e/user-machine.json"
ADMIN_M_KEY="$MACHINEKEY_DIR/e2e/admin-machine.json"
USER_M_UID=$(create_machine "e2e-ui-user-m" "user" "$USER_M_KEY")
echo "    e2e-ui-user-m → $USER_M_UID"
ADMIN_M_UID=$(create_machine "e2e-ui-admin-m" "admin" "$ADMIN_M_KEY")
echo "    e2e-ui-admin-m → $ADMIN_M_UID"

# OIDC audience for JWT validation: project id (Zitadel project-scoped aud)
DATABASE_URL="postgres://e2e:e2e@127.0.0.1:5433/e2e_ui"
# Keep Auth.js session cookie decryption stable across re-bootstrap.
# Rotating AUTH_SECRET → JWTSessionError: no matching decryption secret.
if [[ -z "${AUTH_SECRET:-}" && -f "$OUT" ]]; then
  _prev=$(grep -E '^AUTH_SECRET=' "$OUT" 2>/dev/null | head -1 | cut -d= -f2- || true)
  _prev="${_prev%\"}"
  _prev="${_prev#\"}"
  _prev="${_prev%\'}"
  _prev="${_prev#\'}"
  if [[ -n "$_prev" ]]; then
    AUTH_SECRET="$_prev"
    echo "==> Reusing AUTH_SECRET from existing $OUT (session cookies stay valid)"
  fi
fi
AUTH_SECRET="${AUTH_SECRET:-$(openssl rand -hex 32)}"

umask 077
# Dotenv-style values: double-quote so both `source e2e-ui.env` and Make
# `include` keep the same unquoted semantics (shell single-quotes break Make).
# Strip any accidental outer quotes from prior env pollution before writing.
_dq() {
  local v="$1"
  # peel one layer of wrapping '…' or "…"
  if [[ "$v" =~ ^\'.*\'$ || "$v" =~ ^\".*\"$ ]]; then
    v="${v:1:${#v}-2}"
  fi
  # escape for double-quoted dotenv
  v="${v//\\/\\\\}"
  v="${v//\"/\\\"}"
  printf '%s' "$v"
}

cat > "$OUT" <<EOF
# Generated by scripts/up.sh — source before make run
# Format: KEY="value" (Make-include safe; do not use shell single-quotes)
E2E_STACK=1
DATABASE_URL="$(_dq "$DATABASE_URL")"
OIDC_ISSUER="$(_dq "$ZITADEL_HOST")"
OIDC_AUDIENCE="$(_dq "$PROJECT_ID")"
OIDC_JWKS_URI="$(_dq "$ZITADEL_HOST/oauth/v2/keys")"
OIDC_CLIENT_ID="$(_dq "$CLIENT_ID")"
OIDC_CLIENT_SECRET="$(_dq "${CLIENT_SECRET:-}")"
AUTH_SECRET="$(_dq "$AUTH_SECRET")"
AUTH_TRUST_HOST=true
OIDC_SCOPES="openid profile email offline_access urn:zitadel:iam:org:project:id:${PROJECT_ID}:aud urn:zitadel:iam:org:project:roles"
ZITADEL_PROJECT_ID="$(_dq "$PROJECT_ID")"
# Server-only: custom /login uses Session API + CreateCallback (never expose to browser).
ZITADEL_SERVICE_USER_TOKEN="$(_dq "$LOGIN_PAT")"
LOGIN_V2_BASE_URI="$(_dq "$LOGIN_V2_BASE_URI")"
PUBLIC_ORIGIN="$(_dq "$UI_ORIGIN")"
UI_INTERNAL_ORIGIN="http://localhost:5180"
E2E_UI_ORIGIN="$(_dq "$UI_ORIGIN")"
E2E_API_ORIGIN="$(_dq "$API_ORIGIN")"
E2E_MACHINE_USER_KEY="$(_dq "$USER_M_KEY")"
E2E_MACHINE_ADMIN_KEY="$(_dq "$ADMIN_M_KEY")"
E2E_MACHINE_USER_ID="$(_dq "$USER_M_UID")"
E2E_MACHINE_ADMIN_ID="$(_dq "$ADMIN_M_UID")"
E2E_HUMAN_ALICE=alice
E2E_HUMAN_BOB=bob
E2E_HUMAN_PASSWORD="Password1!"
# Zitadel user ids (OIDC sub) — use as provider_subject for auth_users joins
E2E_HUMAN_ALICE_UID="$(_dq "$ALICE_UID")"
E2E_HUMAN_BOB_UID="$(_dq "$BOB_UID")"
E2E_HUMAN_ADMIN_UID="$(_dq "$ADMIN_HUMAN_UID")"
# Shared secret for POST /zitadel.ingress.v1 (import IdP users → auth_users RM)
ZITADEL_INGESTOR_SECRET="$(_dq "e2e-zitadel-ingestor-secret")"
BIND="127.0.0.1:8791"
EOF

echo "==> Wrote $OUT"
# Avoid dumping service PAT in terminal (still in file).
grep -v 'ZITADEL_SERVICE_USER_TOKEN\|OIDC_CLIENT_SECRET\|AUTH_SECRET' "$OUT" || true
echo "  … secrets redacted in console (present in file)"
echo ""
echo "Next:"
echo "  set -a && source $OUT && set +a"
echo "  make run   # restart so UI loads OIDC_* + ZITADEL_SERVICE_USER_TOKEN"
echo "  Custom login: $LOGIN_V2_BASE_URI/login  (Auth.js authorize → your pages → callback)"
echo "  Demo users: alice / bob / admin · Password1!"
