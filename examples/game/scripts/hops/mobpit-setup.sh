#!/usr/bin/env bash
# Mob Pit Environment setup (runs on the host before the chart):
# - builds what the pods load from this checkout: JS SDK, WebAssembly, the
#   cells worker (`./run.sh build`; incremental);
# - mobpit: this Environment's ticket, internal and admin secrets (made once);
# - mobpit-relay-tls: the services' relay certificate for
#   mobpit-relay.<namespace>.svc.cluster.local, signed by the mobpit-system CA;
# - mobpit-rustfs: the fleet bucket's credentials, for the worker deployer.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
GAME="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

for s in mobpit-relay-ca:mobpit-system celld-rustfs:mobpit-celld; do
	if ! has_secret "${s#*:}" "${s%%:*}"; then
		echo "mobpit setup: ${s#*:}/${s%%:*} missing; enable mobpit-system first (hops local env enable mobpit-system)" >&2
		exit 1
	fi
done

"$GAME/run.sh" build

if ! has_secret "$NAMESPACE" mobpit; then
	log "creating the Environment's secrets"
	# Not test-only: the relay is HTTPS.
	k -n "$NAMESPACE" create secret generic mobpit \
		--from-literal=GAME_TICKET_SECRET="$(hex 32)" \
		--from-literal=DISTRIBUTED_INTERNAL_SECRET="$(hex 32)" \
		--from-literal=DG_DEMO_ADMIN_SECRET="$(hex 24)"
fi

host="mobpit-relay.$NAMESPACE.svc.cluster.local"
if ! has_secret "$NAMESPACE" mobpit-relay-tls; then
	log "issuing the relay certificate for $host"
	tmp="$(mktemp -d)"
	trap 'rm -rf "$tmp"' EXIT
	k -n mobpit-system get secret mobpit-relay-ca -o jsonpath='{.data.ca\.crt}' | base64 -d >"$tmp/ca.crt"
	k -n mobpit-system get secret mobpit-relay-ca -o jsonpath='{.data.ca\.key}' | base64 -d >"$tmp/ca.key"
	openssl req -newkey rsa:2048 -sha256 -nodes \
		-keyout "$tmp/tls.key" -out "$tmp/tls.csr" -subj "/CN=$host" 2>/dev/null
	printf 'subjectAltName=DNS:%s\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n' "$host" >"$tmp/ext"
	openssl x509 -req -sha256 -in "$tmp/tls.csr" -CA "$tmp/ca.crt" -CAkey "$tmp/ca.key" -CAcreateserial \
		-days 825 -extfile "$tmp/ext" -out "$tmp/tls.crt" 2>/dev/null
	k -n "$NAMESPACE" create secret tls mobpit-relay-tls --cert="$tmp/tls.crt" --key="$tmp/tls.key"
fi

# The fleet's bucket credentials (AWS_*, S3_ENDPOINT), refreshed each run.
k -n mobpit-celld get secret celld-rustfs -o json |
	python3 -c 'import json,sys; s=json.load(sys.stdin); print(json.dumps({"apiVersion":"v1","kind":"Secret","type":"Opaque","metadata":{"name":"mobpit-rustfs"},"data":s["data"]}))' |
	k -n "$NAMESPACE" apply -f - >/dev/null
