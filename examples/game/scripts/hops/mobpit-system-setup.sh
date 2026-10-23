#!/usr/bin/env bash
# mobpit-system setup: the secrets the shared celld fleet needs, made once and
# kept (never regenerated: the fleet's bucket and every Environment's relay
# certificate depend on them).
# - mobpit-rustfs-credentials: RustFS root keys (CelldStack copies them into
#   the fleet namespace with AWS_* aliases).
# - mobpit-relay-ca: a local CA (ca.crt, ca.key). Environments sign their
#   relay certificates with it; celld trusts ca.crt (SSL_CERT_FILE).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if ! has_secret "$NAMESPACE" mobpit-rustfs-credentials; then
	log "creating RustFS credentials"
	k -n "$NAMESPACE" create secret generic mobpit-rustfs-credentials \
		--from-literal=RUSTFS_ACCESS_KEY="$(hex 12)" \
		--from-literal=RUSTFS_SECRET_KEY="$(hex 24)"
fi

if ! has_secret "$NAMESPACE" mobpit-relay-ca; then
	log "creating the relay CA"
	tmp="$(mktemp -d)"
	trap 'rm -rf "$tmp"' EXIT
	openssl req -x509 -newkey rsa:2048 -sha256 -nodes \
		-keyout "$tmp/ca.key" -out "$tmp/ca.crt" -days 3650 \
		-subj "/CN=Mob Pit local relay CA" \
		-addext "basicConstraints=critical,CA:TRUE" \
		-addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
	k -n "$NAMESPACE" create secret generic mobpit-relay-ca \
		--from-file=ca.crt="$tmp/ca.crt" --from-file=ca.key="$tmp/ca.key"
fi
