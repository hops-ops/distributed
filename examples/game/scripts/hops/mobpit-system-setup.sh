#!/usr/bin/env bash
# mobpit-system setup: the RustFS root keys of the shared celld fleet
# (mobpit-rustfs-credentials; CelldStack copies them into the fleet namespace
# with AWS_* aliases). Made once and kept: the fleet's bucket depends on them.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if ! has_secret "$NAMESPACE" mobpit-rustfs-credentials; then
	log "creating RustFS credentials"
	k -n "$NAMESPACE" create secret generic mobpit-rustfs-credentials \
		--from-literal=RUSTFS_ACCESS_KEY="$(hex 12)" \
		--from-literal=RUSTFS_SECRET_KEY="$(hex 24)"
fi

