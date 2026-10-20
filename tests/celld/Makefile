# Thin entrypoint for the canonical celld + Queue + NATS local profile (celld
# version pinned in /.celld-version; set CELLD_BIN to use a non-PATH binary).
# State persists in worker/.celld/dev across reloads and ordinary shutdowns.

.PHONY: up reload logs down test help

PROFILE_DIR := ../e2e-ui
CELLD_HTTP_PORT ?= 18880
CELLD_BIN ?= celld
CELLD_URL ?= http://127.0.0.1:$(CELLD_HTTP_PORT)
NATS_PORT ?= 14222
NATS_URL ?= nats://127.0.0.1:$(NATS_PORT)
DISTRIBUTED_INTERNAL_SECRET ?= test-only-internal-secret-change-me-2026
export DISTRIBUTED_INTERNAL_SECRET

PROFILE_VARS = \
	CELLD_BIN="$(CELLD_BIN)" \
	CELLD_HTTP_PORT="$(CELLD_HTTP_PORT)" \
	CELLD_URL="$(CELLD_URL)" \
	NATS_PORT="$(NATS_PORT)" \
	NATS_URL="$(NATS_URL)"

up:
	$(MAKE) -C $(PROFILE_DIR) up-celld-nats $(PROFILE_VARS)

reload: up

logs:
	tail -F $(PROFILE_DIR)/.make-celld.log $(PROFILE_DIR)/.make-celld-relay.log

down:
	$(MAKE) -C $(PROFILE_DIR) down-celld $(PROFILE_VARS)
	$(MAKE) -C $(PROFILE_DIR) down-celld-nats $(PROFILE_VARS)

test:
	$(MAKE) -C $(PROFILE_DIR) test-celld $(PROFILE_VARS)

help:
	@echo "celld + Queue + NATS profile (CELLD_BIN, version in /.celld-version)"
	@echo "  make up      build/register both Workers and start the shared local profile"
	@echo "  make reload  rebuild and restart without deleting worker/.celld/dev"
	@echo "  make logs    follow aggregate and relay registration logs"
	@echo "  make down    stop celld and NATS; preserve worker/.celld/dev"
	@echo "  make test    validate the already-running profile"
