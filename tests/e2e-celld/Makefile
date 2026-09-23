# Celld GraphQL example — sibling of tests/e2e-ui, not make run there.
#
#   make -C tests/e2e-ui up-celld-nats   # celld 0.4 local store + Queue + NATS
#   make run                             # this GraphQL host + the e2e-ui Svelte app
#
# Distributed owns the GraphQL + sibling UI lifecycle. The independent celld
# worker watcher remains infrastructure until celld exposes a lifecycle probe.

.PHONY: run stop test help ensure-watch

BIND         ?= 127.0.0.1:8791
API_PORT     ?= 8791
UI_PORT      ?= 5180
UI_HOST      ?= localhost
PUBLIC_ORIGIN ?= http://localhost:8791
UI_INTERNAL_ORIGIN ?= http://localhost:$(UI_PORT)
UI_URL       ?= $(PUBLIC_ORIGIN)
ENV_FILE     ?= ../e2e-ui/e2e-ui.env
CELLD_HTTP_PORT ?= 18080
CELLD_URL    ?= http://127.0.0.1:$(CELLD_HTTP_PORT)
NATS_PORT    ?= 14222
NATS_URL     ?= nats://127.0.0.1:$(NATS_PORT)
DISTRIBUTED  ?= cargo run --quiet --manifest-path ../../Cargo.toml -p distributed_cli --bin distributed --
WATCH_WORKER ?= 1
DISTRIBUTED_INTERNAL_SECRET ?= test-only-internal-secret-change-me-2026

ensure-watch:
	@command -v cargo-watch >/dev/null || { echo "installing cargo-watch…"; cargo install cargo-watch; }

run: $(if $(filter 1,$(WATCH_WORKER)),ensure-watch)
	@set -e; \
	if [ -f $(ENV_FILE) ]; then set -a; . ./$(ENV_FILE); set +a; fi; \
	if [ -n "$${E2E_CELLD_DATABASE_URL:-}" ]; then export DATABASE_URL="$$E2E_CELLD_DATABASE_URL"; fi; \
	case "$${DATABASE_URL:-}" in \
	  postgres://*|postgresql://*) ;; \
	  *) \
	    echo "e2e-celld requires Postgres DATABASE_URL — start: make -C ../e2e-ui up"; \
	    echo "or: E2E_CELLD_DATABASE_URL=postgres://… make run"; \
	    exit 1; \
	    ;; \
	esac; \
	if [ -n "$${OIDC_ISSUER:-}" ] && ! curl -sf "$${OIDC_JWKS_URI:-$$OIDC_ISSUER/oauth/v2/keys}" >/dev/null 2>&1; then \
	  echo "OIDC issuer not reachable ($$OIDC_ISSUER) — DevHeaders until: make -C ../e2e-ui up"; \
	  unset OIDC_ISSUER OIDC_AUDIENCE OIDC_JWKS_URI; \
	fi; \
	_celld="$(CELLD_URL)"; \
	code=$$(curl -s -o /dev/null -w '%{http_code}' "$${_celld}/health" 2>/dev/null || echo 000); \
	if [ "$$code" != "200" ]; then \
	  if curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:18880/health" 2>/dev/null | grep -q 200; then \
	    _celld="http://127.0.0.1:18880"; \
	  else \
	    echo "celld not healthy at $$_celld — start: make -C ../e2e-ui up-celld-nats"; \
	    echo "or: CELLD_HTTP_PORT=18880 make run"; \
	    exit 1; \
	  fi; \
	fi; \
	export CELLD_URL="$$_celld"; \
	_nats="$(NATS_URL)"; \
	if ! nc -z 127.0.0.1 $(NATS_PORT) 2>/dev/null; then \
	  echo "NATS not reachable at $$_nats — start: make -C ../e2e-ui up-celld-nats"; \
	  exit 1; \
	fi; \
	export NATS_URL="$$_nats"; \
	export DISTRIBUTED_INTERNAL_SECRET="$(DISTRIBUTED_INTERNAL_SECRET)"; \
	export PUBLIC_E2E_PROFILE="celld-nats"; \
	export BIND="$${BIND:-127.0.0.1:8791}"; \
	export UI_BIND="$${UI_BIND:-$(UI_HOST)}"; \
	export UI_PORT="$${UI_PORT:-$(UI_PORT)}"; \
	export PUBLIC_ORIGIN="$${PUBLIC_ORIGIN:-$(PUBLIC_ORIGIN)}"; \
	export UI_INTERNAL_ORIGIN="$${UI_INTERNAL_ORIGIN:-$(UI_INTERNAL_ORIGIN)}"; \
	export UI_URL="$${UI_URL:-$(UI_URL)}"; \
	export AUTH_URL="$${AUTH_URL:-$$UI_URL}"; \
	export AUTH_USE_SECURE_COOKIES="false"; \
	rm -f .make-worker.pid .make-worker.log; \
	if [ "$(WATCH_WORKER)" = "1" ]; then \
	  echo "worker cargo-watch on (WATCH_WORKER=0 to disable) …"; \
	  $(MAKE) -C ../celld watch > .make-worker.log 2>&1 & \
	  echo $$! > .make-worker.pid; \
	fi; \
	stop_pidfile() { \
		[ -f "$$1" ] || return 0; \
		_pid=$$(cat "$$1"); \
		for _child in $$(pgrep -P "$$_pid" 2>/dev/null); do \
		  pkill -P "$$_child" 2>/dev/null || true; \
		  kill "$$_child" 2>/dev/null || true; \
		done; \
		kill "$$_pid" 2>/dev/null || true; \
		rm -f "$$1"; \
	}; \
	cleanup() { \
		stop_pidfile .make-worker.pid; \
	}; \
	trap cleanup EXIT INT TERM; \
	echo "starting coherent e2e-celld API + sibling UI lifecycle"; \
	$(DISTRIBUTED) dev .

test:
	cargo test --workspace -- --nocapture

stop:
	@stop_pidfile() { \
		[ -f "$$1" ] || return 0; \
		_pid=$$(cat "$$1"); \
		for _child in $$(pgrep -P "$$_pid" 2>/dev/null); do \
		  pkill -P "$$_child" 2>/dev/null || true; \
		  kill "$$_child" 2>/dev/null || true; \
		done; \
		kill "$$_pid" 2>/dev/null || true; \
		rm -f "$$1"; \
	}; \
	stop_pidfile .make-worker.pid; \
	lsof -ti:$(API_PORT) 2>/dev/null | xargs kill -9 2>/dev/null || true; \
	lsof -ti:$(UI_PORT) 2>/dev/null | xargs kill -9 2>/dev/null || true; \
	echo stopped

help:
	@echo "e2e-celld (new example — not tests/e2e-ui)"
	@echo "  make test  cargo test --workspace (CI)"
	@echo "  make run   distributed dev owns GraphQL + sibling UI; worker reload runs alongside"
	@echo "  WATCH_WORKER=0  skip worker-build + celld deploy watch"
	@echo "  make stop  stop API + UI + worker watch"
	@echo "  infra:     make -C ../e2e-ui up && make -C ../e2e-ui up-celld-nats"
	@echo "  Postgres read models from e2e-ui.env DATABASE_URL (not sqlite)"
