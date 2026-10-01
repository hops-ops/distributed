# e2e-ui template — run from this directory.
#
#   make up          # Docker: Postgres + Zitadel + bootstrap env
#   make run         # coherent build + API + UI supervision (Vite HMR stays active)
#   make test         # offline unit/suite/UI structural
#   make test-browser # Playwright UI e2e (needs make up + make run)
#   make up-celld-nats / test-celld / down-celld-nats
#                    # optional celld+NATS profile (not make run; CI live path)

.PHONY: all up down run run-api stop test ci-offline test-domain test-suite \
	test-browser test-browser-install application-build js-install js-build ui-install ui-test \
	contracts-check check clean help \
	up-celld-nats down-celld-nats down-celld test-celld test-celld-http test-celld-nats

# Defaults only — do NOT `include e2e-ui.env` (shell-quoted dotenv breaks Make).
# Recipes `source` the env file so values stay clean.
BIND         ?= 127.0.0.1:8791
API_PORT     ?= 8791
BASE_URL     ?= http://127.0.0.1:8791
UI_PORT      ?= 5180
# localhost required for Auth.js cookies over HTTP (SvelteKit Secure default)
PUBLIC_ORIGIN ?= http://localhost:8791
UI_INTERNAL_ORIGIN ?= http://localhost:$(UI_PORT)
UI_URL       ?= $(PUBLIC_ORIGIN)
UI_HOST      ?= localhost
ENV_FILE     ?= e2e-ui.env
NPM          ?= npm
JS_DIR       ?= ../../js
DISTRIBUTED  ?= cargo run --quiet --manifest-path ../../Cargo.toml -p distributed_cli --bin distributed --
CARGO_TEST_FLAGS ?= -- --nocapture

DATABASE_URL ?= sqlite:./e2e-ui.db?mode=rwc

# Optional celld+NATS profile (same UI, not the one-process playground).
REPO_ROOT        := $(abspath ../..)
PROFILE_COMPOSE  := celld-nats-profile/docker-compose.yml
CELLD_HTTP_PORT  ?= 18080
NATS_PORT        ?= 14222
CELLD_URL        ?= http://127.0.0.1:$(CELLD_HTTP_PORT)
NATS_URL         ?= nats://127.0.0.1:$(NATS_PORT)
DISTRIBUTED_INTERNAL_SECRET ?= test-only-internal-secret-change-me-2026
CELLD_PROJECT    := ../celld/worker
CELLD_RELAY_PROJECT := ../celld/relay-worker
CELLD_NODE_PID   := .make-celld-node.pid
CELLD_LOG        := .make-celld.log
CELLD_RELAY_LOG  := .make-celld-relay.log

all: run

## Docker stack + OIDC bootstrap → e2e-ui.env
up:
	chmod +x scripts/up.sh
	./scripts/up.sh

down:
	docker compose -f docker/docker-compose.yml down

## Coherent application build + actual API/UI supervision.
## Vite still owns Svelte, CSS, and GraphQL hot updates.
run:
	@if [ -f $(ENV_FILE) ]; then set -a; . ./$(ENV_FILE); set +a; fi; \
	PUBLIC_ORIGIN="$${PUBLIC_ORIGIN:-$(PUBLIC_ORIGIN)}" UI_URL="$${PUBLIC_ORIGIN:-$(PUBLIC_ORIGIN)}" UI_INTERNAL_ORIGIN="$${UI_INTERNAL_ORIGIN:-$(UI_INTERNAL_ORIGIN)}" cargo run --quiet --manifest-path ../../Cargo.toml -p distributed_cli --bin distributed -- dev .

run-api:
	@if [ -f $(ENV_FILE) ]; then set -a; . ./$(ENV_FILE); set +a; fi; \
	cargo run -p e2e-runner

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
	stop_pidfile .make-ui.pid; \
	stop_pidfile .make-runner.pid; \
	lsof -ti:$(API_PORT) 2>/dev/null | xargs kill -9 2>/dev/null || true; \
	lsof -ti:$(UI_PORT) 2>/dev/null | xargs kill -9 2>/dev/null || true; \
	echo stopped

## Optional celld + NATS profile of the same UI. Does NOT replace make run.
## celld 0.4 keeps its local object store under tests/celld/worker/.celld/dev.
up-celld-nats:
	@set -e; \
	command -v docker >/dev/null || { echo "docker required"; exit 1; }; \
	command -v celld >/dev/null || { echo "celld CLI required: curl -fsSL https://celld.dev/install.sh | sh"; exit 1; }; \
	celld dev --help >/dev/null 2>&1 || { echo "celld 0.4.0+ required (missing 'celld dev')"; exit 1; }; \
	command -v worker-build >/dev/null || { echo "worker-build required: cargo install worker-build"; exit 1; }; \
	$(MAKE) --no-print-directory down-celld; \
	if lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN >/dev/null 2>&1; then \
		echo "refusing to start celld: port $(CELLD_HTTP_PORT) is owned by an untracked listener"; \
		lsof -nP -iTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN; \
		exit 1; \
	fi; \
	echo "optional profile — not make run"; \
	echo "NATS: $(PROFILE_COMPOSE)"; \
	if docker compose -f $(PROFILE_COMPOSE) ps --status running --services 2>/dev/null | grep -qx nats; then \
		echo "NATS compose already running on $(NATS_URL)"; \
	elif nc -z 127.0.0.1 $(NATS_PORT) 2>/dev/null; then \
		echo "port $(NATS_PORT) is already in use (not this compose project)."; \
		echo "stop the other listener, or: NATS_PORT=14223 make up-celld-nats"; \
		docker ps --format 'table {{.Names}}\t{{.Ports}}' | grep -E '14222|nats' || true; \
		exit 1; \
	else \
		docker compose -f $(PROFILE_COMPOSE) up -d; \
	fi; \
	echo "building aggregate and Queue relay Workers…"; \
	( cd ../celld/worker && worker-build --release ); \
	( cd ../celld/relay-worker && worker-build --release ); \
	mkdir -p $(CELLD_PROJECT)/build/relay-worker; \
	cp -R $(CELLD_RELAY_PROJECT)/build/. $(CELLD_PROJECT)/build/relay-worker; \
	echo "registering Queue consumer in the persistent celld dev store…"; \
	celld dev $(CELLD_PROJECT)/relay.wrangler.jsonc --host 127.0.0.1 --port $(CELLD_HTTP_PORT) --logs > $(CELLD_RELAY_LOG) 2>&1 & \
	relay_launcher_pid=$$!; \
	relay_ok=0; \
	for i in $$(seq 1 120); do \
		code=$$(curl -s -o /dev/null -w '%{http_code}' "$(CELLD_URL)/.well-known/celld/health" 2>/dev/null || echo 000); \
		if [ "$$code" = "200" ]; then relay_ok=1; break; fi; \
		sleep 0.5; \
	done; \
	if [ "$$relay_ok" != "1" ]; then \
		echo "celld relay registration failed:"; tail -80 $(CELLD_RELAY_LOG); \
		kill "$$relay_launcher_pid" 2>/dev/null || true; exit 1; \
	fi; \
	relay_node_pid=""; \
	for i in $$(seq 1 80); do \
		relay_node_pid=$$(lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN 2>/dev/null | head -1); \
		[ -n "$$relay_node_pid" ] && break; \
		sleep 0.25; \
	done; \
	[ -n "$$relay_node_pid" ] || { echo "celld relay listener PID was not found"; kill "$$relay_launcher_pid" 2>/dev/null || true; exit 1; }; \
	kill "$$relay_launcher_pid" 2>/dev/null || true; \
	kill "$$relay_node_pid" 2>/dev/null || true; \
	for i in $$(seq 1 80); do \
		lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN >/dev/null 2>&1 || break; \
		sleep 0.25; \
	done; \
	if lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN >/dev/null 2>&1; then \
		kill -KILL "$$relay_node_pid" 2>/dev/null || true; \
		for i in $$(seq 1 20); do \
			lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN >/dev/null 2>&1 || break; \
			sleep 0.1; \
		done; \
	fi; \
	if lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN >/dev/null 2>&1; then \
		echo "celld relay listener remained after TERM and KILL"; exit 1; \
	fi; \
	echo "starting aggregate Worker from the same celld dev store…"; \
	celld dev $(CELLD_PROJECT) --host 127.0.0.1 --port $(CELLD_HTTP_PORT) --logs > $(CELLD_LOG) 2>&1 & \
	launcher_pid=$$!; \
	ok=0; \
	for i in $$(seq 1 120); do \
		code=$$(curl -s -o /dev/null -w '%{http_code}' "$(CELLD_URL)/health" 2>/dev/null || echo 000); \
		if [ "$$code" = "200" ]; then ok=1; break; fi; \
		sleep 0.5; \
	done; \
	if [ "$$ok" != "1" ]; then \
		echo "celld not healthy at $(CELLD_URL)/health"; tail -80 $(CELLD_LOG); \
		kill "$$launcher_pid" 2>/dev/null || true; exit 1; \
	fi; \
	node_pid=$$(lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN); \
	echo "$$launcher_pid $$node_pid" > $(CELLD_NODE_PID); \
	echo ""; \
	echo "  CELLD_URL $(CELLD_URL)"; \
	echo "  NATS_URL  $(NATS_URL)"; \
	echo "  smoke:    make test-celld-nats"; \
	echo "  stop:     make down-celld-nats   (NATS only)"; \
	echo "            make down-celld        (celld; state preserved)"; \
	echo "  playground remains: make run"; \
	echo ""

down-celld-nats:
	docker compose -f $(PROFILE_COMPOSE) down
	-@docker rm -f e2e-ui-celld-nats-optional >/dev/null 2>&1 || true
	@echo "NATS profile stopped. celld untouched (make down-celld). playground untouched (make down)."

down-celld:
	@set -e; \
	if [ -f $(CELLD_NODE_PID) ]; then \
		launcher_pid=$$(awk '{print $$1}' $(CELLD_NODE_PID)); \
		node_pid=$$(awk '{print $$2}' $(CELLD_NODE_PID)); \
		launcher_command=$$(ps -p "$$launcher_pid" -o command= 2>/dev/null || true); \
		node_command=$$(ps -p "$$node_pid" -o command= 2>/dev/null || true); \
		case "$$launcher_command" in \
			*"celld dev $(CELLD_PROJECT) --host 127.0.0.1 --port $(CELLD_HTTP_PORT)"*) kill "$$launcher_pid" 2>/dev/null || true ;; \
			"") ;; \
			*) echo "refusing to stop unexpected tracked process $$launcher_pid: $$launcher_command"; exit 1 ;; \
		esac; \
		case "$$node_command" in \
			*"celld --no-control-plane --bucket celld-dev --listen 127.0.0.1:$(CELLD_HTTP_PORT)"*) kill "$$node_pid" 2>/dev/null || true ;; \
			"") ;; \
			*) echo "refusing to stop unexpected celld node $$node_pid: $$node_command"; exit 1 ;; \
		esac; \
		for i in $$(seq 1 160); do \
			lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN >/dev/null 2>&1 || break; \
			sleep 0.25; \
		done; \
		if lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN >/dev/null 2>&1; then \
			echo "tracked celld listener on port $(CELLD_HTTP_PORT) did not stop"; \
			lsof -nP -iTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN; \
			exit 1; \
		fi; \
		rm -f $(CELLD_NODE_PID); \
	fi; \
	if lsof -tiTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN >/dev/null 2>&1; then \
		echo "refusing to stop untracked listener on port $(CELLD_HTTP_PORT)"; \
		lsof -nP -iTCP:$(CELLD_HTTP_PORT) -sTCP:LISTEN; \
		exit 1; \
	fi; \
	echo "celld stopped; $(CELLD_PROJECT)/.celld/dev preserved. NATS: make down-celld-nats."

## Live celld tests (needs make up-celld-nats). Fixture-only checks still run
## in root `cargo test` without CELLD_URL; these set CELLD_URL so HTTP runs.
test-celld: test-celld-http test-celld-nats

test-celld-http:
	cd $(REPO_ROOT) && \
	  CELLD_URL="$(CELLD_URL)" \
	  DISTRIBUTED_INTERNAL_SECRET="$(DISTRIBUTED_INTERNAL_SECRET)" \
	  cargo test --test celld -- --nocapture

test-celld-nats:
	@echo "optional profile smoke — default make test / make run unchanged"
	cd $(REPO_ROOT) && \
	  CELLD_URL="$(CELLD_URL)" NATS_URL="$(NATS_URL)" \
	  DISTRIBUTED_INTERNAL_SECRET="$(DISTRIBUTED_INTERNAL_SECRET)" \
	  cargo test --test e2e_ui_celld_nats_profile --features graphql,http,sqlite -- --nocapture

application-build:
	$(DISTRIBUTED) build .

test: application-build
	$(MAKE) test-domain test-suite ui-test
	@echo "OK — offline domain + suite + UI build + typecheck + structural tests"

## CI-only orchestration: build the real application through Distributed, then
## overlap read-only Rust and UI verification without concurrent UI writers.
ci-offline:
	$(DISTRIBUTED) build .
	@set -eu; \
	status=0; \
	$(MAKE) test-domain test-suite & rust_pid=$$!; \
	( cd ui && $(NPM) test ) & ui_pid=$$!; \
	wait $$rust_pid || status=$$?; \
	wait $$ui_pid || status=$$?; \
	[ $$status -eq 0 ] || exit $$status; \
	echo "OK — generated drift + offline Rust/UI suites"

test-domain:
	cargo test -p todo-domain -p chat-domain -p blob-domain $(CARGO_TEST_FLAGS)

test-suite:
	cargo test -p e2e-suite --test behavioral $(CARGO_TEST_FLAGS)

## Browser e2e (Playwright). Requires make up + make run (UI :5180, API :8791).
test-browser-install:
	$(NPM) install
	npx playwright install chromium

test-browser: test-browser-install
	@if [ ! -f $(ENV_FILE) ]; then echo "missing $(ENV_FILE) — run make up"; exit 1; fi
	@code=$$(curl -s -o /dev/null -w '%{http_code}' "$(UI_URL)/" 2>/dev/null || echo 000); \
	if [ "$$code" = "000" ]; then echo "UI not reachable at $(UI_URL) — start: make run"; exit 1; fi
	$(NPM) run test:browser

js-install:
	cd $(JS_DIR) && $(NPM) install --ignore-scripts

js-build: js-install
	cd $(JS_DIR) && $(NPM) run build

## This fixture links checkout JS so CI exercises the framework source under test.
## distributed build/dev owns every Rust/WASM pure declared by the application.
ui-install: js-build
	cd ui && $(NPM) install

ui-test:
	cd ui && $(NPM) test

## Aggregate read-only contract check (repo root catalog when present).
contracts-check:
	@if [ -f ../../contracts/catalog.json ] || [ -f contracts/catalog.json ]; then \
	  $(MAKE) -C ../.. contracts-check; \
	else \
	  echo "contracts-check: no catalog yet; skipping aggregate gate"; \
	fi

check: application-build
	cargo check --workspace
	cargo test -p todo-domain -p chat-domain -p blob-domain --no-run
	cargo test -p e2e-suite --test behavioral --no-run

clean: stop
	cargo clean
	rm -f .make-runner.log e2e-ui.db
	rm -rf ui/node_modules ui/build ui/.svelte-kit ui/src/lib/blob/pkg

help:
	@echo "e2e-ui"
	@echo "  make up         Postgres + Zitadel + OIDC bootstrap → e2e-ui.env"
	@echo "  make run        coherent build + API + UI supervision (Vite HMR active)"
	@echo "  make test       offline suite + UI structural"
	@echo "  make ci-offline coherent build + offline suites with safe pipeline overlap"
	@echo "  make down       playground docker compose down (not celld/NATS)"
	@echo "  make up-celld-nats    optional celld+NATS profile (not make run)"
	@echo "  rebuild/restart with make up-celld-nats after Worker source changes"
	@echo "  make test-celld      live --test celld + e2e_ui_celld_nats_profile (CI)"
	@echo "  make test-celld-nats  cargo test --test e2e_ui_celld_nats_profile"
	@echo "  make down-celld-nats  stop NATS profile only"
	@echo "  make down-celld       stop celld and preserve its local dev store"
	@echo "  GRAPHIQL=0      disable GraphiQL when running the API"
