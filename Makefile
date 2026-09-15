SHELL = /bin/sh

CARGO ?= cargo
BATS ?= bats
DOCKER_COMPOSE ?= docker compose

DISTRIBUTED_BIN ?= $(CURDIR)/target/debug/distributed

COMPOSE_UP_FLAGS ?= -d --wait
COMPOSE_DOWN_FLAGS ?= --remove-orphans
KEEP_COMPOSE ?= 0

CARGO_TEST_ARGS ?= --workspace --all-features

DATABASE_URL ?= postgres://sourced:sourced@localhost:5432/distributed
AMQP_URL ?= amqp://guest:guest@localhost:5672/%2f
KAFKA_BROKERS ?= 127.0.0.1:9092
NATS_URL ?= nats://localhost:4222

test: test-local

.PHONY: test test-local test-cargo test-cli-lifecycle compose-up compose-down

test-local:
	set -eu; \
	if [ "$(KEEP_COMPOSE)" != "1" ]; then \
		cleanup() { \
			$(DOCKER_COMPOSE) down $(COMPOSE_DOWN_FLAGS); \
		}; \
		trap cleanup EXIT HUP INT TERM; \
	fi; \
	$(DOCKER_COMPOSE) up $(COMPOSE_UP_FLAGS); \
	export DATABASE_URL="$(DATABASE_URL)"; \
	export AMQP_URL="$(AMQP_URL)"; \
	export KAFKA_BROKERS="$(KAFKA_BROKERS)"; \
	export NATS_URL="$(NATS_URL)"; \
	$(CARGO) test $(CARGO_TEST_ARGS) --all-targets; \
	$(CARGO) test $(CARGO_TEST_ARGS) --doc

test-cargo:
	$(CARGO) test $(CARGO_TEST_ARGS) --all-targets
	$(CARGO) test $(CARGO_TEST_ARGS) --doc

test-cli-lifecycle:
	$(CARGO) build -p distributed_cli --bin distributed
	DISTRIBUTED_BIN="$(DISTRIBUTED_BIN)" $(BATS) distributed_cli/tests/bats/lifecycle.bats

compose-up:
	$(DOCKER_COMPOSE) up $(COMPOSE_UP_FLAGS)

compose-down:
	$(DOCKER_COMPOSE) down $(COMPOSE_DOWN_FLAGS)

LOAD_MANIFEST ?= tests/load/Cargo.toml
LOAD_REPO ?= memory
LOAD_BIND ?= 127.0.0.1:8790
LOAD_SCENARIO ?= unique-create
LOAD_CONCURRENCY ?= 32
LOAD_DURATION ?= 15s
LOAD_WARMUP ?= 2s
LOAD_DATABASE_URL ?= $(DATABASE_URL)
LOAD_SQLITE_PATH ?= target/load.sqlite

LOAD_FEATURES ?= kafka,rabbitmq
LOAD_FILTER ?=
LOAD_SUITE_FLAGS ?=
LOAD_SNAPSHOTS ?=

.PHONY: load-host load-client load-run load-matrix load-test load-suite

## Opt-in load harness (not part of `make test`). See tests/load/src/bin/*.rs --help.
load-host:
	$(CARGO) run --manifest-path $(LOAD_MANIFEST) --release --bin load-host -- \
		--repo $(LOAD_REPO) --bind $(LOAD_BIND) \
		$(if $(LOAD_DATABASE_URL),--database-url $(LOAD_DATABASE_URL),) \
		--sqlite-path $(LOAD_SQLITE_PATH) \
		$(if $(LOAD_SNAPSHOTS),--snapshots $(LOAD_SNAPSHOTS),)

load-client:
	$(CARGO) run --manifest-path $(LOAD_MANIFEST) --release --bin load-client -- \
		--url http://$(LOAD_BIND) --scenario $(LOAD_SCENARIO) \
		--concurrency $(LOAD_CONCURRENCY) --duration $(LOAD_DURATION) \
		--warmup $(LOAD_WARMUP) --repo $(LOAD_REPO) \
		$(if $(LOAD_SNAPSHOTS),--snapshots $(LOAD_SNAPSHOTS),)

## Build, start host, wait for /health, run client, stop host.
load-run:
	@set -eu; \
	$(CARGO) build --manifest-path $(LOAD_MANIFEST) --release --bins; \
	host_bin="tests/load/target/release/load-host"; \
	client_bin="tests/load/target/release/load-client"; \
	if [ ! -x "$$host_bin" ]; then host_bin="target/release/load-host"; fi; \
	if [ ! -x "$$client_bin" ]; then client_bin="target/release/load-client"; fi; \
	host_args="--repo $(LOAD_REPO) --bind $(LOAD_BIND) --sqlite-path $(LOAD_SQLITE_PATH)"; \
	if [ -n "$(LOAD_DATABASE_URL)" ]; then host_args="$$host_args --database-url $(LOAD_DATABASE_URL)"; fi; \
	if [ -n "$(LOAD_SNAPSHOTS)" ]; then host_args="$$host_args --snapshots $(LOAD_SNAPSHOTS)"; fi; \
	$$host_bin $$host_args & host_pid=$$!; \
	cleanup() { kill $$host_pid >/dev/null 2>&1 || true; wait $$host_pid >/dev/null 2>&1 || true; }; \
	trap cleanup EXIT INT TERM; \
	ready=0; \
	for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		if curl -sf "http://$(LOAD_BIND)/health" >/dev/null; then ready=1; break; fi; \
		sleep 0.25; \
	done; \
	if [ "$$ready" != "1" ]; then echo "load-host did not become healthy at http://$(LOAD_BIND)/health" >&2; exit 1; fi; \
	client_args="--url http://$(LOAD_BIND) --scenario $(LOAD_SCENARIO) \
		--concurrency $(LOAD_CONCURRENCY) --duration $(LOAD_DURATION) \
		--warmup $(LOAD_WARMUP) --repo $(LOAD_REPO)"; \
	if [ -n "$(LOAD_SNAPSHOTS)" ]; then client_args="$$client_args --snapshots $(LOAD_SNAPSHOTS)"; fi; \
	$$client_bin $$client_args

## Compare memory, sqlite, and postgres (postgres needs `make compose-up` or a live DATABASE_URL).
load-matrix:
	@set -eu; \
	for repo in memory sqlite postgres; do \
		echo "======== $$repo / $(LOAD_SCENARIO) ========"; \
		$(MAKE) load-run LOAD_REPO=$$repo LOAD_SCENARIO=$(LOAD_SCENARIO) \
			LOAD_CONCURRENCY=$(LOAD_CONCURRENCY) LOAD_DURATION=$(LOAD_DURATION) \
			LOAD_WARMUP=$(LOAD_WARMUP) LOAD_BIND=$(LOAD_BIND); \
	done

load-test:
	$(CARGO) test --manifest-path $(LOAD_MANIFEST)

## Full Counter suite: every dispatch, bus (incl. kafka/rabbitmq), lock, snapshot, scenario.
##   make compose-up && make load-suite
##   make load-suite LOAD_FILTER=direct LOAD_DURATION=3s
##   make load-run LOAD_SNAPSHOTS=10
load-suite:
	DATABASE_URL="$(LOAD_DATABASE_URL)" \
	NATS_URL="$(NATS_URL)" \
	KAFKA_BROKERS="$(KAFKA_BROKERS)" \
	AMQP_URL="$(AMQP_URL)" \
	$(CARGO) run --manifest-path $(LOAD_MANIFEST) --release \
		$(if $(LOAD_FEATURES),--features $(LOAD_FEATURES),) \
		--bin load-suite -- \
		--duration $(LOAD_DURATION) --warmup $(LOAD_WARMUP) \
		--concurrency $(LOAD_CONCURRENCY) \
		$(if $(LOAD_FILTER),--filter $(LOAD_FILTER),) \
		$(LOAD_SUITE_FLAGS)

.PHONY: contracts-check

## Read-only aggregate contract lifecycle check (never writes tracked files).
contracts-check:
	$(CARGO) run -p distributed_cli --quiet -- contracts check --root . --catalog contracts/catalog.json --output human
