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

.PHONY: contracts-check

## Read-only aggregate contract lifecycle check (never writes tracked files).
contracts-check:
	$(CARGO) run -p distributed_cli --quiet -- contracts check --root . --catalog distributed.contracts.json --output human
