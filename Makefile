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

REDIS_URL ?= redis://localhost:6379

# Lib tests that write unscoped tables each get a database of their own,
# recreated on every run so the suite is re-runnable.
POSTGRES_ADMIN_URL ?= postgres://sourced:sourced@localhost:5432
TEST_DATABASES = distributed_projection_protocol distributed_snapshots distributed_rebuild distributed_unique_key_test

# Every service variable `test-local` provides. Naming them in
# DISTRIBUTED_REQUIRE_SERVICES makes a test that cannot reach its service
# fail instead of returning early (see tests/common/src/env.rs).
REQUIRED_SERVICES = DATABASE_URL,AMQP_URL,KAFKA_BROKERS,NATS_URL,REDIS_URL,DISTRIBUTED_TEST_POSTGRES_URL,DISTRIBUTED_SNAPSHOT_TEST_POSTGRES_URL,DISTRIBUTED_REBUILD_TEST_POSTGRES_URL,DISTRIBUTED_UNIQUE_KEY_TEST_POSTGRES_URL,DISTRIBUTED_ARCHIVE_TEST_NATS_URL

test: test-local

.PHONY: test test-local test-cargo test-cli-lifecycle compose-up compose-down

## The whole workspace against real services. This is what CI runs.
test-local:
	set -eu; \
	if [ "$(KEEP_COMPOSE)" != "1" ]; then \
		cleanup() { \
			$(DOCKER_COMPOSE) down $(COMPOSE_DOWN_FLAGS); \
		}; \
		trap cleanup EXIT HUP INT TERM; \
	fi; \
	$(DOCKER_COMPOSE) up $(COMPOSE_UP_FLAGS); \
	for database in $(TEST_DATABASES); do \
		$(DOCKER_COMPOSE) exec -T postgres dropdb -U sourced --if-exists --force "$$database"; \
		$(DOCKER_COMPOSE) exec -T postgres createdb -U sourced "$$database"; \
	done; \
	export DATABASE_URL="$(DATABASE_URL)"; \
	export AMQP_URL="$(AMQP_URL)"; \
	export KAFKA_BROKERS="$(KAFKA_BROKERS)"; \
	export NATS_URL="$(NATS_URL)"; \
	export REDIS_URL="$(REDIS_URL)"; \
	export DISTRIBUTED_ARCHIVE_TEST_NATS_URL="$(NATS_URL)"; \
	export DISTRIBUTED_TEST_POSTGRES_URL="$(POSTGRES_ADMIN_URL)/distributed_projection_protocol"; \
	export DISTRIBUTED_SNAPSHOT_TEST_POSTGRES_URL="$(POSTGRES_ADMIN_URL)/distributed_snapshots"; \
	export DISTRIBUTED_REBUILD_TEST_POSTGRES_URL="$(POSTGRES_ADMIN_URL)/distributed_rebuild"; \
	export DISTRIBUTED_UNIQUE_KEY_TEST_POSTGRES_URL="$(POSTGRES_ADMIN_URL)/distributed_unique_key_test"; \
	export DISTRIBUTED_REQUIRE_SERVICES="$(REQUIRED_SERVICES)"; \
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
