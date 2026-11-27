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

# Lib tests that write unscoped tables each get a database of their own.
# Tests with fixed table names also take a throwaway schema in it, so a run
# never depends on what an earlier one left; recreating starts CI clean.
POSTGRES_ADMIN_URL ?= postgres://sourced:sourced@localhost:5432
TEST_DATABASES = distributed_projection_protocol distributed_snapshots distributed_rebuild distributed_unique_key_test

# Every service variable `test-local` provides. Naming them in
# DISTRIBUTED_REQUIRE_SERVICES makes a test that cannot reach its service
# fail instead of returning early (see tests/common/src/env.rs).
REQUIRED_SERVICES = DATABASE_URL,AMQP_URL,KAFKA_BROKERS,NATS_URL,REDIS_URL,DISTRIBUTED_TEST_POSTGRES_URL,DISTRIBUTED_SNAPSHOT_TEST_POSTGRES_URL,DISTRIBUTED_REBUILD_TEST_POSTGRES_URL,DISTRIBUTED_UNIQUE_KEY_TEST_POSTGRES_URL,DISTRIBUTED_ARCHIVE_TEST_NATS_URL

# Shell fragment for a recipe that runs tests against the compose services:
# recreates TEST_DATABASES, then exports every service variable and
# DISTRIBUTED_REQUIRE_SERVICES. The services must be up. Start the recipe
# with `set -eu;` and put the fragment before the test command, on one
# logical line: `set -eu; $(TEST_SERVICES_ENV) cargo nextest run ...`.
TEST_SERVICES_ENV = \
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
	export DISTRIBUTED_REQUIRE_SERVICES="$(REQUIRED_SERVICES)";

# Shell fragment that starts the compose services and, unless KEEP_COMPOSE=1,
# stops them when the recipe exits. Same rules as TEST_SERVICES_ENV, which
# follows it: `set -eu; $(COMPOSE_SESSION) $(TEST_SERVICES_ENV) ...`.
COMPOSE_SESSION = \
	if [ "$(KEEP_COMPOSE)" != "1" ]; then \
		cleanup() { \
			$(DOCKER_COMPOSE) down $(COMPOSE_DOWN_FLAGS); \
		}; \
		trap cleanup EXIT HUP INT TERM; \
	fi; \
	$(DOCKER_COMPOSE) up $(COMPOSE_UP_FLAGS);

# `make coverage-local` writes lcov.info and summary.json here.
COVERAGE_DIR = target/coverage

# Per-test coverage footprints (docs/guides/testing.md). The full run writes
# FOOTPRINTS_DIR; a partial run (ARGS=...) must name its own OUT=.
FOOTPRINTS_DIR = target/test-footprints
FOOTPRINTS_ALLOWLIST = scripts/test-footprints/timing-dependent-lines.txt

test: test-local

.PHONY: test test-local coverage-local test-cargo test-cli-lifecycle test-footprints test-footprints-diff compose-up compose-down

## The whole workspace against real services. This is what CI runs.
## Tests run under cargo-nextest, one process per test (.config/nextest.toml;
## NEXTEST_PROFILE=ci for JUnit and no fail-fast); nextest does not run
## doctests, so `cargo test --doc` follows.
test-local:
	set -eu; \
	$(COMPOSE_SESSION) \
	$(TEST_SERVICES_ENV) \
	$(CARGO) nextest run $(CARGO_TEST_ARGS) --all-targets; \
	$(CARGO) test $(CARGO_TEST_ARGS) --doc

## `test-local`'s nextest run under cargo-llvm-cov, then one line coverage
## number for the workspace's production code: per crate and TOTAL, with
## COVERAGE_DIR/lcov.info. Test files are dropped by the footprint
## classifier (scripts/coverage/ignore_regex.py); doctests do not run. Builds
## in cargo-llvm-cov's default target/llvm-cov-target, which `make
## test-footprints` also uses: dependencies stay warm for both, but every run
## first removes old profiles and the workspace crates' artifacts (`llvm-cov
## clean --workspace`), so the proc-macro crates' build-time expansion is
## measured afresh and no earlier run leaks in. Run one at a time. The
## reports cover the packages CARGO_TEST_ARGS selects (`--workspace`, `-p`;
## `llvm-cov report` takes no `--exclude`).
coverage-local:
	set -eu; \
	$(COMPOSE_SESSION) \
	$(TEST_SERVICES_ENV) \
	$(CARGO) llvm-cov clean --workspace; \
	$(CARGO) llvm-cov nextest --no-report $(CARGO_TEST_ARGS) --all-targets; \
	select=; set -- $(CARGO_TEST_ARGS); \
	while [ $$# -gt 0 ]; do \
		case "$$1" in \
			--workspace | -p?* | --package=*) select="$$select $$1" ;; \
			-p | --package) select="$$select $$1 $${2-}"; shift ;; \
		esac; \
		shift; \
	done; \
	ignore=$$(python3 -B scripts/coverage/ignore_regex.py); \
	mkdir -p $(COVERAGE_DIR); \
	$(CARGO) llvm-cov report $$select --lcov --output-path $(COVERAGE_DIR)/lcov.info --ignore-filename-regex "$$ignore"; \
	$(CARGO) llvm-cov report $$select --json --summary-only --output-path $(COVERAGE_DIR)/summary.json --ignore-filename-regex "$$ignore"; \
	python3 -B scripts/coverage/summary.py $(COVERAGE_DIR)/summary.json

test-cargo:
	$(CARGO) nextest run $(CARGO_TEST_ARGS) --all-targets
	$(CARGO) test $(CARGO_TEST_ARGS) --doc

## Every test, one instrumented process each, against the compose services
## (run `make compose-up` first). ARGS="..." goes to nextest, e.g.
## ARGS="-p distributed_cli --run-ignored all" OUT=target/footprints-cli.
test-footprints:
	@if [ -n "$(ARGS)" ] && { [ -z "$(OUT)" ] || [ "$(abspath $(OUT))" = "$(abspath $(FOOTPRINTS_DIR))" ]; }; then \
		echo "test-footprints: a partial run (ARGS=...) needs OUT=<dir> other than $(FOOTPRINTS_DIR)" >&2; exit 2; \
	fi
	set -eu; \
	$(TEST_SERVICES_ENV) \
	scripts/test-footprints/run.sh "$(or $(OUT),$(FOOTPRINTS_DIR))" $(ARGS)

## Lines covered in BASE (a saved output directory) and not in OUT (default
## the full run's). Exits non-zero when a covered production line was lost.
test-footprints-diff:
	@if [ -z "$(BASE)" ]; then \
		echo "usage: make test-footprints-diff BASE=<saved output dir> [OUT=<dir>]" >&2; exit 2; \
	fi
	python3 -B scripts/test-footprints/diff.py "$(BASE)" "$(or $(OUT),$(FOOTPRINTS_DIR))" --allowlist $(FOOTPRINTS_ALLOWLIST)

test-cli-lifecycle:
	$(CARGO) build -p distributed_cli --bin distributed
	DISTRIBUTED_BIN="$(DISTRIBUTED_BIN)" $(BATS) distributed_cli/tests/bats/lifecycle.bats

compose-up:
	$(DOCKER_COMPOSE) up $(COMPOSE_UP_FLAGS)

compose-down:
	$(DOCKER_COMPOSE) down $(COMPOSE_DOWN_FLAGS)
