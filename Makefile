.PHONY: install dev test test-infra lint image-build infra-up infra-up-local infra-pull infra-down

# The overlay is what publishes the infrastructure ports on 127.0.0.1; the base
# file alone keeps them inside the Compose network, which is right for a
# deployment and useless for anything running on the host.
DEV_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.dev.yml

install:
	cargo fetch

dev:
	cargo run

# tests/api.rs drives the real pipeline end to end, so it needs PostgreSQL,
# ClickHouse, Redpanda and RustFS listening on the host: run `make test-infra`
# first. Without them the integration tests fail while building their state,
# which reads as a broken build rather than as missing infrastructure.
test:
	cargo test

# Exactly what the integration suite talks to, and nothing more — naming the
# services keeps `up` from building the api and web images, which the suite does
# not use and which cost minutes. CI runs this same target, so there is one
# definition of the test environment. `--wait` matters: a broker that is up but
# has not elected a leader fails the first `partition_client` and nothing else,
# which reads as a broken test rather than as a broker that needed another
# second.
#
# Only the two databases are started with `up --wait`, because they are the only
# two with no one-shot dependency: `up --wait` exits 1 when any container it
# started has exited, including a chown sidecar that exited 0
# (docker/compose#10596, open). `run` has no such problem — it honours the same
# depends_on conditions — so RustFS comes up behind the bucket line and Redpanda
# behind the retention line, each waiting for the health check its sidecar sits
# in front of, and each failing here with its own message instead of as a
# confusing error inside a test.
test-infra:
	$(DEV_COMPOSE) up -d --wait postgres clickhouse
	$(DEV_COMPOSE) run --rm create-bucket
	# Not needed by the suite — retention has no bearing on a test that produces
	# a handful of records. It runs here so that CI executes the sidecar on every
	# build: its admin host flag, its `key=value` form and its `--no-confirm` are
	# each the kind of mistake that fails this one container and nothing else,
	# which on a deployment means the topic quietly keeps whatever it had.
	$(DEV_COMPOSE) run --rm redpanda-config

lint:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

image-build:
	$(DEV_COMPOSE) build api web

infra-up:
	docker compose up -d

infra-up-local:
	$(DEV_COMPOSE) up -d

infra-pull:
	docker compose pull api web

infra-down:
	docker compose down
