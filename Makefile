.PHONY: install dev test lint image-build infra-up infra-up-local infra-pull infra-down

install:
	cargo fetch

dev:
	cargo run

test:
	cargo test

lint:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

image-build:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml build api web

infra-up:
	docker compose up -d

infra-up-local:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

infra-pull:
	docker compose pull api web

infra-down:
	docker compose down
