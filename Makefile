.PHONY: install dev test lint image-build infra-up infra-down

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
	docker compose build api web

infra-up:
	docker compose up -d

infra-down:
	docker compose down
