ARG RUST_VERSION=1.95

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 mjai

WORKDIR /app
COPY --from=builder /app/target/release/mjai-management /usr/local/bin/mjai-management

ENV MJAI_LISTEN=0.0.0.0:8000 \
    MJAI_DATA_DIR=/var/lib/mjai \
    RUST_LOG=mjai_management=info,tower_http=info

RUN mkdir -p /var/lib/mjai && chown -R mjai:mjai /var/lib/mjai
USER mjai

EXPOSE 8000
VOLUME ["/var/lib/mjai"]
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD curl --fail --silent http://127.0.0.1:8000/healthz || exit 1

ENTRYPOINT ["mjai-management"]
