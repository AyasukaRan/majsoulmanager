ARG RUST_VERSION=1.95

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
# The binary applies both schemas itself, so they are `include_str!`d into it:
# the initdb mounts only run when a data volume is first created and cannot
# reach an existing deployment.
COPY migrations ./migrations
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
# python3 and curl_cffi are here for the protocol modules, which run as child
# processes and find their interpreter through their own shebang.
#
# Not optional for registration: it has no builtin, because rustls cannot
# produce Chrome's ClientHello and a brand new account has nothing else to be
# judged on. An image without this can install the module and still not run it.
# The login module becomes usable as a side effect; it stays off by default.
#
# `--break-system-packages` because bookworm marks its Python externally managed
# (PEP 668). There is no system package manager to conflict with inside a
# single-purpose image, and the alternative — a venv — would need its bin on
# PATH ahead of /usr/bin for a shebang that says `python3` to find it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl python3 python3-pip \
    && pip3 install --no-cache-dir --break-system-packages 'curl_cffi>=0.14' \
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
