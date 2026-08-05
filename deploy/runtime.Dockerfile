FROM rust:1.91-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates
RUN cargo build --release -p agent-hub-runtime

FROM node:24-bookworm-slim AS pi-builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git unzip \
    && rm -rf /var/lib/apt/lists/*
COPY .git/modules/third_party/pi /app/.git/modules/third_party/pi
COPY third_party/pi third_party/pi
COPY third_party/pi-model-data third_party/pi-model-data
COPY third_party/pi-patches third_party/pi-patches
COPY scripts/build-pi-standalone.sh scripts/build-pi-standalone.sh
RUN printf 'gitdir: ../../.git/modules/third_party/pi\n' > third_party/pi/.git \
    && git config --global --add safe.directory /app/third_party/pi \
    && scripts/build-pi-standalone.sh --out /opt/pi-runtime

FROM debian:bookworm-slim AS runtime-base
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl fd-find git jq openssh-client python3 ripgrep \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 agenthub \
    && useradd --system --uid 10001 --gid agenthub --no-create-home agenthub \
    && mkdir -p /var/lib/agent-hub-runtime /workspace /agent-state \
    && chown -R agenthub:agenthub /var/lib/agent-hub-runtime

FROM runtime-base
COPY --from=builder /app/target/release/agent-hub-runtime /usr/local/bin/agent-hub-runtime
COPY --from=pi-builder --chown=root:root /opt/pi-runtime /opt/agent-hub/pi
RUN chmod -R a-w /opt/agent-hub/pi
ENV ENGINE_BIN=/opt/agent-hub/pi/pi
ENV RUNTIME_ENGINE_VERSION=0.81.1
# The control process needs CAP_SYS_ADMIN to create per-Session mount
# namespaces; Pi itself is dropped to UID/GID 10001 inside the pre-exec hook.
USER root
CMD ["agent-hub-runtime"]
