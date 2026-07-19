FROM node:24-bookworm-slim AS frontend-builder
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci
COPY frontend .
RUN npm run build

FROM rust:1.91-bookworm AS backend-builder
WORKDIR /app
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates
RUN cargo build --release -p agent-hub-backend

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 agenthub \
    && useradd --system --uid 10001 --gid agenthub --no-create-home agenthub \
    && mkdir -p /var/lib/agent-hub/codex-artifacts \
    && chown -R agenthub:agenthub /var/lib/agent-hub
COPY --from=backend-builder /app/target/release/agent-hub-backend /usr/local/bin/agent-hub-backend
COPY --from=frontend-builder /app/dist /app/frontend/dist
USER 10001:10001
CMD ["agent-hub-backend"]
