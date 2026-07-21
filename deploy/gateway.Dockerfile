FROM golang:1.26.4-alpine AS builder
WORKDIR /src
COPY gateway/go.mod gateway/go.sum ./
RUN go mod download
COPY gateway/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/model-gateway ./cmd/model-gateway

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 agenthub \
    && adduser -S -D -H -u 10001 -G agenthub agenthub
COPY --from=builder /out/model-gateway /usr/local/bin/model-gateway
USER 10001:10001
EXPOSE 8090
HEALTHCHECK --interval=5s --timeout=3s --retries=20 \
    CMD wget -q -O /dev/null http://127.0.0.1:8090/readyz || exit 1
ENTRYPOINT ["model-gateway"]
