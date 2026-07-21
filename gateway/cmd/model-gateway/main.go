package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/agent-hub/model-gateway/internal/gateway"
)

const (
	defaultListenAddress    = "0.0.0.0:8090"
	defaultMaxEnvelopeBytes = 32 << 20
	defaultUpstreamTimeout  = 60 * time.Second
	defaultStreamIdle       = 120 * time.Second
)

func main() {
	logger := log.New(os.Stdout, "model-gateway ", log.LstdFlags|log.LUTC)
	handler, err := gateway.NewHandler(gateway.Config{
		AuthToken:         os.Getenv("MODEL_GATEWAY_AUTH_TOKEN"),
		MaxEnvelopeBytes:  envInt64("MODEL_GATEWAY_MAX_ENVELOPE_BYTES", defaultMaxEnvelopeBytes),
		UpstreamTimeout:   envDuration("MODEL_GATEWAY_UPSTREAM_TIMEOUT", defaultUpstreamTimeout),
		StreamIdleTimeout: envDuration("MODEL_GATEWAY_STREAM_IDLE_TIMEOUT", defaultStreamIdle),
		Logger:            logger,
	})
	if err != nil {
		logger.Fatal(err)
	}
	address := os.Getenv("MODEL_GATEWAY_LISTEN_ADDR")
	if address == "" {
		address = defaultListenAddress
	}
	server := &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Printf("shutdown_error=%q", err.Error())
		}
	}()
	logger.Printf("listening address=%s", address)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal(err)
	}
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		log.Fatalf("%s must be a positive duration", name)
	}
	return parsed
}

func envInt64(name string, fallback int64) int64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		log.Fatalf("%s must be a positive integer", name)
	}
	return parsed
}
