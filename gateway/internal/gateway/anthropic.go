package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/maximhq/bifrost/core/providers/anthropic"
	"github.com/maximhq/bifrost/core/providers/openai"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

const anthropicAPIVersion = "2023-06-01"

type anthropicEngine struct {
	upstreamTimeout          time.Duration
	streamIdleTimeoutSeconds int
	logger                   schemas.Logger
}

func newAnthropicEngine(upstreamTimeout, streamIdleTimeout time.Duration) *anthropicEngine {
	streamIdleSeconds := int(streamIdleTimeout.Round(time.Second) / time.Second)
	if streamIdleSeconds < 1 {
		streamIdleSeconds = 1
	}
	return &anthropicEngine{
		upstreamTimeout:          upstreamTimeout,
		streamIdleTimeoutSeconds: streamIdleSeconds,
		logger:                   discardBifrostLogger{},
	}
}

func (e *anthropicEngine) client(ctx context.Context) *fasthttp.Client {
	return &fasthttp.Client{
		Name:                          "agent-hub-model-gateway",
		DialTimeout:                   requestDialer(ctx),
		ReadTimeout:                   e.upstreamTimeout,
		WriteTimeout:                  e.upstreamTimeout,
		MaxConnWaitTimeout:            e.upstreamTimeout,
		MaxIdemponentCallAttempts:     1,
		NoDefaultUserAgentHeader:      true,
		DisableHeaderNamesNormalizing: false,
		RetryIfErr: func(*fasthttp.Request, int, error) (bool, bool) {
			return false, false
		},
	}
}

func requestDialer(ctx context.Context) fasthttp.DialFuncWithTimeout {
	return func(address string, timeout time.Duration) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		tracked := &requestConn{Conn: conn, closed: make(chan struct{})}
		go func() {
			select {
			case <-ctx.Done():
				_ = tracked.Close()
			case <-tracked.closed:
			}
		}()
		return tracked, nil
	}
}

type requestConn struct {
	net.Conn
	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

func (c *requestConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

func (s *server) proxyAnthropic(w http.ResponseWriter, r *http.Request, envelope proxyEnvelope, body []byte, endpoint *url.URL) {
	if err := validateResponsesRepresentability(body, protocolAnthropicMessages); err != nil {
		writeError(w, http.StatusBadRequest, "unsupported_protocol_feature", err.Error(), envelope.APIKey)
		return
	}
	var wireRequest openai.OpenAIResponsesRequest
	if err := json.Unmarshal(body, &wireRequest); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_responses_request", "invalid Responses request", envelope.APIKey)
		return
	}
	if strings.TrimSpace(wireRequest.Model) == "" {
		writeError(w, http.StatusBadRequest, "invalid_responses_request", "model is required", envelope.APIKey)
		return
	}
	downstreamCtx, cancelDownstream := downstreamContext(w, r)
	defer cancelDownstream()
	bifrostCtx, cancelBifrost := schemas.NewBifrostContextWithCancel(downstreamCtx)
	defer cancelBifrost()
	bifrostCtx.SetValue(schemas.BifrostContextKeyRequestID, envelope.RequestID)
	bifrostCtx.SetValue(schemas.BifrostContextKeyDirectKey, schemas.Key{
		ID:     envelope.RequestID,
		Name:   "request-scoped",
		Value:  schemas.SecretVar{Val: envelope.APIKey},
		Models: schemas.WhiteList{"*"},
	})
	safeHeaders := safeAnthropicHeaders(envelope.Headers)
	bifrostCtx.SetValue(schemas.BifrostContextKeyExtraHeaders, safeHeaders)

	request := wireRequest.ToBifrostResponsesRequest(bifrostCtx)
	if request == nil {
		writeError(w, http.StatusBadRequest, "invalid_responses_request", "invalid Responses request", envelope.APIKey)
		return
	}
	request.Provider = schemas.Anthropic
	request.Fallbacks = nil
	mergeAnthropicRequestParameters(request, envelope.RequestParameters)
	requestURL := upstreamRequestURL(endpoint, "/v1/messages", envelope.Query)
	stream := wireRequest.Stream != nil && *wireRequest.Stream
	if stream {
		s.proxyAnthropicStream(w, r, envelope, bifrostCtx, requestURL, request, safeHeaders)
		return
	}
	s.proxyAnthropicJSON(w, r, envelope, bifrostCtx, requestURL, request, safeHeaders)
}

func mergeAnthropicRequestParameters(request *schemas.BifrostResponsesRequest, parameters modelRequestParameters) {
	if request.Params == nil {
		request.Params = &schemas.ResponsesParameters{}
	}
	if parameters.Temperature != nil {
		request.Params.Temperature = parameters.Temperature
		request.Params.TopP = nil
	}
	if parameters.TopP != nil {
		request.Params.TopP = parameters.TopP
		request.Params.Temperature = nil
	}
	if parameters.MaxTokens != nil {
		value := int(*parameters.MaxTokens)
		request.Params.MaxOutputTokens = &value
	}
}

func (s *server) proxyAnthropicJSON(
	w http.ResponseWriter,
	r *http.Request,
	envelope proxyEnvelope,
	bifrostCtx *schemas.BifrostContext,
	requestURL string,
	request *schemas.BifrostResponsesRequest,
	headers map[string][]string,
) {
	response, bifrostErr := anthropic.HandleAnthropicResponsesRequest(
		bifrostCtx,
		s.anthropic.client(bifrostCtx),
		requestURL,
		request,
		anthropic.AnthropicRequestBuildConfig{
			Provider:    schemas.Anthropic,
			IsStreaming: false,
		},
		anthropicAuthHeaders(bifrostCtx),
		flattenHeaders(headers),
		nil,
		s.anthropic.logger,
	)
	if bifrostErr != nil {
		if r.Context().Err() != nil {
			return
		}
		writeBifrostError(w, bifrostErr, envelope.APIKey)
		return
	}
	payload, err := marshalOpenAIWire(response.WithDefaults())
	if err != nil {
		writeError(w, http.StatusBadGateway, "response_conversion_error", "unable to encode converted response", envelope.APIKey)
		return
	}
	copyBifrostResponseHeaders(w.Header(), bifrostCtx, envelope.APIKey)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func (s *server) proxyAnthropicStream(
	w http.ResponseWriter,
	r *http.Request,
	envelope proxyEnvelope,
	bifrostCtx *schemas.BifrostContext,
	requestURL string,
	request *schemas.BifrostResponsesRequest,
	headers map[string][]string,
) {
	requestBody, err := buildAnthropicBody(bifrostCtx, request)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_responses_request", err.Error(), envelope.APIKey)
		return
	}
	stream, bifrostErr := anthropic.HandleAnthropicResponsesStream(
		bifrostCtx,
		s.anthropic.client(bifrostCtx),
		requestURL,
		requestBody,
		map[string]string{
			"Content-Type":      "application/json",
			"anthropic-version": anthropicAPIVersion,
			"Accept":            "text/event-stream",
			"Cache-Control":     "no-cache",
			"x-api-key":         directKeyValue(bifrostCtx),
		},
		flattenHeaders(headers),
		s.anthropic.streamIdleTimeoutSeconds,
		nil,
		false,
		false,
		schemas.Anthropic,
		identityPostHook,
		nil,
		nil,
		s.anthropic.logger,
		nil,
	)
	if bifrostErr != nil {
		if r.Context().Err() != nil {
			return
		}
		writeBifrostError(w, bifrostErr, envelope.APIKey)
		return
	}
	copyBifrostResponseHeaders(w.Header(), bifrostCtx, envelope.APIKey)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "close")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	for chunk := range stream {
		if chunk == nil {
			continue
		}
		if chunk.BifrostError != nil {
			payload := openAIStreamError(chunk.BifrostError, envelope.APIKey)
			_ = writeSSE(w, "error", payload)
			return
		}
		if chunk.BifrostResponsesStreamResponse == nil {
			continue
		}
		converted := chunk.BifrostResponsesStreamResponse.WithDefaults()
		if converted == nil {
			continue
		}
		payload, err := marshalOpenAIWire(converted)
		if err != nil {
			_ = writeSSE(w, "error", openAIStreamError(nil, envelope.APIKey))
			return
		}
		if err := writeSSE(w, string(converted.Type), payload); err != nil {
			return
		}
	}
}

func downstreamContext(w http.ResponseWriter, r *http.Request) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(r.Context())
	notifier, ok := w.(http.CloseNotifier)
	if !ok {
		return ctx, cancel
	}
	disconnected := notifier.CloseNotify()
	go func() {
		select {
		case <-disconnected:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func buildAnthropicBody(ctx *schemas.BifrostContext, request *schemas.BifrostResponsesRequest) ([]byte, error) {
	body, bifrostErr := anthropic.BuildAnthropicResponsesRequestBody(ctx, request, anthropic.AnthropicRequestBuildConfig{
		Provider:    schemas.Anthropic,
		IsStreaming: true,
	})
	if bifrostErr != nil || len(body) == 0 {
		return nil, errAnthropicRequestBuild
	}
	return body, nil
}

func anthropicAuthHeaders(ctx *schemas.BifrostContext) map[string]string {
	return map[string]string{
		"anthropic-version": anthropicAPIVersion,
		"x-api-key":         directKeyValue(ctx),
	}
}

func directKeyValue(ctx *schemas.BifrostContext) string {
	key, ok := ctx.Value(schemas.BifrostContextKeyDirectKey).(schemas.Key)
	if !ok {
		return ""
	}
	return key.Value.GetValue()
}

func safeAnthropicHeaders(source map[string][]string) map[string][]string {
	destination := make(http.Header)
	copySafeRequestHeaders(destination, source)
	destination.Del("Authorization")
	destination.Del("X-Api-Key")
	destination.Del("Anthropic-Version")
	return map[string][]string(destination)
}

func flattenHeaders(headers map[string][]string) map[string]string {
	result := make(map[string]string, len(headers))
	for name, values := range headers {
		if len(values) > 0 {
			result[name] = strings.Join(values, ", ")
		}
	}
	return result
}

func identityPostHook(_ *schemas.BifrostContext, response *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError) {
	return response, bifrostErr
}

func copyBifrostResponseHeaders(destination http.Header, ctx *schemas.BifrostContext, secret string) {
	headers, ok := ctx.Value(schemas.BifrostContextKeyProviderResponseHeaders).(map[string]string)
	if !ok {
		return
	}
	source := make(http.Header, len(headers))
	for name, value := range headers {
		source.Set(name, value)
	}
	copySafeResponseHeaders(destination, source, secret)
}

func marshalOpenAIWire(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var document any
	if err := json.Unmarshal(payload, &document); err != nil {
		return nil, err
	}
	stripBifrostFields(document)
	return json.Marshal(document)
}

func stripBifrostFields(value any) {
	switch typed := value.(type) {
	case map[string]any:
		delete(typed, "extra_fields")
		delete(typed, "provider_extra_fields")
		for _, child := range typed {
			stripBifrostFields(child)
		}
	case []any:
		for _, child := range typed {
			stripBifrostFields(child)
		}
	}
}

func writeBifrostError(w http.ResponseWriter, bifrostErr *schemas.BifrostError, secret string) {
	status := http.StatusBadGateway
	message := "upstream request failed"
	code := "upstream_error"
	if bifrostErr != nil {
		status = statusFromBifrostError(bifrostErr.StatusCode)
		if bifrostErr.Error != nil {
			if bifrostErr.Error.Message != "" {
				message = bifrostErr.Error.Message
			}
			if bifrostErr.Error.Type != nil && *bifrostErr.Error.Type != "" {
				code = *bifrostErr.Error.Type
			}
		}
	}
	writeError(w, status, code, message, secret)
}

func openAIStreamError(bifrostErr *schemas.BifrostError, secret string) []byte {
	message := "upstream stream failed"
	code := "upstream_stream_error"
	if bifrostErr != nil && bifrostErr.Error != nil {
		if bifrostErr.Error.Message != "" {
			message = bifrostErr.Error.Message
		}
		if bifrostErr.Error.Type != nil && *bifrostErr.Error.Type != "" {
			code = *bifrostErr.Error.Type
		}
	}
	payload, _ := json.Marshal(map[string]string{
		"type":    "error",
		"code":    code,
		"message": sanitizeMessage(message, secret),
	})
	return payload
}

type discardBifrostLogger struct{}

func (discardBifrostLogger) Debug(string, ...any)                   {}
func (discardBifrostLogger) Info(string, ...any)                    {}
func (discardBifrostLogger) Warn(string, ...any)                    {}
func (discardBifrostLogger) Error(string, ...any)                   {}
func (discardBifrostLogger) Fatal(string, ...any)                   {}
func (discardBifrostLogger) SetLevel(schemas.LogLevel)              {}
func (discardBifrostLogger) SetOutputType(schemas.LoggerOutputType) {}
func (discardBifrostLogger) LogHTTPRequest(schemas.LogLevel, string) schemas.LogEventBuilder {
	return schemas.NoopLogEvent
}

var errAnthropicRequestBuild = errors.New("unable to convert Responses request")
