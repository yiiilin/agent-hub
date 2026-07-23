package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"
)

const (
	healthPath    = "/healthz"
	readinessPath = "/readyz"
	proxyPath     = "/internal/v1/responses"

	protocolOpenAIResponses       = "openai_responses"
	protocolOpenAIChatCompletions = "openai_chat_completions"
	protocolAnthropicMessages     = "anthropic_messages"
)

type Config struct {
	AuthToken         string
	MaxEnvelopeBytes  int64
	UpstreamTimeout   time.Duration
	StreamIdleTimeout time.Duration
	Logger            *log.Logger
}

type proxyEnvelope struct {
	RequestID       string               `json:"request_id"`
	Protocol        string               `json:"protocol"`
	RequestSettings modelRequestSettings `json:"request_settings"`
	Endpoint        string               `json:"endpoint"`
	APIKey          string               `json:"api_key"`
	Query           string               `json:"query,omitempty"`
	Headers         map[string][]string  `json:"headers,omitempty"`
	BodyBase64      string               `json:"body_base64"`
}

type server struct {
	authDigest       [sha256.Size]byte
	maxEnvelopeBytes int64
	openAIClient     *http.Client
	anthropic        *anthropicEngine
	logger           *log.Logger
}

func NewHandler(config Config) (http.Handler, error) {
	if strings.TrimSpace(config.AuthToken) == "" {
		return nil, errors.New("gateway auth token is required")
	}
	if config.MaxEnvelopeBytes <= 0 {
		return nil, errors.New("max envelope bytes must be positive")
	}
	if config.UpstreamTimeout <= 0 {
		return nil, errors.New("upstream timeout must be positive")
	}
	if config.StreamIdleTimeout <= 0 {
		return nil, errors.New("stream idle timeout must be positive")
	}
	logger := config.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: config.UpstreamTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		DisableKeepAlives:     true,
		DisableCompression:    true,
		TLSHandshakeTimeout:   config.UpstreamTimeout,
		ResponseHeaderTimeout: config.UpstreamTimeout,
		ExpectContinueTimeout: time.Second,
	}
	openAIClient := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	s := &server{
		authDigest:       sha256.Sum256([]byte(config.AuthToken)),
		maxEnvelopeBytes: config.MaxEnvelopeBytes,
		openAIClient:     openAIClient,
		anthropic:        newAnthropicEngine(config.UpstreamTimeout, config.StreamIdleTimeout),
		logger:           logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc(healthPath, s.handleHealth)
	mux.HandleFunc(readinessPath, s.handleHealth)
	mux.Handle(proxyPath, s)
	return mux, nil
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"status":"ok"}`)
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if !s.authorized(r.Header.Get("Authorization")) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "unauthorized", "")
		return
	}

	var envelope proxyEnvelope
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.maxEnvelopeBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_envelope", "invalid gateway request", "")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_envelope", "invalid gateway request", "")
		return
	}
	body, upstreamURL, err := validateEnvelope(envelope)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_envelope", err.Error(), envelope.APIKey)
		return
	}
	s.logger.Printf("request_id=%s protocol=%s", envelope.RequestID, envelope.Protocol)

	switch envelope.Protocol {
	case protocolOpenAIResponses:
		s.proxyOpenAI(w, r, envelope, body, upstreamURL)
	case protocolOpenAIChatCompletions:
		s.proxyOpenAIChat(w, r, envelope, body, upstreamURL)
	case protocolAnthropicMessages:
		s.proxyAnthropic(w, r, envelope, body, upstreamURL)
	default:
		writeError(w, http.StatusBadRequest, "unsupported_protocol", "unsupported upstream protocol", envelope.APIKey)
	}
}

func (s *server) authorized(value string) bool {
	scheme, token, ok := strings.Cut(value, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.Contains(token, " ") {
		return false
	}
	digest := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(digest[:], s.authDigest[:]) == 1
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("multiple JSON values")
}

func validateEnvelope(envelope proxyEnvelope) ([]byte, *url.URL, error) {
	if !validRequestID(envelope.RequestID) {
		return nil, nil, errors.New("invalid request id")
	}
	if envelope.Protocol != protocolOpenAIResponses &&
		envelope.Protocol != protocolOpenAIChatCompletions &&
		envelope.Protocol != protocolAnthropicMessages {
		return nil, nil, errors.New("unsupported upstream protocol")
	}
	if err := envelope.RequestSettings.validate(envelope.Protocol); err != nil {
		return nil, nil, err
	}
	if envelope.APIKey == "" {
		return nil, nil, errors.New("provider credential is required")
	}
	if strings.HasPrefix(envelope.Query, "?") || strings.Contains(envelope.Query, "#") {
		return nil, nil, errors.New("invalid upstream query")
	}
	endpoint, err := url.Parse(envelope.Endpoint)
	if err != nil || endpoint == nil || !endpoint.IsAbs() || endpoint.Host == "" {
		return nil, nil, errors.New("invalid upstream endpoint")
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, nil, errors.New("unsupported upstream endpoint scheme")
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, nil, errors.New("upstream endpoint must not contain credentials, query, or fragment")
	}
	body, err := base64.StdEncoding.Strict().DecodeString(envelope.BodyBase64)
	if err != nil {
		return nil, nil, errors.New("invalid request body encoding")
	}
	if len(body) == 0 || !json.Valid(body) {
		return nil, nil, errors.New("request body must be JSON")
	}
	return body, endpoint, nil
}

func validRequestID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || strings.ContainsRune("._:-", char) {
			continue
		}
		return false
	}
	return true
}

func upstreamRequestURL(endpoint *url.URL, path, query string) string {
	copy := *endpoint
	copy.Path = strings.TrimRight(copy.Path, "/") + path
	copy.RawPath = ""
	copy.RawQuery = query
	return copy.String()
}

func (s *server) proxyOpenAI(w http.ResponseWriter, r *http.Request, envelope proxyEnvelope, body []byte, endpoint *url.URL) {
	requestURL := upstreamRequestURL(endpoint, "/v1/responses", envelope.Query)
	upstreamRequest, err := http.NewRequestWithContext(r.Context(), http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusBadGateway, "upstream_request_error", "unable to create upstream request", envelope.APIKey)
		return
	}
	copySafeRequestHeaders(upstreamRequest.Header, envelope.Headers)
	upstreamRequest.Header.Set("Authorization", "Bearer "+envelope.APIKey)
	if upstreamRequest.Header.Get("Content-Type") == "" {
		upstreamRequest.Header.Set("Content-Type", "application/json")
	}

	response, err := s.openAIClient.Do(upstreamRequest)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		writeError(w, status, "upstream_transport_error", "upstream request failed", envelope.APIKey)
		return
	}
	defer response.Body.Close()
	copySafeResponseHeaders(w.Header(), response.Header, envelope.APIKey)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(response.StatusCode)
	streamResponse(w, response.Body)
}

func streamResponse(w http.ResponseWriter, body io.Reader) {
	flusher, canFlush := w.(http.Flusher)
	buffer := make([]byte, 32*1024)
	for {
		read, readErr := body.Read(buffer)
		if read > 0 {
			if _, err := w.Write(buffer[:read]); err != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr != nil {
			return
		}
	}
}

func copySafeRequestHeaders(destination http.Header, source map[string][]string) {
	connectionHeaders := namedConnectionHeaders(source)
	for name, values := range source {
		canonical := textproto.CanonicalMIMEHeaderKey(name)
		if unsafeRequestHeader(canonical, connectionHeaders) {
			continue
		}
		for _, value := range values {
			destination.Add(canonical, value)
		}
	}
}

func copySafeResponseHeaders(destination, source http.Header, secret string) {
	connectionHeaders := namedConnectionHeadersFromHTTP(source)
	for name, values := range source {
		canonical := textproto.CanonicalMIMEHeaderKey(name)
		if unsafeResponseHeader(canonical, connectionHeaders) || headerValuesContain(values, secret) {
			continue
		}
		for _, value := range values {
			destination.Add(canonical, value)
		}
	}
}

func headerValuesContain(values []string, secret string) bool {
	if secret == "" {
		return false
	}
	for _, value := range values {
		if strings.Contains(value, secret) {
			return true
		}
	}
	return false
}

func namedConnectionHeaders(headers map[string][]string) map[string]struct{} {
	result := make(map[string]struct{})
	for name, values := range headers {
		if !strings.EqualFold(name, "Connection") {
			continue
		}
		for _, value := range values {
			for _, token := range strings.Split(value, ",") {
				result[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(token))] = struct{}{}
			}
		}
	}
	return result
}

func namedConnectionHeadersFromHTTP(headers http.Header) map[string]struct{} {
	result := make(map[string]struct{})
	for _, value := range headers.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			result[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(token))] = struct{}{}
		}
	}
	return result
}

func unsafeRequestHeader(name string, connectionHeaders map[string]struct{}) bool {
	if _, listed := connectionHeaders[name]; listed {
		return true
	}
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "x-agent-hub-") || strings.HasPrefix(lower, "x-bifrost-") {
		return true
	}
	switch lower {
	case "authorization", "proxy-authorization", "cookie", "host", "content-length",
		"connection", "keep-alive", "proxy-authenticate", "te", "trailer",
		"transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func unsafeResponseHeader(name string, connectionHeaders map[string]struct{}) bool {
	if _, listed := connectionHeaders[name]; listed {
		return true
	}
	switch strings.ToLower(name) {
	case "authorization", "proxy-authorization", "set-cookie", "content-length", "connection",
		"keep-alive", "proxy-authenticate", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func writeError(w http.ResponseWriter, status int, code, message, secret string) {
	message = sanitizeMessage(message, secret)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"type":    "gateway_error",
			"code":    code,
			"message": message,
		},
	})
}

func sanitizeMessage(message, secret string) string {
	if secret != "" {
		message = strings.ReplaceAll(message, secret, "[REDACTED]")
	}
	message = strings.Map(func(char rune) rune {
		if char == '\n' || char == '\r' || char == '\t' {
			return ' '
		}
		if char < 0x20 || char == 0x7f {
			return -1
		}
		return char
	}, message)
	if len(message) > 1024 {
		message = message[:1024]
	}
	if strings.TrimSpace(message) == "" {
		return "model gateway request failed"
	}
	return message
}

func statusFromBifrostError(errStatus *int) int {
	if errStatus == nil || *errStatus < 400 || *errStatus > 599 {
		return http.StatusBadGateway
	}
	return *errStatus
}

func writeSSE(w http.ResponseWriter, event string, payload []byte) error {
	if _, err := fmt.Fprintf(w, "event: %s\ndata: ", event); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "\n\n"); err != nil {
		return err
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}
