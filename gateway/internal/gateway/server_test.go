package gateway

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testGatewayToken = "gateway-test-token"

func newTestHandler(t *testing.T, logOutput io.Writer) http.Handler {
	t.Helper()
	handler, err := NewHandler(Config{
		AuthToken:         testGatewayToken,
		MaxEnvelopeBytes:  1 << 20,
		UpstreamTimeout:   5 * time.Second,
		StreamIdleTimeout: 2 * time.Second,
		Logger:            log.New(logOutput, "", 0),
	})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func proxyRequest(t *testing.T, ctx context.Context, serverURL string, envelope proxyEnvelope, token string) *http.Request {
	t.Helper()
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+proxyPath, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func rawProxyRequest(t *testing.T, serverURL string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, serverURL+proxyPath, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create raw request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testGatewayToken)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func encodedBody(body string) string {
	return base64.StdEncoding.EncodeToString([]byte(body))
}

func openAIEnvelope(endpoint, body string) proxyEnvelope {
	return proxyEnvelope{
		RequestID: "req-openai",
		Protocol:  protocolOpenAIResponses,
		Endpoint:  endpoint,
		APIKey:    "provider-secret",
		Query:     "include=usage%2Edetails&trace=one",
		Headers: map[string][]string{
			"Accept":                       {"text/event-stream"},
			"Content-Type":                 {"application/json"},
			"X-Trace-Id":                   {"trace-123"},
			"Authorization":                {"Bearer runtime-secret"},
			"Cookie":                       {"session=secret"},
			"Connection":                   {"keep-alive"},
			"X-Agent-Hub-Gateway-Internal": {"must-not-pass"},
		},
		BodyBase64: encodedBody(body),
	}
}

func anthropicEnvelope(endpoint string, stream bool) proxyEnvelope {
	body := fmt.Sprintf(`{"model":"claude-test","input":[{"role":"user","content":[{"type":"input_text","text":"private prompt"}]}],"max_output_tokens":64,"stream":%t}`, stream)
	return proxyEnvelope{
		RequestID: "req-anthropic",
		Protocol:  protocolAnthropicMessages,
		Endpoint:  endpoint,
		APIKey:    "shared-anthropic-secret",
		Headers: map[string][]string{
			"Accept":       {"application/json"},
			"Content-Type": {"application/json"},
			"X-Trace-Id":   {"anthropic-trace"},
		},
		BodyBase64: encodedBody(body),
	}
}

func TestHealthReadinessAndProxyAuthentication(t *testing.T) {
	handler := newTestHandler(t, io.Discard)
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, path := range []string{healthPath, readinessPath} {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, resp.StatusCode)
		}
	}

	req := proxyRequest(t, context.Background(), server.URL, openAIEnvelope("https://provider.example", `{}`), "")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unauthenticated proxy request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", resp.StatusCode)
	}
}

func TestOpenAIResponsesIsByteTransparentAndRewritesAuthentication(t *testing.T) {
	originalBody := " \n{\n  \"model\": \"gpt-test\", \"input\": \"hello\"\n}\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read upstream body: %v", err)
		}
		if r.URL.Path != "/business/v1/responses" {
			t.Errorf("upstream path = %q", r.URL.Path)
		}
		if r.URL.RawQuery != "include=usage%2Edetails&trace=one" {
			t.Errorf("upstream query = %q", r.URL.RawQuery)
		}
		if string(body) != originalBody {
			t.Errorf("upstream body changed: %q", body)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer provider-secret" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("X-Trace-Id"); got != "trace-123" {
			t.Errorf("X-Trace-Id = %q", got)
		}
		for _, forbidden := range []string{"Cookie", "X-Agent-Hub-Gateway-Internal"} {
			if got := r.Header.Get(forbidden); got != "" {
				t.Errorf("unsafe header %s reached upstream: %q", forbidden, got)
			}
		}
		if got := r.Header.Get("Connection"); got != "close" {
			t.Errorf("Connection = %q, want gateway-owned close", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", "upstream-request")
		w.Header().Set("X-Provider-Echo", "received provider-secret")
		w.Header().Set("Set-Cookie", "provider-cookie=secret")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, ` {"id":"resp_1","status":"completed"} `)
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	gatewayServer := httptest.NewServer(newTestHandler(t, &logs))
	defer gatewayServer.Close()
	req := proxyRequest(t, context.Background(), gatewayServer.URL, openAIEnvelope(upstream.URL+"/business", originalBody), testGatewayToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	responseBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read proxy response: %v", err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if string(responseBody) != ` {"id":"resp_1","status":"completed"} ` {
		t.Fatalf("response body changed: %q", responseBody)
	}
	if resp.Header.Get("X-Request-Id") != "upstream-request" {
		t.Fatalf("missing upstream request id")
	}
	if resp.Header.Get("Set-Cookie") != "" {
		t.Fatalf("provider cookie reached Hub")
	}
	if resp.Header.Get("X-Provider-Echo") != "" {
		t.Fatalf("provider credential echo reached Hub")
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q", resp.Header.Get("Cache-Control"))
	}
	if strings.Contains(logs.String(), "provider-secret") || strings.Contains(logs.String(), "hello") {
		t.Fatalf("gateway logs contain request secret or prompt: %s", logs.String())
	}
}

func TestOpenAIResponsesPreservesCompressedResponseBytes(t *testing.T) {
	var compressed bytes.Buffer
	zipper := gzip.NewWriter(&compressed)
	if _, err := zipper.Write([]byte(`{"id":"resp_gzip","status":"completed"}`)); err != nil {
		t.Fatalf("compress fixture: %v", err)
	}
	if err := zipper.Close(); err != nil {
		t.Fatalf("close fixture compressor: %v", err)
	}
	expected := append([]byte(nil), compressed.Bytes()...)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "" {
			t.Errorf("gateway added Accept-Encoding = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(expected)
	}))
	defer upstream.Close()
	gatewayServer := httptest.NewServer(newTestHandler(t, io.Discard))
	defer gatewayServer.Close()

	req := proxyRequest(t, context.Background(), gatewayServer.URL, openAIEnvelope(upstream.URL, `{"model":"gpt-test"}`), testGatewayToken)
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read compressed response: %v", err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q", resp.Header.Get("Content-Encoding"))
	}
	if !bytes.Equal(body, expected) {
		t.Fatalf("compressed response body changed")
	}
}

func TestOpenAIResponsesFlushesTheFirstChunkAndDoesNotRetry(t *testing.T) {
	firstWritten := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\"}\n\n")
		w.(http.Flusher).Flush()
		close(firstWritten)
		<-release
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
	}))
	defer upstream.Close()
	gatewayServer := httptest.NewServer(newTestHandler(t, io.Discard))
	defer gatewayServer.Close()

	req := proxyRequest(t, context.Background(), gatewayServer.URL, openAIEnvelope(upstream.URL, `{"model":"gpt-test","stream":true}`), testGatewayToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer resp.Body.Close()
	<-firstWritten
	first, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatalf("read first streamed line: %v", err)
	}
	if first != "event: response.created\n" {
		t.Fatalf("first streamed line = %q", first)
	}
	close(release)
	_, _ = io.Copy(io.Discard, resp.Body)
	if calls.Load() != 1 {
		t.Fatalf("upstream calls = %d", calls.Load())
	}
}

func anthropicProvider(t *testing.T, label, apiKey string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tenant/v1/messages" {
			t.Errorf("%s path = %q", label, r.URL.Path)
		}
		if got := r.Header.Get("X-Api-Key"); got != apiKey {
			t.Errorf("%s x-api-key = %q", label, got)
		}
		if got := r.Header.Get("Anthropic-Version"); got != "2023-06-01" {
			t.Errorf("%s anthropic-version = %q", label, got)
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("%s decode request: %v", label, err)
		}
		if request["model"] != "claude-test" {
			t.Errorf("%s model = %#v", label, request["model"])
		}
		if stream, present := request["stream"]; present && stream != false {
			t.Errorf("%s stream = %#v", label, request["stream"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"msg_%s","type":"message","role":"assistant","model":"claude-test","content":[{"type":"text","text":"from-%s"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":2}}`, label, label)
	}))
}

func TestAnthropicMessagesConvertsResponsesAndKeepsEndpointsRequestScoped(t *testing.T) {
	providerA := anthropicProvider(t, "a", "anthropic-secret-a")
	defer providerA.Close()
	providerB := anthropicProvider(t, "b", "anthropic-secret-b")
	defer providerB.Close()
	gatewayServer := httptest.NewServer(newTestHandler(t, io.Discard))
	defer gatewayServer.Close()

	type result struct {
		want string
		got  string
		err  error
	}
	results := make(chan result, 24)
	var wg sync.WaitGroup
	for index := range 24 {
		label, endpoint := "a", providerA.URL+"/tenant"
		if index%2 == 1 {
			label, endpoint = "b", providerB.URL+"/tenant"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			envelope := anthropicEnvelope(endpoint, false)
			envelope.APIKey = "anthropic-secret-" + label
			req := proxyRequest(t, context.Background(), gatewayServer.URL, envelope, testGatewayToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				results <- result{want: label, err: err}
				return
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				results <- result{want: label, err: readErr}
				return
			}
			if resp.StatusCode != http.StatusOK {
				results <- result{want: label, err: fmt.Errorf("status %d: %s", resp.StatusCode, body)}
				return
			}
			results <- result{want: label, got: string(body)}
		}()
	}
	wg.Wait()
	close(results)
	for item := range results {
		if item.err != nil {
			t.Errorf("request for %s: %v", item.want, item.err)
			continue
		}
		var response map[string]any
		if err := json.Unmarshal([]byte(item.got), &response); err != nil {
			t.Errorf("decode response for %s: %v", item.want, err)
			continue
		}
		if response["status"] != "completed" || response["model"] != "claude-test" {
			t.Errorf("unexpected response for %s: %s", item.want, item.got)
		}
		if !strings.Contains(item.got, "from-"+item.want) {
			t.Errorf("endpoint crossed for %s: %s", item.want, item.got)
		}
		usage, _ := response["usage"].(map[string]any)
		if usage["input_tokens"] != float64(3) || usage["output_tokens"] != float64(2) {
			t.Errorf("usage for %s = %#v", item.want, usage)
		}
		if _, leaked := response["extra_fields"]; leaked {
			t.Errorf("Bifrost internal fields leaked: %s", item.got)
		}
	}
}

func TestAnthropicStreamPropagatesDownstreamCancellation(t *testing.T) {
	upstreamCancelled := make(chan struct{})
	forceUpstreamStop := make(chan struct{})
	defer close(forceUpstreamStop)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n")
		w.(http.Flusher).Flush()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				close(upstreamCancelled)
				return
			case <-forceUpstreamStop:
				return
			case <-ticker.C:
				if _, err := io.WriteString(w, "event: ping\ndata: {\"type\":\"ping\"}\n\n"); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer upstream.Close()
	gatewayServer := httptest.NewServer(newTestHandler(t, io.Discard))
	defer gatewayServer.Close()

	req := proxyRequest(t, context.Background(), gatewayServer.URL, anthropicEnvelope(upstream.URL, true), testGatewayToken)
	conn, err := net.Dial("tcp", strings.TrimPrefix(gatewayServer.URL, "http://"))
	if err != nil {
		t.Fatalf("connect to gateway: %v", err)
	}
	if err := req.Write(conn); err != nil {
		conn.Close()
		t.Fatalf("write stream request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		conn.Close()
		t.Fatalf("read stream response: %v", err)
	}
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if !strings.HasPrefix(line, "event: response.") {
		t.Fatalf("first event line = %q", line)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close downstream connection: %v", err)
	}
	select {
	case <-upstreamCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream stream was not cancelled")
	}
}

func TestAnthropicStreamNormalizesTerminalUsage(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"model":"claude-test","id":"msg_usage","type":"message","role":"assistant","content":[],"stop_reason":null,"usage":{"input_tokens":7,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}`,
		`{"type":"message_stop"}`,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode Anthropic request: %v", err)
		}
		if request["stream"] != true {
			t.Errorf("stream = %#v", request["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range events {
			var decoded map[string]any
			if err := json.Unmarshal([]byte(event), &decoded); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", decoded["type"], event)
		}
	}))
	defer upstream.Close()
	gatewayServer := httptest.NewServer(newTestHandler(t, io.Discard))
	defer gatewayServer.Close()

	req := proxyRequest(t, context.Background(), gatewayServer.URL, anthropicEnvelope(upstream.URL, true), testGatewayToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read converted stream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q", got)
	}

	var completed map[string]any
	for _, line := range strings.Split(string(body), "\n") {
		payload, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			t.Fatalf("decode gateway event %q: %v", payload, err)
		}
		if event["type"] == "response.completed" {
			completed = event
		}
	}
	if completed == nil {
		t.Fatalf("missing response.completed event: %s", body)
	}
	response, _ := completed["response"].(map[string]any)
	usage, _ := response["usage"].(map[string]any)
	if usage["input_tokens"] != float64(7) || usage["output_tokens"] != float64(4) || usage["total_tokens"] != float64(11) {
		t.Fatalf("terminal usage = %#v", usage)
	}
}

func TestAnthropicStreamPreservesFunctionToolsAndReasoning(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"model":"claude-test","id":"msg_tool","type":"message","role":"assistant","content":[],"stop_reason":null,"usage":{"input_tokens":11,"output_tokens":0}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"check the weather"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-tool"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"lookup_weather","input":{}}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"city\":\"Paris\"}"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":6}}`,
		`{"type":"message_stop"}`,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode Anthropic request: %v", err)
		}
		tools, _ := request["tools"].([]any)
		if len(tools) != 1 {
			t.Errorf("tools = %#v", request["tools"])
		} else {
			tool, _ := tools[0].(map[string]any)
			if tool["name"] != "lookup_weather" {
				t.Errorf("tool = %#v", tool)
			}
			inputSchema, _ := tool["input_schema"].(map[string]any)
			if inputSchema["type"] != "object" {
				t.Errorf("tool input schema = %#v", inputSchema)
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range events {
			var decoded map[string]any
			if err := json.Unmarshal([]byte(event), &decoded); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", decoded["type"], event)
		}
	}))
	defer upstream.Close()
	gatewayServer := httptest.NewServer(newTestHandler(t, io.Discard))
	defer gatewayServer.Close()

	envelope := anthropicEnvelope(upstream.URL, true)
	envelope.BodyBase64 = encodedBody(`{
		"model":"claude-test",
		"input":[{"role":"user","content":[{"type":"input_text","text":"weather in Paris"}]}],
		"tools":[{"type":"function","name":"lookup_weather","description":"Look up weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"],"additionalProperties":false}}],
		"max_output_tokens":64,
		"stream":true
	}`)
	req := proxyRequest(t, context.Background(), gatewayServer.URL, envelope, testGatewayToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read converted stream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}

	stream := string(body)
	for _, expected := range []string{
		`"type":"response.reasoning_summary_text.delta"`,
		`"delta":"check the weather"`,
		`"type":"response.function_call_arguments.delta"`,
		`"delta":"{\"city\":\"Paris\"}"`,
		`"type":"function_call"`,
		`"name":"lookup_weather"`,
	} {
		if !strings.Contains(stream, expected) {
			t.Errorf("converted stream missing %s: %s", expected, body)
		}
	}

	var completed map[string]any
	for _, line := range strings.Split(stream, "\n") {
		payload, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			t.Fatalf("decode gateway event %q: %v", payload, err)
		}
		if event["type"] == "response.completed" {
			completed = event
		}
	}
	if completed == nil {
		t.Fatalf("missing response.completed event: %s", body)
	}
	response, _ := completed["response"].(map[string]any)
	usage, _ := response["usage"].(map[string]any)
	if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(6) || usage["total_tokens"] != float64(17) {
		t.Fatalf("terminal usage = %#v", usage)
	}
}

func TestAnthropicErrorPreservesStatusDoesNotRetryOrLogSecrets(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"rate_limit_error","message":"key shared-anthropic-secret rejected for private prompt"}}`)
	}))
	defer upstream.Close()
	var logs bytes.Buffer
	gatewayServer := httptest.NewServer(newTestHandler(t, &logs))
	defer gatewayServer.Close()

	req := proxyRequest(t, context.Background(), gatewayServer.URL, anthropicEnvelope(upstream.URL, false), testGatewayToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read error response: %v", err)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream calls = %d", calls.Load())
	}
	if !strings.Contains(string(body), "rate_limit_error") {
		t.Fatalf("error type missing: %s", body)
	}
	if strings.Contains(string(body), "shared-anthropic-secret") {
		t.Fatalf("provider credential leaked in error: %s", body)
	}
	if strings.Contains(logs.String(), "shared-anthropic-secret") || strings.Contains(logs.String(), "private prompt") {
		t.Fatalf("gateway logs contain provider secret or prompt: %s", logs.String())
	}
}

func TestGatewayRejectsInvalidEnvelopesBeforeCallingUpstream(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer upstream.Close()
	valid := openAIEnvelope(upstream.URL, `{"model":"gpt-test"}`)
	validJSON, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("marshal valid envelope: %v", err)
	}

	unknownProtocol := valid
	unknownProtocol.Protocol = "unknown"
	unknownProtocolJSON, _ := json.Marshal(unknownProtocol)
	invalidEndpoint := valid
	invalidEndpoint.Endpoint = "ftp://provider.example"
	invalidEndpointJSON, _ := json.Marshal(invalidEndpoint)
	invalidBody := valid
	invalidBody.BodyBase64 = "%%%"
	invalidBodyJSON, _ := json.Marshal(invalidBody)
	tests := []struct {
		name string
		body []byte
	}{
		{name: "malformed JSON", body: []byte(`{"request_id":`)},
		{name: "unknown field", body: append(validJSON[:len(validJSON)-1], []byte(`,"unexpected":true}`)...)},
		{name: "unknown protocol", body: unknownProtocolJSON},
		{name: "unsupported endpoint", body: invalidEndpointJSON},
		{name: "invalid body encoding", body: invalidBodyJSON},
	}

	gatewayServer := httptest.NewServer(newTestHandler(t, io.Discard))
	defer gatewayServer.Close()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resp, err := http.DefaultClient.Do(rawProxyRequest(t, gatewayServer.URL, test.body))
			if err != nil {
				t.Fatalf("proxy request: %v", err)
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				t.Fatalf("read response: %v", readErr)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d: %s", resp.StatusCode, body)
			}
			if resp.Header.Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q", resp.Header.Get("Cache-Control"))
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid envelopes reached upstream %d times", calls.Load())
	}
}
