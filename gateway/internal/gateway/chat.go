package gateway

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/maximhq/bifrost/core/providers/openai"
	"github.com/maximhq/bifrost/core/schemas"
)

func (s *server) proxyOpenAIChat(w http.ResponseWriter, r *http.Request, envelope proxyEnvelope, body []byte, endpoint *url.URL) {
	if err := validateResponsesRepresentability(body, protocolOpenAIChatCompletions); err != nil {
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
	safeHeaders := safeOpenAICompatibleHeaders(envelope.Headers)
	bifrostCtx.SetValue(schemas.BifrostContextKeyExtraHeaders, safeHeaders)

	responsesRequest := wireRequest.ToBifrostResponsesRequest(bifrostCtx)
	if responsesRequest == nil {
		writeError(w, http.StatusBadRequest, "invalid_responses_request", "invalid Responses request", envelope.APIKey)
		return
	}
	responsesRequest.Provider = schemas.OpenAI
	responsesRequest.Fallbacks = nil
	chatRequest := responsesRequest.ToChatRequest()
	chatRequest.Provider = schemas.OpenAI
	prependChatInstructions(chatRequest, wireRequest.Instructions)
	mergeChatRequestParameters(chatRequest, envelope.RequestParameters)

	requestURL := upstreamRequestURL(endpoint, "/v1/chat/completions", envelope.Query)
	if wireRequest.Stream != nil && *wireRequest.Stream {
		s.proxyOpenAIChatStream(w, r, envelope, bifrostCtx, requestURL, chatRequest, safeHeaders)
		return
	}
	s.proxyOpenAIChatJSON(w, r, envelope, bifrostCtx, requestURL, chatRequest, safeHeaders)
}

func prependChatInstructions(request *schemas.BifrostChatRequest, instructions *string) {
	if request == nil || instructions == nil || *instructions == "" {
		return
	}
	message := schemas.ChatMessage{
		Role: schemas.ChatMessageRoleSystem,
		Content: &schemas.ChatMessageContent{
			ContentStr: instructions,
		},
	}
	request.Input = append([]schemas.ChatMessage{message}, request.Input...)
}

func mergeChatRequestParameters(request *schemas.BifrostChatRequest, parameters modelRequestParameters) {
	if request.Params == nil {
		request.Params = &schemas.ChatParameters{}
	}
	if parameters.Temperature != nil {
		request.Params.Temperature = parameters.Temperature
	}
	if parameters.TopP != nil {
		request.Params.TopP = parameters.TopP
	}
	if parameters.MaxCompletionTokens != nil {
		value := int(*parameters.MaxCompletionTokens)
		request.Params.MaxCompletionTokens = &value
	}
}

func (s *server) proxyOpenAIChatJSON(
	w http.ResponseWriter,
	r *http.Request,
	envelope proxyEnvelope,
	bifrostCtx *schemas.BifrostContext,
	requestURL string,
	request *schemas.BifrostChatRequest,
	headers map[string][]string,
) {
	response, bifrostErr := openai.HandleOpenAIChatCompletionRequest(
		bifrostCtx,
		s.anthropic.client(bifrostCtx),
		requestURL,
		request,
		map[string]string{"Authorization": "Bearer " + envelope.APIKey},
		flattenHeaders(headers),
		false,
		false,
		schemas.OpenAI,
		nil,
		nil,
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
	converted := response.ToBifrostResponsesResponse().WithDefaults()
	payload, err := marshalOpenAIWire(converted)
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

func (s *server) proxyOpenAIChatStream(
	w http.ResponseWriter,
	r *http.Request,
	envelope proxyEnvelope,
	bifrostCtx *schemas.BifrostContext,
	requestURL string,
	request *schemas.BifrostChatRequest,
	headers map[string][]string,
) {
	bifrostCtx.SetValue(schemas.BifrostContextKeyIsResponsesToChatCompletionFallback, true)
	stream, bifrostErr := openai.HandleOpenAIChatCompletionStreaming(
		bifrostCtx,
		s.anthropic.client(bifrostCtx),
		requestURL,
		request,
		map[string]string{"Authorization": "Bearer " + envelope.APIKey},
		flattenHeaders(headers),
		s.anthropic.streamIdleTimeoutSeconds,
		false,
		false,
		schemas.OpenAI,
		identityPostHook,
		nil,
		nil,
		nil,
		nil,
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
			_ = writeSSE(w, "error", openAIStreamError(chunk.BifrostError, envelope.APIKey))
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

func safeOpenAICompatibleHeaders(source map[string][]string) map[string][]string {
	destination := make(http.Header)
	copySafeRequestHeaders(destination, source)
	destination.Del("Authorization")
	return map[string][]string(destination)
}
