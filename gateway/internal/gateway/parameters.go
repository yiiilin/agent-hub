package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// modelRequestParameters is the small, protocol-specific portion of a Hub
// envelope. It is deliberately decoded strictly so a newly added field cannot
// be silently dropped while converting the request.
type modelRequestParameters struct {
	Protocol            string   `json:"protocol"`
	Temperature         *float64 `json:"temperature,omitempty"`
	TopP                *float64 `json:"top_p,omitempty"`
	MaxCompletionTokens *uint32  `json:"max_completion_tokens,omitempty"`
	MaxTokens           *uint32  `json:"max_tokens,omitempty"`
}

func (p modelRequestParameters) validate(protocol string) error {
	if p.Protocol == "" {
		return fmt.Errorf("request parameters are required")
	}
	if p.Protocol != protocol {
		return fmt.Errorf("request parameter protocol must match upstream protocol")
	}
	switch protocol {
	case protocolOpenAIResponses:
		if p.Temperature != nil || p.TopP != nil || p.MaxCompletionTokens != nil || p.MaxTokens != nil {
			return fmt.Errorf("Responses request parameters cannot contain protocol-specific overrides")
		}
	case protocolOpenAIChatCompletions:
		if p.MaxTokens != nil {
			return fmt.Errorf("Chat Completions request parameters do not support max_tokens; use max_completion_tokens")
		}
		if err := validateParameterNumber("temperature", p.Temperature, 2); err != nil {
			return err
		}
		if err := validateParameterNumber("top_p", p.TopP, 1); err != nil {
			return err
		}
		if p.MaxCompletionTokens != nil && *p.MaxCompletionTokens == 0 {
			return fmt.Errorf("max_completion_tokens must be positive")
		}
	case protocolAnthropicMessages:
		if p.MaxCompletionTokens != nil {
			return fmt.Errorf("Anthropic Messages request parameters do not support max_completion_tokens; use max_tokens")
		}
		if p.Temperature != nil && p.TopP != nil {
			return fmt.Errorf("Anthropic Messages request parameters cannot set both temperature and top_p")
		}
		if err := validateParameterNumber("temperature", p.Temperature, 1); err != nil {
			return err
		}
		if err := validateParameterNumber("top_p", p.TopP, 1); err != nil {
			return err
		}
		if p.MaxTokens != nil && *p.MaxTokens == 0 {
			return fmt.Errorf("max_tokens must be positive")
		}
	default:
		return fmt.Errorf("unsupported upstream protocol")
	}
	return nil
}

func validateParameterNumber(name string, value *float64, maximum float64) error {
	if value == nil {
		return nil
	}
	if *value < 0 || *value > maximum {
		return fmt.Errorf("%s must be between 0 and %g", name, maximum)
	}
	return nil
}

func (p *modelRequestParameters) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("request parameters cannot be null")
	}

	var tag struct {
		Protocol string `json:"protocol"`
	}
	if err := json.Unmarshal(data, &tag); err != nil {
		return err
	}
	if tag.Protocol == "" {
		return fmt.Errorf("request parameter protocol is required")
	}

	*p = modelRequestParameters{Protocol: tag.Protocol}
	switch tag.Protocol {
	case protocolOpenAIResponses:
		var value struct {
			Protocol string `json:"protocol"`
		}
		if err := decodeStrictJSON(data, &value); err != nil {
			return err
		}
	case protocolOpenAIChatCompletions:
		var value struct {
			Protocol            string   `json:"protocol"`
			Temperature         *float64 `json:"temperature"`
			TopP                *float64 `json:"top_p"`
			MaxCompletionTokens *uint32  `json:"max_completion_tokens"`
		}
		if err := decodeStrictJSON(data, &value); err != nil {
			return err
		}
		p.Temperature = value.Temperature
		p.TopP = value.TopP
		p.MaxCompletionTokens = value.MaxCompletionTokens
	case protocolAnthropicMessages:
		var value struct {
			Protocol    string   `json:"protocol"`
			Temperature *float64 `json:"temperature"`
			TopP        *float64 `json:"top_p"`
			MaxTokens   *uint32  `json:"max_tokens"`
		}
		if err := decodeStrictJSON(data, &value); err != nil {
			return err
		}
		p.Temperature = value.Temperature
		p.TopP = value.TopP
		p.MaxTokens = value.MaxTokens
	default:
		return fmt.Errorf("unsupported upstream protocol")
	}
	return nil
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}
