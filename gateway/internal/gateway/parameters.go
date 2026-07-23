package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// modelRequestSettings is the small, protocol-specific portion of a Hub
// envelope. It is deliberately decoded strictly so a newly added field cannot
// be silently dropped while converting the request.
type modelRequestSettings struct {
	Protocol            string   `json:"protocol"`
	Temperature         *float64 `json:"temperature,omitempty"`
	TopP                *float64 `json:"top_p,omitempty"`
	MaxCompletionTokens *uint32  `json:"max_completion_tokens,omitempty"`
	MaxTokens           *uint32  `json:"max_tokens,omitempty"`
}

func (s modelRequestSettings) validate(protocol string) error {
	if s.Protocol == "" {
		return fmt.Errorf("request settings are required")
	}
	if s.Protocol != protocol {
		return fmt.Errorf("request settings protocol must match upstream protocol")
	}
	switch protocol {
	case protocolOpenAIResponses:
		if s.Temperature != nil || s.TopP != nil || s.MaxCompletionTokens != nil || s.MaxTokens != nil {
			return fmt.Errorf("Responses request settings cannot contain protocol-specific overrides")
		}
	case protocolOpenAIChatCompletions:
		if s.MaxTokens != nil {
			return fmt.Errorf("Chat Completions request settings do not support max_tokens; use max_completion_tokens")
		}
		if err := validateSettingNumber("temperature", s.Temperature, 2); err != nil {
			return err
		}
		if err := validateSettingNumber("top_p", s.TopP, 1); err != nil {
			return err
		}
		if s.MaxCompletionTokens != nil && *s.MaxCompletionTokens == 0 {
			return fmt.Errorf("max_completion_tokens must be positive")
		}
	case protocolAnthropicMessages:
		if s.MaxCompletionTokens != nil {
			return fmt.Errorf("Anthropic Messages request settings do not support max_completion_tokens; use max_tokens")
		}
		if s.Temperature != nil && s.TopP != nil {
			return fmt.Errorf("Anthropic Messages request settings cannot set both temperature and top_p")
		}
		if err := validateSettingNumber("temperature", s.Temperature, 1); err != nil {
			return err
		}
		if err := validateSettingNumber("top_p", s.TopP, 1); err != nil {
			return err
		}
		if s.MaxTokens != nil && *s.MaxTokens == 0 {
			return fmt.Errorf("max_tokens must be positive")
		}
	default:
		return fmt.Errorf("unsupported upstream protocol")
	}
	return nil
}

func validateSettingNumber(name string, value *float64, maximum float64) error {
	if value == nil {
		return nil
	}
	if *value < 0 || *value > maximum {
		return fmt.Errorf("%s must be between 0 and %g", name, maximum)
	}
	return nil
}

func (s *modelRequestSettings) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("request settings cannot be null")
	}

	var tag struct {
		Protocol string `json:"protocol"`
	}
	if err := json.Unmarshal(data, &tag); err != nil {
		return err
	}
	if tag.Protocol == "" {
		return fmt.Errorf("request settings protocol is required")
	}

	*s = modelRequestSettings{Protocol: tag.Protocol}
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
		s.Temperature = value.Temperature
		s.TopP = value.TopP
		s.MaxCompletionTokens = value.MaxCompletionTokens
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
		s.Temperature = value.Temperature
		s.TopP = value.TopP
		s.MaxTokens = value.MaxTokens
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
