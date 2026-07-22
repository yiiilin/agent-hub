package gateway

import (
	"encoding/json"
	"fmt"
)

var chatResponsesFields = map[string]struct{}{
	"input": {}, "instructions": {}, "max_output_tokens": {}, "metadata": {},
	"model": {}, "parallel_tool_calls": {}, "prompt_cache_key": {},
	"prompt_cache_options": {}, "prompt_cache_retention": {}, "reasoning": {},
	"safety_identifier": {}, "service_tier": {}, "store": {}, "stream": {},
	"stream_options": {}, "temperature": {}, "text": {}, "top_logprobs": {},
	"top_p": {}, "tool_choice": {}, "tools": {},
}

var anthropicResponsesFields = map[string]struct{}{
	"input": {}, "instructions": {}, "max_output_tokens": {}, "model": {},
	"reasoning": {}, "service_tier": {}, "stream": {}, "temperature": {},
	"text": {}, "top_p": {}, "tool_choice": {}, "tools": {}, "user": {},
}

func validateResponsesRepresentability(body []byte, protocol string) error {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(body, &document); err != nil {
		return fmt.Errorf("request body must be a JSON object")
	}
	if document == nil {
		return unsupportedProtocolFeature("request body must be a JSON object")
	}

	allowed := chatResponsesFields
	if protocol == protocolAnthropicMessages {
		allowed = anthropicResponsesFields
	}
	for field := range document {
		if _, ok := allowed[field]; !ok {
			return unsupportedProtocolFeature("Responses field " + field + " cannot be represented for " + protocol)
		}
	}

	if model, ok := document["model"]; !ok || string(model) == "null" {
		return unsupportedProtocolFeature("model is required")
	}
	if input, ok := document["input"]; ok {
		if err := validateResponsesInput(input, protocol); err != nil {
			return err
		}
		if protocol == protocolAnthropicMessages && hasNonNullJSONValue(document["instructions"]) && responsesInputHasSystemRole(input) {
			return unsupportedProtocolFeature("Anthropic Messages cannot preserve instructions together with system or developer input messages")
		}
	} else {
		return unsupportedProtocolFeature("input is required")
	}
	if reasoning, ok := document["reasoning"]; ok && string(reasoning) != "null" {
		if err := validateReasoning(reasoning, protocol); err != nil {
			return err
		}
	}
	if tools, ok := document["tools"]; ok && string(tools) != "null" {
		if err := validateResponsesTools(tools, protocol); err != nil {
			return err
		}
	}
	if toolChoice, ok := document["tool_choice"]; ok && string(toolChoice) != "null" {
		if err := validateResponsesToolChoice(toolChoice, protocol); err != nil {
			return err
		}
	}
	if protocol == protocolAnthropicMessages {
		if hasJSONNumber(document["temperature"]) && hasJSONNumber(document["top_p"]) {
			return unsupportedProtocolFeature("Anthropic Messages cannot receive both temperature and top_p")
		}
	}
	return nil
}

func validateReasoning(raw json.RawMessage, protocol string) error {
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return unsupportedProtocolFeature("reasoning must be an object")
	}
	allowed := map[string]struct{}{"effort": {}, "max_tokens": {}}
	if protocol == protocolAnthropicMessages {
		allowed["summary"] = struct{}{}
	}
	for field := range value {
		if _, ok := allowed[field]; !ok {
			return unsupportedProtocolFeature("reasoning field " + field + " cannot be represented for " + protocol)
		}
	}
	return nil
}

func validateResponsesInput(raw json.RawMessage, protocol string) error {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return unsupportedProtocolFeature("input cannot be represented for " + protocol)
	}
	for _, item := range items {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(item, &value); err != nil || value == nil {
			return unsupportedProtocolFeature("input item must be an object")
		}
		typeName := "message"
		if rawType, ok := value["type"]; ok {
			if err := json.Unmarshal(rawType, &typeName); err != nil {
				return unsupportedProtocolFeature("input item type must be a string")
			}
		}
		if err := validateResponsesInputItem(value, typeName, protocol); err != nil {
			return err
		}
	}
	return nil
}

func validateResponsesInputItem(value map[string]json.RawMessage, typeName, protocol string) error {
	switch typeName {
	case "message":
		if err := validateAllowedFields(value, portableMessageFields, "input message", protocol); err != nil {
			return err
		}
		if err := validateMessageRole(value, protocol); err != nil {
			return err
		}
		content, ok := value["content"]
		if !ok {
			return unsupportedProtocolFeature("input message content is required")
		}
		return validateMessageContent(content, protocol)
	case "function_call":
		if err := validateAllowedFields(value, portableFunctionCallFields, "function_call item", protocol); err != nil {
			return err
		}
		for _, field := range []string{"call_id", "name", "arguments"} {
			if err := validateRequiredStringField(value, field, "function_call item"); err != nil {
				return err
			}
		}
		return nil
	case "function_call_output":
		if err := validateAllowedFields(value, portableFunctionCallOutputFields, "function_call_output item", protocol); err != nil {
			return err
		}
		if err := validateRequiredStringField(value, "call_id", "function_call_output item"); err != nil {
			return err
		}
		return validateRequiredStringField(value, "output", "function_call_output item")
	case "reasoning":
		return validateReasoningInputItem(value, protocol)
	case "refusal":
		if err := validateAllowedFields(value, portableRefusalFields, "refusal item", protocol); err != nil {
			return err
		}
		content, ok := value["content"]
		if !ok {
			return unsupportedProtocolFeature("refusal item content is required")
		}
		return validateRefusalContent(content, protocol)
	default:
		return unsupportedProtocolFeature("input item type " + typeName + " cannot be represented for " + protocol)
	}
}

var portableMessageFields = fieldSet("type", "role", "content")
var portableFunctionCallFields = fieldSet("type", "call_id", "name", "arguments")
var portableFunctionCallOutputFields = fieldSet("type", "call_id", "output")
var portableReasoningFields = fieldSet("type", "content", "summary", "encrypted_content")
var portableRefusalFields = fieldSet("type", "content")

func fieldSet(names ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

func validateAllowedFields(value map[string]json.RawMessage, allowed map[string]struct{}, subject, protocol string) error {
	for field := range value {
		if _, ok := allowed[field]; !ok {
			return unsupportedProtocolFeature(subject + " field " + field + " cannot be represented for " + protocol)
		}
	}
	return nil
}

func validateMessageRole(value map[string]json.RawMessage, protocol string) error {
	role, ok := value["role"]
	if !ok {
		return unsupportedProtocolFeature("input message role is required")
	}
	var name string
	if err := json.Unmarshal(role, &name); err != nil {
		return unsupportedProtocolFeature("input message role must be a string")
	}
	switch name {
	case "assistant", "user", "system", "developer":
		return nil
	default:
		return unsupportedProtocolFeature("input message role " + name + " cannot be represented for " + protocol)
	}
}

func validateRequiredStringField(value map[string]json.RawMessage, field, subject string) error {
	raw, ok := value[field]
	if !ok || string(raw) == "null" {
		return unsupportedProtocolFeature(subject + " field " + field + " is required")
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return unsupportedProtocolFeature(subject + " field " + field + " must be a string")
	}
	return nil
}

func validateOptionalStringField(value map[string]json.RawMessage, field, subject string) error {
	raw, ok := value[field]
	if !ok || string(raw) == "null" {
		return nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return unsupportedProtocolFeature(subject + " field " + field + " must be a string")
	}
	return nil
}

func validateMessageContent(raw json.RawMessage, protocol string) error {
	if string(raw) == "null" {
		return unsupportedProtocolFeature("message content cannot be represented for " + protocol)
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return nil
	}
	var blocks []json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return unsupportedProtocolFeature("message content cannot be represented for " + protocol)
	}
	for _, block := range blocks {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(block, &value); err != nil || value == nil {
			return unsupportedProtocolFeature("message content block must be an object")
		}
		var typeName string
		if err := json.Unmarshal(value["type"], &typeName); err != nil {
			return unsupportedProtocolFeature("message content block type is required")
		}
		switch typeName {
		case "input_text", "output_text":
			if err := validateAllowedFields(value, fieldSet("type", "text"), "message content block", protocol); err != nil {
				return err
			}
			if err := validateRequiredStringField(value, "text", "message content block"); err != nil {
				return err
			}
		case "input_image":
			allowed := fieldSet("type", "image_url")
			if protocol == protocolOpenAIChatCompletions {
				allowed["detail"] = struct{}{}
			}
			if err := validateAllowedFields(value, allowed, "input_image block", protocol); err != nil {
				return err
			}
			if err := validateRequiredStringField(value, "image_url", "input_image block"); err != nil {
				return err
			}
			if err := validateOptionalStringField(value, "detail", "input_image block"); err != nil {
				return err
			}
		case "input_file":
			if err := validateAllowedFields(value, fieldSet("type", "file_id", "file_data", "filename"), "input_file block", protocol); err != nil {
				return err
			}
			if !hasNonNullJSONValue(value["file_id"]) {
				if !hasNonNullJSONValue(value["file_data"]) {
					return unsupportedProtocolFeature("input_file block requires file_id or file_data")
				}
			}
			for _, field := range []string{"file_id", "file_data", "filename"} {
				if err := validateOptionalStringField(value, field, "input_file block"); err != nil {
					return err
				}
			}
		case "input_audio":
			if protocol != protocolOpenAIChatCompletions {
				return unsupportedProtocolFeature("message content block type input_audio cannot be represented for " + protocol)
			}
			if err := validateAllowedFields(value, fieldSet("type", "input_audio"), "input_audio block", protocol); err != nil {
				return err
			}
			if err := validateInputAudio(value["input_audio"], protocol); err != nil {
				return err
			}
		default:
			return unsupportedProtocolFeature("message content block type " + typeName + " cannot be represented for " + protocol)
		}
	}
	return nil
}

func validateInputAudio(raw json.RawMessage, protocol string) error {
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return unsupportedProtocolFeature("input_audio block input_audio must be an object")
	}
	if err := validateAllowedFields(value, fieldSet("format", "data"), "input_audio payload", protocol); err != nil {
		return err
	}
	for _, field := range []string{"format", "data"} {
		if err := validateRequiredStringField(value, field, "input_audio payload"); err != nil {
			return err
		}
	}
	return nil
}

func validateReasoningInputItem(value map[string]json.RawMessage, protocol string) error {
	if err := validateAllowedFields(value, portableReasoningFields, "reasoning item", protocol); err != nil {
		return err
	}
	hasPortableValue := false
	if content, ok := value["content"]; ok && string(content) != "null" {
		hasPortableValue = true
		if err := validateReasoningContent(content, protocol); err != nil {
			return err
		}
	}
	if summary, ok := value["summary"]; ok && string(summary) != "null" {
		hasPortableValue = true
		if err := validateReasoningSummary(summary, protocol); err != nil {
			return err
		}
	}
	if encrypted, ok := value["encrypted_content"]; ok && string(encrypted) != "null" {
		hasPortableValue = true
		if err := validateOptionalStringField(value, "encrypted_content", "reasoning item"); err != nil {
			return err
		}
	}
	if !hasPortableValue {
		return unsupportedProtocolFeature("reasoning item has no representable content")
	}
	return nil
}

func validateReasoningContent(raw json.RawMessage, protocol string) error {
	var blocks []json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return unsupportedProtocolFeature("reasoning item content cannot be represented for " + protocol)
	}
	for _, block := range blocks {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(block, &value); err != nil || value == nil {
			return unsupportedProtocolFeature("reasoning content block must be an object")
		}
		if err := validateAllowedFields(value, fieldSet("type", "text", "signature"), "reasoning content block", protocol); err != nil {
			return err
		}
		var typeName string
		if err := json.Unmarshal(value["type"], &typeName); err != nil || typeName != "reasoning_text" {
			return unsupportedProtocolFeature("reasoning content block type cannot be represented for " + protocol)
		}
		if err := validateRequiredStringField(value, "text", "reasoning content block"); err != nil {
			return err
		}
		if err := validateOptionalStringField(value, "signature", "reasoning content block"); err != nil {
			return err
		}
	}
	return nil
}

func validateReasoningSummary(raw json.RawMessage, protocol string) error {
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return unsupportedProtocolFeature("reasoning item summary cannot be represented for " + protocol)
	}
	for _, entry := range entries {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(entry, &value); err != nil || value == nil {
			return unsupportedProtocolFeature("reasoning summary item must be an object")
		}
		if err := validateAllowedFields(value, fieldSet("type", "text"), "reasoning summary item", protocol); err != nil {
			return err
		}
		var typeName string
		if err := json.Unmarshal(value["type"], &typeName); err != nil || typeName != "summary_text" {
			return unsupportedProtocolFeature("reasoning summary item type cannot be represented for " + protocol)
		}
		if err := validateRequiredStringField(value, "text", "reasoning summary item"); err != nil {
			return err
		}
	}
	return nil
}

func validateRefusalContent(raw json.RawMessage, protocol string) error {
	if string(raw) == "null" {
		return unsupportedProtocolFeature("refusal item content cannot be represented for " + protocol)
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return nil
	}
	var blocks []json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return unsupportedProtocolFeature("refusal item content cannot be represented for " + protocol)
	}
	for _, block := range blocks {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(block, &value); err != nil || value == nil {
			return unsupportedProtocolFeature("refusal content block must be an object")
		}
		if err := validateAllowedFields(value, fieldSet("type", "refusal"), "refusal content block", protocol); err != nil {
			return err
		}
		var typeName string
		if err := json.Unmarshal(value["type"], &typeName); err != nil || typeName != "refusal" {
			return unsupportedProtocolFeature("refusal content block type cannot be represented for " + protocol)
		}
		if err := validateRequiredStringField(value, "refusal", "refusal content block"); err != nil {
			return err
		}
	}
	return nil
}

func validateResponsesTools(raw json.RawMessage, protocol string) error {
	var tools []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &tools); err != nil {
		return unsupportedProtocolFeature("tools must be an array")
	}
	for _, tool := range tools {
		var typeName string
		if err := json.Unmarshal(tool["type"], &typeName); err != nil {
			return unsupportedProtocolFeature("tool type is required")
		}
		if protocol == protocolOpenAIChatCompletions {
			if typeName != "function" {
				return unsupportedProtocolFeature("tool type " + typeName + " cannot be represented for Chat Completions")
			}
		} else if !anthropicToolTypes[typeName] {
			return unsupportedProtocolFeature("tool type " + typeName + " cannot be represented for Anthropic Messages")
		}
		if typeName == "function" {
			var name string
			if err := json.Unmarshal(tool["name"], &name); err != nil || name == "" {
				return unsupportedProtocolFeature("function tool name is required")
			}
		}
	}
	return nil
}

var anthropicToolTypes = map[string]bool{
	"function": true, "mcp": true, "local_shell": true, "web_search": true,
	"web_search_preview": true, "web_fetch": true, "code_interpreter": true,
	"computer_use_preview": true, "memory": true, "tool_search": true, "advisor": true,
}

func validateResponsesToolChoice(raw json.RawMessage, protocol string) error {
	var choice string
	if json.Unmarshal(raw, &choice) == nil {
		if choice == "auto" || choice == "none" || choice == "required" {
			return nil
		}
		return unsupportedProtocolFeature("tool_choice value cannot be represented")
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return unsupportedProtocolFeature("tool_choice must be a string or object")
	}
	var typeName string
	if err := json.Unmarshal(value["type"], &typeName); err != nil {
		return unsupportedProtocolFeature("tool_choice type is required")
	}
	if typeName != "function" {
		return unsupportedProtocolFeature("tool_choice type " + typeName + " cannot be represented for " + protocol)
	}
	if protocol == protocolOpenAIChatCompletions {
		var name string
		if err := json.Unmarshal(value["name"], &name); err != nil || name == "" {
			return unsupportedProtocolFeature("function tool_choice name is required")
		}
	}
	return nil
}

func unsupportedProtocolFeature(message string) error {
	return protocolFeatureError{message: message}
}

type protocolFeatureError struct {
	message string
}

func (e protocolFeatureError) Error() string { return e.message }

func hasJSONNumber(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	var number json.Number
	return json.Unmarshal(raw, &number) == nil
}

func hasNonNullJSONValue(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

func responsesInputHasSystemRole(raw json.RawMessage) bool {
	var items []map[string]json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return false
	}
	for _, item := range items {
		var role string
		if json.Unmarshal(item["role"], &role) == nil && (role == "system" || role == "developer") {
			return true
		}
	}
	return false
}
