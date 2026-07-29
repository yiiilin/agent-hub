#!/bin/sh
set -eu

if [ "${1:-}" = "--version" ]; then
  printf '%s\n' "0.81.1"
  exit 0
fi

session_dir=""
session_path=""
has_rpc_mode="false"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --mode)
      [ "${2:-}" = "rpc" ] || { echo "fake Pi only supports --mode rpc" >&2; exit 64; }
      has_rpc_mode="true"
      shift 2
      ;;
    --session-dir)
      session_dir="${2:-}"
      [ -n "$session_dir" ] || { echo "--session-dir requires a value" >&2; exit 64; }
      shift 2
      ;;
    --session)
      session_path="${2:-}"
      [ -n "$session_path" ] || { echo "--session requires a value" >&2; exit 64; }
      shift 2
      ;;
    --no-extensions|--no-themes|--no-prompt-templates|--no-context-files|--approve|--no-approve)
      shift
      ;;
    --tools|--exclude-tools|--thinking|--name|--extension|-e)
      [ -n "${2:-}" ] || { echo "$1 requires a value" >&2; exit 64; }
      shift 2
      ;;
    *)
      echo "unsupported fake Pi argument: $1" >&2
      exit 64
      ;;
  esac
done

[ "$has_rpc_mode" = "true" ] || { echo "fake Pi requires --mode rpc" >&2; exit 64; }
[ -n "$session_dir" ] || { echo "fake Pi requires --session-dir" >&2; exit 64; }

mkdir -p "$session_dir"
if [ -z "$session_path" ]; then
  session_path="$session_dir/fake-pi-session.jsonl"
fi
case "$session_path" in
  "$session_dir"/*) ;;
  *) echo "fake Pi session must be inside --session-dir" >&2; exit 64 ;;
esac
session_id="fake-pi-$(basename "$session_path" .jsonl)"
if [ ! -e "$session_path" ]; then
  jq -cn --arg id "$session_id" '{type:"session",id:$id}' >"$session_path"
fi

protocol_error() {
  echo "invalid fake Pi RPC request" >&2
  exit 65
}

request_id() {
  printf '%s\n' "$1" | jq -ce '.id // null' 2>/dev/null || protocol_error
}

response() {
  line="$1"
  command="$2"
  data="$3"
  id="$(request_id "$line")"
  jq -cn --argjson id "$id" --arg command "$command" --argjson data "$data" \
    '{type:"response",id:$id,command:$command,success:true,data:$data}'
}

error_response() {
  line="$1"
  command="$2"
  error="$3"
  id="$(request_id "$line")"
  jq -cn --argjson id "$id" --arg command "$command" --arg error "$error" \
    '{type:"response",id:$id,command:$command,success:false,error:$error}'
}

load_models() {
  models_path="${PI_CODING_AGENT_DIR:-${HOME:?HOME is required}/.pi/agent}/models.json"
  loaded_models="$(jq -ce '.providers | select(type == "object")' "$models_path")" || {
    echo "invalid fake Pi models config" >&2
    return 1
  }
}

append_session_event() {
  jq -cn --arg type "$1" --arg now "$(date +%s)000" '{type:$type,timestamp:($now|tonumber)}' >>"$session_path"
}

model_content() {
  prompt="$1"
  if [ "${FAKE_PI_DISABLE_MODEL:-}" = "1" ]; then
    printf '%s\n' "Fake Pi response for: $prompt"
    return 0
  fi
  provider="$model_provider"
  base_url="$(printf '%s\n' "$loaded_models" | jq -er --arg provider "$provider" '.[$provider].baseUrl | select(type == "string" and length > 0)')" || return 1
  binding_id="$(printf '%s\n' "$loaded_models" | jq -er --arg provider "$provider" '.[$provider].headers["x-agent-hub-model-binding-id"] | select(type == "string" and length > 0)')" || return 1
  request_body="$(jq -cn --arg model "$model_id" --arg prompt "$prompt" '{model:$model,input:$prompt,max_output_tokens:256}')"
  response_body="$(curl -fsS \
    -H 'Content-Type: application/json' \
    -H "x-agent-hub-model-binding-id: $binding_id" \
    --data-binary "$request_body" \
    "${base_url%/}/responses")"
  printf '%s\n' "$response_body" | jq -er '
    if (.output_text? | type) == "string" and (.output_text | length) > 0 then .output_text
    else [(.output? // [])[]? | (.content? // [])[]? | select(.type == "output_text") | .text] | join("") end
    | select(type == "string" and length > 0)
  '
}

emit_completion() {
  content="$1"
  jq -cn '{type:"tool_execution_start",toolCallId:"fake-pi-bash-1",toolName:"bash",args:{command:"printf fake-pi"}}'
  if [ "${FAKE_PI_DUPLICATE_EVENTS:-}" = "1" ]; then
    jq -cn '{type:"tool_execution_start",toolCallId:"fake-pi-bash-1",toolName:"bash",args:{command:"printf fake-pi"}}'
  fi
  jq -cn '{type:"tool_execution_update",toolCallId:"fake-pi-bash-1",toolName:"bash",args:{command:"printf fake-pi"},partialResult:{content:[{type:"text",text:"fake-pi"}]}}'
  jq -cn '{type:"tool_execution_end",toolCallId:"fake-pi-bash-1",toolName:"bash",result:{content:[{type:"text",text:"fake-pi"}]},isError:false}'
  if [ "${FAKE_PI_DUPLICATE_EVENTS:-}" = "1" ]; then
    jq -cn '{type:"tool_execution_end",toolCallId:"fake-pi-bash-1",toolName:"bash",result:{content:[{type:"text",text:"fake-pi"}]},isError:false}'
  fi
  jq -cn --arg content "$content" '{type:"message_update",message:{role:"assistant"},assistantMessageEvent:{type:"text_delta",contentIndex:0,delta:$content,partial:{}}}'
  jq -cn --arg content "$content" '{type:"turn_end",message:{role:"assistant",content:[{type:"text",text:$content}],usage:{input:42,output:24,totalTokens:66}},toolResults:[]}'
  jq -cn --arg content "$content" '{type:"agent_end",messages:[{role:"assistant",content:[{type:"text",text:$content}]}],willRetry:false}'
  jq -cn '{type:"agent_settled"}'
  append_session_event "assistant"
  active_turn="false"
  held_turn="false"
}

emit_integration_tool_request() {
  arguments='{"message":"fixture:integration","attachments":[{"kind":"text","name":"qa-note.txt","content_type":"text/plain","size_bytes":32,"text":"quoted text, arrays [1, 2], and a second line\nkept exactly","url":null}]}'
  integration_catalog="${PI_CODING_AGENT_DIR:-${HOME:?HOME is required}/.pi/agent}/agent-hub-integration-tools.json"
  tool_name="$(jq -er '.[0].name | select(type == "string" and length > 0)' "$integration_catalog")"
  jq -cn --arg tool_name "$tool_name" --argjson arguments "$arguments" '{type:"tool_execution_start",toolCallId:"platform|tool-call|fc_integration_echo",toolName:$tool_name,args:$arguments}'
  jq -cn --arg tool_name "$tool_name" --argjson arguments "$arguments" '{type:"tool_execution_end",toolCallId:"platform|tool-call|fc_integration_echo",toolName:$tool_name,args:$arguments,result:{content:[{type:"text",text:"Integration tool request delegated to Agent Hub."}],details:{pending:true},terminate:true},isError:false}'
  jq -cn --arg tool_name "$tool_name" --argjson arguments "$arguments" '{type:"turn_end",message:{role:"assistant",content:[{type:"toolCall",id:"platform|tool-call|fc_integration_echo",name:$tool_name,arguments:$arguments}],stopReason:"toolUse",usage:{input:42,output:12,totalTokens:54}},toolResults:[]}'
  jq -cn '{type:"agent_end",messages:[],willRetry:false}'
  jq -cn '{type:"agent_settled"}'
  append_session_event "assistant"
  active_turn="false"
  held_turn="false"
}

emit_failure() {
  jq -cn '{type:"message_update",message:{role:"assistant"},assistantMessageEvent:{type:"error",reason:"error",error:{role:"assistant",content:[],stopReason:"error"}}}'
  jq -cn '{type:"turn_end",message:{role:"assistant",content:[],usage:{input:7,output:0,totalTokens:7},stopReason:"error"},toolResults:[]}'
  jq -cn '{type:"agent_end",messages:[],willRetry:false}'
  jq -cn '{type:"agent_settled"}'
  append_session_event "failed"
  active_turn="false"
  held_turn="false"
}

emit_retry_and_compaction() {
  jq -cn '{type:"compaction_start",reason:"threshold"}'
  jq -cn '{type:"summarization_retry_scheduled",attempt:1,maxAttempts:2,delayMs:10,errorMessage:"fixture-sensitive-error"}'
  jq -cn '{type:"summarization_retry_attempt_start",source:"compaction",reason:"threshold"}'
  jq -cn '{type:"summarization_retry_finished"}'
  jq -cn '{type:"compaction_end",reason:"threshold",result:{summary:"fixture-sensitive-summary"},aborted:false,willRetry:false}'
  jq -cn '{type:"auto_retry_start",attempt:1,maxAttempts:2,delayMs:10,errorMessage:"fixture-sensitive-error"}'
  jq -cn '{type:"auto_retry_end",success:true,attempt:1}'
}

active_turn="false"
held_turn="false"
model_provider=""
model_id=""
thinking_level="medium"
models_loaded="false"
if [ -f "${PI_CODING_AGENT_DIR:-${HOME:?HOME is required}/.pi/agent}/models.json" ]; then
  load_models
  models_loaded="true"
fi

while IFS= read -r line; do
  if [ -n "${FAKE_PI_REQUEST_LOG:-}" ]; then
    printf '%s\n' "$line" >>"$FAKE_PI_REQUEST_LOG"
  fi
  command="$(printf '%s\n' "$line" | jq -er '.type | select(type == "string")' 2>/dev/null)" || protocol_error
  case "$command" in
    get_state)
      response "$line" "get_state" "$(jq -cn --arg file "$session_path" --arg id "$session_id" --arg provider "$model_provider" --arg model "$model_id" --arg thinking "$thinking_level" '{sessionFile:$file,sessionId:$id,model:(if $provider == "" then null else {provider:$provider,id:$model} end),thinkingLevel:$thinking}')"
      ;;
    reload_resources)
      response "$line" "reload_resources" "null"
      ;;
    reload_models)
      load_models
      models_loaded="true"
      response "$line" "reload_models" "null"
      ;;
    set_model)
      requested_provider="$(printf '%s\n' "$line" | jq -er '.provider | select(type == "string" and length > 0)' 2>/dev/null)" || protocol_error
      requested_model="$(printf '%s\n' "$line" | jq -er '.modelId | select(type == "string" and length > 0)' 2>/dev/null)" || protocol_error
      if [ "$models_loaded" = "true" ] && ! printf '%s\n' "$loaded_models" | jq -e --arg provider "$requested_provider" --arg model "$requested_model" '.[$provider].models | any(.id == $model)' >/dev/null; then
        error_response "$line" "set_model" "Model not found: $requested_provider/$requested_model"
        continue
      fi
      model_provider="$requested_provider"
      model_id="$requested_model"
      response "$line" "set_model" "$(jq -cn --arg provider "$model_provider" --arg id "$model_id" '{provider:$provider,id:$id}')"
      ;;
    set_thinking_level)
      thinking_level="$(printf '%s\n' "$line" | jq -er '.level | select(. == "off" or . == "minimal" or . == "low" or . == "medium" or . == "high" or . == "xhigh" or . == "max")' 2>/dev/null)" || protocol_error
      response "$line" "set_thinking_level" "null"
      ;;
    prompt)
      prompt="$(printf '%s\n' "$line" | jq -er '.message | select(type == "string" and length > 0)' 2>/dev/null)" || protocol_error
      [ "$active_turn" = "false" ] || protocol_error
      response "$line" "prompt" "null"
      if [ "${FAKE_PI_MALFORMED_AFTER_PROMPT:-}" = "1" ]; then
        printf '%s\n' 'not-json'
        continue
      fi
      active_turn="true"
      jq -cn '{type:"agent_start"}'
      jq -cn '{type:"turn_start"}'
      if [ "${FAKE_PI_DUPLICATE_EVENTS:-}" = "1" ]; then
        jq -cn '{type:"turn_start"}'
      fi
      jq -cn '{type:"message_update",message:{role:"assistant"},assistantMessageEvent:{type:"thinking_delta",contentIndex:0,delta:"Preparing the response.",partial:{}}}'
      append_session_event "user"
      if [ "$prompt" = "fixture:hold" ]; then
        held_turn="true"
      elif [ "$prompt" = "fixture:fail" ]; then
        emit_failure
      elif [ "${prompt#fixture:integration}" != "$prompt" ]; then
        emit_integration_tool_request
      else
        if [ "$prompt" = "fixture:retry" ]; then
          emit_retry_and_compaction
        fi
        emit_completion "$(model_content "$prompt")"
      fi
      ;;
    steer)
      steer="$(printf '%s\n' "$line" | jq -er '.message | select(type == "string" and length > 0)' 2>/dev/null)" || protocol_error
      [ "$active_turn" = "true" ] || protocol_error
      response "$line" "steer" "null"
      jq -cn --arg steer "$steer" '{type:"queue_update",steering:[$steer],followUp:[]}'
      if [ "$held_turn" = "true" ] && [ "$steer" = "fixture:release" ]; then
        emit_completion "Fake Pi released held turn."
      fi
      ;;
    abort)
      [ "$active_turn" = "true" ] || protocol_error
      response "$line" "abort" "null"
      jq -cn '{type:"message_update",message:{role:"assistant"},assistantMessageEvent:{type:"error",reason:"aborted",partial:{}}}'
      jq -cn '{type:"turn_end",message:{role:"assistant",content:[],stopReason:"aborted"},toolResults:[]}'
      jq -cn '{type:"agent_end",messages:[],willRetry:false}'
      jq -cn '{type:"agent_settled"}'
      append_session_event "aborted"
      active_turn="false"
      held_turn="false"
      ;;
    *) protocol_error ;;
  esac
done
