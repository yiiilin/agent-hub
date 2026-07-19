#!/bin/sh
set -eu

if [ "${1:-}" != "app-server" ] || [ "${2:-}" != "--listen" ] || [ "${3:-}" != "stdio://" ]; then
  echo "usage: fake-codex-app-server.sh app-server --listen stdio://" >&2
  exit 64
fi

protocol_error() {
  echo "invalid fake app-server JSON-RPC request" >&2
  exit 65
}

ensure_transcript() {
  case "$thread_id" in
    */*) return 0 ;;
  esac
  transcript_dir="${CODEX_HOME}/sessions"
  transcript_path="${transcript_dir}/rollout-${thread_id}.jsonl"
  mkdir -p "$transcript_dir"
  if [ ! -e "$transcript_path" ]; then
    jq -cn --arg thread "$thread_id" '{
      type: "fake_app_server_fixture",
      thread_id: $thread
    }' >"$transcript_path"
  fi
}

model_content() {
  prompt="${1:-fake codex smoke}"
  config_path="${CODEX_HOME}/config.toml"
  model_provider="$(sed -n 's/^model_provider = "\(.*\)"$/\1/p' "$config_path" | head -n 1)"
  if [ -z "$model_provider" ]; then
    echo "missing default model provider" >&2
    return 1
  fi
  provider_value() {
    awk -v provider="model_providers.${model_provider}" -v key="$1" '
      /^\[/ {
        section = substr($0, 2, length($0) - 2)
        in_provider = section == provider || index(section, provider ".") == 1
        next
      }
      in_provider && index($0, key " = \"") == 1 {
        value = $0
        sub("^[^=]*= \"", "", value)
        sub("\"$", "", value)
        print value
        exit
      }
    ' "$config_path"
  }
  base_url="$(provider_value base_url)"
  if [ -z "$base_url" ]; then
    echo "missing model proxy base_url" >&2
    return 1
  fi
  connection_id="$(provider_value x-agent-hub-model-connection-id)"
  if [ -z "$connection_id" ]; then
    echo "missing model proxy connection id" >&2
    return 1
  fi

  request_body="$(jq -cn --arg prompt "$prompt" '{
    model: "hub-proxy-smoke",
    input: [{role: "user", content: [{type: "input_text", text: $prompt}]}]
  }')"
  response="$(curl -fsS \
    -H 'Content-Type: application/json' \
    -H "x-agent-hub-model-connection-id: ${connection_id}" \
    --data-binary "$request_body" \
    "${base_url}/responses")"
  content="$(printf '%s\n' "$response" | jq -er '
    .output_text | select(type == "string" and length > 0)
  ' 2>/dev/null)" || {
    echo "missing output_text from model proxy" >&2
    return 1
  }
  printf '%s\n' "$content"
}

emit_agent_completion() {
  content="$1"
  completed_turn_id="$active_turn_id"
  jq -cn --arg content "$content" '{
    jsonrpc: "2.0",
    method: "item/agentMessage/delta",
    params: {delta: $content}
  }'
  jq -cn '{
    jsonrpc: "2.0",
    method: "thread/tokenUsage/updated",
    params: {last: {input_tokens: 42, output_tokens: 24}, driver: "app-server"}
  }'
  jq -cn --arg content "$content" --arg thread "$thread_id" --arg turn "$completed_turn_id" '{
    jsonrpc: "2.0",
    method: "turn/completed",
    params: {
      threadId: $thread,
      turn: {id: $turn, status: "completed", items: [{type: "agentMessage", text: $content}]}
    }
  }'
  active_turn_id=""
  held_console_turn="false"
}

thread_id="fake-app-server-thread"
active_turn_id=""
held_console_turn="false"
turn_sequence=0
while IFS= read -r line; do
  method="$(printf '%s\n' "$line" | jq -er '
    if .jsonrpc == "2.0" and (.method | type) == "string" then .method else error("invalid request") end
  ' 2>/dev/null)" || protocol_error

  case "$method" in
    initialize)
      printf '%s\n' "$line" | jq -e '
        ((.params.clientInfo.name | type) == "string") and
        ((.params.clientInfo.name | length) > 0) and
        ((.params.clientInfo.version | type) == "string")
      ' >/dev/null 2>&1 || protocol_error
      request_id="$(printf '%s\n' "$line" | jq -ce '.id' 2>/dev/null)" || protocol_error
      jq -cn --argjson id "$request_id" '{
        jsonrpc: "2.0",
        id: $id,
        result: {serverInfo: {name: "fake-codex", version: "0.1.0"}}
      }'
      ;;
    initialized)
      ;;
    thread/unsubscribe)
      printf '%s\n' "$line" | jq -e --arg thread "$thread_id" '
        .params.threadId == $thread
      ' >/dev/null 2>&1 || protocol_error
      request_id="$(printf '%s\n' "$line" | jq -ce '.id' 2>/dev/null)" || protocol_error
      jq -cn --argjson id "$request_id" '{
        jsonrpc: "2.0",
        id: $id,
        result: {}
      }'
      ;;
    thread/start|thread/resume)
      printf '%s\n' "$line" | jq -e '
        ((.params.cwd | type) == "string") and
        ((.params.cwd | length) > 0) and
        (.params.approvalPolicy == "never") and
        ((.params.developerInstructions | type) == "string")
      ' >/dev/null 2>&1 || protocol_error
      request_id="$(printf '%s\n' "$line" | jq -ce '.id' 2>/dev/null)" || protocol_error
      if [ "$method" = "thread/resume" ]; then
        thread_id="$(printf '%s\n' "$line" | jq -er '
          .params.threadId | select(type == "string" and length > 0)
        ' 2>/dev/null)" || protocol_error
      else
        cwd="$(printf '%s\n' "$line" | jq -er '
          .params.cwd | select(type == "string" and length > 0)
        ' 2>/dev/null)" || protocol_error
        workspace_dir="${cwd%/}"
        session_dir="${workspace_dir%/*}"
        session_id="${session_dir##*/}"
        case "$session_id" in
          ""|*[!A-Za-z0-9._-]*) protocol_error ;;
        esac
        thread_id="fake-app-server-thread-${session_id}"
      fi
      ensure_transcript
      jq -cn --argjson id "$request_id" --arg thread "$thread_id" '{
        jsonrpc: "2.0",
        id: $id,
        result: {thread: {id: $thread, sessionId: "fake-app-server-session"}}
      }'
      ;;
    turn/start)
      printf '%s\n' "$line" | jq -e --arg thread "$thread_id" '
        (.params.threadId == $thread) and
        ((.params.source | type) == "string") and
        ((.params.input | type) == "array") and
        ((.params.input | length) > 0) and
        all(.params.input[]; .type == "text" and ((.text | type) == "string"))
      ' >/dev/null 2>&1 || protocol_error
      request_id="$(printf '%s\n' "$line" | jq -ce '.id' 2>/dev/null)" || protocol_error
      source="$(printf '%s\n' "$line" | jq -er '.params.source' 2>/dev/null)" || protocol_error
      turn_prompt="$(printf '%s\n' "$line" | jq -er '[.params.input[] | select(.type == "text") | .text] | join("\n")' 2>/dev/null)" || protocol_error
      hub_run_id="$(printf '%s\n' "$line" | jq -er '
        .params.metadata.agent_hub_run_id // ""
        | select(type == "string")
      ' 2>/dev/null)" || protocol_error
      turn_sequence=$((turn_sequence + 1))
      if [ -n "$hub_run_id" ]; then
        active_turn_id="fake-app-server-turn-${hub_run_id}"
      else
        active_turn_id="fake-app-server-turn-${turn_sequence}"
      fi
      held_console_turn="false"

      jq -cn --argjson id "$request_id" --arg turn "$active_turn_id" '{
        jsonrpc: "2.0",
        id: $id,
        result: {turn: {id: $turn, status: "inProgress", items: []}}
      }'
      jq -cn --arg thread "$thread_id" --arg turn "$active_turn_id" '{
        jsonrpc: "2.0",
        method: "turn/started",
        params: {threadId: $thread, turn: {id: $turn, status: "inProgress", items: []}}
      }'

      if [ "$source" = "console" ]; then
        hold_turn="$(printf '%s\n' "$line" | jq -r '
          any(.params.input[]; .type == "text" and .text == "fixture:hold")
        ' 2>/dev/null)" || protocol_error
        if [ "$hold_turn" = "true" ]; then
          held_console_turn="true"
          continue
        fi
      fi

      if [ "$source" = "fixture:protocol" ]; then
        continue
      elif [ "$source" = "integration:message" ]; then
        wants_tool="$(printf '%s\n' "$line" | jq -r '
          [.params.input[] | select(.type == "text") | .text]
          | join("\n")
          | ascii_downcase
          | contains("tool")
        ' 2>/dev/null)" || protocol_error
        case "$wants_tool" in
          true|false) ;;
          *) protocol_error ;;
        esac
      else
        wants_tool="false"
      fi

      if [ "$wants_tool" = "true" ]; then
        tool_item="$(printf '%s\n' "$line" | jq -ce --arg id "platform-tool-call" '
          (.params.metadata.integration_context.attachments // []) as $attachments
          | (.params.dynamicTools[0] // error("missing dynamic tool")) as $tool
          | ($tool.name // $tool.function.name // error("missing tool name")) as $tool_name
          | ([.params.input[] | select(.type == "text") | .text] | join("\n")) as $message
          | if ($attachments | type) != "array" or ($tool_name | type) != "string" or ($tool_name | length) == 0
            then error("invalid integration context")
            else {
              type: "toolRequest",
              id: $id,
              toolName: $tool_name,
              arguments: {message: $message, attachments: $attachments}
            }
            end
        ' 2>/dev/null)" || protocol_error
        jq -cn --argjson item "$tool_item" '{
          jsonrpc: "2.0",
          method: "item/completed",
          params: {item: $item}
        }'
        jq -cn --argjson item "$tool_item" --arg thread "$thread_id" --arg turn "$active_turn_id" '{
          jsonrpc: "2.0",
          method: "turn/completed",
          params: {
            threadId: $thread,
            turn: {id: $turn, status: "completed", items: [$item]}
          }
        }'
        active_turn_id=""
      elif [ "$source" = "integration:tool_result" ]; then
        tool_result="$(printf '%s\n' "$line" | jq -c '
          .params.metadata.integration_context.tool_result
          | if . == null then error("invalid tool result") else . end
        ' 2>/dev/null)" || protocol_error
        content="$(model_content "$turn_prompt") completed integration tool result: $tool_result"
        emit_agent_completion "$content"
      else
        content="$(model_content "$turn_prompt")"
        emit_agent_completion "$content"
      fi
      ;;
    turn/steer)
      printf '%s\n' "$line" | jq -e --arg thread "$thread_id" --arg turn "$active_turn_id" '
        ($turn | length) > 0 and
        (.params.threadId == $thread) and
        (.params.expectedTurnId == $turn) and
        ((.params.input | type) == "array") and
        ((.params.input | length) > 0) and
        all(.params.input[]; .type == "text" and ((.text | type) == "string"))
      ' >/dev/null 2>&1 || protocol_error
      request_id="$(printf '%s\n' "$line" | jq -ce '.id' 2>/dev/null)" || protocol_error
      jq -cn --argjson id "$request_id" --arg turn "$active_turn_id" '{
        jsonrpc: "2.0",
        id: $id,
        result: {turnId: $turn}
      }'
      release_turn="$(printf '%s\n' "$line" | jq -r '
        any(.params.input[]; .type == "text" and .text == "fixture:release")
      ' 2>/dev/null)" || protocol_error
      if [ "$held_console_turn" = "true" ] && [ "$release_turn" = "true" ]; then
        emit_agent_completion "Fake Codex released held console turn."
      fi
      ;;
    turn/interrupt)
      printf '%s\n' "$line" | jq -e --arg thread "$thread_id" --arg turn "$active_turn_id" '
        ($turn | length) > 0 and
        (.params.threadId == $thread) and
        (.params.turnId == $turn)
      ' >/dev/null 2>&1 || protocol_error
      request_id="$(printf '%s\n' "$line" | jq -ce '.id' 2>/dev/null)" || protocol_error
      interrupted_turn_id="$active_turn_id"
      jq -cn --argjson id "$request_id" '{
        jsonrpc: "2.0",
        id: $id,
        result: {}
      }'
      jq -cn --arg thread "$thread_id" --arg turn "$interrupted_turn_id" '{
        jsonrpc: "2.0",
        method: "turn/completed",
        params: {threadId: $thread, turn: {id: $turn, status: "interrupted", items: []}}
      }'
      active_turn_id=""
      held_console_turn="false"
      ;;
    *)
      protocol_error
      ;;
  esac
done
