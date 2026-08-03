#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

AGENT_HUB_HUB_URL="${AGENT_HUB_HUB_URL:?Set AGENT_HUB_HUB_URL}"
AGENT_ID="${AGENT_ID:?Set AGENT_ID to the private maintenance Agent}"
SKILL_DIR="$repo_root/skills/agent-hub-maintenance"

"$repo_root/scripts/build-agent-hub-skill.sh"

cli() {
  "$repo_root/target/release/agent-hub-cli" "$@"
}

skill_id=$(cli skills list | jq -r --arg name "agent-hub-maintenance" '
  .[]? | select(.name == $name) | .id
' | head -1)

if [[ -z "$skill_id" ]]; then
  skill_json=$(cli skills create --name "agent-hub-maintenance" --description "Diagnose and maintain an Agent Hub deployment through its management API." --content-file "$SKILL_DIR/SKILL.md")
  skill_id=$(jq -r '.id' <<<"$skill_json")
fi

cli skills package upload "$skill_id" --dir "$SKILL_DIR" >/dev/null
cli agents update "$AGENT_ID" --add-skill "$skill_id" >/dev/null

echo "agent-hub-maintenance Skill installed (id=$skill_id) and bound to Agent $AGENT_ID"
