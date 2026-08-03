#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

cargo build --release -p agent-hub-cli
mkdir -p skills/agent-hub-maintenance/bin
cp target/release/agent-hub-cli skills/agent-hub-maintenance/bin/agent-hub
chmod 0755 skills/agent-hub-maintenance/bin/agent-hub

echo "Skill package prepared at $repo_root/skills/agent-hub-maintenance"

