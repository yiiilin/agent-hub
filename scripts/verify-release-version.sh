#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

release_version=$(tr -d '[:space:]' < VERSION)
expected_version=${1:-$release_version}

if [[ ! "$release_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "VERSION must use MAJOR.MINOR.PATCH: $release_version" >&2
  exit 1
fi

if [ "$release_version" != "$expected_version" ]; then
  echo "release tag version $expected_version does not match VERSION $release_version" >&2
  exit 1
fi

check_json_version() {
  local file=$1
  local actual
  actual=$(jq -r '.version' "$file")
  if [ "$actual" != "$release_version" ]; then
    echo "$file version $actual does not match $release_version" >&2
    return 1
  fi
}

check_cargo_version() {
  local file=$1
  local actual
  actual=$(sed -n 's/^version = "\([^"]*\)"/\1/p' "$file" | head -1)
  if [ "$actual" != "$release_version" ]; then
    echo "$file version $actual does not match $release_version" >&2
    return 1
  fi
}

check_cargo_lock_version() {
  local package=$1
  local actual
  actual=$(awk -v package="$package" '
    $0 == "[[package]]" { in_package = 1; name = ""; version = ""; next }
    in_package && $1 == "name" { name = $3; gsub(/"/, "", name); next }
    in_package && $1 == "version" {
      version = $3
      gsub(/"/, "", version)
      if (name == package) {
        print version
        exit
      }
    }
  ' Cargo.lock)
  if [ "$actual" != "$release_version" ]; then
    echo "Cargo.lock package $package version ${actual:-<missing>} does not match $release_version" >&2
    return 1
  fi
}

check_json_lock_package_version() {
  local file=$1
  local package=$2
  local actual
  actual=$(jq -r --arg package "$package" '.packages[$package].version // empty' "$file")
  if [ "$actual" != "$release_version" ]; then
    echo "$file package $package version ${actual:-<missing>} does not match $release_version" >&2
    return 1
  fi
}

check_file_contains() {
  local file=$1
  local pattern=$2
  if ! grep -Fq -- "$pattern" "$file"; then
    echo "$file does not contain required release metadata: $pattern" >&2
    return 1
  fi
}

check_file_occurrences() {
  local file=$1
  local pattern=$2
  local expected=$3
  local actual
  actual=$(grep -Fc -- "$pattern" "$file" || true)
  if [ "$actual" -ne "$expected" ]; then
    echo "$file must contain $expected occurrence(s) of: $pattern (found $actual)" >&2
    return 1
  fi
}

check_json_version frontend/package.json
check_json_lock_package_version frontend/package-lock.json ''
check_json_lock_package_version frontend/package-lock.json '../sdk/typescript'
check_json_version sdk/typescript/package.json
check_json_lock_package_version sdk/typescript/package-lock.json ''
check_cargo_version crates/backend/Cargo.toml
check_cargo_version crates/runtime/Cargo.toml
check_cargo_version crates/shared/Cargo.toml
check_cargo_version crates/agent-hub-cli/Cargo.toml
check_cargo_lock_version agent-hub-backend
check_cargo_lock_version agent-hub-runtime
check_cargo_lock_version agent-hub-shared
check_cargo_lock_version agent-hub-cli

check_file_occurrences compose.yml "agent-hub:\${AGENT_HUB_IMAGE_TAG:-$release_version}" 1
check_file_occurrences compose.yml "agent-hub-runtime:\${AGENT_HUB_IMAGE_TAG:-$release_version}" 2
check_file_contains .env.example "AGENT_HUB_IMAGE_TAG=$release_version"
check_file_contains docs/operations.md "ghcr.io/yiiilin/agent-hub:$release_version"
check_file_contains docs/operations.md "ghcr.io/yiiilin/agent-hub-runtime:$release_version"
check_file_contains docs/operations.md "release_tag=v$release_version"

release_notes_title=$(sed -n '1s/^[[:space:]]*//;1s/[[:space:]]*$//;1p' RELEASE_NOTES.md)
expected_release_notes_title="# 🚀 Agent Hub v$release_version"
if [ "$release_notes_title" != "$expected_release_notes_title" ]; then
  echo "RELEASE_NOTES.md title does not match $expected_release_notes_title" >&2
  exit 1
fi

model_manifest=third_party/pi-model-data/v0.81.1/.manifest.json
if ! jq -e '
  type == "object"
  and (keys | sort) == ["files", "schemaVersion", "structureHash"]
  and .schemaVersion == 1
  and (.structureHash | type == "string" and test("^[0-9a-f]{64}$"))
  and (.files | type == "object" and length > 0)
  and all(
    .files | to_entries[];
    (.key | test("^[a-z0-9][a-z0-9.-]*\\.json$"))
    and (.value | type == "string" and test("^[0-9a-f]{64}$"))
  )
' "$model_manifest" >/dev/null; then
  echo "$model_manifest must contain only the versioned SHA-256 manifest schema" >&2
  exit 1
fi

echo "release version $release_version is consistent"
