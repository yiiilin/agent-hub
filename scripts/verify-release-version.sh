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

check_json_version frontend/package.json
check_json_version sdk/typescript/package.json
check_cargo_version crates/backend/Cargo.toml
check_cargo_version crates/runtime/Cargo.toml
check_cargo_version crates/shared/Cargo.toml

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
