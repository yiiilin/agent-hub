#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
config_path="${GITLEAKS_CONFIG:-${repo_root}/.gitleaks.toml}"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <image>" >&2
  exit 64
fi

for command in docker gitleaks jq tar; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 69
  fi
done

image=$1
docker image inspect "$image" >/dev/null
[[ -f "$config_path" ]] || {
  echo "gitleaks config is missing: $config_path" >&2
  exit 66
}

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/agent-hub-image-scan.XXXXXX")
cleanup() {
  if [[ -d "$work_dir" && "$work_dir" == "${TMPDIR:-/tmp}/agent-hub-image-scan."* ]]; then
    chmod -R u+rwX -- "$work_dir"
    find "$work_dir" -depth -delete
  fi
}
trap cleanup EXIT

archive_path="${work_dir}/image.tar"
saved_dir="${work_dir}/saved"
layer_dir="${work_dir}/layers"
mkdir -p "$saved_dir" "$layer_dir"

docker image save --output "$archive_path" "$image"
tar --extract --file "$archive_path" --directory "$saved_dir"

manifest_path="${saved_dir}/manifest.json"
[[ -f "$manifest_path" ]] || {
  echo "docker image archive has no manifest.json" >&2
  exit 65
}

mapfile -t config_files < <(jq -r '.[].Config' "$manifest_path" | LC_ALL=C sort -u)
mapfile -t layer_files < <(jq -r '.[].Layers[]' "$manifest_path" | LC_ALL=C sort -u)
[[ ${#config_files[@]} -gt 0 && ${#layer_files[@]} -gt 0 ]] || {
  echo "docker image archive has no config or layers" >&2
  exit 65
}

for config_file in "${config_files[@]}"; do
  [[ "$config_file" != /* && "$config_file" != *".."* && -f "${saved_dir}/${config_file}" ]] || {
    echo "docker image archive contains an invalid config path" >&2
    exit 65
  }
  gitleaks dir "${saved_dir}/${config_file}" \
    --config "$config_path" \
    --redact=100 \
    --no-banner \
    --verbose
done

layer_index=0
for layer_file in "${layer_files[@]}"; do
  [[ "$layer_file" != /* && "$layer_file" != *".."* && -f "${saved_dir}/${layer_file}" ]] || {
    echo "docker image archive contains an invalid layer path" >&2
    exit 65
  }
  layer_index=$((layer_index + 1))
  destination=$(printf '%s/%04d' "$layer_dir" "$layer_index")
  mkdir -p "$destination"
  tar --extract --file "${saved_dir}/${layer_file}" --directory "$destination"
done

gitleaks dir "$layer_dir" \
  --config "$config_path" \
  --redact=100 \
  --no-banner \
  --verbose

echo "image secret scan passed: $image"
