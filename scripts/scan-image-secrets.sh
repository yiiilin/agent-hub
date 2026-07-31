#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
config_path="${GITLEAKS_CONFIG:-${repo_root}/.gitleaks.toml}"
# Canonical SHA-256 of the sorted RuleID/Match pairs emitted for pinned Pi 0.81.1
# by Gitleaks 8.28.0. Count and digest must both match; no raw finding is retained.
pi_findings_count=6
pi_findings_sha256=1072658bfb09a9b992f1b8218baa5b15744ce48294753388946a4f168f5340ae

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <image>" >&2
  exit 64
fi

for command in awk docker gitleaks grep jq sha256sum strings tar; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 69
  fi
done

if [[ -n "${GITLEAKS_VERSION:-}" ]]; then
  actual_gitleaks_version=$(gitleaks version)
  [[ "$actual_gitleaks_version" == "$GITLEAKS_VERSION" ]] || {
    echo "expected Gitleaks $GITLEAKS_VERSION, got $actual_gitleaks_version" >&2
    exit 69
  }
fi

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

binary_index=0
scan_application_binary() {
  local candidate=$1
  local relative_path=${candidate#${layer_dir}/}
  local strings_path report_path scan_status finding_count finding_hash

  binary_index=$((binary_index + 1))
  strings_path=$(printf '%s/binary-%04d.strings' "$work_dir" "$binary_index")
  report_path=$(printf '%s/binary-%04d-findings.json' "$work_dir" "$binary_index")
  strings -a -n 8 "$candidate" > "$strings_path"

  set +e
  gitleaks dir "$strings_path" \
    --config "$config_path" \
    --redact=100 \
    --no-banner \
    --log-level error \
    --report-format json \
    --report-path "$report_path"
  scan_status=$?
  set -e

  if [[ "$scan_status" -eq 0 ]]; then
    return
  fi
  if [[ "$scan_status" -ne 1 || ! -f "$report_path" ]]; then
    echo "Gitleaks failed while scanning application binary: $relative_path" >&2
    return 1
  fi

  finding_count=$(jq 'length' "$report_path")
  finding_hash=$(
    jq -cS 'sort_by(.RuleID, .Match) | map({RuleID, Match})' "$report_path" \
      | sha256sum \
      | awk '{print $1}'
  )
  if [[ "$relative_path" == */opt/agent-hub/pi/pi \
    && "$finding_count" -eq "$pi_findings_count" \
    && "$finding_hash" == "$pi_findings_sha256" ]]; then
    echo "accepted pinned Pi binary scanner baseline: ${pi_findings_count} known code-pattern findings"
    return
  fi

  echo "unapproved findings in application binary: $relative_path" >&2
  jq -r 'group_by(.RuleID)[] | "  \(.[0].RuleID): \(length)"' "$report_path" >&2
  return 1
}

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

while IFS= read -r -d '' candidate; do
  if LC_ALL=C grep -Iq . "$candidate"; then
    continue
  fi
  echo "scanning application binary strings: ${candidate#${layer_dir}/}"
  scan_application_binary "$candidate"
done < <(
  find "$layer_dir" -type f \
    \( -path "$layer_dir/*/usr/local/bin/*" -o -path "$layer_dir/*/opt/agent-hub/*" \) \
    -print0
)

echo "image secret scan passed: $image"
