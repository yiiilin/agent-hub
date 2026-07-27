#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PI_DIR="${ROOT_DIR}/third_party/pi"
PI_VERSION="0.81.1"
PI_COMMIT="20be4b18d4c57487f8993d2762bace129f0cf7c6"
PI_PATCH="${ROOT_DIR}/third_party/pi-patches/0001-add-rpc-reload-models.patch"
PI_PATCH_SHA256="50d376fa2288d1e1f16392a82a74ca7185db035c455e0bf88cc7b87cd374f7d9"
MODEL_DATA_DIR="${ROOT_DIR}/third_party/pi-model-data/v${PI_VERSION}"
MODEL_DATA_SHA256="27928526a62db7d9f808b9efebe1d2529d782ace46c9e9dacc327c7dfb2a261e"
BUN_VERSION="1.3.14"
BUN_ARCHIVE_SHA256="a063908ae08b7852ca10939bbdc6ceed3ddabce8fb9402dce83d65d73b36e6c7"
BUN_ARCHIVE_URL="https://github.com/oven-sh/bun/releases/download/bun-v${BUN_VERSION}/bun-linux-x64-baseline.zip"
OUTPUT_DIR="${ROOT_DIR}/target/pi-runtime/linux-x64"
INSTALL_DEPENDENCIES=true
TEMPORARY_ARCHIVE=""
PI_BUILD_DIR=""

cleanup() {
  if [[ -n "$TEMPORARY_ARCHIVE" ]]; then
    rm -f "$TEMPORARY_ARCHIVE"
  fi
  if [[ -n "$PI_BUILD_DIR" ]]; then
    rm -rf "$PI_BUILD_DIR"
  fi
}

trap cleanup EXIT

usage() {
  cat <<'EOF'
Usage: scripts/build-pi-standalone.sh [--out <directory>] [--skip-install]

Build the pinned Pi submodule as a Linux x64 baseline standalone release
directory. The output intentionally contains the Pi binary plus runtime assets;
it is not a single-file distribution.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      OUTPUT_DIR="${2:?--out requires a directory}"
      shift 2
      ;;
    --skip-install)
      INSTALL_DEPENDENCIES=false
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

require_file() {
  [[ -f "$1" ]] || { echo "required Pi runtime file is missing: $1" >&2; exit 1; }
}

tree_sha256() {
  (
    cd "$1"
    while IFS= read -r -d '' path; do
      sha256sum "${path#./}"
    done < <(find . -type f -print0 | LC_ALL=C sort -z)
  ) | sha256sum | awk '{print $1}'
}

[[ -d "$PI_DIR/.git" || -f "$PI_DIR/.git" ]] || {
  echo "Pi submodule is not initialized; run git submodule update --init --recursive" >&2
  exit 1
}
[[ "$(git -C "$PI_DIR" rev-parse HEAD)" == "$PI_COMMIT" ]] || {
  echo "Pi submodule must be pinned to ${PI_COMMIT}" >&2
  exit 1
}
require_file "$PI_PATCH"
[[ "$(sha256sum "$PI_PATCH" | awk '{print $1}')" == "$PI_PATCH_SHA256" ]] || {
  echo "Pi RPC compatibility patch checksum mismatch" >&2
  exit 1
}
[[ -d "$MODEL_DATA_DIR" ]] || {
  echo "Pi model-data snapshot is missing: ${MODEL_DATA_DIR}" >&2
  exit 1
}
[[ "$(tree_sha256 "$MODEL_DATA_DIR")" == "$MODEL_DATA_SHA256" ]] || {
  echo "Pi model-data snapshot checksum mismatch" >&2
  exit 1
}

if [[ -n "${BUN_BIN:-}" ]]; then
  BUN="${BUN_BIN}"
else
  BUN_CACHE_DIR="${XDG_CACHE_HOME:-${HOME}/.cache}/agent-hub/bun/${BUN_VERSION}/linux-x64-baseline"
  BUN_ARCHIVE="${BUN_CACHE_DIR}/bun-linux-x64-baseline.zip"
  BUN="${BUN_CACHE_DIR}/bun-linux-x64-baseline/bun"
  mkdir -p "$BUN_CACHE_DIR"
  if [[ ! -f "$BUN_ARCHIVE" ]]; then
    TEMPORARY_ARCHIVE="$(mktemp "${BUN_CACHE_DIR}/bun.XXXXXX.zip")"
    curl --fail --location --silent --show-error "$BUN_ARCHIVE_URL" --output "$TEMPORARY_ARCHIVE"
    mv "$TEMPORARY_ARCHIVE" "$BUN_ARCHIVE"
    TEMPORARY_ARCHIVE=""
  fi
  [[ "$(sha256sum "$BUN_ARCHIVE" | awk '{print $1}')" == "$BUN_ARCHIVE_SHA256" ]] || {
    echo "Bun ${BUN_VERSION} archive checksum mismatch" >&2
    exit 1
  }
  if [[ ! -x "$BUN" ]]; then
    rm -rf "${BUN_CACHE_DIR}/bun-linux-x64-baseline"
    unzip -q "$BUN_ARCHIVE" -d "$BUN_CACHE_DIR"
  fi
fi

[[ -x "$BUN" ]] || { echo "Bun binary is not executable: $BUN" >&2; exit 1; }
[[ "$("$BUN" --version)" == "$BUN_VERSION" ]] || {
  echo "expected Bun ${BUN_VERSION}, got $("$BUN" --version)" >&2
  exit 1
}

PI_BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/agent-hub-pi-build.XXXXXX")"
git -C "$PI_DIR" archive "$PI_COMMIT" | tar -xf - -C "$PI_BUILD_DIR"
(
  cd "$PI_BUILD_DIR"
  git apply --no-index "$PI_PATCH"
)

if [[ "$INSTALL_DEPENDENCIES" == true ]]; then
  npm --prefix "$PI_BUILD_DIR" ci --ignore-scripts
else
  [[ -d "$PI_DIR/node_modules" ]] || {
    echo "Pi dependencies are missing; omit --skip-install to install them" >&2
    exit 1
  }
  cp -al "$PI_DIR/node_modules" "$PI_BUILD_DIR/node_modules"
fi

PI_DATA_DIR="$PI_BUILD_DIR/packages/ai/src/providers/data"
rm -rf "$PI_DATA_DIR"
mkdir -p "$PI_DATA_DIR"
cp -a "$MODEL_DATA_DIR/." "$PI_DATA_DIR/"
[[ "$(tree_sha256 "$PI_DATA_DIR")" == "$MODEL_DATA_SHA256" ]] || {
  echo "Pi copied model-data snapshot checksum mismatch" >&2
  exit 1
}

npm --prefix "$PI_BUILD_DIR" run build:offline
RPC_MODE_JS="$PI_BUILD_DIR/packages/coding-agent/dist/modes/rpc/rpc-mode.js"
require_file "$RPC_MODE_JS"
grep -q 'case "reload_resources"' "$RPC_MODE_JS" || {
  echo "compiled Pi RPC mode is missing reload_resources" >&2
  exit 1
}
grep -q 'case "reload_models"' "$RPC_MODE_JS" || {
  echo "compiled Pi RPC mode is missing reload_models" >&2
  exit 1
}

rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
"$BUN" build --compile \
  --target=bun-linux-x64-baseline \
  "$PI_BUILD_DIR/packages/coding-agent/dist/bun/cli.js" \
  "$PI_BUILD_DIR/packages/coding-agent/src/utils/image-resize-worker.ts" \
  --outfile "$OUTPUT_DIR/pi"

CODING_AGENT_DIR="$PI_BUILD_DIR/packages/coding-agent"
NODE_MODULES_DIR="$PI_BUILD_DIR/node_modules"
cp -a "$CODING_AGENT_DIR/package.json" "$OUTPUT_DIR/"
cp -a "$CODING_AGENT_DIR/README.md" "$OUTPUT_DIR/"
cp -a "$CODING_AGENT_DIR/CHANGELOG.md" "$OUTPUT_DIR/"
cp -a "$NODE_MODULES_DIR/@silvia-odwyer/photon-node/photon_rs_bg.wasm" "$OUTPUT_DIR/"
cp -a "$CODING_AGENT_DIR/dist/modes/interactive/theme" "$OUTPUT_DIR/"
cp -a "$CODING_AGENT_DIR/dist/modes/interactive/assets" "$OUTPUT_DIR/"
cp -a "$CODING_AGENT_DIR/dist/core/export-html" "$OUTPUT_DIR/"
cp -a "$CODING_AGENT_DIR/docs" "$OUTPUT_DIR/"
cp -a "$CODING_AGENT_DIR/examples" "$OUTPUT_DIR/"

mkdir -p "$OUTPUT_DIR/node_modules/@mariozechner"
cp -a "$NODE_MODULES_DIR/@mariozechner/clipboard" "$OUTPUT_DIR/node_modules/@mariozechner/"
cp -a "$NODE_MODULES_DIR/@mariozechner/clipboard-linux-x64-gnu" "$OUTPUT_DIR/node_modules/@mariozechner/"
cp -a \
  "$NODE_MODULES_DIR/@mariozechner/clipboard-linux-x64-gnu/clipboard.linux-x64-gnu.node" \
  "$OUTPUT_DIR/node_modules/@mariozechner/clipboard/"

require_file "$OUTPUT_DIR/pi"
require_file "$OUTPUT_DIR/package.json"
require_file "$OUTPUT_DIR/theme/dark.json"
require_file "$OUTPUT_DIR/theme/light.json"
require_file "$OUTPUT_DIR/photon_rs_bg.wasm"
require_file "$OUTPUT_DIR/export-html/index.js"
require_file "$OUTPUT_DIR/node_modules/@mariozechner/clipboard/clipboard.linux-x64-gnu.node"
[[ "$("$OUTPUT_DIR/pi" --version)" == "$PI_VERSION" ]] || {
  echo "standalone Pi version does not match ${PI_VERSION}" >&2
  exit 1
}

echo "Pi ${PI_VERSION} baseline release directory: ${OUTPUT_DIR}"
