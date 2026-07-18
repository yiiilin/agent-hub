#!/usr/bin/env bash
set -euo pipefail

QA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec node "${QA_ROOT}/runner.mjs" "$@"
