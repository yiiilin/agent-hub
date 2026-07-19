#!/bin/sh
set -eu

case "$0" in
  */*) install_dir="${0%/*}" ;;
  *) install_dir="." ;;
esac
version="${install_dir##*/}"
case "$version" in
  .staging-*)
    version="${version#.staging-}"
    version="${version%-????????-????-????-????-????????????}"
    ;;
esac

case "${1:-}" in
  --version)
    printf 'codex-cli %s\n' "$version"
    ;;
  app-server)
    if [ "${2:-}" = "--help" ]; then
      printf 'Usage: codex app-server --listen stdio://\n'
      exit 0
    fi
    exec /usr/local/bin/fake-codex "$@"
    ;;
  *)
    echo 'usage: codex --version | app-server [--help]' >&2
    exit 64
    ;;
esac
