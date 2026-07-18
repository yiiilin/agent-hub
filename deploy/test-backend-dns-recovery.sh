#!/bin/sh
set -eu

project=${E2E_COMPOSE_PROJECT:-agent-hub-audit}
compose_file=${E2E_COMPOSE_FILE:-deploy/docker-compose.yml}
base_url=${E2E_BASE_URL:-http://localhost:15173}
blocker_name="${project}-backend-dns-blocker"
wait_timeout_secs=${DNS_RECOVERY_WAIT_TIMEOUT_SECS:-60}
curl_connect_timeout_secs=${DNS_RECOVERY_CURL_CONNECT_TIMEOUT_SECS:-2}
curl_max_time_secs=${DNS_RECOVERY_CURL_MAX_TIME_SECS:-3}
backend_replaced=false
replacement_verified=false
blocker_names=""

compose() {
  docker compose -p "$project" -f "$compose_file" "$@"
}

require_positive_integer() {
  name=$1
  value=$2
  case $value in
    ''|*[!0-9]*|0)
      echo "$name must be a positive integer" >&2
      exit 2
      ;;
  esac
}

wait_for_url_200() {
  endpoint=${1%%\?*}
  url=$2
  deadline=$(( $(date +%s) + wait_timeout_secs ))
  last_status="not attempted"
  while :; do
    now=$(date +%s)
    remaining=$((deadline - now))
    if [ "$remaining" -le 0 ]; then
      break
    fi
    request_timeout=$curl_max_time_secs
    if [ "$remaining" -lt "$request_timeout" ]; then
      request_timeout=$remaining
    fi
    if status=$(curl -sS -o /dev/null -w '%{http_code}' \
      --connect-timeout "$curl_connect_timeout_secs" \
      --max-time "$request_timeout" \
      "$url"); then
      curl_status=0
    else
      curl_status=$?
    fi
    if [ "$status" = 200 ]; then
      return 0
    fi
    if [ "$curl_status" -eq 0 ]; then
      last_status="HTTP ${status:-000}"
    else
      last_status="curl exit $curl_status (HTTP ${status:-000})"
    fi
    now=$(date +%s)
    if [ "$now" -ge "$deadline" ]; then
      break
    fi
    sleep 1
  done
  echo "timed out after ${wait_timeout_secs}s waiting for ${endpoint}; last status: ${last_status}" >&2
  return 1
}

wait_for_http_200() {
  path=$1
  wait_for_url_200 "$path" "${base_url}${path}"
}

remove_blockers() {
  remove_status=0
  for name in $blocker_names; do
    if docker container inspect "$name" >/dev/null 2>&1; then
      if ! docker rm -f "$name" >/dev/null; then
        echo "failed to remove backend DNS blocker" >&2
        remove_status=1
      fi
    fi
  done
  blocker_names=""
  return "$remove_status"
}

occupy_old_backend_ip() {
  blocker_index=1
  while [ "$blocker_index" -le 32 ]; do
    name="${blocker_name}-${blocker_index}"
    blocker_names="$blocker_names $name"
    blocker_id=$(docker run -d --rm --name "$name" --network "$network_name" "$blocker_image" sleep 300) || return 1
    blocker_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$blocker_id") || return 1
    if [ "$blocker_ip" = "$old_backend_ip" ]; then
      return 0
    fi
    blocker_index=$((blocker_index + 1))
  done
  echo "could not occupy the previous backend address with bounded blocker allocation" >&2
  return 1
}

wait_for_backend_readiness() {
  backend_id=$(compose ps -q backend) || return 1
  if [ -z "$backend_id" ]; then
    echo "restored backend container was not created" >&2
    return 1
  fi
  backend_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$backend_id") || return 1
  wait_for_url_200 /readyz "http://${backend_ip}:8080/readyz"
}

restore_backend() {
  restore_status=0
  if ! compose rm -sf backend >/dev/null; then
    echo "failed to remove the unverified replacement backend" >&2
    restore_status=1
  fi
  if ! remove_blockers; then
    restore_status=1
  fi
  if ! compose up -d --no-deps --force-recreate backend >/dev/null; then
    echo "failed to recreate backend during cleanup" >&2
    return 1
  fi
  if ! wait_for_backend_readiness; then
    echo "recreated backend did not become ready during cleanup" >&2
    return 1
  fi
  return "$restore_status"
}

on_exit() {
  main_status=$1
  trap - EXIT INT TERM
  cleanup_status=0
  if [ "$replacement_verified" = true ]; then
    if ! remove_blockers; then
      cleanup_status=1
    fi
  elif [ "$backend_replaced" = true ]; then
    if ! restore_backend; then
      cleanup_status=1
    fi
  elif ! remove_blockers; then
    cleanup_status=1
  fi
  if [ "$cleanup_status" -ne 0 ]; then
    echo "backend DNS recovery cleanup failed" >&2
    if [ "$main_status" -eq 0 ]; then
      main_status=$cleanup_status
    fi
  fi
  exit "$main_status"
}

require_positive_integer DNS_RECOVERY_WAIT_TIMEOUT_SECS "$wait_timeout_secs"
require_positive_integer DNS_RECOVERY_CURL_CONNECT_TIMEOUT_SECS "$curl_connect_timeout_secs"
require_positive_integer DNS_RECOVERY_CURL_MAX_TIME_SECS "$curl_max_time_secs"

if [ -n "${DNS_RECOVERY_WAIT_ONLY_PATH:-}" ]; then
  wait_for_http_200 "$DNS_RECOVERY_WAIT_ONLY_PATH"
  exit
fi

trap 'on_exit $?' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_http_200 /healthz
wait_for_http_200 /api/auth/providers

frontend_id=$(compose ps -q frontend)
backend_id=$(compose ps -q backend)
test -n "$frontend_id"
test -n "$backend_id"
frontend_started_at=$(docker inspect -f '{{.State.StartedAt}}' "$frontend_id")
old_backend_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$backend_id")
network_name=$(docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$backend_id")
blocker_image=$(docker inspect -f '{{.Config.Image}}' "$(compose ps -q postgres)")

backend_replaced=true
compose rm -sf backend >/dev/null
occupy_old_backend_ip
compose up -d --no-deps --force-recreate backend >/dev/null

new_backend_id=$(compose ps -q backend)
new_backend_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$new_backend_id")
if [ "$new_backend_ip" = "$old_backend_ip" ]; then
  echo "backend recreate did not change its container address" >&2
  exit 1
fi

wait_for_http_200 /healthz
wait_for_http_200 /api/auth/providers

current_frontend_id=$(compose ps -q frontend)
current_frontend_started_at=$(docker inspect -f '{{.State.StartedAt}}' "$current_frontend_id")
test "$current_frontend_id" = "$frontend_id"
test "$current_frontend_started_at" = "$frontend_started_at"
replacement_verified=true

printf 'frontend=%s started_at=%s backend=%s backend_ip=%s->%s healthz=200 api=200\n' \
  "$frontend_id" "$frontend_started_at" "$new_backend_id" "$old_backend_ip" "$new_backend_ip"
