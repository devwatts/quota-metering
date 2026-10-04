#!/bin/sh
set -eu
cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
    printf '%s\n' 'ERROR: Docker with Compose v2 is required.' >&2
    exit 1
fi

completion_dir=$(mktemp -d "${TMPDIR:-/tmp}/quota-demo.XXXXXX")
compose_pid=

cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [ -n "$compose_pid" ]; then
        kill -TERM "$compose_pid" 2>/dev/null || true
        wait "$compose_pid" 2>/dev/null || true
    fi
    if ! docker compose stop --timeout 5 >/dev/null 2>&1; then
        printf '%s\n' 'ERROR: Could not stop all demo containers; inspect docker compose ps.' >&2
        [ "$status" -ne 0 ] || status=1
    fi
    rm -rf "$completion_dir"
    exit "$status"
}

fail() {
    printf 'ERROR: %s\n' "$1" >&2
    docker compose ps -a >&2 || true
    docker compose logs --no-color --tail 40 >&2 || true
    exit 1
}

trap cleanup EXIT
trap 'printf "%s\n" "ERROR: Demo interrupted before completion." >&2; exit 130' INT
trap 'printf "%s\n" "ERROR: Demo terminated before completion." >&2; exit 143' TERM

# Fresh containers prevent a previous run's completion marker from being reused.
docker compose up --build --force-recreate --timeout 5 \
    --abort-on-container-exit --exit-code-from demo &
compose_pid=$!
if wait "$compose_pid"; then
    compose_pid=
else
    compose_status=$?
    compose_pid=
    fail "Startup or demo failed (Compose exit $compose_status). See service logs below."
fi

if ! docker compose cp demo:/tmp/quota-demo-completed "$completion_dir/completed" >/dev/null 2>&1; then
    fail 'Demo exited without completing its request and accounting checks.'
fi
printf '%s\n' 'Demo completed: requests and Redis/PostgreSQL accounting checks passed.'
