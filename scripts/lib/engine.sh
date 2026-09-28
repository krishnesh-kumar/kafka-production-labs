# shellcheck shell=bash
# Container engine abstraction, sourced by labs.sh and every lab script.
#
#   CONTAINER_ENGINE=docker|podman   pick one explicitly
#                                    (default: docker if its daemon answers, otherwise podman)
#
# Sets and exports:
#   ENGINE    docker | podman                  (for exec / stop / start / inspect / run)
#   COMPOSE   "docker compose" | "podman compose"
# Defines:
#   compose_up [args]      $COMPOSE up -d --wait, or up -d + wait_healthy if --wait is unsupported
#   wait_healthy [secs]    wait until every container of the current compose project is healthy,
#                          running (no healthcheck) or exited 0 (one-shot jobs)
#   host_path <dir>        a directory in the form the engine accepts for bind mounts

# Git Bash (MSYS) rewrites arguments that look like paths ("/opt/kafka/bin/..." becomes
# "C:/Program Files/Git/opt/..."), which breaks `exec` into Linux containers.
case "$(uname -s)" in MINGW* | MSYS* | CYGWIN*) export MSYS_NO_PATHCONV=1 ;; esac

_labs_pick_engine() {
  if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
    echo "$CONTAINER_ENGINE"
  elif command -v docker > /dev/null 2>&1 && docker info > /dev/null 2>&1; then
    echo docker
  elif command -v podman > /dev/null 2>&1; then
    echo podman
  elif command -v docker > /dev/null 2>&1; then
    echo docker # installed but not answering: let the first real command explain why
  else
    echo none
  fi
}

ENGINE="$(_labs_pick_engine)"
case "$ENGINE" in
  docker)
    COMPOSE="docker compose"
    ;;
  podman)
    COMPOSE="podman compose"
    # `podman compose` hands the file to a provider (docker-compose v2 recommended) that talks to the
    # Podman API socket. Podman's API has no BuildKit endpoint, so builds use the classic builder.
    export DOCKER_BUILDKIT=0 COMPOSE_BAKE=false
    export PODMAN_COMPOSE_WARNING_LOGS="${PODMAN_COMPOSE_WARNING_LOGS:-false}"
    ;;
  none)
    echo "No container engine found: install Docker or Podman (see the root README)." >&2
    return 1 2> /dev/null || exit 1
    ;;
  *)
    echo "CONTAINER_ENGINE must be docker or podman, got '$ENGINE'." >&2
    return 1 2> /dev/null || exit 1
    ;;
esac
export CONTAINER_ENGINE="$ENGINE" ENGINE COMPOSE

if [[ -z "${LABS_ENGINE_ANNOUNCED:-}" ]]; then
  echo "Container engine: $ENGINE (compose: $COMPOSE)" >&2
  export LABS_ENGINE_ANNOUNCED=1
fi

host_path() {
  if [[ -n "${MSYS_NO_PATHCONV:-}" ]]; then
    (cd "$1" && pwd -W)
  else
    (cd "$1" && pwd)
  fi
}

wait_healthy() {
  local timeout="${1:-300}" deadline ids verdict
  deadline=$((SECONDS + timeout))
  while :; do
    ids="$($COMPOSE ps -a -q 2> /dev/null || true)"
    if [[ -n "$ids" ]]; then
      # shellcheck disable=SC2086 # ids is a whitespace-separated list on purpose
      verdict="$($ENGINE inspect $ids | python3 -c '
import json, sys
bad, waiting = [], []
for c in json.load(sys.stdin):
    name = c.get("Name", "?").lstrip("/")
    st = c.get("State", {})
    status = st.get("Status", "?")
    health = (st.get("Health") or st.get("Healthcheck") or {}).get("Status", "")
    if status == "exited":
        if st.get("ExitCode", 1) != 0:
            bad.append("%s exited with code %s" % (name, st.get("ExitCode")))
    elif status == "running" and health in ("", "healthy"):
        pass
    elif health == "unhealthy":
        bad.append(name + " is unhealthy")
    else:
        waiting.append("%s (%s)" % (name, ", ".join(x for x in (status, health) if x)))
if bad:
    print("FAIL " + "; ".join(bad))
elif waiting:
    print("WAIT " + ", ".join(waiting))
else:
    print("OK")
')"
      case "$verdict" in
        OK) return 0 ;;
        FAIL*) echo "wait_healthy: ${verdict#FAIL }" >&2; return 1 ;;
      esac
    fi
    if ((SECONDS >= deadline)); then
      echo "wait_healthy: timed out after ${timeout}s: ${verdict:-no containers}" >&2
      return 1
    fi
    sleep 3
  done
}

compose_up() {
  local timeout="${LABS_WAIT_TIMEOUT:-300}"
  if $COMPOSE up --help 2> /dev/null | grep -q -- '--wait-timeout'; then
    $COMPOSE up -d --wait --wait-timeout "$timeout" "$@"
  else
    $COMPOSE up -d "$@" && wait_healthy "$timeout"
  fi
}
