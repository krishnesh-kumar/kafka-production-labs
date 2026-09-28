#!/usr/bin/env bash
# One entry point for every lab, on Docker or Podman.
#
#   ./labs.sh list              labs and whether they are running
#   ./labs.sh up <nn>           start a lab and wait until it is healthy
#   ./labs.sh drill <nn>        run the lab's drill / demo (exits non-zero if a check fails)
#   ./labs.sh down <nn>         stop the lab and delete its volumes
#   ./labs.sh test [03|04]      unit tests, run inside containers (no local Java or Python packages)
#
#   CONTAINER_ENGINE=podman ./labs.sh up 01     force an engine (default: docker if it answers)
#
# Labs share host ports (9092, 8080, ...), so `up` refuses to start one while another is running.
# Lab 04 (health check) runs against lab 01's cluster: `up 04` starts lab 01.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
source "$ROOT/scripts/lib/engine.sh"   # sets $ENGINE (docker|podman) and $COMPOSE

usage() { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }
die() { echo "labs.sh: $*" >&2; exit 1; }

norm() { # "1" | "01" -> "01"
  [[ "${1:-}" =~ ^[0-9]{1,2}$ ]] || die "expected a lab number like 01, got '${1:-}'"
  printf '%02d' "$((10#$1))"
}

lab_dir() { # nn -> labs/<nn>-<name>
  local d
  d=$(compgen -G "$ROOT/labs/$(norm "$1")-*" | head -1 || true)
  [[ -n "$d" && -d "$d" ]] || die "no lab $1 (try ./labs.sh list)"
  echo "$d"
}

# The lab whose containers a lab uses. Lab 04 has no compose file: it runs against lab 01.
stack_of() { case "$(norm "$1")" in 04) echo 01 ;; *) norm "$1" ;; esac; }

running_stacks() { # prints the NN of every lab with running containers (container names are labNN-*)
  { $ENGINE ps --format '{{.Names}}' 2> /dev/null || true; } | sed -n 's/^lab\([0-9][0-9]\)-.*/\1/p' | sort -u
}

cmd_list() {
  local running d nn state
  running=" $(running_stacks | tr '\n' ' ') "
  for d in "$ROOT"/labs/[0-9][0-9]-*/; do
    d=${d%/}; nn=$(basename "$d" | cut -c1-2)
    state="stopped"
    [[ "$running" == *" $(stack_of "$nn") "* ]] && state="running"
    [[ "$nn" == 04 ]] && state="$state (uses lab 01's cluster)"
    printf '  %s  %-22s %s\n' "$nn" "$(basename "$d" | cut -c4-)" "$state"
  done
}

cmd_up() {
  local nn stack other
  nn=$(norm "$1"); lab_dir "$nn" > /dev/null; stack=$(stack_of "$nn")
  for other in $(running_stacks); do
    [[ "$other" == "$stack" ]] && continue
    die "lab $other is running and uses the same host ports. Stop it first: ./labs.sh down $other"
  done
  cd "$(lab_dir "$stack")"
  echo "Starting lab $stack ($(basename "$PWD"))"
  if [[ "$stack" == 03 ]]; then
    # init-topics is a one-shot job, so wait with wait_healthy (accepts "exited 0") instead of --wait.
    $COMPOSE --profile demo pull --quiet generator   # used by the drill; pull it now, not mid-demo
    $COMPOSE up -d --build --quiet-pull
    wait_healthy "${LABS_WAIT_TIMEOUT:-300}"
  else
    compose_up --quiet-pull
  fi
  $COMPOSE ps
}

require_running() { # nn
  running_stacks | grep -qx "$(stack_of "$1")" || die "lab $(stack_of "$1") is not running. Start it: ./labs.sh up $1"
}

cmd_drill() {
  local nn d
  nn=$(norm "$1"); d=$(lab_dir "$nn")
  require_running "$nn"
  case "$nn" in
    01) "$d/scripts/failure-drill.sh" ;;
    02) "$d/scripts/register-connectors.sh" && "$d/scripts/cdc-demo.sh" ;;
    03) "$d/scripts/run-demo.sh" ;;
    04) "$d/scripts/seed-demo-cluster.sh" && "$d/scripts/run-health-check.sh" ;;
    *) die "lab $nn has no drill registered in labs.sh" ;;
  esac
}

cmd_down() {
  local nn stack
  nn=$(norm "$1"); lab_dir "$nn" > /dev/null; stack=$(stack_of "$nn")
  cd "$(lab_dir "$stack")"
  if [[ "$stack" == 03 ]]; then
    $COMPOSE --profile demo down -v --remove-orphans
  else
    $COMPOSE down -v --remove-orphans
  fi
}

test_04() {
  echo "==> Lab 04 unit tests (pytest in docker.io/library/python:3.12-slim)"
  $ENGINE run --rm -e PYTHONDONTWRITEBYTECODE=1 \
    -v "$(host_path "$(lab_dir 04)"):/lab:ro,z" -w /lab docker.io/library/python:3.12-slim sh -c '
      pip install -q --root-user-action=ignore --disable-pip-version-check -r requirements.txt -r requirements-dev.txt &&
      python -m pytest -q -p no:cacheprovider tests'
}

test_03() {
  echo "==> Lab 03 unit tests (mvn verify in docker.io/library/maven:3.9-eclipse-temurin-17)"
  # Build from a copy so the container never writes target/ into your checkout; cache ~/.m2 in a volume.
  $ENGINE run --rm -v "$(host_path "$(lab_dir 03)/app"):/src:ro,z" -v labs-m2-cache:/root/.m2 \
    docker.io/library/maven:3.9-eclipse-temurin-17 sh -c 'cp -r /src /build && cd /build && mvn -B -ntp verify'
}

cmd_test() {
  case "${1:-all}" in
    all) test_04 && test_03 ;;
    3 | 03) test_03 ;;
    4 | 04) test_04 ;;
    *) die "unit tests exist for labs 03 and 04" ;;
  esac
}

case "${1:-}" in
  list) cmd_list ;;
  up | drill | down) [[ $# -eq 2 ]] || usage 1; "cmd_$1" "$2" ;;
  test) cmd_test "${2:-all}" ;;
  -h | --help | help | "") usage 0 ;;
  *) echo "unknown command: $1" >&2; usage 1 ;;
esac
