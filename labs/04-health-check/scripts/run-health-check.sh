#!/usr/bin/env bash
# Run kafka-healthcheck-lite against the lab 01 cluster and check that it finds the seeded mistakes.
#
# The CLI runs in a python container on lab 01's network, so the host needs no Python packages.
# Requires lab 01 running and seeded:  ./labs.sh up 04 && ./scripts/seed-demo-cluster.sh
# Writes the report to $REPORT (default: report.md in this lab's directory).
set -euo pipefail
cd "$(dirname "$0")/.."
source ../../scripts/lib/engine.sh   # sets $ENGINE (docker|podman) and $COMPOSE

REPORT="${REPORT:-report.md}"
IMAGE="docker.io/library/python:3.12-slim"
NETWORK="lab01_default"

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\n\033[31mHEALTH CHECK FAILED: %s\033[0m\n' "$*"; exit 1; }

step "Run the health check against lab 01 (bootstrap kafka-1:19092, read-only)"
$ENGINE run --rm --network "$NETWORK" -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$(host_path .):/lab:ro,z" -w /lab "$IMAGE" sh -c '
    pip install -q --root-user-action=ignore --disable-pip-version-check -r requirements.txt >&2 &&
    python -m healthcheck --bootstrap kafka-1:19092' > "$REPORT" || fail "the CLI did not run"
echo "Report written to $REPORT ($(grep -c '^### ' "$REPORT") findings)"

step "Summary and top fixes from the report"
sed -n '/^## Summary/,/^## Findings/p' "$REPORT" | sed '$d'

step "The seeded mistakes are all reported"
for expected in "min.insync.replicas equals the replication factor" \
                "replication factor 1" \
                "no active members"; do
  grep -q "$expected" "$REPORT" || fail "report does not mention: $expected"
  echo "found: $expected"
done

printf '\n\033[32mHEALTH CHECK PASSED\033[0m\n'
cat <<'TXT'

What this shows a client:
  - A read-only pass (describe and list APIs only) finds the configurations that turn a routine
    broker restart into an outage or data loss, and ranks them.
  - It is a first pass: the report ends with what an automated check cannot judge.
TXT
