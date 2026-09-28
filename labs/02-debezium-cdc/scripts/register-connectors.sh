#!/usr/bin/env bash
# Register (or update) both connectors and wait until their tasks are RUNNING.
set -euo pipefail
cd "$(dirname "$0")/.."
CONNECT="${CONNECT_URL:-http://localhost:8083}"

for file in connectors/*.json; do
  name=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$file")
  config=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$file")
  echo "Registering $name"
  # Connect validates the config (including a test connection to PostgreSQL) before accepting it, so
  # a database or DNS entry that is not reachable yet gives HTTP 400. Retry a few times, then give up
  # and show Connect's answer.
  code="" tmp=$(mktemp)
  for attempt in 1 2 3 4 5 6; do
    code=$(curl -s -o "$tmp" -w '%{http_code}' -X PUT -H "Content-Type: application/json" \
      --data "$config" "$CONNECT/connectors/$name/config" || echo 000)
    [[ "$code" == 200 || "$code" == 201 ]] && break
    echo "  attempt $attempt: HTTP $code $(head -c 300 "$tmp" 2> /dev/null)"
    sleep 5
  done
  rm -f "$tmp"
  [[ "$code" == 200 || "$code" == 201 ]] || { echo "$name was not accepted by Kafka Connect"; exit 1; }
done

for file in connectors/*.json; do
  name=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$file")
  for _ in $(seq 1 60); do
    state=$(curl -sf "$CONNECT/connectors/$name/status" | python3 -c '
import json,sys
s=json.load(sys.stdin); tasks=s.get("tasks",[])
print("RUNNING" if s["connector"]["state"]=="RUNNING" and tasks and all(t["state"]=="RUNNING" for t in tasks) else "WAIT")' || echo WAIT)
    [[ "$state" == "RUNNING" ]] && break
    sleep 2
  done
  [[ "$state" == "RUNNING" ]] || { curl -s "$CONNECT/connectors/$name/status"; echo; echo "$name did not reach RUNNING"; exit 1; }
  echo "$name: RUNNING"
done
