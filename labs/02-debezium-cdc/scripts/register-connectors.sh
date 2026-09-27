#!/usr/bin/env bash
# Register (or update) both connectors and wait until their tasks are RUNNING.
set -euo pipefail
cd "$(dirname "$0")/.."
CONNECT="${CONNECT_URL:-http://localhost:8083}"

for file in connectors/*.json; do
  name=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$file")
  config=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$file")
  echo "Registering $name"
  curl -sf -X PUT -H "Content-Type: application/json" --data "$config" "$CONNECT/connectors/$name/config" > /dev/null
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
