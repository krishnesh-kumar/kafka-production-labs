#!/usr/bin/env bash
# Feed 14 simulated minutes of payments and check that exactly one merchant is alerted and then cleared.
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\n\033[31mDEMO FAILED: %s\033[0m\n' "$*"; exit 1; }

step "Generate payments (merchant-3 has an incident from minute 4 to 7)"
docker compose --profile demo run --rm generator

step "Alerts emitted by the Streams app"
alerts=""
for _ in $(seq 1 24); do
  alerts=$(docker exec lab03-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 \
    --topic merchant-alerts --from-beginning --timeout-ms 10000 2>/dev/null || true)
  grep -q '"CLEARED"' <<< "$alerts" && break
  sleep 5
done
echo "$alerts" | python3 -c '
import json, sys, datetime
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    a = json.loads(line)
    end = datetime.datetime.fromtimestamp(a["windowEnd"] / 1000).strftime("%H:%M:%S")
    print("%-8s %s  window ending %s  %d/%d failed (%.0f%%)" % (a["state"], a["merchantId"], end, a["failed"], a["total"], a["failureRate"] * 100))'

raised=$(grep -c '"RAISED"' <<< "$alerts" || true)
cleared=$(grep -c '"CLEARED"' <<< "$alerts" || true)
others=$(grep -v 'merchant-3' <<< "$alerts" | grep -c 'merchant' || true)
[[ "$raised" -eq 1 ]] || fail "expected 1 RAISED, got $raised"
[[ "$cleared" -eq 1 ]] || fail "expected 1 CLEARED, got $cleared"
[[ "$others" -eq 0 ]] || fail "healthy merchants must not alert"

# Every merchant pays once per simulated second, so both decisions must come from a full 5-minute
# window (~300 payments). A smaller window means payments were dropped as late, i.e. the event-time
# processing did not see the traffic it was given.
short=$(grep '"merchant-3"' <<< "$alerts" | python3 -c '
import json, sys
print(sum(1 for line in sys.stdin if line.strip() and json.loads(line)["total"] < 280))')
[[ "$short" -eq 0 ]] || fail "an alert was decided on a partial window (payments dropped as late?)"
expired=$(docker logs lab03-alerting 2>&1 | grep -c "Skipping record for expired window" || true)
echo "Payments dropped as later than the grace period: $expired"
[[ "$expired" -eq 0 ]] || fail "$expired payments were dropped as late"

printf '\n\033[32mDEMO PASSED\033[0m\n'
cat <<'TXT'

What this shows a client:
  - One RAISED and one CLEARED per incident, not an alert per payment: the latch lives in a state store.
  - Raise at 30%, clear at 10%: no flapping when the rate hovers near a single threshold.
  - Windows run on event time, so late data inside the grace period gives the same answer as live traffic.
TXT
