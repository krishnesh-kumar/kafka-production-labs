#!/usr/bin/env bash
# CDC walkthrough that checks itself:
#   1. initial snapshot of existing rows (op=r)
#   2. insert / update / delete become c / u / d events, delete also emits a tombstone
#   3. transactional outbox row becomes a clean domain event on outbox.event.order
#   4. drill: Kafka Connect is down while 50 orders are written; nothing is lost when it comes back
set -euo pipefail
cd "$(dirname "$0")/.."

PSQL="docker exec -i lab02-postgres psql -U shop -d shop -v ON_ERROR_STOP=1 -qAt"
KAFKA="docker exec lab02-kafka /opt/kafka/bin"
CONNECT="${CONNECT_URL:-http://localhost:8083}"

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\n\033[31mDEMO FAILED: %s\033[0m\n' "$*"; exit 1; }

consume() { # topic -> all records currently in the topic (key<TAB>value)
  $KAFKA/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic "$1" --from-beginning \
    --timeout-ms 15000 --property print.key=true --property print.headers="${2:-false}" 2>/dev/null || true
}

wait_for() { # topic pattern min_count [print_headers]
  local n=0
  for _ in $(seq 1 12); do
    n=$(consume "$1" "${4:-false}" | grep -c -- "$2" || true)
    [[ "$n" -ge "$3" ]] && { echo "$n"; return 0; }
    sleep 5
  done
  echo "$n"; return 1
}

step "1. Initial snapshot: existing orders arrive as op=r"
n=$(wait_for shop.public.orders '"op":"r"' 2) || fail "snapshot events missing (found $n)"
echo "Snapshot events on shop.public.orders: $n"

step "2. Insert, update, delete"
$PSQL <<'SQL'
INSERT INTO orders (id, customer_id, status, total_minor) VALUES (1001, 3, 'PENDING', 7500);
UPDATE orders SET status = 'PAID', updated_at = now() WHERE id = 1001;
DELETE FROM orders WHERE id = 1001;
SQL
for op in c u d; do
  n=$(wait_for shop.public.orders "\"op\":\"$op\"" 1) || fail "no op=$op event"
  echo "op=$op events: $n"
done
consume shop.public.orders | grep '"op":"u"' | head -1 | python3 -c '
import json,sys
line=sys.stdin.read().split("\t",1)[1]
e=json.loads(line)
print("  update before:", {k:e["before"][k] for k in ("id","status")})
print("  update after: ", {k:e["after"][k] for k in ("id","status")})'
tomb=$(consume shop.public.orders | awk -F'\t' '$2=="null"' | wc -l)
echo "Tombstones (for log compaction): $tomb"
[[ "$tomb" -ge 1 ]] || fail "expected a tombstone after DELETE"

step "3. Transactional outbox: order + event committed together"
$PSQL <<'SQL'
BEGIN;
INSERT INTO orders (id, customer_id, status, total_minor) VALUES (2001, 2, 'PAID', 31900);
INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload)
VALUES (gen_random_uuid(), 'order', '2001', 'OrderPaid',
        '{"orderId": 2001, "customerId": 2, "totalMinor": 31900, "currency": "EUR"}');
COMMIT;
SQL
# The event type travels in the eventType header (the value is only the payload), so print headers.
n=$(wait_for outbox.event.order 'eventType:OrderPaid' 1 true) || fail "outbox event missing on outbox.event.order"
consume outbox.event.order true | tail -1
echo "Key = aggregate id (per-order ordering), header eventType = OrderPaid, value = the payload only."

step "4. Drill: Kafka Connect goes down while the database keeps writing"
docker stop lab02-connect > /dev/null
$PSQL -c "INSERT INTO orders (customer_id, status, total_minor)
          SELECT 1, 'WRITTEN_DURING_OUTAGE', 1000 + g FROM generate_series(1, 50) g;"
echo "Replication slot while Connect is down (WAL retained for the connector):"
$PSQL -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
          FROM pg_replication_slots ORDER BY slot_name;"
docker start lab02-connect > /dev/null
for _ in $(seq 1 60); do curl -sf "$CONNECT/connectors" > /dev/null && break; sleep 3; done
./scripts/register-connectors.sh > /dev/null
n=$(wait_for shop.public.orders 'WRITTEN_DURING_OUTAGE' 50) || fail "only $n of 50 outage rows reached Kafka"
echo "Rows written during the outage that reached Kafka: $n / 50"

printf '\n\033[32mDEMO PASSED\033[0m\n'
cat <<'TXT'

What this shows a client:
  - Debezium resumes from its replication slot: an outage delays events, it does not lose them.
  - The flip side: while the connector is down the slot pins WAL on the database. Alert on slot lag.
  - The outbox gives consumers business events, not table rows, with no dual-write race.
TXT
