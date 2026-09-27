#!/usr/bin/env bash
# Failure drill: lose one broker and show what keeps working and what does not.
#
#   1. Topic "payments": RF=3, min.insync.replicas=2  -> survives one broker loss for acks=all writes
#   2. Topic "payments-strict": RF=3, min.insync.replicas=3 -> a common misconfiguration: one broker down
#      and every acks=all write is rejected with NotEnoughReplicas
#   3. Broker comes back, ISR recovers, under-replicated partitions return to zero
#
# Exits non-zero if the cluster does not behave as expected, so CI can run it.
set -euo pipefail
cd "$(dirname "$0")/.."

KAFKA="docker exec lab01-kafka-1 /opt/kafka/bin"
BS="localhost:19092"
VICTIM="lab01-kafka-3"

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\n\033[31mDRILL FAILED: %s\033[0m\n' "$*"; exit 1; }

urp_count() {
  $KAFKA/kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions --topic "$1" 2>/dev/null \
    | grep -c "Partition:" || true
}

end_offset_sum() {
  $KAFKA/kafka-get-offsets.sh --bootstrap-server "$BS" --topic "$1" --time -1 \
    | awk -F: '{s += $3} END {print s + 0}'
}

produce() { # topic count
  $KAFKA/kafka-producer-perf-test.sh --topic "$1" --num-records "$2" --record-size 200 --throughput -1 \
    --producer-props bootstrap.servers="$BS" acks=all 2>&1 | tail -1
}

step "Create topics"
$KAFKA/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists --topic payments \
  --partitions 6 --replication-factor 3 --config min.insync.replicas=2
$KAFKA/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists --topic payments-strict \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=3
start_offsets=$(end_offset_sum payments)

step "Write 1000 records with acks=all while all 3 brokers are up"
out=$(produce payments 1000); echo "$out"
[[ "$out" == 1000\ records\ sent* ]] || fail "baseline produce did not complete"

step "Stop $VICTIM"
docker stop "$VICTIM" > /dev/null
for _ in $(seq 1 30); do
  [[ $(urp_count payments) -gt 0 ]] && break
  sleep 2
done
echo "Under-replicated partitions on 'payments': $(urp_count payments)"
[[ $(urp_count payments) -gt 0 ]] || fail "expected under-replicated partitions after stopping a broker"
$KAFKA/kafka-topics.sh --bootstrap-server "$BS" --describe --topic payments | sed -n '2,7p'

step "Write 1000 more records to 'payments' (min.insync.replicas=2): should still succeed"
out=$(produce payments 1000); echo "$out"
[[ "$out" == 1000\ records\ sent* ]] || fail "acks=all writes should survive one broker loss when min.insync.replicas=2"

step "Write to 'payments-strict' (min.insync.replicas=3): should be rejected"
# docker exec needs -i, otherwise the producer sees an empty stdin, sends nothing and exits 0.
strict=$(echo "order-42" | docker exec -i lab01-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BS" --topic payments-strict \
  --producer-property acks=all --producer-property retries=0 --producer-property enable.idempotence=false 2>&1 || true)
if grep -q "NotEnoughReplicas" <<< "$strict"; then
  echo "Rejected as expected: NotEnoughReplicasException"
else
  echo "$strict"
  fail "expected NotEnoughReplicasException on payments-strict"
fi

step "Nothing acknowledged was lost"
written=$(( $(end_offset_sum payments) - start_offsets ))
echo "Records in 'payments' since the drill started: $written"
[[ "$written" -eq 2000 ]] || fail "expected 2000 records, found $written"

step "Start $VICTIM again and wait for the ISR to recover"
docker start "$VICTIM" > /dev/null
for _ in $(seq 1 60); do
  [[ $(urp_count payments) -eq 0 && $(urp_count payments-strict) -eq 0 ]] && break
  sleep 3
done
[[ $(urp_count payments) -eq 0 ]] || fail "partitions still under-replicated after the broker returned"
echo "All partitions fully replicated again."

printf '\n\033[32mDRILL PASSED\033[0m\n'
cat <<'EOF'

What this shows a client:
  - RF=3 with min.insync.replicas=2 and acks=all keeps writing through a broker loss with no data loss.
  - min.insync.replicas equal to the replication factor turns any single broker restart into an outage.
  - Under-replicated partitions are the signal to alert on (see Grafana: http://localhost:3000).
EOF
