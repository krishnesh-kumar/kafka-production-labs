#!/usr/bin/env bash
# Add some realistic mistakes to the lab 01 cluster so the health check has something to find.
# Requires lab 01 running (docker compose -f ../01-kafka-platform/docker-compose.yml up -d --wait).
set -euo pipefail
K="docker exec lab01-kafka-1 /opt/kafka/bin"
BS="localhost:19092"

create() { $K/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists "$@" > /dev/null; }

create --topic payments        --partitions 6 --replication-factor 3 --config min.insync.replicas=2
create --topic orders-legacy   --partitions 3 --replication-factor 1 --config min.insync.replicas=1
create --topic ledger-strict   --partitions 3 --replication-factor 3 --config min.insync.replicas=3
create --topic audit-log       --partitions 3 --replication-factor 3 --config retention.ms=-1
create --topic clickstream     --partitions 6 --replication-factor 3 --config min.insync.replicas=1

# A consumer group that read part of the topic and then went away: lag with no members.
$K/kafka-producer-perf-test.sh --topic payments --num-records 5000 --record-size 200 --throughput -1 \
  --producer-props bootstrap.servers="$BS" acks=all > /dev/null
$K/kafka-console-consumer.sh --bootstrap-server "$BS" --topic payments --group invoice-service \
  --from-beginning --max-messages 1500 > /dev/null 2>&1
echo "Seeded: 5 topics (3 with deliberate misconfigurations) and an abandoned consumer group."
