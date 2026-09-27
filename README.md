# Kafka production labs

[![labs](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/labs.yml/badge.svg)](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/labs.yml)

These are runnable labs for the Kafka and Debezium CDC failure modes I check first when I review a production setup. Each lab:

- starts with one `docker compose` command;
- ends with a script that breaks something on purpose and checks what happens;
- runs in CI on every push, so the scripts stay true.

| Lab | What it shows | Stack |
|---|---|---|
| [01 · Kafka platform](labs/01-kafka-platform) | A 3-broker KRaft cluster and a broker-loss drill. It shows why RF=3 with `min.insync.replicas=2` keeps `acks=all` writes going, and why `min.insync.replicas=3` turns one restart into an outage. It also ships the alerts and dashboard I'd set up first. | Kafka 3.9 (KRaft), Kafka UI, kafka-exporter, Prometheus, Grafana |
| [02 · Debezium CDC + outbox](labs/02-debezium-cdc) | PostgreSQL changes streamed as `c/u/d` events with before and after images, plus tombstones and the transactional outbox. A drill takes Connect down while the database keeps writing, and nothing is lost. | PostgreSQL 16, Debezium 3.0, Kafka Connect |
| [03 · Kafka Streams alerting](labs/03-streams-alerting) | Per-merchant payment failure rate over a 5-minute sliding window, on event time. It raises and clears each alert once, with hysteresis so the alert doesn't flap. Nine TopologyTestDriver tests cover window edges, late events and hysteresis. | Kafka Streams 3.9 (Java 17), Python generator |
| [04 · Health check CLI](labs/04-health-check) | A read-only first pass over a cluster: replication, ISR, unclean election, retention, leader balance and consumer lag. The output is a ranked markdown report ([sample](labs/04-health-check/sample-report.md)). | Python, confluent-kafka AdminClient |

## Run a lab

```bash
git clone https://github.com/krishnesh-kumar/kafka-production-labs.git
cd kafka-production-labs/labs/01-kafka-platform
docker compose up -d --wait
./scripts/failure-drill.sh
docker compose down -v
```

**You need:**
- Docker with Compose v2.
- bash and python3 for the scripts.
- About 4 GB of free RAM for lab 01, or 2 GB for the others.

The labs use the same host ports (9092, 8080), so run one at a time.

**Simplified on purpose:**
- PLAINTEXT listeners and no auth.
- JSON instead of Avro or Protobuf.
- Single-broker clusters in labs 02 and 03.

Each lab's README has a "Production notes" section on what changes for real systems.

## Why these labs exist

Most Kafka incidents I see aren't exotic. They come from four settings problems:

- a topic whose `min.insync.replicas` equals its replication factor;
- a replication slot quietly holding WAL while a connector is down;
- consumer lag nobody alerts on;
- an alert that fires a thousand times instead of once.

These labs reproduce those problems in a few minutes, and show the fix.

## Work with me

I'm Krishnesh Kumar, a lead backend engineer. My full-time job is running production Kafka on Kubernetes (Strimzi) and the stream processing built on it: ingestion, Kafka Streams, and Debezium / Kafka Connect CDC.

Alongside that role I take a small number of scoped engagements for fintech, e-commerce, logistics and SaaS teams:

1. **Kafka + CDC Health Check.** A fixed-price, read-only review of your cluster, topics, producers, consumers and connectors, with a risk-ranked report and a readout call. Lab 04 is a small, automated slice of it.
2. **Debezium / Kafka Connect CDC pipeline builds.**
3. **Kafka Streams features:** windows, state stores, alerting.

I'm based in India (IST) and work async with teams in Europe and the US. You can reach me on [LinkedIn](https://www.linkedin.com/in/krishneshkumar/).

## License

MIT
