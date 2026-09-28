# Kafka production labs

[![lab-01](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/lab-01.yml/badge.svg)](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/lab-01.yml)
[![lab-02](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/lab-02.yml/badge.svg)](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/lab-02.yml)
[![lab-03](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/lab-03.yml/badge.svg)](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/lab-03.yml)
[![lab-04](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/lab-04.yml/badge.svg)](https://github.com/krishnesh-kumar/kafka-production-labs/actions/workflows/lab-04.yml)

These are runnable labs for the Kafka and Debezium CDC failure modes I check first when I review a production setup. Each lab:

- starts with one command, on Docker or Podman;
- ends with a script that breaks something on purpose and checks what happens;
- runs in CI on every push, on both Docker and Podman, so the scripts stay true.
  Transcripts of passing runs are in [docs/results](docs/results).

| Lab | What it shows | Stack |
|---|---|---|
| [01 · Kafka platform](labs/01-kafka-platform) | A 3-broker KRaft cluster and a broker-loss drill. It shows why RF=3 with `min.insync.replicas=2` keeps `acks=all` writes going, and why `min.insync.replicas=3` turns one restart into an outage. It also ships the alerts and dashboard I'd set up first. | Kafka 3.9 (KRaft), Kafka UI, kafka-exporter, Prometheus, Grafana |
| [02 · Debezium CDC + outbox](labs/02-debezium-cdc) | PostgreSQL changes streamed as `c/u/d` events with before and after images, plus tombstones and the transactional outbox. A drill takes Connect down while the database keeps writing, and nothing is lost. | PostgreSQL 16, Debezium 3.0, Kafka Connect |
| [03 · Kafka Streams alerting](labs/03-streams-alerting) | Per-merchant payment failure rate over a 5-minute sliding window, on event time. It raises and clears each alert once, with hysteresis so the alert doesn't flap. Nine TopologyTestDriver tests cover window edges, late events and hysteresis. | Kafka Streams 3.9 (Java 17), Python generator |
| [04 · Health check CLI](labs/04-health-check) | A read-only first pass over a cluster: replication, ISR, unclean election, retention, leader balance and consumer lag. The output is a ranked markdown report ([sample](labs/04-health-check/sample-report.md)). | Python, confluent-kafka AdminClient |

## Run a lab

```bash
git clone https://github.com/krishnesh-kumar/kafka-production-labs.git
cd kafka-production-labs
./labs.sh up 01       # start the lab and wait until every container is healthy
./labs.sh drill 01    # break something on purpose; exits non-zero if a check fails
./labs.sh down 01     # stop it and delete its volumes
```

`./labs.sh list` shows the labs and which one is running. `./labs.sh test` runs the lab 03 and lab 04 unit tests inside containers. The labs use the same host ports (9092, 8080), so `up` refuses to start a lab while another one is running.

`labs.sh` uses Docker when its daemon answers, and Podman otherwise. Set `CONTAINER_ENGINE=docker` or `CONTAINER_ENGINE=podman` to choose. You can also run a lab by hand: each lab's README shows the compose command and the scripts.

**You need:**
- Docker with Compose v2, or Podman with docker-compose v2 as its compose provider.
- bash and python3 for the scripts.
- About 4 GB of free RAM for lab 01, or 2 GB for the others.

### Run on Windows with Podman

1. **Give the Podman machine enough memory.** Lab 01 runs three brokers plus monitoring, so give the machine 6 to 8 GB:

   ```bash
   podman machine stop
   podman machine set --cpus 4 --memory 8192
   podman machine start
   ```

2. **Use docker-compose v2 as the compose provider.** `podman compose` hands the file to an external provider. docker-compose v2 supports `up --wait`, profiles and `depends_on: condition: service_healthy`, which the labs use. Podman Desktop can install it for you (its Compose setup), or download the `docker-compose` binary from the Docker Compose GitHub releases and put it on your `PATH`. Check with `podman compose version`: it prints which provider it runs. If podman-compose is picked instead, point Podman at docker-compose with the `PODMAN_COMPOSE_PROVIDER` environment variable.

3. **Run the scripts from bash:** WSL or Git Bash. Clone inside that shell, so the scripts keep LF line endings (`.gitattributes` enforces this).

4. **Start a lab:**

   ```bash
   CONTAINER_ENGINE=podman ./labs.sh up 01
   CONTAINER_ENGINE=podman ./labs.sh drill 01
   CONTAINER_ENGINE=podman ./labs.sh down 01
   ```

CI runs every lab on rootless Podman with docker-compose v2 on Linux (ubuntu-latest). The Windows steps above follow the same setup but are not run in CI.

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
