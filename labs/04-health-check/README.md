# Lab 04 · kafka-healthcheck-lite

A read-only first pass over a Kafka cluster. It uses only describe and list admin APIs, and produces a ranked markdown report: **[sample report](sample-report.md)**.

This is a small, automated slice of the Kafka + CDC Health Check I do by hand. The report ends with the areas an automated pass can't judge.

## Run against lab 01

```bash
(cd ../01-kafka-platform && docker compose up -d --wait)
./scripts/seed-demo-cluster.sh                  # adds realistic mistakes
pip install -r requirements.txt
python -m healthcheck --bootstrap localhost:9092 --out report.md
```

**Against your own cluster** (read-only), pass client settings with `--config`:

```bash
python -m healthcheck --bootstrap broker:9093 \
  --config security.protocol=SASL_SSL --config sasl.mechanism=SCRAM-SHA-512 \
  --config sasl.username=... --config sasl.password=...
```

**In CI:** `--fail-on HIGH` exits 1 when a HIGH or CRITICAL finding exists, so a pipeline can stop on it.

## Checks

| Area | Check | Severity |
|---|---|---|
| Cluster | Fewer than 3 brokers | High |
| Cluster | `unclean.leader.election.enable=true` | High |
| Cluster | `__consumer_offsets` RF below 3 | High |
| Cluster | Auto topic creation on | Medium |
| Cluster | Default RF or `min.insync.replicas` too low | Medium |
| Topics | Offline partitions | Critical |
| Topics | `min.insync.replicas` ≥ RF (one restart blocks `acks=all`) | Critical |
| Topics | RF=1 on a multi-broker cluster | Critical |
| Topics | RF=2 | High |
| Topics | Under-replicated partitions | High |
| Topics | Per-topic unclean election | High |
| Topics | RF=3 with `min.insync.replicas=1` | Medium |
| Topics | Unbounded retention on delete-policy topics | Low |
| Topics | Leader imbalance across brokers | Low |
| Consumers | Groups with lag and no members (abandoned or down) | Medium |
| Consumers | Groups above 10,000 lag | Medium |

All rules live in `healthcheck/checks.py` as pure functions over a snapshot, and are unit-tested in `tests/` with no Kafka needed:

```bash
pip install pytest && python -m pytest -q tests
```
