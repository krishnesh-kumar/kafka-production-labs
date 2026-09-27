"""Simulate card payments for five merchants, with one incident.

Event time is simulated: 14 minutes of traffic are produced in a few seconds, and the
Streams app windows on each payment's own timestamp, so the demo runs fast but behaves
like real time.

  - every merchant: 1 payment per simulated second, ~4% failures
  - merchant-3:     60% failures between minute 4 and minute 7 (a broken acquirer, say)
"""
import json
import os
import random
import time
import uuid

from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("INPUT_TOPIC", "payments")
MINUTES = int(os.environ.get("SIMULATED_MINUTES", "14"))
MERCHANTS = [f"merchant-{i}" for i in range(1, 6)]
INCIDENT = ("merchant-3", 4 * 60, 7 * 60)

random.seed(7)
producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True,
                     "linger.ms": 20})

start = int(time.time() * 1000) - MINUTES * 60_000  # simulated clock ends "now"
sent = 0
for second in range(MINUTES * 60):
    for merchant in MERCHANTS:
        in_incident = merchant == INCIDENT[0] and INCIDENT[1] <= second < INCIDENT[2]
        fail_rate = 0.60 if in_incident else 0.04
        payment = {
            "paymentId": str(uuid.uuid4()),
            "merchantId": merchant,
            "amountMinor": random.randint(500, 250_000),
            "status": "FAILED" if random.random() < fail_rate else "SUCCEEDED",
            "timestamp": start + second * 1000 + random.randint(0, 999),
        }
        producer.produce(TOPIC, key=merchant, value=json.dumps(payment))
        sent += 1
    producer.poll(0)

producer.flush(30)
print(f"sent {sent} payments covering {MINUTES} simulated minutes; incident on {INCIDENT[0]} "
      f"from minute {INCIDENT[1] // 60} to {INCIDENT[2] // 60}")
