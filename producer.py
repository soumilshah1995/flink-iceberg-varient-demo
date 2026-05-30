#!/usr/bin/env python3
"""Publish order JSON to Kafka for the Flink SQL -> Iceberg VARIANT pipeline."""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer

BOOTSTRAP = "broker:9092"
TOPIC = "orders_variant"

faker = Faker()
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)


def nested_variant_blob() -> dict:
    return {
        "schema_version": 1,
        "kind": "order",
        "user": {
            "id": str(uuid.uuid4())[:12],
            "labels": [faker.word() for _ in range(random.randint(1, 3))],
            "prefs": {
                "theme": random.choice(["light", "dark"]),
                "notify": random.choice([True, False]),
            },
        },
        "line_items": [
            {
                "sku": faker.bothify(text="??-###").upper(),
                "qty": random.randint(1, 5),
                "unit_price": str(round(random.uniform(1.0, 99.99), 2)),
            }
            for _ in range(random.randint(1, 2))
        ],
        "scores": {f"k{i}": random.randint(0, 100) for i in range(2)},
        "at": datetime.now().isoformat(),
        "note": None if random.random() < 0.2 else faker.sentence(nb_words=4),
    }


def order_event() -> dict:
    return {
        "order_id": str(uuid.uuid4()),
        "site_id": random.choice(["siteA", "siteB", "siteC", "siteD"]),
        "product_name": faker.word(),
        "order_value": str(random.randint(10, 1000)),
        "priority": random.choice(["LOW", "MEDIUM", "HIGH"]),
        "order_date": faker.date_between(start_date="-30d", end_date="today").strftime("%Y-%m-%d"),
        "ts": str(datetime.now().timestamp()),
        "event_variant": nested_variant_blob(),
    }


def main() -> None:
    for _ in range(20):
        order = order_event()
        producer.send(TOPIC, json.dumps(order).encode("utf-8"))
        print(f"{TOPIC}: {order['order_id']} ({order['site_id']})")

    producer.flush()
    print("Done.")


if __name__ == "__main__":
    main()
