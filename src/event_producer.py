"""Clickstream simulator for Azure Event Hubs.

Sends synthetic page-view / click / purchase events so the streaming pipeline has
something to consume. Deliberately emits a small proportion of duplicate and
late-arriving events so the Silver layer's dedupe and watermark logic has real work
to do rather than passing trivially.

Usage:
    export EVENTHUB_CONN="Endpoint=sb://<ns>.servicebus.windows.net/;..."
    python event_producer.py --events-per-second 20 --duration 600
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from azure.eventhub import EventData, EventHubProducerClient

EVENT_TYPES = ["page_view", "click", "scroll", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [50, 30, 12, 6, 2]
COUNTRIES = ["IN", "US", "GB", "DE", "SG", "AU", "CA", "JP"]
DEVICES = ["mobile", "desktop", "tablet"]
PAGES = [
    "/", "/products", "/products/laptops", "/products/phones",
    "/cart", "/checkout", "/account", "/support",
]

DUPLICATE_RATE = 0.03   # at-least-once delivery in the real world
LATE_EVENT_RATE = 0.05  # events that arrive out of order
MALFORMED_RATE = 0.01   # exercises the quarantine path


def build_event(user_pool):
    now = datetime.now(timezone.utc)

    # A slice of events carry a timestamp minutes in the past.
    if random.random() < LATE_EVENT_RATE:
        event_time = now - timedelta(minutes=random.randint(1, 15))
    else:
        event_time = now

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0],
        "user_id": random.choice(user_pool),
        "session_id": str(uuid.uuid4())[:8],
        "page_url": "https://shop.example.com" + random.choice(PAGES),
        "referrer": random.choice(["google", "direct", "email", "social"]),
        "country": random.choice(COUNTRIES),
        "device": random.choice(DEVICES),
        "duration_ms": random.randint(200, 30000),
        "event_time": event_time.isoformat(),
    }

    # Occasionally emit something the quality rules should reject.
    if random.random() < MALFORMED_RATE:
        broken = random.choice(["event_type", "duration_ms"])
        event["event_type"] = "??" if broken == "event_type" else event["event_type"]
        event["duration_ms"] = -1 if broken == "duration_ms" else event["duration_ms"]

    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eventhub-name", default="clickstream")
    parser.add_argument("--events-per-second", type=int, default=10)
    parser.add_argument("--duration", type=int, default=300, help="seconds")
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args()

    conn_str = os.environ.get("EVENTHUB_CONN")
    if not conn_str:
        sys.exit("EVENTHUB_CONN environment variable is not set.")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=conn_str, eventhub_name=args.eventhub_name
    )

    user_pool = [f"user_{i:04d}" for i in range(500)]
    sent = 0
    started = time.time()

    print(f"Sending ~{args.events_per_second}/s for {args.duration}s to '{args.eventhub_name}'")

    try:
        with producer:
            while time.time() - started < args.duration:
                batch = producer.create_batch()
                for _ in range(args.batch_size):
                    event = build_event(user_pool)
                    batch.add(EventData(json.dumps(event)))
                    sent += 1

                    if random.random() < DUPLICATE_RATE:
                        batch.add(EventData(json.dumps(event)))
                        sent += 1

                producer.send_batch(batch)
                elapsed = time.time() - started
                expected = elapsed * args.events_per_second
                if sent > expected:
                    time.sleep((sent - expected) / args.events_per_second)

                print(f"  sent={sent}  elapsed={elapsed:.0f}s", end="\r", flush=True)
    except KeyboardInterrupt:
        print("\nStopped by user.")

    print(f"\nDone. {sent} events sent in {time.time() - started:.0f}s.")


if __name__ == "__main__":
    main()
