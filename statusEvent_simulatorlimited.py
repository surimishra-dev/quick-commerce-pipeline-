from google.cloud import pubsub_v1
import json
import time
import random
from datetime import datetime, timezone

PROJECT_ID = "beaming-talent-475009-t2"
TOPIC_ID = "statusEvent"

ORDER_STATUSES = ["picked_up", "in_transit", "delivered", "cancelled"]


def generate_status_event(order_id, courier_id):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status = random.choice(ORDER_STATUSES)
    return {
        "order_id": order_id,
        "courier_id": courier_id,
        "status": status,
        "ts": ts,
    }


def publish_status_events(duration_seconds=10):
    """Publishes random order status events to Pub/Sub for a fixed time window."""
    print("🚀 Starting Pub/Sub publishing for", duration_seconds, "seconds...", flush=True)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    courier_ids = [
        "C100","C101","C102","C103","C104","C105","C106","C107","C108",
        "C109","C110","C111","C112","C113","C114","C115","C116","C117","C118"
    ]

    start_time = time.time()
    published_count = 0

    while time.time() - start_time < duration_seconds:
        for i, courier_id in enumerate(courier_ids):
            if time.time() - start_time >= duration_seconds:
                break

            order_id = 1000 + i
            event = generate_status_event(order_id, courier_id)
            data_bytes = json.dumps(event).encode("utf-8")

            publisher.publish(topic_path, data=data_bytes)
            print(f"✅ Published: {event}", flush=True)
            published_count += 1
            time.sleep(0.5)

    print(f"🛑 Finished publishing {published_count} messages after {duration_seconds} seconds.", flush=True)
