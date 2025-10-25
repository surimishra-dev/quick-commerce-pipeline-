from google.cloud import pubsub_v1
import json
import time
import random
from datetime import datetime, timezone

def publish_status_events_to_pubsub(duration_seconds=10):
    """
    Publishes random order status events to a Pub/Sub topic for a fixed duration.
    Used inside Airflow DAG.
    """
    PROJECT_ID = "beaming-talent-475009-t2"
    TOPIC_ID = "statusEvent"

    # Initialize Pub/Sub publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    ORDER_STATUSES = ["picked_up", "in_transit", "delivered", "cancelled"]

    def generate_status_event(order_id, courier_id):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        status = random.choice(ORDER_STATUSES)
        return {
            "order_id": order_id,
            "courier_id": courier_id,
            "status": status,
            "ts": ts
        }

    courier_ids = [
        "C100","C101","C102","C103","C104","C105",
        "C106","C107","C108","C109","C110","C111",
        "C112","C113","C114","C115","C116","C117","C118"
    ]

    print(f"🚀 Starting Pub/Sub publishing for {duration_seconds} seconds...")
    start_time = time.time()
    order_base = 1
    message_count = 0

    try:
        while (time.time() - start_time) < duration_seconds:
            for i, courier_id in enumerate(courier_ids):
                if (time.time() - start_time) >= duration_seconds:
                    break

                order_id = order_base + i
                event = generate_status_event(order_id, courier_id)
                data = json.dumps(event).encode("utf-8")

                publisher.publish(topic_path, data=data)
                print(f"✅ Published: {event}")
                message_count += 1
                time.sleep(1)  # Delay between messages

        print(f"🛑 Finished publishing {message_count} messages after {duration_seconds} seconds.")

    except Exception as e:
        print(f"❌ Error during publishing: {e}")
        raise e
