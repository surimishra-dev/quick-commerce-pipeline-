from google.cloud import pubsub_v1
import json
import time
import random
from datetime import datetime, timezone

# ===== GCP SETTINGS =====
PROJECT_ID = "beaming-talent-475009-t2" #"flawless-agency-474210-p4"
TOPIC_ID = "statusEvent"  # Pub/Sub topic name for order status events

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# ===== Possible order statuses =====
ORDER_STATUSES = ["picked_up", "in_transit", "delivered", "cancelled"]

# ===== Function to generate random status event =====
def generate_status_event(order_id, courier_id):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status = random.choice(ORDER_STATUSES)
    return {
        "order_id": order_id,
        "courier_id": courier_id,
        "status": status,
        "ts": ts
    }

# ===== Stream status data continuously =====
try:
    print("🚀 Starting StatusEvent data stream...")
    order_base = 1
    courier_ids = ["C100","C101","C102","C103","C104","C105","C106","C107","C108","C109","C110","C111","C112","C113","C114","C115","C116","C117","C118"]

    while True:
        for i, courier_id in enumerate(courier_ids):
            order_id = order_base + i
            event = generate_status_event(order_id, courier_id)
            data_str = json.dumps(event)
            data_bytes = data_str.encode("utf-8")

            # Publish to Pub/Sub
            future = publisher.publish(topic_path, data=data_bytes)
            print(f"✅ Published: {data_str}")

            time.sleep(1)  # delay between messages

except KeyboardInterrupt:
    print("🛑 StatusEvent stream stopped.")
