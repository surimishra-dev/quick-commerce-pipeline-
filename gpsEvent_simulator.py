from google.cloud import pubsub_v1
import json
import time
import random
from datetime import datetime, timezone

# GCP settings
PROJECT_ID = "flawless-agency-474210-p4"
TOPIC_ID = "GPSEvent"

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Function to simulate random GPS data
def generate_gps_event(courier_id):
    lat_base, lon_base = 12.9716, 77.5946  # Bengaluru
    lat = lat_base + random.uniform(-0.01, 0.01)
    lon = lon_base + random.uniform(-0.01, 0.01)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "courier_id": courier_id,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "ts": ts
    }

# Stream data continuously
try:
    print("🚀 Starting GPS data stream...")
    while True:
        for courier_id in ["C123", "C124", "C125"]:
            event = generate_gps_event(courier_id)
            data_str = json.dumps(event)
            data_bytes = data_str.encode("utf-8")
            
            # Publish to Pub/Sub topic
            future = publisher.publish(topic_path, data=data_bytes)
            print(f"✅ Published: {data_str}")
            
            time.sleep(1)  # delay between messages
except KeyboardInterrupt:
    print("🛑 GPS stream stopped.")
