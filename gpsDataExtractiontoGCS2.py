from google.cloud import pubsub_v1, storage
import json
import pandas as pd
import os
from datetime import datetime
import time

# ===== CONFIGURATION =====
PROJECT_ID = "flawless-agency-474210-p4"
SUBSCRIPTION_ID = "GPSEvent-sub"  # existing subscription name
BUCKET_NAME = "dataproc-staging-asia-south1-925894589695-qxkvzrhv"
LOCAL_DIR = "/tmp/gps_data"
BATCH_SIZE = 50          # messages per CSV
UPLOAD_INTERVAL = 30     # seconds between GCS uploads
GCS_PREFIX = "GPSEventData/"

os.makedirs(LOCAL_DIR, exist_ok=True)

# ===== Initialize Clients =====
subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# ===== Buffers =====
buffer = []

def callback(message):
    global buffer
    try:
        data = json.loads(message.data.decode("utf-8"))
        buffer.append(data)
        message.ack()
        print(f"✅ Received: {data['courier_id']}")
    except Exception as e:
        print(f"⚠️ Failed to process message: {e}")
        message.nack()

subscriber.subscribe(subscription_path, callback=callback)
print(f"🚀 Listening to Pub/Sub subscription: {subscription_path}")

def flush_to_gcs():
    """Flush buffered data to CSV and upload to GCS."""
    global buffer
    if not buffer:
        return
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_file = f"{LOCAL_DIR}/gps_{timestamp}.csv"
    df = pd.DataFrame(buffer)
    df.to_csv(csv_file, index=False)
    print(f"💾 Saved {len(buffer)} records → {csv_file}")

    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"{GCS_PREFIX}gps_{timestamp}.csv"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(csv_file)
    print(f"☁️ Uploaded to: gs://{BUCKET_NAME}/{blob_name}")

    buffer = []
    os.remove(csv_file)

# ===== Periodic Flusher Loop =====
try:
    while True:
        time.sleep(UPLOAD_INTERVAL)
        flush_to_gcs()
except KeyboardInterrupt:
    print("🛑 Stopping listener…")
    flush_to_gcs()
