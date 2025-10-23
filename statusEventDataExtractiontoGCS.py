from google.cloud import pubsub_v1, storage
import json
import pandas as pd
import os
from datetime import datetime
import time

# ===== CONFIGURATION =====
PROJECT_ID = "beaming-talent-475009-t2" #"flawless-agency-474210-p4"
SUBSCRIPTION_ID = "statusEvent-sub"   # 🔹 Pub/Sub subscription for StatusEvent
BUCKET_NAME = "dataproc-staging-asia-south1-297094044725-gxm4u7vu" #"dataproc-staging-asia-south1-925894589695-qxkvzrhv"
LOCAL_DIR = "/tmp/status_data"
BATCH_SIZE = 50          # messages per CSV
UPLOAD_INTERVAL = 30     # seconds between GCS uploads
GCS_PREFIX = "StatusEventData/"

# Ensure local directory exists
os.makedirs(LOCAL_DIR, exist_ok=True)

# ===== Initialize Clients =====
subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# ===== Buffers =====
buffer = []

def callback(message):
    """Callback executed on every Pub/Sub message."""
    global buffer
    try:
        data = json.loads(message.data.decode("utf-8"))
        buffer.append(data)
        message.ack()
        print(f"✅ Received: order_id={data.get('order_id')} courier_id={data.get('courier_id')} status={data.get('status')}")
    except Exception as e:
        print(f"⚠️ Failed to process message: {e}")
        message.nack()

# Subscribe to Pub/Sub
subscriber.subscribe(subscription_path, callback=callback)
print(f"🚀 Listening to Pub/Sub subscription: {subscription_path}")

def flush_to_gcs():
    """Flush buffered data to CSV and upload to GCS."""
    global buffer
    if not buffer:
        return

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_file = f"{LOCAL_DIR}/status_{timestamp}.csv"
    df = pd.DataFrame(buffer)

    # Ensure column order
    columns = ["order_id", "courier_id", "status", "ts"]
    df = df[[c for c in columns if c in df.columns]]

    df.to_csv(csv_file, index=False)
    print(f"💾 Saved {len(buffer)} records → {csv_file}")

    # Upload to GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"{GCS_PREFIX}status_{timestamp}.csv"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(csv_file)
    print(f"☁️ Uploaded to: gs://{BUCKET_NAME}/{blob_name}")

    # Clear local buffer and remove local file
    buffer = []
    os.remove(csv_file)

# ===== Periodic Flusher Loop =====
try:
    while True:
        time.sleep(UPLOAD_INTERVAL)
        flush_to_gcs()
except KeyboardInterrupt:
    print("🛑 Stopping StatusEvent listener…")
    flush_to_gcs()
