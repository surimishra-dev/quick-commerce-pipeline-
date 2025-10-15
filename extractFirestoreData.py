from pymongo import MongoClient
from urllib.parse import quote_plus
import pandas as pd
from google.cloud import storage
import os

# MongoDB-compatible Firestore connection details
username = "abhranil"  # MongoDB username
password = "ljcVOu0GqPUpc7PBnrsVCzr_Q9kFMy59NBFuiTrKf01_Lf0z"  # Your new password for the MongoDB user
host = "89848341-4b54-406a-b047-a2e911f2ee64.asia-south1.firestore.goog"  # Firestore MongoDB endpoint
database_name = "quick-commerce-inventory"  # Database name
collection_name = "InventoryData"

# URL-encode password
encoded_password = quote_plus(password)

# Build the MongoDB URI using the connection string
uri = f"mongodb://{username}:{encoded_password}@{host}:443/{database_name}?loadBalanced=true&tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false"

GCS bucket configuration
CSV_FILE = "inventory_data.csv"
BUCKET_NAME = "dataproc-staging-asia-south1-925894589695-qxkvzrhv"  # 🔹 Replace with your actual bucket name
DESTINATION_BLOB_NAME = "Inventorydata/inventory_data.csv

# Create a MongoClient instance
client = MongoClient(uri, serverSelectionTimeoutMS=10000)  # Timeout in milliseconds

# Try connecting to the server
try:
    # Ping the database to test the connection
    client.admin.command("ping")
    print("Connected to MongoDB-compatible Firestore successfully!")
    
    # Access the specific collection (InventoryData)
    db = client[database_name]
    collection = db["InventoryData"]

    # Fetch the first 3 documents from the collection
    docs = list(collection.find())
    
    # Print the documents
    for doc in docs:
        print(doc)
        
except Exception as e:
    print("Failed to connect or fetch data:", e)

 # Convert documents to Pandas DataFrame
df = pd.DataFrame(docs)
print("Print the Dataframe")
print(df)

 #Save DataFrame to CSV
df.to_csv(CSV_FILE, index=False)
print(f"💾 Data saved locally to: {CSV_FILE}")

#Upload CSV to GCS bucket
print("☁️ Uploading CSV to GCS bucket...")
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(DESTINATION_BLOB_NAME)
blob.upload_from_filename(CSV_FILE)
print(f"✅ File uploaded to gs://{BUCKET_NAME}/{DESTINATION_BLOB_NAME}")

except Exception as e:
    print("❌ Failed to connect or fetch data:", e)

finally:
    try:
        client.close()
        print("🔒 MongoDB connection closed.")
    except:
        pass


