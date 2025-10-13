import pandas as pd

from google.cloud import datastore

client = datastore.Client()

# Option 1: Use VM's default service account (if running inside GCP VM)
db = datastore.Client()

# -----------------------------
# Set the collection name
# -----------------------------
collection_name = "Inventory"  # Replace with your collection
collection_ref = db.collection(collection_name)
print("Connected to Firestore")
# -----------------------------
# Fetch all documents
# -----------------------------
docs = collection_ref.stream()

# -----------------------------
# Convert documents to list of dicts
# -----------------------------
data = [doc.to_dict() for doc in docs]

# -----------------------------
# Convert to Pandas DataFrame
# -----------------------------
df = pd.DataFrame(data)

# -----------------------------
# Display DataFrame
# -----------------------------
print(f"Fetched {len(df)} documents from collection '{collection_name}'")
print(df.head())

# -----------------------------
# Optional: Save to CSV
# -----------------------------
# df.to_csv("firestore_data.csv", index=False)
