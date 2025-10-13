from google.cloud import datastore
import pandas as pd

# Initialize Datastore client
client = datastore.Client()  # uses VM default credentials or JSON key

# Set the kind (Datastore uses 'Kind' instead of 'Collection')
kind = "InventoryData"  # Replace with your Datastore kind name

# Create a query
query = client.query(kind=kind)

# Fetch all results
results = list(query.fetch())

# Convert results to list of dictionaries
data = []
for entity in results:
    # Each entity is a dictionary-like object
    entity_dict = dict(entity)
    entity_dict["id"] = entity.key.id_or_name  # include the key ID
    data.append(entity_dict)

# Convert to Pandas DataFrame
df = pd.DataFrame(data)

# Display data
print(f"Fetched {len(df)} entities from kind '{kind}'")
print(df.head())

# Optional: save to CSV
# df.to_csv("datastore_data.csv", index=False)
