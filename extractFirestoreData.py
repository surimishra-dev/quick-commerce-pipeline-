from google.cloud import datastore
import pandas as pd

# Initialize client (uses default VM credentials)
client = datastore.Client(project="flawless-agency-474210-p4")

# Replace with your Kind name
kind_name = "InventoryData"

# Query all entities
query = client.query(kind=kind_name)
entities = list(query.fetch())

# Convert to list of dictionaries
rows = []
for e in entities:
    record = dict(e)
    record["_id"] = e.key.id_or_name  # store key as id
    rows.append(record)

# Convert to DataFrame
df = pd.DataFrame(rows)

print(f"Fetched {len(df)} entities from kind '{kind_name}'")
print(df.head())

