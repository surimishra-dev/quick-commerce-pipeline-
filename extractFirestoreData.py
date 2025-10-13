from google.cloud import datastore
import pandas as pd

def fetch_datastore_data(project_id, kind_name):
    """
    Fetch all entities from a Firestore Datastore-mode kind.
    """
    # Initialize Datastore client
    client = datastore.Client(project=project_id)

    # Create a query for the given kind
    query = client.query(kind=kind_name)

    # Fetch results
    results = list(query.fetch())

    if not results:
        print(f"No data found in kind '{kind_name}'.")
        return pd.DataFrame()

    # Convert each entity to a dictionary
    data = []
    for entity in results:
        entity_dict = dict(entity)
        entity_dict["_id"] = entity.key.id_or_name  # include entity key
        data.append(entity_dict)

    # Convert to Pandas DataFrame
    df = pd.DataFrame(data)

    print(f"✅ Fetched {len(df)} entities from kind '{kind_name}'")
    print(df.head())

    return df


if __name__ == "__main__":
    # Replace these with your values
    PROJECT_ID = "flawless-agency-474210-p4"   # your GCP project ID
    KIND_NAME = "InventoryData"                        # your Datastore kind name

    # Fetch data
    df = fetch_datastore_data(PROJECT_ID, KIND_NAME)

    # Optional: Save to CSV
    if not df.empty:
        df.to_csv("datastore_data.csv", index=False)
        print("📁 Data saved to datastore_data.csv")
