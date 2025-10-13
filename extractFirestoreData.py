from pymongo import MongoClient
from urllib.parse import quote_plus

# MongoDB-compatible Firestore connection details
username = "abhranil"  # MongoDB username
password = "ljcVOu0GqPUpc7PBnrsVCzr_Q9kFMy59NBFuiTrKf01_Lf0z"  # Your new password for the MongoDB user
host = "89848341-4b54-406a-b047-a2e911f2ee64.asia-south1.firestore.goog"  # Firestore MongoDB endpoint
database_name = "quick-commerce-inventory"  # Database name

# URL-encode password
encoded_password = quote_plus(password)

# Build the MongoDB URI using the connection string
uri = f"mongodb://{username}:{encoded_password}@{host}:443/{database_name}?loadBalanced=true&tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false"

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
