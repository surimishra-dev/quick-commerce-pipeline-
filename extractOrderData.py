# extractOrderData.py

import pandas as pd
from datetime import datetime
from google.cloud.sql.connector import Connector
import sqlalchemy
from google.cloud import storage

PROJECT_ID = "flawless-agency-474210-p4"
INSTANCE_CONNECTION_NAME = "flawless-agency-474210-p4:asia-south1:quickcommerce89"  # format: project:region:instance
DB_USER = "abhranil"
DB_PASS = "Abhranil@89"
DB_NAME = "QuickCommerce"
BUCKET_NAME = "dataproc-staging-asia-south1-925894589695-qxkvzrhv"
GCS_FOLDER = "OrdersData"

def run_extract():
    connector = Connector()

    # Cloud SQL connection function
    def getconn():
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pymysql",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME
        )
        return conn

    engine = sqlalchemy.create_engine("mysql+pymysql://", creator=getconn)

    # Fetch data
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM Orders", conn)
        print(f"✅ Fetched {len(df)} records")
        print(df.head())

    # Save to local temp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_csv_path = f"orders_data_{timestamp}.csv"
    df.to_csv(local_csv_path, index=False)
    print(f"✅ Data saved locally as {local_csv_path}")


    # Upload to GCS
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{GCS_FOLDER}/{local_csv_path.split('/')[-1]}")
    blob.upload_from_filename(local_csv_path)
    print(f"✅ Uploaded to GCS bucket '{BUCKET_NAME}' at '{GCS_FOLDER}/{local_csv_path.split('/')[-1]}'")
