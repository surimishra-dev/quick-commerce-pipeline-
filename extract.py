    

# Create a cursor
#cur = db.cursor()

# Run your query
#cur.execute("SELECT * FROM Orders")

# Fetch all rows
#rows = cur.fetchall()
#for row in rows:
#    print(row)

#db.close()


import MySQLdb
import pandas as pd
from datetime import datetime 
# from google.cloud import storage

try:
    conn = MySQLdb.connect(
        host="10.29.208.3",   # replace with your Cloud SQL public IP
        user="suryanshm",
        password="Suri_mishra@12",
        database="quick-commerce",
        port=3306
    )

    print("✅ Connected to Cloud SQL!")
    cur=conn.cursor()
    cur.execute("SELECT * FROM orders")
    rows=cur.fetchall()
    if not rows:
        print("⚠️ No records found in 'orders' table.")
    else:
        # Print sample output (first column as you had)
        print(f"✅ Fetched {len(rows)} records.")
        for row in rows[:5]:  # print first few rows for verification
            print(row[0])

        # ---------- 3️⃣ Convert to DataFrame ----------
        column_names = [desc[0] for desc in cur.description]  # Get column names
        df = pd.DataFrame(rows, columns=column_names)

        print("✅ Data successfully loaded into DataFrame!")
        print(df.head())

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_csv_path = f"orders_data_{timestamp}.csv"
    df.to_csv(local_csv_path, index=False)
    print(f"✅ Data saved locally as {local_csv_path}")

    bucket_name = "dataproc-staging-asia-south1-297094044725-gxm4u7vu"
    destination_blob = f"orders_data/{local_csv_path}"  # folder + file name

        # Create a GCS client (ensure your VM has permissions or a service account key)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

        # Upload the CSV file to GCS
    blob.upload_from_filename(local_csv_path)
    print(f"✅ File uploaded to GCS bucket '{bucket_name}' at '{destination_blob}'")

except MySQLdb.Error as err:
    print(f"❌ Error: {err}")

# finally:
#     if conn.is_connected():
#         conn.close()
#         print("🔒 Connection closed.")

