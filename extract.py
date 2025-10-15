

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
    for row in cur.fetchall():
        print(row[0])
    column_names = [desc[0] for desc in cur.description]  # get column names from cursor
    df = pd.DataFrame(rows, columns=column_names)
    print("✅ Data loaded into DataFrame!")
    print(df.head())

except MySQLdb.Error as err:
    print(f"❌ Error: {err}")

# finally:
#     if conn.is_connected():
#         conn.close()
#         print("🔒 Connection closed.")

