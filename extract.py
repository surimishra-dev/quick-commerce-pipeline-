

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
    cur.execute("SELECT * FROM Orders")
    for row in cur.fetchall():
        print row[0]

except MySQLdb.Error as err:
    print(f"❌ Error: {err}")

# finally:
#     if conn.is_connected():
#         conn.close()
#         print("🔒 Connection closed.")

